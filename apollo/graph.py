from collections import defaultdict
from itertools import chain
import logging
from uuid import uuid4

from igraph import Graph

from modelforge import Model, merge_strings, split_strings, assemble_sparse_matrix, \
    disassemble_sparse_matrix, register_model
from modelforge.progress_bar import progress_bar
import numpy
from pyspark.sql.types import Row
from scipy.sparse import csr_matrix
from sourced.ml.utils import create_spark

from apollo.cassandra_utils import get_db, patch_tables
from apollo.query import weighted_jaccard


@register_model
class ConnectedComponentsModel(Model):
    """
    Model to store the connected components.
    """
    NAME = "connected_components"

    def construct(self, connected_components, element_to_buckets, element_to_id):
        self.id_to_cc = numpy.zeros(len(element_to_id), dtype=numpy.uint32)
        for cc, ids in connected_components.items():
            for id_ in ids:
                self.id_to_cc[id_] = cc
        self.id_to_element = [None] * len(element_to_id)
        for k, v in element_to_id.items():
            self.id_to_element[v] = k
        data = numpy.ones(sum(map(len, element_to_buckets)), dtype=numpy.uint8)
        indices = numpy.zeros(len(data), dtype=numpy.uint32)
        indptr = numpy.zeros(len(element_to_buckets) + 1, dtype=numpy.uint32)
        pos = 0
        for i, element in enumerate(element_to_buckets):
            indices[pos:(pos + len(element))] = element
            pos += len(element)
            indptr[i + 1] = pos
        self.id_to_buckets = csr_matrix((data, indices, indptr))
        return self

    def _load_tree(self, tree):
        self.id_to_cc = tree["cc"]
        self.id_to_cc[0]  # do not remove - loads the array from disk
        self.id_to_element = split_strings(tree["elements"])
        self.id_to_buckets = assemble_sparse_matrix(tree["buckets"])

    def dump(self):
        return "Number of connected components: %s\nNumber of unique elements: %s" % (
            len(numpy.unique(self.id_to_cc)), len(self.id_to_element))

    def _generate_tree(self):
        return {"cc": self.id_to_cc, "elements": merge_strings(self.id_to_element),
                "buckets": disassemble_sparse_matrix(self.id_to_buckets)}


def find_connected_components(args):
    log = logging.getLogger("graph")
    session = get_db(args)
    table = args.tables["hashtables"]
    rows = session.execute("SELECT DISTINCT hashtable FROM %s" % table)
    hashtables = sorted(r.hashtable for r in rows)
    log.info("Detected %d hashtables", len(hashtables))
    buckets = []
    element_ids = {}
    prev_len = 0
    for hashtable in hashtables:
        rows = session.execute(
            "SELECT sha1, value FROM %s WHERE hashtable=%d" % (table, hashtable))
        band = None
        bucket = []
        for row in rows:
            eid = element_ids.setdefault(row.sha1, len(element_ids))
            if row.value != band:
                if band is not None:
                    buckets.append(bucket.copy())
                    bucket.clear()
                band = row.value
                bucket.append(eid)
                continue
            bucket.append(eid)
        if bucket:
            buckets.append(bucket)
        log.info("Fetched %d, %d buckets", hashtable, len(buckets) - prev_len)
        prev_len = len(buckets)

    element_to_buckets = [[] for _ in range(len(element_ids))]
    for i, bucket in enumerate(buckets):
        for element in bucket:
            element_to_buckets[element].append(i)

    # Statistics about buckets
    levels = (logging.ERROR, logging.INFO)
    log.info("Number of buckets: %d", len(buckets))
    log.log(levels[len(element_ids) >= len(buckets[0])],
            "Number of elements: %d", len(element_ids))
    epb = sum(map(len, buckets)) / len(buckets)
    log.log(levels[epb >= 1], "Average number of elements per bucket: %.1f", epb)
    nb = min(map(len, element_to_buckets))
    log.log(levels[nb == len(hashtables)], "Min number of buckets per element: %s", nb)
    nb = max(map(len, element_to_buckets))
    log.log(levels[nb == len(hashtables)], "Max number of buckets per element: %s", nb)
    log.info("Running CC analysis")

    unvisited_buckets = set(range(len(buckets)))
    connected_components_element = defaultdict(set)

    cc_id = 0  # connected component counter
    while unvisited_buckets:
        pending = {unvisited_buckets.pop()}
        while pending:
            bucket = pending.pop()
            elements = buckets[bucket]
            connected_components_element[cc_id].update(elements)
            for element in elements:
                element_buckets = element_to_buckets[element]
                for b in element_buckets:
                    if b in unvisited_buckets:
                        pending.add(b)
                        unvisited_buckets.remove(b)
        # increase number of connected components
        cc_id += 1
    log.info("CC number: %d", len(connected_components_element))

    log.info("Writing %s", args.output)
    ConnectedComponentsModel() \
        .construct(connected_components_element, element_to_buckets, element_ids) \
        .save(args.output)


def dumpcc(args):
    model = ConnectedComponentsModel().load(args.input)
    ccs = defaultdict(list)
    for i, cc in enumerate(model.id_to_cc):
        ccs[cc].append(i)
    for _, cc in sorted(ccs.items()):
        print(" ".join(model.id_to_element[i] for i in cc))


@register_model
class CommunitiesModel(Model):
    """
    Model to store the node communities.
    """
    NAME = "communities"

    def construct(self, communities, id_to_element):
        self.communities = communities
        self.id_to_element = id_to_element
        return self

    def _load_tree(self, tree):
        self.id_to_element = split_strings(tree["elements"])
        data, indptr = tree["data"], tree["indptr"]
        self.communities = [data[i:j] for i, j in zip(indptr, indptr[1:])]

    def _generate_tree(self):
        size = sum(map(len, self.communities))
        data = numpy.zeros(size, dtype=numpy.uint32)
        indptr = numpy.zeros(len(self.communities) + 1, dtype=numpy.int64)
        pos = 0
        for i, community in enumerate(self.communities):
            data[pos:pos + len(community)] = community
            pos += len(community)
            indptr[i + 1] = pos
        return {"data": data, "indptr": indptr, "elements": merge_strings(self.id_to_element)}


def detect_communities(args):
    log = logging.getLogger("cmd")
    ccsmodel = ConnectedComponentsModel().load(args.input)
    log.info("Building the connected components")
    ccs = defaultdict(list)
    for i, c in enumerate(ccsmodel.id_to_cc):
        ccs[c].append(i)
    buckmat = ccsmodel.id_to_buckets
    buckindices = buckmat.indices
    buckindptr = buckmat.indptr
    total_nvertices = buckmat.shape[0]
    linear = args.edges in ("linear", "1")
    graphs = []
    communities = []
    if not linear:
        log.info("Transposing the matrix")
        buckmat_csc = buckmat.T.tocsr()
    fat_ccs = []
    for vertices in ccs.values():
        if len(vertices) == 1:
            continue
        if len(vertices) == 2:
            communities.append(vertices)
            continue
        fat_ccs.append(vertices)
    log.info("Building %d graphs", len(fat_ccs))
    for vertices in progress_bar(fat_ccs, log, expected_size=len(fat_ccs)):
        if linear:
            edges = []
            weights = []
            bucket_weights = buckmat.sum(axis=0)
            buckets = set()
            for i in vertices:
                for j in range(buckindptr[i], buckindptr[i + 1]):
                    bucket = buckindices[j]
                    weights.append(bucket_weights[0, bucket])
                    bucket += total_nvertices
                    buckets.add(bucket)
                    edges.append((str(i), str(bucket)))
        else:
            edges = set()
            weights = None
            buckets = set()
            for i in vertices:
                for j in range(buckindptr[i], buckindptr[i + 1]):
                    buckets.add(buckindices[j])
            for bucket in buckets:
                buckverts = \
                    buckmat_csc.indices[buckmat_csc.indptr[bucket]:buckmat_csc.indptr[bucket + 1]]
                for i, x in enumerate(buckverts):
                    for y in buckverts:
                        if x < y:
                            edges.add((str(x), str(y)))
            buckets.clear()
            edges = list(edges)
        graph = Graph(directed=False)
        graph.add_vertices(list(map(str, vertices + list(buckets))))
        graph.add_edges(edges)
        graph.edge_weights = weights
        graphs.append(graph)
    log.info("Launching the community detection")
    detector = CommunityDetector(algorithm=args.algorithm, config=args.params)
    if not args.no_spark:
        spark = create_spark("cmd-%s" % uuid4(), args).sparkContext
        communities.extend(spark.parallelize(graphs).flatMap(detector).collect())
    else:
        communities.extend(chain.from_iterable(progress_bar(
            (detector(g) for g in graphs), log, expected_size=len(graphs))))
    log.info("Overall communities: %d", len(communities))
    log.info("Average community size: %.1f", numpy.mean([len(c) for c in communities]))
    log.info("Median community size: %.1f", numpy.median([len(c) for c in communities]))
    log.info("Max community size: %d", max(map(len, communities)))
    log.info("Writing %s", args.output)
    CommunitiesModel().construct(communities, ccsmodel.id_to_element).save(args.output)


class CommunityDetector:
    def __init__(self, algorithm, config):
        self.algorithm = algorithm
        self.config = config

    def __call__(self, graph):
        action = getattr(graph, "community_" + self.algorithm)
        if self.algorithm == "infomap":
            kwargs = {"edge_weights": graph.edge_weights}
        elif self.algorithm == "leading_eigenvector_naive":
            kwargs = {}
        else:
            kwargs = {"weights": graph.edge_weights}
        if self.algorithm == "edge_betweenness":
            kwargs["directed"] = False
        result = action(**kwargs, **self.config)

        if hasattr(result, "as_clustering"):
            result = result.as_clustering()

        output = [[] for _ in range(len(result.sizes()))]
        for i, memb in enumerate(result.membership):
            output[memb].append(int(graph.vs[i]["name"]))

        return output


def dumpcmd(args):
    model = CommunitiesModel().load(args.input)
    id_to_element = model.id_to_element
    for community in model.communities:
        s = " ".join(id_to_element[i] for i in community if i < len(id_to_element))
        if s:
            print(s)


class CommunityEvaluator:
    def __init__(self, threshold, vocabulary_size):
        self.threshold = threshold
        self.vocabulary_size = vocabulary_size

    def __call__(self, community):
        cid, contents = community
        elements = defaultdict(list)
        for t in contents:
            elements[t[0]].append(t[1:])
        if len(elements) == 1:
            return (0,) * 4
        for key, vals in elements.items():
            vec  = numpy.zeros(self.vocabulary_size, dtype=numpy.float32)
            for i, w in vals:
                vec[i] = w
            elements[key] = vec
        misses = 0
        loss = 0
        for x, e1 in elements.items():
            for y, e2 in elements.items():
                if x >= y:
                    continue
                sim = weighted_jaccard(e1, e2)
                if sim < self.threshold:
                    loss += (sim - self.threshold) ** 2
                    misses += 1
        count = len(elements) * (len(elements) - 1) / 2
        return misses, misses / count, loss, loss / count


def evaluate_communities(args):
    log = logging.getLogger("evalcc")
    model = CommunitiesModel().load(args.input)
    patch_tables(args)
    spark = create_spark("evalcc-%s" % uuid4(), args)
    log.info("Preparing the communities' RDD")
    items = []
    for i, c in progress_bar(enumerate(model.communities), log,
                             expected_size=len(model.communities)):
        for m in c:
            if m < len(model.id_to_element):
                items.append(Row(sha1=model.id_to_element[m], community=i))
    log.info("Running")
    items_in_spark = spark.sparkContext.parallelize(items).toDF()
    bags = spark \
        .read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=args.tables["bags"], keyspace=args.keyspace) \
        .load()
    log.info("Loaded the bags, calculating the vocabulary")
    vocabulary = bags.drop("sha1", "value").distinct().rdd.map(lambda x: x.item).collect()
    vocabulary = {v: i for i, v in enumerate(vocabulary)}
    log.info("Vocabulary size: %d", len(vocabulary))
    element_to_id = {e: i for i, e in enumerate(model.id_to_element)}
    metrics = items_in_spark.join(bags, "sha1").rdd \
        .map(lambda r: (r.community, (element_to_id[r.sha1], vocabulary[r.item], r.value))) \
        .groupByKey() \
        .map(CommunityEvaluator(args.threshold, len(vocabulary))) \
        .reduce(lambda v1, v2: [v1[i] + v2[i] for i in range(4)])
    log.info("Total misses: %d", metrics[0])
    log.info("Average normalized misses: %f", metrics[1] / len(model.communities))
    log.info("Total loss: %f", metrics[2])
    log.info("Average normalized loss: %f", numpy.sqrt(metrics[3] / len(model.communities)))
