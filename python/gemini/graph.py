from collections import defaultdict
import logging

from modelforge import Model, merge_strings, split_strings, assemble_sparse_matrix, \
    disassemble_sparse_matrix, register_model
import numpy
from scipy.sparse import csr_matrix

from gemini.cassandra_utils import get_db


@register_model
class ConnectedComponentsModel(Model):
    """
    Model to store connected components.
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
            indptr[i + 1] = indptr[i] + len(element)
        self.element_to_buckets = csr_matrix((data, indices, indptr))
        return self

    def _load_tree(self, tree):
        self.id_to_cc = tree["cc"]
        self.id_to_cc[0]  # do not remove - loads the array from disk
        self.id_to_element = split_strings(tree["elements"])
        self.element_to_buckets = assemble_sparse_matrix(tree["buckets"])

    def dump(self):
        return "Number of connected components: %s\nNumber of unique elements: %s" % (
            len(numpy.unique(self.id_to_cc)), len(self.id_to_element))

    def _generate_tree(self):
        return {"cc": self.id_to_cc, "elements": merge_strings(self.id_to_element),
                "buckets": disassemble_sparse_matrix(self.element_to_buckets)}


def ccgraph(args):
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
