import logging
import os
from uuid import uuid4


from sourced.ml.engine import create_engine
from sourced.ml.repo2 import wmhash
from sourced.ml.repo2.base import UastExtractor, Transformer, Cacher, UastDeserializer, Engine
from pyspark.sql.types import Row

from apollo import cassandra_utils


class BagsSaver(Transformer):
    def __init__(self, keyspace, table, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace
        self.table = table

    def __call__(self, head):
        rows = head.flatMap(self.explode)
        if self.explained:
            self._log.info("toDebugString():\n%s", rows.toDebugString().decode())
        rows.toDF() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=self.table, keyspace=self.keyspace) \
            .save()

    def explode(self, record):
        key = record[0]
        for col, val in record[1].items():
            yield Row(sha1=key, item=col, value=float(val))


class MetadataSaver(Transformer):
    def __init__(self, keyspace, table, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace
        self.table = table

    def __call__(self, head):
        rows = head.map(lambda x: Row(sha1=x.blob_id, url=self.format_url(x)))
        if self.explained:
            self._log.info("toDebugString():\n%s", rows.toDebugString().decode())
        rows.toDF() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=self.table, keyspace=self.keyspace) \
            .save()

    @staticmethod
    def format_url(row):
        if row.repository_id.startswith("github.com"):
            return "https://%s/blob/%s/%s" % (row.repository_id, row.commit_hash, row.path)
        if row.repository_id.startswith("bitbucket.org"):
            return "https://%s/src/%s/%s" % (row.repository_id, row.commit_hash, row.path)
        return "[%s %s %s]" % (row.repository_id, row.commit_hash, row.path)


class DzhigurdaFiles(Transformer):
    def __init__(self, dzhigurda, **kwargs):
        super().__init__(**kwargs)
        self.dzhigurda = dzhigurda

    def __call__(self, engine):
        commits = engine.repositories.references.head_ref.commits
        chosen = commits.filter(commits.index <= self.dzhigurda)
        return chosen.tree_entries.blobs


def source2bags(args):
    log = logging.getLogger("bags")
    if os.path.exists(args.batches):
        log.critical("%s must not exist", args.batches)
        return 1
    if not args.config:
        args.config = []
    cassandra_utils.configure(args)
    engine = create_engine("source2bags-%s" % uuid4(), args.repositories, args)
    extractors = [wmhash.__extractors__[s](
        args.min_docfreq, **wmhash.__extractors__[s].get_kwargs_fromcmdline(args))
        for s in args.feature]
    pipeline = Engine(engine, explain=args.explain).link(DzhigurdaFiles(args.dzhigurda))
    uasts = pipeline.link(UastExtractor(languages=[args.language]))
    if args.persist is not None:
        uasts = uasts.link(Cacher(args.persist))
    uasts.link(MetadataSaver(args.keyspace, args.tables["meta"]))
    uasts = uasts.link(UastDeserializer())
    uasts.link(wmhash.Repo2DocFreq(extractors))
    pipeline.explode()
    bags = uasts.link(wmhash.Repo2WeightedSet(extractors))
    if args.persist is not None:
        bags = bags.link(Cacher(args.persist))
    batcher = bags.link(wmhash.BagsBatcher(extractors))
    batcher.link(wmhash.BagsBatchSaver(args.batches, batcher))
    bags.link(BagsSaver(args.keyspace, args.tables["bags"]))
    bags.explode()
    log.info("Writing %s", args.docfreq)
    batcher.model.save(args.docfreq)
    if args.graph:
        log.info("Dumping the graph to %s", args.graph)
        with open(args.graph, "w") as f:
            pipeline.graph(stream=f)
    if args.pause:
        input("Press Enter to exit...")
