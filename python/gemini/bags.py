import logging
import os
from uuid import uuid4


from sourced.ml.engine import create_engine
from sourced.ml.repo2 import wmhash
from sourced.ml.repo2.base import UastExtractor, Transformer, Cacher, UastDeserializer
from pyspark.sql.types import Row

from gemini import cassandra_utils


class CassandraSaver(Transformer):
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
        args.min_docfreq,**wmhash.__extractors__[s].get_kwargs_fromcmdline(args))
        for s in args.feature]
    pipeline = UastExtractor(engine, languages=[args.language], explain=args.explain)
    if args.persist is not None:
        uasts = pipeline.link(Cacher(args.persist))
    else:
        uasts = pipeline
    uasts = uasts.link(UastDeserializer())
    uasts.link(wmhash.Repo2DocFreq(extractors)).execute()
    bags = uasts.link(wmhash.Repo2WeightedSet(extractors))
    if args.persist is not None:
        bags = bags.link(Cacher(args.persist))
    batcher = bags.link(wmhash.BagsBatcher(extractors))
    batcher.link(wmhash.BagsBatchSaver(args.batches, batcher))
    bags.link(CassandraSaver(args.keyspace, args.tables["bags"]))
    bags.explode()
    log.info("Writing %s", args.docfreq)
    batcher.model.save(args.docfreq)
    if args.graph:
        log.info("Dumping the graph to %s", args.graph)
        with open(args.graph, "w") as f:
            pipeline.graph(stream=f)
    if args.pause:
        input("Press Enter to exit...")
