import logging
import pickle
import sys
from uuid import uuid4

from ast2vec.engine import create_engine
from ast2vec.repo2 import wmhash
from ast2vec.repo2.base import UastExtractor, Transformer, Cacher
from pyspark.sql.types import Row


class CassandraSaver(Transformer):
    def __init__(self, keyspace, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace

    def __call__(self, head):
        df = head.map(lambda x: Row(sha1=x[0], bag=x[1])).toDF()
        df \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .options(table="hashes", keyspace=self.keyspace) \
            .save()


def source2bags(args):
    log = logging.getLogger("source2bags")
    engine = create_engine("source2bags-%s" % uuid4(), args.repositories, args)
    log.info("docfreq phase")
    extractors = [wmhash.__extractors__[s](args.min_docfreq) for s in args.feature]
    pipeline = UastExtractor(engine, languages=[args.language])
    if args.persist is not None:
        uasts = pipeline.link(Cacher(args.persist))
    else:
        uasts = pipeline
    repo2docfreq = wmhash.Repo2DocFreq(extractors)
    uasts.link(repo2docfreq)
    pipeline.execute()
    log.info("bag phase")
    uasts.unlink(repo2docfreq)
    bags = uasts.link(wmhash.Repo2WeightedSet(extractors))
    if args.persist is not None:
        bags = bags.link(Cacher(args.persist))
    batcher = wmhash.BagsBatcher(extractors)
    bags.link(batcher).link(wmhash.BagsBatchSaver(args.output))
    pipeline.execute()
    # bags.unlink(batcher)
    #bags.link(CassandraSaver("gemini")).execute()
