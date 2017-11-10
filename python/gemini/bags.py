import logging
import os
import pickle
import sys
from uuid import uuid4

from ast2vec.repo2 import wmhash
from ast2vec.repo2.base import Repo2FinalizerBase
from pyspark.sql import SparkSession
from sourced.engine import Engine


class CassandraFinalizer(Repo2FinalizerBase):
    def __call__(self, processed):
        pass


def source2bags(args):
    log = logging.getLogger("source2bags")
    os.putenv("PYSPARK_PYTHON", sys.executable)
    session_name = "source2bags-%s" % uuid4()
    log.info("Starting %s on %s", session_name, args.spark)
    builder = SparkSession.builder.master(args.spark).appName(session_name)
    builder = builder.config(
        "spark.jars.packages", "tech.sourced:engine:%s" % args.engine)
    builder = builder.config(
        "spark.tech.sourced.bblfsh.grpc.host", args.bblfsh)
    # TODO(vmarkovtsev): figure out why is this option needed
    builder = builder.config(
        "spark.tech.sourced.engine.cleanup.skip", "true")
    builder = builder.config("spark.local.dir", args.spark_local_dir)
    for cfg in args.config:
        builder = builder.config(*cfg.split("=", 1))
    session = builder.getOrCreate()
    engine = Engine(session, args.repositories)
    log.info("docfreq phase")
    extractors = [wmhash.__extractors__[s](args.min_docfreq) for s in args.feature]
    repo2docfreq = wmhash.Repo2DocFreq(
        engine, languages=[args.language], extractors=extractors)
    try:
        repo2docfreq.process_files()
    except pickle.PicklingError as e:
        if e.__cause__ is not None and len(e.__cause__.args) == 2:
            for obj in e.__cause__.args[1]:
                print(obj, file=sys.stderr)
                print(file=sys.stderr)
        raise e from None
    for ex in extractors:
        print(ex.ndocs)
