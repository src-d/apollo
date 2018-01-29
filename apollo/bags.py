import logging
import os
from uuid import uuid4


from sourced.ml.utils import create_engine, EngineConstants
from sourced.ml.extractors import __extractors__
from sourced.ml.transformers import UastExtractor, Transformer, Cacher, UastDeserializer, Engine, \
    FieldsSelector, ParquetSaver, Uast2TermFreq, Uast2DocFreq, Uast2Quant, BagsBatchSaver, \
    BagsBatcher, Indexer, TFIDF

from pyspark.sql.types import Row

from apollo import cassandra_utils


class BagsSaver(Transformer):
    def __init__(self, keyspace, table, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace
        self.table = table

    def __call__(self, head):
        rows = head.map(lambda row: Row(sha1=row.document,
                                        item=row.token,
                                        value=float(row.value)))
        if self.explained:
            self._log.info("toDebugString():\n%s", rows.toDebugString().decode())
        rows.toDF() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=self.table, keyspace=self.keyspace) \
            .save()


class MetadataSaver(Transformer):
    def __init__(self, keyspace, table, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace
        self.table = table

    def __call__(self, head):
        rows = head.rdd.map(lambda x: Row(
            sha1=x.blob_id, repo=x.repository_id, commit=x.commit_hash, path=x.path))
        if self.explained:
            self._log.info("toDebugString():\n%s", rows.toDebugString().decode())
        rows.toDF() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=self.table, keyspace=self.keyspace) \
            .save()


class DzhigurdaFiles(Transformer):
    def __init__(self, dzhigurda, **kwargs):
        super().__init__(**kwargs)
        self.dzhigurda = dzhigurda

    def __call__(self, engine):
        commits = engine.repositories.references.head_ref.commits
        if self.dzhigurda < 0:
            # Use all available commits
            chosen = commits
        else:
            chosen = commits.filter(commits.index <= self.dzhigurda)
        return chosen.tree_entries.blobs


def preprocess_source(args):
    log = logging.getLogger("preprocess_source")
    if os.path.exists(args.output):
        log.critical("%s must not exist", args.output)
        return 1
    if not args.config:
        args.config = []

    try:
        engine = create_engine("source2bags-%s" % uuid4(), **args.__dict__)
        pipeline = Engine(engine, explain=args.explain).link(DzhigurdaFiles(args.dzhigurda))
        uasts = pipeline.link(UastExtractor(languages=[args.language]))
        fields = uasts.link(FieldsSelector(fields=args.fields))
        saver = fields.link(ParquetSaver(save_loc=args.output))

        saver.explode()
    finally:
        if args.pause:
            input("Press Enter to exit...")


def source2bags(args):
    log = logging.getLogger("bags")
    document_column_name = EngineConstants.Columns.BlobId
    if os.path.exists(args.batches):
        log.critical("%s must not exist", args.batches)
        return 1
    if not args.config:
        args.config = []
    try:
        cassandra_utils.configure(args)
        engine = create_engine("source2bags-%s" % uuid4(), **args.__dict__)
        log.info("Enabled extractors: %s", args.feature)
        extractors = [__extractors__[s](
            args.min_docfreq, **__extractors__[s].get_kwargs_fromcmdline(args))
            for s in args.feature]
        pipeline = Engine(engine, explain=args.explain).link(DzhigurdaFiles(args.dzhigurda))
        uasts = pipeline.link(UastExtractor(languages=[args.language]))
        if args.persist is not None:
            uasts = uasts.link(Cacher(args.persist))
        uasts.link(MetadataSaver(args.keyspace, args.tables["meta"]))
        uasts = uasts.link(UastDeserializer())

        # TODO(zurk): Fix and add Uast2Quant part
        df_transformer = Uast2DocFreq(extractors, document_column_name)
        df_pipeline = uasts.link(df_transformer)
        df = df_pipeline.execute()
        tf_pipeline = uasts.link(Uast2TermFreq(extractors, document_column_name))
        tf = tf_pipeline.execute()

        bags = TFIDF(tf=tf, df=df) \
            .link(Cacher.maybe(args.persist))
        batcher = bags.link(BagsBatcher(df, df_transformer.ndocs))
        batcher.link(BagsBatchSaver(args.batches, batcher))
        bags.link(BagsSaver(args.keyspace, args.tables["bags"]))
        bags.explode()
        log.info("Writing docfreq to %s", args.docfreq)
        batcher.model.save(args.docfreq)
        if args.graph:
            log.info("Dumping the graph to %s", args.graph)
            with open(args.graph, "w") as f:
                pipeline.graph(stream=f)
    finally:
        if args.pause:
            input("Press Enter to exit...")
