import logging
import os
from uuid import uuid4

from sourced.ml.extractors import create_extractors_from_args
from sourced.ml.models import OrderedDocumentFrequencies, QuantizationLevels
from sourced.ml.transformers import UastExtractor, Transformer, Ignition, \
    FieldsSelector, ParquetSaver, create_parquet_loader, Moder, Cacher, UastRow2Document, \
    Uast2BagFeatures, Indexer, Uast2Quant, UastDeserializer, TFIDF, BagFeatures2DocFreq, \
    BagFeatures2TermFreq, BOWWriter
from sourced.ml.utils.engine import pause, pipeline_graph, create_engine

from pyspark.sql.types import Row

from apollo import cassandra_utils


class BagsSaver(Transformer):
    def __init__(self, keyspace, table, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace
        self.table = table

    def __call__(self, head):
        rows = head.map(lambda row: Row(
            sha1=row.document, item=row.token, value=float(row.value)))
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
        rows = head.map(lambda x: Row(
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


@pause
def preprocess_source(args):
    log = logging.getLogger("preprocess_source")
    if os.path.exists(args.output):
        log.critical("%s must not exist", args.output)
        return 1
    if not args.config:
        args.config = []

    engine = create_engine("source2bags-%s" % uuid4(), **args.__dict__)
    ignition = Ignition(engine, explain=args.explain)
    ignition \
        .link(DzhigurdaFiles(args.dzhigurda)) \
        .link(UastExtractor(languages=args.languages)) \
        .link(FieldsSelector(fields=args.fields)) \
        .link(ParquetSaver(save_loc=args.output)) \
        .execute()
    pipeline_graph(args, log, ignition)


@pause
def source2bags(args):
    log = logging.getLogger("sourced2bags")
    extractors = create_extractors_from_args(args)
    session_name = "sourced2bags-%s" % uuid4()
    cassandra_utils.configure(args)
    if args.parquet:
        start_point = create_parquet_loader(session_name, **args.__dict__)
        root = start_point
    else:
        engine = create_engine(session_name, **args.__dict__)
        root = engine
        start_point = Ignition(engine, explain=args.explain) \
            .link(DzhigurdaFiles(dzhigurda=args.dzhigurda)) \
            .link(UastExtractor(languages=args.languages))

    uast_extractor = start_point.link(Moder(args.mode)) \
        .link(Cacher.maybe(args.persist))
    log.info("Writing metadata to DB... ")
    uast_extractor.link(MetadataSaver(keyspace=args.keyspace, table=args.tables["meta"])) \
        .execute()

    uast_extractor = uast_extractor.link(UastRow2Document())
    log.info("Extracting UASTs and indexing documents...")
    document_indexer = Indexer(Uast2BagFeatures.Columns.document)
    uast_extractor.link(document_indexer).execute()
    ndocs = len(document_indexer)
    log.info("Number of documents: %d", ndocs)
    uast_extractor = uast_extractor.link(UastDeserializer())
    quant = Uast2Quant(extractors)
    uast_extractor.link(quant).execute()
    if quant.levels:
        log.info("Writing quantization levels to %s", args.quant)
        QuantizationLevels().construct(quant.levels).save(args.quant)
    uast_extractor = uast_extractor \
        .link(Uast2BagFeatures(extractors)) \
        .link(Cacher.maybe(args.persist))
    log.info("Calculating the document frequencies...")
    df = uast_extractor.link(BagFeatures2DocFreq()).execute()
    log.info("Writing docfreq to %s", args.docfreq)
    df_model = OrderedDocumentFrequencies() \
        .construct(ndocs, df) \
        .prune(args.min_docfreq) \
        .greatest(args.vocabulary_size) \
        .save(args.docfreq)
    bags = uast_extractor \
        .link(BagFeatures2TermFreq()) \
        .link(TFIDF(df_model)) \
        .link(document_indexer) \
        .link(Indexer(Uast2BagFeatures.Columns.token, df_model.order)) \
        .link(Cacher.maybe(args.persist))
    bags.link(BOWWriter(document_indexer, df_model, args.bow, args.batch))\
        .execute()
    log.info("Writing bags to DB... ")
    bags.link(BagsSaver(keyspace=args.keyspace, table=args.tables["bags"])) \
        .execute()
    pipeline_graph(args, log, root)
