import logging
import os
from uuid import uuid4

from sourced.ml.cmd_entries.repos2bow import repos2bow_entry_template
from sourced.ml.transformers import Transformer, FieldsSelector, ParquetSaver, \
    create_uast_source, Moder
from sourced.ml.utils.engine import pause, pipeline_graph
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
        return head


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
    session_name = "preprocess_source-%s" % uuid4()

    if os.path.exists(args.output):
        log.critical("%s must not exist", args.output)
        return 1
    if not args.config:
        args.config = []
    root, start_point = create_uast_source(args, session_name,
                                           select=lambda: DzhigurdaFiles(args.dzhigurda))

    start_point \
        .link(Moder(args.mode)) \
        .link(FieldsSelector(fields=args.fields)) \
        .link(ParquetSaver(save_loc=args.output)) \
        .execute()
    pipeline_graph(args, log, root)


def source2bags(args):
    cassandra_utils.configure(args)
    return repos2bow_entry_template(
        args,
        select=lambda: DzhigurdaFiles(args.dzhigurda),
        cache_hook=lambda: MetadataSaver(args.keyspace, args.tables["meta"]),
        save_hook=lambda: BagsSaver(args.keyspace, args.tables["bags"]))
