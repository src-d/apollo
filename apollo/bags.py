from pyspark.sql.types import Row
from sourced.ml.cmd import repos2bow_template, repos2bow_index
from sourced.ml.transformers import Transformer

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


def preprocess(args):
    return repos2bow_index(args)


def source2bags(args):
    cassandra_utils.configure(args)
    return repos2bow_template(
        args,
        cache_hook=lambda: MetadataSaver(args.keyspace, args.tables["meta"]),
        save_hook=lambda: BagsSaver(args.keyspace, args.tables["bags"]))
