import os
import shutil
import argparse
import unittest
from pyspark import Row

from sourced.ml.models import OrderedDocumentFrequencies, QuantizationLevels, BOW
from sourced.ml.utils.spark import create_spark

from apollo.bags import BagsSaver, MetadataSaver, source2bags
from apollo.__main__ import CASSANDRA_PACKAGE
from apollo.cassandra_utils import reset_db
from tests.cluster_utils import count_table, create_session, extract_row


TEST_SIVA = os.path.join(os.path.dirname(__file__), "siva_files")


class BagsTests(unittest.TestCase):
    def setUp(self):
        os.mkdir("/tmp/bags/")
        reset_db(get_args())

    def tearDown(self):
        reset_db(get_args())
        shutil.rmtree("/tmp/bags")

    def test_table_savers(self):
        spark = create_spark("table_savers", spark="local[*]", spark_log_level="WARN",
                             spark_local_dir="/tmp", config=[],
                             packages=[CASSANDRA_PACKAGE], dep_zip=False)

        bags = spark.sparkContext.parallelize([
            Row(document="cf23df2207d99b", token="test.feature", value="2.5")])
        self.assertEqual(BagsSaver(keyspace="apollo", table="bags")(bags), bags)
        metadata = spark.sparkContext.parallelize([
            Row(blob_id="a74fbe169e3eba", repository_id="fjd74876fc239",
                commit_hash="035e633b65d94", path="test_path")])
        self.assertIsNone(MetadataSaver(keyspace="apollo", table="meta")(metadata))
        session = create_session()
        row = extract_row(session, "bags")
        self.assertListEqual([row.sha1, row.item, row.value],
                             ["cf23df2207d99b", "test.feature", 2.5])
        row = extract_row(session, "meta")
        self.assertListEqual([row.sha1, row.repo, row.commit, row.path],
                             ["a74fbe169e3eba", "fjd74876fc239", "035e633b65d94", "test_path"])
        spark.stop()

    def test_source2bags(self):
        source2bags(get_args(doc_out="/tmp/bags/docfreq.asdf"))
        self.assertTrue(os.path.exists("/tmp/bags/docfreq.asdf"))
        self.assertEqual(len(OrderedDocumentFrequencies().load("/tmp/bags/docfreq.asdf")), 227)
        self.assertTrue(os.path.exists("/tmp/bags/quant.asdf"))
        self.assertEqual(len(QuantizationLevels().load("/tmp/bags/quant.asdf")), 1)
        self.assertTrue(os.path.exists("/tmp/bags/bow_001.asdf"))
        self.assertEqual(len(BOW().load("/tmp/bags/bow_001.asdf")), 120)
        session = create_session()
        self.assertEqual(count_table(session, "meta"), 120)
        self.assertEqual(count_table(session, "bags"), 4818)


def get_args(doc_in=None, doc_out=None):
    return argparse.Namespace(
        bow="/tmp/bags/bow.asdf", batch=2000000, dzhigurda=0, bblfsh="localhost", engine=None,
        repository_format="siva", spark="local[*]", config=[], memory="", graph=False,
        spark_local_dir="/tmp", spark_log_level="WARN", persist=None,
        explain=False, repositories=TEST_SIVA, parquet=False, packages=[CASSANDRA_PACKAGE],
        languages=["Python"], mode="file", quant="/tmp/bags/quant.asdf",
        feature=["children"], min_docfreq=1, docfreq_in=doc_in, docfreq_out=doc_out,
        vocabulary_size=10000000, partitions=200, shuffle=False, cassandra="localhost",
        keyspace="apollo", tables=None, hashes_only=False, log_level="INFO", pause=False)
