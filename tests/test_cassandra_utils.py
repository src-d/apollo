import json
import argparse
import unittest

from apollo.cassandra_utils import patch_tables, configure, get_db, reset_db
from tests.cluster_utils import count_table, create_session


class CassandraUtilsTests(unittest.TestCase):
    table_names = ("bags", "meta", "hashes", "hashtables", "hashtables2")
    insert_query = "INSERT INTO %s.%s (%s) VALUES (%s)"
    values = {"bags": "'sha_b','item_b','val_b'", "meta": "'sha_m','repo_m','commit_m','path_m'",
              "hashes": "'sha_h','val_h'",
              "hashtables": "'sha_h1','ht_1','val_1'", "hashtables2": "'sha_h2','ht_2','val_2'"}
    tables_map = """
{"bags": "bagstest", "meta": "metatest", "hashes": "hashestest", "hashtables": "hashtablestest",
"hashtables2": "hashtables2test"}
"""
    tables = {"bags": ["sha1,item,value", "'cf23df2207d99','test.feature',3.5"],
              "meta": ["sha1,repo,commit,path",
                       "'a74fbe169e3eba','test_repo','035e633b65d94','test/path'"],
              "hashes": ["sha1,value", "'cf23df2207d99',0x0000000000000003"],
              "hashtables": ["sha1,hashtable,value", "'cf23df2207d99',1,0x0000000000000001"],
              "hashtables2": ["sha1,hashtable,value", "'cf23df2207d99',2,0x0000000000000002"]
              }

    def test_resetdb(self):
        reset_db(get_args())
        session = create_session()
        self.assertIsNotNone(session)
        for table in self.table_names:
            count = count_table(session, table)
            self.assertEqual(count, 0)
            session.execute(self.insert_query % ("apollo", table, self.tables[table][0],
                                                 self.tables[table][1]))
            count = count_table(session, table)
            self.assertEqual(count, 1)
        reset_db(get_args(hashes_only=True))
        for table in self.table_names:
            if "hash" in table:
                count = count_table(session, table)
                self.assertEqual(count, 0)
            else:
                count = count_table(session, table)
                self.assertEqual(count, 1)
        reset_db(get_args())
        for table in self.table_names:
            count = count_table(session, table)
            self.assertEqual(count, 0)

        reset_db(get_args(tables=self.tables_map))
        new_table = json.loads(self.tables_map)
        for table in self.table_names:
            count = count_table(session, new_table[table])
            self.assertEqual(count, 0)

        reset_db(get_args(keyspace="not_apollo"))
        self.assertIsNotNone(create_session("not_apollo"))

    def test_patch_tables(self):
        args = argparse.Namespace(tables=None)
        patch_tables(args)
        self.assertEqual(args.tables, {n: n for n in self.table_names})
        args = argparse.Namespace(tables=self.tables_map)
        patch_tables(args)
        self.assertEqual(args.tables, {n: n + "test" for n in self.table_names})

    def test_configure(self):
        args = argparse.Namespace(tables=None, cassandra="cassandra:9043", config=["fake"])
        args = configure(args)
        self.assertDictEqual(args.tables, {n: n for n in self.table_names})
        self.assertListEqual(["fake", "spark.cassandra.connection.host=cassandra",
                             "spark.cassandra.connection.port=9043"], sorted(args.config))
        args = argparse.Namespace(tables=None, cassandra="cassandra", config=["fake"])
        args = configure(args)
        self.assertDictEqual(args.tables, {n: n for n in self.table_names})
        self.assertListEqual(["fake", "spark.cassandra.connection.host=cassandra",
                              "spark.cassandra.connection.port=9042"], sorted(args.config))

    def test_get_db(self):
        session = get_db(get_args())
        # self.assertEqual(session.keyspace, "apollo")
        self.assertEqual(session.cluster.port, 9042)


def get_args(hashes_only=False, keyspace="apollo", tables=""):
    return argparse.Namespace(hashes_only=hashes_only, cassandra="localhost", keyspace=keyspace,
                              tables=tables, log_level="INFO")
