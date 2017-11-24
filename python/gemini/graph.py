from copy import deepcopy
import logging

from modelforge.model import Model, merge_strings, split_strings
from modelforge.models import register_model

from gemini.cassandra_utils import get_db


@register_model
class MinHashBucketsModel(Model):
    """
    Model to store Buckets with sha1.
    """
    NAME = "mhbuckets"

    def construct(self, buckets):
        self.sha1_to_id, self.buckets = self.convert_buckets(buckets)
        return self

    def _load_tree(self, tree):
        sha1_list = split_strings(tree["sha1_list"])
        self.sha1_to_id = {}
        for i, sha1 in enumerate(sha1_list):
            self.sha1_to_id[sha1] = i
        self.buckets = tree["buckets"]

    def dump(self):
        return "Number of buckets: %s\nNumber of unique sha1: %s" % (
            len(self.buckets), len(self.sha1_to_id))

    def _generate_tree(self):
        sha1_list = []
        for sha1, _ in sorted(self.sha1_to_id.items(), key=lambda kv: kv[1]):
            sha1_list.append(sha1)

        return {"sha1_list": merge_strings(sha1_list), "buckets": self.buckets}

    @staticmethod
    def convert_buckets(buckets):
        sha1_to_id = {}

        new_buckets = []
        for bucket in buckets:
            new_bucket = []
            for sha1 in bucket:
                new_bucket.append(sha1_to_id.setdefault(sha1, len(sha1_to_id)))
            new_buckets.append(new_bucket)
        return sha1_to_id, new_buckets


def print_hash_graph(args):
    log = logging.getLogger("graph")
    session = get_db(args)
    table = args.tables["hashtables"]
    rows = session.execute("SELECT DISTINCT hashtable FROM %s" % table)
    hashtables = sorted(r.hashtable for r in rows)
    log.info("Detected %d hashtables", len(hashtables))
    buckets = []
    for hashtable in hashtables:
        log.info("Fetching %d", hashtable)
        rows = session.execute(
            "SELECT sha1, value FROM %s WHERE hashtable=%d" % (table, hashtable))
        band = None
        bucket = []
        for row in rows:
            if row.value != band:
                band = row.value
                buckets.append(deepcopy(bucket))
                bucket.clear()
                bucket.append(row.sha1)
                continue
            bucket.append(row.sha1)

    MinHashBucketsModel().construct(buckets=buckets).save(args.output)
