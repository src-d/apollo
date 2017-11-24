from collections import defaultdict
import logging

from modelforge.model import Model, merge_strings, split_strings
from modelforge.models import register_model
import numpy as np

from gemini.graph import MinHashBucketsModel


@register_model
class ConnectedComponentsModel(Model):
    """
    Model to store connected components.
    """
    NAME = "ccsha1"

    def construct(self, connected_components_id, sha1_to_id):
        self.id_to_cc = {}
        for cc, ids in connected_components_id.items():
            for sha1id in ids:
                self.id_to_cc[sha1id] = cc

        self.id_to_sha1 = dict([(v, k) for k, v in sha1_to_id.items()])
        return self

    def _load_tree(self, tree):
        ids = tree["ids"]
        sha1_list = split_strings(tree["sha1_list"])
        cc = tree["connected_components"]
        sha1_list[0], ids[0], cc[0]

        self.id_to_cc = {}
        self.id_to_sha1 = {}

        for id_, sha1, cc_ in zip(ids, sha1_list, cc):
            self.id_to_cc[id_] = cc_
            self.id_to_sha1[id_] = sha1

    def dump(self):
        return "Number of connected components: %s\nNumber of unique sha1: %s" % (
            len(set(self.id_to_cc.values())), len(self.id_to_sha1))

    def _generate_tree(self):
        sha1_list = []
        ids = []
        cc = []
        for id_, sha1 in self.id_to_sha1.items():
            ids.append(id_)
            sha1_list.append(sha1)
            cc.append(self.id_to_cc[id_])
        return {"ids": np.array(ids, dtype=np.int), "sha1_list": merge_strings(sha1_list),
                "connected_components": np.array(cc, dtype=np.int)}


def repo_to_group(data_id):
    sha1_to_buckets = defaultdict(list)
    buckets_id = []

    for i, row in enumerate(data_id):
        for sha1 in row:
            sha1_to_buckets[sha1].append(i)
        buckets_id.append(i)
    return sha1_to_buckets, buckets_id


def connected_components(args):
    log = logging.getLogger("connected_components")

    mhbuckets = MinHashBucketsModel().load(args.path)

    sha1_to_bucket, buckets = repo_to_group(mhbuckets.buckets)

    # Statistics about buckets
    data = sha1_to_bucket.values()
    log.info("Average number of buckets per sha1: %s" % (sum(map(len, data)) / len(data)),
             "\nMin number of buckets per sha1: %s" % (min(map(len, data))),
             "\nMax number of buckets per sha1: %s" % (max(map(len, data))))

    visited_buckets = set()
    connected_components_sha1 = defaultdict(set)
    connected_components_buckets = defaultdict(set)

    cc_id = 0  # connected component counter
    while buckets:
        bucket = buckets.pop()
        if bucket not in visited_buckets:

            visited_buckets.add(bucket)
            queue = set([bucket])
            while queue:
                bucket_ = queue.pop()
                sha1s = mhbuckets.buckets[bucket_]

                connected_components_sha1[cc_id].update(sha1s)
                visited_buckets.add(bucket_)
                connected_components_buckets[cc_id].add(bucket_)

                for sha1 in sha1s:
                    sha1_buckets = sha1_to_bucket[sha1]
                    for b_ in sha1_buckets:
                        if b_ not in visited_buckets:
                            queue.add(b_)

            # increase number of connected components
            cc_id += 1

    ConnectedComponentsModel().construct(connected_components_sha1, mhbuckets.sha1_to_id).save(
        args.output)
