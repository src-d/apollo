from collections import defaultdict
import itertools
import unittest

from apollo.graph import _find_connected_component


class ConnectedComponentsTest(unittest.TestCase):
    def test_empty_connected_component(self):
        buckets = []
        element_to_buckets = defaultdict(set)

        res = _find_connected_component(buckets, element_to_buckets)
        self.assertEqual(0, len(res))
        self.assertTrue(set(itertools.chain(*buckets)) == set(itertools.chain(*res.values())))

    def test_one_connected_component(self):
        buckets = []
        element_to_buckets = defaultdict(set)

        # Create one connected component
        for _ in range(5):
            bucket_id = len(buckets)
            buckets.append([bucket_id, bucket_id + 1])
            element_to_buckets[bucket_id].add(bucket_id)
            element_to_buckets[bucket_id + 1].add(bucket_id)
        res = _find_connected_component(buckets, element_to_buckets)
        self.assertEqual(1, len(res))
        self.assertTrue(set(itertools.chain(*buckets)) == set(itertools.chain(*res.values())))

    def test_two_connected_components(self):
        buckets = []
        element_to_buckets = defaultdict(set)

        # Create one connected component
        for _ in range(5):
            bucket_id = len(buckets)
            buckets.append([bucket_id, bucket_id + 1])
            element_to_buckets[bucket_id].add(bucket_id)
            element_to_buckets[bucket_id + 1].add(bucket_id)

        bucket_id = len(buckets)
        buckets.append([bucket_id])
        element_to_buckets[bucket_id].add(bucket_id)

        # Create another connected component
        for _ in range(5):
            bucket_id = len(buckets)
            buckets.append([bucket_id, bucket_id + 1])
            element_to_buckets[bucket_id].add(bucket_id)
            element_to_buckets[bucket_id + 1].add(bucket_id)

        res = _find_connected_component(buckets, element_to_buckets)
        self.assertEqual(2, len(res))
        self.assertTrue(set(itertools.chain(*buckets)) == set(itertools.chain(*res.values())))


if __name__ == "__main__":
    unittest.main()
