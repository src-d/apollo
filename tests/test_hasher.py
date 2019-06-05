import os
import argparse
import unittest
import numpy as np

from apollo.hasher import weighted_minhash, hash_file

TEST_MODELS = os.path.join(os.path.dirname(__file__), "models")
TEST_FILE = os.path.join(os.path.dirname(__file__), "raw_files", "hello_world.py")


class HasherTests(unittest.TestCase):
    def test_hash_file(self):
        with self.assertRaises(ValueError):
            hash_file(argparse.Namespace(feature=None))
        hashes, bag = hash_file(argparse.Namespace(
            feature=["id"], docfreq=os.path.join(TEST_MODELS, "docfreq.asdf"), min_docfreq=1,
            bblfsh="localhost:9432", params=os.path.join(TEST_MODELS, "params.asdf"),
            file=TEST_FILE))
        true_hashes = np.array([
            [64, 0], [129, 0], [64, 0], [258, 1], [129, 0], [258, 0], [129, 1], [158, 0], [258, 1],
            [64, 0], [158, 0], [158, 0], [258, 0], [258, 0], [64, 1], [129, 1], [258, 0], [258, 1],
            [129, 0], [258, 1], [64, 1], [129, 0], [258, 0], [258, 0], [258, 1], [158, 0], [64, 0],
            [64, 0], [258, 1], [64, 0], [158, 0], [258, 1], [258, 0], [129, 1], [258, 1], [158, 0],
            [64, 1], [64, 1], [64, 1], [129, 0], [158, 0], [158, 0], [258, 0], [258, 1], [258, 0],
            [258, 0], [258, 1], [258, 0], [158, 0], [158, 0], [258, 1], [64, 0], [258, 0],
            [158, 0], [258, 1], [129, 1], [129, 0], [158, 0], [129, 1], [129, 0], [129, 0],
            [258, 1], [64, 1], [129, 0], [258, 1], [64, 1], [64, 0], [129, 1], [64, 0], [158, 0],
            [258, 1], [129, 1], [158, 0], [258, 0], [258, 1], [158, 0], [158, 0], [129, 0],
            [64, 1], [129, 1], [64, 0], [64, 1], [129, 1], [64, 1], [64, 1], [158, 0], [258, 2],
            [258, 1], [258, 4], [258, 0], [64, 0], [64, 1], [158, 0], [258, 0], [258, 0], [158, 0],
            [129, 0], [129, 0], [129, 0], [64, 0], [64, 0], [258, 0], [64, 1], [129, 1], [158, 0],
            [129, 0], [138, 0], [258, 0], [129, 0], [158, 1], [64, 0], [158, 0], [158, 0],
            [129, 0], [258, 0], [258, 1], [64, 0], [258, 0], [258, 0], [158, 0], [158, 0], [64, 0],
            [258, 1], [258, 0], [129, 1], [258, 1], [64, 0], [64, 2]])
        for test_hash, true_hash in zip(hashes, true_hashes):
            self.assertEqual(test_hash[0], true_hash[0])
            self.assertEqual(test_hash[1], true_hash[1])

        self.assertEqual(len(bag), 305)
        self.assertListEqual(sorted([int(b * 1000) for b in bag if b != 0]),
                             [281, 1042, 1523, 1652, 2413])

    def test_weighted_minhash(self):
        sample_size = 5
        rs = np.array([[0.18855471, 0.55860609, 0.32074904, 0.94503203, 0.80788293, 0.36952139],
                       [0.91219665, 0.82054127, 0.87922649, 0.96163543, 0.17032792, 0.15737487],
                       [0.74526589, 0.02589764, 0.41895161, 0.23385183, 0.03583533, 0.02133354],
                       [0.52707616, 0.05924468, 0.18467144, 0.69506453, 0.80334504, 0.73779166],
                       [0.62265897, 0.37852388, 0.22519028, 0.31774524, 0.25369227, 0.16474114]])

        with self.assertRaises(ValueError):
            weighted_minhash(None, 6, rs, None, None)
        v = np.random.rand(11)
        with self.assertRaises(ValueError):
            weighted_minhash(v, sample_size, rs, None, None)
        v = np.zeros(10)
        with self.assertRaises(ValueError):
            weighted_minhash(v, sample_size, rs, None, None)
        v = np.array([2.30596176, 7.59096823, 5.18146929, 6.95338507, 0.45129917, 1.68648719])
        ln_cs = np.array([0.14465902, 0.65940428, 0.33196988, 0.34084091, 0.25082798])
        betas = np.array([0.29659419, 0.63912469, 0.25138534, 0.17869953, 0.27172536])
        hashes = np.array([[3, 2], [1, 3], [3, 8], [1, 34], [1, 5]])
        for test_hash, true_hash in zip(weighted_minhash(v, sample_size, rs, ln_cs, betas),
                                        hashes):
            self.assertEqual(test_hash[0], true_hash[0])
            self.assertEqual(test_hash[1], true_hash[1])
