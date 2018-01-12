from collections import namedtuple
from copy import deepcopy
import os
import unittest
from unittest.mock import patch
import tempfile

import numpy
from scipy.sparse import csr_matrix
from sourced.ml.models import OrderedDocumentFrequencies
from sourced.ml.transformers import BagsBatch
from sourced.ml.extractors import __extractors__
import sourced


from apollo.hasher import modify_feature_weights


class DummyClass: pass


def dict_to_arguments(d):
    res = DummyClass()

    for key in d:
        setattr(res, key, d[key])

    return res


class FeatureWeightTest(unittest.TestCase):
    FakeExtractor = namedtuple("FakeExtractor", ("NAME", "NAMESPACE"))

    def setUp(self):
        docs = 1
        freq = 1
        default_weight = 1
        docfreqs = []
        self.extractors = {}
        self.extractor_args = {}
        for i in range(2):
            namespace = "extractor%s." % i
            feat_freq = {}
            for j in range(2):
                feat_freq[namespace + str(j)] = freq
            docfreqs.append(feat_freq)

            self.extractors[namespace] = self.FakeExtractor(NAME=namespace, NAMESPACE=namespace)
            self.extractor_args["%s_weight" % namespace] = default_weight

        # Create tmp file and save OrderedDocumentFrequencies there
        self.tmp_file = tempfile.NamedTemporaryFile(prefix="test_weighting", delete=False)
        model = OrderedDocumentFrequencies().construct(docs, docfreqs)
        model.save(self.tmp_file.name)

        # arguments.docfreq
        self.docfreq_args = {"docfreq": self.tmp_file.name}

        # batches
        self.batches = [BagsBatch(keys=None, matrix=csr_matrix(numpy.eye(4)))]

    def tearDown(self):
        self.tmp_file.close()
        try:
            os.remove(self.tmp_file.name)
        except:
            pass

    def test_empty_extractors(self):
        arguments = dict_to_arguments(self.docfreq_args)
        with patch.dict(sourced.ml.extractors.__extractors__, self.extractors, clear=True):
            result = modify_feature_weights(deepcopy(self.batches), arguments)
            self.assertEqual(len(result), len(self.batches))
            for bathc_res, batch_init in zip(result, self.batches):
                bathc_res.matrix.sort_indices()
                batch_init.matrix.sort_indices()

                self.assertTrue(numpy.array_equal(bathc_res.matrix.indices,
                                                  batch_init.matrix.indices))
                self.assertTrue(numpy.array_equal(bathc_res.matrix.data, batch_init.matrix.data))
                self.assertTrue(numpy.array_equal(bathc_res.matrix.indptr,
                                                  batch_init.matrix.indptr))

    def test_extractor_weight_1(self):
        self.docfreq_args.update(self.extractor_args)
        arguments = dict_to_arguments(self.docfreq_args)
        with patch.dict(sourced.ml.extractors.__extractors__, self.extractors, clear=True):
            result = modify_feature_weights(deepcopy(self.batches), arguments)
            self.assertEqual(len(result), len(self.batches))
            for bathc_res, batch_init in zip(result, self.batches):
                bathc_res.matrix.sort_indices()
                batch_init.matrix.sort_indices()

                self.assertTrue(numpy.array_equal(bathc_res.matrix.indices,
                                                  batch_init.matrix.indices))
                self.assertTrue(numpy.array_equal(bathc_res.matrix.data, batch_init.matrix.data))
                self.assertTrue(numpy.array_equal(bathc_res.matrix.indptr,
                                                  batch_init.matrix.indptr))

    def test_empty_batches(self):
        self.docfreq_args.update(self.extractor_args)
        arguments = dict_to_arguments(self.docfreq_args)
        with patch.dict(sourced.ml.extractors.__extractors__, self.extractors, clear=True):
            result = modify_feature_weights([], arguments)
            self.assertEqual(len(result), 0)

    def test_no_docfreq(self):
        no_file = tempfile.NamedTemporaryFile(prefix="test_weighting", delete=False)
        no_file.close()
        try:
            os.remove(no_file.name)
        except:
            pass

        no_docfreq = {"docfreq": no_file.name}
        no_docfreq.update(self.extractor_args)
        arguments = dict_to_arguments(self.docfreq_args)
        with patch.dict(sourced.ml.extractors.__extractors__, self.extractors, clear=True):
            self.assertRaises(Exception, modify_feature_weights(self.batches, arguments))

    def test_normal_run(self):
        self.docfreq_args.update(self.extractor_args)
        weight = 2
        for key in self.docfreq_args:
            if "_weight" in key:
                self.docfreq_args[key] *= weight  # make not 1
        arguments = dict_to_arguments(self.docfreq_args)
        with patch.dict(sourced.ml.extractors.__extractors__, self.extractors, clear=True):
            result = modify_feature_weights(deepcopy(self.batches), arguments)
            self.assertEqual(len(result), len(self.batches))
            for bathc_res, batch_init in zip(result, self.batches):
                bathc_res.matrix.sort_indices()
                batch_init.matrix.sort_indices()

                self.assertTrue(numpy.array_equal(bathc_res.matrix.indices,
                                                  batch_init.matrix.indices))
                self.assertTrue(numpy.array_equal(bathc_res.matrix.data,
                                                  batch_init.matrix.data * weight))
                self.assertTrue(numpy.array_equal(bathc_res.matrix.indptr,
                                                  batch_init.matrix.indptr))
        pass


if __name__ == "__main__":
    unittest.main()
