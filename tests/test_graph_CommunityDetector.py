import itertools
import unittest

from igraph import Graph

from apollo.graph import CommunityDetector


class CommunityDetectorTest(unittest.TestCase):
    def setUp(self):
        edges = [(0, 1)]
        weights = [1]
        nvertices = 2
        self.graph = Graph(n=nvertices, edges=edges, directed=False)
        self.graph.edge_weights = weights


def test_generator(algorithm):
    def test_community_detection(self):
        cmd = CommunityDetector(algorithm=algorithm, config={})
        res = cmd(self.graph)
        self.assertEqual(len(set(itertools.chain(*res))), 2)  # Check number of unique vertices

    return test_community_detection


if __name__ == "__main__":
    algorithms = ["spinglass", "optimal_modularity", "multilevel","label_propagation",
                  "leading_eigenvector", "leading_eigenvector", "infomap", "walktrap",
                  "fastgreedy"]
    for algorithm in algorithms:
        test_name = "test_community_detection_%s" % algorithm
        test = test_generator(algorithm)
        setattr(CommunityDetectorTest, test_name, test)
    print([method for method in dir(CommunityDetectorTest)
           if "test_community_detection_" in method])
    unittest.main()

