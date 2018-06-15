import unittest

from apollo.query import weighted_jaccard, format_url


class QueryTest(unittest.TestCase):
    def test_weighted_jaccard(self):
        test_input = [[[0, 1], [1, 0]], [[1, 1], [1, 0]], [[0, 1], [1, 1]], [[1, 1], [1, 1]],
                      [[0, 1], [-1, 0]], [[1, 1], [-1, 0]], [[0, 1], [-1, -1]], [[1, 1], [-1, -1]],
                      [[-1, -1], [-1, 0]], [[0, -1], [-1, -1]], [[-1, -1], [-1, -1]]]
        self.assertListEqual([weighted_jaccard(vec[0], vec[1]) for vec in test_input],
                             [0.0, 0.5, 0.5, 1.0, -1.0, -0.5, -2.0, -1.0, 2.0, 2.0, 1.0])

    def test_format_url(self):
        self.assertEqual(format_url("any_url", "my_commit", "my_path"),
                         "[any_url my_commit my_path]")
        self.assertEqual(format_url("any_url.git", "my_commit", "my_path"),
                         "[any_url my_commit my_path]")
        self.assertEqual(format_url("github.com/my_repo", "my_commit", "my_path"),
                         "https://github.com/my_repo/blob/my_commit/my_path")
        self.assertEqual(format_url("github.com/my_repo.git", "my_commit", "my_path"),
                         "https://github.com/my_repo/blob/my_commit/my_path")
        self.assertEqual(format_url("gitlab.com/my_repo", "my_commit", "my_path"),
                         "https://gitlab.com/my_repo/blob/my_commit/my_path")
        self.assertEqual(format_url("gitlab.com/my_repo.git", "my_commit", "my_path"),
                         "https://gitlab.com/my_repo/blob/my_commit/my_path")
        self.assertEqual(format_url("bitbucket.org/my_repo", "my_commit", "my_path"),
                         "https://bitbucket.org/my_repo/src/my_commit/my_path")
        self.assertEqual(format_url("bitbucket.org/my_repo.git", "my_commit", "my_path"),
                         "https://bitbucket.org/my_repo/src/my_commit/my_path")
