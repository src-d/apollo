import argparse
import logging
import sys
from time import time

from modelforge.logs import setup_logging

from gemini.bags import source2bags
from gemini.hasher import hash_batches
from gemini.query import query
from gemini.warmup import warmup


CASSANDRA_PACKAGE = "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3"


def get_parser() -> argparse.ArgumentParser:
    """
    Create main parser.

    :return: Parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO",
                        choices=logging._nameToLevel,
                        help="Logging verbosity.")

    def add_spark_args(my_parser):
        my_parser.add_argument(
            "-s", "--spark", default="local[*]", help="Spark's master address.")
        my_parser.add_argument(
            "--config", nargs="+", default=[], help="Spark configuration (key=value).")
        my_parser.add_argument(
            "--package", nargs="+", default=[CASSANDRA_PACKAGE], help="Additional Spark package.")
        my_parser.add_argument(
            "--spark-local-dir", default="/tmp/spark", help="Spark local directory.")

    def add_engine_args(my_parser):
        add_spark_args(my_parser)
        my_parser.add_argument(
            "--bblfsh", default="localhost", help="Babelfish server's address.")
        my_parser.add_argument(
            "--engine", default="0.1.7", help="source{d} engine version.")

    def add_cassandra_args(my_parser):
        my_parser.add_argument(
            "--cassandra", default="0.0.0.0:9042", help="Cassandra's host:port.")
        my_parser.add_argument("--keyspace", default="gemini",
                               help="Cassandra's key space.")

    def add_wmh_args(my_parser, params_help, help_suffix="."):
        my_parser.add_argument("--size", type=int, default=128, help="Hash size" + help_suffix)
        my_parser.add_argument("-p", "--params", required=True, help=params_help)
        my_parser.add_argument("-t", "--threshold", required=True, type=float,
                               help="Jaccard similarity threshold.")  # no help_suffix
        my_parser.add_argument("--false-positive-weight", type=float, default=0.5,
                               help="Used to adjust the relative importance of "
                                    "minimizing false positives count when optimizing "
                                    "for the Jaccard similarity threshold" + help_suffix)
        my_parser.add_argument("--false-negative-weight", type=float, default=0.5,
                               help="Used to adjust the relative importance of "
                                    "minimizing false negatives count when optimizing "
                                    "for the Jaccard similarity threshold" + help_suffix)

    subparsers = parser.add_subparsers(help="Commands", dest="command")
    source2bags_parser = subparsers.add_parser(
        "bags", help="Convert source code to weighted sets.")
    source2bags_parser.set_defaults(handler=source2bags)
    source2bags_parser.add_argument(
        "-r", "--repositories", required=True,
        help="The path to the repositories.")
    source2bags_parser.add_argument(
        "--batches", required=True,
        help="[OUT] The path to the Parquet files with bag batches.")
    source2bags_parser.add_argument(
        "--docfreq", required=True,
        help="[OUT] The path to the OrderedDocumentFrequencies model.")
    source2bags_parser.add_argument(
        "--vocabulary-size", default=10000000, type=int,
        help="The maximum vocabulary size.")
    source2bags_parser.add_argument(
        "--min-docfreq", default=1, type=int,
        help="The minimum document frequency of each element.")
    source2bags_parser.add_argument(
        "-f", "--feature", nargs="+", choices=("id", "node2vec", "treestats"),
        help="The feature extraction schemes to apply.")
    source2bags_parser.add_argument(
        "-l", "--language", choices=("Java", "Python"),
        help="The programming language to analyse.")
    source2bags_parser.add_argument(
        "--persist", default=None, help="Persistence type (StorageClass.*).")
    source2bags_parser.add_argument(
        "--graph", help="Write the tree in Graphviz format to this file.")
    add_cassandra_args(source2bags_parser)
    add_engine_args(source2bags_parser)

    warmup_parser = subparsers.add_parser(
        "warmup", help="Initialize source{d} engine.")
    warmup_parser.set_defaults(handler=warmup)
    add_engine_args(warmup_parser)

    hash_parser = subparsers.add_parser(
        "hash", help="Run MinHashCUDA on the bag batches.")
    hash_parser.set_defaults(handler=hash_batches)
    hash_parser.add_argument("input",
                             help="Path to the directory with Parquet files.")
    hash_parser.add_argument("--seed", type=int, default=int(time()),
                             help="Random generator's seed.")
    hash_parser.add_argument("--mhc-verbosity", type=int, default=1,
                             help="MinHashCUDA logs verbosity level.")
    hash_parser.add_argument("--devices", type=int, default=0,
                             help="Or-red indices of NVIDIA devices to use. 0 means all.")
    add_wmh_args(hash_parser, params_help="Path to the output file with WMH parameters.")
    add_cassandra_args(hash_parser)
    add_spark_args(hash_parser)

    query_parser = subparsers.add_parser("query", help="Query for similar files.")
    query_parser.set_defaults(handler=query)
    mode_group = query_parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("-i", "--id", action="store_true",
                            help="Query for this id (id mode).")
    mode_group.add_argument("-f", "--file", action="store_true",
                            help="Query for this file (file mode).")
    query_parser.add_argument("-x", "--precise", action="store_true",
                              help="Calculate the precise set.")
    query_parser.add_argument("-o", "--format", choices=("human", "json"), help="Output format.")
    add_wmh_args(query_parser, params_help="Path to the Weighted MinHash parameters (file mode).",
                 help_suffix=" (file mode).")
    add_cassandra_args(query_parser)

    return parser


def main():
    """
    Creates all the argparse-rs and invokes the function from set_defaults().

    :return: The result of the function from set_defaults().
    """

    parser = get_parser()
    args = parser.parse_args()
    args.log_level = logging._nameToLevel[args.log_level]
    setup_logging(args.log_level)
    try:
        handler = args.handler
    except AttributeError:
        def print_usage(_):
            parser.print_usage()

        handler = print_usage
    return handler(args)

if __name__ == "__main__":
    sys.exit(main())
