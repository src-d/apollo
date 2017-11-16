import argparse
import logging
import sys

from modelforge.logs import setup_logging

from gemini.bags import source2bags
from gemini.hasher import hash_batches
from gemini.warmup import warmup


def get_parser() -> argparse.ArgumentParser:
    """
    Create main parser.

    :return: Parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO",
                        choices=logging._nameToLevel,
                        help="Logging verbosity.")

    def add_engine_args(my_parser):
        my_parser.add_argument(
            "-s", "--spark", default="local[*]", help="Spark's master address.")
        my_parser.add_argument(
            "--config", nargs="+", default=[], help="Spark configuration (key=value).")
        my_parser.add_argument(
            "--bblfsh", default="localhost", help="Babelfish server's address.")
        my_parser.add_argument(
            "--engine", default="0.1.7", help="source{d} engine version.")
        my_parser.add_argument(
            "--spark-local-dir", default="/tmp/spark", help="Spark local directory.")

    subparsers = parser.add_subparsers(help="Commands", dest="command")
    source2bags_parser = subparsers.add_parser(
        "bags", help="Convert source code to weighted sets.")
    source2bags_parser.set_defaults(handler=source2bags)
    source2bags_parser.add_argument(
        "-r", "--repositories", required=True,
        help="The path to the repositories.")
    source2bags_parser.add_argument(
        "-o", "--output", required=True,
        help="The path to the Parquet files with bag batches.")
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
        "--cassandra", default="0.0.0.0:9042", help="Persistence type (StorageClass.*).")
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
