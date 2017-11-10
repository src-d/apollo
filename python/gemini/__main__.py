import argparse
import logging
import sys

from modelforge.logs import setup_logging

from gemini.bags import source2bags


def get_parser() -> argparse.ArgumentParser:
    """
    Create main parser.

    :return: Parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO",
                        choices=logging._nameToLevel,
                        help="Logging verbosity.")

    subparsers = parser.add_subparsers(help="Commands", dest="command")
    source2bags_parser = subparsers.add_parser(
        "bags", help="Convert source code to weighted sets.")
    source2bags_parser.set_defaults(handler=source2bags)
    source2bags_parser.add_argument(
        "-r", "--repositories", required=True,
        help="The path to the repositories.")
    source2bags_parser.add_argument(
        "-s", "--spark", default="local[*]", help="Spark's master address.")
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
        "--config", nargs="+", default=[], help="Spark configuration (key=value).")
    source2bags_parser.add_argument(
        "--bblfsh", default="localhost", help="Babelfish server's address.")
    source2bags_parser.add_argument(
        "--engine", default="0.1.6", help="source{d} engine version.")
    source2bags_parser.add_argument(
        "--spark-local-dir", default="/tmp/spark", help="Spark local directory.")
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
