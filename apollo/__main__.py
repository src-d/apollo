import argparse
import json
import logging
import sys
from time import time

from igraph import Graph
from modelforge.logs import setup_logging
from sourced.ml import extractors
from sourced.ml.utils import add_engine_args, add_spark_args
from sourced.ml.cmd_entries import ArgumentDefaultsHelpFormatterNoNone
from sourced.ml.cmd_entries.args import add_bow_args, add_feature_args, add_repo2_args


from apollo.bags import preprocess_source, source2bags
from apollo.cassandra_utils import reset_db
from apollo.graph import find_connected_components, dumpcc, detect_communities, dumpcmd, \
    evaluate_communities
from apollo.hasher import hash_batches
from apollo.query import query
from apollo.warmup import warmup


CASSANDRA_PACKAGE = "com.datastax.spark:spark-cassandra-connector_2.11:2.0.3"


def get_parser() -> argparse.ArgumentParser:
    """
    Create the cmdline argument parser.
    """
    parser = argparse.ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatterNoNone)
    parser.add_argument("--log-level", default="INFO", choices=logging._nameToLevel,
                        help="Logging verbosity.")

    def add_feature_weight_arg(my_parser):
        help_desc = "%s's weight - all features from this extractor will be multiplied by this " \
                  "factor"
        for ex in extractors.__extractors__.values():
            my_parser.add_argument("--%s-weight" % ex.NAME, default=1, type=float,
                                   help=help_desc % ex.__name__)

    def add_cassandra_args(my_parser):
        my_parser.add_argument(
            "--cassandra", default="0.0.0.0:9042", help="Cassandra's host:port.")
        my_parser.add_argument("--keyspace", default="apollo",
                               help="Cassandra's key space.")
        my_parser.add_argument(
            "--tables", help="Table name mapping (JSON): bags, hashes, hashtables, hashtables2.")

    def add_wmh_args(my_parser, params_help: str, add_hash_size: bool, required: bool):
        if add_hash_size:
            my_parser.add_argument("--size", type=int, default=128, help="Hash size.")
        my_parser.add_argument("-p", "--params", required=required, help=params_help)
        my_parser.add_argument("-t", "--threshold", required=required, type=float,
                               help="Jaccard similarity threshold.")
        my_parser.add_argument("--false-positive-weight", type=float, default=0.5,
                               help="Used to adjust the relative importance of "
                                    "minimizing false positives count when optimizing "
                                    "for the Jaccard similarity threshold.")
        my_parser.add_argument("--false-negative-weight", type=float, default=0.5,
                               help="Used to adjust the relative importance of "
                                    "minimizing false negatives count when optimizing "
                                    "for the Jaccard similarity threshold.")

    def add_template_args(my_parser, default_template):
        my_parser.add_argument("--batch", type=int, default=100,
                               help="Number of hashes to query at a time.")
        my_parser.add_argument("--template", default=default_template,
                               help="Jinja2 template to render.")

    def add_dzhigurda_arg(my_parser):
        my_parser.add_argument(
            "--dzhigurda", default=0, type=int,
            help="Index of the examined commit in the history.")

    # Create and construct subparsers
    subparsers = parser.add_subparsers(help="Commands", dest="command")

    # ------------------------------------------------------------------------
    warmup_parser = subparsers.add_parser(
        "warmup", help="Initialize source{d} engine.")
    warmup_parser.set_defaults(handler=warmup)
    add_engine_args(warmup_parser, default_packages=[CASSANDRA_PACKAGE])

    # ------------------------------------------------------------------------
    db_parser = subparsers.add_parser("resetdb", help="Destructively initialize the database.")
    db_parser.set_defaults(handler=reset_db)
    add_cassandra_args(db_parser)
    db_parser.add_argument(
        "--hashes-only", action="store_true",
        help="Only clear the tables: hashes, hashtables, hashtables2. Do not touch the rest.")

    # ------------------------------------------------------------------------
    preprocessing_parser = subparsers.add_parser(
        "preprocess", help="Generate the dataset with good candidates for duplication.")
    preprocessing_parser.set_defaults(handler=preprocess_source)
    add_repo2_args(preprocessing_parser)
    add_dzhigurda_arg(preprocessing_parser)
    preprocessing_parser.add_argument(
        "-o", "--output", required=True,
        help="[OUT] Path to the Parquet files with bag batches.")
    default_fields = ["blob_id", "repository_id", "content", "path", "commit_hash", "uast"]
    preprocessing_parser.add_argument(
        "-f", "--fields", action="append", default=default_fields,
        help="Fields to select from DF to save.")

    # ------------------------------------------------------------------------
    source2bags_parser = subparsers.add_parser(
        "bags", help="Convert source code to weighted sets.")
    source2bags_parser.set_defaults(handler=source2bags)
    add_bow_args(source2bags_parser)
    add_dzhigurda_arg(source2bags_parser)
    add_repo2_args(source2bags_parser, default_packages=[CASSANDRA_PACKAGE])
    add_feature_args(source2bags_parser)
    add_cassandra_args(source2bags_parser)

    # ------------------------------------------------------------------------
    hash_parser = subparsers.add_parser(
        "hash", help="Run MinHashCUDA on the bag batches.")
    hash_parser.set_defaults(handler=hash_batches)
    hash_parser.add_argument("-i", "--input",
                             help="Path to the directory with Parquet files.")
    hash_parser.add_argument("--seed", type=int, default=int(time()),
                             help="Random generator's seed.")
    hash_parser.add_argument("--mhc-verbosity", type=int, default=1,
                             help="MinHashCUDA logs verbosity level.")
    hash_parser.add_argument("--devices", type=int, default=0,
                             help="Or-red indices of NVIDIA devices to use. 0 means all.")
    hash_parser.add_argument("--docfreq", default=None,
                             help="Path to OrderedDocumentFrequencies (file mode).")
    add_wmh_args(hash_parser, "Path to the output file with WMH parameters.", True, True)
    add_cassandra_args(hash_parser)
    add_spark_args(hash_parser, default_packages=[CASSANDRA_PACKAGE])
    add_feature_weight_arg(hash_parser)

    # ------------------------------------------------------------------------
    query_parser = subparsers.add_parser("query", help="Query for similar files.")
    query_parser.set_defaults(handler=query)
    mode_group = query_parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("-i", "--id", help="Query for this id (id mode).")
    mode_group.add_argument("-c", "--file", help="Query for this file (file mode).")
    query_parser.add_argument("--docfreq", help="Path to OrderedDocumentFrequencies (file mode).")
    query_parser.add_argument("--min-docfreq", default=1, type=int,
                              help="The minimum document frequency of each feature.")
    query_parser.add_argument(
        "--bblfsh", default="localhost:9432", help="Babelfish server's address.")
    query_parser.add_argument("--precise", action="store_true",
                              help="Calculate the precise set.")
    add_wmh_args(query_parser, "Path to the Weighted MinHash parameters.", False, False)
    add_feature_args(query_parser, required=False)
    add_template_args(query_parser, "query.md.jinja2")
    add_cassandra_args(query_parser)

    # ------------------------------------------------------------------------
    cc_parser = subparsers.add_parser(
        "cc", help="Load the similar pairs of files and run connected components analysis.")
    cc_parser.set_defaults(handler=find_connected_components)
    add_cassandra_args(cc_parser)
    cc_parser.add_argument("-o", "--output", required=True,
                           help="[OUT] Path to connected components ASDF model.")

    # ------------------------------------------------------------------------
    dumpcc_parser = subparsers.add_parser(
        "dumpcc", help="Output the connected components to stdout.")
    dumpcc_parser.set_defaults(handler=dumpcc)
    dumpcc_parser.add_argument("-i", "--input", required=True,
                               help="Path to connected components ASDF model.")
    # ------------------------------------------------------------------------
    community_parser = subparsers.add_parser(
        "cmd", help="Run Community Detection analysis on the connected components from \"cc\".")
    community_parser.set_defaults(handler=detect_communities)
    community_parser.add_argument("-i", "--input", required=True,
                                  help="Path to connected components ASDF model.")
    community_parser.add_argument("-o", "--output", required=True,
                                  help="[OUT] Path to the communities ASDF model.")
    community_parser.add_argument("--edges", choices=("linear", "quadratic", "1", "2"),
                                  default="linear",
                                  help="The method to generate the graph's edges: bipartite - "
                                       "linear and fast, but may not fit some the CD algorithms, "
                                       "or all to all within a bucket - quadratic and slow, but "
                                       "surely fits all the algorithms.")
    cmd_choices = [k[10:] for k in dir(Graph) if k.startswith("community_")]
    community_parser.add_argument("-a", "--algorithm", choices=cmd_choices,
                                  default="walktrap",
                                  help="The community detection algorithm to apply.")
    community_parser.add_argument("-p", "--params", type=json.loads, default={},
                                  help="Parameters for the algorithm (**kwargs, JSON format).")
    community_parser.add_argument("--no-spark", action="store_true", help="Do not use Spark.")
    add_spark_args(community_parser)

    # ------------------------------------------------------------------------
    dumpcmd_parser = subparsers.add_parser(
        "dumpcmd", help="Output the detected communities to stdout.")
    dumpcmd_parser.set_defaults(handler=dumpcmd)
    dumpcmd_parser.add_argument("input", help="Path to the communities ASDF model.")
    add_template_args(dumpcmd_parser, "report.md.jinja2")
    add_cassandra_args(dumpcmd_parser)

    # ------------------------------------------------------------------------
    evalcc_parser = subparsers.add_parser(
        "evalcc", help="Evaluate the communities: calculate the precise similarity and the "
                       "fitness metric.")
    evalcc_parser.set_defaults(handler=evaluate_communities)
    evalcc_parser.add_argument("-t", "--threshold", required=True, type=float,
                               help="Jaccard similarity threshold.")
    evalcc_parser.add_argument("-i", "--input", required=True,
                               help="Path to the communities model.")

    add_spark_args(evalcc_parser, default_packages=[CASSANDRA_PACKAGE])
    add_cassandra_args(evalcc_parser)

    # TODO: retable [.....] -> [.] [.] [.] [.] [.]
    return parser


def main():
    """
    Creates all the argument parsers and invokes the function from set_defaults().

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
