import logging

from sourced.ml.repo2 import wmhash


def hash_batches(args):
    log = logging.getLogger("hash")
    log.info("Loading files from %s", args.input)
    loader = wmhash.BagsBatchParquetLoader(args.input)
    batches = list(loader)
    log.info("%d batches, shapes: %s", len(batches),
             ", ".join(str(b.matrix.shape) for b in batches))
