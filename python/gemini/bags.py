import logging
import pickle
import sys
from uuid import uuid4

from ast2vec.engine import create_engine
from ast2vec.repo2 import wmhash
from ast2vec.repo2.base import UastExtractor


def source2bags(args):
    log = logging.getLogger("source2bags")
    engine = create_engine("source2bags-%s" % uuid4(), args.repositories, args)
    log.info("docfreq phase")
    extractors = [wmhash.__extractors__[s](args.min_docfreq) for s in args.feature]
    pipeline = UastExtractor(engine, languages=[args.language])
    repo2docfreq = wmhash.Repo2DocFreq(extractors)
    pipeline.link(repo2docfreq)
    try:
        pipeline.execute()
    except pickle.PicklingError as e:
        if e.__cause__ is not None and len(e.__cause__.args) == 2:
            for obj in e.__cause__.args[1]:
                print(obj, file=sys.stderr)
                print(file=sys.stderr)
        raise e from None
    log.info("bag phase")
    pipeline.unlink(repo2docfreq)
    pipeline.link(wmhash.Repo2WeightedSet(extractors)).link(wmhash.BagsBatcher(extractors))
    print(pipeline.execute())
