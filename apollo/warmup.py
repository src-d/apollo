from sourced.ml.extractors.helpers import filter_kwargs
from sourced.ml.utils import create_engine


def warmup(args):
    engine_args = filter_kwargs(args.__dict__, create_engine)
    create_engine("warmup", "/tmp", **engine_args)
