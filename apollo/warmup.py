from sourced.ml.utils import create_engine


def warmup(args):
    create_engine("warmup", "/tmp", args)
