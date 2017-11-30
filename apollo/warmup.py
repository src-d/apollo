from sourced.ml.engine import create_engine


def warmup(args):
    create_engine("warmup", "/tmp", args)
