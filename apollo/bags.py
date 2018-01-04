import logging
import glob
import os
import shutil
import tempfile
from uuid import uuid4


from sourced.ml.utils import create_engine
from sourced.ml.extractors import __extractors__
from sourced.ml.transformers import UastExtractor, Transformer, Cacher, UastDeserializer, Engine, \
    FieldsSelector, ParquetSaver, Repo2WeightedSet, Repo2DocFreq, BagsBatchSaver, BagsBatcher

from pyspark.sql.types import Row

from apollo import cassandra_utils


class BagsSaver(Transformer):
    def __init__(self, keyspace, table, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace
        self.table = table

    def __call__(self, head):
        rows = head.flatMap(self.explode)
        if self.explained:
            self._log.info("toDebugString():\n%s", rows.toDebugString().decode())
        rows.toDF() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=self.table, keyspace=self.keyspace) \
            .save()

    def explode(self, record):
        key = record[0]
        for col, val in record[1].items():
            yield Row(sha1=key, item=col, value=float(val))


class MetadataSaver(Transformer):
    def __init__(self, keyspace, table, **kwargs):
        super().__init__(**kwargs)
        self.keyspace = keyspace
        self.table = table

    def __call__(self, head):
        rows = head.rdd.map(lambda x: Row(
            sha1=x.blob_id, repo=x.repository_id, commit=x.commit_hash, path=x.path))
        if self.explained:
            self._log.info("toDebugString():\n%s", rows.toDebugString().decode())
        rows.toDF() \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=self.table, keyspace=self.keyspace) \
            .save()


class DzhigurdaFiles(Transformer):
    def __init__(self, dzhigurda, **kwargs):
        super().__init__(**kwargs)
        self.dzhigurda = dzhigurda

    def __call__(self, engine):
        commits = engine.repositories.references.head_ref.commits
        if self.dzhigurda < 0:
            # Use all available commits
            chosen = commits
        else:
            chosen = commits.filter(commits.index <= self.dzhigurda)
        return chosen.tree_entries.blobs


def _prepare_chunks(directory: str, n_files: int=-1):
    """
    Purpose: limit number of siva files to be processed by engine.
    Generate temporary directories with maximum number of symlinks to files equal to n_files.

    :param directory: directory with files
    :param n_files: number of files per chunk. If negative then use all of them
    :return: generator of temporary directories (with links to siva files) and flag to delete it or
             not
    """
    if n_files < 0:
        delete_directory = False
        yield directory, delete_directory
    else:
        delete_directory = True
        assert n_files > 0
        siva_files = list(sorted(glob.iglob(os.path.normpath(directory) + "/**/*.siva",
                                            recursive=True)))

        for i in range(0, len(siva_files), n_files):
            chunk = siva_files[i:min(i + n_files, len(siva_files))]

            tmp_dir = os.path.join(tempfile.gettempdir(), str(uuid4()))
            assert not os.path.exists(tmp_dir)
            os.makedirs(tmp_dir)

            # create symlinks to files
            for file in chunk:
                _, filename = os.path.split(file)
                os.symlink(file, os.path.join(tmp_dir, filename))
            yield tmp_dir, delete_directory


def preprocess_source(args):
    log = logging.getLogger("preprocess_source")
    if os.path.exists(args.output):
        log.critical("%s must not exist", args.output)
        return 1
    if not args.config:
        args.config = []

    try:
        engine_args = args.__dict__.copy()
        del engine_args["repositories"]
        saved_dirs = []
        for i, (tmp_dir, delete_dir) in enumerate(_prepare_chunks(args.repositories,
                                                                  n_files=args.n_files)):
            log.info("%s chunk in progress" % (i + 1))
            # preprocessing pipeline
            engine = create_engine("source2bags-%s" % uuid4(), repositories=tmp_dir, **engine_args)
            pipeline = Engine(engine, explain=args.explain).link(DzhigurdaFiles(args.dzhigurda))
            uasts = pipeline.link(UastExtractor(languages=[args.language]))
            fields = uasts.link(FieldsSelector(fields=args.fields))
            # save to subdirectory
            saved_dirs.append(os.path.join(args.output, str(uuid4())))
            saver = fields.link(ParquetSaver(save_loc=saved_dirs[-1]))
            saver.explode()

            if delete_dir:
                for file in glob.iglob(os.path.normpath(tmp_dir) + "/*.siva"):
                    # never delete real files, only symlinks
                    assert os.path.islink(file), "%s is not symlink" % file

                shutil.rmtree(tmp_dir)
    finally:
        if args.pause:
            input("Press Enter to exit...")


def source2bags(args):
    log = logging.getLogger("bags")
    if os.path.exists(args.batches):
        log.critical("%s must not exist", args.batches)
        return 1
    if not args.config:
        args.config = []
    try:
        cassandra_utils.configure(args)
        engine = create_engine("source2bags-%s" % uuid4(), **args.__dict__)
        extractors = [__extractors__[s](
            args.min_docfreq, **__extractors__[s].get_kwargs_fromcmdline(args))
            for s in args.feature]
        pipeline = Engine(engine, explain=args.explain).link(DzhigurdaFiles(args.dzhigurda))
        uasts = pipeline.link(UastExtractor(languages=[args.language]))
        if args.persist is not None:
            uasts = uasts.link(Cacher(args.persist))
        uasts.link(MetadataSaver(args.keyspace, args.tables["meta"]))
        uasts = uasts.link(UastDeserializer())
        uasts.link(Repo2DocFreq(extractors))
        pipeline.explode()
        bags = uasts.link(Repo2WeightedSet(extractors))
        if args.persist is not None:
            bags = bags.link(Cacher(args.persist))
        batcher = bags.link(BagsBatcher(extractors))
        batcher.link(BagsBatchSaver(args.batches, batcher))
        bags.link(BagsSaver(args.keyspace, args.tables["bags"]))
        bags.explode()
        log.info("Writing docfreq to %s", args.docfreq)
        batcher.model.save(args.docfreq)
        if args.graph:
            log.info("Dumping the graph to %s", args.graph)
            with open(args.graph, "w") as f:
                pipeline.graph(stream=f)
    finally:
        if args.pause:
            input("Press Enter to exit...")
