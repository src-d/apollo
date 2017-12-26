import logging
import os
from uuid import uuid4

from bblfsh import BblfshClient
from modelforge.model import Model
from modelforge.models import register_model
import numpy
from pyspark.sql.types import Row
from scipy.integrate import quad as integrate
from sourced.ml.utils import create_spark
from sourced.ml.transformers import BagsBatchParquetLoader
from sourced.ml.extractors import __extractors__

from apollo import cassandra_utils

#####################################################################################
# Begin code from https://github.com/ekzhu/datasketch/blob/master/datasketch/lsh.py #
#####################################################################################


def _false_positive_probability(threshold, b, r):
    def _probability(s):
        return 1 - (1 - s**float(r))**float(b)
    a, err = integrate(_probability, 0.0, threshold)
    return a


def _false_negative_probability(threshold, b, r):
    def _probability(s):
        return 1 - (1 - (1 - s**float(r))**float(b))
    a, err = integrate(_probability, threshold, 1.0)
    return a


def calc_hashtable_params(threshold, sample_size, false_positive_weight=0.5,
                          false_negative_weight=0.5):
    """
    Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
    of probabilities of false positive and false negative.

    :return: tuple(number of hashtables, size of each band).
    """
    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, sample_size + 1):
        max_r = int(sample_size / b)
        for r in range(1, max_r+1):
            fp = _false_positive_probability(threshold, b, r)
            fn = _false_negative_probability(threshold, b, r)
            error = fp*false_positive_weight + fn*false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt


#####################################################################################
# End code from https://github.com/ekzhu/datasketch/blob/master/datasketch/lsh.py   #
#####################################################################################


@register_model
class WeightedMinHashParameters(Model):
    """
    The randomly generated parameters of the Weighted MinHash-er.
    """
    NAME = "wmhparams"

    def construct(self, rs, ln_cs, betas):
        self.rs = rs
        self.ln_cs = ln_cs
        self.betas = betas
        rs[0] + ln_cs[0] + betas[0]  # do not remove - this loads the arrays from disk
        return self

    def _load_tree(self, tree):
        self.construct(rs=tree["rs"], ln_cs=tree["ln_cs"], betas=tree["betas"])

    def dump(self):
        return """Shape: %s""" % (self.rs.shape,)

    def _generate_tree(self):
        return {"rs": self.rs, "ln_cs": self.ln_cs, "betas": self.betas}


class HashExploder:
    def __init__(self, htnum, band_size):
        self.htnum = htnum
        self.band_size = band_size

    def __call__(self, record):
        key, wmh = record
        for hti in range(self.htnum):
            yield Row(sha1=key, hashtable=hti,
                      value=bytearray(wmh[hti * self.band_size:(hti + 1) * self.band_size].data))


def hash_batches(args):
    log = logging.getLogger("hash")
    log.info("Loading files from %s", args.input)
    loader = BagsBatchParquetLoader(args.input)
    batches = list(loader)
    log.info("%d batches, shapes: %s", len(batches),
             ", ".join(str(b.matrix.shape) for b in batches))
    if not batches:
        return
    htnum, band_size = calc_hashtable_params(
        args.threshold, args.size, args.false_positive_weight, args.false_negative_weight)
    log.info("Number of hash tables: %d", htnum)
    log.info("Band size: %d", band_size)
    cassandra_utils.configure(args)
    spark = create_spark("hash-%s" % uuid4(), args).sparkContext
    voc_size = batches[0].matrix.shape[-1]
    for b in batches:
        if b.matrix.shape[-1] != voc_size:
            raise ValueError("The vocabulary sizes does not match: %d != %d"
                             % (b.matrix.shape[-1], voc_size))
    log.info("Initializing the generator")
    deferred = os.path.isfile(args.params)
    import libMHCUDA  # delayed import which requires CUDA and friends
    gen = libMHCUDA.minhash_cuda_init(
        voc_size, args.size, seed=args.seed, devices=args.devices, verbosity=args.mhc_verbosity,
        deferred=deferred)
    if deferred:
        model = WeightedMinHashParameters().load(args.params)
        libMHCUDA.minhash_cuda_assign_vars(gen, model.rs, model.ln_cs, model.betas)
    else:
        log.info("Writing %s", args.params)
        params = libMHCUDA.minhash_cuda_retrieve_vars(gen)
        WeightedMinHashParameters().construct(*params).save(args.params)
    tables = args.tables
    try:
        for i, batch in enumerate(batches):
            log.info("Processing batch %d / %d", i + 1, len(batches))
            hashes = libMHCUDA.minhash_cuda_calc(gen, batch.matrix)
            job = [(k, h) for k, h in zip(batch.keys, hashes)]
            log.info("Saving the hashtables")
            df = spark.parallelize(job).flatMap(HashExploder(htnum, band_size)).toDF()
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=tables["hashtables"], keyspace=args.keyspace) \
                .save()
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=tables["hashtables2"], keyspace=args.keyspace) \
                .save()
            log.info("Saving the hashes")
            spark.parallelize(job) \
                .map(lambda x: Row(sha1=x[0], value=bytearray(x[1].data))) \
                .toDF() \
                .write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=tables["hashes"], keyspace=args.keyspace) \
                .save()
    finally:
        libMHCUDA.minhash_cuda_fini(gen)


def hash_file(args):
    if not args.feature:
        raise ValueError("extractors must not be empty")
    log = logging.getLogger("hash_file")
    vocab = OrderedDocumentFrequencies().load(args.docfreq)
    params = WeightedMinHashParameters().load(args.params)
    log.info("Extracting UAST from %s", args.file)
    uast = BblfshClient(args.bblfsh).parse(args.file).uast
    log.info("Populating the bag")
    extractors = [__extractors__[s](
        args.min_docfreq, **__extractors__[s].get_kwargs_fromcmdline(args))
        for s in args.feature]
    bag = numpy.zeros(len(vocab), dtype=numpy.float32)
    for ex in extractors:
        ex.ndocs = vocab.docs
        ex.docfreq = vocab
        for k, v in ex.extract(uast):
            bag[vocab.order[k]] = v
    log.info("Bag size: %d", len(bag.nonzero()[0]))
    log.info("Hashing")
    return weighted_minhash(bag, params.rs.shape[0], params.rs, params.ln_cs, params.betas), bag


def weighted_minhash(v, sample_size, rs, ln_cs, betas):
    hashvalues = numpy.zeros((sample_size, 2), dtype=numpy.uint32)
    vzeros = (v == 0)
    if vzeros.all():
        raise ValueError("Input is all zeros")
    v[vzeros] = numpy.nan
    vlog = numpy.log(v)
    v[vzeros] = 0
    for i in range(sample_size):
        t = numpy.floor((vlog / rs[i]) + betas[i])
        ln_y = (t - betas[i]) * rs[i]
        ln_a = ln_cs[i] - ln_y - rs[i]
        k = numpy.nanargmin(ln_a)
        hashvalues[i][0], hashvalues[i][1] = k, int(t[k])
    return hashvalues
