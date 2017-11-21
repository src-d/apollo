import codecs
import logging
import numpy

from cassandra.cluster import Cluster
from sourced.ml.repo2 import wmhash

from gemini.hasher import hash_file, calc_hashtable_params


def query(args):
    log = logging.getLogger("query")
    cashost, casport = args.cassandra.split(":")
    cluster = Cluster((cashost,), int(casport))
    log.info("Connecting to %s", args.cassandra)
    session = cluster.connect(args.keyspace)
    if args.id:
        rows = session.execute(
            "SELECT hashtable, value FROM hashtables2 WHERE sha1='%s'" % args.id)
        bands = [(r.hashtable, r.value) for r in rows]
    else:
        # args.file
        wmh, bag = hash_file(args.file, args.params, args.docfreq, args.bblfsh, args.feature)
        htnum, band_size = calc_hashtable_params(
            args.threshold, len(wmh) // 8, args.false_positive_weight, args.false_negative_weight)
        log.info("Number of hash tables: %d", htnum)
        log.info("Band size: %d", band_size)
        bands = [(i, bytearray(wmh[i * band_size:(i + 1) * band_size].data))
                 for i in range(htnum)]
    similar = set()
    log.info("Looking for similar items")
    for i, band in bands:
        rows = session.execute(
            "SELECT sha1 FROM hashtables WHERE hashtable=%d AND value=0x%s"
            % (i, codecs.encode(band, "hex").decode()))
        similar.update(r.sha1 for r in rows)
    log.info("Fetched %d items", len(similar))
    if args.precise:
        # Precise bags
        vocab = wmhash.OrderedDocumentFrequencies().load(args.docfreq)
        log.info("Calculating the precise result")
        if args.id:
            rows = session.execute("SELECT item, value FROM bags WHERE sha1='%s'" % args.id)
            bag = numpy.zeros(len(vocab), dtype=numpy.float32)
            for row in rows:
                bag[vocab.order[row.item]] = row.value
        # Fetch other bags from the DB
        precise = []
        for x in similar:
            rows = session.execute("SELECT item, value FROM bags WHERE sha1='%s'" % x)
            other_bag = numpy.zeros(len(vocab), dtype=numpy.float32)
            for row in rows:
                other_bag[vocab.order[row.item]] = row.value
            if weighted_jaccard(bag, other_bag) >= args.threshold:
                precise.append(x)
            log.info("Survived: %.2f", len(precise) / len(similar))
        similar = precise
    for h in similar:
        print(h)


def weighted_jaccard(vec1, vec2):
    return numpy.minimum(vec1, vec2).sum() / numpy.maximum(vec1, vec2).sum()
