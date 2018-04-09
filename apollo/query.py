import codecs
import logging
import os
import sys

import jinja2
import numpy
from sourced.ml.models import OrderedDocumentFrequencies

from apollo.cassandra_utils import get_db, BatchedHashResolver
from apollo.hasher import hash_file, calc_hashtable_params


def query(args):
    log = logging.getLogger("query")
    session = get_db(args)
    tables = args.tables
    if args.id:
        rows = session.execute(
            "SELECT hashtable, value FROM %s WHERE sha1='%s'" % (tables["hashtables2"], args.id))
        bands = [(r.hashtable, r.value) for r in rows]
    else:
        # args.file
        if not args.feature:
            log.critical("-f / --feature must be specified at least once in file query mode")
            return 1
        if not args.params:
            log.critical("-p / --params must be specified in file query mode")
            return 1
        wmh, bag = hash_file(args)
        htnum, band_size = calc_hashtable_params(
            args.threshold, len(wmh), args.false_positive_weight, args.false_negative_weight)
        log.info("Number of hash tables: %d", htnum)
        log.info("Band size: %d", band_size)
        bands = [(i, bytearray(wmh[i * band_size:(i + 1) * band_size].data))
                 for i in range(htnum)]
    similar = set()
    log.info("Looking for similar items")
    for i, band in bands:
        rows = session.execute(
            "SELECT sha1 FROM %s WHERE hashtable=%d AND value=0x%s"
            % (tables["hashtables"], i, codecs.encode(band, "hex").decode()))
        similar.update(r.sha1 for r in rows)
    log.info("Fetched %d items", len(similar))
    if args.precise:
        # Precise bags
        vocab = OrderedDocumentFrequencies().load(args.docfreq)
        log.info("Calculating the precise result")
        if args.id:
            rows = session.execute(
                "SELECT item, value FROM %s WHERE sha1='%s'" % (tables["bags"], args.id))
            bag = numpy.zeros(len(vocab), dtype=numpy.float32)
            for row in rows:
                bag[vocab.order[row.item]] = row.value
        # Fetch other bags from the DB
        precise = []
        for x in similar:
            rows = session.execute(
                "SELECT item, value FROM %s WHERE sha1='%s'" % (tables["bags"], x))
            other_bag = numpy.zeros(len(vocab), dtype=numpy.float32)
            for row in rows:
                other_bag[vocab.order[row.item]] = row.value
            if weighted_jaccard(bag, other_bag) >= args.threshold:
                precise.append(x)
            log.info("Survived: %.2f", len(precise) / len(similar))
        similar = precise
    if args.id:
        try:
            similar.remove(args.id)
        except KeyError:
            # o_O
            pass

    similar = [s.split("@")[1] for s in similar]
    stream_template(args.template, sys.stdout, size=len(similar),
                    origin=args.id if args.id else os.path.abspath(args.file),
                    items=BatchedHashResolver(similar, args.batch, session, tables["meta"]))


def weighted_jaccard(vec1, vec2):
    return numpy.minimum(vec1, vec2).sum() / numpy.maximum(vec1, vec2).sum()


def format_url(repo, commit, path):
    if repo.endswith(".git"):
        repo = repo[:-4]
    if repo.startswith("github.com") or repo.startswith("gitlab.com"):
        return "https://%s/blob/%s/%s" % (repo, commit, path)
    if repo.startswith("bitbucket.org"):
        return "https://%s/src/%s/%s" % (repo, commit, path)
    return "[%s %s %s]" % (repo, commit, path)


def stream_template(name, dest, **kwargs):
    log = logging.getLogger("jinja2")
    log.info("Loading the template")
    loader = jinja2.FileSystemLoader(("/", os.path.dirname(__file__), os.getcwd()),
                                     followlinks=True)
    env = jinja2.Environment(
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=False,
    )
    template = loader.load(env, name)
    log.info("Rendering")
    template.stream(**kwargs, format_url=format_url).dump(dest)
