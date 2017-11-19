import logging

from gemini.hasher import hash_file, calc_hashtable_params


def query(args):
    log = logging.getLogger("query")
    if args.id:
        # TODO(vmarkovtsev): load from the DB
        wmh = None
    else:
        # args.file
        wmh, bag = hash_file(args.file, args.params, args.docfreq, args.bblfsh, args.feature)
    htnum, band_size = calc_hashtable_params(
        args.threshold, len(wmh) // 8, args.false_positive_weight, args.false_negative_weight)
    log.info("Number of hash tables: %d", htnum)
    log.info("Band size: %d", band_size)
    bands = [bytearray(wmh[i * band_size:(i + 1) * band_size].data) for i in range(htnum)]
    # TODO(vmarkovtsev): Query DB using these bands
    if args.precise:
        # Precise bags
        if args.id:
            # Fetch the bag from the DB
            pass
        # Fetch other bags from the DB
