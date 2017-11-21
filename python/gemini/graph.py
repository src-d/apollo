import logging

from gemini.cassandra_utils import get_db


def print_hash_graph(args):
    log = logging.getLogger("graph")
    session = get_db(args)
    table = args.tables["hashtables"]
    rows = session.execute("SELECT DISTINCT hashtable FROM %s" % table)
    hashtables = sorted(r.hashtable for r in rows)
    log.info("Detected %d hashtables", len(hashtables))
    for hashtable in hashtables:
        log.info("Fetching %d", hashtable)
        rows = session.execute(
            "SELECT sha1, value FROM %s WHERE hashtable=%d" % (table, hashtable))
        band = None
        group = set()
        for row in rows:
            if row.value != band:
                band = row.value
                group.clear()
                group.add(row.sha1)
                continue
            myself = row.sha1
            for sha1 in group:
                print(myself, sha1)
            group.add(myself)
