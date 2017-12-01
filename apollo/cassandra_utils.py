from collections import defaultdict
from datetime import datetime
import logging
import json
import platform
import re
import sys

import modelforge.logs
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import RoundRobinPolicy


def patch_tables(args):
    if args.tables and isinstance(args.tables, str):
        tables = args.tables
    else:
        tables = ""
    defaults = ("bags", "meta", "hashes", "hashtables", "hashtables2")
    args.tables = {n: n for n in defaults}
    if tables:
        args.tables.update(json.loads(tables))


def configure(args):
    cas_host, cas_port = args.cassandra.split(":")
    args.config.append("spark.cassandra.connection.host=" + cas_host)
    args.config.append("spark.cassandra.connection.port=" + cas_port)
    patch_tables(args)
    return args


def get_db(args):
    log = logging.getLogger("cassandra")
    patch_tables(args)
    cashost, casport = args.cassandra.split(":")

    def get_cluster():
        return Cluster((cashost,), port=int(casport),
                       load_balancing_policy=RoundRobinPolicy())
    cluster = get_cluster()
    log.info("Connecting to %s", args.cassandra)
    try:
        session = cluster.connect(args.keyspace)
    except NoHostAvailable:
        log.warning("Keyspace %s does not exist", args.keyspace)
        cluster = get_cluster()
        session = cluster.connect()
    return session


def reset_db(args):
    db = get_db(args)

    def cql(cmd):
        print(cmd + ";")
        db.execute(cmd)

    if not args.hashes_only:
        cql("DROP KEYSPACE IF EXISTS %s" % args.keyspace)
        cql("CREATE KEYSPACE %s WITH REPLICATION = {"
            "'class' : 'SimpleStrategy', 'replication_factor' : 1}" % args.keyspace)
        print("USE %s;" % args.keyspace)
        db.set_keyspace(args.keyspace)
    tables = args.tables
    if not args.hashes_only:
        cql("CREATE TABLE %s (sha1 ascii, item ascii, value float, PRIMARY KEY (sha1, item))"
            % tables["bags"])
        cql("CREATE TABLE %s (sha1 ascii, url text, PRIMARY KEY (sha1, url))"
            % tables["meta"])
    else:
        cql("DROP TABLE IF EXISTS %s" % tables["hashes"])
        cql("DROP TABLE IF EXISTS %s" % tables["hashtables"])
        cql("DROP TABLE IF EXISTS %s" % tables["hashtables2"])
    cql("CREATE TABLE %s (sha1 ascii, value blob, PRIMARY KEY (sha1))" % tables["hashes"])
    cql("CREATE TABLE %s (sha1 ascii, hashtable tinyint, value blob, "
        "PRIMARY KEY (hashtable, value, sha1))" % tables["hashtables"])
    cql("CREATE TABLE %s (sha1 ascii, hashtable tinyint, value blob, "
        "PRIMARY KEY (sha1, hashtable))" % tables["hashtables2"])


def sha1_to_url(args):
    session = get_db(args)
    sha1re = re.compile("([a-f0-9]{40})")
    buffer = []
    pending = set()
    batch_size = args.batch * 2  # 50% are hashes

    def output():
        nonlocal buffer, pending
        if not buffer:
            return
        query = [s[0] for s in buffer[:batch_size] if isinstance(s, tuple)]
        rows = session.execute("select sha1, url from %s where sha1 in (%s)" % (
            args.tables["meta"], ",".join("'%s'" % q for q in query)
        ))
        pending -= set(query)
        urls = defaultdict(list)
        for r in rows:
            urls[r.sha1].append(r.url)
        for s in buffer[:batch_size]:
            if isinstance(s, tuple):
                myurls = urls.get(s[0])
                if not myurls:
                    sys.stdout.write(s[0])
                elif len(myurls) == 1:
                    sys.stdout.write(myurls[0])
                else:
                    sys.stdout.write("[%s]" % " ".join(myurls))
            else:
                sys.stdout.write(s)
        buffer = buffer[batch_size:]

    for line in sys.stdin:
        splitted = sha1re.split(line)
        hashes = splitted[-2:0:-1]
        pending.update(hashes)
        if hashes:
            parity = splitted[0] == hashes[-1]
        else:
            parity = 0  # there is always a trailing \n
        for i, s in enumerate(splitted):
            if i % 2 == parity:
                buffer.append(s)
            else:
                buffer.append((s,))
        while len(buffer) > batch_size:
            output()
    while buffer:
        output()


class ColorFormatter(logging.Formatter):
    """
    logging Formatter which prints messages with colors.
    """
    GREEN_MARKERS = [" ok", "ok:", "finished", "completed", "ready",
                     "done", "running", "success", "saved"]
    GREEN_RE = re.compile("|".join(GREEN_MARKERS))
    BEER_MUG = platform.uname().release.endswith("-moby")
    FUR_TREE = datetime.now().month == 12 and datetime.now().day >= 8

    def formatMessage(self, record):
        level_color = "0"
        text_color = "0"
        fmt = ""
        if record.levelno <= logging.DEBUG:
            fmt = "\033[0;37m" + logging.BASIC_FORMAT + "\033[0m"
        elif record.levelno <= logging.INFO:
            level_color = "1;36"
            lmsg = record.message.lower()
            if self.GREEN_RE.search(lmsg):
                text_color = "1;32"
        elif record.levelno <= logging.WARNING:
            level_color = "1;33"
        elif record.levelno <= logging.CRITICAL:
            level_color = "1;31"
        if self.BEER_MUG:
            spice = "🍺 "
        elif self.FUR_TREE:
            spice = "🎄 "
        else:
            spice = ""
        if not fmt:
            fmt = "\033[" + level_color + \
                  "m" + spice + "%(levelname)s\033[0m:%(name)s:\033[" + text_color + \
                  "m%(message)s\033[0m"
        return fmt % record.__dict__

modelforge.logs.ColorFormatter = ColorFormatter
