import logging
import json

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import RoundRobinPolicy


def patch_tables(args):
    if args.tables and isinstance(args.tables, str):
        tables = args.tables
    else:
        tables = ""
    defaults = ("bags", "hashes", "hashtables", "hashtables2")
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
    else:
        cql("DROP TABLE IF EXISTS %s" % tables["hashes"])
        cql("DROP TABLE IF EXISTS %s" % tables["hashtables"])
        cql("DROP TABLE IF EXISTS %s" % tables["hashtables2"])
    cql("CREATE TABLE %s (sha1 ascii, value blob, PRIMARY KEY (sha1))" % tables["hashes"])
    cql("CREATE TABLE %s (sha1 ascii, hashtable tinyint, value blob, "
        "PRIMARY KEY (hashtable, value, sha1))" % tables["hashtables"])
    cql("CREATE TABLE %s (sha1 ascii, hashtable tinyint, value blob, "
        "PRIMARY KEY (sha1, hashtable))" % tables["hashtables2"])
