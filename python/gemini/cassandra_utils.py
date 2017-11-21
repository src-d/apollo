import logging

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import RoundRobinPolicy


def configure(args):
    cas_host, cas_port = args.cassandra.split(":")
    args.config.append("spark.cassandra.connection.host=" + cas_host)
    args.config.append("spark.cassandra.connection.port=" + cas_port)
    return args


def get_db(args):
    log = logging.getLogger("cassandra")
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

    cql("DROP KEYSPACE IF EXISTS %s" % args.keyspace)
    cql("CREATE KEYSPACE %s WITH REPLICATION = {"
        "'class' : 'SimpleStrategy', 'replication_factor' : 1}" % args.keyspace)
    print("USE %s;" % args.keyspace)
    db.set_keyspace(args.keyspace)
    cql("CREATE TABLE bags (sha1 ascii, item ascii, value float, PRIMARY KEY (sha1, item)) ;")
    cql("CREATE TABLE hashes (sha1 ascii, value blob, PRIMARY KEY (sha1));")
    cql("CREATE TABLE hashtables (sha1 ascii, hashtable tinyint, value blob, "
        "PRIMARY KEY (hashtable, value));")
    cql("CREATE TABLE hashtables2 (sha1 ascii, hashtable tinyint, value blob, "
        "PRIMARY KEY (sha1, hashtable));")
