from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import RoundRobinPolicy


def create_session(keyspace="apollo"):
    cluster = Cluster(("localhost",), port=9042,
                      load_balancing_policy=RoundRobinPolicy())
    try:
        session = cluster.connect(keyspace)
    except NoHostAvailable:
        session = None
    return session


def count_table(session, table, keyspace="apollo"):
    for row in session.execute("SELECT COUNT(*) from %s.%s;" % (keyspace, table)):
        return row.count


def extract_row(session, table, keyspace="apollo"):
    for row in session.execute("SELECT * from %s.%s;" % (keyspace, table)):
        return row
