def configure(args):
    cas_host, cas_port = args.cassandra.split(":")
    args.config.append("spark.cassandra.connection.host=" + cas_host)
    args.config.append("spark.cassandra.connection.port=" + cas_port)
    return args
