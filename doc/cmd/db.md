# Cassandra/Scylla arguments

For all of the commands that require access to the database, you can specify the following arguments:

- `--cassandra`: Specific address of your Scylla/Cassandra DB, if you are not running it locally (the format is `<address>:<port>`, if you are using the default 9042 port then no need to specify it, e.g. `--cassandra scylla`)
- `--keyspace`: Specific name of the Cassandra key space, defaults to `apollo`
- `--tables`: Specific table mapping, use JSON format to modify it, e.g. `--tables {"bags": "bags_2", "hashes": "hashes_2", "hashtables": "hashtables_2", "hashtables2": "hashtables2_2"}`
