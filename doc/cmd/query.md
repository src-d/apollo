# Query command

This command finds items similar to the one specified, and outputs them using the `query.md.jinja2` report file. There are two query modes, that are mutually exclusive. For both of them you can specify the following arguments:

- `--precise`: Whether to calculate the precise set or not
- `--template`: Path to `query.md.jinja2`
- `--batch`: Number of hashes to query simultaneously, defaults to 100
- [Cassandra/Scylla arguments](db.md)

**Id mode:**

In this mode, the file is already in the databases, it's features have been extracted. You only need to specify which file you wish to pick with:

- `-i`/`--id`: SHA1 identifier of the file.

**File mode:**

In this mode, the file is not in the database, so additionally we have to extract the bag of features from that file and apply the MinHashCUDA algorithm on them. You must specify the following arguments:

- `-c`/`--file`: Absolute path of the file
- `--bblfsh`: Same as in [engine arguments]((https://github.com/src-d/ml/blob/master/doc/spark.md))
- `--docfreq`: Path to the input Ordered Document Frequency model created while running the `bags`command (optional)
- `--min-docfreq`: Specific minimum document frequency of each feature, defaults to 1
- [Feature arguments](bags.md) also used by the `bags` command
- [WMH arguments](hash.md) also used by the `hash` command
