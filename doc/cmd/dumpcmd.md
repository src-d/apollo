# DumpCMD command

This command outputs a report on the given Community Detection model to stdout, you can specify the following arguments:

- `--template`: Path to the `report.md.jinja2` file
- `--batch`: Same as in `query` the number of hashes to query simultaneously, defaults to 100
- `-i`/`--input`: Path to the input Community Detection model
- [Cassandra/Scylla arguments](db.md)
