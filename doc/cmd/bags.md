# Bags command

This command converts input repositories to unordered weighted bags of features that are stored in DB, writes MinHashCuda batches, and writes the Ordered Documents Frequency model as well as the optional Quantization Levels model. You can specify the following arguments:

- `-r`/`--repositories` : Path to the input files
- `--parquet`: If your input files are Parquet files
- `--graph`: Path to the output Graphviz file, if you wish to keep the tree
- `-l`/`--languages` : Languages to keep, defaults to all languages detected by Babelfish
- `--dzhigurda`: Index of the last commit to keep, defaults to 0 (only the head), 1 is HEAD~2, etc
- `--bow`: Path to the output batches
- `--batch`: The maximum size of a single batch in bytes
- `--min-docfreq`: Specific minimum document frequency of each feature, defaults to 1
- `--docfreq-in`: Path to a precomputed Ordered Document Frequency model
- `--docfreq-out`: Path to the output Ordered Document Frequency model (can not be used with `docfreq-in`)
- `-v`/`--vocabulary-size`: to specify the maximum vocabulary size, defaults to 10 million
- `--cached-index-path`: Path to a precomputed Document Frequency model storing an index of the documents to be extracted
- `--partitions`: to repartition data, this will specify new number of partitions 
- `--shuffle`: to repartition data, this will allow data shuffling (vital if number of partitions increases !) 
- [Feature arguments](features.md)
- [Spark and Engine arguments](https://github.com/src-d/ml/blob/master/doc/spark.md)
- [Cassandra/Scylla arguments](db.md)
