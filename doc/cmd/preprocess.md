# Preprocess command

This command computes the index and Ordered Document Frequency model for the input repositories, 
and optionally the Quantization Levels model if selected features support it. Currently running the
bags command on large inputs can result in failures, this allows you to create all the necessary
data to run on subsets of your repositories. As you will be applying TF-IDF, be aware that your 
subsets must be disjoint, i.e. if you are running in `repo` mode then repos **must not** be spread 
out in different subsets, or there will be duplicate features. You can specify the following 
arguments:

- `-r`/`--repositories` : Path to the input files
- `--parquet`: If your input files are Parquet files
- `--graph`: Path to the output Graphviz file, if you wish to keep the tree
- `-l`/`--languages` : Languages to keep, defaults to all languages detected by Babelfish
- `--dzhigurda`: Index of the last commit to keep, defaults to 0 (only the head), 1 is HEAD~2, etc
- `--bow`: Path to the output batches
- `--batch`: The maximum size of a single batch in bytes
- `--min-docfreq`: Specific minimum document frequency of each feature, defaults to 1
- `--docfreq-out`: Path to the output Ordered Document Frequency model
- `-v`/`--vocabulary-size`: to specify the maximum vocabulary size, defaults to 10 million
- `--cached-index-path`: Path to the output Document Frequency model storing the index of all documents
- `--partitions`: to repartition data, this will specify new number of partitions 
- `--shuffle`: to repartition data, this will allow data shuffling (vital if number of partitions increases !) 
- [Feature arguments](features.md)
- [Spark and Engine arguments](https://github.com/src-d/ml/blob/master/doc/spark.md)
