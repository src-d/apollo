# Hash command

__Currently does not work in Spark Cluster mode.__

This command applies the MinHashCUDA algorithm on previously written batches, stores hashes and hash tables in DB and saves the Weighted MinHash (WMH) parameters.

- `-i`/`--input`: Path to the input batch(es)
- `--seed`: Specific random generator (useful for cross execution comparisons), default to a random number depending of the time
- `--mhc-verbosity`: MinHashCuda log level, specify 0 for silence or 2 for full logs, 1 is the default and just shows progress
- `--devices`: Index of NVIDIA device to use, defaults to 0 (all available)
- `--docfreq`: Path to the input Ordered Document Frequency model
- `--size`: Hash size, defaults to 128
- `--partitions`: to repartition data, this will specify new number of partitions 
- `--shuffle`: to repartition data, this will allow data shuffling (vital if number of partitions increases !) 
- [Cassandra/Scylla arguments](db.md)
- [Spark arguments](https://github.com/src-d/ml/blob/master/doc/spark.md)

You must also specify WMH arguments:

- `-p`/`--params`: Path to the output WMH parameters
- `-t`/`--threshold`: Jacquard Similarity threshold (float in [0,1]) over which we consider there is similarity
- `--false-positive-weight`: Parameter that adjusts the relative importance of minimizing false positives count when optimizing for the Jacquard similarity threshold, default to .5
- `--false-negative-weight`: Same for false negatives
