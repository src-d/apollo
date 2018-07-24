# EvalCC command

__Currently does not work in Spark Cluster mode.__

This command calculates the precise similarity and fitness metrics for the given Community Detection model, you can specify the following arguments:

- `-i`/`--input`: Path to the input Community Detection model;
- `-t`/`--threshold`: Jacquard Similarity threshold (float in [0,1]) over which we consider there is similarity - to calculate number of misses
- [Cassandra/Scylla arguments](db.md)
- [Spark arguments](https://github.com/src-d/ml/blob/master/doc/spark.md)

__Note:__

To run this command it is advised to set the Spark parameter `spark.default.parallelism` to a
higher value then the default 200 partitions if you are running on large amounts the data.
