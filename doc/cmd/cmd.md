# CMD command

__Currently does not work in Spark Cluster mode.__

This command runs the community detection on a previously created Connected Components 
model, and saves them in CCs in [this `Model`](/doc/model/cmd.md). You can specify 
the following arguments:

- `-i`/`--input`: Path to the input Connected Components model
- `-o`/`--output`: Path to the output Community Detection model
- `--edges`: Specific method to be used to generate edges, quadratic will connect each item in each bucket to all other items, while the  default, linear, will create for each bucket an artificial vertex to which all items will be connected. Depending on the method the edges will be created in quadratic or linear time, relatively to the number of buckets
- `--no-spark`: If you do not want to use Spark - *but who would want that ?*
- [Spark arguments](https://github.com/src-d/ml/blob/master/doc/spark.md) if you choose to use it
- `-a`/`--algorithm`: Community detection algorithm to apply, defaults to `walktrap`, check out the [igraph](http://igraph.org/c/doc/igraph-Community.html) doc to learn more. See below for the full list of available algorithms and their parameters as of today (see code in `igraph/__init__.py` for description of parameters), we excluded parameters that modify the returned object
- `-p`/`--params`: Depending on the algorithm, you may need to specify parameters (JSON format)

| Algorithm  | Parameters |
|:----------:|:----------:|
|community_fastgreedy|weights (for edges)|
|community_infomap|edge_weights, vertex_weights, trials|
|community_leading_eigenvector_naive|clusters|
|community_leading_eigenvector|clusters, weights (for edges), arpack_options|
|community_label_propagation|weights (for edges), initial, fixed|
|community_multilevel|weights (for edges)|
|community_optimal_modularity|weights (for edges)|
|community_edge_betweenness|weights (for edges), clusters, directed|
|community_spinglass|weights (for edges), spins, parupdate, start_temp, stop_temp, cool_fact, update_rule, gamma, _lambda, implementation|
|community_walktrap|weights (for edges), steps|
