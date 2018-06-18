# Feature arguments

For all of the commands that extract features, you must specify the following arguments:

- `-x`/`--mode`: Mode to select for analysis, defaults to `file`, can also be `repo` or `func`
- `--quant`: Path to the input or output Quantization Levels model (optional, only supported for
the `children` extractor)
- `-f`/`--feature`: Features to extract from each item, at the moment among the ones below


| Feature  | Description                        |
|----------|:---------------------------------:|
| graphlet | Converts the UAST to a weighted bag of graphlets, a graphlet of a UAST node is composed from the node itself, its parent and its children|
| lit      | Converts the UAST to a weighted bag of literals (UAST node role)
| id       | Converts the UAST to a weighted bag of identifiers (UAST node role)
| children | Converts the UAST to a bag of (internal type, quantized number of children) pairs, see [quantization](https://en.wikipedia.org/wiki/Quantization_(signal_processing)) for more info |  
| uast2seq | Converts the UAST to a bag of sequences of nodes, we use Depth First Search for the traversal of the UAST | 
| node2vec | Converts the UAST to a bag of vectorized sequences produced through a random walk

You can check out the [Babelfish documentation](https://doc.bblf.sh/) for more information about UASTs. The weights of each feature in a bag are always computed from the observed frequencies. 

- `--<feature>-<arg>`: For each of the above features you can also specify arguments:

| Feature  | Flag                              | Default | Description |
|----------|:---------------------------------:|:-------:|:------------:|
| graphlet | --graphlet-weight                 | 1       | Weight of this feature relative to the others (used by TF-IDF) |
| lit      | --lit-weight                      | 1       | Weight of this feature relative to the others (used by TF-IDF) |
| id       | --id-split-stem                   | False   | Whether to split identifiers and consider each part to be a separate one, or not |
| id       | --id-weight                       | 1       | Weight of this feature relative to the others (used by TF-IDF) |
| children | --children-npartitions            | 10      | Number of partitions on which we apply quantization |
| uast2seq | --uast2seq-weight                 | 1       | Weight of this feature relative to the others (used by TF-IDF) |
| uast2seq | --uast2seq-seq-len                | 5       | Length(s) of sequences, can be a list |
| uast2seq | --uast2seq-stride                 | 1       | Stride used to iterate through the sequenced UAST to extract subsequences of chosen length |
| node2vec | --node2vec-weight                 | 1       | Weight of this feature relative to the others (used by TF-IDF)
| node2vec | --node2vec-seq-len                | (5, 6)  | Length(s) of sequences to be vectorized, can be a list
| node2vec | --node2vec-p-explore-neighborhood | 0.5     | Likelihood of immediately revisiting a node in the walk (*return parameter*)|
| node2vec | --node2vec-stride                 | 1       | Strides used to iterate through the walk sequences to extract subsequences of chosen length |
| node2vec | --node2vec-seed                   | 42      | Seed to use to generate the random walk |
| node2vec | --node2vec-q-leave-neighborhood   | 0.5     | Modulates the ability to differentiate between inward and outward nodes (*in out parameter*) |
| node2vec | --node2vec-n-walks                | 5       | Number of walks from each node. |
| node2vec | --node2vec-n-steps                | 19      | Number of steps in each walk. |