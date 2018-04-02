Apollo
======

Advanced code deduplicator from hell. Powered by [source\{d\} ML](https://github.com/src-d/ml),
[source\{d\} engine](https://github.com/src-d/engine) and [minhashcuda](https://github.com/src-d/minhashcuda).
Agnostic to the analysed language thanks to [Babelfish](https://doc.bblf.sh). Python 3, PySpark, CUDA inside.

### What is this?

source{d}'s effort to research and solve the code deduplication problem. At scale, as usual.
A [code clone](https://en.wikipedia.org/wiki/Duplicate_code) is several snippets of code with few differences.
For now this project focuses on find near-duplicate projects and files; it will eventually support
functions and snippets in the future.

### Should I use it?

If you've got hundreds of thousands of files or more, consider. Otherwise, use one of the many
existing tools which may be already integrated into your IDE.

### Difference from [src-d/gemini](https://github.com/src-d/gemini)?

This guy is my brother. Apollo focuses on research, extensibility, flexibility and rapid
changes, while Gemini focuses on performance and serious production usage. All the proven and 
tested features will be eventually ported to Gemini. At the same time, Gemini may reuse some
of Apollo's code.

### Algorithm

Apollo takes the "hash'em all" approach. We extract unordered weighted features from code aka "weighted bags",
apply [Weighted MinHash](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36928.pdf)
and then design the [Locality Sensitive Hashing index](http://infolab.stanford.edu/~ullman/mmds/ch3.pdf).
All items which appear in the same hashtable bucket are considered the same. The size of the hash
and the number of hashtables depend on the [weighted Jaccard similarity](https://en.wikipedia.org/wiki/Jaccard_index#Generalized_Jaccard_similarity_and_distance)
threshold (hence Weighted MinHash).

The features include identifiers such as variable, function or class names, literal values and *structural elements*.
The latter carries the topological information, and we currently support several variants: "node2vec",
"deterministic node2vec" and "role-children atoms". Graphlets are upcoming. Different features
have different weights which will be tuned by a hyperparameter optimization algorithm or even an SGD
(not yet implemented).

It's not all unfortunately! Dumping the huge graph of pairwise similarities is of little practicality.
We need to group (cluster) the neighborhoods of densely connected nodes. Apollo solves this problem
in two steps:

1. Run [connected components](https://en.wikipedia.org/wiki/Connected_component_(graph_theory))
analysis to find disjoint parts in the similarity graph.
2. Run [community detection](https://en.wikipedia.org/wiki/Community_structure) to cluster the components.
The clusters are with overlaps.

### Implementation

Apollo is structured as a series of commands in CLI. It stores data in [Cassandra](http://cassandra.apache.org/)
(compatible with [Scylla](http://www.scylladb.com/)) and
writes MinHashCuda batches on disk. Community detection is delegated to [igraph](http://igraph.org/python/).

* `resetdb` (erases) and initializes a Cassandra keyspace.
* `bags` extracts the features, stores them in the database and writes MinHashCuda batches on disk.
Runs source{d} engine through PySpark.
* `hash` performs the hashing, writes the hashtables to the database and hashing parameters on disk
in [Modelforge](https://github.com/src-d/modelforge) format.
* `cc` fetches the buckets, runs the connected component analysis and writes the result on disk in Modelforge
format. Uses PySpark.
* `cmd` reads the connected components and performs the community detection (by default, walktrap).
Uses PySpark.
* `query` outputs items similar to the specified. In case of files, the path or the sha1 are accepted.
* `dumpcmd` outputs the groups of similar items.

### Installation

```
mount -o bind /path/to/sourced-ml bundle/ml
mount -o bind /path/to/spark-2.2.0-bin-hadoop2.7 bundle/spark
mount -o bind /path/to/sourced-engine bundle/engine
docker build -t srcd/apollo .
docker run --name scylla -p 9042:9042 -v /var/lib/scylla:/var/lib/scylla -d scylladb/scylla --developer-mode=1
docker run -it --rm --link scylla srcd/apollo resetdb --cassandra scylla
docker run -d --name bblfshd --privileged -p 9432:9432 -v /var/lib/bblfshd:/var/lib/bblfshd bblfsh/bblfshd
docker exec -it bblfshd bblfshctl driver install --all
```

You are going to need [grip](https://github.com/joeyespo/grip) to instantly render Markdown reports
in your browser. There multiple Docker options available, e.g.
[1](https://github.com/psycofdj/docker-grip), [2](https://github.com/fstab/docker-grip),
[3](https://github.com/kba/grip-docker).

### Contributions

...are welcome! See [CONTRIBUTING](CONTRIBUTING.md) and [code of conduct](CODE_OF_CONDUCT.md).

### License

[GPL](LICENSE.md).

## Docker command snippets

### Bags

```
docker run -it --rm -v /path/to/io:/io --link bblfshd --link scylla srcd/apollo bags -r /io/siva \
--bow /io/bags/bow.asdf --docfreq /io/bags/docfreq.asdf -f id -f lit -f uast2seq --uast2seq-seq-len 4 \
-l Java -s 'local[*]' --min-docfreq 5 --bblfsh bblfshd --cassandra scylla --persist MEMORY_ONLY \
--config spark.executor.memory=4G --config spark.driver.memory=10G --config spark.driver.maxResultSize=4G
```

### Hash

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo hash /io/batches/bow*.asdf -p /io/bags/params.asdf \
-t 0.8 --cassandra scylla
```

### Query sha1

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo query -i <sha1> --precise \
--docfreq /io/bags/docfreq.asdf -t 0.8 --cassandra scylla
```

### Query file

```
docker run -it --rm -v /path/to/io:/io -v .:/q --link bblfshd --link scylla srcd/apollo query \
-f /q/myfile.java --bblfsh bblfshd --cassandra scylla --precise --docfreq /io/docfreq.asdf \
--params /io/params.asdf -t 0.9 | grip -b -
```

### Connected components

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo cc -o /io/ccs.asdf
```

### Dump connected components

```
docker run -it --rm -v /path/to/io:/io srcd/apollo dumpcc -o /io/ccs.asdf
```

### Community detection

```
docker run -it --rm -v /path/to/io:/io srcd/apollo cmd -i /io/ccs.asdf -o /io/communities.asdf -s 'local[*]'
```

### Dump communities (final report)

```
docker run -it --rm -v /path/to/io:/io srcd/apollo dumpcmd /io/communities.asdf | grip -b -
```
