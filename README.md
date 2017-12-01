Apollo
======

### Installation

```
mount -o bind /path/to/sourced-ml bundle/ml
mount -o bind /path/to/spark-2.2.0-bin-hadoop2.7 bundle/spark
mount -o bind /path/to/sourced-engine bundle/engine
docker build -t srcd/apollo .
docker run --name scylla -p 9042:9042 -v /var/lib/scylla:/var/lib/scylla -d scylladb/scylla --developer-mode=1
docker run -it --rm --link scylla srcd/apollo resetdb --cassandra scylla:9042
docker run -d --name bblfshd --privileged -p 9432:9432 -v /var/lib/bblfshd:/var/lib/bblfshd bblfsh/bblfshd
docker exec -it bblfshd bblfshctl driver install --all
```

### Bags

```
docker run -it --rm -v /path/to/io:/io --link bblfshd --link scylla srcd/apollo bags -r /io/siva \
--batches /io/bags --docfreq /io/bags/docfreq.asdf -f id -f lit -f uast2seq --uast2seq-seq-len 4 \
-l Java -s 'local[*]' --min-docfreq 5 --bblfsh bblfshd --cassandra scylla:9042 --persist MEMORY_ONLY \
--config spark.executor.memory=4G --config spark.driver.memory=10G --config spark.driver.maxResultSize=4G
```

### Hash

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo hash /io/batches -p /io/bags/params.asdf \
-t 0.8 --cassandra scylla:9042
```

### Query sha1

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo query -i <sha1> --precise \
--docfreq /io/bags/docfreq.asdf -t 0.8 --cassandra scylla:9042 | docker -it --rm --link scylla \
srcd/apollo urls --cassandra scylla:9042
```

### Query file

```
docker run -it --rm -v /path/to/io:/io -v .:/q --link bblfshd --link scylla srcd/apollo query \
-f /q/myfile.java --bblfsh bblfshd --cassandra scylla:9042 --precise --docfreq /io/docfreq.asdf \
--params /io/params.asdf -t 0.9
```

### Connected components

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo cc -o /io/ccs.asdf
```

### Dump connected components

```
docker run -it --rm -v /path/to/io:/io srcd/apollo dumpcc -o /io/ccs.asdf | docker -it --rm \
--link scylla srcd/apollo urls --cassandra scylla:9042 
```

### Community detection

```
docker run -it --rm -v /path/to/io:/io srcd/apollo cmd -i /io/ccs.asdf -o /io/communities.asdf -s 'local[*]'
```

### Dump communities (final report)

```
docker run -it --rm -v /path/to/io:/io srcd/apollo dumpcmd -o /io/communities.asdf | docker -it --rm \
--link scylla srcd/apollo urls --cassandra scylla:9042 
```
