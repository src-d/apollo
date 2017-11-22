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
docker run -it --rm -v /path/to/io:/io --link bblfshd --link scylla srcd/apollo bags -r /io/siva --batches /io/batches --docfreq /io/docfreq.asdf -f id -l Java -s 'local[*]' --min-docfreq 5 --bblfsh bblfshd --cassandra scylla:9042 --persist MEMORY_ONLY
```

### Hashing

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo hash /io/batches -p /io/params.asdf -t 0.9 --cassandra scylla:9042
```

### Query sha1

```
docker run -it --rm -v /path/to/io:/io --link scylla srcd/apollo query -i ef8b407d95028e94ada6fe18badeb44c9d1bfe22 --precise --docfreq /io/docfreq.asdf -t 0.9 --cassandra scylla:9042
```

### Query file

```
docker run -it --rm -v /path/to/io:/io -v .:/q --link bblfshd --link scylla srcd/apollo query -f /q/myfile.java --bblfsh bblfshd --cassandra scylla:9042 --precise --docfreq /io/docfreq.asdf --params /io/params.asdf -t 0.9
```

### Dump hash graph

```
docker run -it --rm --link scylla srcd/apollo hashgraph --cassandra scylla:9042 | sort | uniq > graph.txt
```
