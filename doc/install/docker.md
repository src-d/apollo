# Docker image installation

### Requirements

* NVIDIA GPU

Needed to be installed:

* [Babelfish](https://doc.bblf.sh/user/getting-started.html) as `bblfshd`.
* [Cassandra](https://hub.docker.com/r/library/cassandra/) or [ScyllaDB](https://hub.docker.com/r/scylladb/scylla/) as `cassandra`.

### Magic command

```
docker run --rm -it srcd/apollo --help
```

We will imply the following command by `apollo` throughout the examples:

```
docker run -it --rm -v /path/to/io:/io -w /io --privileged --link bblfshd --link cassandra srcd/apollo
```

`--privileged` is needed to access the NVIDIA devices inside the container without the pain of
manually specifying them, can be replaced with `--device /dev/nvidiactl --device /dev/nvidia-uvm --device /dev/nvidia0` +
other `/dev/nvidia*` if you've got multiple cards.