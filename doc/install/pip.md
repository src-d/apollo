# Pip installation

### Requirements

* Python 3.4+
* Linux or macOS. **Windows will not work.**
* NVIDIA GPU

Needed to be installed:

* [source{d} engine](https://github.com/src-d/engine) with all of the dependencies such as Babelfish
* [libMHCUDA](https://github.com/src-d/minhashcuda) Python package
* [sourced.ml](https://github.com/src-d/ml) @ `develop` branch
* [Cassandra](http://cassandra.apache.org/) or [ScyllaDB](http://www.scylladb.com/)

### Magic command

```
pip3 install git+https://github.com/src-d/apollo
```

It should run without any errors.

### Testing

```
apollo --help
```