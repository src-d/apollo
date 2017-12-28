## Model
A model is the artifact from running an analysis pipeline.
It is plain data with some methods to access it.
A model can be serialized to bytes and deserialized from bytes.
The underlying storage format is specific to [src-d/modelforge](https://github.com/src-d/modelforge)
and is currently [ASDF](https://github.com/spacetelescope/asdf)
with [lz4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression.

## Pipeline
A tree of linked `sourced.ml.transformers.Transformer` objects which can be executed on PySpark/source{d} engine.
The result is often written on disk as [Parquet](https://parquet.apache.org/) or model files
or to a database.

## Feature
A property of the source code sample.

## Weighted MinHash
An algorithm to approximate the [Weighted Jaccard Similarity](https://en.wikipedia.org/wiki/Jaccard_index#Generalized_Jaccard_similarity_and_distance)
between all the pairs of source code samples in linear time and space. Described by
[Sergey Ioffe](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36928.pdf).