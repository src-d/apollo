# Connected Components Model

This model stores the connected components found in the pairwise similarity
graph after hashing by the `cc` command. 

**A quick reminder**

A document hashes to as many buckets as there are hashtables, which means if there are 
3 hashtables, then a document hashes to 3 buckets. The number of hashtables increases 
as the similarity threshold decreases. Any two documents that hash to at least one bucket 
in common are in the same component.

The model has the following parameters:

- `cc.id_to_cc`: a numpy array of integers of the size of the number of documents, where
document `i` is in the community number `cc.id_to_cc[i]`;
- `cc.id_to_elements`: like in `sourced.ml`'s `BOW` model, a Python dictionary
mapping each document to it's name, e.g. if documents are files, then `cc.id_to_elements[i]`
is file `i`'s filename;
- `cc.id_to_buckets`: a Scipy sparse CSR matrix of the shape `number of documents` 
x `number of buckets`, where the element in row `i` and column `j` is equal to 1 if
document `i` hashes to buck `j`, and 0 if not.

Example:

```
from apollo.graph import ConnectedComponentsModel

cc = ConnectedComponentsModel().load("cc.asdf")
print(cc.dump())  # prints the number of CCs and documents
```