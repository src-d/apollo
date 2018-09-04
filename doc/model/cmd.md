# Communities Model

This model stores the communities detected by the `cmd` command from a previously
created Connected Component model. It's contents heavily depends on the algorithm 
chosen (and it's parameters), but more importantly by the edge creation method, 
as is described in [the doc](/doc/cmd/cmd.md). Indeed, if the default linear method 
is chosen, then the communities will not only consist of documents, but also
of **buckets**, as they will have been added to the CC graphs as artificial vertices. 
This means that, in this case, some communities may consist *only* of buckets.

The model has the following parameters:

- `cc.id_to_elements`: like in `sourced.ml`'s `BOW` model, a Python dictionary
mapping each document to it's name, e.g. if documents are files, then `cc.id_to_elements[i]`
is file `i`'s filename;
- `cc.communities`: a list of lists of integers, where each integer in `cc.communities[i]`
is in the `i`th community. If an element `e` in a community is an integer smaller 
then the length of the `cc.id_to_elements` dictionary, then it's a document. If not, 
it is the bucket number `e - len(cc.id_to_elements)` in the Connected Components 
model's `id_to_buckets` parameter which has been used as input.

The model also has this method:
- `cc.count_elements`: it counts the number of distinct documents in the communities
(not all documents in the dictionary may be in a community, as we don't care for 
communities of one). Buckets are not counted by this method. 

Example:

```
from apollo.graph import CommunitiesModel

cmd = CommunitiesModel().load("cc.asdf")
print(cmd.dump())  # prints the number of communities (even if containing only buckets)
print("Number of distinct documents: %s" % (cmd.count_elements()))
```