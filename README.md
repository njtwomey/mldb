# MLDB

## Motivation 

This is a simple and light-weight Pythonic library for lazy evaluation of computational graphs with a particular emphasis on define end-to-end pipelines for machine learning settings. 

This library is massively inspired by [HyperStream](https://github.com/IRC-SPHERE/HyperStream) which I collaborated on in the past. However, this is an incredibly lightweight version of HyperStream and the two are not at all equivalent. HyperStream is almost certainly a much better-suited library for most interesting purposes since it allows complex relationships between data sources, nodes, factors and plates to be defined in quite a general way. On the other hand, this library encodes minimal complexity on the graph itself, therefore requiring this functionality be handled by the functions instead.  

## Basic usage

Here is a minimal example of how the library can be used:

```python
from mldb import ComputationGraph

# A simple function that returns some data
def load_data(): 
    return [[1, 2, 3], [4, 5, 6]]

# Define the computational graph
graph = ComputationGraph()

# Define a node
node = graph.node(func=load_data)

# Print the node object
print(node) 
# Prints: <NodeWrapper sources=[] kwargs=[] factor=load_data sink=5c7afc35-184f-4830-ba08-5ae9ba47e67e>

# Evaluate the node
print(node.evaluate()) 
# Prints: [[1, 2, 3], [4, 5, 6]]
```

Since the example here is so straightforward, the usefulness of the library may not immediately be apparent. However, it is easy to build up a more complex set of relationships between nodes. For computational reasons, the library allows the output of nodes to be cached to eliminate redundant computation. Furthermore, one can direct the output of nodes to 'backends' which offer persistent caching of function outputs.  

The example below shows how one may branch out and extract features from the same root node (`data`), then how these features can be aggregated back into one node and cached to file. 

```python
from mldb import ComputationGraph, PickleBackend
import numpy as np 

np.random.seed(1234)

def load_data(): 
    print("loading data")
    return np.random.rand(100, 4)

def max_row(data): 
    print("finding max")
    return data.max(axis=1, keepdims=True)

def min_row(data): 
    print("finding min")
    return data.max(axis=1, keepdims=True)

def ptp_row(data): 
    print("finding peak-to-peak")
    return data.ptp(axis=1, keepdims=True)

def concat(**kwargs):  # Less general option: def concat(max_feats, min_feats, ptp_feats): ...
    print("concatenating features")
    return np.concatenate(
        [kwargs[key] for key in sorted(kwargs.keys())], 
        axis=1
    )

graph = ComputationGraph()

# Add a backend to the graph named "pickle"
graph.add_backend("pickle", PickleBackend("."))

# Load the data
data = graph.node(func=load_data)

# Extract features from the data
max_feats = graph.node(func=max_row, data=data)
min_feats = graph.node(func=min_row, data=data)
ptp_feats = graph.node(func=ptp_row, data=data)

# Aggregate the features and direct the output to the "pickle" backend
feats = graph.node(
    func=concat, name="feats", backend="pickle",
    max_feats=max_feats, 
    min_feats=min_feats, 
    ptp_feats=ptp_feats, 
)

print(feats) 
print(feats.evaluate().shape)  
```

The first time this script is executed, its output should be:

```
<NodeWrapper sources=[max_feats,min_feats,ptp_feats] kwargs=[] factor=concat sink=feats>
(100, 4)
finding max
finding min
finding peak-to-peak
concatenating features
(100, 3)
```

A file called `feats.pkl` will be created in the current working directory when this script is executed. The file contains the contents of `feats` node. Because `feats` is attached to a `pickle`ing backend, when the script above is executed a second time the printed output changes to: 

```
<NodeWrapper sources=[max_feats,min_feats,ptp_feats] kwargs=[] factor=concat sink=feats>
(100, 4)
(100, 3)
```

which has not required the raw data loader or any of the intermediate functions to be called. If we imagine that some of these functions are expensive to execute, a significant saving has been produced with little overhead in our coding since the library has transparently directed the outputs to be stored in persistent memory. In principal all nodes can be serialised to file, but my preference is to serialise only those nodes that are computationally intensive to compute. 


