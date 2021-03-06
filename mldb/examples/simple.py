import numpy as np

from mldb import ComputationGraph
from mldb import PickleBackend


def load_data(N, D, rng):
    print("load_data")
    return rng.normal(0, 1, (N, D))


def extract_features(data, features):
    print("extract_features")
    return np.concatenate([feat(data, axis=1, keepdims=True) for feat in features], axis=1)


def get_weights(D):
    print("get_weights")
    return np.random.normal(0, 1, (D, 1)) / 4, 0.5


def dot_add(feats, wb):
    print("dot_add")
    w, b = wb
    return feats.dot(w) + b


def sigmoid(z_score):
    print("sigmoid")
    return 1.0 / (1.0 + np.exp(-z_score))


feat_funcs = [np.min, np.max, np.std, np.mean, np.median, np.ptp]


graph = ComputationGraph()

# Define a random seed
rng = graph.node(func=lambda: np.random.RandomState(1234))

# Generate some random data
data = graph.node(func=load_data, kwargs=dict(rng=rng, N=10000, D=3))

# Extract features from the data
features = graph.node(func=extract_features, kwargs=dict(data=data, features=feat_funcs))


# Generate parameters for a linear model
params = graph.node(func=get_weights, kwargs=dict(D=len(feat_funcs)))

# Inner product between features and weights
z_score = graph.node(func=dot_add, kwargs=dict(feats=features, wb=params))

# Go from the score to the predictions
predictions = graph.node(func=sigmoid, kwargs=dict(z_score=z_score))


for key, value in graph.nodes.items():
    print(key)
    print(value)
    print(value.evaluate())
    print()

for key, value in graph.nodes.items():
    print(key)
    print(value)
    print(value.evaluate())
    print()
