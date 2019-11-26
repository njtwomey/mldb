from mldb.backends.joblib_backend import JoblibBackend

__all__ = ['ScikitLearnBackend']

class ScikitLearnBackend(JoblibBackend):
    def __init__(self, path):
        super(ScikitLearnBackend, self).__init__(ext='sklearn', path=path)
