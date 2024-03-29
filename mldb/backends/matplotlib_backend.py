from matplotlib.pyplot import Figure

from mldb.backends.filesystem_backend import FileSystemBase
from mldb.backends.filesystem_backend import FileSystemInterface
from mldb.backends.validation import validate_dtype

__all__ = ["MatPlotLibInterface", "MatPlotLibBackend", "PNGBackend", "PDFBackend"]


class MatPlotLibInterface(FileSystemInterface):
    def load(self):
        return True

    def save(self, data):
        fig = data
        validate_dtype(fig, Figure)
        fig.savefig(self.path)
        fig.clf()


class MatPlotLibBackend(FileSystemBase):
    def __init__(self, path, ext):
        super(MatPlotLibBackend, self).__init__(interface=MatPlotLibInterface, ext=ext, path=path)


class PNGBackend(MatPlotLibBackend):
    def __init__(self, path):
        super(PNGBackend, self).__init__(path=path, ext="png")


class PDFBackend(MatPlotLibBackend):
    def __init__(self, path):
        super(PDFBackend, self).__init__(path=path, ext="pdf")
