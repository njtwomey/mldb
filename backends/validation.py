from mldb.logger import get_logger

logger = get_logger(__name__)

__all__ = ["validate_dtype"]


def validate_dtype(obj, dtypes):
    """

    Args:
        obj: input object
        dtypes: data type, or list of data types

    Returns: True if the type of obj is in the allowable set
    Raises: TypeError if obj is not
    """
    if isinstance(obj, dtypes):
        return
    logger.exception(f"The object {obj} is of the wrong type {type(object)} is not in {dtypes}")
    raise TypeError
