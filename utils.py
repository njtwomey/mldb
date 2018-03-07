from numpy.lib.stride_tricks import as_strided as ast
import numpy as np

import playhouse.postgres_ext as peewee
import dill


# def connect(db_name, db_credentials=None, drop_all=False, create_hstore=True):
#     """
#
#     :param db_name:
#     :param db_credentials:
#     :param drop_all:
#     :return:
#     """
#
#     db_defaults = dict(user='', password='', host='localhost', port=5432)
#     db_credentials = db_credentials or {}
#     for kk, vv in db_defaults.items():
#         if kk not in db_credentials:
#             db_credentials[kk] = vv
#
#     db.init(db_name, **db_credentials)
#
#     if drop_all:
#         db.drop_tables(all_tables, safe=True, cascade=True)
#
#     try:
#         db.create_tables(all_tables, safe=True)
#     except peewee.ProgrammingError as ex:
#         if create_hstore:
#             db.execute_sql('CREATE EXTENSION hstore;')
#             db.create_tables(all_tables, safe=True)
#         else:
#             raise ex
#
#     return db


def serialise_function(func):
    """
    
    :param func:
    :return:
    """
    
    return dill.dumps(func)


def deserialise_function(func_str):
    """
    
    :param func_str:
    :return:
    """
    
    return dill.loads(func_str)


def norm_shape(shape):
    try:
        i = int(shape)
        return (i,)
    except TypeError:
        # shape was not a number
        pass
    
    try:
        t = tuple(shape)
        return t
    except TypeError:
        # shape was not iterable
        pass
    
    raise TypeError('shape must be an int, or a tuple of ints')


def sliding_window(a, ws, ss=None, flatten=True):
    '''
    based on: https://stackoverflow.com/questions/22685274/divide-an-image-into-5x5-blocks-in-python-and-compute-histogram-for-each-block/22701004
    
    Return a sliding window over a in any number of dimensions

    Parameters:
        a  - an n-dimensional numpy array
        ws - an int (a is 1D) or tuple (a is 2D or greater) representing the size
             of each dimension of the window
        ss - an int (a is 1D) or tuple (a is 2D or greater) representing the
             amount to slide the window in each dimension. If not specified, it
             defaults to ws.
        flatten - if True, all slices are flattened, otherwise, there is an
                  extra dimension for each dimension of the input.

    Returns
        an array containing each n-dimensional window from a
    '''
    
    if None is ss:
        # ss was not provided. the windows will not overlap in any direction.
        ss = ws
    ws = norm_shape(ws)
    ss = norm_shape(ss)
    
    # convert ws, ss, and a.shape to numpy arrays so that we can do math in every
    # dimension at once.
    ws = np.array(ws)
    ss = np.array(ss)
    shape = np.array(a.shape)
    
    # ensure that ws, ss, and a.shape all have the same number of dimensions
    ls = [len(shape), len(ws), len(ss)]
    if 1 != len(set(ls)):
        raise ValueError(
            'a.shape, ws and ss must all have the same length. They were %s' % str(ls))
    
    # ensure that ws is smaller than a in every dimension
    if np.any(ws > shape):
        raise ValueError(
            'ws cannot be larger than a in any dimension. a.shape was %s and ws was %s' % (str(a.shape), str(ws)))
    
    # how many slices will there be in each dimension?
    newshape = norm_shape(((shape - ws) // ss) + 1)
    # the shape of the strided array will be the number of slices in each dimension
    # plus the shape of the window (tuple addition)
    newshape += norm_shape(ws)
    # the strides tuple will be the array's strides multiplied by step size, plus
    # the array's strides (tuple addition)
    newstrides = norm_shape(np.array(a.strides) * ss) + a.strides
    strided = ast(a, shape=newshape, strides=newstrides)
    if not flatten:
        return strided
    
    # Collapse strided so that it has one more dimension than the window.  I.e.,
    # the new array is a flat list of slices.
    meat = len(ws) if ws.shape else 0
    firstdim = (np.product(newshape[:-meat]),) if ws.shape else ()
    dim = firstdim + (newshape[-meat:])
    dim = list(filter(lambda i: i != 1, dim))
    
    return strided.reshape(dim)
