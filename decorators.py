from functools import wraps
from tqdm import tqdm

import pandas as pd


def concatenate_aggregates(items):
    assert isinstance(items, list)
    
    item = items[0]
    
    if isinstance(item, pd.DataFrame):
        return pd.concat(items)
    elif isinstance(item, list):
        if isinstance(item[0], dict):
            return [ii for item in items for ii in item]
    
    raise NotImplementedError('{} {}'.format(type(items), type(item)))


def one_by_one(keys):
    """
    
    :param keys:
    :return:
    """
    
    if isinstance(keys, str):
        keys = [keys]
        
    assert isinstance(keys, (list, tuple))
    
    def real_decorator(func):
        @wraps(func)
        def wrapper(**kwargs):
            values = []
            kwargs_copy = kwargs.copy()
            for vals in tqdm(zip(*[kwargs[key] for key in keys]), total=len(kwargs[keys[0]])):
                for key, val in zip(keys, vals):
                    kwargs_copy[key] = val
                val = func(**kwargs_copy)
                if val:
                    values.append(val)
            return values
        
        return wrapper
    
    return real_decorator


def groupby(dfarg, keys):
    """
    
    :param dfarg:
    :param keys:
    :return:
    """
    
    if not isinstance(keys, (list, tuple)):
        keys = [keys]
    
    def real_decorator(func):
        @wraps(func)
        def wrapper(**kwargs):
            assert dfarg in kwargs, '{} not in {}; func={}'.format(
                dfarg,
                kwargs.keys(),
                func.__name__
            )
            
            df = pd.DataFrame(kwargs[dfarg])
            
            results = []
            kwargs_copy = kwargs.copy()
            for key, group in tqdm(df.groupby(keys), 'Extracting {}'.format(func.__name__)):
                kwargs_copy[dfarg] = group
                gres = func(**kwargs_copy)
                results.append(gres)
            return results
        
        return wrapper
    
    return real_decorator


def kfold(k, predefined=None, stratified=False):
    """
    
    :param k:
    :param predefined:
    :param stratified:
    :return:
    """
    
    def real_decorator(func):
        @wraps(func)
        def wrapper(**kwargs):
            return results
        
        return wrapper
    
    return real_decorator
