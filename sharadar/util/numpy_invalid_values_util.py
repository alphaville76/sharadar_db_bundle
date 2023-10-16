import warnings
import numpy as np

def nansubtract(a, b):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        if np.shape(a) == np.shape(b):
            return np.subtract(a, b, out=np.full_like(b, fill_value=np.nan), where=np.isfinite(b))
        return np.subtract(a, b)

def nandivide(a, b):
    return np.divide(a, b, out=np.full_like(a, fill_value=np.nan), where=b != 0)

def nanlog(a):
    return np.log(a, out=np.full_like(a, fill_value=np.nan), where=a > 0)

def nanlog1p(a):
    return np.log1p(a, out=np.full_like(a, fill_value=np.nan), where=a > 0)

def nanmean(a, axis=0):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanmean(a, axis)

def nanvar(a, axis=0):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanvar(a, axis)

def nanstd(a, axis=0):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return np.nanstd(a, axis)
