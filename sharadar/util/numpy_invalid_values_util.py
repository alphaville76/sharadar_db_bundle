import numpy as np

def nandivide(a, b):
    return np.divide(a, b, out=np.full_like(a, fill_value=np.nan), where=b != 0)

def nanlog(a):
    return np.log(a, out=np.full_like(a, fill_value=np.nan), where=a > 0)

def nanlog1p(a):
    return np.log1p(a, out=np.full_like(a, fill_value=np.nan), where=a > 0)
