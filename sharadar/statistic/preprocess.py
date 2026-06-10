import numpy as np
import pandas as pd

def normalize(data_nostd):
    """
    Transform features by scaling each feature to a [0, 1] range
    """
    std = np.nanstd(data_nostd, axis=0)
    with np.errstate(divide="ignore", invalid="ignore"):
        data = data_nostd / std
    data = np.where(np.isfinite(data), data, 0)
    
    min = data.min(axis=0)
    max = data.max(axis=0)
    
    range_vals = max - min
    with np.errstate(divide="ignore", invalid="ignore"):
        result = (data - min) / range_vals
    result = np.where(np.isfinite(result), result, 0)
    
    return result


def winsorize_iqr(df, mult=1.5):
    """
    Cap the outliers the the IQR teoretical lower/upper bounds
    Univariate
    """
    q1, q3 = np.percentile(df, [25, 75], axis=0)
    iqr = q3 - q1
    lower_bound = pd.Series(q1 - (iqr * mult), index=df.columns)
    upper_bound = pd.Series(q3 + (iqr * mult), index=df.columns)
    return df.clip(lower=lower_bound, upper=upper_bound, axis=1)
