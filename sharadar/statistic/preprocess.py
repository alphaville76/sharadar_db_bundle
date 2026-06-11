import numpy as np
import pandas as pd


def normalize(data_nostd):
    '''
    Transform features by scaling each feature to a [0, 1] range.
    Handles NaN values by ignoring them during computation.
    '''
    # Preserve DataFrame index and columns if input is a DataFrame
    is_df = isinstance(data_nostd, pd.DataFrame)
    if is_df:
        index = data_nostd.index
        columns = data_nostd.columns
        data_nostd = data_nostd.values

    std = np.nanstd(data_nostd, axis=0)
    with np.errstate(divide='ignore', invalid='ignore'):
        data = data_nostd / std
    data = np.where(np.isfinite(data), data, 0)

    min_vals = np.nanmin(data, axis=0)
    max_vals = np.nanmax(data, axis=0)

    range_vals = max_vals - min_vals
    with np.errstate(divide='ignore', invalid='ignore'):
        result = (data - min_vals) / range_vals
    result = np.where(np.isfinite(result), result, 0)

    if is_df:
        result = pd.DataFrame(result, index=index, columns=columns)

    return result


def winsorize_iqr(df, mult=1.5):
    '''
    Cap the outliers to the IQR theoretical lower/upper bounds.
    Univariate winsorization that handles NaN values.
    '''
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Input must be a pandas DataFrame')
    
    if df.empty:
        return df.copy()
    
    # Use nanpercentile to handle NaN values
    q1 = np.nanpercentile(df.values, 25, axis=0)
    q3 = np.nanpercentile(df.values, 75, axis=0)
    iqr = q3 - q1
    lower_bound = pd.Series(q1 - (iqr * mult), index=df.columns)
    upper_bound = pd.Series(q3 + (iqr * mult), index=df.columns)
    return df.clip(lower=lower_bound, upper=upper_bound, axis=1)
