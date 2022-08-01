import numpy as np
from sharadar.util.numpy_invalid_values_util import *

a = np.array([-1, 0, 1, 2, 3], dtype=float)
b = np.array([0, 0, 0, 2, 2], dtype=float)
c = np.array([np.nan, 0, np.inf, 2, 2], dtype=float)

np.testing.assert_array_equal(np.array([np.nan, np.nan, np.nan, 1.,  1.5]), nandivide(a, b))

np.testing.assert_array_almost_equal(np.array([np.nan, np.nan, 0., 0.69314718, 1.09861229]), nanlog(a))

np.testing.assert_array_almost_equal(np.array([np.nan, np.nan, 0.69314718, 1.09861229, 1.38629436]), nanlog1p(a))

np.testing.assert_array_almost_equal(np.array([np.nan,  0., np.nan,  0.,  0.]), nansubtract(c, c))