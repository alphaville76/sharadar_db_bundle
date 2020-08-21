from interface import implements
import numpy as np
from zipline.data.fx.base import FXRateReader


class SimpleFXRateReader(implements(FXRateReader)):
    """An FXRateReader that doesn't any currency conversion
    """

    def get_rates(self, rate, quote, bases, dts):
        return np.ones((len(dts), len(bases)))