from sharadar.pipeline.factors import logscale
import numpy as np

def test_logscale():
    assert (logscale(np.zeros(5) + np.exp(1) - 1.0) == np.ones(5)).all()
    assert logscale(-(np.exp(1) - 1)) == -1.0
    assert (logscale(np.zeros(5)) == np.zeros(5)).all()

if __name__ == '__main__':
    test_logscale()

