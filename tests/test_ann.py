import numpy as np
import pandas as pd
from sharadar.statistic.ann import NeuralNetwork


class TestNeuralNetwork:
    def test_init(self):
        nn = NeuralNetwork(hidden_units=5, alpha=0.0001, activation='tanh')
        assert nn is not None
        assert nn.n_h == 5

    def test_init_default_hidden_units(self):
        nn = NeuralNetwork()
        assert nn.n_h is None

    def test_init_invalid_activation(self):
        try:
            NeuralNetwork(activation='invalid')
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_forward_propagation_shape(self):
        nn = NeuralNetwork(hidden_units=5)
        nn.n_x = 4
        nn.n_y = 2
        nn.parameters = nn.initialize_parameters()
        X = np.random.randn(10, 4)
        output, cache = nn.forward_propagation(X, nn.parameters)
        assert output.shape == (10, 2)

    def test_predict_shape(self):
        nn = NeuralNetwork(hidden_units=5)
        nn.n_x = 4
        nn.n_y = 2
        nn.parameters = nn.initialize_parameters()
        X = np.random.randn(10, 4)
        predictions = nn.predict(X)
        assert predictions.shape == (10, 2)

    def test_fit_runs(self):
        np.random.seed(42)
        nn = NeuralNetwork(hidden_units=4, alpha=0.01)
        X = np.random.randn(50, 2)
        Y = pd.Series((X[:, 0] + X[:, 1] > 0).astype(float))
        nn.fit(X, Y)
        assert nn.parameters is not None

    def test_compute_cost_positive(self):
        np.random.seed(42)
        nn = NeuralNetwork(hidden_units=4, alpha=0.01)
        nn.n_x = 2
        nn.n_y = 1
        nn.parameters = nn.initialize_parameters()
        X = np.random.randn(10, 2)
        A2, cache = nn.forward_propagation(X, nn.parameters)
        Y = np.random.randint(0, 2, (10, 1)).astype(float)
        cost = nn.compute_cost(A2, Y, nn.parameters)
        assert np.isfinite(cost)
        assert cost > 0