import numpy as np
import pytest
from sharadar.statistic.ann import NeuralNetwork


class TestNeuralNetwork:
    def test_init(self):
        nn = NeuralNetwork(input_size=4, hidden_size=5, num_classes=3)
        assert nn is not None

    def test_forward_propagation_shape(self):
        nn = NeuralNetwork(input_size=4, hidden_size=5, num_classes=3)
        X = np.random.randn(10, 4)
        output = nn.forward_propagation(X)
        assert output.shape == (10, 3)

    def test_forward_propagation_probabilities(self):
        nn = NeuralNetwork(input_size=4, hidden_size=5, num_classes=3)
        X = np.random.randn(10, 4)
        output = nn.forward_propagation(X)
        row_sums = output.sum(axis=1)
        np.testing.assert_array_almost_equal(row_sums, np.ones(10), decimal=5)

    def test_predict(self):
        nn = NeuralNetwork(input_size=4, hidden_size=5, num_classes=3)
        X = np.random.randn(10, 4)
        predictions = nn.predict(X)
        assert predictions.shape == (10,)
        assert all(0 <= p < 3 for p in predictions)

    def test_compute_cost(self):
        nn = NeuralNetwork(input_size=2, hidden_size=3, num_classes=2, reg_lambda=0.01)
        X = np.random.randn(5, 2)
        y = np.array([0, 1, 0, 1, 0])
        cost = nn.compute_cost(X, y)
        assert np.isfinite(cost)
        assert cost > 0

    def test_fit_reduces_cost(self):
        np.random.seed(42)
        nn = NeuralNetwork(input_size=2, hidden_size=4, num_classes=2, reg_lambda=0.01)
        X = np.random.randn(50, 2)
        y = (X[:, 0] + X[:, 1] > 0).astype(int)
        
        cost_before = nn.compute_cost(X, y)
        nn.fit(X, y)
        cost_after = nn.compute_cost(X, y)
        
        assert cost_after < cost_before

    def test_fit_learns_simple_pattern(self):
        np.random.seed(42)
        nn = NeuralNetwork(input_size=2, hidden_size=10, num_classes=2, reg_lambda=0.001)
        X = np.vstack([np.random.randn(25, 2) + [2, 2],
                       np.random.randn(25, 2) + [-2, -2]])
        y = np.array([0]*25 + [1]*25)
        
        nn.fit(X, y)
        predictions = nn.predict(X)
        accuracy = np.mean(predictions == y)
        assert accuracy > 0.8
