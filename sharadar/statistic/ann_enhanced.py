"""
Enhanced Neural Network implementations for time-series financial prediction.

Improvements over base ann.py:
1. WarmStartNN: Initializes from previous month's weights (transfer learning)
   - Economic principle: market regimes change gradually, not abruptly
   - Previous weights encode learned relationships that persist month-to-month
   
2. EMA-weighted ensemble scoring: Recent models weighted more than older ones
   - Economic principle: recent market structure more predictive than distant past
   
3. DeepNN: Two hidden layers for capturing non-linear factor interactions
   - Financial basis: factor interactions (e.g., value + quality) are non-linear
"""
import numpy as np
from sharadar.statistic.ann import NeuralNetwork


class WarmStartNN(NeuralNetwork):
    """
    Neural Network that can be initialized from a previous model's weights.
    This implements transfer learning: each month's model starts from where
    the previous month ended, enabling faster convergence and more stable
    weight evolution (similar to online learning / incremental learning).
    
    Economic rationale: Factor loadings and market structure change gradually.
    A model trained on month T-1 data is a better starting point for month T
    than random initialization.
    """
    
    def __init__(self, previous_parameters=None, warmstart_blend=0.7, **kwargs):
        """
        Args:
            previous_parameters: dict with W1, b1, W2, b2 from previous model
            warmstart_blend: how much to blend previous weights vs random init
                            0.0 = fully random, 1.0 = fully from previous
        """
        super().__init__(**kwargs)
        self.previous_parameters = previous_parameters
        self.warmstart_blend = warmstart_blend
    
    def initialize_parameters(self):
        """Blend previous weights with random initialization."""
        random_params = super().initialize_parameters()
        
        if self.previous_parameters is None:
            return random_params
        
        # Check dimension compatibility
        prev = self.previous_parameters
        if prev['W1'].shape != random_params['W1'].shape:
            # Dimensions changed (different feature count), fall back to random
            return random_params
        
        # Blend: warmstart_blend * previous + (1-warmstart_blend) * random
        blend = self.warmstart_blend
        blended = {}
        for key in ['W1', 'b1', 'W2', 'b2']:
            prev_val = prev[key]
            rand_val = random_params[key]
            if prev_val.shape == rand_val.shape:
                blended[key] = blend * prev_val + (1 - blend) * rand_val
            else:
                blended[key] = rand_val
        
        return blended


class DeepNN(NeuralNetwork):
    """
    Two hidden layer neural network.
    Architecture: Input -> H1 -> H2 -> Output (sigmoid)
    
    The second layer allows the network to capture non-linear interactions
    between factors (e.g., value works differently in high vs low quality stocks).
    
    Uses smaller hidden layers to avoid overfitting with limited monthly data.
    """
    
    def __init__(self, hidden_units=None, hidden_units_2=None, previous_parameters=None, 
                 warmstart_blend=0.7, **kwargs):
        super().__init__(hidden_units=hidden_units, **kwargs)
        self.n_h2 = hidden_units_2  # second hidden layer size
        self.previous_parameters = previous_parameters
        self.warmstart_blend = warmstart_blend
    
    def fit(self, X, Y):
        if len(Y.shape) == 1:
            Y = Y.values.reshape((Y.shape[0], 1))
        self._validate_size(X, Y)
        
        self.n_x = X.shape[1]
        self.n_y = Y.shape[1]
        
        if self.n_h is None:
            self.n_h = self.n_x + 1  # smaller first layer for deep net
        if self.n_h2 is None:
            self.n_h2 = max(4, self.n_h // 2)  # second layer smaller
        
        self.parameters = self.initialize_parameters()
        self.train(X, Y)
        return self
    
    def initialize_parameters(self):
        R1 = self.rand(self.n_h, self.n_x + 1)
        R2 = self.rand(self.n_h2, self.n_h + 1)
        R3 = self.rand(self.n_y, self.n_h2 + 1)
        
        params = {
            "W1": R1[:, 1:], "b1": R1[:, 0:1],
            "W2": R2[:, 1:], "b2": R2[:, 0:1],
            "W3": R3[:, 1:], "b3": R3[:, 0:1]
        }
        
        if self.previous_parameters is not None:
            prev = self.previous_parameters
            blend = self.warmstart_blend
            for key in params:
                if key in prev and prev[key].shape == params[key].shape:
                    params[key] = blend * prev[key] + (1 - blend) * params[key]
        
        return params
    
    def unravel_parameters(self, params_flat):
        idx = 0
        W1 = params_flat[idx:idx + self.n_h * self.n_x].reshape(self.n_h, self.n_x, order='F')
        idx += self.n_h * self.n_x
        b1 = params_flat[idx:idx + self.n_h].reshape(self.n_h, 1, order='F')
        idx += self.n_h
        W2 = params_flat[idx:idx + self.n_h2 * self.n_h].reshape(self.n_h2, self.n_h, order='F')
        idx += self.n_h2 * self.n_h
        b2 = params_flat[idx:idx + self.n_h2].reshape(self.n_h2, 1, order='F')
        idx += self.n_h2
        W3 = params_flat[idx:idx + self.n_y * self.n_h2].reshape(self.n_y, self.n_h2, order='F')
        idx += self.n_y * self.n_h2
        b3 = params_flat[idx:idx + self.n_y].reshape(self.n_y, 1, order='F')
        return {"W1": W1, "b1": b1, "W2": W2, "b2": b2, "W3": W3, "b3": b3}
    
    def ravel_parameters(self, parameters):
        return np.concatenate([
            parameters["W1"].ravel(order='F'), parameters["b1"].ravel(order='F'),
            parameters["W2"].ravel(order='F'), parameters["b2"].ravel(order='F'),
            parameters["W3"].ravel(order='F'), parameters["b3"].ravel(order='F')
        ])
    
    def ravel_gradient(self, grads):
        return np.concatenate([
            grads["dW1"].ravel(order='F'), grads["db1"].ravel(order='F'),
            grads["dW2"].ravel(order='F'), grads["db2"].ravel(order='F'),
            grads["dW3"].ravel(order='F'), grads["db3"].ravel(order='F')
        ])
    
    def forward_propagation(self, X, parameters):
        W1, b1 = parameters["W1"], parameters["b1"]
        W2, b2 = parameters["W2"], parameters["b2"]
        W3, b3 = parameters["W3"], parameters["b3"]
        
        Z1 = np.dot(X, W1.T) + self.bias_k * b1.T
        if self.activation == 'tanh':
            A1 = np.tanh(Z1)
        elif self.activation == 'sigmoid':
            A1 = self.sigmoid(Z1)
        elif self.activation == 'relu':
            A1 = self.relu(Z1)
        
        Z2 = np.dot(A1, W2.T) + self.bias_k * b2.T
        if self.activation == 'tanh':
            A2 = np.tanh(Z2)
        elif self.activation == 'sigmoid':
            A2 = self.sigmoid(Z2)
        elif self.activation == 'relu':
            A2 = self.relu(Z2)
        
        Z3 = np.dot(A2, W3.T) + self.bias_k * b3.T
        A3 = self.sigmoid(Z3)  # output always sigmoid for [0,1]
        
        cache = {"Z1": Z1, "A1": A1, "Z2": Z2, "A2": A2, "Z3": Z3, "A3": A3}
        return A3, cache
    
    def compute_cost(self, A3, Y, parameters):
        m = Y.shape[0]
        cost = np.sum(-Y * np.log(A3 + 1e-8) - (1.0 - Y) * np.log(1.0 - A3 + 1e-8)) / m
        W1, W2, W3 = parameters["W1"], parameters["W2"], parameters["W3"]
        cost += (self.alpha / (2 * m)) * (np.sum(W1**2) + np.sum(W2**2) + np.sum(W3**2))
        return np.squeeze(cost)
    
    def backward_propagation(self, parameters, cache, X, Y):
        m = X.shape[0]
        W1, W2, W3 = parameters["W1"], parameters["W2"], parameters["W3"]
        A1, A2, A3 = cache["A1"], cache["A2"], cache["A3"]
        
        # Output layer
        dZ3 = A3 - Y
        dW3 = np.dot(dZ3.T, A2) / m + (self.alpha / m) * W3
        db3 = np.sum(dZ3.T, axis=1, keepdims=True) / m
        
        # Second hidden layer
        dZ2 = np.dot(dZ3, W3)
        if self.activation == 'tanh':
            dZ2 = dZ2 * (1. - np.power(A2, 2.))
        elif self.activation == 'sigmoid':
            dZ2 = dZ2 * (A2 * (1. - A2))
        elif self.activation == 'relu':
            dZ2[A2 <= 0] = 0
        dW2 = np.dot(dZ2.T, A1) / m + (self.alpha / m) * W2
        db2 = np.sum(dZ2.T, axis=1, keepdims=True) / m
        
        # First hidden layer
        dZ1 = np.dot(dZ2, W2)
        if self.activation == 'tanh':
            dZ1 = dZ1 * (1. - np.power(A1, 2.))
        elif self.activation == 'sigmoid':
            dZ1 = dZ1 * (A1 * (1. - A1))
        elif self.activation == 'relu':
            dZ1[A1 <= 0] = 0
        dW1 = np.dot(dZ1.T, X) / m + (self.alpha / m) * W1
        db1 = np.sum(dZ1.T, axis=1, keepdims=True) / m
        
        return {"dW1": dW1, "db1": db1, "dW2": dW2, "db2": db2, "dW3": dW3, "db3": db3}


def compute_score_ema(X, models, decay=0.85):
    """
    EMA-weighted ensemble scoring. Recent models get exponentially more weight.
    
    Economic rationale: Market microstructure and factor premia evolve over time.
    A model trained 1 month ago is more relevant than one trained 12 months ago.
    Decay=0.85 means model from 6 months ago has ~37% weight of most recent.
    
    Args:
        X: current features
        models: list of trained models (oldest first)
        decay: weight decay factor per month (0.8-0.95 reasonable)
    """
    scores = []
    weights = []
    n = len(models)
    
    for i, model in enumerate(models):
        y_hat = model.predict(X)
        y_hat = y_hat.reshape((y_hat.shape[0]))
        scores.append(y_hat)
        # Exponential weight: most recent model (last) gets weight 1.0
        weights.append(decay ** (n - 1 - i))
    
    import pandas as pd
    scores_df = pd.DataFrame(np.transpose(scores), index=X.index)
    weights = np.array(weights) / sum(weights)  # normalize
    score = scores_df.mul(weights, axis=1).sum(axis=1)
    score = score - score.mean()
    return score