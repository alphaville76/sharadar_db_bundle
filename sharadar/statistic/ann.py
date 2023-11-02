import numpy as np
import time
import numpy as np
import pandas as pd
from scipy import optimize
from sharadar.util.logger import log


class NeuralNetwork(object):
    def __init__(self, hidden_units=None, alpha=0.0001, activation='tanh', initw='uniform', bias_k=1.0):
        self.n_h = hidden_units

        if activation not in ['tanh', 'sigmoid', 'relu']:
            raise ValueError("Activation function %s not allowed." % activation)

        if initw not in ['uniform', 'normal']:
            raise ValueError("Initial weight %s not allowed." % activation)

        self.activation = activation
        self.initw = initw

        # Regularization Parameter:
        self.alpha = alpha
        self.bias_k = bias_k

    #def __repr__(self):
    #    return str(self.parameters)

    def unravel_parameters(self, params_flat):
        W1_start = 0
        W1_end = self.n_h * self.n_x
        W1 = np.reshape(params_flat[W1_start:W1_end], (self.n_h, self.n_x), order='F')

        b1_end = W1_end + self.n_h
        b1 = np.reshape(params_flat[W1_end:b1_end], (self.n_h, 1), order='F')

        W2_end = b1_end + self.n_y * self.n_h
        W2 = np.reshape(params_flat[b1_end:W2_end], (self.n_y, self.n_h), order='F')

        b2_end = W2_end + self.n_y
        b2 = np.reshape(params_flat[W2_end:b2_end], (self.n_y, 1), order='F')

        return {"W1": W1, "b1": b1, "W2": W2, "b2": b2}

    def param_vars(self, parameters):
        return parameters["W1"], parameters["b1"], parameters["W2"], parameters["b2"]

    def ravel_parameters(self, parameters):
        W1, b1, W2, b2 = self.param_vars(parameters)
        return np.concatenate((W1.ravel(order='F'), b1.ravel(order='F'), W2.ravel(order='F'), b2.ravel(order='F')))

    def ravel_gradient(self, grads):
        dW1, db1, dW2, db2 = grads["dW1"], grads["db1"], grads["dW2"], grads["db2"]
        return np.concatenate((dW1.ravel(order='F'), db1.ravel(order='F'), dW2.ravel(order='F'), db2.ravel(order='F')))

    def cost_function_wrapper(self, params_flat, X, Y):
        parameters = self.unravel_parameters(params_flat)
        self.parameters = parameters

        A2, cache = self.forward_propagation(X, parameters)

        cost = self.compute_cost(A2, Y, parameters)
        self.J.append(cost)

        grads = self.backward_propagation(parameters, cache, X, Y)
        grads_flat = self.ravel_gradient(grads)

        return cost, grads_flat

    def train(self, X, Y):
        # Make empty list to store costs:
        self.J = []

        params_flat0 = self.ravel_parameters(self.parameters)

        options = {'maxiter': 200, 'disp': False}

        opt_res = optimize.minimize(self.cost_function_wrapper, params_flat0, \
                                    jac=True, method='L-BFGS-B', \
                                    args=(X, Y), options=options)
        self.parameters = self.unravel_parameters(opt_res.x)


    def _validate_size(self, X, Y):
        if X.shape[0] != Y.shape[0]:
            raise ValueError('X and Y must have the same lenght.')

    def fit(self, X, Y):
        if len(Y.shape) == 1:
            Y = Y.values.reshape((Y.shape[0], 1))

        self._validate_size(X, Y)

        self.n_x = X.shape[1]
        self.n_y = Y.shape[1]

        if self.n_h is None:
            self.n_h = 2 * self.n_x + 1

        # Initial Random Weights (parameters)
        self.parameters = self.initialize_parameters()

        self.train(X, Y)

        return self

    def rand(self, units_out, units_in):
        init_bound = np.sqrt(6) / np.sqrt(units_in + units_out)
        return np.random.rand(units_out, units_in) * 2 * init_bound - init_bound

    def initialize_parameters(self):
        """
        Random initialize weight matrixes and bias vectors.

        W1 and W2 represent the weights of the input layer and hidden layer,
        while b1 and b2 represent the bias added to the hidden layer and the output layer.
        """
        R1 = self.rand(self.n_h, self.n_x + 1)
        R2 = self.rand(self.n_y, self.n_h + 1)

        return {"W1": R1[:, 1:], "b1": R1[:, 0], "W2": R2[:, 1:], "b2": R2[:, 0]}

    def predict(self, X):
        Y_hat, cache = self.forward_propagation(X, self.parameters)
        return Y_hat

    def sigmoid(self, x):
        return 1 / (1 + np.exp(-x))

    def relu(self, x):
        """
        Rectified linear unit (ReLU) function.
        """
        return np.maximum(0., x)

    def forward_propagation(self, X, parameters):
        """
        Used also for predictions

        Argument:
        X -- input data of size (n_x, m)
        parameters -- python dictionary containing your parameters (output of initialization function)

        Returns:
        A2 -- The sigmoid output of the second activation
        cache -- a dictionary containing "Z1", "A1", "Z2" and "A2"
        """
        # Retrieve each parameter from the dictionary "parameters"
        W1, b1, W2, b2 = self.param_vars(parameters)

        # Implement Forward Propagation to calculate A2 (probabilities)
        Z1 = np.dot(X, W1.T) + self.bias_k*b1.T
        if self.activation == 'tanh':
            A1 = np.tanh(Z1)
        elif self.activation == 'sigmoid':
            A1 = self.sigmoid(Z1)
        elif self.activation == 'relu':
            A1 = self.relu(Z1)
        Z2 = np.dot(A1, W2.T) + self.bias_k*b2.T
        A2 = self.sigmoid(Z2)

        cache = {"Z1": Z1, "A1": A1, "Z2": Z2, "A2": A2}

        return A2, cache

    def compute_cost(self, A2, Y, parameters):
        """
        Computes the cross-entropy cost given in equation (13)

        Arguments:
        A2 -- The sigmoid output of the second activation, of shape (1, number of examples)
        Y -- "true" labels vector of shape (1, number of examples)

        Returns:
        cost -- cross-entropy cost given equation (13)
        """
        # number of example
        m = Y.shape[0]

        # cost = 0.5 * sum((Y - A2) ** 2) / m
        cost = np.sum(np.sum(-Y * np.log(A2) - (1.0 - Y) * np.log(1.0 - A2))) / m

        # Do not regularize the bias terms
        W1, W2 = parameters["W1"], parameters["W2"]
        cost = cost + (self.alpha / (2 * m)) * (np.sum(W1 ** 2) + np.sum(W2 ** 2))

        # E.g., turns [[17]] into 17
        cost = np.squeeze(cost)

        return cost

    def backward_propagation(self, parameters, cache, X, Y):
        """
        Implement the backward propagation using the instructions above.

        Arguments:
        parameters -- python dictionary containing our parameters
        cache -- a dictionary containing "Z1", "A1", "Z2" and "A2".
        X -- input data of shape (2, number of examples)
        Y -- "true" labels vector of shape (1, number of examples)

        Returns:
        grads -- python dictionary containing your gradients with respect to different parameters
        """
        m = X.shape[0]

        W1, W2 = parameters["W1"], parameters["W2"]
        A1, A2 = cache["A1"], cache["A2"]

        # Backward propagation: calculate dW1, db1, dW2, db2.
        dZ2 = A2 - Y
        dW2 = np.dot(dZ2.T, A1) / m
        # add regularization term for derivatives
        dW2 = dW2 + (self.alpha / m) * W2

        db2 = np.sum(dZ2.T, axis=1, keepdims=True) / m

        dZ1 = np.dot(dZ2, W2)
        if self.activation == 'tanh':
            # multipy for the tanh derivative
            dZ1 = dZ1 * (1. - np.power(A1, 2.))
        elif self.activation == 'sigmoid':
            # multipy for the sigmoid derivative
            dZ1 = dZ1 * (A1 * (1. - A1))
        elif self.activation == 'relu':
            # When the relu activation layer <= 0, you should set dZ1 to 0 as well.
            dZ1[A1 <= 0] = 0

        dW1 = np.dot(dZ1.T, X) / m
        # add regularization term for derivatives
        dW1 = dW1 + (self.alpha / m) * W1

        db1 = np.sum(dZ1.T, axis=1, keepdims=True) / m

        return {"dW1": dW1, "db1": db1, "dW2": dW2, "db2": db2}

