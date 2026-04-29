"""
Adaptive Context Modeling Module

Implements multi-order context modeling with adaptive probability mixing.
This is the second stage of the AtomZip pipeline — after pattern substitution
has reduced the data, context modeling captures remaining statistical
dependencies for efficient entropy coding.

Novel aspect: Uses a "confidence-weighted" mixing strategy where each context
model's prediction is weighted by its recent prediction accuracy (measured by
log-loss), inspired by online learning techniques. This differs from PAQ-style
neural mixing by using a simpler but faster update rule that still adapts
quickly to changing data statistics.
"""

from typing import List, Dict, Tuple
import math


class OrderModel:
    """Single fixed-order context model with adaptive probabilities."""

    def __init__(self, order: int, alphabet_size: int = 256):
        self.order = order
        self.alphabet_size = alphabet_size
        # Count tables: context -> [count for each symbol]
        self.counts: Dict[Tuple, List[int]] = {}
        # Total counts per context
        self.totals: Dict[Tuple, int] = {}
        # Recent prediction accuracy (exponential moving average of log-loss)
        self.confidence = 1.0 / (order + 1)
        # Learning rate for confidence updates
        self.lr = 0.01

    def _get_context(self, history: bytes) -> Tuple:
        """Extract context from recent history."""
        if len(history) < self.order:
            return ()
        return tuple(history[-self.order:]) if self.order > 0 else ()

    def predict(self, context: Tuple) -> List[float]:
        """Get probability distribution for next symbol given context."""
        if context not in self.counts:
            # Uniform distribution for unseen context
            return [1.0 / self.alphabet_size] * self.alphabet_size

        counts = self.counts[context]
        total = self.totals[context]

        # Smoothed probabilities (add-0.5 smoothing)
        probs = []
        for c in counts:
            probs.append((c + 0.5) / (total + self.alphabet_size * 0.5))
        return probs

    def update(self, context: Tuple, symbol: int, prob: float):
        """Update model with observed symbol and its predicted probability."""
        if context not in self.counts:
            self.counts[context] = [0] * self.alphabet_size
            self.totals[context] = 0

        self.counts[context][symbol] += 1
        self.totals[context] += 1

        # Update confidence based on prediction accuracy
        # Lower prob = worse prediction = lower confidence
        log_loss = -math.log(max(prob, 1e-10))
        # Normalize log loss (max is log(alphabet_size))
        normalized_loss = log_loss / math.log(self.alphabet_size)
        self.confidence = (1 - self.lr) * self.confidence + self.lr * (1 - normalized_loss)


class ContextModel:
    """
    Multi-order context model with confidence-weighted mixing.

    Combines predictions from multiple order-N models using adaptive weights
    based on each model's recent prediction accuracy.
    """

    def __init__(self, max_order: int = 4, alphabet_size: int = 256):
        self.max_order = max_order
        self.alphabet_size = alphabet_size
        self.models = [OrderModel(o, alphabet_size) for o in range(max_order + 1)]
        self.history = bytearray()

    def predict(self) -> List[float]:
        """
        Get mixed probability distribution for the next symbol.

        Each model's prediction is weighted by its confidence score.
        The weighted predictions are then normalized.
        """
        predictions = []
        weights = []

        for model in self.models:
            context = model._get_context(bytes(self.history))
            pred = model.predict(context)
            predictions.append(pred)
            weights.append(max(model.confidence, 0.001))

        # Normalize weights
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]

        # Mix predictions
        mixed = [0.0] * self.alphabet_size
        for pred, weight in zip(predictions, weights):
            for i in range(self.alphabet_size):
                mixed[i] += pred[i] * weight

        # Normalize
        total = sum(mixed)
        if total > 0:
            mixed = [p / total for p in mixed]
        else:
            mixed = [1.0 / self.alphabet_size] * self.alphabet_size

        return mixed

    def update(self, symbol: int):
        """Update all models with the observed symbol."""
        for model in self.models:
            context = model._get_context(bytes(self.history))
            pred = model.predict(context)
            prob = pred[symbol]
            model.update(context, symbol, prob)

        self.history.append(symbol)
        # Keep history bounded
        if len(self.history) > self.max_order + 1:
            self.history = self.history[-(self.max_order + 1):]

    def get_frequency_table(self) -> List[int]:
        """
        Get current frequency table for range coding.

        Converts probability distribution to integer frequencies suitable
        for range coding. Uses a precision of 16 bits (65536 total).
        """
        probs = self.predict()
        precision = 1 << 16  # 65536
        freqs = []

        for p in probs:
            f = max(1, int(p * precision))
            freqs.append(f)

        # Adjust to ensure total equals precision
        total = sum(freqs)
        diff = precision - total
        if diff != 0:
            # Distribute difference to the most probable symbols
            indices = sorted(range(self.alphabet_size),
                           key=lambda i: freqs[i], reverse=True)
            for i in range(abs(diff)):
                idx = indices[i % len(indices)]
                freqs[idx] += 1 if diff > 0 else -1
                freqs[idx] = max(1, freqs[idx])

        return freqs
