"""
AtomZip Codec — Recursive Entropic Pattern Collapse (REPC) Algorithm

Core Innovation: Unlike traditional BPE (Byte Pair Encoding) which selects patterns
based solely on frequency, REPC uses an "Information Entropy Gain" criterion that
considers both frequency AND context diversity. Patterns appearing in many different
contexts are preferred because replacing them reduces the global entropy of the data
stream more effectively.

Algorithm Pipeline:
  1. Recursive Pattern Collapse: Iteratively find and replace byte patterns using
     information-theoretic scoring (entropy gain = freq × len × context_diversity - overhead)
  2. Adaptive Context Modeling: Multi-order context (order-0 through order-N) with
     adaptive probability mixing using neural-inspired update rules
  3. Range Coding: Final entropy coding using range coder for near-optimal bit encoding
"""

from .compress import AtomZipCompressor
from .decompress import AtomZipDecompressor
from .pattern import PatternExtractor
from .context import ContextModel
from .entropy import RangeEncoder, RangeDecoder, SimpleEntropyCoder

__all__ = [
    'AtomZipCompressor',
    'AtomZipDecompressor',
    'PatternExtractor',
    'ContextModel',
    'RangeEncoder',
    'RangeDecoder',
    'SimpleEntropyCoder',
]

__version__ = '1.0.0'
