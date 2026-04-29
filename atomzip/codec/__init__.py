"""
AtomZip 编解码器 — 动态递归自适应压缩 (DRAC) v4

核心创新: BWT 上下文聚簇 + LZMA2 RAW 极限压缩
  - 策略0: LZMA2 RAW (基线)
  - 策略1: Delta 差分编码 + LZMA2 RAW
  - 策略2: BWT (全文件单块) + LZMA2 RAW

关键发现: BWT + LZMA2 (不加 MTF) 的组合比传统 BWT+MTF+Huffman
效果更好。BWT 将相似上下文字符聚簇后，LZMA2 的 LZ77 匹配器能
找到更长的匹配序列，而 MTF 反而会打乱这种空间局部性。
"""

from .compress import AtomZipCompressor
from .decompress import AtomZipDecompressor
from .transform import (
    bwt_encode, bwt_decode,
    delta_encode, delta_decode,
)

__all__ = [
    'AtomZipCompressor',
    'AtomZipDecompressor',
    'bwt_encode', 'bwt_decode',
    'delta_encode', 'delta_decode',
]

__version__ = '4.0.0'
