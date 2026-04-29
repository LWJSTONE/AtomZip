"""
AtomZip 编解码器 — 动态递归自适应压缩 (DRAC) v4

核心创新: 自适应数据感知预处理 + LZMA2 RAW 极限压缩
  - 策略0: LZMA2 RAW (基线)
  - 策略1: Delta 差分编码 + LZMA2 RAW
  - 策略2: BWT + MTF + LZMA2 RAW (按上下文聚簇)
  - 策略3: Delta + BWT + MTF + LZMA2 RAW (创新组合)

BWT (Burrows-Wheeler 变换) 将相似上下文的字符聚簇在一起，
MTF (Move-to-Front 变换) 将频繁字符转换为小整数值，
LZMA2 在此基础上实现远超传统方法的压缩效果。
"""

from .compress import AtomZipCompressor
from .decompress import AtomZipDecompressor
from .transform import (
    bwt_encode, bwt_decode,
    mtf_encode, mtf_decode,
    delta_encode, delta_decode,
)

__all__ = [
    'AtomZipCompressor',
    'AtomZipDecompressor',
    'bwt_encode', 'bwt_decode',
    'mtf_encode', 'mtf_decode',
    'delta_encode', 'delta_decode',
]

__version__ = '4.0.0'
