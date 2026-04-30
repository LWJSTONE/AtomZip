"""
AtomZip 编解码器 — 动态递归自适应压缩 (DRAC) v5

核心创新: 多策略竞争 + 智能预处理 + 穷举参数搜索
  - 策略0:  LZMA2 RAW (基线，含穷举参数)
  - 策略1:  Delta(stride=1) + LZMA2 RAW
  - 策略2:  BWT (全文件单块) + LZMA2 RAW
  - 策略3:  RLE + BWT + LZMA2 RAW
  - 策略4:  BWT + RLE + LZMA2 RAW
  - 策略5:  Delta(stride=N) + BWT + LZMA2 RAW
  - 策略6:  文本字典 + LZMA2 RAW
  - 策略7:  JSON键去重 + BWT + LZMA2 RAW
  - 策略8:  日志模板 + LZMA2 RAW
  - 策略9:  列转置 + BWT + LZMA2 RAW
  - 策略10: 列转置 + Delta + BWT + LZMA2 RAW
  - 策略11: 文本字典 + BWT + LZMA2 RAW
  - 策略12: JSON键去重 + Delta + BWT + LZMA2 RAW
  - 策略13: 日志模板 + BWT + LZMA2 RAW

关键发现: BWT + LZMA2 (不加 MTF) 比传统 BWT+MTF+Huffman 效果更好。
v5 新增: RLE/文本字典/JSON键去重/日志模板/列转置/穷举LZMA2参数。
"""

from .compress_v5 import AtomZipCompressor
from .decompress_v5 import AtomZipDecompressor
from .transform_v5 import (
    bwt_encode, bwt_decode,
    delta_encode, delta_decode,
    rle_encode, rle_decode,
)

__all__ = [
    'AtomZipCompressor',
    'AtomZipDecompressor',
    'bwt_encode', 'bwt_decode',
    'delta_encode', 'delta_decode',
    'rle_encode', 'rle_decode',
]

__version__ = '5.0.0'
