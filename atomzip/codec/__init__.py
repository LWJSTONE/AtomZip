"""
AtomZip 编解码器 — 极限压缩引擎 v7

核心创新: 迭代BPE + N-gram字典 + 增强预处理 + 20+策略竞争
  - 策略0:  LZMA2 RAW (基线)
  - 策略1:  LZMA2 Delta滤镜
  - 策略2:  BWT + LZMA2 (C引擎, 全文件)
  - 策略3:  BWT + RLE + LZMA2
  - 策略4:  BWT + Delta + LZMA2
  - 策略5:  文本字典 (+BWT) + LZMA2
  - 策略6:  JSON键去重 (+BWT) + LZMA2
  - 策略7:  日志模板 (+BWT) + LZMA2
  - 策略8:  列转置 + BWT + LZMA2
  - 策略9:  BWT + RLE + Delta + LZMA2
  - 策略10: BCJ + LZMA2
  - 策略11: BCJ + Delta + LZMA2
  - 策略12: 递归BWT (双层BWT)
  ★ v7新策略:
  - 策略14: BPE + BWT + LZMA2
  - 策略15: BPE + LZMA2
  - 策略16: N-gram字典 + BWT + LZMA2
  - 策略17: N-gram字典 + LZMA2
  - 策略18: BPE + N-gram + BWT + LZMA2
  - 策略19: CSV列压缩 + BWT + LZMA2
  - 策略20: JSON扁平化 + BWT + LZMA2
  - 策略21: 日志字段压缩 + BWT + LZMA2
  - 策略22: BPE + Delta + BWT + LZMA2
  - 策略23: 文本字典 + BPE + BWT + LZMA2
  - 策略24: CSV列压缩 + BPE + BWT + LZMA2
  - 策略25: JSON键去重 + BPE + BWT + LZMA2
  - 策略26: 日志模板 + BPE + BWT + LZMA2
  - 策略27: 列转置 + BPE + BWT + LZMA2
  - 策略28: 列转置 + BPE + Delta + BWT + LZMA2

关键改进:
  v7: 迭代BPE编码 + N-gram字典压缩 + 增强CSV/JSON/Log列压缩
  v6: C语言BWT引擎 + 全文件BWT + 穷举LZMA2参数
  v5: RLE/文本字典/JSON键去重/日志模板/列转置
  v4: BWT+LZMA2核心, 发现不加MTF更优
"""

from .compress_v7 import AtomZipCompressor
from .decompress_v7 import AtomZipDecompressor
from .transform_v7 import (
    bwt_encode, bwt_decode,
    rle_encode, rle_decode,
    bpe_encode, bpe_decode,
)

__all__ = [
    'AtomZipCompressor',
    'AtomZipDecompressor',
    'bwt_encode', 'bwt_decode',
    'rle_encode', 'rle_decode',
    'bpe_encode', 'bpe_decode',
]

__version__ = '7.0.0'
