"""
AtomZip 编解码器 — 极限压缩引擎 v9

核心创新: 深度结构提取 + 全局去重 + 多轮压缩
  v9: 深度JSON/日志/CSV提取 + 全局去重 + 递归BPE + 文本段落去重
  v8: 超激进BPE + 词级字典 + 增强N-gram + 35+策略竞争
  v7: 迭代BPE编码 + N-gram字典压缩 + 增强CSV/JSON/Log列压缩
  v6: C语言BWT引擎 + 全文件BWT + 穷举LZMA2参数
  v5: RLE/文本字典/JSON键去重/日志模板/列转置
  v4: BWT+LZMA2核心, 发现不加MTF更优
"""

from .compress_v9 import AtomZipCompressor
from .decompress_v9 import AtomZipDecompressor
from .transform_v9 import (
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

__version__ = '9.0.0'
