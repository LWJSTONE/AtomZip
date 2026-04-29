"""
AtomZip 编解码器 — 动态递归自适应压缩 (DRAC) v3

核心创新: REPC（递归熵模式坍缩）评分准则
  不同于传统BPE仅按频率选择模式，REPC基于信息熵增益
  (频率 × 上下文多样性)选择模式，使每次替换最大化降低
  全局数据熵。

压缩流水线:
  1. RLE 预处理: 对连续重复字节(≥4)进行游程编码
  2. 字节重映射: 转义稀有字节，创建空闲字节值
  3. REPC BPE: 基于熵增益评分的分层字节对编码
  4. LZMA 极限压缩: LZMA2大字典+范围编码
  5. 自适应策略: 高级别下多策略竞争，选取最优
"""

from .compress import AtomZipCompressor
from .decompress import AtomZipDecompressor
from .pattern import PatternExtractor

__all__ = [
    'AtomZipCompressor',
    'AtomZipDecompressor',
    'PatternExtractor',
]

__version__ = '3.0.0'
