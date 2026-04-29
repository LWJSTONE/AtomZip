"""
AtomZip 压缩引擎 — DRAC (动态递归自适应压缩) v4

核心创新: BWT 上下文聚簇 + LZMA2 RAW 极限压缩

压缩流水线:
  策略0: LZMA2 RAW (基线，无预处理)
  策略1: Delta 差分编码 + LZMA2 RAW
  策略2: BWT (全文件单块) + LZMA2 RAW

策略2的关键发现:
  BWT (Burrows-Wheeler 变换) 将相似上下文的字符聚簇在一起，
  使 LZMA2 的 LZ77 匹配器能找到更长的匹配序列。
  不同于 bzip2 (BWT+MTF+Huffman)，我们不加 MTF 变换，
  因为 MTF 会打乱字节的空间局部性，反而损害 LZMA2 的
  LZ77 匹配能力。仅用 BWT 聚簇 + LZMA2 压缩的组合在
  多种数据类型上显著优于单纯的 LZMA2 压缩。

  此外，使用整个文件作为单个 BWT 块 (而非 64KB 分块)
  避免了块边界信息丢失，大幅提升压缩效果。

压缩级别 (1-9):
  1-3: 快速 (仅策略0)
  4-6: 均衡 (策略0-1)
  7-9: 极限 (策略0-2，多策略竞争选最优)

优势: 使用 LZMA2 RAW 格式节省约56字节的 XZ 容器开销。
"""

import struct
import time
import lzma
from typing import Tuple, List

from .transform import (
    bwt_encode, bwt_decode,
    delta_encode, delta_decode,
    serialize_block_info,
    BWT_MAX_DATA_SIZE
)

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 4


def _get_lzma_filters(preset: int = 9, dict_size: int = 0) -> list:
    """获取 LZMA2 滤镜参数。

    preset: LZMA 预设级别 (0-9)
    dict_size: 字典大小 (字节)，0 表示使用 preset 默认值。
               LZMA2 的最大字典大小为 768MB (0x30000000)。
    """
    if dict_size > 0:
        return [{'id': lzma.FILTER_LZMA2,
                 'preset': preset | lzma.PRESET_EXTREME,
                 'dict_size': dict_size}]
    return [{'id': lzma.FILTER_LZMA2, 'preset': preset | lzma.PRESET_EXTREME}]


class AtomZipCompressor:
    """DRAC v4: BWT 上下文聚簇 + LZMA2 RAW 极限压缩"""

    def __init__(self, level: int = 5, verbose: bool = False):
        self.level = max(1, min(9, level))
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        """压缩数据，返回压缩后的字节流。"""
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v4] 开始压缩 {original_size:,} 字节...")

        if original_size == 0:
            return self._build_empty_header()

        # === 策略0: LZMA2 RAW (基线) ===
        result_lzma = self._strategy_lzma_only(data)

        if self.level < 4:
            best = result_lzma
            strategy = 0
        elif self.level < 7:
            # 中级别: 策略0 + 1
            result_delta = self._strategy_delta(data)

            candidates = [
                (len(result_lzma), result_lzma, 0),
                (len(result_delta), result_delta, 1),
            ]
            candidates.sort(key=lambda x: x[0])
            best = candidates[0][1]
            strategy = candidates[0][2]
        else:
            # 高级别: 所有策略竞争
            candidates = [
                (len(result_lzma), result_lzma, 0),
            ]

            # 策略1: Delta + LZMA2
            result_delta = self._strategy_delta(data)
            candidates.append((len(result_delta), result_delta, 1))

            # 策略2: BWT + LZMA2 (全文件单块，仅对不太大的数据)
            if original_size <= BWT_MAX_DATA_SIZE:
                result_bwt = self._strategy_bwt(data)
                candidates.append((len(result_bwt), result_bwt, 2))

            candidates.sort(key=lambda x: x[0])
            best = candidates[0][1]
            strategy = candidates[0][2]

        elapsed = time.time() - start_time
        ratio = original_size / max(1, len(best))

        if self.verbose:
            print(f"[AtomZip v4] 压缩完成: {original_size:,} -> {len(best):,} 字节 "
                  f"(比率: {ratio:.2f}:1, 耗时: {elapsed:.2f}秒, 策略: {strategy})")

        return best

    # ─────────────────────────────────────────
    #  各压缩策略实现
    # ─────────────────────────────────────────

    def _strategy_lzma_only(self, data: bytes) -> bytes:
        """策略0: 仅 LZMA2 RAW 极限压缩。"""
        lzma_data = self._lzma_compress(data)
        return self._build_output(data, lzma_data, strategy=0,
                                  extra_header=b'')

    def _strategy_delta(self, data: bytes) -> bytes:
        """策略1: Delta 差分编码 + LZMA2 RAW。"""
        delta_data, first_byte = delta_encode(data)
        lzma_data = self._lzma_compress(delta_data)

        extra = bytearray()
        extra.append(first_byte)

        if self.verbose:
            print(f"  Delta: {len(data):,} -> {len(delta_data):,} 字节 "
                  f"(首字节: {first_byte:#x})")
            print(f"  LZMA2: {len(delta_data):,} -> {len(lzma_data):,} 字节")

        return self._build_output(data, lzma_data, strategy=1,
                                  extra_header=bytes(extra))

    def _strategy_bwt(self, data: bytes) -> bytes:
        """策略2: BWT (全文件单块) + LZMA2 RAW (不加 MTF)。

        BWT 聚簇相似上下文的字符，使 LZMA2 的 LZ77 匹配器
        能找到更长的匹配。不加 MTF 是因为 MTF 会打乱字节
        分布，反而损害 LZMA2 的上下文建模。
        使用全文件单块避免块边界信息丢失。
        """
        # 阶段1: BWT 全文件单块编码 (block_size=0 表示全文件)
        bwt_data, block_info = bwt_encode(data, block_size=0)

        # 阶段2: LZMA2 RAW 压缩
        # 对 BWT 后的数据，使用匹配数据大小的字典以获得最佳效果
        dict_size = min(max(1 << 16, len(data)), 1 << 28)  # 64KB ~ 256MB
        lzma_data = self._lzma_compress(bwt_data, dict_size=dict_size)

        # 额外头部: BWT 块信息
        extra = serialize_block_info(block_info)

        if self.verbose:
            print(f"  BWT: {len(data):,} -> {len(bwt_data):,} 字节 "
                  f"({len(block_info)} 个块, 全文件单块)")
            print(f"  LZMA2: {len(bwt_data):,} -> {len(lzma_data):,} 字节 "
                  f"(字典: {dict_size >> 20}MB)")

        return self._build_output(data, lzma_data, strategy=2,
                                  extra_header=extra)

    # ─────────────────────────────────────────
    #  内部工具方法
    # ─────────────────────────────────────────

    def _lzma_compress(self, data: bytes, dict_size: int = 0) -> bytes:
        """使用 LZMA2 RAW 格式极限压缩（无XZ容器开销）。"""
        preset = min(9, max(6, self.level))
        filters = _get_lzma_filters(preset, dict_size=dict_size)
        return lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)

    def _build_output(self, data: bytes, lzma_data: bytes,
                      strategy: int, extra_header: bytes) -> bytes:
        """构建最终输出字节流。"""
        result = bytearray()
        original_size = len(data)

        # 文件头 (12字节固定部分)
        result.extend(ATOMZIP_MAGIC)                      # 4B: 魔数
        result.append(FORMAT_VERSION)                     # 1B: 版本号
        result.extend(struct.pack('>I', original_size))   # 4B: 原始大小
        result.append(strategy)                           # 1B: 压缩策略
        result.extend(struct.pack('>H', len(extra_header)))  # 2B: 额外头部大小

        # 额外头部 (策略相关的元数据)
        if extra_header:
            result.extend(extra_header)

        # LZMA2 RAW 压缩数据
        result.extend(struct.pack('>I', len(lzma_data)))  # 4B: LZMA数据大小
        result.extend(lzma_data)                          # 变长: LZMA数据

        return bytes(result)

    def _build_empty_header(self) -> bytes:
        """构建空文件的头部。"""
        result = bytearray()
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>I', 0))       # 原始大小 = 0
        result.append(0)                           # 策略 = 0
        result.extend(struct.pack('>H', 0))       # 额外头部 = 0
        result.extend(struct.pack('>I', 0))       # LZMA大小 = 0
        return bytes(result)
