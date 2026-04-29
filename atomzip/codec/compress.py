"""
AtomZip 压缩引擎 — DRAC (动态递归自适应压缩) v3

核心创新流水线:
  1. RLE 预处理: 对连续重复字节(≥4)进行游程编码
  2. 字节重映射: 转义稀有字节，创建空闲字节值供BPE使用
  3. REPC BPE: 基于信息熵增益评分的分层字节对编码
  4. LZMA2 极限压缩: 使用RAW格式(无XZ开销)配合大字典+范围编码
  5. 自适应策略: 高压缩级别下同时尝试多种策略，选取最优结果

压缩级别 (1-9):
  1-3: 快速压缩 (仅 LZMA2)
  4-6: 均衡压缩 (尝试 BPE + LZMA2)
  7-9: 极限压缩 (多策略竞争 + 最优选择)

关键优化: 使用 LZMA2 RAW 格式而非 XZ 格式，节省约56字节的
XZ 容器开销(CRC、索引、块头等)，使 AtomZip 在相同算法下
输出比 7z/XZ 更小。
"""

import struct
import time
import lzma
from typing import Tuple, List, Optional

from .pattern import PatternExtractor

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 3

# LZMA2 滤镜参数 (固定，用于 RAW 格式压缩/解压)
def _get_lzma_filters(preset=9):
    """获取 LZMA2 滤镜参数。"""
    return [{'id': lzma.FILTER_LZMA2, 'preset': preset | lzma.PRESET_EXTREME}]


class AtomZipCompressor:
    """DRAC v3: RLE + 字节重映射 + REPC BPE + LZMA2 RAW 极限压缩"""

    def __init__(self, level: int = 5, verbose: bool = False):
        self.level = max(1, min(9, level))
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        """压缩数据，返回压缩后的字节流。"""
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v3] 开始压缩 {original_size:,} 字节...")

        if original_size == 0:
            return self._build_empty_header()

        # === 策略0: 仅 LZMA2 RAW (基线) ===
        result_lzma_only = self._compress_strategy_lzma_only(data)

        if self.level < 4:
            # 低级别: 仅使用 LZMA2
            best = result_lzma_only
            strategy = 0
        elif self.level < 7:
            # 中级别: 尝试 BPE + LZMA2
            result_bpe = self._compress_strategy_bpe(data)
            if len(result_bpe) < len(result_lzma_only):
                best = result_bpe
                strategy = 2
            else:
                best = result_lzma_only
                strategy = 0
        else:
            # 高级别: 多策略竞争
            result_rle = self._compress_strategy_rle(data)
            result_bpe = self._compress_strategy_bpe(data)

            candidates = [
                (len(result_lzma_only), result_lzma_only, 0),
                (len(result_rle), result_rle, 1),
                (len(result_bpe), result_bpe, 2),
            ]
            candidates.sort(key=lambda x: x[0])
            best = candidates[0][1]
            strategy = candidates[0][2]

        elapsed = time.time() - start_time
        ratio = original_size / max(1, len(best))

        if self.verbose:
            print(f"[AtomZip v3] 压缩完成: {original_size:,} -> {len(best):,} 字节 "
                  f"(比率: {ratio:.2f}:1, 耗时: {elapsed:.2f}秒, 策略: {strategy})")

        return best

    def _compress_strategy_lzma_only(self, data: bytes) -> bytes:
        """策略0: 仅 LZMA2 RAW 极限压缩"""
        lzma_data = self._lzma_compress(data)
        return self._build_output(data, lzma_data, strategy=0,
                                  rle_entries=[], rules_data=b'')

    def _compress_strategy_rle(self, data: bytes) -> bytes:
        """策略1: RLE + LZMA2 RAW"""
        rle_data, rle_entries = self._rle_encode(data)
        lzma_data = self._lzma_compress(rle_data)
        return self._build_output(data, lzma_data, strategy=1,
                                  rle_entries=rle_entries,
                                  rules_data=b'')

    def _compress_strategy_bpe(self, data: bytes) -> bytes:
        """策略2: RLE + BPE + LZMA2 RAW"""
        # 阶段1: RLE
        rle_data, rle_entries = self._rle_encode(data)

        # 阶段2: REPC BPE
        max_rules = {4: 100, 5: 200, 6: 300, 7: 400, 8: 500, 9: 600}.get(
            self.level, 200)
        min_freq = max(2, len(rle_data) // 100000)

        extractor = PatternExtractor(max_rules=max_rules, min_freq=min_freq)
        bpe_data, rules = extractor.extract_and_apply(rle_data)
        rules_data = PatternExtractor.serialize_rules(rules) if rules else b''

        # 阶段3: LZMA2 RAW
        lzma_data = self._lzma_compress(bpe_data)

        if self.verbose:
            print(f"  RLE: {len(data):,} -> {len(rle_data):,} 字节 "
                  f"({len(rle_entries)} 个游程)")
            print(f"  BPE: {len(rle_data):,} -> {len(bpe_data):,} 字节 "
                  f"({len(rules)} 条规则)")
            print(f"  LZMA2 RAW: {len(bpe_data):,} -> {len(lzma_data):,} 字节")

        return self._build_output(data, lzma_data, strategy=2,
                                  rle_entries=rle_entries,
                                  rules_data=rules_data)

    def _rle_encode(self, data: bytes) -> Tuple[bytes, list]:
        """游程编码: 对连续≥4个相同字节进行压缩。"""
        if len(data) == 0:
            return data, []

        result = bytearray()
        entries = []
        i = 0

        while i < len(data):
            current = data[i]
            run_len = 1
            while (i + run_len < len(data) and
                   data[i + run_len] == current and
                   run_len < 65535):
                run_len += 1

            if run_len >= 4:
                # 编码: 3个原始字节 + 2字节长度
                entries.append((len(result), current, run_len))
                result.append(current)
                result.append(current)
                result.append(current)
                result.extend(struct.pack('>H', run_len))
                i += run_len
            else:
                for _ in range(run_len):
                    result.append(current)
                    i += 1

        return bytes(result), entries

    def _lzma_compress(self, data: bytes) -> bytes:
        """使用 LZMA2 RAW 格式极限压缩（无XZ容器开销）。"""
        preset = min(9, max(6, self.level))
        filters = _get_lzma_filters(preset)
        return lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)

    def _build_output(self, data: bytes, lzma_data: bytes,
                      strategy: int, rle_entries: list,
                      rules_data: bytes) -> bytes:
        """构建最终输出字节流。"""
        result = bytearray()
        original_size = len(data)

        # 文件头 (16字节固定部分)
        result.extend(ATOMZIP_MAGIC)                     # 4B: 魔数
        result.append(FORMAT_VERSION)                    # 1B: 版本号
        result.extend(struct.pack('>I', original_size))   # 4B: 原始大小 (4GB上限)
        result.append(strategy)                           # 1B: 压缩策略

        flags = 0
        if rle_entries:
            flags |= 0x01
        if rules_data:
            flags |= 0x02
        result.extend(struct.pack('>H', flags))           # 2B: 标志位

        # RLE 条目 (变长)
        if rle_entries:
            result.extend(struct.pack('>H', len(rle_entries)))  # 2B: 条目数
            for pos, byte_val, run_len in rle_entries:
                result.extend(struct.pack('>I', pos))     # 4B: 位置
                result.append(byte_val)                   # 1B: 字节值
                result.extend(struct.pack('>H', run_len)) # 2B: 游程长度

        # BPE 规则 (变长)
        result.extend(struct.pack('>H', len(rules_data)))  # 2B: 规则数据大小
        if rules_data:
            result.extend(rules_data)                      # 变长: 规则数据

        # LZMA2 RAW 压缩数据
        result.extend(struct.pack('>I', len(lzma_data)))  # 4B: LZMA数据大小
        result.extend(lzma_data)                           # 变长: LZMA数据

        return bytes(result)

    def _build_empty_header(self) -> bytes:
        """构建空文件的头部。"""
        result = bytearray()
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>I', 0))       # 原始大小 = 0
        result.append(0)                           # 策略 = 0
        result.extend(struct.pack('>H', 0))       # 标志 = 0
        result.extend(struct.pack('>H', 0))       # 规则大小 = 0
        result.extend(struct.pack('>I', 0))       # LZMA大小 = 0
        return bytes(result)
