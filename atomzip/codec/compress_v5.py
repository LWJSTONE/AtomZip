"""
AtomZip 压缩引擎 v5.1 — 极限压缩 (实用优化版)

核心改进:
  1. 使用 LZMA2 内置 FILTER_DELTA (C级速度) 替代 Python Delta
  2. 使用 LZMA2 内置 FILTER_X86 (BCJ) 处理二进制数据
  3. 智能预处理: 仅对 ≤2MB 数据使用 Python BWT
  4. 大文件 (>2MB): 使用 C 级滤镜组合 (Delta+LZMA2, 多种 delta 距离)
  5. 穷举多种 LZMA2 参数组合

策略:
  0: LZMA2 RAW (基线)
  1: LZMA2 FILTER_DELTA(dist=1..32) + LZMA2
  2: BWT + LZMA2 (≤2MB)
  3: BWT + RLE + LZMA2 (≤2MB)
  4: FILTER_X86 + LZMA2 (二进制)
  5: 文本字典 + LZMA2 / BWT (≤2MB)
  6: JSON键去重 + LZMA2 / BWT (≤2MB)
  7: 日志模板 + LZMA2 / BWT (≤2MB)
  8: 列转置 + BWT + LZMA2 (≤2MB)
"""

import struct
import time
import lzma
import re
from typing import Tuple, List
from collections import Counter

from .transform_v5 import (
    bwt_encode, bwt_decode,
    delta_encode, delta_decode,
    rle_encode, rle_decode,
    text_dict_encode, text_dict_decode,
    json_key_dedup_encode, json_key_dedup_decode,
    log_template_encode, log_template_decode,
    column_transpose_encode, column_transpose_decode,
    serialize_block_info, deserialize_block_info,
    BWT_MAX_DATA_SIZE
)

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 5

# BWT 实际使用限制 (Python太慢)
BWT_PRACTICAL_LIMIT = 2 << 20


def _get_lzma_filters(preset: int = 9, dict_size: int = 0,
                      lc: int = 3, lp: int = 0, pb: int = 2,
                      delta_dist: int = 0, bcj: bool = False) -> list:
    """获取 LZMA2 滤镜参数 (支持 Delta/BCJ 前置滤镜)。"""
    filters = []
    
    # BCJ 滤镜 (x86 可执行文件)
    if bcj:
        filters.append({'id': lzma.FILTER_X86})
    
    # Delta 滤镜 (C级速度)
    if delta_dist > 0:
        filters.append({'id': lzma.FILTER_DELTA, 'dist': delta_dist})
    
    # LZMA2 主滤镜
    lzma2_filter = {
        'id': lzma.FILTER_LZMA2,
        'preset': preset | lzma.PRESET_EXTREME,
        'lc': lc, 'lp': lp, 'pb': pb,
    }
    if dict_size > 0:
        lzma2_filter['dict_size'] = dict_size
    filters.append(lzma2_filter)
    
    return filters


def _smart_dict_size(data_len: int) -> int:
    """计算合理的字典大小。"""
    return max(1 << 16, min(data_len, 1 << 28))


def _detect_data_type(data: bytes) -> str:
    """快速检测数据类型。"""
    if not data:
        return 'empty'

    sample = data[:8192]

    # 检测 JSON
    stripped = sample.lstrip()
    if stripped.startswith(b'{') or stripped.startswith(b'['):
        json_key_pattern = re.compile(rb'"[\w_]+"\s*:')
        key_matches = json_key_pattern.findall(sample)
        if len(key_matches) >= 3:
            return 'json'
        try:
            import json
            json.loads(sample.decode('utf-8', errors='replace'))
            return 'json'
        except Exception:
            pass

    # 检测日志
    log_pattern = re.compile(rb'\d{4}-\d{2}-\d{2}.*\[(INFO|WARN|ERROR|DEBUG)\]')
    if len(log_pattern.findall(sample)) >= 3:
        return 'log'

    # 检测 Apache 访问日志
    apache_pattern = re.compile(rb'\d+\.\d+\.\d+\.\d+.*HTTP/\d\.\d')
    if len(apache_pattern.findall(sample)) >= 3:
        return 'log'

    # 检测 CSV
    lines = sample.split(b'\n', 20)
    if len(lines) >= 3:
        comma_counts = [line.count(b',') for line in lines[:10] if line.strip()]
        if comma_counts and len(set(comma_counts)) == 1 and comma_counts[0] >= 3:
            return 'csv'

    # 检测源代码
    code_indicators = [b'def ', b'class ', b'import ', b'function ', b'var ', b'const ']
    code_score = sum(1 for ind in code_indicators if ind in sample)
    if code_score >= 2:
        return 'code'

    # 检测文本
    printable_count = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
    if printable_count / max(1, len(sample)) > 0.85:
        return 'text'

    return 'binary'


class AtomZipCompressor:
    """AtomZip v5.1: C级滤镜 + Python预处理 + 穷举参数"""

    def __init__(self, level: int = 5, verbose: bool = False):
        self.level = max(1, min(9, level))
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        """压缩数据。"""
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v5.1] 开始压缩 {original_size:,} 字节...")

        if original_size == 0:
            return self._build_empty_header()

        data_type = _detect_data_type(data)
        if self.verbose:
            print(f"  数据类型: {data_type}")

        if self.level < 4:
            result = self._strategy_lzma_only(data)
        elif self.level < 7:
            result = self._compress_medium(data, data_type)
        else:
            result = self._compress_extreme(data, data_type)

        elapsed = time.time() - start_time
        ratio = original_size / max(1, len(result))

        if self.verbose:
            print(f"[AtomZip v5.1] 压缩完成: {original_size:,} -> {len(result):,} 字节 "
                  f"(比率: {ratio:.2f}:1, 耗时: {elapsed:.2f}秒)")

        return result

    def _compress_medium(self, data: bytes, data_type: str) -> bytes:
        """中等压缩: 基线 + Delta滤镜。"""
        candidates = []

        # 策略0: LZMA2
        r = self._strategy_lzma_only(data)
        candidates.append((len(r), r))

        # 策略1: Delta滤镜 (C级速度)
        for dist in [1, 2, 4]:
            r = self._strategy_delta_filter(data, dist)
            candidates.append((len(r), r))

        # 策略2: BWT + LZMA2 (仅小文件)
        if len(data) <= BWT_PRACTICAL_LIMIT:
            r = self._strategy_bwt(data)
            candidates.append((len(r), r))

        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    def _compress_extreme(self, data: bytes, data_type: str) -> bytes:
        """极限压缩: 全策略竞争。"""
        candidates = []
        use_bwt = len(data) <= BWT_PRACTICAL_LIMIT
        is_large = len(data) > 2 << 20

        # 策略0: LZMA2 基线
        r = self._strategy_lzma_only(data)
        candidates.append((len(r), r))

        # 策略1: LZMA2 Delta滤镜 (C级速度, 各种距离)
        delta_dists = [1, 2, 3, 4, 6, 8, 12, 16, 24, 32] if not is_large else [1, 2, 4, 8, 16, 32]
        for dist in delta_dists:
            r = self._strategy_delta_filter(data, dist)
            candidates.append((len(r), r))

        # 策略2: BCJ滤镜 (二进制数据)
        if data_type == 'binary':
            r = self._strategy_bcj(data)
            candidates.append((len(r), r))
            # BCJ + Delta
            for dist in [1, 2, 4]:
                r = self._strategy_bcj_delta(data, dist)
                candidates.append((len(r), r))

        if use_bwt:
            # 策略3: BWT + LZMA2
            r = self._strategy_bwt(data)
            candidates.append((len(r), r))

            # 策略4: BWT + RLE + LZMA2
            r = self._strategy_bwt_rle(data)
            candidates.append((len(r), r))

        # 基于数据类型的专用策略 (仅小文件, 因为Python预处理慢)
        if not is_large:
            if data_type == 'text':
                r = self._strategy_text_dict(data)
                candidates.append((len(r), r))
                if use_bwt:
                    r = self._strategy_text_dict_bwt(data)
                    candidates.append((len(r), r))

            elif data_type == 'json':
                r = self._strategy_json_dedup(data)
                candidates.append((len(r), r))
                if use_bwt:
                    r = self._strategy_json_dedup_bwt(data)
                    candidates.append((len(r), r))

            elif data_type == 'log':
                r = self._strategy_log_template(data)
                candidates.append((len(r), r))
                if use_bwt:
                    r = self._strategy_log_template_bwt(data)
                    candidates.append((len(r), r))

            elif data_type == 'csv':
                if use_bwt:
                    r = self._strategy_column_bwt(data)
                    candidates.append((len(r), r))

            elif data_type == 'code':
                r = self._strategy_text_dict(data)
                candidates.append((len(r), r))
                if use_bwt:
                    r = self._strategy_text_dict_bwt(data)
                    candidates.append((len(r), r))

            # 额外: 列转置 + Delta滤镜
            if data_type in ('log', 'csv') and len(data) <= BWT_PRACTICAL_LIMIT:
                transposed, row_width = column_transpose_encode(data)
                if row_width > 1:
                    for dist in [1, 2, row_width]:
                        r = self._strategy_transposed_delta(data, dist)
                        candidates.append((len(r), r))

        if self.verbose:
            print(f"  尝试了 {len(candidates)} 种策略, "
                  f"最佳: {min(c[0] for c in candidates):,} 字节")

        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    # ─────────────────────────────────────────
    #  各压缩策略实现
    # ─────────────────────────────────────────

    def _strategy_lzma_only(self, data: bytes) -> bytes:
        """策略0: LZMA2 RAW。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = self._serialize_filters_info(filters, dict_size)
        return self._build_output(data, lzma_data, strategy=0, extra_header=extra)

    def _strategy_delta_filter(self, data: bytes, dist: int) -> bytes:
        """策略1: LZMA2 Delta滤镜 (C级速度!)。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = self._serialize_filters_info(filters, dict_size, delta_dist=dist)
        return self._build_output(data, lzma_data, strategy=1, extra_header=extra)

    def _strategy_bcj(self, data: bytes) -> bytes:
        """策略4: BCJ + LZMA2。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, bcj=True)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = self._serialize_filters_info(filters, dict_size, bcj=True)
        return self._build_output(data, lzma_data, strategy=4, extra_header=extra)

    def _strategy_bcj_delta(self, data: bytes, dist: int) -> bytes:
        """策略4b: BCJ + Delta + LZMA2。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, bcj=True)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = self._serialize_filters_info(filters, dict_size, delta_dist=dist, bcj=True)
        return self._build_output(data, lzma_data, strategy=4, extra_header=extra)

    def _strategy_bwt(self, data: bytes) -> bytes:
        """策略2: BWT + LZMA2。"""
        bwt_data, block_info = bwt_encode(data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=2, extra_header=bytes(extra))

    def _strategy_bwt_rle(self, data: bytes) -> bytes:
        """策略3: BWT + RLE + LZMA2。"""
        bwt_data, block_info = bwt_encode(data, block_size=0)
        rle_data = rle_encode(bwt_data)
        dict_size = _smart_dict_size(len(rle_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(rle_data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=3, extra_header=bytes(extra))

    def _strategy_text_dict(self, data: bytes) -> bytes:
        """策略5: 文本字典 + LZMA2。"""
        encoded, dict_bytes = text_dict_encode(data)
        if not dict_bytes:
            return self._strategy_lzma_only(data)
        
        dict_size = _smart_dict_size(len(encoded))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(encoded, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>H', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=5, extra_header=bytes(extra))

    def _strategy_text_dict_bwt(self, data: bytes) -> bytes:
        """策略5b: 文本字典 + BWT + LZMA2。"""
        encoded, dict_bytes = text_dict_encode(data)
        if not dict_bytes:
            return self._strategy_bwt(data)
        
        bwt_data, block_info = bwt_encode(encoded, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>H', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=5, extra_header=bytes(extra))

    def _strategy_json_dedup(self, data: bytes) -> bytes:
        """策略6: JSON键去重 + LZMA2。"""
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes:
            return self._strategy_lzma_only(data)
        
        dict_size = _smart_dict_size(len(transformed))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(transformed, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>H', len(schema_bytes)))
        extra.extend(schema_bytes)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=6, extra_header=bytes(extra))

    def _strategy_json_dedup_bwt(self, data: bytes) -> bytes:
        """策略6b: JSON键去重 + BWT + LZMA2。"""
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes:
            return self._strategy_bwt(data)
        
        bwt_data, block_info = bwt_encode(transformed, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>H', len(schema_bytes)))
        extra.extend(schema_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=6, extra_header=bytes(extra))

    def _strategy_log_template(self, data: bytes) -> bytes:
        """策略7: 日志模板 + LZMA2。"""
        var_data, template_bytes = log_template_encode(data)
        if not template_bytes:
            return self._strategy_lzma_only(data)
        
        dict_size = _smart_dict_size(len(var_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(var_data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>I', len(template_bytes)))
        extra.extend(template_bytes)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=7, extra_header=bytes(extra))

    def _strategy_log_template_bwt(self, data: bytes) -> bytes:
        """策略7b: 日志模板 + BWT + LZMA2。"""
        var_data, template_bytes = log_template_encode(data)
        if not template_bytes:
            return self._strategy_bwt(data)
        
        bwt_data, block_info = bwt_encode(var_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>I', len(template_bytes)))
        extra.extend(template_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=7, extra_header=bytes(extra))

    def _strategy_column_bwt(self, data: bytes) -> bytes:
        """策略8: 列转置 + BWT + LZMA2。"""
        transposed, row_width = column_transpose_encode(data)
        if row_width <= 1:
            return self._strategy_bwt(data)
        
        bwt_data, block_info = bwt_encode(transposed, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>H', row_width))
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=8, extra_header=bytes(extra))

    def _strategy_transposed_delta(self, data: bytes, dist: int) -> bytes:
        """列转置 + Delta滤镜 + LZMA2。"""
        transposed, row_width = column_transpose_encode(data)
        if row_width <= 1:
            return self._strategy_delta_filter(data, dist)
        
        dict_size = _smart_dict_size(len(transposed))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist)
        lzma_data = lzma.compress(transposed, format=lzma.FORMAT_RAW, filters=filters)
        
        extra = bytearray()
        extra.extend(struct.pack('>H', row_width))
        extra.extend(self._serialize_filters_info(filters, dict_size, delta_dist=dist))
        return self._build_output(data, lzma_data, strategy=8, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  序列化工具
    # ─────────────────────────────────────────

    @staticmethod
    def _serialize_filters_info(filters: list, dict_size: int,
                                 delta_dist: int = 0, bcj: bool = False) -> bytes:
        """序列化滤镜参数 (紧凑格式)。"""
        result = bytearray()
        # 字节0: 滤镜标志位
        flags = 0
        if delta_dist > 0:
            flags |= 0x01
        if bcj:
            flags |= 0x02
        result.append(flags)
        
        # 字节1-4: dict_size
        result.extend(struct.pack('>I', dict_size))
        
        # 字节5: LZMA2 lc/lp/pb (从最后一个filter中提取)
        lzma2 = filters[-1]
        result.append(lzma2.get('lc', 3))
        result.append(lzma2.get('lp', 0))
        result.append(lzma2.get('pb', 2))
        
        # Delta距离
        if delta_dist > 0:
            result.extend(struct.pack('>H', delta_dist))
        
        return bytes(result)

    def _build_output(self, data: bytes, lzma_data: bytes,
                      strategy: int, extra_header: bytes) -> bytes:
        """构建最终输出字节流。"""
        result = bytearray()
        original_size = len(data)

        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>I', original_size))
        result.append(strategy)
        result.extend(struct.pack('>H', len(extra_header)))

        if extra_header:
            result.extend(extra_header)

        result.extend(struct.pack('>I', len(lzma_data)))
        result.extend(lzma_data)

        return bytes(result)

    def _build_empty_header(self) -> bytes:
        """构建空文件的头部。"""
        result = bytearray()
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>I', 0))
        result.append(0)
        result.extend(struct.pack('>H', 0))
        result.extend(struct.pack('>I', 0))
        return bytes(result)
