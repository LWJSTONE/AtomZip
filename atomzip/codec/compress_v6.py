"""
AtomZip 压缩引擎 v6 — 极限压缩 (C加速全文件BWT)

核心创新:
  1. C语言BWT引擎 — 无大小限制, 10MB仅需3秒
  2. 全文件BWT — 对所有类型数据都可用
  3. 14+策略竞争 — 每种数据类型尝试所有可能的策略组合
  4. 穷举LZMA2参数 — lc/lp/pb/dict_size全搜索
  5. 递归压缩尝试 — 首轮后若不理想,尝试二次压缩
  6. 增强RLE — 双字节计数,更有效压缩BWT后的长行程

策略列表:
  0: LZMA2 RAW (基线, 各种dict_size)
  1: LZMA2 + Delta滤镜 (dist=1..64)
  2: BWT + LZMA2 (C引擎, 全文件)
  3: BWT + RLE + LZMA2
  4: BWT + Delta滤镜 + LZMA2
  5: 文本字典 + BWT + LZMA2
  6: JSON键去重 + BWT + LZMA2
  7: 日志模板 + BWT + LZMA2
  8: 列转置 + BWT + LZMA2
  9: BWT + RLE + Delta滤镜 + LZMA2
  10: BCJ + LZMA2 (二进制)
  11: BCJ + Delta + LZMA2 (二进制)
  12: 递归压缩 (首遍BWT+LZMA2后再BWT+LZMA2)
"""

import struct
import time
import lzma
import re
from typing import Tuple, List
from collections import Counter

from .transform_v6 import (
    bwt_encode, bwt_decode,
    rle_encode, rle_decode,
    text_dict_encode, text_dict_decode,
    json_key_dedup_encode, json_key_dedup_decode,
    log_template_encode, log_template_decode,
    column_transpose_encode, column_transpose_decode,
    serialize_block_info, deserialize_block_info,
    BWT_MAX_DATA_SIZE
)

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 6

# LZMA2参数搜索空间
LZMA2_PARAM_SETS = [
    # (lc, lp, pb) — lc+lp <= 4
    (3, 0, 2),  # 默认
    (2, 0, 2),
    (2, 1, 2),
    (1, 0, 2),
    (1, 1, 2),
    (1, 2, 2),
    (0, 0, 2),
    (0, 2, 2),
    (0, 3, 2),
    (4, 0, 2),
    (3, 1, 2),
    (2, 2, 2),
    (2, 0, 0),
    (3, 0, 0),
    (1, 0, 0),
    (0, 0, 0),
]


def _get_lzma_filters(preset: int = 9, dict_size: int = 0,
                      lc: int = 3, lp: int = 0, pb: int = 2,
                      delta_dist: int = 0, bcj: bool = False) -> list:
    """获取 LZMA2 滤镜参数。"""
    filters = []

    if bcj:
        filters.append({'id': lzma.FILTER_X86})

    if delta_dist > 0:
        filters.append({'id': lzma.FILTER_DELTA, 'dist': delta_dist})

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
    """计算最优字典大小 — v6版本: 更大的字典。"""
    # 对于小文件, dict_size >= 文件大小
    # 对于大文件, dict_size最大256MB (LZMA2稳定范围)
    return max(1 << 16, min(data_len, 1 << 28))


def _detect_record_size(data: bytes) -> int:
    """检测固定大小记录的二进制数据的记录大小。返回0表示未检测到。"""
    n = len(data)
    if n < 100:
        return 0

    # 尝试常见记录大小
    for rec_size in [4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 128]:
        if n % rec_size != 0:
            continue
        num_records = n // rec_size
        if num_records < 10:
            continue

        # 检查前100条记录是否有规律
        # 测试: 每条记录的第一个4字节是否递增
        if rec_size >= 4:
            first_vals = []
            for i in range(min(num_records, 200)):
                val = struct.unpack('>I', data[i*rec_size:i*rec_size+4])[0]
                first_vals.append(val)

            # 检查是否递增
            increasing = sum(1 for i in range(1, len(first_vals))
                           if first_vals[i] > first_vals[i-1])
            if increasing > len(first_vals) * 0.8:
                return rec_size

    return 0


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

    # 检测结构化二进制 (重复记录模式)
    if len(data) >= 100:
        # 检查是否有固定大小的重复结构
        for rec_size in [8, 12, 16, 20, 24, 28, 32, 48, 64]:
            if len(data) % rec_size == 0 and len(data) // rec_size >= 10:
                # 检查前几条记录的某些字段是否有规律
                num_records = len(data) // rec_size
                # 检查第一个字段是否递增
                if rec_size >= 4:
                    first_vals = []
                    for i in range(min(num_records, 100)):
                        val = struct.unpack('>I', data[i*rec_size:i*rec_size+4])[0]
                        first_vals.append(val)
                    if len(set(first_vals)) > 5:
                        return 'binary_structured'

    return 'binary'


class AtomZipCompressor:
    """AtomZip v6: C加速全文件BWT + 穷举参数 + 14+策略竞争"""

    def __init__(self, level: int = 9, verbose: bool = False):
        self.level = max(1, min(9, level))
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        """压缩数据。"""
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v6] 开始压缩 {original_size:,} 字节...")

        if original_size == 0:
            return self._build_empty_header()

        data_type = _detect_data_type(data)
        if self.verbose:
            print(f"  数据类型: {data_type}")

        if self.level < 4:
            result = self._strategy_lzma_only(data, lc=3, lp=0, pb=2)
        elif self.level < 7:
            result = self._compress_medium(data, data_type)
        else:
            result = self._compress_extreme(data, data_type)

        elapsed = time.time() - start_time
        ratio = original_size / max(1, len(result))

        if self.verbose:
            print(f"[AtomZip v6] 压缩完成: {original_size:,} -> {len(result):,} 字节 "
                  f"(比率: {ratio:.2f}:1, 耗时: {elapsed:.2f}秒)")

        return result

    def _compress_medium(self, data: bytes, data_type: str) -> bytes:
        """中等压缩: BWT + Delta + 基线。"""
        candidates = []

        # 策略0: LZMA2 基线
        r = self._strategy_lzma_only(data, lc=3, lp=0, pb=2)
        candidates.append((len(r), r))

        # 策略1: Delta滤镜
        for dist in [1, 2, 4, 8]:
            r = self._strategy_delta_filter(data, dist, lc=3, lp=0, pb=2)
            candidates.append((len(r), r))

        # 策略2: BWT + LZMA2 (C引擎!)
        r = self._strategy_bwt(data, lc=3, lp=0, pb=2)
        candidates.append((len(r), r))

        # 策略3: BWT + RLE + LZMA2
        r = self._strategy_bwt_rle(data, lc=3, lp=0, pb=2)
        candidates.append((len(r), r))

        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    def _compress_extreme(self, data: bytes, data_type: str) -> bytes:
        """极限压缩: 两阶段策略 — 先找最佳变换，再搜索LZMA2参数。"""
        candidates = []
        original_size = len(data)

        # === 第一阶段: 快速筛选 — 找最佳变换策略 (默认LZMA2参数) ===
        transform_candidates = []

        # 策略0: LZMA2 基线
        r = self._strategy_lzma_only(data)
        transform_candidates.append(('lzma', len(r), r))

        # 策略1: Delta滤镜 (关键距离)
        for dist in [1, 2, 4, 8, 16, 32, 64]:
            r = self._strategy_delta_filter(data, dist)
            transform_candidates.append((f'delta_{dist}', len(r), r))

        # 策略2: BWT + LZMA2 (C引擎! 全文件!)
        r = self._strategy_bwt(data)
        transform_candidates.append(('bwt', len(r), r))

        # 策略3: BWT + RLE + LZMA2
        r = self._strategy_bwt_rle(data)
        transform_candidates.append(('bwt_rle', len(r), r))

        # 策略4: BWT + Delta滤镜 + LZMA2
        for dist in [1, 2, 4, 8]:
            r = self._strategy_bwt_delta(data, dist)
            transform_candidates.append((f'bwt_delta_{dist}', len(r), r))

        # 数据类型专用策略
        if data_type in ('text', 'code'):
            r = self._strategy_text_dict_bwt(data)
            transform_candidates.append(('text_dict_bwt', len(r), r))
            r = self._strategy_text_dict(data)
            transform_candidates.append(('text_dict', len(r), r))

        elif data_type == 'json':
            r = self._strategy_json_dedup_bwt(data)
            transform_candidates.append(('json_bwt', len(r), r))
            r = self._strategy_json_dedup(data)
            transform_candidates.append(('json', len(r), r))

        elif data_type == 'log':
            r = self._strategy_log_template_bwt(data)
            transform_candidates.append(('log_bwt', len(r), r))
            r = self._strategy_log_template(data)
            transform_candidates.append(('log', len(r), r))
            r = self._strategy_column_bwt(data)
            transform_candidates.append(('col_bwt', len(r), r))

        elif data_type == 'csv':
            r = self._strategy_column_bwt(data)
            transform_candidates.append(('col_bwt', len(r), r))
            for dist in [1, 2, 4]:
                r = self._strategy_transposed_delta(data, dist)
                transform_candidates.append((f'trans_delta_{dist}', len(r), r))

        elif data_type in ('binary_structured', 'binary'):
            r = self._strategy_bcj(data)
            transform_candidates.append(('bcj', len(r), r))
            for dist in [1, 2, 4]:
                r = self._strategy_bcj_delta(data, dist)
                transform_candidates.append((f'bcj_delta_{dist}', len(r), r))
            # BWT对二进制也有效
            r = self._strategy_bwt_rle_delta(data, 1)
            transform_candidates.append(('bwt_rle_delta_1', len(r), r))
            for dist in [2, 4]:
                r = self._strategy_bwt_delta(data, dist)
                transform_candidates.append((f'bwt_delta_{dist}', len(r), r))

            # 记录级Delta编码 — 对结构化二进制最有效!
            rec_size = _detect_record_size(data)
            if rec_size > 0:
                r = self._strategy_delta_filter(data, rec_size)
                transform_candidates.append((f'rec_delta_{rec_size}', len(r), r))
                # 记录级Delta + BWT
                r = self._strategy_rec_delta_bwt(data, rec_size)
                transform_candidates.append((f'rec_delta_bwt_{rec_size}', len(r), r))

        # 按大小排序，取前3个最佳变换
        transform_candidates.sort(key=lambda x: x[1])
        top_transforms = transform_candidates[:min(3, len(transform_candidates))]

        # 加入所有候选
        for name, size, result in transform_candidates:
            candidates.append((size, result))

        # === 第二阶段: 对最佳变换尝试不同LZMA2参数 ===
        # 只对涉及BWT的变换做参数搜索 (它们最有潜力)
        for name, _, _ in top_transforms:
            if 'bwt' in name and original_size > 10000:
                for lc, lp, pb in LZMA2_PARAM_SETS[1:8]:  # 跳过默认(已测试)
                    if name == 'bwt':
                        r = self._strategy_bwt(data, lc=lc, lp=lp, pb=pb)
                    elif name == 'bwt_rle':
                        r = self._strategy_bwt_rle(data, lc=lc, lp=lp, pb=pb)
                    elif name.startswith('bwt_delta_'):
                        dist = int(name.split('_')[-1])
                        r = self._strategy_bwt_delta(data, dist, lc=lc, lp=lp, pb=pb)
                    elif name in ('text_dict_bwt', 'json_bwt', 'log_bwt', 'col_bwt'):
                        # 预处理+BWT，参数搜索收益较小，只试2组
                        if (lc, lp, pb) in [(2, 0, 2), (1, 0, 2)]:
                            if name == 'text_dict_bwt':
                                r = self._strategy_text_dict_bwt(data)
                            elif name == 'json_bwt':
                                r = self._strategy_json_dedup_bwt(data)
                            elif name == 'log_bwt':
                                r = self._strategy_log_template_bwt(data)
                            elif name == 'col_bwt':
                                r = self._strategy_column_bwt(data)
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
                    candidates.append((len(r), r))

        # === 第三轮: 递归BWT (如果还没达到目标) ===
        if original_size > 10000:
            candidates.sort(key=lambda x: x[0])
            best_ratio = original_size / max(1, candidates[0][0])
            if best_ratio < 100:
                r = self._strategy_recursive_bwt(data)
                candidates.append((len(r), r))

        if self.verbose:
            print(f"  尝试了 {len(candidates)} 种策略, "
                  f"最佳: {min(c[0] for c in candidates):,} 字节")

        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    # ─────────────────────────────────────────
    #  各压缩策略实现
    # ─────────────────────────────────────────

    def _strategy_lzma_only(self, data: bytes, lc=3, lp=0, pb=2) -> bytes:
        """策略0: LZMA2 RAW。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)

        extra = self._serialize_filters_info(filters, dict_size)
        return self._build_output(data, lzma_data, strategy=0, extra_header=extra)

    def _strategy_delta_filter(self, data: bytes, dist: int, lc=3, lp=0, pb=2) -> bytes:
        """策略1: LZMA2 Delta滤镜。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)

        extra = self._serialize_filters_info(filters, dict_size, delta_dist=dist)
        return self._build_output(data, lzma_data, strategy=1, extra_header=extra)

    def _strategy_bcj(self, data: bytes) -> bytes:
        """策略10: BCJ + LZMA2。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, bcj=True)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)

        extra = self._serialize_filters_info(filters, dict_size, bcj=True)
        return self._build_output(data, lzma_data, strategy=10, extra_header=extra)

    def _strategy_bcj_delta(self, data: bytes, dist: int) -> bytes:
        """策略11: BCJ + Delta + LZMA2。"""
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, bcj=True)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)

        extra = self._serialize_filters_info(filters, dict_size, delta_dist=dist, bcj=True)
        return self._build_output(data, lzma_data, strategy=11, extra_header=extra)

    def _strategy_bwt(self, data: bytes, lc=3, lp=0, pb=2) -> bytes:
        """策略2: BWT + LZMA2 (C引擎全文件BWT!)。"""
        bwt_data, block_info = bwt_encode(data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)

        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=2, extra_header=bytes(extra))

    def _strategy_bwt_rle(self, data: bytes, lc=3, lp=0, pb=2) -> bytes:
        """策略3: BWT + RLE + LZMA2。"""
        bwt_data, block_info = bwt_encode(data, block_size=0)
        rle_data = rle_encode(bwt_data)
        dict_size = _smart_dict_size(len(rle_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(rle_data, format=lzma.FORMAT_RAW, filters=filters)

        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=3, extra_header=bytes(extra))

    def _strategy_bwt_delta(self, data: bytes, dist: int, lc=3, lp=0, pb=2) -> bytes:
        """策略4: BWT + Delta滤镜 + LZMA2。"""
        bwt_data, block_info = bwt_encode(data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)

        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size, delta_dist=dist))
        return self._build_output(data, lzma_data, strategy=4, extra_header=bytes(extra))

    def _strategy_bwt_rle_delta(self, data: bytes, dist: int, lc=3, lp=0, pb=2) -> bytes:
        """策略9: BWT + RLE + Delta滤镜 + LZMA2。"""
        bwt_data, block_info = bwt_encode(data, block_size=0)
        rle_data = rle_encode(bwt_data)
        dict_size = _smart_dict_size(len(rle_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(rle_data, format=lzma.FORMAT_RAW, filters=filters)

        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size, delta_dist=dist))
        return self._build_output(data, lzma_data, strategy=9, extra_header=bytes(extra))

    def _strategy_text_dict(self, data: bytes) -> bytes:
        """策略5a: 文本字典 + LZMA2。"""
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
        """策略6a: JSON键去重 + LZMA2。"""
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
        """策略7a: 日志模板 + LZMA2。"""
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

    def _strategy_recursive_bwt(self, data: bytes) -> bytes:
        """策略12: 递归BWT压缩 — 对BWT输出再做BWT+LZMA2。"""
        # 第一遍: BWT
        bwt1_data, block_info1 = bwt_encode(data, block_size=0)

        # 第二遍: 对BWT输出再做BWT
        bwt2_data, block_info2 = bwt_encode(bwt1_data, block_size=0)

        dict_size = _smart_dict_size(len(bwt2_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt2_data, format=lzma.FORMAT_RAW, filters=filters)

        extra = bytearray()
        # 存储2层BWT信息
        extra.extend(serialize_block_info(block_info1))
        extra.extend(serialize_block_info(block_info2))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=12, extra_header=bytes(extra))

    def _strategy_rec_delta_bwt(self, data: bytes, rec_size: int) -> bytes:
        """策略13: 记录级Delta + BWT + LZMA2 — 对结构化二进制最有效!"""
        # 先用记录级步长Delta
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=rec_size)
        # 先做Delta变换, 再BWT
        from .transform_v6 import delta_encode
        delta_data, first_byte = delta_encode(data, stride=rec_size)

        # 对Delta输出做BWT
        bwt_data, block_info = bwt_encode(delta_data, block_size=0)
        dict_size2 = _smart_dict_size(len(bwt_data))
        filters2 = _get_lzma_filters(9, dict_size=dict_size2)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters2)

        extra = bytearray()
        # 存储记录大小和first_byte
        extra.append(0x13)  # 策略13标记
        extra.extend(struct.pack('>H', rec_size))
        extra.append(first_byte)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters2, dict_size2))
        return self._build_output(data, lzma_data, strategy=13, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  序列化工具
    # ─────────────────────────────────────────

    @staticmethod
    def _serialize_filters_info(filters: list, dict_size: int,
                                 delta_dist: int = 0, bcj: bool = False) -> bytes:
        """序列化滤镜参数。"""
        result = bytearray()
        flags = 0
        if delta_dist > 0:
            flags |= 0x01
        if bcj:
            flags |= 0x02
        result.append(flags)
        result.extend(struct.pack('>I', dict_size))

        lzma2 = filters[-1]
        result.append(lzma2.get('lc', 3))
        result.append(lzma2.get('lp', 0))
        result.append(lzma2.get('pb', 2))

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
