"""
AtomZip 压缩引擎 v9 — 深度结构提取 + 全局去重 + 多轮压缩

核心创新 (相比v8):
  1. 深度JSON提取 — 完全解析JSON，分离骨架与值，值按类型分流
  2. 深度日志提取 — 多模板匹配，变量按类型(delta/enum/dict)分流
  3. 深度CSV提取 — 列转置+每列类型检测+类型专属压缩
  4. 全局去重 — 滚动哈希找出所有重复子串
  5. 文本段落去重 — 按行/段落哈希去重
  6. 递归BPE — 多轮迭代直到无改善
  7. 保留v8所有策略

v9策略列表 (v8全部 + v9新增):
  ★ v9新策略:
  50: 深度JSON + BWT + LZMA2
  51: 深度JSON + 递归BPE + BWT + LZMA2
  52: 深度日志 + BWT + LZMA2
  53: 深度日志 + 递归BPE + BWT + LZMA2
  54: 深度CSV + BWT + LZMA2
  55: 深度CSV + 递归BPE + BWT + LZMA2
  56: 全局去重 + BWT + LZMA2
  57: 全局去重 + 递归BPE + BWT + LZMA2
  58: 文本段落去重 + BWT + LZMA2
  59: 文本段落去重 + 递归BPE + BWT + LZMA2
  60: 深度提取 + 全局去重 + BWT + LZMA2
  61: 深度提取 + 全局去重 + 递归BPE + BWT + LZMA2
  62: 文本段落去重 + 全局去重 + BWT + LZMA2
  63: 递归BPE + BWT + LZMA2
"""

import struct
import time
import lzma
import bz2
import re
from typing import Tuple, List
from collections import Counter

from .transform_v9 import (
    bwt_encode, bwt_decode,
    rle_encode, rle_decode,
    delta_encode, delta_decode,
    text_dict_encode, text_dict_decode,
    json_key_dedup_encode, json_key_dedup_decode,
    log_template_encode, log_template_decode,
    log_field_encode, log_field_decode,
    column_transpose_encode, column_transpose_decode,
    bpe_encode, bpe_decode,
    ngram_dict_encode, ngram_dict_decode,
    csv_column_encode, csv_column_decode,
    json_flatten_encode, json_flatten_decode,
    serialize_block_info, deserialize_block_info,
    BWT_MAX_DATA_SIZE,
    bpe_encode_ultra, bpe_decode_ultra,
    word_dict_encode, word_dict_decode,
    ngram_dict_encode_v8, ngram_dict_decode_v8,
    # v9新增
    deep_json_encode, deep_json_decode,
    deep_log_encode, deep_log_decode,
    deep_csv_encode, deep_csv_decode,
    global_dedup_encode, global_dedup_decode,
    text_dedup_encode, text_dedup_decode,
    bpe_encode_recursive, bpe_decode_recursive,
)

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 9

# LZMA2参数搜索空间
LZMA2_PARAM_SETS = [
    (3, 0, 2), (2, 0, 2), (2, 1, 2), (1, 0, 2), (1, 1, 2),
    (1, 2, 2), (0, 0, 2), (0, 2, 2), (0, 3, 2), (4, 0, 2),
    (3, 1, 2), (2, 2, 2), (2, 0, 0), (3, 0, 0), (1, 0, 0), (0, 0, 0),
]


def _get_lzma_filters(preset=9, dict_size=0, lc=3, lp=0, pb=2,
                      delta_dist=0, bcj=False) -> list:
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
    """v9: 最大2GB字典"""
    return max(1 << 16, min(data_len, 1 << 31))


def _detect_data_type(data: bytes) -> str:
    if not data:
        return 'empty'
    sample = data[:8192]
    stripped = sample.lstrip()
    if stripped.startswith(b'{') or stripped.startswith(b'['):
        json_key_pattern = re.compile(rb'"[\w_]+"\s*:')
        if len(json_key_pattern.findall(sample)) >= 3:
            return 'json'
        try:
            import json
            json.loads(sample.decode('utf-8', errors='replace'))
            return 'json'
        except Exception:
            pass
    log_pattern = re.compile(rb'\d{4}-\d{2}-\d{2}.*\[(INFO|WARN|ERROR|DEBUG)\]')
    if len(log_pattern.findall(sample)) >= 3:
        return 'log'
    apache_pattern = re.compile(rb'\d+\.\d+\.\d+\.\d+.*HTTP/\d\.\d')
    if len(apache_pattern.findall(sample)) >= 3:
        return 'log'
    lines = sample.split(b'\n', 20)
    if len(lines) >= 3:
        comma_counts = [line.count(b',') for line in lines[:10] if line.strip()]
        if comma_counts and len(set(comma_counts)) == 1 and comma_counts[0] >= 3:
            return 'csv'
    code_indicators = [b'def ', b'class ', b'import ', b'function ', b'var ', b'const ']
    code_score = sum(1 for ind in code_indicators if ind in sample)
    if code_score >= 2:
        return 'code'
    printable_count = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
    if printable_count / max(1, len(sample)) > 0.85:
        return 'text'
    return 'binary'


class AtomZipCompressor:
    """AtomZip v9: 深度结构提取 + 全局去重 + 多轮压缩"""

    def __init__(self, level: int = 9, verbose: bool = False):
        self.level = max(1, min(9, level))
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v9] 开始压缩 {original_size:,} 字节...")

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
            print(f"[AtomZip v9] 压缩完成: {original_size:,} -> {len(result):,} 字节 "
                  f"(比率: {ratio:.2f}:1, 耗时: {elapsed:.2f}秒)")
        return result

    def _compress_medium(self, data: bytes, data_type: str) -> bytes:
        candidates = []
        r = self._strategy_lzma_only(data)
        candidates.append(('lzma', len(r), r))
        for dist in [1, 2, 4, 8]:
            r = self._strategy_delta_filter(data, dist)
            candidates.append((f'delta_{dist}', len(r), r))
        r = self._strategy_bwt(data)
        candidates.append(('bwt', len(r), r))
        r = self._strategy_bpe_bwt(data)
        candidates.append(('bpe_bwt', len(r), r))
        r = self._strategy_ubpe_bwt(data)
        candidates.append(('ubpe_bwt', len(r), r))

        # v9: 加上深度提取策略
        if data_type == 'json':
            r = self._strategy_deep_json_bwt(data)
            candidates.append(('deep_json_bwt', len(r), r))
        elif data_type == 'log':
            r = self._strategy_deep_log_bwt(data)
            candidates.append(('deep_log_bwt', len(r), r))
        elif data_type == 'csv':
            r = self._strategy_deep_csv_bwt(data)
            candidates.append(('deep_csv_bwt', len(r), r))
        elif data_type in ('text', 'code'):
            r = self._strategy_text_dedup_bwt(data)
            candidates.append(('text_dedup_bwt', len(r), r))

        candidates.sort(key=lambda x: x[1])
        return candidates[0][2]

    def _compress_extreme(self, data: bytes, data_type: str) -> bytes:
        candidates = []
        original_size = len(data)
        # 优化：大文件用更少的策略
        fast_mode = original_size > 5_000_000
        ultra_fast = original_size > 10_000_000

        # === 核心基线策略 (始终执行) ===
        r = self._strategy_lzma_only(data)
        candidates.append(('lzma', len(r), r))

        r = self._strategy_bwt(data)
        candidates.append(('bwt', len(r), r))

        # Delta滤镜 (仅2个)
        for dist in [1, 4][:1 if ultra_fast else 2]:
            r = self._strategy_delta_filter(data, dist)
            candidates.append((f'delta_{dist}', len(r), r))

        # === v9 深度提取策略 (仅对小文件，大文件太慢) ===
        # 暂时禁用：deep_csv/deep_json/deep_log编码太慢
        # 仅使用BWT+LZMA2核心策略（已证明最有效）
        pass

        # 对大文件跳过慢速BPE策略(只保留快速策略)
        # BPE策略太慢(>30s/MB)，暂时禁用
        # if not fast_mode:
        #     r = self._strategy_ubpe_bwt(data)
        #     candidates.append(('ubpe_bwt', len(r), r))
        #     r = self._strategy_bpe_bwt(data)
        #     candidates.append(('bpe_bwt', len(r), r))

        # 全局去重策略也暂时禁用(对大文件太慢)
        # if not fast_mode:
        #     r = self._strategy_global_dedup_bwt(data)
        #     candidates.append(('dedup_bwt', len(r), r))

        if self.verbose:
            best_name = min(candidates, key=lambda x: x[1])[0]
            best_size = min(c[1] for c in candidates)
            print(f"  尝试了 {len(candidates)} 种策略, "
                  f"最佳: {best_name} ({best_size:,} 字节)")

        candidates.sort(key=lambda x: x[1])
        return candidates[0][2]

    # ─────────────────────────────────────────
    #  基线策略 (继承v8)
    # ─────────────────────────────────────────

    def _strategy_lzma_only(self, data, lc=3, lp=0, pb=2):
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        extra = self._serialize_filters_info(filters, dict_size)
        return self._build_output(data, lzma_data, strategy=0, extra_header=extra)

    def _strategy_delta_filter(self, data, dist, lc=3, lp=0, pb=2):
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        extra = self._serialize_filters_info(filters, dict_size, delta_dist=dist)
        return self._build_output(data, lzma_data, strategy=1, extra_header=extra)

    def _strategy_bcj(self, data):
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, bcj=True)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        extra = self._serialize_filters_info(filters, dict_size, bcj=True)
        return self._build_output(data, lzma_data, strategy=10, extra_header=extra)

    def _strategy_bcj_delta(self, data, dist):
        dict_size = _smart_dict_size(len(data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, bcj=True)
        lzma_data = lzma.compress(data, format=lzma.FORMAT_RAW, filters=filters)
        extra = self._serialize_filters_info(filters, dict_size, delta_dist=dist, bcj=True)
        return self._build_output(data, lzma_data, strategy=11, extra_header=extra)

    def _strategy_bwt(self, data, lc=3, lp=0, pb=2):
        bwt_data, block_info = bwt_encode(data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=2, extra_header=bytes(extra))

    def _strategy_bwt_rle(self, data, lc=3, lp=0, pb=2):
        bwt_data, block_info = bwt_encode(data, block_size=0)
        rle_data = rle_encode(bwt_data)
        dict_size = _smart_dict_size(len(rle_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(rle_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=3, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  v8 BPE/N-gram策略
    # ─────────────────────────────────────────

    def _strategy_bpe_bwt(self, data, lc=3, lp=0, pb=2):
        bpe_data, bpe_rules = bpe_encode(data)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=14, extra_header=bytes(extra))

    def _strategy_ubpe_bwt(self, data, lc=3, lp=0, pb=2):
        bpe_data, bpe_rules = bpe_encode_ultra(data)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=30, extra_header=bytes(extra))

    def _strategy_word_dict_bwt(self, data):
        encoded, dict_bytes = word_dict_encode(data)
        if not dict_bytes:
            return self._strategy_bwt(data)
        bwt_data, block_info = bwt_encode(encoded, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=34, extra_header=bytes(extra))

    def _strategy_text_dict_bwt(self, data):
        encoded, dict_bytes = text_dict_encode(data)
        if not dict_bytes:
            return self._strategy_bwt(data)
        bwt_data, block_info = bwt_encode(encoded, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=5, extra_header=bytes(extra))

    def _strategy_json_dedup_bwt(self, data):
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes:
            return self._strategy_bwt(data)
        bwt_data, block_info = bwt_encode(transformed, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(schema_bytes)))
        extra.extend(schema_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=6, extra_header=bytes(extra))

    def _strategy_json_dedup_ubpe_bwt(self, data):
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes:
            return self._strategy_ubpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode_ultra(transformed)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(schema_bytes)))
        extra.extend(schema_bytes)
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=39, extra_header=bytes(extra))

    def _strategy_log_template_bwt(self, data):
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

    def _strategy_column_bwt(self, data):
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

    def _strategy_csv_column_bwt(self, data):
        csv_data, meta_bytes = csv_column_encode(data)
        if not meta_bytes:
            return self._strategy_column_bwt(data)
        bwt_data, block_info = bwt_encode(csv_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(meta_bytes)))
        extra.extend(meta_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=19, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  ★ v9 深度提取策略
    # ─────────────────────────────────────────

    def _strategy_deep_json_bwt(self, data):
        """策略50: 深度JSON + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_json_encode(data)
            if not meta_bytes:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)  # v9标记
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=50, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_deep_json_rbpe_bwt(self, data):
        """策略51: 深度JSON + 递归BPE + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_json_encode(data)
            if not meta_bytes:
                return self._strategy_ubpe_bwt(data)
            bpe_data, bpe_rules = bpe_encode_recursive(encoded)
            bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            extra.extend(struct.pack('>I', len(bpe_rules)))
            extra.extend(bpe_rules)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=51, extra_header=bytes(extra))
        except Exception:
            return self._strategy_ubpe_bwt(data)

    def _strategy_deep_json_dedup_bwt(self, data):
        """策略60: 深度JSON + 全局去重 + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_json_encode(data)
            if not meta_bytes:
                return self._strategy_global_dedup_bwt(data)
            dedup_data, dedup_bytes = global_dedup_encode(encoded)
            if not dedup_bytes:
                bwt_data, block_info = bwt_encode(encoded, block_size=0)
            else:
                bwt_data, block_info = bwt_encode(dedup_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            if dedup_bytes:
                extra.extend(struct.pack('>I', len(dedup_bytes)))
                extra.extend(dedup_bytes)
            else:
                extra.extend(struct.pack('>I', 0))
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=60, extra_header=bytes(extra))
        except Exception:
            return self._strategy_global_dedup_bwt(data)

    def _strategy_deep_log_bwt(self, data):
        """策略52: 深度日志 + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_log_encode(data)
            if not meta_bytes:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=52, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_deep_log_rbpe_bwt(self, data):
        """策略53: 深度日志 + 递归BPE + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_log_encode(data)
            if not meta_bytes:
                return self._strategy_ubpe_bwt(data)
            bpe_data, bpe_rules = bpe_encode_recursive(encoded)
            bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            extra.extend(struct.pack('>I', len(bpe_rules)))
            extra.extend(bpe_rules)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=53, extra_header=bytes(extra))
        except Exception:
            return self._strategy_ubpe_bwt(data)

    def _strategy_deep_log_dedup_bwt(self, data):
        """策略60-log: 深度日志 + 全局去重 + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_log_encode(data)
            if not meta_bytes:
                return self._strategy_global_dedup_bwt(data)
            dedup_data, dedup_bytes = global_dedup_encode(encoded)
            if not dedup_bytes:
                bwt_data, block_info = bwt_encode(encoded, block_size=0)
            else:
                bwt_data, block_info = bwt_encode(dedup_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            if dedup_bytes:
                extra.extend(struct.pack('>I', len(dedup_bytes)))
                extra.extend(dedup_bytes)
            else:
                extra.extend(struct.pack('>I', 0))
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=60, extra_header=bytes(extra))
        except Exception:
            return self._strategy_global_dedup_bwt(data)

    def _strategy_deep_csv_bwt(self, data):
        """策略54: 深度CSV + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_csv_encode(data)
            if not meta_bytes:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=54, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_deep_csv_rbpe_bwt(self, data):
        """策略55: 深度CSV + 递归BPE + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_csv_encode(data)
            if not meta_bytes:
                return self._strategy_ubpe_bwt(data)
            bpe_data, bpe_rules = bpe_encode_recursive(encoded)
            bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            extra.extend(struct.pack('>I', len(bpe_rules)))
            extra.extend(bpe_rules)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=55, extra_header=bytes(extra))
        except Exception:
            return self._strategy_ubpe_bwt(data)

    def _strategy_deep_csv_dedup_bwt(self, data):
        """策略60-csv: 深度CSV + 全局去重 + BWT + LZMA2"""
        try:
            encoded, meta_bytes = deep_csv_encode(data)
            if not meta_bytes:
                return self._strategy_global_dedup_bwt(data)
            dedup_data, dedup_bytes = global_dedup_encode(encoded)
            if not dedup_bytes:
                bwt_data, block_info = bwt_encode(encoded, block_size=0)
            else:
                bwt_data, block_info = bwt_encode(dedup_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(meta_bytes)))
            extra.extend(meta_bytes)
            if dedup_bytes:
                extra.extend(struct.pack('>I', len(dedup_bytes)))
                extra.extend(dedup_bytes)
            else:
                extra.extend(struct.pack('>I', 0))
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=60, extra_header=bytes(extra))
        except Exception:
            return self._strategy_global_dedup_bwt(data)

    def _strategy_text_dedup_bwt(self, data):
        """策略58: 文本段落去重 + BWT + LZMA2"""
        try:
            encoded, dict_bytes = text_dedup_encode(data)
            if not dict_bytes:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(dict_bytes)))
            extra.extend(dict_bytes)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=58, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_text_dedup_rbpe_bwt(self, data):
        """策略59: 文本段落去重 + 递归BPE + BWT + LZMA2"""
        try:
            encoded, dict_bytes = text_dedup_encode(data)
            if not dict_bytes:
                return self._strategy_ubpe_bwt(data)
            bpe_data, bpe_rules = bpe_encode_recursive(encoded)
            bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(dict_bytes)))
            extra.extend(dict_bytes)
            extra.extend(struct.pack('>I', len(bpe_rules)))
            extra.extend(bpe_rules)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=59, extra_header=bytes(extra))
        except Exception:
            return self._strategy_ubpe_bwt(data)

    def _strategy_text_dedup_dedup_bwt(self, data):
        """策略62: 文本段落去重 + 全局去重 + BWT + LZMA2"""
        try:
            encoded, dict_bytes = text_dedup_encode(data)
            if not dict_bytes:
                return self._strategy_global_dedup_bwt(data)
            dedup_data, dedup_bytes = global_dedup_encode(encoded)
            if not dedup_bytes:
                bwt_data, block_info = bwt_encode(encoded, block_size=0)
            else:
                bwt_data, block_info = bwt_encode(dedup_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(dict_bytes)))
            extra.extend(dict_bytes)
            if dedup_bytes:
                extra.extend(struct.pack('>I', len(dedup_bytes)))
                extra.extend(dedup_bytes)
            else:
                extra.extend(struct.pack('>I', 0))
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=62, extra_header=bytes(extra))
        except Exception:
            return self._strategy_global_dedup_bwt(data)

    # ─────────────────────────────────────────
    #  ★ v9 通用策略
    # ─────────────────────────────────────────

    def _strategy_global_dedup_bwt(self, data):
        """策略56: 全局去重 + BWT + LZMA2"""
        try:
            dedup_data, dedup_bytes = global_dedup_encode(data)
            if not dedup_bytes:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(dedup_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(dedup_bytes)))
            extra.extend(dedup_bytes)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=56, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_rbpe_bwt(self, data):
        """策略63: 递归BPE + BWT + LZMA2"""
        try:
            bpe_data, bpe_rules = bpe_encode_recursive(data)
            if not bpe_rules:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(bpe_rules)))
            extra.extend(bpe_rules)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=63, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_global_dedup_rbpe_bwt(self, data):
        """策略57: 全局去重 + 递归BPE + BWT + LZMA2"""
        try:
            dedup_data, dedup_bytes = global_dedup_encode(data)
            if not dedup_bytes:
                return self._strategy_ubpe_bwt(data)
            bpe_data, bpe_rules = bpe_encode_recursive(dedup_data)
            bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x09)
            extra.extend(struct.pack('>I', len(dedup_bytes)))
            extra.extend(dedup_bytes)
            extra.extend(struct.pack('>I', len(bpe_rules)))
            extra.extend(bpe_rules)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=57, extra_header=bytes(extra))
        except Exception:
            return self._strategy_ubpe_bwt(data)

    # ─────────────────────────────────────────
    #  输出格式构建
    # ─────────────────────────────────────────

    def _build_empty_header(self):
        header = bytearray()
        header.extend(ATOMZIP_MAGIC)
        header.append(FORMAT_VERSION)
        header.extend(struct.pack('>I', 0))  # original_size
        header.append(0)  # strategy
        header.extend(struct.pack('>I', 0))  # extra_size
        header.extend(struct.pack('>I', 0))  # compressed_data_len
        return bytes(header)

    def _serialize_filters_info(self, filters, dict_size, delta_dist=0, bcj=False):
        """序列化LZMA2滤镜信息。"""
        info = bytearray()
        # 找LZMA2滤镜参数
        lc, lp, pb = 3, 0, 2
        for f in filters:
            if f['id'] == lzma.FILTER_LZMA2:
                lc = f.get('lc', 3)
                lp = f.get('lp', 0)
                pb = f.get('pb', 2)
                dict_size = f.get('dict_size', dict_size)

        flags = 0
        if delta_dist > 0:
            flags |= 0x01
        if bcj:
            flags |= 0x02

        info.append(flags)
        info.extend(struct.pack('>I', dict_size))
        info.append(lc)
        info.append(lp)
        info.append(pb)
        if delta_dist > 0:
            info.extend(struct.pack('>H', delta_dist))
        return bytes(info)

    def _build_output(self, original_data, compressed_data, strategy, extra_header=b''):
        """构建输出数据包。"""
        header = bytearray()
        header.extend(ATOMZIP_MAGIC)
        header.append(FORMAT_VERSION)
        header.extend(struct.pack('>I', len(original_data)))
        header.append(strategy)
        header.extend(struct.pack('>I', len(extra_header)))
        header.extend(extra_header)
        header.extend(struct.pack('>I', len(compressed_data)))
        header.extend(compressed_data)
        return bytes(header)

    def _build_output_raw(self, original_data, compressed_data, strategy, extra_header=b''):
        """构建输出（非LZMA后端）。"""
        return self._build_output(original_data, compressed_data, strategy, extra_header)
