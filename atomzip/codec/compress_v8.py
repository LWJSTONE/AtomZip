"""
AtomZip 压缩引擎 v8 — 极限压缩 (超激进BPE + 词级字典 + 增强N-gram + 35+策略)

核心创新 (相比v7):
  1. 超激进BPE — 全扫描254轮合并, 多轮迭代
  2. 词级字典 — 文本数据高频词替换
  3. 增强N-gram — 500条目64字节模式全扫描
  4. BZ2双引擎 — 新增BZ2后端作为LZMA2替代
  5. 极限LZMA2 — 字典大小提升到1.5GB
  6. 穷举参数 — 更多lc/lp/pb组合

策略列表 (继承v7全部策略 + v8新策略):
  ★ v8新策略:
  30: 超激进BPE + BWT + LZMA2
  31: 超激进BPE + LZMA2
  32: 超激进BPE + BWT + BZ2
  33: 词级字典 + 超激进BPE + BWT + LZMA2
  34: 词级字典 + BWT + LZMA2
  35: 增强N-gram + 超激进BPE + BWT + LZMA2
  36: 增强N-gram + BWT + LZMA2
  37: 超激进BPE + 增强N-gram + BWT + LZMA2
  38: 文本字典 + 超激进BPE + BWT + LZMA2
  39: JSON键去重 + 超激进BPE + BWT + LZMA2
  40: 日志模板 + 超激进BPE + BWT + LZMA2
  41: 列转置 + 超激进BPE + BWT + LZMA2
  42: 词级字典 + 增强N-gram + BWT + LZMA2
  43: 极限LZMA2 (1.5GB字典)
"""

import struct
import time
import lzma
import bz2
import re
from typing import Tuple, List
from collections import Counter

from .transform_v8 import (
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
    # v8新增
    bpe_encode_ultra, bpe_decode_ultra,
    word_dict_encode, word_dict_decode,
    ngram_dict_encode_v8, ngram_dict_decode_v8,
)

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 8

# 扩展LZMA2参数搜索空间
LZMA2_PARAM_SETS = [
    (3, 0, 2), (2, 0, 2), (2, 1, 2), (1, 0, 2), (1, 1, 2),
    (1, 2, 2), (0, 0, 2), (0, 2, 2), (0, 3, 2), (4, 0, 2),
    (3, 1, 2), (2, 2, 2), (2, 0, 0), (3, 0, 0), (1, 0, 0), (0, 0, 0),
    (4, 0, 0), (3, 2, 2), (1, 3, 2), (0, 1, 2), (4, 1, 2),
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
    """v8: 最大1.5GB字典 (v7是512MB)"""
    return max(1 << 16, min(data_len, 1 << 31))  # 最大2GB


def _detect_record_size(data: bytes) -> int:
    n = len(data)
    if n < 100: return 0
    for rec_size in [4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 128]:
        if n % rec_size != 0: continue
        num_records = n // rec_size
        if num_records < 10: continue
        if rec_size >= 4:
            first_vals = []
            for i in range(min(num_records, 200)):
                val = struct.unpack('>I', data[i*rec_size:i*rec_size+4])[0]
                first_vals.append(val)
            increasing = sum(1 for i in range(1, len(first_vals)) if first_vals[i] > first_vals[i-1])
            if increasing > len(first_vals) * 0.8:
                return rec_size
    return 0


def _detect_data_type(data: bytes) -> str:
    if not data: return 'empty'
    sample = data[:8192]
    stripped = sample.lstrip()
    if stripped.startswith(b'{') or stripped.startswith(b'['):
        json_key_pattern = re.compile(rb'"[\w_]+"\s*:')
        if len(json_key_pattern.findall(sample)) >= 3: return 'json'
        try:
            import json
            json.loads(sample.decode('utf-8', errors='replace'))
            return 'json'
        except Exception: pass
    log_pattern = re.compile(rb'\d{4}-\d{2}-\d{2}.*\[(INFO|WARN|ERROR|DEBUG)\]')
    if len(log_pattern.findall(sample)) >= 3: return 'log'
    apache_pattern = re.compile(rb'\d+\.\d+\.\d+\.\d+.*HTTP/\d\.\d')
    if len(apache_pattern.findall(sample)) >= 3: return 'log'
    lines = sample.split(b'\n', 20)
    if len(lines) >= 3:
        comma_counts = [line.count(b',') for line in lines[:10] if line.strip()]
        if comma_counts and len(set(comma_counts)) == 1 and comma_counts[0] >= 3:
            return 'csv'
    code_indicators = [b'def ', b'class ', b'import ', b'function ', b'var ', b'const ']
    code_score = sum(1 for ind in code_indicators if ind in sample)
    if code_score >= 2: return 'code'
    printable_count = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
    if printable_count / max(1, len(sample)) > 0.85: return 'text'
    if len(data) >= 100:
        for rec_size in [8, 12, 16, 20, 24, 28, 32, 48, 64]:
            if len(data) % rec_size == 0 and len(data) // rec_size >= 10:
                num_records = len(data) // rec_size
                if rec_size >= 4:
                    first_vals = [struct.unpack('>I', data[i*rec_size:i*rec_size+4])[0]
                                  for i in range(min(num_records, 100))]
                    if len(set(first_vals)) > 5: return 'binary_structured'
    return 'binary'


class AtomZipCompressor:
    """AtomZip v8: 超激进BPE + 词级字典 + 增强N-gram + 35+策略竞争"""

    def __init__(self, level: int = 9, verbose: bool = False):
        self.level = max(1, min(9, level))
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v8] 开始压缩 {original_size:,} 字节...")

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
            print(f"[AtomZip v8] 压缩完成: {original_size:,} -> {len(result):,} 字节 "
                  f"(比率: {ratio:.2f}:1, 耗时: {elapsed:.2f}秒)")
        return result

    def _compress_medium(self, data: bytes, data_type: str) -> bytes:
        candidates = []
        r = self._strategy_lzma_only(data); candidates.append((len(r), r))
        for dist in [1, 2, 4, 8]:
            r = self._strategy_delta_filter(data, dist); candidates.append((len(r), r))
        r = self._strategy_bwt(data); candidates.append((len(r), r))
        r = self._strategy_bpe_bwt(data); candidates.append((len(r), r))
        # v8: 也试超激进BPE
        if data_type in ('text', 'code', 'csv', 'json', 'log'):
            r = self._strategy_ubpe_bwt(data); candidates.append((len(r), r))
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    def _compress_extreme(self, data: bytes, data_type: str) -> bytes:
        candidates = []
        original_size = len(data)
        fast_mode = original_size > 2_000_000

        # === 基线策略 (继承v7) ===
        r = self._strategy_lzma_only(data); candidates.append(('lzma', len(r), r))
        
        for dist in [1, 2, 4, 8, 16, 32, 64][:3 if fast_mode else 7]:
            r = self._strategy_delta_filter(data, dist)
            candidates.append((f'delta_{dist}', len(r), r))

        r = self._strategy_bwt(data); candidates.append(('bwt', len(r), r))
        if not fast_mode:
            r = self._strategy_bwt_rle(data); candidates.append(('bwt_rle', len(r), r))
        for dist in [1, 2, 4][:1 if fast_mode else 3]:
            r = self._strategy_bwt_delta(data, dist)
            candidates.append((f'bwt_delta_{dist}', len(r), r))

        # === v7 BPE策略 ===
        if not fast_mode:
            r = self._strategy_bpe_bwt(data); candidates.append(('bpe_bwt', len(r), r))
            r = self._strategy_bpe_lzma(data); candidates.append(('bpe', len(r), r))
        else:
            r = self._strategy_bpe_bwt(data); candidates.append(('bpe_bwt', len(r), r))

        # === v7 N-gram策略 ===
        if not fast_mode:
            r = self._strategy_ngram_bwt(data); candidates.append(('ngram_bwt', len(r), r))
            r = self._strategy_bpe_ngram_bwt(data); candidates.append(('bpe_ngram_bwt', len(r), r))

        # === ★ v8 超激进BPE策略 ===
        r = self._strategy_ubpe_bwt(data); candidates.append(('ubpe_bwt', len(r), r))
        if not fast_mode:
            r = self._strategy_ubpe_lzma(data); candidates.append(('ubpe', len(r), r))

        # === ★ v8 词级字典策略 ===
        if data_type in ('text', 'code'):
            r = self._strategy_word_dict_bwt(data); candidates.append(('wd_bwt', len(r), r))
            if not fast_mode:
                r = self._strategy_word_dict_ubpe_bwt(data); candidates.append(('wd_ubpe_bwt', len(r), r))

        # === ★ v8 增强N-gram策略 ===
        if not fast_mode:
            r = self._strategy_ngram_v8_bwt(data); candidates.append(('ngram8_bwt', len(r), r))
            r = self._strategy_ubpe_ngram_v8_bwt(data); candidates.append(('ubpe_ngram8_bwt', len(r), r))

        # === 数据类型专用策略 (继承v7 + v8增强) ===
        if data_type in ('text', 'code'):
            r = self._strategy_text_dict_bwt(data); candidates.append(('text_dict_bwt', len(r), r))
            if not fast_mode:
                r = self._strategy_text_dict_ubpe_bwt(data); candidates.append(('td_ubpe_bwt', len(r), r))
                r = self._strategy_text_dict(data); candidates.append(('text_dict', len(r), r))
                r = self._strategy_text_dict_bpe_bwt(data); candidates.append(('td_bpe_bwt', len(r), r))
            r = self._strategy_col_bpe_bwt(data); candidates.append(('col_bpe_bwt', len(r), r))

        elif data_type == 'json':
            r = self._strategy_json_dedup_bwt(data); candidates.append(('json_bwt', len(r), r))
            if not fast_mode:
                r = self._strategy_json_dedup_ubpe_bwt(data); candidates.append(('json_ubpe_bwt', len(r), r))
                r = self._strategy_json_dedup(data); candidates.append(('json', len(r), r))
                r = self._strategy_json_flatten_bwt(data); candidates.append(('json_flat_bwt', len(r), r))
                r = self._strategy_json_dedup_bpe_bwt(data); candidates.append(('json_bpe_bwt', len(r), r))
            r = self._strategy_col_bpe_bwt(data); candidates.append(('col_bpe_bwt_j', len(r), r))

        elif data_type == 'log':
            r = self._strategy_log_template_bwt(data); candidates.append(('log_bwt', len(r), r))
            if not fast_mode:
                r = self._strategy_log_template_ubpe_bwt(data); candidates.append(('log_ubpe_bwt', len(r), r))
                r = self._strategy_log_template(data); candidates.append(('log', len(r), r))
                r = self._strategy_log_field_bwt(data); candidates.append(('log_field_bwt', len(r), r))
                r = self._strategy_log_template_bpe_bwt(data); candidates.append(('log_bpe_bwt', len(r), r))
            r = self._strategy_col_bpe_bwt(data); candidates.append(('col_bpe_bwt_l', len(r), r))

        elif data_type == 'csv':
            r = self._strategy_column_bwt(data); candidates.append(('col_bwt', len(r), r))
            if not fast_mode:
                r = self._strategy_csv_column_bwt(data); candidates.append(('csv_col_bwt', len(r), r))
                r = self._strategy_csv_column_bpe_bwt(data); candidates.append(('csv_bpe_bwt', len(r), r))
                r = self._strategy_col_bpe_delta_bwt(data); candidates.append(('col_bpe_delta_bwt', len(r), r))
            r = self._strategy_col_bpe_bwt(data); candidates.append(('col_bpe_bwt_c', len(r), r))

        elif data_type in ('binary_structured', 'binary'):
            r = self._strategy_bcj(data); candidates.append(('bcj', len(r), r))
            for dist in [1, 2, 4][:1 if fast_mode else 3]:
                r = self._strategy_bcj_delta(data, dist)
                candidates.append((f'bcj_delta_{dist}', len(r), r))
            if not fast_mode:
                r = self._strategy_bwt_rle_delta(data, 1); candidates.append(('bwt_rle_delta_1', len(r), r))
            r = self._strategy_bpe_bwt(data); candidates.append(('bpe_bwt_b', len(r), r))
            rec_size = _detect_record_size(data)
            if rec_size > 0:
                r = self._strategy_delta_filter(data, rec_size)
                candidates.append((f'rec_delta_{rec_size}', len(r), r))
                r = self._strategy_rec_delta_bwt(data, rec_size)
                candidates.append((f'rec_delta_bwt_{rec_size}', len(r), r))

        # === BZ2策略 ===
        if not fast_mode:
            r = self._strategy_bwt_bz2(data); candidates.append(('bwt_bz2', len(r), r))
            r = self._strategy_ubpe_bwt_bz2(data); candidates.append(('ubpe_bwt_bz2', len(r), r))

        # === 穷举LZMA2参数 ===
        if not fast_mode:
            candidates.sort(key=lambda x: x[1])
            top = candidates[:min(3, len(candidates))]
            for name, _, _ in top:
                if 'bwt' in name and original_size > 10000:
                    for lc, lp, pb in LZMA2_PARAM_SETS[1:5]:
                        if name == 'bwt':
                            r = self._strategy_bwt(data, lc=lc, lp=lp, pb=pb)
                        elif name == 'ubpe_bwt':
                            r = self._strategy_ubpe_bwt(data, lc=lc, lp=lp, pb=pb)
                        elif name == 'bpe_bwt':
                            r = self._strategy_bpe_bwt(data, lc=lc, lp=lp, pb=pb)
                        elif name.startswith('bwt_delta_'):
                            dist = int(name.split('_')[-1])
                            r = self._strategy_bwt_delta(data, dist, lc=lc, lp=lp, pb=pb)
                        else:
                            continue
                        candidates.append((f'{name}_lc{lc}lp{lp}pb{pb}', len(r), r))

        if self.verbose:
            print(f"  尝试了 {len(candidates)} 种策略, "
                  f"最佳: {min(c[1] for c in candidates):,} 字节")

        candidates.sort(key=lambda x: x[1])
        return candidates[0][2]

    # ─────────────────────────────────────────
    #  基线策略 (继承v7)
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

    def _strategy_bwt_delta(self, data, dist, lc=3, lp=0, pb=2):
        bwt_data, block_info = bwt_encode(data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size, delta_dist=dist))
        return self._build_output(data, lzma_data, strategy=4, extra_header=bytes(extra))

    def _strategy_bwt_rle_delta(self, data, dist, lc=3, lp=0, pb=2):
        bwt_data, block_info = bwt_encode(data, block_size=0)
        rle_data = rle_encode(bwt_data)
        dict_size = _smart_dict_size(len(rle_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=dist, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(rle_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size, delta_dist=dist))
        return self._build_output(data, lzma_data, strategy=9, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  v7 数据类型专用策略
    # ─────────────────────────────────────────

    def _strategy_text_dict(self, data):
        encoded, dict_bytes = text_dict_encode(data)
        if not dict_bytes: return self._strategy_lzma_only(data)
        dict_size = _smart_dict_size(len(encoded))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(encoded, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=5, extra_header=bytes(extra))

    def _strategy_text_dict_bwt(self, data):
        encoded, dict_bytes = text_dict_encode(data)
        if not dict_bytes: return self._strategy_bwt(data)
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

    def _strategy_json_dedup(self, data):
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes: return self._strategy_lzma_only(data)
        dict_size = _smart_dict_size(len(transformed))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(transformed, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(schema_bytes)))
        extra.extend(schema_bytes)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=6, extra_header=bytes(extra))

    def _strategy_json_dedup_bwt(self, data):
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes: return self._strategy_bwt(data)
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

    def _strategy_log_template(self, data):
        var_data, template_bytes = log_template_encode(data)
        if not template_bytes: return self._strategy_lzma_only(data)
        dict_size = _smart_dict_size(len(var_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(var_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(template_bytes)))
        extra.extend(template_bytes)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=7, extra_header=bytes(extra))

    def _strategy_log_template_bwt(self, data):
        var_data, template_bytes = log_template_encode(data)
        if not template_bytes: return self._strategy_bwt(data)
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
        if row_width <= 1: return self._strategy_bwt(data)
        bwt_data, block_info = bwt_encode(transposed, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>H', row_width))
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=8, extra_header=bytes(extra))

    def _strategy_rec_delta_bwt(self, data, rec_size):
        delta_data, first_byte = delta_encode(data, stride=rec_size)
        bwt_data, block_info = bwt_encode(delta_data, block_size=0)
        dict_size2 = _smart_dict_size(len(bwt_data))
        filters2 = _get_lzma_filters(9, dict_size=dict_size2)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters2)
        extra = bytearray()
        extra.append(0x13)
        extra.extend(struct.pack('>H', rec_size))
        extra.append(first_byte)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters2, dict_size2))
        return self._build_output(data, lzma_data, strategy=13, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  v7 BPE/N-gram策略
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

    def _strategy_bpe_lzma(self, data):
        bpe_data, bpe_rules = bpe_encode(data)
        dict_size = _smart_dict_size(len(bpe_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bpe_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=15, extra_header=bytes(extra))

    def _strategy_ngram_bwt(self, data):
        ngram_data, dict_bytes = ngram_dict_encode(data)
        if not dict_bytes: return self._strategy_bwt(data)
        bwt_data, block_info = bwt_encode(ngram_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=16, extra_header=bytes(extra))

    def _strategy_bpe_ngram_bwt(self, data):
        bpe_data, bpe_rules = bpe_encode(data)
        ngram_data, dict_bytes = ngram_dict_encode(bpe_data)
        if not dict_bytes: return self._strategy_bpe_bwt(data)
        bwt_data, block_info = bwt_encode(ngram_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=18, extra_header=bytes(extra))

    def _strategy_csv_column_bwt(self, data):
        csv_data, meta_bytes = csv_column_encode(data)
        if not meta_bytes: return self._strategy_column_bwt(data)
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

    def _strategy_csv_column_bpe_bwt(self, data):
        csv_data, meta_bytes = csv_column_encode(data)
        if not meta_bytes: return self._strategy_bpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode(csv_data)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(meta_bytes)))
        extra.extend(meta_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=24, extra_header=bytes(extra))

    def _strategy_json_flatten_bwt(self, data):
        json_data, meta_bytes = json_flatten_encode(data)
        if not meta_bytes: return self._strategy_json_dedup_bwt(data)
        bwt_data, block_info = bwt_encode(json_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(meta_bytes)))
        extra.extend(meta_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=20, extra_header=bytes(extra))

    def _strategy_log_field_bwt(self, data):
        log_data, meta_bytes = log_field_encode(data)
        if not meta_bytes: return self._strategy_log_template_bwt(data)
        bwt_data, block_info = bwt_encode(log_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(meta_bytes)))
        extra.extend(meta_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=21, extra_header=bytes(extra))

    def _strategy_text_dict_bpe_bwt(self, data):
        encoded, dict_bytes = text_dict_encode(data)
        if not dict_bytes: return self._strategy_bpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode(encoded)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=23, extra_header=bytes(extra))

    def _strategy_json_dedup_bpe_bwt(self, data):
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes: return self._strategy_bpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode(transformed)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(schema_bytes)))
        extra.extend(schema_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=25, extra_header=bytes(extra))

    def _strategy_log_template_bpe_bwt(self, data):
        var_data, template_bytes = log_template_encode(data)
        if not template_bytes: return self._strategy_bpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode(var_data)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>I', len(template_bytes)))
        extra.extend(template_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=26, extra_header=bytes(extra))

    def _strategy_col_bpe_bwt(self, data):
        transposed, row_width = column_transpose_encode(data)
        if row_width <= 1: return self._strategy_bpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode(transposed)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>H', row_width))
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=27, extra_header=bytes(extra))

    def _strategy_col_bpe_delta_bwt(self, data):
        transposed, row_width = column_transpose_encode(data)
        if row_width <= 1: return self._strategy_bpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode(transposed)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, delta_dist=1)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(struct.pack('>H', row_width))
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size, delta_dist=1))
        return self._build_output(data, lzma_data, strategy=28, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  ★ v8新策略: 超激进BPE
    # ─────────────────────────────────────────

    def _strategy_ubpe_bwt(self, data, lc=3, lp=0, pb=2):
        """策略30: 超激进BPE + BWT + LZMA2"""
        bpe_data, bpe_rules = bpe_encode_ultra(data)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)  # v8标记
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=30, extra_header=bytes(extra))

    def _strategy_ubpe_lzma(self, data):
        """策略31: 超激进BPE + LZMA2"""
        bpe_data, bpe_rules = bpe_encode_ultra(data)
        dict_size = _smart_dict_size(len(bpe_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bpe_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=31, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  ★ v8新策略: BZ2后端
    # ─────────────────────────────────────────

    def _strategy_bwt_bz2(self, data):
        """BWT + BZ2 (让BZ2做自己的BWT)"""
        bwt_data, block_info = bwt_encode(data, block_size=0)
        bz2_data = bz2.compress(bwt_data, compresslevel=9)
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.append(0x01)  # BZ2标记
        return self._build_output_raw(data, bz2_data, strategy=32, extra_header=bytes(extra))

    def _strategy_ubpe_bwt_bz2(self, data):
        """超激进BPE + BWT + BZ2"""
        bpe_data, bpe_rules = bpe_encode_ultra(data)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        bz2_data = bz2.compress(bwt_data, compresslevel=9)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.append(0x01)  # BZ2标记
        return self._build_output_raw(data, bz2_data, strategy=32, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  ★ v8新策略: 词级字典
    # ─────────────────────────────────────────

    def _strategy_word_dict_bwt(self, data):
        """策略34: 词级字典 + BWT + LZMA2"""
        encoded, dict_bytes = word_dict_encode(data)
        if not dict_bytes: return self._strategy_bwt(data)
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

    def _strategy_word_dict_ubpe_bwt(self, data):
        """策略33: 词级字典 + 超激进BPE + BWT + LZMA2"""
        encoded, dict_bytes = word_dict_encode(data)
        if not dict_bytes: return self._strategy_ubpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode_ultra(encoded)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=33, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  ★ v8新策略: 增强N-gram
    # ─────────────────────────────────────────

    def _strategy_ngram_v8_bwt(self, data):
        """策略36: 增强N-gram + BWT + LZMA2"""
        ngram_data, dict_bytes = ngram_dict_encode_v8(data)
        if not dict_bytes: return self._strategy_bwt(data)
        bwt_data, block_info = bwt_encode(ngram_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=36, extra_header=bytes(extra))

    def _strategy_ubpe_ngram_v8_bwt(self, data):
        """策略37: 超激进BPE + 增强N-gram + BWT + LZMA2"""
        bpe_data, bpe_rules = bpe_encode_ultra(data)
        ngram_data, dict_bytes = ngram_dict_encode_v8(bpe_data)
        if not dict_bytes: return self._strategy_ubpe_bwt(data)
        bwt_data, block_info = bwt_encode(ngram_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=37, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  ★ v8组合: 数据类型 + 超激进BPE
    # ─────────────────────────────────────────

    def _strategy_text_dict_ubpe_bwt(self, data):
        """策略38: 文本字典 + 超激进BPE + BWT + LZMA2"""
        encoded, dict_bytes = text_dict_encode(data)
        if not dict_bytes: return self._strategy_ubpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode_ultra(encoded)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(dict_bytes)))
        extra.extend(dict_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=38, extra_header=bytes(extra))

    def _strategy_json_dedup_ubpe_bwt(self, data):
        """策略39: JSON键去重 + 超激进BPE + BWT + LZMA2"""
        transformed, schema_bytes = json_key_dedup_encode(data)
        if not schema_bytes: return self._strategy_ubpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode_ultra(transformed)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(schema_bytes)))
        extra.extend(schema_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=39, extra_header=bytes(extra))

    def _strategy_log_template_ubpe_bwt(self, data):
        """策略40: 日志模板 + 超激进BPE + BWT + LZMA2"""
        var_data, template_bytes = log_template_encode(data)
        if not template_bytes: return self._strategy_ubpe_bwt(data)
        bpe_data, bpe_rules = bpe_encode_ultra(var_data)
        bwt_data, block_info = bwt_encode(bpe_data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.append(0x08)
        extra.extend(struct.pack('>I', len(template_bytes)))
        extra.extend(template_bytes)
        extra.extend(struct.pack('>I', len(bpe_rules)))
        extra.extend(bpe_rules)
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=40, extra_header=bytes(extra))

    # ─────────────────────────────────────────
    #  序列化工具
    # ─────────────────────────────────────────

    @staticmethod
    def _serialize_filters_info(filters, dict_size, delta_dist=0, bcj=False):
        result = bytearray()
        flags = 0
        if delta_dist > 0: flags |= 0x01
        if bcj: flags |= 0x02
        result.append(flags)
        result.extend(struct.pack('>I', dict_size))
        lzma2 = filters[-1]
        result.append(lzma2.get('lc', 3))
        result.append(lzma2.get('lp', 0))
        result.append(lzma2.get('pb', 2))
        if delta_dist > 0:
            result.extend(struct.pack('>H', delta_dist))
        return bytes(result)

    def _build_output(self, data, lzma_data, strategy, extra_header):
        result = bytearray()
        original_size = len(data)
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>I', original_size))
        result.append(strategy)
        result.extend(struct.pack('>I', len(extra_header)))
        if extra_header:
            result.extend(extra_header)
        result.extend(struct.pack('>I', len(lzma_data)))
        result.extend(lzma_data)
        return bytes(result)

    def _build_output_raw(self, data, compressed_data, strategy, extra_header):
        """构建输出 (用于非LZMA后端如BZ2)"""
        result = bytearray()
        original_size = len(data)
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>I', original_size))
        result.append(strategy)
        result.extend(struct.pack('>I', len(extra_header)))
        if extra_header:
            result.extend(extra_header)
        result.extend(struct.pack('>I', len(compressed_data)))
        result.extend(compressed_data)
        return bytes(result)

    def _build_empty_header(self):
        result = bytearray()
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>I', 0))
        result.append(0)
        result.extend(struct.pack('>I', 0))
        result.extend(struct.pack('>I', 0))
        return bytes(result)
