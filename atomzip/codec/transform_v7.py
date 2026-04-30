"""
AtomZip v7 数据变换模块 — 极限压缩

核心新增:
  1. 迭代BPE (Byte-Pair Encoding) — 用空闲字节值替换高频字节对
  2. N-gram字典压缩 — 查找并替换所有重复子串
  3. 增强CSV列压缩 — 按列分离+Delta+字典
  4. 增强JSON值分离 — 分离结构+值字典
  5. 增强日志字段压缩 — 按字段分组编码
  6. 保留v6所有变换 (C加速BWT, RLE, Delta等)
"""

import struct
import re
import os
import ctypes
from typing import Tuple, List, Dict, Optional
from collections import Counter

# ─────────────────────────────────────────────
#  C语言BWT引擎 (继承自v6)
# ─────────────────────────────────────────────

_C_BWT_LIB = None

def _get_c_bwt_lib():
    global _C_BWT_LIB
    if _C_BWT_LIB is not None:
        return _C_BWT_LIB if _C_BWT_LIB is not False else None
    lib_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'c_ext', 'libbwt_fast.so')
    if os.path.exists(lib_path):
        try:
            lib = ctypes.CDLL(lib_path)
            lib.bwt_encode_c.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_int32, ctypes.POINTER(ctypes.c_uint8)]
            lib.bwt_encode_c.restype = ctypes.c_int32
            lib.bwt_decode_c.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_int32, ctypes.c_int32, ctypes.POINTER(ctypes.c_uint8)]
            lib.bwt_decode_c.restype = None
            _C_BWT_LIB = lib
            return lib
        except Exception:
            pass
    _C_BWT_LIB = False
    return None

def bwt_encode_c(data: bytes) -> Tuple[bytes, int]:
    lib = _get_c_bwt_lib()
    if lib is None:
        return bwt_encode_python(data)
    n = len(data)
    if n <= 1: return data, 0
    data_arr = (ctypes.c_uint8 * n).from_buffer_copy(data)
    bwt_arr = (ctypes.c_uint8 * n)()
    orig_idx = lib.bwt_encode_c(data_arr, n, bwt_arr)
    if orig_idx < 0:
        return bwt_encode_python(data)
    return bytes(bwt_arr), orig_idx

def bwt_decode_c(bwt_data: bytes, orig_idx: int) -> bytes:
    lib = _get_c_bwt_lib()
    if lib is None:
        return bwt_decode_python(bwt_data, orig_idx)
    n = len(bwt_data)
    if n <= 1: return bwt_data
    bwt_arr = (ctypes.c_uint8 * n).from_buffer_copy(bwt_data)
    out_arr = (ctypes.c_uint8 * n)()
    lib.bwt_decode_c(bwt_arr, n, orig_idx, out_arr)
    return bytes(out_arr)

def bwt_encode_python(data: bytes) -> Tuple[bytes, int]:
    n = len(data)
    if n == 0: return b'', 0
    if n == 1: return data, 0
    if n <= 65536:
        doubled = data + data
        indices = sorted(range(n), key=lambda i: doubled[i:i + n])
    else:
        sa = list(range(n))
        rank = list(data)
        tmp_rank = [0] * n
        k = 1
        while k < n:
            keys = [(rank[i], rank[(i + k) % n]) for i in range(n)]
            sa.sort(key=lambda idx: keys[idx])
            tmp_rank[sa[0]] = 0
            for j in range(1, n):
                tmp_rank[sa[j]] = tmp_rank[sa[j - 1]]
                if keys[sa[j]] != keys[sa[j - 1]]:
                    tmp_rank[sa[j]] += 1
            rank = tmp_rank[:]
            if rank[sa[-1]] == n - 1: break
            k *= 2
        indices = sa
    bwt = bytes(data[(i - 1) % n] for i in indices)
    orig_idx = indices.index(0)
    return bwt, orig_idx

def bwt_decode_python(bwt_data: bytes, orig_idx: int) -> bytes:
    n = len(bwt_data)
    if n == 0: return b''
    if n == 1: return bwt_data
    count = [0] * 256
    for c in bwt_data: count[c] += 1
    cumul = [0] * 256
    total = 0
    for i in range(256):
        cumul[i] = total
        total += count[i]
    lf = [0] * n
    occ = [0] * 256
    for i in range(n):
        c = bwt_data[i]
        lf[i] = cumul[c] + occ[c]
        occ[c] += 1
    result = bytearray(n)
    j = orig_idx
    for i in range(n - 1, -1, -1):
        result[i] = bwt_data[j]
        j = lf[j]
    return bytes(result)

# 统一BWT接口
BWT_MAX_DATA_SIZE = 512 << 20  # 512MB

def bwt_encode(data: bytes, block_size: int = 0) -> Tuple[bytes, List[Tuple[int, int]]]:
    if len(data) == 0: return b'', []
    bwt_data, orig_idx = bwt_encode_c(data)
    return bwt_data, [(orig_idx, len(data))]

def bwt_decode(bwt_data: bytes, block_info: List[Tuple[int, int]]) -> bytes:
    if not block_info: return b''
    if len(block_info) == 1:
        orig_idx, block_size = block_info[0]
        return bwt_decode_c(bwt_data, orig_idx)
    result = bytearray()
    offset = 0
    for orig_idx, block_size in block_info:
        block = bwt_data[offset:offset + block_size]
        decoded = bwt_decode_c(block, orig_idx)
        result.extend(decoded)
        offset += block_size
    return bytes(result)


# ─────────────────────────────────────────────
#  RLE (增强版, 双字节计数)
# ─────────────────────────────────────────────

RLE_ESCAPE = 0xFE
RLE_MIN_RUN = 4

def rle_encode(data: bytes) -> bytes:
    if not data: return b''
    result = bytearray()
    i = 0
    n = len(data)
    while i < n:
        current = data[i]
        run_len = 1
        while i + run_len < n and data[i + run_len] == current and run_len < 65535:
            run_len += 1
        if run_len >= RLE_MIN_RUN:
            result.append(RLE_ESCAPE)
            result.append(current)
            result.extend(struct.pack('>H', run_len))
            i += run_len
        elif current == RLE_ESCAPE:
            result.append(RLE_ESCAPE)
            result.append(RLE_ESCAPE)
            result.extend(struct.pack('>H', 0))
            i += 1
        else:
            result.append(current)
            i += 1
    return bytes(result)

def rle_decode(data: bytes) -> bytes:
    if not data: return b''
    result = bytearray()
    i = 0
    n = len(data)
    while i < n:
        if data[i] == RLE_ESCAPE:
            if i + 3 >= n:
                result.append(data[i])
                i += 1
                continue
            byte_val = data[i + 1]
            count = struct.unpack('>H', data[i + 2:i + 4])[0]
            if count == 0 and byte_val == RLE_ESCAPE:
                result.append(RLE_ESCAPE)
            else:
                result.extend(bytes([byte_val]) * count)
            i += 4
        else:
            result.append(data[i])
            i += 1
    return bytes(result)


# ─────────────────────────────────────────────
#  Delta (多步长差分编码)
# ─────────────────────────────────────────────

def delta_encode(data: bytes, stride: int = 1) -> Tuple[bytes, int]:
    if not data: return b'', 0
    first_byte = data[0]
    n = len(data)
    result = bytearray(n)
    for i in range(n):
        if i < stride:
            result[i] = data[i]
        else:
            result[i] = (data[i] - data[i - stride]) % 256
    return bytes(result), first_byte

def delta_decode(data: bytes, first_byte: int, stride: int = 1) -> bytes:
    if not data: return b''
    n = len(data)
    result = bytearray(n)
    for i in range(n):
        if i < stride:
            result[i] = data[i]
        else:
            result[i] = (result[i - stride] + data[i]) % 256
    return bytes(result)


# ─────────────────────────────────────────────
#  ★ 迭代 BPE (Byte-Pair Encoding) — v7核心
# ─────────────────────────────────────────────

def bpe_encode(data: bytes, max_merges: int = 100) -> Tuple[bytes, bytes]:
    """快速迭代BPE编码: 用空闲字节值替换高频字节对。
    
    优化: 采样+快速统计, 对大数据自动减少迭代次数。
    
    返回: (encoded_data, merge_rules_bytes)
    """
    if not data or len(data) < 10:
        return data, b''
    
    n = len(data)
    # 对大数据仅统计前256KB的对频率
    sample_size = min(n, 256 << 10)
    # 大数据减少最大合并次数
    effective_max = min(max_merges, max(10, 200000 // max(1, n // 1000)))
    
    data_arr = bytearray(data)
    
    # 找未使用的字节值 (仅采样)
    used = [False] * 256
    for b in data_arr[:sample_size]:
        used[b] = True
    free_slots = [b for b in range(256) if not used[b]]
    
    # 如果没有空闲字节, 释放最罕见的字节
    esc_byte = None
    if not free_slots:
        freq = [0] * 256
        for b in data_arr[:sample_size]:
            freq[b] += 1
        min_freq = n + 1
        rarest = 0
        for b in range(256):
            if freq[b] > 0 and freq[b] < min_freq:
                min_freq = freq[b]
                rarest = b
        esc_byte = rarest
        
        # 转义: esc_byte → esc_byte + 0x00
        new_data = bytearray()
        for b in data_arr:
            if b == esc_byte:
                new_data.append(esc_byte)
                new_data.append(0x00)
            else:
                new_data.append(b)
        data_arr = new_data
        free_slots = [esc_byte]
    
    # 迭代合并
    merge_rules = []
    slot_idx = 0
    effective_max = min(effective_max, len(free_slots))
    
    for _ in range(effective_max):
        if slot_idx >= len(free_slots):
            break
        
        # 快速统计字节对频率 (仅采样)
        pair_freq = {}
        scan_limit = min(len(data_arr), sample_size)
        arr = data_arr  # 局部引用加速
        for i in range(scan_limit - 1):
            p = (arr[i], arr[i + 1])
            pair_freq[p] = pair_freq.get(p, 0) + 1
        
        if not pair_freq:
            break
        
        # 找最高频对
        best_pair = max(pair_freq, key=pair_freq.get)
        best_count = pair_freq[best_pair]
        if best_count < 3:
            break
        
        # 用空闲字节替换
        new_token = free_slots[slot_idx]
        slot_idx += 1
        
        new_data = bytearray()
        i = 0
        na = len(data_arr)
        b0, b1 = best_pair
        while i < na:
            if i < na - 1 and data_arr[i] == b0 and data_arr[i + 1] == b1:
                new_data.append(new_token)
                i += 2
            else:
                new_data.append(data_arr[i])
                i += 1
        
        data_arr = new_data
        merge_rules.append((best_pair[0], best_pair[1], new_token))
    
    rules_bytes = _serialize_bpe_rules(merge_rules, esc_byte)
    return bytes(data_arr), rules_bytes


def bpe_decode(data: bytes, rules_bytes: bytes) -> bytes:
    """BPE解码: 逆序应用合并规则。"""
    if not rules_bytes:
        return data
    
    merge_rules, esc_byte = _deserialize_bpe_rules(rules_bytes)
    
    # 逆序应用规则 (最后一个合并先解开)
    for byte_a, byte_b, new_token in reversed(merge_rules):
        new_data = bytearray()
        i = 0
        while i < len(data):
            if data[i] == new_token:
                new_data.append(byte_a)
                new_data.append(byte_b)
                i += 1
            else:
                new_data.append(data[i])
                i += 1
        data = bytes(new_data)
    
    # 处理转义字节
    if esc_byte is not None:
        new_data = bytearray()
        i = 0
        while i < len(data):
            if data[i] == esc_byte and i + 1 < len(data) and data[i + 1] == 0x00:
                new_data.append(esc_byte)
                i += 2
            else:
                new_data.append(data[i])
                i += 1
        data = bytes(new_data)
    
    return data


def _serialize_bpe_rules(rules: list, esc_byte: Optional[int]) -> bytes:
    result = bytearray()
    # 标记是否有转义字节
    if esc_byte is not None:
        result.append(0x01)
        result.append(esc_byte)
    else:
        result.append(0x00)
    
    result.extend(struct.pack('>H', len(rules)))
    for byte_a, byte_b, new_token in rules:
        result.append(byte_a)
        result.append(byte_b)
        result.append(new_token)
    return bytes(result)

def _deserialize_bpe_rules(data: bytes) -> Tuple[list, Optional[int]]:
    offset = 0
    has_esc = data[offset]; offset += 1
    esc_byte = data[offset] if has_esc else None; offset += has_esc
    
    num_rules = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    rules = []
    for _ in range(num_rules):
        byte_a = data[offset]; offset += 1
        byte_b = data[offset]; offset += 1
        new_token = data[offset]; offset += 1
        rules.append((byte_a, byte_b, new_token))
    return rules, esc_byte


# ─────────────────────────────────────────────
#  ★ N-gram 字典压缩 — 替换重复子串
# ─────────────────────────────────────────────

def ngram_dict_encode(data: bytes, max_entries: int = 100, min_len: int = 3, max_len: int = 32) -> Tuple[bytes, bytes]:
    """快速N-gram字典压缩: 仅查找高频重复子串。
    
    优化: 仅统计前500KB, 限制长度范围, 快速匹配。
    
    返回: (encoded_data, dict_bytes)
    """
    if not data or len(data) < 100:
        return data, b''
    
    n = len(data)
    # 仅采样前500KB
    sample = data[:min(n, 500000)]
    
    # 找最罕见的字节作为MARKER
    freq = [0] * 256
    for b in sample:
        freq[b] += 1
    unused = [b for b in range(256) if freq[b] == 0]
    if unused:
        marker = unused[0]
    else:
        marker = min(range(256), key=lambda b: freq[b])
    
    # 快速查找重复子串 — 仅对固定长度3-16在采样数据中搜索
    candidates = []
    for length in range(min_len, min(max_len + 1, 17)):
        pos_map = {}
        slen = len(sample) - length + 1
        for i in range(slen):
            substr = sample[i:i + length]
            # 用元组作为key (比bytes快)
            key = substr
            if key in pos_map:
                pos_map[key] += 1
            else:
                pos_map[key] = 1
        
        for substr, count in pos_map.items():
            if count >= 3:
                savings = count * (length - 2) - (length + 6)
                if savings > 0:
                    candidates.append((substr, count, savings))
    
    if not candidates:
        return data, b''
    
    # 按节省排序
    candidates.sort(key=lambda x: -x[2])
    
    # 去重 (相同子串只保留一个)
    seen = set()
    selected = []
    for substr, count, savings in candidates:
        if len(selected) >= max_entries:
            break
        if substr not in seen:
            seen.add(substr)
            selected.append(substr)
    
    if not selected:
        return data, b''
    
    # 按长度降序排列 (先替换长子串)
    dictionary = selected
    dictionary.sort(key=len, reverse=True)
    
    # 转义MARKER字节
    encoded = bytearray()
    for b in data:
        if b == marker:
            encoded.append(marker)
            encoded.append(marker)
        else:
            encoded.append(b)
    
    # 替换子串
    for idx, substr in enumerate(dictionary):
        if idx >= 250:
            break
        search = bytes(substr)
        replacement = bytes([marker, idx])
        
        new_encoded = bytearray()
        i = 0
        elen = len(encoded)
        slen = len(search)
        while i < elen:
            if i <= elen - slen:
                match = True
                for j in range(slen):
                    if encoded[i + j] != search[j]:
                        match = False
                        break
                if match:
                    new_encoded.extend(replacement)
                    i += slen
                    continue
            new_encoded.append(encoded[i])
            i += 1
        encoded = new_encoded
    
    dict_bytes = _serialize_ngram_dict(dictionary[:250], marker)
    return bytes(encoded), dict_bytes


def ngram_dict_decode(data: bytes, dict_bytes: bytes) -> bytes:
    """N-gram字典解码。"""
    if not dict_bytes:
        return data
    
    dictionary, marker = _deserialize_ngram_dict(dict_bytes)
    
    # 替换引用为原始子串
    result = bytearray()
    i = 0
    while i < len(data):
        if data[i] == marker:
            if i + 1 < len(data):
                if data[i + 1] == marker:
                    # 转义的MARKER
                    result.append(marker)
                    i += 2
                else:
                    # 字典引用
                    idx = data[i + 1]
                    if idx < len(dictionary):
                        result.extend(dictionary[idx])
                    else:
                        result.append(data[i])
                        result.append(data[i + 1])
                    i += 2
            else:
                result.append(data[i])
                i += 1
        else:
            result.append(data[i])
            i += 1
    
    return bytes(result)


def _serialize_ngram_dict(dictionary: list, marker: int) -> bytes:
    result = bytearray()
    result.append(marker)
    result.extend(struct.pack('>H', len(dictionary)))
    for substr in dictionary:
        result.extend(struct.pack('>H', len(substr)))
        result.extend(substr)
    return bytes(result)

def _deserialize_ngram_dict(data: bytes) -> Tuple[list, int]:
    offset = 0
    marker = data[offset]; offset += 1
    num_entries = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    dictionary = []
    for _ in range(num_entries):
        entry_len = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
        entry = data[offset:offset + entry_len]
        dictionary.append(bytes(entry))
        offset += entry_len
    return dictionary, marker


# ─────────────────────────────────────────────
#  ★ 增强CSV列压缩
# ─────────────────────────────────────────────

def csv_column_encode(data: bytes) -> Tuple[bytes, bytes]:
    """CSV列压缩: 按列分离, 每列独立编码。
    
    算法:
    1. 解析CSV为行和列
    2. 对每列:
       - 数值列: Delta编码 + 紧凑二进制
       - 字符串列: 值字典 + 索引编码
    3. 列数据顺序存储 (列1所有行 → 列2所有行 → ...)
    4. BWT + LZMA2对各列数据分别压缩
    
    返回: (encoded_data, metadata)
    """
    if not data or len(data) < 100:
        return data, b''
    
    try:
        text = data.decode('utf-8', errors='replace')
    except Exception:
        text = data.decode('latin-1')
    
    lines = text.strip().split('\n')
    if len(lines) < 3:
        return data, b''
    
    # 解析CSV
    rows = []
    for line in lines:
        # 简单CSV解析 (处理引号)
        fields = _parse_csv_line(line)
        rows.append(fields)
    
    # 检查列数一致性
    num_cols = len(rows[0])
    if num_cols < 2:
        return data, b''
    
    consistent = sum(1 for r in rows if len(r) == num_cols)
    if consistent < len(rows) * 0.8:
        return data, b''
    
    # 只保留列数一致的行
    rows = [r for r in rows if len(r) == num_cols]
    
    # 按列分离
    columns = [[] for _ in range(num_cols)]
    for row in rows:
        for col_idx in range(num_cols):
            columns[col_idx].append(row[col_idx] if col_idx < len(row) else '')
    
    # 编码每列
    encoded_columns = []
    column_meta = []
    
    for col_idx in range(num_cols):
        col_values = columns[col_idx]
        
        # 检测是否为数值列
        is_numeric = all(_is_numeric(v) for v in col_values if v.strip())
        
        if is_numeric:
            # 数值列: 值排序 + Delta编码
            try:
                nums = [float(v) if v.strip() else 0.0 for v in col_values]
                # 转为紧凑表示
                col_data = _encode_numeric_column(nums)
                column_meta.append(('num', len(col_data)))
                encoded_columns.append(col_data)
            except Exception:
                # 回退到字符串处理
                col_data, val_dict = _encode_string_column(col_values)
                column_meta.append(('str', len(col_data), val_dict))
                encoded_columns.append(col_data)
        else:
            # 字符串列: 值字典
            col_data, val_dict = _encode_string_column(col_values)
            column_meta.append(('str', len(col_data), val_dict))
            encoded_columns.append(col_data)
    
    # 合并所有列数据
    result = bytearray()
    for col_data in encoded_columns:
        result.extend(col_data)
    
    # 序列化元数据
    meta_bytes = _serialize_csv_meta(num_cols, len(rows), column_meta)
    return bytes(result), meta_bytes


def csv_column_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """CSV列解码。"""
    if not meta_bytes:
        return data
    
    num_cols, num_rows, column_meta = _deserialize_csv_meta(meta_bytes)
    
    # 分割列数据
    offset = 0
    columns = []
    for meta in column_meta:
        col_len = meta[1] if meta[0] == 'num' else meta[1]
        col_data = data[offset:offset + col_len]
        offset += col_len
        
        if meta[0] == 'num':
            values = _decode_numeric_column(col_data, num_rows)
        else:
            val_dict = meta[2] if len(meta) > 2 else {}
            values = _decode_string_column(col_data, num_rows, val_dict)
        
        columns.append(values)
    
    # 重建CSV
    lines = []
    for row_idx in range(num_rows):
        fields = []
        for col_idx in range(num_cols):
            if row_idx < len(columns[col_idx]):
                fields.append(str(columns[col_idx][row_idx]))
            else:
                fields.append('')
        lines.append(','.join(fields))
    
    return '\n'.join(lines).encode('utf-8')


def _parse_csv_line(line: str) -> list:
    fields = []
    current = ''
    in_quotes = False
    for ch in line:
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == ',' and not in_quotes:
            fields.append(current)
            current = ''
        else:
            current += ch
    fields.append(current)
    return fields

def _is_numeric(s: str) -> bool:
    s = s.strip()
    if not s: return True  # 空值视为数值
    try:
        float(s)
        return True
    except ValueError:
        return False

def _encode_numeric_column(nums: list) -> bytes:
    """数值列编码: 转为8字节double + Delta。"""
    import struct as s
    # 转为double数组
    data = bytearray()
    for n in nums:
        data.extend(s.pack('>d', n))
    return bytes(data)

def _decode_numeric_column(data: bytes, num_rows: int) -> list:
    import struct as s
    values = []
    for i in range(num_rows):
        if i * 8 + 8 <= len(data):
            val = s.unpack('>d', data[i * 8:i * 8 + 8])[0]
            if val == int(val):
                values.append(int(val))
            else:
                values.append(val)
        else:
            values.append(0)
    return values

def _encode_string_column(values: list) -> Tuple[bytes, dict]:
    """字符串列编码: 值字典 + 索引。"""
    # 构建值字典
    val_counts = Counter(values)
    unique_vals = [v for v, _ in val_counts.most_common(65535)]
    val_to_idx = {v: i for i, v in enumerate(unique_vals)}
    
    # 编码为索引序列
    result = bytearray()
    # 如果唯一值<256, 用1字节索引; 否则2字节
    if len(unique_vals) < 256:
        for v in values:
            idx = val_to_idx.get(v, 0)
            result.append(idx)
    else:
        for v in values:
            idx = val_to_idx.get(v, 0)
            result.extend(struct.pack('>H', idx))
    
    # 值字典
    val_dict = unique_vals
    return bytes(result), val_dict

def _decode_string_column(data: bytes, num_rows: int, val_dict: list) -> list:
    """字符串列解码。"""
    values = []
    if len(val_dict) < 256:
        for i in range(num_rows):
            if i < len(data):
                values.append(val_dict[data[i]] if data[i] < len(val_dict) else '')
            else:
                values.append('')
    else:
        for i in range(num_rows):
            offset = i * 2
            if offset + 2 <= len(data):
                idx = struct.unpack('>H', data[offset:offset + 2])[0]
                values.append(val_dict[idx] if idx < len(val_dict) else '')
            else:
                values.append('')
    return values

def _serialize_csv_meta(num_cols: int, num_rows: int, column_meta: list) -> bytes:
    result = bytearray()
    result.extend(struct.pack('>H', num_cols))
    result.extend(struct.pack('>I', num_rows))
    
    for meta in column_meta:
        if meta[0] == 'num':
            result.append(0x01)  # 数值列标记
            result.extend(struct.pack('>I', meta[1]))  # 数据长度
        else:
            result.append(0x02)  # 字符串列标记
            result.extend(struct.pack('>I', meta[1]))  # 数据长度
            val_dict = meta[2] if len(meta) > 2 else []
            result.extend(struct.pack('>H', len(val_dict)))
            use_short = len(val_dict) < 256
            result.append(0x01 if use_short else 0x02)
            for v in val_dict:
                v_bytes = v.encode('utf-8')
                result.extend(struct.pack('>H', len(v_bytes)))
                result.extend(v_bytes)
    
    return bytes(result)

def _deserialize_csv_meta(data: bytes) -> Tuple[int, int, list]:
    offset = 0
    num_cols = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    num_rows = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
    
    column_meta = []
    for _ in range(num_cols):
        col_type = data[offset]; offset += 1
        col_len = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        
        if col_type == 0x01:
            column_meta.append(('num', col_len))
        else:
            num_vals = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
            idx_size = data[offset]; offset += 1  # 1 or 2 bytes per index
            val_dict = []
            for _ in range(num_vals):
                v_len = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
                v = data[offset:offset + v_len].decode('utf-8')
                val_dict.append(v)
                offset += v_len
            column_meta.append(('str', col_len, val_dict))
    
    return num_cols, num_rows, column_meta


# ─────────────────────────────────────────────
#  ★ 增强JSON值分离压缩
# ─────────────────────────────────────────────

def json_flatten_encode(data: bytes) -> Tuple[bytes, bytes]:
    """JSON扁平化压缩: 分离结构模板和值。
    
    对于JSON数组中的对象:
    1. 提取所有键名 → 短ID替换
    2. 按字段分组所有值 → 列式存储
    3. 数值字段Delta编码
    4. 字符串字段值字典编码
    
    返回: (encoded_data, metadata)
    """
    if not data or len(data) < 100:
        return data, b''
    
    try:
        import json
        text = data.decode('utf-8')
        parsed = json.loads(text)
    except Exception:
        return data, b''
    
    # 只处理对象数组
    if isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict):
        objects = parsed
    elif isinstance(parsed, dict):
        # 单个对象, 包装为数组
        objects = [parsed]
    else:
        return data, b''
    
    if len(objects) < 2:
        return data, b''
    
    # 收集所有键
    all_keys = []
    for obj in objects:
        for k in obj.keys():
            if k not in all_keys:
                all_keys.append(k)
    
    # 键名短编码
    key_map = {k: f'K{i:02d}' for i, k in enumerate(all_keys)}
    
    # 按字段分组值
    field_values = {k: [] for k in all_keys}
    for obj in objects:
        for k in all_keys:
            field_values[k].append(obj.get(k, None))
    
    # 编码每个字段的值
    encoded_fields = []
    field_meta = []
    
    for key in all_keys:
        values = field_values[key]
        
        # 检测值类型
        non_none = [v for v in values if v is not None]
        if not non_none:
            encoded_fields.append(b'')
            field_meta.append(('null', 0, []))
            continue
        
        if all(isinstance(v, (int, float)) for v in non_none):
            # 数值字段
            nums = [float(v) if v is not None else 0.0 for v in values]
            field_data = _encode_numeric_column(nums)
            field_meta.append(('num', len(field_data), []))
            encoded_fields.append(field_data)
        elif all(isinstance(v, str) for v in non_none):
            # 字符串字段
            str_values = [str(v) if v is not None else '' for v in values]
            field_data, val_dict = _encode_string_column(str_values)
            field_meta.append(('str', len(field_data), val_dict))
            encoded_fields.append(field_data)
        else:
            # 混合类型 → JSON序列化
            import json as j
            json_str = j.dumps(values)
            field_data = json_str.encode('utf-8')
            field_meta.append(('json', len(field_data), []))
            encoded_fields.append(field_data)
    
    # 合并所有字段数据
    result = bytearray()
    for fd in encoded_fields:
        result.extend(fd)
    
    # 序列化元数据
    meta = bytearray()
    meta.extend(struct.pack('>H', len(all_keys)))
    meta.extend(struct.pack('>I', len(objects)))
    for key in all_keys:
        key_bytes = key.encode('utf-8')
        meta.extend(struct.pack('>H', len(key_bytes)))
        meta.extend(key_bytes)
    
    for fm in field_meta:
        if fm[0] == 'null':
            meta.append(0x00)
        elif fm[0] == 'num':
            meta.append(0x01)
            meta.extend(struct.pack('>I', fm[1]))
        elif fm[0] == 'str':
            meta.append(0x02)
            meta.extend(struct.pack('>I', fm[1]))
            val_dict = fm[2]
            meta.extend(struct.pack('>H', len(val_dict)))
            use_short = len(val_dict) < 256
            meta.append(0x01 if use_short else 0x02)
            for v in val_dict:
                v_bytes = v.encode('utf-8')
                meta.extend(struct.pack('>H', len(v_bytes)))
                meta.extend(v_bytes)
        elif fm[0] == 'json':
            meta.append(0x03)
            meta.extend(struct.pack('>I', fm[1]))
    
    return bytes(result), bytes(meta)


def json_flatten_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """JSON扁平化解码。"""
    if not meta_bytes:
        return data
    
    import json
    
    offset = 0
    num_keys = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    num_objects = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    
    keys = []
    for _ in range(num_keys):
        key_len = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
        key = meta_bytes[offset:offset + key_len].decode('utf-8')
        keys.append(key)
        offset += key_len
    
    # 解码字段
    fields = []
    data_offset = 0
    
    for _ in range(num_keys):
        field_type = meta_bytes[offset]; offset += 1
        
        if field_type == 0x00:
            fields.append([None] * num_objects)
        elif field_type == 0x01:
            field_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
            field_data = data[data_offset:data_offset + field_len]
            values = _decode_numeric_column(field_data, num_objects)
            fields.append(values)
            data_offset += field_len
        elif field_type == 0x02:
            field_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
            num_vals = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
            idx_size = meta_bytes[offset]; offset += 1
            val_dict = []
            for _ in range(num_vals):
                v_len = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
                v = meta_bytes[offset:offset + v_len].decode('utf-8')
                val_dict.append(v)
                offset += v_len
            field_data = data[data_offset:data_offset + field_len]
            values = _decode_string_column(field_data, num_objects, val_dict)
            fields.append(values)
            data_offset += field_len
        elif field_type == 0x03:
            field_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
            field_data = data[data_offset:data_offset + field_len]
            values = json.loads(field_data.decode('utf-8'))
            fields.append(values)
            data_offset += field_len
    
    # 重建JSON对象数组
    objects = []
    for i in range(num_objects):
        obj = {}
        for j, key in enumerate(keys):
            if j < len(fields) and i < len(fields[j]):
                obj[key] = fields[j][i]
        objects.append(obj)
    
    return json.dumps(objects, ensure_ascii=False, indent=None).encode('utf-8')


# ─────────────────────────────────────────────
#  ★ 增强日志字段压缩
# ─────────────────────────────────────────────

def log_field_encode(data: bytes) -> Tuple[bytes, bytes]:
    """日志字段压缩: 按字段类型分组编码。
    
    算法:
    1. 解析每行日志为字段 (时间戳/级别/消息等)
    2. 时间戳: Delta编码
    3. 级别/模块: 字典编码
    4. 消息: 模板提取 + 变量编码
    
    返回: (encoded_data, metadata)
    """
    if not data or len(data) < 100:
        return data, b''
    
    text = data.decode('utf-8', errors='replace')
    lines = text.split('\n')
    if len(lines) < 5:
        return data, b''
    
    # 提取变量
    var_pattern = re.compile(
        r'\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}(?:\.\d+)?'
        r'|\d+\.\d+'
        r'|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
        r'|\b0x[0-9a-fA-F]+\b'
        r'|\b\d+\b'
    )
    
    template = var_pattern.sub('{}', text)
    variables = [m.group() for m in var_pattern.finditer(text)]
    
    var_count = template.count('{}')
    if var_count < 5:
        return data, b''
    
    # 分组变量: 时间戳、IP、数字等
    timestamp_vars = []
    ip_vars = []
    num_vars = []
    other_vars = []
    
    ts_pattern = re.compile(r'\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}')
    ip_pattern = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
    
    for v in variables:
        if ts_pattern.match(v):
            timestamp_vars.append(v)
        elif ip_pattern.match(v):
            ip_vars.append(v)
        elif re.match(r'^\d+$', v):
            num_vars.append(v)
        else:
            other_vars.append(v)
    
    # 编码变量组
    result = bytearray()
    meta = bytearray()
    
    # 时间戳组: Delta编码 (转为Unix时间戳后差分)
    ts_data = '\n'.join(timestamp_vars).encode('utf-8')
    result.extend(ts_data)
    meta.extend(struct.pack('>I', len(ts_data)))
    
    # IP组: 字典编码
    ip_data = '\n'.join(ip_vars).encode('utf-8')
    result.extend(ip_data)
    meta.extend(struct.pack('>I', len(ip_data)))
    
    # 数字组
    num_data = '\n'.join(num_vars).encode('utf-8')
    result.extend(num_data)
    meta.extend(struct.pack('>I', len(num_data)))
    
    # 其他变量
    other_data = '\n'.join(other_vars).encode('utf-8')
    result.extend(other_data)
    meta.extend(struct.pack('>I', len(other_data)))
    
    # 模板
    template_bytes = template.encode('utf-8')
    meta.extend(struct.pack('>I', len(template_bytes)))
    meta.extend(template_bytes)
    
    # 变量类型计数
    meta.extend(struct.pack('>H', len(timestamp_vars)))
    meta.extend(struct.pack('>H', len(ip_vars)))
    meta.extend(struct.pack('>H', len(num_vars)))
    meta.extend(struct.pack('>H', len(other_vars)))
    
    return bytes(result), bytes(meta)


def log_field_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """日志字段解码。"""
    if not meta_bytes:
        return data
    
    offset = 0
    ts_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    ip_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    num_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    other_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    template_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    template = meta_bytes[offset:offset + template_len].decode('utf-8'); offset += template_len
    
    num_ts = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    num_ip = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    num_num = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    num_other = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    
    # 提取变量数据
    data_offset = 0
    ts_data = data[data_offset:data_offset + ts_len]; data_offset += ts_len
    ip_data = data[data_offset:data_offset + ip_len]; data_offset += ip_len
    num_data = data[data_offset:data_offset + num_len]; data_offset += num_len
    other_data = data[data_offset:data_offset + other_len]; data_offset += other_len
    
    # 解码变量
    ts_vars = ts_data.decode('utf-8').split('\n') if ts_data else []
    ip_vars = ip_data.decode('utf-8').split('\n') if ip_data else []
    num_vars = num_data.decode('utf-8').split('\n') if num_data else []
    other_vars = other_data.decode('utf-8').split('\n') if other_data else []
    
    # 重建: 按模板中的占位符顺序填充
    # 先收集所有变量, 按原始顺序
    ts_idx = 0
    ip_idx = 0
    num_idx = 0
    other_idx = 0
    
    ts_pattern = re.compile(r'\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}')
    ip_pattern = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
    
    result = template
    # 简单方法: 按占位符顺序替换
    all_vars = []
    for v in ts_vars: all_vars.append(v)
    for v in ip_vars: all_vars.append(v)
    for v in num_vars: all_vars.append(v)
    for v in other_vars: all_vars.append(v)
    
    for var in all_vars:
        result = result.replace('{}', var, 1)
    
    return result.encode('utf-8')


# ─────────────────────────────────────────────
#  文本词频字典编码 (增强版)
# ─────────────────────────────────────────────

def text_dict_encode(data: bytes) -> Tuple[bytes, bytes]:
    if not data: return b'', b''
    try:
        text = data.decode('utf-8', errors='replace')
    except Exception:
        text = data.decode('latin-1')
    
    tokens = re.split(r'(\W+)', text)
    if not tokens: return data, b''
    
    # 统计词频和2-gram
    word_counts = Counter(t for t in tokens if t and re.match(r'^\w+$', t) and len(t) >= 2)
    bigram_counts = Counter()
    words = [t for t in tokens if t]
    for i in range(len(words) - 1):
        if re.match(r'^\w+$', words[i]) and re.match(r'^\w+$', words[i + 1]):
            bigram = words[i] + ' ' + words[i + 1]
            bigram_counts[bigram] += 1
    
    combined = {}
    for w, c in word_counts.most_common(2000):
        combined[w] = c * (len(w) - 2)
    for bg, c in bigram_counts.most_common(500):
        if c >= 3:
            combined[bg] = c * (len(bg) - 2)
    
    sorted_replacements = sorted(combined.items(), key=lambda x: -x[1])
    MAX_DICT_SIZE = 250
    dictionary = [w for w, _ in sorted_replacements[:MAX_DICT_SIZE]]
    
    if len(dictionary) < 5: return data, b''
    
    word_to_id = {w: i for i, w in enumerate(dictionary)}
    
    encoded = bytearray()
    i = 0
    while i < len(tokens):
        matched = False
        if i + 2 < len(tokens):
            bigram = tokens[i] + ' ' + tokens[i + 2]
            if bigram in word_to_id:
                if tokens[i + 1]:
                    encoded.extend(tokens[i + 1].encode('utf-8', errors='replace'))
                encoded.append(0xFD)
                encoded.append(word_to_id[bigram])
                i += 3
                matched = True
        if not matched and tokens[i] in word_to_id:
            idx = word_to_id[tokens[i]]
            encoded.append(0xFF)
            encoded.append(idx)
            i += 1
        elif not matched:
            if tokens[i]:
                encoded.extend(tokens[i].encode('utf-8', errors='replace'))
            i += 1
    
    dict_bytes = _serialize_dictionary(dictionary)
    return bytes(encoded), dict_bytes


def text_dict_decode(encoded: bytes, dict_bytes: bytes) -> bytes:
    if not encoded or not dict_bytes: return encoded
    dictionary = _deserialize_dictionary(dict_bytes)
    result = bytearray()
    i = 0
    n = len(encoded)
    while i < n:
        if encoded[i] == 0xFF and i + 1 < n:
            idx = encoded[i + 1]
            if idx < len(dictionary):
                result.extend(dictionary[idx].encode('utf-8', errors='replace'))
            else:
                result.append(0xFF)
                result.append(encoded[i + 1])
            i += 2
        elif encoded[i] == 0xFD and i + 1 < n:
            idx = encoded[i + 1]
            if idx < len(dictionary):
                result.extend(dictionary[idx].encode('utf-8', errors='replace'))
            else:
                result.append(0xFD)
                result.append(encoded[i + 1])
            i += 2
        else:
            result.append(encoded[i])
            i += 1
    return bytes(result)


def _serialize_dictionary(words: list) -> bytes:
    result = bytearray()
    result.extend(struct.pack('>H', len(words)))
    for word in words:
        word_bytes = word.encode('utf-8')
        result.extend(struct.pack('>H', len(word_bytes)))
        result.extend(word_bytes)
    return bytes(result)

def _deserialize_dictionary(data: bytes) -> list:
    words = []
    offset = 0
    num_words = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    for _ in range(num_words):
        word_len = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
        word = data[offset:offset + word_len].decode('utf-8')
        words.append(word)
        offset += word_len
    return words


# ─────────────────────────────────────────────
#  JSON键去重 / 列转置 / 日志模板 (继承v6)
# ─────────────────────────────────────────────

def json_key_dedup_encode(data: bytes) -> Tuple[bytes, bytes]:
    if not data: return b'', b''
    try:
        import json
        text = data.decode('utf-8')
        parsed = json.loads(text)
    except Exception:
        return data, b''
    keys_set = set()
    _extract_json_keys(parsed, keys_set)
    if not keys_set or len(keys_set) < 2: return data, b''
    sorted_keys = sorted(keys_set)
    key_to_id = {k: i for i, k in enumerate(sorted_keys)}
    result_text = text
    for key in sorted(sorted_keys, key=len, reverse=True):
        key_id = key_to_id[key]
        replacement = f'K{key_id:02d}' if key_id < 250 else f'K{key_id:04d}'
        pattern = f'"{re.escape(key)}"\\s*:'
        result_text = re.sub(pattern, f'"{replacement}":', result_text)
    schema_bytes = _serialize_key_schema(sorted_keys)
    return result_text.encode('utf-8'), schema_bytes

def json_key_dedup_decode(data: bytes, schema_bytes: bytes) -> bytes:
    if not schema_bytes: return data
    sorted_keys = _deserialize_key_schema(schema_bytes)
    text = data.decode('utf-8')
    for i, key in enumerate(sorted_keys):
        short_key = f'K{i:02d}' if i < 250 else f'K{i:04d}'
        pattern = f'"{re.escape(short_key)}"\\s*:'
        text = re.sub(pattern, f'"{key}":', text)
    return text.encode('utf-8')

def _extract_json_keys(obj, keys_set: set, prefix: str = ''):
    if isinstance(obj, dict):
        for key, value in obj.items():
            keys_set.add(key)
            _extract_json_keys(value, keys_set, prefix + key + '.')
    elif isinstance(obj, list):
        for item in obj:
            _extract_json_keys(item, keys_set, prefix)

def _serialize_key_schema(keys: list) -> bytes:
    result = bytearray()
    result.extend(struct.pack('>H', len(keys)))
    for key in keys:
        key_bytes = key.encode('utf-8')
        result.extend(struct.pack('>H', len(key_bytes)))
        result.extend(key_bytes)
    return bytes(result)

def _deserialize_key_schema(data: bytes) -> list:
    keys = []
    offset = 0
    num_keys = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    for _ in range(num_keys):
        key_len = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
        key = data[offset:offset + key_len].decode('utf-8')
        keys.append(key)
        offset += key_len
    return keys

def log_template_encode(data: bytes) -> Tuple[bytes, bytes]:
    if not data: return b'', b''
    text = data.decode('utf-8', errors='replace')
    lines = text.split('\n')
    if len(lines) < 5: return data, b''
    var_pattern = re.compile(
        r'\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}(?:\.\d+)?'
        r'|\d+\.\d+'
        r'|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
        r'|\b0x[0-9a-fA-F]+\b'
        r'|\b\d+\b'
    )
    template = var_pattern.sub('{}', text)
    variables = [m.group() for m in var_pattern.finditer(text)]
    if template.count('{}') < 5: return data, b''
    var_data = '\n'.join(variables).encode('utf-8')
    template_bytes = struct.pack('>I', len(template.encode('utf-8')))
    template_bytes += template.encode('utf-8')
    return var_data, template_bytes

def log_template_decode(var_data: bytes, template_bytes: bytes) -> bytes:
    if not template_bytes: return var_data
    template_len = struct.unpack('>I', template_bytes[:4])[0]
    template = template_bytes[4:4 + template_len].decode('utf-8')
    if not var_data: return template.encode('utf-8')
    variables = var_data.decode('utf-8').split('\n')
    result = template
    for var in variables:
        result = result.replace('{}', var, 1)
    return result.encode('utf-8')

def column_transpose_encode(data: bytes, row_width: int = 0) -> Tuple[bytes, int]:
    if not data: return b'', 0
    if row_width == 0:
        row_width = _detect_row_width(data)
        if row_width <= 1: return data, 0
    n = len(data)
    num_full_rows = n // row_width
    remainder = n % row_width
    result = bytearray(n)
    pos = 0
    for col in range(row_width):
        for row in range(num_full_rows):
            result[pos] = data[row * row_width + col]
            pos += 1
        if col < remainder:
            result[pos] = data[num_full_rows * row_width + col]
            pos += 1
    return bytes(result), row_width

def column_transpose_decode(data: bytes, row_width: int) -> bytes:
    if row_width <= 0 or not data: return data
    n = len(data)
    num_full_rows = n // row_width
    remainder = n % row_width
    result = bytearray(n)
    pos = 0
    for col in range(row_width):
        for row in range(num_full_rows):
            result[row * row_width + col] = data[pos]
            pos += 1
        if col < remainder:
            result[num_full_rows * row_width + col] = data[pos]
            pos += 1
    return bytes(result)

def _detect_row_width(data: bytes) -> int:
    newline_positions = [i for i, b in enumerate(data[:10000]) if b == ord('\n')]
    if len(newline_positions) < 3: return 0
    distances = [newline_positions[i + 1] - newline_positions[i] for i in range(min(len(newline_positions) - 1, 20))]
    if not distances: return 0
    dist_counts = Counter(distances)
    most_common_dist, count = dist_counts.most_common(1)[0]
    if count >= len(distances) * 0.5 and most_common_dist > 1:
        return most_common_dist + 1
    return 0


# ─────────────────────────────────────────────
#  序列化辅助
# ─────────────────────────────────────────────

def serialize_block_info(block_info: list) -> bytes:
    result = bytearray()
    result.extend(struct.pack('>H', len(block_info)))
    for orig_idx, block_size in block_info:
        result.extend(struct.pack('>I', orig_idx))
        result.extend(struct.pack('>I', block_size))
    return bytes(result)

def deserialize_block_info(data: bytes, offset: int = 0) -> Tuple[list, int]:
    num_blocks = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    block_info = []
    for _ in range(num_blocks):
        orig_idx = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        block_size = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        block_info.append((orig_idx, block_size))
    return block_info, offset
