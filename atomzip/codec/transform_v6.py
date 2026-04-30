"""
AtomZip v6 数据变换模块 — C加速版

核心改进:
  1. C语言BWT引擎 (通过ctypes调用), 支持100MB+文件
  2. 增强RLE编码 (双字节计数, 支持超长行程)
  3. 多粒度块去重
  4. 增强文本/JSON/CSV/日志预处理
  5. 保留v5所有变换
"""

import struct
import re
import os
import ctypes
from typing import Tuple, List, Dict, Optional
from collections import Counter

# ─────────────────────────────────────────────
#  C语言BWT引擎 (通过ctypes)
# ─────────────────────────────────────────────

_C_BWT_LIB = None

def _get_c_bwt_lib():
    """延迟加载C语言BWT库。"""
    global _C_BWT_LIB
    if _C_BWT_LIB is not None:
        return _C_BWT_LIB

    lib_path = os.path.join(os.path.dirname(__file__), 'c_ext', 'libbwt_fast.so')
    if not os.path.exists(lib_path):
        # 尝试相对路径
        lib_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'c_ext', 'libbwt_fast.so')

    if os.path.exists(lib_path):
        try:
            lib = ctypes.CDLL(lib_path)
            lib.bwt_encode_c.argtypes = [
                ctypes.POINTER(ctypes.c_uint8),
                ctypes.c_int32,
                ctypes.POINTER(ctypes.c_uint8)
            ]
            lib.bwt_encode_c.restype = ctypes.c_int32
            lib.bwt_decode_c.argtypes = [
                ctypes.POINTER(ctypes.c_uint8),
                ctypes.c_int32,
                ctypes.c_int32,
                ctypes.POINTER(ctypes.c_uint8)
            ]
            lib.bwt_decode_c.restype = None
            _C_BWT_LIB = lib
            return lib
        except Exception:
            pass

    _C_BWT_LIB = False  # 标记为不可用
    return None


def bwt_encode_c(data: bytes) -> Tuple[bytes, int]:
    """使用C引擎进行BWT编码。返回 (bwt_data, orig_idx)。"""
    lib = _get_c_bwt_lib()
    if lib is None:
        return bwt_encode_python(data)

    n = len(data)
    if n == 0:
        return b'', 0
    if n == 1:
        return data, 0

    data_arr = (ctypes.c_uint8 * n).from_buffer_copy(data)
    bwt_arr = (ctypes.c_uint8 * n)()

    orig_idx = lib.bwt_encode_c(data_arr, n, bwt_arr)
    if orig_idx < 0:
        return bwt_encode_python(data)

    return bytes(bwt_arr), orig_idx


def bwt_decode_c(bwt_data: bytes, orig_idx: int) -> bytes:
    """使用C引擎进行BWT解码。"""
    lib = _get_c_bwt_lib()
    if lib is None:
        return bwt_decode_python(bwt_data, orig_idx)

    n = len(bwt_data)
    if n == 0:
        return b''
    if n == 1:
        return bwt_data

    bwt_arr = (ctypes.c_uint8 * n).from_buffer_copy(bwt_data)
    out_arr = (ctypes.c_uint8 * n)()

    lib.bwt_decode_c(bwt_arr, n, orig_idx, out_arr)
    return bytes(out_arr)


# ─────────────────────────────────────────────
#  Python后备BWT (用于C库不可用时)
# ─────────────────────────────────────────────

def bwt_encode_python(data: bytes) -> Tuple[bytes, int]:
    """Python版BWT编码 (后备方案, 仅用于小数据)。"""
    n = len(data)
    if n == 0:
        return b'', 0
    if n == 1:
        return data, 0

    if n <= 65536:
        doubled = data + data
        indices = sorted(range(n), key=lambda i: doubled[i:i + n])
    else:
        # 前缀倍增法
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
            if rank[sa[-1]] == n - 1:
                break
            k *= 2
        indices = sa

    bwt = bytes(data[(i - 1) % n] for i in indices)
    orig_idx = indices.index(0)
    return bwt, orig_idx


def bwt_decode_python(bwt_data: bytes, orig_idx: int) -> bytes:
    """Python版BWT解码 (LF映射)。"""
    n = len(bwt_data)
    if n == 0:
        return b''
    if n == 1:
        return bwt_data

    count = [0] * 256
    for c in bwt_data:
        count[c] += 1

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


# ─────────────────────────────────────────────
#  统一BWT接口 (自动选择C或Python)
# ─────────────────────────────────────────────

BWT_MAX_DATA_SIZE = 256 << 20  # 256MB (C引擎可以处理)

def bwt_encode(data: bytes, block_size: int = 0) -> Tuple[bytes, List[Tuple[int, int]]]:
    """BWT编码 (全文件单块模式, 使用C加速)。"""
    if len(data) == 0:
        return b'', []

    bwt_data, orig_idx = bwt_encode_c(data)
    return bwt_data, [(orig_idx, len(data))]


def bwt_decode(bwt_data: bytes, block_info: List[Tuple[int, int]]) -> bytes:
    """BWT解码。"""
    if not block_info:
        return b''

    # 单块模式
    if len(block_info) == 1:
        orig_idx, block_size = block_info[0]
        return bwt_decode_c(bwt_data, orig_idx)

    # 多块模式 (v5兼容)
    result = bytearray()
    offset = 0
    for orig_idx, block_size in block_info:
        block = bwt_data[offset:offset + block_size]
        decoded = bwt_decode_c(block, orig_idx)
        result.extend(decoded)
        offset += block_size
    return bytes(result)


# ─────────────────────────────────────────────
#  增强RLE编码 (双字节计数)
# ─────────────────────────────────────────────

RLE_ESCAPE = 0xFE
RLE_MIN_RUN = 4

def rle_encode(data: bytes) -> bytes:
    """增强RLE编码: 支持超长行程 (计数可达65535)。"""
    if not data:
        return b''

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
            # 双字节计数
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
    """增强RLE解码。"""
    if not data:
        return b''

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
    """差分编码。"""
    if not data:
        return b'', 0
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
    """差分解码。"""
    if not data:
        return b''
    n = len(data)
    result = bytearray(n)
    for i in range(n):
        if i < stride:
            result[i] = data[i]
        else:
            result[i] = (result[i - stride] + data[i]) % 256
    return bytes(result)


# ─────────────────────────────────────────────
#  多粒度块去重
# ─────────────────────────────────────────────

def block_dedup_encode(data: bytes, block_sizes: List[int] = None) -> Tuple[bytes, bytes]:
    """
    多粒度块去重:
    1. 尝试多种块大小 (16, 32, 64, 128, 256, 512, 1024)
    2. 找到重复块最多的块大小
    3. 用引用替换重复块

    返回: (encoded_data, dedup_info_bytes)
    """
    if not data or len(data) < 64:
        return data, b''

    if block_sizes is None:
        block_sizes = [16, 32, 64, 128, 256, 512, 1024]

    best_saving = 0
    best_bs = 0
    best_blocks = []
    best_unique = []
    best_refs = []

    for bs in block_sizes:
        if bs > len(data) // 2:
            continue

        # 切块
        blocks = []
        for i in range(0, len(data) - bs + 1, bs):
            blocks.append(data[i:i + bs])

        # 剩余部分
        remainder_start = len(blocks) * bs
        remainder = data[remainder_start:]

        if not blocks:
            continue

        # 统计重复
        block_counts = Counter(blocks)
        unique_blocks = [b for b, c in block_counts.items() if c >= 2]
        duplicate_count = sum(c - 1 for b, c in block_counts.items() if c >= 2)

        if duplicate_count < 2:
            continue

        # 计算节省
        # 每个重复块节省 bs 字节, 但需要引用开销 (4字节索引)
        saving = duplicate_count * bs - duplicate_count * 4 - len(unique_blocks) * bs
        if saving > best_saving:
            best_saving = saving
            best_bs = bs
            # 构建编码方案
            block_to_idx = {}
            unique_list = []
            idx = 0
            for b in blocks:
                if b in block_to_idx:
                    continue
                block_to_idx[b] = idx
                unique_list.append(b)
                idx += 1

            best_blocks = unique_list
            best_unique = block_to_idx
            best_refs = [block_to_idx.get(b, -1) for b in blocks]

    if best_saving <= 0 or best_bs == 0:
        return data, b''

    # 编码
    # 格式: [block_size(2)] [num_unique(2)] [unique_blocks...] [refs...] [remainder]
    result = bytearray()

    # 序列化去重信息
    dedup_info = bytearray()
    dedup_info.extend(struct.pack('>H', best_bs))
    dedup_info.extend(struct.pack('>H', len(best_blocks)))
    for block in best_blocks:
        dedup_info.extend(struct.pack('>H', len(block)))
        dedup_info.extend(block)

    # 编码引用和唯一块
    encoded = bytearray()
    # 标记: 0xFF 0x01 = 引用 (后跟2字节索引), 0xFF 0x02 = 唯一块 (后跟数据)
    ref_id = 0
    block_idx_map = {}
    for i, b in enumerate(best_blocks):
        block_idx_map[b] = i

    for ref in best_refs:
        if ref >= 0 and len(best_blocks) > 1:
            # 使用引用
            encoded.append(0xFF)
            encoded.append(0x01)
            encoded.extend(struct.pack('>H', ref))
        else:
            # 直接写入块
            block = best_blocks[ref] if ref >= 0 else data[ref * best_bs:(ref + 1) * best_bs]
            encoded.append(0xFF)
            encoded.append(0x02)
            encoded.extend(struct.pack('>H', len(block)))
            encoded.extend(block)

    # 添加剩余部分
    remainder_start = len(best_refs) * best_bs
    if remainder_start < len(data):
        encoded.extend(data[remainder_start:])

    return bytes(encoded), bytes(dedup_info)


# ─────────────────────────────────────────────
#  增强文本词频字典编码
# ─────────────────────────────────────────────

def text_dict_encode(data: bytes) -> Tuple[bytes, bytes]:
    """增强文本词频字典编码: 支持更长的词和更高效的编码。"""
    if not data:
        return b'', b''

    try:
        text = data.decode('utf-8', errors='replace')
    except Exception:
        text = data.decode('latin-1')

    # 分割为词和非词
    tokens = re.split(r'(\W+)', text)
    if not tokens:
        return data, b''

    # 统计词频 (包括2-gram和3-gram)
    word_counts = Counter(t for t in tokens if t and re.match(r'^\w+$', t) and len(t) >= 2)

    # 也统计高频2-gram
    bigram_counts = Counter()
    words = [t for t in tokens if t]
    for i in range(len(words) - 1):
        if re.match(r'^\w+$', words[i]) and re.match(r'^\w+$', words[i + 1]):
            bigram = words[i] + ' ' + words[i + 1]
            bigram_counts[bigram] += 1

    # 合并: 2-gram替换能节省更多空间
    combined = {}
    for w, c in word_counts.most_common(2000):
        combined[w] = c * (len(w) - 2)  # 节省 = (词长 - 编码长) * 频率
    for bg, c in bigram_counts.most_common(500):
        if c >= 3:
            combined[bg] = c * (len(bg) - 2)

    # 选择节省最多的
    sorted_replacements = sorted(combined.items(), key=lambda x: -x[1])
    MAX_DICT_SIZE = 250  # 用1字节编码
    dictionary = [w for w, _ in sorted_replacements[:MAX_DICT_SIZE]]

    if len(dictionary) < 5:
        return data, b''

    word_to_id = {w: i for i, w in enumerate(dictionary)}

    # 编码
    encoded = bytearray()
    i = 0
    tokens_list = tokens

    while i < len(tokens_list):
        # 尝试2-gram匹配
        matched = False
        if i + 2 < len(tokens_list):
            bigram = tokens_list[i] + ' ' + tokens_list[i + 2]  # 跳过非词分隔
            if bigram in word_to_id:
                # 输出分隔符 + 编码
                if tokens_list[i + 1]:
                    encoded.extend(tokens_list[i + 1].encode('utf-8', errors='replace'))
                encoded.append(0xFD)  # 2-gram标记
                encoded.append(word_to_id[bigram])
                i += 3
                matched = True

        if not matched and tokens_list[i] in word_to_id:
            idx = word_to_id[tokens_list[i]]
            encoded.append(0xFF)
            encoded.append(idx)
            i += 1
        elif not matched:
            if tokens_list[i]:
                encoded.extend(tokens_list[i].encode('utf-8', errors='replace'))
            i += 1

    # 序列化字典
    dict_bytes = _serialize_dictionary(dictionary)
    return bytes(encoded), dict_bytes


def text_dict_decode(encoded: bytes, dict_bytes: bytes) -> bytes:
    """文本词频字典解码。"""
    if not encoded or not dict_bytes:
        return encoded

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


def _serialize_dictionary(words: List[str]) -> bytes:
    """序列化字典。"""
    result = bytearray()
    result.extend(struct.pack('>H', len(words)))
    for word in words:
        word_bytes = word.encode('utf-8')
        result.extend(struct.pack('>H', len(word_bytes)))
        result.extend(word_bytes)
    return bytes(result)


def _deserialize_dictionary(data: bytes) -> List[str]:
    """反序列化字典。"""
    words = []
    offset = 0
    num_words = struct.unpack('>H', data[offset:offset + 2])[0]
    offset += 2

    for _ in range(num_words):
        word_len = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2
        word = data[offset:offset + word_len].decode('utf-8')
        words.append(word)
        offset += word_len

    return words


# ─────────────────────────────────────────────
#  JSON 键去重变换
# ─────────────────────────────────────────────

def json_key_dedup_encode(data: bytes) -> Tuple[bytes, bytes]:
    """JSON键去重: 分离键名，消除重复。"""
    if not data:
        return b'', b''

    try:
        import json
        text = data.decode('utf-8')
        parsed = json.loads(text)
    except (UnicodeDecodeError, Exception):
        return data, b''

    keys_set = set()
    _extract_json_keys(parsed, keys_set)

    if not keys_set or len(keys_set) < 2:
        return data, b''

    sorted_keys = sorted(keys_set)
    key_to_id = {k: i for i, k in enumerate(sorted_keys)}

    result_text = text
    for key in sorted(sorted_keys, key=len, reverse=True):
        key_id = key_to_id[key]
        if key_id < 250:
            replacement = f'K{key_id:02d}'
        else:
            replacement = f'K{key_id:04d}'
        pattern = f'"{re.escape(key)}"\\s*:'
        result_text = re.sub(pattern, f'"{replacement}":', result_text)

    schema_bytes = _serialize_key_schema(sorted_keys)
    return result_text.encode('utf-8'), schema_bytes


def json_key_dedup_decode(data: bytes, schema_bytes: bytes) -> bytes:
    """JSON键去重逆变换。"""
    if not schema_bytes:
        return data

    sorted_keys = _deserialize_key_schema(schema_bytes)
    text = data.decode('utf-8')

    for i, key in enumerate(sorted_keys):
        if i < 250:
            short_key = f'K{i:02d}'
        else:
            short_key = f'K{i:04d}'
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


def _serialize_key_schema(keys: List[str]) -> bytes:
    result = bytearray()
    result.extend(struct.pack('>H', len(keys)))
    for key in keys:
        key_bytes = key.encode('utf-8')
        result.extend(struct.pack('>H', len(key_bytes)))
        result.extend(key_bytes)
    return bytes(result)


def _deserialize_key_schema(data: bytes) -> List[str]:
    keys = []
    offset = 0
    num_keys = struct.unpack('>H', data[offset:offset + 2])[0]
    offset += 2
    for _ in range(num_keys):
        key_len = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2
        key = data[offset:offset + key_len].decode('utf-8')
        keys.append(key)
        offset += key_len
    return keys


# ─────────────────────────────────────────────
#  日志模板提取
# ─────────────────────────────────────────────

def log_template_encode(data: bytes) -> Tuple[bytes, bytes]:
    """日志模板提取: 分离固定格式和可变部分。"""
    if not data:
        return b'', b''

    text = data.decode('utf-8', errors='replace')
    lines = text.split('\n')

    if len(lines) < 5:
        return data, b''

    var_pattern = re.compile(
        r'\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}(?:\.\d+)?'
        r'|\d+\.\d+'
        r'|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
        r'|\b0x[0-9a-fA-F]+\b'
        r'|\b\d+\b'
    )

    template = var_pattern.sub('{}', text)
    variables = [m.group() for m in var_pattern.finditer(text)]

    if template.count('{}') < 5:
        return data, b''

    var_data = '\n'.join(variables).encode('utf-8')
    template_bytes = struct.pack('>I', len(template.encode('utf-8')))
    template_bytes += template.encode('utf-8')

    return var_data, template_bytes


def log_template_decode(var_data: bytes, template_bytes: bytes) -> bytes:
    """日志模板逆变换。"""
    if not template_bytes:
        return var_data

    template_len = struct.unpack('>I', template_bytes[:4])[0]
    template = template_bytes[4:4 + template_len].decode('utf-8')

    if not var_data:
        return template.encode('utf-8')

    variables = var_data.decode('utf-8').split('\n')
    result = template
    for var in variables:
        result = result.replace('{}', var, 1)

    return result.encode('utf-8')


# ─────────────────────────────────────────────
#  列转置变换
# ─────────────────────────────────────────────

def column_transpose_encode(data: bytes, row_width: int = 0) -> Tuple[bytes, int]:
    """列转置: 行优先 → 列优先。"""
    if not data:
        return b'', 0

    if row_width == 0:
        row_width = _detect_row_width(data)
        if row_width <= 1:
            return data, 0

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
    """列转置逆变换。"""
    if row_width <= 0 or not data:
        return data

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
    if len(newline_positions) < 3:
        return 0
    distances = [newline_positions[i + 1] - newline_positions[i]
                 for i in range(min(len(newline_positions) - 1, 20))]
    if not distances:
        return 0
    dist_counts = Counter(distances)
    most_common_dist, count = dist_counts.most_common(1)[0]
    if count >= len(distances) * 0.5 and most_common_dist > 1:
        return most_common_dist + 1
    return 0


# ─────────────────────────────────────────────
#  序列化辅助
# ─────────────────────────────────────────────

def serialize_block_info(block_info: List[Tuple[int, int]]) -> bytes:
    result = bytearray()
    result.extend(struct.pack('>H', len(block_info)))
    for orig_idx, block_size in block_info:
        result.extend(struct.pack('>I', orig_idx))
        result.extend(struct.pack('>I', block_size))
    return bytes(result)


def deserialize_block_info(data: bytes, offset: int = 0) -> Tuple[List[Tuple[int, int]], int]:
    num_blocks = struct.unpack('>H', data[offset:offset + 2])[0]
    offset += 2
    block_info = []
    for _ in range(num_blocks):
        orig_idx = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        block_size = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        block_info.append((orig_idx, block_size))
    return block_info, offset
