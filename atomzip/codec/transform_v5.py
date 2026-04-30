"""
AtomZip v5 数据变换模块 — 扩展变换集

新增变换:
  - RLE (行程编码): 压缩连续重复字节，特别适合 BWT 后的数据
  - 多步长 Delta: 支持不同步长的差分编码
  - 文本词频字典: 对文本数据进行词级替换
  - JSON 键去重: 分离 JSON 键值，消除重复键
  - 日志模板提取: 检测日志格式模板，仅编码变量部分
  - 列转置: 对结构化数据按列重组，提高同列数据的聚集度

保留 v4 变换:
  - BWT (Burrows-Wheeler 变换): 单块全文件模式
  - Delta (差分编码): 基础步长1
"""

import struct
import re
from typing import Tuple, List, Dict, Optional
from collections import Counter

# BWT 最大数据大小 (8MB, 比v4提升4倍)
BWT_MAX_DATA_SIZE = 8 << 20

# RLE 编码的特殊标记字节 (选择出现频率最低的字节作为转义符)
RLE_ESCAPE = 0xFE  # RLE 转义字节
RLE_MIN_RUN = 4    # 最小行程长度 (低于此长度不做RLE编码)


# ─────────────────────────────────────────────
#  BWT (Burrows-Wheeler Transform) — 继承自v4
# ─────────────────────────────────────────────

def _build_cyclic_sa(data: bytes) -> List[int]:
    """构建循环后缀数组 (前缀倍增法, O(n log² n))"""
    n = len(data)
    if n <= 1:
        return [0]

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

    return sa


def bwt_encode_block(data: bytes) -> Tuple[bytes, int]:
    """对单个数据块执行 BWT 编码。"""
    n = len(data)
    if n == 0:
        return b'', 0
    if n == 1:
        return data, 0

    if n <= 4096:
        doubled = data + data
        indices = sorted(range(n), key=lambda i: doubled[i:i + n])
    else:
        indices = _build_cyclic_sa(data)

    bwt = bytes(data[(i - 1) % n] for i in indices)
    orig_idx = indices.index(0)

    return bwt, orig_idx


def bwt_decode_block(bwt: bytes, orig_idx: int) -> bytes:
    """对单个数据块执行 BWT 解码 (LF 映射算法)。"""
    n = len(bwt)
    if n == 0:
        return b''
    if n == 1:
        return bwt

    count = [0] * 256
    for c in bwt:
        count[c] += 1

    cumul = [0] * 256
    total = 0
    for i in range(256):
        cumul[i] = total
        total += count[i]

    lf = [0] * n
    occ = [0] * 256
    for i in range(n):
        c = bwt[i]
        lf[i] = cumul[c] + occ[c]
        occ[c] += 1

    result = bytearray(n)
    j = orig_idx
    for i in range(n - 1, -1, -1):
        result[i] = bwt[j]
        j = lf[j]

    return bytes(result)


def bwt_encode(data: bytes, block_size: int = 0) -> Tuple[bytes, List[Tuple[int, int]]]:
    """分块 BWT 编码。block_size=0 表示全文件单块。"""
    if len(data) == 0:
        return b'', []

    if block_size == 0 or block_size >= len(data):
        bwt, orig_idx = bwt_encode_block(data)
        return bwt, [(orig_idx, len(data))]

    blocks = []
    offset = 0
    while offset < len(data):
        end = min(offset + block_size, len(data))
        block = data[offset:end]
        bwt, orig_idx = bwt_encode_block(block)
        blocks.append((bwt, orig_idx, len(block)))
        offset = end

    bwt_data = b''.join(b for b, _, _ in blocks)
    block_info = [(orig_idx, bs) for _, orig_idx, bs in blocks]

    return bwt_data, block_info


def bwt_decode(bwt_data: bytes, block_info: List[Tuple[int, int]]) -> bytes:
    """分块 BWT 解码。"""
    if not block_info:
        return b''

    result = bytearray()
    offset = 0
    for orig_idx, block_size in block_info:
        block = bwt_data[offset:offset + block_size]
        decoded = bwt_decode_block(block, orig_idx)
        result.extend(decoded)
        offset += block_size

    return bytes(result)


# ─────────────────────────────────────────────
#  RLE (行程编码)
# ─────────────────────────────────────────────

def rle_encode(data: bytes) -> bytes:
    """
    行程编码: 将连续重复的字节序列压缩为 (字节, 计数) 对。

    格式:
      非转义字节: 直接输出
      转义序列: RLE_ESCAPE, 字节, 计数
        - 计数 0 表示 RLE_ESCAPE 字节本身
        - 计数 N (>=RLE_MIN_RUN-1) 表示 字节 重复 N+1 次

    对 BWT 后的数据特别有效，因为 BWT 会产生大量连续相同字节。
    """
    if not data:
        return b''

    result = bytearray()
    i = 0
    n = len(data)

    while i < n:
        current = data[i]
        run_len = 1
        while i + run_len < n and data[i + run_len] == current and run_len < 255:
            run_len += 1

        if run_len >= RLE_MIN_RUN:
            # 编码为转义序列
            result.append(RLE_ESCAPE)
            result.append(current)
            result.append(run_len)
            i += run_len
        elif current == RLE_ESCAPE:
            # 转义 RLE_ESCAPE 字节本身
            result.append(RLE_ESCAPE)
            result.append(RLE_ESCAPE)
            result.append(0)
            i += 1
        else:
            result.append(current)
            i += 1

    return bytes(result)


def rle_decode(data: bytes) -> bytes:
    """行程解码: 还原 RLE 编码的数据。"""
    if not data:
        return b''

    result = bytearray()
    i = 0
    n = len(data)

    while i < n:
        if data[i] == RLE_ESCAPE:
            if i + 2 >= n:
                # 格式错误，当作原始字节
                result.append(data[i])
                i += 1
                continue
            byte_val = data[i + 1]
            count = data[i + 2]
            if count == 0 and byte_val == RLE_ESCAPE:
                # 转义 RLE_ESCAPE 字节本身
                result.append(RLE_ESCAPE)
            else:
                # 重复 count 次
                result.extend(bytes([byte_val]) * count)
            i += 3
        else:
            result.append(data[i])
            i += 1

    return bytes(result)


# ─────────────────────────────────────────────
#  Delta (多步长差分编码)
# ─────────────────────────────────────────────

def delta_encode(data: bytes, stride: int = 1) -> Tuple[bytes, int]:
    """
    差分编码: delta[i] = (data[i] - data[i-stride]) mod 256

    stride=1: 相邻字节差分 (默认)
    stride>1: 跨步差分，适合周期性数据

    返回: (encoded, first_byte)
    """
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
    """差分解码: 还原多步长差分编码的数据。"""
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
#  文本词频字典编码
# ─────────────────────────────────────────────

def text_dict_encode(data: bytes) -> Tuple[bytes, bytes]:
    """
    文本词频字典编码:
    1. 将数据分割为词 (word) 和非词 (non-word) 交替序列
    2. 对词频率排序，高频词用短编码替换
    3. 编码格式: 词ID用变长编码 (1-3字节)

    返回: (encoded_data, dictionary_bytes)
    """
    if not data:
        return b'', b''

    # 分割为词和非词
    text = data.decode('latin-1')  # 逐字节安全解码
    tokens = re.split(r'(\W+)', text)  # 交替: word, non-word, word, ...

    if not tokens:
        return data, b''

    # 统计词频
    word_counts = Counter(t for t in tokens if t and re.match(r'^\w+$', t))

    # 构建字典: 高频词 → 短编码
    # 编码方案: 0-249 → 1字节, 250-5049 → 2字节, 5050+ → 3字节
    MAX_DICT_SIZE = 5049
    sorted_words = [w for w, _ in word_counts.most_common(MAX_DICT_SIZE)]
    word_to_id = {w: i for i, w in enumerate(sorted_words)}

    # 编码数据
    encoded = bytearray()
    for token in tokens:
        if not token:
            continue
        if token in word_to_id:
            idx = word_to_id[token]
            if idx < 250:
                encoded.append(0xFF)  # 词标记
                encoded.append(idx)
            elif idx < 5050:
                encoded.append(0xFE)  # 2字节词标记
                encoded.append((idx - 250) >> 8)
                encoded.append((idx - 250) & 0xFF)
            else:
                # 不应该到达这里 (MAX_DICT_SIZE限制)
                encoded.extend(token.encode('latin-1'))
        else:
            # 非词或不在字典中的词 → 直接输出
            encoded.extend(token.encode('latin-1'))

    # 序列化字典
    dict_bytes = _serialize_dictionary(sorted_words)

    return bytes(encoded), dict_bytes


def text_dict_decode(encoded: bytes, dict_bytes: bytes) -> bytes:
    """文本词频字典解码。"""
    if not encoded or not dict_bytes:
        return encoded

    sorted_words = _deserialize_dictionary(dict_bytes)

    result = bytearray()
    i = 0
    n = len(encoded)

    while i < n:
        if encoded[i] == 0xFF and i + 1 < n:
            # 1字节词ID
            idx = encoded[i + 1]
            if idx < len(sorted_words):
                result.extend(sorted_words[idx].encode('latin-1'))
            else:
                result.append(0xFF)
                result.append(encoded[i + 1])
            i += 2
        elif encoded[i] == 0xFE and i + 2 < n:
            # 2字节词ID
            idx = 250 + (encoded[i + 1] << 8) + encoded[i + 2]
            if idx < len(sorted_words):
                result.extend(sorted_words[idx].encode('latin-1'))
            else:
                result.append(0xFE)
                result.append(encoded[i + 1])
                result.append(encoded[i + 2])
            i += 3
        else:
            result.append(encoded[i])
            i += 1

    return bytes(result)


def _serialize_dictionary(words: List[str]) -> bytes:
    """序列化词频字典。"""
    result = bytearray()
    result.extend(struct.pack('>H', len(words)))
    for word in words:
        word_bytes = word.encode('latin-1')
        result.extend(struct.pack('>H', len(word_bytes)))
        result.extend(word_bytes)
    return bytes(result)


def _deserialize_dictionary(data: bytes) -> List[str]:
    """反序列化词频字典。"""
    words = []
    offset = 0
    num_words = struct.unpack('>H', data[offset:offset + 2])[0]
    offset += 2

    for _ in range(num_words):
        word_len = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2
        word = data[offset:offset + word_len].decode('latin-1')
        words.append(word)
        offset += word_len

    return words


# ─────────────────────────────────────────────
#  JSON 键去重变换
# ─────────────────────────────────────────────

def json_key_dedup_encode(data: bytes) -> Tuple[bytes, bytes]:
    """
    JSON 键去重变换:
    1. 检测 JSON 数据中的重复键模式
    2. 提取键列表，用短ID替换
    3. 仅保留值部分

    返回: (transformed_data, key_schema_bytes)
    """
    if not data:
        return b'', b''

    # 尝试解析 JSON
    try:
        import json
        text = data.decode('utf-8')
        parsed = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError):
        # 不是有效 JSON，返回原始数据
        return data, b''

    # 提取键模式
    keys_set = set()
    _extract_json_keys(parsed, keys_set)

    if not keys_set:
        return data, b''

    # 重新序列化，替换键为短编码
    sorted_keys = sorted(keys_set)
    key_to_id = {k: i for i, k in enumerate(sorted_keys)}

    # 构建替换后的文本
    # 简单方法: 在文本中替换键名
    result_text = text
    # 按键名长度降序排列，避免短键替换影响长键
    for key in sorted(sorted_keys, key=len, reverse=True):
        key_id = key_to_id[key]
        # 替换 "key" 为 "K##" 格式
        if key_id < 250:
            replacement = f'K{key_id:02d}'
        else:
            replacement = f'K{key_id:04d}'
        # 只替换键 (在引号中且后面跟着冒号)
        pattern = f'"{re.escape(key)}"\\s*:'
        result_text = re.sub(pattern, f'"{replacement}":', result_text)

    # 序列化键模式
    schema_bytes = _serialize_key_schema(sorted_keys)

    return result_text.encode('utf-8'), schema_bytes


def json_key_dedup_decode(data: bytes, schema_bytes: bytes) -> bytes:
    """JSON 键去重逆变换。"""
    if not schema_bytes:
        return data

    sorted_keys = _deserialize_key_schema(schema_bytes)

    text = data.decode('utf-8')

    # 逆替换: "K##" → 原始键名
    for i, key in enumerate(sorted_keys):
        if i < 250:
            short_key = f'K{i:02d}'
        else:
            short_key = f'K{i:04d}'

        pattern = f'"{re.escape(short_key)}"\\s*:'
        text = re.sub(pattern, f'"{key}":', text)

    return text.encode('utf-8')


def _extract_json_keys(obj, keys_set: set, prefix: str = ''):
    """递归提取 JSON 对象的所有键名。"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            keys_set.add(key)
            _extract_json_keys(value, keys_set, prefix + key + '.')
    elif isinstance(obj, list):
        for item in obj:
            _extract_json_keys(item, keys_set, prefix)


def _serialize_key_schema(keys: List[str]) -> bytes:
    """序列化键模式。"""
    result = bytearray()
    result.extend(struct.pack('>H', len(keys)))
    for key in keys:
        key_bytes = key.encode('utf-8')
        result.extend(struct.pack('>H', len(key_bytes)))
        result.extend(key_bytes)
    return bytes(result)


def _deserialize_key_schema(data: bytes) -> List[str]:
    """反序列化键模式。"""
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
    """
    日志模板提取:
    1. 检测日志行的固定格式
    2. 提取可变部分 (数字、IP、路径等)
    3. 仅编码可变部分

    返回: (variable_parts, template_bytes)
    """
    if not data:
        return b'', b''

    text = data.decode('utf-8', errors='replace')
    lines = text.split('\n')

    if len(lines) < 5:
        return data, b''

    # 尝试检测日志模板
    # 简单方法: 将数字、IP、时间戳等可变部分替换为占位符
    template_parts = []
    variable_data = bytearray()

    # 用正则匹配可变部分: 数字、浮点数、IP地址、十六进制等
    var_pattern = re.compile(
        r'\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}(?:\.\d+)?'  # 时间戳
        r'|\d+\.\d+'       # 浮点数
        r'|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'  # IP地址
        r'|\b0x[0-9a-fA-F]+\b'  # 十六进制
        r'|\b\d+\b'        # 整数
    )

    # 构建模板: 固定部分 + 变量占位符
    template = var_pattern.sub('{}', text)

    # 提取变量值
    variables = []
    for match in var_pattern.finditer(text):
        variables.append(match.group())

    # 如果模板太相似于原始数据 (变量太少)，放弃
    var_count = template.count('{}')
    if var_count < 5:
        return data, b''

    # 编码变量
    var_data = '\n'.join(variables).encode('utf-8')

    # 序列化模板
    template_bytes = struct.pack('>I', len(template.encode('utf-8')))
    template_bytes += template.encode('utf-8')

    return var_data, template_bytes


def log_template_decode(var_data: bytes, template_bytes: bytes) -> bytes:
    """日志模板逆变换。"""
    if not template_bytes:
        return var_data

    # 反序列化模板
    template_len = struct.unpack('>I', template_bytes[:4])[0]
    template = template_bytes[4:4 + template_len].decode('utf-8')

    # 解码变量
    if not var_data:
        return template.encode('utf-8')

    variables = var_data.decode('utf-8').split('\n')

    # 填充模板
    result = template
    for var in variables:
        result = result.replace('{}', var, 1)

    return result.encode('utf-8')


# ─────────────────────────────────────────────
#  列转置变换
# ─────────────────────────────────────────────

def column_transpose_encode(data: bytes, row_width: int = 0) -> Tuple[bytes, int]:
    """
    列转置变换: 将行优先数据重组为列优先。

    对结构化数据 (JSON行、CSV、固定宽度记录) 特别有效，
    因为同列数据具有更高的相似性，压缩效果更好。

    row_width=0: 自动检测行宽度
    """
    if not data:
        return b'', 0

    # 自动检测行宽度 (寻找最频繁的换行间隔)
    if row_width == 0:
        row_width = _detect_row_width(data)
        if row_width <= 1:
            return data, 0

    n = len(data)
    num_full_rows = n // row_width
    remainder = n % row_width

    # 转置: 行优先 → 列优先
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
    """列转置逆变换: 列优先 → 行优先。"""
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
    """自动检测数据的行宽度。"""
    # 寻找换行符位置
    newline_positions = [i for i, b in enumerate(data[:10000]) if b == ord('\n')]

    if len(newline_positions) < 3:
        return 0

    # 计算行间距
    distances = [newline_positions[i + 1] - newline_positions[i]
                 for i in range(min(len(newline_positions) - 1, 20))]

    if not distances:
        return 0

    # 最频繁的行间距 (加1包含换行符本身)
    dist_counts = Counter(distances)
    most_common_dist, count = dist_counts.most_common(1)[0]

    # 如果最常见的间距出现次数占多数，则认为是固定宽度
    if count >= len(distances) * 0.5 and most_common_dist > 1:
        return most_common_dist + 1  # 包含换行符

    return 0


# ─────────────────────────────────────────────
#  序列化辅助
# ─────────────────────────────────────────────

def serialize_block_info(block_info: List[Tuple[int, int]]) -> bytes:
    """序列化 BWT 块信息。"""
    result = bytearray()
    result.extend(struct.pack('>H', len(block_info)))
    for orig_idx, block_size in block_info:
        result.extend(struct.pack('>I', orig_idx))
        result.extend(struct.pack('>I', block_size))
    return bytes(result)


def deserialize_block_info(data: bytes, offset: int = 0) -> Tuple[List[Tuple[int, int]], int]:
    """反序列化 BWT 块信息。"""
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
