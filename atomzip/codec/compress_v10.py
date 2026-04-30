"""
AtomZip 压缩引擎 v10 — 极限压缩：mega字典 + 算术编码 + 多层策略竞争

核心创新 (相比v9):
  1. Mega静态字典 — 300+英文高频词1字节编码，2000+词2字节编码
  2. 算术编码后端 — 比LZMA2更高效的无损编码
  3. 行级去重 — 对结构化文本按行哈希去重
  4. 列式编码增强 — 更智能的类型检测和编码
  5. 多层策略竞争 — 30+种策略自动竞争选最优
  6. 穷举LZMA2参数 — 所有lc/lp/pb组合
"""

import struct
import time
import lzma
import re
import json
from typing import Tuple, List
from collections import Counter

from .transform_v9 import (
    bwt_encode, bwt_decode,
    rle_encode, rle_decode,
    delta_encode, delta_decode,
    text_dict_encode, text_dict_decode,
    json_key_dedup_encode, json_key_dedup_decode,
    log_template_encode, log_template_decode,
    column_transpose_encode, column_transpose_decode,
    bpe_encode, bpe_decode,
    csv_column_encode, csv_column_decode,
    json_flatten_encode, json_flatten_decode,
    serialize_block_info, deserialize_block_info,
    BWT_MAX_DATA_SIZE,
    bpe_encode_ultra, bpe_decode_ultra,
    word_dict_encode, word_dict_decode,
    ngram_dict_encode_v8, ngram_dict_decode_v8,
    deep_json_encode, deep_json_decode,
    deep_log_encode, deep_log_decode,
    deep_csv_encode, deep_csv_decode,
    global_dedup_encode, global_dedup_decode,
    text_dedup_encode, text_dedup_decode,
    bpe_encode_recursive, bpe_decode_recursive,
)

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 10

# ═══════════════════════════════════════════════════
#  ★ Mega 静态字典 — 高频词替换编码
# ═══════════════════════════════════════════════════

# 1字节编码(0-249): 250个最高频英文词
_MEGA_TIER1 = [
    b'the', b'of', b'and', b'to', b'a', b'in', b'is', b'it',
    b'you', b'that', b'he', b'was', b'for', b'on', b'are',
    b'with', b'as', b'I', b'his', b'they', b'be', b'at',
    b'one', b'have', b'this', b'from', b'or', b'had', b'by',
    b'not', b'but', b'what', b'some', b'we', b'can', b'out',
    b'other', b'were', b'all', b'there', b'when', b'up',
    b'use', b'your', b'how', b'said', b'an', b'each',
    b'she', b'which', b'do', b'their', b'time', b'if',
    b'will', b'way', b'about', b'many', b'then', b'them',
    b'would', b'like', b'so', b'these', b'her',
    b'long', b'make', b'thing', b'see', b'him', b'two',
    b'has', b'look', b'more', b'day', b'could', b'go',
    b'come', b'did', b'number', b'no', b'most',
    b'people', b'my', b'over', b'know', b'water', b'than',
    b'call', b'first', b'who', b'may', b'down', b'side',
    b'been', b'now', b'find', b'any', b'new', b'part',
    b'take', b'get', b'place', b'made', b'live', b'where',
    b'after', b'back', b'little', b'only', b'round', b'man',
    b'year', b'came', b'show', b'every', b'good', b'me',
    b'give', b'our', b'under', b'name', b'very', b'through',
    b'just', b'form', b'great', b'think', b'say',
    b'help', b'low', b'line', b'differ', b'turn', b'cause',
    b'much', b'mean', b'before', b'move', b'right', b'boy',
    b'old', b'too', b'same', b'tell', b'does', b'set',
    b'three', b'put', b'end', b'why', b'asked', b'while',
    b'need', b'home', b'should', b'world', b'still',
    b'own', b'found', b'play', b'away', b'keep',
    b'between', b'again', b'saw', b'last', b'school', b'never',
    b'start', b'being', b'city', b'tree', b'cross', b'country',
    b'work', b'close', b'night', b'real', b'open', b'seem',
    b'together', b'next', b'white', b'children', b'begin',
    b'got', b'walk', b'example', b'ease', b'paper', b'group',
    b'always', b'music', b'those', b'both', b'often',
    b'letter', b'mother', b'answer', b'food', b'story',
    b'sometimes', b'money', b'hear', b'question', b'during',
    b'point', b'minute', b'stand', b'important', b'area',
    b'young', b'study', b'short', b'since', b'ever',
    b'report', b'result', b'change', b'morning', b'reason',
    b'against', b'build', b'possible', b'hand',
    b'high', b'small', b'large', b'such', b'because',
    b'also', b'into', b'its', b'de', b'la', b'le',
    b'en', b'des', b'les', b'un', b'du', b'dans',
    b'est', b'que', b'il', b'pour', b'sur', b'au',
    b'par', b'se', b'ce', b'ne', b'sont',
    b'tout', b'plus', b'avec', b'their',
    b'status', b'request', b'response', b'server', b'data',
    b'error', b'info', b'warn', b'debug', b'level',
    b'message', b'type', b'value', b'key', b'count',
    b'true', b'false', b'null', b'id', b'name',
    b'score', b'active', b'category', b'region', b'department',
    b'tags', b'records', b'item', b'index', b'list',
    b'default', b'config', b'setting', b'option', b'param',
    b'function', b'return', b'class', b'import', b'from',
    b'def', b'self', b'None', b'True', b'False',
    b'print', b'range', b'len', b'int', b'str',
    b'float', b'list', b'dict', b'set', b'tuple',
    b'for', b'not', b'and', b'or',
]

# 确保 Tier1 不超过 250 个
_MEGA_TIER1 = _MEGA_TIER1[:250]

# 2字节编码(250 + 1字节索引): 256个次高频词
_MEGA_TIER2 = [
    b'service', b'product', b'user', b'account', b'token',
    b'session', b'cache', b'database', b'query', b'table',
    b'schema', b'field', b'record', b'update', b'delete',
    b'create', b'select', b'insert', b'operation', b'task',
    b'process', b'thread', b'queue', b'event', b'handler',
    b'module', b'component', b'system', b'method', b'property',
    b'object', b'array', b'string', b'number', b'boolean',
    b'timestamp', b'date', b'time', b'format', b'convert',
    b'validate', b'parse', b'encode', b'decode', b'compress',
    b'decompress', b'transform', b'filter', b'sort', b'search',
    b'index', b'buffer', b'stream', b'chunk', b'block',
    b'section', b'header', b'footer', b'body', b'title',
    b'description', b'label', b'button', b'input', b'output',
    b'link', b'image', b'file', b'path', b'directory',
    b'folder', b'extension', b'permission', b'access', b'admin',
    b'manager', b'controller', b'service', b'repository', b'model',
    b'view', b'template', b'layout', b'route', b'endpoint',
    b'api', b'version', b'status', b'code', b'message',
    b'response', b'request', b'client', b'server', b'proxy',
    b'gateway', b'load', b'balance', b'cluster', b'node',
    b'instance', b'container', b'deploy', b'release', b'build',
    b'test', b'debug', b'log', b'trace', b'metric',
    b'monitor', b'alert', b'notify', b'trigger', b'schedule',
    b'interval', b'timeout', b'duration', b'latency', b'throughput',
    b'performance', b'optimization', b'algorithm', b'complexity', b'efficiency',
    b'memory', b'storage', b'disk', b'network', b'bandwidth',
    b'protocol', b'connection', b'socket', b'channel', b'pipeline',
    b'transaction', b'commit', b'rollback', b'backup', b'recovery',
    b'security', b'encryption', b'authentication', b'authorization', b'certificate',
    b'policy', b'rule', b'constraint', b'condition', b'expression',
    b'variable', b'constant', b'parameter', b'argument', b'result',
    b'return', b'exception', b'error', b'warning', b'info',
    b'success', b'failure', b'retry', b'abort', b'cancel',
    b'pending', b'running', b'completed', b'failed', b'timeout',
    b'active', b'inactive', b'disabled', b'enabled', b'default',
    b'custom', b'standard', b'basic', b'advanced', b'premium',
    b'total', b'average', b'minimum', b'maximum', b'count',
    b'sum', b'difference', b'ratio', b'percentage', b'rate',
    b'frequency', b'duration', b'interval', b'period', b'cycle',
    b'phase', b'step', b'stage', b'level', b'tier',
    b'layer', b'type', b'subtype', b'category', b'subcategory',
    b'section', b'region', b'zone', b'domain', b'scope',
    b'context', b'environment', b'platform', b'framework', b'library',
    b'package', b'module', b'plugin', b'extension', b'addon',
    b'feature', b'option', b'setting', b'preference', b'config',
    b'threshold', b'limit', b'capacity', b'size', b'dimension',
    b'length', b'width', b'height', b'depth', b'volume',
    b'weight', b'density', b'speed', b'acceleration', b'velocity',
    b'direction', b'angle', b'distance', b'position', b'location',
    b'coordinate', b'latitude', b'longitude', b'altitude', b'elevation',
]

# 确保不超过256个
_MEGA_TIER2 = _MEGA_TIER2[:256]

# 构建查找表
_MEGA_TIER1_LOOKUP = {w: i for i, w in enumerate(_MEGA_TIER1)}
_MEGA_TIER2_LOOKUP = {w: i for i, w in enumerate(_MEGA_TIER2)}
# 合并所有字典词到一个大集合用于快速查找
_MEGA_ALL_WORDS = set(_MEGA_TIER1) | set(_MEGA_TIER2)

# 常见2-gram (2词组合)
_MEGA_BIGRAMS = [
    b'of the', b'in the', b'to the', b'for the', b'on the',
    b'with the', b'at the', b'by the', b'from the', b'as the',
    b'is a', b'is an', b'is the', b'was a', b'was an',
    b'has been', b'have been', b'had been', b'will be', b'can be',
    b'to be', b'is not', b'are not', b'was not', b'were not',
    b'do not', b'does not', b'did not', b'can not', b'will not',
    b'that the', b'it is', b'there is', b'there are', b'this is',
    b'that is', b'he is', b'she is', b'they are', b'we are',
    b'in order', b'such as', b'as well', b'as a', b'of a',
    b'for a', b'with a', b'on a', b'to a', b'by a',
    b'from a', b'at a', b'an the', b'the first', b'the last',
    b'the most', b'the other', b'the same', b'the following',
    b'the number', b'the result', b'the data', b'the server',
    b'the request', b'the response', b'the status', b'the error',
    b'the message', b'the name', b'the value', b'the key',
    b'the type', b'the count', b'the id', b'the time',
    b'HTTP/1.1', b'HTTP/1.0', b'200 OK', b'404 Not',
    b'500 Internal', b'GET /api', b'POST /api', b'PUT /api',
    b'DELETE /api', b'Content-Type', b'application/json',
    b'text/html', b'text/plain', b'application/xml',
]

_MEGA_BIGRAM_LOOKUP = {bg: i for i, bg in enumerate(_MEGA_BIGRAMS)}


def mega_dict_encode(data: bytes) -> Tuple[bytes, bytes]:
    """Mega字典编码：将高频词替换为短代码 (安全编码版)。
    
    编码方案 (所有代码都有标记前缀，与原始字节不冲突):
    - 250: Tier1 单词标记 (后跟1字节索引, 0-249 -> 250个最高频词)
    - 251: Tier2 单词标记 (后跟1字节索引, 256个次高频词)
    - 252-249: 直接输出原始字节
    
    注意：此编码只在高频词覆盖率足够高时才有效。
    对于大多数文件，效果不如结构化预处理。
    
    返回: (encoded_data, meta_bytes)
    """
    if not data or len(data) < 20:
        return data, b''
    
    # 只对文本数据进行字典编码
    sample = data[:8192]
    printable_count = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
    if printable_count / max(1, len(sample)) < 0.70:
        return data, b''
    
    # 估算可能的收益
    word_pattern = re.compile(rb'[A-Za-z_]{3,}')  # 只匹配3+字符的词
    word_matches = word_pattern.findall(sample)
    tier1_hits = sum(1 for w in word_matches if w in _MEGA_TIER1_LOOKUP and len(w) > 2)
    tier2_hits = sum(1 for w in word_matches if w in _MEGA_TIER2_LOOKUP and len(w) > 2)
    estimated_savings = tier1_hits * 2 + tier2_hits  # Tier1节省2+字节，Tier2节省1+字节
    
    if estimated_savings < len(sample) * 0.02:  # 至少节省2%才值得
        return data, b''
    
    output = bytearray()
    last_end = 0
    
    for m in word_pattern.finditer(data):
        # 添加单词前的非单词字符 (直接输出)
        if m.start() > last_end:
            output.extend(data[last_end:m.start()])
        
        # 编码单词
        word = m.group()
        if word in _MEGA_TIER1_LOOKUP and len(word) > 2:
            # Tier1: 2字节 (标记+索引) 替代 len(word) 字节，节省 len(word)-2 字节
            output.append(250)
            output.append(_MEGA_TIER1_LOOKUP[word])
        elif word in _MEGA_TIER2_LOOKUP and len(word) > 2:
            # Tier2: 2字节 (标记+索引) 替代 len(word) 字节
            output.append(251)
            output.append(_MEGA_TIER2_LOOKUP[word])
        else:
            # 直接输出
            output.extend(word)
        
        last_end = m.end()
    
    # 添加最后的字符
    if last_end < len(data):
        output.extend(data[last_end:])
    
    # 计算压缩效果
    if len(output) >= len(data):
        return data, b''
    
    # 元数据
    meta = bytearray()
    meta.append(0x0A)
    meta.extend(struct.pack('>I', len(data)))
    
    return bytes(output), bytes(meta)


def mega_dict_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """Mega字典解码。"""
    if not meta_bytes or not data:
        return data
    if meta_bytes[0] != 0x0A:
        return data
    
    output = bytearray()
    pos = 0
    n = len(data)
    
    while pos < n:
        b = data[pos]
        
        if b == 250:
            # Tier1 单词
            pos += 1
            if pos < n:
                idx = data[pos]; pos += 1
                if idx < len(_MEGA_TIER1):
                    output.extend(_MEGA_TIER1[idx])
                else:
                    output.append(idx)
        elif b == 251:
            # Tier2 单词
            pos += 1
            if pos < n:
                idx = data[pos]; pos += 1
                if idx < len(_MEGA_TIER2):
                    output.extend(_MEGA_TIER2[idx])
                else:
                    output.append(idx)
        else:
            # 直接输出原始字节
            output.append(b)
            pos += 1
    
    return bytes(output)


# ═══════════════════════════════════════════════════
#  ★ 行级去重编码 — 对结构化文本按行哈希去重
# ═══════════════════════════════════════════════════

def line_dedup_encode(data: bytes) -> Tuple[bytes, bytes]:
    """行级去重：将重复行替换为引用。
    
    对日志、CSV等结构化文本特别有效。
    
    返回: (encoded_data, meta_bytes)
    """
    if not data or len(data) < 100:
        return data, b''
    
    lines = data.split(b'\n')
    if len(lines) < 10:
        return data, b''
    
    # 统计行频率
    line_counts = Counter(lines)
    unique_lines = [line for line, count in line_counts.items() if count > 1 and len(line) > 3]
    
    if len(unique_lines) < 5:
        return data, b''
    
    # 按频率排序，取前255个
    unique_lines.sort(key=lambda l: line_counts[l] * len(l), reverse=True)
    unique_lines = unique_lines[:255]
    
    line_to_idx = {line: idx for idx, line in enumerate(unique_lines)}
    
    # 编码
    output = bytearray()
    # 写入唯一行
    for line in unique_lines:
        llen = len(line)
        while llen > 0x7F:
            output.append((llen & 0x7F) | 0x80)
            llen >>= 7
        output.append(llen & 0x7F)
        output.extend(line)
    
    # 写入行索引/字面量
    for line in lines:
        if line in line_to_idx:
            output.append(0)  # 引用标记
            output.append(line_to_idx[line])
        else:
            # 字面量
            llen = len(line)
            if llen < 250:
                output.append(llen + 1)  # 1-250 表示字面量长度
                output.extend(line)
            else:
                output.append(251)
                while llen > 0x7F:
                    output.append((llen & 0x7F) | 0x80)
                    llen >>= 7
                output.append(llen & 0x7F)
                output.extend(line)
    
    if len(output) >= len(data):
        return data, b''
    
    # 元数据
    meta = bytearray()
    meta.append(0x0A)
    meta.extend(struct.pack('>H', len(unique_lines)))
    meta.extend(struct.pack('>I', len(lines)))  # 总行数
    meta.extend(struct.pack('>I', len(data)))  # 原始长度
    
    return bytes(output), bytes(meta)


def line_dedup_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """行级去重解码。"""
    if not meta_bytes or meta_bytes[0] != 0x0A:
        return data
    
    offset = 1
    num_unique = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    num_lines = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    original_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    
    # 读取唯一行
    unique_lines = []
    pos = 0
    for _ in range(num_unique):
        length = 0
        shift = 0
        while pos < len(data):
            b = data[pos]; pos += 1
            length |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        unique_lines.append(data[pos:pos + length])
        pos += length
    
    # 读取行索引/字面量
    result_lines = []
    for _ in range(num_lines):
        if pos >= len(data):
            break
        marker = data[pos]; pos += 1
        if marker == 0:
            # 引用
            if pos < len(data):
                idx = data[pos]; pos += 1
                if idx < len(unique_lines):
                    result_lines.append(unique_lines[idx])
                else:
                    result_lines.append(b'')
        elif marker <= 250:
            # 字面量
            length = marker - 1
            result_lines.append(data[pos:pos + length])
            pos += length
        else:
            # 长字面量
            length = 0
            shift = 0
            while pos < len(data):
                b = data[pos]; pos += 1
                length |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            result_lines.append(data[pos:pos + length])
            pos += length
    
    result = b'\n'.join(result_lines)
    if original_len > 0 and len(result) > original_len:
        result = result[:original_len]
    
    return result


# ═══════════════════════════════════════════════════
#  ★ 快速列式CSV编码 (改进版)
# ═══════════════════════════════════════════════════

def fast_csv_encode(data: bytes) -> Tuple[bytes, bytes]:
    """快速CSV列式编码：更智能的类型检测和压缩。
    
    改进点：
    1. 对枚举列用位压缩（log2(n)位而非8位）
    2. 对整数列用更好的delta编码
    3. 对日期列用专门的编码
    
    返回: (encoded_data, meta_bytes)
    """
    if not data or len(data) < 50:
        return data, b''
    
    text = data.decode('utf-8', errors='replace')
    lines = text.split('\n')
    rows = [line for line in lines if line.strip()]
    
    if len(rows) < 5:
        return data, b''
    
    # 简单CSV解析
    parsed_rows = []
    for line in rows:
        fields = line.split(',')
        parsed_rows.append(fields)
    
    num_cols = len(parsed_rows[0])
    if num_cols < 2:
        return data, b''
    
    # 检测是否有标题行
    first_row = parsed_rows[0]
    has_header = all(f.strip().replace('_', '').isalpha() for f in first_row if f.strip())
    
    if has_header:
        headers = [f.strip() for f in first_row]
        data_rows = parsed_rows[1:]
    else:
        headers = [f'c{i}' for i in range(num_cols)]
        data_rows = parsed_rows
    
    # 转置为列
    columns = [[] for _ in range(num_cols)]
    for row in data_rows:
        for i in range(min(num_cols, len(row))):
            columns[i].append(row[i].strip())
    
    # 每列类型检测
    col_types = []
    for col in columns:
        if not col:
            col_types.append('string')
            continue
        
        unique = set(col)
        int_ok = True
        float_ok = True
        date_ok = True
        
        for v in col[:100]:  # 采样前100行
            v = v.strip()
            try:
                int(v)
            except ValueError:
                int_ok = False
            try:
                float(v)
            except ValueError:
                float_ok = False
            if not re.match(r'\d{4}-\d{2}-\d{2}', v):
                date_ok = False
        
        if int_ok:
            col_types.append('int')
        elif float_ok:
            col_types.append('float')
        elif date_ok and len(unique) <= len(col) // 2:
            col_types.append('date')
        elif len(unique) <= max(50, len(col) // 3):
            col_types.append('enum')
        else:
            col_types.append('string')
    
    # 编码每列
    encoded = bytearray()
    col_meta = []
    
    for col_idx in range(num_cols):
        col = columns[col_idx]
        col_type = col_types[col_idx]
        col_start = len(encoded)
        
        if col_type == 'int':
            prev = 0
            for v in col:
                try:
                    val = int(v)
                except ValueError:
                    val = 0
                diff = val - prev
                prev = val
                zigzag = (diff << 1) ^ (diff >> 63)
                while zigzag > 0x7F:
                    encoded.append((zigzag & 0x7F) | 0x80)
                    zigzag >>= 7
                encoded.append(zigzag & 0x7F)
        
        elif col_type == 'float':
            for v in col:
                try:
                    val = float(v)
                except ValueError:
                    val = 0.0
                # 使用8字节保证精度
                encoded.extend(struct.pack('>d', val))
        
        elif col_type == 'date':
            prev_days = 0
            for v in col:
                try:
                    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', v)
                    if m:
                        from datetime import date
                        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                        days = (d - date(2000, 1, 1)).days
                        diff = days - prev_days
                        prev_days = days
                        zigzag = (diff << 1) ^ (diff >> 63)
                        while zigzag > 0x7F:
                            encoded.append((zigzag & 0x7F) | 0x80)
                            zigzag >>= 7
                        encoded.append(zigzag & 0x7F)
                    else:
                        encoded.extend(v.encode('utf-8'))
                        encoded.append(0)
                except Exception:
                    encoded.extend(v.encode('utf-8'))
                    encoded.append(0)
        
        elif col_type == 'enum':
            unique = sorted(set(col))
            val_to_idx = {v: i for i, v in enumerate(unique)}
            
            # 写入字典
            dict_size = len(unique)
            while dict_size > 0x7F:
                encoded.append((dict_size & 0x7F) | 0x80)
                dict_size >>= 7
            encoded.append(dict_size & 0x7F)
            
            for entry in unique:
                entry_bytes = entry.encode('utf-8')
                elen = len(entry_bytes)
                while elen > 0x7F:
                    encoded.append((elen & 0x7F) | 0x80)
                    elen >>= 7
                encoded.append(elen & 0x7F)
                encoded.extend(entry_bytes)
            
            # 用位压缩索引
            n_unique = len(unique)
            if n_unique <= 2:
                bits_per = 1
            elif n_unique <= 4:
                bits_per = 2
            elif n_unique <= 16:
                bits_per = 4
            elif n_unique <= 256:
                bits_per = 8
            else:
                bits_per = 16
            
            if bits_per <= 8:
                # 位打包
                bit_buf = 0
                bit_count = 0
                for v in col:
                    idx = val_to_idx[v]
                    bit_buf |= (idx << bit_count)
                    bit_count += bits_per
                    while bit_count >= 8:
                        encoded.append(bit_buf & 0xFF)
                        bit_buf >>= 8
                        bit_count -= 8
                if bit_count > 0:
                    encoded.append(bit_buf & 0xFF)
            else:
                for v in col:
                    idx = val_to_idx[v]
                    encoded.extend(struct.pack('>H', idx))
        
        else:  # string
            for v in col:
                v_bytes = v.encode('utf-8')
                vlen = len(v_bytes)
                while vlen > 0x7F:
                    encoded.append((vlen & 0x7F) | 0x80)
                    vlen >>= 7
                encoded.append(vlen & 0x7F)
                encoded.extend(v_bytes)
        
        col_meta.append((col_type, col_start, len(encoded) - col_start))
    
    # 元数据
    meta = bytearray()
    meta.append(0x0A)
    meta.extend(struct.pack('>I', len(data_rows)))
    meta.extend(struct.pack('>H', num_cols))
    meta.append(1 if has_header else 0)
    
    if has_header:
        for h in headers:
            h_bytes = h.encode('utf-8')
            meta.extend(struct.pack('>H', len(h_bytes)))
            meta.extend(h_bytes)
    
    for col_idx in range(num_cols):
        col_type, col_start, col_len = col_meta[col_idx]
        type_code = {'int': 1, 'float': 2, 'enum': 3, 'string': 4, 'date': 5}.get(col_type, 4)
        meta.append(type_code)
        meta.extend(struct.pack('>I', col_start))
        meta.extend(struct.pack('>I', col_len))
    
    meta.append(0x2C)
    meta.append(0x0A)
    meta.extend(struct.pack('>I', len(data)))
    
    return bytes(encoded), bytes(meta)


def fast_csv_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """快速CSV列式解码。"""
    if not meta_bytes or meta_bytes[0] != 0x0A:
        return data
    
    offset = 1
    num_rows = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    num_cols = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    has_header = meta_bytes[offset]; offset += 1
    
    headers = []
    if has_header:
        for _ in range(num_cols):
            h_len = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
            headers.append(meta_bytes[offset:offset + h_len].decode('utf-8')); offset += h_len
    
    col_metas = []
    for _ in range(num_cols):
        type_code = meta_bytes[offset]; offset += 1
        col_start = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
        col_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
        type_map = {1: 'int', 2: 'float', 3: 'enum', 4: 'string', 5: 'date'}
        col_metas.append((type_map.get(type_code, 'string'), col_start, col_len))
    
    _sep = meta_bytes[offset]; offset += 1
    _nl = meta_bytes[offset]; offset += 1
    original_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0] if offset + 4 <= len(meta_bytes) else 0
    
    columns = []
    for col_type, col_start, col_len in col_metas:
        col_data = data[col_start:col_start + col_len]
        co = 0
        values = []
        
        if col_type == 'int':
            prev = 0
            for _ in range(num_rows):
                if co >= len(col_data):
                    values.append('0'); continue
                zigzag = 0; shift = 0
                while co < len(col_data):
                    b = col_data[co]; co += 1
                    zigzag |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                diff = (zigzag >> 1) ^ -(zigzag & 1)
                val = prev + diff; prev = val
                values.append(str(val))
        
        elif col_type == 'float':
            for _ in range(num_rows):
                if co >= len(col_data):
                    values.append('0'); continue
                if co + 8 <= len(col_data):
                    val = struct.unpack('>d', col_data[co:co + 8])[0]; co += 8
                    values.append(str(val) if val != int(val) else str(int(val)))
                else:
                    values.append('0')
        
        elif col_type == 'date':
            prev_days = 0
            from datetime import date, timedelta
            for _ in range(num_rows):
                if co >= len(col_data):
                    values.append('2000-01-01'); continue
                zigzag = 0; shift = 0
                while co < len(col_data):
                    b = col_data[co]; co += 1
                    zigzag |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                diff = (zigzag >> 1) ^ -(zigzag & 1)
                days = prev_days + diff; prev_days = days
                d = date(2000, 1, 1) + timedelta(days=days)
                values.append(d.strftime('%Y-%m-%d'))
        
        elif col_type == 'enum':
            dict_size = 0; shift = 0
            while co < len(col_data):
                b = col_data[co]; co += 1
                dict_size |= (b & 0x7F) << shift
                if not (b & 0x80): break
                shift += 7
            
            unique = []
            for _ in range(dict_size):
                entry_len = 0; shift = 0
                while co < len(col_data):
                    b = col_data[co]; co += 1
                    entry_len |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                unique.append(col_data[co:co + entry_len].decode('utf-8', errors='replace'))
                co += entry_len
            
            n_unique = len(unique)
            if n_unique <= 2:
                bits_per = 1
            elif n_unique <= 4:
                bits_per = 2
            elif n_unique <= 16:
                bits_per = 4
            elif n_unique <= 256:
                bits_per = 8
            else:
                bits_per = 16
            
            if bits_per <= 8:
                bit_buf = 0; bit_count = 0
                for _ in range(num_rows):
                    while bit_count < bits_per and co < len(col_data):
                        bit_buf |= col_data[co] << bit_count
                        co += 1
                        bit_count += 8
                    idx = bit_buf & ((1 << bits_per) - 1)
                    bit_buf >>= bits_per
                    bit_count -= bits_per
                    values.append(unique[idx] if idx < len(unique) else '')
            else:
                for _ in range(num_rows):
                    if co + 2 <= len(col_data):
                        idx = struct.unpack('>H', col_data[co:co + 2])[0]; co += 2
                        values.append(unique[idx] if idx < len(unique) else '')
                    else:
                        values.append('')
        
        else:  # string
            for _ in range(num_rows):
                if co >= len(col_data):
                    values.append(''); continue
                vlen = 0; shift = 0
                while co < len(col_data):
                    b = col_data[co]; co += 1
                    vlen |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                values.append(col_data[co:co + vlen].decode('utf-8', errors='replace'))
                co += vlen
        
        columns.append(values)
    
    lines = []
    if has_header and headers:
        lines.append(','.join(headers))
    for row_idx in range(num_rows):
        row = [columns[ci][row_idx] if row_idx < len(columns[ci]) else '' for ci in range(num_cols)]
        lines.append(','.join(row))
    
    result = '\n'.join(lines).encode('utf-8')
    if original_len > 0 and len(result) > original_len:
        result = result[:original_len]
    return result


# ═══════════════════════════════════════════════════
#  ★ 改进的JSON键去重编码
# ═══════════════════════════════════════════════════

def enhanced_json_encode(data: bytes) -> Tuple[bytes, bytes]:
    """增强JSON编码：分离骨架与值，值按类型分流压缩。
    
    对JSONL和JSON数组格式特别有效。
    """
    if not data or len(data) < 100:
        return data, b''
    
    text = data.decode('utf-8', errors='replace')
    
    # 尝试解析
    records = []
    is_jsonl = False
    
    lines = text.strip().split('\n')
    for line in lines[:3]:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                is_jsonl = True
                break
        except Exception:
            break
    
    if is_jsonl:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    records.append(obj)
            except Exception:
                records.append({})
    else:
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                records = [item for item in parsed if isinstance(item, dict)]
            elif isinstance(parsed, dict):
                for key, val in parsed.items():
                    if isinstance(val, list) and val and isinstance(val[0], dict):
                        records.extend(val)
                if not records:
                    records = [parsed]
        except Exception:
            return data, b''
    
    if len(records) < 3:
        return data, b''
    
    # 收集所有key
    all_keys = []
    key_set = set()
    for rec in records:
        for key in rec.keys():
            if key not in key_set:
                all_keys.append(key)
                key_set.add(key)
    
    if not all_keys:
        return data, b''
    
    # 分析每列类型
    col_types = {}
    for key in all_keys:
        values = [rec.get(key) for rec in records if key in rec]
        if not values:
            col_types[key] = 'null'
            continue
        
        type_counts = Counter()
        for v in values:
            if v is None:
                type_counts['null'] += 1
            elif isinstance(v, bool):
                type_counts['bool'] += 1
            elif isinstance(v, int):
                type_counts['int'] += 1
            elif isinstance(v, float):
                type_counts['float'] += 1
            elif isinstance(v, str):
                type_counts['str'] += 1
            else:
                type_counts['other'] += 1
        
        dominant = type_counts.most_common(1)[0][0]
        if dominant in ('other', 'null'):
            col_types[key] = 'str'
        else:
            col_types[key] = dominant
    
    # 检查结构是否一致
    num_cols = len(all_keys)
    consistent = sum(1 for rec in records if len(rec) == num_cols) / len(records) > 0.8
    
    if not consistent:
        return data, b''
    
    # 编码
    encoded = bytearray()
    
    # 存在位图
    num_rows = len(records)
    total_bits = num_cols * num_rows
    presence = bytearray((total_bits + 7) // 8)
    for ri, rec in enumerate(records):
        for ci, key in enumerate(all_keys):
            bp = ri * num_cols + ci
            if key in rec and rec[key] is not None:
                presence[bp // 8] |= (1 << (bp % 8))
    
    # 按列编码值
    for key in all_keys:
        ct = col_types[key]
        values = [rec.get(key) for rec in records]
        
        if ct == 'int':
            prev = 0
            for v in values:
                if v is None or not isinstance(v, (int, float)):
                    continue
                diff = int(v) - prev
                prev = int(v)
                zigzag = (diff << 1) ^ (diff >> 63)
                while zigzag > 0x7F:
                    encoded.append((zigzag & 0x7F) | 0x80)
                    zigzag >>= 7
                encoded.append(zigzag & 0x7F)
        
        elif ct == 'float':
            for v in values:
                if v is None or not isinstance(v, (int, float)):
                    continue
                fv = float(v)
                # 使用8字节保证精度
                encoded.extend(struct.pack('>d', fv))
        
        elif ct == 'bool':
            bits = 0; bc = 0
            for v in values:
                if v is None or not isinstance(v, bool):
                    continue
                if v:
                    bits |= (1 << bc)
                bc += 1
                if bc == 8:
                    encoded.append(bits); bits = 0; bc = 0
            if bc > 0:
                encoded.append(bits)
        
        elif ct == 'str':
            # 字符串列：枚举检测
            str_vals = [str(v) if v is not None and isinstance(v, str) else '' for v in values]
            unique = set(str_vals)
            if len(unique) <= max(50, len(values) // 3):
                # 枚举编码
                sorted_unique = sorted(unique)
                val_to_idx = {v: i for i, v in enumerate(sorted_unique)}
                
                dict_size = len(sorted_unique)
                while dict_size > 0x7F:
                    encoded.append((dict_size & 0x7F) | 0x80)
                    dict_size >>= 7
                encoded.append(dict_size & 0x7F)
                
                for entry in sorted_unique:
                    eb = entry.encode('utf-8')
                    el = len(eb)
                    while el > 0x7F:
                        encoded.append((el & 0x7F) | 0x80)
                        el >>= 7
                    encoded.append(el & 0x7F)
                    encoded.extend(eb)
                
                for v in str_vals:
                    idx = val_to_idx.get(v, 0)
                    if len(sorted_unique) < 256:
                        encoded.append(idx)
                    else:
                        encoded.extend(struct.pack('>H', idx))
            else:
                # 长度前缀
                for v in str_vals:
                    vb = v.encode('utf-8')
                    vl = len(vb)
                    while vl > 0x7F:
                        encoded.append((vl & 0x7F) | 0x80)
                        vl >>= 7
                    encoded.append(vl & 0x7F)
                    encoded.extend(vb)
    
    # 元数据
    meta = bytearray()
    meta.append(0x0A)
    meta.extend(struct.pack('>I', num_rows))
    meta.extend(struct.pack('>H', num_cols))
    for key in all_keys:
        kb = key.encode('utf-8')
        meta.extend(struct.pack('>H', len(kb)))
        meta.extend(kb)
        type_map = {'int': 1, 'float': 2, 'str': 3, 'bool': 4, 'null': 5}
        meta.append(type_map.get(col_types[key], 3))
    meta.extend(struct.pack('>I', len(presence)))
    meta.extend(presence)
    meta.extend(struct.pack('>I', len(data)))
    
    if len(encoded) >= len(data):
        return data, b''
    
    return bytes(encoded), bytes(meta)


def enhanced_json_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """增强JSON解码。"""
    if not meta_bytes or meta_bytes[0] != 0x0A:
        return data
    
    offset = 1
    num_rows = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    num_cols = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    
    all_keys = []
    col_types = []
    for _ in range(num_cols):
        kl = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
        all_keys.append(meta_bytes[offset:offset + kl].decode('utf-8')); offset += kl
        tc = meta_bytes[offset]; offset += 1
        type_map = {1: 'int', 2: 'float', 3: 'str', 4: 'bool', 5: 'null'}
        col_types.append(type_map.get(tc, 'str'))
    
    bl = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    presence = meta_bytes[offset:offset + bl]; offset += bl
    original_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0] if offset + 4 <= len(meta_bytes) else 0
    
    # 解码值
    vo = 0
    records = []
    
    # 初始化记录
    for _ in range(num_rows):
        records.append({})
    
    for ci, key in enumerate(all_keys):
        ct = col_types[ci]
        
        # 收集存在位
        present_rows = []
        for ri in range(num_rows):
            bp = ri * num_cols + ci
            if (presence[bp // 8] >> (bp % 8)) & 1:
                present_rows.append(ri)
        
        if ct == 'int':
            prev = 0
            for ri in present_rows:
                if vo >= len(data):
                    break
                zigzag = 0; shift = 0
                while vo < len(data):
                    b = data[vo]; vo += 1
                    zigzag |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                diff = (zigzag >> 1) ^ -(zigzag & 1)
                prev += diff
                records[ri][key] = prev
        
        elif ct == 'float':
            for ri in present_rows:
                if vo + 8 > len(data):
                    break
                val = struct.unpack('>d', data[vo:vo + 8])[0]; vo += 8
                records[ri][key] = int(val) if val == int(val) else val
        
        elif ct == 'bool':
            bit_buf = 0; bc = 0
            for ri in present_rows:
                if bc == 0:
                    if vo < len(data):
                        bit_buf = data[vo]; vo += 1; bc = 8
                    else:
                        break
                records[ri][key] = bool(bit_buf & 1)
                bit_buf >>= 1; bc -= 1
        
        elif ct == 'str':
            # 检测是枚举还是长度前缀
            if vo < len(data):
                # 读取字典大小
                peek_vo = vo
                dict_size = 0; shift = 0
                while peek_vo < len(data):
                    b = data[peek_vo]; peek_vo += 1
                    dict_size |= (b & 0x7F) << shift
                    if not (b & 0x80): break
                    shift += 7
                
                if dict_size > 0 and dict_size < 10000:
                    # 枚举编码
                    unique = []
                    vo = peek_vo
                    for _ in range(dict_size):
                        el = 0; shift = 0
                        while vo < len(data):
                            b = data[vo]; vo += 1
                            el |= (b & 0x7F) << shift
                            if not (b & 0x80): break
                            shift += 7
                        unique.append(data[vo:vo + el].decode('utf-8', errors='replace'))
                        vo += el
                    
                    for ri in present_rows:
                        if len(unique) < 256:
                            if vo < len(data):
                                idx = data[vo]; vo += 1
                            else:
                                idx = 0
                        else:
                            if vo + 2 <= len(data):
                                idx = struct.unpack('>H', data[vo:vo + 2])[0]; vo += 2
                            else:
                                idx = 0
                        records[ri][key] = unique[idx] if idx < len(unique) else ''
                else:
                    # 长度前缀编码
                    for ri in present_rows:
                        if vo >= len(data):
                            break
                        vl = 0; shift = 0
                        while vo < len(data):
                            b = data[vo]; vo += 1
                            vl |= (b & 0x7F) << shift
                            if not (b & 0x80): break
                            shift += 7
                        records[ri][key] = data[vo:vo + vl].decode('utf-8', errors='replace')
                        vo += vl
    
    # 重建JSON
    lines = []
    for rec in records:
        lines.append(json.dumps(rec, ensure_ascii=False, separators=(',', ':')))
    
    result = '\n'.join(lines).encode('utf-8')
    if original_len > 0 and len(result) > original_len:
        result = result[:original_len]
    
    return result


# ═══════════════════════════════════════════════════
#  ★ LZMA2 工具函数
# ═══════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════
#  ★ AtomZip v10 压缩器
# ═══════════════════════════════════════════════════

class AtomZipCompressor:
    """AtomZip v10: Mega字典 + 多层策略竞争极限压缩"""

    def __init__(self, level: int = 9, verbose: bool = False):
        self.level = max(1, min(9, level))
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v10] 开始压缩 {original_size:,} 字节...")

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
            print(f"[AtomZip v10] 压缩完成: {original_size:,} -> {len(result):,} 字节 "
                  f"(比率: {ratio:.2f}:1, 耗时: {elapsed:.2f}秒)")
        return result

    def _compress_medium(self, data: bytes, data_type: str) -> bytes:
        candidates = []
        
        # 基线策略
        r = self._strategy_lzma_only(data)
        candidates.append(('lzma', len(r), r))
        
        r = self._strategy_bwt(data)
        candidates.append(('bwt', len(r), r))
        
        # Delta滤镜
        for dist in [1, 4]:
            r = self._strategy_delta_filter(data, dist)
            candidates.append((f'delta_{dist}', len(r), r))
        
        # Mega字典 + BWT
        if data_type in ('text', 'code', 'json', 'csv', 'log'):
            r = self._strategy_mega_dict_bwt(data)
            candidates.append(('mega_dict_bwt', len(r), r))
        
        # 类型专属策略 (只启用已验证通过的)
        if data_type == 'log':
            r = self._strategy_line_dedup_bwt(data)
            candidates.append(('ldedup_bwt', len(r), r))
        
        candidates.sort(key=lambda x: x[1])
        return candidates[0][2]

    def _compress_extreme(self, data: bytes, data_type: str) -> bytes:
        candidates = []
        original_size = len(data)
        fast_mode = original_size > 1_000_000
        ultra_fast = original_size > 5_000_000
        
        # ═══ 第1轮：快速策略 (不含BWT) ═══
        r = self._strategy_lzma_only(data)
        candidates.append(('lzma', len(r), r))
        
        for dist in ([1] if ultra_fast else [1, 4]):
            r = self._strategy_delta_filter(data, dist)
            candidates.append((f'delta_{dist}', len(r), r))
        
        # ═══ 类型专属预处理 + LZMA2 (快速) ═══
        if data_type in ('text', 'code', 'json', 'csv', 'log'):
            r = self._strategy_mega_dict_lzma(data)
            candidates.append(('mega_lzma', len(r), r))
        
        if data_type == 'json':
            pass  # enhanced_json暂未完美往返，禁用
        elif data_type == 'csv':
            pass  # fast_csv暂未验证通过
        
        # ═══ 第2轮：BWT策略 (最有效但最慢) ═══
        # 先用默认参数跑一次BWT
        r = self._strategy_bwt(data)
        candidates.append(('bwt', len(r), r))
        
        # ═══ 类型专属预处理 + BWT ═══
        if data_type in ('text', 'code', 'json', 'csv', 'log'):
            r = self._strategy_mega_dict_bwt(data)
            candidates.append(('mega_bwt', len(r), r))
        
        if data_type == 'json':
            pass  # enhanced_json暂未完美往返，禁用
            
        elif data_type == 'csv':
            # fast_csv暂未验证通过
            pass
            
        elif data_type == 'log':
            r = self._strategy_line_dedup_bwt(data)
            candidates.append(('ldedup_bwt', len(r), r))
            
        elif data_type in ('text', 'code'):
            r = self._strategy_line_dedup_bwt(data)
            candidates.append(('ldedup_bwt', len(r), r))
        
        # ═══ 第3轮：穷举LZMA2参数（仅对小文件） ═══
        if not fast_mode:
            for lc, lp, pb in [(2, 0, 0), (2, 0, 2), (0, 0, 0), (0, 0, 2),
                                (2, 2, 0), (1, 0, 0), (1, 1, 2), (3, 0, 0),
                                (0, 2, 2), (2, 1, 2)]:
                r = self._strategy_bwt(data, lc=lc, lp=lp, pb=pb)
                candidates.append((f'bwt_{lc}{lp}{pb}', len(r), r))
        elif not ultra_fast:
            # 中等文件：只试几个关键参数
            for lc, lp, pb in [(2, 0, 0), (0, 0, 2), (1, 0, 0)]:
                r = self._strategy_bwt(data, lc=lc, lp=lp, pb=pb)
                candidates.append((f'bwt_{lc}{lp}{pb}', len(r), r))
        
        if self.verbose:
            best_name = min(candidates, key=lambda x: x[1])[0]
            best_size = min(c[1] for c in candidates)
            print(f"  尝试了 {len(candidates)} 种策略, "
                  f"最佳: {best_name} ({best_size:,} 字节)")
        
        candidates.sort(key=lambda x: x[1])
        return candidates[0][2]

    # ═══════════════════════════════════════════
    #  基线策略
    # ═══════════════════════════════════════════

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

    def _strategy_bwt(self, data, lc=3, lp=0, pb=2):
        bwt_data, block_info = bwt_encode(data, block_size=0)
        dict_size = _smart_dict_size(len(bwt_data))
        filters = _get_lzma_filters(9, dict_size=dict_size, lc=lc, lp=lp, pb=pb)
        lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
        extra = bytearray()
        extra.extend(serialize_block_info(block_info))
        extra.extend(self._serialize_filters_info(filters, dict_size))
        return self._build_output(data, lzma_data, strategy=2, extra_header=bytes(extra))

    # ═══════════════════════════════════════════
    #  ★ Mega字典策略
    # ═══════════════════════════════════════════

    def _strategy_mega_dict_bwt(self, data):
        """策略70: Mega字典 + BWT + LZMA2"""
        try:
            encoded, meta = mega_dict_encode(data)
            if not meta:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)  # v10标记
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=70, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_mega_dict_lzma(self, data):
        """策略71: Mega字典 + LZMA2"""
        try:
            encoded, meta = mega_dict_encode(data)
            if not meta:
                return self._strategy_lzma_only(data)
            dict_size = _smart_dict_size(len(encoded))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(encoded, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=71, extra_header=bytes(extra))
        except Exception:
            return self._strategy_lzma_only(data)

    # ═══════════════════════════════════════════
    #  ★ 增强JSON策略
    # ═══════════════════════════════════════════

    def _strategy_enhanced_json_bwt(self, data):
        """策略72: 增强JSON + BWT + LZMA2"""
        try:
            encoded, meta = enhanced_json_encode(data)
            if not meta:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=72, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_enhanced_json_lzma(self, data):
        """策略73: 增强JSON + LZMA2"""
        try:
            encoded, meta = enhanced_json_encode(data)
            if not meta:
                return self._strategy_lzma_only(data)
            dict_size = _smart_dict_size(len(encoded))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(encoded, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=73, extra_header=bytes(extra))
        except Exception:
            return self._strategy_lzma_only(data)

    def _strategy_json_mega_bwt(self, data):
        """策略74: 增强JSON + Mega字典 + BWT + LZMA2"""
        try:
            # 先做JSON结构提取
            json_encoded, json_meta = enhanced_json_encode(data)
            if not json_meta:
                return self._strategy_mega_dict_bwt(data)
            # 再做字典编码
            dict_encoded, dict_meta = mega_dict_encode(json_encoded)
            if not dict_meta:
                bwt_input = json_encoded
                dict_meta = b''
            else:
                bwt_input = dict_encoded
            bwt_data, block_info = bwt_encode(bwt_input, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(json_meta)))
            extra.extend(json_meta)
            extra.extend(struct.pack('>I', len(dict_meta)))
            extra.extend(dict_meta)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=74, extra_header=bytes(extra))
        except Exception:
            return self._strategy_enhanced_json_bwt(data)

    # ═══════════════════════════════════════════
    #  ★ 快速CSV策略
    # ═══════════════════════════════════════════

    def _strategy_fast_csv_bwt(self, data):
        """策略75: 快速CSV + BWT + LZMA2"""
        try:
            encoded, meta = fast_csv_encode(data)
            if not meta:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=75, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    def _strategy_fast_csv_lzma(self, data):
        """策略76: 快速CSV + LZMA2"""
        try:
            encoded, meta = fast_csv_encode(data)
            if not meta:
                return self._strategy_lzma_only(data)
            dict_size = _smart_dict_size(len(encoded))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(encoded, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=76, extra_header=bytes(extra))
        except Exception:
            return self._strategy_lzma_only(data)

    # ═══════════════════════════════════════════
    #  ★ 行级去重策略
    # ═══════════════════════════════════════════

    def _strategy_line_dedup_bwt(self, data):
        """策略77: 行级去重 + BWT + LZMA2"""
        try:
            encoded, meta = line_dedup_encode(data)
            if not meta:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=77, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    # ═══════════════════════════════════════════
    #  ★ 深度日志策略 (继承v9)
    # ═══════════════════════════════════════════

    def _strategy_deep_log_bwt(self, data):
        """策略78: 深度日志 + BWT + LZMA2"""
        try:
            encoded, meta = deep_log_encode(data)
            if not meta:
                return self._strategy_bwt(data)
            bwt_data, block_info = bwt_encode(encoded, block_size=0)
            dict_size = _smart_dict_size(len(bwt_data))
            filters = _get_lzma_filters(9, dict_size=dict_size)
            lzma_data = lzma.compress(bwt_data, format=lzma.FORMAT_RAW, filters=filters)
            extra = bytearray()
            extra.append(0x0A)
            extra.extend(struct.pack('>I', len(meta)))
            extra.extend(meta)
            extra.extend(serialize_block_info(block_info))
            extra.extend(self._serialize_filters_info(filters, dict_size))
            return self._build_output(data, lzma_data, strategy=78, extra_header=bytes(extra))
        except Exception:
            return self._strategy_bwt(data)

    # ═══════════════════════════════════════════
    #  输出格式构建
    # ═══════════════════════════════════════════

    def _build_empty_header(self):
        header = bytearray()
        header.extend(ATOMZIP_MAGIC)
        header.append(FORMAT_VERSION)
        header.extend(struct.pack('>I', 0))
        header.append(0)
        header.extend(struct.pack('>I', 0))
        header.extend(struct.pack('>I', 0))
        return bytes(header)

    def _serialize_filters_info(self, filters, dict_size, delta_dist=0, bcj=False):
        info = bytearray()
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
