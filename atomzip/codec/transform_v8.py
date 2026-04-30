"""
AtomZip v8 数据变换模块 — 极限压缩引擎

v8核心创新:
  1. 超激进BPE — 全扫描, 254轮合并, 多轮迭代
  2. 词级字典 — 文本数据词频替换
  3. 增强N-gram — 500条目, 64字节模式
  4. 保留v7所有变换 (C加速BWT, RLE, Delta, BPE, N-gram等)
"""

import struct
import re
import os
import ctypes
from typing import Tuple, List, Dict, Optional
from collections import Counter

# 导入v7所有变换
from .transform_v7 import (
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
    _serialize_bpe_rules, _deserialize_bpe_rules,
)


# ─────────────────────────────────────────────
#  ★ 超激进BPE — 全扫描, 254轮合并, 多轮迭代
# ─────────────────────────────────────────────

def bpe_encode_ultra(data: bytes, max_merges: int = 254) -> Tuple[bytes, bytes]:
    """超激进BPE编码: 全扫描, 254轮合并, 多轮迭代。
    
    相比v7的bpe_encode:
    1. 全扫描 — 扫描整个数据(大数据用2MB采样), 不限制256KB
    2. 254轮合并 — 使用所有可用空闲字节
    3. 多轮迭代 — 第一轮用完后检查新释放的字节继续合并
    4. 最低对频次2 — 比v7的3更低
    
    返回: (encoded_data, merge_rules_bytes)
    """
    if not data or len(data) < 10:
        return data, b''
    
    n = len(data)
    # 大数据(>5MB)用2MB采样, 小数据全扫描
    sample_size = min(n, 2 << 20)
    
    data_arr = bytearray(data)
    
    # 找未使用的字节值 (全扫描或大采样)
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
        
        new_data = bytearray()
        for b in data_arr:
            if b == esc_byte:
                new_data.append(esc_byte)
                new_data.append(0x00)
            else:
                new_data.append(b)
        data_arr = new_data
        free_slots = [esc_byte]
    
    merge_rules = []
    slot_idx = 0
    effective_max = min(max_merges, len(free_slots))
    
    for merge_round in range(3):  # 最多3轮
        if slot_idx >= len(free_slots):
            # 检查是否有新释放的字节
            if merge_round < 2:
                used_now = [False] * 256
                for b in data_arr[:min(len(data_arr), sample_size)]:
                    used_now[b] = True
                new_free = [b for b in range(256) if not used_now[b] and b not in free_slots]
                if new_free:
                    free_slots.extend(new_free)
                    effective_max = min(max_merges, len(free_slots))
                else:
                    break
            else:
                break
        
        for _ in range(effective_max - slot_idx):
            if slot_idx >= len(free_slots):
                break
            
            # 统计字节对频率 (采样或全扫描)
            pair_freq = {}
            scan_limit = min(len(data_arr), sample_size)
            arr = data_arr
            for i in range(scan_limit - 1):
                p = (arr[i], arr[i + 1])
                pair_freq[p] = pair_freq.get(p, 0) + 1
            
            if not pair_freq:
                break
            
            best_pair = max(pair_freq, key=pair_freq.get)
            best_count = pair_freq[best_pair]
            if best_count < 2:  # v8: 最低频次2 (v7是3)
                break
            
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


def bpe_decode_ultra(data: bytes, rules_bytes: bytes) -> bytes:
    """超激进BPE解码: 与v7 bpe_decode相同逻辑。"""
    if not rules_bytes:
        return data
    
    merge_rules, esc_byte = _deserialize_bpe_rules(rules_bytes)
    
    # 逆序应用规则
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


# ─────────────────────────────────────────────
#  ★ 词级字典压缩 — 文本数据词频替换
# ─────────────────────────────────────────────

def word_dict_encode(data: bytes, max_words: int = 8000) -> Tuple[bytes, bytes]:
    """词级字典压缩: 将高频词替换为短编码。
    
    算法:
    1. 将文本分割为词和非词(空白/标点)
    2. 统计词频, 按节省空间排序
    3. 高频词(长度>=3)替换为 MARKER + INDEX 编码
    4. 使用空闲字节或最罕见字节作为MARKER
    
    返回: (encoded_data, dict_bytes)
    """
    if not data or len(data) < 100:
        return data, b''
    
    # 检测是否为文本
    sample = data[:8192]
    printable = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
    if printable / max(1, len(sample)) < 0.7:
        return data, b''
    
    # 分割为词和非词token
    # 使用正则: 匹配连续的ASCII字母数字 或 单个非ASCII/非字母数字字节
    tokens = []
    word_pattern = re.compile(rb'[A-Za-z][A-Za-z0-9_]*')
    last_end = 0
    for m in word_pattern.finditer(data):
        if m.start() > last_end:
            tokens.append((False, data[last_end:m.start()]))  # 非词
        tokens.append((True, m.group()))  # 词
        last_end = m.end()
    if last_end < len(data):
        tokens.append((False, data[last_end:]))
    
    # 统计词频
    word_freq = Counter()
    for is_word, token in tokens:
        if is_word and len(token) >= 3:
            word_freq[token] += 1
    
    if not word_freq:
        return data, b''
    
    # 按节省空间排序: 频次 * (词长 - 2) - 4 (编码开销)
    word_savings = []
    for word, count in word_freq.items():
        savings = count * (len(word) - 2) - 4
        if savings > 0:
            word_savings.append((word, count, savings))
    
    if not word_savings:
        return data, b''
    
    word_savings.sort(key=lambda x: -x[2])
    
    # 选取最多max_words个词
    selected_words = [w for w, _, _ in word_savings[:max_words]]
    
    # 找MARKER字节
    freq = [0] * 256
    for b in data[:min(len(data), 2 << 20)]:
        freq[b] += 1
    unused = [b for b in range(256) if freq[b] == 0]
    
    esc_byte = None
    if unused:
        marker = unused[0]
    else:
        # 使用最罕见的字节
        marker = min(range(256), key=lambda b: freq[b])
        esc_byte = marker
    
    # 构建词到索引的映射
    word_to_idx = {}
    for idx, word in enumerate(selected_words):
        word_to_idx[word] = idx
    
    # 编码
    use_two_byte_idx = len(selected_words) > 250
    encoded = bytearray()
    
    for is_word, token in tokens:
        if is_word and token in word_to_idx:
            idx = word_to_idx[token]
            encoded.append(marker)
            if use_two_byte_idx:
                encoded.extend(struct.pack('>H', idx))
            else:
                encoded.append(idx)
        else:
            # 非词或不在字典中的词: 直接写入, 需要转义MARKER
            for b in token:
                if b == marker:
                    if esc_byte is not None:
                        encoded.append(marker)
                        encoded.append(0xFF)  # 转义MARKER
                    else:
                        encoded.append(marker)
                        encoded.append(0xFF)  # 转义MARKER
                else:
                    encoded.append(b)
    
    # 序列化字典
    dict_bytes = _serialize_word_dict(selected_words, marker, use_two_byte_idx, esc_byte)
    return bytes(encoded), dict_bytes


def word_dict_decode(data: bytes, dict_bytes: bytes) -> bytes:
    """词级字典解码。"""
    if not dict_bytes:
        return data
    
    words, marker, use_two_byte_idx, esc_byte = _deserialize_word_dict(dict_bytes)
    
    result = bytearray()
    i = 0
    n = len(data)
    
    while i < n:
        if data[i] == marker:
            if i + 1 < n and data[i + 1] == 0xFF:
                # 转义的MARKER
                result.append(marker)
                i += 2
            elif use_two_byte_idx:
                if i + 2 < n:
                    idx = struct.unpack('>H', data[i + 1:i + 3])[0]
                    if idx < len(words):
                        result.extend(words[idx])
                    else:
                        result.append(data[i])
                        result.append(data[i + 1])
                        result.append(data[i + 2])
                    i += 3
                else:
                    result.append(data[i])
                    i += 1
            else:
                if i + 1 < n:
                    idx = data[i + 1]
                    if idx < len(words):
                        result.extend(words[idx])
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


def _serialize_word_dict(words: list, marker: int, use_two_byte_idx: bool, esc_byte) -> bytes:
    result = bytearray()
    # 标记: 是否有转义字节
    if esc_byte is not None:
        result.append(0x01)
        result.append(esc_byte)
    else:
        result.append(0x00)
    
    result.append(marker)
    result.append(0x02 if use_two_byte_idx else 0x01)  # 索引大小
    result.extend(struct.pack('>H', len(words)))
    
    for word in words:
        result.extend(struct.pack('>H', len(word)))
        result.extend(word)
    
    return bytes(result)


def _deserialize_word_dict(data: bytes) -> Tuple[list, int, bool, Optional[int]]:
    offset = 0
    has_esc = data[offset]; offset += 1
    esc_byte = data[offset] if has_esc else None; offset += has_esc
    
    marker = data[offset]; offset += 1
    idx_size_flag = data[offset]; offset += 1
    use_two_byte_idx = (idx_size_flag == 0x02)
    
    num_words = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    
    words = []
    for _ in range(num_words):
        wlen = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
        word = data[offset:offset + wlen]
        words.append(bytes(word))
        offset += wlen
    
    return words, marker, use_two_byte_idx, esc_byte


# ─────────────────────────────────────────────
#  ★ 增强N-gram — 500条目, 64字节模式, 全扫描
# ─────────────────────────────────────────────

def ngram_dict_encode_v8(data: bytes, max_entries: int = 500, min_len: int = 3, max_len: int = 64) -> Tuple[bytes, bytes]:
    """增强N-gram字典压缩: 全扫描, 500条目, 64字节模式。
    
    改进:
    1. 采样提升到10MB (v7是500KB)
    2. 最多500条目 (v7是100)
    3. 模式最长64字节 (v7是16)
    4. 更精确的节省空间计算
    
    返回: (encoded_data, dict_bytes)
    """
    if not data or len(data) < 100:
        return data, b''
    
    n = len(data)
    sample = data[:min(n, 10 << 20)]  # 10MB采样
    
    # 找MARKER字节
    freq = [0] * 256
    for b in sample:
        freq[b] += 1
    unused = [b for b in range(256) if freq[b] == 0]
    if unused:
        marker = unused[0]
    else:
        marker = min(range(256), key=lambda b: freq[b])
    
    # 查找重复子串 — 长度3-64
    candidates = []
    # 对较长模式用步长采样加速
    for length in range(min_len, min(max_len + 1, 65)):
        pos_map = {}
        slen = len(sample) - length + 1
        # 对长模式用步长
        step = 1 if length <= 16 else (2 if length <= 32 else 3)
        for i in range(0, slen, step):
            substr = sample[i:i + length]
            key = substr
            if key in pos_map:
                pos_map[key] += 1 + (step - 1)  # 估算实际频次
            else:
                pos_map[key] = 1
        
        for substr, count in pos_map.items():
            if count >= 3:
                # 更精确的节省计算: 考虑替换开销
                savings = count * (length - 2) - (length + 6) - 2
                if savings > 0:
                    candidates.append((substr, count, savings))
    
    if not candidates:
        return data, b''
    
    candidates.sort(key=lambda x: -x[2])
    
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
    selected.sort(key=len, reverse=True)
    
    # 使用2字节索引如果条目>250
    use_two_byte_idx = len(selected) > 250
    
    # 转义MARKER字节
    encoded = bytearray()
    for b in data:
        if b == marker:
            encoded.append(marker)
            encoded.append(marker)
        else:
            encoded.append(b)
    
    # 替换子串
    max_idx = 65535 if use_two_byte_idx else 250
    for idx, substr in enumerate(selected[:max_idx]):
        search = bytes(substr)
        if use_two_byte_idx:
            replacement = bytes([marker]) + struct.pack('>H', idx)
        else:
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
    
    dict_bytes = _serialize_ngram_dict_v8(selected[:max_idx], marker, use_two_byte_idx)
    return bytes(encoded), dict_bytes


def ngram_dict_decode_v8(data: bytes, dict_bytes: bytes) -> bytes:
    """增强N-gram字典解码。"""
    if not dict_bytes:
        return data
    
    dictionary, marker, use_two_byte_idx = _deserialize_ngram_dict_v8(dict_bytes)
    
    result = bytearray()
    i = 0
    n = len(data)
    
    while i < n:
        if data[i] == marker:
            if i + 1 < n:
                if data[i + 1] == marker:
                    # 转义的MARKER
                    result.append(marker)
                    i += 2
                elif use_two_byte_idx:
                    if i + 2 < n:
                        idx = struct.unpack('>H', data[i + 1:i + 3])[0]
                        if idx < len(dictionary):
                            result.extend(dictionary[idx])
                        else:
                            result.append(data[i])
                            result.extend(data[i + 1:i + 3])
                        i += 3
                    else:
                        result.append(data[i])
                        i += 1
                else:
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


def _serialize_ngram_dict_v8(dictionary: list, marker: int, use_two_byte_idx: bool) -> bytes:
    result = bytearray()
    result.append(marker)
    result.append(0x02 if use_two_byte_idx else 0x01)
    result.extend(struct.pack('>H', len(dictionary)))
    for substr in dictionary:
        result.extend(struct.pack('>H', len(substr)))
        result.extend(substr)
    return bytes(result)


def _deserialize_ngram_dict_v8(data: bytes) -> Tuple[list, int, bool]:
    offset = 0
    marker = data[offset]; offset += 1
    idx_flag = data[offset]; offset += 1
    use_two_byte_idx = (idx_flag == 0x02)
    
    num_entries = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    dictionary = []
    for _ in range(num_entries):
        entry_len = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
        entry = data[offset:offset + entry_len]
        dictionary.append(bytes(entry))
        offset += entry_len
    return dictionary, marker, use_two_byte_idx
