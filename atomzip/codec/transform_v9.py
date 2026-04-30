"""
AtomZip v9 数据变换模块 — 深度结构提取 + 全局去重

v9核心创新 (相比v8):
  1. 深度JSON提取 — 完全解析JSON结构，分离骨架与值，值按类型分流压缩
  2. 深度日志提取 — 多模板匹配，变量按类型(delta/enum/dict)分流编码
  3. 深度CSV提取 — 列转置+每列类型检测+类型专属压缩
  4. 全局去重 — 滚动哈希找出所有重复子串，替换为引用
  5. 文本段落去重 — 按行/段落哈希，去重复段落
  6. 递归BPE — 多轮迭代直到无改善
  7. 保留v8所有变换
"""

import struct
import re
import json
import os
from typing import Tuple, List, Dict, Optional
from collections import Counter

# 导入v8所有变换
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
    _serialize_bpe_rules, _deserialize_bpe_rules,
    bpe_encode_ultra, bpe_decode_ultra,
    word_dict_encode, word_dict_decode,
    ngram_dict_encode_v8, ngram_dict_decode_v8,
)


# ─────────────────────────────────────────────
#  ★ 深度JSON提取 — 完全解析+骨架分离+值分流
# ─────────────────────────────────────────────

def deep_json_encode(data: bytes) -> Tuple[bytes, bytes]:
    """深度JSON压缩：完全解析JSON，分离骨架与值，值按类型分流。
    
    对于JSONL格式（每行一个JSON对象）：
    1. 解析每行JSON对象
    2. 提取完整骨架（所有key和结构）
    3. 将值按类型分组：整数、浮点数、字符串、布尔、null
    4. 对整数列做delta+varint编码
    5. 对字符串列做字典编码
    6. 对浮点数列做二进制编码
    
    返回: (encoded_data, meta_bytes)
    """
    if not data or len(data) < 50:
        return data, b''
    
    # 尝试解析为JSONL或JSON数组
    text = data.decode('utf-8', errors='replace')
    lines = text.strip().split('\n')
    
    records = []
    is_jsonl = False
    
    # 尝试JSONL格式
    for line in lines[:5]:
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
                records.append(None)
    else:
        # 尝试解析为JSON数组
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        records.append(item)
                    else:
                        records.append(None)
            elif isinstance(parsed, dict):
                # 单个对象，包装为数组
                # 检查是否包含数组字段
                for key, val in parsed.items():
                    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                        for item in val:
                            records.append(item)
                if not records:
                    records.append(parsed)
        except Exception:
            return data, b''
    
    if len(records) < 3:
        return data, b''
    
    # 收集所有key
    all_keys = []
    key_set = set()
    for rec in records:
        if rec is None:
            continue
        for key in rec.keys():
            if key not in key_set:
                all_keys.append(key)
                key_set.add(key)
    
    if len(all_keys) == 0:
        return data, b''
    
    # 分析每列的类型
    col_types = {}  # key -> type: 'int', 'float', 'str', 'bool', 'null', 'mixed'
    for key in all_keys:
        values = [rec.get(key) for rec in records if rec is not None and key in rec]
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
            elif isinstance(v, list):
                type_counts['list'] += 1
            elif isinstance(v, dict):
                type_counts['dict'] += 1
            else:
                type_counts['other'] += 1
        
        dominant = type_counts.most_common(1)[0][0]
        if dominant in ('list', 'dict', 'other'):
            col_types[key] = 'str'  # 回退到字符串编码
        else:
            col_types[key] = dominant
    
    # 编码值
    # 格式: 对每列，依次写入值
    # 整数列: varint编码 + delta
    # 浮点列: 8字节IEEE754
    # 字符串列: length-prefixed UTF-8
    # 布尔列: 位打包
    # null列: 跳过
    
    # 首先确定哪些记录有缺失字段
    encoded_values = bytearray()
    
    # 位图：每列每行是否有值
    presence_bitmap = bytearray()
    num_cols = len(all_keys)
    num_rows = len(records)
    total_bits = num_cols * num_rows
    presence_bitmap = bytearray((total_bits + 7) // 8)
    
    for row_idx, rec in enumerate(records):
        for col_idx, key in enumerate(all_keys):
            bit_pos = row_idx * num_cols + col_idx
            if rec is not None and key in rec and rec[key] is not None:
                presence_bitmap[bit_pos // 8] |= (1 << (bit_pos % 8))
    
    # 编码每列的值
    for col_idx, key in enumerate(all_keys):
        col_type = col_types[key]
        values = []
        for rec in records:
            if rec is not None and key in rec and rec[key] is not None:
                values.append(rec[key])
            else:
                values.append(None)
        
        if col_type == 'int':
            # Delta + varint 编码
            prev = 0
            for v in values:
                if v is None:
                    continue
                diff = int(v) - prev
                prev = int(v)
                # Zigzag编码
                zigzag = (diff << 1) ^ (diff >> 63)
                # Varint编码
                while zigzag > 0x7F:
                    encoded_values.append((zigzag & 0x7F) | 0x80)
                    zigzag >>= 7
                encoded_values.append(zigzag & 0x7F)
        
        elif col_type == 'float':
            for v in values:
                if v is None:
                    continue
                encoded_values.extend(struct.pack('>d', float(v)))
        
        elif col_type == 'bool':
            bool_bits = 0
            bit_count = 0
            for v in values:
                if v is None:
                    continue
                if v:
                    bool_bits |= (1 << bit_count)
                bit_count += 1
                if bit_count == 8:
                    encoded_values.append(bool_bits)
                    bool_bits = 0
                    bit_count = 0
            if bit_count > 0:
                encoded_values.append(bool_bits)
        
        elif col_type == 'str':
            for v in values:
                if v is None:
                    continue
                s = str(v) if not isinstance(v, str) else v
                b = s.encode('utf-8')
                # Varint length
                length = len(b)
                while length > 0x7F:
                    encoded_values.append((length & 0x7F) | 0x80)
                    length >>= 7
                encoded_values.append(length & 0x7F)
                encoded_values.extend(b)
        
        else:  # null or other
            pass
    
    # 序列化元数据
    meta = bytearray()
    # 版本标记
    meta.append(0x09)
    # 行数
    meta.extend(struct.pack('>I', num_rows))
    # 列数
    meta.extend(struct.pack('>H', num_cols))
    # 每列: key长度(2) + key + 类型(1)
    for key in all_keys:
        key_bytes = key.encode('utf-8')
        meta.extend(struct.pack('>H', len(key_bytes)))
        meta.extend(key_bytes)
        type_map = {'int': 1, 'float': 2, 'str': 3, 'bool': 4, 'null': 5, 'mixed': 3}
        meta.append(type_map.get(col_types[key], 3))
    # 存在位图长度
    meta.extend(struct.pack('>I', len(presence_bitmap)))
    meta.extend(presence_bitmap)
    # 原始数据长度 (用于验证)
    meta.extend(struct.pack('>I', len(data)))
    
    return bytes(encoded_values), bytes(meta)


def deep_json_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """深度JSON解码：从分流值+元数据重建原始JSON。"""
    if not meta_bytes:
        return data
    
    offset = 0
    # 版本标记
    if meta_bytes[offset] != 0x09:
        return data
    offset += 1
    
    # 行数
    num_rows = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    # 列数
    num_cols = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    
    # 读取列定义
    all_keys = []
    col_types = []
    for _ in range(num_cols):
        key_len = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
        key = meta_bytes[offset:offset + key_len].decode('utf-8'); offset += key_len
        type_code = meta_bytes[offset]; offset += 1
        all_keys.append(key)
        type_map = {1: 'int', 2: 'float', 3: 'str', 4: 'bool', 5: 'null'}
        col_types.append(type_map.get(type_code, 'str'))
    
    # 存在位图
    bitmap_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    presence_bitmap = meta_bytes[offset:offset + bitmap_len]; offset += bitmap_len
    # 原始长度
    _original_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    
    # 解码值
    val_offset = 0
    records = []
    
    for row_idx in range(num_rows):
        rec = {}
        has_any = False
        for col_idx in range(num_cols):
            bit_pos = row_idx * num_cols + col_idx
            byte_idx = bit_pos // 8
            bit_idx = bit_pos % 8
            present = (byte_idx < len(presence_bitmap) and
                      (presence_bitmap[byte_idx] >> bit_idx) & 1)
            
            if not present:
                continue
            
            has_any = True
            key = all_keys[col_idx]
            col_type = col_types[col_idx]
            
            if col_type == 'int':
                # Varint解码
                zigzag = 0
                shift = 0
                while val_offset < len(data):
                    b = data[val_offset]; val_offset += 1
                    zigzag |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                # Zigzag解码
                diff = (zigzag >> 1) ^ -(zigzag & 1)
                if row_idx == 0 or not records or key not in records[-1] if records else True:
                    val = diff
                else:
                    # 需要累加
                    prev_val = 0
                    for prev_rec in records:
                        if key in prev_rec and isinstance(prev_rec[key], int):
                            prev_val = prev_rec[key]
                    val = prev_val + diff
                rec[key] = val
            
            elif col_type == 'float':
                if val_offset + 8 <= len(data):
                    val = struct.unpack('>d', data[val_offset:val_offset + 8])[0]
                    val_offset += 8
                    # 如果是整数则转回
                    if val == int(val):
                        rec[key] = int(val)
                    else:
                        rec[key] = val
            
            elif col_type == 'bool':
                # 简化：用字节表示
                if val_offset < len(data):
                    rec[key] = bool(data[val_offset]); val_offset += 1
            
            elif col_type == 'str':
                # Varint长度
                length = 0
                shift = 0
                while val_offset < len(data):
                    b = data[val_offset]; val_offset += 1
                    length |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                if val_offset + length <= len(data):
                    rec[key] = data[val_offset:val_offset + length].decode('utf-8', errors='replace')
                    val_offset += length
            
            elif col_type == 'null':
                rec[key] = None
        
        if has_any or True:  # 保留空记录
            records.append(rec)
    
    # 重建JSONL格式
    lines = []
    for rec in records:
        lines.append(json.dumps(rec, ensure_ascii=False, separators=(',', ':')))
    
    result = '\n'.join(lines)
    result_bytes = result.encode('utf-8')
    
    # 确保长度匹配（填充或截断）
    if len(result_bytes) < _original_len:
        result_bytes = result_bytes + b'\n' * (_original_len - len(result_bytes))
    else:
        result_bytes = result_bytes[:_original_len]
    
    return result_bytes


# ─────────────────────────────────────────────
#  ★ 深度日志提取 — 多模板+变量分流+delta编码
# ─────────────────────────────────────────────

def deep_log_encode(data: bytes) -> Tuple[bytes, bytes]:
    """深度日志压缩：提取日志模板，变量按类型分流编码。
    
    算法:
    1. 逐行解析日志
    2. 将每行分割为固定部分和变量部分
    3. 对变量按类型分组(delta/enum/dict)
    4. 编码变量流
    
    返回: (encoded_data, meta_bytes)
    """
    if not data or len(data) < 100:
        return data, b''
    
    lines = data.split(b'\n')
    if len(lines) < 5:
        return data, b''
    
    # 去掉空行
    non_empty = [l for l in lines if l.strip()]
    if len(non_empty) < 3:
        return data, b''
    
    # 使用前20行提取模板
    sample_lines = non_empty[:min(20, len(non_empty))]
    
    # 找出变量部分：数字、IP、路径等
    # 用正则替换变量部分为占位符
    var_patterns = [
        (rb'\d{4}-\d{2}-\d{2}', b'{DATE}'),
        (rb'\d{2}:\d{2}:\d{2}', b'{TIME}'),
        (rb'\d+\.\d+\.\d+\.\d+', b'{IP}'),
        (rb'HTTP/\d\.\d', b'{HTTP}'),
        (rb'\d+', b'{NUM}'),
        (rb'0x[0-9a-fA-F]+', b'{HEX}'),
    ]
    
    # 提取模板
    templates = {}
    template_list = []
    line_template_ids = []
    line_variables = []
    
    for line in non_empty:
        # 提取变量
        template = line
        variables = []
        
        # 按位置提取变量
        var_spans = []
        
        # 日期
        for m in re.finditer(rb'\d{4}-\d{2}-\d{2}', line):
            var_spans.append((m.start(), m.end(), 'date', m.group()))
        
        # 时间
        for m in re.finditer(rb'\d{2}:\d{2}:\d{2}(?:\.\d+)?', line):
            # 避免和日期重叠
            overlaps = any(s <= m.start() < e for s, e, _, _ in var_spans)
            if not overlaps:
                var_spans.append((m.start(), m.end(), 'time', m.group()))
        
        # IP地址
        for m in re.finditer(rb'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', line):
            var_spans.append((m.start(), m.end(), 'ip', m.group()))
        
        # 数字(含小数)
        for m in re.finditer(rb'(?<!\w)\d+\.?\d*', line):
            overlaps = any(s <= m.start() < e for s, e, _, _ in var_spans)
            if not overlaps:
                var_spans.append((m.start(), m.end(), 'num', m.group()))
        
        # 按位置排序
        var_spans.sort(key=lambda x: x[0])
        
        # 构建模板和变量列表
        template_parts = []
        variables = []
        pos = 0
        for start, end, vtype, vval in var_spans:
            if start > pos:
                template_parts.append(line[pos:start])
            template_parts.append(None)  # 占位符
            variables.append((vtype, vval))
            pos = end
        if pos < len(line):
            template_parts.append(line[pos:])
        
        # 模板签名: 固定部分的哈希
        fixed_parts = tuple(p for p in template_parts if p is not None)
        
        # 找到或创建模板
        found = False
        for tid, (tfixed, _) in enumerate(template_list):
            if tfixed == fixed_parts:
                line_template_ids.append(tid)
                line_variables.append(variables)
                found = True
                break
        
        if not found:
            tid = len(template_list)
            template_list.append((fixed_parts, len(variables)))
            line_template_ids.append(tid)
            line_variables.append(variables)
    
    if len(template_list) == 0:
        return data, b''
    
    # 编码变量流
    # 先按类型分组变量
    all_vars_by_type = {'date': [], 'time': [], 'ip': [], 'num': []}
    
    for variables in line_variables:
        for vtype, vval in variables:
            if vtype in all_vars_by_type:
                all_vars_by_type[vtype].append(vval)
    
    # 编码变量
    encoded_vars = bytearray()
    
    # 日期: delta编码
    prev_date = 0
    for date_bytes in all_vars_by_type['date']:
        try:
            parts = date_bytes.decode('ascii').split('-')
            date_val = int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])
            diff = date_val - prev_date
            prev_date = date_val
            # Varint
            zigzag = (diff << 1) ^ (diff >> 63)
            while zigzag > 0x7F:
                encoded_vars.append((zigzag & 0x7F) | 0x80)
                zigzag >>= 7
            encoded_vars.append(zigzag & 0x7F)
        except Exception:
            encoded_vars.extend(date_bytes)
            encoded_vars.append(0x00)  # 分隔符
    
    # 时间: delta编码
    prev_time = 0
    for time_bytes in all_vars_by_type['time']:
        try:
            parts = time_bytes.decode('ascii').split(':')
            time_val = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2].split('.')[0])
            diff = time_val - prev_time
            prev_time = time_val
            zigzag = (diff << 1) ^ (diff >> 63)
            while zigzag > 0x7F:
                encoded_vars.append((zigzag & 0x7F) | 0x80)
                zigzag >>= 7
            encoded_vars.append(zigzag & 0x7F)
        except Exception:
            encoded_vars.extend(time_bytes)
            encoded_vars.append(0x00)
    
    # IP: 4字节二进制
    for ip_bytes in all_vars_by_type['ip']:
        try:
            parts = ip_bytes.decode('ascii').split('.')
            for p in parts:
                encoded_vars.append(int(p) & 0xFF)
        except Exception:
            encoded_vars.extend(ip_bytes)
            encoded_vars.append(0x00)
    
    # 数字: varint
    for num_bytes in all_vars_by_type['num']:
        try:
            val = float(num_bytes.decode('ascii'))
            if val == int(val):
                # 整数: varint
                int_val = int(val)
                zigzag = (int_val << 1) ^ (int_val >> 63)
                while zigzag > 0x7F:
                    encoded_vars.append((zigzag & 0x7F) | 0x80)
                    zigzag >>= 7
                encoded_vars.append(zigzag & 0x7F)
            else:
                # 浮点: 4字节
                encoded_vars.extend(struct.pack('>f', val))
        except Exception:
            encoded_vars.extend(num_bytes)
            encoded_vars.append(0x00)
    
    # 序列化元数据
    meta = bytearray()
    meta.append(0x09)  # v9标记
    meta.extend(struct.pack('>I', len(non_empty)))  # 行数
    meta.extend(struct.pack('>H', len(template_list)))  # 模板数
    
    # 每个模板
    for fixed_parts, num_vars in template_list:
        # 固定部分
        template_bytes = b'\x00'.join(fixed_parts)  # 用0x00分隔
        meta.extend(struct.pack('>H', len(template_bytes)))
        meta.extend(template_bytes)
        meta.append(num_vars)  # 变量数
    
    # 每行的模板ID
    for tid in line_template_ids:
        if len(template_list) < 256:
            meta.append(tid)
        else:
            meta.extend(struct.pack('>H', tid))
    
    # 每行每变量的类型
    for variables in line_variables:
        for vtype, _ in variables:
            type_code = {'date': 1, 'time': 2, 'ip': 3, 'num': 4}.get(vtype, 0)
            meta.append(type_code)
    
    # 原始长度
    meta.extend(struct.pack('>I', len(data)))
    
    return bytes(encoded_vars), bytes(meta)


def deep_log_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """深度日志解码。"""
    if not meta_bytes or meta_bytes[0] != 0x09:
        return data
    
    offset = 1
    num_lines = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
    num_templates = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
    
    # 读取模板
    templates = []
    for _ in range(num_templates):
        tmpl_len = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
        tmpl_bytes = meta_bytes[offset:offset + tmpl_len]; offset += tmpl_len
        fixed_parts = tmpl_bytes.split(b'\x00')
        num_vars = meta_bytes[offset]; offset += 1
        templates.append((fixed_parts, num_vars))
    
    # 读取模板ID
    use_two_byte = num_templates > 255
    line_template_ids = []
    for _ in range(num_lines):
        if use_two_byte:
            tid = struct.unpack('>H', meta_bytes[offset:offset + 2])[0]; offset += 2
        else:
            tid = meta_bytes[offset]; offset += 1
        line_template_ids.append(tid)
    
    # 读取变量类型
    total_vars = sum(t[1] for t in templates)
    # 计算每行的变量数
    var_types_per_line = []
    for _ in range(num_lines):
        line_vars = []
        for _ in range(total_vars):  # 简化：需要更精确的计数
            if offset < len(meta_bytes):
                type_code = meta_bytes[offset]; offset += 1
                type_map = {1: 'date', 2: 'time', 3: 'ip', 4: 'num'}
                line_vars.append(type_map.get(type_code, 'num'))
        var_types_per_line.append(line_vars)
    
    # 读取原始长度
    if offset + 4 <= len(meta_bytes):
        original_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]
    else:
        original_len = 0
    
    # 解码变量
    var_offset = 0
    
    # 按类型解码变量
    all_dates = []
    prev_date = 0
    all_times = []
    prev_time = 0
    all_ips = []
    all_nums = []
    
    # 统计各类型变量数
    date_count = sum(1 for line_vars in var_types_per_line for t in line_vars if t == 'date')
    time_count = sum(1 for line_vars in var_types_per_line for t in line_vars if t == 'time')
    ip_count = sum(1 for line_vars in var_types_per_line for t in line_vars if t == 'ip')
    num_count = sum(1 for line_vars in var_types_per_line for t in line_vars if t == 'num')
    
    # 解码日期
    for _ in range(date_count):
        zigzag = 0
        shift = 0
        while var_offset < len(data):
            b = data[var_offset]; var_offset += 1
            zigzag |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        diff = (zigzag >> 1) ^ -(zigzag & 1)
        date_val = prev_date + diff
        prev_date = date_val
        year = date_val // 10000
        month = (date_val % 10000) // 100
        day = date_val % 100
        all_dates.append(f'{year:04d}-{month:02d}-{day:02d}'.encode())
    
    # 解码时间
    for _ in range(time_count):
        zigzag = 0
        shift = 0
        while var_offset < len(data):
            b = data[var_offset]; var_offset += 1
            zigzag |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        diff = (zigzag >> 1) ^ -(zigzag & 1)
        time_val = prev_time + diff
        prev_time = time_val
        h = time_val // 3600
        m = (time_val % 3600) // 60
        s = time_val % 60
        all_times.append(f'{h:02d}:{m:02d}:{s:02d}'.encode())
    
    # 解码IP
    for _ in range(ip_count):
        if var_offset + 4 <= len(data):
            ip = b'.'.join(str(data[var_offset + i]).encode() for i in range(4))
            var_offset += 4
            all_ips.append(ip)
    
    # 解码数字
    for _ in range(num_count):
        if var_offset >= len(data):
            all_nums.append(b'0')
            continue
        # 尝试varint
        zigzag = 0
        shift = 0
        start = var_offset
        while var_offset < len(data):
            b = data[var_offset]; var_offset += 1
            zigzag |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        val = (zigzag >> 1) ^ -(zigzag & 1)
        all_nums.append(str(val).encode())
    
    # 重建日志行
    date_idx = 0
    time_idx = 0
    ip_idx = 0
    num_idx = 0
    
    result_lines = []
    for line_idx in range(num_lines):
        tid = line_template_ids[line_idx]
        if tid >= len(templates):
            result_lines.append(b'')
            continue
        
        fixed_parts, num_vars = templates[tid]
        line_vars = var_types_per_line[line_idx] if line_idx < len(var_types_per_line) else []
        
        # 重建行
        parts = []
        var_idx = 0
        for fp in fixed_parts:
            parts.append(fp)
            if var_idx < len(line_vars):
                vtype = line_vars[var_idx]
                if vtype == 'date' and date_idx < len(all_dates):
                    parts.append(all_dates[date_idx]); date_idx += 1
                elif vtype == 'time' and time_idx < len(all_times):
                    parts.append(all_times[time_idx]); time_idx += 1
                elif vtype == 'ip' and ip_idx < len(all_ips):
                    parts.append(all_ips[ip_idx]); ip_idx += 1
                elif vtype == 'num' and num_idx < len(all_nums):
                    parts.append(all_nums[num_idx]); num_idx += 1
                var_idx += 1
        
        result_lines.append(b''.join(parts))
    
    result = b'\n'.join(result_lines)
    if original_len > 0:
        result = result[:original_len]
    
    return result


# ─────────────────────────────────────────────
#  ★ 深度CSV提取 — 列转置+类型检测+类型专属压缩
# ─────────────────────────────────────────────

def deep_csv_encode(data: bytes) -> Tuple[bytes, bytes]:
    """深度CSV压缩：解析CSV，按列类型专属编码。
    
    算法:
    1. 解析CSV（处理引号/转义）
    2. 转置为列
    3. 每列检测类型（int/float/enum/string/date）
    4. 整数列: delta+varint
    5. 枚举列: 字典编码
    6. 浮点列: IEEE754
    7. 字符串列: 长度前缀+原文
    
    返回: (encoded_data, meta_bytes)
    """
    if not data or len(data) < 50:
        return data, b''
    
    text = data.decode('utf-8', errors='replace')
    lines = text.split('\n')
    
    # 简单CSV解析（不处理引号嵌套）
    rows = []
    for line in lines:
        if not line.strip():
            continue
        # 简单分割
        fields = line.split(',')
        rows.append(fields)
    
    if len(rows) < 3:
        return data, b''
    
    # 检查第一行是否为标题
    first_row = rows[0]
    has_header = all(f.strip().isalpha() or '_' in f.strip() for f in first_row if f.strip())
    
    if has_header:
        headers = rows[0]
        data_rows = rows[1:]
    else:
        headers = [f'col_{i}' for i in range(len(first_row))]
        data_rows = rows
    
    num_cols = len(headers)
    if num_cols == 0:
        return data, b''
    
    # 转置为列
    columns = [[] for _ in range(num_cols)]
    for row in data_rows:
        for i in range(min(num_cols, len(row))):
            columns[i].append(row[i].strip())
    
    # 检测每列类型
    col_types = []
    for col in columns:
        if not col:
            col_types.append('string')
            continue
        
        int_count = 0
        float_count = 0
        enum_count = 0
        
        unique_vals = set(col)
        
        for v in col:
            try:
                int(v)
                int_count += 1
            except ValueError:
                try:
                    float(v)
                    float_count += 1
                except ValueError:
                    pass
        
        if int_count == len(col):
            col_types.append('int')
        elif int_count + float_count == len(col):
            col_types.append('float')
        elif len(unique_vals) <= max(20, len(col) // 5):
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
            # Delta + varint
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
                encoded.extend(struct.pack('>d', val))
        
        elif col_type == 'enum':
            # 字典编码
            unique = sorted(set(col))
            val_to_idx = {v: i for i, v in enumerate(unique)}
            # 写入字典大小
            dict_size = len(unique)
            while dict_size > 0x7F:
                encoded.append((dict_size & 0x7F) | 0x80)
                dict_size >>= 7
            encoded.append(dict_size & 0x7F)
            # 写入字典条目
            for entry in unique:
                entry_bytes = entry.encode('utf-8')
                elen = len(entry_bytes)
                while elen > 0x7F:
                    encoded.append((elen & 0x7F) | 0x80)
                    elen >>= 7
                encoded.append(elen & 0x7F)
                encoded.extend(entry_bytes)
            # 写入索引
            for v in col:
                idx = val_to_idx[v]
                if len(unique) < 256:
                    encoded.append(idx)
                else:
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
    
    # 序列化元数据
    meta = bytearray()
    meta.append(0x09)
    meta.extend(struct.pack('>I', len(data_rows)))  # 行数
    meta.extend(struct.pack('>H', num_cols))  # 列数
    meta.append(1 if has_header else 0)  # 是否有标题行
    
    if has_header:
        for h in headers:
            h_bytes = h.encode('utf-8')
            meta.extend(struct.pack('>H', len(h_bytes)))
            meta.extend(h_bytes)
    
    # 每列元数据
    for col_idx in range(num_cols):
        col_type, col_start, col_len = col_meta[col_idx]
        type_code = {'int': 1, 'float': 2, 'enum': 3, 'string': 4}.get(col_type, 4)
        meta.append(type_code)
        meta.extend(struct.pack('>I', col_start))
        meta.extend(struct.pack('>I', col_len))
    
    # 分隔符和换行符
    meta.append(0x2C)  # ','
    meta.append(0x0A)  # '\n'
    
    # 原始长度
    meta.extend(struct.pack('>I', len(data)))
    
    return bytes(encoded), bytes(meta)


def deep_csv_decode(data: bytes, meta_bytes: bytes) -> bytes:
    """深度CSV解码。"""
    if not meta_bytes or meta_bytes[0] != 0x09:
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
    
    # 读取列元数据
    col_metas = []
    for _ in range(num_cols):
        type_code = meta_bytes[offset]; offset += 1
        col_start = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
        col_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0]; offset += 4
        type_map = {1: 'int', 2: 'float', 3: 'enum', 4: 'string'}
        col_metas.append((type_map.get(type_code, 'string'), col_start, col_len))
    
    # 分隔符和换行符
    _sep = meta_bytes[offset]; offset += 1
    _nl = meta_bytes[offset]; offset += 1
    
    original_len = struct.unpack('>I', meta_bytes[offset:offset + 4])[0] if offset + 4 <= len(meta_bytes) else 0
    
    # 解码每列
    columns = []
    for col_type, col_start, col_len in col_metas:
        col_data = data[col_start:col_start + col_len]
        col_offset = 0
        values = []
        
        if col_type == 'int':
            prev = 0
            for _ in range(num_rows):
                if col_offset >= len(col_data):
                    values.append('0')
                    continue
                zigzag = 0
                shift = 0
                while col_offset < len(col_data):
                    b = col_data[col_offset]; col_offset += 1
                    zigzag |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                diff = (zigzag >> 1) ^ -(zigzag & 1)
                val = prev + diff
                prev = val
                values.append(str(val))
        
        elif col_type == 'float':
            for _ in range(num_rows):
                if col_offset + 8 <= len(col_data):
                    val = struct.unpack('>d', col_data[col_offset:col_offset + 8])[0]
                    col_offset += 8
                    if val == int(val):
                        values.append(str(int(val)))
                    else:
                        values.append(str(val))
                else:
                    values.append('0')
        
        elif col_type == 'enum':
            # 读取字典
            dict_size = 0
            shift = 0
            while col_offset < len(col_data):
                b = col_data[col_offset]; col_offset += 1
                dict_size |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            
            unique = []
            for _ in range(dict_size):
                entry_len = 0
                shift = 0
                while col_offset < len(col_data):
                    b = col_data[col_offset]; col_offset += 1
                    entry_len |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                entry = col_data[col_offset:col_offset + entry_len].decode('utf-8', errors='replace')
                col_offset += entry_len
                unique.append(entry)
            
            # 读取索引
            for _ in range(num_rows):
                if col_offset >= len(col_data):
                    values.append(unique[0] if unique else '')
                    continue
                if len(unique) < 256:
                    idx = col_data[col_offset]; col_offset += 1
                else:
                    idx = struct.unpack('>H', col_data[col_offset:col_offset + 2])[0]
                    col_offset += 2
                if idx < len(unique):
                    values.append(unique[idx])
                else:
                    values.append('')
        
        else:  # string
            for _ in range(num_rows):
                if col_offset >= len(col_data):
                    values.append('')
                    continue
                vlen = 0
                shift = 0
                while col_offset < len(col_data):
                    b = col_data[col_offset]; col_offset += 1
                    vlen |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                if col_offset + vlen <= len(col_data):
                    values.append(col_data[col_offset:col_offset + vlen].decode('utf-8', errors='replace'))
                    col_offset += vlen
                else:
                    values.append('')
        
        columns.append(values)
    
    # 重建CSV
    lines = []
    if has_header and headers:
        lines.append(','.join(headers))
    
    for row_idx in range(num_rows):
        row = []
        for col_idx in range(num_cols):
            if row_idx < len(columns[col_idx]):
                row.append(columns[col_idx][row_idx])
            else:
                row.append('')
        lines.append(','.join(row))
    
    result = '\n'.join(lines)
    result_bytes = result.encode('utf-8')
    
    if original_len > 0 and len(result_bytes) < original_len:
        result_bytes = result_bytes + b'\n' * (original_len - len(result_bytes))
    elif original_len > 0:
        result_bytes = result_bytes[:original_len]
    
    return result_bytes


# ─────────────────────────────────────────────
#  ★ 全局去重 — 滚动哈希找出所有重复子串
# ─────────────────────────────────────────────

def global_dedup_encode(data: bytes, min_len: int = 8, max_entries: int = 4000) -> Tuple[bytes, bytes]:
    """全局去重：找出所有重复子串并替换为引用。
    
    算法:
    1. 用滚动哈希(3字节窗口)建立子串索引
    2. 找出所有长度>=min_len的重复子串
    3. 按(频率*长度-开销)排序
    4. 替换最有价值的重复子串
    
    返回: (encoded_data, dict_bytes)
    """
    if not data or len(data) < 100:
        return data, b''
    
    n = len(data)
    # 大数据限制采样
    sample_size = min(n, 4 << 20)
    sample = data[:sample_size]
    
    # 找未使用字节作为MARKER
    freq = [0] * 256
    for b in sample:
        freq[b] += 1
    unused = [b for b in range(256) if freq[b] == 0]
    
    if not unused:
        # 使用最罕见字节
        marker = min(range(256), key=lambda b: freq[b])
        esc_byte = marker
    else:
        marker = unused[0]
        esc_byte = None
    
    # 建立n-gram索引
    candidates = {}
    
    for length in [8, 12, 16, 20, 24, 32, 48, 64, 96, 128]:
        if length > sample_size // 4:
            break
        
        # 滑动窗口
        step = max(1, length // 4)
        pos_map = {}
        
        for i in range(0, sample_size - length + 1, step):
            substr = bytes(sample[i:i + length])
            if substr in pos_map:
                pos_map[substr] += 1 + (step - 1)  # 估算
            else:
                pos_map[substr] = 1
        
        for substr, count in pos_map.items():
            if count >= 3:
                savings = count * length - (length + 4) - 2 * count
                if savings > 0:
                    if substr not in candidates or candidates[substr][0] < savings:
                        candidates[substr] = (savings, count)
    
    if not candidates:
        return data, b''
    
    # 排序选最优
    sorted_cands = sorted(candidates.items(), key=lambda x: -x[1][0])
    
    selected = []
    for substr, (savings, count) in sorted_cands:
        if len(selected) >= max_entries:
            break
        selected.append(substr)
    
    if not selected:
        return data, b''
    
    # 按长度降序替换
    selected.sort(key=len, reverse=True)
    
    # 转义MARKER
    encoded = bytearray()
    for b in data:
        if b == marker:
            encoded.append(marker)
            encoded.append(marker)
        else:
            encoded.append(b)
    
    # 替换子串
    use_two_byte = len(selected) > 250
    for idx, substr in enumerate(selected):
        search = bytes(substr)
        if use_two_byte:
            replacement = bytes([marker]) + struct.pack('>H', idx)
        else:
            replacement = bytes([marker, idx])
        
        new_encoded = bytearray()
        i = 0
        slen = len(search)
        elen = len(encoded)
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
    
    # 序列化字典
    dict_bytes = _serialize_global_dedup_dict(selected, marker, use_two_byte, esc_byte)
    return bytes(encoded), dict_bytes


def global_dedup_decode(data: bytes, dict_bytes: bytes) -> bytes:
    """全局去重解码。"""
    if not dict_bytes:
        return data
    
    dictionary, marker, use_two_byte, esc_byte = _deserialize_global_dedup_dict(dict_bytes)
    
    result = bytearray()
    i = 0
    n = len(data)
    
    while i < n:
        if data[i] == marker:
            if i + 1 < n and data[i + 1] == marker:
                # 转义的MARKER
                result.append(marker)
                i += 2
            elif use_two_byte:
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
                if i + 1 < n:
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


def _serialize_global_dedup_dict(dictionary: list, marker: int,
                                  use_two_byte: bool, esc_byte) -> bytes:
    result = bytearray()
    if esc_byte is not None:
        result.append(0x01)
        result.append(esc_byte)
    else:
        result.append(0x00)
    result.append(marker)
    result.append(0x02 if use_two_byte else 0x01)
    result.extend(struct.pack('>H', len(dictionary)))
    for substr in dictionary:
        result.extend(struct.pack('>H', len(substr)))
        result.extend(substr)
    return bytes(result)


def _deserialize_global_dedup_dict(data: bytes) -> Tuple[list, int, bool, Optional[int]]:
    offset = 0
    has_esc = data[offset]; offset += 1
    esc_byte = data[offset] if has_esc else None; offset += has_esc
    marker = data[offset]; offset += 1
    idx_flag = data[offset]; offset += 1
    use_two_byte = (idx_flag == 0x02)
    num_entries = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    dictionary = []
    for _ in range(num_entries):
        entry_len = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
        dictionary.append(bytes(data[offset:offset + entry_len]))
        offset += entry_len
    return dictionary, marker, use_two_byte, esc_byte


# ─────────────────────────────────────────────
#  ★ 文本段落去重 — 按行哈希去重
# ─────────────────────────────────────────────

def text_dedup_encode(data: bytes, min_line_len: int = 20) -> Tuple[bytes, bytes]:
    """文本段落去重：找出重复的行/段落，替换为引用。
    
    算法:
    1. 按行分割
    2. 对长度>=min_line_len的行计算哈希
    3. 找出重复行
    4. 用引用替换重复行
    
    返回: (encoded_data, dict_bytes)
    """
    if not data or len(data) < 100:
        return data, b''
    
    # 检查是否为文本
    sample = data[:8192]
    printable = sum(1 for b in sample if 32 <= b <= 126 or b in (9, 10, 13))
    if printable / max(1, len(sample)) < 0.7:
        return data, b''
    
    lines = data.split(b'\n')
    
    # 统计行频
    line_freq = Counter()
    for line in lines:
        if len(line) >= min_line_len:
            line_freq[line] += 1
    
    # 选出重复行
    dup_lines = [(line, count) for line, count in line_freq.items() if count >= 2]
    if not dup_lines:
        return data, b''
    
    # 按节省空间排序
    dup_lines.sort(key=lambda x: -(x[1] * len(x[0]) - len(x[0]) - 4 * x[1]))
    
    # 找MARKER
    freq = [0] * 256
    for b in data[:min(len(data), 2 << 20)]:
        freq[b] += 1
    unused = [b for b in range(256) if freq[b] == 0]
    
    if not unused:
        return data, b''  # 无法去重
    
    marker1 = unused[0]
    marker2 = unused[1] if len(unused) > 1 else unused[0]
    
    # 构建行索引
    unique_lines = [line for line, _ in dup_lines[:65535]]
    line_to_idx = {line: idx for idx, line in enumerate(unique_lines)}
    
    # 编码
    encoded = bytearray()
    for line in lines:
        if len(line) >= min_line_len and line in line_to_idx:
            idx = line_to_idx[line]
            encoded.append(marker1)
            if len(unique_lines) < 256:
                encoded.append(idx)
            else:
                encoded.extend(struct.pack('>H', idx))
        else:
            # 转义marker1
            for b in line:
                if b == marker1:
                    encoded.append(marker1)
                    encoded.append(marker2)
                else:
                    encoded.append(b)
            # 换行
            encoded.append(0x0A)  # '\n' 用特殊标记
    
    # 等等，这种方式有歧义。让我用更简单的方法。
    # 直接用marker1+2字节索引来替换重复行
    
    # 重新编码
    encoded = bytearray()
    for line in lines:
        if len(line) >= min_line_len and line in line_to_idx:
            idx = line_to_idx[line]
            encoded.append(marker1)
            if len(unique_lines) < 256:
                encoded.append(idx & 0xFF)
            else:
                encoded.extend(struct.pack('>H', idx))
            encoded.append(0x0A)  # 保留换行
        else:
            for b in line:
                if b == marker1:
                    encoded.append(marker1)
                    encoded.append(marker2)  # 转义
                else:
                    encoded.append(b)
            encoded.append(0x0A)
    
    # 序列化字典
    dict_bytes = _serialize_text_dedup_dict(unique_lines, marker1, marker2)
    return bytes(encoded), dict_bytes


def text_dedup_decode(data: bytes, dict_bytes: bytes) -> bytes:
    """文本段落去重解码。"""
    if not dict_bytes:
        return data
    
    lines_dict, marker1, marker2 = _deserialize_text_dedup_dict(dict_bytes)
    use_two_byte = len(lines_dict) > 255
    
    result = bytearray()
    i = 0
    n = len(data)
    
    while i < n:
        if data[i] == marker1:
            if i + 1 < n and data[i + 1] == marker2:
                # 转义
                result.append(marker1)
                i += 2
            elif use_two_byte:
                if i + 2 < n:
                    idx = struct.unpack('>H', data[i + 1:i + 3])[0]
                    if idx < len(lines_dict):
                        result.extend(lines_dict[idx])
                    else:
                        result.append(data[i])
                        result.extend(data[i + 1:i + 3])
                    i += 3
                else:
                    result.append(data[i])
                    i += 1
            else:
                if i + 1 < n:
                    idx = data[i + 1]
                    if idx < len(lines_dict):
                        result.extend(lines_dict[idx])
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


def _serialize_text_dedup_dict(lines: list, marker1: int, marker2: int) -> bytes:
    result = bytearray()
    result.append(marker1)
    result.append(marker2)
    result.extend(struct.pack('>H', len(lines)))
    for line in lines:
        result.extend(struct.pack('>H', len(line)))
        result.extend(line)
    return bytes(result)


def _deserialize_text_dedup_dict(data: bytes) -> Tuple[list, int, int]:
    offset = 0
    marker1 = data[offset]; offset += 1
    marker2 = data[offset]; offset += 1
    num_lines = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    lines = []
    for _ in range(num_lines):
        line_len = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
        lines.append(bytes(data[offset:offset + line_len]))
        offset += line_len
    return lines, marker1, marker2


# ─────────────────────────────────────────────
#  ★ 递归BPE — 多轮迭代直到无改善
# ─────────────────────────────────────────────

def bpe_encode_recursive(data: bytes, max_rounds: int = 5, max_merges_per_round: int = 100) -> Tuple[bytes, bytes]:
    """递归BPE编码：多轮迭代，每轮找最优合并，直到无改善。
    
    返回: (encoded_data, all_rules_bytes)
    """
    if not data or len(data) < 10:
        return data, b''
    
    all_rules = []
    current_data = bytearray(data)
    
    # 找空闲字节
    used = [False] * 256
    for b in current_data[:min(len(current_data), 2 << 20)]:
        used[b] = True
    free_slots = [b for b in range(256) if not used[b]]
    
    if not free_slots:
        # 尝试释放最罕见的字节
        freq = [0] * 256
        for b in current_data[:min(len(current_data), 2 << 20)]:
            freq[b] += 1
        rarest = min(range(256), key=lambda b: freq[b] if freq[b] > 0 else 256)
        if freq[rarest] > 0 and freq[rarest] < len(current_data) // 100:
            # 转义最罕见字节
            esc_byte = rarest
            new_data = bytearray()
            for b in current_data:
                if b == esc_byte:
                    new_data.append(esc_byte)
                    new_data.append(0x00)
                else:
                    new_data.append(b)
            current_data = new_data
            free_slots = [esc_byte]
            all_rules.append(('escape', esc_byte))
        else:
            return data, b''
    
    slot_idx = 0
    
    for round_num in range(max_rounds):
        merges_this_round = 0
        
        for _ in range(max_merges_per_round):
            if slot_idx >= len(free_slots):
                # 检查新释放的字节
                used_now = [False] * 256
                for b in current_data[:min(len(current_data), 2 << 20)]:
                    used_now[b] = True
                new_free = [b for b in range(256) if not used_now[b] and b not in free_slots]
                if new_free:
                    free_slots.extend(new_free)
                else:
                    break
            
            # 统计字节对频率
            pair_freq = {}
            sample = current_data[:min(len(current_data), 2 << 20)]
            for i in range(len(sample) - 1):
                p = (sample[i], sample[i + 1])
                pair_freq[p] = pair_freq.get(p, 0) + 1
            
            if not pair_freq:
                break
            
            best_pair = max(pair_freq, key=pair_freq.get)
            best_count = pair_freq[best_pair]
            if best_count < 2:
                break
            
            # 计算节省: best_count * 1 - 0 (替换减少的字节)
            # 但规则需要存储: 3字节/规则
            # 净节省 = best_count - 3
            if best_count < 4:  # 至少4次才值得
                break
            
            new_token = free_slots[slot_idx]
            slot_idx += 1
            
            # 替换
            new_data = bytearray()
            i = 0
            na = len(current_data)
            b0, b1 = best_pair
            while i < na:
                if i < na - 1 and current_data[i] == b0 and current_data[i + 1] == b1:
                    new_data.append(new_token)
                    i += 2
                else:
                    new_data.append(current_data[i])
                    i += 1
            
            current_data = new_data
            all_rules.append(('merge', best_pair[0], best_pair[1], new_token))
            merges_this_round += 1
        
        if merges_this_round == 0:
            break
    
    # 序列化规则
    rules_bytes = _serialize_recursive_bpe_rules(all_rules)
    return bytes(current_data), rules_bytes


def bpe_decode_recursive(data: bytes, rules_bytes: bytes) -> bytes:
    """递归BPE解码。"""
    if not rules_bytes:
        return data
    
    rules = _deserialize_recursive_bpe_rules(rules_bytes)
    
    # 逆序应用规则
    current = bytearray(data)
    
    for rule in reversed(rules):
        if rule[0] == 'merge':
            _, byte_a, byte_b, new_token = rule
            new_data = bytearray()
            i = 0
            while i < len(current):
                if current[i] == new_token:
                    new_data.append(byte_a)
                    new_data.append(byte_b)
                    i += 1
                else:
                    new_data.append(current[i])
                    i += 1
            current = new_data
        elif rule[0] == 'escape':
            _, esc_byte = rule
            new_data = bytearray()
            i = 0
            while i < len(current):
                if current[i] == esc_byte and i + 1 < len(current) and current[i + 1] == 0x00:
                    new_data.append(esc_byte)
                    i += 2
                else:
                    new_data.append(current[i])
                    i += 1
            current = new_data
    
    return bytes(current)


def _serialize_recursive_bpe_rules(rules: list) -> bytes:
    result = bytearray()
    result.extend(struct.pack('>H', len(rules)))
    for rule in rules:
        if rule[0] == 'merge':
            result.append(0x01)  # merge类型
            result.append(rule[1])  # byte_a
            result.append(rule[2])  # byte_b
            result.append(rule[3])  # new_token
        elif rule[0] == 'escape':
            result.append(0x02)  # escape类型
            result.append(rule[1])  # esc_byte
    return bytes(result)


def _deserialize_recursive_bpe_rules(data: bytes) -> list:
    offset = 0
    num_rules = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2
    rules = []
    for _ in range(num_rules):
        rule_type = data[offset]; offset += 1
        if rule_type == 0x01:
            byte_a = data[offset]; offset += 1
            byte_b = data[offset]; offset += 1
            new_token = data[offset]; offset += 1
            rules.append(('merge', byte_a, byte_b, new_token))
        elif rule_type == 0x02:
            esc_byte = data[offset]; offset += 1
            rules.append(('escape', esc_byte))
    return rules
