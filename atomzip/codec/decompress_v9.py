"""
AtomZip 解压引擎 v9 — 支持所有v7/v8/v9策略

v9新增策略逆变换:
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
  62: 文本段落去重 + 全局去重 + BWT + LZMA2
  63: 递归BPE + BWT + LZMA2
"""

import struct
import time
import lzma
import bz2

from .transform_v9 import (
    bwt_decode, delta_decode, rle_decode,
    text_dict_decode, json_key_dedup_decode,
    log_template_decode, log_field_decode,
    column_transpose_decode,
    bpe_decode, ngram_dict_decode,
    csv_column_decode, json_flatten_decode,
    deserialize_block_info,
    bpe_decode_ultra,
    word_dict_decode,
    ngram_dict_decode_v8,
    # v9新增
    deep_json_decode,
    deep_log_decode,
    deep_csv_decode,
    global_dedup_decode,
    text_dedup_decode,
    bpe_decode_recursive,
)
from .compress_v9 import ATOMZIP_MAGIC, FORMAT_VERSION, _get_lzma_filters


class AtomZipDecompressor:
    """AtomZip v9 解压器"""

    def __init__(self, verbose=False):
        self.verbose = verbose

    def decompress(self, data: bytes) -> bytes:
        start_time = time.time()
        offset = 0

        if len(data) < 14:
            raise ValueError("数据过短，不是有效的 AtomZip 文件")

        magic = data[offset:offset + 4]; offset += 4
        if magic != ATOMZIP_MAGIC:
            raise ValueError(f"无效的文件魔数: {magic!r}")

        version = data[offset]; offset += 1
        if version not in (4, 5, 6, 7, 8, 9):
            raise ValueError(f"不支持的版本号: {version}")

        original_size = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        strategy = data[offset]; offset += 1

        if version >= 7:
            extra_size = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        else:
            extra_size = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2

        if original_size == 0:
            return b''

        extra_header = data[offset:offset + extra_size]; offset += extra_size
        compressed_data_len = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        compressed_data = data[offset:offset + compressed_data_len]

        # 判断后端类型
        is_bz2 = self._is_bz2_strategy(strategy, extra_header)

        if is_bz2:
            intermediate = bz2.decompress(compressed_data)
        else:
            filters = self._build_filters(strategy, extra_header, original_size)
            intermediate = lzma.decompress(compressed_data, format=lzma.FORMAT_RAW, filters=filters)

        result = self._reverse_strategy(strategy, intermediate, extra_header, original_size)
        result = result[:original_size]

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"[AtomZip v9] 解压完成: {len(data):,} -> {len(result):,} 字节 "
                  f"(耗时: {elapsed:.3f}秒, 策略: {strategy})")

        return result

    def _is_bz2_strategy(self, strategy, extra_header):
        if strategy == 32 and extra_header:
            return self._check_bz2_marker(extra_header)
        return False

    def _check_bz2_marker(self, extra_header):
        if not extra_header or len(extra_header) < 2:
            return False
        try:
            offset = 0
            if offset + 2 <= len(extra_header):
                num_blocks = struct.unpack('>H', extra_header[offset:offset + 2])[0]
                offset += 2 + num_blocks * 8
                if offset < len(extra_header) and extra_header[offset] == 0x01:
                    return True
            if extra_header[0] == 0x08:
                offset = 1
                if offset + 4 <= len(extra_header):
                    bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
                    offset += 4 + bpe_len
                    if offset + 2 <= len(extra_header):
                        num_blocks = struct.unpack('>H', extra_header[offset:offset + 2])[0]
                        offset += 2 + num_blocks * 8
                        if offset < len(extra_header) and extra_header[offset] == 0x01:
                            return True
        except Exception:
            pass
        return False

    def _build_filters(self, strategy, extra_header, original_size):
        filters_info = self._extract_filters_info(strategy, extra_header)
        filters = []
        if filters_info.get('bcj', False):
            filters.append({'id': lzma.FILTER_X86})
        delta_dist = filters_info.get('delta_dist', 0)
        if delta_dist > 0:
            filters.append({'id': lzma.FILTER_DELTA, 'dist': delta_dist})
        dict_size = filters_info.get('dict_size', _smart_dict_size_default(original_size))
        lzma2_filter = {
            'id': lzma.FILTER_LZMA2,
            'preset': 9 | lzma.PRESET_EXTREME,
            'lc': filters_info.get('lc', 3),
            'lp': filters_info.get('lp', 0),
            'pb': filters_info.get('pb', 2),
            'dict_size': dict_size,
        }
        filters.append(lzma2_filter)
        return filters

    def _extract_filters_info(self, strategy, extra_header):
        info = {'lc': 3, 'lp': 0, 'pb': 2, 'dict_size': 0, 'delta_dist': 0, 'bcj': False}
        if not extra_header:
            return info

        try:
            offset = self._skip_strategy_prefix(strategy, extra_header)
            if offset < 0 or offset + 8 > len(extra_header):
                return info
            self._read_filters_from_offset(extra_header, offset, info)
        except Exception:
            pass

        if info['lc'] + info['lp'] > 4 or info['dict_size'] == 0:
            info.update({'lc': 3, 'lp': 0, 'pb': 2})
            info['dict_size'] = 0

        return info

    def _skip_strategy_prefix(self, strategy, extra_header):
        offset = 0

        # v9策略: 检查0x09标记
        v9_strategies = (50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 62, 63)
        if strategy in v9_strategies and extra_header and extra_header[0] == 0x09:
            return self._skip_v9_prefix(strategy, extra_header)

        # v8策略
        v8_strategies = (30, 31, 33, 34, 36, 37, 38, 39, 40)
        if strategy in v8_strategies and extra_header and extra_header[0] == 0x08:
            offset = 1
            return self._skip_v8_prefix(strategy, extra_header, offset)

        # 基本策略
        if strategy in (0, 1, 10, 11):
            return 0
        if strategy in (2, 3):
            return self._skip_block_infos(extra_header, 1)
        if strategy in (4, 9):
            return self._skip_block_infos(extra_header, 1)
        if strategy == 5:
            if len(extra_header) < 4:
                return -1
            dict_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + dict_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 6:
            if len(extra_header) < 4:
                return -1
            schema_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + schema_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 7:
            if len(extra_header) < 4:
                return -1
            template_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + template_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 8:
            offset = 2
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy in (14, 15):
            if len(extra_header) < 4:
                return -1
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            if strategy == 14:
                remaining = extra_header[4 + bpe_len:]
                bi = self._try_skip_block_info(remaining)
                return 4 + bpe_len + bi
            return 4 + bpe_len
        if strategy in (19, 20, 21, 22):
            if len(extra_header) < 4:
                return -1
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy in (27, 28):
            offset = 2
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        return 0

    def _skip_v9_prefix(self, strategy, extra_header):
        """跳过v9策略的前缀数据 (在0x09标记之后)"""
        offset = 1  # 跳过0x09标记

        # 策略50: deep_json + BWT
        if strategy == 50:
            if offset + 4 > len(extra_header):
                return -1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略51: deep_json + rbpe + BWT
        if strategy == 51:
            if offset + 4 > len(extra_header):
                return -1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + meta_len
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略52: deep_log + BWT
        if strategy == 52:
            if offset + 4 > len(extra_header):
                return -1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略53: deep_log + rbpe + BWT
        if strategy == 53:
            if offset + 4 > len(extra_header):
                return -1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + meta_len
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略54: deep_csv + BWT
        if strategy == 54:
            if offset + 4 > len(extra_header):
                return -1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略55: deep_csv + rbpe + BWT
        if strategy == 55:
            if offset + 4 > len(extra_header):
                return -1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + meta_len
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略56: global_dedup + BWT
        if strategy == 56:
            if offset + 4 > len(extra_header):
                return -1
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dedup_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略57: global_dedup + rbpe + BWT
        if strategy == 57:
            if offset + 4 > len(extra_header):
                return -1
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dedup_len
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略58: text_dedup + BWT
        if strategy == 58:
            if offset + 4 > len(extra_header):
                return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略59: text_dedup + rbpe + BWT
        if strategy == 59:
            if offset + 4 > len(extra_header):
                return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略60: deep_* + global_dedup + BWT
        if strategy == 60:
            if offset + 4 > len(extra_header):
                return -1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + meta_len
            if offset + 4 > len(extra_header):
                return -1
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dedup_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略62: text_dedup + global_dedup + BWT
        if strategy == 62:
            if offset + 4 > len(extra_header):
                return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            if offset + 4 > len(extra_header):
                return -1
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dedup_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略63: rbpe + BWT
        if strategy == 63:
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        return 0

    def _skip_v8_prefix(self, strategy, extra_header, offset):
        """跳过v8策略的前缀数据"""
        if strategy == 30:
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 31:
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            return offset + 4 + bpe_len
        if strategy == 34:
            if offset + 4 > len(extra_header):
                return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 33:
            if offset + 4 > len(extra_header):
                return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 39:
            if offset + 4 > len(extra_header):
                return -1
            schema_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + schema_len
            if offset + 4 > len(extra_header):
                return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        return 0

    def _skip_block_infos(self, data, count):
        offset = 0
        for _ in range(count):
            if offset + 2 > len(data):
                return offset
            num_blocks = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2 + num_blocks * 8
        return offset

    def _try_skip_block_info(self, data):
        if len(data) < 2:
            return 0
        num_blocks = struct.unpack('>H', data[:2])[0]
        if num_blocks == 0:
            return 2
        if num_blocks > 1000:
            return 0
        needed = 2 + num_blocks * 8
        if needed + 8 > len(data):
            return 0
        return needed

    def _read_filters_from_offset(self, data, start, info):
        if start + 8 > len(data):
            return
        offset = start
        flags = data[offset]
        info['dict_size'] = struct.unpack('>I', data[offset + 1:offset + 5])[0]
        info['lc'] = data[offset + 5]
        info['lp'] = data[offset + 6]
        info['pb'] = data[offset + 7]
        if flags & 0x01 and offset + 10 <= len(data):
            info['delta_dist'] = struct.unpack('>H', data[offset + 8:offset + 10])[0]
        if flags & 0x02:
            info['bcj'] = True

    def _reverse_strategy(self, strategy, intermediate, extra_header, original_size):
        # === 基本策略 ===
        if strategy == 0:
            return intermediate
        elif strategy == 1:
            return intermediate
        elif strategy == 2:
            block_info = self._try_extract_block_info(extra_header)
            if block_info:
                return bwt_decode(intermediate, block_info)
            return intermediate
        elif strategy == 3:
            rle_decoded = rle_decode(intermediate)
            block_info = self._try_extract_block_info_from_offset(extra_header, 0)
            if block_info:
                return bwt_decode(rle_decoded, block_info)
            return rle_decoded
        elif strategy == 4:
            block_info = self._try_extract_block_info(extra_header)
            if block_info:
                return bwt_decode(intermediate, block_info)
            return intermediate
        elif strategy in (10, 11):
            return intermediate
        elif strategy == 5:
            if len(extra_header) < 4:
                return intermediate
            dict_len = struct.unpack('>I', extra_header[:4])[0]
            dict_bytes = extra_header[4:4 + dict_len]
            remaining = extra_header[4 + dict_len:]
            block_info = self._try_extract_block_info(remaining)
            if block_info:
                bwt_decoded = bwt_decode(intermediate, block_info)
                return text_dict_decode(bwt_decoded, dict_bytes)
            return text_dict_decode(intermediate, dict_bytes)
        elif strategy == 6:
            if len(extra_header) < 4:
                return intermediate
            schema_len = struct.unpack('>I', extra_header[:4])[0]
            schema_bytes = extra_header[4:4 + schema_len]
            remaining = extra_header[4 + schema_len:]
            block_info = self._try_extract_block_info(remaining)
            if block_info:
                bwt_decoded = bwt_decode(intermediate, block_info)
                return json_key_dedup_decode(bwt_decoded, schema_bytes)
            return json_key_dedup_decode(intermediate, schema_bytes)
        elif strategy == 7:
            if len(extra_header) < 4:
                return intermediate
            template_len = struct.unpack('>I', extra_header[:4])[0]
            template_bytes = extra_header[4:4 + template_len]
            remaining = extra_header[4 + template_len:]
            block_info = self._try_extract_block_info(remaining)
            if block_info:
                bwt_decoded = bwt_decode(intermediate, block_info)
                return log_template_decode(bwt_decoded, template_bytes)
            return log_template_decode(intermediate, template_bytes)
        elif strategy == 8:
            row_width = struct.unpack('>H', extra_header[:2])[0]
            remaining = extra_header[2:]
            block_info = self._try_extract_block_info(remaining)
            if block_info:
                bwt_decoded = bwt_decode(intermediate, block_info)
                return column_transpose_decode(bwt_decoded, row_width)
            return column_transpose_decode(intermediate, row_width)
        elif strategy == 9:
            rle_decoded = rle_decode(intermediate)
            block_info = self._try_extract_block_info(extra_header)
            if block_info:
                return bwt_decode(rle_decoded, block_info)
            return rle_decoded
        elif strategy == 14:
            if len(extra_header) < 4:
                return intermediate
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            remaining = extra_header[4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode(data, bpe_rules)
        elif strategy == 19:
            if len(extra_header) < 4:
                return intermediate
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            remaining = extra_header[4 + meta_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return csv_column_decode(data, meta_bytes)

        # === v8策略 ===
        elif strategy == 30:
            offset = 1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode_ultra(data, bpe_rules)
        elif strategy == 34:
            offset = 1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            dict_bytes = extra_header[offset + 4:offset + 4 + dict_len]
            offset += 4 + dict_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return word_dict_decode(data, dict_bytes)
        elif strategy == 39:
            offset = 1
            schema_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            schema_bytes = extra_header[offset + 4:offset + 4 + schema_len]
            offset += 4 + schema_len
            if offset < len(extra_header) and extra_header[offset] == 0x08:
                offset += 1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode_ultra(data, bpe_rules)
            return json_key_dedup_decode(data, schema_bytes)
        elif strategy == 32:
            if extra_header and extra_header[0] == 0x08:
                offset = 1
                bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
                bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
                offset += 4 + bpe_len
                remaining = extra_header[offset:]
                block_info = self._try_extract_block_info(remaining)
                data = intermediate
                if block_info:
                    data = bwt_decode(data, block_info)
                return bpe_decode_ultra(data, bpe_rules)
            else:
                block_info = self._try_extract_block_info(extra_header)
                if block_info:
                    return bwt_decode(intermediate, block_info)
                return intermediate

        # === ★ v9 深度提取策略 ===
        elif strategy == 50:
            # 深度JSON + BWT
            offset = 1  # 0x09标记
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            meta_bytes = extra_header[offset + 4:offset + 4 + meta_len]
            offset += 4 + meta_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return deep_json_decode(data, meta_bytes)

        elif strategy == 51:
            # 深度JSON + 递归BPE + BWT
            offset = 1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            meta_bytes = extra_header[offset + 4:offset + 4 + meta_len]
            offset += 4 + meta_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode_recursive(data, bpe_rules)
            return deep_json_decode(data, meta_bytes)

        elif strategy == 52:
            # 深度日志 + BWT
            offset = 1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            meta_bytes = extra_header[offset + 4:offset + 4 + meta_len]
            offset += 4 + meta_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return deep_log_decode(data, meta_bytes)

        elif strategy == 53:
            # 深度日志 + 递归BPE + BWT
            offset = 1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            meta_bytes = extra_header[offset + 4:offset + 4 + meta_len]
            offset += 4 + meta_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode_recursive(data, bpe_rules)
            return deep_log_decode(data, meta_bytes)

        elif strategy == 54:
            # 深度CSV + BWT
            offset = 1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            meta_bytes = extra_header[offset + 4:offset + 4 + meta_len]
            offset += 4 + meta_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return deep_csv_decode(data, meta_bytes)

        elif strategy == 55:
            # 深度CSV + 递归BPE + BWT
            offset = 1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            meta_bytes = extra_header[offset + 4:offset + 4 + meta_len]
            offset += 4 + meta_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode_recursive(data, bpe_rules)
            return deep_csv_decode(data, meta_bytes)

        elif strategy == 56:
            # 全局去重 + BWT
            offset = 1
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            dedup_bytes = extra_header[offset + 4:offset + 4 + dedup_len]
            offset += 4 + dedup_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return global_dedup_decode(data, dedup_bytes)

        elif strategy == 57:
            # 全局去重 + 递归BPE + BWT
            offset = 1
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            dedup_bytes = extra_header[offset + 4:offset + 4 + dedup_len]
            offset += 4 + dedup_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode_recursive(data, bpe_rules)
            return global_dedup_decode(data, dedup_bytes)

        elif strategy == 58:
            # 文本段落去重 + BWT
            offset = 1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            dict_bytes = extra_header[offset + 4:offset + 4 + dict_len]
            offset += 4 + dict_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return text_dedup_decode(data, dict_bytes)

        elif strategy == 59:
            # 文本段落去重 + 递归BPE + BWT
            offset = 1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            dict_bytes = extra_header[offset + 4:offset + 4 + dict_len]
            offset += 4 + dict_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode_recursive(data, bpe_rules)
            return text_dedup_decode(data, dict_bytes)

        elif strategy == 60:
            # 深度提取 + 全局去重 + BWT
            offset = 1
            meta_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            meta_bytes = extra_header[offset + 4:offset + 4 + meta_len]
            offset += 4 + meta_len
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4
            dedup_bytes = extra_header[offset:offset + dedup_len] if dedup_len > 0 else b''
            offset += dedup_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            if dedup_bytes:
                data = global_dedup_decode(data, dedup_bytes)
            # 判断meta类型 (根据meta_bytes开头判断)
            if meta_bytes and meta_bytes[0] == 0x09:
                # 检查meta的第二字节来区分类型
                # 需要更精确的判断...
                # 简化：根据meta内容判断
                try:
                    return deep_json_decode(data, meta_bytes)
                except Exception:
                    try:
                        return deep_log_decode(data, meta_bytes)
                    except Exception:
                        try:
                            return deep_csv_decode(data, meta_bytes)
                        except Exception:
                            return data
            return data

        elif strategy == 62:
            # 文本段落去重 + 全局去重 + BWT
            offset = 1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            dict_bytes = extra_header[offset + 4:offset + 4 + dict_len]
            offset += 4 + dict_len
            dedup_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4
            dedup_bytes = extra_header[offset:offset + dedup_len] if dedup_len > 0 else b''
            offset += dedup_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            if dedup_bytes:
                data = global_dedup_decode(data, dedup_bytes)
            return text_dedup_decode(data, dict_bytes)

        elif strategy == 63:
            # 递归BPE + BWT
            offset = 1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode_recursive(data, bpe_rules)

        else:
            raise ValueError(f"未知的压缩策略: {strategy}")

    def _try_extract_block_info(self, remaining):
        if len(remaining) < 10:
            return None
        try:
            num_blocks = struct.unpack('>H', remaining[:2])[0]
            if num_blocks == 0 or num_blocks > 1000:
                return None
            needed = 2 + num_blocks * 8
            if needed + 8 > len(remaining):
                return None
            bi = []
            offset = 2
            for _ in range(num_blocks):
                orig_idx = struct.unpack('>I', remaining[offset:offset + 4])[0]; offset += 4
                block_size = struct.unpack('>I', remaining[offset:offset + 4])[0]; offset += 4
                bi.append((orig_idx, block_size))
            return bi
        except Exception:
            return None

    def _try_extract_block_info_from_offset(self, extra_header, start):
        remaining = extra_header[start:]
        return self._try_extract_block_info(remaining)


def _smart_dict_size_default(original_size):
    return max(1 << 16, min(original_size, 1 << 30))
