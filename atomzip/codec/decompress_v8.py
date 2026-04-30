"""
AtomZip 解压引擎 v8 — 支持所有v7和v8策略

v8新增策略逆变换:
  30: 超激进BPE + BWT + LZMA2
  31: 超激进BPE + LZMA2
  32: BWT + BZ2 / 超激进BPE + BWT + BZ2
  34: 词级字典 + BWT + LZMA2
  33: 词级字典 + 超激进BPE + BWT + LZMA2
  36: 增强N-gram + BWT + LZMA2
  37: 超激进BPE + 增强N-gram + BWT + LZMA2
  38: 文本字典 + 超激进BPE + BWT + LZMA2
  39: JSON键去重 + 超激进BPE + BWT + LZMA2
  40: 日志模板 + 超激进BPE + BWT + LZMA2
"""

import struct
import time
import lzma
import bz2

from .transform_v8 import (
    bwt_decode, delta_decode, rle_decode,
    text_dict_decode, json_key_dedup_decode,
    log_template_decode, log_field_decode,
    column_transpose_decode,
    bpe_decode, ngram_dict_decode,
    csv_column_decode, json_flatten_decode,
    deserialize_block_info,
    # v8新增
    bpe_decode_ultra,
    word_dict_decode,
    ngram_dict_decode_v8,
)
from .compress_v8 import ATOMZIP_MAGIC, FORMAT_VERSION, _get_lzma_filters


class AtomZipDecompressor:
    """AtomZip v8 解压器"""

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
        if version not in (4, 5, 6, 7, 8):
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
            print(f"[AtomZip v8] 解压完成: {len(data):,} -> {len(result):,} 字节 "
                  f"(耗时: {elapsed:.3f}秒, 策略: {strategy})")

        return result

    def _is_bz2_strategy(self, strategy, extra_header):
        """检查是否使用BZ2后端"""
        if strategy == 32 and extra_header:
            # 检查extra_header末尾是否有BZ2标记
            # BZ2策略在extra_header最后有0x01标记
            # 需要解析来确定
            return self._check_bz2_marker(extra_header)
        return False

    def _check_bz2_marker(self, extra_header):
        """检查extra_header中是否有BZ2标记"""
        if not extra_header or len(extra_header) < 2:
            return False
        # BZ2策略在extra末尾放了一个0x01标记
        # 需要从后往前找
        # 简化: 如果策略是32且extra_header包含BZ2标记字节
        try:
            # 跳过block_info找BZ2标记
            offset = 0
            if offset + 2 <= len(extra_header):
                num_blocks = struct.unpack('>H', extra_header[offset:offset + 2])[0]
                offset += 2 + num_blocks * 8
                if offset < len(extra_header) and extra_header[offset] == 0x01:
                    return True
            # 检查是否有v8标记 + BPE rules
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
        if not extra_header: return info

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
        """跳过策略特定的前置数据"""
        offset = 0
        
        # v8策略: 检查0x08标记
        v8_strategies = (30, 31, 33, 34, 36, 37, 38, 39, 40)
        
        if strategy in v8_strategies and extra_header and extra_header[0] == 0x08:
            offset = 1  # 跳过v8标记
            return self._skip_v8_prefix(strategy, extra_header, offset)
        
        # 复用v7的逻辑
        if strategy in (0, 1, 10, 11):
            return 0
        if strategy in (2, 3):
            return self._skip_block_infos(extra_header, 1)
        if strategy in (4, 9):
            return self._skip_block_infos(extra_header, 1)
        if strategy == 5:
            if len(extra_header) < 4: return -1
            dict_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + dict_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 6:
            if len(extra_header) < 4: return -1
            schema_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + schema_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 7:
            if len(extra_header) < 4: return -1
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
        if strategy == 12:
            return self._skip_block_infos(extra_header, 2)
        if strategy == 13:
            offset = 4
            if offset + 4 <= len(extra_header):
                bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
                offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 14:
            if len(extra_header) < 4: return -1
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 15:
            if len(extra_header) < 4: return -1
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            return 4 + bpe_len
        if strategy == 16:
            if len(extra_header) < 4: return -1
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + ngram_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 17:
            if len(extra_header) < 4: return -1
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            return 4 + ngram_len
        if strategy == 18:
            if len(extra_header) < 4: return -1
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + bpe_len
            if offset + 4 > len(extra_header): return -1
            ngram_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + ngram_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        # strategies 19-28 复用v7逻辑
        if strategy in (19, 20, 21, 22):
            if len(extra_header) < 4: return -1
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy in (23, 25, 26):
            if len(extra_header) < 4: return -1
            d_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + d_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy == 24:
            if len(extra_header) < 4: return -1
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + meta_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        if strategy in (27, 28):
            offset = 2
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        return 0

    def _skip_v8_prefix(self, strategy, extra_header, offset):
        """跳过v8策略的前缀数据 (在0x08标记之后)"""
        # 策略30: ubpe_bwt -> bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 30:
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        # 策略31: ubpe_lzma -> bpe_rules_len(4) + bpe_rules
        if strategy == 31:
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            return offset + 4 + bpe_len
        
        # 策略33: word_dict + ubpe + bwt -> dict_len(4) + dict + bpe_len(4) + bpe + block_info
        if strategy == 33:
            if offset + 4 > len(extra_header): return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        # 策略34: word_dict + bwt -> dict_len(4) + dict + block_info
        if strategy == 34:
            if offset + 4 > len(extra_header): return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        # 策略36: ngram_v8 + bwt -> dict_len(4) + dict + block_info
        if strategy == 36:
            if offset + 4 > len(extra_header): return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        # 策略37: ubpe + ngram_v8 + bwt -> bpe_len(4) + bpe + ngram_len(4) + ngram + block_info
        if strategy == 37:
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            if offset + 4 > len(extra_header): return -1
            ngram_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + ngram_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        # 策略38: text_dict + ubpe + bwt -> dict_len(4) + dict + bpe_len(4) + bpe + block_info
        if strategy == 38:
            if offset + 4 > len(extra_header): return -1
            dict_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + dict_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        # 策略39: json_dedup + ubpe + bwt -> schema_len(4) + schema + bpe_len(4) + bpe + block_info
        if strategy == 39:
            if offset + 4 > len(extra_header): return -1
            schema_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + schema_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        # 策略40: log_template + ubpe + bwt -> template_len(4) + template + bpe_len(4) + bpe + block_info
        if strategy == 40:
            if offset + 4 > len(extra_header): return -1
            template_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + template_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi
        
        return 0

    def _skip_block_infos(self, data, count):
        offset = 0
        for _ in range(count):
            if offset + 2 > len(data): return offset
            num_blocks = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2 + num_blocks * 8
        return offset

    def _try_skip_block_info(self, data):
        if len(data) < 2: return 0
        num_blocks = struct.unpack('>H', data[:2])[0]
        if num_blocks == 0: return 2
        if num_blocks > 1000: return 0
        needed = 2 + num_blocks * 8
        if needed + 8 > len(data): return 0
        return needed

    def _read_filters_from_offset(self, data, start, info):
        if start + 8 > len(data): return
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
        # === v7策略 (与v7解压器相同) ===
        if strategy == 0: return intermediate
        elif strategy == 1: return intermediate
        elif strategy == 2:
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(intermediate, block_infos[0])
            return intermediate
        elif strategy == 3:
            rle_decoded = rle_decode(intermediate)
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(rle_decoded, block_infos[0])
            return rle_decoded
        elif strategy == 4:
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(intermediate, block_infos[0])
            return intermediate
        elif strategy in (10, 11): return intermediate
        elif strategy == 5:
            dict_len = struct.unpack('>I', extra_header[:4])[0]
            dict_bytes = extra_header[4:4 + dict_len]
            remaining = extra_header[4 + dict_len:]
            block_info = self._try_extract_block_info(remaining)
            if block_info:
                bwt_decoded = bwt_decode(intermediate, block_info)
                return text_dict_decode(bwt_decoded, dict_bytes)
            return text_dict_decode(intermediate, dict_bytes)
        elif strategy == 6:
            schema_len = struct.unpack('>I', extra_header[:4])[0]
            schema_bytes = extra_header[4:4 + schema_len]
            remaining = extra_header[4 + schema_len:]
            block_info = self._try_extract_block_info(remaining)
            if block_info:
                bwt_decoded = bwt_decode(intermediate, block_info)
                return json_key_dedup_decode(bwt_decoded, schema_bytes)
            return json_key_dedup_decode(intermediate, schema_bytes)
        elif strategy == 7:
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
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(rle_decoded, block_infos[0])
            return rle_decoded
        elif strategy == 12:
            block_infos, _ = self._extract_block_infos(extra_header, 2)
            if len(block_infos) >= 2 and block_infos[1] and block_infos[0]:
                decoded2 = bwt_decode(intermediate, block_infos[1])
                decoded1 = bwt_decode(decoded2, block_infos[0])
                return decoded1
            return intermediate
        elif strategy == 13:
            extra = extra_header
            offset = 0
            if extra[offset] == 0x13:
                offset += 1
                rec_size = struct.unpack('>H', extra[offset:offset + 2])[0]; offset += 2
                first_byte = extra[offset]; offset += 1
                bpe_rules = b''
                if offset + 4 <= len(extra):
                    bpe_len = struct.unpack('>I', extra[offset:offset + 4])[0]
                    bpe_rules = extra[offset + 4:offset + 4 + bpe_len]
                    offset += 4 + bpe_len
                remaining = extra[offset:]
                block_info = self._try_extract_block_info(remaining)
                data = intermediate
                if block_info:
                    data = bwt_decode(data, block_info)
                if bpe_rules:
                    data = bpe_decode(data, bpe_rules)
                return delta_decode(data, first_byte, rec_size)
            block_infos, _ = self._extract_block_infos(extra, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(intermediate, block_infos[0])
            return intermediate

        # === v7 BPE/N-gram策略 ===
        elif strategy == 14:
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            remaining = extra_header[4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode(data, bpe_rules)
        elif strategy == 15:
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            return bpe_decode(intermediate, bpe_rules)
        elif strategy == 16:
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            ngram_dict = extra_header[4:4 + ngram_len]
            remaining = extra_header[4 + ngram_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return ngram_dict_decode(data, ngram_dict)
        elif strategy == 17:
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            ngram_dict = extra_header[4:4 + ngram_len]
            return ngram_dict_decode(intermediate, ngram_dict)
        elif strategy == 18:
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            offset = 4 + bpe_len
            ngram_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            ngram_dict = extra_header[offset + 4:offset + 4 + ngram_len]
            remaining = extra_header[offset + 4 + ngram_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = ngram_dict_decode(data, ngram_dict)
            return bpe_decode(data, bpe_rules)
        elif strategy == 19:
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            remaining = extra_header[4 + meta_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return csv_column_decode(data, meta_bytes)
        elif strategy == 20:
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            remaining = extra_header[4 + meta_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return json_flatten_decode(data, meta_bytes)
        elif strategy == 21:
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            remaining = extra_header[4 + meta_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return log_field_decode(data, meta_bytes)
        elif strategy == 22:
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            remaining = extra_header[4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode(data, bpe_rules)
        elif strategy == 23:
            dict_len = struct.unpack('>I', extra_header[:4])[0]
            dict_bytes = extra_header[4:4 + dict_len]
            offset = 4 + dict_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            remaining = extra_header[offset + 4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode(data, bpe_rules)
            return text_dict_decode(data, dict_bytes)
        elif strategy == 24:
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            offset = 4 + meta_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            remaining = extra_header[offset + 4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode(data, bpe_rules)
            return csv_column_decode(data, meta_bytes)
        elif strategy == 25:
            schema_len = struct.unpack('>I', extra_header[:4])[0]
            schema_bytes = extra_header[4:4 + schema_len]
            offset = 4 + schema_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            remaining = extra_header[offset + 4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode(data, bpe_rules)
            return json_key_dedup_decode(data, schema_bytes)
        elif strategy == 26:
            template_len = struct.unpack('>I', extra_header[:4])[0]
            template_bytes = extra_header[4:4 + template_len]
            offset = 4 + template_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            remaining = extra_header[offset + 4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode(data, bpe_rules)
            return log_template_decode(data, template_bytes)
        elif strategy == 27:
            row_width = struct.unpack('>H', extra_header[:2])[0]
            offset = 2
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            remaining = extra_header[offset + 4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode(data, bpe_rules)
            return column_transpose_decode(data, row_width)
        elif strategy == 28:
            row_width = struct.unpack('>H', extra_header[:2])[0]
            offset = 2
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            remaining = extra_header[offset + 4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode(data, bpe_rules)
            return column_transpose_decode(data, row_width)

        # === ★ v8新策略逆变换 ===
        elif strategy == 30:
            # 超激进BPE + BWT + LZMA2
            offset = 1  # 跳过0x08标记
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode_ultra(data, bpe_rules)

        elif strategy == 31:
            # 超激进BPE + LZMA2
            offset = 1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            return bpe_decode_ultra(intermediate, bpe_rules)

        elif strategy == 32:
            # BWT + BZ2 或 超激进BPE + BWT + BZ2
            # 检查是否有BPE规则
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

        elif strategy == 33:
            # 词级字典 + 超激进BPE + BWT + LZMA2
            offset = 1  # 0x08标记
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
            data = bpe_decode_ultra(data, bpe_rules)
            return word_dict_decode(data, dict_bytes)

        elif strategy == 34:
            # 词级字典 + BWT + LZMA2
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

        elif strategy == 36:
            # 增强N-gram + BWT + LZMA2
            offset = 1
            ngram_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            ngram_dict = extra_header[offset + 4:offset + 4 + ngram_len]
            offset += 4 + ngram_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return ngram_dict_decode_v8(data, ngram_dict)

        elif strategy == 37:
            # 超激进BPE + 增强N-gram + BWT + LZMA2
            offset = 1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            ngram_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            ngram_dict = extra_header[offset + 4:offset + 4 + ngram_len]
            offset += 4 + ngram_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = ngram_dict_decode_v8(data, ngram_dict)
            return bpe_decode_ultra(data, bpe_rules)

        elif strategy == 38:
            # 文本字典 + 超激进BPE + BWT + LZMA2
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
            data = bpe_decode_ultra(data, bpe_rules)
            return text_dict_decode(data, dict_bytes)

        elif strategy == 39:
            # JSON键去重 + 超激进BPE + BWT + LZMA2
            offset = 1
            schema_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            schema_bytes = extra_header[offset + 4:offset + 4 + schema_len]
            offset += 4 + schema_len
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

        elif strategy == 40:
            # 日志模板 + 超激进BPE + BWT + LZMA2
            offset = 1
            template_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            template_bytes = extra_header[offset + 4:offset + 4 + template_len]
            offset += 4 + template_len
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            bpe_rules = extra_header[offset + 4:offset + 4 + bpe_len]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            data = bpe_decode_ultra(data, bpe_rules)
            return log_template_decode(data, template_bytes)

        else:
            raise ValueError(f"未知的压缩策略: {strategy}")

    def _extract_block_infos(self, extra_header, count):
        block_infos = []
        offset = 0
        for _ in range(count):
            if offset + 2 > len(extra_header): break
            num_blocks = struct.unpack('>H', extra_header[offset:offset + 2])[0]
            bi = []
            offset += 2
            for _ in range(num_blocks):
                if offset + 8 > len(extra_header): break
                orig_idx = struct.unpack('>I', extra_header[offset:offset + 4])[0]; offset += 4
                block_size = struct.unpack('>I', extra_header[offset:offset + 4])[0]; offset += 4
                bi.append((orig_idx, block_size))
            block_infos.append(bi)
        return block_infos, offset

    def _try_extract_block_info(self, remaining):
        if len(remaining) < 10: return None
        try:
            num_blocks = struct.unpack('>H', remaining[:2])[0]
            if num_blocks == 0 or num_blocks > 1000: return None
            needed = 2 + num_blocks * 8
            if needed + 8 > len(remaining): return None
            bi = []
            offset = 2
            for _ in range(num_blocks):
                orig_idx = struct.unpack('>I', remaining[offset:offset + 4])[0]; offset += 4
                block_size = struct.unpack('>I', remaining[offset:offset + 4])[0]; offset += 4
                bi.append((orig_idx, block_size))
            return bi
        except Exception:
            return None


def _smart_dict_size_default(original_size):
    return max(1 << 16, min(original_size, 1 << 30))
