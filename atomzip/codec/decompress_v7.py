"""
AtomZip 解压引擎 v7 — 多策略逆向流水线

支持所有v7策略:
  0: LZMA2 only
  1: LZMA2 Delta滤镜
  2: BWT + LZMA2
  3: BWT + RLE + LZMA2
  4: BWT + Delta滤镜 + LZMA2
  5: 文本字典 (+BWT) + LZMA2
  6: JSON键去重 (+BWT) + LZMA2
  7: 日志模板 (+BWT) + LZMA2
  8: 列转置 (+BWT/Delta) + LZMA2
  9: BWT + RLE + Delta + LZMA2
  10: BCJ + LZMA2
  11: BCJ + Delta + LZMA2
  12: 递归BWT (双层BWT)
  ★ v7新策略:
  14: BPE + BWT + LZMA2 / BPE + BWT + RLE + LZMA2
  15: BPE + LZMA2
  16: N-gram字典 + BWT + LZMA2
  17: N-gram字典 + LZMA2
  18: BPE + N-gram + BWT + LZMA2
  19: CSV列压缩 + BWT + LZMA2
  20: JSON扁平化 + BWT + LZMA2
  21: 日志字段压缩 + BWT + LZMA2
  22: BPE + Delta + BWT + LZMA2
  23: 文本字典 + BPE + BWT + LZMA2
  25: JSON键去重 + BPE + BWT + LZMA2
  26: 日志模板 + BPE + BWT + LZMA2
  27: 列转置 + BPE + BWT + LZMA2
  28: 列转置 + BPE + Delta + BWT + LZMA2
"""

import struct
import time
import lzma

from .transform_v7 import (
    bwt_decode, delta_decode,
    rle_decode,
    text_dict_decode,
    json_key_dedup_decode,
    log_template_decode,
    log_field_decode,
    column_transpose_decode,
    bpe_decode,
    ngram_dict_decode,
    csv_column_decode,
    json_flatten_decode,
    deserialize_block_info,
)
from .compress_v7 import ATOMZIP_MAGIC, FORMAT_VERSION, _get_lzma_filters


class AtomZipDecompressor:
    """AtomZip v7 解压器"""

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
        if version not in (4, 5, 6, 7):
            raise ValueError(f"不支持的版本号: {version}")

        original_size = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        strategy = data[offset]; offset += 1

        # v7使用4字节extra_header_size, v6及更早使用2字节
        if version >= 7:
            extra_size = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        else:
            extra_size = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2

        if original_size == 0:
            return b''

        extra_header = data[offset:offset + extra_size]; offset += extra_size
        lzma_data_len = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        lzma_data = data[offset:offset + lzma_data_len]

        # 构建解压滤镜
        filters = self._build_filters(strategy, extra_header, original_size)

        # LZMA2 RAW 解压
        intermediate = lzma.decompress(lzma_data, format=lzma.FORMAT_RAW, filters=filters)

        # 根据策略逆变换
        result = self._reverse_strategy(strategy, intermediate, extra_header, original_size)
        result = result[:original_size]

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"[AtomZip v7] 解压完成: {len(data):,} -> {len(result):,} 字节 "
                  f"(耗时: {elapsed:.3f}秒, 策略: {strategy})")

        return result

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
            # 滤镜信息总是在extra_header末尾
            # 格式: [flags(1B)] [dict_size(4B)] [lc(1B)] [lp(1B)] [pb(1B)] [delta_dist(2B,可选)]
            # 需要先跳过策略特定的前置数据

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
        """跳过策略特定的前置数据, 返回滤镜信息的起始偏移。"""
        offset = 0

        # 策略0,1: 无前置数据
        if strategy in (0, 1, 10, 11):
            return 0

        # 策略2,3: block_info
        if strategy in (2, 3):
            return self._skip_block_infos(extra_header, 1)

        # 策略4,9: block_info
        if strategy in (4, 9):
            return self._skip_block_infos(extra_header, 1)

        # 策略5: dict_len(4) + dict_bytes + block_info
        if strategy == 5:
            if len(extra_header) < 4: return -1
            dict_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + dict_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略6: schema_len(4) + schema_bytes + block_info
        if strategy == 6:
            if len(extra_header) < 4: return -1
            schema_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + schema_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略7: template_len(4) + template_bytes + block_info
        if strategy == 7:
            if len(extra_header) < 4: return -1
            template_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + template_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略8: row_width(2) + block_info
        if strategy == 8:
            offset = 2
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略12: 2x block_info
        if strategy == 12:
            return self._skip_block_infos(extra_header, 2)

        # 策略13: 0x13 + rec_size(2) + first_byte(1) + block_info
        if strategy == 13:
            offset = 4  # 0x13 + rec_size(2) + first_byte
            # 可能有BPE规则
            if offset + 4 <= len(extra_header):
                bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
                offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # ★ v7新策略
        # 策略14: bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 14:
            if len(extra_header) < 4: return -1
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略15: bpe_rules_len(4) + bpe_rules
        if strategy == 15:
            if len(extra_header) < 4: return -1
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            return 4 + bpe_len

        # 策略16: ngram_dict_len(4) + ngram_dict + block_info
        if strategy == 16:
            if len(extra_header) < 4: return -1
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + ngram_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略17: ngram_dict_len(4) + ngram_dict
        if strategy == 17:
            if len(extra_header) < 4: return -1
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            return 4 + ngram_len

        # 策略18: bpe_rules_len(4) + bpe_rules + ngram_dict_len(4) + ngram_dict + block_info
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

        # 策略19: csv_meta_len(4) + csv_meta + block_info
        if strategy == 19:
            if len(extra_header) < 4: return -1
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略20: json_meta_len(4) + json_meta + block_info
        if strategy == 20:
            if len(extra_header) < 4: return -1
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略21: log_meta_len(4) + log_meta + block_info
        if strategy == 21:
            if len(extra_header) < 4: return -1
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + meta_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略22: bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 22:
            if len(extra_header) < 4: return -1
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略23: dict_len(4) + dict_bytes + bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 23:
            if len(extra_header) < 4: return -1
            dict_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + dict_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略25: schema_len(4) + schema + bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 25:
            if len(extra_header) < 4: return -1
            schema_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + schema_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略26: template_len(4) + template + bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 26:
            if len(extra_header) < 4: return -1
            template_len = struct.unpack('>I', extra_header[:4])[0]
            offset = 4 + template_len
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略27: row_width(2) + bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 27:
            offset = 2
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略28: row_width(2) + bpe_rules_len(4) + bpe_rules + block_info
        if strategy == 28:
            offset = 2
            if offset + 4 > len(extra_header): return -1
            bpe_len = struct.unpack('>I', extra_header[offset:offset + 4])[0]
            offset += 4 + bpe_len
            remaining = extra_header[offset:]
            bi = self._try_skip_block_info(remaining)
            return offset + bi

        # 策略24: csv_meta_len(4) + csv_meta + bpe_rules_len(4) + bpe_rules + block_info
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

        return 0  # 未知策略, 尝试从开头读

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
        if needed > len(data): return 0
        # 验证后面是否有合理的滤镜数据
        rest = data[needed:]
        if len(rest) >= 8:
            return needed
        return 0

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
        if strategy == 0:
            return intermediate
        elif strategy == 1:
            return intermediate
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
        elif strategy in (10, 11):
            return intermediate
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

        # ★ v7新策略逆变换
        elif strategy == 14:
            # BPE + BWT + LZMA2 (或 BPE + BWT + RLE + LZMA2)
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            remaining = extra_header[4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode(data, bpe_rules)

        elif strategy == 15:
            # BPE + LZMA2
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            return bpe_decode(intermediate, bpe_rules)

        elif strategy == 16:
            # N-gram字典 + BWT + LZMA2
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            ngram_dict = extra_header[4:4 + ngram_len]
            remaining = extra_header[4 + ngram_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return ngram_dict_decode(data, ngram_dict)

        elif strategy == 17:
            # N-gram字典 + LZMA2
            ngram_len = struct.unpack('>I', extra_header[:4])[0]
            ngram_dict = extra_header[4:4 + ngram_len]
            return ngram_dict_decode(intermediate, ngram_dict)

        elif strategy == 18:
            # BPE + N-gram + BWT + LZMA2
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
            # CSV列压缩 + BWT + LZMA2
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            remaining = extra_header[4 + meta_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return csv_column_decode(data, meta_bytes)

        elif strategy == 20:
            # JSON扁平化 + BWT + LZMA2
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            remaining = extra_header[4 + meta_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return json_flatten_decode(data, meta_bytes)

        elif strategy == 21:
            # 日志字段压缩 + BWT + LZMA2
            meta_len = struct.unpack('>I', extra_header[:4])[0]
            meta_bytes = extra_header[4:4 + meta_len]
            remaining = extra_header[4 + meta_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return log_field_decode(data, meta_bytes)

        elif strategy == 22:
            # BPE + Delta + BWT + LZMA2
            bpe_len = struct.unpack('>I', extra_header[:4])[0]
            bpe_rules = extra_header[4:4 + bpe_len]
            remaining = extra_header[4 + bpe_len:]
            block_info = self._try_extract_block_info(remaining)
            data = intermediate
            if block_info:
                data = bwt_decode(data, block_info)
            return bpe_decode(data, bpe_rules)

        elif strategy == 23:
            # 文本字典 + BPE + BWT + LZMA2
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

        elif strategy == 25:
            # JSON键去重 + BPE + BWT + LZMA2
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
            # 日志模板 + BPE + BWT + LZMA2
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
            # 列转置 + BPE + BWT + LZMA2
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
            # 列转置 + BPE + Delta + BWT + LZMA2
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

        elif strategy == 24:
            # CSV列压缩 + BPE + BWT + LZMA2
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

        elif strategy == 13:
            # 记录级Delta + BWT + LZMA2 (或 + BPE + BWT)
            extra = extra_header
            offset = 0
            if extra[offset] == 0x13:
                offset += 1
                rec_size = struct.unpack('>H', extra[offset:offset + 2])[0]; offset += 2
                first_byte = extra[offset]; offset += 1
                # 检查是否有BPE规则
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

            # fallback
            block_infos, _ = self._extract_block_infos(extra, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(intermediate, block_infos[0])
            return intermediate

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
