"""
AtomZip 解压引擎 v6 — 多策略逆向流水线

支持策略:
  0: LZMA2 only
  1: LZMA2 Delta滤镜
  2: BWT + LZMA2
  3: BWT + RLE + LZMA2
  4: BWT + Delta滤镜 + LZMA2
  5: 文本字典 (+ 可选BWT) + LZMA2
  6: JSON键去重 (+ 可选BWT) + LZMA2
  7: 日志模板 (+ 可选BWT) + LZMA2
  8: 列转置 (+ BWT/Delta) + LZMA2
  9: BWT + RLE + Delta滤镜 + LZMA2
  10: BCJ + LZMA2
  11: BCJ + Delta + LZMA2
  12: 递归BWT (双层BWT)
"""

import struct
import time
import lzma

from .transform_v6 import (
    bwt_decode, delta_decode,
    rle_decode,
    text_dict_decode,
    json_key_dedup_decode,
    log_template_decode,
    column_transpose_decode,
    deserialize_block_info,
)
from .compress_v6 import ATOMZIP_MAGIC, FORMAT_VERSION, _get_lzma_filters


class AtomZipDecompressor:
    """AtomZip v6 解压器"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def decompress(self, data: bytes) -> bytes:
        """解压数据，返回原始字节流。"""
        start_time = time.time()
        offset = 0

        if len(data) < 12:
            raise ValueError("数据过短，不是有效的 AtomZip 文件")

        magic = data[offset:offset + 4]; offset += 4
        if magic != ATOMZIP_MAGIC:
            raise ValueError(f"无效的文件魔数: {magic!r}")

        version = data[offset]; offset += 1
        if version not in (4, 5, 6):
            raise ValueError(f"不支持的版本号: {version}")

        original_size = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        strategy = data[offset]; offset += 1
        extra_size = struct.unpack('>H', data[offset:offset + 2])[0]; offset += 2

        if original_size == 0:
            return b''

        extra_header = data[offset:offset + extra_size]; offset += extra_size
        lzma_data_len = struct.unpack('>I', data[offset:offset + 4])[0]; offset += 4
        lzma_data = data[offset:offset + lzma_data_len]

        # 提取滤镜参数并构建解压滤镜
        filters = self._build_filters(strategy, extra_header, original_size)

        # LZMA2 RAW 解压
        intermediate = lzma.decompress(lzma_data, format=lzma.FORMAT_RAW, filters=filters)

        # 根据策略逆变换
        result = self._reverse_strategy(strategy, intermediate, extra_header, original_size)
        result = result[:original_size]

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"[AtomZip v6] 解压完成: {len(data):,} -> {len(result):,} 字节 "
                  f"(耗时: {elapsed:.3f}秒, 策略: {strategy})")

        return result

    def _build_filters(self, strategy: int, extra_header: bytes,
                       original_size: int) -> list:
        """从额外头部构建解压滤镜。"""
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

    def _extract_filters_info(self, strategy: int, extra_header: bytes) -> dict:
        """从额外头部提取滤镜信息。"""
        info = {'lc': 3, 'lp': 0, 'pb': 2, 'dict_size': 0, 'delta_dist': 0, 'bcj': False}

        if not extra_header:
            return info

        try:
            # 滤镜信息在extra_header的末尾
            # 格式: [flags(1B)] [dict_size(4B)] [lc(1B)] [lp(1B)] [pb(1B)] [delta_dist(2B,可选)]
            # 不同策略在滤镜信息之前有不同数据

            # 策略0: 无前置数据
            # 策略1: 无前置数据
            # 策略2,3,4,9: block_info + 滤镜信息
            # 策略5: dict_bytes + block_info? + 滤镜信息
            # 策略6: schema_bytes + block_info? + 滤镜信息
            # 策略7: template_bytes + block_info? + 滤镜信息
            # 策略8: row_width(2) + block_info? + 滤镜信息
            # 策略10,11: 滤镜信息
            # 策略12: 2x block_info + 滤镜信息

            if strategy == 0:
                self._read_filters_from_end(extra_header, 0, info)

            elif strategy == 1:
                self._read_filters_from_end(extra_header, 0, info)

            elif strategy in (2, 3):
                # block_info + 滤镜信息
                self._read_filters_after_block_info(extra_header, 1, info)

            elif strategy == 4 or strategy == 9:
                # block_info + 滤镜信息(含delta)
                self._read_filters_after_block_info(extra_header, 1, info)

            elif strategy == 5:
                # dict_len(2) + dict_bytes + [block_info] + 滤镜信息
                dict_len = struct.unpack('>H', extra_header[:2])[0]
                offset = 2 + dict_len
                self._read_filters_from_offset(extra_header, offset, info)

            elif strategy == 6:
                schema_len = struct.unpack('>H', extra_header[:2])[0]
                offset = 2 + schema_len
                self._read_filters_from_offset(extra_header, offset, info)

            elif strategy == 7:
                template_len = struct.unpack('>I', extra_header[:4])[0]
                offset = 4 + template_len
                self._read_filters_from_offset(extra_header, offset, info)

            elif strategy == 8:
                # row_width(2) + [block_info] + 滤镜信息
                self._read_filters_from_offset(extra_header, 2, info)

            elif strategy in (10, 11):
                self._read_filters_from_end(extra_header, 0, info)

            elif strategy == 12:
                # 2x block_info + 滤镜信息
                self._read_filters_after_block_info(extra_header, 2, info)

        except Exception:
            pass

        if info['lc'] + info['lp'] > 4 or info['dict_size'] == 0:
            info.update({'lc': 3, 'lp': 0, 'pb': 2})
            info['dict_size'] = 0

        return info

    def _read_filters_from_end(self, extra_header: bytes, skip_prefix: int, info: dict):
        """从extra_header末尾读取滤镜信息。"""
        if len(extra_header) < skip_prefix + 8:
            return

        offset = skip_prefix
        flags = extra_header[offset]
        info['dict_size'] = struct.unpack('>I', extra_header[offset + 1:offset + 5])[0]
        info['lc'] = extra_header[offset + 5]
        info['lp'] = extra_header[offset + 6]
        info['pb'] = extra_header[offset + 7]

        if flags & 0x01 and len(extra_header) >= offset + 10:
            info['delta_dist'] = struct.unpack('>H', extra_header[offset + 8:offset + 10])[0]
        if flags & 0x02:
            info['bcj'] = True

    def _read_filters_after_block_info(self, extra_header: bytes, num_block_infos: int, info: dict):
        """跳过block_info后读取滤镜信息。"""
        offset = 0
        for _ in range(num_block_infos):
            if offset + 2 > len(extra_header):
                return
            num_blocks = struct.unpack('>H', extra_header[offset:offset + 2])[0]
            offset += 2 + num_blocks * 8

        self._read_filters_from_end(extra_header[offset:], 0, info)

    def _read_filters_from_offset(self, extra_header: bytes, start: int, info: dict):
        """从指定偏移量读取滤镜信息 (可能前面有block_info)。"""
        remaining = extra_header[start:]

        # 尝试先跳过block_info
        if len(remaining) >= 2:
            # 检查是否看起来像block_info
            num_blocks = struct.unpack('>H', remaining[:2])[0]
            if num_blocks < 1000 and 2 + num_blocks * 8 < len(remaining):
                # 可能是block_info
                after_bi = 2 + num_blocks * 8
                rest = remaining[after_bi:]
                if len(rest) >= 8:
                    self._read_filters_from_end(rest, 0, info)
                    if info['dict_size'] > 0:
                        return

        # 直接从offset读取
        if len(remaining) >= 8:
            self._read_filters_from_end(remaining, 0, info)

    def _reverse_strategy(self, strategy: int, intermediate: bytes,
                          extra_header: bytes, original_size: int) -> bytes:
        """根据策略逆向变换。"""
        if strategy == 0:
            return intermediate

        elif strategy == 1:
            return intermediate

        elif strategy == 2:
            # BWT + LZMA2
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(intermediate, block_infos[0])
            return intermediate

        elif strategy == 3:
            # BWT + RLE + LZMA2
            rle_decoded = rle_decode(intermediate)
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(rle_decoded, block_infos[0])
            return rle_decoded

        elif strategy == 4:
            # BWT + Delta滤镜 + LZMA2 (Delta已由LZMA2处理)
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(intermediate, block_infos[0])
            return intermediate

        elif strategy in (10, 11):
            return intermediate

        elif strategy == 5:
            # 文本字典 (+ 可选BWT)
            dict_len = struct.unpack('>H', extra_header[:2])[0]
            dict_bytes = extra_header[2:2 + dict_len]
            remaining = extra_header[2 + dict_len:]

            # 检查是否有BWT block_info
            block_info = self._try_extract_block_info(remaining)
            if block_info:
                bwt_decoded = bwt_decode(intermediate, block_info)
                return text_dict_decode(bwt_decoded, dict_bytes)

            return text_dict_decode(intermediate, dict_bytes)

        elif strategy == 6:
            schema_len = struct.unpack('>H', extra_header[:2])[0]
            schema_bytes = extra_header[2:2 + schema_len]
            remaining = extra_header[2 + schema_len:]

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
            # BWT + RLE + Delta (Delta已由LZMA2处理)
            rle_decoded = rle_decode(intermediate)
            block_infos, _ = self._extract_block_infos(extra_header, 1)
            if block_infos and block_infos[0]:
                return bwt_decode(rle_decoded, block_infos[0])
            return rle_decoded

        elif strategy == 12:
            # 递归BWT (双层)
            block_infos, _ = self._extract_block_infos(extra_header, 2)
            if len(block_infos) >= 2 and block_infos[1] and block_infos[0]:
                # 第二层BWT解码
                decoded2 = bwt_decode(intermediate, block_infos[1])
                # 第一层BWT解码
                decoded1 = bwt_decode(decoded2, block_infos[0])
                return decoded1
            return intermediate

        else:
            raise ValueError(f"未知的压缩策略: {strategy}")

    def _extract_block_infos(self, extra_header: bytes, count: int) -> tuple:
        """从extra_header开头提取指定数量的block_info。"""
        block_infos = []
        offset = 0
        for _ in range(count):
            if offset + 2 > len(extra_header):
                break
            num_blocks = struct.unpack('>H', extra_header[offset:offset + 2])[0]
            bi = []
            offset += 2
            for _ in range(num_blocks):
                if offset + 8 > len(extra_header):
                    break
                orig_idx = struct.unpack('>I', extra_header[offset:offset + 4])[0]
                offset += 4
                block_size = struct.unpack('>I', extra_header[offset:offset + 4])[0]
                offset += 4
                bi.append((orig_idx, block_size))
            block_infos.append(bi)
        return block_infos, offset

    def _try_extract_block_info(self, remaining: bytes):
        """尝试从剩余字节中提取block_info。"""
        if len(remaining) < 10:  # 至少需要block_info(2+8) + filters(8)
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
                orig_idx = struct.unpack('>I', remaining[offset:offset + 4])[0]
                offset += 4
                block_size = struct.unpack('>I', remaining[offset:offset + 4])[0]
                offset += 4
                bi.append((orig_idx, block_size))

            return bi
        except Exception:
            return None


def _smart_dict_size_default(original_size: int) -> int:
    """默认字典大小。"""
    return max(1 << 16, min(original_size, 1 << 30))
