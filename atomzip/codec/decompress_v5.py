"""
AtomZip 解压引擎 v5.1 — 多策略逆向流水线

策略:
  0: LZMA2 only
  1: LZMA2 Delta滤镜
  2: LZMA2 → BWT
  3: LZMA2 → RLE → BWT
  4: LZMA2 BCJ (+ Delta)
  5: LZMA2 → 文本字典 (或 BWT→文本字典)
  6: LZMA2 → JSON键去重 (或 BWT→JSON键去重)
  7: LZMA2 → 日志模板 (或 BWT→日志模板)
  8: LZMA2 → BWT → 列转置 (或 Delta→列转置)
"""

import struct
import time
import lzma

from .transform_v5 import (
    bwt_decode, delta_decode,
    rle_decode,
    text_dict_decode,
    json_key_dedup_decode,
    log_template_decode,
    column_transpose_decode,
    deserialize_block_info,
)
from .compress_v5 import ATOMZIP_MAGIC, FORMAT_VERSION, _get_lzma_filters


class AtomZipDecompressor:
    """AtomZip v5.1 解压器"""

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
        if version not in (4, 5):
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
            print(f"[AtomZip v5.1] 解压完成: {len(data):,} -> {len(result):,} 字节 "
                  f"(耗时: {elapsed:.3f}秒, 策略: {strategy})")

        return result

    def _build_filters(self, strategy: int, extra_header: bytes,
                       original_size: int) -> list:
        """从额外头部构建解压滤镜。"""
        # 从 extra_header 提取滤镜信息
        # 格式: [flags(1B)] [dict_size(4B)] [lc(1B)] [lp(1B)] [pb(1B)] [delta_dist(2B,可选)]
        # 可能在前面还有其他策略数据, 滤镜信息在特定位置
        
        # 尝试从 extra_header 末尾提取滤镜信息
        filters_info = self._extract_filters_info(strategy, extra_header)
        
        filters = []
        
        # BCJ 滤镜
        if filters_info.get('bcj', False):
            filters.append({'id': lzma.FILTER_X86})
        
        # Delta 滤镜
        delta_dist = filters_info.get('delta_dist', 0)
        if delta_dist > 0:
            filters.append({'id': lzma.FILTER_DELTA, 'dist': delta_dist})
        
        # LZMA2 主滤镜
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
        
        # 根据策略解析 extra_header
        try:
            if strategy == 0:
                # [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)]
                if len(extra_header) >= 8:
                    info['dict_size'] = struct.unpack('>I', extra_header[1:5])[0]
                    info['lc'] = extra_header[5]
                    info['lp'] = extra_header[6]
                    info['pb'] = extra_header[7]
                    
            elif strategy == 1:
                # [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)] [delta_dist(2)]
                if len(extra_header) >= 10:
                    info['dict_size'] = struct.unpack('>I', extra_header[1:5])[0]
                    info['lc'] = extra_header[5]
                    info['lp'] = extra_header[6]
                    info['pb'] = extra_header[7]
                    info['delta_dist'] = struct.unpack('>H', extra_header[8:10])[0]
                    
            elif strategy == 4:
                # [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)] [delta_dist(2),可选]
                if len(extra_header) >= 8:
                    flags = extra_header[0]
                    info['bcj'] = bool(flags & 0x02)
                    info['dict_size'] = struct.unpack('>I', extra_header[1:5])[0]
                    info['lc'] = extra_header[5]
                    info['lp'] = extra_header[6]
                    info['pb'] = extra_header[7]
                    if flags & 0x01 and len(extra_header) >= 10:
                        info['delta_dist'] = struct.unpack('>H', extra_header[8:10])[0]
                    
            elif strategy in (2, 3):
                # [block_info...] [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)]
                # 滤镜信息在末尾 8 字节
                if len(extra_header) >= 8:
                    offset = len(extra_header) - 8
                    info['dict_size'] = struct.unpack('>I', extra_header[offset+1:offset+5])[0]
                    info['lc'] = extra_header[offset+5]
                    info['lp'] = extra_header[offset+6]
                    info['pb'] = extra_header[offset+7]
                    
            elif strategy == 5:
                # [dict_size(2)] [dict_bytes...] [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)]
                # 先读取字典大小
                dict_len = struct.unpack('>H', extra_header[:2])[0]
                # 滤镜信息在 dict_bytes 之后
                filter_start = 2 + dict_len
                if len(extra_header) >= filter_start + 8:
                    info['dict_size'] = struct.unpack('>I', extra_header[filter_start+1:filter_start+5])[0]
                    info['lc'] = extra_header[filter_start+5]
                    info['lp'] = extra_header[filter_start+6]
                    info['pb'] = extra_header[filter_start+7]
                    
            elif strategy == 6:
                # [schema_size(2)] [schema_bytes...] [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)]
                schema_len = struct.unpack('>H', extra_header[:2])[0]
                filter_start = 2 + schema_len
                if len(extra_header) >= filter_start + 8:
                    info['dict_size'] = struct.unpack('>I', extra_header[filter_start+1:filter_start+5])[0]
                    info['lc'] = extra_header[filter_start+5]
                    info['lp'] = extra_header[filter_start+6]
                    info['pb'] = extra_header[filter_start+7]
                    
            elif strategy == 7:
                # [template_size(4)] [template_bytes...] [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)]
                template_len = struct.unpack('>I', extra_header[:4])[0]
                filter_start = 4 + template_len
                if len(extra_header) >= filter_start + 8:
                    info['dict_size'] = struct.unpack('>I', extra_header[filter_start+1:filter_start+5])[0]
                    info['lc'] = extra_header[filter_start+5]
                    info['lp'] = extra_header[filter_start+6]
                    info['pb'] = extra_header[filter_start+7]
                    
            elif strategy == 8:
                # [row_width(2)] [... possible block_info ...] [flags(1)] [dict_size(4)] [lc(1)] [lp(1)] [pb(1)] [delta_dist(2),可选]
                # 需要判断是否有 delta
                if len(extra_header) >= 10:
                    # 从末尾读取
                    flags = extra_header[-10] if len(extra_header) >= 11 else 0
                    offset = len(extra_header) - 10
                    if offset >= 0:
                        info['dict_size'] = struct.unpack('>I', extra_header[offset+1:offset+5])[0]
                        info['lc'] = extra_header[offset+5]
                        info['lp'] = extra_header[offset+6]
                        info['pb'] = extra_header[offset+7]
                        if flags & 0x01:
                            info['delta_dist'] = struct.unpack('>H', extra_header[offset+8:offset+10])[0]
                    else:
                        # 简单回退
                        if len(extra_header) >= 8:
                            info['dict_size'] = struct.unpack('>I', extra_header[-7:-3])[0]
                            info['lc'] = extra_header[-3]
                            info['lp'] = extra_header[-2]
                            info['pb'] = extra_header[-1]
        except Exception:
            pass
        
        # 验证参数
        if info['lc'] + info['lp'] > 4 or info['dict_size'] == 0:
            info.update({'lc': 3, 'lp': 0, 'pb': 2})
        
        return info

    def _reverse_strategy(self, strategy: int, intermediate: bytes,
                          extra_header: bytes, original_size: int) -> bytes:
        """根据策略逆向变换。"""
        if strategy == 0:
            # LZMA2 only (可能带Delta/BCJ滤镜, 但已由LZMA2解压处理)
            return intermediate

        elif strategy == 1:
            # Delta滤镜 (已由LZMA2解压处理)
            return intermediate

        elif strategy == 2:
            # BWT
            block_info, _ = deserialize_block_info(extra_header[:len(extra_header) - 8])
            return bwt_decode(intermediate, block_info)

        elif strategy == 3:
            # BWT + RLE
            rle_decoded = rle_decode(intermediate)
            block_info, _ = deserialize_block_info(extra_header[:len(extra_header) - 8])
            return bwt_decode(rle_decoded, block_info)

        elif strategy == 4:
            # BCJ (+ Delta) 已由LZMA2解压处理
            return intermediate

        elif strategy == 5:
            # 文本字典 (可能有BWT)
            dict_len = struct.unpack('>H', extra_header[:2])[0]
            dict_bytes = extra_header[2:2 + dict_len]
            remaining = extra_header[2 + dict_len:]
            
            # 检查是否有 BWT block_info (在滤镜信息之前)
            filter_info_size = 8  # flags + dict_size + lc + lp + pb
            bwt_region = remaining[:len(remaining) - filter_info_size]
            
            if bwt_region:
                block_info, _ = deserialize_block_info(bwt_region)
                if block_info:
                    bwt_decoded = bwt_decode(intermediate, block_info)
                    return text_dict_decode(bwt_decoded, dict_bytes)
            
            return text_dict_decode(intermediate, dict_bytes)

        elif strategy == 6:
            # JSON键去重 (可能有BWT)
            schema_len = struct.unpack('>H', extra_header[:2])[0]
            schema_bytes = extra_header[2:2 + schema_len]
            remaining = extra_header[2 + schema_len:]
            
            filter_info_size = 8
            bwt_region = remaining[:len(remaining) - filter_info_size]
            
            if bwt_region:
                block_info, _ = deserialize_block_info(bwt_region)
                if block_info:
                    bwt_decoded = bwt_decode(intermediate, block_info)
                    return json_key_dedup_decode(bwt_decoded, schema_bytes)
            
            return json_key_dedup_decode(intermediate, schema_bytes)

        elif strategy == 7:
            # 日志模板 (可能有BWT)
            template_len = struct.unpack('>I', extra_header[:4])[0]
            template_bytes = extra_header[4:4 + template_len]
            remaining = extra_header[4 + template_len:]
            
            filter_info_size = 8
            bwt_region = remaining[:len(remaining) - filter_info_size]
            
            if bwt_region:
                block_info, _ = deserialize_block_info(bwt_region)
                if block_info:
                    bwt_decoded = bwt_decode(intermediate, block_info)
                    return log_template_decode(bwt_decoded, template_bytes)
            
            return log_template_decode(intermediate, template_bytes)

        elif strategy == 8:
            # 列转置 (可能有BWT/Delta)
            row_width = struct.unpack('>H', extra_header[:2])[0]
            
            # 检查是否有 BWT block_info
            remaining = extra_header[2:]
            filter_info_size = 8
            bwt_region = remaining[:len(remaining) - filter_info_size]
            
            if bwt_region:
                block_info, _ = deserialize_block_info(bwt_region)
                if block_info:
                    bwt_decoded = bwt_decode(intermediate, block_info)
                    return column_transpose_decode(bwt_decoded, row_width)
            
            # 可能是 Delta滤镜 + 列转置
            return column_transpose_decode(intermediate, row_width)

        else:
            raise ValueError(f"未知的压缩策略: {strategy}")


def _smart_dict_size_default(original_size: int) -> int:
    """默认字典大小。"""
    return max(1 << 16, min(original_size, 1 << 28))
