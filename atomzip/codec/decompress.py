"""
AtomZip 解压引擎 — DRAC v4 逆向流水线

根据压缩策略逆向执行:
  策略0: LZMA2 RAW 解压
  策略1: LZMA2 RAW 解压 → Delta 解码
  策略2: LZMA2 RAW 解压 → BWT 解码
"""

import struct
import time
import lzma

from .transform import (
    bwt_decode, delta_decode,
    deserialize_block_info,
)
from .compress import ATOMZIP_MAGIC, FORMAT_VERSION, _get_lzma_filters


class AtomZipDecompressor:
    """DRAC v4 解压器"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def decompress(self, data: bytes) -> bytes:
        """解压数据，返回原始字节流。"""
        start_time = time.time()
        offset = 0

        # === 解析文件头 ===
        if len(data) < 12:
            raise ValueError("数据过短，不是有效的 AtomZip 文件")

        # 魔数验证
        magic = data[offset:offset + 4]
        offset += 4
        if magic != ATOMZIP_MAGIC:
            raise ValueError(f"无效的文件魔数: {magic!r} (期望: {ATOMZIP_MAGIC!r})")

        # 版本号
        version = data[offset]
        offset += 1
        if version != FORMAT_VERSION:
            raise ValueError(f"不支持的版本号: {version} (当前支持: {FORMAT_VERSION})")

        # 原始大小
        original_size = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4

        # 压缩策略
        strategy = data[offset]
        offset += 1

        # 额外头部大小
        extra_header_size = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2

        if original_size == 0:
            if self.verbose:
                print("[AtomZip v4] 空文件，无需解压")
            return b''

        # 读取额外头部 (策略元数据)
        extra_header = data[offset:offset + extra_header_size]
        offset += extra_header_size

        # === 解析 LZMA2 RAW 数据 ===
        lzma_data_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        lzma_data = data[offset:offset + lzma_data_len]

        # === 执行解压 ===
        # 阶段1: LZMA2 RAW 解压
        filters = _get_lzma_filters(preset=9)
        intermediate = lzma.decompress(lzma_data, format=lzma.FORMAT_RAW,
                                       filters=filters)

        # 阶段2: 根据策略逆变换
        if strategy == 0:
            # 策略0: 无额外变换
            result = intermediate
        elif strategy == 1:
            # 策略1: Delta 解码
            first_byte = extra_header[0]
            result = delta_decode(intermediate, first_byte)
        elif strategy == 2:
            # 策略2: BWT 解码 (不需要 MTF 逆变换)
            block_info, _ = deserialize_block_info(extra_header)
            result = bwt_decode(intermediate, block_info)
        else:
            raise ValueError(f"未知的压缩策略: {strategy}")

        # 截取到原始大小
        result = result[:original_size]

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"[AtomZip v4] 解压完成: {len(data):,} -> {len(result):,} 字节 "
                  f"(耗时: {elapsed:.3f}秒, 策略: {strategy})")

        return result
