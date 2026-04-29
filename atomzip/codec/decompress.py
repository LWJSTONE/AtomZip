"""
AtomZip 解压引擎 — DRAC v3 逆向流水线

根据压缩策略逆向执行:
  策略0: LZMA2 RAW 解压
  策略1: LZMA2 RAW 解压 → RLE 解码
  策略2: LZMA2 RAW 解压 → BPE 逆替换 → RLE 解码
"""

import struct
import time
import lzma

from .pattern import PatternExtractor
from .compress import ATOMZIP_MAGIC, FORMAT_VERSION, _get_lzma_filters


class AtomZipDecompressor:
    """DRAC v3 解压器"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def decompress(self, data: bytes) -> bytes:
        """解压数据，返回原始字节流。"""
        start_time = time.time()
        offset = 0

        # === 解析文件头 ===
        if len(data) < 16:
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

        # 标志位
        flags = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2

        if original_size == 0:
            if self.verbose:
                print("[AtomZip v3] 空文件，无需解压")
            return b''

        # === 解析 RLE 条目 ===
        rle_entries = []
        if flags & 0x01:
            num_entries = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            for _ in range(num_entries):
                pos = struct.unpack('>I', data[offset:offset + 4])[0]
                offset += 4
                byte_val = data[offset]
                offset += 1
                run_len = struct.unpack('>H', data[offset:offset + 2])[0]
                offset += 2
                rle_entries.append((pos, byte_val, run_len))

        # === 解析 BPE 规则 ===
        rules_data_len = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2
        rules = []
        if rules_data_len > 0:
            rules_data = data[offset:offset + rules_data_len]
            offset += rules_data_len
            rules, _ = PatternExtractor.deserialize_rules(rules_data)

        # === 解析 LZMA2 RAW 数据 ===
        lzma_data_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        lzma_data = data[offset:offset + lzma_data_len]

        # === 执行解压 ===
        # 阶段1: LZMA2 RAW 解压
        filters = _get_lzma_filters(preset=9)
        intermediate = lzma.decompress(lzma_data, format=lzma.FORMAT_RAW,
                                        filters=filters)

        # 阶段2: BPE 逆替换
        if rules and (flags & 0x02):
            intermediate = PatternExtractor.apply_rules_reverse(intermediate, rules)

        # 阶段3: RLE 解码
        if rle_entries and (flags & 0x01):
            intermediate = self._rle_decode(intermediate, rle_entries)

        # 截取到原始大小
        result = intermediate[:original_size]

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"[AtomZip v3] 解压完成: {len(data):,} -> {len(result):,} 字节 "
                  f"(耗时: {elapsed:.3f}秒)")

        return result

    def _rle_decode(self, data: bytes, rle_entries: list) -> bytes:
        """RLE 解码: 将编码的游程恢复为原始数据。"""
        if not rle_entries:
            return data

        sorted_entries = sorted(rle_entries, key=lambda x: x[0])
        result = bytearray()
        src_pos = 0

        for entry_pos, byte_val, run_len in sorted_entries:
            # 复制游程位置之前的普通数据
            if src_pos < entry_pos:
                result.extend(data[src_pos:entry_pos])
            # 展开游程
            result.extend(bytes([byte_val]) * run_len)
            # 跳过编码表示(3个字节 + 2字节长度 = 5字节)
            src_pos = entry_pos + 5

        # 复制剩余数据
        if src_pos < len(data):
            result.extend(data[src_pos:])

        return bytes(result)
