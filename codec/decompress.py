"""
AtomZip Decompressor — REPC v2: zlib -> BPE -> RLE
"""

import struct
import time
import zlib

from .pattern import PatternExtractor
from .compress import ATOMZIP_MAGIC, FORMAT_VERSION


class AtomZipDecompressor:
    def __init__(self, verbose=False):
        self.verbose = verbose

    def decompress(self, data: bytes) -> bytes:
        start_time = time.time()
        offset = 0

        if len(data) < 17:
            raise ValueError("Invalid AtomZip data")

        magic = data[offset:offset + 4]
        offset += 4
        if magic != ATOMZIP_MAGIC:
            raise ValueError(f"Invalid magic: {magic}")

        version = data[offset]
        offset += 1
        original_size = struct.unpack('>Q', data[offset:offset + 8])[0]
        offset += 8
        flags = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4

        # RLE entries
        rle_entries = []
        if flags & 0x01:
            rle_encoded_size = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
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

        # Rules
        rules_data_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        rules_data = data[offset:offset + rules_data_len]
        offset += rules_data_len
        rules, _ = PatternExtractor.deserialize_rules(rules_data)

        # zlib data
        zlib_data_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        zlib_data = data[offset:offset + zlib_data_len]

        # Stage 3: zlib decompress
        bpe_data = zlib.decompress(zlib_data)

        # Stage 2: Reverse BPE
        expanded = PatternExtractor.apply_rules_reverse(bpe_data, rules)

        # Stage 1: Reverse RLE
        if rle_entries:
            original = self._rle_decode(expanded, rle_entries)
        else:
            original = expanded

        original = original[:original_size]

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"[AtomZip v2] Done: {len(data)} -> {len(original)} bytes ({elapsed:.2f}s)")

        return original

    def _rle_decode(self, data, rle_entries):
        if not rle_entries:
            return data
        sorted_entries = sorted(rle_entries, key=lambda x: x[0])
        result = bytearray()
        src_pos = 0
        for entry_pos, byte_val, run_len in sorted_entries:
            if src_pos < entry_pos:
                result.extend(data[src_pos:entry_pos])
            result.extend(bytes([byte_val]) * run_len)
            src_pos = entry_pos + 5
        if src_pos < len(data):
            result.extend(data[src_pos:])
        return bytes(result)
