"""
AtomZip Decompressor — Main Decompression Pipeline

Reverses the compression pipeline:
  1. Parse compressed stream header and metadata
  2. Reverse entropy coding (Huffman decode)
  3. Reverse pattern substitution (expand rules in reverse order)
  4. Reverse RLE preprocessing
"""

import struct
import time
from typing import Optional

from .pattern import PatternExtractor
from .entropy import SimpleEntropyCoder
from .compress import ATOMZIP_MAGIC, FORMAT_VERSION


class AtomZipDecompressor:
    """Main decompressor reversing all pipeline stages."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def decompress(self, data: bytes) -> bytes:
        """Decompress AtomZip compressed data."""
        start_time = time.time()
        offset = 0

        # Parse header
        if len(data) < 17:
            raise ValueError("Invalid AtomZip data: too short")

        magic = data[offset:offset + 4]
        offset += 4
        if magic != ATOMZIP_MAGIC:
            raise ValueError(f"Invalid magic: expected {ATOMZIP_MAGIC}, got {magic}")

        version = data[offset]
        offset += 1
        if version != FORMAT_VERSION:
            raise ValueError(f"Unsupported version: {version}")

        original_size = struct.unpack('>Q', data[offset:offset + 8])[0]
        offset += 8

        flags = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4

        if self.verbose:
            print(f"[AtomZip] Decompressing to {original_size} bytes...")

        # Parse RLE entries
        rle_entries = []
        rle_encoded_size = 0
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

        # Parse rules
        rules_data_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        rules_data = data[offset:offset + rules_data_len]
        offset += rules_data_len
        rules, _ = PatternExtractor.deserialize_rules(rules_data)

        # Parse coded data
        coded_data_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        coded_data = data[offset:offset + coded_data_len]
        offset += coded_data_len

        # Stage 3: Reverse entropy coding
        if self.verbose:
            print("[AtomZip] Stage 3: Reversing Huffman coding...")

        coder = SimpleEntropyCoder()
        transformed_data = coder.decode(coded_data)

        if self.verbose:
            print(f"  Huffman decoded: {len(coded_data)} -> {len(transformed_data)} bytes")

        # Stage 2: Reverse pattern substitution
        if self.verbose:
            print("[AtomZip] Stage 2: Reversing pattern substitution...")

        rle_result = PatternExtractor.apply_rules_reverse(transformed_data, rules)

        if self.verbose:
            print(f"  Pattern expand: {len(transformed_data)} -> {len(rle_result)} bytes "
                  f"({len(rules)} rules)")

        # Stage 1: Reverse RLE
        if self.verbose:
            print("[AtomZip] Stage 1: Reversing RLE...")

        if rle_entries:
            original = self._rle_decode(rle_result, rle_entries)
        else:
            original = rle_result

        # Ensure correct size
        original = original[:original_size]

        elapsed = time.time() - start_time
        if self.verbose:
            print(f"[AtomZip] Decompression complete: {len(data)} -> {len(original)} bytes")
            print(f"  Time: {elapsed:.3f}s")

        return original

    def _rle_decode(self, data: bytes, rle_entries: list) -> bytes:
        """
        Reverse RLE encoding.

        Each rle_entry: (pos_in_encoded, byte_value, original_run_length)
        The encoded form is: byte byte byte count_hi count_lo (5 bytes)
        The original form is: byte repeated original_run_length times
        """
        if not rle_entries:
            return data

        # Sort entries by position
        sorted_entries = sorted(rle_entries, key=lambda x: x[0])

        result = bytearray()
        src_pos = 0

        for entry_pos, byte_val, run_len in sorted_entries:
            # Copy data before this RLE entry
            if src_pos < entry_pos:
                result.extend(data[src_pos:entry_pos])

            # Expand the run
            result.extend(bytes([byte_val]) * run_len)

            # Skip the 5-byte encoded run in source
            src_pos = entry_pos + 5  # 3 bytes + 2 bytes count

        # Copy remaining data
        if src_pos < len(data):
            result.extend(data[src_pos:])

        return bytes(result)
