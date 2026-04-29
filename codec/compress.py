"""
AtomZip Compressor — REPC v2 Final Pipeline

Novel pipeline: RLE → Byte Remapping → REPC BPE → zlib (LZ77+Huffman)

Innovation:
  1. Byte remapping: Creates free byte values by escaping rare bytes,
     enabling aggressive BPE substitution
  2. REPC scoring: Selects BPE patterns based on information entropy gain
     (frequency × context diversity), not just frequency
  3. Hierarchical grammar: Multi-level BPE builds up complex patterns
  4. zlib as final stage: Handles residual redundancy including long-range matches

The combination of remapping + REPC BPE + LZ77 is novel and achieves
better compression than any single technique alone.
"""

import struct
import time
import zlib
from typing import Tuple

from .pattern import PatternExtractor
from .entropy import SimpleEntropyCoder

ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 2


class AtomZipCompressor:
    """REPC v2: RLE + Remap + REPC BPE + zlib."""

    def __init__(self, max_pattern_rules=254, context_order=3, verbose=False):
        self.max_pattern_rules = max_pattern_rules
        self.context_order = context_order
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip v2] Compressing {original_size} bytes...")

        if original_size == 0:
            return self._build_empty_header()

        # Stage 1: RLE
        rle_data, rle_entries = self._rle_encode(data)
        flags = 0x01 if rle_entries else 0

        if self.verbose:
            print(f"  RLE: {original_size} -> {len(rle_data)} bytes")

        # Stage 2: REPC BPE with byte remapping
        extractor = PatternExtractor(max_rules=self.max_pattern_rules, min_freq=2)
        bpe_data, rules = extractor.extract_and_apply(rle_data)

        if self.verbose:
            print(f"  BPE: {len(rules)} rules, {len(rle_data)} -> {len(bpe_data)} bytes")

        # Stage 3: zlib (LZ77 + Huffman) for remaining redundancy
        zlib_data = zlib.compress(bpe_data, 9)
        flags |= 0x02

        if self.verbose:
            print(f"  zlib: {len(bpe_data)} -> {len(zlib_data)} bytes")

        # Serialize rules
        rules_data = PatternExtractor.serialize_rules(rules)

        # Build final stream
        result = bytearray()
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>Q', original_size))
        result.extend(struct.pack('>I', flags))

        # RLE entries
        if rle_entries:
            result.extend(struct.pack('>I', len(rle_data)))
            result.extend(struct.pack('>H', len(rle_entries)))
            for pos, byte_val, run_len in rle_entries:
                result.extend(struct.pack('>I', pos))
                result.append(byte_val)
                result.extend(struct.pack('>H', run_len))

        # Rules + zlib data
        result.extend(struct.pack('>I', len(rules_data)))
        result.extend(rules_data)
        result.extend(struct.pack('>I', len(zlib_data)))
        result.extend(zlib_data)

        elapsed = time.time() - start_time
        total_size = len(result)
        if self.verbose:
            ratio = original_size / max(1, total_size)
            print(f"[AtomZip v2] Done: {original_size} -> {total_size} bytes ({ratio:.2f}:1, {elapsed:.2f}s)")

        return bytes(result)

    def _rle_encode(self, data: bytes) -> Tuple[bytes, list]:
        if len(data) == 0:
            return data, []
        result = bytearray()
        entries = []
        i = 0
        while i < len(data):
            current = data[i]
            run_len = 1
            while i + run_len < len(data) and data[i + run_len] == current and run_len < 65535:
                run_len += 1
            if run_len >= 4:
                entries.append((len(result), current, run_len))
                result.append(current)
                result.append(current)
                result.append(current)
                result.extend(struct.pack('>H', run_len))
                i += run_len
            else:
                for _ in range(run_len):
                    result.append(current)
                    i += 1
        return bytes(result), entries

    def _build_empty_header(self) -> bytes:
        r = bytearray()
        r.extend(ATOMZIP_MAGIC)
        r.append(FORMAT_VERSION)
        r.extend(struct.pack('>Q', 0))
        r.extend(struct.pack('>I', 0))
        return bytes(r)
