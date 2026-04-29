"""
AtomZip Compressor — Main Compression Pipeline

Orchestrates the full compression pipeline:
  1. Preprocessing: Run-length encoding for simple repeats
  2. Pattern Substitution: REPC algorithm (entropy-gain based pattern collapse)
  3. Entropy Coding: Huffman coding with frequency table
"""

import struct
import time
from typing import Optional, Tuple

from .pattern import PatternExtractor
from .entropy import SimpleEntropyCoder


# Magic bytes for AtomZip format
ATOMZIP_MAGIC = b'AZIP'
FORMAT_VERSION = 1


class AtomZipCompressor:
    """Main compressor combining all pipeline stages."""

    def __init__(self, max_pattern_rules: int = 200, context_order: int = 3,
                 verbose: bool = False):
        self.max_pattern_rules = max_pattern_rules
        self.context_order = context_order
        self.verbose = verbose

    def compress(self, data: bytes) -> bytes:
        """
        Compress data using the AtomZip REPC algorithm.

        Stream format:
          [4B: magic "AZIP"]
          [1B: version]
          [8B: original size]
          [4B: flags]
          [4B: RLE-encoded data size] (if RLE flag set)
          [N bytes: RLE entries] (if RLE flag set)
          [4B: rules data length]
          [M bytes: serialized rules]
          [4B: coded data length]
          [K bytes: Huffman-coded data]
        """
        start_time = time.time()
        original_size = len(data)

        if self.verbose:
            print(f"[AtomZip] Compressing {original_size} bytes...")

        if original_size == 0:
            return self._build_empty_header()

        # Stage 1: Preprocessing — Run-Length Encoding
        if self.verbose:
            print("[AtomZip] Stage 1: RLE preprocessing...")

        rle_data, rle_entries = self._rle_encode(data)
        flags = 0
        if rle_entries:
            flags |= 0x01

        if self.verbose:
            print(f"  RLE: {original_size} -> {len(rle_data)} bytes")

        # Stage 2: Pattern Substitution (REPC)
        if self.verbose:
            print("[AtomZip] Stage 2: Pattern substitution (REPC)...")

        extractor = PatternExtractor(
            max_rules=self.max_pattern_rules,
            min_freq=2
        )
        transformed_data, rules = extractor.extract_and_apply(rle_data)

        if self.verbose:
            print(f"  Extracted {len(rules)} rules")
            print(f"  Pattern sub: {len(rle_data)} -> {len(transformed_data)} bytes "
                  f"({100 * len(transformed_data) / max(1, len(rle_data)):.1f}%)")

        # Stage 3: Entropy Coding
        if self.verbose:
            print("[AtomZip] Stage 3: Entropy coding (Huffman)...")

        coder = SimpleEntropyCoder()
        coded_data = coder.encode(transformed_data)

        if self.verbose:
            print(f"  Huffman coded: {len(transformed_data)} -> {len(coded_data)} bytes "
                  f"({100 * len(coded_data) / max(1, len(transformed_data)):.1f}%)")

        # Serialize rules
        rules_data = PatternExtractor.serialize_rules(rules)

        # Build final stream
        result = bytearray()
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>Q', original_size))
        result.extend(struct.pack('>I', flags))

        # RLE entries (if any)
        if rle_entries:
            result.extend(struct.pack('>I', len(rle_data)))
            result.extend(struct.pack('>H', len(rle_entries)))
            for pos, byte_val, run_len in rle_entries:
                result.extend(struct.pack('>I', pos))
                result.append(byte_val)
                result.extend(struct.pack('>H', run_len))

        # Rules
        result.extend(struct.pack('>I', len(rules_data)))
        result.extend(rules_data)

        # Coded data
        result.extend(struct.pack('>I', len(coded_data)))
        result.extend(coded_data)

        elapsed = time.time() - start_time
        total_size = len(result)
        ratio = original_size / max(1, total_size)

        if self.verbose:
            print(f"[AtomZip] Complete: {original_size} -> {total_size} bytes")
            print(f"  Ratio: {ratio:.2f}:1 | Savings: {100*(1-total_size/original_size):.1f}%")
            print(f"  Time: {elapsed:.3f}s")

        return bytes(result)

    def _rle_encode(self, data: bytes) -> Tuple[bytes, list]:
        """
        Run-length encoding for consecutive repeated bytes (runs of 4+).
        Returns (encoded_data, rle_entries).
        Each entry: (position_in_encoded, byte_value, original_run_length)
        """
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
                # Record the position in the *output* where this run starts
                entries.append((len(result), current, run_len))
                # Encode as: byte byte byte count_hi count_lo (5 bytes for long runs)
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
        result = bytearray()
        result.extend(ATOMZIP_MAGIC)
        result.append(FORMAT_VERSION)
        result.extend(struct.pack('>Q', 0))
        result.extend(struct.pack('>I', 0))
        return bytes(result)
