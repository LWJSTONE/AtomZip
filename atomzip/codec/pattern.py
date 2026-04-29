"""
Pattern Extraction Module — REPC v2 with Byte Remapping

Key innovation: Before BPE, remap bytes so that rare bytes are escaped,
creating many free byte values for BPE substitution rules.

Remapping strategy:
  - Count byte frequencies
  - The 128 most-frequent bytes stay as single-byte symbols (0x00-0x7F)
  - The remaining 128 bytes are encoded as: 0x80 + original_byte (2 bytes)
  - Now byte values 0x81-0xFF are FREE (254 values for BPE rules!)

This is much more efficient than iterative byte freeing because:
  - The remapping is applied once, consistently
  - We get 254 free bytes (vs 0-5 with iterative freeing)
  - Decompression is straightforward (reverse the mapping)
"""

import struct
from collections import defaultdict
from typing import List, Tuple


class PatternExtractor:
    """BPE with REPC scoring + byte remapping for unlimited free bytes."""

    def __init__(self, max_rules: int = 254, min_freq: int = 2):
        self.max_rules = min(max_rules, 254)  # Max 254 free bytes
        self.min_freq = min_freq

    def extract_and_apply(self, data: bytes) -> Tuple[bytes, List[Tuple[int, bytes]]]:
        """
        Remap bytes + hierarchical BPE with REPC scoring.
        """
        if len(data) < 4:
            return data, []

        # Step 1: Byte remapping — create free bytes
        remapped, remap_rules = self._remap_bytes(data)

        # Step 2: BPE with REPC scoring on the remapped data
        data_arr = bytearray(remapped)
        rules: List[Tuple[int, bytes]] = list(remap_rules)

        # Find free bytes (should be many after remapping)
        used = set(data_arr)
        free_bytes = sorted([b for b in range(256) if b not in used])

        iteration = 0
        while free_bytes and iteration < self.max_rules:
            # Count pairs
            pair_freq = defaultdict(int)
            pair_left_ctx = defaultdict(set)
            pair_right_ctx = defaultdict(set)

            for i in range(len(data_arr) - 1):
                pair = (data_arr[i], data_arr[i + 1])
                pair_freq[pair] += 1
                if i > 0:
                    pair_left_ctx[pair].add(data_arr[i - 1])
                else:
                    pair_left_ctx[pair].add(None)
                if i + 2 < len(data_arr):
                    pair_right_ctx[pair].add(data_arr[i + 2])
                else:
                    pair_right_ctx[pair].add(None)

            # Find best pair
            best_score = 0
            best_pair = None

            for pair, freq in pair_freq.items():
                if freq < self.min_freq:
                    continue
                left_div = len(pair_left_ctx[pair])
                right_div = len(pair_right_ctx[pair])
                diversity = (left_div * right_div) / max(1, freq)
                score = freq * (1.0 + min(diversity, 3.0))

                if score > best_score:
                    best_score = score
                    best_pair = pair

            if best_pair is None or best_score < 2:
                break

            # Replace pair with free byte
            replacement = free_bytes.pop(0)
            pattern = bytes(best_pair)

            new_data = bytearray()
            i = 0
            while i < len(data_arr):
                if i <= len(data_arr) - 2 and data_arr[i] == best_pair[0] and data_arr[i + 1] == best_pair[1]:
                    new_data.append(replacement)
                    i += 2
                else:
                    new_data.append(data_arr[i])
                    i += 1

            data_arr = new_data
            rules.append((replacement, pattern))
            iteration += 1

            # Check for new free bytes
            current_used = set(data_arr)
            new_free = [b for b in range(256) if b not in current_used and b not in free_bytes]
            free_bytes.extend(sorted(new_free))

        return bytes(data_arr), rules

    def _remap_bytes(self, data: bytes) -> Tuple[bytes, List[Tuple[int, bytes]]]:
        """
        Remap bytes to create free byte values.

        Strategy: Find the escape byte (least frequent), then remap so that
        only the most common bytes remain as single bytes. Less common bytes
        are encoded as escape + original_byte.

        The escape byte + 0x00 means "literal escape byte"
        The escape byte + N means "original byte N"
        After remapping, all byte values except the escape byte and the
        common bytes are free.
        """
        if len(data) < 10:
            return data, []

        freq = [0] * 256
        for b in data:
            freq[b] += 1

        # Find the least frequent byte to use as escape
        sorted_by_freq = sorted(range(256), key=lambda b: freq[b])
        escape_byte = sorted_by_freq[0]

        # Find which bytes are "common" (appear at least threshold times)
        # We want to maximize free bytes while minimizing expansion
        # Strategy: only keep bytes that appear more than 2 times as single bytes
        # All others get escaped

        # Actually, let's be smarter: calculate the optimal split
        # Cost of keeping byte B as single: 0 extra bytes
        # Cost of escaping byte B: freq[B] extra bytes (each occurrence takes 2 bytes instead of 1)
        # Benefit of freeing byte B: 1 free byte for BPE, which can save ~N bytes
        # where N is the frequency of the best pair

        # Simple heuristic: escape bytes that appear <= threshold times
        # The threshold depends on how much BPE can save per rule
        # A BPE rule saves (freq - 1) bytes in the data, costing 0 extra (single-byte symbols)
        # So a byte worth escaping is one where freq[byte] < expected_savings_per_rule

        # For most data, escaping bytes with freq < 10 is worthwhile
        # because each BPE rule typically saves > 10 bytes

        # Count how many byte values are used
        used_bytes = [b for b in range(256) if freq[b] > 0]
        num_used = len(used_bytes)

        # If we have enough free bytes already, skip remapping
        free_count = 256 - num_used
        if free_count >= 50:
            return data, []

        # Calculate optimal threshold
        # We want at least 50 free bytes, so we need to escape (num_used + free_count - 50) bytes
        # But we should only escape if the cost is worth it
        target_free = max(100, num_used // 2)  # Target: at least 100 free bytes

        # Sort by frequency (ascending) and escape the least frequent ones
        bytes_by_freq = sorted(used_bytes, key=lambda b: freq[b])

        # Determine how many bytes to escape
        bytes_to_escape = set()
        expansion_cost = 0

        for b in bytes_by_freq:
            if 256 - (num_used - len(bytes_to_escape)) >= target_free:
                break
            if b == escape_byte:
                continue  # Don't escape the escape byte itself
            bytes_to_escape.add(b)
            expansion_cost += freq[b]

        # Only remap if the expansion is reasonable (< 20% of data)
        if expansion_cost > len(data) * 0.2 or len(bytes_to_escape) < 10:
            return data, []

        # Perform remapping
        new_data = bytearray()
        for b in data:
            if b in bytes_to_escape:
                new_data.append(escape_byte)
                new_data.append(b)
            elif b == escape_byte:
                # Escape the escape byte itself
                new_data.append(escape_byte)
                new_appended = new_data[-1]
                # Use a special marker for the escape byte itself
                new_data.append(escape_byte)  # escape + escape = literal escape byte
            else:
                new_data.append(b)

        # Create remap rules for decompression
        # These tell the decompressor how to reverse the remapping
        remap_rules = []

        # Rule for the escape byte: escape+escape -> escape_byte
        remap_rules.append((0x100 + escape_byte, bytes([escape_byte, escape_byte])))

        # Rule for each escaped byte: escape+byte -> original_byte
        for b in bytes_to_escape:
            remap_rules.append((0x100 + b, bytes([escape_byte, b])))

        return bytes(new_data), remap_rules

    @staticmethod
    def serialize_rules(rules: List[Tuple[int, bytes]]) -> bytes:
        """Serialize rules with type flags."""
        result = bytearray()
        result.extend(struct.pack('>H', len(rules)))
        for replacement_byte, pattern in rules:
            if replacement_byte >= 0x100:
                result.append(0x01)  # Escape/remap rule
                result.append(replacement_byte & 0xFF)
            else:
                result.append(0x00)  # Normal BPE rule
                result.append(replacement_byte)
            pat_len = len(pattern)
            result.extend(struct.pack('>H', pat_len))
            result.extend(pattern)
        return bytes(result)

    @staticmethod
    def deserialize_rules(data: bytes) -> Tuple[List[Tuple[int, bytes]], int]:
        """Deserialize rules."""
        offset = 0
        if len(data) < 2:
            return [], 0
        num_rules = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2
        rules = []
        for _ in range(num_rules):
            if offset >= len(data):
                break
            type_flag = data[offset]
            offset += 1
            if offset >= len(data):
                break
            byte_val = data[offset]
            offset += 1
            if offset + 2 > len(data):
                break
            pat_len = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            if offset + pat_len > len(data):
                break
            pattern = bytes(data[offset:offset + pat_len])
            offset += pat_len
            if type_flag == 0x01:
                rules.append((0x100 + byte_val, pattern))
            else:
                rules.append((byte_val, pattern))
        return rules, offset

    @staticmethod
    def apply_rules_reverse(data: bytes, rules: List[Tuple[int, bytes]]) -> bytes:
        """Reverse substitutions: BPE first, then escape/remap rules."""
        result = bytearray(data)

        bpe_rules = [(rid, pat) for rid, pat in rules if rid < 0x100]
        escape_rules = [(rid, pat) for rid, pat in rules if rid >= 0x100]

        # Reverse BPE rules (in reverse order)
        for replacement_byte, pattern in reversed(bpe_rules):
            new_result = bytearray()
            i = 0
            while i < len(result):
                if result[i] == replacement_byte:
                    new_result.extend(pattern)
                    i += 1
                else:
                    new_result.append(result[i])
                    i += 1
            result = new_result

        # Reverse escape/remap rules (in reverse order)
        for virtual_id, pattern in reversed(escape_rules):
            actual_byte = virtual_id & 0xFF
            new_result = bytearray()
            i = 0
            while i < len(result):
                if i <= len(result) - len(pattern) and bytes(result[i:i + len(pattern)]) == pattern:
                    new_result.append(actual_byte)
                    i += len(pattern)
                else:
                    new_result.append(result[i])
                    i += 1
            result = new_result

        return bytes(result)
