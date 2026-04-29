"""
Pattern Extraction Module — Recursive Entropic Pattern Collapse (REPC)

Core Innovation: Unlike traditional BPE which selects patterns by frequency alone,
REPC uses an information-entropy-gain criterion that considers both frequency AND
context diversity. Patterns appearing in many different contexts are preferred
because replacing them reduces the global entropy of the data stream more effectively.

Scoring: Score(pair) = frequency * (1 + min(context_diversity, 3.0))

This module implements hierarchical BPE (Byte Pair Encoding) with the REPC scoring
criterion. After each replacement, new pairs form (including with replacement bytes),
enabling hierarchical pattern building that captures longer-range structure.
"""

import struct
from collections import defaultdict
from typing import List, Tuple


class PatternExtractor:
    """Extracts and substitutes patterns using entropy-gain criterion."""

    def __init__(self, max_rules: int = 200, min_freq: int = 2):
        self.max_rules = max_rules
        self.min_freq = min_freq

    def extract_and_apply(self, data: bytes) -> Tuple[bytes, List[Tuple[int, bytes]]]:
        """
        Hierarchical BPE with entropy-gain criterion.

        Process:
        1. Find all byte pairs and their frequencies
        2. Score each pair using the REPC formula (freq x (1 + context_diversity))
        3. Replace the highest-scoring pair with an unused byte value
        4. The replacement byte can form new pairs with neighbors, enabling
           hierarchical grammar building
        5. Repeat until no free bytes remain or no beneficial pairs exist

        If all 256 byte values are used, free up the least frequent byte by
        escaping it with the second-least-frequent byte.

        Returns:
            (transformed_data, rules) where rules is [(replacement_byte, original_pattern), ...]
        """
        if len(data) < 4:
            return data, []

        data = bytearray(data)
        rules: List[Tuple[int, bytes]] = []

        # Find free bytes (unused byte values)
        used = set(data)
        free_bytes = sorted([b for b in range(256) if b not in used])

        # If no free bytes, escape the least frequent byte to free it
        if not free_bytes:
            data, free_bytes, escape_rules = self._free_least_frequent(data)
            rules.extend(escape_rules)

        iteration = 0
        while free_bytes and iteration < self.max_rules:
            # Count all adjacent pairs and their context diversity
            pair_freq = defaultdict(int)
            pair_left_ctx = defaultdict(set)
            pair_right_ctx = defaultdict(set)

            for i in range(len(data) - 1):
                pair = (data[i], data[i + 1])
                pair_freq[pair] += 1
                if i > 0:
                    pair_left_ctx[pair].add(data[i - 1])
                else:
                    pair_left_ctx[pair].add(None)
                if i + 2 < len(data):
                    pair_right_ctx[pair].add(data[i + 2])
                else:
                    pair_right_ctx[pair].add(None)

            if not pair_freq:
                break

            # Find best pair using REPC scoring
            best_score = 0
            best_pair = None

            for pair, freq in pair_freq.items():
                if freq < self.min_freq:
                    continue

                # REPC Score: frequency x (1 + context_diversity)
                left_div = len(pair_left_ctx[pair])
                right_div = len(pair_right_ctx[pair])
                diversity = (left_div * right_div) / max(1, freq)
                score = freq * (1.0 + min(diversity, 3.0))

                if score > best_score:
                    best_score = score
                    best_pair = pair

            if best_pair is None or best_score < 2:
                break

            # Replace the pair with the next free byte
            replacement = free_bytes.pop(0)
            pattern = bytes(best_pair)

            # Perform replacement (left-to-right, non-overlapping)
            new_data = bytearray()
            i = 0
            while i < len(data):
                if i <= len(data) - 2 and data[i] == best_pair[0] and data[i + 1] == best_pair[1]:
                    new_data.append(replacement)
                    i += 2
                else:
                    new_data.append(data[i])
                    i += 1

            data = new_data
            rules.append((replacement, pattern))
            iteration += 1

            # Check if new free bytes became available
            current_used = set(data)
            new_free = [b for b in range(256) if b not in current_used and b not in free_bytes]
            free_bytes.extend(sorted(new_free))

        return bytes(data), rules

    def _free_least_frequent(self, data: bytearray) -> Tuple[bytearray, list, list]:
        """
        Free the least frequent byte by escaping it with the second-least frequent byte.

        Replaces all occurrences of the least-frequent byte (target) with:
          escape_byte + 0x00
        And escapes the escape byte itself as:
          escape_byte + 0x01

        Returns (modified_data, new_free_bytes, escape_rules)
        """
        freq = [0] * 256
        for b in data:
            freq[b] += 1

        candidates = [(freq[b], b) for b in range(256) if freq[b] > 0]
        candidates.sort()

        if len(candidates) < 2:
            return data, [], []

        target_byte = candidates[0][1]
        escape_byte = candidates[1][1]

        # Perform escaping
        new_data = bytearray()
        for b in data:
            if b == target_byte:
                new_data.append(escape_byte)
                new_data.append(0x00)
            elif b == escape_byte:
                new_data.append(escape_byte)
                new_data.append(0x01)
            else:
                new_data.append(b)

        # Create escape rules with virtual IDs (0x100+actual_byte)
        # These represent: "the 2-byte sequence should be replaced by the single byte"
        escape_rules = [
            (0x100 + target_byte, bytes([escape_byte, 0x00])),
            (0x100 + escape_byte, bytes([escape_byte, 0x01])),
        ]

        return new_data, [target_byte], escape_rules

    @staticmethod
    def serialize_rules(rules: List[Tuple[int, bytes]]) -> bytes:
        """
        Serialize rules into a compact byte stream.

        Format per rule:
          [1 byte: type flag] 0x00=normal BPE, 0x01=escape
          [1 byte: replacement_byte (for normal) or actual_byte (for escape)]
          [2 bytes: pattern length (big-endian)]
          [N bytes: pattern data]
        """
        result = bytearray()
        result.extend(struct.pack('>H', len(rules)))
        for replacement_byte, pattern in rules:
            if replacement_byte >= 0x100:
                # Escape rule
                result.append(0x01)  # type flag
                result.append(replacement_byte & 0xFF)
            else:
                # Normal BPE rule
                result.append(0x00)  # type flag
                result.append(replacement_byte)
            pat_len = len(pattern)
            result.extend(struct.pack('>H', pat_len))
            result.extend(pattern)
        return bytes(result)

    @staticmethod
    def deserialize_rules(data: bytes) -> Tuple[List[Tuple[int, bytes]], int]:
        """Deserialize rules from byte stream."""
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
        """
        Reverse all substitutions to recover original data.
        Process BPE rules in reverse order, then escape rules in reverse order.
        """
        result = bytearray(data)

        # Separate rules by type
        escape_rules = [(rid, pat) for rid, pat in rules if rid >= 0x100]
        bpe_rules = [(rid, pat) for rid, pat in rules if rid < 0x100]

        # Reverse BPE rules (in reverse order of application)
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

        # Reverse escape rules (in reverse order)
        # These replace 2-byte sequences with single bytes
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
