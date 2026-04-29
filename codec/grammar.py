"""
Grammar-Based Pattern Extraction — REPC v2 Core Module

Key improvement over v1: Uses integer non-terminal symbols instead of free bytes,
removing the 256-rule limit. This allows unlimited hierarchical grammar construction.

The grammar is a context-free grammar where:
  - Terminals: byte values 0-255
  - Non-terminals: integer IDs >= 256
  - Each production rule: non-terminal -> [symbol, symbol] (pair replacement)

The REPC scoring criterion is used to select which pairs to replace:
  Score(pair) = frequency * (1 + min(context_diversity, 3.0))

After grammar construction, single-use rules are inlined (they waste space)
and rules are renumbered compactly starting from 256.
"""

import struct
from collections import defaultdict
from typing import List, Dict, Tuple


class GrammarExtractor:
    """Grammar-based pattern extraction with REPC scoring — unlimited rules."""

    def __init__(self, max_rules: int = 3000, min_freq: int = 2):
        self.max_rules = max_rules
        self.min_freq = min_freq

    def extract_and_apply(self, data: bytes) -> Tuple[List[int], Dict[int, List[int]]]:
        """
        Build a grammar from data using REPC-enhanced hierarchical BPE.

        Unlike the free-byte approach (limited to 256 rules), this uses
        integer non-terminal IDs starting from 256, allowing unlimited rules.

        Returns:
            symbols: Start symbol sequence after all replacements
            rules: Grammar rules {non_terminal_id: [rhs_symbols]}
        """
        if len(data) < 4:
            return list(data), {}

        symbols = list(data)
        rules: Dict[int, List[int]] = {}
        next_nt = 256

        # Adaptive min_freq based on data size for speed
        adaptive_min_freq = max(self.min_freq, len(data) // 50000)
        # Adaptive max rules
        adaptive_max_rules = min(self.max_rules, len(data) // 2)

        iteration = 0
        while iteration < adaptive_max_rules:
            # Count pairs and context diversity in one pass
            pair_freq = defaultdict(int)
            pair_left_ctx = defaultdict(set)
            pair_right_ctx = defaultdict(set)

            n = len(symbols)
            for i in range(n - 1):
                pair = (symbols[i], symbols[i + 1])
                pair_freq[pair] += 1
                if i > 0:
                    pair_left_ctx[pair].add(symbols[i - 1])
                else:
                    pair_left_ctx[pair].add(None)
                if i + 2 < n:
                    pair_right_ctx[pair].add(symbols[i + 2])
                else:
                    pair_right_ctx[pair].add(None)

            if not pair_freq:
                break

            # Find best pair using REPC scoring
            best_score = 0
            best_pair = None
            best_freq = 0

            for pair, freq in pair_freq.items():
                if freq < adaptive_min_freq:
                    continue
                # Only replace if it saves space: freq occurrences saved,
                # each saves 1 symbol (pair of 2 -> 1 non-terminal),
                # but costs: rule definition (2 symbols + overhead)
                net_savings = freq - 1  # symbols saved in data, minus rule cost of 1
                if net_savings < 1:
                    continue

                # REPC Score: frequency * (1 + context_diversity)
                left_div = len(pair_left_ctx[pair])
                right_div = len(pair_right_ctx[pair])
                diversity = (left_div * right_div) / max(1, freq)
                score = freq * (1.0 + min(diversity, 3.0))

                if score > best_score:
                    best_score = score
                    best_pair = pair
                    best_freq = freq

            if best_pair is None:
                break

            # Create new grammar rule
            nt = next_nt
            next_nt += 1
            rules[nt] = list(best_pair)

            # Replace all non-overlapping occurrences (left-to-right)
            new_symbols = []
            i = 0
            while i < n:
                if i < n - 1 and symbols[i] == best_pair[0] and symbols[i + 1] == best_pair[1]:
                    new_symbols.append(nt)
                    i += 2
                else:
                    new_symbols.append(symbols[i])
                    i += 1

            symbols = new_symbols
            iteration += 1

            # Stop if data is very short already
            if len(symbols) < 4:
                break

        # Inline single-use rules (they don't save space)
        symbols, rules = self._inline_single_use(symbols, rules)

        # Renumber non-terminals compactly
        symbols, rules = self._renumber(symbols, rules)

        return symbols, rules

    def _inline_single_use(self, symbols: List[int],
                           rules: Dict[int, List[int]]) -> Tuple[List[int], Dict[int, List[int]]]:
        """Inline rules used only once — they waste space."""
        rules = dict(rules)  # Copy

        changed = True
        while changed:
            changed = False
            # Count usage
            usage = defaultdict(int)
            for s in symbols:
                if s >= 256:
                    usage[s] += 1
            for nt, rhs in rules.items():
                for s in rhs:
                    if s >= 256:
                        usage[s] += 1

            # Find and inline single-use rules
            for nt in list(rules.keys()):
                if usage.get(nt, 0) <= 1 and nt in rules:
                    rhs = rules[nt]
                    # Replace in symbols
                    new_symbols = []
                    for s in symbols:
                        if s == nt:
                            new_symbols.extend(rhs)
                        else:
                            new_symbols.append(s)
                    symbols = new_symbols
                    # Replace in other rules
                    for other_nt in list(rules.keys()):
                        if other_nt == nt:
                            continue
                        new_rhs = []
                        for s in rules[other_nt]:
                            if s == nt:
                                new_rhs.extend(rhs)
                            else:
                                new_rhs.append(s)
                        rules[other_nt] = new_rhs
                    del rules[nt]
                    changed = True
                    break  # Restart after each inline

        return symbols, rules

    def _renumber(self, symbols: List[int],
                  rules: Dict[int, List[int]]) -> Tuple[List[int], Dict[int, List[int]]]:
        """Renumber non-terminals compactly starting from 256."""
        if not rules:
            return symbols, rules

        old_nts = sorted(rules.keys())
        nt_map = {}
        for i, old_nt in enumerate(old_nts):
            nt_map[old_nt] = 256 + i

        new_rules = {}
        for old_nt, rhs in rules.items():
            new_nt = nt_map[old_nt]
            new_rhs = [nt_map.get(s, s) for s in rhs]
            new_rules[new_nt] = new_rhs

        new_symbols = [nt_map.get(s, s) for s in symbols]
        return new_symbols, new_rules

    @staticmethod
    def serialize_grammar(symbols: List[int],
                          rules: Dict[int, List[int]]) -> bytes:
        """Serialize grammar to a compact byte stream using varint encoding."""
        result = bytearray()

        # Number of rules
        _encode_varint(result, len(rules))

        # Rules (sorted by non-terminal ID)
        for nt in sorted(rules.keys()):
            rhs = rules[nt]
            _encode_varint(result, nt)
            _encode_varint(result, len(rhs))
            for s in rhs:
                _encode_varint(result, s)

        # Start symbol sequence
        _encode_varint(result, len(symbols))
        for s in symbols:
            _encode_varint(result, s)

        return bytes(result)

    @staticmethod
    def deserialize_grammar(data: bytes) -> Tuple[List[int], Dict[int, List[int]], int]:
        """
        Deserialize grammar from byte stream.

        Returns (symbols, rules, bytes_consumed).
        """
        offset = 0

        # Number of rules
        num_rules, offset = _decode_varint(data, offset)

        rules = {}
        for _ in range(num_rules):
            nt, offset = _decode_varint(data, offset)
            rhs_len, offset = _decode_varint(data, offset)
            rhs = []
            for _ in range(rhs_len):
                s, offset = _decode_varint(data, offset)
                rhs.append(s)
            rules[nt] = rhs

        # Start symbol sequence
        seq_len, offset = _decode_varint(data, offset)
        symbols = []
        for _ in range(seq_len):
            s, offset = _decode_varint(data, offset)
            symbols.append(s)

        return symbols, rules, offset

    @staticmethod
    def expand_grammar(symbols: List[int],
                       rules: Dict[int, List[int]]) -> bytes:
        """Expand grammar to recover original byte sequence."""
        result = list(symbols)

        # Iteratively expand non-terminals until only terminals remain
        max_nt = max(rules.keys()) if rules else 255
        changed = True
        while changed:
            changed = False
            new_result = []
            for s in result:
                if s >= 256 and s in rules:
                    new_result.extend(rules[s])
                    changed = True
                else:
                    new_result.append(s)
            result = new_result

        return bytes(s for s in result if 0 <= s <= 255)


def _encode_varint(buf: bytearray, value: int):
    """Encode integer as variable-length byte sequence."""
    if value < 0:
        raise ValueError("Negative values not supported")
    elif value < 0x80:
        buf.append(value)
    elif value < 0x4000:
        buf.append((value >> 7) | 0x80)
        buf.append(value & 0x7F)
    elif value < 0x200000:
        buf.append((value >> 14) | 0xC0)
        buf.append((value >> 7) & 0x7F | 0x80)
        buf.append(value & 0x7F)
    elif value < 0x10000000:
        buf.append((value >> 21) | 0xE0)
        buf.append((value >> 14) & 0x7F | 0x80)
        buf.append((value >> 7) & 0x7F | 0x80)
        buf.append(value & 0x7F)
    else:
        buf.append(0xF0)
        buf.extend(struct.pack('>I', value))


def _decode_varint(data: bytes, offset: int) -> Tuple[int, int]:
    """Decode variable-length integer, returns (value, new_offset)."""
    if offset >= len(data):
        raise ValueError("Unexpected end of data in varint decoding")

    b = data[offset]

    if b < 0x80:
        return b, offset + 1
    elif b < 0xC0:
        if offset + 1 >= len(data):
            raise ValueError("Unexpected end of data")
        value = ((b & 0x3F) << 7) | (data[offset + 1] & 0x7F)
        return value, offset + 2
    elif b < 0xE0:
        if offset + 2 >= len(data):
            raise ValueError("Unexpected end of data")
        value = ((b & 0x1F) << 14) | ((data[offset + 1] & 0x7F) << 7) | (data[offset + 2] & 0x7F)
        return value, offset + 3
    elif b < 0xF0:
        if offset + 3 >= len(data):
            raise ValueError("Unexpected end of data")
        value = (((b & 0x0F) << 21) |
                 ((data[offset + 1] & 0x7F) << 14) |
                 ((data[offset + 2] & 0x7F) << 7) |
                 (data[offset + 3] & 0x7F))
        return value, offset + 4
    else:
        if offset + 4 >= len(data):
            raise ValueError("Unexpected end of data")
        value = struct.unpack('>I', data[offset + 1:offset + 5])[0]
        return value, offset + 5
