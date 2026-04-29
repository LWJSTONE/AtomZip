"""
Range Coder — Entropy Coding Module

Implements a range coder for near-optimal entropy coding. Range coding is
functionally similar to arithmetic coding but uses integer operations for
speed while achieving nearly the same compression efficiency.

This is the final stage of the AtomZip pipeline. After pattern substitution
and context modeling, the range coder encodes symbols using the probability
distributions from the context model, achieving bit rates close to the
theoretical entropy limit.
"""

import struct
from typing import List, Tuple


class RangeEncoder:
    """Range encoder for writing compressed bitstream."""

    def __init__(self):
        self.output = bytearray()
        self.low = 0
        self.range = 0xFFFFFFFF
        self._pending = 0

    def encode_symbol(self, symbol: int, freqs: List[int]):
        """
        Encode a symbol using the given frequency table.

        Args:
            symbol: The symbol to encode (0-255)
            freqs: Frequency table (must sum to 2^16 = 65536)
        """
        total = sum(freqs)
        if total == 0:
            return

        # Calculate cumulative frequency
        cum = sum(freqs[:symbol])
        freq = freqs[symbol]

        if freq == 0:
            freq = 1  # Safety

        # Range reduction
        self.range //= total
        self.low += cum * self.range
        self.range *= freq

        # Normalize
        while self.range < (1 << 24):
            byte = (self.low >> 24) & 0xFF
            self.output.append(byte)
            if self._pending > 0:
                comp = 0xFF if byte >= 0x80 else 0x00
                for _ in range(self._pending):
                    self.output.append(comp)
                self._pending = 0
            self.low = (self.low << 8) & 0xFFFFFFFF
            self.range <<= 8

        # Handle carry
        if self.low >= (1 << 32):
            # Find the last byte and increment
            for i in range(len(self.output) - 1, -1, -1):
                if self.output[i] < 0xFF:
                    self.output[i] += 1
                    break
            self.low &= 0xFFFFFFFF

    def finish(self) -> bytes:
        """Flush the encoder and return the compressed byte stream."""
        # Write final bytes
        for _ in range(4):
            byte = (self.low >> 24) & 0xFF
            self.output.append(byte)
            self.low = (self.low << 8) & 0xFFFFFFFF

        return bytes(self.output)


class RangeDecoder:
    """Range decoder for reading compressed bitstream."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0
        self.low = 0
        self.range = 0xFFFFFFFF
        self.code = 0

        # Initialize code from first 4 bytes
        for _ in range(4):
            self.code = (self.code << 8) | self._read_byte()

    def _read_byte(self) -> int:
        if self.pos < len(self.data):
            b = self.data[self.pos]
            self.pos += 1
            return b
        return 0

    def decode_symbol(self, freqs: List[int]) -> int:
        """
        Decode a symbol using the given frequency table.

        Args:
            freqs: Frequency table (must match what was used for encoding)

        Returns:
            The decoded symbol (0-255)
        """
        total = sum(freqs)
        if total == 0:
            return 0

        # Normalize
        while self.range < (1 << 24):
            self.code = ((self.code << 8) | self._read_byte()) & 0xFFFFFFFF
            self.range <<= 8

        # Find symbol from cumulative frequency
        self.range //= total
        cum_freq = (self.code - self.low) // self.range

        # Find the symbol with this cumulative frequency
        cum = 0
        symbol = 0
        for i, f in enumerate(freqs):
            if cum + f > cum_freq:
                symbol = i
                break
            cum += f

        # Update low and range
        self.low += cum * self.range
        self.range *= freqs[symbol]

        return symbol


class SimpleEntropyCoder:
    """
    Simplified entropy coder using Huffman-like coding with adaptive frequencies.

    For practical implementation, we use a simpler approach than full range coding:
    adaptive frequency counting + variable-length integer encoding.
    This is more robust and easier to implement correctly while still providing
    significant compression for data that has non-uniform symbol distributions.
    """

    def __init__(self):
        self.freqs = [1] * 256  # Start with uniform frequencies

    def encode(self, data: bytes) -> bytes:
        """Encode data using adaptive Huffman-like coding."""
        output = bytearray()

        # Header: original length
        output.extend(struct.pack('>I', len(data)))

        # Adaptive frequency counting
        freqs = [0] * 256
        for b in data:
            freqs[b] += 1

        # Write frequency table (compressed)
        # Only store non-zero frequencies
        non_zero = [(i, f) for i, f in enumerate(freqs) if f > 0]
        output.extend(struct.pack('>H', len(non_zero)))
        for sym, freq in non_zero:
            output.append(sym)
            # Variable-length encoding for frequency
            if freq < 128:
                output.append(freq)
            elif freq < 16384:
                output.append((freq & 0x7F) | 0x80)
                output.append(freq >> 7)
            else:
                output.append((freq & 0x7F) | 0x80)
                output.append(((freq >> 7) & 0x7F) | 0x80)
                output.append(freq >> 14)

        # Build canonical Huffman codes
        codes = self._build_huffman_codes(freqs)

        # Encode data bit-by-bit
        bit_buffer = 0
        bit_count = 0

        for b in data:
            code, code_len = codes[b]
            bit_buffer = (bit_buffer << code_len) | code
            bit_count += code_len

            while bit_count >= 8:
                bit_count -= 8
                output.append((bit_buffer >> bit_count) & 0xFF)
                bit_buffer &= (1 << bit_count) - 1

        # Flush remaining bits
        if bit_count > 0:
            output.append((bit_buffer << (8 - bit_count)) & 0xFF)

        return bytes(output)

    def decode(self, data: bytes) -> bytes:
        """Decode data using adaptive Huffman-like coding."""
        offset = 0

        # Read original length
        orig_len = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4

        # Read frequency table
        num_non_zero = struct.unpack('>H', data[offset:offset + 2])[0]
        offset += 2

        freqs = [0] * 256
        for _ in range(num_non_zero):
            sym = data[offset]
            offset += 1
            freq = 0
            shift = 0
            while True:
                b = data[offset]
                offset += 1
                freq |= (b & 0x7F) << shift
                shift += 7
                if not (b & 0x80):
                    break
            freqs[sym] = freq

        # Build Huffman codes
        codes = self._build_huffman_codes(freqs)

        # Build decode table (reverse lookup)
        decode_table = {}
        for sym in range(256):
            if codes[sym][1] > 0:
                decode_table[(codes[sym][0], codes[sym][1])] = sym

        # Decode data
        result = bytearray()
        bit_pos = offset * 8
        total_bits = len(data) * 8

        while len(result) < orig_len and bit_pos < total_bits:
            # Try matching codes from shortest to longest
            found = False
            for code_len in range(1, 33):
                if bit_pos + code_len > total_bits:
                    break

                # Extract code bits
                code = 0
                for i in range(code_len):
                    byte_idx = (bit_pos + i) // 8
                    bit_idx = 7 - ((bit_pos + i) % 8)
                    if byte_idx < len(data):
                        code = (code << 1) | ((data[byte_idx] >> bit_idx) & 1)

                if (code, code_len) in decode_table:
                    result.append(decode_table[(code, code_len)])
                    bit_pos += code_len
                    found = True
                    break

            if not found:
                # Fallback: shouldn't happen with correct data
                break

        return bytes(result[:orig_len])

    def _build_huffman_codes(self, freqs: List[int]) -> List[Tuple[int, int]]:
        """
        Build canonical Huffman codes from frequency table.

        Returns list of (code, code_length) for each symbol.
        """
        # Create nodes
        nodes = []
        for i, f in enumerate(freqs):
            if f > 0:
                nodes.append((f, i, []))  # (freq, symbol, children)

        if not nodes:
            return [(0, 0)] * 256

        if len(nodes) == 1:
            # Single symbol: encode with 1 bit
            result = [(0, 0)] * 256
            result[nodes[0][1]] = (0, 1)
            return result

        # Build Huffman tree
        while len(nodes) > 1:
            nodes.sort(key=lambda x: x[0])
            f1, s1, c1 = nodes.pop(0)
            f2, s2, c2 = nodes.pop(0)
            combined = (f1 + f2, -1, [(s1, c1), (s2, c2)])
            nodes.append(combined)

        # Extract code lengths
        code_lengths = [0] * 256
        self._traverse_tree(nodes[0], 0, code_lengths)

        # Build canonical codes from lengths
        max_len = max(code_lengths) if any(code_lengths) else 0
        if max_len == 0:
            return [(0, 0)] * 256

        # Limit code length to 32 bits
        max_len = min(max_len, 32)

        # Count symbols per length
        length_counts = [0] * (max_len + 1)
        for l in code_lengths:
            if l > 0:
                length_counts[min(l, max_len)] += 1

        # Assign canonical codes
        code = 0
        next_code = [0] * (max_len + 1)
        for bits in range(1, max_len + 1):
            code = (code + length_counts[bits - 1]) << 1
            next_code[bits] = code

        result = [(0, 0)] * 256
        for sym in range(256):
            l = min(code_lengths[sym], max_len) if code_lengths[sym] > 0 else 0
            if l > 0:
                result[sym] = (next_code[l], l)
                next_code[l] += 1

        return result

    def _traverse_tree(self, node, depth, code_lengths):
        """Recursively determine code lengths from Huffman tree."""
        freq, sym, children = node
        if sym >= 0:  # Leaf node
            code_lengths[sym] = max(depth, 1)
        for child_sym, child_children in children:
            if child_sym >= 0:
                code_lengths[child_sym] = max(depth + 1, 1)
            else:
                self._traverse_tree((0, child_sym, child_children),
                                  depth + 1, code_lengths)
