"""
LZ77 Compression Module — Bit-Level Encoding

Unlike the previous byte-level LZ77 (which used 0xFF markers), this version
uses a compact encoding that preserves the byte distribution better:
  - Uses a flag byte to indicate literal vs match runs
  - Matches are encoded as (offset, length) pairs with compact varint
  - Literals are encoded as raw bytes

This is designed to work well with grammar BPE, which needs a diverse
but structured byte distribution to find good patterns.
"""

import struct
from collections import defaultdict
from typing import List, Tuple

MIN_MATCH_LEN = 4
MAX_MATCH_LEN = 65535
WINDOW_SIZE = 1 << 16


def lz77_compress(data: bytes) -> bytes:
    """Compress using LZ77 with compact flag-based encoding."""
    if len(data) < MIN_MATCH_LEN + 1:
        return _encode_all_literals(data)

    result = bytearray()
    # Header
    result.extend(struct.pack('>I', len(data)))

    # Build hash chain
    hash_table = defaultdict(list)

    i = 0
    n = len(data)
    literal_buf = bytearray()  # Buffer for pending literals

    while i < n:
        best_len = 0
        best_dist = 0

        if i + MIN_MATCH_LEN <= n:
            h = _hash4(data, i)
            candidates = hash_table.get(h, [])

            for j in range(len(candidates) - 1, max(len(candidates) - 64, -1), -1):
                pos = candidates[j]
                dist = i - pos
                if dist <= 0 or dist > WINDOW_SIZE:
                    continue

                match_len = 0
                limit = min(MAX_MATCH_LEN, n - i)
                while match_len < limit and data[pos + match_len] == data[i + match_len]:
                    match_len += 1

                if match_len > best_len:
                    best_len = match_len
                    best_dist = dist
                    if best_len >= 512:
                        break

        if best_len >= MIN_MATCH_LEN:
            # Flush pending literals first
            if literal_buf:
                _flush_literals(result, literal_buf)
                literal_buf = bytearray()

            # Encode match
            _encode_match(result, best_dist, best_len)

            # Update hash table
            for k in range(i, min(i + best_len, n - 3)):
                h = _hash4(data, k)
                hash_table[h].append(k)
                if len(hash_table[h]) > 512:
                    hash_table[h] = hash_table[h][-256:]
            i += best_len
        else:
            # Buffer literal
            literal_buf.append(data[i])

            if i + 3 < n:
                h = _hash4(data, i)
                hash_table[h].append(i)
                if len(hash_table[h]) > 512:
                    hash_table[h] = hash_table[h][-256:]
            i += 1

    # Flush remaining literals
    if literal_buf:
        _flush_literals(result, literal_buf)

    # End marker: match with length=0
    result.append(0x00)  # literal count = 0 with match flag
    result.append(0x00)  # match length = 0 (end signal)

    return bytes(result)


def lz77_decompress(data: bytes) -> bytes:
    """Decompress LZ77 data."""
    offset = 0
    if len(data) < 4:
        return data

    orig_len = struct.unpack('>I', data[offset:offset + 4])[0]
    offset += 4

    result = bytearray()

    while offset < len(data) and len(result) < orig_len:
        # Read flag byte
        if offset >= len(data):
            break
        flag = data[offset]
        offset += 1

        is_match = flag & 0x80
        count = flag & 0x7F

        if is_match:
            # Match: count = match_length (0 = end signal)
            if count == 0:
                # Check next byte for end signal
                if offset < len(data) and data[offset] == 0x00:
                    break
                # Otherwise count=0 means 128
                count = 128

            # Read offset (varint)
            dist, offset = _read_varint(data, offset)

            # Copy from back-reference (handles overlapping copies)
            start_pos = len(result) - dist
            if start_pos < 0 or dist <= 0:
                # Invalid back-reference, skip
                continue
            for k in range(count):
                # Must read from current result each time (allows overlapping)
                idx = start_pos + (k % dist)
                if idx < len(result):
                    result.append(result[idx])
                else:
                    result.append(0)
        else:
            # Literal run: count bytes follow (count=0 means 128)
            if count == 0:
                count = 128
            for _ in range(count):
                if offset < len(data):
                    result.append(data[offset])
                    offset += 1

    return bytes(result[:orig_len])


def _encode_all_literals(data: bytes) -> bytes:
    """Encode data as all literals."""
    result = bytearray()
    result.extend(struct.pack('>I', len(data)))
    i = 0
    while i < len(data):
        chunk_size = min(127, len(data) - i)
        result.append(chunk_size)  # literal flag + count
        result.extend(data[i:i + chunk_size])
        i += chunk_size
    result.append(0x00)  # End marker
    result.append(0x00)
    return bytes(result)


def _flush_literals(result: bytearray, literal_buf: bytearray):
    """Flush buffered literals to output."""
    i = 0
    while i < len(literal_buf):
        chunk_size = min(127, len(literal_buf) - i)
        result.append(chunk_size)  # literal flag: high bit 0, count in low 7 bits
        result.extend(literal_buf[i:i + chunk_size])
        i += chunk_size


def _encode_match(result: bytearray, dist: int, length: int):
    """Encode a match as: flag_byte + dist_varint + optional_length_ext."""
    # Flag byte: high bit = 1 (match), low 7 bits = length (1-127)
    # If length > 127, encode 127 in flag and extend
    remaining = length
    while remaining > 0:
        chunk_len = min(127, remaining)
        if chunk_len == 0:
            chunk_len = 127
        result.append(0x80 | chunk_len)
        _write_varint(result, dist)
        remaining -= chunk_len


def _hash4(data: bytes, pos: int) -> int:
    """4-byte hash for better match quality."""
    if pos + 3 >= len(data):
        return 0
    return ((data[pos] << 12) ^ (data[pos + 1] << 8) ^
            (data[pos + 2] << 4) ^ data[pos + 3]) & 0xFFFF


def _write_varint(buf: bytearray, value: int):
    """Write varint."""
    while value >= 0x80:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value & 0x7F)


def _read_varint(data: bytes, offset: int) -> Tuple[int, int]:
    """Read varint."""
    value = 0
    shift = 0
    while offset < len(data):
        b = data[offset]
        offset += 1
        value |= (b & 0x7F) << shift
        shift += 7
        if not (b & 0x80):
            break
    return value, offset
