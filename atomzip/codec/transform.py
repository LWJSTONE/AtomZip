"""
AtomZip 数据变换模块 — BWT / Delta / 序列化

提供压缩流水线使用的数据变换:
  - BWT (Burrows-Wheeler 变换): 按上下文聚簇字符，使相似内容聚集
  - Delta (差分编码): 编码相邻字节的差值，降低数值幅度

核心发现:
  BWT + LZMA2 (不加 MTF) 的组合比 BWT + MTF + LZMA2 效果更好。
  原因: MTF 虽然产生了大量零值，但同时打乱了字节的空间局部性，
  使 LZMA2 的 LZ77 匹配器无法利用跨块的长距离重复。
  不加 MTF 时，BWT 输出保持了"相同上下文的字符聚簇"这一关键性质，
  LZMA2 能在聚簇区域找到高效的长匹配。

BWT 块策略:
  使用整个文件作为单个 BWT 块。虽然 O(n log² n) 复杂度对大文件
  速度较慢，但单块 BWT 的压缩效果远优于分块（避免块边界信息丢失）。
  对于超过 2MB 的文件，退回到不使用 BWT 策略。
"""

import struct
from typing import Tuple, List

# 允许使用 BWT 策略的最大数据大小 (2MB)
BWT_MAX_DATA_SIZE = 2 << 20


# ─────────────────────────────────────────────
#  BWT (Burrows-Wheeler Transform)
# ─────────────────────────────────────────────

def _build_cyclic_sa(data: bytes) -> List[int]:
    """
    构建循环后缀数组，用于 BWT 编码。

    使用前缀倍增法 (prefix doubling)，复杂度 O(n log² n)。
    对每个位置 i，排序键为 (rank[i], rank[(i+k)%n])。
    当所有排名唯一时提前终止。
    """
    n = len(data)
    if n <= 1:
        return [0]

    sa = list(range(n))
    rank = list(data)
    tmp_rank = [0] * n

    k = 1
    while k < n:
        # 构建排序键 — 捕获当前 rank，避免闭包问题
        keys = [(rank[i], rank[(i + k) % n]) for i in range(n)]
        sa.sort(key=lambda idx: keys[idx])

        # 更新排名
        tmp_rank[sa[0]] = 0
        for j in range(1, n):
            tmp_rank[sa[j]] = tmp_rank[sa[j - 1]]
            if keys[sa[j]] != keys[sa[j - 1]]:
                tmp_rank[sa[j]] += 1

        rank = tmp_rank[:]

        # 所有排名唯一 → 排序完成
        if rank[sa[-1]] == n - 1:
            break

        k *= 2

    return sa


def bwt_encode_block(data: bytes) -> Tuple[bytes, int]:
    """
    对单个数据块执行 BWT 编码。

    返回:
        (bwt_output, orig_idx)
        bwt_output: BWT 变换后的数据
        orig_idx: 原始字符串在排序后的位置 (用于解码)
    """
    n = len(data)
    if n == 0:
        return b'', 0
    if n == 1:
        return data, 0

    # 小块 (<=4KB) 使用直接排序 (速度更快)
    if n <= 4096:
        doubled = data + data
        indices = sorted(range(n), key=lambda i: doubled[i:i + n])
    else:
        # 大块使用前缀倍增法
        indices = _build_cyclic_sa(data)

    # BWT 输出 = 排序矩阵的最后一列
    bwt = bytes(data[(i - 1) % n] for i in indices)

    # 找到原始字符串在排序中的位置
    orig_idx = indices.index(0)

    return bwt, orig_idx


def bwt_decode_block(bwt: bytes, orig_idx: int) -> bytes:
    """
    对单个数据块执行 BWT 解码 (逆变换)。

    使用 LF 映射 (Last-to-First) 算法，
    从 orig_idx 出发追踪 n 步还原原始数据。
    """
    n = len(bwt)
    if n == 0:
        return b''
    if n == 1:
        return bwt

    # 统计每个字符的出现次数
    count = [0] * 256
    for c in bwt:
        count[c] += 1

    # 计算累积计数 (首列中各字符的起始位置)
    cumul = [0] * 256
    total = 0
    for i in range(256):
        cumul[i] = total
        total += count[i]

    # 构建 LF 映射
    lf = [0] * n
    occ = [0] * 256
    for i in range(n):
        c = bwt[i]
        lf[i] = cumul[c] + occ[c]
        occ[c] += 1

    # 从 orig_idx 出发，逆序重建原始数据
    result = bytearray(n)
    j = orig_idx
    for i in range(n - 1, -1, -1):
        result[i] = bwt[j]
        j = lf[j]

    return bytes(result)


def bwt_encode(data: bytes, block_size: int = 0) -> Tuple[bytes, List[Tuple[int, int]]]:
    """
    分块 BWT 编码。

    block_size=0 表示使用整个文件作为单个块 (最优压缩)。
    对数据按 block_size 分块，对每块独立做 BWT。

    返回:
        (bwt_data, block_info)
        bwt_data: 所有 BWT 块拼接后的数据
        block_info: [(orig_idx, block_size), ...] 每块的元信息
    """
    if len(data) == 0:
        return b'', []

    # block_size=0 表示全文件单块
    if block_size == 0 or block_size >= len(data):
        bwt, orig_idx = bwt_encode_block(data)
        return bwt, [(orig_idx, len(data))]

    blocks = []
    offset = 0
    while offset < len(data):
        end = min(offset + block_size, len(data))
        block = data[offset:end]
        bwt, orig_idx = bwt_encode_block(block)
        blocks.append((bwt, orig_idx, len(block)))
        offset = end

    # 拼接所有 BWT 块
    bwt_data = b''.join(b for b, _, _ in blocks)
    block_info = [(orig_idx, bs) for _, orig_idx, bs in blocks]

    return bwt_data, block_info


def bwt_decode(bwt_data: bytes, block_info: List[Tuple[int, int]]) -> bytes:
    """
    分块 BWT 解码。

    根据 block_info 将 bwt_data 分割为多个块，
    对每块独立做逆 BWT，然后拼接还原。
    """
    if not block_info:
        return b''

    result = bytearray()
    offset = 0
    for orig_idx, block_size in block_info:
        block = bwt_data[offset:offset + block_size]
        decoded = bwt_decode_block(block, orig_idx)
        result.extend(decoded)
        offset += block_size

    return bytes(result)


# ─────────────────────────────────────────────
#  Delta (差分编码)
# ─────────────────────────────────────────────

def delta_encode(data: bytes) -> Tuple[bytes, int]:
    """
    差分编码: delta[i] = (data[i] - data[i-1]) mod 256

    相邻字节值相近的数据 (如文本、结构化二进制) 经差分编码后
    会出现大量 0 和小值，有利于后续压缩。

    返回:
        (encoded, first_byte)
        encoded: 差分编码后的数据 (长度与输入相同)
        first_byte: 原始数据的第一个字节 (用于解码)
    """
    if not data:
        return b'', 0

    first_byte = data[0]
    result = bytearray(len(data))
    result[0] = 0
    for i in range(1, len(data)):
        result[i] = (data[i] - data[i - 1]) % 256

    return bytes(result), first_byte


def delta_decode(data: bytes, first_byte: int) -> bytes:
    """
    差分解码: 逆运算恢复原始数据。
    """
    if not data:
        return b''

    result = bytearray(len(data))
    result[0] = first_byte
    for i in range(1, len(data)):
        result[i] = (result[i - 1] + data[i]) % 256

    return bytes(result)


# ─────────────────────────────────────────────
#  序列化辅助
# ─────────────────────────────────────────────

def serialize_block_info(block_info: List[Tuple[int, int]]) -> bytes:
    """
    序列化 BWT 块信息: [(orig_idx, block_size), ...]

    格式:
      2字节: 块数量
      4字节: 最后一个块的大小 (用于检测非均匀分块)
      对每个块:
        4字节: orig_idx
        4字节: 实际块大小
    """
    result = bytearray()
    result.extend(struct.pack('>H', len(block_info)))  # num_blocks (uint16)

    for orig_idx, block_size in block_info:
        result.extend(struct.pack('>I', orig_idx))   # orig_idx (uint32)
        result.extend(struct.pack('>I', block_size)) # block_size (uint32)

    return bytes(result)


def deserialize_block_info(data: bytes, offset: int = 0) -> Tuple[List[Tuple[int, int]], int]:
    """反序列化 BWT 块信息。"""
    num_blocks = struct.unpack('>H', data[offset:offset + 2])[0]
    offset += 2

    block_info = []
    for _ in range(num_blocks):
        orig_idx = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        block_size = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        block_info.append((orig_idx, block_size))

    return block_info, offset
