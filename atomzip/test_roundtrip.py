#!/usr/bin/env python3
"""
AtomZip v4 往返验证测试

测试所有压缩策略的无损正确性:
  - 策略0: LZMA2 RAW
  - 策略1: Delta + LZMA2 RAW
  - 策略2: BWT (全文件单块) + LZMA2 RAW
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from codec.compress import AtomZipCompressor
from codec.decompress import AtomZipDecompressor


def test_small_data():
    """测试小数据 (边界情况)。"""
    print("=" * 60)
    print("  测试小数据 (边界情况)")
    print("=" * 60)

    test_cases = [
        (b'', "空数据"),
        (b'\x00', "单字节 0x00"),
        (b'\xff', "单字节 0xFF"),
        (b'aaaa', "4个相同字符"),
        (b'abcd', "4个不同字符"),
        (b'\x00' * 100, "100个零字节"),
        (bytes(range(256)), "0-255全字节"),
    ]

    for data, desc in test_cases:
        for level in [1, 5, 9]:
            try:
                compressor = AtomZipCompressor(level=level, verbose=False)
                compressed = compressor.compress(data)
                decompressor = AtomZipDecompressor(verbose=False)
                decompressed = decompressor.decompress(compressed)
                match = data == decompressed
                status = "通过" if match else "失败"
                print(f"  [{status}] {desc} (级别 {level}): "
                      f"{len(data)} -> {len(compressed)} -> {len(decompressed)} 字节")
                if not match:
                    print(f"    原始: {data[:32].hex()}")
                    print(f"    结果: {decompressed[:32].hex()}")
                    return False
            except Exception as e:
                print(f"  [失败] {desc} (级别 {level}): {e}")
                import traceback
                traceback.print_exc()
                return False

    return True


def test_each_strategy():
    """分别测试每个策略。"""
    print()
    print("=" * 60)
    print("  测试各策略单独压缩/解压")
    print("=" * 60)

    import random
    random.seed(42)

    test_data = {
        "重复文本": b'hello world ' * 500,
        "递增序列": bytes(i % 256 for i in range(1000)),
        "随机数据": bytes(random.randint(0, 255) for _ in range(2000)),
        "结构化JSON": b'{"key": "value", "num": 12345}\n' * 100,
    }

    for name, data in test_data.items():
        for level in [1, 5, 9]:
            try:
                compressor = AtomZipCompressor(level=level, verbose=False)
                compressed = compressor.compress(data)
                decompressor = AtomZipDecompressor(verbose=False)
                decompressed = decompressor.decompress(compressed)
                match = data == decompressed
                ratio = len(data) / max(1, len(compressed))
                status = "通过" if match else "失败"
                print(f"  [{status}] {name} (级别 {level}): "
                      f"{len(data)} -> {len(compressed)} 字节, 比率 {ratio:.2f}:1")
                if not match:
                    for i in range(min(len(data), len(decompressed))):
                        if data[i] != decompressed[i]:
                            print(f"    首个差异: 位置 {i}, "
                                  f"期望 {data[i]:#x}, 实际 {decompressed[i]:#x}")
                            break
                    if len(data) != len(decompressed):
                        print(f"    长度不匹配: 期望 {len(data)}, 实际 {len(decompressed)}")
                    return False
            except Exception as e:
                print(f"  [失败] {name} (级别 {level}): {e}")
                import traceback
                traceback.print_exc()
                return False

    return True


def test_bwt_transform():
    """单独测试 BWT 变换的正确性。"""
    print()
    print("=" * 60)
    print("  测试 BWT 变换往返")
    print("=" * 60)

    from codec.transform import bwt_encode, bwt_decode, bwt_encode_block, bwt_decode_block

    test_cases = [
        (b'banana', "banana"),
        (b'abracadabra', "abracadabra"),
        (b'\x00' * 10, "10个零字节"),
        (bytes(range(50)), "0-49递增"),
        (b'Hello, World!', "Hello World"),
    ]

    for data, desc in test_cases:
        # 单块 BWT
        bwt, orig_idx = bwt_encode_block(data)
        decoded = bwt_decode_block(bwt, orig_idx)
        match = data == decoded
        status = "通过" if match else "失败"
        print(f"  [{status}] 单块 BWT: {desc}")
        if not match:
            print(f"    原始: {data.hex()}")
            print(f"    结果: {decoded.hex()}")
            return False

    # 全文件单块 BWT (block_size=0)
    longer_data = b'The quick brown fox jumps over the lazy dog. ' * 100
    bwt_data, block_info = bwt_encode(longer_data, block_size=0)
    decoded = bwt_decode(bwt_data, block_info)
    match = longer_data == decoded
    status = "通过" if match else "失败"
    print(f"  [{status}] 全文件单块 BWT: {len(longer_data)} 字节, {len(block_info)} 个块")
    if not match:
        for i in range(min(len(longer_data), len(decoded))):
            if longer_data[i] != decoded[i]:
                print(f"    首个差异: 位置 {i}")
                break
        return False

    return True


def test_delta_transform():
    """单独测试 Delta 变换的正确性。"""
    print()
    print("=" * 60)
    print("  测试 Delta 变换往返")
    print("=" * 60)

    from codec.transform import delta_encode, delta_decode

    test_cases = [
        (b'abcdefg', "递增字符"),
        (b'\x00\x01\x02\x03', "递增字节"),
        (b'\xff\x00\x01\x02', "环绕差分"),
        (b'aaaaaaa', "相同字符"),
    ]

    for data, desc in test_cases:
        encoded, first_byte = delta_encode(data)
        decoded = delta_decode(encoded, first_byte)
        match = data == decoded
        status = "通过" if match else "失败"
        print(f"  [{status}] Delta: {desc} (首字节: {first_byte:#x})")
        if not match:
            print(f"    原始: {data.hex()}")
            print(f"    结果: {decoded.hex()}")
            return False

    return True


def test_file_roundtrip():
    """测试实际文件的往返正确性。"""
    print()
    print("=" * 60)
    print("  测试实际文件往返")
    print("=" * 60)

    from pathlib import Path
    test_dir = Path(__file__).parent / "tests" / "test_files"

    if not test_dir.exists():
        print("  测试目录不存在，跳过")
        return True

    files = sorted(test_dir.iterdir())
    files = [f for f in files if f.is_file()]

    for filepath in files:
        with open(filepath, 'rb') as f:
            data = f.read()

        for level in [1, 5, 9]:
            try:
                compressor = AtomZipCompressor(level=level, verbose=False)
                compressed = compressor.compress(data)
                decompressor = AtomZipDecompressor(verbose=False)
                decompressed = decompressor.decompress(compressed)
                match = data == decompressed
                ratio = len(data) / max(1, len(compressed))
                status = "通过" if match else "失败"
                print(f"  [{status}] {filepath.name} (级别 {level}): "
                      f"{len(data):,} -> {len(compressed):,} 字节, "
                      f"比率 {ratio:.2f}:1")
                if not match:
                    for i in range(min(len(data), len(decompressed))):
                        if data[i] != decompressed[i]:
                            print(f"    首个差异: 位置 {i}, "
                                  f"期望 {data[i]:#x}, 实际 {decompressed[i]:#x}")
                            break
                    if len(data) != len(decompressed):
                        print(f"    长度不匹配: 期望 {len(data)}, "
                              f"实际 {len(decompressed)}")
                    return False
            except Exception as e:
                print(f"  [失败] {filepath.name} (级别 {level}): {e}")
                import traceback
                traceback.print_exc()
                return False

    return True


if __name__ == '__main__':
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║        AtomZip v4 往返验证测试                      ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    all_pass = True

    if not test_bwt_transform():
        all_pass = False
        print("\n!!! BWT 变换测试失败 !!!")

    if not test_delta_transform():
        all_pass = False
        print("\n!!! Delta 变换测试失败 !!!")

    if not test_small_data():
        all_pass = False
        print("\n!!! 小数据测试失败 !!!")

    if not test_each_strategy():
        all_pass = False
        print("\n!!! 策略测试失败 !!!")

    if not test_file_roundtrip():
        all_pass = False
        print("\n!!! 文件往返测试失败 !!!")

    print()
    if all_pass:
        print("╔══════════════════════════════════════════════════════╗")
        print("║        所有往返测试通过!                             ║")
        print("╚══════════════════════════════════════════════════════╝")
    else:
        print("╔══════════════════════════════════════════════════════╗")
        print("║        存在测试失败!                                 ║")
        print("╚══════════════════════════════════════════════════════╝")

    sys.exit(0 if all_pass else 1)
