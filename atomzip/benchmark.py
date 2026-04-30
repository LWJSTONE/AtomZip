#!/usr/bin/env python3
"""
AtomZip 基准测试 — 与 LZMA (7z极限) 和 gzip (最佳) 对比

测试多种文件类型: 文本、二进制、结构化数据、源代码、JSON、日志。
对比压缩比率、压缩时间和解压时间。
"""

import sys
import os
import time
import json
import lzma
import gzip
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from codec.compress_v10 import AtomZipCompressor
from codec.decompress_v10 import AtomZipDecompressor


def benchmark_atomzip(filepath: str, level: int = 9) -> dict:
    """测试 AtomZip 压缩和解压。"""
    with open(filepath, 'rb') as f:
        data = f.read()

    original_size = len(data)

    compressor = AtomZipCompressor(level=level, verbose=False)

    # 压缩
    start = time.time()
    compressed = compressor.compress(data)
    comp_time = time.time() - start

    # 解压
    decompressor = AtomZipDecompressor(verbose=False)
    start = time.time()
    decompressed = decompressor.decompress(compressed)
    decomp_time = time.time() - start

    verified = data == decompressed

    return {
        'original_size': original_size,
        'compressed_size': len(compressed),
        'ratio': original_size / max(1, len(compressed)),
        'space_savings': 100 * (1 - len(compressed) / max(1, original_size)),
        'comp_time': comp_time,
        'decomp_time': decomp_time,
        'verified': verified,
    }


def benchmark_lzma(filepath: str) -> dict:
    """测试 LZMA/7z 极限压缩。"""
    with open(filepath, 'rb') as f:
        data = f.read()

    original_size = len(data)

    filters = [
        {'id': lzma.FILTER_LZMA2,
         'preset': 9 | lzma.PRESET_EXTREME}
    ]

    # 压缩
    start = time.time()
    compressed = lzma.compress(data, format=lzma.FORMAT_XZ, filters=filters)
    comp_time = time.time() - start

    # 解压
    start = time.time()
    decompressed = lzma.decompress(compressed)
    decomp_time = time.time() - start

    verified = data == decompressed

    return {
        'original_size': original_size,
        'compressed_size': len(compressed),
        'ratio': original_size / max(1, len(compressed)),
        'space_savings': 100 * (1 - len(compressed) / max(1, original_size)),
        'comp_time': comp_time,
        'decomp_time': decomp_time,
        'verified': verified,
    }


def benchmark_gzip(filepath: str) -> dict:
    """测试 gzip 最佳压缩。"""
    with open(filepath, 'rb') as f:
        data = f.read()

    original_size = len(data)

    # 压缩
    start = time.time()
    compressed = gzip.compress(data, compresslevel=9)
    comp_time = time.time() - start

    # 解压
    start = time.time()
    decompressed = gzip.decompress(compressed)
    decomp_time = time.time() - start

    verified = data == decompressed

    return {
        'original_size': original_size,
        'compressed_size': len(compressed),
        'ratio': original_size / max(1, len(compressed)),
        'space_savings': 100 * (1 - len(compressed) / max(1, original_size)),
        'comp_time': comp_time,
        'decomp_time': decomp_time,
        'verified': verified,
    }


def main():
    test_dir = Path(__file__).parent / "tests" / "test_files"

    if not test_dir.exists():
        print(f"测试目录不存在: {test_dir}")
        sys.exit(1)

    files = sorted(test_dir.iterdir())
    files = [f for f in files if f.is_file()]

    print()
    print("╔══════════════════════════════════════════════════════════════════════════╗")
    print("║           AtomZip v10 基准测试 — 与 LZMA (7z极限) 和 gzip 对比         ║")
    print("╚══════════════════════════════════════════════════════════════════════════╝")
    print()

    all_results = []

    for filepath in files:
        name = filepath.name
        size = filepath.stat().st_size
        print(f"  ▶ {name} ({size:,} 字节)")

        # AtomZip
        try:
            az = benchmark_atomzip(str(filepath), level=9)
            status = "通过" if az['verified'] else "失败"
            print(f"    AtomZip: {az['compressed_size']:>10,} 字节 | "
                  f"比率: {az['ratio']:>6.2f}:1 | "
                  f"节省: {az['space_savings']:>5.1f}% | "
                  f"压缩: {az['comp_time']:.3f}s | 解压: {az['decomp_time']:.3f}s | "
                  f"验证: {status}")
        except Exception as e:
            az = None
            print(f"    AtomZip: 错误 - {e}")

        # LZMA (7z极限)
        try:
            lz = benchmark_lzma(str(filepath))
            status = "通过" if lz['verified'] else "失败"
            print(f"    LZMA:    {lz['compressed_size']:>10,} 字节 | "
                  f"比率: {lz['ratio']:>6.2f}:1 | "
                  f"节省: {lz['space_savings']:>5.1f}% | "
                  f"压缩: {lz['comp_time']:.3f}s | 解压: {lz['decomp_time']:.3f}s | "
                  f"验证: {status}")
        except Exception as e:
            lz = None
            print(f"    LZMA:    错误 - {e}")

        # gzip
        try:
            gz = benchmark_gzip(str(filepath))
            status = "通过" if gz['verified'] else "失败"
            print(f"    gzip:    {gz['compressed_size']:>10,} 字节 | "
                  f"比率: {gz['ratio']:>6.2f}:1 | "
                  f"节省: {gz['space_savings']:>5.1f}% | "
                  f"压缩: {gz['comp_time']:.3f}s | 解压: {gz['decomp_time']:.3f}s | "
                  f"验证: {status}")
        except Exception as e:
            gz = None
            print(f"    gzip:    错误 - {e}")

        all_results.append({
            'file': name,
            'original_size': size,
            'atomzip': az,
            'lzma': lz,
            'gzip': gz,
        })
        print()

    # === 汇总表 ===
    print()
    print("╔══════════════════════════════════════════════════════════════════════════╗")
    print("║                           汇总对比表                                    ║")
    print("╚══════════════════════════════════════════════════════════════════════════╝")
    print()

    header = (f"{'文件名':<24} {'原始':>10}  "
              f"{'AtomZip':>10} {'AZ比率':>9}  "
              f"{'LZMA':>10} {'LZMA比率':>10}  "
              f"{'gzip':>10} {'gzip比率':>10}")
    print(header)
    print("─" * len(header))

    for r in all_results:
        az = r['atomzip']
        lz = r['lzma']
        gz = r['gzip']

        az_size = f"{az['compressed_size']:>10,}" if az else "N/A"
        az_ratio = f"{az['ratio']:>8.2f}:1" if az else "N/A"
        lz_size = f"{lz['compressed_size']:>10,}" if lz else "N/A"
        lz_ratio = f"{lz['ratio']:>9.2f}:1" if lz else "N/A"
        gz_size = f"{gz['compressed_size']:>10,}" if gz else "N/A"
        gz_ratio = f"{gz['ratio']:>9.2f}:1" if gz else "N/A"

        print(f"{r['file']:<24} {r['original_size']:>10,}  "
              f"{az_size} {az_ratio}  "
              f"{lz_size} {lz_ratio}  "
              f"{gz_size} {gz_ratio}")

    # 平均值
    print("─" * len(header))
    az_ratios = [r['atomzip']['ratio'] for r in all_results if r['atomzip']]
    lz_ratios = [r['lzma']['ratio'] for r in all_results if r['lzma']]
    gz_ratios = [r['gzip']['ratio'] for r in all_results if r['gzip']]

    az_avg = f"{sum(az_ratios)/len(az_ratios):>8.2f}:1" if az_ratios else "N/A"
    lz_avg = f"{sum(lz_ratios)/len(lz_ratios):>9.2f}:1" if lz_ratios else "N/A"
    gz_avg = f"{sum(gz_ratios)/len(gz_ratios):>9.2f}:1" if gz_ratios else "N/A"

    print(f"{'平均比率':<24} {'':>10}  "
          f"{'':>10} {az_avg}  "
          f"{'':>10} {lz_avg}  "
          f"{'':>10} {gz_avg}")

    # === AtomZip vs LZMA 详细对比 ===
    print()
    print("╔══════════════════════════════════════════════════════════════════════════╗")
    print("║                  AtomZip vs LZMA (7z极限) 详细对比                     ║")
    print("╚══════════════════════════════════════════════════════════════════════════╝")
    print()

    az_wins = 0
    for r in all_results:
        az = r['atomzip']
        lz = r['lzma']
        if az and lz:
            size_diff = az['compressed_size'] - lz['compressed_size']
            ratio_pct = (az['ratio'] / lz['ratio'] - 1) * 100

            if size_diff < 0:
                verdict = f"AtomZip 更小 {abs(size_diff):,} 字节 (优 {abs(ratio_pct):.1f}%)"
                az_wins += 1
            else:
                verdict = f"LZMA 更小 {size_diff:,} 字节 (优 {abs(ratio_pct):.1f}%)"

            print(f"  {r['file']}:")
            print(f"    {verdict}")
            print(f"    压缩速度: AtomZip {az['comp_time']:.3f}s vs LZMA {lz['comp_time']:.3f}s")
            print(f"    解压速度: AtomZip {az['decomp_time']:.3f}s vs LZMA {lz['decomp_time']:.3f}s")
            print()

    print(f"  AtomZip 在 {az_wins}/{len(all_results)} 个文件上优于 LZMA")

    # === 保存JSON结果 ===
    results_json = []
    for r in all_results:
        entry = {
            'file': r['file'],
            'original_size': r['original_size'],
        }
        if r['atomzip']:
            entry['atomzip'] = {
                'compressed_size': r['atomzip']['compressed_size'],
                'ratio': round(r['atomzip']['ratio'], 2),
                'space_savings': round(r['atomzip']['space_savings'], 1),
                'comp_time': round(r['atomzip']['comp_time'], 4),
                'decomp_time': round(r['atomzip']['decomp_time'], 4),
                'verified': r['atomzip']['verified'],
            }
        if r['lzma']:
            entry['lzma'] = {
                'compressed_size': r['lzma']['compressed_size'],
                'ratio': round(r['lzma']['ratio'], 2),
                'space_savings': round(r['lzma']['space_savings'], 1),
                'comp_time': round(r['lzma']['comp_time'], 4),
                'decomp_time': round(r['lzma']['decomp_time'], 4),
                'verified': r['lzma']['verified'],
            }
        if r['gzip']:
            entry['gzip'] = {
                'compressed_size': r['gzip']['compressed_size'],
                'ratio': round(r['gzip']['ratio'], 2),
                'space_savings': round(r['gzip']['space_savings'], 1),
                'comp_time': round(r['gzip']['comp_time'], 4),
                'decomp_time': round(r['gzip']['decomp_time'], 4),
                'verified': r['gzip']['verified'],
            }
        results_json.append(entry)

    results_path = Path(__file__).parent / "benchmark_results.json"
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(results_json, f, indent=2, ensure_ascii=False)
    print(f"\n  测试结果已保存至: {results_path}")


if __name__ == '__main__':
    main()
