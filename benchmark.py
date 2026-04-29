#!/usr/bin/env python3
"""
AtomZip Benchmark — Compare with 7z/LZMA extreme compression.

Tests multiple file types: text, binary, structured data, source code, JSON, logs.
Compares compression ratio, compression time, and decompression time.
"""

import sys
import os
import time
import json
import lzma
import tempfile
import shutil
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from codec.compress import AtomZipCompressor
from codec.decompress import AtomZipDecompressor


def benchmark_atomzip(filepath: str, level: int = 9) -> dict:
    """Benchmark AtomZip compression and decompression."""
    with open(filepath, 'rb') as f:
        data = f.read()

    original_size = len(data)

    # Configure based on level
    max_rules = {1: 50, 2: 100, 3: 150, 4: 200, 5: 256,
                 6: 300, 7: 400, 8: 500, 9: 600}.get(level, 600)

    compressor = AtomZipCompressor(max_pattern_rules=max_rules, verbose=False)

    # Compression
    start = time.time()
    compressed = compressor.compress(data)
    comp_time = time.time() - start

    # Decompression
    decompressor = AtomZipDecompressor(verbose=False)
    start = time.time()
    decompressed = decompressor.decompress(compressed)
    decomp_time = time.time() - start

    verified = data == decompressed

    return {
        'original_size': original_size,
        'compressed_size': len(compressed),
        'ratio': original_size / max(1, len(compressed)),
        'space_savings': 100 * (1 - len(compressed) / original_size),
        'comp_time': comp_time,
        'decomp_time': decomp_time,
        'verified': verified,
    }


def benchmark_lzma(filepath: str) -> dict:
    """Benchmark LZMA/7z extreme compression using Python's lzma module."""
    with open(filepath, 'rb') as f:
        data = f.read()

    original_size = len(data)

    # LZMA with extreme settings (equivalent to 7z ultra)
    # FORMAT_XZ supports LZMA2 filter; FORMAT_ALONE only supports LZMA1
    filters = [
        {'id': lzma.FILTER_LZMA2,
         'preset': 9 | lzma.PRESET_EXTREME}
    ]

    # Compression
    start = time.time()
    compressed = lzma.compress(data, format=lzma.FORMAT_XZ, filters=filters)
    comp_time = time.time() - start

    # Decompression
    start = time.time()
    decompressed = lzma.decompress(compressed)
    decomp_time = time.time() - start

    verified = data == decompressed

    return {
        'original_size': original_size,
        'compressed_size': len(compressed),
        'ratio': original_size / max(1, len(compressed)),
        'space_savings': 100 * (1 - len(compressed) / original_size),
        'comp_time': comp_time,
        'decomp_time': decomp_time,
        'verified': verified,
    }


def benchmark_gzip(filepath: str) -> dict:
    """Benchmark gzip compression."""
    import gzip
    with open(filepath, 'rb') as f:
        data = f.read()

    original_size = len(data)

    # Compression
    start = time.time()
    compressed = gzip.compress(data, compresslevel=9)
    comp_time = time.time() - start

    # Decompression
    start = time.time()
    decompressed = gzip.decompress(compressed)
    decomp_time = time.time() - start

    verified = data == decompressed

    return {
        'original_size': original_size,
        'compressed_size': len(compressed),
        'ratio': original_size / max(1, len(compressed)),
        'space_savings': 100 * (1 - len(compressed) / original_size),
        'comp_time': comp_time,
        'decomp_time': decomp_time,
        'verified': verified,
    }


def main():
    test_dir = Path(__file__).parent / "tests" / "test_files"

    if not test_dir.exists():
        print(f"Test directory not found: {test_dir}")
        sys.exit(1)

    files = sorted(test_dir.iterdir())
    files = [f for f in files if f.is_file()]

    print("=" * 100)
    print("AtomZip Benchmark — Comparison with LZMA (7z extreme) and gzip (best)")
    print("=" * 100)
    print()

    all_results = []

    for filepath in files:
        name = filepath.name
        size = filepath.stat().st_size
        print(f"--- {name} ({size:,} bytes) ---")

        # AtomZip
        try:
            az = benchmark_atomzip(str(filepath), level=9)
            print(f"  AtomZip: {az['compressed_size']:>10,} bytes | "
                  f"Ratio: {az['ratio']:>6.2f}:1 | "
                  f"Savings: {az['space_savings']:>5.1f}% | "
                  f"Comp: {az['comp_time']:.3f}s | Decomp: {az['decomp_time']:.3f}s | "
                  f"{'OK' if az['verified'] else 'FAIL'}")
        except Exception as e:
            az = None
            print(f"  AtomZip: ERROR - {e}")

        # LZMA (7z extreme equivalent)
        try:
            lz = benchmark_lzma(str(filepath))
            print(f"  LZMA:    {lz['compressed_size']:>10,} bytes | "
                  f"Ratio: {lz['ratio']:>6.2f}:1 | "
                  f"Savings: {lz['space_savings']:>5.1f}% | "
                  f"Comp: {lz['comp_time']:.3f}s | Decomp: {lz['decomp_time']:.3f}s | "
                  f"{'OK' if lz['verified'] else 'FAIL'}")
        except Exception as e:
            lz = None
            print(f"  LZMA:    ERROR - {e}")

        # gzip
        try:
            gz = benchmark_gzip(str(filepath))
            print(f"  gzip:    {gz['compressed_size']:>10,} bytes | "
                  f"Ratio: {gz['ratio']:>6.2f}:1 | "
                  f"Savings: {gz['space_savings']:>5.1f}% | "
                  f"Comp: {gz['comp_time']:.3f}s | Decomp: {gz['decomp_time']:.3f}s | "
                  f"{'OK' if gz['verified'] else 'FAIL'}")
        except Exception as e:
            gz = None
            print(f"  gzip:    ERROR - {e}")

        all_results.append({
            'file': name,
            'original_size': size,
            'atomzip': az,
            'lzma': lz,
            'gzip': gz,
        })
        print()

    # Summary table
    print("\n" + "=" * 100)
    print("COMPARISON SUMMARY")
    print("=" * 100)
    print()

    # Table header
    print(f"{'File':<25} {'Original':>10}  {'AtomZip':>10} {'AZ Ratio':>9}  "
          f"{'LZMA':>10} {'LZMA Ratio':>10}  "
          f"{'gzip':>10} {'gzip Ratio':>10}")
    print("-" * 110)

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

        print(f"{r['file']:<25} {r['original_size']:>10,}  "
              f"{az_size} {az_ratio}  "
              f"{lz_size} {lz_ratio}  "
              f"{gz_size} {gz_ratio}")

    # Averages
    print("\n" + "-" * 110)
    az_ratios = [r['atomzip']['ratio'] for r in all_results if r['atomzip']]
    lz_ratios = [r['lzma']['ratio'] for r in all_results if r['lzma']]
    gz_ratios = [r['gzip']['ratio'] for r in all_results if r['gzip']]

    az_avg = f"{sum(az_ratios)/len(az_ratios):>8.2f}:1" if az_ratios else "N/A"
    lz_avg = f"{sum(lz_ratios)/len(lz_ratios):>9.2f}:1" if lz_ratios else "N/A"
    gz_avg = f"{sum(gz_ratios)/len(gz_ratios):>9.2f}:1" if gz_ratios else "N/A"

    print(f"{'Average Ratio':<25} {'':>10}  {'':>10} {az_avg}  "
          f"{'':>10} {lz_avg}  "
          f"{'':>10} {gz_avg}")

    # AtomZip vs LZMA comparison
    print("\n" + "=" * 100)
    print("ATOMZIP vs LZMA (7z Extreme) — DETAILED COMPARISON")
    print("=" * 100)
    print()

    for r in all_results:
        az = r['atomzip']
        lz = r['lzma']
        if az and lz:
            size_diff = az['compressed_size'] - lz['compressed_size']
            ratio_pct = (az['ratio'] / lz['ratio'] - 1) * 100
            speed_comp = az['comp_time'] / max(0.001, lz['comp_time'])
            speed_decomp = az['decomp_time'] / max(0.001, lz['decomp_time'])

            if size_diff < 0:
                verdict = f"AtomZip SMALLER by {abs(size_diff):,} bytes ({abs(ratio_pct):.1f}% better)"
            else:
                verdict = f"LZMA smaller by {size_diff:,} bytes ({abs(ratio_pct):.1f}%)"

            print(f"  {r['file']}:")
            print(f"    {verdict}")
            print(f"    Comp speed: AtomZip {az['comp_time']:.3f}s vs LZMA {lz['comp_time']:.3f}s "
                  f"({speed_comp:.1f}x)")
            print(f"    Decomp speed: AtomZip {az['decomp_time']:.3f}s vs LZMA {lz['decomp_time']:.3f}s "
                  f"({speed_decomp:.1f}x)")
            print()

    # Save results as JSON for report generation
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
    with open(results_path, 'w') as f:
        json.dump(results_json, f, indent=2)
    print(f"\nBenchmark results saved to: {results_path}")


if __name__ == '__main__':
    main()
