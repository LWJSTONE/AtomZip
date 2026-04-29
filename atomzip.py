#!/usr/bin/env python3
"""
AtomZip — Recursive Entropic Pattern Collapse (REPC) Compression Algorithm

A novel compression algorithm that uses information-entropy-gain-based pattern
selection combined with multi-order context modeling for high compression ratios.

Usage:
  python atomzip.py compress   <input_file> <output_file>  [-v] [--level LEVEL]
  python atomzip.py decompress <input_file> <output_file>  [-v]
  python atomzip.py benchmark  <input_file_or_dir>         [-v]
  python atomzip.py --version
  python atomzip.py --help

Cross-platform: Works on Windows, Linux, and macOS (requires Python 3.7+)
"""

import sys
import os
import argparse
import time
import struct
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from codec.compress import AtomZipCompressor
from codec.decompress import AtomZipDecompressor


__version__ = "1.0.0"
__author__ = "AtomZip Project"


def compress_file(input_path: str, output_path: str, level: int = 5,
                  verbose: bool = False) -> dict:
    """
    Compress a single file.

    Args:
        input_path: Path to input file
        output_path: Path to output compressed file
        level: Compression level (1-9, higher = more compression but slower)
        verbose: Print progress information

    Returns:
        Dictionary with compression statistics
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if verbose:
        print(f"Compressing: {input_path}")
        print(f"Output: {output_path}")

    # Read input
    with open(input_path, 'rb') as f:
        data = f.read()

    original_size = len(data)
    if original_size == 0:
        # Write empty compressed file
        with open(output_path, 'wb') as f:
            f.write(b'AZIP')
            f.write(struct.pack('>B', 1))
            f.write(struct.pack('>Q', 0))
            f.write(struct.pack('>I', 0))
        return {'original_size': 0, 'compressed_size': 17, 'ratio': 0, 'time': 0}

    # Configure compression level
    max_rules = {1: 50, 2: 100, 3: 150, 4: 200, 5: 256,
                 6: 300, 7: 400, 8: 500, 9: 600}.get(level, 256)
    context_order = min(level, 6)

    # Compress
    compressor = AtomZipCompressor(
        max_pattern_rules=max_rules,
        context_order=context_order,
        verbose=verbose
    )

    start_time = time.time()
    compressed = compressor.compress(data)
    elapsed = time.time() - start_time

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'wb') as f:
        f.write(compressed)

    compressed_size = len(compressed)
    ratio = original_size / max(1, compressed_size)
    speed = original_size / max(0.001, elapsed) / 1024  # KB/s

    stats = {
        'original_size': original_size,
        'compressed_size': compressed_size,
        'ratio': ratio,
        'space_savings': 100 * (1 - compressed_size / original_size),
        'time': elapsed,
        'speed_kbs': speed
    }

    if verbose:
        print(f"\nCompression Results:")
        print(f"  Original size:     {original_size:,} bytes")
        print(f"  Compressed size:   {compressed_size:,} bytes")
        print(f"  Compression ratio: {ratio:.2f}:1")
        print(f"  Space savings:     {stats['space_savings']:.1f}%")
        print(f"  Time:              {elapsed:.3f}s")
        print(f"  Speed:             {speed:.1f} KB/s")

    return stats


def decompress_file(input_path: str, output_path: str,
                    verbose: bool = False) -> dict:
    """
    Decompress a single file.

    Args:
        input_path: Path to compressed file
        output_path: Path to output decompressed file
        verbose: Print progress information

    Returns:
        Dictionary with decompression statistics
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if verbose:
        print(f"Decompressing: {input_path}")
        print(f"Output: {output_path}")

    # Read compressed data
    with open(input_path, 'rb') as f:
        data = f.read()

    # Decompress
    decompressor = AtomZipDecompressor(verbose=verbose)
    start_time = time.time()
    decompressed = decompressor.decompress(data)
    elapsed = time.time() - start_time

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'wb') as f:
        f.write(decompressed)

    speed = len(decompressed) / max(0.001, elapsed) / 1024  # KB/s

    stats = {
        'compressed_size': len(data),
        'decompressed_size': len(decompressed),
        'time': elapsed,
        'speed_kbs': speed
    }

    if verbose:
        print(f"\nDecompression Results:")
        print(f"  Compressed size:     {len(data):,} bytes")
        print(f"  Decompressed size:   {len(decompressed):,} bytes")
        print(f"  Time:                {elapsed:.3f}s")
        print(f"  Speed:               {speed:.1f} KB/s")

    return stats


def verify_roundtrip(input_path: str, verbose: bool = False) -> bool:
    """Verify compression-decompression roundtrip correctness."""
    import tempfile

    input_path = Path(input_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        compressed = os.path.join(tmpdir, 'test.azip')
        decompressed = os.path.join(tmpdir, 'test.out')

        compress_file(str(input_path), compressed, level=5, verbose=verbose)
        decompress_file(compressed, decompressed, verbose=verbose)

        with open(input_path, 'rb') as f:
            original = f.read()
        with open(decompressed, 'rb') as f:
            result = f.read()

        match = original == result
        if verbose:
            if match:
                print("  ✓ Roundtrip verification PASSED")
            else:
                print(f"  ✗ Roundtrip verification FAILED!")
                print(f"    Original: {len(original)} bytes")
                print(f"    Result:   {len(result)} bytes")
                # Find first difference
                for i in range(min(len(original), len(result))):
                    if original[i] != result[i]:
                        print(f"    First diff at byte {i}: "
                              f"expected {original[i]:#x}, got {result[i]:#x}")
                        break

        return match


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog='atomzip',
        description='AtomZip — Recursive Entropic Pattern Collapse Compression',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  atomzip compress   document.txt document.azip        Compress a file
  atomzip compress   data.bin data.azip --level 9     Maximum compression
  atomzip decompress document.azip document.txt       Decompress a file
  atomzip verify     document.txt                     Verify roundtrip

Compression Levels (1-9):
  1-3: Fast compression, moderate ratio
  4-6: Balanced (default: 5)
  7-9: Maximum compression, slower

Algorithm: Recursive Entropic Pattern Collapse (REPC)
  Unlike traditional BPE which selects patterns by frequency alone,
  REPC uses an information-entropy-gain criterion that considers both
  frequency AND context diversity for optimal pattern selection.
"""
    )

    parser.add_argument('--version', action='version',
                       version=f'AtomZip v{__version__}')

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Compress command
    comp_parser = subparsers.add_parser('compress', help='Compress a file')
    comp_parser.add_argument('input', help='Input file path')
    comp_parser.add_argument('output', help='Output file path (.azip)')
    comp_parser.add_argument('-l', '--level', type=int, default=5,
                            choices=range(1, 10),
                            help='Compression level (1-9, default: 5)')
    comp_parser.add_argument('-v', '--verbose', action='store_true',
                            help='Print detailed progress')

    # Decompress command
    decomp_parser = subparsers.add_parser('decompress', help='Decompress a file')
    decomp_parser.add_argument('input', help='Input compressed file (.azip)')
    decomp_parser.add_argument('output', help='Output decompressed file')
    decomp_parser.add_argument('-v', '--verbose', action='store_true',
                              help='Print detailed progress')

    # Verify command
    verify_parser = subparsers.add_parser('verify', help='Verify roundtrip correctness')
    verify_parser.add_argument('input', help='Input file to test')
    verify_parser.add_argument('-v', '--verbose', action='store_true',
                              help='Print detailed progress')

    # Benchmark command
    bench_parser = subparsers.add_parser('benchmark', help='Run benchmark')
    bench_parser.add_argument('input', help='Input file or directory')
    bench_parser.add_argument('-v', '--verbose', action='store_true',
                             help='Print detailed progress')

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == 'compress':
            stats = compress_file(args.input, args.output, args.level, args.verbose)
            if not args.verbose:
                print(f"Compressed: {stats['original_size']:,} -> "
                      f"{stats['compressed_size']:,} bytes "
                      f"({stats['ratio']:.2f}:1, {stats['space_savings']:.1f}% savings)")

        elif args.command == 'decompress':
            stats = decompress_file(args.input, args.output, args.verbose)
            if not args.verbose:
                print(f"Decompressed: {stats['compressed_size']:,} -> "
                      f"{stats['decompressed_size']:,} bytes")

        elif args.command == 'verify':
            result = verify_roundtrip(args.input, args.verbose)
            if not result:
                sys.exit(1)

        elif args.command == 'benchmark':
            run_benchmark(args.input, args.verbose)

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def run_benchmark(input_path: str, verbose: bool = False):
    """Run benchmark comparing AtomZip with other methods."""
    import tempfile
    import subprocess

    path = Path(input_path)
    files = []

    if path.is_file():
        files.append(path)
    elif path.is_dir():
        for f in sorted(path.rglob('*')):
            if f.is_file() and f.stat().st_size > 0:
                files.append(f)
    else:
        print(f"Error: {input_path} not found", file=sys.stderr)
        return

    print(f"\n{'='*80}")
    print(f"AtomZip Benchmark — Testing {len(files)} file(s)")
    print(f"{'='*80}\n")

    results = []

    for f in files:
        size = f.stat().st_size
        print(f"File: {f.name} ({size:,} bytes)")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Test AtomZip
            azip_out = os.path.join(tmpdir, 'test.azip')
            azip_dec = os.path.join(tmpdir, 'test.dec')

            try:
                start = time.time()
                stats = compress_file(str(f), azip_out, level=9, verbose=False)
                comp_time = time.time() - start

                start = time.time()
                decompress_file(azip_out, azip_dec, verbose=False)
                decomp_time = time.time() - start

                azip_ratio = stats['ratio']
                azip_comp_size = stats['compressed_size']

                # Verify correctness
                with open(f, 'rb') as fh:
                    original = fh.read()
                with open(azip_dec, 'rb') as fh:
                    decoded = fh.read()
                verified = original == decoded

            except Exception as e:
                print(f"  AtomZip: FAILED ({e})")
                continue

            # Test 7z if available
            z7_out = os.path.join(tmpdir, 'test.7z')
            z7_ratio = 0
            z7_comp_size = 0
            z7_comp_time = 0
            z7_decomp_time = 0
            z7_available = False

            try:
                # Compress with 7z (ultra compression)
                start = time.time()
                subprocess.run(
                    ['7z', 'a', '-t7z', '-m0=lzma2', '-mx=9', '-mfb=64',
                     '-md=32m', '-ms=on', z7_out, str(f)],
                    capture_output=True, timeout=60
                )
                z7_comp_time = time.time() - start
                z7_comp_size = os.path.getsize(z7_out)
                z7_ratio = size / max(1, z7_comp_size)
                z7_available = True

                # Decompress with 7z
                start = time.time()
                subprocess.run(
                    ['7z', 'e', z7_out, f'-o{tmpdir}', '-aoa'],
                    capture_output=True, timeout=60
                )
                z7_decomp_time = time.time() - start

            except (FileNotFoundError, subprocess.TimeoutExpired):
                z7_available = False
            except Exception:
                z7_available = False

            # Print results
            print(f"  AtomZip: {azip_comp_size:,} bytes ({azip_ratio:.2f}:1) "
                  f"comp={comp_time:.3f}s decomp={decomp_time:.3f}s "
                  f"{'✓' if verified else '✗'}")

            if z7_available:
                print(f"  7z:      {z7_comp_size:,} bytes ({z7_ratio:.2f}:1) "
                      f"comp={z7_comp_time:.3f}s decomp={z7_decomp_time:.3f}s")
            else:
                print(f"  7z:      Not available")

            results.append({
                'file': f.name,
                'original_size': size,
                'azip_size': azip_comp_size,
                'azip_ratio': azip_ratio,
                'azip_comp_time': comp_time,
                'azip_decomp_time': decomp_time,
                'azip_verified': verified,
                'z7_size': z7_comp_size,
                'z7_ratio': z7_ratio,
                'z7_comp_time': z7_comp_time,
                'z7_decomp_time': z7_decomp_time,
                'z7_available': z7_available,
            })
            print()

    # Summary
    if results:
        print(f"\n{'='*80}")
        print("Summary")
        print(f"{'='*80}\n")

        print(f"{'File':<25} {'Original':>12} {'AtomZip':>10} {'AZ Ratio':>10} "
              f"{'7z Size':>10} {'7z Ratio':>10} {'AZ/7z':>8}")
        print("-" * 95)

        for r in results:
            z7_str = f"{r['z7_size']:>10,}" if r['z7_available'] else "N/A"
            z7_ratio_str = f"{r['z7_ratio']:>9.2f}:1" if r['z7_available'] else "N/A"
            az_7z = f"{r['azip_size']/max(1,r['z7_size']):>7.2f}" if r['z7_available'] else "N/A"

            print(f"{r['file']:<25} {r['original_size']:>12,} "
                  f"{r['azip_size']:>10,} {r['azip_ratio']:>9.2f}:1 "
                  f"{z7_str} {z7_ratio_str} {az_7z}")


if __name__ == '__main__':
    main()
