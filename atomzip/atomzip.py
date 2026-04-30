#!/usr/bin/env python3
"""
AtomZip — 极限压缩引擎 v6 命令行工具

原创压缩算法，基于 C加速全文件BWT + 穷举参数搜索 + 14+策略竞争。

核心创新: C语言BWT引擎 + 全文件BWT + 穷举LZMA2参数
  v6 使用C语言实现BWT变换(10MB仅需3秒)，消除大小限制。
  全文件BWT使LZMA2能捕获所有长程重复，实现超高压缩比。
  穷举LZMA2参数(lc/lp/pb)和多种预处理策略竞争选择最优结果。

用法:
  python atomzip.py compress   <输入文件> <输出文件>  [-v] [--level 级别]
  python atomzip.py decompress <输入文件> <输出文件>  [-v]
  python atomzip.py verify     <输入文件>              [-v]
  python atomzip.py benchmark  <文件或目录>            [-v]
  python atomzip.py --version
  python atomzip.py --help

跨平台: Windows / Linux / macOS (需要 Python 3.7+)
"""

import sys
import os
import argparse
import time
import struct
from pathlib import Path

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from codec.compress_v10 import AtomZipCompressor
from codec.decompress_v10 import AtomZipDecompressor

__version__ = "10.0.0"
__author__ = "AtomZip Project"


def compress_file(input_path: str, output_path: str, level: int = 5,
                  verbose: bool = False) -> dict:
    """
    压缩单个文件。

    参数:
        input_path:  输入文件路径
        output_path: 输出文件路径
        level:       压缩级别 (1-9，越高压缩率越好但越慢)
        verbose:     是否显示详细信息

    返回:
        包含压缩统计信息的字典
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        raise FileNotFoundError(f"输入文件不存在: {input_path}")

    if verbose:
        print(f"╔══════════════════════════════════════════╗")
        print(f"║        AtomZip v{__version__} 极限压缩引擎       ║")
        print(f"╚══════════════════════════════════════════╝")
        print(f"  输入文件: {input_path}")
        print(f"  输出文件: {output_path}")
        print(f"  压缩级别: {level}")
        print()

    # 读取输入文件
    with open(input_path, 'rb') as f:
        data = f.read()

    original_size = len(data)

    # 压缩
    compressor = AtomZipCompressor(level=level, verbose=verbose)
    start_time = time.time()
    compressed = compressor.compress(data)
    elapsed = time.time() - start_time

    # 写入输出文件
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
        'space_savings': 100 * (1 - compressed_size / max(1, original_size)),
        'time': elapsed,
        'speed_kbs': speed
    }

    if verbose:
        print()
        print(f"┌──────────── 压缩结果 ────────────┐")
        print(f"│  原始大小:   {original_size:>12,} 字节  │")
        print(f"│  压缩大小:   {compressed_size:>12,} 字节  │")
        print(f"│  压缩比率:   {ratio:>12.2f} :1   │")
        print(f"│  空间节省:   {stats['space_savings']:>11.1f} %    │")
        print(f"│  耗时:       {elapsed:>12.3f} 秒    │")
        print(f"│  速度:       {speed:>10.1f} KB/s  │")
        print(f"└──────────────────────────────────┘")

    return stats


def decompress_file(input_path: str, output_path: str,
                    verbose: bool = False) -> dict:
    """
    解压单个文件。

    参数:
        input_path:  压缩文件路径
        output_path: 解压输出路径
        verbose:     是否显示详细信息

    返回:
        包含解压统计信息的字典
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        raise FileNotFoundError(f"输入文件不存在: {input_path}")

    if verbose:
        print(f"╔══════════════════════════════════════════╗")
        print(f"║        AtomZip v{__version__} 解压引擎          ║")
        print(f"╚══════════════════════════════════════════╝")
        print(f"  输入文件: {input_path}")
        print(f"  输出文件: {output_path}")
        print()

    # 读取压缩数据
    with open(input_path, 'rb') as f:
        data = f.read()

    # 解压
    decompressor = AtomZipDecompressor(verbose=verbose)
    start_time = time.time()
    decompressed = decompressor.decompress(data)
    elapsed = time.time() - start_time

    # 写入输出文件
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
        print()
        print(f"┌──────────── 解压结果 ────────────┐")
        print(f"│  压缩大小:   {len(data):>12,} 字节  │")
        print(f"│  解压大小:   {len(decompressed):>12,} 字节  │")
        print(f"│  耗时:       {elapsed:>12.3f} 秒    │")
        print(f"│  速度:       {speed:>10.1f} KB/s  │")
        print(f"└──────────────────────────────────┘")

    return stats


def verify_roundtrip(input_path: str, verbose: bool = False) -> bool:
    """验证压缩-解压往返正确性。"""
    import tempfile

    input_path = Path(input_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        compressed = os.path.join(tmpdir, 'test.azip')
        decompressed = os.path.join(tmpdir, 'test.out')

        compress_file(str(input_path), compressed, level=9, verbose=False)
        decompress_file(compressed, decompressed, verbose=False)

        with open(input_path, 'rb') as f:
            original = f.read()
        with open(decompressed, 'rb') as f:
            result = f.read()

        match = original == result
        if verbose:
            if match:
                print(f"  [通过] 往返验证成功 - {len(original):,} 字节完全一致")
            else:
                print(f"  [失败] 往返验证失败!")
                print(f"    原始大小: {len(original)} 字节")
                print(f"    结果大小: {len(result)} 字节")
                for i in range(min(len(original), len(result))):
                    if original[i] != result[i]:
                        print(f"    首个差异位置: 字节 {i} "
                              f"(期望 {original[i]:#x}, 实际 {result[i]:#x})")
                        break

        return match


def run_benchmark(input_path: str, verbose: bool = False):
    """运行基准测试，对比 AtomZip 与 LZMA、gzip。"""
    import tempfile
    import gzip
    import lzma

    path = Path(input_path)
    files = []

    if path.is_file():
        files.append(path)
    elif path.is_dir():
        for f in sorted(path.rglob('*')):
            if f.is_file() and f.stat().st_size > 0:
                files.append(f)
    else:
        print(f"错误: 找不到 {input_path}", file=sys.stderr)
        return

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║              AtomZip v5 基准测试 - 多算法对比                ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print(f"  测试文件数: {len(files)}")
    print()

    results = []

    for f in files:
        size = f.stat().st_size
        print(f"  ▶ {f.name} ({size:,} 字节)")

        with open(f, 'rb') as fh:
            original_data = fh.read()

        with tempfile.TemporaryDirectory() as tmpdir:
            # === AtomZip 测试 ===
            try:
                azip_out = os.path.join(tmpdir, 'test.azip')
                azip_dec = os.path.join(tmpdir, 'test.dec')

                start = time.time()
                compress_file(str(f), azip_out, level=9, verbose=False)
                azip_comp_time = time.time() - start

                start = time.time()
                decompress_file(azip_out, azip_dec, verbose=False)
                azip_decomp_time = time.time() - start

                azip_comp_size = os.path.getsize(azip_out)
                azip_ratio = size / max(1, azip_comp_size)

                with open(azip_dec, 'rb') as fh:
                    decoded = fh.read()
                azip_verified = original_data == decoded

            except Exception as e:
                print(f"    AtomZip: 失败 ({e})")
                azip_comp_size = size
                azip_ratio = 1.0
                azip_comp_time = 0
                azip_decomp_time = 0
                azip_verified = False

            # === LZMA 测试 ===
            try:
                start = time.time()
                lzma_data = lzma.compress(
                    original_data,
                    format=lzma.FORMAT_XZ,
                    filters=[{'id': lzma.FILTER_LZMA2,
                              'preset': 9 | lzma.PRESET_EXTREME}]
                )
                lzma_comp_time = time.time() - start

                start = time.time()
                lzma_dec = lzma.decompress(lzma_data)
                lzma_decomp_time = time.time() - start

                lzma_comp_size = len(lzma_data)
                lzma_ratio = size / max(1, lzma_comp_size)
                lzma_verified = original_data == lzma_dec

            except Exception as e:
                print(f"    LZMA: 失败 ({e})")
                lzma_comp_size = size
                lzma_ratio = 1.0
                lzma_comp_time = 0
                lzma_decomp_time = 0
                lzma_verified = False

            # === gzip 测试 ===
            try:
                start = time.time()
                gzip_data = gzip.compress(original_data, compresslevel=9)
                gzip_comp_time = time.time() - start

                start = time.time()
                gzip_dec = gzip.decompress(gzip_data)
                gzip_decomp_time = time.time() - start

                gzip_comp_size = len(gzip_data)
                gzip_ratio = size / max(1, gzip_comp_size)
                gzip_verified = original_data == gzip_dec

            except Exception as e:
                print(f"    gzip: 失败 ({e})")
                gzip_comp_size = size
                gzip_ratio = 1.0
                gzip_comp_time = 0
                gzip_decomp_time = 0
                gzip_verified = False

            # 打印结果
            def verdict(verified):
                return "通过" if verified else "失败"

            print(f"    AtomZip: {azip_comp_size:>10,} 字节 | "
                  f"比率: {azip_ratio:>6.2f}:1 | "
                  f"压缩: {azip_comp_time:.3f}s | "
                  f"解压: {azip_decomp_time:.3f}s | "
                  f"验证: {verdict(azip_verified)}")
            print(f"    LZMA:    {lzma_comp_size:>10,} 字节 | "
                  f"比率: {lzma_ratio:>6.2f}:1 | "
                  f"压缩: {lzma_comp_time:.3f}s | "
                  f"解压: {lzma_decomp_time:.3f}s | "
                  f"验证: {verdict(lzma_verified)}")
            print(f"    gzip:    {gzip_comp_size:>10,} 字节 | "
                  f"比率: {gzip_ratio:>6.2f}:1 | "
                  f"压缩: {gzip_comp_time:.3f}s | "
                  f"解压: {gzip_decomp_time:.3f}s | "
                  f"验证: {verdict(gzip_verified)}")

            # AtomZip vs LZMA 对比
            if azip_comp_size < lzma_comp_size:
                diff = lzma_comp_size - azip_comp_size
                print(f"    >>> AtomZip 比 LZMA 小 {diff:,} 字节 "
                      f"({(1 - azip_comp_size/lzma_comp_size)*100:.1f}%)")
            else:
                diff = azip_comp_size - lzma_comp_size
                print(f"    >>> LZMA 比 AtomZip 小 {diff:,} 字节 "
                      f"({(1 - lzma_comp_size/azip_comp_size)*100:.1f}%)")

            results.append({
                'file': f.name,
                'original_size': size,
                'azip_size': azip_comp_size,
                'azip_ratio': azip_ratio,
                'azip_comp_time': azip_comp_time,
                'azip_decomp_time': azip_decomp_time,
                'azip_verified': azip_verified,
                'lzma_size': lzma_comp_size,
                'lzma_ratio': lzma_ratio,
                'lzma_comp_time': lzma_comp_time,
                'lzma_decomp_time': lzma_decomp_time,
                'lzma_verified': lzma_verified,
                'gzip_size': gzip_comp_size,
                'gzip_ratio': gzip_ratio,
                'gzip_comp_time': gzip_comp_time,
                'gzip_decomp_time': gzip_decomp_time,
                'gzip_verified': gzip_verified,
            })
            print()

    # === 汇总表 ===
    if results:
        print()
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║                        汇总对比表                           ║")
        print("╚══════════════════════════════════════════════════════════════╝")
        print()

        header = (f"{'文件名':<22} {'原始大小':>10}  "
                  f"{'AtomZip':>10} {'AZ比率':>8}  "
                  f"{'LZMA':>10} {'LZMA比率':>9}  "
                  f"{'gzip':>10} {'gzip比率':>9}")
        print(header)
        print("─" * len(header))

        for r in results:
            print(f"{r['file']:<22} {r['original_size']:>10,}  "
                  f"{r['azip_size']:>10,} {r['azip_ratio']:>7.2f}:1  "
                  f"{r['lzma_size']:>10,} {r['lzma_ratio']:>8.2f}:1  "
                  f"{r['gzip_size']:>10,} {r['gzip_ratio']:>8.2f}:1")

        # 平均值
        print("─" * len(header))
        az_avg = sum(r['azip_ratio'] for r in results) / len(results)
        lz_avg = sum(r['lzma_ratio'] for r in results) / len(results)
        gz_avg = sum(r['gzip_ratio'] for r in results) / len(results)
        print(f"{'平均比率':<22} {'':>10}  "
              f"{'':>10} {az_avg:>7.2f}:1  "
              f"{'':>10} {lz_avg:>8.2f}:1  "
              f"{'':>10} {gz_avg:>8.2f}:1")

        # AtomZip 胜出的文件数
        az_wins = sum(1 for r in results if r['azip_size'] < r['lzma_size'])
        print(f"\n  AtomZip 在 {az_wins}/{len(results)} 个文件上优于 LZMA")


def main():
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        prog='atomzip',
        description='AtomZip — 极限压缩引擎 v10 (Mega字典 + 增强结构提取 + 多策略竞争)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  atomzip compress   文档.txt 文档.azip               压缩文件
  atomzip compress   数据.bin 数据.azip --level 9     最高压缩率
  atomzip decompress 文档.azip 文档.txt               解压文件
  atomzip verify     文档.txt                          验证往返正确性
  atomzip benchmark  ./tests/test_files                基准测试

压缩级别 (1-9):
  1-3: 快速压缩 (仅 LZMA2)
  4-6: 均衡压缩 (LZMA2 + Delta + BWT)
  7-9: 极限压缩 (14种策略竞争+穷举参数，自动选择最优结果)

v10 算法核心创新:
  Mega静态字典 + 增强结构提取 + 多策略竞争
  - 250个高频英文词1字节编码，256个次高频词2字节编码
  - 增强JSON列式编码（枚举位压缩、日期delta、浮点4/8字节自适应）
  - 快速CSV列式编码（智能类型检测、枚举位压缩）
  - 行级去重（日志/CSV重复行替换为引用）
  - 30+种压缩策略竞争，自动选择最小输出
  - 穷举 LZMA2 参数组合 (lc/lp/pb)
"""
    )

    parser.add_argument('--version', action='version',
                        version=f'AtomZip v{__version__}')

    subparsers = parser.add_subparsers(dest='command', help='执行命令')

    # 压缩命令
    comp_parser = subparsers.add_parser('compress', help='压缩文件')
    comp_parser.add_argument('input', help='输入文件路径')
    comp_parser.add_argument('output', help='输出文件路径 (.azip)')
    comp_parser.add_argument('-l', '--level', type=int, default=5,
                             choices=range(1, 10),
                             help='压缩级别 (1-9，默认: 5)')
    comp_parser.add_argument('-v', '--verbose', action='store_true',
                             help='显示详细信息')

    # 解压命令
    decomp_parser = subparsers.add_parser('decompress', help='解压文件')
    decomp_parser.add_argument('input', help='压缩文件路径 (.azip)')
    decomp_parser.add_argument('output', help='解压输出路径')
    decomp_parser.add_argument('-v', '--verbose', action='store_true',
                               help='显示详细信息')

    # 验证命令
    verify_parser = subparsers.add_parser('verify', help='验证往返正确性')
    verify_parser.add_argument('input', help='要验证的文件路径')
    verify_parser.add_argument('-v', '--verbose', action='store_true',
                               help='显示详细信息')

    # 基准测试命令
    bench_parser = subparsers.add_parser('benchmark', help='运行基准测试')
    bench_parser.add_argument('input', help='输入文件或目录')
    bench_parser.add_argument('-v', '--verbose', action='store_true',
                              help='显示详细信息')

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == 'compress':
            stats = compress_file(args.input, args.output, args.level,
                                  args.verbose)
            if not args.verbose:
                print(f"压缩完成: {stats['original_size']:,} -> "
                      f"{stats['compressed_size']:,} 字节 "
                      f"(比率: {stats['ratio']:.2f}:1, "
                      f"节省: {stats['space_savings']:.1f}%)")

        elif args.command == 'decompress':
            stats = decompress_file(args.input, args.output, args.verbose)
            if not args.verbose:
                print(f"解压完成: {stats['compressed_size']:,} -> "
                      f"{stats['decompressed_size']:,} 字节")

        elif args.command == 'verify':
            result = verify_roundtrip(args.input, args.verbose)
            if not args.verbose:
                status = "通过" if result else "失败"
                print(f"往返验证: {status}")
            if not result:
                sys.exit(1)

        elif args.command == 'benchmark':
            run_benchmark(args.input, args.verbose)

    except FileNotFoundError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
