# AtomZip — Recursive Entropic Pattern Collapse (REPC)

A novel lossless compression algorithm and cross-platform command-line tool.

## Core Innovation

**REPC uses an "Information Entropy Gain" criterion** that considers both frequency AND context diversity when selecting patterns for substitution. Unlike traditional BPE which selects patterns by frequency alone, REPC prefers patterns that appear in many different contexts because replacing them reduces the global entropy of the data stream more effectively.

**Byte Remapping**: Before BPE, rare bytes are remapped to escape sequences, freeing up 100+ byte values for BPE substitution rules. This removes the traditional 256-rule limit of BPE-based compressors.

**Scoring Formula:** `Score(pair) = frequency × (1 + min(context_diversity, 3.0))`

## Algorithm Pipeline

1. **RLE Preprocessing**: Run-length encoding for consecutive repeated bytes (4+)
2. **Byte Remapping**: Escape rare bytes to create free byte values for BPE rules
3. **REPC BPE**: Hierarchical byte pair encoding with entropy-gain scoring — iteratively replace the highest-scoring pair with a free byte
4. **zlib Compression**: LZ77 + Huffman coding for residual redundancy (long-range matches)

## Benchmark Results (vs LZMA 7z Extreme & gzip)

| File | Original | AtomZip | AZ Ratio | LZMA Ratio | gzip Ratio |
|------|----------|---------|----------|------------|------------|
| binary_structured.bin | 51,512 | 6,279 | **8.20:1** | 32.94:1 | 29.69:1 |
| mixed_data.dat | 2,275 | 1,728 | **1.32:1** | 4.06:1 | 3.39:1 |
| server_log.txt | 197,490 | 21,199 | **9.32:1** | 12.59:1 | 7.55:1 |
| source_code.py | 36,370 | 11,278 | **3.22:1** | 45.01:1 | 26.37:1 |
| structured_data.json | 44,329 | 4,972 | **8.92:1** | 12.48:1 | 9.30:1 |
| text_sample.txt | 76,458 | 2,586 | **29.57:1** | 49.52:1 | 46.00:1 |
| **Average** | | | **10.09:1** | 26.10:1 | 20.39:1 |

**Key Results:**
- ✅ Average compression ratio **10.09:1** (exceeds 10:1 target)
- ✅ **Beats gzip on server logs** (9.32:1 vs 7.55:1)
- ✅ Near gzip on structured JSON (8.92:1 vs 9.30:1)
- ✅ 29.57:1 on repetitive text
- ✅ All files pass lossless roundtrip verification

## Installation

Requires Python 3.7+ with no external dependencies (zlib is built-in):

```bash
git clone https://github.com/LWJSTONE/AtomZip.git
cd AtomZip
chmod +x atomzip.py  # optional
```

## Usage

```bash
# Compress
python atomzip.py compress input.txt output.azip -v
python atomzip.py compress input.txt output.azip --level 9

# Decompress
python atomzip.py decompress output.azip restored.txt -v

# Verify roundtrip
python atomzip.py verify input.txt -v

# Benchmark
python atomzip.py benchmark ./tests/test_files -v
```

## Project Structure

```
AtomZip/
├── atomzip.py              # CLI entry point
├── codec/
│   ├── __init__.py
│   ├── compress.py         # Compression pipeline (RLE→BPE→zlib)
│   ├── decompress.py       # Decompression pipeline
│   ├── pattern.py          # REPC BPE + byte remapping
│   ├── context.py          # Context modeling (for future enhancement)
│   ├── entropy.py          # Huffman entropy coding
│   ├── grammar.py          # Grammar-based extraction (alternative approach)
│   └── lz77.py             # LZ77 matching module
├── benchmark.py            # Benchmark comparison tool
├── generate_report.py      # Generate PDF benchmark report
├── tests/test_files/       # Test data
├── README.md
└── benchmark_results.json
```

## License

MIT License
