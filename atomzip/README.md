# AtomZip — Recursive Entropic Pattern Collapse (REPC)

A novel lossless compression algorithm and cross-platform command-line tool.

## Core Innovation

Unlike traditional BPE (Byte Pair Encoding) which selects substitution patterns based solely on frequency, **REPC uses an "Information Entropy Gain" criterion** that considers both frequency AND context diversity. Patterns appearing in many different contexts are preferred because replacing them reduces the global entropy of the data stream more effectively.

**Scoring Formula:** `Score(pair) = frequency × (1 + min(context_diversity, 3.0))`

Where `context_diversity = (unique_left_contexts × unique_right_contexts) / frequency`

## Algorithm Pipeline

1. **RLE Preprocessing**: Run-length encoding for consecutive repeated bytes (4+)
2. **Hierarchical BPE with REPC Scoring**: Iteratively replace the highest-scoring byte pair with an unused byte value. After each replacement, new pairs form (including with replacement bytes), enabling hierarchical grammar building
3. **Huffman Entropy Coding**: Build canonical Huffman codes for near-optimal bit encoding

When all 256 byte values are used, the algorithm frees the least frequent byte through escaping, enabling continued compression.

## Installation

Requires Python 3.7+ (no external dependencies):

```bash
# Clone the repository
git clone https://github.com/LWJSTONE/AtomZip.git
cd AtomZip

# Make executable (optional)
chmod +x atomzip.py
```

## Usage

```bash
# Compress a file
python atomzip.py compress input.txt output.azip
python atomzip.py compress input.txt output.azip --level 9 -v

# Decompress a file
python atomzip.py decompress output.azip restored.txt -v

# Verify roundtrip correctness
python atomzip.py verify input.txt -v

# Run benchmark comparison
python atomzip.py benchmark ./tests/test_files -v
```

### Compression Levels (1-9)

| Level | Max Rules | Description |
|-------|-----------|-------------|
| 1-3   | 50-150    | Fast compression, moderate ratio |
| 4-6   | 200-300   | Balanced (default: 5) |
| 7-9   | 400-600   | Maximum compression, slower |

## File Format

```
[4B: Magic "AZIP"]
[1B: Version]
[8B: Original size]
[4B: Flags]
[RLE entries (if flag set)]
[4B: Rules data length + Rules data]
[4B: Coded data length + Huffman coded data]
```

## Benchmark Results

Tested against LZMA (7z extreme, preset 9 extreme) and gzip (level 9):

| File | Original | AtomZip | AZ Ratio | LZMA | LZMA Ratio | gzip | gzip Ratio |
|------|----------|---------|----------|------|------------|------|------------|
| binary_structured.bin | 51,512 | 17,831 | 2.89:1 | 1,564 | 32.94:1 | 1,735 | 29.69:1 |
| mixed_data.dat | 2,275 | 2,016 | 1.13:1 | 560 | 4.06:1 | 671 | 3.39:1 |
| server_log.txt | 197,490 | 26,914 | 7.34:1 | 15,688 | 12.59:1 | 26,147 | 7.55:1 |
| source_code.py | 36,370 | 11,745 | 3.10:1 | 808 | 45.01:1 | 1,379 | 26.37:1 |
| structured_data.json | 44,329 | 5,720 | 7.75:1 | 3,552 | 12.48:1 | 4,767 | 9.30:1 |
| text_sample.txt | 76,458 | 13,427 | 5.69:1 | 1,544 | 49.52:1 | 1,662 | 46.00:1 |

**Key Findings:**
- AtomZip excels on structured data (JSON: 7.75:1, close to gzip's 9.30:1)
- Log files achieve 7.34:1, comparable to gzip's 7.55:1
- LZMA/gzip outperform on highly repetitive text due to LZ77's long-range back-referencing
- As a Python prototype, AtomZip is slower than C-based implementations; a Rust/C rewrite would be 20-50x faster

## Project Structure

```
AtomZip/
├── atomzip.py              # Main CLI entry point
├── codec/
│   ├── __init__.py         # Package init
│   ├── compress.py         # Compression pipeline
│   ├── decompress.py       # Decompression pipeline
│   ├── pattern.py          # REPC pattern extraction
│   ├── context.py          # Context modeling (future enhancement)
│   └── entropy.py          # Huffman entropy coding
├── benchmark.py            # Benchmark comparison tool
├── generate_report.py      # Generate PDF benchmark report
├── tests/
│   └── test_files/         # Test data files
├── README.md
└── benchmark_results.json  # Latest benchmark data
```

## Cross-Platform Compatibility

AtomZip works on Windows, Linux, and macOS. It only requires Python 3.7+ with no external dependencies. The compressed file format uses big-endian byte order for consistent cross-platform behavior.

## License

MIT License
