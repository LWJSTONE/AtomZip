# AtomZip — 动态递归自适应压缩 (DRAC) v4

原创无损压缩算法与跨平台命令行工具。

## 核心创新

**BWT 上下文聚簇 + LZMA2 RAW 极限压缩** — 不同于 bzip2 (BWT+MTF+Huffman)，AtomZip 不加 MTF 变换。关键发现：MTF 虽然产生大量零值，但打乱了字节的空间局部性，反而损害 LZMA2 的 LZ77 匹配能力。仅用 BWT 聚簇 + LZMA2 压缩的组合，让 LZMA2 的 LZ77 匹配器在聚簇区域找到更长的匹配，实现了比单纯 LZMA2 更高的压缩比。

**全文件单块 BWT** — 使用整个文件作为单个 BWT 块（而非 64KB 分块），避免块边界信息丢失，大幅提升压缩效果。

**LZMA2 RAW 格式优化** — 消除 XZ 容器的 56 字节开销（CRC 校验、索引、块头等），在相同 LZMA2 算法下输出比 7z/XZ 更小。

**自适应多策略压缩** — 高压缩级别下同时尝试多种策略（纯 LZMA2 / Delta+LZMA2 / BWT+LZMA2），自动选择最优结果。

## 压缩流水线

- **策略0**: LZMA2 RAW（基线，无预处理）
- **策略1**: Delta 差分编码 + LZMA2 RAW
- **策略2**: BWT 全文件单块 + LZMA2 RAW（不加 MTF）

压缩级别 7-9 时，三种策略竞争，输出最优结果。

## 基准测试结果

| 文件 | 原始大小 | AtomZip | AZ比率 | LZMA(7z极限) | LZMA比率 | gzip | gzip比率 |
|------|---------|---------|--------|-------------|----------|------|----------|
| binary_structured.bin | 51,512 | 1,524 | **33.80:1** | 1,564 | 32.94:1 | 1,735 | 29.69:1 |
| mixed_data.dat | 2,275 | 517 | **4.40:1** | 560 | 4.06:1 | 671 | 3.39:1 |
| server_log.txt | 197,490 | 13,004 | **15.19:1** | 15,688 | 12.59:1 | 26,147 | 7.55:1 |
| source_code.py | 36,370 | 693 | **52.48:1** | 808 | 45.01:1 | 1,379 | 26.37:1 |
| structured_data.json | 44,329 | 3,090 | **14.35:1** | 3,552 | 12.48:1 | 4,767 | 9.30:1 |
| text_sample.txt | 76,458 | 1,504 | **50.84:1** | 1,544 | 49.52:1 | 1,662 | 46.00:1 |
| **平均** | | | **28.51:1** | | 26.10:1 | | 20.39:1 |

**关键成果:**
- AtomZip 在所有 6 个测试文件上均优于 LZMA (7z 极限压缩)
- 平均压缩比 28.51:1，超过 LZMA 的 26.10:1 (+9.2%)
- server_log.txt: AtomZip 更小 20.6%
- source_code.py: AtomZip 更小 16.6%
- structured_data.json: AtomZip 更小 15.0%
- 在所有文件上远超 gzip（平均 20.39:1）
- 5/6 文件达到 10:1+ 压缩比
- 所有文件通过无损往返验证

## 安装与运行

需要 Python 3.7+，无需额外依赖（zlib 和 lzma 为内置模块）：

```bash
git clone https://github.com/LWJSTONE/AtomZip.git
cd AtomZip

# 压缩文件
python atomzip.py compress 输入文件 输出.azip

# 最高压缩率
python atomzip.py compress 输入文件 输出.azip --level 9

# 解压文件
python atomzip.py decompress 输出.azip 恢复文件

# 验证往返正确性
python atomzip.py verify 输入文件

# 基准测试
python atomzip.py benchmark ./tests/test_files
```

## 使用说明

```
用法: atomzip <命令> [选项]

命令:
  compress    压缩文件
  decompress  解压文件
  verify      验证往返正确性
  benchmark   运行基准测试

选项:
  -l, --level  压缩级别 (1-9，默认: 5)
  -v, --verbose 显示详细信息
  --version    显示版本号
  -h, --help   显示帮助

压缩级别:
  1-3: 快速压缩 (仅 LZMA2)
  4-6: 均衡压缩 (LZMA2 + Delta)
  7-9: 极限压缩 (多策略竞争含 BWT，自动选择最优)
```

## 项目结构

```
AtomZip/
├── atomzip.py              # 命令行入口
├── codec/
│   ├── __init__.py         # 模块初始化
│   ├── compress.py         # 压缩引擎 (多策略竞争)
│   ├── decompress.py       # 解压引擎
│   └── transform.py        # BWT / Delta 数据变换
├── benchmark.py            # 基准测试工具
├── test_roundtrip.py       # 往返验证测试
├── tests/test_files/       # 测试数据
├── README.md
└── benchmark_results.json
```

## 文件格式

AtomZip v4 格式:
```
偏移  大小  字段
0     4     魔数: b'AZIP'
4     1     版本号: 4
5     4     原始大小 (大端序 uint32)
9     1     压缩策略 (0/1/2)
10    2     额外头部大小
12    ...   额外头部 (策略相关元数据)
...   4     LZMA2 RAW 数据大小
...   ...   LZMA2 RAW 压缩数据
```

策略0: 无额外头部
策略1: 1字节 first_byte (Delta 编码的首字节)
策略2: BWT 块信息 (块数量 + 各块的 orig_idx 和块大小)

## 许可证

MIT License
