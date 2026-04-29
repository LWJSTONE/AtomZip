# AtomZip — 动态递归自适应压缩 (DRAC) v3

原创无损压缩算法与跨平台命令行工具。

## 核心创新

**REPC（递归熵模式坍缩）评分准则** — 不同于传统BPE仅按频率选择模式，REPC基于信息熵增益（频率 × 上下文多样性）选择模式，使每次替换最大化降低全局数据熵。

**LZMA2 RAW 格式优化** — 消除XZ容器的56字节开销（CRC校验、索引、块头等），在相同LZMA2算法下输出比7z/XZ更小。

**自适应多策略压缩** — 高压缩级别下同时尝试多种策略（纯LZMA2 / RLE+LZMA2 / RLE+BPE+LZMA2），自动选择最优结果。

**评分公式:** `Score(pair) = frequency × (1 + min(context_diversity, 3.0))`

## 压缩流水线

1. **RLE 预处理**: 对连续重复字节（≥4）进行游程编码
2. **字节重映射**: 转义稀有字节，创建空闲字节值供BPE使用（突破256规则限制）
3. **REPC BPE**: 基于熵增益评分的分层字节对编码，迭代替换最高分模式
4. **LZMA2 RAW 极限压缩**: 大字典（64MB）+ 范围编码，无XZ容器开销
5. **自适应策略选择**: 多策略竞争，输出最优结果

## 基准测试结果

| 文件 | 原始大小 | AtomZip | AZ比率 | LZMA(7z极限) | LZMA比率 | gzip | gzip比率 |
|------|---------|---------|--------|-------------|----------|------|----------|
| binary_structured.bin | 51,512 | 1,526 | **33.76:1** | 1,564 | 32.94:1 | 1,735 | 29.69:1 |
| mixed_data.dat | 2,275 | 519 | **4.38:1** | 560 | 4.06:1 | 671 | 3.39:1 |
| server_log.txt | 197,490 | 15,648 | **12.62:1** | 15,688 | 12.59:1 | 26,147 | 7.55:1 |
| source_code.py | 36,370 | 767 | **47.42:1** | 808 | 45.01:1 | 1,379 | 26.37:1 |
| structured_data.json | 44,329 | 3,514 | **12.61:1** | 3,552 | 12.48:1 | 4,767 | 9.30:1 |
| text_sample.txt | 76,458 | 1,506 | **50.77:1** | 1,544 | 49.52:1 | 1,662 | 46.00:1 |
| **平均** | | | **26.93:1** | | 26.10:1 | | 20.39:1 |

**关键成果:**
- 🏆 AtomZip 在所有6个测试文件上均优于 LZMA (7z极限压缩)
- 🏆 平均压缩比 26.93:1，超过 LZMA 的 26.10:1
- 🏆 在所有文件上远超 gzip（平均 20.39:1）
- ✅ 5/6 文件达到 10:1+ 压缩比
- ✅ 所有文件通过无损往返验证

## 安装与运行

需要 Python 3.7+，无需额外依赖（zlib 和 lzma 为内置模块）：

```bash
git clone https://github.com/LWJSTONE/AtomZip.git
cd AtomZip

# 压缩文件
python atomzip.py compress 输入文件 输出.azip

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
  4-6: 均衡压缩 (尝试 BPE + LZMA2)
  7-9: 极限压缩 (多策略竞争，自动选择最优)
```

## 项目结构

```
AtomZip/
├── atomzip.py              # 命令行入口
├── codec/
│   ├── __init__.py         # 模块初始化
│   ├── compress.py         # 压缩流水线 (RLE→BPE→LZMA2 RAW)
│   ├── decompress.py       # 解压流水线
│   └── pattern.py          # REPC BPE + 字节重映射
├── benchmark.py            # 基准测试工具
├── generate_report.py      # 报告生成
├── tests/test_files/       # 测试数据
├── README.md
└── benchmark_results.json
```

## 文件格式

AtomZip v3 格式:
```
偏移  大小  字段
0     4     魔数: b'AZIP'
4     1     版本号: 3
5     4     原始大小 (大端序 uint32)
9     1     压缩策略 (0/1/2)
10    2     标志位 (bit0: RLE, bit1: BPE)
12    ...   RLE 条目 (如有)
...   2     BPE 规则数据大小
...   ...   BPE 规则数据 (如有)
...   4     LZMA2 RAW 数据大小
...   ...   LZMA2 RAW 压缩数据
```

## 许可证

MIT License
