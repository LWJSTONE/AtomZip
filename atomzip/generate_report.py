#!/usr/bin/env python3
"""Generate AtomZip benchmark comparison report as PDF."""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Read benchmark results
results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "benchmark_results.json")
with open(results_path) as f:
    results = json.load(f)

# Generate PDF using ReportLab
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, gray
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY, TA_RIGHT
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, PageBreak, KeepTogether, HRFlowable)
from reportlab.lib import colors
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Register Chinese fonts
try:
    pdfmetrics.registerFont(TTFont('NotoSansSC', '/usr/share/fonts/truetype/chinese/NotoSansSC[wght].ttf'))
    pdfmetrics.registerFont(TTFont('NotoSerifSC', '/usr/share/fonts/truetype/noto-serif-sc/NotoSerifSC[wght].ttf'))
    FONT_BODY = 'NotoSerifSC'
    FONT_HEADING = 'NotoSansSC'
except:
    FONT_BODY = 'Helvetica'
    FONT_HEADING = 'Helvetica-Bold'

# Colors
PRIMARY = HexColor('#1a365d')
SECONDARY = HexColor('#2d5a87')
ACCENT = HexColor('#e53e3e')
LIGHT_BG = HexColor('#f7fafc')
TABLE_HEADER_BG = HexColor('#1a365d')
TABLE_ALT_BG = HexColor('#edf2f7')
BORDER_COLOR = HexColor('#cbd5e0')

# Page setup
output_path = "/home/z/my-project/download/AtomZip_Benchmark_Report.pdf"
doc = SimpleDocTemplate(
    output_path,
    pagesize=A4,
    topMargin=2*cm,
    bottomMargin=2*cm,
    leftMargin=2.5*cm,
    rightMargin=2.5*cm,
)

# Styles
styles = getSampleStyleSheet()

title_style = ParagraphStyle(
    'CustomTitle',
    parent=styles['Title'],
    fontName=FONT_HEADING,
    fontSize=24,
    leading=30,
    textColor=PRIMARY,
    spaceAfter=12,
    alignment=TA_CENTER,
)

subtitle_style = ParagraphStyle(
    'CustomSubtitle',
    parent=styles['Normal'],
    fontName=FONT_BODY,
    fontSize=12,
    leading=18,
    textColor=SECONDARY,
    spaceAfter=20,
    alignment=TA_CENTER,
)

h1_style = ParagraphStyle(
    'H1',
    parent=styles['Heading1'],
    fontName=FONT_HEADING,
    fontSize=16,
    leading=22,
    textColor=PRIMARY,
    spaceBefore=20,
    spaceAfter=10,
    borderWidth=0,
    borderPadding=0,
)

h2_style = ParagraphStyle(
    'H2',
    parent=styles['Heading2'],
    fontName=FONT_HEADING,
    fontSize=13,
    leading=18,
    textColor=SECONDARY,
    spaceBefore=14,
    spaceAfter=8,
)

body_style = ParagraphStyle(
    'CustomBody',
    parent=styles['Normal'],
    fontName=FONT_BODY,
    fontSize=10,
    leading=16,
    textColor=black,
    spaceAfter=8,
    alignment=TA_JUSTIFY,
    firstLineIndent=20,
)

body_no_indent = ParagraphStyle(
    'CustomBodyNoIndent',
    parent=body_style,
    firstLineIndent=0,
)

table_header_style = ParagraphStyle(
    'TableHeader',
    fontName=FONT_HEADING,
    fontSize=8,
    leading=11,
    textColor=white,
    alignment=TA_CENTER,
)

table_cell_style = ParagraphStyle(
    'TableCell',
    fontName=FONT_BODY,
    fontSize=8,
    leading=11,
    textColor=black,
    alignment=TA_CENTER,
)

table_cell_left = ParagraphStyle(
    'TableCellLeft',
    fontName=FONT_BODY,
    fontSize=8,
    leading=11,
    textColor=black,
    alignment=TA_LEFT,
)

highlight_style = ParagraphStyle(
    'Highlight',
    parent=body_style,
    fontName=FONT_HEADING,
    fontSize=11,
    leading=17,
    textColor=ACCENT,
    spaceBefore=12,
    spaceAfter=12,
    alignment=TA_CENTER,
    borderWidth=1,
    borderColor=ACCENT,
    borderPadding=8,
)

# Build document
story = []

# Title
story.append(Spacer(1, 2*cm))
story.append(Paragraph("AtomZip \u538b\u7f29\u7b97\u6cd5\u57fa\u51c6\u6d4b\u8bd5\u62a5\u544a", title_style))
story.append(Paragraph("Recursive Entropic Pattern Collapse (REPC) Algorithm", subtitle_style))
story.append(Paragraph("\u539f\u521b\u7b97\u6cd5\u8bbe\u8ba1\u00b7\u5b9e\u73b0\u00b7\u6027\u80fd\u5bf9\u6bd4", subtitle_style))
story.append(Spacer(1, 1*cm))
story.append(HRFlowable(width="80%", thickness=2, color=PRIMARY, spaceAfter=20))

# Section 1: Algorithm Overview
story.append(Paragraph("1. \u7b97\u6cd5\u6982\u8ff0", h1_style))
story.append(Paragraph(
    "AtomZip\u662f\u4e00\u79cd\u57fa\u4e8e\u9012\u5f52\u4fe1\u606f\u71b5\u6a21\u5f0f\u585e\u7f29\uff08Recursive Entropic Pattern Collapse, REPC\uff09\u539f\u7406\u7684\u5168\u65b0\u65e0\u635f\u538b\u7f29\u7b97\u6cd5\u3002"
    "\u4e0e\u4f20\u7edfBPE\uff08Byte Pair Encoding\uff09\u4ec5\u6839\u636e\u9891\u7387\u9009\u62e9\u6a21\u5f0f\u4e0d\u540c\uff0cREPC\u7b97\u6cd5\u5f15\u5165\u4e86\u201c\u4fe1\u606f\u71b5\u589e\u76ca\u201d\u51c6\u5219\u2014\u2014"
    "\u540c\u65f6\u8003\u8651\u6a21\u5f0f\u7684\u51fa\u73b0\u9891\u7387\u548c\u4e0a\u4e0b\u6587\u591a\u6837\u6027\uff0c\u4f18\u5148\u9009\u62e9\u90a3\u4e9b\u5728\u591a\u79cd\u4e0d\u540c\u4e0a\u4e0b\u6587\u4e2d\u51fa\u73b0\u7684\u6a21\u5f0f\u8fdb\u884c\u66ff\u6362\uff0c"
    "\u56e0\u4e3a\u66ff\u6362\u8fd9\u7c7b\u6a21\u5f0f\u80fd\u66f4\u6709\u6548\u5730\u964d\u4f4e\u6570\u636e\u6d41\u7684\u5168\u5c40\u71b5\u503c\u3002",
    body_style
))
story.append(Paragraph(
    "\u7b97\u6cd5\u7684\u6838\u5fc3\u8bc4\u5206\u516c\u5f0f\u4e3a\uff1aScore(pair) = frequency \u00d7 (1 + min(context_diversity, 3.0))\u3002"
    "\u5176\u4e2d context_diversity = (\u552f\u4e00\u5de6\u4e0a\u4e0b\u6587\u6570 \u00d7 \u552f\u4e00\u53f3\u4e0a\u4e0b\u6587\u6570) / frequency\u3002"
    "\u8fd9\u79cd\u8bbe\u8ba1\u4f7f\u5f97\u7b97\u6cd5\u5728\u9009\u62e9\u66ff\u6362\u5019\u9009\u65f6\uff0c\u4e0d\u4ec5\u5173\u6ce8\u6a21\u5f0f\u51fa\u73b0\u7684\u9891\u7e41\u7a0b\u5ea6\uff0c"
    "\u8fd8\u8003\u8651\u4e86\u66ff\u6362\u8be5\u6a21\u5f0f\u5bf9\u5168\u5c40\u6570\u636e\u7ed3\u6784\u7684\u5f71\u54cd\uff0c\u4ece\u800c\u5b9e\u73b0\u66f4\u4f18\u7684\u6574\u4f53\u538b\u7f29\u6548\u679c\u3002",
    body_style
))

# Section 2: Pipeline
story.append(Paragraph("2. \u538b\u7f29\u6d41\u6c34\u7ebf", h1_style))
story.append(Paragraph(
    "AtomZip\u7684\u538b\u7f29\u6d41\u7a0b\u5206\u4e3a\u4e09\u4e2a\u9636\u6bb5\uff0c\u4f9d\u6b21\u5904\u7406\u8f93\u5165\u6570\u636e\uff1a",
    body_style
))
story.append(Paragraph(
    "\u9636\u6bb51\uff1aRLE\u9884\u5904\u7406\u3002\u5bf9\u8fde\u7eed\u91cd\u590d4\u6b21\u4ee5\u4e0a\u7684\u5b57\u8282\u8fdb\u884c\u6e38\u7a0b\u7f16\u7801\uff08Run-Length Encoding\uff09\uff0c"
    "\u5c06\u957f\u91cd\u590d\u5e8f\u5217\u538b\u7f29\u4e3a\u7d27\u51d1\u7684\u5b57\u8282+\u8ba1\u6570\u683c\u5f0f\u3002\u8fd9\u4e00\u6b65\u9aa4\u5bf9\u4e8e\u5305\u542b\u5927\u91cf\u91cd\u590d\u5b57\u8282\u7684\u6570\u636e\uff08\u5982\u4e8c\u8fdb\u5236\u6587\u4ef6\u4e2d\u7684\u96f6\u586b\u5145\uff09\u975e\u5e38\u6709\u6548\u3002",
    body_style
))
story.append(Paragraph(
    "\u9636\u6bb52\uff1a\u9012\u5f52\u4fe1\u606f\u71b5\u6a21\u5f0f\u585e\u7f29\uff08REPC\uff09\u3002\u8fed\u4ee3\u5730\u627e\u5230\u5f97\u5206\u6700\u9ad8\u7684\u5b57\u8282\u5bf9\uff0c"
    "\u7528\u672a\u4f7f\u7528\u7684\u5b57\u8282\u503c\u66ff\u6362\u5b83\u3002\u6bcf\u6b21\u66ff\u6362\u540e\uff0c\u65b0\u7684\u66ff\u6362\u5b57\u8282\u53ef\u4ee5\u4e0e\u76f8\u90bb\u5b57\u8282\u5f62\u6210\u65b0\u7684\u5bf9\uff0c"
    "\u4ece\u800c\u5b9e\u73b0\u5c42\u6b21\u5316\u7684\u8bed\u6cd5\u6784\u5efa\u3002\u5f53\u6240\u6709256\u4e2a\u5b57\u8282\u503c\u90fd\u5df2\u4f7f\u7528\u65f6\uff0c\u7b97\u6cd5\u901a\u8fc7\u8f6c\u4e49\u6700\u4f4e\u9891\u5b57\u8282\u6765\u91ca\u653e\u65b0\u7684\u7a7a\u95f4\u3002",
    body_style
))
story.append(Paragraph(
    "\u9636\u6bb53\uff1aHuffman\u71b5\u7f16\u7801\u3002\u5bf9\u7ecf\u8fc7\u6a21\u5f0f\u66ff\u6362\u540e\u7684\u6570\u636e\u6d41\u8fdb\u884c\u7edf\u8ba1\u5efa\u6a21\uff0c"
    "\u6784\u5efa\u89c4\u8303\u5316Huffman\u7f16\u7801\u8868\uff0c\u5b9e\u73b0\u63a5\u8fd1\u71b5\u6781\u9650\u7684\u4f4d\u7f16\u7801\u3002\u8fd9\u4e00\u9636\u6bb5\u5145\u5206\u5229\u7528\u4e86\u524d\u4e24\u4e2a\u9636\u6bb5\u4ea7\u751f\u7684\u975e\u5747\u5300\u7b26\u53f7\u5206\u5e03\u3002",
    body_style
))

# Section 3: Test Environment
story.append(Paragraph("3. \u6d4b\u8bd5\u73af\u5883\u4e0e\u6570\u636e", h1_style))
story.append(Paragraph(
    "\u6d4b\u8bd5\u4f7f\u7528\u4e86\u516d\u79cd\u4e0d\u540c\u7c7b\u578b\u7684\u6587\u4ef6\uff0c\u6db5\u76d6\u4e86\u5e38\u89c1\u7684\u6570\u636e\u7c7b\u578b\uff1a"
    "\u6587\u672c\u6587\u4ef6\uff08text_sample.txt, 76KB\uff09\u3001\u7ed3\u6784\u5316\u4e8c\u8fdb\u5236\uff08binary_structured.bin, 51KB\uff09\u3001"
    "\u6df7\u5408\u6570\u636e\uff08mixed_data.dat, 2KB\uff09\u3001\u6e90\u4ee3\u7801\uff08source_code.py, 36KB\uff09\u3001"
    "JSON\u7ed3\u6784\u5316\u6570\u636e\uff08structured_data.json, 44KB\uff09\u3001\u670d\u52a1\u5668\u65e5\u5fd7\uff08server_log.txt, 197KB\uff09\u3002"
    "\u5bf9\u6bd4\u7b97\u6cd5\u5305\u62ecLZMA\uff087z\u6781\u9650\u538b\u7f29\uff0cPreset 9 Extreme\uff09\u548cgzip\uff08\u6700\u4f73\u538b\u7f29\uff0clevel 9\uff09\u3002",
    body_style
))

# Section 4: Results Table
story.append(Paragraph("4. \u538b\u7f29\u6bd4\u8f83\u7ed3\u679c", h1_style))

# Build table data
header = [
    Paragraph("\u6587\u4ef6", table_header_style),
    Paragraph("\u539f\u59cb\u5927\u5c0f", table_header_style),
    Paragraph("AtomZip", table_header_style),
    Paragraph("AZ\u538b\u7f29\u6bd4", table_header_style),
    Paragraph("LZMA", table_header_style),
    Paragraph("LZMA\u538b\u7f29\u6bd4", table_header_style),
    Paragraph("gzip", table_header_style),
    Paragraph("gzip\u538b\u7f29\u6bd4", table_header_style),
]

table_data = [header]
for r in results:
    az = r.get('atomzip', {})
    lz = r.get('lzma', {})
    gz = r.get('gzip', {})

    row = [
        Paragraph(r['file'], table_cell_left),
        Paragraph(f"{r['original_size']:,}", table_cell_style),
        Paragraph(f"{az.get('compressed_size', 0):,}", table_cell_style),
        Paragraph(f"{az.get('ratio', 0):.2f}:1", table_cell_style),
        Paragraph(f"{lz.get('compressed_size', 0):,}", table_cell_style),
        Paragraph(f"{lz.get('ratio', 0):.2f}:1", table_cell_style),
        Paragraph(f"{gz.get('compressed_size', 0):,}", table_cell_style),
        Paragraph(f"{gz.get('ratio', 0):.2f}:1", table_cell_style),
    ]
    table_data.append(row)

# Add averages
az_ratios = [r['atomzip']['ratio'] for r in results if 'atomzip' in r and r['atomzip']]
lz_ratios = [r['lzma']['ratio'] for r in results if 'lzma' in r and r['lzma']]
gz_ratios = [r['gzip']['ratio'] for r in results if 'gzip' in r and r['gzip']]

avg_row = [
    Paragraph("\u5e73\u5747", ParagraphStyle('BoldCell', parent=table_cell_left, fontName=FONT_HEADING)),
    Paragraph("", table_cell_style),
    Paragraph("", table_cell_style),
    Paragraph(f"{sum(az_ratios)/len(az_ratios):.2f}:1" if az_ratios else "N/A",
              ParagraphStyle('BoldCell', parent=table_cell_style, fontName=FONT_HEADING)),
    Paragraph("", table_cell_style),
    Paragraph(f"{sum(lz_ratios)/len(lz_ratios):.2f}:1" if lz_ratios else "N/A",
              ParagraphStyle('BoldCell', parent=table_cell_style, fontName=FONT_HEADING)),
    Paragraph("", table_cell_style),
    Paragraph(f"{sum(gz_ratios)/len(gz_ratios):.2f}:1" if gz_ratios else "N/A",
              ParagraphStyle('BoldCell', parent=table_cell_style, fontName=FONT_HEADING)),
]
table_data.append(avg_row)

col_widths = [90, 65, 60, 55, 60, 55, 60, 55]
t = Table(table_data, colWidths=col_widths, repeatRows=1)

table_style = TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), TABLE_HEADER_BG),
    ('TEXTCOLOR', (0, 0), (-1, 0), white),
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('FONTSIZE', (0, 0), (-1, -1), 8),
    ('TOPPADDING', (0, 0), (-1, -1), 4),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ('LEFTPADDING', (0, 0), (-1, -1), 3),
    ('RIGHTPADDING', (0, 0), (-1, -1), 3),
    ('GRID', (0, 0), (-1, -1), 0.5, BORDER_COLOR),
    ('ROWBACKGROUNDS', (0, 1), (-1, -2), [white, TABLE_ALT_BG]),
    ('BACKGROUND', (0, -1), (-1, -1), HexColor('#e2e8f0')),
    ('LINEABOVE', (0, -1), (-1, -1), 1.5, PRIMARY),
])
t.setStyle(table_style)
story.append(t)

# Section 5: Speed Comparison
story.append(Paragraph("5. \u538b\u7f29/\u89e3\u538b\u901f\u5ea6\u5bf9\u6bd4", h1_style))

speed_header = [
    Paragraph("\u6587\u4ef6", table_header_style),
    Paragraph("AZ\u538b\u7f29(s)", table_header_style),
    Paragraph("AZ\u89e3\u538b(s)", table_header_style),
    Paragraph("LZMA\u538b\u7f29(s)", table_header_style),
    Paragraph("LZMA\u89e3\u538b(s)", table_header_style),
    Paragraph("gzip\u538b\u7f29(s)", table_header_style),
    Paragraph("gzip\u89e3\u538b(s)", table_header_style),
]

speed_data = [speed_header]
for r in results:
    az = r.get('atomzip', {})
    lz = r.get('lzma', {})
    gz = r.get('gzip', {})

    row = [
        Paragraph(r['file'], table_cell_left),
        Paragraph(f"{az.get('comp_time', 0):.3f}", table_cell_style),
        Paragraph(f"{az.get('decomp_time', 0):.3f}", table_cell_style),
        Paragraph(f"{lz.get('comp_time', 0):.3f}", table_cell_style),
        Paragraph(f"{lz.get('decomp_time', 0):.3f}", table_cell_style),
        Paragraph(f"{gz.get('comp_time', 0):.3f}", table_cell_style),
        Paragraph(f"{gz.get('decomp_time', 0):.3f}", table_cell_style),
    ]
    speed_data.append(row)

speed_col_widths = [90, 65, 65, 65, 65, 65, 65]
st = Table(speed_data, colWidths=speed_col_widths, repeatRows=1)
st.setStyle(table_style)
story.append(st)

# Section 6: Analysis
story.append(Paragraph("6. \u7ed3\u679c\u5206\u6790", h1_style))
story.append(Paragraph(
    "\u4ece\u6d4b\u8bd5\u7ed3\u679c\u53ef\u4ee5\u770b\u51fa\uff0cAtomZip\u5728\u7ed3\u6784\u5316\u6570\u636e\uff08JSON\u3001\u65e5\u5fd7\uff09\u4e0a\u8868\u73b0\u8f83\u597d\uff0c"
    "\u538b\u7f29\u6bd4\u5206\u522b\u8fbe\u52307.75:1\u548c7.34:1\uff0c\u63a5\u8fd1gzip\u7684\u6c34\u5e73\u3002\u8fd9\u662f\u56e0\u4e3a\u7ed3\u6784\u5316\u6570\u636e\u4e2d\u5b58\u5728\u5927\u91cf\u91cd\u590d\u7684\u6a21\u5f0f\uff08\u5982JSON\u952e\u540d\u3001\u65e5\u5fd7\u683c\u5f0f\u5b57\u7b26\u4e32\uff09\uff0c"
    "BPE\u98ce\u683c\u7684\u6a21\u5f0f\u66ff\u6362\u80fd\u6709\u6548\u6355\u6349\u8fd9\u4e9b\u91cd\u590d\u7ed3\u6784\u3002\u7279\u522b\u662f\u5bf9\u4e8eJSON\u6587\u4ef6\uff0cAtomZip\u7684\u538b\u7f29\u6bd4\uff087.75:1\uff09\u4e0egzip\uff089.30:1\uff09\u5dee\u8ddd\u4e0d\u5927\uff0c"
    "\u5c55\u73b0\u4e86REPC\u7b97\u6cd5\u5728\u7ed3\u6784\u5316\u6570\u636e\u4e0a\u7684\u6f5c\u529b\u3002",
    body_style
))
story.append(Paragraph(
    "\u7136\u800c\uff0c\u5bf9\u4e8e\u5305\u542b\u5927\u91cf\u4e0d\u540c\u6bb5\u843d\u7684\u7eaf\u6587\u672c\u548c\u6e90\u4ee3\u7801\u6587\u4ef6\uff0cAtomZip\u7684\u538b\u7f29\u6bd4\u660e\u663e\u4f4e\u4e8eLZMA\u548cgzip\u3002"
    "\u8fd9\u662f\u56e0\u4e3aLZMA\u548cgzip\u91c7\u7528\u7684LZ77\u6ed1\u52a8\u7a97\u53e3\u673a\u5236\u53ef\u4ee5\u7528\u6781\u5c11\u7684\u4f4d\u6570\u8868\u793a\u8fdc\u8ddd\u79bb\u7684\u91cd\u590d\uff08\u901a\u8fc7\u8ddd\u79bb-\u957f\u5ea6\u5bf9\uff09\uff0c"
    "\u800cBPE\u7c7b\u7b97\u6cd5\u9700\u8981\u901a\u8fc7\u591a\u5c42\u6b21\u7684\u66ff\u6362\u624d\u80fd\u6784\u5efa\u51fa\u7b49\u4ef7\u7684\u8868\u793a\u3002\u8fd9\u662f\u57fa\u4e8e\u8bed\u6cd5\u7684\u538b\u7f29\u65b9\u6cd5\u4e0e\u57fa\u4e8e\u5b57\u5178\u7684\u65b9\u6cd5\u4e4b\u95f4\u7684\u56fa\u6709\u5dee\u5f02\u3002",
    body_style
))
story.append(Paragraph(
    "\u5728\u901f\u5ea6\u65b9\u9762\uff0cAtomZip\u4f5c\u4e3aPython\u539f\u578b\u5b9e\u73b0\uff0c\u538b\u7f29\u901f\u5ea6\u663e\u8457\u6162\u4e8eC\u8bed\u8a00\u5b9e\u73b0\u7684LZMA\u548cgzip\u3002"
    "\u4e3b\u8981\u74f6\u9888\u5728\u4e8e\u6bcf\u6b21BPE\u8fed\u4ee3\u9700\u8981\u626b\u63cf\u6574\u4e2a\u6570\u636e\u6765\u8ba1\u7b97\u5bf9\u9891\u7387\u548c\u4e0a\u4e0b\u6587\u591a\u6837\u6027\uff0c\u65f6\u95f4\u590d\u6742\u5ea6\u4e3aO(n\u00d7k)\uff0c\u5176\u4e2dn\u4e3a\u6570\u636e\u957f\u5ea6\uff0ck\u4e3a\u8fed\u4ee3\u6b21\u6570\u3002"
    "\u82e5\u91c7\u7528C/Rust\u91cd\u5199\u6838\u5fc3\u5faa\u73af\u5e76\u4f7f\u7528\u66f4\u9ad8\u6548\u7684\u6570\u636e\u7ed3\u6784\uff08\u5982\u540e\u7f00\u6570\u7ec4\uff09\uff0c\u901f\u5ea6\u53ef\u63d0\u5347\u6570\u5341\u500d\u3002",
    body_style
))

# Section 7: Innovation
story.append(Paragraph("7. \u7b97\u6cd5\u521b\u65b0\u70b9\u603b\u7ed3", h1_style))
story.append(Paragraph(
    "\u4e0e\u4f20\u7edf\u538b\u7f29\u7b97\u6cd5\u76f8\u6bd4\uff0cAtomZip\u7684REPC\u7b97\u6cd5\u5177\u6709\u4ee5\u4e0b\u521b\u65b0\u7279\u70b9\uff1a"
    "\u4f20\u7edfBPE\u4ec5\u4ee5\u51fa\u73b0\u9891\u7387\u4e3a\u552f\u4e00\u6807\u51c6\u9009\u62e9\u66ff\u6362\u5019\u9009\uff0c\u800cREPC\u5f15\u5165\u4e86\u4e0a\u4e0b\u6587\u591a\u6837\u6027\u56e0\u5b50\uff0c"
    "\u4f18\u5148\u66ff\u6362\u90a3\u4e9b\u5728\u591a\u79cd\u4e0d\u540c\u4e0a\u4e0b\u6587\u4e2d\u51fa\u73b0\u7684\u6a21\u5f0f\uff0c\u56e0\u4e3a\u8fd9\u7c7b\u6a21\u5f0f\u7684\u66ff\u6362\u80fd\u66f4\u6709\u6548\u5730\u964d\u4f4e\u5168\u5c40\u4fe1\u606f\u71b5\u3002"
    "\u6b64\u5916\uff0c\u5c42\u6b21\u5316BPE\u673a\u5236\u5141\u8bb8\u7b97\u6cd5\u9012\u5f52\u5730\u6784\u5efa\u8bed\u6cd5\uff0c\u4ece\u800c\u538b\u7f29\u66f4\u957f\u7684\u91cd\u590d\u7ed3\u6784\u3002"
    "\u5f53\u5b57\u8282\u5b57\u6bcd\u8868\u8017\u5c3d\u65f6\uff0c\u7b97\u6cd5\u901a\u8fc7\u8f6c\u4e49\u6700\u4f4e\u9891\u5b57\u8282\u6765\u91ca\u653e\u65b0\u7684\u66ff\u6362\u7a7a\u95f4\uff0c"
    "\u5b9e\u73b0\u4e86\u5bf9\u5b8c\u6574\u5b57\u8282\u5b57\u6bcd\u8868\u6570\u636e\u7684\u538b\u7f29\u652f\u6301\u3002",
    body_style
))

# Core innovation summary
story.append(Spacer(1, 20))
story.append(Paragraph(
    "\u6838\u5fc3\u521b\u65b0\u70b9\uff1aREPC\u7b97\u6cd5\u4ee5\u201c\u4fe1\u606f\u71b5\u589e\u76ca\u201d\u800c\u975e\u7eaf\u9891\u7387\u4f5c\u4e3a\u6a21\u5f0f\u66ff\u6362\u7684\u9009\u62e9\u51c6\u5219\uff0c"
    "\u4f18\u5148\u66ff\u6362\u9ad8\u4e0a\u4e0b\u6587\u591a\u6837\u6027\u7684\u6a21\u5f0f\u4ee5\u66f4\u6709\u6548\u5730\u964d\u4f4e\u5168\u5c40\u6570\u636e\u6d41\u71b5\u503c\u3002",
    highlight_style
))

# Build PDF
doc.build(story)
print(f"Report generated: {output_path}")
print(f"File size: {os.path.getsize(output_path):,} bytes")
