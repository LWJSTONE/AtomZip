#!/usr/bin/env python3
"""
生成大型真实测试文件用于 AtomZip v5 基准测试。

生成更大 (1-10MB) 更真实的测试文件:
  - server_log.txt: 服务器日志 (10MB)
  - structured_data.json: JSON结构化数据 (5MB)
  - source_code.py: 源代码 (3MB)
  - text_sample.txt: 英文文本 (5MB)
  - binary_structured.bin: 结构化二进制 (5MB)
  - access_log.txt: Apache访问日志 (10MB)
  - database_dump.csv: 数据库导出CSV (8MB)
"""

import os
import random
import json
import string
from pathlib import Path


def gen_server_log(output_path: str, target_size: int = 10 << 20):
    """生成服务器日志文件 (~10MB)。"""
    levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
    services = [f'Service{i}' for i in range(20)]
    endpoints = ['/api/users', '/api/products', '/api/orders', '/api/auth',
                 '/api/search', '/api/config', '/api/health', '/api/metrics',
                 '/api/payments', '/api/notifications']
    statuses = ['OK', 'OK', 'OK', 'OK', 'OK', 'FAIL', 'TIMEOUT', 'ERROR']
    methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']

    with open(output_path, 'w') as f:
        current_size = 0
        line_num = 0
        while current_size < target_size:
            date = f'2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}'
            time = f'{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}'
            level = random.choice(levels)
            service = random.choice(services)
            method = random.choice(methods)
            endpoint = random.choice(endpoints)
            status = random.choice(statuses)
            bytes_sent = random.randint(100, 50000)
            latency = random.uniform(0.1, 500.0)
            req_id = random.randint(0, 999999)
            user_id = random.randint(1000, 9999)

            line = (f'{date} {time} [{level}] {service} - '
                    f'{method} {endpoint} req={req_id} '
                    f'status={status} bytes={bytes_sent} '
                    f'latency={latency:.1f}ms user={user_id}\n')
            f.write(line)
            current_size += len(line)
            line_num += 1

    print(f"  生成: {output_path} ({os.path.getsize(output_path):,} 字节, {line_num} 行)")


def gen_structured_json(output_path: str, target_size: int = 5 << 20):
    """生成结构化 JSON 数据 (~5MB)。"""
    categories = [f'cat_{i}' for i in range(50)]
    regions = ['US', 'EU', 'APAC', 'LATAM', 'MEA']
    departments = ['Engineering', 'Marketing', 'Sales', 'Finance', 'HR', 'Operations']

    records = []
    record_id = 0
    current_size = 0

    while current_size < target_size:
        record = {
            "id": record_id,
            "name": f"item_{record_id}",
            "value": round(random.uniform(0, 10000), 2),
            "active": random.choice([True, False]),
            "category": random.choice(categories),
            "region": random.choice(regions),
            "department": random.choice(departments),
            "score": round(random.uniform(0, 100), 1),
            "count": random.randint(0, 10000),
            "tags": random.sample([f"tag_{i}" for i in range(100)], k=random.randint(1, 5)),
        }
        records.append(record)
        current_size += len(json.dumps(record))
        record_id += 1

    data = {"records": records, "total": len(records), "version": "2.0"}

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"  生成: {output_path} ({os.path.getsize(output_path):,} 字节, {len(records)} 条记录)")


def gen_source_code(output_path: str, target_size: int = 3 << 20):
    """生成重复模式的源代码 (~3MB)。"""
    templates = [
        # 模板1: 函数定义
        '''def function_{name}(x, y, z=None):
    """处理 {name} 相关业务逻辑。"""
    if x > 0:
        result = x * y + {offset}
    else:
        result = x + y - {offset}
    for j in range({count}):
        result += j * x
    if z is not None:
        result += z
    return result

''',
        # 模板2: 类定义
        '''class Handler{name}:
    """{name} 处理器。"""
    def __init__(self, name="{name}", value={value}):
        self.name = name
        self.value = value
        self.data = []
        self._initialized = True

    def process(self, data):
        """处理输入数据。"""
        if not data:
            return None
        result = self.value
        for item in data:
            result += item
        self.data.append(result)
        return result

    def validate(self, input_data):
        """验证输入数据。"""
        if not input_data:
            return False
        if len(input_data) > {limit}:
            return False
        return True

    def transform(self, data, mode="default"):
        """转换数据。"""
        if mode == "default":
            return [x * self.value for x in data]
        elif mode == "reverse":
            return [x // self.value for x in reversed(data)]
        return data

''',
        # 模板3: 配置
        '''CONFIG_{name} = {{
    "name": "{name}",
    "version": "{version}",
    "max_retries": {retries},
    "timeout": {timeout},
    "enabled": True,
    "debug": False,
    "log_level": "{level}",
    "params": {{
        "batch_size": {batch},
        "threshold": {threshold},
        "iterations": {iterations},
    }}
}}

''',
    ]

    with open(output_path, 'w') as f:
        f.write('"""Auto-generated source code for compression testing."""\n\n')
        current_size = 0
        idx = 0

        while current_size < target_size:
            template = templates[idx % len(templates)]
            name = f'{idx:04d}'
            code = template.format(
                name=name,
                offset=random.randint(0, 1000),
                count=random.randint(0, 10),
                value=random.randint(0, 100),
                limit=random.randint(100, 10000),
                version=f'{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}',
                retries=random.randint(1, 10),
                timeout=random.randint(10, 300),
                level=random.choice(['DEBUG', 'INFO', 'WARN', 'ERROR']),
                batch=random.choice([16, 32, 64, 128, 256]),
                threshold=round(random.uniform(0.01, 0.99), 2),
                iterations=random.randint(100, 10000),
            )
            f.write(code)
            current_size += len(code)
            idx += 1

    print(f"  生成: {output_path} ({os.path.getsize(output_path):,} 字节, {idx} 个定义)")


def gen_text_sample(output_path: str, target_size: int = 5 << 20):
    """生成英文文本 (~5MB)。"""
    paragraphs = [
        "The advancement of technology has fundamentally transformed how we interact with the world around us. From the invention of the printing press to the development of the internet, each technological leap has brought about profound changes in society, culture, and the economy. The digital revolution, in particular, has reshaped industries, created new forms of communication, and enabled unprecedented access to information.",
        "Machine learning algorithms have become increasingly sophisticated in recent years, enabling computers to perform tasks that were once thought to be exclusively human domains. Natural language processing, computer vision, and reinforcement learning have all made remarkable progress, leading to applications in healthcare, finance, autonomous vehicles, and many other fields.",
        "The field of data science continues to evolve rapidly, with new techniques and methodologies emerging on a regular basis. Statistical analysis, data visualization, and predictive modeling are just a few of the core competencies that data scientists must master. The ability to extract meaningful insights from large and complex datasets is increasingly valuable in today's data-driven world.",
        "Cloud computing has revolutionized the way organizations manage their IT infrastructure. By leveraging cloud services, companies can scale their resources dynamically, reduce capital expenditures, and focus on their core business objectives. Major cloud providers offer a wide range of services, from virtual machines and storage to machine learning APIs and serverless computing.",
        "Cybersecurity remains a critical concern for organizations of all sizes. The increasing frequency and sophistication of cyber attacks demands constant vigilance and investment in security measures. Threat detection, incident response, and security awareness training are essential components of a comprehensive cybersecurity strategy.",
        "The Internet of Things (IoT) is connecting billions of devices worldwide, generating massive amounts of data and enabling new applications in smart homes, industrial automation, healthcare monitoring, and environmental sensing. However, the proliferation of IoT devices also raises significant security and privacy challenges.",
        "Quantum computing represents a paradigm shift in computational capability. While still in its early stages, quantum computers have the potential to solve problems that are intractable for classical computers, such as breaking encryption algorithms, simulating molecular interactions, and optimizing complex logistics networks.",
        "Blockchain technology has expanded beyond its original application in cryptocurrency to find use cases in supply chain management, digital identity verification, decentralized finance, and voting systems. The immutability and transparency of blockchain ledgers make them particularly well-suited for applications requiring trust and auditability.",
        "DevOps practices have transformed software development and deployment processes, emphasizing collaboration between development and operations teams. Continuous integration, continuous delivery, and infrastructure as code have become standard practices in modern software organizations, enabling faster and more reliable software releases.",
        "The rise of edge computing represents a shift from centralized cloud architectures to distributed processing at the network edge. By processing data closer to its source, edge computing reduces latency, conserves bandwidth, and enables real-time decision making for applications such as autonomous vehicles and industrial automation.",
    ]

    with open(output_path, 'w') as f:
        current_size = 0
        while current_size < target_size:
            para = random.choice(paragraphs)
            f.write(para + '\n\n')
            current_size += len(para) + 2

    print(f"  生成: {output_path} ({os.path.getsize(output_path):,} 字节)")


def gen_binary_structured(output_path: str, target_size: int = 5 << 20):
    """生成结构化二进制数据 (~5MB)。"""
    with open(output_path, 'wb') as f:
        current_size = 0
        record_num = 0
        while current_size < target_size:
            # 每条记录: 4字节ID + 8字节时间戳 + 4字节值 + 2字节状态 + 2字节类型 + 4字节保留
            record_id = record_num & 0xFFFFFFFF
            timestamp = 1700000000 + record_num * 100
            value = random.randint(0, 1000000)
            status = random.choice([0, 1, 2, 3])
            rec_type = random.choice([1, 2, 3, 4, 5])
            reserved = 0

            record = struct_pack = __import__('struct').pack('>IqIHHI',
                        record_id, timestamp, value, status, rec_type, reserved)
            f.write(record)
            current_size += len(record)
            record_num += 1

    print(f"  生成: {output_path} ({os.path.getsize(output_path):,} 字节, {record_num} 条记录)")


def gen_access_log(output_path: str, target_size: int = 10 << 20):
    """生成 Apache 访问日志 (~10MB)。"""
    ips = [f'192.168.{random.randint(1,254)}.{random.randint(1,254)}' for _ in range(100)]
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    paths = ['/index.html', '/about', '/products', '/api/data', '/css/style.css',
             '/js/app.js', '/images/logo.png', '/api/users', '/api/products',
             '/api/orders', '/favicon.ico', '/robots.txt', '/sitemap.xml',
             '/docs/guide', '/docs/api', '/dashboard', '/login', '/logout']
    codes = [200, 200, 200, 200, 301, 304, 400, 401, 403, 404, 500]
    agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
              'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
              'curl/7.68.0', 'python-requests/2.28.0']

    with open(output_path, 'w') as f:
        current_size = 0
        while current_size < target_size:
            ip = random.choice(ips)
            method = random.choice(methods)
            path = random.choice(paths)
            code = random.choice(codes)
            size = random.randint(100, 100000)
            agent = random.choice(agents)
            date = f'{random.randint(1,28):02d}/{random.choice(["Jan","Feb","Mar","Apr","May","Jun"])}' \
                   f'/2025:{random.randint(0,23):02d}:{random.randint(0,59):02d}:' \
                   f'{random.randint(0,59):02d} +0800'

            line = f'{ip} - - [{date}] "{method} {path} HTTP/1.1" {code} {size} "{agent}"\n'
            f.write(line)
            current_size += len(line)

    print(f"  生成: {output_path} ({os.path.getsize(output_path):,} 字节)")


def gen_database_csv(output_path: str, target_size: int = 8 << 20):
    """生成数据库导出 CSV (~8MB)。"""
    categories = [f'cat_{i}' for i in range(50)]
    departments = ['Engineering', 'Marketing', 'Sales', 'Finance', 'HR']
    countries = ['US', 'CN', 'JP', 'DE', 'UK', 'FR', 'KR', 'AU', 'CA', 'IN']

    with open(output_path, 'w') as f:
        f.write('id,name,email,age,department,salary,country,category,score,status,created_at\n')
        current_size = 0
        record_id = 0

        while current_size < target_size:
            name = f'user_{record_id}'
            email = f'{name}@example.com'
            age = random.randint(20, 65)
            dept = random.choice(departments)
            salary = random.randint(30000, 200000)
            country = random.choice(countries)
            cat = random.choice(categories)
            score = round(random.uniform(0, 100), 1)
            status = random.choice(['active', 'inactive', 'pending'])
            created = f'2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}'

            line = f'{record_id},{name},{email},{age},{dept},{salary},{country},{cat},{score},{status},{created}\n'
            f.write(line)
            current_size += len(line)
            record_id += 1

    print(f"  生成: {output_path} ({os.path.getsize(output_path):,} 字节, {record_id} 行)")


def main():
    output_dir = Path(__file__).parent / 'tests' / 'test_files'
    output_dir.mkdir(parents=True, exist_ok=True)

    print()
    print("╔══════════════════════════════════════════════════╗")
    print("║     AtomZip v5 测试文件生成器                    ║")
    print("╚══════════════════════════════════════════════════╝")
    print()

    random.seed(42)  # 可重现性

    gen_server_log(str(output_dir / 'server_log.txt'), target_size=10 << 20)
    gen_structured_json(str(output_dir / 'structured_data.json'), target_size=5 << 20)
    gen_source_code(str(output_dir / 'source_code.py'), target_size=3 << 20)
    gen_text_sample(str(output_dir / 'text_sample.txt'), target_size=5 << 20)
    gen_binary_structured(str(output_dir / 'binary_structured.bin'), target_size=5 << 20)
    gen_access_log(str(output_dir / 'access_log.txt'), target_size=10 << 20)
    gen_database_csv(str(output_dir / 'database_dump.csv'), target_size=8 << 20)

    print()
    print("  所有测试文件生成完成!")
    total = sum(f.stat().st_size for f in output_dir.iterdir() if f.is_file())
    print(f"  总计: {total:,} 字节 ({total / (1 << 20):.1f} MB)")


if __name__ == '__main__':
    main()
