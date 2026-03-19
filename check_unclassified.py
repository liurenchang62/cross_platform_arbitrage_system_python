import csv
from collections import Counter
from pathlib import Path


def analyze_all_keywords(file_path):
    keyword_counter = Counter()
    total_records = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_records += 1
            for kw in row['keywords'].split(','):
                if kw and len(kw) > 2:  # 过滤掉太短的词
                    keyword_counter[kw] += 1

    print(f"📊 总记录数: {total_records}")
    print(f"\n🔑 所有出现次数 > 5 的关键词:")
    print("-" * 50)

    for kw, count in keyword_counter.most_common():
        if count > 5:
            print(f"{kw}: {count}")


# 运行
file_path = r"D:\cross_market_arbitrage_project\logs\unclassified\unclassified-2026-03-16.csv"
analyze_all_keywords(file_path)