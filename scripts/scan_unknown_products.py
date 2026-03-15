"""
扫描未知产品结构：下载 1 天样本 → 分析表头/编码/分隔符 → 输出推荐规则。

用法：conda run -n quant python scripts/scan_unknown_products.py
"""

import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quantclass_sync_internal.archive import extract_archive
from quantclass_sync_internal.config import load_secrets_from_file
from quantclass_sync_internal.constants import (
    DEFAULT_API_BASE,
    DEFAULT_USER_SECRETS_FILE,
    KNOWN_DATASETS,
)
from quantclass_sync_internal.csv_engine import decode_text, detect_delimiter, looks_like_header
from quantclass_sync_internal.http_client import get_download_link, get_latest_time, save_file


def build_headers(api_key: str) -> dict:
    """构建 API 请求头。"""
    return {
        "api-key": api_key,
        "content-type": "application/json",
        "user-agent": "quantclass-sync/scanner",
    }


def analyze_csv(path: Path) -> dict:
    """分析单个 CSV 文件的结构。"""
    try:
        text, encoding = decode_text(path, preferred_encoding=None)
    except Exception as e:
        return {"error": f"decode: {e}"}

    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return {"error": "empty"}

    sample = lines[:3]
    delimiter = detect_delimiter(sample)

    import csv
    from io import StringIO

    parsed = []
    for row in csv.reader(sample, delimiter=delimiter):
        if row and any(c.strip() for c in row):
            parsed.append(list(row))
    if not parsed:
        return {"error": "no parsed rows"}

    first = parsed[0]
    second = parsed[1] if len(parsed) > 1 else []

    has_note = False
    if looks_like_header(first):
        header = first
    elif looks_like_header(second):
        header = second
        has_note = True
    else:
        header = first

    # 统计总行数
    total_lines = len(lines)
    data_rows = total_lines - (2 if has_note else 1)

    # 猜测 sort/key 列：找日期/时间列
    date_cols = []
    time_cols = []
    code_cols = []
    import re
    for col in header:
        col_lower = col.lower().strip()
        if any(k in col_lower for k in ["date", "日期", "时间"]):
            date_cols.append(col)
        if any(k in col_lower for k in ["candle_begin_time", "candle_end_time"]):
            time_cols.append(col)
        if any(k in col_lower for k in ["code", "代码", "symbol", "指标", "名称"]):
            code_cols.append(col)

    # 检查第一个数据行的日期列值格式
    date_format = None
    if parsed and len(parsed) > (1 if not has_note else 2):
        data_row_idx = 2 if has_note else 1
        if data_row_idx < len(parsed):
            data_row = parsed[data_row_idx]
        elif len(lines) > (2 if has_note else 1):
            data_row = next(csv.reader([lines[2 if has_note else 1]], delimiter=delimiter))
        else:
            data_row = []
        for col in date_cols + time_cols:
            if col in header:
                idx = header.index(col)
                if idx < len(data_row):
                    val = data_row[idx].strip()
                    if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", val):
                        date_format = "datetime"
                    elif re.match(r"\d{4}-\d{2}-\d{2}", val):
                        date_format = "date"
                    elif re.match(r"\d{8}", val):
                        date_format = "date_compact"

    return {
        "encoding": encoding,
        "delimiter": delimiter,
        "has_note": has_note,
        "note": lines[0][:60] if has_note else None,
        "header": header,
        "col_count": len(header),
        "data_rows": data_rows,
        "date_cols": date_cols,
        "time_cols": time_cols,
        "code_cols": code_cols,
        "date_format": date_format,
        "file_size_kb": path.stat().st_size / 1024,
    }


def main():
    # 读凭证
    api_key, hid = load_secrets_from_file(DEFAULT_USER_SECRETS_FILE)
    if not api_key or not hid:
        print("错误：未找到 API 凭证，请检查 user_secrets.env")
        sys.exit(1)
    headers = build_headers(api_key)

    # 读 catalog
    with open("catalog.txt") as f:
        catalog = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    unknown = [p for p in catalog if p not in KNOWN_DATASETS]
    print(f"待扫描产品: {len(unknown)}")

    results = {}
    with tempfile.TemporaryDirectory() as work_dir:
        work = Path(work_dir)
        for i, product in enumerate(sorted(unknown)):
            print(f"\n[{i+1}/{len(unknown)}] {product}")

            try:
                # 获取最新日期
                latest = get_latest_time(DEFAULT_API_BASE, product, hid, headers)
                if not latest:
                    print(f"  跳过：无最新日期")
                    results[product] = {"error": "no_latest_time"}
                    continue
                print(f"  最新日期: {latest}")

                # 获取下载链接
                url = get_download_link(DEFAULT_API_BASE, product, latest, hid, headers)
                if not url:
                    print(f"  跳过：无下载链接")
                    results[product] = {"error": "no_download_link"}
                    continue

                # 下载
                dl_dir = work / product
                dl_dir.mkdir(parents=True, exist_ok=True)
                # 从 URL 推断文件名
                from urllib.parse import urlparse
                url_path = urlparse(url).path
                filename = Path(url_path).name or f"{product}-{latest}.tar.gz"
                dl_path = dl_dir / filename
                save_file(url, dl_path, headers, product)
                print(f"  下载完成: {dl_path.name} ({dl_path.stat().st_size / 1024:.0f} KB)")

                # 解压
                extract_dir = dl_dir / "extract"
                extract_dir.mkdir(exist_ok=True)
                extract_archive(dl_path, extract_dir)

                # 找 CSV 文件分析
                csv_files = sorted(extract_dir.rglob("*.csv"))
                if not csv_files:
                    print(f"  跳过：解压后无 CSV 文件")
                    results[product] = {"error": "no_csv_files", "latest": latest}
                    continue

                print(f"  CSV 文件数: {len(csv_files)}")
                # 分析前 3 个文件
                analyses = []
                for csv_file in csv_files[:3]:
                    a = analyze_csv(csv_file)
                    a["filename"] = csv_file.name
                    analyses.append(a)

                # 汇总
                first = analyses[0]
                results[product] = {
                    "latest": latest,
                    "csv_count": len(csv_files),
                    "encoding": first.get("encoding"),
                    "delimiter": first.get("delimiter"),
                    "has_note": first.get("has_note"),
                    "col_count": first.get("col_count"),
                    "header": first.get("header"),
                    "date_cols": first.get("date_cols"),
                    "time_cols": first.get("time_cols"),
                    "code_cols": first.get("code_cols"),
                    "date_format": first.get("date_format"),
                    "sample_files": [a.get("filename") for a in analyses],
                    "data_rows_sample": first.get("data_rows"),
                }

                # 输出关键信息
                h = first.get("header", [])
                print(f"  编码: {first.get('encoding')}, 分隔符: {repr(first.get('delimiter'))}")
                print(f"  Note: {first.get('has_note')}, 列数: {first.get('col_count')}")
                print(f"  表头: {h[:5]}{'...' if len(h) > 5 else ''}")
                print(f"  日期列: {first.get('date_cols')}, 时间列: {first.get('time_cols')}")
                print(f"  代码列: {first.get('code_cols')}")

            except Exception as e:
                print(f"  错误: {e}")
                results[product] = {"error": str(e)}

            # 避免 API 限流
            time.sleep(0.5)

    # 保存结果
    output_path = Path("docs/product_scan_results.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存到 {output_path}")

    # 输出推荐规则摘要
    print(f"\n{'=' * 70}")
    print("推荐规则摘要")
    print(f"{'=' * 70}")

    for product in sorted(results):
        r = results[product]
        if "error" in r:
            print(f"\n{product}: 错误 - {r['error']}")
            continue

        encoding = r.get("encoding", "utf-8")
        has_note = r.get("has_note", False)
        date_cols = r.get("date_cols", [])
        time_cols = r.get("time_cols", [])
        code_cols = r.get("code_cols", [])

        # 推荐 sort_cols
        sort_cols = time_cols or date_cols[:1]
        # 推荐 key_cols
        key_cols = (code_cols[:1] if code_cols else []) + sort_cols

        print(f"\n{product}:")
        print(f"  encoding={repr(encoding)}, has_note={has_note}")
        print(f"  推荐 key_cols={tuple(key_cols)}")
        print(f"  推荐 sort_cols={tuple(sort_cols)}")
        print(f"  CSV 数: {r.get('csv_count')}, 列数: {r.get('col_count')}")


if __name__ == "__main__":
    main()
