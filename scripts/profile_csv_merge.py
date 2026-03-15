"""
CSV 合并阶段性能分析脚本。

对真实数据文件做分阶段计时，定位瓶颈在 I/O 还是 CPU（解析/去重/排序）。
用法：python3 scripts/profile_csv_merge.py [--product PRODUCT] [--sample N]
"""

import argparse
import json
import random
import statistics
import sys
import tempfile
import time
from pathlib import Path

# 让脚本能 import 项目模块
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quantclass_sync_internal.csv_engine import (
    merge_payload,
    read_csv_payload,
    write_csv_payload,
)
from quantclass_sync_internal.models import CsvPayload, RULES


def profile_single_file(path: Path, rule):
    """对单个 CSV 文件做分阶段计时，返回各阶段耗时字典。"""

    result = {"file": path.name, "size_kb": path.stat().st_size / 1024}

    # 阶段 0：纯 I/O 基线（读字节，不解析）
    t0 = time.perf_counter()
    raw_bytes = path.read_bytes()
    result["raw_read_ms"] = (time.perf_counter() - t0) * 1000
    result["size_kb"] = len(raw_bytes) / 1024
    result["lines"] = raw_bytes.count(b"\n")

    # 阶段 1：read_csv_payload（编码检测 + CSV 解析 + 表头识别）
    t0 = time.perf_counter()
    existing = read_csv_payload(path, preferred_encoding=rule.encoding)
    result["read_parse_ms"] = (time.perf_counter() - t0) * 1000
    result["rows"] = len(existing.rows)

    # 阶段 2：模拟 merge（用最后 100 行做 incoming，模拟增量追加）
    n_incoming = min(100, len(existing.rows))
    if n_incoming > 0 and existing.header:
        incoming = CsvPayload(
            note=existing.note,
            header=list(existing.header),
            rows=[list(r) for r in existing.rows[-n_incoming:]],
            encoding=existing.encoding,
            delimiter=existing.delimiter,
        )
        t0 = time.perf_counter()
        merged, added = merge_payload(existing, incoming, rule)
        result["merge_ms"] = (time.perf_counter() - t0) * 1000
        result["merged_rows"] = len(merged.rows)
    else:
        result["merge_ms"] = 0
        result["merged_rows"] = result["rows"]

    # 阶段 3：write_csv_payload（写入临时文件）
    if existing.header:
        # 用 merged 如果有，否则用 existing
        payload_to_write = merged if n_incoming > 0 else existing
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "out.csv"
            t0 = time.perf_counter()
            write_csv_payload(tmp_path, payload_to_write, rule, dry_run=False)
            result["write_ms"] = (time.perf_counter() - t0) * 1000
    else:
        result["write_ms"] = 0

    result["total_ms"] = (
        result["read_parse_ms"] + result["merge_ms"] + result["write_ms"]
    )

    return result


def main():
    parser = argparse.ArgumentParser(description="CSV 合并性能分析")
    parser.add_argument(
        "--product",
        default="coin-binance-swap-candle-csv-1h",
        help="要分析的产品目录名",
    )
    parser.add_argument("--sample", type=int, default=50, help="采样文件数")
    parser.add_argument(
        "--data-root",
        default=None,
        help="数据根目录（默认从 user_config.json 读取）",
    )
    args = parser.parse_args()

    # 确定数据根目录
    if args.data_root:
        data_root = Path(args.data_root)
    else:
        config_path = Path(__file__).resolve().parent.parent / "user_config.json"
        with open(config_path) as f:
            data_root = Path(json.load(f)["data_root"])

    product_dir = data_root / args.product
    if not product_dir.exists():
        print(f"产品目录不存在: {product_dir}")
        sys.exit(1)

    # 查找规则
    rule = RULES.get(args.product)
    if rule is None:
        # 尝试去掉 -csv-1h 等后缀
        for name in RULES:
            if args.product.startswith(name):
                rule = RULES[name]
                break
    if rule is None:
        print(f"未找到产品规则: {args.product}，使用默认规则")
        from quantclass_sync_internal.models import DatasetRule
        rule = DatasetRule(name=args.product, encoding="utf-8", has_note=False,
                          key_cols=(), sort_cols=())

    # 收集 CSV 文件
    csv_files = sorted([p for p in product_dir.rglob("*.csv") if p.is_file()])
    total = len(csv_files)
    print(f"产品: {args.product}")
    print(f"规则: {rule.name} (key_cols={rule.key_cols}, sort_cols={rule.sort_cols})")
    print(f"CSV 文件总数: {total}")

    # 采样：按大小分层（小/中/大各取 1/3）
    csv_files_sorted_by_size = sorted(csv_files, key=lambda p: p.stat().st_size)
    n = min(args.sample, total)
    if n >= total:
        sample = csv_files
    else:
        # 分三层：小(前1/3)、中(中间1/3)、大(后1/3)
        third = total // 3
        small = csv_files_sorted_by_size[:third]
        medium = csv_files_sorted_by_size[third : 2 * third]
        large = csv_files_sorted_by_size[2 * third :]
        per_tier = n // 3
        sample = (
            random.sample(small, min(per_tier, len(small)))
            + random.sample(medium, min(per_tier, len(medium)))
            + random.sample(large, min(n - 2 * per_tier, len(large)))
        )

    print(f"采样: {len(sample)} 文件\n")

    # 逐文件分析
    results = []
    for i, path in enumerate(sample):
        try:
            r = profile_single_file(path, rule)
            results.append(r)
            if (i + 1) % 10 == 0:
                print(f"  进度: {i + 1}/{len(sample)}")
        except Exception as e:
            print(f"  跳过 {path.name}: {e}")

    if not results:
        print("无有效结果")
        return

    # 汇总
    print("\n" + "=" * 70)
    print("分阶段耗时汇总")
    print("=" * 70)

    phases = [
        ("raw_read_ms", "纯 I/O 读取"),
        ("read_parse_ms", "读取+解析"),
        ("merge_ms", "合并(去重+排序)"),
        ("write_ms", "写入"),
        ("total_ms", "总计(不含纯I/O)"),
    ]

    for key, label in phases:
        vals = [r[key] for r in results]
        total_ms = sum(vals)
        print(f"\n{label}:")
        print(f"  总计: {total_ms:.0f} ms")
        print(f"  均值: {statistics.mean(vals):.1f} ms")
        print(f"  中位: {statistics.median(vals):.1f} ms")
        print(f"  P90:  {sorted(vals)[int(len(vals) * 0.9)]:.1f} ms")
        print(f"  P99:  {sorted(vals)[min(int(len(vals) * 0.99), len(vals) - 1)]:.1f} ms")
        print(f"  最大: {max(vals):.1f} ms")

    # 占比分析
    print("\n" + "=" * 70)
    print("时间占比分析（不含纯 I/O 基线）")
    print("=" * 70)
    total_read = sum(r["read_parse_ms"] for r in results)
    total_merge = sum(r["merge_ms"] for r in results)
    total_write = sum(r["write_ms"] for r in results)
    grand_total = total_read + total_merge + total_write
    if grand_total > 0:
        print(f"  读取+解析: {total_read:.0f} ms ({total_read / grand_total * 100:.1f}%)")
        print(f"  合并:      {total_merge:.0f} ms ({total_merge / grand_total * 100:.1f}%)")
        print(f"  写入:      {total_write:.0f} ms ({total_write / grand_total * 100:.1f}%)")
        print(f"  总计:      {grand_total:.0f} ms")

    # 推算全量耗时
    avg_total = statistics.mean([r["total_ms"] for r in results])
    print(f"\n推算全量 {total} 文件: {avg_total * total / 1000:.1f} s")

    # 文件大小与耗时相关性
    print("\n" + "=" * 70)
    print("按文件大小分层（小/中/大各 ~1/3）")
    print("=" * 70)
    results_sorted = sorted(results, key=lambda r: r["size_kb"])
    third = len(results_sorted) // 3
    for label, chunk in [
        ("小文件", results_sorted[:third]),
        ("中文件", results_sorted[third : 2 * third]),
        ("大文件", results_sorted[2 * third :]),
    ]:
        if not chunk:
            continue
        avg_size = statistics.mean([r["size_kb"] for r in chunk])
        avg_rows = statistics.mean([r["rows"] for r in chunk])
        avg_read = statistics.mean([r["read_parse_ms"] for r in chunk])
        avg_merge = statistics.mean([r["merge_ms"] for r in chunk])
        avg_write = statistics.mean([r["write_ms"] for r in chunk])
        avg_t = statistics.mean([r["total_ms"] for r in chunk])
        print(
            f"  {label} (avg {avg_size:.0f} KB, {avg_rows:.0f} 行): "
            f"读 {avg_read:.0f}ms + 合 {avg_merge:.0f}ms + 写 {avg_write:.0f}ms = {avg_t:.0f}ms"
        )

    # 输出最慢的 5 个文件
    print("\n" + "=" * 70)
    print("最慢 5 个文件")
    print("=" * 70)
    for r in sorted(results, key=lambda r: -r["total_ms"])[:5]:
        print(
            f"  {r['file']}: {r['size_kb']:.0f}KB, {r['rows']}行, "
            f"读 {r['read_parse_ms']:.0f}ms + 合 {r['merge_ms']:.0f}ms + 写 {r['write_ms']:.0f}ms = {r['total_ms']:.0f}ms"
        )


if __name__ == "__main__":
    main()
