"""
追加快捷路径 vs 完整合并 基准对比。

在真实数据文件的临时副本上测量两条路径的耗时差异。
不修改原始数据文件。

用法：python3 scripts/benchmark_fast_path.py [--product PRODUCT] [--sample N]
"""

import argparse
import json
import random
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quantclass_sync_internal.csv_engine import (
    read_csv_payload,
    resolve_sort_indices,
    row_sort_key,
    sortable_value,
    sync_payload_to_target,
)
from quantclass_sync_internal.models import CsvPayload, DatasetRule, RULES


def make_fake_incoming(existing: CsvPayload, rule: DatasetRule) -> CsvPayload:
    """构造 1 行比已有数据更新的 incoming payload。"""
    if not existing.rows or not existing.header:
        return existing

    sort_indices = resolve_sort_indices(existing.header, rule)
    if not sort_indices:
        return existing

    # 取最后一行，把排序列的日期/时间 +1 单位
    last_row = list(existing.rows[-1])
    from datetime import datetime, timedelta
    for idx in sort_indices:
        if idx < len(last_row):
            val = last_row[idx].strip()
            # YYYY-MM-DD HH:MM:SS（含时间）
            if len(val) == 19 and val[4] == "-" and val[10] == " ":
                try:
                    dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                    last_row[idx] = (dt + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
            # YYYY-MM-DD（纯日期）
            elif len(val) == 10 and val[4] == "-" and val[7] == "-":
                try:
                    dt = datetime.strptime(val, "%Y-%m-%d")
                    last_row[idx] = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
                except Exception:
                    pass

    return CsvPayload(
        note=existing.note,
        header=list(existing.header),
        rows=[last_row],
        encoding=existing.encoding,
        delimiter=existing.delimiter,
    )


def benchmark_file(src_path: Path, rule: DatasetRule):
    """对单个文件做快捷路径 vs 完整合并的对比。"""
    existing = read_csv_payload(src_path, preferred_encoding=rule.encoding)
    if not existing.rows or not existing.header:
        return None

    incoming = make_fake_incoming(existing, rule)
    if not incoming.rows:
        return None

    result = {
        "file": src_path.name,
        "rows": len(existing.rows),
        "size_kb": src_path.stat().st_size / 1024,
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        # --- 快捷路径 ---
        fast_target = Path(tmpdir) / "fast.csv"
        shutil.copy2(src_path, fast_target)

        t0 = time.perf_counter()
        status_f, added_f, audit_f = sync_payload_to_target(
            incoming, fast_target, rule, dry_run=False
        )
        result["fast_ms"] = (time.perf_counter() - t0) * 1000
        result["fast_status"] = status_f
        result["fast_path_hit"] = audit_f.checked_files == 1 and status_f == "updated"

        # --- 完整合并（用无 sort_cols 的规则强制走完整路径）---
        full_target = Path(tmpdir) / "full.csv"
        shutil.copy2(src_path, full_target)
        full_rule = DatasetRule(
            name=rule.name + "-full",
            encoding=rule.encoding,
            has_note=rule.has_note,
            key_cols=rule.key_cols,
            sort_cols=(),  # 无排序列 -> 不走快捷路径
        )

        t0 = time.perf_counter()
        status_m, added_m, audit_m = sync_payload_to_target(
            incoming, full_target, full_rule, dry_run=False
        )
        result["full_ms"] = (time.perf_counter() - t0) * 1000
        result["speedup"] = result["full_ms"] / max(result["fast_ms"], 0.01)

    return result


def main():
    parser = argparse.ArgumentParser(description="追加快捷路径基准对比")
    parser.add_argument("--product", default="stock-trading-data-pro")
    parser.add_argument("--sample", type=int, default=50)
    parser.add_argument("--data-root", default=None)
    args = parser.parse_args()

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

    rule = RULES.get(args.product)
    if rule is None:
        for name in RULES:
            if args.product.startswith(name):
                rule = RULES[name]
                break
    if rule is None:
        print(f"未找到产品规则: {args.product}")
        sys.exit(1)

    csv_files = sorted([p for p in product_dir.rglob("*.csv") if p.is_file()])
    total = len(csv_files)
    print(f"产品: {args.product}")
    print(f"规则: sort_cols={rule.sort_cols}, key_cols={rule.key_cols}")
    print(f"CSV 文件: {total}")

    n = min(args.sample, total)
    sample = random.sample(csv_files, n) if n < total else csv_files
    print(f"采样: {len(sample)} 文件\n")

    results = []
    for i, path in enumerate(sample):
        try:
            r = benchmark_file(path, rule)
            if r:
                results.append(r)
            if (i + 1) % 10 == 0:
                print(f"  进度: {i + 1}/{len(sample)}")
        except Exception as e:
            print(f"  跳过 {path.name}: {e}")

    if not results:
        print("无有效结果")
        return

    hit_count = sum(1 for r in results if r["fast_path_hit"])
    miss_count = len(results) - hit_count

    print(f"\n{'=' * 60}")
    print(f"快捷路径命中率: {hit_count}/{len(results)} ({hit_count / len(results) * 100:.0f}%)")
    print(f"{'=' * 60}")

    if hit_count > 0:
        hits = [r for r in results if r["fast_path_hit"]]
        fast_times = [r["fast_ms"] for r in hits]
        full_times = [r["full_ms"] for r in hits]
        speedups = [r["speedup"] for r in hits]

        print(f"\n命中快捷路径的文件 ({hit_count} 个):")
        print(f"  快捷路径: 均值 {statistics.mean(fast_times):.2f}ms, 中位 {statistics.median(fast_times):.2f}ms")
        print(f"  完整合并: 均值 {statistics.mean(full_times):.1f}ms, 中位 {statistics.median(full_times):.1f}ms")
        print(f"  加速比:   均值 {statistics.mean(speedups):.1f}x, 中位 {statistics.median(speedups):.1f}x")

        total_fast = sum(fast_times)
        total_full = sum(full_times)
        print(f"\n  采样总计: 快捷 {total_fast:.0f}ms vs 完整 {total_full:.0f}ms")
        print(f"  推算全量 {total} 文件:")
        est_fast = statistics.mean(fast_times) * total / 1000
        est_full = statistics.mean(full_times) * total / 1000
        print(f"    快捷路径: {est_fast:.1f}s")
        print(f"    完整合并: {est_full:.1f}s")
        print(f"    节省:     {est_full - est_fast:.1f}s ({(1 - est_fast / est_full) * 100:.0f}%)")

    if miss_count > 0:
        misses = [r for r in results if not r["fast_path_hit"]]
        print(f"\n未命中的文件 ({miss_count} 个):")
        for r in misses[:5]:
            print(f"  {r['file']}: status={r['fast_status']}")

    # 最快和最慢
    if hit_count >= 5:
        print(f"\n最慢 5 个（快捷路径）:")
        for r in sorted(hits, key=lambda x: -x["fast_ms"])[:5]:
            print(f"  {r['file']}: {r['rows']}行, 快捷 {r['fast_ms']:.2f}ms vs 完整 {r['full_ms']:.1f}ms ({r['speedup']:.0f}x)")


if __name__ == "__main__":
    main()
