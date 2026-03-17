"""SyncStats.skipped_empty_split 计数测试。"""

import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.file_sync import sync_daily_aggregate_file


class TestSkippedEmptySplit(unittest.TestCase):
    """验证空 split_value 行被跳过时 stats.skipped_empty_split 正确递增。"""

    def _write_csv(self, path: Path, lines, encoding="gb18030"):
        """将行列表写入 CSV 文件（添加备注行模拟真实格式）。"""
        # stock-trading-data-pro 有 has_note=True，首行为备注行
        content = "\n".join(lines) + "\n"
        path.write_bytes(content.encode(encoding))

    def test_empty_split_value_counted(self):
        """含空 split_value 的行应被计入 skipped_empty_split，不写入目标文件。"""
        with tempfile.TemporaryDirectory() as d:
            data_root = Path(d)
            product = "stock-trading-data-pro"
            # 聚合日文件，文件名即日期
            src = data_root / "2026-01-02.csv"
            # 首行备注，第二行表头，后续数据行
            # 股票代码为空的行 -> split_value 为空 -> 应被跳过计数
            self._write_csv(src, [
                "数据说明",
                "股票代码,交易日期,收盘价",
                "sh600000,2026-01-02,10.5",  # 正常行
                ",2026-01-02,99.0",           # 空股票代码 -> 应跳过
                "sh600001,2026-01-02,20.0",   # 正常行
                "   ,2026-01-02,88.0",        # 仅空白 -> normalize 后为空 -> 应跳过
            ])

            stats, _ = sync_daily_aggregate_file(
                src=src,
                product=product,
                data_root=data_root,
                dry_run=False,
            )

            # 2 行空 split_value 应被计数
            self.assertEqual(stats.skipped_empty_split, 2)
            # 2 行正常行应生成对应文件
            self.assertGreater(stats.created_files + stats.updated_files, 0)

    def test_no_empty_split_value(self):
        """无空 split_value 行时，skipped_empty_split 应为 0。"""
        with tempfile.TemporaryDirectory() as d:
            data_root = Path(d)
            product = "stock-trading-data-pro"
            src = data_root / "2026-01-02.csv"
            self._write_csv(src, [
                "数据说明",
                "股票代码,交易日期,收盘价",
                "sh600000,2026-01-02,10.5",
                "sh600001,2026-01-02,20.0",
            ])

            stats, _ = sync_daily_aggregate_file(
                src=src,
                product=product,
                data_root=data_root,
                dry_run=False,
            )

            self.assertEqual(stats.skipped_empty_split, 0)

    def test_merge_accumulates_skipped_empty_split(self):
        """SyncStats.merge 应正确累加 skipped_empty_split 字段。"""
        from quantclass_sync_internal.models import SyncStats

        a = SyncStats(skipped_empty_split=3)
        b = SyncStats(skipped_empty_split=5)
        a.merge(b)
        self.assertEqual(a.skipped_empty_split, 8)


if __name__ == "__main__":
    unittest.main()
