"""data_root 校验、CSV 日期推断、discover 点目录过滤的测试。"""

import contextlib
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from quantclass_sync_internal.config import (
    discover_local_products,
    ensure_data_root_ready,
    validate_data_root_not_product_dir,
)
from quantclass_sync_internal.data_query import (
    _check_temporal_integrity,
    infer_local_date_from_csv,
)
from quantclass_sync_internal.models import DatasetRule


def _make_rule(*, date_col="交易日期", encoding="gb18030", has_note=True):
    """创建测试用的 DatasetRule。"""
    return DatasetRule(
        name="test-product",
        encoding=encoding,
        has_note=has_note,
        key_cols=("股票代码", date_col),
        sort_cols=(date_col,),
        date_filter_col=date_col,
    )


# === data_root 校验 ===


class TestValidateDataRootNotProductDir(unittest.TestCase):
    """validate_data_root_not_product_dir 信号检测。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_timestamp_signal_triggers(self):
        """信号 1：根目录下有 timestamp.txt -> 报错。"""
        (self.root / "timestamp.txt").write_text("2026-03-01,2026-03-01 10:00:00\n")
        with self.assertRaises(RuntimeError) as cm:
            validate_data_root_not_product_dir(self.root)
        self.assertIn("timestamp.txt", str(cm.exception))
        self.assertIn("产品子目录", str(cm.exception))

    def test_date_dirs_signal_triggers(self):
        """信号 2：>= 2 个日期子目录 -> 报错。"""
        (self.root / "2026-03-01").mkdir()
        (self.root / "2026-03-02").mkdir()
        with self.assertRaises(RuntimeError) as cm:
            validate_data_root_not_product_dir(self.root)
        self.assertIn("日期子目录", str(cm.exception))

    def test_one_date_dir_passes(self):
        """仅 1 个日期子目录，不触发。"""
        (self.root / "2026-03-01").mkdir()
        # 不应抛异常
        validate_data_root_not_product_dir(self.root)

    def test_normal_product_dirs_pass(self):
        """正常产品目录结构通过。"""
        (self.root / "stock-trading-data").mkdir()
        (self.root / "stock-etf-trading-data").mkdir()
        validate_data_root_not_product_dir(self.root)

    def test_empty_dir_passes(self):
        """空目录通过（首次使用场景）。"""
        validate_data_root_not_product_dir(self.root)

    def test_ensure_data_root_ready_calls_validation(self):
        """ensure_data_root_ready 内部调用校验。"""
        (self.root / "timestamp.txt").write_text("2026-03-01\n")
        with self.assertRaises(RuntimeError):
            ensure_data_root_ready(self.root)

    def test_ensure_data_root_create_if_missing_empty_dir(self):
        """create_if_missing=True 新建空目录，两个信号都不触发。"""
        new_dir = self.root / "brand-new"
        result = ensure_data_root_ready(new_dir, create_if_missing=True)
        self.assertTrue(result.exists())


# === CSV 日期推断 ===


class TestInferLocalDateFromCsv(unittest.TestCase):
    """infer_local_date_from_csv 推断逻辑。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_infer_with_gb18030_csv(self):
        """有 CSV（gb18030 编码）无 timestamp -> 正确推断最大日期。"""
        rule = _make_rule(encoding="gb18030", has_note=True)
        pdir = self.data_root / "test-product"
        pdir.mkdir()
        # 写入 gb18030 编码 CSV（含备注行，模拟真实数据格式）
        csv_content = "温馨提示：仅供学习\n股票代码,交易日期\n000001,2026-03-10\n000001,2026-03-13\n"
        (pdir / "000001.csv").write_bytes(csv_content.encode("gb18030"))
        csv_content2 = "温馨提示：仅供学习\n股票代码,交易日期\n000002,2026-03-11\n000002,2026-03-12\n"
        (pdir / "000002.csv").write_bytes(csv_content2.encode("gb18030"))

        result = infer_local_date_from_csv(self.data_root, "test-product", rule)
        self.assertEqual(result, "2026-03-13")

    def test_infer_no_csv_returns_none(self):
        """无 CSV 文件 -> 返回 None。"""
        rule = _make_rule()
        pdir = self.data_root / "test-product"
        pdir.mkdir()

        result = infer_local_date_from_csv(self.data_root, "test-product", rule)
        self.assertIsNone(result)

    def test_infer_no_rule_returns_none(self):
        """无 rule -> 返回 None。"""
        result = infer_local_date_from_csv(self.data_root, "test-product", None)
        self.assertIsNone(result)

    def test_infer_no_date_col_returns_none(self):
        """rule 无日期列 -> 返回 None。"""
        rule = DatasetRule(
            name="test-product",
            encoding="utf-8-sig",
            has_note=False,
            key_cols=("col1",),
            sort_cols=(),
            date_filter_col=None,
        )
        pdir = self.data_root / "test-product"
        pdir.mkdir()
        (pdir / "data.csv").write_text("col1\nabc\n", encoding="utf-8")

        result = infer_local_date_from_csv(self.data_root, "test-product", rule)
        self.assertIsNone(result)

    def test_infer_utf8_csv(self):
        """UTF-8 编码 CSV 同样可推断。"""
        rule = _make_rule(encoding="utf-8-sig", has_note=False)
        pdir = self.data_root / "test-product"
        pdir.mkdir()
        (pdir / "data.csv").write_text(
            "股票代码,交易日期\nA,2026-03-15\nB,2026-03-14\n", encoding="utf-8"
        )

        result = infer_local_date_from_csv(self.data_root, "test-product", rule)
        self.assertEqual(result, "2026-03-15")


# === 回补集成（orchestrator） ===


class TestCatchupCsvInference(unittest.TestCase):
    """回补模式下 CSV 推断的集成测试。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _make_plan_and_ctx(self, product_name="stock-trading-data-pro"):
        """创建测试用的 plan 和 command context。"""
        from quantclass_sync_internal.models import (
            CommandContext, ProductPlan, RunReport, RULES,
        )
        from quantclass_sync_internal.constants import STRATEGY_MERGE_KNOWN
        plan = ProductPlan(name=product_name, strategy=STRATEGY_MERGE_KNOWN)
        ctx = CommandContext(
            data_root=self.data_root,
            api_base="https://fake.api",
            run_id="test-run",
            work_dir=self.data_root / ".work",
            dry_run=False,
        )
        report = RunReport(
            run_id="test-run",
            schema_version="2",
            started_at="2026-03-18T10:00:00",
            mode="local",
        )
        return plan, ctx, report

    def _write_pro_csv(self, filename, date_value):
        """写入一个 stock-trading-data-pro 格式的 CSV 文件（gb18030 + 备注行）。"""
        pdir = self.data_root / "stock-trading-data-pro"
        pdir.mkdir(parents=True, exist_ok=True)
        content = f"温馨提示：仅供学习\n股票代码,交易日期\n000001,{date_value}\n"
        (pdir / filename).write_bytes(content.encode("gb18030"))

    @patch("quantclass_sync_internal.orchestrator.get_latest_times")
    @patch("quantclass_sync_internal.orchestrator.read_local_timestamp_date")
    def test_no_timestamp_with_csv_triggers_catchup(self, mock_ts, mock_latest):
        """无 timestamp + 有 CSV -> 用推断日期走回补。"""
        from quantclass_sync_internal.orchestrator import _resolve_requested_dates_for_plan

        mock_ts.return_value = None  # 无 timestamp
        mock_latest.return_value = ["2026-03-15", "2026-03-16", "2026-03-17"]

        plan, ctx, report = self._make_plan_and_ctx()
        self._write_pro_csv("data.csv", "2026-03-13")

        dates, skipped = _resolve_requested_dates_for_plan(
            plan, ctx, hid="test", headers={},
            requested_date_time="", force_update=False,
            report=report, t_product_start=time.time(),
            catch_up_to_latest=True,
        )
        # 不应被跳过，应返回需要回补的日期（仅 2026-03-13 之后的日期）
        # stock-trading-data-pro 是 BUSINESS_DAY_ONLY_PRODUCTS，会过滤非业务日
        # 2026-03-15/16/17 分别为周日/周一/周二；周日被过滤，只剩 03-16 和 03-17
        self.assertFalse(skipped)
        self.assertEqual(sorted(dates), ["2026-03-16", "2026-03-17"])

    @patch("quantclass_sync_internal.orchestrator.get_latest_times")
    @patch("quantclass_sync_internal.orchestrator.read_local_timestamp_date")
    def test_no_timestamp_with_csv_already_latest_skips(self, mock_ts, mock_latest):
        """无 timestamp + 有 CSV + 已是最新 -> skip。"""
        from quantclass_sync_internal.orchestrator import _resolve_requested_dates_for_plan

        mock_ts.return_value = None
        mock_latest.return_value = ["2026-03-17"]

        plan, ctx, report = self._make_plan_and_ctx()
        # CSV 数据已经到 03-17（和 API 一样新）
        self._write_pro_csv("data.csv", "2026-03-17")

        dates, skipped = _resolve_requested_dates_for_plan(
            plan, ctx, hid="test", headers={},
            requested_date_time="", force_update=False,
            report=report, t_product_start=time.time(),
            catch_up_to_latest=True,
        )
        # 应被跳过
        self.assertTrue(skipped)
        self.assertEqual(dates, [])

    @patch("quantclass_sync_internal.orchestrator.get_latest_times")
    @patch("quantclass_sync_internal.orchestrator.read_local_timestamp_date")
    def test_no_timestamp_no_csv_downloads_latest(self, mock_ts, mock_latest):
        """无 timestamp + 无 CSV -> 只下载 latest。"""
        from quantclass_sync_internal.orchestrator import _resolve_requested_dates_for_plan

        mock_ts.return_value = None
        mock_latest.return_value = ["2026-03-17"]

        plan, ctx, report = self._make_plan_and_ctx()
        # 产品目录为空，无 CSV
        pdir = self.data_root / "stock-trading-data-pro"
        pdir.mkdir(parents=True)

        dates, skipped = _resolve_requested_dates_for_plan(
            plan, ctx, hid="test", headers={},
            requested_date_time="", force_update=False,
            report=report, t_product_start=time.time(),
            catch_up_to_latest=True,
        )
        self.assertFalse(skipped)
        self.assertEqual(dates, ["2026-03-17"])

    @patch("quantclass_sync_internal.orchestrator.get_latest_times")
    @patch("quantclass_sync_internal.orchestrator.read_local_timestamp_date")
    def test_mirror_unknown_product_no_inference(self, mock_ts, mock_latest):
        """mirror_unknown 产品（无 RULES）有 CSV 也不触发推断，直接下载 latest。"""
        from quantclass_sync_internal.orchestrator import _resolve_requested_dates_for_plan
        from quantclass_sync_internal.models import ProductPlan
        from quantclass_sync_internal.constants import STRATEGY_MIRROR_UNKNOWN

        mock_ts.return_value = None
        mock_latest.return_value = ["2026-03-17"]

        # 使用一个不在 RULES 中的产品名
        plan = ProductPlan(name="unknown-mirror-product", strategy=STRATEGY_MIRROR_UNKNOWN)
        _, ctx, report = self._make_plan_and_ctx()
        # 创建带 CSV 的产品目录
        pdir = self.data_root / "unknown-mirror-product"
        pdir.mkdir(parents=True)
        (pdir / "data.csv").write_text("col1,col2\n1,2\n", encoding="utf-8")

        dates, skipped = _resolve_requested_dates_for_plan(
            plan, ctx, hid="test", headers={},
            requested_date_time="", force_update=False,
            report=report, t_product_start=time.time(),
            catch_up_to_latest=True,
        )
        # mirror_unknown 无 rule，不推断，直接下载 latest
        self.assertFalse(skipped)
        self.assertEqual(dates, ["2026-03-17"])


# === 健康检查 temporal integrity ===


class TestTemporalIntegrityInferredMode(unittest.TestCase):
    """_check_temporal_integrity 推断模式行为。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_inferred_mode_skips_check7(self):
        """无 timestamp + 有 CSV -> 推断模式下 #7 跳过。"""
        product = "test-crypto"
        rule = _make_rule(date_col="candle_begin_time", encoding="utf-8-sig", has_note=False)
        pdir = self.data_root / product
        pdir.mkdir()
        # 有 CSV 数据但无 timestamp.txt
        (pdir / "data.csv").write_text(
            "股票代码,candle_begin_time\nBTC,2026-03-10\nBTC,2026-03-15\n",
            encoding="utf-8",
        )
        # 不写 timestamp.txt

        issues = _check_temporal_integrity(self.data_root, product, rule, None)
        # 推断模式下 #7（date_exceeds_timestamp / timestamp_data_gap）不应出现
        type_set = {i["type"] for i in issues}
        self.assertNotIn("date_exceeds_timestamp", type_set)
        self.assertNotIn("timestamp_data_gap", type_set)

    def test_inferred_mode_check8_runs(self):
        """无 timestamp + 有 CSV + coin 产品有缺口 -> #8 正常检测。"""
        product = "coin-test"
        rule = DatasetRule(
            name=product,
            encoding="utf-8-sig",
            has_note=False,
            key_cols=("symbol", "candle_begin_time"),
            sort_cols=("candle_begin_time",),
            date_filter_col="candle_begin_time",
        )
        pdir = self.data_root / product
        pdir.mkdir()
        # 制造有缺口的日期数据（03-10, 03-12，缺 03-11）
        csv_content = "symbol,candle_begin_time\nBTC,2026-03-10\nBTC,2026-03-12\n"
        (pdir / "data.csv").write_text(csv_content, encoding="utf-8")
        # 不写 timestamp.txt

        issues = _check_temporal_integrity(self.data_root, product, rule, None)
        # coin 产品用自然日检查，缺 03-11 应报 missing_trading_days
        missing_issues = [i for i in issues if i["type"] == "missing_trading_days"]
        self.assertEqual(len(missing_issues), 1)
        self.assertIn("2026-03-11", missing_issues[0]["detail"])

    def test_normal_mode_check7_runs(self):
        """有 timestamp + CSV 日期超出 timestamp -> #7 正常报错。"""
        from quantclass_sync_internal.constants import TIMESTAMP_FILE_NAME

        product = "test-product"
        rule = _make_rule(encoding="utf-8-sig", has_note=False)
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-03-10,2026-03-10 10:00:00\n")
        (pdir / "data.csv").write_text(
            "股票代码,交易日期\nA,2026-03-15\n", encoding="utf-8"
        )

        issues = _check_temporal_integrity(self.data_root, product, rule, None)
        type_set = {i["type"] for i in issues}
        self.assertIn("date_exceeds_timestamp", type_set)


# === discover_local_products 点目录过滤 ===


class TestDiscoverSkipsDotDirs(unittest.TestCase):
    """discover_local_products 跳过点目录。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_dot_dirs_excluded(self):
        """点目录（.cache, .quantclass_sync）被跳过。"""
        # 创建点目录并放入数据文件
        cache_dir = self.data_root / ".cache"
        cache_dir.mkdir()
        (cache_dir / "data.csv").write_text("col1\nval\n")

        qs_dir = self.data_root / ".quantclass_sync"
        qs_dir.mkdir()
        (qs_dir / "status.csv").write_text("col1\nval\n")

        # 创建正常产品目录
        normal_dir = self.data_root / "stock-product"
        normal_dir.mkdir()
        (normal_dir / "data.csv").write_text("col1\nval\n")

        discovered = discover_local_products(self.data_root, ["stock-product"])
        names = [d.name for d in discovered]
        self.assertIn("stock-product", names)
        self.assertNotIn(".cache", names)
        self.assertNotIn(".quantclass_sync", names)

    def test_normal_products_unaffected(self):
        """正常产品目录不受影响。"""
        for name in ["product-a", "product-b"]:
            d = self.data_root / name
            d.mkdir()
            (d / "data.csv").write_text("col1\nval\n")

        discovered = discover_local_products(
            self.data_root, ["product-a", "product-b"]
        )
        names = {d.name for d in discovered}
        self.assertEqual(names, {"product-a", "product-b"})


if __name__ == "__main__":
    unittest.main()
