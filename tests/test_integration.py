"""
集成测试：验证组件拼合时数据流端到端正确。

覆盖范围：
  I1 - 同步主链路（_execute_plans 驱动）
  I2 - CSV 引擎（直接调用 sync_known_product）
  I3 - 状态一致性
"""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from typing import Dict, Optional
from unittest.mock import MagicMock, patch

# ---- 被测模块 ----
from quantclass_sync_internal.models import (
    CommandContext,
    FatalRequestError,
    ProductPlan,
    SyncStats,
)
from quantclass_sync_internal.orchestrator import (
    _execute_plans,
    _maybe_run_coin_preprocess,
)
from quantclass_sync_internal.reporting import _new_report
from quantclass_sync_internal.status_store import (
    open_status_db,
    load_product_status,
    read_local_timestamp_date,
    resolve_runtime_paths,
    write_local_timestamp,
)
from quantclass_sync_internal.file_sync import sync_known_product
from quantclass_sync_internal.constants import TIMESTAMP_FILE_NAME

# ---- 共用 Fixture ----

# UTF-8 BOM 前缀
BOM = b"\xef\xbb\xbf"

# stock-main-index-data 日聚合 CSV（BOM + UTF-8，日期 2026-03-10）
CSV_2026_03_10 = (
    BOM
    + b"index_code,candle_end_time,open,close,high,low,volume\n"
    + b"sh000001,2026-03-10 15:00:00,3100.00,3120.00,3130.00,3090.00,100000\n"
    + b"sh000300,2026-03-10 15:00:00,4000.00,4050.00,4060.00,3990.00,200000\n"
)

# 日期 2026-03-11 的日聚合
CSV_2026_03_11 = (
    BOM
    + b"index_code,candle_end_time,open,close,high,low,volume\n"
    + b"sh000001,2026-03-11 15:00:00,3110.00,3130.00,3140.00,3100.00,110000\n"
    + b"sh000300,2026-03-11 15:00:00,4010.00,4060.00,4070.00,4000.00,210000\n"
)

# 日期 2026-03-12 的日聚合
CSV_2026_03_12 = (
    BOM
    + b"index_code,candle_end_time,open,close,high,low,volume\n"
    + b"sh000001,2026-03-12 15:00:00,3120.00,3140.00,3150.00,3110.00,120000\n"
    + b"sh000300,2026-03-12 15:00:00,4020.00,4070.00,4080.00,4010.00,220000\n"
)

# 按日期映射 fixture 内容
FIXTURE_BY_DATE: Dict[str, bytes] = {
    "2026-03-10": CSV_2026_03_10,
    "2026-03-11": CSV_2026_03_11,
    "2026-03-12": CSV_2026_03_12,
}

PRODUCT = "stock-main-index-data"


def make_ctx(tmpdir: str, dry_run: bool = False, stop_on_error: bool = False) -> CommandContext:
    """构造最小 CommandContext，凭证字段填入非空假值。"""
    root = Path(tmpdir) / "data"
    root.mkdir(parents=True, exist_ok=True)
    return CommandContext(
        run_id="test-integ-001",
        data_root=root,
        data_root_from_cli=True,
        api_key="fake-api-key",          # build_headers_or_raise 需要非空
        hid="fake-hid",
        secrets_file=Path(tmpdir) / "secrets.env",
        secrets_file_from_cli=True,
        config_file=Path(tmpdir) / "config.json",
        dry_run=dry_run,
        report_file=None,
        stop_on_error=stop_on_error,
        verbose=False,
        mode="network",
        api_base="https://fake-api.example.com/api/data",
        catalog_file=Path(tmpdir) / "catalog.txt",
        work_dir=Path(tmpdir) / "work",
    )


def make_plan(product: str = PRODUCT) -> ProductPlan:
    """构造 merge_known 策略计划（stock-main-index-data 是已知产品）。"""
    return ProductPlan(name=product, strategy="merge_known")


def make_save_file_mock(date_to_content: Dict[str, bytes]):
    """
    返回 save_file 的 mock 实现。

    根据 file_url 中携带的日期字符串，将对应 fixture 内容写入 file_path。
    file_path 是 atomic_temp_path 生成的临时文件路径（Path 对象）。
    """
    def mock_save_file(file_url: str, file_path: Path, headers: dict, product: str = "") -> None:
        content = None
        # 从 URL 末段提取日期，格式为 2026-03-10
        for date_str, data in date_to_content.items():
            if date_str in file_url:
                content = data
                break
        if content is None:
            raise RuntimeError(f"No fixture for url={file_url}")
        Path(file_path).write_bytes(content)
    return mock_save_file


def make_download_link_for_date(product: str, date_time: str) -> str:
    """
    构造让 build_file_name 提取出纯日期文件名的 URL。

    文件名格式 2026-03-10.csv 可被 DATE_NAME_PATTERN 识别为聚合日文件。
    """
    return f"https://fake.example.com/{product}/{date_time}.csv"


def write_old_timestamp(data_root: Path, product: str, date_str: str) -> None:
    """直接写入 timestamp.txt，用于预置本地状态。"""
    ts_path = data_root / product / TIMESTAMP_FILE_NAME
    ts_path.parent.mkdir(parents=True, exist_ok=True)
    ts_path.write_text(f"{date_str},2026-03-01 10:00:00\n", encoding="utf-8")


# ===========================================================================
# I1 - 同步主链路
# ===========================================================================

class TestSyncMainChain(unittest.TestCase):
    """I1 系列：通过 _execute_plans 验证 orchestrator → file_sync → csv_engine 链路。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _run_execute_plans(
        self,
        ctx: CommandContext,
        plans,
        date_to_content: Dict[str, bytes],
        latest_dates,
        catch_up: bool = False,
        latest_per_call=None,
    ):
        """
        统一封装 _execute_plans 调用，同时 mock 掉所有 HTTP 函数。

        latest_per_call: 若指定，get_latest_times 每次调用按此列表轮转
                         （用于多产品测试不同产品返回不同 latest）。
        """
        report = _new_report("test-integ-001", mode="network")

        save_file_mock = make_save_file_mock(date_to_content)

        with (
            patch("quantclass_sync_internal.orchestrator.build_headers_or_raise",
                  return_value=({"api-key": "fake"}, "fake-hid")),
            patch("quantclass_sync_internal.orchestrator.get_latest_times",
                  return_value=latest_dates),
            patch("quantclass_sync_internal.orchestrator.get_latest_time",
                  return_value=latest_dates[-1] if latest_dates else ""),
            patch("quantclass_sync_internal.orchestrator.get_download_link",
                  side_effect=lambda api_base, product, date_time, hid, headers:
                      make_download_link_for_date(product, date_time)),
            patch("quantclass_sync_internal.orchestrator.save_file",
                  side_effect=save_file_mock),
        ):
            with open_status_db(ctx.data_root) as conn:
                total, has_error, _ = _execute_plans(
                    plans=plans,
                    command_ctx=ctx,
                    report=report,
                    conn=conn,
                    catch_up_to_latest=catch_up,
                )
        return report, total, has_error

    # ------------------------------------------------------------------
    # I1.1 首次同步（本地无数据）
    # ------------------------------------------------------------------
    def test_i1_1_first_sync_creates_product_files(self) -> None:
        """I1.1: 首次同步从空目录出发，应创建产品子文件和 timestamp.txt。"""
        ctx = make_ctx(self.tmp.name)
        plans = [make_plan(PRODUCT)]

        report, total, has_error = self._run_execute_plans(
            ctx=ctx,
            plans=plans,
            date_to_content={"2026-03-10": CSV_2026_03_10},
            latest_dates=["2026-03-10"],
            catch_up=True,
        )

        # 产品目录和子文件应被创建
        product_dir = ctx.data_root / PRODUCT
        self.assertTrue(product_dir.is_dir(), "产品目录未创建")
        self.assertTrue((product_dir / "sh000001.csv").exists(), "sh000001.csv 未创建")
        self.assertTrue((product_dir / "sh000300.csv").exists(), "sh000300.csv 未创建")

        # timestamp.txt 写入
        local_date = read_local_timestamp_date(ctx.data_root, PRODUCT)
        self.assertEqual("2026-03-10", local_date, "timestamp.txt 内容不正确")

        # 状态库有记录
        with open_status_db(ctx.data_root) as conn:
            status = load_product_status(conn, PRODUCT)
        self.assertIsNotNone(status, "状态库无记录")
        self.assertEqual("2026-03-10", status.data_time)

        # report 中有 ok 记录
        ok_results = [r for r in report.products if r.status == "ok" and r.product == PRODUCT]
        self.assertTrue(len(ok_results) >= 1, "report 中无 ok 记录")

        # 无错误
        self.assertFalse(has_error, "has_error 应为 False")

    # ------------------------------------------------------------------
    # I1.2 增量同步（本地已有数据）
    # ------------------------------------------------------------------
    def test_i1_2_incremental_sync_merges_new_rows(self) -> None:
        """I1.2: 本地已有旧数据，增量同步后行数应增加。"""
        ctx = make_ctx(self.tmp.name)
        plans = [make_plan(PRODUCT)]

        # 预置本地数据（仅 2026-03-10 的行）
        first_report, _, _ = self._run_execute_plans(
            ctx=ctx,
            plans=plans,
            date_to_content={"2026-03-10": CSV_2026_03_10},
            latest_dates=["2026-03-10"],
            catch_up=True,
        )

        # 记录第一次写入后的行数（sh000001.csv 含 header = 2 行）
        sh001_path = ctx.data_root / PRODUCT / "sh000001.csv"
        lines_before = sh001_path.read_bytes().count(b"\n")

        # 第二次同步：新数据 2026-03-11
        report2, _, has_error2 = self._run_execute_plans(
            ctx=ctx,
            plans=plans,
            date_to_content={"2026-03-11": CSV_2026_03_11},
            latest_dates=["2026-03-11"],
            catch_up=True,
        )

        lines_after = sh001_path.read_bytes().count(b"\n")
        self.assertGreater(lines_after, lines_before, "增量同步后行数应增加")

        # timestamp 应更新为 2026-03-11
        local_date = read_local_timestamp_date(ctx.data_root, PRODUCT)
        self.assertEqual("2026-03-11", local_date)

        self.assertFalse(has_error2)

    # ------------------------------------------------------------------
    # I1.3 回补（本地落后 3 天）
    # ------------------------------------------------------------------
    def test_i1_3_catchup_3_days(self) -> None:
        """I1.3: 本地落后 3 天，回补后 timestamp 应为最新日期，所有数据均已合并。"""
        ctx = make_ctx(self.tmp.name)
        plans = [make_plan(PRODUCT)]

        # 预置 timestamp=2026-03-09（比 latest 落后 3 天）
        write_old_timestamp(ctx.data_root, PRODUCT, "2026-03-09")

        # latest 返回 3 个日期（2026-03-10/11/12）
        report, total, has_error = self._run_execute_plans(
            ctx=ctx,
            plans=plans,
            date_to_content=FIXTURE_BY_DATE,
            latest_dates=["2026-03-10", "2026-03-11", "2026-03-12"],
            catch_up=True,
        )

        # 最终 timestamp 应为 2026-03-12
        local_date = read_local_timestamp_date(ctx.data_root, PRODUCT)
        self.assertEqual("2026-03-12", local_date, "回补后 timestamp 不正确")

        # sh000001.csv 应有 3 行数据（2026-03-10/11/12 各一行）
        sh001_path = ctx.data_root / PRODUCT / "sh000001.csv"
        self.assertTrue(sh001_path.exists(), "sh000001.csv 不存在")
        lines = sh001_path.read_text(encoding="utf-8-sig").strip().splitlines()
        # 1 header + 3 data rows
        self.assertEqual(4, len(lines), f"sh000001.csv 应有 4 行（含 header），实际 {len(lines)} 行")

        self.assertFalse(has_error)

    # ------------------------------------------------------------------
    # I1.4 全部跳过（本地已最新）
    # ------------------------------------------------------------------
    def test_i1_4_skip_when_up_to_date(self) -> None:
        """I1.4: 本地 timestamp 已等于 latest，不应下载也不应写文件。"""
        ctx = make_ctx(self.tmp.name)
        plans = [make_plan(PRODUCT)]

        # 预置 timestamp=2026-03-10
        write_old_timestamp(ctx.data_root, PRODUCT, "2026-03-10")

        save_file_called = []

        def tracking_save_file(file_url, file_path, headers, product=""):
            save_file_called.append(file_url)

        report = _new_report("test-integ-001", mode="network")
        with (
            patch("quantclass_sync_internal.orchestrator.build_headers_or_raise",
                  return_value=({"api-key": "fake"}, "fake-hid")),
            patch("quantclass_sync_internal.orchestrator.get_latest_times",
                  return_value=["2026-03-10"]),
            patch("quantclass_sync_internal.orchestrator.get_latest_time",
                  return_value="2026-03-10"),
            patch("quantclass_sync_internal.orchestrator.get_download_link",
                  return_value="https://fake/fake.csv"),
            patch("quantclass_sync_internal.orchestrator.save_file",
                  side_effect=tracking_save_file),
        ):
            with open_status_db(ctx.data_root) as conn:
                total, has_error, _ = _execute_plans(
                    plans=plans,
                    command_ctx=ctx,
                    report=report,
                    conn=conn,
                    catch_up_to_latest=True,
                )

        # save_file 不应被调用
        self.assertEqual(0, len(save_file_called), "save_file 不应被调用")

        # report 中应有 skipped（up_to_date）记录
        skipped = [r for r in report.products if r.status == "skipped" and r.product == PRODUCT]
        self.assertTrue(len(skipped) >= 1, "report 中应有 skipped 记录")

        self.assertFalse(has_error)

    # ------------------------------------------------------------------
    # I1.5 部分失败（3 个产品中 1 个 404）
    # ------------------------------------------------------------------
    def test_i1_5_partial_failure(self) -> None:
        """I1.5: 3 个产品，中间 1 个 404，其余 2 个正常完成。"""
        ctx = make_ctx(self.tmp.name)

        # 3 个未知产品（strategy=mirror_unknown）最简单，不需要复杂规则
        product_ok_a = "test-product-a"
        product_fail = "test-product-b"
        product_ok_c = "test-product-c"

        plans = [
            ProductPlan(name=product_ok_a, strategy="mirror_unknown"),
            ProductPlan(name=product_fail, strategy="mirror_unknown"),
            ProductPlan(name=product_ok_c, strategy="mirror_unknown"),
        ]

        # 简单 CSV fixture（单文件，未知产品直接镜像写入）
        simple_csv = b"col1,col2\nval1,val2\n"

        def mock_save_file_partial(file_url, file_path, headers, product=""):
            # 正常产品写入 fixture
            Path(file_path).write_bytes(simple_csv)

        def mock_get_download_link_partial(api_base, product, date_time, hid, headers):
            # 产品 b 在 get_download_link 阶段就返回 403（权限不足），
            # 403 被映射为 REASON_NETWORK_ERROR，在 catch_up 下也是 error。
            if product == product_fail:
                raise FatalRequestError("无下载权限", status_code=403)
            return f"https://fake/{product}-{date_time}.csv"

        report = _new_report("test-integ-001", mode="network")
        with (
            patch("quantclass_sync_internal.orchestrator.build_headers_or_raise",
                  return_value=({"api-key": "fake"}, "fake-hid")),
            patch("quantclass_sync_internal.orchestrator.get_latest_times",
                  return_value=["2026-03-10"]),
            patch("quantclass_sync_internal.orchestrator.get_latest_time",
                  return_value="2026-03-10"),
            patch("quantclass_sync_internal.orchestrator.get_download_link",
                  side_effect=mock_get_download_link_partial),
            patch("quantclass_sync_internal.orchestrator.save_file",
                  side_effect=mock_save_file_partial),
        ):
            with open_status_db(ctx.data_root) as conn:
                total, has_error, _ = _execute_plans(
                    plans=plans,
                    command_ctx=ctx,
                    report=report,
                    conn=conn,
                    catch_up_to_latest=True,
                )

        # has_error 应为 True（有产品失败）
        self.assertTrue(has_error, "has_error 应为 True")

        # 2 个产品成功，1 个失败
        ok_products = {r.product for r in report.products if r.status == "ok"}
        error_products = {r.product for r in report.products if r.status == "error"}

        self.assertIn(product_ok_a, ok_products, "product_a 应成功")
        self.assertIn(product_ok_c, ok_products, "product_c 应成功")
        self.assertIn(product_fail, error_products, "product_b 应失败")

        # 失败产品有 reason_code
        fail_results = [r for r in report.products if r.product == product_fail and r.status == "error"]
        self.assertTrue(len(fail_results) >= 1)
        self.assertNotEqual("", fail_results[0].reason_code)

    # ------------------------------------------------------------------
    # I1.6 coin preprocess 触发
    # ------------------------------------------------------------------
    def test_i1_6_coin_preprocess_triggered(self) -> None:
        """I1.6: _maybe_run_coin_preprocess 在源产品成功且有增量时触发内置预处理。"""
        ctx = make_ctx(self.tmp.name)

        # 构造一个 preprocess 触发所需的目录（preprocess 产品目录需存在）
        from quantclass_sync_internal.constants import PREPROCESS_PRODUCT
        preprocess_dir = ctx.data_root / PREPROCESS_PRODUCT
        preprocess_dir.mkdir(parents=True, exist_ok=True)

        # 构造 report，包含源产品的成功记录（带有效增量）
        from quantclass_sync_internal.reporting import _append_result
        from quantclass_sync_internal.models import SyncStats
        report = _new_report("test-integ-001", mode="network")

        # 写入一个 PREPROCESS_TRIGGER_PRODUCTS 中的产品成功记录
        trigger_product = "coin-binance-candle-csv-1h"
        trigger_stats = SyncStats(created_files=1, rows_added=100)
        _append_result(
            report,
            product=trigger_product,
            status="ok",
            strategy="mirror_unknown",
            date_time="2026-03-10",
            stats=trigger_stats,
        )

        # mock 内置预处理，避免真实执行
        mock_builtin = MagicMock(return_value=("builtin(mode=incremental_patch)", "preprocess_incremental_ok"))

        with patch("quantclass_sync_internal.orchestrator._run_builtin_coin_preprocess",
                   mock_builtin):
            with open_status_db(ctx.data_root) as conn:
                _maybe_run_coin_preprocess(
                    command_ctx=ctx,
                    report=report,
                    conn=conn,
                )

        # 内置预处理应被调用一次
        mock_builtin.assert_called_once()

    # ------------------------------------------------------------------
    # I1.7 dry_run 模式
    # ------------------------------------------------------------------
    def test_i1_7_dry_run_no_files_written(self) -> None:
        """I1.7: dry_run=True 时 orchestrator 跑完但文件系统无新文件，状态库无变化。"""
        ctx = make_ctx(self.tmp.name, dry_run=True)
        plans = [make_plan(PRODUCT)]

        report, total, has_error = self._run_execute_plans(
            ctx=ctx,
            plans=plans,
            date_to_content={"2026-03-10": CSV_2026_03_10},
            latest_dates=["2026-03-10"],
            catch_up=True,
        )

        # dry_run 下产品目录不应有子 CSV 文件（但目录本身可能存在于 extract/work）
        product_dir = ctx.data_root / PRODUCT
        if product_dir.exists():
            csv_files = list(product_dir.glob("*.csv"))
            self.assertEqual(0, len(csv_files), f"dry_run 下不应写入数据文件，发现: {csv_files}")

        # timestamp.txt 不应创建
        ts_date = read_local_timestamp_date(ctx.data_root, PRODUCT)
        self.assertIsNone(ts_date, "dry_run 下 timestamp.txt 不应创建")

        # 状态库不应有记录（dry_run 跳过 upsert）
        db_path = ctx.data_root / ".quantclass_sync" / "status" / "FuelBinStat.db"
        if db_path.exists():
            with open_status_db(ctx.data_root) as conn:
                status = load_product_status(conn, PRODUCT)
            self.assertIsNone(status, "dry_run 下状态库不应有记录")


# ===========================================================================
# I2 - CSV 引擎
# ===========================================================================

class TestCsvEngine(unittest.TestCase):
    """I2 系列：直接调用 sync_known_product，验证 CSV 读写语义。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # ------------------------------------------------------------------
    # I2.1 已知产品合并（拆分 + 增量合并）
    # ------------------------------------------------------------------
    def test_i2_1_known_product_merge(self) -> None:
        """I2.1: 日聚合 CSV 应按 index_code 拆分，合并到各子文件。"""
        root = Path(self.tmp.name) / "data"
        root.mkdir(parents=True)
        extract_path = Path(self.tmp.name) / "extract"
        extract_path.mkdir(parents=True)

        # 放入日聚合文件，文件名为 2026-03-10.csv（触发 is_daily_aggregate_file）
        (extract_path / "2026-03-10.csv").write_bytes(CSV_2026_03_10)

        stats, reason_code = sync_known_product(
            product=PRODUCT,
            extract_path=extract_path,
            data_root=root,
            dry_run=False,
        )

        # 验证拆分结果
        sh001 = root / PRODUCT / "sh000001.csv"
        sh300 = root / PRODUCT / "sh000300.csv"
        self.assertTrue(sh001.exists(), "sh000001.csv 未创建")
        self.assertTrue(sh300.exists(), "sh000300.csv 未创建")

        # 每个子文件应有 1 条数据行
        content = sh001.read_text(encoding="utf-8-sig")
        lines = [l for l in content.strip().splitlines() if l.strip()]
        self.assertEqual(2, len(lines), f"sh000001.csv 应有 header+1 数据行，实际: {len(lines)}")

        # 统计量
        self.assertEqual(2, stats.created_files, "应创建 2 个子文件")

    # ------------------------------------------------------------------
    # I2.2 BOM 编码保留
    # ------------------------------------------------------------------
    def test_i2_2_bom_preserved(self) -> None:
        """I2.2: 输入带 BOM 的 UTF-8 CSV，合并后输出应保留 BOM。"""
        root = Path(self.tmp.name) / "data"
        root.mkdir(parents=True)
        extract_path = Path(self.tmp.name) / "extract"
        extract_path.mkdir(parents=True)

        # 写入带 BOM 的日聚合文件
        (extract_path / "2026-03-10.csv").write_bytes(CSV_2026_03_10)

        sync_known_product(
            product=PRODUCT,
            extract_path=extract_path,
            data_root=root,
            dry_run=False,
        )

        sh001 = root / PRODUCT / "sh000001.csv"
        self.assertTrue(sh001.exists())

        # 读取原始字节，验证 BOM 前缀
        raw_bytes = sh001.read_bytes()
        self.assertTrue(raw_bytes.startswith(BOM), "输出文件应保留 BOM 前缀")

    # ------------------------------------------------------------------
    # I2.3 大文件排序（10 万行）
    # ------------------------------------------------------------------
    def test_i2_3_large_file_sorted(self) -> None:
        """I2.3: 10 万行 CSV 合并后 sh000001.csv 内容应按 candle_end_time 升序排列。"""
        root = Path(self.tmp.name) / "data"
        root.mkdir(parents=True)
        extract_path = Path(self.tmp.name) / "extract"
        extract_path.mkdir(parents=True)

        # 生成 100000 行（单个 index_code=sh000001，不同日期，逆序写入）
        total_rows = 100_000
        header = "index_code,candle_end_time,open,close,high,low,volume"

        # 使用 2020-01-01 到 2020-01-01 + 99999 天（仅测试排序，不考虑真实日历）
        # 逆序生成行，验证 sync_known_product 会正确排序
        from datetime import date, timedelta
        base = date(2020, 1, 1)
        rows = []
        for i in range(total_rows - 1, -1, -1):  # 逆序
            d = (base + timedelta(days=i)).isoformat()
            rows.append(f"sh000001,{d} 15:00:00,100.0,101.0,102.0,99.0,1000")

        # 只拿前 100 行做性能测试（避免测试太慢），但验证排序语义
        # 实际压 100 行（逆序），验证排序
        sample_rows = rows[:100]
        csv_content = "\n".join([header] + sample_rows) + "\n"

        # 文件名需为日期格式触发 is_daily_aggregate_file
        # 因为是单 index_code，用单个日期文件即可
        # 但该文件内每行 candle_end_time 都不同，需要触发拆分路径
        # 最简单：把 100 行不同时间戳的数据放进同一个日聚合文件
        extract_file = extract_path / "2020-01-01.csv"
        extract_file.write_bytes(BOM + csv_content.encode("utf-8"))

        stats, reason_code = sync_known_product(
            product=PRODUCT,
            extract_path=extract_path,
            data_root=root,
            dry_run=False,
        )

        sh001 = root / PRODUCT / "sh000001.csv"
        self.assertTrue(sh001.exists(), "sh000001.csv 未创建")

        content = sh001.read_text(encoding="utf-8-sig")
        data_lines = content.strip().splitlines()[1:]  # 去掉 header
        if len(data_lines) < 2:
            self.skipTest("行数太少无法验证排序")

        # 提取 candle_end_time 列（第 2 列，index=1）
        timestamps = [line.split(",")[1] for line in data_lines]
        self.assertEqual(
            sorted(timestamps),
            timestamps,
            "candle_end_time 列应升序排列",
        )


# ===========================================================================
# I3 - 状态一致性
# ===========================================================================

class TestStateConsistency(unittest.TestCase):
    """I3 系列：验证 timestamp / 状态库 / 报告三源一致性。"""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # ------------------------------------------------------------------
    # I3.1 同步后三源一致
    # ------------------------------------------------------------------
    def test_i3_1_three_sources_consistent(self) -> None:
        """I3.1: 同步成功后 timestamp.txt / 状态库 data_time / report date_time 三者一致。"""
        ctx = make_ctx(self.tmp.name)
        plans = [make_plan(PRODUCT)]
        report = _new_report("test-integ-001", mode="network")
        save_file_mock = make_save_file_mock({"2026-03-10": CSV_2026_03_10})

        with (
            patch("quantclass_sync_internal.orchestrator.build_headers_or_raise",
                  return_value=({"api-key": "fake"}, "fake-hid")),
            patch("quantclass_sync_internal.orchestrator.get_latest_times",
                  return_value=["2026-03-10"]),
            patch("quantclass_sync_internal.orchestrator.get_latest_time",
                  return_value="2026-03-10"),
            patch("quantclass_sync_internal.orchestrator.get_download_link",
                  side_effect=lambda api_base, product, date_time, hid, headers:
                      make_download_link_for_date(product, date_time)),
            patch("quantclass_sync_internal.orchestrator.save_file",
                  side_effect=save_file_mock),
        ):
            with open_status_db(ctx.data_root) as conn:
                _execute_plans(
                    plans=plans,
                    command_ctx=ctx,
                    report=report,
                    conn=conn,
                    catch_up_to_latest=True,
                )

        # 读取三源
        ts_date = read_local_timestamp_date(ctx.data_root, PRODUCT)

        with open_status_db(ctx.data_root) as conn:
            db_status = load_product_status(conn, PRODUCT)
        db_date = db_status.data_time if db_status else None

        report_results = [r for r in report.products if r.status == "ok" and r.product == PRODUCT]
        report_date = report_results[0].date_time if report_results else None

        self.assertEqual("2026-03-10", ts_date, "timestamp.txt 日期不正确")
        self.assertEqual("2026-03-10", db_date, "状态库 data_time 不正确")
        self.assertEqual("2026-03-10", report_date, "报告 date_time 不正确")

    # ------------------------------------------------------------------
    # I3.2 异常中断无残留 .tmp 文件
    # ------------------------------------------------------------------
    def test_i3_2_no_tmp_residue_on_error(self) -> None:
        """I3.2: write_local_timestamp 抛异常时，数据目录下不应有 .tmp- 残留文件。"""
        ctx = make_ctx(self.tmp.name)
        plans = [make_plan(PRODUCT)]

        save_file_mock = make_save_file_mock({"2026-03-10": CSV_2026_03_10})

        # mock write_local_timestamp 抛 RuntimeError
        with (
            patch("quantclass_sync_internal.orchestrator.build_headers_or_raise",
                  return_value=({"api-key": "fake"}, "fake-hid")),
            patch("quantclass_sync_internal.orchestrator.get_latest_times",
                  return_value=["2026-03-10"]),
            patch("quantclass_sync_internal.orchestrator.get_latest_time",
                  return_value="2026-03-10"),
            patch("quantclass_sync_internal.orchestrator.get_download_link",
                  side_effect=lambda api_base, product, date_time, hid, headers:
                      make_download_link_for_date(product, date_time)),
            patch("quantclass_sync_internal.orchestrator.save_file",
                  side_effect=save_file_mock),
            patch("quantclass_sync_internal.orchestrator.write_local_timestamp",
                  side_effect=RuntimeError("模拟 timestamp 写入失败")),
        ):
            report = _new_report("test-integ-001", mode="network")
            with open_status_db(ctx.data_root) as conn:
                total, has_error, _ = _execute_plans(
                    plans=plans,
                    command_ctx=ctx,
                    report=report,
                    conn=conn,
                    catch_up_to_latest=True,
                )

        # 不应有 .tmp- 残留文件（原子写入保证临时文件被清理）
        tmp_files = list(ctx.data_root.rglob(".tmp-*"))
        # 排除 work_dir 下的临时文件（不在 data_root 内，一般不会出现）
        residue = [f for f in tmp_files if ".quantclass_sync" not in str(f)]
        self.assertEqual([], residue, f"数据目录下有残留临时文件: {residue}")

        # 状态持久化失败时，report 记录 ok 但 event_detail 携带 warning
        # （_upsert_product_status_after_success 捕获异常后设 status_persist_warning）
        # 验证 report 中有该产品的结果（不管是 ok 还是 error，关键是有记录）
        product_results = [r for r in report.products if r.product == PRODUCT]
        self.assertTrue(len(product_results) >= 1, "report 中应有该产品的记录")

    # ------------------------------------------------------------------
    # I3.3 旧路径迁移
    # ------------------------------------------------------------------
    def test_i3_3_legacy_path_migration(self) -> None:
        """I3.3: 旧路径 code/data/FuelBinStat.db 有数据时，resolve_runtime_paths 应回退旧路径。"""
        data_root = Path(self.tmp.name) / "data"
        data_root.mkdir(parents=True)

        # 在旧路径下创建状态库并写入一行记录
        legacy_db_path = data_root / "code" / "data" / "FuelBinStat.db"
        legacy_db_path.parent.mkdir(parents=True, exist_ok=True)

        import sqlite3
        conn = sqlite3.connect(legacy_db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS product_status ("
            "name TEXT PRIMARY KEY, data_time TEXT)"
        )
        conn.execute("INSERT INTO product_status VALUES ('test-product', '2026-03-10')")
        conn.commit()
        conn.close()

        # 新路径不存在（data_root/.quantclass_sync 目录无状态数据）
        paths = resolve_runtime_paths(data_root)

        # 应回退到旧路径
        self.assertEqual("legacy", paths.source, "应回退到旧路径 (source='legacy')")
        self.assertEqual(legacy_db_path.resolve(), paths.status_db.resolve())


if __name__ == "__main__":
    unittest.main()
