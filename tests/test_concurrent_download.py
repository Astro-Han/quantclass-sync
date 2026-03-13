"""并发下载测试：验证 _execute_plans 的 max_workers 参数行为。"""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from quantclass_sync_internal.models import (
    CommandContext,
    ProductPlan,
    RunReport,
    SyncStats,
)
from quantclass_sync_internal.orchestrator import _execute_plans
from quantclass_sync_internal.reporting import _new_report


def _make_ctx(**overrides) -> CommandContext:
    """构造最小可用的 CommandContext。"""
    defaults = {
        "run_id": "test-concurrent",
        "data_root": "/tmp/test-data",
        "api_key": "test-key",
        "hid": "test-hid",
        "dry_run": False,
        "stop_on_error": False,
        "verbose": False,
        "api_base": "https://api.example.com",
    }
    defaults.update(overrides)
    return CommandContext(**defaults)


class TestMaxWorkersParameter(unittest.TestCase):
    """max_workers 参数的基本行为。"""

    @patch("quantclass_sync_internal.orchestrator.build_headers_or_raise")
    @patch("quantclass_sync_internal.orchestrator._reset_http_metrics")
    @patch("quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan")
    @patch("quantclass_sync_internal.orchestrator.process_product")
    @patch("quantclass_sync_internal.orchestrator._upsert_product_status_after_success")
    def test_sequential_with_max_workers_1(
        self, mock_upsert, mock_process, mock_resolve, mock_reset, mock_headers
    ):
        """max_workers=1 时串行执行，产品按顺序处理。"""
        mock_headers.return_value = ({"Authorization": "test"}, "test-hid")
        order = []

        def fake_resolve(plan, **kwargs):
            order.append(("resolve", plan.name))
            return ["2026-03-13"], False

        mock_resolve.side_effect = fake_resolve

        def fake_process(**kwargs):
            order.append(("process", kwargs["plan"].name))
            return kwargs["plan"].name, "2026-03-13", SyncStats(), "", "ok"

        mock_process.side_effect = fake_process

        plans = [ProductPlan(name="product-a", strategy="merge_known"),
                 ProductPlan(name="product-b", strategy="merge_known")]
        ctx = _make_ctx()
        report = _new_report("test", "network")

        _execute_plans(plans, ctx, report, max_workers=1)

        # 串行：a 全部完成后再处理 b
        self.assertEqual(order, [
            ("resolve", "product-a"), ("process", "product-a"),
            ("resolve", "product-b"), ("process", "product-b"),
        ])

    @patch("quantclass_sync_internal.orchestrator.build_headers_or_raise")
    @patch("quantclass_sync_internal.orchestrator._reset_http_metrics")
    @patch("quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan")
    @patch("quantclass_sync_internal.orchestrator.process_product")
    @patch("quantclass_sync_internal.orchestrator._upsert_product_status_after_success")
    def test_concurrent_executes_all_products(
        self, mock_upsert, mock_process, mock_resolve, mock_reset, mock_headers
    ):
        """max_workers>1 时所有产品都应被处理。"""
        mock_headers.return_value = ({"Authorization": "test"}, "test-hid")
        processed = []
        lock = threading.Lock()

        def fake_resolve(plan, **kwargs):
            return ["2026-03-13"], False

        mock_resolve.side_effect = fake_resolve

        def fake_process(**kwargs):
            with lock:
                processed.append(kwargs["plan"].name)
            time.sleep(0.01)  # 模拟网络延迟
            return kwargs["plan"].name, "2026-03-13", SyncStats(updated_files=1), "", "ok"

        mock_process.side_effect = fake_process

        plans = [ProductPlan(name=f"product-{i}", strategy="merge_known") for i in range(5)]
        ctx = _make_ctx()
        report = _new_report("test", "network")

        total, has_error, _ = _execute_plans(plans, ctx, report, max_workers=3)

        self.assertFalse(has_error)
        self.assertEqual(len(processed), 5)
        self.assertEqual(set(processed), {f"product-{i}" for i in range(5)})
        # 统计合并正确
        self.assertEqual(total.updated_files, 5)

    @patch("quantclass_sync_internal.orchestrator.build_headers_or_raise")
    @patch("quantclass_sync_internal.orchestrator._reset_http_metrics")
    @patch("quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan")
    @patch("quantclass_sync_internal.orchestrator.process_product")
    @patch("quantclass_sync_internal.orchestrator._upsert_product_status_after_success")
    def test_concurrent_actually_parallel(
        self, mock_upsert, mock_process, mock_resolve, mock_reset, mock_headers
    ):
        """验证并发确实是并行而非串行。

        用计数器在 sleep 前 +1、后 -1，测量 sleep 期间的同时活跃峰值，
        比累计线程数更准确地反映真实并发度。
        """
        mock_headers.return_value = ({"Authorization": "test"}, "test-hid")
        # 当前同时活跃线程计数和峰值记录
        active_count = [0]
        peak_active = [0]
        lock = threading.Lock()

        def fake_resolve(plan, **kwargs):
            return ["2026-03-13"], False

        mock_resolve.side_effect = fake_resolve

        def fake_process(**kwargs):
            # 进入 sleep 前 +1，退出后 -1，测量 sleep 中的并发峰值
            with lock:
                active_count[0] += 1
                peak_active[0] = max(peak_active[0], active_count[0])
            time.sleep(0.05)  # 确保线程有重叠窗口
            with lock:
                active_count[0] -= 1
            return kwargs["plan"].name, "2026-03-13", SyncStats(), "", "ok"

        mock_process.side_effect = fake_process

        plans = [ProductPlan(name=f"p-{i}", strategy="merge_known") for i in range(4)]
        ctx = _make_ctx()
        report = _new_report("test", "network")

        _execute_plans(plans, ctx, report, max_workers=4)

        # 至少有 2 个线程同时处于 sleep（证明并行生效）
        self.assertGreaterEqual(peak_active[0], 2)


class TestConcurrentErrorHandling(unittest.TestCase):
    """并发模式下的错误处理。"""

    @patch("quantclass_sync_internal.orchestrator.build_headers_or_raise")
    @patch("quantclass_sync_internal.orchestrator._reset_http_metrics")
    @patch("quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan")
    @patch("quantclass_sync_internal.orchestrator.process_product")
    @patch("quantclass_sync_internal.orchestrator._upsert_product_status_after_success")
    def test_one_failure_does_not_block_others(
        self, mock_upsert, mock_process, mock_resolve, mock_reset, mock_headers
    ):
        """一个产品失败不影响其他产品完成。"""
        mock_headers.return_value = ({"Authorization": "test"}, "test-hid")
        from quantclass_sync_internal.models import ProductSyncError

        def fake_resolve(plan, **kwargs):
            return ["2026-03-13"], False

        mock_resolve.side_effect = fake_resolve

        def fake_process(**kwargs):
            name = kwargs["plan"].name
            if name == "fail-product":
                raise ProductSyncError("test error", reason_code="network_error")
            return name, "2026-03-13", SyncStats(updated_files=1), "", "ok"

        mock_process.side_effect = fake_process

        plans = [
            ProductPlan(name="ok-1", strategy="merge_known"),
            ProductPlan(name="fail-product", strategy="merge_known"),
            ProductPlan(name="ok-2", strategy="merge_known"),
        ]
        ctx = _make_ctx()
        report = _new_report("test", "network")

        total, has_error, _ = _execute_plans(plans, ctx, report, max_workers=3)

        self.assertTrue(has_error)
        # 2 个成功产品的 stats 应合并
        self.assertEqual(total.updated_files, 2)
        # report 应包含 3 个结果
        ok_count = sum(1 for r in report.products if r.status == "ok")
        error_count = sum(1 for r in report.products if r.status == "error")
        self.assertEqual(ok_count, 2)
        self.assertEqual(error_count, 1)

    @patch("quantclass_sync_internal.orchestrator.build_headers_or_raise")
    @patch("quantclass_sync_internal.orchestrator._reset_http_metrics")
    @patch("quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan")
    @patch("quantclass_sync_internal.orchestrator.process_product")
    @patch("quantclass_sync_internal.orchestrator._upsert_product_status_after_success")
    def test_stop_on_error_forces_sequential(
        self, mock_upsert, mock_process, mock_resolve, mock_reset, mock_headers
    ):
        """stop_on_error=True 时即使 max_workers>1 也应串行。"""
        mock_headers.return_value = ({"Authorization": "test"}, "test-hid")
        from quantclass_sync_internal.models import ProductSyncError

        call_order = []

        def fake_resolve(plan, **kwargs):
            call_order.append(plan.name)
            return ["2026-03-13"], False

        mock_resolve.side_effect = fake_resolve

        def fake_process(**kwargs):
            name = kwargs["plan"].name
            if name == "fail-first":
                raise ProductSyncError("stop here", reason_code="network_error")
            return name, "2026-03-13", SyncStats(), "", "ok"

        mock_process.side_effect = fake_process

        plans = [
            ProductPlan(name="fail-first", strategy="merge_known"),
            ProductPlan(name="should-not-run", strategy="merge_known"),
        ]
        ctx = _make_ctx(stop_on_error=True)
        report = _new_report("test", "network")

        total, has_error, _ = _execute_plans(plans, ctx, report, max_workers=4)

        self.assertTrue(has_error)
        # stop-on-error 时第二个产品不应被处理
        self.assertEqual(call_order, ["fail-first"])


class TestStatsAccumulation(unittest.TestCase):
    """并发模式下统计累加的正确性。"""

    @patch("quantclass_sync_internal.orchestrator.build_headers_or_raise")
    @patch("quantclass_sync_internal.orchestrator._reset_http_metrics")
    @patch("quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan")
    @patch("quantclass_sync_internal.orchestrator.process_product")
    @patch("quantclass_sync_internal.orchestrator._upsert_product_status_after_success")
    def test_stats_merge_thread_safe(
        self, mock_upsert, mock_process, mock_resolve, mock_reset, mock_headers
    ):
        """多线程下 SyncStats 累加结果应精确。"""
        mock_headers.return_value = ({"Authorization": "test"}, "test-hid")

        def fake_resolve(plan, **kwargs):
            return ["2026-03-13"], False

        mock_resolve.side_effect = fake_resolve

        def fake_process(**kwargs):
            return kwargs["plan"].name, "2026-03-13", SyncStats(
                created_files=1, updated_files=2, rows_added=10
            ), "", "ok"

        mock_process.side_effect = fake_process

        n = 20
        plans = [ProductPlan(name=f"p-{i}", strategy="merge_known") for i in range(n)]
        ctx = _make_ctx()
        report = _new_report("test", "network")

        total, has_error, _ = _execute_plans(plans, ctx, report, max_workers=4)

        self.assertFalse(has_error)
        self.assertEqual(total.created_files, n)
        self.assertEqual(total.updated_files, n * 2)
        self.assertEqual(total.rows_added, n * 10)


if __name__ == "__main__":
    unittest.main()
