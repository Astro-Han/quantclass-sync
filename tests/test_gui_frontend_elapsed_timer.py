"""GUI 前端同步总耗时计时逻辑测试。"""

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_JS = PROJECT_ROOT / "quantclass_sync_internal" / "gui" / "assets" / "app.js"
INDEX_HTML = PROJECT_ROOT / "quantclass_sync_internal" / "gui" / "assets" / "index.html"


def _run_app_js(expression: str):
    """在 Node VM 中执行 app.js 组件逻辑并返回 JSON 结果。"""
    node = shutil.which("node")
    if node is None:
        raise unittest.SkipTest("node is required for frontend VM tests")

    script = textwrap.dedent(
        f"""
        (async () => {{
          const fs = require("fs");
          const vm = require("vm");

          const source = fs.readFileSync({json.dumps(str(APP_JS))}, "utf8");
          let alpineInit = null;
          let componentFactory = null;
          const timers = [];
          const clearedTimers = [];
          let nextTimerId = 1;

          const context = {{
            console,
            __nowMs: 0,
            __timers: timers,
            __clearedTimers: clearedTimers,
            document: {{
              addEventListener: (event, callback) => {{
                if (event === "alpine:init") alpineInit = callback;
              }},
            }},
            window: {{
              addEventListener: () => {{}},
              pywebview: {{ api: {{}} }},
            }},
            Alpine: {{
              data: (_name, factory) => {{
                componentFactory = factory;
              }},
            }},
            setTimeout: (fn, ms) => {{
              const id = nextTimerId++;
              timers.push({{ id, fn, ms }});
              return id;
            }},
            clearTimeout: (id) => {{
              clearedTimers.push(id);
            }},
            Date: {{
              now: () => context.__nowMs,
            }},
          }};

          vm.createContext(context);
          vm.runInContext(source, context, {{ filename: "app.js" }});
          if (!alpineInit) throw new Error("alpine:init listener not registered");
          alpineInit();
          if (!componentFactory) throw new Error("app component factory not registered");
          context.component = componentFactory();

          const result = await Promise.resolve(vm.runInContext({json.dumps(expression)}, context));
          process.stdout.write(JSON.stringify(result));
        }})().catch((error) => {{
          console.error(error && error.stack ? error.stack : String(error));
          process.exit(1);
        }});
        """
    )
    completed = subprocess.run(
        [node, "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


class TestGuiFrontendElapsedTimer(unittest.TestCase):
    """顶部总耗时应由前端本地推算，而不是只跟随后端跳变。"""

    def test_start_polling_keeps_display_elapsed_advancing_between_backend_polls(self):
        result = _run_app_js(
            """
            (async () => {
              component.syncStatus = "syncing";
              component.syncStartedAtMs = 100000;
              window.pywebview.api.get_sync_progress = async () => ({
                status: "syncing",
                elapsed_seconds: 5,
                completed: 1,
                total: 2,
              });

              __nowMs = 105000;
              component.startPolling();
              let timer = __timers.shift();
              await timer.fn();

              const first = {
                elapsedSeconds: component.elapsedSeconds,
                displayElapsedSeconds: component.displayElapsedSeconds,
                estimatedRemaining: component.estimatedRemaining(),
                pollTimer: component.pollTimer,
                timersLeft: __timers.length,
              };

              __nowMs = 106000;
              timer = __timers.shift();
              await timer.fn();

              return {
                first,
                second: {
                  elapsedSeconds: component.elapsedSeconds,
                  displayElapsedSeconds: component.displayElapsedSeconds,
                  estimatedRemaining: component.estimatedRemaining(),
                  pollTimer: component.pollTimer,
                  timersLeft: __timers.length,
                  syncStartedAtMs: component.syncStartedAtMs,
                },
              };
            })()
            """
        )

        self.assertEqual(result["first"]["elapsedSeconds"], 5)
        self.assertEqual(result["first"]["displayElapsedSeconds"], 5)
        self.assertEqual(result["first"]["estimatedRemaining"], "5 秒")
        self.assertEqual(result["second"]["elapsedSeconds"], 5)
        self.assertEqual(result["second"]["displayElapsedSeconds"], 6)
        self.assertEqual(result["second"]["estimatedRemaining"], "5 秒")
        self.assertEqual(result["second"]["syncStartedAtMs"], 100000)
        self.assertEqual(result["second"]["timersLeft"], 1)
        self.assertEqual(result["second"]["pollTimer"], 3)

    def test_sync_elapsed_from_progress_restores_local_start_time_for_display(self):
        result = _run_app_js(
            """
            (() => {
              __nowMs = 200000;
              component.syncElapsedFromProgress({
                status: "syncing",
                elapsed_seconds: 5,
              });
              __nowMs = 201000;
              component.syncElapsedFromProgress({
                status: "syncing",
                elapsed_seconds: 5,
              });
              return {
                syncStartedAtMs: component.syncStartedAtMs,
                elapsedSeconds: component.elapsedSeconds,
                displayElapsedSeconds: component.displayElapsedSeconds,
              };
            })()
            """
        )

        self.assertEqual(result["syncStartedAtMs"], 195000)
        self.assertEqual(result["elapsedSeconds"], 5)
        self.assertEqual(result["displayElapsedSeconds"], 6)

    def test_confirm_needed_freezes_display_elapsed_and_eta(self):
        result = _run_app_js(
            """
            (async () => {
              component.syncStatus = "syncing";
              component.syncStartedAtMs = 100000;
              component.total = 3;
              component.completed = 1;
              window.pywebview.api.get_sync_progress = async () => ({
                status: "confirm_needed",
                elapsed_seconds: 5,
                completed: 1,
                total: 3,
                estimate: { pending: true },
              });

              __nowMs = 105000;
              component.startPolling();
              let timer = __timers.shift();
              await timer.fn();

              return {
                elapsedSeconds: component.elapsedSeconds,
                displayElapsedSeconds: component.displayElapsedSeconds,
                estimatedRemaining: component.estimatedRemaining(),
                syncStartedAtMs: component.syncStartedAtMs,
                pollTimer: component.pollTimer,
                timersLeft: __timers.length,
              };
            })()
            """
        )

        self.assertEqual(result["elapsedSeconds"], 5)
        self.assertEqual(result["displayElapsedSeconds"], 5)
        self.assertEqual(result["estimatedRemaining"], "10 秒")
        self.assertIsNone(result["syncStartedAtMs"])
        self.assertEqual(result["timersLeft"], 0)
        self.assertIsNone(result["pollTimer"])

    def test_confirm_sync_restarts_polling_and_restores_local_elapsed(self):
        result = _run_app_js(
            """
            (async () => {
              const snapshots = [
                {
                  status: "confirm_needed",
                  elapsed_seconds: 5,
                  completed: 1,
                  total: 3,
                  estimate: { pending: true },
                },
                {
                  status: "syncing",
                  elapsed_seconds: 5,
                  completed: 1,
                  total: 3,
                },
              ];
              let idx = 0;
              window.pywebview.api.get_sync_progress = async () => snapshots[Math.min(idx++, snapshots.length - 1)];
              window.pywebview.api.confirm_sync = async () => ({ ok: true });

              __nowMs = 105000;
              component.startPolling();
              let timer = __timers.shift();
              await timer.fn();

              const paused = {
                elapsedSeconds: component.elapsedSeconds,
                displayElapsedSeconds: component.displayElapsedSeconds,
                estimatedRemaining: component.estimatedRemaining(),
                pollTimer: component.pollTimer,
                timersLeft: __timers.length,
              };

              __nowMs = 105200;
              await component.confirmSync();

              const resumed = {
                estimateData: component.estimateData,
                syncStartedAtMs: component.syncStartedAtMs,
                pollTimer: component.pollTimer,
                timersLeft: __timers.length,
              };

              __nowMs = 106000;
              timer = __timers.shift();
              await timer.fn();

              return {
                paused,
                resumed,
                afterResumePoll: {
                  elapsedSeconds: component.elapsedSeconds,
                  displayElapsedSeconds: component.displayElapsedSeconds,
                  estimatedRemaining: component.estimatedRemaining(),
                  timersLeft: __timers.length,
                  pollTimer: component.pollTimer,
                  syncStartedAtMs: component.syncStartedAtMs,
                },
              };
            })()
            """
        )

        self.assertEqual(result["paused"]["elapsedSeconds"], 5)
        self.assertEqual(result["paused"]["displayElapsedSeconds"], 5)
        self.assertEqual(result["paused"]["pollTimer"], None)
        self.assertEqual(result["paused"]["timersLeft"], 0)

        self.assertEqual(result["resumed"]["estimateData"], None)
        self.assertEqual(result["resumed"]["syncStartedAtMs"], 100200)
        self.assertEqual(result["resumed"]["pollTimer"], 2)
        self.assertEqual(result["resumed"]["timersLeft"], 1)

        self.assertEqual(result["afterResumePoll"]["elapsedSeconds"], 5)
        self.assertAlmostEqual(result["afterResumePoll"]["displayElapsedSeconds"], 5.8)
        self.assertEqual(result["afterResumePoll"]["estimatedRemaining"], "10 秒")
        self.assertEqual(result["afterResumePoll"]["timersLeft"], 1)
        self.assertEqual(result["afterResumePoll"]["pollTimer"], 3)
        self.assertEqual(result["afterResumePoll"]["syncStartedAtMs"], 100200)

    def test_start_polling_treats_done_error_and_idle_as_terminal_states(self):
        result = _run_app_js(
            """
            (async () => {
              async function runSnapshot(snapshot) {
                component.syncStatus = "syncing";
                component.syncStartedAtMs = 100000;
                component.elapsedSeconds = 99;
                component.errorMessage = "";
                component.runSummary = null;
                component.postprocessing = true;
                component.postprocessDetail = "phase";
                component.estimateData = { pending: true };
                component.historyLoaded = true;
                component.checkUpdateResult = { keep: true };
                component.pollTimer = null;
                __timers.length = 0;
                __clearedTimers.length = 0;
                window.pywebview.api.get_sync_progress = async () => snapshot;

                __nowMs = 200000;
                component.startPolling();
                const timer = __timers.shift();
                await timer.fn();

                return {
                  syncStatus: component.syncStatus,
                  elapsedSeconds: component.elapsedSeconds,
                  displayElapsedSeconds: component.displayElapsedSeconds,
                  syncStartedAtMs: component.syncStartedAtMs,
                  pollTimer: component.pollTimer,
                  timersLeft: __timers.length,
                  errorMessage: component.errorMessage,
                  runSummary: component.runSummary,
                  postprocessing: component.postprocessing,
                  postprocessDetail: component.postprocessDetail,
                  estimateData: component.estimateData,
                  historyLoaded: component.historyLoaded,
                  checkUpdateResult: component.checkUpdateResult,
                };
              }

              return {
                done: await runSnapshot({
                  status: "done",
                  elapsed_seconds: 7,
                  run_summary: { ok: 1 },
                }),
                error: await runSnapshot({
                  status: "error",
                  elapsed_seconds: 8,
                  error_message: "boom",
                  run_summary: { error: 1 },
                }),
                idle: await runSnapshot({
                  status: "idle",
                  elapsed_seconds: 4,
                }),
              };
            })()
            """
        )

        self.assertEqual(result["done"]["syncStatus"], "done")
        self.assertEqual(result["done"]["elapsedSeconds"], 7)
        self.assertEqual(result["done"]["displayElapsedSeconds"], 7)
        self.assertIsNone(result["done"]["syncStartedAtMs"])
        self.assertFalse(result["done"]["postprocessing"])
        self.assertEqual(result["done"]["postprocessDetail"], "")
        self.assertIsNone(result["done"]["estimateData"])
        self.assertEqual(result["done"]["timersLeft"], 0)
        self.assertIsNone(result["done"]["pollTimer"])

        self.assertEqual(result["error"]["syncStatus"], "error")
        self.assertEqual(result["error"]["elapsedSeconds"], 8)
        self.assertEqual(result["error"]["displayElapsedSeconds"], 8)
        self.assertEqual(result["error"]["errorMessage"], "boom")
        self.assertIsNone(result["error"]["syncStartedAtMs"])
        self.assertFalse(result["error"]["postprocessing"])
        self.assertEqual(result["error"]["postprocessDetail"], "")
        self.assertIsNone(result["error"]["estimateData"])
        self.assertEqual(result["error"]["timersLeft"], 0)
        self.assertIsNone(result["error"]["pollTimer"])

        self.assertEqual(result["idle"]["syncStatus"], "idle")
        self.assertEqual(result["idle"]["elapsedSeconds"], 4)
        self.assertEqual(result["idle"]["displayElapsedSeconds"], 4)
        self.assertIsNone(result["idle"]["syncStartedAtMs"])
        self.assertFalse(result["idle"]["postprocessing"])
        self.assertEqual(result["idle"]["postprocessDetail"], "")
        self.assertIsNone(result["idle"]["estimateData"])
        self.assertEqual(result["idle"]["timersLeft"], 0)
        self.assertIsNone(result["idle"]["pollTimer"])

    def test_index_html_uses_display_elapsed_seconds_binding(self):
        html = INDEX_HTML.read_text(encoding="utf-8")

        self.assertIn("formatDuration(displayElapsedSeconds)", html)
