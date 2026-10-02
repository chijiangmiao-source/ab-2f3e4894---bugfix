"""服务层裁决测试：并发竞争、过期、重传冲突、两处断电恢复、持久记录篡改。

运行：python -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace

from app.device import PayloadDevice
from app.service import ApiError, DutyService
from app.store import Store


class FakeClock:
    def __init__(self, start: int = 1_000_000) -> None:
        self.t = start

    def __call__(self) -> int:
        return self.t

    def advance(self, ms: int) -> None:
        self.t += ms


class _Crash(Exception):
    """替代 os._exit：抛出而非真正退出当前进程。"""


def crash_exiter(code: int) -> None:
    raise _Crash(code)


class ServiceTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "duty.db")
        self.clock = FakeClock()
        self.device = PayloadDevice()
        self.store = Store(self.db_path, "test-key")
        self.store.recover(self.clock())
        self.cfg = SimpleNamespace(crash_before_persist=False,
                                   crash_after_persist=False)
        self.svc = DutyService(self.store, self.device, self.cfg,
                               clock=self.clock, exiter=crash_exiter)

    def tearDown(self) -> None:
        try:
            self.store.close()
        finally:
            self.tmp.cleanup()

    def selection_body(self, device="DEV-1", op="OP-A",
                       summary="ARM 红外载荷", ttl_ms=60_000,
                       request_id=None):
        body = {"device_id": device, "op_id": op, "summary": summary,
                "expires_at_ms": self.clock() + ttl_ms}
        if request_id is not None:
            body["request_id"] = request_id
        return body

    def execution_body(self, device="DEV-1", op="OP-A",
                       summary="ARM 红外载荷", request_id=None):
        body = {"device_id": device, "op_id": op, "summary": summary}
        if request_id is not None:
            body["request_id"] = request_id
        return body

    def reopen_service(self, cfg=None) -> DutyService:
        """模拟重启：重新打开数据库并完成启动恢复。"""
        store = Store(self.db_path, "test-key")
        recovery = store.recover(self.clock())
        self._last_recovery = recovery
        device = PayloadDevice()
        return DutyService(store, device, cfg or self.cfg,
                           clock=self.clock, exiter=crash_exiter), store, \
            device, recovery


class TestSelectionRules(ServiceTestBase):
    def test_create_and_get(self):
        resp = self.svc.create_selection(self.selection_body())
        self.assertEqual(resp["status"], "ACTIVE")
        self.assertFalse(resp["replayed"])
        self.assertEqual(resp["device_id"], "DEV-1")

    def test_retransmit_same_content_replays(self):
        body = self.selection_body()
        first = self.svc.create_selection(body)
        second = self.svc.create_selection(dict(body))
        self.assertTrue(second["replayed"])
        self.assertEqual(second["id"], first["id"])

    def test_retransmit_same_request_id_replays(self):
        first = self.svc.create_selection(
            self.selection_body(request_id="SEL-1"))
        second = self.svc.create_selection(
            self.selection_body(request_id="SEL-1"))
        self.assertEqual(second["id"], first["id"])
        self.assertTrue(second["replayed"])

    def test_retransmit_request_id_field_conflict(self):
        self.svc.create_selection(
            self.selection_body(summary="摘要甲", request_id="SEL-1"))
        with self.assertRaises(ApiError) as cm:
            self.svc.create_selection(
                self.selection_body(summary="摘要乙", request_id="SEL-1"))
        self.assertEqual(cm.exception.code, "RETRANSMIT_FIELD_CONFLICT")

    def test_only_one_active_selection_per_device(self):
        self.svc.create_selection(self.selection_body(summary="摘要甲"))
        with self.assertRaises(ApiError) as cm:
            self.svc.create_selection(self.selection_body(summary="摘要乙"))
        self.assertEqual(cm.exception.code, "ACTIVE_SELECTION_EXISTS")

    def test_no_reselect_after_success(self):
        self.svc.create_selection(self.selection_body(summary="摘要甲"))
        self.svc.execute(self.execution_body(summary="摘要甲"))
        with self.assertRaises(ApiError) as cm:
            self.svc.create_selection(self.selection_body(summary="摘要乙"))
        self.assertEqual(cm.exception.code, "SELECTION_ALREADY_EXECUTED")

    def test_expired_blocks_execute_but_allows_new_selection(self):
        body = self.selection_body(ttl_ms=1000, summary="旧命令")
        old = self.svc.create_selection(body)
        self.clock.advance(2000)
        with self.assertRaises(ApiError) as cm:
            self.svc.execute(self.execution_body(summary="旧命令"))
        self.assertEqual(cm.exception.code, "SELECTION_EXPIRED")
        # 过期后允许新选择，旧选择不得复活。
        new = self.svc.create_selection(
            self.selection_body(summary="新命令"))
        self.assertEqual(new["status"], "ACTIVE")
        self.assertNotEqual(new["id"], old["id"])
        old_row = self.store.find_selection(old["id"])
        self.assertEqual(old_row["status"], "EXPIRED")


class TestExecutionRules(ServiceTestBase):
    def test_execute_success_is_idempotent_replay(self):
        self.svc.create_selection(self.selection_body())
        r1 = self.svc.execute(self.execution_body(request_id="EXE-1"))
        self.assertFalse(r1["replayed"])
        self.assertEqual(r1["state"], "EXECUTED")
        r2 = self.svc.execute(self.execution_body(request_id="EXE-1"))
        self.assertTrue(r2["replayed"])
        self.assertEqual(r2["id"], r1["id"])
        self.assertEqual(r2["result"], r1["result"])

    def test_execute_without_selection_rejected(self):
        with self.assertRaises(ApiError) as cm:
            self.svc.execute(self.execution_body(device="DEV-X"))
        self.assertEqual(cm.exception.code, "NO_SELECTION")

    def test_other_op_id_rejected(self):
        self.svc.create_selection(self.selection_body(op="OP-A"))
        with self.assertRaises(ApiError) as cm:
            self.svc.execute(self.execution_body(op="OP-B"))
        self.assertEqual(cm.exception.code, "OP_ID_MISMATCH")

    def test_summary_conflict_rejected(self):
        self.svc.create_selection(self.selection_body(summary="摘要甲"))
        with self.assertRaises(ApiError) as cm:
            self.svc.execute(self.execution_body(summary="摘要乙"))
        self.assertEqual(cm.exception.code, "SUMMARY_MISMATCH")

    def test_execution_request_id_field_conflict(self):
        self.svc.create_selection(self.selection_body(summary="摘要甲"))
        self.svc.execute(
            self.execution_body(summary="摘要甲", request_id="EXE-9"))
        with self.assertRaises(ApiError) as cm:
            self.svc.execute(
                self.execution_body(summary="摘要乙", request_id="EXE-9"))
        self.assertEqual(cm.exception.code, "RETRANSMIT_FIELD_CONFLICT")

    def test_expired_selection_not_executable(self):
        self.svc.create_selection(self.selection_body(ttl_ms=500))
        self.clock.advance(600)
        with self.assertRaises(ApiError) as cm:
            self.svc.execute(self.execution_body())
        self.assertEqual(cm.exception.code, "SELECTION_EXPIRED")


class TestConcurrentExecution(ServiceTestBase):
    def test_two_concurrent_consumers_only_one_first_success(self):
        self.svc.create_selection(self.selection_body())
        results = []
        errors = []
        barrier = threading.Barrier(2)

        def worker():
            try:
                barrier.wait()
                results.append(self.svc.execute(self.execution_body()))
            except ApiError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        firsts = [r for r in results if not r["replayed"]]
        replays = [r for r in results if r["replayed"]]
        self.assertEqual(len(firsts), 1, "必须只有一个首次消费成功")
        self.assertEqual(len(replays), 1, "另一个必须回放最终结果")
        # 同一最终结果：执行记录 id 与结果体一致。
        self.assertEqual(replays[0]["id"], firsts[0]["id"])
        self.assertEqual(replays[0]["result"], firsts[0]["result"])
        self.assertEqual(replays[0]["state"], "EXECUTED",
                         "竞争失败方不得得到处理中状态")
        # 设备只被实际驱动一次。
        self.assertEqual(self.device.drive_count("OP-A"), 1)

    def test_many_concurrent_consumers(self):
        self.svc.create_selection(self.selection_body())
        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            r = self.svc.execute(self.execution_body())
            with lock:
                results.append(r)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(len(results), 8)
        self.assertEqual(sum(1 for r in results if not r["replayed"]), 1)
        self.assertEqual(len({r["id"] for r in results}), 1)
        self.assertTrue(all(r["state"] == "EXECUTED" for r in results))
        self.assertEqual(self.device.drive_count("OP-A"), 1)

    def test_loser_request_id_replays_same_final(self):
        """竞争失败方携带自己的请求标识，随后按该标识重传仍命中同一终态。"""
        self.svc.create_selection(self.selection_body())
        holder = threading.Event()
        proceed = threading.Event()

        def first_consumer():
            # 直接进入执行路径，在设备驱动处挂住，制造竞争窗口。
            orig = self.device.execute

            def slow(device_id, op_id, summary):
                holder.set()
                proceed.wait(5)
                return orig(device_id, op_id, summary)

            self.device.execute = slow  # type: ignore[assignment]
            self.svc.execute(self.execution_body(request_id="WIN-1"))

        t = threading.Thread(target=first_consumer)
        t.start()
        holder.wait(5)
        # 首消费者在 T1 后、T2 前；第二个请求并发竞争，应阻塞到终态后回放。
        loser_result = []
        t2 = threading.Thread(
            target=lambda: loser_result.append(
                self.svc.execute(self.execution_body(request_id="LOSE-1"))))
        t2.start()
        # 给 loser 时间进入等待，然后放行首消费者。
        proceed.set()
        t.join(10)
        t2.join(10)
        self.assertEqual(len(loser_result), 1)
        loser = loser_result[0]
        self.assertTrue(loser["replayed"])
        # 用 loser 自己的请求标识重传，应回放同一条终态执行记录。
        again = self.svc.execute(self.execution_body(request_id="LOSE-1"))
        win = self.svc.execute(self.execution_body(request_id="WIN-1"))
        self.assertTrue(again["replayed"])
        self.assertEqual(again["id"], loser["id"])
        self.assertEqual(again["id"], win["id"])
        self.assertEqual(again["result"], win["result"])

    def test_same_request_id_concurrent_replay_gets_final_not_pending(self):
        """相同请求标识的执行重传若正赶上首消费进行中，也必须拿到同一终态。"""
        self.svc.create_selection(self.selection_body())
        holder = threading.Event()
        proceed = threading.Event()
        orig = self.device.execute

        def slow(device_id, op_id, summary):
            holder.set()
            proceed.wait(5)
            return orig(device_id, op_id, summary)

        self.device.execute = slow  # type: ignore[assignment]
        outcomes = []
        lock = threading.Lock()

        def first():
            try:
                r = self.svc.execute(
                    self.execution_body(request_id="DUP-1"))
                with lock:
                    outcomes.append(("first", r))
            except ApiError as e:
                with lock:
                    outcomes.append(("first-err", e))

        t1 = threading.Thread(target=first)
        t1.start()
        holder.wait(5)

        def second():
            try:
                r = self.svc.execute(
                    self.execution_body(request_id="DUP-1"))
                with lock:
                    outcomes.append(("second", r))
            except ApiError as e:
                with lock:
                    outcomes.append(("second-err", e))

        t2 = threading.Thread(target=second)
        t2.start()
        time.sleep(0.2)
        proceed.set()
        t1.join(10)
        t2.join(10)

        self.assertEqual(len(outcomes), 2)
        by_name = dict(outcomes)
        self.assertFalse(by_name["first"]["replayed"])
        second_r = by_name["second"]
        self.assertTrue(second_r["replayed"],
                        "并发重传必须回放终态，而不是收到处理中错误")
        self.assertEqual(second_r["id"], by_name["first"]["id"])
        self.assertEqual(second_r["state"], "EXECUTED")
        self.assertEqual(self.device.drive_count("OP-A"), 1)


class TestCrashRecovery(ServiceTestBase):
    def test_crash_before_result_persist_recovers_to_retryable(self):
        crash_cfg = SimpleNamespace(crash_before_persist=True,
                                    crash_after_persist=False)
        svc = DutyService(self.store, self.device, crash_cfg,
                          clock=self.clock, exiter=crash_exiter)
        svc.create_selection(self.selection_body())
        with self.assertRaises(_Crash):
            svc.execute(self.execution_body())

        # —— 重启 ——
        svc2, store2, device2, recovery = self.reopen_service()
        self.assertEqual(len(recovery["rolled_back_executions"]), 1,
                         "重启必须回滚落盘前遗留的悬空执行")
        sel = store2.latest_selection_for_device("DEV-1")
        self.assertEqual(sel["status"], "ACTIVE",
                         "落盘前断电必须恢复为可安全重试的未执行态")
        self.assertIsNone(
            store2.latest_execution_for_selection(sel["id"]))
        # 安全重试成功。
        r = svc2.execute(self.execution_body())
        self.assertEqual(r["state"], "EXECUTED")
        self.assertFalse(r["replayed"])
        self.assertEqual(device2.drive_count("OP-A"), 1)
        store2.close()

    def test_crash_before_persist_but_selection_expired_by_restart(self):
        crash_cfg = SimpleNamespace(crash_before_persist=True,
                                    crash_after_persist=False)
        svc = DutyService(self.store, self.device, crash_cfg,
                          clock=self.clock, exiter=crash_exiter)
        svc.create_selection(self.selection_body(ttl_ms=1000))
        with self.assertRaises(_Crash):
            svc.execute(self.execution_body())

        # 重启前时间已越过失效时刻。
        self.clock.advance(2000)
        svc2, store2, _, _ = self.reopen_service()
        sel = store2.latest_selection_for_device("DEV-1")
        self.assertEqual(sel["status"], "EXPIRED",
                         "重启时已过期的悬空选择应恢复为 EXPIRED")
        with self.assertRaises(ApiError) as cm:
            svc2.execute(self.execution_body())
        self.assertEqual(cm.exception.code, "SELECTION_EXPIRED")
        store2.close()

    def test_crash_after_result_persist_replays_executed(self):
        crash_cfg = SimpleNamespace(crash_before_persist=False,
                                    crash_after_persist=True)
        svc = DutyService(self.store, self.device, crash_cfg,
                          clock=self.clock, exiter=crash_exiter)
        svc.create_selection(self.selection_body(summary="高危指令"))
        with self.assertRaises(_Crash):
            svc.execute(self.execution_body(summary="高危指令"))

        # —— 重启 ——
        svc2, store2, device2, recovery = self.reopen_service()
        self.assertEqual(recovery["rolled_back_executions"], [])
        sel = store2.latest_selection_for_device("DEV-1")
        self.assertEqual(sel["status"], "CONSUMED",
                         "落盘后断电必须恢复为可回放的已执行态")
        exe = store2.latest_execution_for_selection(sel["id"])
        self.assertEqual(exe["state"], "EXECUTED")
        # 重传回放同一最终结果。
        r = svc2.execute(self.execution_body(summary="高危指令"))
        self.assertTrue(r["replayed"])
        self.assertEqual(r["id"], exe["id"])
        self.assertEqual(
            r["result"]["receipt"],
            json.loads(exe["result_json"])["receipt"])
        # 已成功执行后不得再次选择（即使字段变化）。
        with self.assertRaises(ApiError) as cm:
            svc2.create_selection(
                self.selection_body(summary="另一条指令"))
        self.assertEqual(cm.exception.code, "SELECTION_ALREADY_EXECUTED")
        # 旧选择不得复活。
        self.assertEqual(
            store2.find_selection(sel["id"])["status"], "CONSUMED")
        self.assertEqual(device2.drive_count("OP-A"), 0,
                         "回放不得再次驱动设备")
        store2.close()

    def test_old_selection_not_revived_after_restart(self):
        self.svc.create_selection(
            self.selection_body(summary="旧命令", ttl_ms=1000))
        self.clock.advance(2000)
        svc2, store2, _, _ = self.reopen_service()
        old = store2.latest_selection_for_device("DEV-1")
        self.assertEqual(old["status"], "EXPIRED")
        new = svc2.create_selection(
            self.selection_body(summary="新命令"))
        self.assertEqual(new["status"], "ACTIVE")
        self.assertEqual(
            store2.find_selection(old["id"])["status"], "EXPIRED")
        store2.close()


class TestIntegrity(ServiceTestBase):
    def _tamper(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("UPDATE selections SET summary=? WHERE device_id=?",
                     ("被篡改的摘要", "DEV-1"))
        conn.commit()
        conn.close()

    def test_tampered_record_marks_health_degraded(self):
        self.svc.create_selection(self.selection_body())
        self.assertEqual(self.svc.health()["status"], "ok")
        self._tamper()
        health = self.svc.health()
        self.assertEqual(health["status"], "degraded")
        self.assertTrue(health["integrity_errors"])

    def test_tampered_record_fails_closed(self):
        self.svc.create_selection(self.selection_body())
        self._tamper()
        with self.assertRaises(ApiError) as cm:
            self.svc.create_selection(
                self.selection_body(device="DEV-2"))
        self.assertEqual(cm.exception.code,
                         "PERSISTENCE_INTEGRITY_FAILED")
        with self.assertRaises(ApiError) as cm2:
            self.svc.execute(self.execution_body())
        self.assertEqual(cm2.exception.code,
                         "PERSISTENCE_INTEGRITY_FAILED")

    def test_recovery_reports_tampering(self):
        self.svc.create_selection(self.selection_body())
        self.store.close()
        self._tamper()
        store = Store(self.db_path, "test-key")
        recovery = store.recover(self.clock())
        self.assertTrue(recovery["integrity_errors"])
        store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
