"""选择—执行业务规则与并发裁决。

裁决要点：
  * 选择以 (设备标识, 稳定操作标识, 命令摘要, 失效时刻) 创建；
  * 同一设备最多保留一条未失效选择；成功执行后不得再次选择；
  * 选择/执行重传：相同请求标识或相同内容的重传回放原裁决；
    字段变化一律以冲突拒绝，绝不静默当作新请求；
  * 执行只接受“同操作标识 + 摘要一致 + 仍在有效期”的请求；
  * 并发执行竞争同一选择：仅一个首次消费成功，其余阻塞到终态后
    回放同一最终结果，而不会得到处理中状态；
  * 在结果落盘前/后注入中断后，启动恢复回放到可安全重试或可回放已执行态；
  * 存在无法校验的持久记录时服务降级（fail-closed），健康检查反映异常。
"""
from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional

from .device import PayloadDevice
from .store import STATE_EXECUTED, STATE_UNKNOWN, Store


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class ApiError(Exception):
    def __init__(self, http_status: int, code: str, message: str,
                 extra: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.http_status = http_status

        self.code = code
        self.message = message
        self.extra = extra or {}

    def to_dict(self) -> Dict[str, Any]:
        return {"error": self.code, "message": self.message, **self.extra}


class DutyService:
    def __init__(self, store: Store, device: PayloadDevice,
                 config: Any, clock: Callable[[], int] = now_ms,
                 exiter: Callable[[int], None] = os._exit) -> None:
        self.store = store
        self.device = device
        self.config = config
        self.now = clock
        self._exiter = exiter
        # 每个选择一把消费锁：竞争方在锁外阻塞，持锁者完成 T1->设备->T2
        # 全流程后释放；竞争方拿到锁时只可能看到终态（已执行/已过期）。
        self._locks_guard = threading.Lock()
        self._consume_locks: Dict[int, threading.Lock] = {}

    # ---------- 工具 ----------
    @staticmethod
    def _require_str(body: Dict[str, Any], name: str) -> str:
        value = body.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ApiError(400, "INVALID_FIELD",
                           f"字段 {name} 必须是非空字符串")
        return value

    def _require_expiry(self, body: Dict[str, Any]) -> int:
        """仅做类型校验；是否已过期需在重放判定之后再判断。"""
        value = body.get("expires_at_ms")
        if isinstance(value, bool) or not isinstance(value, int):
            raise ApiError(400, "INVALID_FIELD",
                           "字段 expires_at_ms 必须是毫秒级整数时间戳")
        return value

    def _require_healthy(self) -> None:
        errors = self.store.verify_all_integrity()
        if errors:
            raise ApiError(503, "PERSISTENCE_INTEGRITY_FAILED",
                           "持久记录无法校验，服务拒绝变更操作",
                           {"integrity_errors": errors})

    def _selection_json(self, row: Any, replayed: bool) -> Dict[str, Any]:
        return {"id": row["id"], "device_id": row["device_id"],
                "op_id": row["op_id"], "summary": row["summary"],
                "expires_at_ms": row["expires_at_ms"],
                "status": row["status"],
                "created_at_ms": row["created_at_ms"],
                "replayed": replayed}

    @staticmethod
    def _execution_json(row: Any, replayed: bool) -> Dict[str, Any]:
        return {"id": row["id"], "selection_id": row["selection_id"],
                "device_id": row["device_id"], "op_id": row["op_id"],
                "summary": row["summary"], "state": row["state"],
                "result": json.loads(row["result_json"])
                if row["result_json"] else None,
                "finished_at_ms": row["finished_at_ms"],
                "replayed": replayed}

    @contextmanager
    def _consumer_lock(self, selection_id: int) -> Iterator[threading.Lock]:
        with self._locks_guard:
            lock = self._consume_locks.setdefault(
                selection_id, threading.Lock())
        lock.acquire()
        try:
            yield lock
        finally:
            lock.release()

    # ---------- 创建选择 ----------
    def create_selection(self, body: Dict[str, Any]) -> Dict[str, Any]:
        self._require_healthy()
        device_id = self._require_str(body, "device_id")
        op_id = self._require_str(body, "op_id")
        summary = self._require_str(body, "summary")
        expires_at = self._require_expiry(body)
        request_id = body.get("request_id")
        if request_id is not None and not isinstance(request_id, str):
            raise ApiError(400, "INVALID_FIELD",
                           "字段 request_id 必须是字符串")

        # 1) 请求标识重传：存在则必须字段完全一致，否则明确冲突。
        if request_id:
            prior = self.store.find_by_request("selection", request_id)
            if prior is not None:
                if (prior["device_id"], prior["op_id"],
                        prior["summary"], prior["expires_at_ms"]) != \
                        (device_id, op_id, summary, expires_at):
                    raise ApiError(
                        409, "RETRANSMIT_FIELD_CONFLICT",
                        "相同请求标识的选择重传字段发生变化",
                        {"existing": self._selection_json(prior, True)})
                return self._selection_json(prior, replayed=True)

        latest = self.store.latest_selection_for_device(device_id)
        if latest is not None:
            latest = self.store.expire_if_due(latest, self.now())
            same_fields = (
                latest["op_id"] == op_id
                and latest["summary"] == summary
                and latest["expires_at_ms"] == expires_at)
            # 2) 相同内容重传（无请求标识）：无论原选择最终状态都回放原裁决。
            if same_fields:
                return self._selection_json(latest, replayed=True)
            # 3) 字段变化：未失效选择在 -> 冲突；已成功执行 -> 不得再次选择。
            if latest["status"] == "ACTIVE":
                raise ApiError(
                    409, "ACTIVE_SELECTION_EXISTS",
                    "同一设备已存在未失效选择，不能重复选择",
                    {"existing": self._selection_json(latest, False)})
            if latest["status"] == "CONSUMED":
                raise ApiError(
                    409, "SELECTION_ALREADY_EXECUTED",
                    "该设备选择已成功执行，不得再次选择",
                    {"existing": self._selection_json(latest, False)})
            # EXPIRED：允许建立新选择；旧选择保持 EXPIRED，不会复活。

        # 确属新选择（非重传）后才要求失效时刻晚于当前。
        if expires_at <= self.now():
            raise ApiError(400, "ALREADY_EXPIRED",
                           "失效时刻必须晚于当前时间")
        row = self.store.insert_selection(
            request_id, device_id, op_id, summary, expires_at, self.now())
        return self._selection_json(row, replayed=False)

    # ---------- 执行 ----------
    def _confirm_unknown(self, row: Any, unknown: Any,
                         request_id: Optional[str]) -> Dict[str, Any]:
        """确认一条“设备已驱动、结果未落盘”的执行（断电恢复后的同一操作）。

        这是首消费在跨进程意义上的延续，不是一次新的执行：设备动作在崩溃
        前已经发生，此处只用与首次下发完全一致的确定性结果把终态补落盘，
        绝不再次进入 device.execute 产生第二次实际驱动。
        """
        claim = self.store.drive_claim_for_selection(row["id"])
        if claim is None:
            # 无驱动承诺却存在 UNKNOWN：防御性 fail-closed，宁可提示稍后重试，
            # 也不允许再次下发高危命令。
            raise ApiError(409, "SELECTION_CONSUMED",
                           "选择已被消费且暂无最终结果，请稍后重试")
        # 承诺内容必须与当前选择一致（三要素），否则不补结果也不重发。
        if (claim["device_id"], claim["op_id"], claim["summary"]) != \
                (row["device_id"], row["op_id"], row["summary"]):
            raise ApiError(409, "SELECTION_CONSUMED",
                           "选择已被消费，且驱动承诺与当前内容不一致")
        result = PayloadDevice.deterministic_result(
            row["device_id"], row["op_id"], row["summary"])
        final_row = self.store.confirm_unknown_execution(
            unknown["id"], result, self.now())
        if request_id:
            self.store.ensure_execution_request_index(
                request_id, final_row["id"])
        return self._execution_json(final_row, replayed=True)

    def _verdict_for_current(self, row: Any,
                             request_id: Optional[str] = None
                             ) -> Dict[str, Any]:
        """对一条非 ACTIVE（或刚过期）选择给出终态裁决：确认、回放或拒绝。"""
        if row["status"] == "CONSUMED":
            final = self.store.latest_execution_for_selection(row["id"])
            # 设备已驱动而结果未见证（结果落盘前断电、重启恢复保留）：
            # 优先确认首次结果——即使已越过失效时刻，高危动作已经发生，
            # 必须让同一次业务操作收敛到一致结论，且不得再次下发。
            if final is not None and final["state"] == STATE_UNKNOWN:
                return self._confirm_unknown(row, final, request_id)
            if final is not None and final["state"] == STATE_EXECUTED:
                # 竞争失败方/重传：回放同一最终结果，而非处理中。
                # 竞争失败方若携带请求标识，补登记幂等索引指向同一终态记录。
                if request_id and final["request_id"] != request_id:
                    self.store.ensure_execution_request_index(
                        request_id, final["id"])
                return self._execution_json(final, replayed=True)
        if row["status"] == "EXPIRED" or row["expires_at_ms"] <= self.now():
            raise ApiError(410, "SELECTION_EXPIRED",
                           "选择已过失效时刻，不得执行")
        if row["status"] == "CONSUMED":
            # 进程内并发下不可达：消费锁串行化了 T1..T2；
            # 跨重启的悬空 EXECUTING 已在启动恢复时裁决（确认或回滚）。
            raise ApiError(409, "SELECTION_CONSUMED",
                           "选择已被消费且暂无最终结果，请稍后重试")
        raise ApiError(409, "SELECTION_NOT_ACTIVE",
                       f"选择状态为 {row['status']}，拒绝执行")

    def execute(self, body: Dict[str, Any]) -> Dict[str, Any]:
        self._require_healthy()
        device_id = self._require_str(body, "device_id")
        op_id = self._require_str(body, "op_id")
        summary = self._require_str(body, "summary")
        request_id = body.get("request_id")
        if request_id is not None and not isinstance(request_id, str):
            raise ApiError(400, "INVALID_FIELD",
                           "字段 request_id 必须是字符串")

        # 1) 请求标识重传：终态已在则回放原结果；字段变化明确冲突。
        #    若首消费仍在进行（EXECUTING），不在此处返回“处理中”，
        #    而是继续向下进入消费锁等待，最终回放同一终态。
        if request_id:
            prior = self.store.find_by_request("execution", request_id)
            if prior is not None:
                if (prior["device_id"], prior["op_id"],
                        prior["summary"]) != (device_id, op_id, summary):
                    raise ApiError(
                        409, "RETRANSMIT_FIELD_CONFLICT",
                        "相同请求标识的执行重传字段发生变化")
                if prior["state"] == "EXECUTED":
                    return self._execution_json(prior, replayed=True)
                # prior 为 EXECUTING：落入下方选择裁决，阻塞到终态后回放。

        latest = self.store.latest_selection_for_device(device_id)
        if latest is None:
            raise ApiError(404, "NO_SELECTION",
                           "该设备不存在选择，拒绝执行")

        # 2) 操作标识不同直接拒绝（无需进入消费竞争）。
        if latest["op_id"] != op_id:
            raise ApiError(403, "OP_ID_MISMATCH",
                           "执行请求的操作标识与选择不一致，拒绝执行",
                           {"selection_op_id": latest["op_id"]})
        # 3) 摘要不一致明确冲突。
        if latest["summary"] != summary:
            raise ApiError(409, "SUMMARY_MISMATCH",
                           "执行请求的命令摘要与选择不一致，拒绝执行")

        selection_id = latest["id"]
        # 4) 同一选择的消费全流程串行：竞争方在锁上等到终态落盘。
        with self._consumer_lock(selection_id):
            current = self.store.expire_if_due(
                self.store.find_selection(selection_id), self.now())
            if current["status"] != "ACTIVE" or \
                    current["expires_at_ms"] <= self.now():
                return self._verdict_for_current(current, request_id)

            # T1：EXECUTING + CONSUMED + 设备驱动台账 CLAIMED 同事务落盘，
            # 首次消费权与“即将实际下发”的承诺一并 durable。
            execution_id = self.store.insert_executing(
                current, request_id, self.now())

            # 台账已 durable，此刻才真正驱动载荷；设备结果由三要素确定。
            result = self.device.execute(device_id, op_id, summary)

            # —— 中断注入点①：执行结果落盘“之前” ——
            # 设备已实际驱动、结果已产生，但 T2 尚未提交；台账 CLAIMED 已
            # 随 T1 durable。重启恢复会把该执行保留为 UNKNOWN（已驱动、结果
            # 未见证），恢复后的同一 op_id 重试只确认首次结果，绝不二次下发。
            if self.config.crash_before_persist:
                self._crash(121)

            try:
                # T2：执行结果落盘提交，台账同步 CONFIRMED。
                final_row = self.store.finish_execution(
                    execution_id, result, self.now())
            except Exception:
                # 落盘失败：设备已经驱动，台账承诺已存在，恢复/回滚路径会
                # 保留为 UNKNOWN 而非退回 ACTIVE，后续重试确认结果、不重发。
                self.store.rollback_executing(execution_id, self.now())
                raise

            # —— 中断注入点②：执行结果落盘“之后” ——
            # T2 已 durable；重启后该选择为已执行，重传回放同一最终结果。
            if self.config.crash_after_persist:
                self._crash(122)

            return self._execution_json(final_row, replayed=False)

    def _crash(self, code: int) -> None:
        # 立即退出，跳过缓冲刷新与 finally，模拟硬断电；
        # SQLite 已提交事务（synchronous=FULL + WAL）保证持久。
        try:
            self.store.conn.close()
        finally:
            self._exiter(code)

    # ---------- 健康 ----------
    def health(self) -> Dict[str, Any]:
        errors = self.store.verify_all_integrity()
        db_ok = self.store.ping()
        return {"status": "ok" if not errors and db_ok else "degraded",
                "db_ok": db_ok, "integrity_errors": errors}
