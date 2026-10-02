"""SQLite 持久化层与启动恢复。

状态机：
  selections.status: ACTIVE -> CONSUMED（执行成功）
                     ACTIVE -> EXPIRED（过期，惰性或恢复时标记）
  executions.state:  EXECUTING -> EXECUTED（结果落盘）

落盘分两段事务：
  T1 写 execution(EXECUTING) + selection(CONSUMED) + 设备驱动台账(CLAIMED)
     并在同一事务提交；随后才真正驱动载荷；
  T2 写 execution(EXECUTED, result) + 台账(CONFIRMED) 并提交。
T1 后、设备驱动前断电 -> 台账无承诺行（理论窗口，设备按 op_id 幂等兜底）；
设备驱动后、T2 前断电 -> 台账 CLAIMED 已 durable，重启时绝不回滚：
  execution 置为 UNKNOWN（已驱动、结果未见证）、selection 保持 CONSUMED，
  恢复后的同一操作只“确认/回放”首次结果，不会第二次实际下发高危命令；
T2 后断电 -> EXECUTED 已落盘，重启后原样回放裁决。

每行入库时在同一事务内用真实主键重算 HMAC 后提交，保证崩溃后任何
已落盘行都能通过（或明确无法通过）完整性校验，不存在“校验窗口”。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional

from . import security

SCHEMA = """
CREATE TABLE IF NOT EXISTS selections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT,
    device_id TEXT NOT NULL,
    op_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    selection_id INTEGER NOT NULL,
    request_id TEXT,
    device_id TEXT NOT NULL,
    op_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    state TEXT NOT NULL,
    result_json TEXT,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS request_index (
    kind TEXT NOT NULL,              -- 'selection' | 'execution'
    request_id TEXT NOT NULL,
    record_id INTEGER NOT NULL,
    mac TEXT NOT NULL,
    PRIMARY KEY (kind, request_id)
);
CREATE TABLE IF NOT EXISTS recovery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    mac TEXT NOT NULL
);
-- 设备驱动台账（跨进程的“是否已实际下发高危命令”承诺）。
-- 每个选择至多一条：T1 同事务登记 CLAIMED（即将/正在驱动设备），
-- T2 同事务置为 CONFIRMED（结果已落盘）。CLAIMED 在崩溃后仍然成立：
-- 恢复时把对应执行保留为 UNKNOWN（已驱动、结果未见证），而非回滚重发。
CREATE TABLE IF NOT EXISTS device_drive_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    selection_id INTEGER NOT NULL UNIQUE,
    execution_id INTEGER,
    device_id TEXT NOT NULL,
    op_id TEXT NOT NULL,
    summary TEXT NOT NULL,
    state TEXT NOT NULL,              -- CLAIMED | CONFIRMED
    claimed_at_ms INTEGER NOT NULL,
    confirmed_at_ms INTEGER,
    mac TEXT NOT NULL
);
"""

# MAC 覆盖的列（不含 mac 自身）。
SEL_COLS = ["id", "request_id", "device_id", "op_id", "summary",
            "expires_at_ms", "status", "created_at_ms"]
EXE_COLS = ["id", "selection_id", "request_id", "device_id", "op_id",
            "summary", "state", "result_json", "started_at_ms",
            "finished_at_ms"]
LEDGER_COLS = ["id", "selection_id", "execution_id", "device_id",
               "op_id", "summary", "state", "claimed_at_ms",
               "confirmed_at_ms"]

# 执行状态：EXECUTING 仅存在于单次执行流程中；进程死亡后经启动恢复，
# 已驱动的悬空执行转为 UNKNOWN（设备已下发、结果未见证），未驱动的才删除。
STATE_EXECUTING = "EXECUTING"
STATE_EXECUTED = "EXECUTED"
STATE_UNKNOWN = "UNKNOWN"
LEDGER_CLAIMED = "CLAIMED"
LEDGER_CONFIRMED = "CONFIRMED"


class Store:
    def __init__(self, db_path: str, mac_key: str) -> None:
        self.db_path = db_path
        self.mac_key = mac_key
        # 所有连接访问（读+写）都在同一把可重入锁下串行。写事务持锁到
        # COMMIT，使其他线程永远看不到事务中间态（例如 MAC 尚未重算的行）。
        self._lock = threading.RLock()
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        # WAL + 同步 FULL：提交即 durable，模拟断电时已提交记录不丢失。
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        self.integrity_errors: List[str] = []

    # ---------- MAC 辅助 ----------
    def _mac(self, table: str, row: Dict[str, Any]) -> str:
        return security.compute_mac(self.mac_key, table, row)

    def _row_mac_ok(self, table: str, row: sqlite3.Row) -> bool:
        d = dict(row)
        mac = d.pop("mac", None)
        return security.verify_mac(self.mac_key, table, d, mac)

    def verify_all_integrity(self) -> List[str]:
        """逐表逐行重算 HMAC。供启动恢复与 /health 使用。

        数据库文件级损坏/不可读也视为无法校验，记录为完整性错误，
        使健康检查反映异常、变更操作 fail-closed，而非抛出 500。
        """
        with self._lock:
            self.integrity_errors = []
            for table in ("selections", "executions", "request_index",
                          "recovery_events", "device_drive_ledger"):
                try:
                    rows = self.conn.execute(
                        f"SELECT * FROM {table}").fetchall()
                except sqlite3.Error as exc:
                    self.integrity_errors.append(
                        f"持久记录不可读/已损坏: table={table} error={exc}")
                    continue
                for r in rows:
                    if not self._row_mac_ok(table, r):
                        self.integrity_errors.append(
                            f"记录完整性校验失败: table={table}"
                            f" id={r['id'] if 'id' in r.keys() else '?'}")
            return list(self.integrity_errors)

    def healthy(self) -> bool:
        return not self.verify_all_integrity()

    def _log_recovery_locked(self, now_ms: int, kind: str, detail: str) -> None:
        cur = self.conn.execute(
            "INSERT INTO recovery_events(at_ms, kind, detail, mac)"
            " VALUES (?,?,?,'')", (now_ms, kind, detail))
        rid = cur.lastrowid
        mac = self._mac("recovery_events",
                        {"id": rid, "at_ms": now_ms, "kind": kind,
                         "detail": detail})
        self.conn.execute("UPDATE recovery_events SET mac=? WHERE id=?",
                          (mac, rid))

    # ---------- 恢复 ----------
    def _rollback_executing_locked(self, ex: sqlite3.Row,
                                   now_ms: int) -> Optional[str]:
        """锁内：删除未驱动的悬空 EXECUTING 并恢复选择状态。返回选择新状态。"""
        sel = self.conn.execute(
            "SELECT * FROM selections WHERE id=?",
            (ex["selection_id"],)).fetchone()
        new_status: Optional[str] = None
        if sel and sel["status"] == "CONSUMED":
            new_status = ("ACTIVE" if sel["expires_at_ms"] > now_ms
                          else "EXPIRED")
            d = dict(sel)
            d["status"] = new_status
            d.pop("mac", None)
            self.conn.execute(
                "UPDATE selections SET status=?, mac=? WHERE id=?",
                (new_status, self._mac("selections", d), sel["id"]))
        self.conn.execute("DELETE FROM executions WHERE id=?", (ex["id"],))
        self.conn.execute(
            "DELETE FROM request_index WHERE kind='execution'"
            " AND record_id=?", (ex["id"],))
        self._log_recovery_locked(
            now_ms, "rollback_dangling_execution",
            json.dumps({"execution_id": ex["id"],
                        "selection_id": ex["selection_id"],
                        "selection_status": new_status},
                       ensure_ascii=False))
        return new_status

    def _retain_unknown_locked(self, ex: sqlite3.Row,
                               now_ms: int) -> None:
        """锁内：设备已驱动而结果未落盘 -> 保留为 UNKNOWN，绝不回滚重发。

        选择维持 CONSUMED；执行记录由 EXECUTING 转为 UNKNOWN
        （设备驱动已发生、但本进程未见证结果），等待恢复后的同一稳定
        操作标识以“确认结果”补全，而不是再次驱动载荷。
        """
        d = dict(ex)
        d["state"] = STATE_UNKNOWN
        d.pop("mac", None)
        self.conn.execute(
            "UPDATE executions SET state=?, mac=? WHERE id=?",
            (STATE_UNKNOWN, self._mac("executions", d), ex["id"]))
        # 台账保持 CLAIMED（尚未有结果落盘），但回填执行 id 以固定关联。
        led = self.conn.execute(
            "SELECT * FROM device_drive_ledger WHERE selection_id=?",
            (ex["selection_id"],)).fetchone()
        if led is not None and led["execution_id"] != ex["id"]:
            ld = dict(led)
            ld["execution_id"] = ex["id"]
            ld.pop("mac", None)
            self.conn.execute(
                "UPDATE device_drive_ledger SET execution_id=?, mac=?"
                " WHERE id=?",
                (ex["id"], self._mac("device_drive_ledger", ld),
                 led["id"]))
        self._log_recovery_locked(
            now_ms, "retain_driven_execution_unknown",
            json.dumps({"execution_id": ex["id"],
                        "selection_id": ex["selection_id"],
                        "op_id": ex["op_id"]},
                       ensure_ascii=False))

    def rollback_executing(self, execution_id: int,
                           now_ms: int) -> Optional[str]:
        """删除一条“设备未驱动”的悬空 EXECUTING，选择回到可重试态。

        若该执行已在台账登记驱动承诺（CLAIMED/CONFIRMED），说明设备可能
        已被实际驱动，拒绝回滚，避免恢复后重发高危命令。返回选择新状态；
        拒绝回滚时返回 None。
        """
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                ex = self.conn.execute(
                    "SELECT * FROM executions WHERE id=? AND state='EXECUTING'",
                    (execution_id,)).fetchone()
                new_status: Optional[str] = None
                if ex is not None:
                    claimed = self.conn.execute(
                        "SELECT 1 FROM device_drive_ledger"
                        " WHERE selection_id=? LIMIT 1",
                        (ex["selection_id"],)).fetchone()
                    if claimed is not None:
                        # 驱动承诺已 durable：不得回滚，交由恢复/确认路径处理。
                        self._retain_unknown_locked(ex, now_ms)
                    else:
                        new_status = self._rollback_executing_locked(
                            ex, now_ms)
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return new_status

    def recover(self, now_ms: int) -> Dict[str, Any]:
        """启动恢复：完整性校验 + 悬空执行裁决 + 过期选择标记。

        对每条悬空 EXECUTING：
          * 设备驱动台账存在承诺行（CLAIMED/CONFIRMED）-> 设备已驱动，
            保留为 UNKNOWN，选择维持 CONSUMED（不重发）；
          * 否则视为设备未驱动，删除执行并把选择恢复为 ACTIVE/EXPIRED。
        """
        errors = self.verify_all_integrity()
        rolled_back: List[int] = []
        retained_unknown: List[int] = []
        if not errors:
            with self._lock:
                self.conn.execute("BEGIN IMMEDIATE")
                try:
                    dangling = self.conn.execute(
                        "SELECT * FROM executions WHERE state='EXECUTING'"
                    ).fetchall()
                    for ex in dangling:
                        claimed = self.conn.execute(
                            "SELECT * FROM device_drive_ledger"
                            " WHERE selection_id=?",
                            (ex["selection_id"],)).fetchone()
                        if claimed is not None:
                            self._retain_unknown_locked(ex, now_ms)
                            retained_unknown.append(ex["id"])
                        else:
                            self._rollback_executing_locked(ex, now_ms)
                            rolled_back.append(ex["id"])
                    # 到点未失效选择标记过期，并同步重算 MAC。
                    # 注意：CONSUMED 选择即使越过失效时刻也不标记，
                    # 因为其高危动作可能已经发生，必须维持可回放/可确认态。
                    due = self.conn.execute(
                        "SELECT * FROM selections WHERE status='ACTIVE'"
                        " AND expires_at_ms<=?", (now_ms,)).fetchall()
                    for sel in due:
                        d = dict(sel)
                        d["status"] = "EXPIRED"
                        d.pop("mac", None)
                        self.conn.execute(
                            "UPDATE selections SET status='EXPIRED', mac=?"
                            " WHERE id=?",
                            (self._mac("selections", d), sel["id"]))
                    self.conn.execute("COMMIT")
                except Exception:
                    self.conn.execute("ROLLBACK")
                    raise
        return {"integrity_errors": errors,
                "rolled_back_executions": rolled_back,
                "retained_unknown_executions": retained_unknown}

    # ---------- 查询 ----------
    def find_selection(self, selection_id: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM selections WHERE id=?",
                (selection_id,)).fetchone()

    def latest_selection_for_device(
            self, device_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM selections WHERE device_id=? ORDER BY id DESC"
                " LIMIT 1", (device_id,)).fetchone()

    def expire_if_due(self, row: sqlite3.Row, now_ms: int) -> sqlite3.Row:
        if row["status"] == "ACTIVE" and row["expires_at_ms"] <= now_ms:
            with self._lock:
                d = dict(row)
                d["status"] = "EXPIRED"
                d.pop("mac", None)
                self.conn.execute(
                    "UPDATE selections SET status='EXPIRED', mac=?"
                    " WHERE id=?",
                    (self._mac("selections", d), row["id"]))
                row = self.conn.execute(
                    "SELECT * FROM selections WHERE id=?",
                    (row["id"],)).fetchone()
        return row

    def find_by_request(self, kind: str,
                        request_id: str) -> Optional[sqlite3.Row]:
        with self._lock:
            idx = self.conn.execute(
                "SELECT * FROM request_index WHERE kind=? AND request_id=?",
                (kind, request_id)).fetchone()
            if not idx:
                return None
            table = "selections" if kind == "selection" else "executions"
            return self.conn.execute(
                f"SELECT * FROM {table} WHERE id=?",
                (idx["record_id"],)).fetchone()

    def latest_execution_for_selection(
            self, selection_id: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM executions WHERE selection_id=?"
                " ORDER BY id DESC LIMIT 1",
                (selection_id,)).fetchone()

    # ---------- 写入 ----------
    def insert_selection(self, request_id: Optional[str], device_id: str,
                         op_id: str, summary: str, expires_at_ms: int,
                         now_ms: int) -> sqlite3.Row:
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self.conn.execute(
                    "INSERT INTO selections(request_id, device_id, op_id,"
                    " summary, expires_at_ms, status, created_at_ms, mac)"
                    " VALUES (?,?,?,?,?,?,?,'')",
                    (request_id, device_id, op_id, summary, expires_at_ms,
                     "ACTIVE", now_ms))
                sel_id = cur.lastrowid
                row = {"id": sel_id, "request_id": request_id,
                       "device_id": device_id, "op_id": op_id,
                       "summary": summary, "expires_at_ms": expires_at_ms,
                       "status": "ACTIVE", "created_at_ms": now_ms}
                self.conn.execute("UPDATE selections SET mac=? WHERE id=?",
                                  (self._mac("selections", row), sel_id))
                if request_id:
                    idx = {"kind": "selection", "request_id": request_id,
                           "record_id": sel_id}
                    self.conn.execute(
                        "INSERT INTO request_index(kind, request_id,"
                        " record_id, mac) VALUES (?,?,?,?)",
                        ("selection", request_id, sel_id,
                         self._mac("request_index", idx)))
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return self.find_selection(sel_id)  # type: ignore[return-value]

    def insert_executing(self, selection: sqlite3.Row,
                         request_id: Optional[str], now_ms: int) -> int:
        """T1：登记 EXECUTING + 选择置 CONSUMED + 台账 CLAIMED（同一事务）。

        设备驱动承诺与首次消费权在这一事务一并 durable：提交之后才允许
        真正驱动载荷。这样即使驱动后、T2 前断电，重启恢复也能据台账识别
        “该稳定操作已驱动”，从而保留终态而非回滚重发。
        """
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self.conn.execute(
                    "INSERT INTO executions(selection_id, request_id,"
                    " device_id, op_id, summary, state, result_json,"
                    " started_at_ms, finished_at_ms, mac)"
                    " VALUES (?,?,?,?,?,?,?,?,?,'')",
                    (selection["id"], request_id, selection["device_id"],
                     selection["op_id"], selection["summary"], "EXECUTING",
                     None, now_ms, None))
                ex_id = cur.lastrowid
                row = {"id": ex_id, "selection_id": selection["id"],
                       "request_id": request_id,
                       "device_id": selection["device_id"],
                       "op_id": selection["op_id"],
                       "summary": selection["summary"], "state": "EXECUTING",
                       "result_json": None, "started_at_ms": now_ms,
                       "finished_at_ms": None}
                self.conn.execute("UPDATE executions SET mac=? WHERE id=?",
                                  (self._mac("executions", row), ex_id))
                self.conn.execute(
                    "UPDATE selections SET status='CONSUMED' WHERE id=?",
                    (selection["id"],))
                d = dict(selection)
                d["status"] = "CONSUMED"
                d.pop("mac", None)
                self.conn.execute(
                    "UPDATE selections SET mac=? WHERE id=?",
                    (self._mac("selections", d), selection["id"]))
                # 设备驱动台账：CLAIMED 与上面的变更同事务提交。
                # selection_id 上有 UNIQUE 约束：每个选择至多一条驱动承诺，
                # 任何重入都无法建立第二条（违反约束即整事务回滚并报错，
                # fail-closed），从持久层保证“同一次业务操作只驱动一次”。
                led_id = self.conn.execute(
                    "INSERT INTO device_drive_ledger(selection_id,"
                    " execution_id, device_id, op_id, summary, state,"
                    " claimed_at_ms, confirmed_at_ms, mac)"
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    (selection["id"], ex_id, selection["device_id"],
                     selection["op_id"], selection["summary"],
                     LEDGER_CLAIMED, now_ms, None, "")).lastrowid
                led = {"id": led_id, "selection_id": selection["id"],
                       "execution_id": ex_id,
                       "device_id": selection["device_id"],
                       "op_id": selection["op_id"],
                       "summary": selection["summary"],
                       "state": LEDGER_CLAIMED, "claimed_at_ms": now_ms,
                       "confirmed_at_ms": None}
                self.conn.execute(
                    "UPDATE device_drive_ledger SET mac=? WHERE id=?",
                    (self._mac("device_drive_ledger", led), led_id))
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return ex_id

    def drive_claim_for_selection(
            self, selection_id: int) -> Optional[sqlite3.Row]:
        """返回某选择的设备驱动承诺行（CLAIMED/CONFIRMED），无则 None。"""
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM device_drive_ledger WHERE selection_id=?",
                (selection_id,)).fetchone()

    def finish_execution(self, execution_id: int, result: Dict[str, Any],
                         now_ms: int) -> sqlite3.Row:
        """T2：结果落盘，EXECUTING -> EXECUTED，台账 CLAIMED -> CONFIRMED。"""
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                result_json = json.dumps(result, ensure_ascii=False,
                                         sort_keys=True)
                self.conn.execute(
                    "UPDATE executions SET state='EXECUTED', result_json=?,"
                    " finished_at_ms=? WHERE id=?",
                    (result_json, now_ms, execution_id))
                row = self.conn.execute(
                    "SELECT * FROM executions WHERE id=?",
                    (execution_id,)).fetchone()
                d = dict(row)
                d.pop("mac", None)
                self.conn.execute("UPDATE executions SET mac=? WHERE id=?",
                                  (self._mac("executions", d), execution_id))
                # 设备驱动台账与结果同事务确认。
                led = self.conn.execute(
                    "SELECT * FROM device_drive_ledger WHERE selection_id=?",
                    (row["selection_id"],)).fetchone()
                if led is not None:
                    ld = dict(led)
                    ld["state"] = LEDGER_CONFIRMED
                    ld["execution_id"] = execution_id
                    ld["confirmed_at_ms"] = now_ms
                    ld.pop("mac", None)
                    self.conn.execute(
                        "UPDATE device_drive_ledger SET state=?,"
                        " execution_id=?, confirmed_at_ms=?, mac=? WHERE id=?",
                        (LEDGER_CONFIRMED, execution_id, now_ms,
                         self._mac("device_drive_ledger", ld), led["id"]))
                req_id = row["request_id"]
                if req_id:
                    idx = {"kind": "execution", "request_id": req_id,
                           "record_id": execution_id}
                    self.conn.execute(
                        "INSERT OR REPLACE INTO request_index(kind,"
                        " request_id, record_id, mac) VALUES (?,?,?,?)",
                        ("execution", req_id, execution_id,
                         self._mac("request_index", idx)))
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return self.conn.execute("SELECT * FROM executions WHERE id=?",
                                 (execution_id,)).fetchone()  # type: ignore

    def confirm_unknown_execution(self, execution_id: int,
                                  result: Dict[str, Any],
                                  now_ms: int) -> sqlite3.Row:
        """恢复后补全：把 UNKNOWN 执行以首次结果补落盘（不重新驱动设备）。

        设备动作在崩溃前已经发生，这里只是把与首次一致的业务结论补记为
        终态：execution UNKNOWN -> EXECUTED，台账 CLAIMED -> CONFIRMED。
        """
        return self.finish_execution(execution_id, result, now_ms)

    def ensure_execution_request_index(self, request_id: str,
                                       execution_id: int) -> None:
        """确保执行请求标识指向某终态执行记录（竞争失败方重放时补登记）。"""
        with self._lock:
            idx = {"kind": "execution", "request_id": request_id,
                   "record_id": execution_id}
            self.conn.execute(
                "INSERT OR IGNORE INTO request_index(kind, request_id,"
                " record_id, mac) VALUES (?,?,?,?)",
                ("execution", request_id, execution_id,
                 self._mac("request_index", idx)))

    def ping(self) -> bool:
        try:
            with self._lock:
                self.conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def close(self) -> None:
        with self._lock:
            self.conn.close()
