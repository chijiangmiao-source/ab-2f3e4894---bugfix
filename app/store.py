"""SQLite 持久化层与启动恢复。

状态机：
  selections.status: ACTIVE -> CONSUMED（执行成功）
                     ACTIVE -> EXPIRED（过期，惰性或恢复时标记）
  executions.state:  EXECUTING -> EXECUTED（结果落盘）
                     EXECUTING -> RECOVERING（重启恢复：设备可能已驱动）
                     RECOVERING -> EXECUTED（恢复后确认/补发并落盘结果）

落盘分两段事务：
  T1 写 execution(EXECUTING) + selection(CONSUMED) 并提交；
  T2 写 execution(EXECUTED, result) 并提交。
T1 后 T2 前断电 -> T1 代表“高危命令已取得持久驱动意向”，重启时**不得删除**：
  执行转为 RECOVERING、选择保持 CONSUMED，由业务层先向设备做无副作用确认，
  已驱动则采信首次结果完成 T2，绝不重复下发；仅当重启时选择已过期才回滚。
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
"""

# MAC 覆盖的列（不含 mac 自身）。
SEL_COLS = ["id", "request_id", "device_id", "op_id", "summary",
            "expires_at_ms", "status", "created_at_ms"]
EXE_COLS = ["id", "selection_id", "request_id", "device_id", "op_id",
            "summary", "state", "result_json", "started_at_ms",
            "finished_at_ms"]


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
                          "recovery_events"):
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
    def _set_execution_state_locked(self, ex: sqlite3.Row,
                                    state: str) -> None:
        """锁内：更新执行状态并重算 MAC。"""
        d = dict(ex)
        d["state"] = state
        d.pop("mac", None)
        self.conn.execute(
            "UPDATE executions SET state=?, mac=? WHERE id=?",
            (state, self._mac("executions", d), ex["id"]))

    def _rollback_executing_locked(self, ex: sqlite3.Row,
                                   now_ms: int) -> Optional[str]:
        """锁内：删除悬空 EXECUTING 并恢复选择状态。返回选择新状态。"""
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

    def _recover_executing_locked(self, ex: sqlite3.Row,
                                  now_ms: int) -> str:
        """把悬空 EXECUTING 转为 RECOVERING，保留持久驱动意向。

        T1 提交意味着该高危命令已取得持久驱动意向，设备可能已实际动作，
        删除记录会让恢复后的重试无从识别这是同一操作的延续，从而把高危
        命令再次下发。业务层随后只能以无副作用确认采信首次结果，或在
        确认“确未下发”时补发一次。

        选择在重启时刻已过期的例外：动作即使发生也已超出业务授权窗口，
        按原逻辑回滚为 EXPIRED 并删除悬空执行。
        """
        sel = self.conn.execute(
            "SELECT * FROM selections WHERE id=?",
            (ex["selection_id"],)).fetchone()
        if sel is not None and sel["expires_at_ms"] <= now_ms:
            self._rollback_executing_locked(ex, now_ms)
            return "rolled_back"
        self._set_execution_state_locked(ex, "RECOVERING")
        self._log_recovery_locked(
            now_ms, "recover_dangling_execution",
            json.dumps({"execution_id": ex["id"],
                        "selection_id": ex["selection_id"],
                        "state": "RECOVERING"},
                       ensure_ascii=False))
        return "recovering"

    def rollback_executing(self, execution_id: int,
                           now_ms: int) -> Optional[str]:
        """删除一条悬空 EXECUTING，选择回到可安全重试态。返回选择新状态。"""
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                ex = self.conn.execute(
                    "SELECT * FROM executions WHERE id=? AND state='EXECUTING'",
                    (execution_id,)).fetchone()
                new_status = (self._rollback_executing_locked(ex, now_ms)
                              if ex is not None else None)
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return new_status

    def mark_recovering(self, execution_id: int,
                        now_ms: int) -> Optional[sqlite3.Row]:
        """EXECUTING -> RECOVERING：设备已驱动但 T2 失败时保留驱动意向。"""
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                ex = self.conn.execute(
                    "SELECT * FROM executions WHERE id=?",
                    (execution_id,)).fetchone()
                if ex is not None and ex["state"] in (
                        "EXECUTING", "RECOVERING"):
                    self._set_execution_state_locked(ex, "RECOVERING")
                    self._log_recovery_locked(
                        now_ms, "mark_recovering_after_persist_failure",
                        json.dumps({"execution_id": execution_id},
                                   ensure_ascii=False))
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        if ex is None:
            return None
        return self.conn.execute("SELECT * FROM executions WHERE id=?",
                                 (execution_id,)).fetchone()

    def resolve_recovering(self, execution_id: int,
                           result: Dict[str, Any], now_ms: int) -> sqlite3.Row:
        """RECOVERING/EXECUTING -> EXECUTED：把确认/补发到的结果落盘（T2）。"""
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT * FROM executions WHERE id=?",
                    (execution_id,)).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"待恢复执行不存在: id={execution_id}")
                if row["state"] not in ("RECOVERING", "EXECUTING"):
                    self.conn.execute("COMMIT")
                    return self.conn.execute(
                        "SELECT * FROM executions WHERE id=?",
                        (execution_id,)).fetchone()
                result_json = json.dumps(result, ensure_ascii=False,
                                         sort_keys=True)
                self.conn.execute(
                    "UPDATE executions SET state='EXECUTED', result_json=?,"
                    " finished_at_ms=? WHERE id=?",
                    (result_json, now_ms, execution_id))
                final = self.conn.execute(
                    "SELECT * FROM executions WHERE id=?",
                    (execution_id,)).fetchone()
                d = dict(final)
                d.pop("mac", None)
                self.conn.execute(
                    "UPDATE executions SET mac=? WHERE id=?",
                    (self._mac("executions", d), execution_id))
                req_id = final["request_id"]
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

    def recover(self, now_ms: int) -> Dict[str, Any]:
        """启动恢复：完整性校验 + 悬空执行恢复 + 过期选择标记。"""
        errors = self.verify_all_integrity()
        recovered: List[int] = []
        rolled_back: List[int] = []
        if not errors:
            with self._lock:
                self.conn.execute("BEGIN IMMEDIATE")
                try:
                    dangling = self.conn.execute(
                        "SELECT * FROM executions WHERE state='EXECUTING'"
                    ).fetchall()
                    for ex in dangling:
                        outcome = self._recover_executing_locked(ex, now_ms)
                        if outcome == "recovering":
                            recovered.append(ex["id"])
                        else:
                            rolled_back.append(ex["id"])
                    # 到点未失效选择标记过期，并同步重算 MAC。
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
                "recovered_executions": recovered,
                "rolled_back_executions": rolled_back}

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
        """T1：登记 EXECUTING 并把选择置为 CONSUMED（同一事务提交）。"""
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
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return ex_id

    def finish_execution(self, execution_id: int, result: Dict[str, Any],
                         now_ms: int) -> sqlite3.Row:
        """T2：结果落盘，EXECUTING -> EXECUTED。"""
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
