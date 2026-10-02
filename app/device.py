"""载荷设备模拟器（高危命令真正下发到载荷的出口）。

设备在总线上的每次**实际下发**都会追加到一条持久驱动台账（JSONL，逐条
flush + fsync）。台账的意义：

  * “实际驱动次数”跨进程重启可观测、可裁决——进程内计数器会随断电归零，
    但持久台账不会，恢复后是否重复下发高危命令一查便知；
  * 地面值班系统恢复一条“已取得持久驱动意向、但结果未落盘”的执行时，
    只能通过 ``confirm()`` 做**无副作用查询**来确认首次动作是否到达、
    采信其既有结果，绝不允许靠重新下发高危命令来“补记”结果。

执行结果完全由 (设备标识, 稳定操作标识, 命令摘要) 决定，确认/回放得到的
业务结论与首次驱动必然一致。
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Any, Dict, List, Optional, Tuple


def deterministic_result(device_id: str, op_id: str,
                         summary: str) -> Dict[str, Any]:
    token = hashlib.sha256(
        f"{device_id}|{op_id}|{summary}".encode("utf-8")
    ).hexdigest()[:16]
    return {"code": 0, "status": "delivered",
            "echo": summary, "receipt": token}


class PayloadDevice:
    def __init__(self, journal_path: Optional[str] = None) -> None:
        self._lock = threading.Lock()
        # journal_path 为 None 时退化为纯进程内台账（仅隔离单测使用）；
        # 生产/验收路径始终指向持久文件，使驱动次数跨重启可观测。
        self._journal_path = journal_path
        self._mem: List[Dict[str, Any]] = []

    # ---------- 持久驱动台账 ----------
    def _append_journal(self, record: Dict[str, Any]) -> None:
        if self._journal_path is None:
            self._mem.append(record)
            return
        directory = os.path.dirname(self._journal_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        # O_APPEND 追加 + fsync：返回前台之前该次驱动已 durable，
        # 之后任何断电都不会抹掉“动作已经发生”这一事实。
        with open(self._journal_path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _read_journal(self) -> List[Dict[str, Any]]:
        if self._journal_path is None:
            return list(self._mem)
        if not os.path.exists(self._journal_path):
            return []
        rows: List[Dict[str, Any]] = []
        with open(self._journal_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    # ---------- 对外动作 ----------
    def execute(self, device_id: str, op_id: str,
                summary: str) -> Dict[str, Any]:
        """把高危命令下发给载荷（有真实副作用），并持久登记该次驱动。"""
        result = deterministic_result(device_id, op_id, summary)
        record = {"device_id": device_id, "op_id": op_id,
                  "summary": summary, "result": result}
        with self._lock:
            self._append_journal(record)
        # 真实系统此处为总线/链路下发；模拟器始终成功，结果确定。
        return result

    def confirm(self, device_id: str, op_id: str
                ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """无副作用查询：该稳定操作标识此前是否已实际驱动过。

        返回 (是否驱动过, 首次驱动的确定结果)。不产生任何总线动作，
        供断电恢复后的安全重试裁决使用——已驱动则只确认、不重发。
        """
        with self._lock:
            found: Optional[Dict[str, Any]] = None
            for row in self._read_journal():
                if row["device_id"] == device_id and row["op_id"] == op_id:
                    found = row  # 取最后一条（同一稳定操作结果确定，任选其一）
            if found is None:
                return False, None
            return True, found["result"]

    def drive_count(self, op_id: str) -> int:
        """观测用：同一操作标识累计实际驱动硬件的次数（跨进程持久统计）。"""
        with self._lock:
            return sum(1 for row in self._read_journal()
                       if row["op_id"] == op_id)
