"""载荷设备模拟器。

高危命令真正下发到设备的出口在此。设备侧按“稳定操作标识”幂等：同一操作
标识重复驱动不会产生重复副作用，且返回内容确定的结果，使执行在断电恢复
后的“安全重试”得到与首次一致的裁决回放。
"""
from __future__ import annotations

import hashlib
import threading
from typing import Any, Dict


class PayloadDevice:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 仅用于观测：同一操作标识实际驱动硬件的次数（进程内）。
        self._drive_count: Dict[str, int] = {}

    def drive_count(self, op_id: str) -> int:
        with self._lock:
            return self._drive_count.get(op_id, 0)

    def execute(self, device_id: str, op_id: str, summary: str) -> Dict[str, Any]:
        """把高危命令下发给载荷。结果完全由输入决定，可安全重放。"""
        with self._lock:
            self._drive_count[op_id] = self._drive_count.get(op_id, 0) + 1
        token = hashlib.sha256(
            f"{device_id}|{op_id}|{summary}".encode("utf-8")
        ).hexdigest()[:16]
        # 真实系统此处为总线/链路下发；模拟器始终成功，结果确定。
        return {"code": 0, "status": "delivered",
                "echo": summary, "receipt": token}
