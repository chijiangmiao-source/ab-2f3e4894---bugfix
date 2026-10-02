"""载荷设备模拟器。

高危命令真正下发到设备的出口在此。设备侧按“稳定操作标识”幂等：同一业务
操作重复到达不应产生重复副作用，且返回内容确定的结果，使执行在断电恢复
后的“安全重试/确认”得到与首次一致的业务结论。

两层去驱动保护：
  * 进程内 ``_drive_count`` 仅用于并发观测（同一进程的竞争只放行一个）；
  * 跨进程重启的“是否已实际驱动”由持久化层的设备驱动台账
    （device_drive_ledger，见 store.py）裁决。service 层在调用
    :meth:`execute` 前，必须先让台账提交点与 T1 在同一事务落盘——
    即使设备已驱动而结果未落盘即断电，重启后同一次业务操作也只会被
    “确认结果”，而不会再次进入本方法产生第二次实际下发。
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from typing import Any, Dict, Optional


class PayloadDevice:
    def __init__(self, audit_path: Optional[str] = None) -> None:
        self._lock = threading.Lock()
        # 仅用于进程内观测：同一操作标识实际驱动硬件的次数。
        # 跨进程重启后的“是否重复驱动”以持久化台账为准，而非此计数。
        self._drive_count: Dict[str, int] = {}
        # 可选的跨进程“实际驱动”审计文件：每真正下发一次追加一行并 fsync。
        # 供断电恢复验收在进程重启后统计真实下发总次数（生产环境可留空）。
        self._audit_path = audit_path or os.environ.get(
            "DUTY_DRIVE_AUDIT_PATH")

    def drive_count(self, op_id: str) -> int:
        with self._lock:
            return self._drive_count.get(op_id, 0)

    def _append_audit(self, device_id: str, op_id: str,
                      summary: str, receipt: str) -> None:
        if not self._audit_path:
            return
        line = json.dumps(
            {"at_ns": time.time_ns(), "device_id": device_id,
             "op_id": op_id, "summary": summary, "receipt": receipt},
            ensure_ascii=False) + "\n"
        # O_APPEND 下各进程的短小写为原子追加；逐次 fsync 模拟下发即留痕。
        fd = os.open(self._audit_path,
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, line.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def deterministic_result(device_id: str, op_id: str,
                             summary: str) -> Dict[str, Any]:
        """设备对一次业务操作的确定性结果：只取决于三要素，可安全重放。

        真实设备在此提供“按稳定操作标识查询上次下发结果”的幂等查询；
        模拟器以输入的哈希充当确定收据。恢复后若结果未及落盘，服务用
        本方法补出与首次下发完全一致的业务结论，而不重复驱动硬件。
        """
        token = hashlib.sha256(
            f"{device_id}|{op_id}|{summary}".encode("utf-8")
        ).hexdigest()[:16]
        return {"code": 0, "status": "delivered",
                "echo": summary, "receipt": token}

    def execute(self, device_id: str, op_id: str, summary: str) -> Dict[str, Any]:
        """把高危命令下发给载荷。结果完全由输入决定，可安全重放。

        调用方（service 层）必须在调用前于持久化层取得该操作的
        “设备驱动提交点”（与 T1 同事务提交）。该提交点一旦 durable，
        即使随即断电，恢复后的同一操作也只确认结果、不再调用本方法。
        """
        with self._lock:
            self._drive_count[op_id] = self._drive_count.get(op_id, 0) + 1
        # 真实系统此处为总线/链路下发；模拟器始终成功，结果确定。
        result = self.deterministic_result(device_id, op_id, summary)
        self._append_audit(device_id, op_id, summary, result["receipt"])
        return result
