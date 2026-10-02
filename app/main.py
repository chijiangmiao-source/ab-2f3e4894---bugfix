"""值班系统进程入口：打开存储 -> 启动恢复 -> 提供 HTTP 服务。"""
from __future__ import annotations

import logging
import os
import sys

from .config import Config
from .device import PayloadDevice
from .httpapi import build_server
from .service import DutyService, now_ms
from .store import Store


def main() -> int:
    config = Config.from_env()
    logging.basicConfig(
        level=os.environ.get("DUTY_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("duty")

    store = Store(config.db_path, config.mac_key)
    recovery = store.recover(now_ms())
    if recovery["integrity_errors"]:
        log.error("启动恢复发现无法校验的持久记录: %s",
                  recovery["integrity_errors"])
    if recovery["recovered_executions"]:
        log.warning("恢复悬空执行为 RECOVERING（待重试无副作用确认）: %s",
                    recovery["recovered_executions"])
    if recovery["rolled_back_executions"]:
        log.warning("回滚重启时已过期的悬空执行: %s",
                    recovery["rolled_back_executions"])

    device = PayloadDevice(config.device_journal_path)
    service = DutyService(store, device, config)
    server = build_server(service, config.http_host, config.http_port)
    log.info("值班系统监听 %s:%s (db=%s)", config.http_host,
             config.http_port, config.db_path)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
