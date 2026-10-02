"""运行配置：路径、密钥与崩溃注入点均可用环境变量覆盖，便于断电恢复测试。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    db_path: str = os.environ.get("DUTY_DB_PATH", "/data/duty.db")
    http_host: str = os.environ.get("DUTY_HTTP_HOST", "0.0.0.0")
    http_port: int = int(os.environ.get("DUTY_HTTP_PORT", "8080"))
    # 持久记录 HMAC 密钥；生产环境应注入强密钥，测试环境可沿用默认。
    mac_key: str = os.environ.get("DUTY_MAC_KEY", "ground-station-duty-secret")

    # —— 崩溃注入（仅用于验收：在执行结果落盘“前/后”注入中断）——
    # before_persist: 已取得提交锁、更新 EXECUTING 后、写 EXECUTED/结果前崩溃
    # after_persist:  执行结果事务已提交、释放锁之后崩溃
    crash_before_persist: bool = False
    crash_after_persist: bool = False

    @staticmethod
    def from_env() -> "Config":
        return Config(
            db_path=os.environ.get("DUTY_DB_PATH", "/data/duty.db"),
            http_host=os.environ.get("DUTY_HTTP_HOST", "0.0.0.0"),
            http_port=int(os.environ.get("DUTY_HTTP_PORT", "8080")),
            mac_key=os.environ.get("DUTY_MAC_KEY",
                                  "ground-station-duty-secret"),
            crash_before_persist=_bool("CRASH_BEFORE_PERSIST"),
            crash_after_persist=_bool("CRASH_AFTER_PERSIST"),
        )
