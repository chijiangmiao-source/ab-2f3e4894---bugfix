"""持久记录完整性保护。

每条选择/执行记录入库时附带 HMAC-SHA256（密钥来自配置），覆盖该行全部
业务字段并绑定表名与记录类型；启动恢复及健康检查逐行重算校验。任何被
外部篡改、缺失 MAC 或无法按规范解析的记录都视为“无法校验的持久记录”，
健康检查必须反映异常。
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Mapping


class RecordIntegrityError(Exception):
    """持久记录无法通过完整性校验（被篡改、缺失 MAC 或格式非法）。"""


def _canonical(payload: Any) -> bytes:
    # sort_keys 保证字段顺序稳定；separators 压缩避免空白差异；
    # ensure_ascii=False + utf-8 使中文命令摘要的编码确定。
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)
    return text.encode("utf-8")


def compute_mac(key: str, table: str, row: Mapping[str, Any]) -> str:
    payload = {"table": table, "row": dict(row)}
    return hmac.new(key.encode("utf-8"), _canonical(payload),
                    hashlib.sha256).hexdigest()


def verify_mac(key: str, table: str, row: Mapping[str, Any],
               mac: str | None) -> bool:
    if not mac:
        return False
    expected = compute_mac(key, table, row)
    return hmac.compare_digest(expected, mac)


def verify_or_raise(key: str, table: str, row: Mapping[str, Any],
                    mac: str | None) -> None:
    if not verify_mac(key, table, row, mac):
        raise RecordIntegrityError(
            f"记录完整性校验失败: table={table} id={row.get('id')!r}")
