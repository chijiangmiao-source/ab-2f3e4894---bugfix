"""HTTP/JSON 接口（仅依赖标准库 http.server）。

路由：
  GET  /health                        健康检查（含持久记录完整性）
  POST /api/v1/selections             创建/重传选择
  POST /api/v1/executions             消费选择执行/回放
  GET  /api/v1/selections?device_id=  查询设备最新选择
所有业务错误返回 JSON：{"error": ..., "message": ...}，并带合适 HTTP 状态码。
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from .service import ApiError, DutyService


class _Handler(BaseHTTPRequestHandler):
    server_version = "DutyService/1.0"

    @property
    def service(self) -> DutyService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静日志
        return

    # ---------- 收发 ----------
    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise ApiError(400, "INVALID_BODY", "请求体必须是 JSON 对象")
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            raise ApiError(400, "INVALID_BODY", "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "INVALID_BODY", "请求体必须是 JSON 对象")
        return data

    def _handle(self, fn) -> None:
        try:
            status, payload = fn()
        except ApiError as exc:
            self._send_json(exc.http_status, exc.to_dict())
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"error": "INTERNAL_ERROR",
                                  "message": str(exc)})
        else:
            self._send_json(status, payload)

    # ---------- 路由 ----------
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            def health():
                payload = self.service.health()
                # 完整性异常时以 5xx 明确反映，便于编排探针/验收判定。
                return (200 if payload["status"] == "ok" else 503, payload)
            self._handle(health)
            return
        if parsed.path == "/api/v1/selections":
            def query():
                qs = parse_qs(parsed.query)
                device_id = (qs.get("device_id") or [""])[0]
                if not device_id:
                    raise ApiError(400, "INVALID_FIELD",
                                   "查询参数 device_id 必填")
                row = self.service.store.latest_selection_for_device(
                    device_id)
                if row is None:
                    raise ApiError(404, "NO_SELECTION", "该设备不存在选择")
                row = self.service.store.expire_if_due(
                    row, self.service.now())
                return (200, self.service._selection_json(row, False))
            self._handle(query)
            return
        self._send_json(404, {"error": "NOT_FOUND", "message": "未知路由"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/v1/selections":
            def create():
                body = self._read_json()
                return (201, self.service.create_selection(body))
            self._handle(create)
            return
        if parsed.path == "/api/v1/executions":
            def execute():
                body = self._read_json()
                return (200, self.service.execute(body))
            self._handle(execute)
            return
        self._send_json(404, {"error": "NOT_FOUND", "message": "未知路由"})


def build_server(service: DutyService, host: str,
                 port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.service = service  # type: ignore[attr-defined]
    return server
