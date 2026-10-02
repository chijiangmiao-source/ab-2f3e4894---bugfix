#!/usr/bin/env python3
"""容器健康探针：/health 返回 200 视为健康，其余（含 503 降级）视为失败。"""
from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request

port = os.environ.get("DUTY_HTTP_PORT", "8080")
url = f"http://127.0.0.1:{port}/health"
try:
    with urllib.request.urlopen(url, timeout=3) as resp:
        sys.exit(0 if resp.status == 200 else 1)
except (urllib.error.URLError, OSError):
    sys.exit(1)
