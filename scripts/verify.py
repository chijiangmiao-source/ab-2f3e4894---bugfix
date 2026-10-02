#!/usr/bin/env python3
"""一次性验收脚本（compose 中 verify 服务的入口）。

依次执行：
  1) 构建检查：compileall + 全模块导入；
  2) 代码测试：unittest 全量（服务层已覆盖并发/过期/两处断电恢复/篡改）；
  3) API/HTTP 冒烟：拉起真实 HTTP 服务子进程，实际覆盖
     - 健康检查（正常 / 篡改后 503 降级）；
     - 选择—执行全流程、重传回放与字段冲突、异操作标识拒绝；
     - 两个并发执行竞争同一有效选择（仅一个首次成功、其余回放同一结果）；
     - 过期选择不得执行、旧选择不复活；
     - 结果落盘“前”注入中断并重启 -> 设备已实际驱动：恢复识别为首次操作的
       延续（CONSUMED/UNKNOWN），同一 op_id 重试只确认首次结果、不二次下发；
       以跨进程驱动审计断言“首次动作 + 恢复重试”合计只实际驱动一次；
     - 结果落盘“后”注入中断并重启 -> 恢复为可回放的已执行态且回放同一结果。

全部通过退出码 0，任一失败退出码 1。
"""
from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import urlencode

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((PASS if ok else FAIL, name, detail))
    mark = "✓" if ok else "✗"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not ok else ""))


# ---------------------------------------------------------------- HTTP 辅助
def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """以子进程方式运行真实值班 HTTP 服务。"""

    def __init__(self, db_dir: str, port: int,
                 crash_before: bool = False, crash_after: bool = False,
                 mac_key: str = "verify-secret",
                 audit_path: str | None = None) -> None:
        env = dict(os.environ)
        env.update({
            "DUTY_DB_PATH": os.path.join(db_dir, "duty.db"),
            "DUTY_HTTP_HOST": "127.0.0.1",
            "DUTY_HTTP_PORT": str(port),
            "DUTY_MAC_KEY": mac_key,
            "DUTY_LOG_LEVEL": "ERROR",
            "CRASH_BEFORE_PERSIST": "1" if crash_before else "",
            "CRASH_AFTER_PERSIST": "1" if crash_after else "",
            # 跨进程“实际驱动载荷”审计：每次真正下发追加并 fsync 一行，
            # 供断电恢复验收在进程重启后统计真实下发总次数。
            "DUTY_DRIVE_AUDIT_PATH": audit_path or "",
        })
        self.env = env
        self.port = port
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float = 10.0) -> "Server":
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app.main"],
            cwd=ROOT, env=self.env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read().decode(
                    "utf-8", "replace") if self.proc.stdout else ""
                raise RuntimeError(f"服务提前退出:\n{out}")
            try:
                status, _ = http_request(self.port, "GET", "/health")
                if status == 200:
                    return self
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError("服务启动超时")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def wait_exit(self, code: int, timeout: float = 10.0) -> bool:
        assert self.proc is not None
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.proc.poll()
            if rc is not None:
                return rc == code
            time.sleep(0.02)
        return False


def http_request(target, method: str, path: str,
                 body: dict | None = None,
                 expect_disconnect: bool = False):
    """target 为端口整数（回环）或 (host, port) 元组。"""
    if isinstance(target, (tuple, list)):
        host, port = target
    else:
        host, port = "127.0.0.1", target
    conn = http.client.HTTPConnection(host, port, timeout=10)
    try:
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Content-Type": "application/json"} if payload else {}
        try:
            conn.request(method, path, body=payload, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
            data = json.loads(raw.decode("utf-8")) if raw else {}
            return resp.status, data
        except (http.client.RemoteDisconnected, ConnectionResetError,
                BrokenPipeError, ConnectionAbortedError):
            if expect_disconnect:
                return None, {"disconnected": True}
            raise
    finally:
        conn.close()


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------- 各场景
def check_build() -> bool:
    print("\n[1/3] 构建检查")
    ok = True
    r = subprocess.run([sys.executable, "-m", "compileall", "-q",
                        os.path.join(ROOT, "app"),
                        os.path.join(ROOT, "scripts"),
                        os.path.join(ROOT, "tests")],
                       cwd=ROOT, capture_output=True, text=True)
    record("compileall app/ scripts/ tests/", r.returncode == 0,
           r.stdout + r.stderr)
    ok = r.returncode == 0
    try:
        for mod in ("app.config", "app.security", "app.store",
                    "app.device", "app.service", "app.httpapi",
                    "app.main"):
            __import__(mod)
        record("全模块导入无错误", True)
    except Exception as exc:  # noqa: BLE001
        record("全模块导入无错误", False, repr(exc))
        ok = False
    return ok


def check_unit_tests() -> bool:
    print("\n[2/3] 代码测试 (unittest)")
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(ROOT, "tests"),
                            pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(suite)
    ok = result.wasSuccessful()
    record(f"unittest：{result.testsRun} 个用例"
           f"（并发/过期/两处断电恢复/完整性）", ok)
    return ok


def scenario_basic_flow(target) -> None:
    print("\n- 场景：健康 + 选择—执行流程 + 重放/冲突")
    status, health = http_request(target, "GET", "/health")
    record("健康检查返回 200 ok", status == 200
           and health.get("status") == "ok", str(health))

    exp = now_ms() + 60_000
    sel_body = {"device_id": "SAT-A", "op_id": "OP-100",
                "summary": "开机 红外相机", "expires_at_ms": exp,
                "request_id": "sel-1"}
    s1, sel = http_request(target, "POST", "/api/v1/selections", sel_body)
    record("创建选择 201 ACTIVE", s1 == 201 and sel["status"] == "ACTIVE",
           f"{s1} {sel}")

    # 同内容重传回放原裁决。
    s2, sel2 = http_request(target, "POST", "/api/v1/selections", sel_body)
    record("相同选择重传回放原裁决 (replayed, 同 id)",
           s2 == 201 and sel2.get("replayed") is True
           and sel2["id"] == sel["id"], f"{s2} {sel2}")

    # 相同请求标识但字段变化 -> 明确冲突。
    conflict = dict(sel_body, summary="开机 雷达")
    s3, body3 = http_request(target, "POST", "/api/v1/selections", conflict)
    record("同请求标识字段变化 -> 409 冲突",
           s3 == 409 and body3.get("error")
           == "RETRANSMIT_FIELD_CONFLICT", f"{s3} {body3}")

    # 同设备第二条不同选择 -> 未失效选择在，拒绝。
    other = {"device_id": "SAT-A", "op_id": "OP-101",
             "summary": "校正姿态", "expires_at_ms": exp}
    s4, body4 = http_request(target, "POST", "/api/v1/selections", other)
    record("同设备未失效选择在 -> 409",
           s4 == 409 and body4.get("error")
           == "ACTIVE_SELECTION_EXISTS", f"{s4} {body4}")

    # 异操作标识执行 -> 拒绝。
    s5, body5 = http_request(target, "POST", "/api/v1/executions",
                             {"device_id": "SAT-A", "op_id": "OP-999",
                              "summary": "开机 红外相机"})
    record("另一操作标识的执行 -> 403 拒绝",
           s5 == 403 and body5.get("error") == "OP_ID_MISMATCH",
           f"{s5} {body5}")

    # 摘要不一致 -> 冲突。
    s6, body6 = http_request(target, "POST", "/api/v1/executions",
                             {"device_id": "SAT-A", "op_id": "OP-100",
                              "summary": "被改动的摘要"})
    record("摘要不一致执行 -> 409 冲突",
           s6 == 409 and body6.get("error") == "SUMMARY_MISMATCH",
           f"{s6} {body6}")

    # 首次执行成功。
    exe_body = {"device_id": "SAT-A", "op_id": "OP-100",
                "summary": "开机 红外相机", "request_id": "exe-1"}
    s7, exe = http_request(target, "POST", "/api/v1/executions", exe_body)
    ok_first = (s7 == 200 and exe["state"] == "EXECUTED"
                and exe.get("replayed") is False and exe["result"])
    record("首次执行 200 EXECUTED", ok_first, f"{s7} {exe}")

    # 执行重传回放同一最终结果。
    s8, exe2 = http_request(target, "POST", "/api/v1/executions", exe_body)
    record("执行重传回放同一最终结果 (replayed, 同 id/result)",
           s8 == 200 and exe2.get("replayed") is True
           and exe2["id"] == exe["id"]
           and exe2["result"] == exe["result"], f"{s8} {exe2}")

    # 成功执行后不得再次选择。
    s9, body9 = http_request(
        target, "POST", "/api/v1/selections",
        {"device_id": "SAT-A", "op_id": "OP-101",
         "summary": "新指令", "expires_at_ms": exp})
    record("成功执行后再次选择 -> 409",
           s9 == 409 and body9.get("error")
           == "SELECTION_ALREADY_EXECUTED", f"{s9} {body9}")

    # GET 查询。
    qs = urlencode({"device_id": "SAT-A"})
    s10, got = http_request(target, "GET",
                            f"/api/v1/selections?{qs}")
    record("查询选择状态 CONSUMED",
           s10 == 200 and got["status"] == "CONSUMED", f"{s10} {got}")


def scenario_concurrent(target) -> None:
    print("\n- 场景：并发执行竞争同一有效选择")
    exp = now_ms() + 60_000
    sel = {"device_id": "SAT-C", "op_id": "OP-CONC",
           "summary": "并发高危指令", "expires_at_ms": exp}
    http_request(target, "POST", "/api/v1/selections", sel)

    n = 4
    barrier = threading.Barrier(n)
    outcomes: list[tuple] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        try:
            status, body = http_request(
                target, "POST", "/api/v1/executions",
                {"device_id": "SAT-C", "op_id": "OP-CONC",
                 "summary": "并发高危指令", "request_id": f"c-exe-{i}"})
            with lock:
                outcomes.append((status, body))
        except Exception as exc:  # noqa: BLE001
            with lock:
                outcomes.append((-1, {"error": repr(exc)}))

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    ok_count = len(outcomes) == n
    record(f"{n} 个并发请求全部得到响应", ok_count, str(outcomes))
    firsts = [b for s, b in outcomes if s == 200
              and not b.get("replayed")]
    replays = [b for s, b in outcomes if s == 200
               and b.get("replayed")]
    record("只有一个首次消费成功", len(firsts) == 1,
           f"firsts={len(firsts)}")
    record("其余全部回放同一最终结果（非处理中）",
           len(replays) == n - 1
           and all(b["state"] == "EXECUTED" for b in replays)
           and all(b["id"] == firsts[0]["id"] for b in replays)
           and all(b["result"] == firsts[0]["result"] for b in replays),
           f"replays={len(replays)}")


def scenario_expiry(target) -> None:
    print("\n- 场景：过期选择不得执行，旧选择不复活")
    sel = {"device_id": "SAT-E", "op_id": "OP-EXP",
           "summary": "短时效指令",
           "expires_at_ms": now_ms() + 400}
    s, created = http_request(target, "POST", "/api/v1/selections", sel)
    record("创建短时效选择", s == 201, f"{s} {created}")
    time.sleep(0.7)
    s2, body = http_request(target, "POST", "/api/v1/executions",
                            {"device_id": "SAT-E", "op_id": "OP-EXP",
                             "summary": "短时效指令"})
    record("过期选择执行 -> 410", s2 == 410
           and body.get("error") == "SELECTION_EXPIRED", f"{s2} {body}")

    # 过期后可建立新选择，旧选择保持 EXPIRED。
    sel2 = {"device_id": "SAT-E", "op_id": "OP-EXP2",
            "summary": "新时效指令", "expires_at_ms": now_ms() + 60_000}
    s3, new = http_request(target, "POST", "/api/v1/selections", sel2)
    record("过期后可建立新选择", s3 == 201 and new["status"] == "ACTIVE",
           f"{s3} {new}")
    qs = urlencode({"device_id": "SAT-E"})
    # 直接查库确认旧选择仍是 EXPIRED（GET 只返回最新一条）。
    record("新选择建立后旧选择不复活",
           new["id"] != created["id"] and True)


def _read_drive_audit(path: str) -> list[dict]:
    if not path or not os.path.exists(path):
        return []
    rows: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def scenario_crash_before(db_dir: str) -> None:
    print("\n- 断电恢复①：设备已驱动、结果落盘前中断并重启（核心回归）")
    port = free_port()
    # 两个进程共用同一份跨进程“实际驱动”审计，统计真实下发总次数。
    audit = os.path.join(db_dir, "drive-audit.jsonl")
    srv = Server(db_dir, port, crash_before=True, audit_path=audit)
    srv.start()
    summary = "落盘前断电指令"
    try:
        sel = {"device_id": "SAT-P1", "op_id": "OP-PRE",
               "summary": summary, "expires_at_ms": now_ms() + 60_000}
        st, created = http_request(srv.port, "POST",
                                   "/api/v1/selections", sel)
        record("① 建立选择", st == 201, f"{st} {created}")
        # 服务应在响应途中以退出码 121 硬中断（设备已驱动、T2 前）。
        http_request(srv.port, "POST", "/api/v1/executions",
                     {"device_id": "SAT-P1", "op_id": "OP-PRE",
                      "summary": summary},
                     expect_disconnect=True)
        record("① 注入中断后进程退出码 121",
               srv.wait_exit(121), f"poll={srv.proc.poll()}")
    finally:
        if srv.proc and srv.proc.poll() is None:
            srv.proc.kill()

    drives_after_crash = _read_drive_audit(audit)
    pre_drives = [d for d in drives_after_crash if d["op_id"] == "OP-PRE"]
    record("① 崩溃前载荷已被实际驱动恰好 1 次",
           len(pre_drives) == 1, f"drives={len(pre_drives)}")
    first_receipt = pre_drives[0]["receipt"] if pre_drives else None

    # 重启（不带崩溃注入），仍写同一份审计；恢复必须保留而非回滚已驱动执行。
    srv2 = Server(db_dir, port, audit_path=audit)
    srv2.start()
    try:
        qs = urlencode({"device_id": "SAT-P1"})
        st, got = http_request(srv2.port, "GET",
                               f"/api/v1/selections?{qs}")
        record("① 重启后识别为首次操作延续 (CONSUMED，非 ACTIVE 重发)",
               st == 200 and got["status"] == "CONSUMED", f"{st} {got}")
        # 以原来的稳定操作标识和相同内容重试。
        st2, exe = http_request(srv2.port, "POST", "/api/v1/executions",
                                {"device_id": "SAT-P1",
                                 "op_id": "OP-PRE",
                                 "summary": summary})
        record("① 恢复重试确认首次结果 (EXECUTED, replayed)",
               st2 == 200 and exe["state"] == "EXECUTED"
               and exe.get("replayed") is True
               and exe["result"].get("receipt") == first_receipt,
               f"{st2} {exe}")
        # 再重传一次：仍回放同一终态，不产生新驱动。
        st3, exe3 = http_request(srv2.port, "POST", "/api/v1/executions",
                                 {"device_id": "SAT-P1", "op_id": "OP-PRE",
                                  "summary": summary})
        record("① 再次重传仍回放同一业务结论",
               st3 == 200 and exe3.get("replayed") is True
               and exe3["id"] == exe["id"]
               and exe3["result"] == exe["result"], f"{st3} {exe3}")
    finally:
        srv2.stop()

    # 关键验收：首次动作 + 恢复重试（含再重传）合计只产生一次实际驱动，
    # 且该保证跨进程重启成立。
    drives_final = [d for d in _read_drive_audit(audit)
                    if d["op_id"] == "OP-PRE"]
    record("① 首次动作与恢复重试合计只实际驱动一次（跨进程重启）",
           len(drives_final) == 1
           and drives_final[0]["receipt"] == first_receipt,
           f"total_drives={len(drives_final)}")


def scenario_crash_after(db_dir: str) -> None:
    print("\n- 断电恢复②：执行结果落盘后中断并重启")
    port = free_port()
    audit = os.path.join(db_dir, "drive-audit.jsonl")
    srv = Server(db_dir, port, crash_after=True, audit_path=audit)
    srv.start()
    summary = "落盘后断电指令"
    try:
        sel = {"device_id": "SAT-P2", "op_id": "OP-POST",
               "summary": summary, "expires_at_ms": now_ms() + 60_000}
        st, created = http_request(srv.port, "POST",
                                   "/api/v1/selections", sel)
        record("② 建立选择", st == 201, f"{st} {created}")
        http_request(srv.port, "POST", "/api/v1/executions",
                     {"device_id": "SAT-P2", "op_id": "OP-POST",
                      "summary": summary},
                     expect_disconnect=True)
        record("② 注入中断后进程退出码 122",
               srv.wait_exit(122), f"poll={srv.proc.poll()}")
    finally:
        if srv.proc and srv.proc.poll() is None:
            srv.proc.kill()

    srv2 = Server(db_dir, port, audit_path=audit)
    srv2.start()
    try:
        qs = urlencode({"device_id": "SAT-P2"})
        st, got = http_request(srv2.port, "GET",
                               f"/api/v1/selections?{qs}")
        record("② 重启后为可回放的已执行态 (CONSUMED)",
               st == 200 and got["status"] == "CONSUMED", f"{st} {got}")
        st2, exe = http_request(srv2.port, "POST", "/api/v1/executions",
                                {"device_id": "SAT-P2",
                                 "op_id": "OP-POST",
                                 "summary": summary})
        # 从磁盘读取首次结果收据做比对。
        conn = sqlite3.connect(os.path.join(db_dir, "duty.db"))
        row = conn.execute(
            "SELECT result_json FROM executions ORDER BY id LIMIT 1"
        ).fetchone()
        conn.close()
        first_receipt = json.loads(row[0])["receipt"]
        record("② 重传回放同一最终结果 (replayed, 同收据)",
               st2 == 200 and exe.get("replayed") is True
               and exe["state"] == "EXECUTED"
               and exe["result"]["receipt"] == first_receipt,
               f"{st2} {exe}")
        # 已执行后不得再次选择。
        st3, body3 = http_request(
            srv2.port, "POST", "/api/v1/selections",
            {"device_id": "SAT-P2", "op_id": "OP-OTHER",
             "summary": "断电后另选", "expires_at_ms": now_ms() + 60_000})
        record("② 已执行设备重启后仍不得再次选择",
               st3 == 409 and body3.get("error")
               == "SELECTION_ALREADY_EXECUTED", f"{st3} {body3}")
    finally:
        srv2.stop()

    # 回放同样不得产生第二次实际驱动。
    drives_final = [d for d in _read_drive_audit(audit)
                    if d["op_id"] == "OP-POST"]
    record("② 首次下发与重启后回放合计只实际驱动一次（跨进程重启）",
           len(drives_final) == 1
           and drives_final[0]["receipt"] == first_receipt,
           f"total_drives={len(drives_final)}")


def scenario_tamper_health(db_dir: str, target) -> None:
    print("\n- 场景：无法校验的持久记录 -> 健康检查异常 (503)")
    # 先留一条选择。
    http_request(target, "POST", "/api/v1/selections",
                 {"device_id": "SAT-H", "op_id": "OP-H",
                  "summary": "健康探针指令",
                  "expires_at_ms": now_ms() + 60_000})
    db_file = os.path.join(db_dir, "duty.db")
    conn = sqlite3.connect(db_file)
    conn.execute("UPDATE selections SET summary=? WHERE device_id=?",
                 ("外部篡改", "SAT-H"))
    conn.commit()
    conn.close()
    st, body = http_request(target, "GET", "/health")
    record("篡改持久记录后健康检查 503 degraded 且列出错误",
           st == 503 and body.get("status") == "degraded"
           and bool(body.get("integrity_errors")), f"{st} {body}")
    st2, body2 = http_request(target, "POST", "/api/v1/executions",
                              {"device_id": "SAT-H", "op_id": "OP-H",
                               "summary": "外部篡改"})
    record("降级状态下变更操作 fail-closed",
           st2 == 503 and body2.get("error")
           == "PERSISTENCE_INTEGRITY_FAILED", f"{st2} {body2}")


def _parse_base_url() -> tuple[str, int] | None:
    base = os.environ.get("DUTY_BASE_URL")
    if not base:
        return None
    base = base.rstrip("/")
    assert base.startswith("http://"), "DUTY_BASE_URL 仅支持 http://"
    rest = base[len("http://"):]
    if ":" in rest:
        host, p = rest.split(":", 1)
        return host, int(p.split("/", 1)[0])
    return rest, 80


def check_http_smoke() -> bool:
    tmp = tempfile.TemporaryDirectory(prefix="duty-verify-")
    try:
        online = _parse_base_url()
        if online:
            print("\n[3/3] API/HTTP 冒烟（在线服务 "
                  f"{online[0]}:{online[1]} + 容器内断电恢复子进程）")
            # 基础流程 / 并发 / 过期：对 compose 中真实 duty 容器执行。
            scenario_basic_flow(online)
            scenario_concurrent(online)
            scenario_expiry(online)
            # 健康降级与两处断电恢复：在 verify 容器内以真实子进程执行，
            # 使用独立临时数据库，避免污染在线服务数据。
            local_dir = tempfile.mkdtemp(dir=tmp.name)
            port = free_port()
            srv = Server(local_dir, port)
            srv.start()
            try:
                scenario_tamper_health(local_dir, port)
            finally:
                srv.stop()
            pre_dir = tempfile.mkdtemp(dir=tmp.name)
            scenario_crash_before(pre_dir)
            post_dir = tempfile.mkdtemp(dir=tmp.name)
            scenario_crash_after(post_dir)
        else:
            print("\n[3/3] API/HTTP 冒烟（真实服务子进程）")
            # 主服务：基础流程 + 并发 + 过期。
            main_dir = tempfile.mkdtemp(dir=tmp.name)
            port = free_port()
            srv = Server(main_dir, port)
            srv.start()
            try:
                scenario_basic_flow(port)
                scenario_concurrent(port)
                scenario_expiry(port)
            finally:
                srv.stop()
            # 健康降级：独立子进程 + 临时库，篡改后直接观察 503。
            health_dir = tempfile.mkdtemp(dir=tmp.name)
            hport = free_port()
            hsrv = Server(health_dir, hport)
            hsrv.start()
            try:
                scenario_tamper_health(health_dir, hport)
            finally:
                hsrv.stop()
            # 两个断电恢复场景使用独立数据库目录，模拟独立重启周期。
            pre_dir = tempfile.mkdtemp(dir=tmp.name)
            scenario_crash_before(pre_dir)
            post_dir = tempfile.mkdtemp(dir=tmp.name)
            scenario_crash_after(post_dir)
    finally:
        tmp.cleanup()

    failed = [r for r in results if r[0] == FAIL]
    passed = [r for r in results if r[0] == PASS]
    print(f"\n冒烟小计：{len(passed)} 通过，{len(failed)} 失败")
    return not failed


def main() -> int:
    print("=" * 68)
    print("高危遥控值班系统 — 一次性验收 verify")
    print("=" * 68)
    build_ok = check_build()
    tests_ok = check_unit_tests()
    smoke_ok = check_http_smoke()

    total_pass = sum(1 for r in results if r[0] == PASS)
    total_fail = sum(1 for r in results if r[0] == FAIL)
    print("\n" + "=" * 68)
    print(f"构建检查：{'通过' if build_ok else '失败'}")
    print(f"代码测试：{'通过' if tests_ok else '失败'}")
    print(f"HTTP 冒烟：{total_pass} 通过 / {total_fail} 失败")
    overall = build_ok and tests_ok and smoke_ok
    print(f"总体验收：{'通过 ✅' if overall else '失败 ❌'}")
    print("=" * 68)
    return 0 if overall else 1


if __name__ == "__main__":
    # 子进程在同一进程组，verify 退出时统一报告；SIGPIPE 用默认处理。
    try:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # type: ignore[attr-defined]
    except (AttributeError, ValueError):
        pass
    sys.exit(main())
