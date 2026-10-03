"""Local real HTTP/MCP -> Gensokyo -> NoneBot v11 asynchronous regression."""

from __future__ import annotations

import json
import concurrent.futures
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
import asyncio
import websockets


ROOT = Path(__file__).resolve().parents[2]
MCP_TOKEN = "fixture-mcp-token"
INTERNAL_TOKEN = "fixture-internal-token"
WS_TOKEN = "fixture-onebot-token"


class MCPClient:
    def __init__(self, origin: str):
        self.origin = origin
        self.session: str | None = None
        self.next_id = 0

    def post(self, body: dict, *, token: str = MCP_TOKEN):
        headers = {
            "Authorization": "Bearer " + token,
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        }
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        request = urllib.request.Request(
            self.origin + "/mcp",
            data=json.dumps(body).encode(),
            headers=headers,
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            session = response.headers.get("Mcp-Session-Id")
            if session:
                self.session = session
            raw = response.read(256 * 1024 + 1)
            assert len(raw) <= 256 * 1024, "oversized MCP response"
            if not raw:
                return None
            content_type = response.headers.get("Content-Type", "")
            if "text/event-stream" in content_type:
                data = [line[5:].strip() for line in raw.decode().splitlines() if line.startswith("data:")]
                assert data, "empty MCP SSE response"
                return json.loads(data[-1])
            return json.loads(raw)

    def rpc(self, method: str, params: dict | None = None, *, notification=False):
        body = {"jsonrpc": "2.0", "method": method, "params": params or {}}
        if not notification:
            self.next_id += 1
            body["id"] = self.next_id
        result = self.post(body)
        if notification:
            return None
        assert isinstance(result, dict) and "error" not in result, result
        return result["result"]

    def initialize(self):
        self.rpc(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "gensokyo-nonebot-fixture", "version": "1"},
            },
        )
        self.rpc("notifications/initialized", notification=True)
        tools = self.rpc("tools/list")["tools"]
        call_ws = next(tool for tool in tools if tool["name"] == "call_ws")
        for name in ("user_id", "group_id"):
            alternatives = call_ws["inputSchema"]["properties"][name]["oneOf"]
            assert {item["type"] for item in alternatives} == {"integer", "string"}, alternatives

    def command(
        self,
        payload: str,
        expected_status: str = "ok",
        user_id: int | str = 11001,
        group_id: int | str = 22001,
    ):
        request_id = str(uuid.uuid4())
        result = self.rpc(
            "tools/call",
            {
                "name": "call_ws",
                "arguments": {
                    "backend_id": "nonebot",
                    "request_id": request_id,
                    "audience": "group",
                    "payload": payload,
                    "user_id": user_id,
                    "group_id": group_id,
                },
            },
        )
        assert not result.get("isError"), result
        texts = [item["text"] for item in result.get("content", []) if item.get("type") == "text"]
        assert len(texts) == 1, result
        parsed = json.loads(texts[0])
        assert parsed["request_id"] == request_id and parsed["status"] == expected_status, parsed
        return parsed


def wait_http(url: str, process: subprocess.Popen, log_path: Path, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited; see {log_path}")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"service did not start: {url}; see {log_path}")


def main():
    binary = os.environ.get("GENSOKYO_BINARY", "")
    if not binary:
        raise SystemExit("GENSOKYO_BINARY must point to a built gensokyo-mcp binary")
    with tempfile.TemporaryDirectory(prefix="gensokyo-nonebot-test-") as temp:
        work = Path(temp)
        nonebot_log = (work / "nonebot.log").open("wb")
        gensokyo_log = (work / "gensokyo.log").open("wb")
        nonebot_env = os.environ.copy()
        nonebot_env.update(
            ONEBOT_WS_TOKEN=WS_TOKEN,
            NONEBOT_HOST="127.0.0.1",
            NONEBOT_PORT="18082",
            ONEBOT_BACKEND_INSTANCE="nonebot-fixture-process",
            NONEBOT_TEST_CONTROL="1",
        )
        nonebot = subprocess.Popen(
            [os.environ.get("PYTHON", sys.executable), "bot.py"],
            cwd=Path(__file__).parent,
            env=nonebot_env,
            stdout=nonebot_log,
            stderr=subprocess.STDOUT,
        )
        gensokyo_env = os.environ.copy()
        gensokyo_env.update(
            LLM_BRIDGE_ENABLED="true",
            LLM_BRIDGE_DATA_DIR=str(work / "state"),
            LLM_BRIDGE_MCP_TOKEN=MCP_TOKEN,
            LLM_BRIDGE_INTERNAL_TOKEN=INTERNAL_TOKEN,
            ONEBOT_WS_TOKEN=WS_TOKEN,
            ONEBOT_SELF_ID="",
            ONEBOT_WS_URL="ws://127.0.0.1:18082/onebot/v11/ws",
            ONEBOT_BACKEND_ID="nonebot",
        )
        gensokyo = None
        try:
            wait_http("http://127.0.0.1:18082/health", nonebot, work / "nonebot.log")
            async def reject_wrong_ws_token():
                try:
                    async with websockets.connect(
                        "ws://127.0.0.1:18082/onebot/v11/ws",
                        additional_headers={"Authorization": "Bearer wrong-token"},
                    ):
                        raise AssertionError("NoneBot accepted an incorrect OneBot token")
                except websockets.exceptions.InvalidStatus as error:
                    assert error.response.status_code == 403, error.response.status_code

            asyncio.run(reject_wrong_ws_token())
            gensokyo = subprocess.Popen(
                [binary, "-t", "http", "-addr", "127.0.0.1:18090"],
                cwd=work,
                env=gensokyo_env,
                stdout=gensokyo_log,
                stderr=subprocess.STDOUT,
            )
            wait_http("http://127.0.0.1:18090/healthz", gensokyo, work / "gensokyo.log")
            deadline = time.monotonic() + 30
            while True:
                try:
                    request = urllib.request.Request(
                        "http://127.0.0.1:18090/internal/backends",
                        headers={"Authorization": "Bearer " + INTERNAL_TOKEN},
                    )
                    with urllib.request.urlopen(request, timeout=2) as response:
                        statuses = json.load(response)["backends"]
                    if any(x["id"] == "nonebot" and x["ready"] and x["version"] == 1 for x in statuses):
                        break
                except (OSError, ValueError, KeyError):
                    pass
                if time.monotonic() >= deadline:
                    raise TimeoutError("NoneBot backend did not complete registration")
                time.sleep(0.1)

            client = MCPClient("http://127.0.0.1:18090")
            client.initialize()
            split = client.command(".splitlate")
            messages = [item["message"] for item in split["outputs"]]
            assert messages == ["first output", "second delayed output"], split
            next_result = client.command(".echo next request")
            assert [item["message"] for item in next_result["outputs"]] == ["next request"], next_result
            string_ids = client.command(".echo string identifiers", user_id="11001", group_id="22001")
            assert [item["message"] for item in string_ids["outputs"]] == ["string identifiers"], string_ids
            invalid_string_id = client.command(".should not dispatch", "failed", user_id="011001")
            assert invalid_string_id["outputs"] == [], invalid_string_id

            identity = client.command(".identity")
            status_request = urllib.request.Request("http://127.0.0.1:18082/test/status")
            with urllib.request.urlopen(status_request, timeout=2) as response:
                status = json.load(response)
            identity_event = [item for item in status["started"] if item["command"] == ".identity"][-1]
            assert identity_event["self_id"] == "10000", identity_event
            assert identity["outputs"][0]["message"] == identity_event["self_id"], identity

            no_output = client.command(".nooutput")
            assert no_output["outputs"] == [] and no_output["status"] == "ok", no_output
            completed_then_late = client.command(".late-after-complete")
            assert completed_then_late["status"] == "ok", completed_then_late
            assert [item["message"] for item in completed_then_late["outputs"]] == [
                "accepted before completion"
            ], completed_then_late
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with urllib.request.urlopen("http://127.0.0.1:18082/test/status", timeout=2) as response:
                    status = json.load(response)
                if any(
                    item["action"] == "send_group_msg"
                    for item in status["rejected"]
                ):
                    break
                time.sleep(0.05)
            assert any(
                item["action"] == "send_group_msg" for item in status["rejected"]
            ), status
            failed = client.rpc(
                "tools/call",
                {
                    "name": "call_ws",
                    "arguments": {
                        "backend_id": "nonebot",
                        "request_id": str(uuid.uuid4()),
                        "audience": "group",
                        "payload": ".fail",
                        "user_id": 11001,
                        "group_id": 22001,
                    },
                },
            )
            assert not failed.get("isError") and json.loads(failed["content"][0]["text"])["status"] == "failed", failed

            late_client = MCPClient("http://127.0.0.1:18090")
            late_client.initialize()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                late_call = pool.submit(late_client.command, ".late", "unknown")
                status_url = "http://127.0.0.1:18082/test/status"
                deadline = time.monotonic() + 10
                old_socket = None
                while time.monotonic() < deadline:
                    with urllib.request.urlopen(status_url, timeout=2) as response:
                        status = json.load(response)
                    started = [item for item in status["started"] if item["command"] == ".late"]
                    if started:
                        old_socket = started[-1]["socket_id"]
                        break
                    time.sleep(0.05)
                assert old_socket, "delayed handler did not start"
                drop = urllib.request.Request("http://127.0.0.1:18082/test/drop", data=b"{}", method="POST")
                with urllib.request.urlopen(drop, timeout=3):
                    pass
                deadline = time.monotonic() + 10
                new_socket = None
                while time.monotonic() < deadline:
                    with urllib.request.urlopen(status_url, timeout=2) as response:
                        status = json.load(response)
                    active = [key for key, value in status["connections"].items() if value["active"]]
                    if active and old_socket not in active:
                        new_socket = active[-1]
                        break
                    time.sleep(0.05)
                assert new_socket, "Gensokyo did not reconnect to the real NoneBot backend"
                unknown = late_call.result(timeout=10)
                assert unknown["status"] == "unknown" and unknown["outputs"] == [], unknown
                deadline = time.monotonic() + 6
                while time.monotonic() < deadline:
                    with urllib.request.urlopen(status_url, timeout=2) as response:
                        status = json.load(response)
                    if "send_group_msg" in status["actions"].get(old_socket, []):
                        break
                    time.sleep(0.05)
                assert "send_group_msg" in status["actions"].get(old_socket, []), status
                assert "send_group_msg" not in status["actions"].get(new_socket, []), status
                backend_request = urllib.request.Request(
                    "http://127.0.0.1:18090/internal/backends",
                    headers={"Authorization": "Bearer " + INTERNAL_TOKEN},
                )
                with urllib.request.urlopen(backend_request, timeout=2) as response:
                    backends = json.load(response)["backends"]
                assert not any(item["id"] == "nonebot" and item["ready"] for item in backends), backends
            print("real Gensokyo HTTP/MCP + NoneBot delayed split and isolation: PASS")
        except BaseException:
            nonebot_log.flush()
            gensokyo_log.flush()
            print("--- NoneBot fixture log ---")
            print((work / "nonebot.log").read_text(errors="replace")[-6000:])
            print("--- Gensokyo log ---")
            print((work / "gensokyo.log").read_text(errors="replace")[-6000:])
            raise
        finally:
            if gensokyo is not None:
                gensokyo.terminate()
                try:
                    gensokyo.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    gensokyo.kill()
            nonebot.terminate()
            try:
                nonebot.wait(timeout=8)
            except subprocess.TimeoutExpired:
                nonebot.kill()
            nonebot_log.close()
            gensokyo_log.close()
            if gensokyo is not None and gensokyo.returncode not in (0, -15):
                print((work / "gensokyo.log").read_text(errors="replace")[-4000:])
                raise RuntimeError(f"Gensokyo exited with {gensokyo.returncode}")


if __name__ == "__main__":
    main()
