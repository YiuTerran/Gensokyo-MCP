"""Reference NoneBot2 OneBot v11 bridge application used by protocol acceptance."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
import weakref
from dataclasses import dataclass

import nonebot
from fastapi import FastAPI
from nonebot import on_message, on_metaevent
from nonebot.adapters.onebot.v11 import (
    Adapter as OneBotAdapter,
    Adapter,
    ActionFailed,
    Bot,
    Event,
    GroupMessageEvent,
    LifecycleMetaEvent,
    Message,
    MessageSegment,
)
from nonebot.matcher import Matcher
from nonebot.utils import DataclassEncoder
from nonebot.adapters.onebot.v11.utils import handle_api_result


HOST = os.getenv("NONEBOT_HOST", "0.0.0.0")
PORT = int(os.getenv("NONEBOT_PORT", "18082"))
BACKEND_INSTANCE = os.getenv("ONEBOT_BACKEND_INSTANCE", str(uuid.uuid4()))
_logger = logging.getLogger("nonebot.bridge_reference")
_test_control = os.getenv("NONEBOT_TEST_CONTROL") == "1"
_test_connections: dict[str, dict[str, object]] = {}
_test_actions: dict[str, list[str]] = {}
_test_rejected: list[dict[str, str]] = []
_test_started: list[dict[str, str]] = []


@dataclass(frozen=True)
class Registration:
    connection_id: str
    socket: object
    adapter: OneBotAdapter

    async def call_api(self, api: str, **data):
        # NoneBot 2.4.6's default Adapter._call_api looks up connections by
        # bot.self_id on every call. Capture the exact WebSocket at register
        # time so an old delayed task can never send on a replacement socket.
        timeout = data.pop("_timeout", self.adapter.config.api_timeout)
        seq = self.adapter._result_store.get_seq()
        socket_id = str(id(self.socket))
        if _test_control:
            _test_actions.setdefault(socket_id, []).append(api)
        fetch = asyncio.create_task(self.adapter._result_store.fetch(seq, timeout))
        try:
            await asyncio.sleep(0)  # install the result future before writing
            await self.socket.send(
                json.dumps(
                    {"action": api, "params": data, "echo": str(seq)},
                    cls=DataclassEncoder,
                )
            )
            response = await fetch
            try:
                return handle_api_result(response)
            except ActionFailed as exc:
                if _test_control:
                    _test_rejected.append({"socket_id": socket_id, "action": api})
                _logger.warning(
                    "OneBot action rejected: %s retcode=%s reason=%s",
                    api,
                    exc.info.get("retcode", "unknown"),
                    exc.info.get("message", "unspecified"),
                )
                raise
        except BaseException:
            if not fetch.done():
                fetch.cancel()
            raise


# Registration captures one concrete adapter socket and uses it for the full
# async handler lifetime; it never looks up a possibly replaced connection.
_registrations: weakref.WeakKeyDictionary[Bot, asyncio.Task[Registration]] = (
    weakref.WeakKeyDictionary()
)
_registration_lock = asyncio.Lock()


async def _register(bot: Bot) -> Registration:
    adapter = bot.adapter
    socket = adapter.connections.get(bot.self_id)
    if socket is None:
        raise RuntimeError("OneBot connection is not active")
    if _test_control:
        _test_connections[str(id(socket))] = {"socket": socket, "active": True}
    emitter = Registration(connection_id="pending", socket=socket, adapter=adapter)
    response = await emitter.call_api(
        "_llm_bridge_register",
        version=1,
        backend_instance=BACKEND_INSTANCE,
        capabilities=["reply", "complete"],
    )
    if not isinstance(response, dict) or response.get("version") != 1:
        raise RuntimeError("bridge registration failed")
    connection_id = response.get("connection_id")
    if not isinstance(connection_id, str) or not connection_id:
        raise RuntimeError("bridge registration response was invalid")
    return Registration(connection_id=connection_id, socket=socket, adapter=adapter)


async def registration_for(bot: Bot) -> Registration:
    async with _registration_lock:
        task = _registrations.get(bot)
        if task is None:
            task = asyncio.create_task(_register(bot))
            _registrations[bot] = task
    # Shield shared registration from one canceled event task.
    return await asyncio.shield(task)


async def release_registration(bot: Bot) -> None:
    async with _registration_lock:
        task = _registrations.pop(bot, None)
    if task is not None and not task.done():
        task.cancel()


async def send_reply(registration: Registration, event: Event, text: str) -> None:
    reply = MessageSegment.reply(int(event.message_id))
    body = Message([reply, MessageSegment.text(text)])
    if event.message_type == "group":
        await registration.call_api(
            "send_group_msg", group_id=int(event.group_id), message=body
        )
    else:
        await registration.call_api("send_private_msg", user_id=int(event.user_id), message=body)


async def send_private_reply(registration: Registration, event: Event, text: str) -> None:
    body = Message(
        [MessageSegment.reply(int(event.message_id)), MessageSegment.text(text)]
    )
    await registration.call_api("send_private_msg", user_id=int(event.user_id), message=body)


async def complete(
    event: Event,
    registration: Registration,
    status: str,
    output_count: int,
) -> None:
    await registration.call_api(
        "_llm_bridge_complete",
        version=1,
        source_message_id=int(event.message_id),
        connection_id=registration.connection_id,
        status=status,
        output_count=output_count,
    )


def install_plugin() -> None:
    lifecycle = on_metaevent(priority=1, block=False)

    @lifecycle.handle()
    async def register_on_connect(bot: Bot, event: Event) -> None:
        if isinstance(event, LifecycleMetaEvent) and event.sub_type == "connect":
            try:
                await registration_for(bot)
            except Exception:
                _logger.warning("bridge registration failed")
        elif isinstance(event, LifecycleMetaEvent) and event.sub_type == "disconnect":
            await release_registration(bot)

    commands = on_message(priority=10, block=True)

    @commands.handle()
    async def handle_bridge_event(matcher: Matcher, bot: Bot, event: Event) -> None:
        registration: Registration | None = None
        count = 0
        status = "ok"
        completion_sent = False
        try:
            registration = await registration_for(bot)
            command = event.get_plaintext().strip()
            socket_id = str(id(registration.socket))
            if _test_control:
                _test_started.append(
                    {
                        "socket_id": socket_id,
                        "message_id": str(event.message_id),
                        "self_id": str(event.self_id),
                        "command": command,
                    }
                )
            if command == ".nooutput":
                pass
            elif command == ".fail":
                raise RuntimeError("intentional reference backend failure")
            elif command == ".identity":
                login_info = await registration.call_api("get_login_info")
                await send_reply(registration, event, str(login_info["user_id"]))
                count += 1
            elif command == ".late-after-complete":
                await send_reply(registration, event, "accepted before completion")
                count += 1
                await complete(event, registration, "ok", count)
                completion_sent = True
                await asyncio.sleep(0.2)
                # The bridge must reject this output after the explicit terminal.
                await send_reply(registration, event, "must be rejected")
            elif command == ".delay":
                await asyncio.sleep(1)
                await send_reply(registration, event, "delayed response")
                count += 1
            elif command == ".late":
                await asyncio.sleep(3)
                await send_reply(registration, event, "late response after reconnect")
                count += 1
            elif command == ".splitlate":
                await send_reply(registration, event, "first output")
                count += 1
                await asyncio.sleep(1.5)
                await send_reply(registration, event, "second delayed output")
                count += 1
            elif command == ".two":
                await send_reply(registration, event, "first output")
                count += 1
                await send_reply(registration, event, "second output")
                count += 1
            elif command == ".secret" and isinstance(event, GroupMessageEvent):
                await send_reply(registration, event, "I sent you a private message.")
                count += 1
                await send_private_reply(registration, event, "This is the private secret message.")
                count += 1
            elif command == ".secret":
                await send_reply(registration, event, "This is the private secret message.")
                count += 1
            elif command.startswith(".echo"):
                await send_reply(registration, event, command[len(".echo") :].strip() or "echo")
                count += 1
            else:
                await send_reply(registration, event, command or "echo")
                count += 1
        except asyncio.CancelledError:
            status = "failed"
            raise
        except Exception as exc:
            status = "failed"
            _logger.warning("bridge command failed (%s)", type(exc).__name__)
        finally:
            if registration is not None and not completion_sent:
                try:
                    await complete(event, registration, status, count)
                except Exception:
                    _logger.warning("bridge completion failed")
        await matcher.finish()


def main() -> None:
    token = os.getenv("ONEBOT_WS_TOKEN", "")
    if not token:
        raise RuntimeError("ONEBOT_WS_TOKEN is required")
    nonebot.init(
        driver="~fastapi", host=HOST, port=PORT, log_level="INFO",
        onebot_access_token=token, _env_file=None,
    )
    driver = nonebot.get_driver()
    driver.register_adapter(Adapter)
    install_plugin()
    app: FastAPI = nonebot.get_app()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "adapter": "OneBot v11"}

    if _test_control:
        @app.get("/test/status")
        async def test_status() -> dict:
            return {
                "connections": {
                    key: {"active": value["active"]}
                    for key, value in _test_connections.items()
                },
                "actions": {key: list(value) for key, value in _test_actions.items()},
                "rejected": list(_test_rejected),
                "started": list(_test_started),
            }

        @app.post("/test/drop")
        async def test_drop() -> dict[str, str]:
            sockets = [
                (key, value["socket"])
                for key, value in _test_connections.items()
                if value["active"]
            ]
            for key, socket in sockets:
                _test_connections[key]["active"] = False
                await socket.close()
            return {"status": "closed"}

    nonebot.run()


if __name__ == "__main__":
    main()
