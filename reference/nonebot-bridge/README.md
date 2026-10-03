# NoneBot2 reference bridge backend

This is a real NoneBot2 application using its OneBot v11 reverse-WebSocket
adapter. It registers each connection independently, waits for registration
completion before handling a synthetic event, sends ordinary OneBot reply
segments, and reports completion only after every send ACK has succeeded.

Run it with Python 3.13 and `uv sync --locked`, then start `uv run python bot.py`
with `ONEBOT_WS_TOKEN` set to the same secret configured in Gensokyo.
It listens on port `18082`; point Gensokyo at
`ws://127.0.0.1:18082/onebot/v11/ws`. In the integration compose network use
`ws://nonebot:18082/onebot/v11/ws`.

Acceptance commands include `.delay`, `.two`, `.splitlate`, `.secret`, and
`.echo`, plus fault fixtures `.nooutput`, `.fail`, and `.late-after-complete`.
`.splitlate` sends one correlated output, waits 1.5 seconds, sends the second,
then completes with the exact successful send count. A later request
uses a different virtual source ID and its own event-local connection binding;
no prior command output can be collected into that result. `.secret` exercises
the group-public/private-output boundary without logging the private body.

`ONEBOT_BACKEND_INSTANCE` can pin a process instance in tests. By default a
fresh UUID is created at process start and remains stable across that process's
socket reconnects, as required by quarantine recovery.

This fixture pins NoneBot2 2.5.0 and nonebot-adapter-onebot 2.4.6. Its
registration captures the adapter's actual WebSocket and uses that emitter for
registration, sends, and completion. NoneBot's default API helper resolves the
current socket by bot ID on every call, which is unsafe for delayed handlers
that outlive a reconnect.
