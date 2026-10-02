import asyncio
import os
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode

import reflex as rx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# ---------------- تنظیمات ----------------
NAME = os.getenv("VLESS_NAME", "Luffy")          # اسم لینک (بعد از #) و انتهای path
HOST_OVERRIDE = os.getenv("VLESS_HOST", "")      # اگر خالی باشد، از دامنه‌ی خود صفحه خوانده می‌شود
UUID_FILE = Path(__file__).parent / "uuid.txt"


def load_uuid() -> str:
    """اولویت: متغیر محیطی VLESS_UUID ← فایل uuid.txt ← ساخت UUID جدید و ذخیره."""
    env = os.getenv("VLESS_UUID", "").strip()
    if env:
        return str(uuid.UUID(env))
    if UUID_FILE.exists():
        return str(uuid.UUID(UUID_FILE.read_text().strip()))
    new = str(uuid.uuid4())
    try:
        UUID_FILE.write_text(new)
    except OSError:
        pass
    return new


USER_UUID = load_uuid()
USER_UUID_BYTES = uuid.UUID(USER_UUID).bytes


# ---------------- سرور VLESS روی WebSocket ----------------
api = FastAPI()


def parse_vless_header(data: bytes):
    """خروجی: (version, host, port, payload) یا None اگر نامعتبر باشد."""
    if len(data) < 24 or data[1:17] != USER_UUID_BYTES:
        return None
    version = data[0]
    i = 18 + data[17]              # رد شدن از addons
    cmd = data[i]
    i += 1
    if cmd != 1:                   # فقط TCP
        return None
    port = int.from_bytes(data[i:i + 2], "big")
    i += 2
    atype = data[i]
    i += 1
    if atype == 1:                 # IPv4
        host = ".".join(str(b) for b in data[i:i + 4])
        i += 4
    elif atype == 2:               # دامنه
        n = data[i]
        i += 1
        host = data[i:i + n].decode()
        i += n
    elif atype == 3:               # IPv6
        host = ":".join(f"{data[i + k]:02x}{data[i + k + 1]:02x}" for k in range(0, 16, 2))
        i += 16
    else:
        return None
    return version, host, port, data[i:]


@api.websocket("/ws/{tag}")
async def vless_ws(ws: WebSocket, tag: str):
    await ws.accept()
    writer = None
    try:
        first = await ws.receive_bytes()
        parsed = parse_vless_header(first)
        if not parsed:
            await ws.close()
            return
        version, host, port, payload = parsed
        reader, writer = await asyncio.open_connection(host, port)
        if payload:
            writer.write(payload)
            await writer.drain()

        async def ws_to_remote():
            while True:
                msg = await ws.receive_bytes()
                writer.write(msg)
                await writer.drain()

        async def remote_to_ws():
            await ws.send_bytes(bytes([version, 0]))   # پاسخ VLESS
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                await ws.send_bytes(chunk)

        tasks = [asyncio.create_task(ws_to_remote()), asyncio.create_task(remote_to_ws())]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in tasks:
            t.cancel()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if writer:
            writer.close()
        try:
            await ws.close()
        except Exception:
            pass


# ---------------- ساخت لینک ----------------
def build_link(host: str) -> str:
    params = {
        "path": f"/ws/{NAME}",
        "security": "tls",
        "alpn": "http/1.1",
        "encryption": "none",
        "insecure": "0",
        "host": host,
        "fp": "chrome",
        "type": "ws",
        "allowInsecure": "0",
        "sni": host,
    }
    query = urlencode(params, quote_via=quote, safe="")
    return f"vless://{USER_UUID}@{host}:443?{query}#{quote(NAME, safe='')}"


# ---------------- صفحه ----------------
class State(rx.State):
    link: str = ""

    @rx.event
    def load(self):
        host = HOST_OVERRIDE or self.router.headers.host
        host = host.split(":")[0]
        # در sandbox فرانت روی 3000 و بک‌اند روی 8000 است
        if host.startswith("3000-"):
            host = "8000-" + host[len("3000-"):]
        self.link = build_link(host)


def index() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.heading(NAME, size="6"),
            rx.box(
                rx.code(State.link, word_break="break-all", white_space="pre-wrap"),
                padding="12px",
                max_width="92vw",
            ),
            rx.button("کپی لینک", on_click=rx.set_clipboard(State.link)),
            spacing="4",
            align="center",
        ),
        min_height="100vh",
        on_mount=State.load,
    )


app = rx.App(api_transformer=api)
app.add_page(index, route="/")
