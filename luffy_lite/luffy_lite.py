import asyncio
import logging
import os
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit

import reflex as rx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# ---------------- تنظیمات ----------------
NAME = os.getenv("VLESS_NAME", "Luffy")  # اسم لینک (بعد از #) و انتهای path
HOST_OVERRIDE = os.getenv(
    "VLESS_HOST", ""
)  # اگر خالی باشد، از دامنه‌ی خود صفحه خوانده می‌شود
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
        logging.exception("Unexpected error")
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
    i = 18 + data[17]  # رد شدن از addons
    cmd = data[i]
    i += 1
    if cmd != 1:  # فقط TCP
        return None
    port = int.from_bytes(data[i : i + 2], "big")
    i += 2
    atype = data[i]
    i += 1
    if atype == 1:  # IPv4
        host = ".".join(str(b) for b in data[i : i + 4])
        i += 4
    elif atype == 2:  # دامنه
        n = data[i]
        i += 1
        host = data[i : i + n].decode()
        i += n
    elif atype == 3:  # IPv6
        host = ":".join(
            f"{data[i + k]:02x}{data[i + k + 1]:02x}" for k in range(0, 16, 2)
        )
        i += 16
    else:
        return None
    return version, host, port, data[i:]


@api.websocket("/ws/{tag}")
async def vless_ws(ws: WebSocket, tag: str):
    await ws.accept()
    writer = None
    disconnected = False
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
            await ws.send_bytes(bytes([version, 0]))  # پاسخ VLESS
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                await ws.send_bytes(chunk)

        tasks = [
            asyncio.create_task(ws_to_remote()),
            asyncio.create_task(remote_to_ws()),
        ]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in tasks:
            t.cancel()
    except WebSocketDisconnect:
        logging.exception("Unexpected error")
        disconnected = True
    except Exception:
        logging.exception("Unexpected error")
    finally:
        if writer:
            writer.close()
        if not disconnected:
            try:
                await ws.close()
            except WebSocketDisconnect:
                logging.exception("Unexpected error")
            except Exception:
                logging.exception("Unexpected error")


# ---------------- ساخت لینک ----------------
def _endpoint(raw_host: str) -> tuple[str, bool]:
    candidate = raw_host.strip()
    if candidate == "::1":
        host = candidate
    else:
        try:
            parsed = urlsplit(
                candidate if "://" in candidate else f"//{candidate}"
            )
            host = parsed.hostname or "localhost"
        except ValueError as e:
            logging.exception(f"Error: {e}")
            host = "localhost"
    host = host.lower()
    if host.endswith(".build.reflexsandbox.com") and host.startswith(
        ("3000-", "8080-")
    ):
        host = f"8000-{host.split('-', 1)[1]}"
    return host, host in ("localhost", "127.0.0.1", "::1")


def build_link(host: str) -> str:
    endpoint, is_local = _endpoint(host)
    uri_host = f"[{endpoint}]" if ":" in endpoint else endpoint
    params = {
        "encryption": "none",
        "security": "none" if is_local else "tls",
        "type": "ws",
        "host": uri_host,
        "path": f"/ws/{quote(NAME, safe='')}",
    }
    if not is_local:
        params.update({"sni": endpoint, "alpn": "http/1.1", "fp": "chrome"})
    query = urlencode(params, quote_via=quote, safe="")
    port = 8000 if is_local else 443
    return (
        f"vless://{USER_UUID}@{uri_host}:{port}?{query}#{quote(NAME, safe='')}"
    )


# ---------------- صفحه ----------------
class State(rx.State):
    link: str = ""
    fallback_host: str = "localhost"
    is_local: bool = True

    def _update_link(self, host: str):
        chosen_host = HOST_OVERRIDE or host or self.fallback_host
        self.link = build_link(chosen_host)
        self.is_local = _endpoint(chosen_host)[1]

    @rx.event
    def load(self):
        self.fallback_host = self.router.headers.host or "localhost"
        self._update_link(self.fallback_host)
        return rx.call_script(
            "window.location.hostname || ''",
            callback=State.set_browser_host,
        )

    @rx.event
    def set_browser_host(self, hostname: str):
        self._update_link(hostname)


def index() -> rx.Component:
    return rx.box(
        rx.box(
            rx.box(
                rx.box(
                    rx.icon(
                        "link",
                        style={
                            "width": "20px",
                            "height": "20px",
                            "color": "#0f766e",
                        },
                    ),
                    style={
                        "display": "flex",
                        "align_items": "center",
                        "justify_content": "center",
                        "width": "44px",
                        "height": "44px",
                        "flex_shrink": "0",
                        "border_radius": "12px",
                        "background_color": "#f0fdfa",
                    },
                ),
                rx.box(
                    rx.el.p(
                        "ابزار اتصال",
                        style={
                            "margin": "0",
                            "font_size": "12px",
                            "font_weight": "600",
                            "color": "#0f766e",
                        },
                    ),
                    rx.el.h1(
                        NAME,
                        style={
                            "margin": "4px 0 0",
                            "font_size": "24px",
                            "line_height": "1.3",
                            "font_weight": "700",
                            "letter_spacing": "-0.02em",
                            "color": "#0f172a",
                        },
                    ),
                    style={"min_width": "0"},
                ),
                style={
                    "display": "flex",
                    "align_items": "center",
                    "gap": "12px",
                },
            ),
            rx.el.p(
                "لینک اتصال VLESS شما برای این آدرس آماده است.",
                style={
                    "margin": "20px 0 0",
                    "font_size": "14px",
                    "line_height": "28px",
                    "color": "#475569",
                },
            ),
            rx.box(
                rx.box(
                    rx.el.span(
                        "لینک اتصال",
                        style={
                            "font_size": "14px",
                            "font_weight": "600",
                            "color": "#1e293b",
                        },
                    ),
                    rx.el.span(
                        rx.cond(
                            State.is_local, "فقط دستگاه فعلی", "آدرس عمومی"
                        ),
                        style={
                            "display": "inline-block",
                            "border_radius": "999px",
                            "background_color": "#f0fdfa",
                            "padding": "5px 10px",
                            "font_size": "12px",
                            "font_weight": "500",
                            "color": "#115e59",
                            "white_space": "nowrap",
                        },
                    ),
                    style={
                        "display": "flex",
                        "align_items": "center",
                        "justify_content": "space-between",
                        "gap": "12px",
                        "margin_bottom": "12px",
                    },
                ),
                rx.el.code(
                    State.link,
                    dir="ltr",
                    style={
                        "display": "block",
                        "box_sizing": "border-box",
                        "width": "100%",
                        "max_width": "100%",
                        "padding": "12px 16px",
                        "border": "1px solid #e2e8f0",
                        "border_radius": "12px",
                        "background_color": "#f8fafc",
                        "color": "#1e293b",
                        "text_align": "left",
                        "font_family": "monospace",
                        "font_size": "13px",
                        "line_height": "24px",
                        "white_space": "pre-wrap",
                        "overflow_wrap": "anywhere",
                        "word_break": "break-all",
                        "user_select": "all",
                    },
                ),
                style={"margin_top": "24px", "min_width": "0"},
            ),
            rx.el.button(
                rx.icon("copy", style={"width": "16px", "height": "16px"}),
                "کپی لینک",
                on_click=rx.set_clipboard(State.link),
                style={
                    "display": "flex",
                    "align_items": "center",
                    "justify_content": "center",
                    "gap": "8px",
                    "box_sizing": "border-box",
                    "width": "100%",
                    "margin_top": "20px",
                    "padding": "12px 16px",
                    "border": "0",
                    "border_radius": "12px",
                    "background_color": "#0f766e",
                    "color": "#ffffff",
                    "font_family": "Tahoma, Arial, sans-serif",
                    "font_size": "14px",
                    "font_weight": "600",
                    "line_height": "20px",
                    "cursor": "pointer",
                },
            ),
            rx.box(
                rx.icon(
                    "info",
                    style={
                        "width": "16px",
                        "height": "16px",
                        "margin_top": "3px",
                        "flex_shrink": "0",
                        "color": "#0f766e",
                    },
                ),
                rx.box(
                    rx.el.p(
                        "لینک محلی فقط روی همین دستگاه کار می‌کند و از دستگاه‌های دیگر قابل دسترسی نیست.",
                        style={
                            "margin": "0",
                            "font_size": "14px",
                            "line_height": "24px",
                            "color": "#334155",
                        },
                    ),
                    rx.el.p(
                        "برای استفاده از آدرس عمومی، سرویس باید واقعاً در یک استقرار قابل دسترس باشد و پروکسی معکوس HTTPS از WebSocket پشتیبانی کند؛ تغییر نام میزبان به‌تنهایی سرویس را عمومی نمی‌کند.",
                        style={
                            "margin": "8px 0 0",
                            "font_size": "14px",
                            "line_height": "24px",
                            "color": "#475569",
                        },
                    ),
                    style={"min_width": "0"},
                ),
                style={
                    "display": "flex",
                    "gap": "12px",
                    "margin_top": "24px",
                    "padding": "16px",
                    "border": "1px solid #ccfbf1",
                    "border_radius": "12px",
                    "background_color": "#f0fdfa",
                },
            ),
            dir="rtl",
            style={
                "box_sizing": "border-box",
                "width": "100%",
                "max_width": "560px",
                "padding": "clamp(24px, 5vw, 32px)",
                "border": "1px solid #e2e8f0",
                "border_radius": "16px",
                "background_color": "#ffffff",
                "color": "#0f172a",
            },
        ),
        on_mount=State.load,
        style={
            "display": "flex",
            "align_items": "center",
            "justify_content": "center",
            "box_sizing": "border-box",
            "width": "100%",
            "min_height": "100dvh",
            "padding": "40px 16px",
            "background_color": "#f7f5ef",
            "color": "#0f172a",
            "font_family": "Tahoma, Arial, sans-serif",
        },
    )


app = rx.App(api_transformer=api, theme=rx.theme(appearance="light"))
app.add_page(index, route="/")
