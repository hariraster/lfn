import asyncio
import ipaddress
import logging
import os
import re
import socket
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit
from typing import Any

import reflex as rx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

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


def parse_vless_header(data: bytes) -> tuple[int, str, int, bytes] | None:
    """Return a complete TCP header and payload, or None for invalid input."""
    if len(data) < 18 or data[1:17] != USER_UUID_BYTES:
        return None
    version = data[0]
    i = 18 + data[17]
    if len(data) < i + 4 or data[i] != 1:
        return None
    port = int.from_bytes(data[i + 1 : i + 3], "big")
    atype = data[i + 3]
    i += 4
    if port == 0:
        return None
    if atype == 1:
        if len(data) < i + 4:
            return None
        host = str(ipaddress.IPv4Address(data[i : i + 4]))
        i += 4
    elif atype == 2:
        if len(data) <= i:
            return None
        n = data[i]
        i += 1
        if n == 0 or len(data) < i + n:
            return None
        try:
            host = data[i : i + n].decode("utf-8")
            host = _normalize_host(host)
        except (ValueError, UnicodeError):
            logging.exception("Unexpected error")
            return None
        i += n
    elif atype == 3:
        if len(data) < i + 16:
            return None
        host = ipaddress.IPv6Address(data[i : i + 16]).compressed
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
            return
        version, host, port, payload = parsed
        try:
            destination = ipaddress.ip_address(host)
        except ValueError:
            destination = None
        if isinstance(destination, ipaddress.IPv6Address):
            reader, writer = await asyncio.open_connection(
                host, port, family=socket.AF_INET6
            )
        else:
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
    except WebSocketDisconnect as exc:
        if exc.code not in (1000, 1001, 1005):
            logging.exception("Unexpected error")
        logging.debug("WebSocket client disconnected")
        disconnected = True
    except Exception:
        logging.exception("Unexpected error")
    finally:
        if writer:
            writer.close()
        if (not disconnected) & (
            ws.application_state != WebSocketState.DISCONNECTED
        ):
            try:
                await ws.close()
            except RuntimeError as exc:
                message = str(exc).lower()
                if not any(
                    marker in message
                    for marker in (
                        "already closed",
                        "already sent",
                        "close message has been sent",
                        "after sending 'websocket.close'",
                        "after sending a close message",
                        "once a close message has been sent",
                        "response already completed",
                    )
                ):
                    logging.exception("Unexpected error")
                logging.debug("WebSocket already closed")
            except Exception:
                logging.exception("Unexpected error")


# ---------------- ساخت لینک ----------------
def _normalize_host(host: str) -> str:
    if not host or "%" in host or any(c.isspace() for c in host):
        raise ValueError("Invalid host")
    try:
        return ipaddress.ip_address(host).compressed
    except ValueError:
        pass
    if ":" in host or re.fullmatch(r"[0-9.]+", host):
        raise ValueError("Invalid IP address")
    try:
        domain = host.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError as e:
        logging.exception(f"Error: {e}")
        raise ValueError("Invalid hostname") from e
    labels = domain.split(".")
    if len(domain) > 253 or any(
        not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
        for label in labels
    ):
        raise ValueError("Invalid hostname")
    return domain


def _endpoint(raw_host: str) -> tuple[str, bool]:
    candidate = raw_host.strip()
    if not candidate:
        raise ValueError("Empty host")
    try:
        literal = ipaddress.ip_address(candidate)
    except ValueError:
        literal = None
    if literal is not None:
        host = _normalize_host(candidate)
    else:
        parsed = urlsplit(candidate if "://" in candidate else f"//{candidate}")
        if parsed.scheme and parsed.scheme.lower() not in (
            "http",
            "https",
            "ws",
            "wss",
        ):
            raise ValueError("Unsupported URL scheme")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("User information is not a host")
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("Invalid port")
        host = _normalize_host(parsed.hostname or "")
        if "://" not in candidate and (
            parsed.path or parsed.query or parsed.fragment
        ):
            raise ValueError("Enter a host or a complete URL")
    if host.endswith(".build.reflexsandbox.com") and host.startswith(
        ("3000-", "8080-")
    ):
        host = f"8000-{host.split('-', 1)[1]}"
    try:
        is_local = ipaddress.ip_address(host).is_loopback
    except ValueError:
        is_local = host == "localhost"
    return host, is_local


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
    browser_host: str = ""
    host_input: str = ""
    host_error: str = ""
    custom_host: bool = False
    is_local: bool = True

    def _update_link(self, host: str) -> bool:
        try:
            new_link = build_link(host)
            _, is_local = _endpoint(host)
        except ValueError:
            self.host_error = "آدرس معتبر نیست؛ IPv6، IPv4، نام میزبان یا نشانی کامل معتبر وارد کنید."
            return False
        self.link = new_link
        self.is_local = is_local
        self.host_input = host.strip()
        self.host_error = ""
        return True

    @rx.event
    def load(self):
        self.fallback_host = self.router.headers.host or "localhost"
        if not self.custom_host:
            self._update_link(
                HOST_OVERRIDE or self.browser_host or self.fallback_host
            )
        return rx.call_script(
            "window.location.hostname || ''",
            callback=State.set_browser_host,
        )

    @rx.event
    def set_browser_host(self, hostname: str):
        self.browser_host = hostname
        if not self.custom_host:
            self._update_link(HOST_OVERRIDE or hostname or self.fallback_host)

    @rx.event
    def apply_host(self, form_data: dict[str, Any]):
        if self._update_link(str(form_data.get("host", ""))):
            self.custom_host = True

    @rx.event
    def reset_host(self):
        self.custom_host = False
        self.link = ""
        self._update_link(
            HOST_OVERRIDE or self.browser_host or self.fallback_host
        )


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
            rx.el.form(
                rx.el.label(
                    "آدرس سرور (IPv6 یا نام میزبان)",
                    html_for="server-host",
                    style={
                        "display": "block",
                        "margin_bottom": "8px",
                        "font_size": "14px",
                        "font_weight": "600",
                        "line_height": "24px",
                        "color": "#1e293b",
                    },
                ),
                rx.el.input(
                    id="server-host",
                    name="host",
                    default_value=State.host_input,
                    key=State.host_input,
                    placeholder="2001:db8::1 یا example.com",
                    dir="ltr",
                    auto_complete="off",
                    aria_describedby="host-help host-error",
                    style={
                        "display": "block",
                        "box_sizing": "border-box",
                        "width": "100%",
                        "min_width": "0",
                        "border": "1px solid #e2e8f0",
                        "border_radius": "12px",
                        "background_color": "#f8fafc",
                        "padding": "10px 12px",
                        "font_family": "Tahoma, Arial, sans-serif",
                        "font_size": "14px",
                        "line_height": "24px",
                        "color": "#1e293b",
                        "text_align": "left",
                        "&:focus": {
                            "outline": "2px solid #0f766e",
                            "outline_offset": "2px",
                            "border_color": "#0f766e",
                        },
                    },
                ),
                rx.el.div(
                    rx.el.button(
                        rx.icon(
                            "check",
                            style={
                                "width": "16px",
                                "height": "16px",
                                "flex_shrink": "0",
                            },
                        ),
                        "اعمال آدرس",
                        type="submit",
                        style={
                            "display": "flex",
                            "align_items": "center",
                            "justify_content": "center",
                            "gap": "8px",
                            "padding": "10px 12px",
                            "border": "1px solid #0f766e",
                            "border_radius": "12px",
                            "background_color": "#0f766e",
                            "color": "#ffffff",
                            "font_family": "Tahoma, Arial, sans-serif",
                            "font_size": "14px",
                            "font_weight": "600",
                            "line_height": "20px",
                            "cursor": "pointer",
                            "&:hover": {"background_color": "#115e59"},
                            "&:focus-visible": {
                                "outline": "2px solid #0f766e",
                                "outline_offset": "2px",
                            },
                        },
                    ),
                    rx.el.button(
                        rx.icon(
                            "rotate-ccw",
                            style={
                                "width": "16px",
                                "height": "16px",
                                "flex_shrink": "0",
                            },
                        ),
                        "بازنشانی",
                        type="button",
                        on_click=State.reset_host,
                        style={
                            "display": "flex",
                            "align_items": "center",
                            "justify_content": "center",
                            "gap": "8px",
                            "padding": "10px 12px",
                            "border": "1px solid #e2e8f0",
                            "border_radius": "12px",
                            "background_color": "#ffffff",
                            "color": "#334155",
                            "font_family": "Tahoma, Arial, sans-serif",
                            "font_size": "14px",
                            "font_weight": "600",
                            "line_height": "20px",
                            "cursor": "pointer",
                            "&:hover": {"background_color": "#f8fafc"},
                            "&:focus-visible": {
                                "outline": "2px solid #0f766e",
                                "outline_offset": "2px",
                            },
                        },
                    ),
                    style={
                        "display": "flex",
                        "flex_wrap": "wrap",
                        "align_items": "center",
                        "gap": "8px",
                        "margin_top": "8px",
                    },
                ),
                rx.el.p(
                    "آدرس پیش‌فرض با بازنشانی برمی‌گردد. پورت لینک برای آدرس عمومی ۴۴۳ و برای loopback محلی ۸۰۰۰ است؛ پورت و مسیر URL ورودی جایگزین این تنظیمات نمی‌شوند.",
                    id="host-help",
                    style={
                        "margin": "8px 0 0",
                        "font_size": "12px",
                        "line_height": "24px",
                        "color": "#475569",
                    },
                ),
                rx.cond(
                    State.host_error != "",
                    rx.el.p(
                        State.host_error,
                        id="host-error",
                        role="alert",
                        style={
                            "margin": "8px 0 0",
                            "font_size": "14px",
                            "line_height": "24px",
                            "color": "#dc2626",
                        },
                    ),
                    rx.el.span(id="host-error"),
                ),
                on_submit=State.apply_host,
                style={
                    "margin_top": "20px",
                    "box_sizing": "border-box",
                    "width": "100%",
                    "min_width": "0",
                    "color": "#1e293b",
                    "font_family": "Tahoma, Arial, sans-serif",
                },
            ),
            rx.el.button(
                rx.icon("copy", style={"width": "16px", "height": "16px"}),
                "کپی لینک",
                on_click=rx.set_clipboard(State.link),
                disabled=State.link == "",
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
                        "برای استفاده عمومی، سرویس باید واقعاً از شبکه IPv6 یا دامنه انتخابی قابل دسترسی باشد و HTTPS، گواهی معتبر برای همان آدرس و WebSocket تنظیم شده باشند؛ وارد کردن یک IPv6 به‌تنهایی سرویس را قابل دسترسی نمی‌کند.",
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
app.add_page(index, route="/", on_load=State.load)
