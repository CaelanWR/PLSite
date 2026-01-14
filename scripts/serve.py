#!/usr/bin/env python3
"""
Local dev server for the Newsfeed static site with a small Kalshi proxy.

Why
  - Browsers often block direct calls to Kalshi due to CORS.
  - This server serves the static files AND proxies `/api/kalshi/*` to Kalshi's
    trade-api, so `live.html` can fetch markets using your credentials locally.

Auth
  - Preferred (no secrets in browser): set env vars and let the proxy sign requests:
      export KALSHI_KEY_ID="..."
      export KALSHI_PRIVATE_KEY_PATH="kalshi.key"
    or bearer:
      export KALSHI_TOKEN="..."
  - Optional: the browser can send `Authorization: Bearer ...` to the proxy.

Run
  python3 scripts/serve.py --port 8000
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from fetch_kalshi_impact import _kalshi_auth_headers  # type: ignore


KALSHI_UPSTREAM_BASE = "https://api.elections.kalshi.com/trade-api/v2"
API_PREFIX = "/api/kalshi"
PRIORS_REFRESH_PATH = "/api/priors/refresh"
BASE_DIR = Path(__file__).resolve().parent.parent


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _pick_kalshi_auth_from_env() -> Optional[Dict[str, Any]]:
    token = (os.environ.get("KALSHI_TOKEN") or "").strip()
    if token:
        return {"kind": "bearer", "token": token}
    key_id = (os.environ.get("KALSHI_KEY_ID") or "").strip()
    key_path = (os.environ.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    if key_id and key_path:
        return {"kind": "key", "key_id": key_id, "private_key_path": key_path}
    return None


def _run_priors_refresh() -> Dict[str, Any]:
    script_path = BASE_DIR / "scripts" / "fetch_market_priors.py"
    if not script_path.exists():
        return {"ok": False, "error": "fetch_market_priors.py not found."}

    start = time.time()
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    elapsed_ms = int((time.time() - start) * 1000)
    if proc.returncode != 0:
        return {
            "ok": False,
            "error": "Market priors refresh failed.",
            "detail": (proc.stderr or proc.stdout or "").strip(),
            "elapsed_ms": elapsed_ms,
        }
    bayes_result = _run_priors_bayes_build()
    message = "Market priors refreshed."
    if bayes_result.get("ok"):
        message = "Market priors refreshed (Bayesian build ok)."
    elif bayes_result.get("error"):
        message = "Market priors refreshed (Bayesian build failed)."
    return {
        "ok": True,
        "message": message,
        "elapsed_ms": elapsed_ms,
        "bayes_ok": bayes_result.get("ok", False),
        "bayes_error": bayes_result.get("error"),
        "bayes_detail": bayes_result.get("detail"),
        "bayes_elapsed_ms": bayes_result.get("elapsed_ms"),
    }


def _run_priors_bayes_build() -> Dict[str, Any]:
    script_path = BASE_DIR / "scripts" / "build_market_priors_bayes.py"
    if not script_path.exists():
        return {"ok": False, "error": "build_market_priors_bayes.py not found."}

    start = time.time()
    proc = subprocess.run(
        [sys.executable, str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    elapsed_ms = int((time.time() - start) * 1000)
    if proc.returncode != 0:
        return {
            "ok": False,
            "error": "Bayesian build failed.",
            "detail": (proc.stderr or proc.stdout or "").strip(),
            "elapsed_ms": elapsed_ms,
        }
    return {
        "ok": True,
        "elapsed_ms": elapsed_ms,
    }


def _log_priors_refresh(result: Dict[str, Any], label: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if result.get("ok"):
        bayes = ""
        if result.get("bayes_ok") is False:
            bayes = " (bayes failed)"
        elif result.get("bayes_ok") is True:
            bayes = " (bayes ok)"
        print(f"[{timestamp}] {label}: ok ({result.get('elapsed_ms', 0)} ms){bayes}", flush=True)
        return
    detail = result.get("detail")
    suffix = f" Detail: {detail}" if detail else ""
    print(
        f"[{timestamp}] {label}: error - {result.get('error', 'unknown')}.{suffix}",
        file=sys.stderr,
        flush=True,
    )


def _start_priors_scheduler(interval_hours: float, run_on_start: bool) -> Optional[threading.Event]:
    if interval_hours <= 0:
        return None
    interval_s = interval_hours * 3600
    stop_event = threading.Event()

    def _loop() -> None:
        if run_on_start:
            _log_priors_refresh(_run_priors_refresh(), "Priors refresh (startup)")
        while not stop_event.wait(interval_s):
            _log_priors_refresh(_run_priors_refresh(), "Priors refresh (scheduled)")

    thread = threading.Thread(target=_loop, name="priors-refresh", daemon=True)
    thread.start()
    return stop_event


class Handler(SimpleHTTPRequestHandler):
    # Make the server a bit quieter.
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: D401 (stdlib signature)
        if self.path.startswith(API_PREFIX):
            return
        super().log_message(fmt, *args)

    def end_headers(self) -> None:
        # Same-origin usage doesn't require CORS, but it's handy if you open files from a different origin.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        super().end_headers()

    def do_OPTIONS(self) -> None:  # noqa: N802 (stdlib naming)
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        if not self.path.startswith(API_PREFIX):
            return super().do_GET()

        # Map /api/kalshi/<path>?<query> -> https://.../trade-api/v2/<path>?<query>
        parsed = urlparse(self.path)
        upstream_path = parsed.path[len(API_PREFIX) :]
        if not upstream_path:
            upstream_path = "/"
        if not upstream_path.startswith("/"):
            upstream_path = "/" + upstream_path
        upstream_url = f"{KALSHI_UPSTREAM_BASE}{upstream_path}"
        if parsed.query:
            upstream_url = f"{upstream_url}?{parsed.query}"

        # Prefer bearer forwarded from browser; otherwise use env-configured auth.
        auth_header = self.headers.get("Authorization", "")
        auth: Optional[Dict[str, Any]] = None
        if auth_header.lower().startswith("bearer "):
            auth = {"kind": "bearer", "token": auth_header.split(" ", 1)[1].strip()}
        else:
            auth = _pick_kalshi_auth_from_env()

        if not auth:
            self.send_response(HTTPStatus.UNAUTHORIZED)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                _json_bytes(
                    {
                        "error": "Kalshi credentials missing.",
                        "hint": "Set KALSHI_TOKEN or (KALSHI_KEY_ID + KALSHI_PRIVATE_KEY_PATH), or send Authorization: Bearer ...",
                    }
                )
            )
            return

        try:
            headers = _kalshi_auth_headers(auth, url=upstream_url, method="GET", user_agent="Newsfeed/local-proxy")
            request = Request(upstream_url, headers=headers, method="GET")
            with urlopen(request, timeout=30) as response:
                raw = response.read()
                content_type = response.headers.get("Content-Type") or "application/json"
                self.send_response(response.status)
                self.send_header("Content-Type", content_type)
                self.end_headers()
                self.wfile.write(raw)
                return
        except HTTPError as exc:
            body = exc.read() if hasattr(exc, "read") else b""
            self.send_response(exc.code)
            self.send_header("Content-Type", exc.headers.get("Content-Type") or "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body or _json_bytes({"error": f"Upstream HTTP {exc.code}"}))
            return
        except URLError as exc:
            self.send_response(HTTPStatus.BAD_GATEWAY)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(_json_bytes({"error": "Upstream connection failed.", "detail": str(exc)}))
            return
        except Exception as exc:  # pragma: no cover
            self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(_json_bytes({"error": "Proxy error.", "detail": str(exc)}))
            return

    def do_POST(self) -> None:  # noqa: N802 (stdlib naming)
        parsed = urlparse(self.path)
        if parsed.path != PRIORS_REFRESH_PATH:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(_json_bytes({"error": "Unknown endpoint."}))
            return

        result = _run_priors_refresh()
        if not result.get("ok"):
            self.send_response(HTTPStatus.BAD_GATEWAY)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                _json_bytes(
                    {
                        "status": "error",
                        "error": result.get("error"),
                        "detail": result.get("detail"),
                        "elapsed_ms": result.get("elapsed_ms"),
                    }
                )
            )
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            _json_bytes(
                {
                    "status": "ok",
                    "message": result.get("message"),
                    "elapsed_ms": result.get("elapsed_ms"),
                    "bayes_ok": result.get("bayes_ok"),
                    "bayes_error": result.get("bayes_error"),
                    "bayes_detail": result.get("bayes_detail"),
                    "bayes_elapsed_ms": result.get("bayes_elapsed_ms"),
                }
            )
        )
        return


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve the Newsfeed site and proxy Kalshi API calls for Live Feeds.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    parser.add_argument(
        "--priors-refresh-interval-hours",
        type=float,
        default=24,
        help="Auto-refresh market priors every N hours (0 to disable).",
    )
    parser.add_argument(
        "--priors-refresh-on-start",
        action="store_true",
        help="Run a market priors refresh once when the server starts.",
    )
    args = parser.parse_args()

    os.chdir(BASE_DIR)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    stop_event = _start_priors_scheduler(args.priors_refresh_interval_hours, args.priors_refresh_on_start)
    print(f"Serving on http://{args.host}:{args.port} (Kalshi proxy at {API_PREFIX}/...)", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down…", flush=True)
    finally:
        if stop_event:
            stop_event.set()
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
