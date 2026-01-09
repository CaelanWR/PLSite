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
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve the Newsfeed site and proxy Kalshi API calls for Live Feeds.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    args = parser.parse_args()

    os.chdir(BASE_DIR)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Serving on http://{args.host}:{args.port} (Kalshi proxy at {API_PREFIX}/...)", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down…", flush=True)
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
