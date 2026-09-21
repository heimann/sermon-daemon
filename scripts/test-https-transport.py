#!/usr/bin/env python3
"""Real TLS and adversarial-peer checks of the shipped private HTTP child.

SERMON_AGENT selects an unpacked candidate; SERMON_RUNNER optionally supplies
e.g. 'qemu-aarch64-static -L /usr/aarch64-linux-gnu'. No external service/key.
"""
import http.server
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import ssl
import subprocess
import tempfile
import threading
import time

REPO = Path(__file__).resolve().parent.parent
AGENT = Path(os.environ.get("SERMON_AGENT", REPO / "zig-out/bin/sermon-agent"))
COMMAND = shlex.split(os.environ.get("SERMON_RUNNER", "")) + [str(AGENT), "--https-worker"]


def run():
    with tempfile.TemporaryDirectory(prefix="tls-", dir=REPO / ".zig-cache") as tmp:
        base = Path(tmp)
        cert, key = base / "cert.pem", base / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost",
                        "-addext", "subjectAltName=DNS:localhost"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        hits, errors = [], []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                try:
                    hits.append(self.path)
                    assert self.headers["x-sermon-ingestion-key"] == "fixture-only-key"
                    assert self.rfile.read(int(self.headers["Content-Length"])) == b'{}'
                    if self.path == "/timeout":
                        time.sleep(7)
                        return
                    status = int(self.path[1:]) if self.path[1:].isdigit() else 200
                    body = b"x" * 8192 if self.path == "/cap" else b"{}"
                    self.send_response(status)
                    if status == 302:
                        self.send_header("Location", "/should-never-follow")
                    if self.path == "/headers":
                        self.send_header("X-Oversize", "x" * 65536)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
                    pass
                except Exception as err:
                    errors.append(repr(err))

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(cert, key)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        origin = f"https://localhost:{server.server_port}"
        env = {**os.environ, "SSL_CERT_FILE": str(cert)}

        def request(path="/ok", cap=8192, url=origin, trusted=True):
            child_env = env.copy()
            if not trusted:
                child_env.pop("SSL_CERT_FILE", None)
            data = json.dumps(dict(origin=url, key="fixture-only-key", path=path, body="{}", cap=cap)).encode()
            return subprocess.run(COMMAND, input=data, env=child_env, capture_output=True, timeout=8)

        try:
            good = request()
            assert good.returncode == 0, good.stderr
            value = json.loads(good.stdout)
            assert value["status"] == 200 and value["body"] == "{}"
            assert abs(value["server_time"] - time.time()) < 3
            before = len(hits)
            for rejected in (request(trusted=False), request(url=origin.replace("localhost", "127.0.0.1"))):
                assert rejected.returncode != 0 and not rejected.stdout
            assert len(hits) == before, "credentials reached an unverified peer"
            for status in (401, 404, 422, 429, 500, 503):
                result = request(f"/{status}")
                assert result.returncode == 0 and json.loads(result.stdout)["status"] == status
            redirect = request("/302")
            assert redirect.returncode != 0 and "/should-never-follow" not in hits
            assert len(json.loads(request("/cap").stdout)["body"]) == 8192
            assert request("/cap", cap=8191).returncode != 0
            assert request("/headers").returncode != 0
            started = time.monotonic()
            stalled = request("/timeout")
            assert stalled.returncode == -signal.SIGALRM and not stalled.stdout, stalled
            assert 4.8 <= time.monotonic() - started < 6.5
            # A TCP peer that never starts TLS must be covered by the SAME timer.
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                listener.listen()
                started = time.monotonic()
                stalled = request(url=f"https://localhost:{listener.getsockname()[1]}")
                assert stalled.returncode == -signal.SIGALRM
                assert 4.8 <= time.monotonic() - started < 6.5
            assert not errors, errors
            print("PASS HTTPS trusted CA/SAN, untrusted CA, hostname mismatch: no credential leakage")
            print("PASS no redirects; non-2xx statuses preserved; exact body/header caps")
            print("PASS five-second TLS-handshake and response deadlines")
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    run()
