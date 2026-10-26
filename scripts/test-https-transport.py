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
        daemon_mode = None
        requests = []
        changed = threading.Condition()

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                try:
                    hits.append(self.path)
                    assert self.headers["x-sermon-ingestion-key"] == "fixture-only-key"
                    raw = self.rfile.read(int(self.headers["Content-Length"]))
                    if daemon_mode is not None:
                        bucket = "control" if self.path.endswith("/claim") else "ingest"
                        with changed:
                            requests.append((bucket, time.monotonic()))
                            changed.notify_all()
                        recovering = daemon_mode[1] == "recovery" and sum(kind == bucket for kind, _ in requests) > 1
                        if bucket == daemon_mode[0] and not recovering:
                            self.path = f"/adversarial/429/{daemon_mode[1]}"
                        else:
                            value = json.loads(raw)
                            response_body = ({"protocol_version": 1, "poll_after_seconds": 5, "request": None}
                                             if bucket == "control" else
                                             {"log_count": len(value.get("logs", [])), "log_rejected_count": 0})
                            payload = json.dumps(response_body).encode()
                            self.send_response(200)
                            self.send_header("Content-Length", str(len(payload)))
                            self.end_headers()
                            self.wfile.write(payload)
                            return
                    else:
                        assert raw == b'{}'
                    if self.path in ("/continue429", "/continue200", "/fake429"):
                        status = 429 if self.path == "/continue429" else 200
                        payload = b'{"retry_after_seconds":120}' if status == 429 else b'429\n'
                        if self.path != "/fake429":
                            self.wfile.write(b"HTTP/1.1 100 Continue\r\n\r\n" * 2)
                        self.wfile.write(f"HTTP/1.1 {status} Fixture\r\n".encode() +
                                         b"X-Marker: 429\\n\r\n" +
                                         f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
                        self.close_connection = True
                        return
                    if self.path.startswith("/adversarial/"):
                        _, _, status, case = self.path.split("/")
                        head = f"HTTP/1.1 {status} Fixture\r\n".encode()
                        payload = b'{}'
                        extra = b""
                        if case == "encoding":
                            extra = b"Content-Encoding: nonsense\r\n"
                        elif case == "length":
                            extra = b"Content-Length: invalid\r\n"
                        elif case == "headers":
                            extra = b"X-Large: " + b"x" * 65536 + b"\r\n"
                        elif case == "utf8":
                            payload = b"\xff"
                        elif case == "oversized":
                            payload = b"x" * 10000
                        elif case == "date":
                            extra = b"Date: invalid\r\n"
                        elif case == "stall":
                            self.wfile.write(head + b"Content-Length: 1\r\n\r\n")
                            self.wfile.flush()
                            # Wait for the child's deadline/close, not a timed sleep.
                            self.connection.recv(1)
                            return
                        elif case == "truncated":
                            self.wfile.write(head + b"Content-Length: 100\r\n\r\n{}")
                            self.close_connection = True
                            return
                        self.wfile.write(head + extra + f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
                        self.close_connection = True
                        return
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

        def response(result):
            # Private status marker survives a later child timeout or bad head.
            if result.stdout.startswith(b"429\n"):
                if result.returncode != 0:
                    return {"status": 429, "body": ""}
                return json.loads(result.stdout[4:])
            assert result.returncode == 0, result
            return json.loads(result.stdout)

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
                assert result.returncode == 0 and response(result)["status"] == status
            continued = response(request("/continue429"))
            assert continued["status"] == 429 and json.loads(continued["body"])["retry_after_seconds"] == 120
            for path in ("/continue200", "/fake429"):
                control = request(path)
                assert not control.stdout.startswith(b"429\n")
                assert response(control)["status"] == 200
            print("PASS repeated 100 Continue, usable 429 delay, marker-looking non-429 header/body controls")
            failures = []
            for case in ("encoding", "length", "headers", "utf8", "oversized", "date", "truncated", "stall"):
                result = request(f"/adversarial/429/{case}")
                try:
                    value = response(result)
                    assert value["status"] == 429 and value["body"] in ("", "{}", [255]), value
                    print(f"PASS 429 preserved through {case}")
                except (AssertionError, ValueError, UnicodeError) as error:
                    failures.append(case)
                    print(f"FAIL 429 {case}: exit={result.returncode}, stdout={result.stdout!r}: {error}")
                control = request(f"/adversarial/200/{case}")
                # Zig represents non-UTF8 byte slices as JSON arrays; short
                # bodies are also returned unchanged by the existing reader.
                if case in ("utf8", "truncated"):
                    value = response(control)
                    assert value["status"] == 200
                    assert value["body"] == ([255] if case == "utf8" else "{}")
                    continue
                try:
                    response(control)
                except (AssertionError, ValueError, UnicodeError):
                    pass
                else:
                    failures.append("non-429-" + case)
                    print(f"FAIL non-429 {case} accepted")
            assert not failures, failures
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

            # Exercise the actual parent parser and scheduling gates, not a
            # Python emulation of the child protocol. Each phase has a fresh
            # disposable daemon/store and requires opposite-bucket progress.
            for bucket, case in (("ingest", "stall"), ("ingest", "headers"),
                                 ("ingest", "utf8"), ("control", "stall"), ("ingest", "recovery")):
                daemon_mode = (bucket, case)
                requests.clear()
                root = base / f"{bucket}-{case}"
                root.mkdir()
                config = root / "config.json"
                config.write_text(json.dumps({"db_path": str(root / "metrics.db"), "interval": 1,
                                              "server_url": origin, "api_key": "fixture-only-key"}))
                with (root / "daemon.log").open("wb") as log:
                    daemon = subprocess.Popen(COMMAND[:-1] + ["--config", str(config)], env=env,
                                              stdout=log, stderr=log)
                    try:
                        def progressed():
                            limited = [at for kind, at in requests if kind == bucket]
                            return limited and sum(kind != bucket and at > limited[0] for kind, at in requests) >= 2

                        with changed:
                            assert changed.wait_for(progressed, timeout=40), (daemon_mode, requests)
                            observed = list(requests)
                        assert sum(kind == bucket for kind, _ in observed) == 1, (daemon_mode, observed)
                        assert all(b[1] - a[1] >= 1.9 for a, b in zip(observed, observed[1:])), observed
                        assert daemon.poll() is None
                        assert not errors, errors
                        print(f"PASS real daemon {bucket}/{case}: other bucket progressed twice, no early retry, shared pacing")
                        if case == "recovery":
                            with changed:
                                assert changed.wait_for(lambda: sum(kind == bucket for kind, _ in requests) >= 2,
                                                        timeout=85), requests
                                resumed = [at for kind, at in requests if kind == bucket]
                            assert resumed[1] - resumed[0] >= 60, resumed
                            assert daemon.poll() is None and not errors, errors
                            print(f"PASS real daemon resumed ingest after fallback pause of {resumed[1] - resumed[0]:.2f}s")
                    finally:
                        daemon.terminate()
                        try:
                            daemon.wait(timeout=12)
                        except subprocess.TimeoutExpired:
                            daemon.kill()
                            daemon.wait()
            daemon_mode = None
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    run()
