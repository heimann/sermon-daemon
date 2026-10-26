#!/usr/bin/env python3
"""Disposable local contract fixture, not a hosted Sermon end-to-end test.

Build first with zig build -Dversion=0.0.2. Uses only stdlib and openssl.
Fixtures exercise the real process/HTTPS/store/spool boundaries.
"""
import datetime as dt
import fcntl
import hashlib
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import uuid
import zlib

REPO = Path(__file__).resolve().parent.parent
AGENT = Path(os.environ.get("SERMON_AGENT", REPO / "zig-out/bin/sermon-agent"))
VERSION = os.environ.get("SERMON_VERSION", "0.0.2")
UTC = dt.timezone.utc

# Synthetic inputs and independently specified wire expectations. Keep benign
# controls in the same store and batches so dropping rows cannot pass this test.
CREDENTIAL_CASES = [
    ("Authorization: Bearer opaque-fixture", "Authorization: Bearer <REDACTED:TOKEN>"),
    ("Authorization: Bearer 'opaque secret'", "Authorization: Bearer '<REDACTED:TOKEN>'"),
    ('Authorization: Basic "opaque\\" secret"', 'Authorization: Basic "<REDACTED:TOKEN>"'),
    ("Authorization: opaque-credential \t", "Authorization: <REDACTED:TOKEN> \t"),
    ("Authorization: ghp_0123456789abcdefghijklmnopqrstuvwx status=401", "Authorization: <REDACTED:TOKEN>"),
    ('{"Authorization":"opaque-credential \t"}', '{"Authorization":"<REDACTED:TOKEN> \t"}'),
    ("Authorization: Bearer \t", "Authorization: Bearer \t"),
    ("postgres://alice:hun'ter2@db/app", "postgres://<REDACTED:VALUE>@db/app"),
    ('{"password":"opaque\\" secret"}', '{"password":"<REDACTED:VALUE>"}'),
    ("exec --token 'opaque value' --mode check", "exec --token '<REDACTED:VALUE>' --mode check"),
    ("exec --token alpha,beta --mode check", "exec --token <REDACTED:VALUE> --mode check"),
    ('exec --token="alpha"beta --mode check', "exec --token=<REDACTED:VALUE> --mode check"),
    ("exec --token alpha\\ beta --mode check", "exec --token <REDACTED:VALUE> --mode check"),
    ("postgres://fixture:opaque@db/app", "postgres://<REDACTED:VALUE>@db/app"),
    ("Authorization: Digest nonce=opaque, response=fixture", "Authorization: Digest <REDACTED:TOKEN>"),
    ("password=private-value", "password=<REDACTED:VALUE>"),
    ("token refresh started", "token refresh started"),
    ("X-Not-Authorization: harmless", "X-Not-Authorization: harmless"),
    ("https://example.com/path@operator", "https://example.com/path@operator"),
]


def stamp(seconds):
    return dt.datetime.fromtimestamp(seconds, UTC).isoformat().replace("+00:00", "Z")


def claim(**filters):
    return {"protocol_version": 1, "poll_after_seconds": 10, "request": {
        "id": str(uuid.uuid4()), "protocol_version": 1,
        "claim_token": str(uuid.uuid4()), "expires_at": stamp(time.time() + 60),
        "filters": {"since": stamp(100), "until": stamp(103),
                    "max_rows": 100, "max_bytes": 131072, **filters}}}


def text(s):
    b = s.encode()
    return struct.pack("<I", len(b)) + b


def opt(s):
    return b"\0" if s is None else b"\1" + text(s)


def seed(root, count=4, messages=None, credential_time=101):
    staging = root / "_staging"
    staging.mkdir(exist_ok=True)
    data = b"\1"
    for i in range(count):
        # Legacy staging records deliberately have no trace field.
        message = messages[i] if messages is not None else 'password=private-value "quoted" ☃'
        timestamp = credential_time if messages is not None else 100 + i % 4
        payload = (struct.pack("<Iq", 1, timestamp) + text("systemd") +
                   opt("api") + opt("api") + opt("api.service") +
                   bytes([i % 5]) + text(message) + b"\0")
        data += struct.pack("<II", len(payload), zlib.crc32(payload)) + payload
    (staging / "logs.log").write_bytes(data)


def save_claim(root, value):
    box = root / "_outbox"
    box.mkdir(exist_ok=True)
    (box / "completion.json").unlink(missing_ok=True)
    (box / "claim.json").write_text(json.dumps(value))
    return box


def child(root):
    return subprocess.run([AGENT, "--retained-log-worker", root],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=8)


def wait_for(check, seconds=40):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(.1)
    raise AssertionError("fixture condition did not arrive before deadline")


def run():
    with tempfile.TemporaryDirectory(prefix="host-log-", dir=REPO / ".zig-cache") as tmp:
        base = Path(tmp)
        root = base / "store"
        root.mkdir()
        seed(root)
        box = save_claim(root, claim(max_priority=1))
        result = child(root)
        assert result.returncode == 0, result.stderr.decode()
        completion = json.loads((box / "completion.json").read_text())
        rows = completion["result"]["rows"]
        assert [r["priority"] for r in rows] == [1, 0], rows
        assert "private-value" not in json.dumps(rows)
        assert all(row["message"] == 'password=<REDACTED:VALUE> "quoted" ☃' for row in rows)
        assert completion["result"]["coverage"]["complete"] is False
        assert completion["result"]["coverage"]["oldest_available_at"] == stamp(100)
        assert completion["result"]["coverage"]["newest_available_at"] == stamp(103)
        assert all(r["trace_id"] is None for r in rows)
        print("PASS legacy staging, UTC boundaries, priority, redaction, honest coverage")

        (box / "completion.json").unlink()
        with (root / "_staging/.roll.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            started = time.monotonic()
            timed = child(root)
            assert timed.returncode == -signal.SIGALRM, timed.stderr.decode()
            assert 4.8 <= time.monotonic() - started < 6.5
        assert not (box / "completion.json").exists()
        expired = claim()
        expired["request"]["expires_at"] = stamp(time.time() - 1)
        save_claim(root, expired)
        assert child(root).returncode != 0
        assert not (box / "completion.json").exists()
        print("PASS hard five-second lock/setup timeout and expired request rejection")

        seed(root, 240)
        save_claim(root, claim(max_rows=200))
        assert child(root).returncode == 0
        capped = json.loads((box / "completion.json").read_text())["result"]
        # 60 rows at each timestamp; 103 is exclusive, 180 remain.
        assert len(capped["rows"]) == 180 and not capped["truncated"]
        save_claim(root, claim(max_rows=100, max_bytes=1024))
        assert child(root).returncode == 0
        capped = json.loads((box / "completion.json").read_text())["result"]
        assert capped["truncated"]
        assert len(json.dumps(capped["rows"], separators=(",", ":"), ensure_ascii=False).encode()) <= 1024
        print("PASS row lookahead and exact compact JSON byte budget")

        # These rows must survive daemon startup retention and restart, unlike
        # the isolated epoch-boundary tests above.
        credential_time = int(time.time()) - 60
        credential_filters = {"since": stamp(credential_time), "until": stamp(credential_time + 1)}
        seed(root, len(CREDENTIAL_CASES), [raw for raw, _ in CREDENTIAL_CASES], credential_time)
        raw_staging = (root / "_staging/logs.log").read_bytes()
        save_claim(root, claim(**credential_filters))
        result = child(root)
        assert result.returncode == 0, result.stderr.decode()
        credential_result = json.loads((box / "completion.json").read_text())["result"]
        expected_messages = sorted(expected for _, expected in CREDENTIAL_CASES)
        actual_messages = sorted(row["message"] for row in credential_result["rows"])
        assert actual_messages == expected_messages, (actual_messages, expected_messages)
        assert (root / "_staging/logs.log").read_bytes() == raw_staging, "query mutated raw local evidence"
        print("PASS real retained-query child: auth/URI/flag/quoted credentials, negative controls, raw store preserved")

        # A real local TLS fixture validates auth/body shape and replays a lost
        # completion acknowledgment across SIGKILL/restart. No production keys.
        cert, key = base / "cert.pem", base / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost",
                        "-addext", "subjectAltName=DNS:localhost"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        work = claim(**credential_filters)
        completions, uploads, errors = [], [], []
        claim_calls = []
        telemetry_failures = []
        rejections = []
        credential_uploads = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                try:
                    assert self.headers.get("x-sermon-ingestion-key") == "fixture-only-key"
                    raw = self.rfile.read(int(self.headers["Content-Length"]))
                    body = json.loads(raw)
                    status = 200
                    if self.path.endswith("/claim"):
                        assert body == {"protocol_version": 1, "daemon_version": VERSION, "trace_id": True}
                        claim_calls.append(time.monotonic())
                        if len(claim_calls) == 1:
                            self.connection.shutdown(socket.SHUT_RDWR)
                            return  # lost claim response: next claim is identical
                        response = work if len(completions) < 2 else {"protocol_version": 1, "poll_after_seconds": 10, "request": None}
                    elif self.path.endswith("/complete"):
                        assert set(body) == {"protocol_version", "claim_token", "result"}
                        assert body["claim_token"] == work["request"]["claim_token"]
                        actual_messages = sorted(row["message"] for row in body["result"]["rows"])
                        assert actual_messages == expected_messages, (actual_messages, expected_messages)
                        completions.append(raw)
                        if len(completions) == 1:
                            self.connection.shutdown(socket.SHUT_RDWR)
                            return  # hosted accepted; acknowledgment lost
                        assert raw == completions[0], "completion replay changed bytes"
                        response = {"status": "completed", "accepted": False}
                    else:
                        logs = body.get("logs", [])
                        if logs:
                            assert len(logs) <= 100
                            assert all("private-value" not in row["message"] for row in logs)
                            for row in logs:
                                if row.get("identifier") == "credential-fixture":
                                    assert row["message"] in expected_messages, row["message"]
                                    credential_uploads.append(row["message"])
                            uploads.append(len(logs))
                        # Mismatched first acknowledgment must preserve all rows.
                        response = {"log_count": 0 if len(uploads) == 1 else len(logs), "log_rejected_count": 0}
                        refused = sum("reject-me" in row["message"] for row in logs)
                        if refused:
                            rejections.append(len(logs))
                            response = {"log_count": len(logs) - refused, "log_rejected_count": refused}
                        if not logs:
                            telemetry_failures.append(time.monotonic())
                            status = 503
                            response = {"error": "fixture_telemetry_unavailable"}
                    encoded = json.dumps(response).encode()
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except Exception as error:
                    errors.append(repr(error))
                    self.close_connection = True

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(cert, key)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        origin = f"https://localhost:{server.server_port}"
        (box / "binding").write_text(hashlib.sha256((origin + "\0fixture-only-key").encode()).hexdigest())
        for name in ("claim.json", "completion.json"):
            (box / name).unlink(missing_ok=True)
        metrics = {"cpu_percent": 0, "cpu_user": 0, "cpu_system": 0, "cpu_iowait": 0,
                   "mem_total": 100, "mem_used": 10, "mem_percent": 10, "swap_total": 0, "swap_used": 0}
        row = {"timestamp": 101, "source": "systemd", "unit": "api", "identifier": "api",
               "systemd_unit": "api.service", "priority": 3, "message": "password=" + ("private-" + "value"), "pid": None}
        for i in range(101):
            credential_row = {**row, "identifier": "credential-fixture",
                              "message": CREDENTIAL_CASES[i % len(CREDENTIAL_CASES)][0]}
            (box / f"upload-{i:032x}.json").write_text(json.dumps({"hostname": "fixture", "collected_at": 101,
                                                               "metrics": metrics, "logs": [credential_row], "processes": [], "disks": []}))
        config = base / "config.json"
        config.write_text(json.dumps({"db_path": str(root / "metrics.db"), "interval": 60,
                                      "server_url": origin, "api_key": "fixture-only-key"}))
        env = {**os.environ, "SSL_CERT_FILE": str(cert)}
        process = None
        try:
            process = subprocess.Popen([AGENT, "--config", config], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            wait_for(lambda: completions or errors)
            assert not errors, errors
            assert (box / "completion.json").exists()
            process.kill()
            process.wait()
            process = subprocess.Popen([AGENT, "--config", config], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            wait_for(lambda: len(completions) >= 2 or errors)
            wait_for(lambda: not list(box.glob("upload-*.json")) or errors)
            assert not errors, errors
            assert uploads[:2] == [100, 100], uploads
            assert sum(uploads[1:]) == 101, uploads
            assert set(credential_uploads) == set(expected_messages), credential_uploads
            assert len(claim_calls) >= 2
            assert telemetry_failures, "fixture never exercised ordinary ingest failure"
            print("PASS TLS/auth/closed claim, lost claim, restart completion replay, <=100 batching, mismatched-ack retry")
            print("PASS claims and protected uploads continue while ordinary telemetry returns 503")
            print("PASS real TLS credential boundaries and negative controls survive upload retry and completion restart replay")

            def spool(name, value):
                (box / (name + ".tmp")).write_text(value if isinstance(value, str) else json.dumps(value))
                (box / (name + ".tmp")).rename(box / name)

            def record(message):
                return {"hostname": "fixture", "collected_at": 101, "metrics": metrics,
                        "logs": [{**row, "message": message}], "processes": [], "disks": []}

            # Poison must neither wedge the queue nor take the daemon down.
            delivered = sum(uploads)
            spool("upload-poison-torn.json", '{"logs":[{"timestamp":')
            spool("upload-poison-shape.json", {"logs": None})
            spool("upload-poison-empty.json", record(""))
            spool("upload-good-after-poison.json", record("good row"))
            wait_for(lambda: not list(box.glob("upload-*.json")) or errors)
            assert not errors, errors
            assert process.poll() is None, "daemon exited on a poison spool record"
            assert sum(uploads) >= delivered + 1, uploads
            assert sorted(p.name for p in box.glob("*.bad")) == [
                "upload-poison-empty.json.bad", "upload-poison-shape.json.bad", "upload-poison-torn.json.bad"]
            print("PASS torn, malformed and empty-message spool records are quarantined; queue keeps draining")

            # Hosted refuses one row without naming it. The daemon bisects, so
            # only that record is set aside and its batch siblings still arrive.
            quarantined = set(box.glob("*.bad"))
            spool("upload-refused.json", record("reject-me"))
            for i in range(7):
                spool(f"upload-sibling-{i}.json", record(f"accepted sibling {i}"))
            wait_for(lambda: not list(box.glob("upload-*.json")) or errors, seconds=180)
            assert not errors, errors
            assert set(box.glob("*.bad")) - quarantined == {box / "upload-refused.json.bad"}
            assert rejections[-1] == 1 and len(rejections) <= 4, rejections
            delivered = sum(uploads)
            spool("upload-good-after-refusal.json", record("good row"))
            wait_for(lambda: not list(box.glob("upload-*.json")) or errors)
            assert sum(uploads) >= delivered + 1 and process.poll() is None
            print("PASS refused row isolated by bisection and quarantined alone; siblings and later uploads delivered")
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    run()
