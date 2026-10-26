#!/usr/bin/env python3
"""Stdlib, disposable, real-entrypoint CLI output contract fixture.

Build sermon first; optionally set SERMON_CLI to another executable. No daemon,
journal, credentials, personal configuration, or Python DuckDB package needed.
For a local shared-library build: LD_LIBRARY_PATH=lib python3 scripts/test-cli-output.py
Staging v1 framing follows staging.zig and test-host-log-protocol.py. Assertions
inspect captured stdout, never echo it (even a failed child may contain controls).
"""
import csv
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import zlib


REPO = Path(__file__).resolve().parent.parent
CLI = Path(os.environ.get("SERMON_CLI", REPO / "zig-out/bin/sermon")).resolve()
HOSTILE = '=q,"back\\slash"\r\n\x1b[31m\x1b]52;c;fixture\x07\x9b\u202e雪☃'
CSV_HOSTILE = "'" + '=q,"back\\slash"\r\n\\x1b[31m\\x1b]52;c;fixture\\x07\\u{9b}\\u{202e}雪☃'
TABLE_HOSTILE = CSV_HOSTILE[1:].replace('\r', '\\x0d').replace('\n', '\\x0a')
ORDINARY = "ordinary-worker"


def text(value):
    raw = value.encode("utf-8")
    return struct.pack("<I", len(raw)) + raw


def optional(value):
    return b"\0" if value is None else b"\1" + text(value)


def segment(root, name, payload):
    (root / "_staging" / (name + ".log")).write_bytes(
        b"\1" + struct.pack("<II", len(payload), zlib.crc32(payload)) + payload)


def seed(root, nonfinite=False):
    (root / "_staging").mkdir(parents=True)
    cpu = float("inf") if nonfinite else 12.5
    user = float("-inf") if nonfinite else 3.5
    memory = float("nan") if nonfinite else 25.0
    segment(root, "metrics", struct.pack("<qIffffQQfQQ", 100, 1,
            cpu, user, 2.0, 1.0, 4096, 1024, memory, 0, 0))
    processes = b""
    for pid, value, state in [(41, HOSTILE, 27), (42, ORDINARY, ord("S"))]:
        processes += (struct.pack("<I", pid) + text(value) + text(value) +
                      bytes([state]) + struct.pack("<fQI", cpu, 1024, 2) +
                      text(value) + struct.pack("<QQ", 10, 20) + text(value) + text(value))
    segment(root, "processes", struct.pack("<qI", 100, 2) + processes)
    segment(root, "disks", struct.pack("<qI", 100, 1) + text(HOSTILE) +
            text(HOSTILE) + struct.pack("<QQf", 4096, 1024, memory))
    logs = b""
    for timestamp, value, unit in [(100, HOSTILE, HOSTILE),
                                   (101, ORDINARY, None), (102, "null", "null")]:
        # Legacy log frames have no trace_id, unlike current trace-aware frames.
        logs += (struct.pack("<q", timestamp) + text(value) + optional(unit) +
                 optional(unit) + optional(unit) + b"\6" + text(value) + b"\0")
    segment(root, "logs", struct.pack("<I", 3) + logs)


def cli(root, fmt, *args):
    result = subprocess.run([str(CLI), "--db", str(root / "metrics.db"),
                             "--format", fmt, *args], capture_output=True)
    assert result.returncode == 0, (
        f"{args[0]}/{fmt}: child exit {result.returncode}; "
        f"stderr={ascii(result.stderr.decode('utf-8', errors='replace'))}")
    return result.stdout.decode("utf-8")


def safe(value, csv_output=False):
    allowed = "\r\n" if csv_output else "\n"
    assert all(c in allowed or ord(c) >= 32 for c in value), "raw C0 control"
    assert not any(0x7f <= ord(c) <= 0x9f or ord(c) in
                   [0x61c, 0x200e, 0x200f, *range(0x202a, 0x202f),
                    *range(0x2066, 0x206a)] for c in value), "raw terminal control"


def no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        assert key not in result, "duplicate JSON key loses a value"
        result[key] = value
    return result


def reject_constant(_):
    raise AssertionError("nonstandard JSON numeric constant")


def json_output(root, *args):
    raw = cli(root, "json", *args)
    safe(raw)
    return json.loads(raw, object_pairs_hook=no_duplicates, parse_constant=reject_constant)


def csv_output(root, *args):
    raw = cli(root, "csv", *args)
    safe(raw, csv_output=True)
    rows = list(csv.reader(io.StringIO(raw, newline=""), strict=True))
    assert rows and all(len(row) == len(rows[0]) for row in rows), "CSV row shape"
    return rows


def check_json(root):
    status = json_output(root, "status")
    assert status["cpu_percent"] == 12.5 and status["mem_total"] == 4096
    assert status["disks"][0]["mount_point"] == HOSTILE
    assert status["disks"][0]["filesystem"] == HOSTILE
    assert {p["name"] for p in status["top_processes"]} == {HOSTILE, ORDINARY}
    processes = {p["pid"]: p for p in json_output(root, "processes")}
    for field in ("name", "cmdline", "username", "cgroup", "unit"):
        assert processes[41][field] == HOSTILE
        assert processes[42][field] == ORDINARY
    assert processes[41]["state"] == "\x1b" and processes[42]["state"] == "S"
    logs = {r["timestamp"]: r for r in json_output(root, "logs")}
    for field in ("source", "unit", "identifier", "systemd_unit", "message"):
        assert logs[100][field] == HOSTILE
    assert logs[101]["unit"] is None and logs[102]["unit"] == "null"
    assert logs[101]["message"] == ORDINARY
    metrics = json_output(root, "metrics", "--period", "1000000d")
    assert len(metrics) == 1 and metrics[0]["cpu_percent"] == 12.5


def check_csv(root):
    for command, width in [("status", 11), ("metrics", 9), ("processes", 12), ("logs", 8)]:
        args = (command, "--period", "1000000d") if command == "metrics" else (command,)
        rows = csv_output(root, *args)
        assert len(rows[0]) == width
        records = [dict(zip(rows[0], row)) for row in rows[1:]]
        if command in ("status", "metrics"):
            assert len(records) == 1 and records[0]["cpu_percent"] == "12.50"
        elif command == "processes":
            assert len(records) == 2
            by_pid = {r["pid"]: r for r in records}
            for field in ("name", "cmdline", "username", "cgroup", "unit"):
                assert by_pid["41"][field] == CSV_HOSTILE
                assert by_pid["42"][field] == ORDINARY
            assert by_pid["41"]["state"] == "\\x1b"
        else:
            assert len(records) == 3
            by_time = {r["timestamp"]: r for r in records}
            assert by_time["100"]["message"] == CSV_HOSTILE
            assert by_time["101"]["unit"] == "" and by_time["102"]["unit"] == "null"


def check_tables(root):
    for command in ("status", "processes", "metrics", "logs"):
        args = (command, "--period", "1000000d") if command == "metrics" else (command,)
        raw = cli(root, "table", *args)
        safe(raw)
        if command != "metrics":
            assert TABLE_HOSTILE in raw and ORDINARY in raw
        else:
            assert "12.5" in raw


def check_query(root):
    alias = HOSTILE.replace('"', '""')
    sql = f'SELECT message AS "{alias}" FROM logs WHERE timestamp = to_timestamp(100)'
    assert json_output(root, "query", sql) == [{HOSTILE: HOSTILE}]
    assert csv_output(root, "query", sql) == [[CSV_HOSTILE], [CSV_HOSTILE]]
    table = cli(root, "table", "query", sql)
    safe(table)
    assert table.count(TABLE_HOSTILE) == 2
    sql = 'SELECT 1 AS x, 2 AS x, 3 AS "x#2", NULL AS absent, \'null\' AS literal'
    assert json_output(root, "query", sql) == [
        {"x": "1", "x#2": "2", "x#2#2": "3", "absent": None, "literal": "null"}]
    assert csv_output(root, "query", sql) == [
        ["x", "x", "x#2", "absent", "literal"], ["1", "2", "3", "", "null"]]
    for prefix, expected in [("=", "'="), ("+", "'+"), ("-", "'-"),
                             ("@", "'@"), ("\t", "'\\x09"),
                             ("\r", "'\r"), ("\n", "'\n"), ("a", "a")]:
        sql = f"SELECT '{prefix}1' AS cell"
        assert csv_output(root, "query", sql) == [["cell"], [expected + "1"]]
    # Ordinary ASCII beyond the table's 20-column minimum must not crash,
    # truncate, wrap, or run into the following cell.
    for length in (19, 20, 21, 256, 4096):
        value = "A" * length
        sql = f"SELECT '{value}' AS ordinary, 'END' AS boundary"
        assert json_output(root, "query", sql) == [{"ordinary": value, "boundary": "END"}]
        assert csv_output(root, "query", sql) == [["ordinary", "boundary"], [value, "END"]]
        table = cli(root, "table", "query", sql)
        safe(table)
        assert value.ljust(20) + " " + "END" in table


def check_nonfinite(root):
    status = json_output(root, "status")
    assert all(status[k] is None for k in ("cpu_percent", "cpu_user", "mem_percent"))
    assert status["cpu_system"] == 2.0
    assert status["disks"][0]["percent"] is None
    assert all(p["cpu_percent"] is None for p in status["top_processes"])
    metrics = json_output(root, "metrics", "--period", "1000000d")
    assert len(metrics) == 1
    assert all(metrics[0][k] is None for k in ("cpu_percent", "cpu_user", "mem_percent"))
    assert all(p["cpu_percent"] is None for p in json_output(root, "processes"))


def main():
    failures = 0
    with tempfile.TemporaryDirectory(prefix="sermon-cli-output-") as tmp:
        root = Path(tmp) / "finite"
        nonfinite = Path(tmp) / "nonfinite"
        seed(root)
        seed(nonfinite, nonfinite=True)
        for check, data in [(check_json, root), (check_csv, root), (check_tables, root),
                            (check_query, root), (check_nonfinite, nonfinite)]:
            try:
                check(data)
            except Exception as error:
                failures += 1
                print(f"FAIL {check.__name__}: {ascii(str(error))}")
            else:
                print(f"PASS {check.__name__}")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
