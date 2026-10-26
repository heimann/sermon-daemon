#!/usr/bin/env python3
"""Linux syscall-level LogTailer initialization regression fixture (Zig 0.15.2).

Run with python3 scripts/test-journal-lifecycle.py. --source accepts a historical
logs.zig without replacing the working tree. Receipts and generated probes are
kept under --output (default .zig-cache/pr43-evidence). No journal is opened:
an inherited seccomp rule kills the child at execve, before journalctl executes.
This still exercises real spawn, pipe setup, fcntl, kill, and wait syscalls.
The negative control removes only JournalTailer's spawn error cleanup in a copy.
Exit 0 means both fcntl cases passed AND both negative controls were rejected;
exit 1 means a regression, and exit 2 means fixture/build failure.
"""

import argparse
import json
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

REPO = Path(__file__).resolve().parent.parent

C_SOURCE = r"""
#define _GNU_SOURCE
#include <errno.h>
#include <stddef.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#if defined(__x86_64__)
#define ARCH AUDIT_ARCH_X86_64
#elif defined(__aarch64__)
#define ARCH AUDIT_ARCH_AARCH64
#else
#error unsupported architecture
#endif

int install_filter(int command) {
    struct sock_filter instructions[] = {
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, ARCH, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
        /* Installed inside the probe: ONLY its subsequent children exec. */
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_execve, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_execveat, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_fcntl, 0, 3),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, args[1])),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, command, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EACCES),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
    struct sock_fprog program = {
        .len = sizeof(instructions) / sizeof(instructions[0]),
        .filter = instructions,
    };
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) return -1;
    return prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &program);
}

/* No polling/sleeps: wait for the exec-denied child, without reaping it.
 * SIGKILL then raw waitpid in Zig also handles the scheduler's live-child case.
 */
int child_to_reap(void) {
    siginfo_t info = {0};
    if (waitid(P_ALL, 0, &info, WEXITED | WNOWAIT)) return -1;
    if (kill(info.si_pid, SIGKILL) && errno != ESRCH) return -1;
    return info.si_pid;
}
"""

ZIG_SOURCE = r"""
const std = @import("std");
const logs = @import("logs");
const linux = std.os.linux;
extern fn install_filter(command: c_int) c_int;
extern fn child_to_reap() c_int;

pub fn main() void {
    var args = std.process.args();
    _ = args.next();
    const name = args.next() orelse std.process.exit(2);
    const command: i32 = if (std.mem.eql(u8, name, "GETFL"))
        std.posix.F.GETFL
    else if (std.mem.eql(u8, name, "SETFL"))
        std.posix.F.SETFL
    else
        std.process.exit(2);
    if (install_filter(command) != 0) std.process.exit(2);

    var allocator: std.heap.DebugAllocator(.{}) = .init;
    var expected_error = false;
    if (logs.LogTailer.init(allocator.allocator(), &.{.systemd})) |value| {
        var tailer = value;
        tailer.deinit();
        std.debug.print("INIT unexpected success\n", .{});
    } else |err| {
        std.debug.print("INIT error={s}\n", .{@errorName(err)});
        expected_error = err == error.Locked;
    }

    var status: u32 = 0;
    const result = linux.waitpid(-1, &status, linux.W.NOHANG);
    const no_children = linux.E.init(result) == .CHILD;
    if (no_children) {
        std.debug.print("CHILDREN ECHILD\n", .{});
    } else if (linux.E.init(result) == .SUCCESS) {
        if (result == 0) {
            const pid = child_to_reap();
            if (pid <= 0) std.process.exit(2);
            const reaped = linux.waitpid(pid, &status, 0);
            if (linux.E.init(reaped) != .SUCCESS or reaped != @as(usize, @intCast(pid)))
                std.process.exit(2);
            std.debug.print("CHILDREN leaked live child killed/reaped pid={d}\n", .{pid});
        } else {
            std.debug.print("CHILDREN leaked zombie reaped pid={d}\n", .{result});
        }
        if (linux.E.init(linux.waitpid(-1, &status, linux.W.NOHANG)) != .CHILD)
            std.process.exit(2);
        std.debug.print("CLEANUP ECHILD\n", .{});
    } else {
        std.debug.print("CHILDREN unexpected errno={s}\n", .{@tagName(linux.E.init(result))});
        std.process.exit(2);
    }
    const leaked = allocator.deinit() == .leak;
    std.debug.print("MEMORY {s}\n", .{if (leaked) "leak" else "ok"});
    // Bitmask: 1 = wrong init result; 2 = uncollected child; 4 = memory leak.
    const exit: u8 = @as(u8, if (expected_error) 0 else 1) |
        @as(u8, if (no_children) 0 else 2) | @as(u8, if (leaked) 4 else 0);
    std.debug.print("RESULT exit={d}\n", .{exit});
    std.process.exit(exit);
}
"""

CLEANUP = """        errdefer {
            _ = process.kill() catch {};
            _ = process.wait() catch {};
        }
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=REPO / "src/agent/logs.zig")
    parser.add_argument("--output", type=Path, default=REPO / ".zig-cache/pr43-evidence")
    options = parser.parse_args()
    if platform.system() != "Linux":
        raise RuntimeError("requires Linux seccomp")
    options.output.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="journal-lifecycle-", dir=options.output.resolve()))
    print(f"Receipts: {run}", flush=True)

    def command(argv, label):
        result = subprocess.run([str(arg) for arg in argv], cwd=REPO,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (run / f"{label}.stdout").write_bytes(result.stdout)
        (run / f"{label}.stderr").write_bytes(result.stderr)
        (run / f"{label}.json").write_text(json.dumps({
            "argv": [str(arg) for arg in argv], "exit": result.returncode,
        }, indent=2) + "\n")
        return result

    version = command(["zig", "version"], "zig-version")
    if version.returncode or version.stdout.strip() != b"0.15.2":
        raise RuntimeError("requires Zig 0.15.2")
    source = options.source.resolve()
    text = source.read_text()
    if text.count(CLEANUP) != 1:
        raise RuntimeError("expected exactly one JournalTailer child errdefer block")
    (run / "negative-logs.zig").write_text(text.replace(CLEANUP, "", 1))
    (run / "source-snapshot.zig").write_text(text)
    (run / "filter.c").write_text(C_SOURCE)
    (run / "probe.zig").write_text(ZIG_SOURCE)
    compiled = command(["cc", "-Wall", "-Wextra", "-Werror", "-c", run / "filter.c",
                        "-o", run / "filter.o"], "cc")
    if compiled.returncode:
        raise RuntimeError(compiled.stderr.decode())
    cases = []
    for variant, module in (("current", run / "source-snapshot.zig"), ("negative", run / "negative-logs.zig")):
        binary = run / variant
        built = command(["zig", "build-exe", "-ODebug", "-lc", run / "filter.o",
                         "--dep", "logs", f"-Mroot={run / 'probe.zig'}",
                         f"-Mlogs={module}", f"-femit-bin={binary}"], f"build-{variant}")
        if built.returncode:
            raise RuntimeError(built.stderr.decode())
        for mode in ("GETFL", "SETFL"):
            result = command([binary, mode], f"{variant}-{mode}")
            log = result.stderr.decode(errors="replace")
            if variant == "current":
                passed = result.returncode == 0 and "CHILDREN ECHILD" in log and "MEMORY ok" in log
            else:
                passed = (result.returncode in (2, 6) and "CHILDREN leaked" in log
                          and "CLEANUP ECHILD" in log and "INIT error=Locked" in log)
            cases.append({"variant": variant, "mode": mode, "exit": result.returncode,
                          "expectation_met": passed})
            print(f"{variant} {mode}: exit={result.returncode}, expectation_met={passed}")
            print(log, end="")
    (run / "summary.json").write_text(json.dumps(cases, indent=2) + "\n")
    return 0 if all(case["expectation_met"] for case in cases) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError) as error:
        print(f"Fixture error: {error}", file=sys.stderr)
        sys.exit(2)
