#!/usr/bin/env python3
"""Fail closed on architecture, loader, dependency, RPATH and ABI drift.

Works without executing target code; package-release invokes this before tar.
The release workflow also executes each archive on its native architecture.
"""
from pathlib import Path
import re
import subprocess
import sys

if sys.flags.optimize:
    raise SystemExit("ELF validation requires Python assertions enabled")


def verify(root, target):
    machine, loader = {
        "x86_64-linux-gnu": ("Advanced Micro Devices X86-64", "/lib64/ld-linux-x86-64.so.2"),
        "aarch64-linux-gnu": ("AArch64", "/lib/ld-linux-aarch64.so.1"),
    }[target]
    system = {"libc.so.6", "libm.so.6", "libpthread.so.0", "libdl.so.2", "librt.so.1",
              "libstdc++.so.6", "libgcc_s.so.1", Path(loader).name}
    for name in ("bin/sermon", "bin/sermon-agent", "lib/libduckdb.so"):
        path = root / name
        def elf(*flags):
            return subprocess.check_output(["readelf", *flags, str(path)], text=True)
        header = elf("-h")
        assert re.search(r"Class:\s+ELF64", header), name
        assert re.search(r"Machine:\s+" + re.escape(machine) + r"\s*$", header, re.M), name
        dynamic = elf("-d")
        needed = set(re.findall(r"\(NEEDED\).*?\[(.*?)\]", dynamic))
        assert needed and not (needed - system - {"libduckdb.so"}), (name, needed)
        rpaths = re.findall(r"\((?:RUNPATH|RPATH)\).*?\[(.*?)\]", dynamic)
        if name.startswith("bin/"):
            assert "libduckdb.so" in needed, name
            assert rpaths == ["$ORIGIN/../lib"], (name, rpaths)
            assert f"[Requesting program interpreter: {loader}]" in elf("-l"), name
        else:
            assert not rpaths and "libduckdb.so" not in needed, (name, rpaths, needed)
        versions = elf("--version-info")
        for abi, limit in (("GLIBC", (2, 28)), ("GLIBCXX", (3, 4, 25)), ("CXXABI", (1, 3, 11))):
            required = [tuple(map(int, v.split("."))) for v in re.findall(r"\b" + abi + r"_([0-9.]+)", versions)]
            assert all(v <= limit for v in required), (name, abi, max(required))
        print(f"PASS ELF {target} {name}: {', '.join(sorted(needed))}; glibc <=2.28")


if __name__ == "__main__":
    verify(Path(sys.argv[1]), sys.argv[2])
