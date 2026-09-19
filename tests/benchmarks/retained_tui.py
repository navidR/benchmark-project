#!/usr/bin/env python3
"""Measure retained TUI startup and view switching on an existing run (Linux)."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import signal
import struct
import subprocess
import termios
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("run", type=Path)
    parser.add_argument("output_prefix", type=Path)
    parser.add_argument("--perf", action="store_true", help="also record a CPU profile")
    args = parser.parse_args()
    prefix = args.output_prefix
    prefix.parent.mkdir(parents=True, exist_ok=True)
    if Path(str(prefix) + ".json").exists():
        parser.error("output already exists; choose a new prefix")
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 45, 160, 0, 0))
    env = dict(os.environ, TERM="xterm-256color", DEBUGINFOD_URLS="")
    command = [str(args.binary.resolve()), "--run", str(args.run.resolve()),
               "--refresh-ms", "250"]
    started = time.monotonic()
    process = subprocess.Popen(command, stdin=slave, stdout=slave, stderr=slave,
                               env=env, start_new_session=True)
    os.close(slave)
    transcript = bytearray()
    result = {"command": command, "latency_ms": []}
    profiler = None
    profile_log = None

    def read_output(timeout):
        if select.select([master], [], [], timeout)[0]:
            try:
                part = os.read(master, 65536)
            except OSError:
                return b""
            transcript.extend(part)
            return part
        return b""

    def wait_for(needle):
        buffer = bytearray()
        deadline = time.monotonic() + 60
        while process.poll() is None and time.monotonic() < deadline:
            buffer.extend(read_output(0.05))
            if needle in buffer:
                return time.monotonic()
        raise RuntimeError("TUI did not display " + repr(needle))

    try:
        if args.perf:
            profile_log = Path(str(prefix) + ".perf.log").open("w")
            profiler = subprocess.Popen(
                ["perf", "record", "-q", "--no-inherit", "-e", "cpu-clock:u",
                 "-F", "99", "--call-graph", "dwarf,8192", "-p", str(process.pid),
                 "-o", str(prefix) + ".perf"],
                env=env, stdout=profile_log, stderr=profile_log)
        result["first_frame_seconds"] = wait_for(b"Node [n]") - started
        for index in range(32):
            time.sleep(0.15)
            while read_output(0):
                pass
            key, heading = ((b"w", b"Wallet [w]") if index % 2 == 0
                            else (b"n", b"Node [n]"))
            sent = time.monotonic()
            os.write(master, key)
            result["latency_ms"].append((wait_for(heading) - sent) * 1000)
        # BBP only: daemon and profiler CPU are not included.
        stat = Path(f"/proc/{process.pid}/stat").read_text().rpartition(")")[2].split()
        result["cpu_seconds"] = (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK")
        status = Path(f"/proc/{process.pid}/status").read_text().splitlines()
        result["peak_rss_kib"] = int(next(
            line for line in status if line.startswith("VmHWM:")).split()[1])
        os.write(master, b"q")
        process.wait(timeout=15)
    finally:
        try:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                deadline = time.monotonic() + 20
                while process.poll() is None and time.monotonic() < deadline:
                    read_output(0.05)
                process.wait(timeout=1)
        finally:
            if profiler is not None:
                try:
                    profiler.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    profiler.send_signal(signal.SIGINT)
                    profiler.wait(timeout=15)
                result["perf_exit_code"] = profiler.returncode
            if profile_log is not None:
                profile_log.close()
            result["exit_code"] = process.poll()
            Path(str(prefix) + ".terminal").write_bytes(transcript)
            Path(str(prefix) + ".json").write_text(json.dumps(result, indent=2) + "\n")
            os.close(master)
    print(json.dumps(result))
    if result["exit_code"] != 0:
        raise RuntimeError("TUI exited unsuccessfully")


if __name__ == "__main__":
    main()
