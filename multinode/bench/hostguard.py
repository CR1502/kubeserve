"""Host-level validity checks for benchmarks run on a laptop.

Every "node" of the kind cluster is a container inside one Linux VM on one Mac, so
a measurement is only meaningful if the Mac itself was not swapping the VM out.
`snapshot()` before and after a run plus `check()` turns that into a hard rule:
if the host swapped more than SWAP_LIMIT_MB during the run, the run is rejected
(the same way bench.loadgen rejects runs during which the host slept).

The 1-minute load average is recorded too (not used to reject), so noisy runs
can be spotted afterwards.
"""

from __future__ import annotations

import platform
import subprocess
import time
from dataclasses import asdict, dataclass

SWAP_LIMIT_MB = 64.0


class HostSwappedError(RuntimeError):
    pass


@dataclass
class HostSnapshot:
    t: float
    swapouts_pages: int  # cumulative pages swapped out by the host
    page_size: int
    swap_used_mb: float
    load1: float

    def to_dict(self) -> dict:
        return asdict(self)


def _sysctl(name: str) -> str:
    return subprocess.run(["sysctl", "-n", name], capture_output=True, text=True, check=True).stdout.strip()


def snapshot() -> HostSnapshot:
    if platform.system() == "Darwin":
        vm_stat = subprocess.run(["vm_stat"], capture_output=True, text=True, check=True).stdout
        page_size = int(vm_stat.split("page size of ")[1].split()[0])
        swapouts = next(int(line.split()[-1].rstrip(".")) for line in vm_stat.splitlines() if line.startswith("Swapouts:"))
        used = _sysctl("vm.swapusage").split("used = ")[1].split()[0]  # e.g. "1683.38M"
        swap_used_mb = float(used.rstrip("M"))
        load1 = float(_sysctl("vm.loadavg").strip("{} ").split()[0])
    else:  # Linux host running Docker directly
        with open("/proc/vmstat") as f:
            stats = dict(line.split() for line in f)
        swapouts, page_size = int(stats.get("pswpout", 0)), 4096
        with open("/proc/meminfo") as f:
            mem = {line.split(":")[0]: int(line.split()[1]) for line in f}
        swap_used_mb = (mem.get("SwapTotal", 0) - mem.get("SwapFree", 0)) / 1024
        with open("/proc/loadavg") as f:
            load1 = float(f.read().split()[0])
    return HostSnapshot(time.time(), swapouts, page_size, swap_used_mb, load1)


def compare(before: HostSnapshot, after: HostSnapshot) -> dict:
    swapped_mb = (after.swapouts_pages - before.swapouts_pages) * before.page_size / 2**20
    return {
        "swapped_out_mb": round(swapped_mb, 1),
        "swap_used_mb_before": before.swap_used_mb,
        "swap_used_mb_after": after.swap_used_mb,
        "load1_before": before.load1,
        "load1_after": after.load1,
    }


def check(before: HostSnapshot, after: HostSnapshot) -> dict:
    """Return the comparison; raise HostSwappedError if the host swapped too much."""
    result = compare(before, after)
    if result["swapped_out_mb"] > SWAP_LIMIT_MB:
        raise HostSwappedError(
            f"the host swapped out {result['swapped_out_mb']:.0f} MiB during this run "
            f"(limit {SWAP_LIMIT_MB:.0f} MiB), so its numbers are invalid; free memory on the host and re-run"
        )
    return result
