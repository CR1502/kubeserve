"""Build-time patch for vLLM 0.31.0: measure cgroup memory as the working set.

Inside a container, vLLM's CPU worker checks that the KV cache fits in
`memory.max - memory.current` of the container's cgroup. On cgroup v2,
`memory.current` also counts the page cache created by reading the model
weights. That cache is reclaimable, but vLLM treats it as used: on Kubernetes
a freshly started pod that has just read its weights looks full, and vLLM
refuses to allocate the KV cache ("Available memory ... 0.02/3.75 GiB").
After that the pod crash-loops, because the page cache stays charged to the
pod across container restarts.

This patch makes vLLM use the working set (memory.current - inactive_file),
the figure the kubelet uses for memory-pressure eviction. If the vLLM source
changes, the build fails rather than shipping an unpatched image.
"""

import importlib.util
import pathlib

OLD = """    if _read_int_file("/sys/fs/cgroup/memory.max") is not None:
        return _read_int_file("/sys/fs/cgroup/memory.current")
"""

NEW = """    if _read_int_file("/sys/fs/cgroup/memory.max") is not None:
        # [kubeserve patch] working set = usage - reclaimable inactive page cache
        usage = _read_int_file("/sys/fs/cgroup/memory.current")
        if usage is None:
            return None
        try:
            with open("/sys/fs/cgroup/memory.stat") as f:
                stat = dict(line.split() for line in f)
            return max(0, usage - int(stat.get("inactive_file", 0)))
        except OSError:
            return usage
"""

package_dir = importlib.util.find_spec("vllm").submodule_search_locations[0]
path = pathlib.Path(package_dir) / "utils" / "cpu_resource_utils.py"
source = path.read_text()
if source.count(OLD) != 1:
    raise SystemExit(f"{path}: patch target not found -- re-check against this vLLM version")
path.write_text(source.replace(OLD, NEW))
print(f"patched {path}")
