"""Thin async wrappers around kubectl (the benchmark reuses the user's kubeconfig)."""

from __future__ import annotations

import asyncio
import json
import subprocess

NAMESPACE = "kubeserve"


async def kubectl(*args: str, check: bool = True) -> str:
    proc = await asyncio.create_subprocess_exec(
        "kubectl", *args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = await proc.communicate()
    if check and proc.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {err.decode().strip()}")
    return out.decode()


async def kubectl_json(*args: str) -> dict:
    return json.loads(await kubectl(*args, "-o", "json"))


def kubectl_sync(*args: str) -> str:
    return subprocess.run(["kubectl", *args], check=True, capture_output=True, text=True).stdout


async def list_pods(selector: str, namespace: str = NAMESPACE) -> list[dict]:
    data = await kubectl_json("get", "pods", "-n", namespace, "-l", selector)
    return data["items"]


def pod_is_ready(pod: dict) -> bool:
    conditions = pod.get("status", {}).get("conditions", [])
    return any(c["type"] == "Ready" and c["status"] == "True" for c in conditions)


def parse_cpu(quantity: str) -> float:
    """Kubernetes CPU quantity ("3", "2500m") -> cores."""
    return float(quantity[:-1]) / 1000 if quantity.endswith("m") else float(quantity)


async def scale(deployment: str, replicas: int, namespace: str = NAMESPACE) -> None:
    await kubectl("scale", "-n", namespace, f"deployment/{deployment}", f"--replicas={replicas}")


async def wait_rollout(deployment: str, timeout: str = "600s", namespace: str = NAMESPACE) -> None:
    await kubectl("rollout", "status", "-n", namespace, f"deployment/{deployment}", f"--timeout={timeout}")
