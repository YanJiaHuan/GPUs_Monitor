import os
import time
import yaml
import socket
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import paramiko

NVIDIA_SMI_QUERY = (
    "nvidia-smi --query-gpu=index,name,uuid,utilization.gpu,"
    "memory.total,memory.used,temperature.gpu --format=csv,noheader,nounits"
)


@dataclass
class Device:
    name: str
    host: str
    user: str
    port: int
    key_paths: List[str]
    password_env: Optional[str]
    password: Optional[str]
    key_passphrase_env: Optional[str]
    key_passphrase: Optional[str]
    allow_agent: bool
    look_for_keys: bool


class Monitor:
    def __init__(self, config_path: str) -> None:
        self.config_path = config_path
        self.config = self._load_config()
        self.refresh_seconds = int(self.config.get("refresh_seconds", 15))
        self.busy_memory_pct = int(self.config.get("busy_memory_pct", 80))
        self.busy_util_pct = int(self.config.get("busy_util_pct", 70))
        self.ssh_timeout_seconds = int(self.config.get("ssh_timeout_seconds", 8))
        self.devices = self._load_devices()

        self._cache_lock = threading.Lock()
        self._cache_ts = 0.0
        self._cache_data: Optional[Dict[str, Any]] = None

    def _load_config(self) -> Dict[str, Any]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _load_devices(self) -> List[Device]:
        devices = []
        for d in self.config.get("devices", []):
            auth = d.get("auth", {}) or {}
            key_path = auth.get("key_path")
            key_paths = auth.get("key_paths") or []
            if isinstance(key_paths, str):
                key_paths = [p.strip() for p in key_paths.split(",") if p.strip()]
            if key_path:
                key_paths = [key_path] + list(key_paths)
            password_env = auth.get("password_env")
            password = auth.get("password")
            key_passphrase_env = auth.get("key_passphrase_env")
            key_passphrase = auth.get("key_passphrase")
            allow_agent = auth.get("allow_agent")
            look_for_keys = auth.get("look_for_keys")
            has_explicit_auth = bool(key_path or password_env or password or key_passphrase or key_passphrase_env)
            if allow_agent is None:
                allow_agent = False if has_explicit_auth else True
            if look_for_keys is None:
                look_for_keys = False if has_explicit_auth else True
            devices.append(
                Device(
                    name=str(d.get("name", d.get("host"))),
                    host=str(d.get("host")),
                    user=str(d.get("user")),
                    port=int(d.get("port", 22)),
                    key_paths=[str(p) for p in key_paths],
                    password_env=password_env,
                    password=password,
                    key_passphrase_env=key_passphrase_env,
                    key_passphrase=key_passphrase,
                    allow_agent=bool(allow_agent),
                    look_for_keys=bool(look_for_keys),
                )
            )
        return devices

    def get_status(self) -> Dict[str, Any]:
        now = time.time()
        with self._cache_lock:
            if self._cache_data and (now - self._cache_ts) < self.refresh_seconds:
                return self._cache_data

        data = self._collect_status()
        with self._cache_lock:
            self._cache_data = data
            self._cache_ts = time.time()
        return data

    def _collect_status(self) -> Dict[str, Any]:
        results = []
        with ThreadPoolExecutor(max_workers=min(16, max(1, len(self.devices)))) as ex:
            future_map = {ex.submit(self._fetch_device, d): d for d in self.devices}
            for fut in as_completed(future_map):
                results.append(fut.result())

        results.sort(key=lambda x: str(x.get("name", "")))
        return {
            "updated_at": int(time.time()),
            "busy_memory_pct": self.busy_memory_pct,
            "busy_util_pct": self.busy_util_pct,
            "devices": results,
        }

    def _fetch_device(self, device: Device) -> Dict[str, Any]:
        try:
            stdout = self._exec_nvidia_smi(device)
            gpus = self._parse_nvidia_smi(stdout)
            for gpu in gpus:
                gpu["busy"] = self._is_busy(gpu)
            status = "ok"
            error = None
        except Exception as exc:  # noqa: BLE001
            gpus = []
            status = "error"
            error = str(exc)

        return {
            "name": device.name,
            "host": device.host,
            "status": status,
            "error": error,
            "gpus": gpus,
        }

    def _exec_nvidia_smi(self, device: Device) -> str:
        if device.host in ("localhost", "127.0.0.1", socket.gethostname()):
            return self._exec_local()

        password = os.environ.get(device.password_env or "") if device.password_env else None
        if not password:
            password = device.password
        return self._exec_ssh(device, password=password)

    def _exec_local(self) -> str:
        import subprocess

        proc = subprocess.run(
            NVIDIA_SMI_QUERY,
            shell=True,
            text=True,
            capture_output=True,
            timeout=self.ssh_timeout_seconds,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or "nvidia-smi failed")
        return proc.stdout

    def _exec_ssh(self, device: Device, password: Optional[str]) -> str:
        key_passphrase = (
            os.environ.get(device.key_passphrase_env or "") if device.key_passphrase_env else None
        )
        if not key_passphrase:
            key_passphrase = device.key_passphrase
        last_error: Optional[Exception] = None
        if device.key_paths:
            for key_path in device.key_paths:
                try:
                    return self._exec_ssh_with_key(device, key_path, key_passphrase)
                except paramiko.AuthenticationException as exc:
                    last_error = exc
                    continue
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue
            if device.allow_agent or device.look_for_keys:
                try:
                    return self._exec_ssh_with_agent(device)
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
            if password:
                return self._exec_ssh_with_password(device, password)
            if last_error:
                try:
                    return self._exec_ssh_system(device)
                except Exception as exc:  # noqa: BLE001
                    raise last_error from exc
        if password:
            return self._exec_ssh_with_password(device, password)
        try:
            return self._exec_ssh_with_agent(device)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        try:
            return self._exec_ssh_system(device)
        except Exception as exc:  # noqa: BLE001
            raise last_error from exc

    def _exec_ssh_with_key(self, device: Device, key_path: str, passphrase: Optional[str]) -> str:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        pkey = self._load_private_key(key_path, passphrase)
        try:
            client.connect(
                hostname=device.host,
                port=device.port,
                username=device.user,
                pkey=pkey,
                key_filename=None,
                timeout=self.ssh_timeout_seconds,
                banner_timeout=self.ssh_timeout_seconds,
                auth_timeout=self.ssh_timeout_seconds,
                allow_agent=False,
                look_for_keys=False,
                disabled_algorithms={"pubkeys": ["ssh-dss"]},
            )
            stdin, stdout, stderr = client.exec_command(NVIDIA_SMI_QUERY, timeout=self.ssh_timeout_seconds)
            _ = stdin  # unused
            output = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
        finally:
            client.close()

        if err.strip() and not output.strip():
            raise RuntimeError(err.strip())
        if not output.strip():
            raise RuntimeError("nvidia-smi returned no output")
        return output

    def _exec_ssh_with_password(self, device: Device, password: str) -> str:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=device.host,
                port=device.port,
                username=device.user,
                password=password,
                key_filename=None,
                timeout=self.ssh_timeout_seconds,
                banner_timeout=self.ssh_timeout_seconds,
                auth_timeout=self.ssh_timeout_seconds,
                allow_agent=False,
                look_for_keys=False,
                disabled_algorithms={"pubkeys": ["ssh-dss"]},
            )
            stdin, stdout, stderr = client.exec_command(NVIDIA_SMI_QUERY, timeout=self.ssh_timeout_seconds)
            _ = stdin  # unused
            output = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
        finally:
            client.close()

        if err.strip() and not output.strip():
            raise RuntimeError(err.strip())
        if not output.strip():
            raise RuntimeError("nvidia-smi returned no output")
        return output

    def _exec_ssh_with_agent(self, device: Device) -> str:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=device.host,
                port=device.port,
                username=device.user,
                key_filename=None,
                timeout=self.ssh_timeout_seconds,
                banner_timeout=self.ssh_timeout_seconds,
                auth_timeout=self.ssh_timeout_seconds,
                allow_agent=device.allow_agent,
                look_for_keys=device.look_for_keys,
                disabled_algorithms={"pubkeys": ["ssh-dss"]},
            )
            stdin, stdout, stderr = client.exec_command(NVIDIA_SMI_QUERY, timeout=self.ssh_timeout_seconds)
            _ = stdin  # unused
            output = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
        finally:
            client.close()

        if err.strip() and not output.strip():
            raise RuntimeError(err.strip())
        if not output.strip():
            raise RuntimeError("nvidia-smi returned no output")
        return output

    def _exec_ssh_system(self, device: Device) -> str:
        import subprocess

        cmd = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={self.ssh_timeout_seconds}",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-p",
            str(device.port),
            f"{device.user}@{device.host}",
            NVIDIA_SMI_QUERY,
        ]
        proc = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            timeout=self.ssh_timeout_seconds + 5,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or "system ssh failed")
        if not proc.stdout.strip():
            raise RuntimeError("nvidia-smi returned no output")
        return proc.stdout

    @staticmethod
    def _load_private_key(key_path: str, passphrase: Optional[str]) -> paramiko.PKey:
        key_errors = []
        for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
            try:
                return key_cls.from_private_key_file(key_path, password=passphrase)
            except Exception as exc:  # noqa: BLE001
                key_errors.append(f"{key_cls.__name__}: {exc}")
        msg = "Unsupported or unreadable key. DSA keys are not supported. Errors: " + " | ".join(key_errors)
        raise RuntimeError(msg)

    def _parse_nvidia_smi(self, stdout: str) -> List[Dict[str, Any]]:
        gpus = []
        for line in stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 7:
                continue
            index, name, uuid, util, mem_total, mem_used, temp = parts[:7]
            gpu = {
                "index": int(index),
                "name": name,
                "uuid": uuid,
                "utilization_gpu": self._to_int(util),
                "memory_total_mb": self._to_int(mem_total),
                "memory_used_mb": self._to_int(mem_used),
                "temperature_c": self._to_int(temp),
            }
            gpu["memory_used_pct"] = self._percent(
                gpu["memory_used_mb"], gpu["memory_total_mb"]
            )
            gpus.append(gpu)
        return gpus

    def _is_busy(self, gpu: Dict[str, Any]) -> bool:
        mem_pct = gpu.get("memory_used_pct", 0)
        util = gpu.get("utilization_gpu", 0)
        return mem_pct >= self.busy_memory_pct or util >= self.busy_util_pct

    @staticmethod
    def _to_int(value: str) -> int:
        try:
            return int(float(value))
        except Exception:
            return 0

    @staticmethod
    def _percent(used: int, total: int) -> int:
        if total <= 0:
            return 0
        return int(round((used / total) * 100))
