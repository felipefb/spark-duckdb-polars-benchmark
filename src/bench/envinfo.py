"""Environment manifest: hardware, OS, interpreter, Java, library versions, relevant env vars."""
from __future__ import annotations

import importlib.metadata as md
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone

import psutil


def _cpu_name():
    if sys.platform == "win32":
        try:
            import winreg
            k = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0")
            return winreg.QueryValueEx(k, "ProcessorNameString")[0].strip()
        except OSError:
            pass
    if sys.platform.startswith("linux"):
        try:
            for line in open("/proc/cpuinfo"):
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
        except OSError:
            pass
    if sys.platform == "darwin":
        try:
            return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
        except OSError:
            pass
    return platform.processor()


def _disks():
    if sys.platform != "win32":
        return None
    try:
        out = subprocess.run(["powershell", "-NoProfile", "-Command",
                              "Get-PhysicalDisk | Select-Object FriendlyName,MediaType,BusType,Size | ConvertTo-Json"],
                             capture_output=True, text=True, timeout=30).stdout
        import json
        return json.loads(out) if out.strip() else None
    except Exception as e:  # noqa: BLE001
        return f"unavailable: {e}"


def _java():
    java = shutil.which("java") or (os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else None)
    if not java:
        return None
    try:
        r = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=60)
        return (r.stderr or r.stdout).strip()
    except OSError as e:
        return f"unavailable: {e}"


def manifest(data_dir=None, config=None) -> dict:
    vm = psutil.virtual_memory()
    pkgs = {}
    for p in ("duckdb", "polars", "pyspark", "pyarrow", "numpy", "psutil", "matplotlib", "pandas", "pytest", "py4j"):
        try:
            pkgs[p] = md.version(p)
        except md.PackageNotFoundError:
            pkgs[p] = None
    du = psutil.disk_usage(str(data_dir) if data_dir else ".")
    project_drive = os.path.splitdrive(os.path.abspath(str(data_dir) if data_dir else "."))[0]
    keys = ("JAVA_HOME", "SPARK_HOME", "HADOOP_HOME", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON", "POLARS_MAX_THREADS",
            "SPARK_LOCAL_HOSTNAME", "TZ", "PYTHONHASHSEED", "OMP_NUM_THREADS", "TEMP", "TMP", "POLARS_TEMP_DIR",
            "SPARK_LOCAL_DIRS", "JAVA_TOOL_OPTIONS")
    return {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "hostname_has_underscore": "_" in platform.node(),
        "os": platform.platform(), "machine": platform.machine(),
        "cpu": _cpu_name(), "cpu_logical": psutil.cpu_count(logical=True), "cpu_physical": psutil.cpu_count(logical=False),
        "cpu_freq_mhz": getattr(psutil.cpu_freq(), "max", None),
        "ram_total_gib": round(vm.total / 2**30, 2), "ram_available_gib_at_start": round(vm.available / 2**30, 2),
        "data_path": str(data_dir), "project_drive": project_drive, "disk_free_gib": round(du.free / 2**30, 2), "physical_disks": _disks(),
        "python": sys.version, "python_executable": sys.executable, "sqlite": sqlite3.sqlite_version,
        "java": _java(), "packages": pkgs,
        "env": {k: os.environ.get(k) for k in keys},
        "config": config,
    }
