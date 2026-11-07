# Windows (PowerShell). Requires: Python 3.11-3.13 (py launcher) and JDK 17+ (set JAVA_HOME or edit below).
param([string]$Py = "3.12", [string]$JavaHome = $env:JAVA_HOME)
if (-not $JavaHome) { Write-Error "Set JAVA_HOME to a JDK 17+ (e.g. Temurin 17 zip extracted to %USERPROFILE%\tools)"; exit 1 }
py -V:$Py -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\pip install -r requirements.lock
