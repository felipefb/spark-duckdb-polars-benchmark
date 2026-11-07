# Dot-source before running: . .\scripts\env.ps1
if (-not $env:JAVA_HOME) { $env:JAVA_HOME = "$env:USERPROFILE\tools\jdk-17.0.21+9" }
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:PYTHONPATH = "src"
$env:SPARK_LOCAL_HOSTNAME = "localhost"   # hostnames with '_' break Spark RPC URLs
$env:TZ = "UTC"
$scratch = Join-Path (Split-Path $PSScriptRoot -Parent) "tmp\scratch"
New-Item -ItemType Directory -Force $scratch | Out-Null
$env:TEMP = $scratch; $env:TMP = $scratch; $env:POLARS_TEMP_DIR = $scratch; $env:SPARK_LOCAL_DIRS = $scratch
$env:JAVA_TOOL_OPTIONS = "-Djava.io.tmpdir=$scratch"
