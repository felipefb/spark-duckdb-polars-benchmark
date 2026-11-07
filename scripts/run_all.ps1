# Full pipeline on Windows: tests (blocking) -> smoke -> baseline -> extension -> report
param([string[]]$Scales = @("smoke","baseline","extension"))
. "$PSScriptRoot\env.ps1"
.\.venv\Scripts\python -m pytest -p no:cacheprovider -rs; if ($LASTEXITCODE -ne 0) { Write-Error "Correctness tests failed: stop."; exit 1 }
foreach ($s in $Scales) { .\.venv\Scripts\python -m bench.cli run --scale $s; if ($LASTEXITCODE -ne 0) { exit 1 } }
.\.venv\Scripts\python -m bench.cli report
