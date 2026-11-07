# Spark (modo local) vs DuckDB vs Polars — benchmark reproduzível, com correção verificada antes da performance

> **Correção primeiro.** Nenhum tempo entra em gráfico ou tabela se a execução correspondente não passou na validação contra uma implementação de referência independente (Python puro, mais SQLite).
> **Escopo:** uma única máquina. O Spark roda em `local[N]`. **Nada aqui diz respeito ao desempenho de um cluster Spark distribuído.**

Ponto de partida: `benchmark_spark_duckdb_polars.zip` (preservado em `original/`). A auditoria e as diferenças estão em [docs/AUDIT.md](docs/AUDIT.md), a semântica em [docs/SEMANTICS.md](docs/SEMANTICS.md), os resultados e limitações em [docs/RESULTS.md](docs/RESULTS.md); números gerados em [results/key_results.md](results/key_results.md); material de publicação em [docs/LINKEDIN.md](docs/LINKEDIN.md).

## Estrutura
```
configs/benchmark.json     threads, rodadas, escalas, configs dos motores (fonte única)
data_generator/generate.py gerador determinístico (numpy PCG64, seed fixa) + manifesto com hash de conteúdo
src/bench/queries.py       SQL, esquemas de saída e implementação Polars (fonte da verdade)
src/bench/reference.py     oráculos independentes: Python puro (streaming) e SQLite
src/bench/validate.py      esquema, contagem, chaves, hash canônico, valores exatos, invariantes
src/bench/worker.py        1 motor × 1 consulta em processo novo; tempos por fase
src/bench/runner.py        orquestração: aquecimento + medições, amostragem de RSS/CPU/IO/spill, validação por execução
src/bench/report.py        estatísticas + gráficos (somente a partir de results/*/runs.csv)
tests/                     fixtures de borda com resultado esperado escrito à mão + testes de mutação do validador
scripts/                   setup e execução (Windows .ps1, Linux/macOS .sh)
results/                   runs.csv, validation.json, environment.json, dataset_manifest.json, charts/, summary_*
docs/                      AUDIT, SEMANTICS, RESULTS (limitações, decisões), LINKEDIN
original/, original_minpatch/  ZIP original intacto / cópia com patch mínimo usada só no diagnóstico
```

## Requisitos
- **Python 3.11–3.13** (testado: 3.12.10). *O Python 3.14 não foi testado; as versões fixadas podem não ter wheels para ele.*
- **Java 17+** para o Spark (testado: Eclipse Temurin 17.0.21). Não precisa de admin: dá para extrair o ZIP do JDK e apontar `JAVA_HOME` para ele.
- RAM: smoke < 1 GiB; 1M ≈ 2 GiB; 10M: pico observado de ~2,7 GiB no Spark (`results/summary_table.md`), mas a pré-checagem exige 9 GiB livres. O driver do Spark fica configurado com 6g. **100M não foi executado** (veja as limitações).
- Disco: 10M ≈ ~0,2 GiB de Parquet, mais as saídas mantidas. Os datasets **não** são versionados (`data/` está no `.gitignore`).

## Instalação e execução
### Windows (PowerShell)
```powershell
py install 3.12                         # se ainda não tiver (Python install manager)
$env:JAVA_HOME="$env:USERPROFILE\tools\jdk-17.0.21+9"   # JDK 17 extraído
.\scripts\setup_env.ps1
.\scripts\run_all.ps1                   # testes (bloqueantes) -> smoke -> 1M -> 10M -> relatório
.\scripts\run_all.ps1 -Scales smoke     # só o smoke test
```
### Linux / macOS
```bash
export JAVA_HOME=/path/to/jdk-17
bash scripts/setup_env.sh
bash scripts/run_all.sh                 # ou: bash scripts/run_all.sh smoke
```
### Comandos individuais
```bash
# com PYTHONPATH=src e o venv ativo
python -m pytest -rs                                   # correção: comando único
python -m bench.cli run --scale smoke                  # gera dados, referência, 1 aquecimento + 5 medições
python -m bench.cli run --scale baseline --engines duckdb polars --queries q1 q2
python -m bench.cli report                             # estatísticas + gráficos
python -m bench.cli env                                # manifesto do ambiente
python data_generator/generate.py --rows 10000 --out data/x
```

## Metodologia (resumo)
- Mesma entrada Parquet para os três motores. Dinheiro em centavos `INT64`. NULLs, órfãos, negativos, zeros e empates estão presentes **no dataset principal**, não só nas fixtures.
- Cada execução roda num **processo novo**: `python -m bench.worker`. Fases: `t_import`, `t_init` (conexão/SparkSession), `t_register` (views/scan), `t_query` (leitura + processamento + materialização em Arrow; nos motores lazy, leitura e processamento **não são separáveis**), `t_write` (o mesmo escritor pyarrow para todos), `t_close`, e `wall_seconds` do processo inteiro.
- **Engine work** = `t_register + t_query`. **End-to-end** = `wall_seconds`. São métricas diferentes e nunca se misturam.
- 1 aquecimento + 5 medições por motor/consulta/escala. Em cada rodada, a ordem dos motores é uma permutação diferente e a ordem das consultas é rotacionada.
- Amostragem a cada 50 ms da árvore de processos (inclui a JVM do Spark): pico de RSS, CPU (user+system), bytes de IO e tamanho do diretório temporário (spill).
- Reporta mediana, p25/p75, IQR, mín/máx e CV. **Nenhum ranking agrega consultas diferentes.**
- Cache do SO: **quente** (não é controlável sem privilégios). O motor começa **frio** em cada processo.

## Troubleshooting
| Sintoma | Causa / solução |
|---|---|
| `Invalid Spark URL: spark://HeartbeatReceiver@MEU_HOST` | Hostname com `_`. Use `SPARK_LOCAL_HOSTNAME=localhost` (os scripts já fazem isso) e `spark.driver.host=localhost` (já está na config). |
| `WARN Shell: Did not find winutils.exe` | Inofensivo aqui: o Spark só **lê** Parquet, e a saída é gravada pelo pyarrow. |
| Mensagens "ÊXITO: o processo … foi finalizado" | É o `taskkill` do pyspark no Windows ao encerrar. O harness não depende do stdout. |
| `java` não encontrado | Defina `JAVA_HOME`. O MSI do winget pede elevação (UAC); o ZIP não pede. |
| `pip` não encontra wheel | Use o Python 3.11–3.13. |
| `PRE-FLIGHT: only X GiB RAM available` | Feche outros aplicativos (navegador, editores, sincronização) ou ajuste `min_available_ram_gib` em `configs/benchmark.json` e documente a mudança. |
| `WinError 1455` (arquivo de paginação muito pequeno) | Memória virtual do Windows esgotada: feche aplicativos ou aumente o pagefile (exige admin). |
| Suspensão do Windows durante a execução | O runner impede a suspensão por inatividade; execuções suspensas viram `INVALID_SUSPEND` e são excluídas. Não feche a tampa do notebook. |
| OOM no Spark em escalas maiores | Ajuste `spark.driver.memory` em `configs/benchmark.json` e registre essa mudança como parte da configuração. |

## Docker
Não incluído nesta versão. Em container, a performance muda (cgroups de CPU/memória, overlay FS, IO virtualizado no Docker Desktop/Windows), então esses resultados **não seriam comparáveis** aos daqui. Fica como trabalho futuro e precisa de um baseline próprio.
