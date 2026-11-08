# Resultados, limitações e decisões — execução de 2026-11-07

> **Todos os números deste projeto saem de scripts.** As fontes são `results/key_results.md`, `results/summary_table.md` e `results/summary_stats.csv`, gerados por `python -m bench.cli report` a partir de `results/<escala>/runs.csv`. Este documento interpreta esses números; quando cita algum, indica o arquivo de origem. Se o benchmark for executado de novo, valem os arquivos gerados, não este texto.

Legenda: **[MEDIDO]** resultado observado · **[ESTIMATIVA]** derivado com premissas · **[HIPÓTESE]** não testado aqui.

## (E) Correção — tabela PASS/FAIL
Fonte: `results/<escala>/validation.json`, `results/pytest_junit.xml`, `logs/pytest_after_migration.log`.

| Verificação | smoke 10k | baseline 1M | extensão 10M |
|---|---|---|---|
| Oráculo Python puro × SQLite | concordam (5/5 consultas) | concordam (5/5) | SQLite não executado (>1M); só Python puro |
| DuckDB q0–q4 | PASS em todas as execuções | PASS | PASS |
| Polars q0–q4 | PASS | PASS | PASS |
| Spark q0–q4 | PASS | PASS | PASS |
| Execuções (1 aquecimento + 5 medições) × 15 combinações motor/consulta | 90/90 PASS (75 medições) | 90/90 PASS (75 medições) | 90/90 PASS (75 medições) |
| Execuções inválidas por suspensão do SO | 0 | 0 | 0 |
| Testes automatizados (`python -m pytest`) | **65/65 PASS**: fixtures de borda com resultado escrito à mão, 3 datasets × 5 consultas × 3 motores, mutações do validador e defeitos do original | | |

Exemplos mínimos reproduzíveis das divergências do **código original** (que agora falham de propósito nos testes): `tests/test_correctness.py::test_original_*`. Detalhes em `docs/AUDIT.md` (O8–O12).

## (F) Resultados medidos
Fonte: `results/key_results.md` (medianas, IQR, razão para o motor mais rápido **na mesma consulta**) e `results/summary_table.md` (p25/p75, init, RSS, CPU).

**Ambiente** [MEDIDO] (`results/<escala>/environment.json`): Intel i7-8565U (4 núcleos / 8 lógicos, notebook), 15,8 GiB RAM, Windows 11, Python 3.12.10, Temurin 17.0.21, duckdb 1.4.1, polars 1.35.2, pyspark 4.0.1 `local[4]`, 4 threads em todos os motores. Dados em `E:` (disco externo USB 3.1), com cache do SO quente.
**Dataset** [MEDIDO] (`results/<escala>/dataset_manifest.json`): sales.parquet com 10k linhas ≈ 0,2 MiB, 1M ≈ 18,5 MiB, 10M ≈ 203,8 MiB. Geração levou ~0,4 s / ~1,0 s / ~5,9 s, medida separadamente.

Observações [MEDIDO], cada uma válida só para a sua consulta e escala:
1. **O Spark local tem um custo fixo de ~8,5–10,5 s só para iniciar a sessão (JVM + SparkSession)**, igual em 10k, 1M e 10M (`key_results.md`, seção "Custo fixo"). Mesmo sem contar isso, o "trabalho do motor" no Spark ficou entre ~6,5 s e ~26 s em todas as escalas. Nesta máquina e nessas escalas, o Spark foi o mais lento em todas as consultas, nas duas métricas.
2. **DuckDB e Polars ficaram na mesma ordem de grandeza.** Nenhum dos dois venceu tudo. Em 10M, o DuckDB teve a menor mediana nas 4 consultas de negócio. Em 1M, **o Polars foi mais rápido no Q4** (agregação de alta cardinalidade) no *tempo do motor*, com faixas p25–p75 sem sobreposição. No *ponta a ponta*, o DuckDB ficou ligeiramente à frente, mas dentro da variação entre execuções: na prática, empate. O vencedor depende do volume e da métrica. As razões estão em `key_results.md`.
3. **O Q3 (janela) é o caso mais caro** para os três motores em 10M, e é onde o Spark fica relativamente menos distante (razão ~10× contra ~20–50× nas outras consultas).
4. **Memória (pico de RSS da árvore de processos):** em 10M, o DuckDB ficou abaixo de ~550 MiB, o Polars entre ~0,6 e 1,5 GiB e o Spark entre ~1,1 e 2,5 GiB (mediana; a JVM está incluída). Veja a ressalva sobre RSS nas limitações.
5. **CPU:** o Spark consome várias vezes o seu tempo de parede em CPU (coluna "CPU s" em `summary_table.md`), por causa da JVM, da compilação JIT e das threads de GC. Isso importa em ambientes que cobram por CPU.
6. **Dispersão:** no 1M e no smoke, alguns pontos têm p75 bem acima da mediana em execuções de ~1 s (veja `wall_seconds_cv` em `summary_stats.csv`). Atribuo isso, sem ter comprovado, a ruído do SO, do Defender e do disco USB. Diferenças menores que o IQR não devem ser interpretadas.

## (G) Limitações e riscos — leia antes de citar qualquer número
- **Uma máquina só, um notebook**, com turbo e gerenciamento de energia do Windows, sem isolamento de CPU. O Zed (editor) ficou aberto durante as medições; Edge, Copilot, OneDrive, Slack e Phone Link foram fechados antes do 10M.
- **Cache do SO quente**, sem controle de cache frio (precisaria de privilégios). Cada motor começa frio, porque cada execução é um processo novo.
- **Disco externo USB 3.1:** os tempos incluem esse caminho de I/O. Num SSD NVMe interno ou num storage de nuvem, os números seriam outros.
- **Spark só em modo local.** **Nada aqui vale para um cluster.** Também não foi medido o Spark com uma sessão já aquecida (cenário típico de notebook ou serviço), que eliminaria o custo fixo de ~8,5–10,5 s a cada consulta. [HIPÓTESE] Nesse cenário, a distância diminuiria bastante. Não foi testado.
- **Escala máxima de 10M linhas (~204 MiB de Parquet)**, tudo cabendo em memória. 100M **não foi executado**: com 15,8 GiB de RAM e outros programas abertos, a checagem de 9 GiB livres só passou depois de fechar aplicativos, e o risco de OOM ou paginação invalidaria a medida. Os resultados **não se generalizam** para dados maiores que a RAM, onde spill e particionamento passam a dominar.
- **RSS amostrado a cada 50 ms** é aproximado: pode perder picos curtos e inclui páginas de arquivos mapeados. Não é um limite rígido de memória.
- **Métrica de I/O descartada:** os contadores de I/O do Windows (psutil) não capturam leituras via mmap. O Polars mostrou ~8 MiB lidos em 10M, o que é implausível, então o I/O **não é publicado** como comparação.
- **Spill:** 0 MiB observado em todos os motores (os diretórios de spill ficam em `E:\...\tmp`). Nenhuma consulta pressionou a memória o bastante.
- **Leitura e processamento não são separáveis** nos motores lazy. `t_query` inclui leitura do Parquet, processamento e materialização em Arrow; o Q0 serve só como *proxy* de leitura.
- **Dados sintéticos** (uniformes, com uma pequena fração de NULL, órfãos e estornos). Distribuições com skew forte, strings longas ou esquemas largos podem mudar o ranking.
- **Versões fixadas.** Outras versões dos motores podem mudar os resultados.
- **Custo de nuvem não calculado:** não há instância, região nem preço rastreável, então nenhum valor em dinheiro foi estimado.
- **Configuração não otimizada** em nenhum dos motores, além do mínimo documentado em `configs/benchmark.json`. Ajustes específicos, como mais threads, pool da JVM ou streaming no Polars, podem mudar os números.
- **Execuções descartadas** (`results/_discarded/`): uma rodada afetada pela suspensão do Windows (Modern Standby), rodadas medidas com os dados no SSD interno (C:) antes da migração para E: e uma tentativa de 10M que esgotou a memória virtual do Windows: a JVM do Spark abortou por falta de memória nativa (`hs_err_pid*.log`, mantido só localmente) e o harness falhou com `WinError 1455`. Nenhuma delas entra nos números publicados.

## (H) Gráficos
`results/charts/` — todos gerados por `report.py`, sempre com mediana e p25–p75, n e ambiente na legenda, e só com execuções PASS:
- `latency_engine_work_<escala>.png` e `latency_end_to_end_<escala>.png`: latência por consulta (escala log)
- `memory_peak_rss_<escala>.png`: pico de RSS
- `throughput_by_scale.png`: linhas/s por escala (só o trabalho do motor)
- `linkedin_16x9.png`: imagem para publicação (10M)
- Falhas: `results/failures.csv` (todas zero). Os gráficos marcariam "FAIL (excluded)" se houvesse.

## Matriz de decisão
| Situação | Observação medida aqui | Recomendação arquitetural (julgamento, **não** medido) |
|---|---|---|
| Consultas analíticas interativas ou ad hoc em dados que cabem numa máquina | DuckDB e Polars em ~0,2–4 s no trabalho do motor em 10M; Spark local ≥ 8 s, mais ~8,5–10,5 s de inicialização | **DuckDB** (SQL) ou **Polars** (API DataFrame em Python), conforme a equipe |
| Pipelines Python com transformações encadeadas | Polars competitivo; no Q4 em 1M foi mais rápido no tempo do motor (empate no ponta a ponta) | **Polars** quando o código já é DataFrame. Atenção ao uso de memória, maior que o do DuckDB nas medições |
| SQL embarcado, baixo uso de memória | DuckDB teve o menor RSS em todas as consultas de 10M | **DuckDB** |
| Jobs curtos e frequentes (≤ segundos) | O custo fixo do Spark local (~8,5–10,5 s) domina | Evitar o Spark local para isso |
| Dados maiores que a RAM de uma máquina, processamento distribuído, governança e ecossistema existente (Delta/Iceberg, Unity/Hive, streaming) | **Não medido aqui** | **Spark em cluster** pode ser a escolha certa. Exige um benchmark próprio, com cluster, custos e configuração documentados |
| Plataforma com Spark já operado, equipe treinada | Não medido | O custo total (operação, governança, equipe) pode superar a diferença de latência. Decida com os dados do seu ambiente |

**Conclusão proporcional à evidência:** nesta máquina, até 10M linhas, para estas 4 consultas, DuckDB e Polars entregaram as mesmas respostas exatas que o Spark local em uma fração do tempo e da memória, e nenhum dos dois venceu todas as consultas. Isso **não** demonstra superioridade geral, nem diz nada sobre Spark distribuído.
