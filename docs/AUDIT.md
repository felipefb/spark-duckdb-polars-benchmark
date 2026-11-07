# (A) Auditoria do ZIP original — `benchmark_spark_duckdb_polars.zip`

Conteúdo: `data_engine_benchmark/benchmark.py` (142 linhas), `README.md`, `requirements.txt` e um `__pycache__/benchmark.cpython-313.pyc`. Esse `.pyc` indica que o arquivo foi **compilado** (verificação de sintaxe) em Python 3.13, o que confirma a ressalva de que o código não tinha sido executado.
Cópia intacta: `original/`. Cópia com o *mínimo* de patches para conseguir observar o comportamento: `original_minpatch/` (diff em `logs/original_minpatch.diff`).

Legenda: **[OBS]** observado em execução nesta máquina · **[TESTE]** demonstrado por teste automatizado · **[INSP]** inspeção de código (não executado).

| # | Severidade | Achado | Evidência |
|---|---|---|---|
| O1 | Bloqueante | `generate` falha: DuckDB 1.4.1 não aceita parâmetro `?` como alvo de `COPY ... TO`. O projeto **não roda** como entregue. | [OBS] `logs/original_generate_10k.log` (`INTERNAL Error: Unsupported parameter type for filename`) |
| O2 | Bloqueante | Harness com **deadlock**: `stdout/stderr=PIPE` só são lidos depois de `poll()`. Quando o Spark falha (stacktrace de ~14 KB > buffer do pipe no Windows), o pai e o filho ficam esperando um ao outro para sempre. O erro original era o hostname com `_` (`Invalid Spark URL: spark://HeartbeatReceiver@SB_FAMILY_HOME`). | [OBS] smoke de 10k parado por >10 min; `logs/original_spark_direct.log` |
| O3 | Bloqueante | Parsing frágil: o harness lê a *última* linha do stdout como JSON, mas no Windows o `pyspark` imprime "ÊXITO: o processo … foi finalizado" (taskkill) depois. O resultado é um `JSONDecodeError` que derruba a rodada inteira, sem gerar `runs.csv`. | [OBS] `logs/original_minpatch_run_10k.log` (1ª tentativa) |
| O4 | Alta | O oráculo é o próprio **DuckDB**: o DuckDB é comparado consigo mesmo, e os "invariantes independentes" também são calculados pelo DuckDB. | [INSP] `validate()` |
| O5 | Alta | Só a **última** repetição é validada, porque o CSV é sobrescrito a cada execução. Se uma execução posterior dá ERROR, o CSV *antigo* continua lá e passa na validação. | [INSP] `write_output`/`validate` |
| O6 | Alta | O `status='PASS'` em `runs.csv` só significa *returncode 0*, não correção. A validação roda depois de todas as medições e não impede que os tempos sejam usados. | [INSP] `measure`/`run` |
| O7 | Alta | Dados por aritmética modular: **nenhum NULL, nenhum órfão no join, nenhum valor negativo ou zero**. Além disso, `product_id = i%1000+1` determina `store_id = i%100+1` e a categoria, ou seja, as colunas são correlacionadas e comprimem de forma artificial. Os casos de borda que o README cita como necessários nunca são exercitados. | [INSP] + [OBS] `all_pass: true` em 10k |
| O8 | Alta (latente) | Polars `.sum()` de um grupo só com NULL retorna **0**, enquanto o SQL retorna **NULL**. | [TESTE] `test_original_polars_sum_of_all_null_returns_zero` |
| O9 | Alta (latente) | Polars Q3 `sort(descending)` sem `nulls_last`: NULL vem **primeiro**, ao contrário do DuckDB/Spark (`DESC` → NULLS LAST). Escolhe a linha errada. | [TESTE] `test_original_polars_q3_null_ordering_differs` |
| O10 | Média (latente) | O invariante `Σq2.n == count(status<>'cancelled')` está matematicamente errado quando existem órfãos ou chaves NULL. Só passa porque os dados não têm nenhum. | [TESTE] `test_original_q2_invariant_breaks_with_orphans` |
| O11 | Média | O desempate do Q3 (`day_id, order_id`) só é total se `order_id` for único. Com duplicatas, o resultado não é determinístico. | [INSP]; coberto por fixture |
| O12 | Média | Tipos não são comparados: o CSV vira string. O DuckDB `SUM(BIGINT)` gera HUGEINT e o Polars `pl.len()` gera UInt32, e essas divergências ficam escondidas. | [TESTE] `test_original_polars_len_is_uint32` |
| O13 | Média | Instrumentação desigual: `seconds` inclui o `import` do polars/pyspark (feito dentro da função), mas não o do duckdb (feito no topo do módulo). Também inclui conversão para objetos Python (`fetchall`/`collect`/`iter_rows`), que penaliza de forma diferente cada motor (o Spark via Py4J). | [INSP] |
| O14 | Média | Metodologia: padrão de 3 repetições, sem aquecimento, sem estatísticas (`statistics` importado e não usado), sem CPU/IO/spill, sem gráficos e sem manifesto de hardware (só `cpu_count`). | [INSP] |
| O15 | Baixa | O Spark usa `spark.driver.memory` padrão (1g) e faz `collect()` de ~1M de linhas no Q3/Q4 em 10M. Risco de OOM: **hipótese, não testada** no original. | [INSP] |
| O16 | Info | Os números de 10k do original (`logs/original_minpatch_results_10k/runs.csv`) foram medidos **com 1 repetição e 2 threads** e não devem ser publicados. | [OBS] |

## Diferenças original → revisado (resumo)
| Aspecto | Original | Revisado |
|---|---|---|
| Gerador | aritmética modular no DuckDB, sem borda | numpy PCG64 com seed fixa, em chunks; NULLs, órfãos, negativos, zeros, empates; hash de conteúdo no manifesto |
| Consultas | Q1–Q4 | Q1–Q4 com a **mesma semântica de negócio**, mais: `CAST(... AS BIGINT)` explícito, `NULLS LAST` explícito, desempate do Q3 estendido com `amount_cents DESC`. **Q0** é novo (proxy de leitura, não é consulta de negócio) |
| Polars | `.sum()`, `pl.len()` UInt32, ordenação sem nulls_last | SUM com semântica SQL, Int64, `nulls_last=True` |
| Oráculo | DuckDB | Python puro (independente), mais SQLite como 2º oráculo (≤1M) e expectativas escritas à mão nas fixtures |
| Validação | só a última execução, CSV como string | **toda** execução: esquema lógico, contagem, chaves, unicidade, hash canônico, invariantes; FAIL é bloqueante |
| Materialização | objetos Python e CSV | Arrow (nativo de cada motor) e o **mesmo** escritor pyarrow Parquet para todos |
| Tempo | 1 número, mistura import/init | fases: import · init · register · query · write · close · wall do processo |
| Recursos | RSS da árvore | RSS da árvore, CPU da árvore, IO de leitura/escrita, tamanho do diretório de spill |
| Protocolo | 3 reps, sem aquecimento | 1 aquecimento + 5 medições; ordem dos motores = permutação da rodada; ordem das consultas rotacionada |
| Subprocesso | PIPE (deadlock), parse do stdout | log em arquivo, resultado em JSON próprio, timeout com kill da árvore |
| Spark | padrões | `local[4]`, driver 6g, UTC, ANSI, AQE, `driver.host=localhost` |
