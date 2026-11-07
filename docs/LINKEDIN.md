# (I) Rascunho para o LinkedIn — revisar antes de publicar

Fonte de todos os números: `results/key_results.md` (gerado), execução de 2026-11-07, escala de 10M linhas.
**Antes de publicar:** confira se os números ainda batem com `key_results.md`.

---

## Post (PT-BR)

**Spark vs DuckDB vs Polars: o mais rápido nem sempre é o melhor — e um benchmark errado pode custar caro**

Muita empresa quer cortar custo e latência dos pipelines analíticos e começa pela pergunta "qual motor é mais rápido?". Antes dela, vem outra: os três estão devolvendo a mesma resposta?

Peguei um benchmark simples de varejo (4 consultas: filtro, join, janela e agregação de alta cardinalidade) e, antes de cronometrar qualquer coisa, auditei o código. O que encontrei:
• o projeto nem rodava como foi entregue;
• o "oráculo" de correção era o próprio DuckDB, comparado consigo mesmo;
• o Polars somava um grupo só com NULL como 0, enquanto o SQL devolve NULL;
• os dados não tinham NULL, órfãos no join nem estornos, então esses bugs nunca apareciam.

Refiz com correção como bloqueio: uma referência independente em Python puro (e SQLite), casos de borda com resultado escrito à mão e validação de TODA execução medida. Nenhum tempo entra no gráfico sem PASS.

Resultado medido (1 notebook, i7-8565U, 16 GB, 10M de linhas, 5 medições após aquecimento, Spark em modo local):
• DuckDB e Polars ficaram na mesma ordem de grandeza: de ~0,2 s a ~4 s de trabalho do motor por consulta;
• o Spark local levou de ~8 a ~26 s, mais ~8,5–10,5 s só para iniciar a sessão;
• nenhum motor venceu tudo: em 1M, o Polars ganhou do DuckDB na agregação de alta cardinalidade.

O que isso NÃO diz: nada sobre Spark em cluster, dados maiores que a RAM, governança ou custo total. Spark continua fazendo sentido quando o problema é distribuído e a plataforma já existe.

A lição: velocidade sem correção verificada é só um número bonito. Valide primeiro, meça depois, e publique as limitações junto.

Código, dados sintéticos com seed fixa e instruções para reproduzir: https://github.com/felipefb/spark-duckdb-polars-benchmark

Inspirado na auditoria de Jacob Verhoeks ("Half the bugs were in the benchmark", nov/2026).

#DataEngineering #DuckDB #Polars #ApacheSpark #Benchmark

---

## Carrossel (6 slides)
1. **Problema** — "Qual motor é mais rápido?" é a segunda pergunta. A primeira: os resultados estão certos? Velocidade sem correção leva a decisões caras.
2. **Motores** — Spark 4.0.1 (modo local), DuckDB 1.4.1 e Polars 1.35.2, com mesma entrada Parquet, mesmas regras e 4 threads. Q1 filtro+agg · Q2 join · Q3 janela · Q4 agg de alta cardinalidade.
3. **Metodologia** — seed fixa · processo novo por execução · 1 aquecimento + 5 medições · ordem alternada · mediana e p25–p75 · fases separadas (init, consulta, escrita) · RSS/CPU da árvore de processos · manifesto do ambiente.
4. **Correção** — referência independente (Python puro + SQLite) · fixtures com NULL, duplicatas, empates, órfãos, negativos e datas-limite · hash canônico + invariantes · 270/270 execuções PASS · 65 testes. No código original: 3 bugs latentes de semântica de NULL e tipos.
5. **Resultados (10M linhas, 1 notebook)** — usar `results/charts/linkedin_16x9.png`. DuckDB e Polars entre ~0,2 e ~4 s; Spark local entre ~8 e ~26 s, mais ~8,5–10,5 s de inicialização. Sem vencedor universal.
6. **Recomendações** — dados que cabem numa máquina: DuckDB (SQL) ou Polars (DataFrame). Spark: escala distribuída e plataforma existente, mas isso pede outro benchmark. Reproduza: https://github.com/felipefb/spark-duckdb-polars-benchmark.

## Imagem 16:9
`results/charts/linkedin_16x9.png`: medianas e p25–p75 por consulta, trabalho do motor × ponta a ponta, com hardware e escala na legenda.

## Short English version
**Spark vs DuckDB vs Polars: fastest isn't always best — and a wrong benchmark is expensive.**
Before timing anything I audited a small retail benchmark: it didn't run, DuckDB was its own correctness oracle, and Polars summed all-NULL groups as 0 instead of NULL (masked because the data had no NULLs). I rebuilt it with a blocking, independent reference (plain Python + SQLite), hand-written edge cases and validation of every measured run. On one laptop (10M rows, 5 runs after warm-up), DuckDB and Polars took ~0.2–4 s of engine time per query; local-mode Spark took ~8–26 s, plus ~8.5–10.5 s session start-up. No universal winner, and nothing here speaks to Spark clusters. Validate first, then measure. Repo: https://github.com/felipefb/spark-duckdb-polars-benchmark

---

# (J) Checklist final de publicação
- [x] Smoke test reproduzível a partir de um ambiente limpo (venv recriado do `requirements.lock` em E:, com 65/65 testes)
- [x] 100% das consultas publicadas com PASS (270/270 execuções em 3 escalas)
- [x] ≥ 5 repetições válidas por consulta e motor (1M e 10M)
- [x] Geração e consultas versionadas; comandos exatos (README); manifesto do ambiente por escala
- [x] Números e gráficos gerados por script (`key_results.md`, `summary_*.csv/md`, `charts/`)
- [x] Limitações explícitas (`docs/RESULTS.md` G), sem afirmação de superioridade universal ou sobre cluster
- [x] Repositório público criado: https://github.com/felipefb/spark-duckdb-polars-benchmark
- [x] Datasets, Parquet, venv e logs de crash da JVM fora do repositório
- [ ] Verificar o link e o título do artigo de Jacob Verhoeks (citados no PROMPT; **não verifiquei a URL**)
- [ ] Opcional: rodar o benchmark em outra máquina (SSD interno/Linux) para checar se a conclusão se mantém
- [ ] Opcional: experimento separado com Spark em sessão aquecida e/ou cluster, com custos documentados
