# (I) Rascunho para o LinkedIn — revisar antes de publicar

Fonte de todos os números: `results/key_results.md` (gerado), execução de 2026-11-07, escala de 10M linhas.
**Antes de publicar:** confira se os números ainda batem com `key_results.md`.

---

## Post (PT-BR)

O DuckDB foi até 50× mais rápido que o Spark no meu teste.

E esse é o dado menos importante deste post. 👇

Peguei um benchmark pronto de Spark vs DuckDB vs Polars (4 consultas de varejo: filtro, join, janela e agregação). Antes de cronometrar qualquer coisa, fiz uma pergunta simples:

os três estão devolvendo a MESMA resposta?

Não estavam sendo verificados de verdade. Na auditoria, encontrei:

❌ o projeto nem rodava como foi entregue
❌ o "juiz" da correção era o próprio DuckDB, comparado consigo mesmo
❌ o Polars somava um grupo só com NULL como 0 (o SQL devolve NULL)
❌ os dados não tinham NULL, órfãos no join nem estornos

Esse último é o mais perigoso: com dados "bonitos demais", nenhum bug aparecia. O benchmark dizia "tudo certo".

Então inverti a ordem: correção primeiro, velocidade depois.
✅ referência independente em Python puro + SQLite
✅ casos de borda com a resposta escrita à mão ANTES de rodar
✅ validação de cada execução medida
✅ resultado errado não entra no gráfico

270 de 270 execuções passaram. Só então olhei o cronômetro.

Em 10 milhões de linhas, num notebook comum:
→ DuckDB: de 0,17 s a 2,5 s por consulta
→ Polars: de 0,29 s a 4 s
→ Spark (modo local): de 8 s a 26 s, mais ~8,5 s só para iniciar a sessão

Mas não houve vencedor universal: em 1 milhão de linhas, o Polars ganhou do DuckDB numa das consultas.

E o que esse teste NÃO prova: nada sobre Spark em cluster, dados maiores que a memória ou custo total. Para esses problemas, o Spark continua sendo uma ótima escolha.

A lição que levo:
velocidade sem correção é só um número bonito. E um benchmark errado pode justificar uma migração cara.

Deixei tudo aberto (código, dados com seed fixa, testes e limitações) para você rodar na sua máquina. Link nos comentários.

E você: já viu uma decisão de arquitetura ser tomada com base num benchmark que ninguém validou?

#DataEngineering #DuckDB #Polars #ApacheSpark #EngenhariaDeDados

### 1º comentário (publicar logo após o post)
Repositório para reproduzir: https://github.com/felipefb/spark-duckdb-polars-benchmark
Inspiração: "Half the bugs were in the benchmark", de Jacob Verhoeks (4/nov/2026): https://jacob.verhoeks.org/blog/2026-11-04-half-the-bugs-were-in-the-benchmark/
Ambiente: Intel i7-8565U, 16 GB, Windows 11, 4 threads por motor, DuckDB 1.4.1, Polars 1.35.2, PySpark 4.0.1. Mediana de 5 medições após aquecimento.

### Dicas de publicação (engajamento)
- **As 2 primeiras linhas** são o gancho: é o que aparece antes do "ver mais". O dado do 50× chama atenção, e a virada ("o menos importante") faz a pessoa abrir o post.
- **Links no 1º comentário**, não no corpo: o LinkedIn tende a reduzir o alcance de posts com link externo.
- **Imagem ou carrossel junto:** use o carrossel (abaixo) ou `results/charts/linkedin_16x9.png`.
- **Responda os comentários na primeira hora**: é quando o algoritmo decide o alcance.
- **A pergunta final** convida experiências pessoais, que geram comentários mais longos do que um "concordo".
- Horário sugerido: terça a quinta, entre 8h e 10h.

---

## Carrossel (6 slides — título curto + 1 ideia por slide)
1. **"O DuckDB foi 50× mais rápido que o Spark. E isso quase não importa."** — Subtítulo: o que um benchmark precisa provar antes de medir velocidade.
2. **"A pergunta errada"** — "Qual é o mais rápido?" vem depois de "todos dão a mesma resposta?". Um motor 10× mais rápido que trata NULL diferente é um bug em produção.
3. **"O que a auditoria encontrou"** — 4 problemas com ❌: não rodava · DuckDB julgando a si mesmo · NULL somado como 0 no Polars · dados sem casos de borda (por isso nenhum bug aparecia).
4. **"Correção primeiro"** — referência independente (Python puro + SQLite) · casos de borda com resposta escrita à mão · cada execução validada · **270/270 PASS** antes de olhar qualquer tempo.
5. **"O resultado (10M de linhas, 1 notebook)"** — imagem `results/charts/linkedin_16x9.png`. DuckDB e Polars entre 0,17 s e 4 s; Spark local entre 8 s e 26 s, mais ~8,5 s de inicialização. Sem vencedor universal.
6. **"Quando usar cada um"** — cabe numa máquina: DuckDB (SQL) ou Polars (DataFrame) · distribuído, maior que a RAM, plataforma existente: Spark (pede outro benchmark) · **Valide primeiro, meça depois.** Código aberto: link no post.

## Imagem 16:9
`results/charts/linkedin_16x9.png`: medianas e p25–p75 por consulta, trabalho do motor × ponta a ponta, com hardware e escala na legenda.

## Short English version
DuckDB was up to 50× faster than Spark in my test.

That's the least important number in this post. 👇

Before timing anything, I asked: do all three engines return the SAME answer? The benchmark I started from couldn't tell. It didn't run, DuckDB was judging itself, and Polars summed all-NULL groups as 0 instead of NULL. The data had no NULLs or orphan keys, so no bug ever showed up.

So I flipped the order: correctness first. Independent reference (plain Python + SQLite), edge cases with hand-written answers, every measured run validated. 270/270 passed. Only then did I look at the clock.

On 10M rows on a regular laptop: DuckDB 0.17–2.5 s per query, Polars 0.29–4 s, local-mode Spark 8–26 s plus ~8.5 s of session start-up. Still no universal winner: at 1M rows, Polars beat DuckDB on one query. And none of this says anything about Spark clusters or data larger than memory.

Speed without verified correctness is just a pretty number.

Have you ever seen an architecture decision made on a benchmark nobody validated?

Repo + source (Jacob Verhoeks, "Half the bugs were in the benchmark") in the comments.

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
- [x] Fonte verificada (HTTP 200, 2026-11-07): "Half the bugs were in the benchmark", Jacob Verhoeks, 4/nov/2026 — https://jacob.verhoeks.org/blog/2026-11-04-half-the-bugs-were-in-the-benchmark/
- [ ] Opcional: rodar o benchmark em outra máquina (SSD interno/Linux) para checar se a conclusão se mantém
- [ ] Opcional: experimento separado com Spark em sessão aquecida e/ou cluster, com custos documentados
