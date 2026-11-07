# Semântica de referência (definida antes da execução)

Fonte da verdade do código: `src/bench/queries.py` (SQL, esquemas e implementação Polars).
Oráculo primário: `src/bench/reference.py::python_reference`, em Python puro, sem SQL e sem código compartilhado com os motores.
Oráculo secundário: SQLite (stdlib), rodando o mesmo SQL, para escalas de até 1M de linhas.
Expectativas escritas à mão: `tests/fixtures.py::EXPECTED_EDGE`.

## Tipos
| coluna | tipo | observações |
|---|---|---|
| order_id | INT64 | único no dataset gerado; fixtures têm duplicatas de propósito |
| customer_id | INT64 NULLável | |
| store_id | INT32 | |
| product_id | INT64 NULLável | 1001..1050 não existem em `products` (órfãos) |
| day_id | INT32 | 0..364; não há timestamps, então **timezone não se aplica às colunas**. Mesmo assim, `TZ=UTC` e `spark.sql.session.timeZone=UTC` ficam fixados |
| status | STRING NULLável | comparação case-sensitive |
| amount_cents | INT64 NULLável | dinheiro em **centavos inteiros**; há valores negativos (estorno) e zero |

Saídas: as contagens e somas são `CAST(... AS BIGINT)` em todos os motores. O validador compara tipos **lógicos**: `string`, `large_string` e `string_view` são todos "string"; os inteiros precisam bater exatamente (int32 ≠ int64).

## Regras
1. **NULL em predicado** (lógica de três valores do SQL): `NULL >= 180`, `NULL = 'paid'` e `NULL <> 'cancelled'` não são verdadeiros, então a linha é excluída.
2. **COUNT(\*)** conta linhas, inclusive as com `amount_cents` NULL.
3. **SUM** ignora NULL. A soma de zero valores não-NULL é **NULL** (não 0). Isso exige `when(count>0)` no Polars.
4. **GROUP BY / PARTITION BY** tratam NULL como um grupo único.
5. **INNER JOIN** descarta chaves NULL e órfãs. Reconciliação: `Σ q2.n + linhas_não_casadas = linhas com status não-NULL e ≠ 'cancelled'`.
6. **Q3, desempate total**: `day_id DESC NULLS LAST, order_id DESC NULLS LAST, amount_cents DESC NULLS LAST`. Linhas que ainda empatam são idênticas nas colunas projetadas, então a saída é determinística.
7. **Duplicatas** nunca são deduplicadas.
8. **Ordem das linhas** não faz parte do resultado: a comparação é por multiconjunto canônico, com NULL ordenado primeiro e uma tag de tipo.
9. **Overflow**: somas em INT64. Com o gerador atual, o máximo possível em 10M de linhas é ~1e12, bem abaixo de 9.2e18. O Spark roda com ANSI (dá erro em overflow), o DuckDB dá erro no CAST, e o Polars daria wrap silencioso. Não testado com valores extremos.
10. **LENGTH(status)** (só no Q0): só há strings ASCII, então caracteres = bytes = code points.

## Consultas
| id | o quê | chave |
|---|---|---|
| q0 | *proxy* de leitura: agregados de todas as colunas (não é consulta de negócio) | — |
| q1 | `day_id >= 180 AND status = 'paid'`, agrupado por (store_id, status) | store_id, status |
| q2 | inner join com products, `status <> 'cancelled'`, agrupado por category (NULL é um grupo) | category |
| q3 | última transação por cliente (ROW_NUMBER = 1) | customer_id |
| q4 | agregação de alta cardinalidade por customer_id (~N/10 grupos) | customer_id |

## Verificações por execução (todas bloqueantes)
esquema lógico · nº de linhas · chaves únicas · conjunto de chaves · hash SHA-256 canônico (= valores exatos) · invariantes:
q0 `n_rows = total` e `sum_amount = Σ amount`; q1 `Σn = linhas filtradas`; q2 reconciliação do join; q3 `linhas = clientes distintos (incl. NULL)`; q4 `Σn = total`, `Σtotal = Σ amount`, `grupos = clientes distintos`.
Os invariantes são calculados em Python puro, direto do Parquet de entrada.
**Toda** repetição medida é validada, não só a última.
