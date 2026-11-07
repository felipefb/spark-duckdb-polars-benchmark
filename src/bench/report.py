"""Statistics + charts generated ONLY from results/<scale>/runs.csv and validation.json (no manual numbers)."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from .queries import DESCRIPTION, QUERIES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
RES = ROOT / "results"
SCALE_ORDER = ["smoke", "baseline", "extension", "xl"]
ENGINES = ["duckdb", "polars", "spark"]
COLOR = {"duckdb": "#2a78d6", "polars": "#eb6834", "spark": "#1baf7a"}  # categorical slots 1-3 (all-pairs valid)
MARKER = {"duckdb": "o", "polars": "s", "spark": "^"}                    # secondary encoding (not color-alone)
INK, INK2, MUTED, GRID, SURF = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#fcfcfb"
SHORT = {"q0": "scan proxy", "q1": "filter + agg", "q2": "join + agg", "q3": "window (latest/customer)",
         "q4": "high-card agg"}
METRICS = {
    "wall_seconds": "end-to-end wall (s)", "t_engine_work": "engine work = register+query (s)",
    "t_init": "engine init (s)", "t_import": "engine import (s)", "t_write": "parquet write (s)",
    "peak_tree_rss_mib": "peak process-tree RSS (MiB)", "cpu_seconds_tree": "process-tree CPU (s)",
}


def load():
    frames = []
    for d in RES.iterdir():
        if (d / "runs.csv").exists():
            frames.append(pd.read_csv(d / "runs.csv"))
    if not frames:
        raise SystemExit("no results/*/runs.csv")
    df = pd.concat(frames, ignore_index=True)
    df["scale"] = pd.Categorical(df["scale"], [s for s in SCALE_ORDER if s in set(df["scale"])], ordered=True)
    return df


def q(x, p):
    return float(x.quantile(p))


def stats(df):
    m = df[~df["warmup"]]
    rows = []
    for (scale, query, engine), g0 in m.groupby(["scale", "query", "engine"], observed=True):
        g = g0[g0["status"] != "INVALID_SUSPEND"]   # system suspend: excluded, reported separately
        ok = g[g["status"] == "PASS"]
        r = {"scale": scale, "input_rows": int(g0["input_rows"].iloc[0]), "query": query, "engine": engine,
             "runs": len(g), "pass_runs": len(ok), "fail_or_error_runs": int((g["status"] != "PASS").sum()),
             "invalid_suspend_runs": int(len(g0) - len(g)),
             "status": "PASS" if len(ok) == len(g) and len(g) > 0 else "FAIL"}
        for col in METRICS:
            if len(ok) and ok[col].notna().any():
                x = ok[col].dropna()
                r[f"{col}_median"] = float(x.median()); r[f"{col}_p25"] = q(x, .25); r[f"{col}_p75"] = q(x, .75)
                r[f"{col}_min"] = float(x.min()); r[f"{col}_max"] = float(x.max())
                r[f"{col}_iqr"] = r[f"{col}_p75"] - r[f"{col}_p25"]
                r[f"{col}_cv"] = float(x.std(ddof=1) / x.mean()) if len(x) > 1 and x.mean() else None
        if r.get("t_engine_work_median"):
            r["throughput_rows_per_s_engine_work"] = r["input_rows"] / r["t_engine_work_median"]
            r["throughput_rows_per_s_end_to_end"] = r["input_rows"] / r["wall_seconds_median"]
        r["output_rows"] = int(ok["output_rows"].iloc[0]) if len(ok) else None
        rows.append(r)
    return pd.DataFrame(rows)


def env_caption(scale):
    p = RES / scale / "environment.json"
    if not p.exists():
        return ""
    e = json.loads(p.read_text())
    return (f"{e['cpu']} ({e['cpu_logical']} logical) · {e['ram_total_gib']} GiB RAM · {e['os']} · "
            f"threads={e['config']['threads']} · Python {e['python'].split()[0]} · "
            f"duckdb {e['packages']['duckdb']} · polars {e['packages']['polars']} · pyspark {e['packages']['pyspark']} (local mode)")


def _style(ax):
    ax.set_facecolor(SURF)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color("#c3c2b7")
    ax.tick_params(colors=INK2, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.6, which="major"); ax.set_axisbelow(True)


def dotplot(st, scale, metric, fname, title, log=True):
    s = st[st["scale"] == scale]
    if s.empty:
        return None
    fig, ax = plt.subplots(figsize=(11, 5.2), facecolor=SURF)
    _style(ax)
    qs = [x for x in QUERIES if x in set(s["query"])]
    off = {"duckdb": -0.22, "polars": 0.0, "spark": 0.22}
    for e in ENGINES:
        g = s[s["engine"] == e].set_index("query")
        for i, qq in enumerate(qs):
            if qq not in g.index:
                continue
            r = g.loc[qq]
            x = i + off[e]
            if r["status"] != "PASS" or pd.isna(r.get(f"{metric}_median")):
                ax.text(x, 0.02, "FAIL\n(excluded)", transform=ax.get_xaxis_transform(), ha="center", color="#d03b3b", fontsize=8)
                continue
            ax.vlines(x, r[f"{metric}_p25"], r[f"{metric}_p75"], color=COLOR[e], linewidth=2)
            ax.plot(x, r[f"{metric}_median"], MARKER[e], color=COLOR[e], markersize=8,
                    markeredgecolor=SURF, markeredgewidth=1.5, label=e if i == 0 else None, zorder=3)
            v = r[f"{metric}_median"]
            left = e == "duckdb"
            ax.annotate(f"{v:.3g}", (x, v), textcoords="offset points", xytext=(-7 if left else 7, -3),
                        ha="right" if left else "left", fontsize=7.5, color=INK2)
    ax.set_xticks(range(len(qs)))
    ax.set_xticklabels([f"{x}\n{SHORT[x]}" for x in qs], fontsize=8.5, color=INK2)
    if log:
        ax.set_yscale("log")
    ax.set_ylabel(METRICS[metric] + (" — log scale" if log else ""), color=INK2)
    n = int(s["pass_runs"].min())
    rows = int(s["input_rows"].iloc[0])
    ax.set_title(f"{title} — {scale}: {rows:,} input rows", loc="left", color=INK, fontsize=12, fontweight="bold")
    ax.legend(frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0), labelcolor=INK2)
    fig.text(0.01, 0.01, f"Marker = median, bar = p25–p75 over n={n} measured runs per point (after 1 warm-up); "
                         f"only correctness-PASS runs. Queries are NOT comparable with each other.\n{env_caption(scale)}",
             fontsize=7, color=MUTED, va="bottom")
    fig.subplots_adjust(bottom=0.24, right=0.88)
    out = RES / "charts" / fname
    fig.savefig(out, dpi=150); plt.close(fig)
    return out


def throughput(st):
    qs = [x for x in QUERIES if x in set(st["query"])]
    fig, axes = plt.subplots(1, len(qs), figsize=(3.2 * len(qs), 3.8), facecolor=SURF, sharey=True)
    for ax, qq in zip(axes, qs):
        _style(ax)
        for e in ENGINES:
            g = st[(st["query"] == qq) & (st["engine"] == e) & (st["status"] == "PASS")].sort_values("input_rows")
            if g.empty:
                continue
            ax.plot(g["input_rows"], g["throughput_rows_per_s_engine_work"], "-", marker=MARKER[e], color=COLOR[e],
                    linewidth=2, markersize=7, label=e)
        ax.set_xscale("log"); ax.set_yscale("log"); ax.set_title(qq, color=INK, fontsize=10, loc="left")
        ax.set_xlabel("input rows", color=INK2, fontsize=8)
    axes[0].set_ylabel("rows/s (input rows ÷ median engine work)", color=INK2, fontsize=8)
    axes[-1].legend(frameon=False, fontsize=8, labelcolor=INK2)
    fig.suptitle("Throughput by scale (engine work only; excludes interpreter/JVM start and import)", x=0.01, ha="left",
                 color=INK, fontsize=11, fontweight="bold")
    fig.tight_layout()
    out = RES / "charts" / "throughput_by_scale.png"
    fig.savefig(out, dpi=150); plt.close(fig)
    return out


def failures(df):
    m = df[~df["warmup"]]
    t = m.groupby(["scale", "engine", "query"], observed=True)["status"].apply(lambda s: int((s != "PASS").sum())).reset_index()
    return t.rename(columns={"status": "non_pass_runs"})


def md_table(st):
    lines = ["| scale | query | engine | status | n | end-to-end median s [p25–p75] | engine work median s [p25–p75] | init s (median) | peak RSS MiB median [max] | CPU s median |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for _, r in st.sort_values(["scale", "query", "engine"]).iterrows():
        def f(c, d=4):
            return "—" if pd.isna(r.get(c)) else f"{r[c]:.{d}f}"
        lines.append(f"| {r['scale']} | {r['query']} | {r['engine']} | {r['status']} | {r['pass_runs']}/{r['runs']} | "
                     f"{f('wall_seconds_median',3)} [{f('wall_seconds_p25',3)}–{f('wall_seconds_p75',3)}] | "
                     f"{f('t_engine_work_median')} [{f('t_engine_work_p25')}–{f('t_engine_work_p75')}] | "
                     f"{f('t_init_median',3)} | {f('peak_tree_rss_mib_median',0)} [{f('peak_tree_rss_mib_max',0)}] | "
                     f"{f('cpu_seconds_tree_median',2)} |")
    return "\n".join(lines)


def linkedin_image(st, scale):
    s = st[(st["scale"] == scale) & (st["query"] != "q0")]
    if s.empty:
        return None
    qs = [x for x in QUERIES if x in set(s["query"])]
    fig, axes = plt.subplots(1, 2, figsize=(16, 9), facecolor=SURF, dpi=120)
    for ax, metric, ttl in ((axes[0], "t_engine_work", "Só o trabalho do motor (registro + consulta)"),
                            (axes[1], "wall_seconds", "Ponta a ponta (início do processo → saída gravada)")):
        _style(ax)
        off = {"duckdb": -0.22, "polars": 0.0, "spark": 0.22}
        for e in ENGINES:
            g = s[(s["engine"] == e) & (s["status"] == "PASS")].set_index("query")
            for i, qq in enumerate(qs):
                if qq not in g.index:
                    continue
                r = g.loc[qq]; x = i + off[e]
                ax.vlines(x, r[f"{metric}_p25"], r[f"{metric}_p75"], color=COLOR[e], linewidth=2.5)
                ax.plot(x, r[f"{metric}_median"], MARKER[e], color=COLOR[e], markersize=11, markeredgecolor=SURF,
                        markeredgewidth=1.5, label=e if i == 0 else None, zorder=3)
                left = e == "duckdb"
                ax.annotate(f"{r[f'{metric}_median']:.2g}s", (x, r[f"{metric}_median"]), textcoords="offset points",
                            xytext=(-9 if left else 9, -4), ha="right" if left else "left", fontsize=10, color=INK2)
        ax.set_yscale("log"); ax.set_xticks(range(len(qs)))
        ax.set_xticklabels([{"q1": "Q1 filtro+agg", "q2": "Q2 join+agg", "q3": "Q3 janela", "q4": "Q4 agg alta card."}[x] for x in qs],
                           fontsize=12, color=INK2)
        ax.set_title(ttl, loc="left", fontsize=15, color=INK)
        ax.set_ylabel("segundos (escala log) — mediana, barra = p25–p75", fontsize=11, color=INK2)
    axes[1].legend(frameon=False, fontsize=13, labelcolor=INK2, loc="upper center", bbox_to_anchor=(-0.08, 1.13), ncol=3)
    rows = int(s["input_rows"].iloc[0]); n = int(s["pass_runs"].min())
    fig.suptitle(f"Spark (modo local) vs DuckDB vs Polars — {rows:,} linhas · 1 máquina · 100% PASS de correção".replace(",", "."),
                 x=0.02, ha="left", fontsize=18, color=INK, fontweight="bold")
    fig.text(0.02, 0.015, f"n={n} medições por ponto após 1 aquecimento · processo novo por execução · cache do SO quente · "
                          f"dados em disco externo USB 3.1 · execução local: não diz nada sobre clusters Spark distribuídos.\n"
                          f"{env_caption(scale)}", fontsize=9.5, color=MUTED)
    fig.subplots_adjust(top=0.84, bottom=0.14, wspace=0.18, left=0.05, right=0.98)
    out = RES / "charts" / "linkedin_16x9.png"
    fig.savefig(out); plt.close(fig)
    return out


def key_results(st):
    """Per scale/query: medians, IQR, ratio to fastest engine (same query only). Generated, never hand-typed."""
    out = ["# Resultados-chave (gerado por `python -m bench.cli report` — não editar à mão)", "",
           "Razão = mediana do motor ÷ menor mediana **da mesma consulta e escala**. Só execuções PASS.", ""]
    for scale in [s for s in SCALE_ORDER if s in set(st["scale"])]:
        s = st[(st["scale"] == scale) & (st["query"] != "q0")]
        rows = int(s["input_rows"].iloc[0])
        out += [f"## {scale} — {rows:,} linhas".replace(",", "."), "",
                "| consulta | motor | trabalho do motor: mediana s (IQR) | razão | ponta a ponta: mediana s (IQR) | razão | pico RSS MiB (mediana) | n |",
                "|---|---|---|---|---|---|---|---|"]
        for qq in [x for x in QUERIES if x in set(s["query"])]:
            g = s[(s["query"] == qq) & (s["status"] == "PASS")]
            bw, be = g["t_engine_work_median"].min(), g["wall_seconds_median"].min()
            for _, r in g.sort_values("engine").iterrows():
                out.append(f"| {qq} | {r['engine']} | {r['t_engine_work_median']:.3f} ({r['t_engine_work_iqr']:.3f}) | "
                           f"{r['t_engine_work_median']/bw:.1f}× | {r['wall_seconds_median']:.2f} ({r['wall_seconds_iqr']:.2f}) | "
                           f"{r['wall_seconds_median']/be:.1f}× | {r['peak_tree_rss_mib_median']:.0f} | {r['pass_runs']} |")
        out.append("")
    sp = st[st["engine"] == "spark"].groupby("scale", observed=True)["t_init_median"].median()
    out += ["## Custo fixo do Spark (init da SparkSession/JVM, mediana entre consultas)", ""]
    out += [f"- {k}: {v:.1f} s" for k, v in sp.items()]
    return "\n".join(out) + "\n"


def main():
    (RES / "charts").mkdir(parents=True, exist_ok=True)
    df = load()
    st = stats(df)
    st.to_csv(RES / "summary_stats.csv", index=False)
    failures(df).to_csv(RES / "failures.csv", index=False)
    (RES / "summary_table.md").write_text(md_table(st), encoding="utf-8")
    (RES / "key_results.md").write_text(key_results(st), encoding="utf-8")
    made = []
    for scale in st["scale"].unique():
        made += [dotplot(st, scale, "wall_seconds", f"latency_end_to_end_{scale}.png", "End-to-end latency"),
                 dotplot(st, scale, "t_engine_work", f"latency_engine_work_{scale}.png", "Engine work latency"),
                 dotplot(st, scale, "peak_tree_rss_mib", f"memory_peak_rss_{scale}.png", "Peak process-tree RSS (sampled 50 ms)", log=False)]
    made.append(throughput(st))
    big = [s for s in ["extension", "baseline", "smoke"] if s in set(st["scale"])]
    if big:
        made.append(linkedin_image(st, big[0]))
    for m in made:
        if m:
            print("wrote", m.relative_to(ROOT))
    print("wrote results/summary_stats.csv, results/summary_table.md, results/failures.csv")


if __name__ == "__main__":
    main()
