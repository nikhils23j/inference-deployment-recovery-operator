"""Make the README figures from the saved results files.

Reads ONLY:
  results-chaos.json   offline chaos simulation, all variants (v2 era)
  results-live.json    the old live k3s run of the v1 controller (read-only, never modified)
and writes PNGs to docs/img/. Every plotted value comes from those files.

    python3 scripts/make_figures.py

Needs matplotlib + numpy (the operator itself doesn't).
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "img"

SURFACE = "#ffffff"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
V2 = "#2a78d6"     # blue: v2, the thing this repo is about
V1 = "#eb6834"     # orange: v1, the old code (also the only thing measured live)
OTHER = "#b5b3ad"  # gray: context variants (baselines, ablations)

VARIANTS = [  # (key, label, colour), top to bottom
    ("v2", "v2", V2),
    ("v2_no_lease", "v2 without Lease", OTHER),
    ("v2_no_refresh", "v2 without 409 re-read", OTHER),
    ("v1", "v1 (old code)", V1),
    ("naive", "naive poller", OTHER),
    ("none", "no controller", OTHER),
]

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 11,
    "axes.edgecolor": AXIS,
    "axes.labelcolor": INK_2,
    "axes.titlecolor": INK,
    "xtick.color": MUTED,
    "ytick.color": INK_2,
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
})


def style(ax, grid_axis="x"):
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.tick_params(axis="both", length=0)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def title(fig, main, sub, tag=None):
    fig.text(0.012, 0.975, main, ha="left", va="top", fontsize=15, weight="bold", color=INK)
    fig.text(0.012, 0.905, sub, ha="left", va="top", fontsize=10.5, color=INK_2)
    if tag:  # evidence-type badge so simulated and live numbers can't be confused
        fig.text(0.988, 0.975, tag, ha="right", va="top", fontsize=10, weight="bold",
                 color=INK, bbox=dict(boxstyle="round,pad=0.4", fc="#f0efec", ec=AXIS))


def save(fig, name):
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"wrote {path.relative_to(ROOT)} ({path.stat().st_size // 1024} KB)")


def g(x):
    """Format a number from the results file without inventing precision."""
    return f"{x:g}"


# --------------------------------------------------------------------------- 1. convergence
def fig_convergence(chaos):
    s, runs = chaos["summary"], chaos["runs"]
    n_runs = s["v2"]["runs"]
    fig, (axl, axr) = plt.subplots(1, 2, figsize=(12, 5.4), sharey=True,
                                   gridspec_kw={"width_ratios": [1, 2.3]})
    fig.subplots_adjust(left=0.15, right=0.98, top=0.76, bottom=0.12, wspace=0.06)
    ys = range(len(VARIANTS))

    # left: converged and stayed healthy, of N
    for y, (key, _, color) in zip(ys, VARIANTS):
        c = s[key]["converged"]
        axl.barh(y + 0.04, c, height=0.5, color=color)
        axl.text(c + n_runs * 0.02, y + 0.04, f"{c}", va="center", ha="left", fontsize=10,
                 color=INK if key in ("v1", "v2") else INK_2)
    axl.set_xlim(0, n_runs * 1.18)
    axl.set_yticks(list(ys))
    axl.set_yticklabels([lbl for _, lbl, _ in VARIANTS])
    axl.set_ylim(len(VARIANTS) - 0.55, -0.75)
    axl.set_xlabel(f"converged and stayed healthy (of {n_runs})")
    axl.set_title("Did it converge?", loc="left", fontsize=11, color=INK, pad=10)
    style(axl)

    # right: per-run convergence time (converged runs only), symlog because of the long tail
    rng = np.random.default_rng(0)  # jitter only; layout, not data
    for y, (key, _, color) in zip(ys, VARIANTS):
        vals = np.array([r["convergence_s"] for r in runs[key] if r["converged"]])
        jitter = rng.uniform(-0.12, 0.2, len(vals))
        axr.scatter(vals, y + jitter, s=9, color=color, alpha=0.45, linewidths=0, zorder=2)
        cs = s[key]["convergence_s"]
        axr.plot([cs["p50"]], [y + 0.04], marker="|", markersize=20, markeredgewidth=2.5, color=INK,
                 zorder=3)
        axr.plot([cs["p90"]], [y + 0.04], marker="|", markersize=13, markeredgewidth=1.5, color=INK_2,
                 zorder=3)
        axr.text(0, y - 0.27, f"p50 {cs['p50']:.3g} s   p90 {cs['p90']:.3g} s   "
                 f"max {cs['max']:.3g} s", va="bottom", ha="left", fontsize=9, color=INK_2)
    axr.set_xscale("symlog", linthresh=1)
    axr.set_xlim(0, 100)
    axr.set_xticks([0, 1, 10, 100])
    axr.set_xticklabels(["0", "1", "10", "100"])
    axr.set_xlabel("simulated seconds from the end of the last fault to stable convergence")
    axr.set_title("How long did it take? (one dot per converged run; tall tick = p50, "
                  "short tick = p90)", loc="left", fontsize=11, color=INK, pad=10)
    style(axr)
    axr.spines["left"].set_visible(False)

    seeds = chaos["seeds"]
    title(fig, "Chaos simulation: convergence across controller variants",
          f"Same {n_runs} random fault schedules (seeds {seeds[0]}..{seeds[1]}) for every "
          "variant. v2 converges every time; its slow tail is the 20 s progress deadline, "
          "15 s Lease takeover and 30 s relist.",
          tag="SIMULATED (fake API server)")
    save(fig, "chaos_convergence.png")


# --------------------------------------------------------------------------- 2. broken images
def fig_broken_image(chaos):
    fam = chaos["by_fault_family"]["with_broken_image"]
    n = fam["schedules"]
    rows = [("v2", "v2", V2), ("v1", "v1 (old code)", V1)]
    fig, (axl, axr) = plt.subplots(1, 2, figsize=(12, 3.8), sharey=True,
                                   gridspec_kw={"width_ratios": [1, 1.6]})
    fig.subplots_adjust(left=0.13, right=0.97, top=0.66, bottom=0.17, wspace=0.08)

    for y, (key, _, color) in enumerate(rows):
        c = fam[key]["converged"]
        axl.barh(y, c, height=0.5, color=color)
        axl.text(c + n * 0.02, y, f"{c} / {n}", va="center", ha="left", fontsize=11, color=INK)
    axl.set_xlim(0, n * 1.3)
    axl.set_yticks(range(len(rows)))
    axl.set_yticklabels([lbl for _, lbl, _ in rows], fontsize=12)
    axl.invert_yaxis()
    axl.set_ylim(len(rows) - 0.5, -0.6)
    axl.set_xlabel(f"schedules that converged (of {n})")
    axl.set_title("Recovered from a broken image?", loc="left", fontsize=11, color=INK, pad=10)
    style(axl)

    xmax = max(fam[k]["unhealthy_s_max"] for k, _, _ in rows) * 1.08
    for y, (key, _, color) in enumerate(rows):
        p50, mx = fam[key]["unhealthy_s_p50"], fam[key]["unhealthy_s_max"]
        axr.plot([p50, mx], [y, y], color=color, linewidth=9, solid_capstyle="round", alpha=0.35)
        axr.plot([p50], [y], "o", markersize=10, color=SURFACE, markeredgecolor=color,
                 markeredgewidth=2.5, zorder=3)
        axr.plot([mx], [y], "o", markersize=7, color=color, zorder=3)
        axr.text(p50, y - 0.22, f"median {p50:.1f} s", ha="center", va="bottom", fontsize=9.5,
                 color=INK_2)
        axr.text(mx, y + 0.22, f"max {mx:.1f} s", ha="center", va="top", fontsize=9.5,
                 color=INK_2)
    axr.set_xlim(0, xmax)
    axr.set_xlabel("total simulated seconds unhealthy per run (from t=10 s)")
    axr.set_title("How long was it broken?", loc="left", fontsize=11, color=INK, pad=10)
    style(axr)

    title(fig, f"The {n} chaos schedules that push a broken image at some point",
          "v1 keeps re-applying whatever spec.image says. v2 rolls back to the last good image "
          "after ProgressDeadlineExceeded.", tag="SIMULATED (fake API server)")
    save(fig, "broken_image_v1_vs_v2.png")


# --------------------------------------------------------------------------- 3. live v1
def fig_live(live):
    recs = live["records"]
    faults = list(dict.fromkeys(r["fault"] for r in recs))  # keep file order
    fig, ax = plt.subplots(figsize=(12, 4.4))
    fig.subplots_adjust(left=0.17, right=0.97, top=0.72, bottom=0.15)
    for y, f in enumerate(faults):
        xs = [r["recovery_s"] for r in recs if r["fault"] == f]
        ax.scatter(xs, [y] * len(xs), s=60, color=V1, alpha=0.75, edgecolors=SURFACE,
                   linewidths=1.5, zorder=3)
    med = statistics.median(r["recovery_s"] for r in recs)
    mx = live["max_recovery_s"]
    ax.axvline(med, color=INK_2, linewidth=1, zorder=2)
    ax.text(med + mx * 0.01, -0.75, f"median {med:.2f} s", ha="left", va="bottom",
            fontsize=9.5, color=INK_2)
    ax.set_yticks(range(len(faults)))
    ax.set_yticklabels(faults, family="monospace", fontsize=10)
    ax.invert_yaxis()
    ax.set_ylim(len(faults) - 0.5, -0.9)
    ax.set_xlim(0, mx * 1.15)
    ax.set_xlabel("seconds from fault injection until the API objects matched again (wall clock)")
    style(ax)
    title(fig, f"Live k3s run of the v1 controller: {live['recovered_within_30s']}/"
               f"{live['cases_run']} faults repaired, max {g(mx)} s",
          "One dot per injected fault (results-live.json). API-object convergence, not Pod "
          "readiness. v2 has NOT been measured live.", tag="LIVE k3s, v1 code")
    save(fig, "live_k3s_v1.png")


def main():
    chaos = json.loads((ROOT / "results-chaos.json").read_text())
    live = json.loads((ROOT / "results-live.json").read_text())
    fig_convergence(chaos)
    fig_broken_image(chaos)
    fig_live(live)


if __name__ == "__main__":
    main()
