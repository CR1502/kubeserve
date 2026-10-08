"""Shared matplotlib styling for the result charts (static PNGs for the README)."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
# Fixed series identity across every chart: vLLM is always slot 1, baseline slot 2.
VLLM = "#2a78d6"
BASELINE = "#eb6834"
AQUA = "#1baf7a"
CRITICAL = "#d03b3b"  # status colour, only for failed requests (always labelled)

plt.rcParams.update(
    {
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "font.size": 10,
        "font.family": "sans-serif",
        "text.color": TEXT_PRIMARY,
        "axes.labelcolor": TEXT_SECONDARY,
        "axes.titlecolor": TEXT_PRIMARY,
        "axes.titlesize": 11,
        "axes.titleweight": "bold",
        "axes.titlelocation": "left",
        "axes.edgecolor": GRID,
        "axes.linewidth": 1.0,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": GRID,
        "grid.linewidth": 1.0,
        "grid.linestyle": "-",
        "xtick.color": TEXT_SECONDARY,
        "ytick.color": TEXT_SECONDARY,
        "xtick.major.size": 0,
        "ytick.major.size": 0,
        "legend.frameon": False,
        "lines.linewidth": 2.0,
        "lines.solid_capstyle": "round",
        "lines.solid_joinstyle": "round",
    }
)


def line_with_markers(ax, x, y, color, label=None, step=False):
    """2px line + 8px markers with a 2px surface-coloured ring."""
    kwargs = dict(color=color, linewidth=2, label=label, zorder=3)
    if step:
        ax.step(x, y, where="post", **kwargs)
    else:
        ax.plot(x, y, marker="o", markersize=8, markeredgecolor=SURFACE, markeredgewidth=2, **kwargs)


def end_label(ax, x, y, text, dy=0):
    """Direct value label at a line's end, in text ink (never the series colour)."""
    ax.annotate(
        text,
        (x, y),
        xytext=(8, dy),
        textcoords="offset points",
        va="center",
        fontsize=9,
        color=TEXT_PRIMARY,
    )


def save(fig, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
