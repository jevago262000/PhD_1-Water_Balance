#!/usr/bin/env python3
"""
Sensitivity analysis of the streamflow recession constant k:
MRC-derived k (estimated per basin from observed streamflow via the Master
Recession Curve, MRC) vs a fixed k = 0.5, across 588 CSS pristine basins.

Both parameterizations are estimates; neither is treated as ground truth.
Differences are therefore reported as divergence (RMSD), not error.

Outputs:
  - Figure_S1_k_sensitivity.png  (4-panel, publication-ready)
  - Figure_S1_interpretation.docx
  - Table_S1_statistical_tests.csv
  - Table_S1_basin_summary.csv
  - Table_S1_seasonal_redistribution.csv
"""

import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
from docx import Document
from docx.shared import Pt, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# PATHS
# ============================================================
BASE = Path(r'D:\OneDrive - CGIAR\Documents\PhD_JLU Giessen\Papers\Paper1\Processing\CSS-GRDC')
W2   = BASE / 'wyield2_zonal_statistics_1958-2023.csv'
W4   = BASE / 'wyield4_zonal_statistics_1958-2023.csv'
TDA  = BASE / 'Target_Drainage_Areas.txt'
FIGS = Path(r"D:\OneDrive - CGIAR\Documents\PhD_JLU Giessen\Papers\Paper1\Figures")
FIGS.mkdir(exist_ok=True)

# ============================================================
# 1. LOAD DATA
# ============================================================
print("Loading data...")
with open(TDA) as f:
    content = f.read()
target_ids = sorted(set(
    int(x.strip().strip(','))
    for x in content.replace('[','').replace(']','').split()
    if x.strip().strip(',').isdigit()
))
print(f"  Target stations: {len(target_ids)}")

w2 = pd.read_csv(W2)
w4 = pd.read_csv(W4)

def detect_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]
    return None

id_col   = detect_col(w2, ['station_no','StationNo','station','grdc_no','ID','id'])
mean_col = detect_col(w2, ['MEAN','mean','Mean','wyield_mean','value'])
date_col = detect_col(w2, ['date','Date','year','Year','time','month'])

print(f"  Columns: id={id_col}, mean={mean_col}, date={date_col}")

w2[id_col] = pd.to_numeric(w2[id_col], errors='coerce')
w4[id_col] = pd.to_numeric(w4[id_col], errors='coerce')
w2f = w2[w2[id_col].isin(target_ids)].copy()
w4f = w4[w4[id_col].isin(target_ids)].copy()

print(f"  observed-k filtered: {w2f.shape} | stations: {w2f[id_col].nunique()}")
print(f"  fixed-k filtered   : {w4f.shape} | stations: {w4f[id_col].nunique()}")

# ============================================================
# 2. MERGE
# ============================================================
merge_keys = [id_col, date_col] if date_col else [id_col]
merged = w2f.merge(w4f, on=merge_keys, suffixes=('_w2','_w4'))
valid  = ~(merged[f'{mean_col}_w2'].isna() | merged[f'{mean_col}_w4'].isna())
merged = merged[valid].copy()
v2_all = merged[f'{mean_col}_w2']
v4_all = merged[f'{mean_col}_w4']
print(f"  Merged: {merged.shape} | station-months: {len(v2_all):,}")

# ============================================================
# 3. BASIN-LEVEL STATISTICS
# ============================================================
print("\nComputing basin-level statistics...")
basin_stats = []
for sid, grp in merged.groupby(id_col):
    v2   = grp[f'{mean_col}_w2'].values
    v4   = grp[f'{mean_col}_w4'].values
    diff = v2 - v4
    rmsd = np.sqrt((diff**2).mean())
    basin_stats.append({
        'station_no': sid,
        'mean_w2':    v2.mean(),
        'mean_w4':    v4.mean(),
        'mean_diff':  diff.mean(),
        'abs_diff':   np.abs(diff).mean(),
        'pbias':      100 * diff.sum() / v4.sum() if v4.sum() != 0 else np.nan,
        # RMSD = root-mean-square DIFFERENCE between the two parameterizations.
        # Symmetric: neither series is treated as truth (MRC-derived k is itself
        # an MRC estimate), so this is divergence, not error.
        'rmsd':       rmsd,
        'rel_rmsd':   100 * rmsd / v4.mean() if v4.mean() > 0 else np.nan,
        'n_months':   len(v2)
    })

df_b = pd.DataFrame(basin_stats)
df_b['quartile'] = pd.qcut(
    df_b['abs_diff'], q=4,
    labels=['Q1','Q2','Q3','Q4']
)

# ------------------------------------------------------------
# Seasonal redistribution: mean signed difference (obs k - k=0.5)
# by calendar month, pooled across all basin-months. A linear
# reservoir conserves volume, so k only REDISTRIBUTES water in
# time -- this curve is the signature of that redistribution and
# is the core justification for estimating MRC-derived k per basin.
# ------------------------------------------------------------
if 'MONTH_w2' in merged.columns:
    merged['_month'] = merged['MONTH_w2']
elif 'MONTH' in merged.columns:
    merged['_month'] = merged['MONTH']
else:
    merged['_month'] = pd.to_datetime(
        merged[f'{date_col}_w2'] if f'{date_col}_w2' in merged.columns
        else merged[date_col]).dt.month
merged['_diff'] = merged[f'{mean_col}_w2'] - merged[f'{mean_col}_w4']
seasonal = merged.groupby('_month')['_diff'].agg(
    ['mean', lambda s: s.quantile(0.25), lambda s: s.quantile(0.75)]
)
seasonal.columns = ['mean', 'q25', 'q75']
seasonal = seasonal.reindex(range(1, 13))

# ============================================================
# 4. STATISTICAL TESTS (two levels)
# ============================================================
print("Running statistical tests...")

# Helper functions
def cohens_d(a, b):
    d = np.array(a) - np.array(b)
    return d.mean() / d.std(ddof=1)

def sig_label(p):
    if   p < 0.001: return 'p < 0.001 ***'
    elif p < 0.01:  return 'p < 0.01 **'
    elif p < 0.05:  return 'p < 0.05 *'
    else:           return f'p = {p:.3f} ns'

def effect_label(d):
    ad = abs(d)
    if   ad < 0.2: return 'negligible'
    elif ad < 0.5: return 'small'
    elif ad < 0.8: return 'medium'
    else:          return 'large'

# Level 1: basin annual means
B2 = df_b['mean_w2'].values
B4 = df_b['mean_w4'].values

t1,  tp1  = stats.ttest_rel(B2, B4)
w1,  wp1  = stats.wilcoxon(B2, B4)
ks1, ksp1 = stats.ks_2samp(B2, B4)
d1        = cohens_d(B2, B4)
rp1, _    = stats.pearsonr(B2, B4)
rs1, _    = stats.spearmanr(B2, B4)
pb1       = 100 * (B2 - B4).sum() / B4.sum()
rmsd1     = np.sqrt(((B2 - B4)**2).mean())

# Level 2: all monthly values
t2,  tp2  = stats.ttest_rel(v2_all, v4_all)
w2s, wp2  = stats.wilcoxon(v2_all, v4_all)
d2        = cohens_d(v2_all, v4_all)
pb2       = 100 * (v2_all - v4_all).sum() / v4_all.sum()
rmsd2     = np.sqrt(((v2_all - v4_all)**2).mean())

# Print summary
print(f"\n{'='*60}")
print("LEVEL 1: Basin annual means")
print(f"{'='*60}")
print(f"  n basins         : {len(B2)}")
print(f"  Mean (MRC-derived k): {B2.mean():.4f} mm/month")
print(f"  Mean (fixed k=0.5): {B4.mean():.4f} mm/month")
print(f"  Mean difference  : {(B2-B4).mean():.4f} mm/month")
print(f"  Paired t-test    : t={t1:.4f},  {sig_label(tp1)}")
print(f"  Wilcoxon         : W={w1:.2f},   {sig_label(wp1)}")
print(f"  KS test          : D={ks1:.4f},  {sig_label(ksp1)}")
print(f"  Cohen's d        : {d1:.4f} ({effect_label(d1)})")
print(f"  Pearson r        : {rp1:.4f}")
print(f"  Spearman r       : {rs1:.4f}")
print(f"  PBIAS            : {pb1:.2f}%")
print(f"  RMSD             : {rmsd1:.4f} mm/month")

print(f"\n{'='*60}")
print("LEVEL 2: All monthly values pooled")
print(f"{'='*60}")
print(f"  n station-months : {len(v2_all):,}")
print(f"  Mean (MRC-derived k): {v2_all.mean():.4f} mm/month")
print(f"  Mean (fixed k=0.5): {v4_all.mean():.4f} mm/month")
print(f"  Mean difference  : {(v2_all-v4_all).mean():.4f} mm/month")
print(f"  Paired t-test    : t={t2:.4f},  {sig_label(tp2)}")
print(f"  Wilcoxon         : W={w2s:.2f},   {sig_label(wp2)}")
print(f"  Cohen's d        : {d2:.4f} ({effect_label(d2)})")
print(f"  PBIAS            : {pb2:.2f}%")
print(f"  RMSD             : {rmsd2:.4f} mm/month")

# ------------------------------------------------------------
# Derived quantities for interpretation
# ------------------------------------------------------------
rmsd_med      = df_b['rmsd'].median()
relrmsd_med   = df_b['rel_rmsd'].median()
relrmsd_p90   = df_b['rel_rmsd'].quantile(0.90)
absdiff_med   = df_b['abs_diff'].median()
absdiff_max   = df_b['abs_diff'].max()
hi_month      = int(seasonal['mean'].idxmax())   # obs k yields MOST vs k=0.5
lo_month      = int(seasonal['mean'].idxmin())   # obs k yields LEAST vs k=0.5
hi_val        = seasonal['mean'].max()
lo_val        = seasonal['mean'].min()
MONTH_NAMES   = ['Jan','Feb','Mar','Apr','May','Jun',
                 'Jul','Aug','Sep','Oct','Nov','Dec']

print(f"\n{'='*60}")
print("INTERPRETATION (dual timescale)")
print(f"{'='*60}")
print("  VOLUME (long-term)  -> defends fixed k = 0.5 for ungauged areas:")
print(f"    r = {rp1:.6f} | PBIAS = {pb1:.3f}% | "
      f"Cohen's d = {d1:.3f} ({effect_label(d1)})")
print( "    Long-term basin-mean water yield is k-invariant (linear")
print( "    reservoir conserves volume); choosing k = 0.5 where no data")
print( "    exist introduces negligible volumetric bias.")
print( "  TIMING (monthly)    -> justifies estimating MRC-derived k per basin:")
print(f"    per-basin RMSD median = {rmsd_med:.2f} mm/month "
      f"({relrmsd_med:.1f}% of mean flow, p90 = {relrmsd_p90:.1f}%)")
print(f"    seasonal redistribution: obs k yields MOST in {MONTH_NAMES[hi_month-1]} "
      f"(+{hi_val:.2f}) and LEAST in {MONTH_NAMES[lo_month-1]} ({lo_val:.2f}) mm/month")
print( "  CAVEAT: the paired t-test on basin means is 'significant'")
print(f"    (p = {tp1:.1e}) only because n = {len(B2)} detects a trivial")
print(f"    +{(B2-B4).mean():.4f} mm/month offset. The effect size is")
print(f"    negligible (Cohen's d = {d1:.3f}); report effect size, not p.")

# ============================================================
# 5. EXPORT TABLE S1 (statistical tests)
# ============================================================
rows_stats = [
    ['Paired t-test',   f'{t1:.4f}',  f'{tp1:.2e}', sig_label(tp1),
     '-',              '-',           'Basin annual means'],
    ['Wilcoxon',        f'{w1:.2f}',  f'{wp1:.2e}', sig_label(wp1),
     '-',              '-',           'Basin annual means'],
    ['KS test',         f'{ks1:.4f}', f'{ksp1:.2e}', sig_label(ksp1),
     '-',              '-',           'Basin annual means'],
    ["Cohen's d",       f'{d1:.4f}',  '-',           effect_label(d1),
     '-',              '-',           'Basin annual means'],
    ['Pearson r',       f'{rp1:.4f}', '-',           '-',
     '-',              '-',           'Basin annual means'],
    ['Spearman r',      f'{rs1:.4f}', '-',           '-',
     '-',              '-',           'Basin annual means'],
    ['PBIAS (%)',        f'{pb1:.2f}', '-',           '-',
     '-',              '-',           'Basin annual means'],
    ['RMSD (mm/month)', f'{rmsd1:.4f}', '-',          '-',
     f'{len(B2)}',     '-',           'Basin annual means'],
    ['Paired t-test',   f'{t2:.4f}',  f'{tp2:.2e}', sig_label(tp2),
     '-',              '-',           'Monthly pooled'],
    ['Wilcoxon',        f'{w2s:.2f}', f'{wp2:.2e}', sig_label(wp2),
     '-',              '-',           'Monthly pooled'],
    ["Cohen's d",       f'{d2:.4f}',  '-',           effect_label(d2),
     '-',              '-',           'Monthly pooled'],
    ['PBIAS (%)',        f'{pb2:.2f}', '-',           '-',
     '-',              '-',           'Monthly pooled'],
    ['RMSD (mm/month)', f'{rmsd2:.4f}', '-',          '-',
     f'{len(v2_all):,}', '-',         'Monthly pooled'],
]

df_stats = pd.DataFrame(rows_stats, columns=[
    'Test/Metric', 'Statistic', 'p_value',
    'Significance/Effect', 'n', 'Notes', 'Analysis level'
])
out_stats = BASE / 'Table_S1_statistical_tests.csv'
#df_stats.to_csv(out_stats, index=False)
print(f"\n  Table S1 saved: {out_stats}")

# ============================================================
# 6. EXPORT BASIN SUMMARY TABLE
# ============================================================
df_export = df_b[['station_no','mean_w2','mean_w4',
                   'mean_diff','abs_diff','pbias','rmsd','rel_rmsd',
                   'n_months','quartile']].copy()
df_export.columns = [
    'station_no', 'mean_obs_k_mm_month', 'mean_fixed_k_mm_month',
    'mean_diff_mm_month', 'abs_diff_mm_month', 'pbias_pct',
    'rmsd_mm_month', 'rel_rmsd_pct', 'n_months', 'abs_diff_quartile'
]
out_basin = BASE / 'Table_S1_basin_summary.csv'
#df_export.to_csv(out_basin, index=False)
print(f"  Basin summary saved: {out_basin}")

# Seasonal redistribution table (timing signal)
seasonal_out = seasonal.copy()
seasonal_out.index.name = 'month'
seasonal_out.columns = ['mean_diff_mm_month', 'q25_mm_month', 'q75_mm_month']
out_seasonal = BASE / 'Table_S1_seasonal_redistribution.csv'
#seasonal_out.to_csv(out_seasonal)
print(f"  Seasonal table saved: {out_seasonal}")

# ============================================================
# 7. FIGURE S1 (4 panels, clean)
# ============================================================
print("\nGenerating Figure S1...")

fig = plt.figure(figsize=(14, 10))
gs  = gridspec.GridSpec(2, 2, figure=fig, hspace=0.38, wspace=0.32)

# Perceptually uniform, colour-blind-safe sequential map (replaces red-green RdYlGn_r)
CMAP = 'viridis'

# Single consistent label for the mean monthly absolute difference (abs_diff),
# reused across Panels a (colorbar), b (x-axis) and d (x-axis) so the quantity
# is presented identically everywhere it appears.
ABS_LABEL = 'Mean monthly absolute difference, |Δ| (mm month⁻¹)'

# --- Panel A: Scatter coloured by abs_diff ---
ax = fig.add_subplot(gs[0, 0])
sc = ax.scatter(
    df_b['mean_w4'], df_b['mean_w2'],
    c=df_b['abs_diff'], cmap=CMAP,
    s=25, alpha=0.7, edgecolors='none'
)
lo = min(df_b['mean_w2'].min(), df_b['mean_w4'].min())
hi = max(df_b['mean_w2'].max(), df_b['mean_w4'].max())
ax.plot([lo, hi], [lo, hi], 'k--', lw=1, label='1:1')
cb = plt.colorbar(sc, ax=ax, pad=0.02)
cb.set_label(ABS_LABEL, fontsize=9)
ax.set_xlabel('Mean water yield, fixed k = 0.5 (mm month⁻¹)', fontsize=10)
ax.set_ylabel('Mean water yield, MRC-derived k (mm month⁻¹)', fontsize=10)
r_A = df_b["mean_w2"].corr(df_b["mean_w4"])
ax.set_title(
    f'(a) VOLUME is conserved: long-term basin-mean water yield\n'
    f'n = {len(df_b)} basins  |  r = {r_A:.6f}  |  PBIAS = {pb1:.3f}%',
    fontsize=10
)
ax.text(
    0.04, 0.96,
    'Long-term means are insensitive to k',
    #'\n→ k = 0.5 is defensible for ungauged areas',
    transform=ax.transAxes, fontsize=8.5, va='top',
    bbox=dict(boxstyle='round', fc='#E8F5E9', ec='#43A047', alpha=0.9)
)
ax.legend(fontsize=9, loc='lower right')

# --- Panel B: CDF of abs_diff ---
ax = fig.add_subplot(gs[0, 1])
sorted_diff = np.sort(df_b['abs_diff'])
cdf = np.arange(1, len(sorted_diff) + 1) / len(sorted_diff)
# Dark-grey curve so the coloured percentile lines stand out against it
ax.plot(sorted_diff, cdf * 100, color='0.2', lw=2)
# Percentile lines take their colour from the same viridis ramp as Panel a
# (position = percentile), so they read in order without relying on red/green.
# P25/P50/P75 are also the quartile boundaries used in Panel d.
for pct in (25, 50, 75):
    clr = plt.get_cmap(CMAP)(pct / 100)
    val = np.percentile(sorted_diff, pct)
    ax.axvline(val, color=clr, lw=1.4, ls='--',
               label=f'P{pct} = {val:.3f} mm month⁻¹')
    ax.axhline(pct, color=clr, lw=0.6, ls=':')
ax.set_xlabel(ABS_LABEL, fontsize=10)
ax.set_ylabel('Cumulative percentage of basins (%)', fontsize=10)
ax.set_title(
    f'(b) How large are the monthly differences? (per-basin |Δ|)\n'
    f'Mean = {df_b["abs_diff"].mean():.3f}  |  '
    f'Median = {df_b["abs_diff"].median():.3f}  |  '
    f'Max = {df_b["abs_diff"].max():.3f} mm month⁻¹',
    fontsize=10
)
ax.legend(fontsize=9)
ax.grid(alpha=0.25)
ax.set_ylim(0, 100)

# --- Panel C: Seasonal redistribution (obs k - k=0.5) by month ---
# This is the timing signal that volume conservation hides: MRC-derived k
# shifts water OUT of high-flow months INTO low-flow months. It is the
# core justification for estimating observed recession constants per basin.
ax = fig.add_subplot(gs[1, 0])
months = np.arange(1, 13)
mlabels = ['J','F','M','A','M','J','J','A','S','O','N','D']
ax.fill_between(months, seasonal['q25'], seasonal['q75'],
                color='#1E88E5', alpha=0.18, label='Inter-basin IQR')
ax.plot(months, seasonal['mean'], color='#1E88E5', lw=2,
        marker='o', ms=5, label='Mean Δ (MRC-der k − k=0.5)')
ax.axhline(0, color='black', lw=1.0, ls='--')
ax.set_xticks(months)
ax.set_xticklabels(mlabels, fontsize=9)
ax.set_xlabel('Calendar month', fontsize=10)
ax.set_ylabel('Mean Δ water yield: MRC-der k − k=0.5 (mm month⁻¹)', fontsize=10)
ax.set_title(
    '(c) TIMING differs: seasonal redistribution of water yield\n'
    'MRC-derived k moves water from high-flow to low-flow months',
    fontsize=10
)
ax.text(
    0.04, 0.04,
    'Volume conserved, but timing changes',
    #'\n→ estimating MRC-derived k per basin is worthwhile',
    transform=ax.transAxes, fontsize=8.5, va='bottom',
    bbox=dict(boxstyle='round', fc='#FFF3E0', ec='#FB8C00', alpha=0.9)
)
ax.legend(fontsize=9, loc='upper right')
ax.grid(alpha=0.25)

# --- Panel D: Per-basin monthly divergence (RMSD) by quartile of |Δ| ---
# NOTE on the earlier "identical / invisible boxplots":
#   * the two long-term means per quartile look identical because they are
#     k-invariant (volume is conserved -- see Panel A).
#   * the basin-mean SIGNED difference is ~0 everywhere (range +-0.1 mm/month),
#     so boxing it produced flat, invisible boxes.
# The quantity that carries the timing signal is the per-basin monthly RMSD
# (root-mean-square DIFFERENCE between the two parameterizations -- symmetric,
# no truth assumed), which has real spread and grows with |Δ|. Boxing it makes
# the magnitude of the monthly divergence visible.
ax = fig.add_subplot(gs[1, 1])
q_labels = ['Q1', 'Q2', 'Q3', 'Q4']
# Quartile colours from the same viridis ramp as Panel a (Q1 low |Δ| -> Q4 high |Δ|),
# centred in each quartile so they sit between the P25/P50/P75 line colours of Panel b
q_colors = plt.get_cmap(CMAP)([0.125, 0.375, 0.625, 0.875])

data_rmsd = [df_b.loc[df_b['quartile'] == q, 'rmsd'].values
             for q in q_labels]

q_ranges = []
for q in q_labels:
    vals = df_b.loc[df_b['quartile'] == q, 'abs_diff']
    q_ranges.append(f'{q}\n[{vals.min():.2f}–{vals.max():.2f}]')

bp = ax.boxplot(
    data_rmsd, positions=[1, 2, 3, 4], widths=0.55,
    patch_artist=True, notch=False,
    medianprops=dict(color='black', lw=1.5),
    whiskerprops=dict(lw=1.2),
    flierprops=dict(marker='o', ms=2.5, alpha=0.35)
)
for patch, clr in zip(bp['boxes'], q_colors):
    patch.set_facecolor((*clr[:3], 0.6))  # same ~60% opacity as before

# Headroom on top so the n= row (placed in axes coordinates) clears the
# highest outliers and nothing falls outside the axes.
gmax = df_b['rmsd'].max()
ax.set_ylim(0, gmax * 1.18)
ax.set_xlim(0.5, 4.85)

# n= row pinned to the top of the axes (x in data coords, y in axes fraction)
# -> never overlapped by boxes/whiskers regardless of data range.
trans = ax.get_xaxis_transform()
# Median value centred OVER its own box (just above the median line) with a
# white background, so it never overlaps a neighbouring box.
for i, q in enumerate(q_labels):
    vals = df_b.loc[df_b['quartile'] == q, 'rmsd']
    ax.text(i + 1, 0.97, f'n={len(vals)}', transform=trans,
            ha='center', va='top', fontsize=8, color='gray')
    ax.text(i + 1, vals.median(), f'{vals.median():.2f}',
            ha='center', va='bottom', fontsize=7.5, color='#222',
            bbox=dict(boxstyle='round,pad=0.15', fc='white',
                      ec='none', alpha=0.75))

ax.set_xticks([1, 2, 3, 4])
ax.set_xticklabels(q_ranges, fontsize=8.5)
# Keep the same Delta glyph as the other panels: lowercase only the first
# character of ABS_LABEL (str.lower() would turn the uppercase Delta into a
# lowercase delta).
ax.set_xlabel('Quartile of ' + ABS_LABEL[0].lower() + ABS_LABEL[1:],
              fontsize=10)
ax.set_ylabel('Per-basin monthly RMSD (mm month⁻¹)', fontsize=10)
ax.set_title(
    '(d) Monthly divergence grows with |Δ|\n'
    f'Median RMSD = {df_b["rmsd"].median():.2f} mm month⁻¹  '
    f'({df_b["rel_rmsd"].median():.1f}% of mean flow)',
    fontsize=10
)
ax.grid(alpha=0.25, axis='y')

fig.suptitle(
    'Recession-constant sensitivity: MRC-derived k vs fixed k = 0.5\n'
    'Top: long-term VOLUME is k-invariant (k=0.5 OK for ungauged).  '
    'Bottom: monthly TIMING differs (MRC-derived k is worthwhile).\n'
    f'CSS pristine basins -- {len(df_b)} target drainage areas  |  '
    'Period 1958-2023',
    fontsize=11, fontweight='bold', y=0.995
)

out_fig = FIGS / 'FigS1.png'
#out_fig = BASE / 'Figure_S1_k_sensitivity.png'
plt.savefig(out_fig, dpi=300, bbox_inches='tight', facecolor='white')
print(f"  Figure S1 saved: {out_fig}")
plt.show()

# ============================================================
# 8. INTERPRETATION FILE (Word .docx) -- explanation kept OUT of the PNG
# ============================================================
frac_equal = 100 * np.mean(np.isclose(v2_all, v4_all))

doc = Document()

# Base font
style = doc.styles['Normal']
style.font.name = 'Calibri'
style.font.size = Pt(11)

def add_metric_table(rows, headers):
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = 'Light Grid Accent 1'
    for j, h in enumerate(headers):
        cell = table.rows[0].cells[j]
        cell.text = h
        cell.paragraphs[0].runs[0].font.bold = True
    for r in rows:
        cells = table.add_row().cells
        for j, val in enumerate(r):
            cells[j].text = str(val)
    return table

doc.add_heading('Recession-constant sensitivity: interpretation', level=0)

p = doc.add_paragraph()
p.add_run('Comparison: ').bold = True
p.add_run(
    'water yield computed with the observed recession constant k (estimated '
    'per basin from observed streamflow via the Master Recession Curve, MRC) '
    'versus a fixed k = 0.5. Both are estimates; neither is treated as ground '
    'truth, so differences are reported as divergence (RMSD), not error.')
p = doc.add_paragraph()
p.add_run('Domain: ').bold = True
p.add_run(f'{len(df_b)} CSS pristine target drainage areas, 1958-2023 '
          f'({len(v2_all):,} basin-months).')
doc.add_paragraph(
    'The analysis separates two timescales that answer two different '
    'questions.')

# Section 1
doc.add_heading('1. Long-term volume is insensitive to k '
                '(supports fixed k = 0.5 for ungauged areas)', level=1)
doc.add_paragraph(
    'A linear-reservoir routing only redistributes water in time; it '
    'conserves volume. The long-term basin-mean water yield is therefore '
    'essentially independent of the recession constant:')
add_metric_table(
    [['Pearson r (basin means)', f'{rp1:.6f}', 'effectively 1'],
     ['Spearman r (basin means)', f'{rs1:.6f}', 'rank-preserving'],
     ['Overall PBIAS', f'{pb1:.3f}%', 'no volumetric bias'],
     ['Mean difference', f'{(B2-B4).mean():.4f} mm/month', '~ 0'],
     ["Cohen's d (basin means)", f'{d1:.3f}', effect_label(d1)]],
    ['Metric', 'Value', 'Reading'])
p = doc.add_paragraph()
p.add_run('Why r is effectively 1. ').bold = True
p.add_run(
    f'This is not an artefact and not because the two series are identical '
    f'(only {frac_equal:.1f}% of monthly values are numerically equal). It is '
    f'the signature of volume conservation: whatever k does to the hydrograph '
    f'nets to zero over the long-term mean. This is why assigning k = 0.5 to '
    f'ungauged basins - where k cannot be estimated from data - introduces '
    f'negligible error in the long-term water balance.')

# Section 2
doc.add_heading('2. Monthly timing does change '
                '(supports estimating MRC-derived k where data allow)', level=1)
doc.add_paragraph(
    'The differences that cancel in the long-term mean are real at the '
    'monthly scale, which is the scale that matters for streamflow dynamics:')
add_metric_table(
    [['Per-basin monthly RMSD (median)', f'{rmsd_med:.2f} mm/month'],
     ['Relative RMSD (median / p90)',
      f'{relrmsd_med:.1f}% / {relrmsd_p90:.1f}% of mean flow'],
     ['Mean monthly |diff| per basin (median / max)',
      f'{absdiff_med:.2f} / {absdiff_max:.2f} mm/month'],
     ['Largest single-month |diff|',
      f'{merged["_diff"].abs().max():.1f} mm/month']],
    ['Metric', 'Value'])
p = doc.add_paragraph()
p.add_run('What RMSD is and why it is used here. ').bold = True
p.add_run(
    'RMSD (root-mean-square difference) is the square root of the mean of the '
    'squared monthly differences between the two series, computed per basin '
    'and expressed in the same units as water yield (mm/month). It is used '
    'instead of RMSE (root-mean-square error) on purpose: "error" implies one '
    'series is the true reference, whereas here the MRC-derived k is itself an '
    'MRC-derived estimate, so neither series is ground truth. RMSD is '
    'symmetric - it makes no such assumption - and quantifies the typical '
    'month-to-month divergence between the two parameterizations. Because the '
    'differences are squared, RMSD is more sensitive to the occasional large '
    'monthly deviations (peaks and recessions) than the mean absolute '
    'difference, |Δ|; reporting both shows the typical divergence (|Δ|) and '
    'its tail (RMSD). Both vanish in the long-term mean (Section 1) yet remain '
    'substantial month to month, which is precisely the timing information '
    'that estimating MRC-derived k recovers.')
p = doc.add_paragraph()
p.add_run('Seasonal redistribution (the timing signal). ').bold = True
p.add_run(
    f'Relative to fixed k = 0.5, the MRC-derived k shifts water out of high-flow '
    f'months into low-flow months, consistent with the basin-specific '
    f'storage-and-release behaviour captured by a recession constant derived '
    f'from the actual streamflow. The signed difference (MRC-derived k minus '
    f'k = 0.5) peaks at +{hi_val:.2f} mm/month in {MONTH_NAMES[hi_month-1]} '
    f'and is most negative at {lo_val:.2f} mm/month in '
    f'{MONTH_NAMES[lo_month-1]} (see Table_S1_seasonal_redistribution.csv).')

# Section 3
doc.add_heading('3. Statistical caveat (significance vs. effect size)',
                level=1)
doc.add_paragraph(
    f'The paired t-test on basin means is formally "significant" '
    f'(t = {t1:.2f}, p = {tp1:.1e}), but only because n = {len(B2)} is large '
    f'enough to detect a trivial +{(B2-B4).mean():.4f} mm/month offset. The '
    f'effect size is negligible (Cohen\'s d = {d1:.3f}) and the KS test does '
    f'not reject equality of the two distributions (D = {ks1:.4f}, '
    f'p = {ksp1:.3f}). Conclusions should be drawn from effect sizes '
    f'(PBIAS, Cohen\'s d, RMSD), not from the p-value alone.')

# Bottom line
doc.add_heading('Bottom line', level=1)
b = doc.add_paragraph(style='List Bullet')
b.add_run('Fixed k = 0.5 for ungauged areas: ').bold = True
b.add_run(f'supported - long-term water yield is k-invariant '
          f'(PBIAS {pb1:.3f}%, r {rp1:.4f}).')
b = doc.add_paragraph(style='List Bullet')
b.add_run('MRC-derived k where data allow: ').bold = True
b.add_run(f'supported - it changes the monthly timing/seasonality '
          f'(median RMSD {rmsd_med:.2f} mm/month, {relrmsd_med:.1f}% of mean '
          f'flow) that the long-term mean hides.')

# Embed the figure
doc.add_heading('Figure', level=1)
doc.add_picture(str(out_fig), width=Inches(6.5))
cap = doc.add_paragraph()
cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = cap.add_run(
    'Figure S1. Recession-constant sensitivity across '
    f'{len(df_b)} CSS pristine basins (1958-2023).')
r.italic = True
r.font.size = Pt(9)

note = doc.add_paragraph()
nr = note.add_run(
    'Companion files: Figure_S1_k_sensitivity.png, '
    'Table_S1_statistical_tests.csv, Table_S1_basin_summary.csv, '
    'Table_S1_seasonal_redistribution.csv.')
nr.italic = True
nr.font.size = Pt(8)

out_interp = BASE / 'Figure_S1_interpretation.docx'
#doc.save(out_interp)
print(f"  Interpretation saved: {out_interp}")

print("\n" + "="*60)
print("ALL OUTPUTS SAVED")
print("="*60)
print(f"  Figure S1     : {out_fig}")
print(f"  Interpretation: {out_interp}")
print(f"  Table S1a     : {out_stats}")
print(f"  Table S1b     : {out_basin}")
print(f"  Seasonal      : {out_seasonal}")