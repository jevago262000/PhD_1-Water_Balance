#!/usr/bin/env python3
"""
Sensitivity analysis: Observed recession constants (wyield2)
vs Fixed k=0.5 (wyield4) across 588 CSS pristine basins.

Outputs:
  - Figure_S1_k_sensitivity.png  (4-panel, publication-ready)
  - Table_S1_statistical_tests.csv
  - Table_S1_basin_summary.csv
"""

import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# PATHS
# ============================================================
BASE = Path(r'D:\OneDrive - CGIAR\Documents\PhD_JLU Giessen\Papers\Paper1\Processing\CSS-GRDC')
W2   = BASE / 'wyield2_zonal_statistics_1958-2023.csv'
W4   = BASE / 'wyield4_zonal_statistics_1958-2023.csv'
TDA  = BASE / 'Target_Drainage_Areas.txt'
#FIGS = BASE / 'Figures'
#FIGS.mkdir(exist_ok=True)

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

print(f"  wyield2 filtered: {w2f.shape} | stations: {w2f[id_col].nunique()}")
print(f"  wyield4 filtered: {w4f.shape} | stations: {w4f[id_col].nunique()}")

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
    basin_stats.append({
        'station_no': sid,
        'mean_w2':    v2.mean(),
        'mean_w4':    v4.mean(),
        'mean_diff':  diff.mean(),
        'abs_diff':   np.abs(diff).mean(),
        'pbias':      100 * diff.sum() / v4.sum() if v4.sum() != 0 else np.nan,
        'rmse':       np.sqrt((diff**2).mean()),
        'n_months':   len(v2)
    })

df_b = pd.DataFrame(basin_stats)
df_b['quartile'] = pd.qcut(
    df_b['abs_diff'], q=4,
    labels=['Q1','Q2','Q3','Q4']
)

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
rm1       = np.sqrt(((B2 - B4)**2).mean())

# Level 2: all monthly values
t2,  tp2  = stats.ttest_rel(v2_all, v4_all)
w2s, wp2  = stats.wilcoxon(v2_all, v4_all)
d2        = cohens_d(v2_all, v4_all)
pb2       = 100 * (v2_all - v4_all).sum() / v4_all.sum()
rm2       = np.sqrt(((v2_all - v4_all)**2).mean())

# Print summary
print(f"\n{'='*60}")
print("LEVEL 1: Basin annual means")
print(f"{'='*60}")
print(f"  n basins         : {len(B2)}")
print(f"  Mean wyield2     : {B2.mean():.4f} mm/month")
print(f"  Mean wyield4     : {B4.mean():.4f} mm/month")
print(f"  Mean difference  : {(B2-B4).mean():.4f} mm/month")
print(f"  Paired t-test    : t={t1:.4f},  {sig_label(tp1)}")
print(f"  Wilcoxon         : W={w1:.2f},   {sig_label(wp1)}")
print(f"  KS test          : D={ks1:.4f},  {sig_label(ksp1)}")
print(f"  Cohen's d        : {d1:.4f} ({effect_label(d1)})")
print(f"  Pearson r        : {rp1:.4f}")
print(f"  Spearman r       : {rs1:.4f}")
print(f"  PBIAS            : {pb1:.2f}%")
print(f"  RMSE             : {rm1:.4f} mm/month")

print(f"\n{'='*60}")
print("LEVEL 2: All monthly values pooled")
print(f"{'='*60}")
print(f"  n station-months : {len(v2_all):,}")
print(f"  Mean wyield2     : {v2_all.mean():.4f} mm/month")
print(f"  Mean wyield4     : {v4_all.mean():.4f} mm/month")
print(f"  Mean difference  : {(v2_all-v4_all).mean():.4f} mm/month")
print(f"  Paired t-test    : t={t2:.4f},  {sig_label(tp2)}")
print(f"  Wilcoxon         : W={w2s:.2f},   {sig_label(wp2)}")
print(f"  Cohen's d        : {d2:.4f} ({effect_label(d2)})")
print(f"  PBIAS            : {pb2:.2f}%")
print(f"  RMSE             : {rm2:.4f} mm/month")

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
    ['RMSE (mm/month)', f'{rm1:.4f}', '-',           '-',
     f'{len(B2)}',     '-',           'Basin annual means'],
    ['Paired t-test',   f'{t2:.4f}',  f'{tp2:.2e}', sig_label(tp2),
     '-',              '-',           'Monthly pooled'],
    ['Wilcoxon',        f'{w2s:.2f}', f'{wp2:.2e}', sig_label(wp2),
     '-',              '-',           'Monthly pooled'],
    ["Cohen's d",       f'{d2:.4f}',  '-',           effect_label(d2),
     '-',              '-',           'Monthly pooled'],
    ['PBIAS (%)',        f'{pb2:.2f}', '-',           '-',
     '-',              '-',           'Monthly pooled'],
    ['RMSE (mm/month)', f'{rm2:.4f}', '-',           '-',
     f'{len(v2_all):,}', '-',         'Monthly pooled'],
]

df_stats = pd.DataFrame(rows_stats, columns=[
    'Test/Metric', 'Statistic', 'p_value',
    'Significance/Effect', 'n', 'Notes', 'Analysis level'
])
out_stats = BASE / 'Table_S1_statistical_tests.csv'
df_stats.to_csv(out_stats, index=False)
print(f"\n  Table S1 saved: {out_stats}")

# ============================================================
# 6. EXPORT BASIN SUMMARY TABLE
# ============================================================
df_export = df_b[['station_no','mean_w2','mean_w4',
                   'mean_diff','abs_diff','pbias','rmse',
                   'n_months','quartile']].copy()
df_export.columns = [
    'station_no', 'mean_wyield2_mm_month', 'mean_wyield4_mm_month',
    'mean_diff_mm_month', 'abs_diff_mm_month', 'pbias_pct',
    'rmse_mm_month', 'n_months', 'abs_diff_quartile'
]
out_basin = BASE / 'Table_S1_basin_summary.csv'
df_export.to_csv(out_basin, index=False)
print(f"  Basin summary saved: {out_basin}")

# ============================================================
# 7. FIGURE S1 (4 panels, clean)
# ============================================================
print("\nGenerating Figure S1...")

fig = plt.figure(figsize=(14, 10))
gs  = gridspec.GridSpec(2, 2, figure=fig, hspace=0.38, wspace=0.32)

CMAP = 'RdYlGn_r'

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
cb.set_label('Mean |observed k - fixed k = 0.5| (mm/month)', fontsize=9)
ax.set_xlabel('Mean water yield, fixed k = 0.5 (mm/month)', fontsize=10)
ax.set_ylabel('Mean water yield, observed k (mm/month)', fontsize=10)
ax.set_title(
    f'(A) Basin-mean water yield: observed k vs fixed k = 0.5\n'
    f'n = {len(df_b)} basins  |  Pearson r = '
    f'{df_b["mean_w2"].corr(df_b["mean_w4"]):.3f}  |  '
    f'PBIAS = {pb1:.2f}%',
    fontsize=10
)
ax.legend(fontsize=9)

# --- Panel B: CDF of abs_diff ---
ax = fig.add_subplot(gs[0, 1])
sorted_diff = np.sort(df_b['abs_diff'])
cdf = np.arange(1, len(sorted_diff) + 1) / len(sorted_diff)
ax.plot(sorted_diff, cdf * 100, color='#1E88E5', lw=2)
for pct, clr in [(25, '#43A047'), (50, '#FF9800'), (75, '#E53935')]:
    val = np.percentile(sorted_diff, pct)
    ax.axvline(val, color=clr, lw=1.2, ls='--',
               label=f'P{pct} = {val:.3f} mm/month')
    ax.axhline(pct, color=clr, lw=0.5, ls=':')
ax.set_xlabel(
    'Mean |observed k - fixed k = 0.5| per basin (mm/month)', fontsize=10)
ax.set_ylabel('Cumulative percentage of basins (%)', fontsize=10)
ax.set_title(
    f'(B) Cumulative distribution of per-basin absolute differences\n'
    f'Mean = {df_b["abs_diff"].mean():.3f}  |  '
    f'Median = {df_b["abs_diff"].median():.3f}  |  '
    f'Max = {df_b["abs_diff"].max():.3f} mm/month',
    fontsize=10
)
ax.legend(fontsize=9)
ax.grid(alpha=0.25)
ax.set_ylim(0, 100)

# --- Panel C: PBIAS vs mean water yield ---
ax = fig.add_subplot(gs[1, 0])
sc2 = ax.scatter(
    df_b['mean_w4'], df_b['pbias'],
    c=df_b['abs_diff'], cmap=CMAP,
    s=20, alpha=0.65, edgecolors='none'
)
ax.axhline(0,  color='black', lw=1.0, ls='--')
ax.axhline(5,  color='gray',  lw=0.8, ls=':')
ax.axhline(-5, color='gray',  lw=0.8, ls=':')
cb2 = plt.colorbar(sc2, ax=ax, pad=0.02)
cb2.set_label('Mean |observed k - fixed k = 0.5| (mm/month)', fontsize=9)
ax.set_xlabel(
    'Mean water yield, fixed k = 0.5 (mm/month)', fontsize=10)
ax.set_ylabel(
    'Percent bias, observed k vs fixed k = 0.5 (%)', fontsize=10)
ax.set_title(
    f'(C) Per-basin percent bias by water yield magnitude\n'
    f'Overall PBIAS = {pb1:.2f}%  |  '
    f'RMSE = {rm1:.3f} mm/month  |  '
    f'n = {len(df_b)} basins',
    fontsize=10
)

# --- Panel D: Box plots by quartile ---
ax = fig.add_subplot(gs[1, 1])
q_labels     = ['Q1','Q2','Q3','Q4']
positions_w2 = [1, 4, 7, 10]
positions_w4 = [2, 5, 8, 11]
tick_pos     = [1.5, 4.5, 7.5, 10.5]

data_w2 = [df_b.loc[df_b['quartile'] == q, 'mean_w2'].values
           for q in q_labels]
data_w4 = [df_b.loc[df_b['quartile'] == q, 'mean_w4'].values
           for q in q_labels]

# Range labels for x-axis
q_ranges = []
for q in q_labels:
    vals = df_b.loc[df_b['quartile'] == q, 'abs_diff']
    q_ranges.append(
        f'{q}\n[{vals.min():.2f}-{vals.max():.2f}]'
    )

bp2 = ax.boxplot(
    data_w2, positions=positions_w2, widths=0.7,
    patch_artist=True, notch=False,
    medianprops=dict(color='black', lw=1.5),
    whiskerprops=dict(lw=1.2),
    flierprops=dict(marker='o', ms=3, alpha=0.4)
)
bp4 = ax.boxplot(
    data_w4, positions=positions_w4, widths=0.7,
    patch_artist=True, notch=False,
    medianprops=dict(color='black', lw=1.5),
    whiskerprops=dict(lw=1.2),
    flierprops=dict(marker='o', ms=3, alpha=0.4)
)
for patch in bp2['boxes']:
    patch.set_facecolor('#1E88E599')
for patch in bp4['boxes']:
    patch.set_facecolor('#FF980099')

# Sample size per quartile
ymin = ax.get_ylim()[0]
for i, q in enumerate(q_labels):
    n = (df_b['quartile'] == q).sum()
    ax.text(tick_pos[i], ymin, f'n={n}',
            ha='center', fontsize=8, color='gray')

ax.set_xticks(tick_pos)
ax.set_xticklabels(q_ranges, fontsize=8.5)
ax.set_xlabel(
    'Quartile of mean |observed k - fixed k = 0.5| (mm/month)', fontsize=10)
ax.set_ylabel('Mean water yield (mm/month)', fontsize=10)
ax.set_title(
    '(D) Water yield distributions by quartile of absolute difference\n'
    '(Q1 = smallest difference, Q4 = largest difference)',
    fontsize=10
)
ax.legend(
    handles=[
        Patch(facecolor='#1E88E599', label='Observed k'),
        Patch(facecolor='#FF980099', label='Fixed k = 0.5')
    ],
    fontsize=9, loc='upper left'
)

fig.suptitle(
    'Sensitivity analysis: water yield under observed recession constants'
    ' vs fixed k = 0.5\n'
    f'CSS pristine basins -- {len(df_b)} target drainage areas  |  '
    'Period 1958-2023',
    fontsize=12, fontweight='bold', y=0.99
)

#out_fig = FIGS / 'Figure_S1_k_sensitivity.png'
out_fig = BASE / 'Figure_S1_k_sensitivity.png'
plt.savefig(out_fig, dpi=300, bbox_inches='tight', facecolor='white')
print(f"  Figure S1 saved: {out_fig}")
plt.show()

print("\n" + "="*60)
print("ALL OUTPUTS SAVED")
print("="*60)
print(f"  Figure S1  : {out_fig}")
print(f"  Table S1a  : {out_stats}")
print(f"  Table S1b  : {out_basin}")