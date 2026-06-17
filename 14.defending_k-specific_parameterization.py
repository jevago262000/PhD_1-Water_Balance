#!/usr/bin/env python3
"""
Extended Analysis: Defending k-specific parameterization
while justifying k=0.5 for ungauged areas.

Strategy:
  1. Identify basins where observed k differs most from 0.5
  2. Show that in those basins, wyield2 ≠ wyield4 substantially
  3. Stratify performance by deviation from k=0.5
  4. Frame: "where k matters, observed k makes a difference;
             where k is close to 0.5, the default is justified"
"""

import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# PATHS
# ============================================================
BASE  = Path(r'D:\OneDrive - CGIAR\Documents\PhD_JLU Giessen\Papers\Paper1\Processing\CSS-GRDC')
W2    = BASE / 'wyield2_zonal_statistics_1958-2023.csv'
W4    = BASE / 'wyield4_zonal_statistics_1958-2023.csv'
TDA   = BASE / 'Target_Drainage_Areas.txt'
#FIGS  = BASE / 'Figures'
#FIGS.mkdir(exist_ok=True)

# ============================================================
# LOAD AND FILTER (reuse from previous script)
# ============================================================
print("Loading data...")
with open(TDA) as f:
    content = f.read()
target_ids = sorted(set(
    int(x.strip().strip(','))
    for x in content.replace('[','').replace(']','').split()
    if x.strip().strip(',').isdigit()
))

w2 = pd.read_csv(W2)
w4 = pd.read_csv(W4)

# Auto-detect columns
def detect_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    cols_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols_lower: return cols_lower[c.lower()]
    return None

id_col   = detect_col(w2, ['station_no','StationNo','station','grdc_no','ID','id'])
mean_col = detect_col(w2, ['MEAN','mean','Mean','wyield_mean','value'])
date_col = detect_col(w2, ['date','Date','year','Year','time','month'])

w2[id_col] = pd.to_numeric(w2[id_col], errors='coerce')
w4[id_col] = pd.to_numeric(w4[id_col], errors='coerce')
w2f = w2[w2[id_col].isin(target_ids)].copy()
w4f = w4[w4[id_col].isin(target_ids)].copy()

merge_keys = [id_col, date_col] if date_col else [id_col]
merged = w2f.merge(w4f, on=merge_keys, suffixes=('_w2','_w4'))
valid  = ~(merged[f'{mean_col}_w2'].isna() | merged[f'{mean_col}_w4'].isna())
merged = merged[valid].copy()

# ============================================================
# BASIN-LEVEL METRICS
# ============================================================
print("Computing basin-level metrics...")

basin_stats = []
for sid, grp in merged.groupby(id_col):
    v2 = grp[f'{mean_col}_w2'].values
    v4 = grp[f'{mean_col}_w4'].values
    diff = v2 - v4
    mean_v2  = v2.mean()
    mean_v4  = v4.mean()
    abs_diff = np.abs(diff).mean()      # MAD per basin
    mean_diff = diff.mean()             # signed mean difference
    pbias_b  = 100*(v2-v4).sum()/v4.sum() if v4.sum()!=0 else np.nan
    rmse_b   = np.sqrt((diff**2).mean())
    r_b      = np.corrcoef(v2, v4)[0,1] if len(v2)>2 else np.nan
    n_months = len(v2)
    basin_stats.append({
        'station_no': sid,
        'mean_w2':    mean_v2,
        'mean_w4':    mean_v4,
        'mean_diff':  mean_diff,
        'abs_diff':   abs_diff,
        'pbias':      pbias_b,
        'rmse':       rmse_b,
        'pearson_r':  r_b,
        'n_months':   n_months
    })

df_b = pd.DataFrame(basin_stats)
df_b['abs_diff_pct'] = 100 * df_b['abs_diff'] / df_b['mean_w4'].replace(0, np.nan)

# ============================================================
# KEY DERIVED METRICS
# ============================================================
# k-deviation proxy: basins where wyield2 and wyield4 differ
# most = basins where observed k deviates most from 0.5
df_b['k_deviation_proxy'] = df_b['abs_diff'].abs()

# Quartile grouping by absolute difference
df_b['quartile'] = pd.qcut(df_b['abs_diff'],
                            q=4,
                            labels=['Q1\n(smallest diff)',
                                    'Q2',
                                    'Q3',
                                    'Q4\n(largest diff)'])

print(f"\nBasin quartile breakdown by absolute difference:")
print(df_b.groupby('quartile')[['abs_diff','pbias','rmse','mean_w2','mean_w4']].mean().round(4))

# Basins where observed k matters most (top 25%)
top25 = df_b[df_b['abs_diff'] >= df_b['abs_diff'].quantile(0.75)]
bot25 = df_b[df_b['abs_diff'] <= df_b['abs_diff'].quantile(0.25)]

print(f"\nTop 25% most different basins (n={len(top25)}):")
print(f"  Mean abs diff  : {top25['abs_diff'].mean():.4f} mm/month")
print(f"  Mean PBIAS     : {top25['pbias'].mean():.2f}%")
print(f"  Mean RMSE      : {top25['rmse'].mean():.4f} mm/month")

print(f"\nBottom 25% most similar basins (n={len(bot25)}):")
print(f"  Mean abs diff  : {bot25['abs_diff'].mean():.4f} mm/month")
print(f"  Mean PBIAS     : {bot25['pbias'].mean():.2f}%")
print(f"  Mean RMSE      : {bot25['rmse'].mean():.4f} mm/month")

# ============================================================
# FIGURE — 6-panel defense of dual argument
# ============================================================
print("\nGenerating figures...")

C2, C4, CG = '#1E88E5', '#FF9800', '#43A047'
CRED       = '#E53935'
CPURP      = '#8E24AA'

fig = plt.figure(figsize=(18, 12))
gs  = gridspec.GridSpec(2, 3, figure=fig, hspace=0.42, wspace=0.38)

# --- Panel A: Distribution of k-deviation proxy (abs_diff per basin) ---
ax = fig.add_subplot(gs[0, 0])
ax.hist(df_b['abs_diff'], bins=35, color=C2, alpha=0.75, edgecolor='white')
ax.axvline(df_b['abs_diff'].quantile(0.25), color='green',  ls='--', lw=1.2, label='Q1/Q2 boundary')
ax.axvline(df_b['abs_diff'].quantile(0.75), color=CRED, ls='--', lw=1.2, label='Q3/Q4 boundary')
ax.set_xlabel('Mean absolute difference per basin (mm/month)', fontsize=10)
ax.set_ylabel('Number of basins', fontsize=10)
ax.set_title('(A) Basin-level k-parameterization impact\n(proxy: |wyield2 – wyield4| per basin)', fontsize=10)
ax.legend(fontsize=8)
ax.text(0.97, 0.95,
        f"n = {len(df_b)} basins\nMost basins: small diff\n→ k=0.5 justified\nSome basins: large diff\n→ observed k adds value",
        transform=ax.transAxes, fontsize=8.5, va='top', ha='right',
        bbox=dict(boxstyle='round,pad=0.4', fc='white', alpha=0.85))

# --- Panel B: PBIAS per basin vs mean runoff ---
ax = fig.add_subplot(gs[0, 1])
sc = ax.scatter(df_b['mean_w4'], df_b['pbias'],
                c=df_b['abs_diff'], cmap='RdYlGn_r',
                s=20, alpha=0.6, edgecolors='none')
ax.axhline(0, color='black', lw=1, ls='--')
ax.axhline(5,  color='gray', lw=0.8, ls=':',  label='±5% threshold')
ax.axhline(-5, color='gray', lw=0.8, ls=':')
cb = plt.colorbar(sc, ax=ax)
cb.set_label('Mean abs diff (mm/month)', fontsize=8)
ax.set_xlabel('Mean water yield — fixed k=0.5 (mm/month)', fontsize=10)
ax.set_ylabel('PBIAS per basin (%)', fontsize=10)
ax.set_title('(B) Per-basin PBIAS: observed k vs fixed k=0.5\n(color = magnitude of k-parameterization impact)', fontsize=10)
ax.legend(fontsize=8)

# --- Panel C: Quartile comparison —  mean diff per quartile ---
ax = fig.add_subplot(gs[0, 2])
q_stats = df_b.groupby('quartile').agg(
    mean_abs=('abs_diff', 'mean'),
    mean_pb=('pbias', 'mean'),
    n=('abs_diff', 'count')
).reset_index()

colors_q = [CG, '#FFC107', '#FF7043', CRED]
bars = ax.bar(q_stats['quartile'], q_stats['mean_abs'],
              color=colors_q, alpha=0.8, edgecolor='white', width=0.6)
for bar, n in zip(bars, q_stats['n']):
    ax.text(bar.get_x() + bar.get_width()/2,
            bar.get_height() + 0.003,
            f'n={n}', ha='center', va='bottom', fontsize=8.5)
ax.set_xlabel('Quartile by absolute difference', fontsize=10)
ax.set_ylabel('Mean |wyield2 – wyield4| (mm/month)', fontsize=10)
ax.set_title('(C) k-parameterization impact by quartile\n(Q4 = basins where observed k adds most value)', fontsize=10)

# --- Panel D: Cumulative distribution of abs_diff ---
ax = fig.add_subplot(gs[1, 0])
sorted_diff = np.sort(df_b['abs_diff'])
cdf         = np.arange(1, len(sorted_diff)+1) / len(sorted_diff)
ax.plot(sorted_diff, cdf * 100, color=C2, lw=2)
# Mark percentiles
for pct, clr in [(50, CG), (75, '#FF7043'), (90, CRED)]:
    val = np.percentile(sorted_diff, pct)
    ax.axvline(val, color=clr, lw=1.2, ls='--',
               label=f'P{pct} = {val:.3f} mm/month')
    ax.axhline(pct, color=clr, lw=0.6, ls=':')
ax.set_xlabel('Mean absolute difference per basin (mm/month)', fontsize=10)
ax.set_ylabel('Cumulative % of basins', fontsize=10)
ax.set_title('(D) Cumulative distribution of k-parameterization impact\n(most basins cluster near zero)', fontsize=10)
ax.legend(fontsize=8)
ax.grid(alpha=0.3)

# --- Panel E: Scatter coloured by quartile ---
ax = fig.add_subplot(gs[1, 1])
cmap_q = {
    'Q1\n(smallest diff)': (CG,     'Q1 — k≈0.5, default justified'),
    'Q2':                  ('#FFC107','Q2'),
    'Q3':                  ('#FF7043','Q3'),
    'Q4\n(largest diff)':  (CRED,    'Q4 — observed k adds value')
}
for q, (clr, lbl) in cmap_q.items():
    sub = df_b[df_b['quartile'] == q]
    ax.scatter(sub['mean_w4'], sub['mean_w2'],
               color=clr, alpha=0.55, s=14,
               edgecolors='none', label=lbl)
lo = min(df_b['mean_w2'].min(), df_b['mean_w4'].min())
hi = max(df_b['mean_w2'].max(), df_b['mean_w4'].max())
ax.plot([lo, hi], [lo, hi], 'k--', lw=1)
ax.set_xlabel('Mean water yield — fixed k=0.5 (mm/month)', fontsize=10)
ax.set_ylabel('Mean water yield — observed k (mm/month)', fontsize=10)
ax.set_title('(E) 1:1 scatter coloured by k-parameterization impact\n(red = basins where observed k matters most)', fontsize=10)
ax.legend(fontsize=7.5, loc='upper left')

# --- Panel F: Dual argument summary text ---
ax = fig.add_subplot(gs[1, 2])
ax.axis('off')

pct50 = np.percentile(sorted_diff, 50)
pct75 = np.percentile(sorted_diff, 75)
pct90 = np.percentile(sorted_diff, 90)
pct_small = (df_b['abs_diff'] < 0.1).mean() * 100
pct_large = (df_b['abs_diff'] > df_b['abs_diff'].quantile(0.75)).mean() * 100

summary = (
    "DUAL ARGUMENT SUMMARY\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "WHY OBSERVED k WAS WORTH IT:\n"
    f"  • Top 25% basins: mean|diff| = {top25['abs_diff'].mean():.3f} mm/month\n"
    f"  • Top 10%: |diff| > {pct90:.3f} mm/month\n"
    "  • Observed k follows MRC (R²=0.98)\n"
    "  • Physically meaningful, basin-specific\n"
    "  • Reduces uncertainty where k ≠ 0.5\n"
    "  • Reproducible, data-driven method\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "WHY k=0.5 IS DEFENSIBLE (ungauged):\n"
    f"  • {pct_small:.0f}% of basins: |diff| < 0.1 mm/month\n"
    f"  • P50 of |diff| = {pct50:.3f} mm/month\n"
    "  • Overall PBIAS = -0.13%\n"
    "  • Pearson r = 1.000 at basin level\n"
    "  • Cohen's d = -0.224 (small effect)\n"
    "  • Necessary for global applicability\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "CONCLUSION:\n"
    "  Tiered parameterization:\n"
    "  → Gauged:   use observed k\n"
    "  → Ungauged: k=0.5 (justified by\n"
    "              sensitivity analysis)"
)

ax.text(0.04, 0.97, summary, transform=ax.transAxes,
        fontsize=8.5, va='top', fontfamily='monospace',
        bbox=dict(boxstyle='round,pad=0.5', fc='#F8F9FA', alpha=0.9))

fig.suptitle(
    "Dual argument: k-specific parameterization was worth it AND k=0.5 is defensible for ungauged areas\n"
    f"CSS pristine basins — {len(df_b)} target drainage areas  |  Period 1958–2023",
    fontsize=12, fontweight='bold', y=0.99
)

#out_fig = FIGS / 'k_parameterization_dual_argument.png'
out_fig = BASE / 'k_parameterization_dual_argument.png'
plt.savefig(out_fig, dpi=300, bbox_inches='tight', facecolor='white')
print(f"  Figure saved: {out_fig}")
plt.show()

# ============================================================
# EXPORT BASIN-LEVEL STATS
# ============================================================
out_csv = BASE / 'basin_level_k_comparison.csv'
df_b.to_csv(out_csv, index=False)
df_b.to_csv(out_csv, index=False)
print(f"  Basin stats saved : {out_csv}")

print("\n✓ Analysis complete!")