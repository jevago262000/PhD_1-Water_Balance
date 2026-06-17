#!/usr/bin/env python3
"""
Statistical Comparison: Observed k (wyield2) vs Fixed k=0.5 (wyield4)
Target: CSS pristine basins in Target_Drainage_Areas.txt
Period: 1958-2023 monthly zonal statistics
"""

import os
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import warnings
warnings.filterwarnings('ignore')

# === FILE PATHS ===
OUT_DIR  = r'D:\OneDrive - CGIAR\Documents\PhD_JLU Giessen\Papers\Paper1\Processing\CSS-GRDC'
PATH_W2  = os.path.join(OUT_DIR, 'wyield2_zonal_statistics_1958-2023.csv')
PATH_W4  = os.path.join(OUT_DIR, 'wyield4_zonal_statistics_1958-2023.csv')
PATH_TDA = os.path.join(OUT_DIR, 'Target_Drainage_Areas.txt')

# === LOAD TARGET DRAINAGE AREAS ===
print("Loading target drainage areas...")
with open(PATH_TDA) as f:
    content = f.read()
target_ids = [int(x.strip().strip(',')) 
              for x in content.replace('[','').replace(']','').split() 
              if x.strip().strip(',').isdigit()]
print(f"  Target stations: {len(target_ids)}")

# === LOAD CSV FILES ===
print("\nLoading CSV files...")
w2 = pd.read_csv(PATH_W2)
w4 = pd.read_csv(PATH_W4)
print(f"  wyield2 shape: {w2.shape}")
print(f"  wyield4 shape: {w4.shape}")
print(f"\n  wyield2 columns: {list(w2.columns)}")
print(f"  wyield4 columns: {list(w4.columns)}")

# === IDENTIFY COLUMN NAMES ===
# Adjust these if your actual column names differ
id_col   = 'station_no'    # station identifier column
mean_col = 'MEAN'          # zonal mean (mm/month)
flow_col = 'flow'          # flow column (m3/s) - adjust if needed

# Auto-detect station ID column
for col in ['station_no', 'StationNo', 'station', 'ID', 'id']:
    if col in w2.columns:
        id_col = col
        break

# Auto-detect mean column
for col in ['MEAN', 'mean', 'Mean', 'wyield_mean']:
    if col in w2.columns:
        mean_col = col
        break

# Auto-detect flow column
for col in ['flow', 'Flow', 'discharge', 'Q', 'q']:
    if col in w2.columns:
        flow_col = col
        break

print(f"\n  Using: id={id_col}, mean={mean_col}, flow={flow_col}")

# === FILTER BY TARGET DRAINAGE AREAS ===
print("\nFiltering by target drainage areas...")
w2_f = w2[w2[id_col].isin(target_ids)].copy()
w4_f = w4[w4[id_col].isin(target_ids)].copy()
print(f"  wyield2 filtered: {w2_f.shape} | Unique stations: {w2_f[id_col].nunique()}")
print(f"  wyield4 filtered: {w4_f.shape} | Unique stations: {w4_f[id_col].nunique()}")

# === MERGE ON STATION + TIME ===
# Detect date/time column
date_col = None
for col in ['DATE', 'date', 'Date', 'YEAR', 'year', 'Year', 'time', 'Time', 'MONTH', 'month', 'Month']:
    if col in w2.columns:
        date_col = col
        break

merge_keys = [id_col, date_col] if date_col else [id_col]
print(f"\nMerging on: {merge_keys}")

merged = w2_f.merge(w4_f, on=merge_keys, suffixes=('_w2', '_w4'))
print(f"  Merged shape: {merged.shape}")

mean_w2 = merged[f'{mean_col}_w2'] if f'{mean_col}_w2' in merged.columns else merged[mean_col+'_w2']
mean_w4 = merged[f'{mean_col}_w4'] if f'{mean_col}_w4' in merged.columns else merged[mean_col+'_w4']
diff    = mean_w2 - mean_w4

# === HELPER FUNCTIONS ===
def cohens_d(a, b):
    """Cohen's d effect size for paired samples"""
    d = a - b
    return d.mean() / d.std()

def interpret_d(d):
    ad = abs(d)
    if   ad < 0.2: return "negligible"
    elif ad < 0.5: return "small"
    elif ad < 0.8: return "medium"
    else:          return "large"

def interpret_p(p):
    if   p < 0.001: return "***  (p<0.001)"
    elif p < 0.01:  return "**   (p<0.01)"
    elif p < 0.05:  return "*    (p<0.05)"
    else:           return "ns   (p≥0.05)"

# ============================================================
# LEVEL 1: BASIN-LEVEL ANNUAL MEANS
# ============================================================
print("\n" + "="*60)
print("LEVEL 1: BASIN-LEVEL ANNUAL MEANS")
print("="*60)

basin_w2 = w2_f.groupby(id_col)[mean_col].mean()
basin_w4 = w4_f.groupby(id_col)[mean_col].mean()
common   = basin_w2.index.intersection(basin_w4.index)
b2, b4   = basin_w2[common].values, basin_w4[common].values

t_stat, t_p   = stats.ttest_rel(b2, b4)
w_stat, w_p   = stats.wilcoxon(b2, b4)
ks_stat, ks_p = stats.ks_2samp(b2, b4)
d             = cohens_d(pd.Series(b2), pd.Series(b4))
r_p, _        = stats.pearsonr(b2, b4)
r_s, _        = stats.spearmanr(b2, b4)
pbias         = 100 * (b2 - b4).sum() / b4.sum()
rmse          = np.sqrt(((b2 - b4)**2).mean())

print(f"\n  Basins compared     : {len(common)}")
print(f"  Mean wyield2        : {b2.mean():.3f} mm/month")
print(f"  Mean wyield4        : {b4.mean():.3f} mm/month")
print(f"  Mean difference     : {(b2-b4).mean():.3f} mm/month")
print(f"\n  Paired t-test       : t={t_stat:.4f}, p={t_p:.4e}  {interpret_p(t_p)}")
print(f"  Wilcoxon signed-rank: W={w_stat:.2f},  p={w_p:.4e}  {interpret_p(w_p)}")
print(f"  KS test             : D={ks_stat:.4f}, p={ks_p:.4e}  {interpret_p(ks_p)}")
print(f"  Cohen's d           : {d:.4f} ({interpret_d(d)})")
print(f"  Pearson r           : {r_p:.4f}")
print(f"  Spearman r          : {r_s:.4f}")
print(f"  PBIAS               : {pbias:.2f}%")
print(f"  RMSE                : {rmse:.4f} mm/month")

# ============================================================
# LEVEL 2: ALL MONTHLY VALUES POOLED
# ============================================================
print("\n" + "="*60)
print("LEVEL 2: ALL MONTHLY VALUES POOLED")
print("="*60)

t2_stat, t2_p = stats.ttest_rel(mean_w2, mean_w4)
w2_stat, w2_p = stats.wilcoxon(mean_w2, mean_w4)
d2            = cohens_d(mean_w2, mean_w4)
pbias2        = 100 * (mean_w2 - mean_w4).sum() / mean_w4.sum()
rmse2         = np.sqrt(((mean_w2 - mean_w4)**2).mean())

print(f"\n  Station-months      : {len(mean_w2):,}")
print(f"  Mean wyield2        : {mean_w2.mean():.3f} mm/month")
print(f"  Mean wyield4        : {mean_w4.mean():.3f} mm/month")
print(f"  Mean difference     : {(mean_w2-mean_w4).mean():.3f} mm/month")
print(f"\n  Paired t-test       : t={t2_stat:.4f}, p={t2_p:.4e}  {interpret_p(t2_p)}")
print(f"  Wilcoxon signed-rank: W={w2_stat:.2f},  p={w2_p:.4e}  {interpret_p(w2_p)}")
print(f"  Cohen's d           : {d2:.4f} ({interpret_d(d2)})")
print(f"  PBIAS               : {pbias2:.2f}%")
print(f"  RMSE                : {rmse2:.4f} mm/month")

# ============================================================
# LEVEL 3: FLOW COMPARISON (m3/s)
# ============================================================
if flow_col and f'{flow_col}_w2' in merged.columns:
    print("\n" + "="*60)
    print("LEVEL 3: FLOW COMPARISON (m3/s)")
    print("="*60)

    flow_w2 = merged[f'{flow_col}_w2']
    flow_w4 = merged[f'{flow_col}_w4']

    t3_stat, t3_p = stats.ttest_rel(flow_w2, flow_w4)
    w3_stat, w3_p = stats.wilcoxon(flow_w2, flow_w4)
    d3            = cohens_d(flow_w2, flow_w4)
    pbias3        = 100 * (flow_w2 - flow_w4).sum() / flow_w4.sum()
    rmse3         = np.sqrt(((flow_w2 - flow_w4)**2).mean())

    print(f"\n  Mean flow wyield2   : {flow_w2.mean():.4f} m3/s")
    print(f"  Mean flow wyield4   : {flow_w4.mean():.4f} m3/s")
    print(f"\n  Paired t-test       : t={t3_stat:.4f}, p={t3_p:.4e}  {interpret_p(t3_p)}")
    print(f"  Wilcoxon signed-rank: W={w3_stat:.2f},  p={w3_p:.4e}  {interpret_p(w3_p)}")
    print(f"  Cohen's d           : {d3:.4f} ({interpret_d(d3)})")
    print(f"  PBIAS               : {pbias3:.2f}%")
    print(f"  RMSE                : {rmse3:.4f} m3/s")

# ============================================================
# VISUALIZATIONS
# ============================================================
print("\nGenerating figures...")

fig = plt.figure(figsize=(16, 12))
gs  = gridspec.GridSpec(2, 3, figure=fig, hspace=0.4, wspace=0.35)

colors = {'w2': '#1E88E5', 'w4': '#FF9800', 'diff': '#43A047'}

# --- Panel 1: Scatter basin means ---
ax1 = fig.add_subplot(gs[0, 0])
ax1.scatter(b4, b2, alpha=0.4, s=15, color=colors['w2'], edgecolors='none')
lims = [min(b2.min(), b4.min()), max(b2.max(), b4.max())]
ax1.plot(lims, lims, 'k--', lw=1, label='1:1 line')
ax1.set_xlabel('Fixed k=0.5 (mm/month)', fontsize=10)
ax1.set_ylabel('Observed k (mm/month)', fontsize=10)
ax1.set_title('Basin mean: observed k vs fixed k=0.5', fontsize=10)
ax1.text(0.05, 0.92, f"r = {r_p:.3f}\nn = {len(common)}",
         transform=ax1.transAxes, fontsize=9,
         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7))
ax1.legend(fontsize=8)

# --- Panel 2: Difference histogram ---
ax2 = fig.add_subplot(gs[0, 1])
diff_basin = b2 - b4
ax2.hist(diff_basin, bins=30, color=colors['diff'], alpha=0.7, edgecolor='white')
ax2.axvline(0, color='black', lw=1.5, linestyle='--')
ax2.axvline(diff_basin.mean(), color='red', lw=1.5, linestyle='-',
            label=f'Mean={diff_basin.mean():.3f}')
ax2.set_xlabel('Difference (observed k - fixed k=0.5) mm/month', fontsize=10)
ax2.set_ylabel('Count (basins)', fontsize=10)
ax2.set_title('Distribution of basin-mean differences', fontsize=10)
ax2.legend(fontsize=8)

# --- Panel 3: Box plots ---
ax3 = fig.add_subplot(gs[0, 2])
bp = ax3.boxplot([b2, b4],
                 labels=['Observed k\n(wyield2)', 'Fixed k=0.5\n(wyield4)'],
                 patch_artist=True, notch=True,
                 boxprops=dict(linewidth=1.2),
                 medianprops=dict(color='black', linewidth=2))
bp['boxes'][0].set_facecolor(colors['w2'] + '80')
bp['boxes'][1].set_facecolor(colors['w4'] + '80')
ax3.set_ylabel('Mean annual water yield (mm/month)', fontsize=10)
ax3.set_title('Distribution comparison', fontsize=10)
sig = "p<0.001***" if w_p < 0.001 else f"p={w_p:.3f}"
ax3.text(1.5, max(b2.max(), b4.max()) * 0.98, sig,
         ha='center', fontsize=9, color='red')

# --- Panel 4: Monthly pooled difference ---
ax4 = fig.add_subplot(gs[1, 0])
ax4.hist(diff, bins=50, color=colors['diff'], alpha=0.7, edgecolor='white')
ax4.axvline(0, color='black', lw=1.5, linestyle='--')
ax4.axvline(diff.mean(), color='red', lw=1.5,
            label=f'Mean={diff.mean():.3f} mm/month')
ax4.set_xlabel('Monthly difference (observed k - fixed k=0.5)', fontsize=10)
ax4.set_ylabel('Count (station-months)', fontsize=10)
ax4.set_title('All monthly values pooled', fontsize=10)
ax4.legend(fontsize=8)

# --- Panel 5: QQ plot of differences ---
ax5 = fig.add_subplot(gs[1, 1])
stats.probplot(diff, dist='norm', plot=ax5)
ax5.set_title('Q-Q plot of monthly differences', fontsize=10)
ax5.get_lines()[0].set(markersize=2, alpha=0.3, color=colors['w2'])
ax5.get_lines()[1].set(color='red', lw=1.5)

# --- Panel 6: Summary text panel ---
ax6 = fig.add_subplot(gs[1, 2])
ax6.axis('off')
summary = (
    "STATISTICAL SUMMARY\n"
    "─────────────────────────────────\n"
    "LEVEL 1 — Basin annual means\n"
    f"  Paired t-test:     {interpret_p(t_p)}\n"
    f"  Wilcoxon:          {interpret_p(w_p)}\n"
    f"  Cohen's d:         {d:.3f} ({interpret_d(d)})\n"
    f"  PBIAS:             {pbias:.2f}%\n"
    f"  RMSE:              {rmse:.3f} mm/month\n"
    f"  Pearson r:         {r_p:.3f}\n"
    "─────────────────────────────────\n"
    "LEVEL 2 — Monthly pooled\n"
    f"  Paired t-test:     {interpret_p(t2_p)}\n"
    f"  Wilcoxon:          {interpret_p(w2_p)}\n"
    f"  Cohen's d:         {d2:.3f} ({interpret_d(d2)})\n"
    f"  PBIAS:             {pbias2:.2f}%\n"
    f"  RMSE:              {rmse2:.3f} mm/month\n"
    "─────────────────────────────────\n"
    f"  n basins:          {len(common)}\n"
    f"  n station-months:  {len(mean_w2):,}"
)
ax6.text(0.05, 0.95, summary, transform=ax6.transAxes,
         fontsize=9, verticalalignment='top', fontfamily='monospace',
         bbox=dict(boxstyle='round', facecolor='#f8f9fa', alpha=0.8))

fig.suptitle(
    "Statistical comparison: Observed recession constants (wyield2) vs Fixed k=0.5 (wyield4)\n"
    "CSS pristine basins — Target drainage areas only",
    fontsize=12, fontweight='bold', y=0.98
)

out_fig = OUT_DIR + r'\statistical_comparison_k_approaches.png'
plt.savefig(out_fig, dpi=300, bbox_inches='tight', facecolor='white')
print(f"\n  Figure saved: {out_fig}")
plt.show()

# ============================================================
# EXPORT RESULTS TABLE
# ============================================================
results = {
    'Test':        ['Paired t-test', 'Wilcoxon', 'KS test', "Cohen's d",
                    'Pearson r', 'Spearman r', 'PBIAS (%)', 'RMSE (mm/month)'],
    'Statistic':   [round(t_stat,4), round(w_stat,4), round(ks_stat,4),
                    round(d,4), round(r_p,4), round(r_s,4),
                    round(pbias,2), round(rmse,4)],
    'p-value':     [round(t_p,6), round(w_p,6), round(ks_p,6),
                    '-', '-', '-', '-', '-'],
    'Significance':[interpret_p(t_p), interpret_p(w_p), interpret_p(ks_p),
                    interpret_d(d), '-', '-', '-', '-'],
    'Level':       ['Basin means']*8
}

df_results = pd.DataFrame(results)
out_csv = OUT_DIR + r'\statistical_results_k_comparison.csv'
df_results.to_csv(out_csv, index=False)
print(f"  Results table saved: {out_csv}")

print("\n✓ Analysis complete!")
print(f"  Outputs in: {OUT_DIR}")