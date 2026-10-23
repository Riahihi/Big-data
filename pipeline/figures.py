"""Bước 8: vẽ biểu đồ cho báo cáo, lưu vào reports/figures

Chạy: python pipeline/figures.py
"""
import json
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib

matplotlib.use("Agg")   # vẽ không cần màn hình (chạy trên server)
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

from pipeline import config as C

# màu cho các đường / cột
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
BLUES = LinearSegmentedColormap.from_list("blues", ["#eef5fd", "#9ec5f4", "#3987e5", "#1c5cab", "#0d366b"])
ORANGES = LinearSegmentedColormap.from_list("oranges", ["#fdf0ea", "#f6b896", "#eb6834", "#b4461b", "#6e2a0f"])
DIVERGING = LinearSegmentedColormap.from_list("div", ["#1c5cab", "#86b6ef", "#f0efec", "#f19a9a", "#b52f2f"])
MONTHS = ["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9", "T10", "T11", "T12"]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 9,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlecolor": INK, "font.family": "DejaVu Sans",
})


def R(name):
    return pd.read_parquet(C.RESULTS_DIR / name)


def savefig(fig, name):
    C.FIGURES.mkdir(parents=True, exist_ok=True)
    fig.savefig(C.FIGURES / f"{name}.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("saved", name)


def core_order():
    locs = [l for l in C.locations_as_dicts() if l["is_core"]]
    return [l["location_id"] for l in sorted(locs, key=lambda l: -l["latitude"])]


def names():
    """Tên tỉnh, thêm tên điểm đo nếu khác (vd Quảng Ninh (Hạ Long))"""
    return {l["location_id"]: l["location_name"] if l["site_name"] == l["location_name"]
            else f"{l['location_name']} ({l['site_name']})" for l in C.locations_as_dicts()}


def fig_q1_lines():
    """Câu 1: nhiệt độ TB tháng 2019–2023 – mỗi địa điểm một ô nhỏ, cùng thang đo y."""
    q1 = R("q1_monthly_temp_core")
    q1["t"] = pd.to_datetime(q1["year_month"] + "-15")
    ids, nm = core_order(), names()
    fig, axes = plt.subplots(2, 5, figsize=(16, 5.6), sharex=True, sharey=True)
    for ax, lid in zip(axes.flat, ids):
        d = q1[q1["location_id"] == lid].sort_values("t")
        ax.fill_between(d["t"], d["avg_daily_min_c"], d["avg_daily_max_c"], color=SERIES[0], alpha=0.15, lw=0,
                        label="TB min–max ngày")
        ax.plot(d["t"], d["avg_temp_c"], color=SERIES[0], lw=1.8, label="TB tháng")
        ax.set_title(nm[lid])
        ax.tick_params(axis="x", rotation=0)
    axes[0, 0].set_ylabel("°C")
    axes[1, 0].set_ylabel("°C")
    axes[0, 0].legend(loc="lower left", fontsize=7, frameon=False)
    fig.suptitle("Câu 1 – Nhiệt độ trung bình tháng tại 10 địa điểm, 2019–2023", x=0.01, ha="left",
                 fontweight="bold", color=INK)
    fig.tight_layout()
    savefig(fig, "q1_monthly_temp_lines")


def fig_q1_heatmap():
    """Câu 1: khí hậu nhiệt độ theo tháng (TB 2019–2023) – heatmap địa điểm × tháng."""
    c = R("q1_climatology_core")
    ids, nm = core_order(), names()
    m = c.pivot(index="location_id", columns="month", values="clim_temp_c").loc[ids]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    im = ax.imshow(m.values, cmap=ORANGES, aspect="auto")
    for i in range(m.shape[0]):
        for j in range(12):
            v = m.values[i, j]
            ax.text(j, i, f"{v:.1f}", ha="center", va="center", fontsize=7.5,
                    color="white" if v > np.nanpercentile(m.values, 65) else INK)
    ax.set_xticks(range(12), MONTHS)
    ax.set_yticks(range(len(ids)), [nm[i] for i in ids])
    ax.grid(False)
    fig.colorbar(im, ax=ax, label="°C", shrink=0.85)
    ax.set_title("Câu 1 – Nhiệt độ TB theo tháng (trung bình 2019–2023), Bắc → Nam", loc="left")
    savefig(fig, "q1_climatology_heatmap")


def fig_q2_bars():
    """Câu 2: tổng lượng mưa tháng – cột theo thời gian, mỗi địa điểm một ô."""
    q2 = R("q2_monthly_precip_core")
    q2["t"] = pd.to_datetime(q2["year_month"] + "-01")
    ids, nm = core_order(), names()
    fig, axes = plt.subplots(2, 5, figsize=(16, 5.6), sharex=True, sharey=True)
    for ax, lid in zip(axes.flat, ids):
        d = q2[q2["location_id"] == lid].sort_values("t")
        ax.bar(d["t"], d["total_precip_mm"], width=24, color=SERIES[0], edgecolor=SURFACE, linewidth=0.5)
        ax.set_title(nm[lid])
    axes[0, 0].set_ylabel("mm/tháng")
    axes[1, 0].set_ylabel("mm/tháng")
    fig.suptitle("Câu 2 – Tổng lượng mưa tháng tại 10 địa điểm, 2019–2023", x=0.01, ha="left",
                 fontweight="bold", color=INK)
    fig.tight_layout()
    savefig(fig, "q2_monthly_precip_bars")


def fig_q2_seasonal():
    """Câu 2: mùa mưa – lượng mưa TB từng tháng (heatmap) cho thấy miền Trung mưa muộn (T9–T12)."""
    q2 = R("q2_monthly_precip_core")
    ids, nm = core_order(), names()
    m = q2.groupby(["location_id", "month"])["total_precip_mm"].mean().unstack().loc[ids]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    im = ax.imshow(m.values, cmap=BLUES, aspect="auto")
    for i in range(m.shape[0]):
        for j in range(12):
            v = m.values[i, j]
            ax.text(j, i, f"{v:.0f}", ha="center", va="center", fontsize=7.5,
                    color="white" if v > np.nanpercentile(m.values, 70) else INK)
    ax.set_xticks(range(12), MONTHS)
    ax.set_yticks(range(len(ids)), [nm[i] for i in ids])
    ax.grid(False)
    fig.colorbar(im, ax=ax, label="mm/tháng", shrink=0.85)
    ax.set_title("Câu 2 – Lượng mưa TB theo tháng (2019–2023): mùa mưa khác nhau giữa các vùng", loc="left")
    savefig(fig, "q2_seasonal_heatmap")


def fig_q3_top5():
    """Câu 3: 5 ngày mưa lớn nhất – mỗi địa điểm một hàng thanh ngang, ghi ngày bên cạnh."""
    q3 = R("q3_top5_rain_days_core")
    ids, nm = core_order(), names()
    fig, axes = plt.subplots(2, 5, figsize=(16, 5.2), sharex=True)
    for ax, lid in zip(axes.flat, ids):
        d = q3[q3["location_id"] == lid].sort_values(["rank", "date"]).head(8)
        y = np.arange(len(d))[::-1]
        ax.barh(y, d["precip_mm"], color=SERIES[0], height=0.62)
        for yy, (_, r) in zip(y, d.iterrows()):
            ax.text(r["precip_mm"] + 3, yy, f"{pd.Timestamp(r['date']):%d/%m/%Y}  {r['precip_mm']:.0f}",
                    va="center", fontsize=7, color=INK2)
        ax.set_yticks(y, [f"#{int(r)}" for r in d["rank"]])
        ax.set_title(nm[lid])
        ax.grid(axis="y", visible=False)
    for ax in axes[1]:
        ax.set_xlabel("mm/ngày")
    axes[0, 0].set_xlim(0, q3["precip_mm"].max() * 1.6)
    fig.suptitle("Câu 3 – 5 ngày có tổng lượng mưa lớn nhất mỗi địa điểm (2019–2023)", x=0.01, ha="left",
                 fontweight="bold", color=INK)
    fig.tight_layout()
    savefig(fig, "q3_top5_rain_days")


def fig_warming_stripes():
    """Mở rộng: dị thường nhiệt độ năm của 34 tỉnh/thành (xanh = lạnh hơn TB, đỏ = nóng hơn)."""
    a = R("a5_annual_anomaly")
    order = [l["location_id"] for l in sorted(C.locations_as_dicts(), key=lambda l: -l["latitude"])]
    m = a.pivot(index="location_id", columns="year", values="anomaly_c").reindex(order).dropna(how="all")
    lim = np.nanmax(np.abs(m.values))
    fig, ax = plt.subplots(figsize=(9, 10))
    im = ax.imshow(m.values, cmap=DIVERGING, norm=TwoSlopeNorm(0, -lim, lim), aspect="auto")
    ax.set_xticks(range(m.shape[1]), m.columns)
    ax.set_yticks(range(m.shape[0]), [names()[i] for i in m.index])
    ax.grid(False)
    fig.colorbar(im, ax=ax, label="Dị thường so với TB 2016–2025 (°C)", shrink=0.6)
    ax.set_title("Dị thường nhiệt độ năm – 34 tỉnh/thành (Bắc → Nam)", loc="left")
    savefig(fig, "adv_warming_stripes")


def fig_trends():
    """Mở rộng: xu hướng nhiệt độ (°C/thập kỷ) kèm khoảng tin cậy 95% từ hồi quy OLS."""
    t = R("a5_temperature_trends").sort_values("trend_c_per_decade_monthly")
    fig, ax = plt.subplots(figsize=(8, 9))
    y = np.arange(len(t))
    ci = 1.96 * t["se_monthly"] * 10
    sig = t["p_monthly"] < 0.05
    ax.errorbar(t["trend_c_per_decade_monthly"], y, xerr=ci, fmt="none", ecolor=GRID, elinewidth=2)
    ax.scatter(t.loc[sig, "trend_c_per_decade_monthly"], y[sig.values], color=SERIES[7], s=36, zorder=3,
               label="p < 0,05")
    ax.scatter(t.loc[~sig, "trend_c_per_decade_monthly"], y[~sig.values], facecolor=SURFACE,
               edgecolor=INK2, s=36, zorder=3, label="không có ý nghĩa thống kê")
    ax.axvline(0, color=INK2, lw=0.8)
    ax.set_yticks(y, t["location_name"])
    ax.set_xlabel("°C / thập kỷ (hồi quy trên dị thường tháng, 2016–2025)")
    ax.legend(frameon=False, loc="lower right")
    ax.set_title("Xu hướng nhiệt độ tại 34 tỉnh/thành", loc="left")
    savefig(fig, "adv_temperature_trends")


def fig_heat_days():
    """Mở rộng: số ngày nắng nóng (Tmax ≥ 35 °C) mỗi năm, trung bình theo vùng."""
    ci = R("a1_climate_indices")
    m = ci.groupby(["region", "year"])["SU35"].mean().unstack(0)
    m = m[m.mean().sort_values(ascending=False).index[:8]]       # tối đa 8 chuỗi
    fig, ax = plt.subplots(figsize=(10, 4.5))
    for i, col in enumerate(m.columns):
        ax.plot(m.index, m[col], color=SERIES[i], lw=2, marker="o", ms=4, label=col)
    ax.set_ylabel("ngày / năm")
    ax.set_xticks(m.index)
    ax.legend(ncol=4, frameon=False, fontsize=8, loc="upper left")
    ax.set_title("Số ngày nắng nóng (Tmax ≥ 35 °C) TB mỗi năm theo vùng", loc="left")
    savefig(fig, "adv_heat_days_by_region")


def fig_diurnal():
    """Mở rộng: xác suất có mưa theo giờ × tháng – Hà Nội vs TP.HCM (mưa chiều tối ở miền Nam)."""
    d = R("a6_diurnal_cycle")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2), sharey=True)
    vmax = d[d["location_id"].isin(["HNI", "HCM"])]["wet_hour_pct"].max()
    for ax, lid in zip(axes, ["HNI", "HCM"]):
        m = d[d["location_id"] == lid].pivot(index="hour", columns="month", values="wet_hour_pct")
        im = ax.imshow(m.values, cmap=BLUES, aspect="auto", vmin=0, vmax=vmax, origin="lower")
        ax.set_xticks(range(12), MONTHS)
        ax.set_yticks(range(0, 24, 3), [f"{h}h" for h in range(0, 24, 3)])
        ax.grid(False)
        ax.set_title(f"{names()[lid]} – % số giờ có mưa (≥ 0,1 mm)", loc="left")
    fig.colorbar(im, ax=axes, label="% giờ có mưa", shrink=0.9)
    savefig(fig, "adv_diurnal_rain")


def fig_return_levels():
    """Mở rộng: lượng mưa ngày ứng với chu kỳ lặp lại (Gumbel) – 4 địa điểm tiêu biểu."""
    r = R("a7_return_levels")
    pick = ["HUE", "DNG", "HNI", "HCM"]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for i, lid in enumerate(pick):
        d = r[r["location_id"] == lid].sort_values("return_period_years")
        ax.plot(d["return_period_years"], d["return_level_mm"], color=SERIES[i], lw=2, marker="o", ms=5)
        ax.text(d["return_period_years"].iloc[-1] * 1.08, d["return_level_mm"].iloc[-1], names()[lid],
                va="center", color=INK2, fontsize=8)
    ax.set_xscale("log")
    ax.set_xticks([2, 5, 10, 25, 50, 100], ["2", "5", "10", "25", "50", "100"])
    ax.set_xlim(1.8, 180)
    ax.set_xlabel("Chu kỳ lặp lại (năm)")
    ax.set_ylabel("mm/ngày")
    ax.set_title("Lượng mưa ngày cực đại theo chu kỳ lặp lại (Gumbel, 2016–2025)", loc="left")
    savefig(fig, "adv_return_levels")


def fig_clusters():
    """Mở rộng: phân cụm khí hậu KMeans – vị trí địa lý (kinh độ/vĩ độ) tô màu theo cụm."""
    cl = R("a9_climate_clusters")
    loc = pd.DataFrame(C.locations_as_dicts())
    d = cl.merge(loc[["location_id", "latitude", "longitude"]], on="location_id")
    fig, axes = plt.subplots(1, 2, figsize=(12, 6.5))
    for k in sorted(d["cluster"].unique()):
        s = d[d["cluster"] == k]
        axes[0].scatter(s["longitude"], s["latitude"], s=70, color=SERIES[k], edgecolor=SURFACE, lw=1.5,
                        label=f"Cụm {k + 1}")
        axes[1].scatter(s["pc1"], s["pc2"], s=70, color=SERIES[k], edgecolor=SURFACE, lw=1.5)
    for _, r in d.iterrows():
        axes[0].annotate(r["site_name"], (r["longitude"], r["latitude"]), xytext=(4, 2),
                         textcoords="offset points", fontsize=6.5, color=INK2)
        axes[1].annotate(r["site_name"], (r["pc1"], r["pc2"]), xytext=(4, 2),
                         textcoords="offset points", fontsize=6.5, color=INK2)
    axes[0].set_xlabel("Kinh độ")
    axes[0].set_ylabel("Vĩ độ")
    axes[0].set_aspect("equal")
    axes[0].legend(frameon=False)
    axes[0].set_title("Cụm khí hậu theo vị trí", loc="left")
    axes[1].set_xlabel("PC1")
    axes[1].set_ylabel("PC2")
    axes[1].set_title("Không gian đặc trưng (PCA 2 chiều)", loc="left")
    savefig(fig, "adv_climate_clusters")


def fig_benchmark():
    """Mức B: dung lượng lưu trữ các định dạng trên cùng tập dữ liệu."""
    p = C.RESULTS_DIR / "benchmark.json"
    if not p.exists():
        return
    b = json.loads(p.read_text())
    labels = {"raw_json_uncompressed": "JSON gốc", "raw_json_gzip": "JSON gốc (gzip)",
              "csv_uncompressed": "CSV", "parquet_snappy_flat": "Parquet (snappy)",
              "parquet_snappy_partitioned": "Parquet phân vùng"}
    s = b["B1_storage_MB"]
    keys = [k for k in labels if k in s]
    fig, ax = plt.subplots(figsize=(8, 3.4))
    y = np.arange(len(keys))[::-1]
    ax.barh(y, [s[k] for k in keys], color=SERIES[0], height=0.6)
    for yy, k in zip(y, keys):
        ax.text(s[k] * 1.01 + 2, yy, f"{s[k]:.0f} MB", va="center", color=INK2, fontsize=8)
    ax.set_yticks(y, [labels[k] for k in keys])
    ax.set_xlabel("MB")
    ax.grid(axis="y", visible=False)
    ax.set_title(f"Dung lượng lưu trữ cùng {b['n_rows']:,} bản ghi giờ".replace(",", "."), loc="left")
    savefig(fig, "benchmark_storage")


def main():
    for f in [fig_q1_lines, fig_q1_heatmap, fig_q2_bars, fig_q2_seasonal, fig_q3_top5, fig_warming_stripes,
              fig_trends, fig_heat_days, fig_diurnal, fig_return_levels, fig_clusters, fig_benchmark]:
        f()


if __name__ == "__main__":
    main()
