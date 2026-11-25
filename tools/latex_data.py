"""Membuat tabel LaTeX dan makro angka (output/latex/) dari hasil pipeline di output/*.csv dan
ml_metrics.json, supaya setiap angka di artikel dan dokumen penjelasan sama persis dengan output program.
Pemakaian: python3 tools/latex_data.py   (setelah ./run.sh)"""
import csv
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "output"
TEX = OUT / "latex"
MODELS = {"Decision Tree dangkal": "dtd", "Decision Tree (fitur dasar)": "dtb", "Decision Tree": "dt",
          "Random Forest": "rf", "GBT": "gbt"}


def num(x, d=0):
    """Format angka Indonesia: titik pemisah ribuan, koma desimal."""
    return f"{float(x):,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def esc(s):
    return str(s).replace("\\", "\\textbackslash{}").replace("_", "\\_").replace("&", "\\&").replace("%", "\\%")


def code(s):
    return f"\\texttt{{{esc(s)}}}"


def rows(name):
    with open(OUT / f"{name}.csv") as f:
        return list(csv.DictReader(f))


def table(name, spec, header, body):
    lines = [f"\\begin{{tabular}}{{{spec}}}", "\\toprule", " & ".join(header) + " \\\\", "\\midrule"]
    lines += [" & ".join(r) + " \\\\" for r in body]
    lines += ["\\bottomrule", "\\end{tabular}"]
    (TEX / f"tab_{name}.tex").write_text("\n".join(lines) + "\n")


TEX.mkdir(exist_ok=True)
m = {}  # nama makro -> isi

# ---------- Dataset & kualitas data ----------
quality = rows("data_quality")
table("data_quality", "lrrrrr", ["Tabel", "Baris", "Kolom", "Sel kosong", "Duplikat baris", "Duplikat kunci"],
      [[code(r["table"]), num(r["rows"]), r["columns"], num(r["null_cells"]), num(r["duplicate_rows"]),
        num(r["duplicate_keys"]) if r["duplicate_keys"] else "--"] for r in quality])
for r in quality:
    m["rows" + "".join(p.title() for p in r["table"].split("_"))] = num(r["rows"])
m["rawRows"] = num(sum(int(r["rows"]) for r in quality))
m["rawSizeMB"] = num(sum(f.stat().st_size for f in (ROOT / "data" / "olist").glob("*.csv")) / 2**20, 1)

clean = rows("cleaning_log")
table("cleaning", "lrrr", ["Langkah", "Sebelum", "Sesudah", "Dibuang"],
      [[esc(r["step"]), num(r["before"]), num(r["after"]), num(r["removed"])] for r in clean])
for key, r in zip(["cleanStatus", "cleanBadDate", "cleanReview", "cleanPayment", "cleanGeoDup", "cleanGeoOut"], clean):
    m[key] = num(r["removed"])
m["ordersBeforeClean"], m["ordersAfterStatus"] = num(clean[0]["before"]), num(clean[0]["after"])
m["geoAfterClean"] = num(clean[-1]["after"])
m["badDates"] = num(clean[1]["before"])

# ---------- Analisis Spark SQL ----------
a1 = rows("a1_monthly_trend")
m["nOrders"] = num(sum(int(r["orders"]) for r in a1))
peak = max(a1, key=lambda r: float(r["revenue"]))
m["peakMonth"], m["peakRevenue"], m["peakOrders"] = peak["month"], num(peak["revenue"], 2), num(peak["orders"])
m["totalRevenue"] = num(sum(float(r["revenue"]) for r in a1), 2)
m["firstMonth"], m["lastMonth"] = a1[0]["month"], a1[-1]["month"]
etl_log = OUT / "logs" / "03_etl.txt"
if etl_log.exists():
    found = re.search(r"curated/orders: [\d,]+ baris, (\d+) kolom", etl_log.read_text())
    m["curatedColumns"] = found[1] if found else "?"
    found = re.search(r"selesai dalam ([\d.]+) detik", etl_log.read_text())
    m["etlSeconds"] = num(found[1], 1) if found else "?"

a2 = rows("a2_top_categories")
table("a2", "lrrrr", ["Kategori", "Pesanan", "Pendapatan (BRL)", "Harga item rata-rata", "Review"],
      [[code(r["category"]), num(r["orders"]), num(r["revenue"], 2), num(r["avg_item_price"], 2),
        num(r["avg_review"], 2)] for r in a2])
m["topCategory"], m["topCategoryRevenue"] = code(a2[0]["category"]), num(a2[0]["revenue"], 2)
m["secondCategory"] = code(a2[1]["category"])
m["mostOrdersCategory"] = code(max(a2, key=lambda r: int(r["orders"]))["category"])

a3 = rows("a3_late_by_state")
table("a3", "lrrrr", ["Negara bagian", "Pesanan terkirim", "Hari kirim", "Jarak (km)", "Terlambat (\\%)"],
      [[r["customer_state"], num(r["delivered_orders"]), num(r["avg_delivery_days"], 1),
        num(r["avg_distance_km"]), num(r["late_pct"], 2)] for r in a3])
m["lateWorstState"], m["lateWorstPct"] = a3[0]["customer_state"], num(a3[0]["late_pct"], 2)
m["lateWorstDays"], m["lateWorstKm"] = num(a3[0]["avg_delivery_days"], 1), num(a3[0]["avg_distance_km"])
m["lateBestState"], m["lateBestPct"] = a3[-1]["customer_state"], num(a3[-1]["late_pct"], 2)
m["lateBestDays"], m["lateBestKm"] = num(a3[-1]["avg_delivery_days"], 1), num(a3[-1]["avg_distance_km"])
m["nStatesAnalysed"] = str(len(a3))

a4 = {r["delivery"]: r for r in rows("a4_late_vs_review")}
on, late = a4["Tepat waktu"], a4["Terlambat"]
table("a4", "lrrr", ["Pengiriman", "Pesanan", "Rata-rata review", "Review 1--2 (\\%)"],
      [[k, num(r["orders"]), num(r["avg_review"], 2), num(r["low_review_pct"], 2)] for k, r in a4.items()])
m["onTimeOrders"], m["lateOrders"] = num(on["orders"]), num(late["orders"])
m["onTimeReview"], m["lateReview"] = num(on["avg_review"], 2), num(late["avg_review"], 2)
m["onTimeLow"], m["lateLow"] = num(on["low_review_pct"], 2), num(late["low_review_pct"], 2)
m["latePct"] = num(100 * int(late["orders"]) / (int(late["orders"]) + int(on["orders"])), 2)

a5 = rows("a5_payment_types")
table("a5", "lrrrr", ["Metode", "Pesanan", "Porsi (\\%)", "Bayar rata-rata (BRL)", "Cicilan rata-rata"],
      [[code(r["payment_type"]), num(r["orders"]), num(r["share_pct"], 2), num(r["avg_payment"], 2),
        num(r["avg_installments"], 2)] for r in a5])
pay = {r["payment_type"]: r for r in a5}
m["cardShare"], m["cardInstallments"] = num(pay["credit_card"]["share_pct"], 2), num(pay["credit_card"]["avg_installments"], 2)
m["cardAvgPay"], m["boletoShare"] = num(pay["credit_card"]["avg_payment"], 2), num(pay["boleto"]["share_pct"], 2)

a6 = rows("a6_orders_per_customer")
table("a6", "rrr", ["Pesanan per pelanggan", "Pelanggan", "Porsi (\\%)"],
      [[r["n_orders"], num(r["customers"]), num(r["pct"], 2)] for r in a6])
m["oneTimePct"] = num(a6[0]["pct"], 2)
m["nCustomers"] = num(sum(int(r["customers"]) for r in a6))
m["repeatPct"] = num(100 - float(a6[0]["pct"]), 2)

# ---------- Machine learning ----------
metrics = json.loads((OUT / "ml_metrics.json").read_text())
m["ambang"] = num(metrics["threshold_brl"], 2)
m["nLatih"], m["nUji"] = num(metrics["train_rows"]), num(metrics["test_rows"])
m["nTinggi"], m["nRendah"] = num(metrics["label_counts"]["1"]), num(metrics["label_counts"]["0"])
m["bestModel"] = metrics["model"]
m["dtDepth"], m["dtMinInst"] = str(metrics["dt_max_depth"]), str(metrics["dt_min_instances_per_node"])
m["dtNodes"], m["cvSeconds"] = num(metrics["dt_num_nodes"]), num(metrics["dt_cv_seconds"], 1)

comparison = rows("ml_model_comparison")
table("models", ">{\\raggedright}p{2.5cm}>{\\raggedright\\arraybackslash}p{4cm}rrrrrr",
      ["Model", "Parameter", "F1 CV", "F1 latih", "Akurasi", "F1 uji", "AUC", "Latih (dtk)"],
      [[r["model"], esc(r["params"]).replace(", ", "\\newline "), num(r["cv_f1"], 4) if r["cv_f1"] else "--",
        num(r["train_f1"], 4), num(r["test_accuracy"], 4), num(r["test_f1"], 4), num(r["test_auc"], 4),
        num(r["fit_seconds"], 1)] for r in comparison])
for r in comparison:
    p = MODELS[r["model"]]
    m[p + "Acc"], m[p + "Fone"], m[p + "Auc"] = num(r["test_accuracy"], 4), num(r["test_f1"], 4), num(r["test_auc"], 4)
    m[p + "TrainFone"], m[p + "Fit"] = num(r["train_f1"], 4), num(r["fit_seconds"], 1)
    m[p + "CvFone"] = num(r["cv_f1"], 4) if r["cv_f1"] else "--"
    mm = metrics["metrics_per_model"][r["model"]]
    for metric, short in [("precisionByLabel", "Prec"), ("recallByLabel", "Rec"), ("fMeasureByLabel", "Fm")]:
        for label in ("tinggi", "rendah"):
            m[p + short + label.title()] = num(mm[f"{metric}_{label}"], 4)
by_name = {r["model"]: r for r in comparison}
gain = lambda a, b: num(100 * (float(by_name[a]["test_f1"]) - float(by_name[b]["test_f1"])), 1)
m["gainFeatures"] = gain("Decision Tree", "Decision Tree (fitur dasar)")
m["gainRf"], m["gainGbt"] = gain("Random Forest", "Decision Tree"), gain("GBT", "Decision Tree")
m["rfTimesSlower"] = num(float(by_name["Random Forest"]["fit_seconds"]) / float(by_name["Decision Tree"]["fit_seconds"]))
m["dtGap"] = num(100 * (float(by_name["Decision Tree"]["train_f1"]) - float(by_name["Decision Tree"]["test_f1"])), 1)
m["bestMacro"] = MODELS[metrics["model"]]

cv = rows("ml_depth_cv")
depths = sorted({int(r["max_depth"]) for r in cv})
mins = sorted({int(r["min_instances_per_node"]) for r in cv})
f1_at = {(int(r["max_depth"]), int(r["min_instances_per_node"])): float(r["cv_f1"]) for r in cv}
table("cv", "r" + "r" * len(mins), ["maxDepth"] + [f"min={k}" for k in mins],
      [[str(d)] + [("\\textbf{%s}" if (d, k) == (metrics["dt_max_depth"], metrics["dt_min_instances_per_node"])
                    else "%s") % num(f1_at[(d, k)], 4) for k in mins] for d in depths])
m["cvFoneDepthTwo"] = num(f1_at[(2, 1)], 4)
m["cvFoneBest"] = num(f1_at[(metrics["dt_max_depth"], metrics["dt_min_instances_per_node"])], 4)
m["cvFoneFifteen"] = num(f1_at[(15, 1)], 4)
m["cvFoneTwentyFive"] = num(f1_at[(25, 1)], 4)
m["cvFoneMinFifty"] = num(f1_at[(metrics["dt_max_depth"], 50)], 4)

cm = {(r["label"], r["prediction"]): int(r["count"]) for r in rows("ml_confusion_matrix")}
m["cmTT"], m["cmTR"] = num(cm[("1.0", "1.0")]), num(cm[("1.0", "0.0")])
m["cmRR"], m["cmRT"] = num(cm[("0.0", "0.0")]), num(cm[("0.0", "1.0")])

imp = rows("ml_feature_importance")
table("importance", "rlr", ["No", "Fitur", "Importance"],
      [[str(i), code(r["feature"]), num(r["importance"], 4)] for i, r in enumerate(imp[:10], 1)])
for letter, r in zip("ABCDE", imp):
    m["imp" + letter], m["impVal" + letter] = code(r["feature"]), num(r["importance"], 4)

scale = rows("ml_scale_experiment")
table("scale", "rrrrr", ["Skala data latih", "Baris", "Waktu latih (dtk)", "Simpangan (dtk)", "F1 uji"],
      [[(f"{num(100 * float(r['scale']))}\\%" if float(r["scale"]) <= 1 else f"$\\times${num(r['scale'])} (duplikasi)"),
        num(r["train_rows"]), num(r["fit_seconds_mean"], 2), num(r["fit_seconds_std"], 2),
        num(r["test_f1"], 4) if r["test_f1"] else "--"] for r in scale])
s = {float(r["scale"]): r for r in scale}
m["scaleTimeMin"], m["scaleFoneMin"] = num(s[0.1]["fit_seconds_mean"], 2), num(s[0.1]["test_f1"], 4)
m["scaleTimeHalf"], m["scaleFoneHalf"] = num(s[0.5]["fit_seconds_mean"], 2), num(s[0.5]["test_f1"], 4)
m["scaleTimeFull"], m["scaleFoneFull"] = num(s[1.0]["fit_seconds_mean"], 2), num(s[1.0]["test_f1"], 4)
m["scaleTimeMax"], m["scaleRowsMax"] = num(s[4.0]["fit_seconds_mean"], 2), num(s[4.0]["train_rows"])
m["scaleTimeDouble"] = num(s[2.0]["fit_seconds_mean"], 2)
m["scaleRatioFour"] = num(float(s[4.0]["fit_seconds_mean"]) / float(s[1.0]["fit_seconds_mean"]), 1)

shallow = (OUT / "ml_decision_tree_shallow.txt").read_text()
m["shallowNodes"] = re.search(r"numNodes=(\d+)", shallow)[1]

# ---------- Screenshot terminal: satu file per log, berisi semua potongan gambarnya ----------
# Dokumen memanggil \shots{nama}{keterangan}; \shotcaption dan \shotname diisi oleh makro itu.
manifest = json.loads((OUT / "screenshots" / "manifest.json").read_text())
for name, files in manifest.items():
    parts = []
    for k, f in enumerate(files, 1):
        part = f" ({k}/{len(files)})" if len(files) > 1 else ""
        parts.append(f"\\begin{{figure}}[H]\\centering\\includegraphics[max width=\\textwidth]{{{f}}}"
                     f"\\caption{{\\shotcaption{{}}{part}}}\\label{{fig:\\shotname-{k}}}\\end{{figure}}")
    (TEX / f"shot_{name}.tex").write_text("\n".join(parts) + "\n")

macros = "\n".join(f"\\newcommand{{\\{k}}}{{{v}}}" for k, v in sorted(m.items()))
(TEX / "angka.tex").write_text("% Dibuat otomatis oleh tools/latex_data.py dari output pipeline. Jangan diedit manual.\n"
                               + macros + "\n")
assert all(re.fullmatch(r"[A-Za-z]+", k) for k in m), [k for k in m if not re.fullmatch(r"[A-Za-z]+", k)]
print(len(m), "makro,", len(list(TEX.glob("tab_*.tex"))), "tabel ->", TEX)
