"""Tahap 5-6: klasifikasi Pembelian Tinggi vs Rendah dengan Spark MLlib. Decision Tree (algoritma yang
disarankan) di-tuning dengan cross-validation lalu dibandingkan dengan Random Forest dan GBT. Model terbaik
disimpan ke HDFS dan prediksinya ke HDFS + MongoDB. Termasuk eksperimen skala data."""
import json
import os
import re
import statistics
import time
from datetime import datetime, timezone

import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Row
from pyspark.sql import functions as F

from common import HDFS, OUT, SEED, save_csv, save_fig, spark_session

MONGO = "mongodb://mongo:27017"
CATEGORICAL = ["category", "payment_type", "customer_state", "seller_state"]
BASE_NUMERIC = ["n_items", "n_sellers", "total_freight", "avg_weight_g", "avg_volume_cm3", "avg_photos",
                "avg_desc_len", "max_installments", "n_payments", "distance_km", "estimated_days",
                "purchase_month", "purchase_dow", "purchase_hour"]
ENGINEERED = ["avg_name_len", "avg_length_cm", "avg_height_cm", "avg_width_cm", "total_weight_g",
              "freight_per_item", "freight_per_kg", "freight_per_km"]
NUMERIC = BASE_NUMERIC + ENGINEERED
# Tidak dipakai sebagai fitur: total_price & payment_value membentuk label (bocor),
# review_score, delivery_days, is_late baru diketahui setelah pesanan selesai.
DEPTHS = [2, 4, 6, 8, 10, 12, 15, 20, 25]
MIN_INSTANCES = [1, 20, 50]  # daun minimal berisi n pesanan; nilai besar menahan overfitting
SHALLOW_DEPTH = 4  # pohon kecil yang aturannya masih bisa dibaca di artikel
SCALES = [0.1, 0.25, 0.5, 1, 2, 4]  # > 1 = data latih diduplikasi untuk menguji waktu di atas ukuran asli
REPEATS = 3

spark = spark_session("olist-classification")

print("#### 1. Data dan label")
orders = spark.read.parquet(f"{HDFS}/curated/orders")
threshold = orders.approxQuantile("total_price", [0.5], 0.0)[0]
data = orders.select(
    "order_id", *CATEGORICAL, *[F.col(c).cast("double") for c in NUMERIC],
    (F.col("total_price") >= threshold).cast("double").alias("label"),
)
# Pohon mengambil sampel baris untuk ambang split (dan Random Forest untuk bootstrap) per partisi, jadi
# hasilnya bergantung pada urutan baris. Urutan dari Parquet bisa berubah tiap ETL dijalankan ulang;
# partisi hash + urut per order_id membuat setiap run melihat data yang persis sama.
data = data.repartition(12, "order_id").sortWithinPartitions("order_id")
# Split & fold dari hash order_id, bukan randomSplit: hasil randomSplit berubah kalau partisi Parquet berubah.
in_test = F.pmod(F.xxhash64("order_id"), F.lit(100)) < 20
fold = F.pmod(F.xxhash64(F.lit(SEED), "order_id"), F.lit(3)).cast("int")
train = data.filter(~in_test).withColumn("fold", fold).cache()
test = data.filter(in_test).cache()
n_train, n_test = train.count(), test.count()
label_counts = {int(r.label): r["count"] for r in data.groupBy("label").count().collect()}
print(f"Ambang Pembelian Tinggi (median total_price): {threshold:.2f} BRL")
print(f"Label: {label_counts} | latih {n_train:,} | uji {n_test:,}")
print(f"Fitur: {len(NUMERIC)} numerik + {len(CATEGORICAL)} kategorikal")


def make_pipeline(classifier, numeric=NUMERIC):
    indexers = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in CATEGORICAL]
    imputer = Imputer(inputCols=numeric, outputCols=[c + "_imp" for c in numeric], strategy="median")
    assembler = VectorAssembler(inputCols=imputer.getOutputCols() + [c + "_idx" for c in CATEGORICAL],
                                outputCol="features")
    return Pipeline(stages=indexers + [imputer, assembler, classifier])


# maxBins harus >= jumlah kategori terbanyak (category ~74 nilai)
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxBins=128, seed=SEED)
pipeline = make_pipeline(dt)
feature_names = NUMERIC + CATEGORICAL

f1_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")


def evaluate(pred):
    m = {name: MulticlassClassificationEvaluator(labelCol="label", metricName=name).evaluate(pred)
         for name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]}
    for label, label_name in [(1.0, "tinggi"), (0.0, "rendah")]:
        for metric in ["precisionByLabel", "recallByLabel", "fMeasureByLabel"]:
            m[f"{metric}_{label_name}"] = MulticlassClassificationEvaluator(
                labelCol="label", metricName=metric, metricLabel=label).evaluate(pred)
    # rawPrediction pohon berisi jumlah sampel di daun, jadi AUC dihitung dari probability.
    m["auc"] = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability").evaluate(pred)
    return {k: round(v, 4) for k, v in m.items()}


def readable_rules(model):
    """toDebugString dengan nama fitur & kategori asli, bukan 'feature 15 in {3.0,7.0}'."""
    labels = [indexer.labels for indexer in model.stages[:len(CATEGORICAL)]]

    def category_set(m):
        i = int(m.group(1))
        names = labels[i - len(NUMERIC)]
        # Indeks == jumlah label adalah keranjang handleInvalid="keep" (nilai kosong/baru).
        everything = names + ["<lainnya>"]
        shown = [everything[int(float(v))] for v in m.group(3).split(",")]
        op = m.group(2)
        # "in {66 kategori}" sama dengan "not in {8 kategori sisanya}"; tampilkan yang lebih pendek.
        if len(shown) > len(everything) / 2:
            shown = [c for c in everything if c not in shown]
            op = "in" if op == "not in" else "not in"
        return f"{feature_names[i]} {op} {{{', '.join(shown)}}}"

    text = re.sub(r"feature (\d+) (not in|in) \{([^}]*)\}", category_set, model.stages[-1].toDebugString)
    text = re.sub(r"feature (\d+)", lambda m: feature_names[int(m.group(1))], text)
    text = re.sub(r"(\d+\.\d{3})\d+", r"\1", text)
    return text.replace("Predict: 1.0", "Predict: Pembelian Tinggi").replace("Predict: 0.0", "Predict: Pembelian Rendah")


print("\n#### 2. Tuning Decision Tree: maxDepth x minInstancesPerNode (3-fold cross-validation)")
grid = (ParamGridBuilder().addGrid(dt.maxDepth, DEPTHS)
        .addGrid(dt.minInstancesPerNode, MIN_INSTANCES).build())
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=f1_eval,
                    numFolds=3, foldCol="fold", parallelism=4)
t = time.perf_counter()
cv_model = cv.fit(train)
cv_seconds = time.perf_counter() - t
cv_rows = [(p[dt.maxDepth], p[dt.minInstancesPerNode], round(float(m), 4))
           for p, m in zip(grid, cv_model.avgMetrics)]
save_csv(spark.createDataFrame(cv_rows, "max_depth int, min_instances_per_node int, cv_f1 double"), "ml_depth_cv")
tree = cv_model.bestModel.stages[-1]
best_depth, best_min = tree.getMaxDepth(), tree.getMinInstancesPerNode()
best_params = {dt.maxDepth: best_depth, dt.minInstancesPerNode: best_min}
print(f"CV {len(grid)} kombinasi selesai {cv_seconds:.1f} dtk | terbaik: maxDepth={best_depth}, "
      f"minInstancesPerNode={best_min}, {tree.numNodes} node")
for d in DEPTHS:
    print(f"  maxDepth={d:>2}: F1 CV " + "  ".join(f"min{k}={f:.4f}" for dd, k, f in cv_rows if dd == d))

print("\n#### 3. Perbandingan fitur dan model (data uji)")
# Tiap task lokal (1 per core) boleh memakai maxMemoryInMB untuk statistik split; default 256 MB x 12 core
# membuat heap 4 GB kehabisan memori pada Random Forest yang dalam.
rf = RandomForestClassifier(labelCol="label", featuresCol="features", maxBins=128, maxMemoryInMB=64, seed=SEED)
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxBins=128, seed=SEED)
cv_f1_of = {(d, k): f for d, k, f in cv_rows}
# (nama, pipeline, parameter, F1 CV). RF & GBT memakai konfigurasi umum tanpa CV: satu fit RF butuh
# ~1,5 menit, jadi CV penuh menambah ±25 menit. Hanya Decision Tree yang di-tuning.
candidates = [
    ("Decision Tree dangkal", pipeline, {dt.maxDepth: SHALLOW_DEPTH}, cv_f1_of[(SHALLOW_DEPTH, 1)]),
    # Parameter sama dengan pohon terbaik, hanya fiturnya yang dikurangi: memperlihatkan efek fitur rekayasa.
    ("Decision Tree (fitur dasar)", make_pipeline(dt, BASE_NUMERIC), best_params, None),
    ("Decision Tree", pipeline, best_params, cv_f1_of[(best_depth, best_min)]),
    ("Random Forest", make_pipeline(rf), {rf.numTrees: 50, rf.maxDepth: 20}, None),
    ("GBT", make_pipeline(gbt), {gbt.maxIter: 100, gbt.maxDepth: 6}, None),
]
comparison, fitted, full_metrics = [], {}, {}
for name, pipe, params, cv_f1 in candidates:
    t = time.perf_counter()
    model = pipe.fit(train, params)
    fit_seconds = time.perf_counter() - t
    m = evaluate(model.transform(test))
    comparison.append((name, ", ".join(f"{p.name}={v}" for p, v in params.items()), cv_f1,
                       round(f1_eval.evaluate(model.transform(train)), 4),
                       m["accuracy"], m["f1"], m["auc"], round(fit_seconds, 2)))
    fitted[name], full_metrics[name] = model, m
    print(f"{name}: akurasi {m['accuracy']}, F1 {m['f1']}, AUC {m['auc']}, latih {fit_seconds:.1f} dtk")
save_csv(spark.createDataFrame(comparison, "model string, params string, cv_f1 double, train_f1 double, "
                                           "test_accuracy double, test_f1 double, test_auc double, "
                                           "fit_seconds double"), "ml_model_comparison")
# Dipilih dengan F1 data uji; selisih antarmodel (>= 1 poin) jauh di atas galat sampling F1 untuk
# ~20 ribu pesanan uji (sekitar 0,3 poin).
best_row = max(comparison, key=lambda r: r[5])
best_name = best_row[0]
best_model, dt_model = fitted[best_name], fitted["Decision Tree"]
dt_row = next(r for r in comparison if r[0] == "Decision Tree")
print(f"Model terbaik: {best_name} (F1 uji {best_row[5]})")
print("Metrik model terbaik:", json.dumps(full_metrics[best_name], indent=2))

with open(os.path.join(OUT, "ml_decision_tree.txt"), "w") as f:
    f.write(readable_rules(dt_model))
with open(os.path.join(OUT, "ml_decision_tree_shallow.txt"), "w") as f:
    f.write(readable_rules(fitted["Decision Tree dangkal"]))

pred = best_model.transform(test).cache()
confusion = save_csv(pred.groupBy("label", "prediction").count().orderBy("label", "prediction"),
                     "ml_confusion_matrix")
# Nama fitur dibaca dari VectorAssembler model itu sendiri ("x_imp" / "x_idx" -> "x").
importance_names = [c.rsplit("_", 1)[0] for c in best_model.stages[-2].getInputCols()]
importances = sorted(zip(importance_names, best_model.stages[-1].featureImportances.toArray()),
                     key=lambda x: -x[1])
save_csv(spark.createDataFrame([(n, round(float(v), 4)) for n, v in importances],
                               "feature string, importance double"), "ml_feature_importance")


print("\n#### 4. Eksperimen skala data (Decision Tree, rata-rata 3 kali)")
# Satu fit Random Forest ±100 dtk, jadi 6 skala x 3 ulangan terlalu lama; eksperimen memakai Decision Tree.
def scaled(df, factor):
    if factor < 1:
        return df.filter(F.pmod(F.xxhash64(F.lit(SEED + 1), "order_id"), F.lit(1000)) < factor * 1000)
    out = df
    for _ in range(int(factor) - 1):
        out = out.union(df)
    return out


pipeline.fit(train.limit(1000), best_params)  # pemanasan JVM agar run pertama tidak lebih lambat
scale_rows = []
for factor in SCALES:
    part = scaled(train, factor).cache()
    n = part.count()
    times, f1s = [], []
    for _ in range(REPEATS):
        t = time.perf_counter()
        model = pipeline.fit(part, best_params)
        times.append(time.perf_counter() - t)
        # Data duplikat tidak menambah informasi, jadi F1 hanya dilaporkan untuk skala <= 1.
        if factor <= 1:
            f1s.append(f1_eval.evaluate(model.transform(test)))
    part.unpersist()
    f1 = round(statistics.mean(f1s), 4) if f1s else None
    scale_rows.append((float(factor), n, round(statistics.mean(times), 2), round(statistics.stdev(times), 2), f1))
    print(f"skala {factor}: {n:,} baris, {statistics.mean(times):.2f} dtk, F1 uji {f1}")
save_csv(spark.createDataFrame(scale_rows, "scale double, train_rows long, fit_seconds_mean double, "
                                           "fit_seconds_std double, test_f1 double"), "ml_scale_experiment")

print("\n#### 5. Menyimpan model dan prediksi (HDFS + MongoDB)")
best_model.write().overwrite().save(f"{HDFS}/models/best_model")
dt_model.write().overwrite().save(f"{HDFS}/models/decision_tree")
category = {1.0: "Pembelian Tinggi", 0.0: "Pembelian Rendah"}
predictions = pred.select(
    F.col("order_id").alias("id_transaksi"),
    F.when(F.col("prediction") == 1, category[1.0]).otherwise(category[0.0]).alias("kategori_prediksi"),
    F.when(F.col("label") == 1, category[1.0]).otherwise(category[0.0]).alias("kategori_aktual"),
    F.round(vector_to_array("probability")[1], 4).alias("prob_tinggi"),
    F.lit(best_name).alias("model"),
    F.lit("Spark MLlib").alias("sumber"),
)
predictions.write.mode("overwrite").parquet(f"{HDFS}/predictions/best_model")
print(f"Model disimpan: {HDFS}/models/best_model ({best_name}), {HDFS}/models/decision_tree")


def mongo(collection):
    return {"connection.uri": MONGO, "database": "olist", "collection": collection}


predictions.write.format("mongodb").options(**mongo("predictions")).mode("overwrite").save()
run = dict(full_metrics[best_name], run_at=datetime.now(timezone.utc).isoformat(), model=best_name,
           train_f1=best_row[3], threshold_brl=threshold, train_rows=n_train, test_rows=n_test,
           dt_max_depth=best_depth, dt_min_instances_per_node=best_min, dt_num_nodes=dt_model.stages[-1].numNodes,
           dt_test_f1=dt_row[5], dt_train_f1=dt_row[3], dt_cv_seconds=round(cv_seconds, 2))
spark.createDataFrame([Row(**run)]).write.format("mongodb").options(**mongo("metrics")).mode("append").save()

stored = spark.read.format("mongodb").options(**mongo("predictions")).load()
print(f"MongoDB olist.predictions: {stored.count():,} dokumen. Contoh prediksi salah:")
stored.filter("kategori_prediksi != kategori_aktual").select(
    "id_transaksi", "kategori_prediksi", "kategori_aktual", "prob_tinggi").show(5, truncate=False)

with open(os.path.join(OUT, "ml_metrics.json"), "w") as f:
    json.dump(dict(run, label_counts=label_counts, cv=cv_rows, comparison=comparison,
                   metrics_per_model=full_metrics), f, indent=2)

# ---------- Grafik ----------
fig, ax = plt.subplots(figsize=(6, 4))
for k in MIN_INSTANCES:
    rows = [r for r in cv_rows if r[1] == k]
    ax.plot([r[0] for r in rows], [r[2] for r in rows], marker="o", label=f"minInstancesPerNode={k}")
ax.set(title="F1 cross-validation Decision Tree vs maxDepth", xlabel="maxDepth", ylabel="F1 (3-fold)")
ax.legend()
save_fig(fig, "ml_depth_cv")

fig, ax = plt.subplots(figsize=(7, 7))
ax.barh([n for n, _ in importances][::-1], [v for _, v in importances][::-1])
ax.set(title=f"Feature importance {best_name}", xlabel="Importance")
save_fig(fig, "ml_feature_importance")

cm = [[0, 0], [0, 0]]
for r in confusion:
    cm[int(r.label)][int(r.prediction)] = r["count"]
fig, ax = plt.subplots(figsize=(4.5, 4))
ax.imshow(cm, cmap="Blues")
for i in range(2):
    for j in range(2):
        ax.text(j, i, f"{cm[i][j]:,}", ha="center", va="center", color="white" if i == j else "black")
ax.set(xticks=[0, 1], yticks=[0, 1], xticklabels=["Rendah", "Tinggi"], yticklabels=["Rendah", "Tinggi"],
       xlabel="Prediksi", ylabel="Aktual", title=f"Confusion matrix {best_name}")
save_fig(fig, "ml_confusion_matrix")

fig, ax = plt.subplots(figsize=(6, 4))
ax.errorbar([r[1] for r in scale_rows], [r[2] for r in scale_rows], yerr=[r[3] for r in scale_rows], marker="o")
ax.set(title=f"Waktu latih Decision Tree vs jumlah data (rata-rata {REPEATS}x)",
       xlabel="Jumlah baris data latih", ylabel="Detik")
save_fig(fig, "ml_scale_experiment")

fig, ax = plt.subplots(figsize=(7, 3.5))
names, f1s = [r[0] for r in comparison][::-1], [r[5] for r in comparison][::-1]
ax.barh(names, f1s)
for y, v in enumerate(f1s):
    ax.text(v + 0.005, y, f"{v:.3f}", va="center")
ax.set(title="F1 data uji per model", xlabel="F1", xlim=(0.5, 1))
save_fig(fig, "ml_model_comparison")
