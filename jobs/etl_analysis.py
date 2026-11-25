"""Tahap 3-4: baca CSV mentah dari HDFS, cek kualitas data, bersihkan, bentuk tabel per pesanan,
simpan ke HDFS (Parquet), lalu jalankan analisis dengan Spark SQL."""
import time

import matplotlib.pyplot as plt
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import HDFS, save_csv, save_fig, spark_session

# nama tabel -> (file CSV, kolom kunci untuk cek duplikat)
TABLES = {
    "customers": ("olist_customers_dataset.csv", ["customer_id"]),
    "geolocation": ("olist_geolocation_dataset.csv", None),
    "order_items": ("olist_order_items_dataset.csv", ["order_id", "order_item_id"]),
    "payments": ("olist_order_payments_dataset.csv", ["order_id", "payment_sequential"]),
    "reviews": ("olist_order_reviews_dataset.csv", ["review_id"]),
    "orders": ("olist_orders_dataset.csv", ["order_id"]),
    "products": ("olist_products_dataset.csv", ["product_id"]),
    "sellers": ("olist_sellers_dataset.csv", ["seller_id"]),
    "category_translation": ("product_category_name_translation.csv", ["product_category_name"]),
}

t_start = time.perf_counter()
spark = spark_session("olist-etl-analysis")

# ---------- 1. Baca dari HDFS + profil kualitas data ----------
print("#### 1. Membaca dataset mentah dari HDFS dan memeriksa kualitas data")
raw, quality = {}, []
for name, (file, key) in TABLES.items():
    # multiLine + escape: komentar review berisi baris baru dan tanda kutip ganda
    df = spark.read.csv(f"{HDFS}/raw/{file}", header=True, inferSchema=True, multiLine=True, escape='"').cache()
    n = df.count()
    print(f"\n=== {name}: {n:,} baris, {len(df.columns)} kolom ===")
    df.printSchema()
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first().asDict()
    quality.append((
        name, n, len(df.columns), sum(nulls.values()),
        "; ".join(f"{c}={v}" for c, v in nulls.items() if v),
        n - df.dropDuplicates().count(),
        n - df.dropDuplicates(key).count() if key else None,
    ))
    raw[name] = df

save_csv(spark.createDataFrame(quality, "table string, rows long, columns int, null_cells long, "
                                        "null_columns string, duplicate_rows long, duplicate_keys long"),
         "data_quality")

# ---------- 2. Pembersihan ----------
print("\n#### 2. Pembersihan data")
log = []


def step(desc, before, after):
    log.append((desc, before, after, before - after))
    print(f"[clean] {desc}: {before:,} -> {after:,}")


ts_cols = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
           "order_delivered_customer_date", "order_estimated_delivery_date"]
orders = raw["orders"].select(
    *[F.to_timestamp(c).alias(c) if c in ts_cols else c for c in raw["orders"].columns]
).dropDuplicates(["order_id"])
n0 = orders.count()
orders = orders.filter(~F.col("order_status").isin("canceled", "unavailable"))
step("orders: buang status canceled/unavailable", n0, orders.count())

delivered, purchased = F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")
bad_dates = orders.filter(delivered < purchased).count()
log.append(("orders: tanggal terima < tanggal beli (dikosongkan)", bad_dates, bad_dates, 0))
orders = orders.withColumn("order_delivered_customer_date", F.when(delivered >= purchased, delivered))

# Satu pesanan bisa punya beberapa review; ambil yang dijawab paling akhir.
latest = Window.partitionBy("order_id").orderBy(F.col("review_answer_timestamp").desc())
reviews = (raw["reviews"].withColumn("review_score", F.col("review_score").cast("int"))
           .filter(F.col("review_score").between(1, 5))
           .withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
           .select("order_id", "review_score"))
step("reviews: 1 review terakhir per pesanan", raw["reviews"].count(), reviews.count())

payments = raw["payments"].filter(F.col("payment_type") != "not_defined")
step("payments: buang payment_type not_defined", raw["payments"].count(), payments.count())
payments = payments.groupBy("order_id").agg(
    F.min_by("payment_type", "payment_sequential").alias("payment_type"),
    F.greatest(F.max("payment_installments"), F.lit(1)).alias("max_installments"),
    F.count("*").alias("n_payments"),
    F.sum("payment_value").alias("payment_value"),
)

geo = raw["geolocation"].dropDuplicates()
step("geolocation: buang baris duplikat", raw["geolocation"].count(), geo.count())
n0 = geo.count()
# Batas kotak Brasil; sebagian koordinat Olist jatuh di luar negeri (data tidak valid).
geo = geo.filter(F.col("geolocation_lat").between(-33.75, 5.27) & F.col("geolocation_lng").between(-73.99, -34.79))
step("geolocation: buang koordinat di luar Brasil", n0, geo.count())
geo = geo.groupBy("geolocation_zip_code_prefix").agg(
    F.avg("geolocation_lat").alias("lat"), F.avg("geolocation_lng").alias("lng"))


def with_geo(df, zip_col, prefix):
    g = geo.select(F.col("geolocation_zip_code_prefix").alias(zip_col),
                   F.col("lat").alias(prefix + "_lat"), F.col("lng").alias(prefix + "_lng"))
    return df.join(g, zip_col, "left")


def haversine_km(lat1, lng1, lat2, lng2):
    dlat, dlng = F.radians(F.col(lat2) - F.col(lat1)), F.radians(F.col(lng2) - F.col(lng1))
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlng / 2) ** 2
    return 2 * 6371 * F.asin(F.sqrt(a))


customers = with_geo(raw["customers"], "customer_zip_code_prefix", "customer")
sellers = with_geo(raw["sellers"], "seller_zip_code_prefix", "seller")

products = raw["products"].join(raw["category_translation"], "product_category_name", "left").select(
    "product_id",
    # Dua kategori tidak ada di tabel terjemahan; pakai nama Portugis, kosong -> unknown.
    F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown")).alias("category"),
    "product_weight_g", "product_photos_qty", "product_description_lenght", "product_name_lenght",
    "product_length_cm", "product_height_cm", "product_width_cm",
    (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")).alias("volume_cm3"),
)
items = (raw["order_items"].join(products, "product_id", "left")
         .join(orders.select("order_id"), "order_id", "left_semi"))

item_agg = items.groupBy("order_id").agg(
    F.count("*").alias("n_items"),
    F.countDistinct("seller_id").alias("n_sellers"),
    F.sum("price").alias("total_price"),
    F.sum("freight_value").alias("total_freight"),
    # Kategori & penjual diambil dari item pertama, bukan item termahal, supaya harga tidak ikut menentukan fitur.
    F.min_by("category", "order_item_id").alias("category"),
    F.min_by("seller_id", "order_item_id").alias("seller_id"),
    F.avg("product_weight_g").alias("avg_weight_g"),
    F.avg("volume_cm3").alias("avg_volume_cm3"),
    F.avg("product_photos_qty").alias("avg_photos"),
    F.avg("product_description_lenght").alias("avg_desc_len"),
    F.avg("product_name_lenght").alias("avg_name_len"),
    F.avg("product_length_cm").alias("avg_length_cm"),
    F.avg("product_height_cm").alias("avg_height_cm"),
    F.avg("product_width_cm").alias("avg_width_cm"),
    F.sum("product_weight_g").alias("total_weight_g"),
)


def ratio(num, den):
    # Penyebut 0/kosong (mis. penjual & pembeli di kode pos yang sama) -> null, nanti diisi median oleh Imputer.
    return F.when(den > 0, num / den)


# Ongkir ditentukan dari berat, ukuran, dan jarak; rasio ini memperlihatkan sisa sinyal yang tidak bisa
# dibentuk sendiri oleh pohon keputusan (pohon hanya memotong satu fitur per node).
distance = haversine_km("customer_lat", "customer_lng", "seller_lat", "seller_lng")
freight = F.col("total_freight")

# ---------- 3. Tabel per pesanan -> HDFS ----------
order_table = (
    orders.join(item_agg, "order_id")  # inner: pesanan tanpa item dibuang
    .join(payments, "order_id", "left")
    .join(customers, "customer_id", "left")
    .join(sellers, "seller_id", "left")
    .join(reviews, "order_id", "left")
    .select(
        "order_id", "customer_unique_id", "order_status", purchased.alias("purchase_ts"),
        "n_items", "n_sellers", "total_price", "total_freight", "category",
        "avg_weight_g", "avg_volume_cm3", "avg_photos", "avg_desc_len",
        "avg_name_len", "avg_length_cm", "avg_height_cm", "avg_width_cm", "total_weight_g",
        ratio(freight, F.col("n_items")).alias("freight_per_item"),
        ratio(freight, F.col("total_weight_g") / 1000).alias("freight_per_kg"),
        ratio(freight, distance).alias("freight_per_km"),
        "payment_type", "max_installments", "n_payments", "payment_value",
        "customer_state", "seller_state",
        distance.alias("distance_km"),
        F.month(purchased).alias("purchase_month"),
        F.dayofweek(purchased).alias("purchase_dow"),
        F.hour(purchased).alias("purchase_hour"),
        F.datediff("order_estimated_delivery_date", purchased).alias("estimated_days"),
        F.datediff(delivered, purchased).alias("delivery_days"),
        # Estimasi hanya berupa tanggal (jam 00:00), jadi bandingkan per tanggal.
        (F.to_date(delivered) > F.to_date("order_estimated_delivery_date")).cast("int").alias("is_late"),
        "review_score",
    )
)
print("\n#### 3. Menyimpan tabel per pesanan ke HDFS (Parquet)")
order_table.write.mode("overwrite").parquet(f"{HDFS}/curated/orders")
items.write.mode("overwrite").parquet(f"{HDFS}/curated/order_items")
save_csv(spark.createDataFrame(log, "step string, before long, after long, removed long"), "cleaning_log")

# ---------- 4. Analisis Spark SQL (dibaca ulang dari HDFS) ----------
spark.read.parquet(f"{HDFS}/curated/orders").createOrReplaceTempView("orders")
spark.read.parquet(f"{HDFS}/curated/order_items").createOrReplaceTempView("items")
print(f"{HDFS}/curated/orders: {spark.table('orders').count():,} baris, {len(spark.table('orders').columns)} kolom")
print("\n#### 4. Analisis dengan Spark SQL")

monthly = save_csv(spark.sql("""
    SELECT date_format(purchase_ts, 'yyyy-MM') AS month, COUNT(*) AS orders,
           ROUND(SUM(total_price), 2) AS revenue, ROUND(AVG(total_price), 2) AS avg_order_value
    FROM orders GROUP BY 1 ORDER BY 1"""), "a1_monthly_trend")

categories = save_csv(spark.sql("""
    SELECT i.category, COUNT(DISTINCT i.order_id) AS orders, ROUND(SUM(i.price), 2) AS revenue,
           ROUND(AVG(i.price), 2) AS avg_item_price, ROUND(AVG(o.review_score), 2) AS avg_review
    FROM items i JOIN orders o USING (order_id)
    GROUP BY i.category ORDER BY revenue DESC LIMIT 10"""), "a2_top_categories")

states = save_csv(spark.sql("""
    SELECT customer_state, COUNT(*) AS delivered_orders, ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
           ROUND(AVG(distance_km)) AS avg_distance_km, ROUND(100 * AVG(is_late), 2) AS late_pct
    FROM orders WHERE is_late IS NOT NULL
    GROUP BY customer_state HAVING COUNT(*) >= 500 ORDER BY late_pct DESC"""), "a3_late_by_state")

late_review = save_csv(spark.sql("""
    SELECT IF(is_late = 1, 'Terlambat', 'Tepat waktu') AS delivery, COUNT(*) AS orders,
           ROUND(AVG(review_score), 2) AS avg_review,
           ROUND(100 * AVG(CAST(review_score <= 2 AS INT)), 2) AS low_review_pct
    FROM orders WHERE is_late IS NOT NULL AND review_score IS NOT NULL
    GROUP BY 1 ORDER BY 1"""), "a4_late_vs_review")

pay = save_csv(spark.sql("""
    SELECT payment_type, COUNT(*) AS orders, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS share_pct,
           ROUND(AVG(payment_value), 2) AS avg_payment, ROUND(AVG(max_installments), 2) AS avg_installments
    FROM orders WHERE payment_type IS NOT NULL
    GROUP BY payment_type ORDER BY orders DESC"""), "a5_payment_types")

# Dasar memilih klasifikasi transaksi, bukan pengelompokan pelanggan.
repeat = save_csv(spark.sql("""
    SELECT n_orders, COUNT(*) AS customers, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM (SELECT customer_unique_id, COUNT(*) AS n_orders FROM orders GROUP BY 1)
    GROUP BY n_orders ORDER BY n_orders"""), "a6_orders_per_customer")

for title, rows in [("A1 Tren bulanan (bulan, pesanan, pendapatan, rata-rata nilai)", monthly),
                    ("A2 10 kategori teratas (kategori, pesanan, pendapatan, harga item, review)", categories),
                    ("A3 Keterlambatan per negara bagian (state, pesanan, hari kirim, jarak, % terlambat)", states),
                    ("A4 Keterlambatan vs review (status, pesanan, rata-rata review, % review 1-2)", late_review),
                    ("A5 Metode pembayaran (metode, pesanan, %, rata-rata bayar, cicilan)", pay),
                    ("A6 Jumlah pesanan per pelanggan (pesanan, pelanggan, %)", repeat)]:
    print(f"\n{title}:")
    for r in rows[:8]:
        print("  ", tuple(r))

# ---------- 5. Grafik ----------
# Bulan awal/akhir dataset hanya berisi beberapa pesanan dan terlihat seperti penurunan tajam.
full_months = [r for r in monthly if r.orders >= 100]
fig, ax = plt.subplots(figsize=(10, 4))
ax.plot([r.month for r in full_months], [r.revenue / 1e3 for r in full_months], marker="o")
ax.set(title="Pendapatan bulanan (ribu BRL, bulan dengan >= 100 pesanan)", xlabel="Bulan", ylabel="Ribu BRL")
ax.tick_params(axis="x", rotation=90)
save_fig(fig, "a1_monthly_trend")

fig, ax = plt.subplots(figsize=(8, 4.5))
ax.barh([r.category for r in categories][::-1], [r.revenue / 1e3 for r in categories][::-1])
ax.set(title="10 kategori dengan pendapatan terbesar", xlabel="Ribu BRL")
save_fig(fig, "a2_top_categories")

fig, ax = plt.subplots(figsize=(9, 4))
ax.bar([r.customer_state for r in states], [r.late_pct for r in states])
ax.set(title="Persentase pesanan terlambat per negara bagian (>= 500 pesanan)", ylabel="% terlambat")
save_fig(fig, "a3_late_by_state")

fig, ax = plt.subplots(figsize=(5, 4))
ax.bar([r.delivery for r in late_review], [r.avg_review for r in late_review])
ax.set(title="Rata-rata skor review", ylabel="Skor (1-5)", ylim=(0, 5))
save_fig(fig, "a4_late_vs_review")

fig, ax = plt.subplots(figsize=(6, 4))
ax.bar([r.payment_type for r in pay], [r.share_pct for r in pay])
ax.set(title="Porsi metode pembayaran", ylabel="% pesanan")
save_fig(fig, "a5_payment_types")

print(f"\nETL + analisis selesai dalam {time.perf_counter() - t_start:.1f} detik")
