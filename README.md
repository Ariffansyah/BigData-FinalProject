# Proyek Akhir Big Data: Analisis Penjualan E-Commerce

Kelompok 1, topik **Analisis penjualan e-commerce**: mengelompokkan pelanggan berdasarkan pola pembelian atau mengklasifikasikan kategori transaksi. Algoritma yang disarankan: **Decision Tree** (Spark MLlib).

Alur: Sumber Data → HDFS → Apache Spark → Spark MLlib → Evaluasi → Draf Artikel. Detail lengkap ada di [Panduan Proyek Akhir Big Data 2026.pdf](Panduan%20Proyek%20Akhir%20Big%20Data%202026.pdf).

## Dokumen

- [artikel/artikel.pdf](artikel/artikel.pdf): draf artikel ilmiah (Bahasa Indonesia, gaya jurnal). Nama anggota, NIM, email, dan kontribusi masih berupa `[placeholder]` yang diisi kelompok di `artikel/artikel.tex`.
- [penjelasan/penjelasan.pdf](penjelasan/penjelasan.pdf): materi presentasi (6 halaman): pembagian tugas 4 anggota, bagian big data, fungsi Spark SQL dan MongoDB, aplikasi demo, dan apa yang harus dipelajari tiap anggota beserta pertanyaan yang mungkin muncul.
- [slides/presentasi.html](slides/presentasi.html): slide presentasi (16 slide, buka di browser; panah/spasi untuk pindah, tekan E untuk mengedit teks, Ctrl+S untuk menyimpan). Gambar di `slides/assets/`; huruf dimuat dari Google Fonts, jadi buka sekali saat ada internet. Cadangan statis tanpa animasi: [slides/presentasi.pdf](slides/presentasi.pdf) (16 halaman 16:9).
- `app-demo/`: aplikasi Next.js untuk demo. Membaca prediksi dari MongoDB (`findOne`, `aggregate` dengan `$group`/`$bucket`/`$sample`, `find().sort()`) dan menampilkan hasil Spark SQL beserta kuerinya. Jalankan `./run.sh app`, buka http://localhost:3000 (butuh Node.js 20+, MongoDB sudah terisi lewat `./run.sh ml`).

Hasil utama: Random Forest F1 0,9023 dan AUC 0,9674 pada data uji; Decision Tree (algoritma yang disarankan) F1 0,8455; Decision Tree dangkal (aturan bisa dibaca) F1 0,7525. Angka lengkap ada di `output/ml_model_comparison.csv`.

## Struktur folder

```
run.sh                         menjalankan pipeline (semua atau per tahap)
compose.yaml, hadoop.env       container HDFS, Spark, MongoDB
spark/Containerfile            image Spark + numpy, pandas, matplotlib
jobs/etl_analysis.py           baca HDFS, kualitas data, cleaning, tabel per pesanan, 6 analisis Spark SQL
jobs/train_models.py           Decision Tree (CV), Random Forest, GBT, skala data, simpan ke HDFS + MongoDB
jobs/common.py                 sesi Spark, alamat HDFS, simpan CSV/grafik
tools/capture.py               jalankan ulang pipeline sambil merekam log + screenshot UI (bukti artikel)
tools/render.py                log terminal -> gambar jendela terminal
tools/latex_data.py            CSV/JSON hasil -> tabel dan angka LaTeX untuk kedua dokumen
data/olist/                    9 CSV dataset
output/                        hasil: CSV, grafik, aturan pohon, ml_metrics.json, logs/, screenshots/, latex/
artikel/, penjelasan/          sumber LaTeX dan PDF
slides/                        slide presentasi HTML + gambar
app-demo/                      aplikasi Next.js demo MongoDB + Spark SQL (./run.sh app)
```

## Cara menjalankan

Butuh podman + podman-compose (atau Docker: `COMPOSE="docker compose" ./run.sh`), RAM bebas ±6 GB, dan internet (unduh dataset dan konektor MongoDB saat pertama kali).

```bash
./run.sh          # semua tahap: data -> up -> hdfs -> etl -> ml -> mongo (±10 menit)
./run.sh hdfs     # atau per tahap, untuk demo / screenshot
```

Membuat ulang bukti dan PDF (butuh Chromium, ImageMagick, TeX Live):

```bash
python3 tools/capture.py      # jalankan ulang pipeline + rekam log & screenshot (±12 menit)
python3 tools/render.py       # log -> gambar terminal
python3 tools/latex_data.py   # hasil -> tabel & angka LaTeX
(cd artikel && latexmk -pdf artikel.tex) && (cd penjelasan && latexmk -pdf penjelasan.tex)
```

Pembagian data latih/uji dan urutan baris dibuat deterministik (hash `order_id`), jadi menjalankan ulang ETL + MLlib menghasilkan model dan angka yang sama; yang berubah hanya waktu proses.

| Tahap | Isi | Bukti untuk artikel |
|---|---|---|
| `data` | Unduh Olist ke `data/olist/` (dilewati kalau sudah ada) | |
| `up` | Jalankan namenode, datanode, Spark, MongoDB (`compose.yaml`) | `podman compose ps`, HDFS UI http://localhost:9870 |
| `hdfs` | `hdfs dfs -put` 9 CSV ke `/olist/raw`, lalu `-ls` dan `-du` | output terminal, HDFS UI → Utilities → Browse |
| `etl` | `jobs/etl_analysis.py`: baca dari HDFS, profil kualitas data, cleaning, tabel per pesanan → `/olist/curated` (Parquet), 6 analisis Spark SQL | `output/data_quality.csv`, `cleaning_log.csv`, `a1`–`a6` (.csv/.png) |
| `ml` | `jobs/train_models.py`: Decision Tree, 3-fold CV untuk `maxDepth` × `minInstancesPerNode`, evaluasi, pohon dangkal (depth 4) untuk aturan yang bisa dibaca, perbandingan fitur dasar vs lengkap dan dengan Random Forest & GBT, eksperimen skala data, simpan model terbaik + prediksinya ke HDFS dan MongoDB | `output/ml_*` (.csv/.png/.json), aturan pohon di `ml_decision_tree_shallow.txt`, perbandingan model di `ml_model_comparison`, Spark UI http://localhost:4040 selama job berjalan |
| `mongo` | Baca kembali koleksi `olist.predictions` dan `olist.metrics` lewat mongosh | output terminal |

`./run.sh ml` butuh ±8 menit (Random Forest ±100 detik per pelatihan). Untuk demo langsung, jalankan `ml` sebelum presentasi, lalu tunjukkan `hdfs`, `etl`, dan `mongo` secara langsung.

Isi HDFS sengaja tidak disimpan di volume: `podman compose down` menghapusnya, jalankan ulang `./run.sh hdfs`. Hasil MongoDB tetap tersimpan di volume `mongo`.

Label ML: **Pembelian Tinggi** jika total harga item dalam pesanan ≥ median, selain itu **Pembelian Rendah** (kelas seimbang). `total_price` dan `payment_value` tidak dipakai sebagai fitur karena membentuk label. `review_score`, `delivery_days`, dan `is_late` juga tidak dipakai karena baru diketahui setelah pesanan selesai.

## Dataset

### Pilihan utama: Olist Brazilian E-Commerce

- Link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Isi: sekitar 100 ribu pesanan dari 2016–2018 di marketplace Brasil, terbagi dalam 9 file CSV (pesanan, item, pembayaran, review, pelanggan, penjual, produk, geolokasi, terjemahan kategori).
- Jumlah record (sebagian): order_items 112.650, order_payments 103.886, customers 99.441, sellers 3.095, geolocation 1.000.163.
- Lisensi: CC BY-NC-SA 4.0 (boleh untuk keperluan akademik, wajib cantumkan sumber). Kalau data hasil cleaning ikut dikumpulkan, cantumkan lisensi yang sama.
- Alasan dipilih: fiturnya beragam (cara bayar, cicilan, ongkir, kategori produk, tanggal kirim vs estimasi, skor review), jadi ada label asli untuk Decision Tree. Karena ada 9 tabel yang perlu di-join, pengolahan di Spark jadi bermakna.
- Ide label (unit data: per pesanan):
  - **Pembelian Tinggi vs Rendah**, sesuai contoh di panduan. Labelnya diambil dari total nilai pesanan, jadi `price` dan `payment_value` tidak boleh ikut jadi fitur supaya tidak bocor.
  - Puas atau tidak puas: `review_score` 1–2 dibanding 4–5.
  - Pengiriman terlambat: `order_delivered_customer_date` lebih lambat dari `order_estimated_delivery_date`.
- Hampir semua pelanggan Olist hanya belanja sekali, jadi opsi "mengelompokkan pelanggan berdasarkan pola pembelian" kurang cocok. Lebih tepat pakai opsi "mengklasifikasikan kategori transaksi".
- Jumlah record di atas sudah dicek ulang dengan Spark dari HDFS dan cocok. Data kosong dan duplikat per tabel ada di `output/data_quality.csv`, langkah cleaning di `output/cleaning_log.csv`.

Download (tidak perlu login):

```bash
curl -L -o brazilian-ecommerce.zip https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d data/olist
```

### Alternatif

| Dataset | Link | Ukuran | Lisensi | Catatan |
|---|---|---|---|---|
| UCI Online Retail II | https://archive.ics.uci.edu/dataset/502/online+retail+ii | 1.067.371 transaksi, 8 kolom, .xlsx (Des 2009 – Des 2011, UK) | CC BY 4.0 | Kolomnya sedikit. Label Decision Tree harus dibuat sendiri, misalnya dari RFM, dan berisiko melingkar kalau fiturnya juga RFM. |
| UCI Online Retail | https://archive.ics.uci.edu/dataset/352/online+retail | Versi lama (2010–2011) dari dataset di atas | CC BY 4.0 | Disebut di panduan. |
| REES46 eCommerce Behavior | https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store | 285 juta event, ~4,6 GB (Okt 2019 – Apr 2020) | © Original Authors, wajib sebut Kaggle dan REES46 | Paling "big data", tapi berat untuk laptop. Cukup pakai satu bulan. |

Download Online Retail II:

```bash
curl -L -o online_retail_ii.zip https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip
```

## Sumber dataset lain (dari panduan)

- Kaggle Datasets: https://www.kaggle.com/datasets
- UCI Machine Learning Repository: https://archive.ics.uci.edu/datasets
- Google Dataset Search: https://datasetsearch.research.google.com/
- Hugging Face Datasets: https://huggingface.co/datasets

## Template artikel (dari panduan)

Artikel ditulis dalam Bahasa Indonesia.

1. https://jurnal.yoctobrain.org/index.php/ijodas/AuthorGuidelineAndTemplate
2. https://drive.google.com/file/d/1DnRnTOG5s1Gd_zM887y0jTP587hnHSRx/view
3. https://talenta.usu.ac.id/JoCAI/template
