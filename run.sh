#!/usr/bin/env bash
# Pipeline: dataset -> HDFS -> Spark (ETL + analisis) -> Spark MLlib (+ MongoDB).
# Pemakaian: ./run.sh [all|data|up|hdfs|etl|ml|mongo|app]   (default: all; app tidak termasuk all)
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE=${COMPOSE:-podman compose}
export PODMAN_COMPOSE_WARNING_LOGS=false

# Cetak perintah sebelum dijalankan, supaya output terminal sekaligus jadi bukti untuk laporan/demo.
x() { echo "\$ $*"; "$@"; }

data() {
  [ -f data/olist/olist_orders_dataset.csv ] && return
  mkdir -p data/olist
  curl -L -o data/brazilian-ecommerce.zip https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce
  unzip -o data/brazilian-ecommerce.zip -d data/olist
}

up() {
  $COMPOSE up -d --build
  # Namenode butuh beberapa detik sebelum menerima koneksi RPC.
  until $COMPOSE exec -T namenode hdfs dfsadmin -safemode wait >/dev/null 2>&1; do sleep 2; done
  # Bukan grep -q: grep -q menutup pipe lebih awal, podman keluar karena SIGPIPE, dan pipefail menganggapnya gagal.
  until $COMPOSE exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep 'Live datanodes (1)' >/dev/null; do sleep 2; done
}

hdfs() {
  x $COMPOSE exec -T namenode hdfs dfs -mkdir -p /olist/raw
  echo "\$ $COMPOSE exec -T namenode bash -c 'hdfs dfs -put -f /data/olist/*.csv /olist/raw/'"
  $COMPOSE exec -T namenode bash -c 'hdfs dfs -put -f /data/olist/*.csv /olist/raw/'
  x $COMPOSE exec -T namenode hdfs dfs -ls -h /olist/raw
  x $COMPOSE exec -T namenode hdfs dfs -du -s -h /olist/raw
}

submit() {
  x $COMPOSE exec -T spark /opt/spark/bin/spark-submit --master 'local[*]' --driver-memory 4g --conf spark.log.level=ERROR "$@"
}

etl() { submit jobs/etl_analysis.py; }

ml() {
  submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.1 --conf spark.jars.ivy=/ivy \
    jobs/train_models.py
}

mongo() {
  echo "\$ $COMPOSE exec -T mongo mongosh --quiet olist"
  for q in 'db.predictions.countDocuments()' \
    'db.predictions.find({}, {_id: 0}).limit(2)' \
    'db.predictions.aggregate([{$group: {_id: {aktual: "$kategori_aktual", prediksi: "$kategori_prediksi"}, n: {$sum: 1}}}, {$sort: {_id: 1}}])' \
    'db.metrics.find({}, {_id: 0, run_at: 1, model: 1, accuracy: 1, f1: 1, auc: 1}).sort({run_at: -1}).limit(2)'; do
    echo "olist> $q"
    $COMPOSE exec -T mongo mongosh --quiet olist --eval "$q"
  done
}

# Aplikasi demo Next.js (app-demo/) yang membaca MongoDB dan hasil Spark SQL: http://localhost:3000
app() {
  cd app-demo
  npm install --no-fund --no-audit
  npm run build
  npm run start
}

case ${1:-all} in
  all) data; up; hdfs; etl; ml; mongo ;;
  data|up|hdfs|etl|ml|mongo|app) "$1" ;;
  *) echo "Pemakaian: $0 [all|data|up|hdfs|etl|ml|mongo|app]"; exit 1 ;;
esac
