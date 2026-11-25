import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

HDFS = "hdfs://namenode:8020/olist"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "output")
SEED = 42


def spark_session(app):
    return SparkSession.builder.appName(app).getOrCreate()


def save_csv(df, name):
    """Simpan hasil kecil ke output/<name>.csv dan kembalikan barisnya untuk grafik."""
    rows = df.collect()
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name + ".csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows(rows)
    return rows


def save_fig(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, name + ".png"), dpi=150)
    plt.close(fig)
