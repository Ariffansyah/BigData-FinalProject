"""Menjalankan ulang pipeline (./run.sh per tahap) sambil merekam output terminal ke output/logs dan
mengambil screenshot UI HDFS & Spark ke output/screenshots. Hasilnya dipakai sebagai bukti di artikel.
Pemakaian: python3 tools/capture.py && python3 tools/render.py   (±12 menit, container harus bisa dijalankan)"""
import os
import pathlib
import re
import subprocess
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
LOGS, SHOTS = ROOT / "output" / "logs", ROOT / "output" / "screenshots"
COMPOSE = os.environ.get("COMPOSE", "podman compose")
os.environ["PODMAN_COMPOSE_WARNING_LOGS"] = "false"
ANSI = re.compile(r"\x1b\[[0-9;]*m")
NOISE = re.compile(r"Emulate Docker CLI using podman|external compose provider")


def clean(text):
    return "\n".join(l for l in ANSI.sub("", text).splitlines() if not NOISE.search(l)).rstrip()


def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=ROOT)
    return clean(r.stdout + r.stderr)


def host_log(name, cmds):
    out = []
    for cmd in cmds:
        out += [f"$ {cmd}", sh(cmd)]
    (LOGS / f"{name}.txt").write_text("\n".join(o for o in out if o) + "\n")


def screenshot(name, url, h=1000, w=1400):
    png = SHOTS / f"{name}.png"
    subprocess.run(["chromium", "--headless", "--disable-gpu", "--hide-scrollbars", f"--window-size={w},{h}",
                    "--virtual-time-budget=8000", f"--screenshot={png}", url], check=True, capture_output=True)
    subprocess.run(["magick", str(png), "-trim", "+repage", str(png)], check=True)


def step(name, shots=()):
    """./run.sh <tahap>; stdout ke log. shots = [(penanda, nama, url, tinggi)]: screenshot Spark UI
    diambil saat penanda tahap muncul, karena UI di port 4040 hanya hidup selama job berjalan."""
    out, err = LOGS / f"{name}.txt", LOGS / f"{name}.stderr"
    with open(out, "w") as fo, open(err, "w") as fe:
        p = subprocess.Popen(["./run.sh", name.split("_", 1)[1]], cwd=ROOT, stdout=fo, stderr=fe)
        pending = list(shots)

        def shoot():
            while p.poll() is None and pending:
                if pending[0][0] in out.read_text():
                    time.sleep(5)  # beri waktu job tahap itu tampil di UI
                    screenshot(*pending.pop(0)[1:])
                time.sleep(0.5)

        t = threading.Thread(target=shoot)
        t.start()
        p.wait()
        t.join()
    out.write_text(clean(out.read_text()) + "\n")
    if p.returncode:
        raise SystemExit(f"./run.sh {name} gagal, lihat {out} dan {err}")


LOGS.mkdir(parents=True, exist_ok=True)
SHOTS.mkdir(parents=True, exist_ok=True)
subprocess.run(["./run.sh", "up"], cwd=ROOT, check=True, capture_output=True)
host_log("01_compose_ps", [f"{COMPOSE} ps"])
step("02_hdfs")
screenshot("ui_hdfs_overview", "http://localhost:9870/dfshealth.html#tab-overview", h=1300)
screenshot("ui_hdfs_raw", "http://localhost:9870/explorer.html#/olist/raw", h=750)
step("03_etl")
step("04_ml", [("#### 2.", "ui_spark_jobs", "http://localhost:4040/jobs/", 1300),
               ("#### 3.", "ui_spark_sql", "http://localhost:4040/SQL/", 1100),
               ("Random Forest", "ui_spark_executors", "http://localhost:4040/executors/", 900)])
host_log("05_hdfs_output", [
    f"{COMPOSE} exec -T namenode hdfs dfs -ls /olist /olist/curated /olist/models /olist/predictions",
    f"{COMPOSE} exec -T namenode hdfs dfs -du -h /olist",
])
screenshot("ui_hdfs_olist", "http://localhost:9870/explorer.html#/olist", h=650)
screenshot("ui_hdfs_curated", "http://localhost:9870/explorer.html#/olist/curated/orders", h=900)
step("06_mongo")
print("selesai:", len(list(LOGS.glob("*.txt"))), "log,", len(list(SHOTS.glob("ui_*.png"))), "screenshot UI")
