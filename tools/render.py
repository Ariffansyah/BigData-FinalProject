"""Mengubah log terminal di output/logs menjadi gambar jendela terminal (output/screenshots/term_*.png).
Output spark-submit dipecah per tahap memakai penanda '#### ' yang dicetak job. Diadaptasi dari prak9."""
import html
import json
import math
import pathlib
import re
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
LOGS, OUT = ROOT / "output" / "logs", ROOT / "output" / "screenshots"
SINGLE_MAX, WIDE = 58, 96

TITLES = {
    "01_compose_ps": "Container yang berjalan (podman compose)",
    "02_hdfs": "Menyimpan dataset mentah ke HDFS",
    "03_etl_1": "ETL 1: Membaca dataset dari HDFS dan memeriksa kualitas data",
    "03_etl_2": "ETL 2: Pembersihan data",
    "03_etl_3": "ETL 3: Menyimpan tabel per pesanan ke HDFS",
    "03_etl_4": "ETL 4: Analisis dengan Spark SQL",
    "04_ml_1": "MLlib 1: Data dan label",
    "04_ml_2": "MLlib 2: Tuning Decision Tree dengan cross-validation",
    "04_ml_3": "MLlib 3: Perbandingan fitur dan model",
    "04_ml_4": "MLlib 4: Eksperimen skala data",
    "04_ml_5": "MLlib 5: Menyimpan model dan prediksi ke HDFS dan MongoDB",
    "05_hdfs_output": "Hasil pengolahan di HDFS",
    "06_mongo": "Membaca kembali hasil prediksi dari MongoDB",
}


def crop(name, lines):
    """Skema tabel pertama ditampilkan utuh; skema tabel lain diringkas (sudah ada di data_quality.csv)."""
    if name != "03_etl_1":
        return lines
    out, roots, in_schema = [], 0, False
    for l in lines:
        if l == "root" or l.strip().startswith("|--"):
            roots += l == "root"
            if roots <= 1:
                out.append(l)
            elif not in_schema:
                out.append("#MARK#[skema kolom dipotong]")
            in_schema = True
        else:
            in_schema = False
            out.append(l)
    return out


def sections(name):
    """Pecah log spark-submit per tahap '#### N.'; bagian sebelum penanda pertama (perintah) jadi _0."""
    raw = (LOGS / f"{name}.txt").read_text().rstrip("\n").split("\n")
    out, cur = {f"{name}_0": []}, f"{name}_0"
    for l in raw:
        m = re.match(r"#### (\d+)\.", l)
        if m:
            cur = f"{name}_{m[1]}"
            out[cur] = []
        out[cur].append(l)
    # baris perintah spark-submit ikut ke tahap pertama supaya gambar pertama memperlihatkan perintahnya
    first = f"{name}_1"
    out[first] = out.pop(f"{name}_0") + out[first]
    return out


def vis(lines, cols):
    return sum(max(1, math.ceil(len(l.expandtabs()) / cols)) for l in lines)


def split(lines):
    """Log panjang dipecah menjadi beberapa gambar berurutan dengan tinggi seimbang."""
    if vis(lines, WIDE) <= SINGLE_MAX:
        return [lines]
    n = math.ceil(vis(lines, WIDE) / SINGLE_MAX)
    while True:
        cap = math.ceil(vis(lines, WIDE) / n)
        chunks = [[]]
        for l in lines:
            if chunks[-1] and vis(chunks[-1] + [l], WIDE) > cap:
                chunks.append([])
            chunks[-1].append(l)
        if len(chunks) <= n and all(vis(c, WIDE) <= SINGLE_MAX for c in chunks):
            return chunks
        n += 1


def fmt(line):
    if line.startswith("#MARK#"):
        return f'<span class="mark">{html.escape(line[6:])}</span>'
    if line.startswith("$ "):
        return f'<span class="pr">$</span> <span class="cmd">{html.escape(line[2:])}</span>'
    if line.startswith("olist> "):
        return f'<span class="pr">olist&gt;</span> <span class="cmd">{html.escape(line[7:])}</span>'
    if line.startswith("#### "):
        return f'<span class="batch">{html.escape(line)}</span>'
    if " WARN " in line or line.startswith(("Traceback", "py4j.", ": java.", "Error:")):
        return f'<span class="warn">{html.escape(line)}</span>'
    return html.escape(line)


PAGE = """<!doctype html><meta charset="utf-8"><style>
body{{margin:0;background:#fff;padding:6px}}
.win{{display:inline-block;background:#1e1f29;border-radius:10px;overflow:hidden}}
.bar{{background:#2b2d3a;padding:10px 16px;font:15px sans-serif;color:#c0c4d0;display:flex;align-items:center;gap:8px}}
.d{{width:12px;height:12px;border-radius:50%;display:inline-block}}
.t{{margin-left:10px}}
pre{{margin:0;padding:16px 18px;font:14px/1.3 "JetBrains Mono","DejaVu Sans Mono",monospace;color:#d4d6dd;
white-space:pre-wrap;word-break:break-all;max-width:{cols}ch;tab-size:4}}
.pr{{color:#6fd3e8}} .cmd{{color:#f2f2f2}} .mark{{color:#8a8fa3;font-style:italic}}
.batch{{color:#f1c76b}} .warn{{color:#e89a8a}}
</style><div class="win"><div class="bar"><span class="d" style="background:#ff5f57"></span>
<span class="d" style="background:#febc2e"></span><span class="d" style="background:#28c840"></span>
<span class="t">{title}</span></div><pre>{body}</pre></div>"""


def render():
    manifest = {}
    tmp = pathlib.Path(tempfile.mkdtemp())
    logs = {l.stem: l.read_text().rstrip("\n").split("\n") for l in sorted(LOGS.glob("*.txt"))
            if l.stem not in ("03_etl", "04_ml")}
    logs.update(sections("03_etl"))
    logs.update(sections("04_ml"))
    for old in OUT.glob("term_*.png"):
        old.unlink()
    for name, lines in sorted(logs.items()):
        # printSchema() dan print("\n...") menghasilkan baris kosong ganda; cukup satu.
        lines = [l for i, l in enumerate(lines) if l.strip() or (i and lines[i - 1].strip())]
        chunks = split(crop(name, lines))
        files = []
        for k, chunk in enumerate(chunks, 1):
            suffix = f"_{k}" if len(chunks) > 1 else ""
            t = TITLES[name] + (f" ({k}/{len(chunks)})" if len(chunks) > 1 else "")
            page = tmp / f"{name}{suffix}.html"
            # tabel show() yang lebar tidak dilipat; gambarnya dikecilkan di LaTeX dengan max width
            cols = max([WIDE] + [len(l) for l in chunk if l.startswith(("+", "|"))])
            page.write_text(PAGE.format(cols=cols, title=html.escape(t),
                                        body="\n".join(fmt(l) for l in chunk)))
            png = OUT / f"term_{name}{suffix}.png"
            h = 140 + 20 * vis(chunk, cols)
            subprocess.run(["chromium", "--headless", "--disable-gpu", "--hide-scrollbars",
                            "--force-device-scale-factor=2", f"--window-size={max(1000, cols * 9 + 80)},{h}",
                            f"--screenshot={png}", page.as_uri()],
                           check=True, capture_output=True)
            # DPI yang sama untuk semua gambar terminal supaya ukuran hurufnya seragam di PDF
            subprocess.run(["magick", str(png), "-trim", "+repage", "-units", "PixelsPerInch",
                            "-density", "298.8", str(png)], check=True)
            files.append(png.name)
        manifest[name] = files
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=1))
    print(sum(len(v) for v in manifest.values()), "gambar terminal")


if __name__ == "__main__":
    render()
