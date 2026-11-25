import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

const ROOT = path.join(process.cwd(), "..");
const TITLES = {
  a1_monthly_trend: "Tren pesanan dan pendapatan per bulan",
  a2_top_categories: "10 kategori dengan pendapatan terbesar",
  a3_late_by_state: "Keterlambatan pengiriman per negara bagian",
  a4_late_vs_review: "Keterlambatan dan skor ulasan",
  a5_payment_types: "Metode pembayaran",
  a6_orders_per_customer: "Jumlah pesanan per pelanggan",
};

function dedent(sql) {
  const lines = sql.replace(/^\n/, "").split("\n");
  const indent = Math.min(...lines.filter((l) => l.trim()).map((l) => l.match(/^ */)[0].length));
  return lines.map((l) => l.slice(indent)).join("\n").trim();
}

// Kueri dibaca langsung dari jobs/etl_analysis.py dan hasilnya dari output/*.csv yang ditulis Spark,
// jadi kueri yang tampil pasti kueri yang menghasilkan tabel di sebelahnya.
export function sparkSqlResults() {
  const source = readFileSync(path.join(ROOT, "jobs", "etl_analysis.py"), "utf8");
  return [...source.matchAll(/save_csv\(spark\.sql\("""([\s\S]*?)"""\),\s*"(\w+)"\)/g)].map(([, sql, name]) => {
    const file = path.join(ROOT, "output", `${name}.csv`);
    const result = { name, title: TITLES[name] ?? name, sql: dedent(sql) };
    if (!existsSync(file)) return { ...result, missing: true };
    // CSV hasil analisis tidak berisi koma di dalam nilai, jadi pemisahan per koma sudah cukup.
    const [header, ...rows] = readFileSync(file, "utf8").trim().split("\n").map((l) => l.split(","));
    return { ...result, header, rows };
  });
}
