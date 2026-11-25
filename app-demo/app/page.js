import { MONGO_URI, db } from "../lib/mongo";
import { sparkSqlResults } from "../lib/spark";

export const dynamic = "force-dynamic";

const TINGGI = "Pembelian Tinggi";
const RENDAH = "Pembelian Rendah";
const SPARK_ROWS = 12;

// Setiap kueri didefinisikan sekali: objek yang sama dijalankan ke MongoDB dan ditampilkan di halaman.
const sampleQuery = [{ $sample: { size: 5 } }, { $project: { _id: 0, id_transaksi: 1, kategori_prediksi: 1 } }];
const confusionQuery = [
  { $group: { _id: { aktual: "$kategori_aktual", prediksi: "$kategori_prediksi" }, jumlah: { $sum: 1 } } },
];
const BOUNDARIES = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0001]; // 1.0001 agar prob 1.0 ikut masuk
const bucketQuery = [{ $bucket: { groupBy: "$prob_tinggi", boundaries: BOUNDARIES, output: { jumlah: { $sum: 1 } } } }];
const runsProjection = { _id: 0, run_at: 1, model: 1, accuracy: 1, f1: 1, auc: 1 };

// JSON berindentasi, tetapi array angka dan objek kecil seperti { "$sum": 1 } ditulis satu baris agar kueri ringkas.
const json = (value) =>
  JSON.stringify(value, null, 2)
    .replace(/\[\s+([^[\]{}]*?)\s+\]/g, (_, inner) => `[${inner.split(/,\s+/).join(", ")}]`)
    .replace(/\{\s+("[^"]+": [^{}[\],\n]+)\s+\}/g, "{ $1 }");
const code = (call, ...args) => `${call}(${args.map(json).join(", ")})`;
const fmt = (v, digits = 2) =>
  v === "" || Number.isNaN(Number(v)) ? v : Number(v).toLocaleString("id-ID", { maximumFractionDigits: digits });

async function loadMongo(id) {
  try {
    const predictions = db().collection("predictions");
    const [total, sample, confusion, buckets, runs, found] = await Promise.all([
      predictions.countDocuments(),
      predictions.aggregate(sampleQuery).toArray(),
      predictions.aggregate(confusionQuery).toArray(),
      predictions.aggregate(bucketQuery).toArray(),
      db().collection("metrics").find({}, { projection: runsProjection }).sort({ run_at: -1 }).limit(5).toArray(),
      id ? predictions.findOne({ id_transaksi: id }, { projection: { _id: 0 } }) : null,
    ]);
    return { total, sample, confusion, buckets, runs, found };
  } catch (error) {
    return { error: error.message };
  }
}

function Query({ children }) {
  return <pre className="query">{children}</pre>;
}

function Lookup({ id, mongo }) {
  return (
    <section id="cek" aria-labelledby="cek-title">
      <h2 id="cek-title">Cek prediksi satu transaksi</h2>
      <p className="lead">
        Aplikasi mencari dokumen di koleksi <code>olist.predictions</code> berdasarkan <code>id_transaksi</code>.
        Prediksi tersedia untuk {fmt(mongo.total)} pesanan data uji.
      </p>
      <form method="get" className="search">
        <label htmlFor="id">ID transaksi</label>
        <input id="id" name="id" defaultValue={id} placeholder="ID pesanan (32 karakter)" autoComplete="off" />
        <button type="submit">Cari</button>
      </form>
      <p className="samples">
        Contoh ID acak dari MongoDB:{" "}
        {mongo.sample.map((s) => (
          <a key={s.id_transaksi} href={`/?id=${s.id_transaksi}#cek`}>
            {s.id_transaksi.slice(0, 8)}… ({s.kategori_prediksi.replace("Pembelian ", "")})
          </a>
        ))}
      </p>
      <div className="panel">
        <div>
          <h3>Kueri</h3>
          <Query>{code("db.predictions.findOne", { id_transaksi: id || "<ID>" }, { _id: 0 })}</Query>
          <Query>{code("db.predictions.aggregate", sampleQuery)}</Query>
        </div>
        <div>
          <h3>Hasil</h3>
          {!id && <p className="muted">Masukkan ID atau klik salah satu contoh di atas.</p>}
          {id && !mongo.found && (
            <p className="notice">
              Tidak ada prediksi untuk <code>{id}</code>. Hanya pesanan data uji yang punya prediksi; coba salah satu
              contoh ID di atas.
            </p>
          )}
          {mongo.found && <Prediction doc={mongo.found} />}
        </div>
      </div>
    </section>
  );
}

function Prediction({ doc }) {
  const correct = doc.kategori_prediksi === doc.kategori_aktual;
  const pct = Math.round(doc.prob_tinggi * 100);
  return (
    <div className="result">
      <p className="verdict">
        Prediksi <strong>{doc.kategori_prediksi}</strong>, aktual <strong>{doc.kategori_aktual}</strong>:{" "}
        <span className={correct ? "ok" : "bad"}>{correct ? "benar" : "salah"}</span>
      </p>
      <div className="meter" role="img" aria-label={`Probabilitas Pembelian Tinggi ${pct} persen`}>
        <div style={{ width: `${pct}%` }} />
      </div>
      <p className="muted">
        Probabilitas Pembelian Tinggi {pct}% (di atas 50% diprediksi Tinggi). Model: {doc.model}, {doc.sumber}.
      </p>
      <h4>Dokumen di MongoDB</h4>
      <Query>{JSON.stringify(doc, null, 2)}</Query>
    </div>
  );
}

function Summary({ mongo }) {
  const count = (aktual, prediksi) =>
    mongo.confusion.find((c) => c._id.aktual === aktual && c._id.prediksi === prediksi)?.jumlah ?? 0;
  const correct = count(TINGGI, TINGGI) + count(RENDAH, RENDAH);
  const maxBucket = Math.max(...mongo.buckets.map((b) => b.jumlah));
  return (
    <section id="ringkasan" aria-labelledby="ringkasan-title">
      <h2 id="ringkasan-title">Ringkasan prediksi dengan agregasi MongoDB</h2>
      <p className="lead">
        Angka di bawah dihitung langsung oleh MongoDB dari {fmt(mongo.total)} dokumen prediksi, bukan disalin dari
        laporan.
      </p>

      <div className="panel">
        <div>
          <h3>Aktual vs prediksi</h3>
          <Query>{code("db.predictions.aggregate", confusionQuery)}</Query>
        </div>
        <div>
          <table>
            <caption>
              Akurasi {fmt((100 * correct) / mongo.total, 2)}% ({fmt(correct)} dari {fmt(mongo.total)} benar)
            </caption>
            <thead>
              <tr>
                <th scope="col">Aktual \ Prediksi</th>
                <th scope="col">Tinggi</th>
                <th scope="col">Rendah</th>
              </tr>
            </thead>
            <tbody>
              {[TINGGI, RENDAH].map((a) => (
                <tr key={a}>
                  <th scope="row">{a.replace("Pembelian ", "")}</th>
                  <td className={a === TINGGI ? "hit" : ""}>{fmt(count(a, TINGGI))}</td>
                  <td className={a === RENDAH ? "hit" : ""}>{fmt(count(a, RENDAH))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="panel">
        <div>
          <h3>Seberapa yakin model</h3>
          <Query>{code("db.predictions.aggregate", bucketQuery)}</Query>
        </div>
        <div>
          <p className="muted">Jumlah pesanan per rentang probabilitas Pembelian Tinggi.</p>
          <ul className="bars">
            {mongo.buckets.map((b) => {
              const i = BOUNDARIES.indexOf(b._id);
              return (
                <li key={b._id}>
                  <span>
                    {fmt(BOUNDARIES[i], 1)}–{fmt(Math.min(BOUNDARIES[i + 1], 1), 1)}
                  </span>
                  <div style={{ width: `${(100 * b.jumlah) / maxBucket}%` }} />
                  <span>{fmt(b.jumlah)}</span>
                </li>
              );
            })}
          </ul>
        </div>
      </div>

      <div className="panel">
        <div>
          <h3>Riwayat pelatihan model</h3>
          <Query>{`${code("db.metrics.find", {}, runsProjection)}\n  .sort({ "run_at": -1 }).limit(5)`}</Query>
        </div>
        <div>
          <table>
            <caption>Satu dokumen ditambahkan setiap kali ./run.sh ml dijalankan</caption>
            <thead>
              <tr>
                <th scope="col">Waktu (UTC)</th>
                <th scope="col">Model</th>
                <th scope="col">Akurasi</th>
                <th scope="col">F1</th>
                <th scope="col">AUC</th>
              </tr>
            </thead>
            <tbody>
              {mongo.runs.map((r) => (
                <tr key={r.run_at}>
                  <td>{r.run_at.slice(0, 16).replace("T", " ")}</td>
                  <td>{r.model}</td>
                  <td>{fmt(r.accuracy, 4)}</td>
                  <td>{fmt(r.f1, 4)}</td>
                  <td>{fmt(r.auc, 4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

function SparkSql() {
  let results;
  try {
    results = sparkSqlResults();
  } catch (error) {
    return (
      <section id="sql" aria-labelledby="sql-title">
        <h2 id="sql-title">Hasil analisis Spark SQL</h2>
        <p className="notice">Kode atau hasil analisis tidak terbaca ({error.message}). Jalankan ./run.sh etl.</p>
      </section>
    );
  }
  return (
    <section id="sql" aria-labelledby="sql-title">
      <h2 id="sql-title">Hasil analisis Spark SQL</h2>
      <p className="lead">
        Kueri ini dijalankan Spark di atas tabel Parquet di HDFS (<code>./run.sh etl</code>), lalu hasilnya disimpan
        ke <code>output/*.csv</code>. Kuerinya dibaca langsung dari <code>jobs/etl_analysis.py</code>.
      </p>
      {results.map((r) => (
        <div className="panel" key={r.name}>
          <div>
            <h3>{r.title}</h3>
            <Query>{r.sql}</Query>
          </div>
          <div>
            {r.missing ? (
              <p className="notice">output/{r.name}.csv belum ada. Jalankan ./run.sh etl.</p>
            ) : (
              <table>
                {r.rows.length > SPARK_ROWS && (
                  <caption>
                    {SPARK_ROWS} dari {r.rows.length} baris
                  </caption>
                )}
                <thead>
                  <tr>
                    {r.header.map((h) => (
                      <th scope="col" key={h}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {r.rows.slice(0, SPARK_ROWS).map((row, i) => (
                    <tr key={i}>
                      {row.map((v, j) => (
                        <td key={j}>{fmt(v)}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      ))}
    </section>
  );
}

export default async function Page({ searchParams }) {
  const id = ((await searchParams).id ?? "").trim();
  const mongo = await loadMongo(id);
  return (
    <main>
      <header>
        <h1>Demo Olist: MongoDB dan Spark SQL</h1>
        <p>
          Prediksi Random Forest dari Spark MLlib disimpan di MongoDB, lalu dibaca aplikasi ini tanpa menjalankan
          Spark. Analisis Spark SQL ditampilkan bersama kuerinya.
        </p>
        <nav aria-label="Bagian halaman">
          <a href="#cek">Cek transaksi</a>
          <a href="#ringkasan">Ringkasan MongoDB</a>
          <a href="#sql">Spark SQL</a>
        </nav>
      </header>
      {mongo.error ? (
        <section id="cek">
          <p className="notice">
            MongoDB di <code>{MONGO_URI}</code> tidak bisa dihubungi ({mongo.error}). Jalankan <code>./run.sh up</code>{" "}
            lalu <code>./run.sh ml</code> agar koleksi prediksi terisi.
          </p>
        </section>
      ) : (
        <>
          <Lookup id={id} mongo={mongo} />
          <Summary mongo={mongo} />
        </>
      )}
      <SparkSql />
    </main>
  );
}
