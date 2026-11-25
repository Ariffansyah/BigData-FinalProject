import { MongoClient } from "mongodb";

export const MONGO_URI = process.env.MONGODB_URI ?? "mongodb://localhost:27017";

// Satu koneksi dipakai ulang. Saat `next dev`, hot reload memuat ulang modul ini, jadi klien disimpan
// di globalThis supaya tidak membuka koneksi baru setiap kali kode berubah.
const client = globalThis._mongoClient ?? new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 3000 });
globalThis._mongoClient = client;

export const db = () => client.db("olist");
