import "./globals.css";

export const metadata = {
  title: "Demo Olist: MongoDB & Spark SQL",
  description: "Membaca hasil prediksi Spark MLlib dari MongoDB dan hasil analisis Spark SQL.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="id">
      <body>{children}</body>
    </html>
  );
}
