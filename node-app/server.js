import express from "express";
import cors from "cors";
import { Pool } from "pg";

const {
  POSTGRES_HOST = "postgres",
  POSTGRES_DB = "analytics",
  POSTGRES_USER = "postgres",
  POSTGRES_PASSWORD = "postgres",
  PORT = 3000
} = process.env;

const pool = new Pool({
  host: POSTGRES_HOST,
  database: POSTGRES_DB,
  user: POSTGRES_USER,
  password: POSTGRES_PASSWORD
});

const app = express();
app.use(cors());
app.use(express.static("public"));

app.get("/api/revenue", async (_req, res) => {
  try {
    // On prend les 60 dernières fenêtres (1h)
    const { rows } = await pool.query(`
      SELECT day_bucket, window_start, window_end, total_revenue, purchase_count
      FROM public.revenue_per_minute
      ORDER BY window_start DESC
      LIMIT 60
    `);
    // renvoyer en ordre chronologique
    res.json(rows.reverse());
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () =>
  console.log(`Node API http://localhost:${PORT}`)
);
