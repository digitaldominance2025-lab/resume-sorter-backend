import { Pool } from "pg";

const pool = new Pool({
  host: process.env.PGHOST || "127.0.0.1",
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE || "resume_sorter",
  user: process.env.PGUSER || "postgres",
  password: process.env.PGPASSWORD || "",
});

export default pool;

