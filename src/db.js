"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const pg_1 = require("pg");
const pool = new pg_1.Pool({
    host: process.env.PGHOST || "127.0.0.1",
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE || "resume_sorter",
    user: process.env.PGUSER || "postgres",
    password: process.env.PGPASSWORD || "",
});
exports.default = pool;
