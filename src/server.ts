// src/server.ts
import express, { Request, Response } from "express";
import cors from "cors";
import dotenv from "dotenv";
import cron from "node-cron";
import fs from "fs";
import path from "path";
import multer from "multer";
import crypto from "crypto";

import { formatISO, addDays as addDaysDfns } from "date-fns";
import { toZonedTime, fromZonedTime } from "date-fns-tz";
import { google } from "googleapis";

import pool from "./db";
import uploadRoutes from "./routes/upload";
import intakeRoutes from "./routes/intake";
import axios from "axios";
import { Webhook } from "svix";
import Stripe from "stripe";

import { scoreResume } from "./services/openaiScore";
import * as emailSvc from "./services/email";
import * as customerUtils from "./utils/customer";

// ✅ Use your single R2 service (do NOT also create AWS SDK client here)
import { r2UploadBuffer, r2DownloadToBuffer } from "./services/r2";

// pdf-parse (classic callable function) - requires: npm i pdf-parse@1.1.1
// eslint-disable-next-line @typescript-eslint/no-var-requires
const pdfParse: any = require("pdf-parse");

dotenv.config();
console.log("SERVER FILE LOADED");

// 🔐 Centralized App Error
class AppError extends Error {
  statusCode: number;
  code: string;
  expose: boolean;

  constructor(message: string, statusCode = 500, code = "internal_error", expose = false) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
    this.expose = expose; // only expose safe messages
  }
}

// ============================
// Helpers
// ============================
// --- logging helper (dev-only noise) ---
const devLog = (...args: any[]) => {
  const env = safeStr(process.env.NODE_ENV).toLowerCase();
  if (env !== "production") console.log(...args);
};
function safeStr(v: any) {
  return (v ?? "").toString().trim();
}
function truthyEnv(name: string) {
  const v = safeStr(process.env[name]).toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "y" || v === "on";
}
function parseISODate(s: string) {
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}
function safeBaseName(key: string) {
  return key.split("/").pop() || "file.bin";
}
function tryJsonParse<T = any>(s: string): T | null {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}
function extractUrlFromHyperlinkFormula(cell: string): string | null {
  const s = safeStr(cell);
  // matches =HYPERLINK("https://...","text")
  const m = s.match(/=HYPERLINK\(\s*"([^"]+)"\s*,/i);
  return m?.[1] || null;
}
function stripR2FromNotes(notes: string): string {
  const n = safeStr(notes);
  if (!n) return "";
  return n
    .split(",")
    .map((s: string) => s.trim())
    .filter((part: string) => part && !part.toLowerCase().startsWith("r2:"))
    .join(", ");
}
function appendNote(existing: string, note: string) {
  const ex = safeStr(existing);
  if (!ex) return note;
  const parts = ex.split(",").map((s: string) => s.trim());
  if (parts.includes(note)) return ex;
  return `${ex}, ${note}`;
}
function genRequestId() {
  return crypto.randomBytes(12).toString("hex");
}
function getRequestId(req: Request | any): string {
  return safeStr(req?.requestId);
}

type LogMeta = Record<string, any>;

function logInfo(event: string, meta: LogMeta = {}) {
  console.log("ℹ️", event, meta);
}
function logWarn(event: string, meta: LogMeta = {}) {
  console.warn("⚠️", event, meta);
}
function logError(event: string, meta: LogMeta = {}) {
  console.error("❌", event, meta);
}

// ============================
// Config (order matters)
// ============================
const PORT = Number(process.env.PORT || 3001);
const TIMEZONE = process.env.TIMEZONE || "America/Regina";

const NODE_ENV = safeStr(process.env.NODE_ENV).toLowerCase();
const IS_PROD = NODE_ENV === "production";
const BASE_URL =
  safeStr(process.env.BASE_URL) ||
  (IS_PROD ? "https://digitaldominance2025.ca" : "http://localhost:3000");

const ADMIN_EMAIL = process.env.ADMIN_EMAIL || "digitaldominance2025@gmail.com";
const MASTER_SHEET_ID = process.env.MASTER_CUSTOMERS_SHEET_ID || "";
const MASTER_SHEET_TAB = process.env.MASTER_CUSTOMERS_SHEET_TAB || "customers";
const NIGHTLY_CRON = process.env.NIGHTLY_CRON || "10 2 * * *";

const DEBUG_ROUTES_ENABLED = truthyEnv("DEBUG_ROUTES") && process.env.NODE_ENV !== "production";
const LOG_AUTHED_GOOGLE_EMAIL = truthyEnv("LOG_AUTHED_GOOGLE_EMAIL");
const INBOUND_FILE_ENABLED = !IS_PROD || truthyEnv("INBOUND_FILE_ENABLED"); // allow in dev, opt-in in prod

// ============================
// Lane A2 (FAIL-CLOSED): inbound-r2 secret required in prod
// ============================
const INBOUND_WEBHOOK_SECRET = safeStr(process.env.INBOUND_WEBHOOK_SECRET);
const INBOUND_SECRET_REQUIRED = IS_PROD || truthyEnv("INBOUND_SECRET_REQUIRED"); // optional override in non-prod

if (INBOUND_SECRET_REQUIRED && !INBOUND_WEBHOOK_SECRET) {
  // Fail-closed: refuse to start if secret required but missing
  console.error("❌ FATAL: INBOUND_WEBHOOK_SECRET is required but not set.");
  process.exit(1);
}

// ============================
// Hardening Step 2: size limits
// ============================
const MAX_ATTACHMENT_BYTES = Number(process.env.MAX_ATTACHMENT_BYTES || 10 * 1024 * 1024); // 10MB
const MAX_TOTAL_ATTACH_BYTES = Number(process.env.MAX_TOTAL_ATTACH_BYTES || 25 * 1024 * 1024); // 25MB
const MAX_ATTACHMENTS = Number(process.env.MAX_ATTACHMENTS || 10);

const MAX_EXTRACTED_CHARS = Number(process.env.MAX_EXTRACTED_CHARS || 120_000);
const MAX_OPENAI_CHARS = Number(process.env.MAX_OPENAI_CHARS || 60_000);

// ============================
// Hardening Step 3: R2 public links (OFF by default)
// ============================
/**
 * ✅ Default SAFE behavior:
 * - R2 public URLs are NOT generated unless R2_PUBLIC_LINKS=true
 * - AND R2_PUBLIC_BASE_URL is set (extra safety switch)
 */

const R2_PUBLIC_BASE_URL = safeStr(process.env.R2_PUBLIC_BASE_URL).replace(/\/+$/, "");
const R2_PUBLIC_LINKS = truthyEnv("R2_PUBLIC_LINKS");
// Resend

const RESEND_API_KEY = safeStr(process.env.RESEND_API_KEY);
const RESEND_WEBHOOK_SECRET = safeStr(process.env.RESEND_WEBHOOK_SECRET);

// Stripe
const STRIPE_SECRET_KEY = safeStr(process.env.STRIPE_SECRET_KEY);
const STRIPE_WEBHOOK_SECRET = safeStr(process.env.STRIPE_WEBHOOK_SECRET);
const STRIPE_PRICE_ID = safeStr(process.env.STRIPE_PRICE_ID);

const APP_URL = safeStr(process.env.APP_URL) || `http://localhost:${PORT}`;
const BILLING_SUCCESS_URL = safeStr(process.env.BILLING_SUCCESS_URL) || `${APP_URL}/billing/success`;
const BILLING_CANCEL_URL = safeStr(process.env.BILLING_CANCEL_URL) || `${APP_URL}/billing/cancel`;

const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY, { apiVersion: "2024-06-20" as any }) : null;

console.log("STRIPE_KEY_MODE:", {
  present: !!STRIPE_SECRET_KEY,
  prefix: STRIPE_SECRET_KEY ? STRIPE_SECRET_KEY.slice(0, 7) : null,
  hasWebhookSecret: !!STRIPE_WEBHOOK_SECRET,
});
// ============================
// Hardening: simple in-memory rate limiter (no deps)
// ============================
type RateRule = { windowMs: number; max: number };

const RATE_LIMITS: Record<string, RateRule> = {
  inbound_file: { windowMs: 60_000, max: 30 }, // 30/min per IP
  inbound_r2: { windowMs: 60_000, max: 60 }, // 60/min per IP
  resend_inbound: { windowMs: 60_000, max: 120 }, // 120/min per IP (svix verified)
};

const rateBuckets = new Map<string, { count: number; resetAt: number }>();

// ✅ Hardening: prevent unbounded growth (memory leak)
const MAX_RATE_BUCKETS = 5000;
function pruneRateBuckets(now: number) {
  // Fast path
  if (rateBuckets.size < MAX_RATE_BUCKETS) return;

  // Remove expired buckets first
  for (const [k, v] of rateBuckets) {
    if (now >= v.resetAt) rateBuckets.delete(k);
    if (rateBuckets.size < MAX_RATE_BUCKETS) return;
  }

  // If still too big, delete oldest-ish entries (Map preserves insertion order)
  while (rateBuckets.size > MAX_RATE_BUCKETS) {
    const firstKey = rateBuckets.keys().next().value;
    if (!firstKey) break;
    rateBuckets.delete(firstKey);
  }
}

function getClientIp(req: Request): string {
  const xf = safeStr(req.headers["x-forwarded-for"]);
  if (xf) return xf.split(",")[0].trim();
  return safeStr((req.socket as any)?.remoteAddress || req.ip || "unknown");
}

function rateLimit(key: keyof typeof RATE_LIMITS) {
  const rule = RATE_LIMITS[key];
  return (req: Request, res: Response, next: any) => {
    const ip = getClientIp(req);
    const now = Date.now();

    // ✅ prune occasionally
    pruneRateBuckets(now);

    const bucketKey = `${key}:${ip}`;

    const cur = rateBuckets.get(bucketKey);
    if (!cur || now >= cur.resetAt) {
      rateBuckets.set(bucketKey, { count: 1, resetAt: now + rule.windowMs });
      res.setHeader("X-RateLimit-Limit", String(rule.max));
      res.setHeader("X-RateLimit-Remaining", String(rule.max - 1));
      res.setHeader("X-RateLimit-Reset", String(Math.floor((now + rule.windowMs) / 1000)));
      return next();
    }

    cur.count += 1;

    const remaining = Math.max(rule.max - cur.count, 0);
    res.setHeader("X-RateLimit-Limit", String(rule.max));
    res.setHeader("X-RateLimit-Remaining", String(remaining));
    res.setHeader("X-RateLimit-Reset", String(Math.floor(cur.resetAt / 1000)));

    if (cur.count > rule.max) {
      return res.status(429).json({
        ok: false,
        error: "rate_limited",
        key,
        windowMs: rule.windowMs,
        max: rule.max,
      });
    }

    return next();
  };
}

// ✅ sha256Hex only hashes
function sha256Hex(input: Buffer | string) {
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(String(input), "utf8");
  return crypto.createHash("sha256").update(buf).digest("hex");
}

// ============================
// Hardening Step 1: file allowlist (pdf/txt/docx only)
// ============================
const ALLOWED_EXTS = new Set(["pdf", "txt", "docx"]);

function extFromFilename(filename: string): string {
  const base = safeStr(filename).toLowerCase();
  const i = base.lastIndexOf(".");
  return i >= 0 ? base.slice(i + 1) : "";
}
function normalizeUploadName(name: string) {
  return safeStr(name || "upload.bin").replace(/[^a-zA-Z0-9._-]+/g, "_");
}
function isAllowedFileByName(filename: string): boolean {
  const ext = extFromFilename(filename);
  return !!ext && ALLOWED_EXTS.has(ext);
}
function unsupportedFileTypePayload(filename: string) {
  const ext = extFromFilename(filename) || "unknown";
  return {
    ok: false,
    error: "unsupported_file_type",
    filename,
    ext,
    allowed: Array.from(ALLOWED_EXTS),
  };
}

// ============================
// Google OAuth
// ============================
const oauth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);

const SCOPES = [
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive",
  "https://www.googleapis.com/auth/userinfo.email",
  "https://www.googleapis.com/auth/userinfo.profile",
  "openid",
];

const TOKENS_DIR = path.join(process.cwd(), ".tokens");
const TOKENS_PATH = path.join(TOKENS_DIR, "google_tokens.json");

function ensureTokensDir() {
  if (!fs.existsSync(TOKENS_DIR)) fs.mkdirSync(TOKENS_DIR, { recursive: true });
}
function saveTokensToDisk(tokens: any) {
  ensureTokensDir();
  fs.writeFileSync(TOKENS_PATH, JSON.stringify(tokens, null, 2), "utf8");
}

let AUTHED_EMAIL_LOGGED = false;
let CACHED_AUTHED_EMAIL: string | null = null;

async function logAuthedGoogleEmailFromTokens(): Promise<string | null> {
  try {
    await oauth2Client.getAccessToken();
    const oauth2 = google.oauth2({ version: "v2", auth: oauth2Client });
    const { data } = await oauth2.userinfo.get();
    const email = (data as any)?.email || null;

    if (email) {
      CACHED_AUTHED_EMAIL = email;
      if (!AUTHED_EMAIL_LOGGED) {
        console.log("🔐 AUTHED GOOGLE EMAIL:", email);
        AUTHED_EMAIL_LOGGED = true;
      }
      return email;
    }

    console.log("⚠️ Could not resolve authed Google email");
    return null;
  } catch (e: any) {
    console.warn("⚠️ logAuthedGoogleEmailFromTokens failed:", e?.message || e);
    return null;
  }
}

async function loadTokensFromEnvOrDisk(): Promise<boolean> {
  try {
    const envRaw = String(process.env.GOOGLE_TOKENS_JSON || "").trim();

    if (envRaw) {
      const tokens = JSON.parse(envRaw);
      oauth2Client.setCredentials(tokens);
      console.log("✅ Loaded Google tokens from GOOGLE_TOKENS_JSON (env)");

      if (LOG_AUTHED_GOOGLE_EMAIL) {
        await logAuthedGoogleEmailFromTokens();
      }
      return true;
    }

    if (!fs.existsSync(TOKENS_PATH)) {
      console.log("ℹ️ No token file found yet. Need /auth once.");
      return false;
    }

    const raw = fs.readFileSync(TOKENS_PATH, "utf8");
    const tokens = JSON.parse(raw);

    oauth2Client.setCredentials(tokens);
    console.log("✅ Loaded Google tokens from disk");

    if (LOG_AUTHED_GOOGLE_EMAIL) {
      await logAuthedGoogleEmailFromTokens();
    }

    return true;
  } catch (e: any) {
    console.log("⚠️ Failed to load Google tokens:", e?.message || e);
    return false;
  }
}

void (async () => {
  await loadTokensFromEnvOrDisk();
})();

// ============================
// DB bootstrap (AUTO)
// ============================
async function ensureDbTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS inbound_docs (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        source TEXT NOT NULL DEFAULT 'unknown',        -- 'resend' | 'inbound-file' | 'inbound-r2'
        to_email TEXT,
        customer_id TEXT,
        resolved_customer_id TEXT,
        match_found BOOLEAN NOT NULL DEFAULT false,
        billing_status TEXT,
        blocked_reason TEXT,

        filename TEXT,
        r2_bucket TEXT,
        r2_key TEXT,
        doc_type TEXT,                                 -- 'RESUME' | 'NON_RESUME'
        extracted_chars INTEGER,
        text_preview TEXT,

        ai_score NUMERIC,
        ai_json JSONB
      );
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_rubrics (
        customer_id TEXT PRIMARY KEY,
        rubric_json JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS inbound_docs_customer_id_idx ON inbound_docs(customer_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS inbound_docs_r2_key_idx ON inbound_docs(r2_key);`);
      // ============================
    // Jobs table (multi-role support)
    // ============================
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_jobs (
        id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        title TEXT NOT NULL,
        rubric_json JSONB NOT NULL,
        created_at TIMESTAMPTZ DEFAULT now()
      );
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS customer_jobs_customer_idx
      ON customer_jobs(customer_id);
    `);
    // ============================
    // Signed Resume Viewing: persist requestId for /r/:requestId lookups
    // ============================
    try {
      await pool.query(`ALTER TABLE inbound_docs ADD COLUMN IF NOT EXISTS request_id TEXT;`);
      await pool.query(`CREATE INDEX IF NOT EXISTS inbound_docs_request_id_idx ON inbound_docs(request_id);`);
      devLog("✅ DB inbound_docs.request_id ensured");
    } catch (e: any) {
      console.warn("⚠️ DB alter inbound_docs.request_id failed (continuing):", e?.message || e);
    }
// Ensure pending_signups table exists (for Stripe trial checkout flow)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pending_signups (
      id TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS pending_signups_created_at_idx
    ON pending_signups(created_at)
  `);
    console.log("✅ DB tables ensured");
  } catch (e: any) {
    console.warn("⚠️ DB bootstrap failed (continuing):", e?.message || e);
  }
}
void ensureDbTables();

// ✅ SINGLE definition (fixed)
async function upsertCustomerRubric(customerId: string, rubric: any) {
  if (!customerId) throw new AppError("missing_customerId", 400, "missing_customerId", true);
  if (rubric == null) throw new AppError("missing_rubric", 400, "missing_rubric", true);
  if (typeof rubric !== "object") throw new AppError("invalid_rubric", 400, "invalid_rubric", true);

  try {
    await pool.query(
      `
      INSERT INTO customer_rubrics (customer_id, rubric_json, updated_at)
      VALUES ($1, $2::jsonb, now())
      ON CONFLICT (customer_id)
      DO UPDATE SET rubric_json = EXCLUDED.rubric_json, updated_at = now()
      `,
      [customerId, JSON.stringify(rubric)]
    );
  } catch (_e: any) {
    // Central handler logs stack; client gets generic message in prod
    throw new AppError("db_error_upserting_rubric", 500, "db_error", false);
  }
}


async function getCustomerRubric(customerId: string): Promise<any | null> {
  if (!customerId) return null;

  try {
    const r = await pool.query(`SELECT rubric_json FROM customer_rubrics WHERE customer_id=$1`, [customerId]);
    return r.rows?.[0]?.rubric_json ?? null;
  } catch (e: any) {
    // Read failures are non-fatal in your flow; keep returning null
    console.warn("⚠️ getCustomerRubric failed (returning null):", e?.message || e);
    return null;
  }
}
// ============================
// JOB MODEL (multi-role hiring)
// ============================

type CustomerJob = {
  id: string;
  customerId: string;
  title: string;
  rubric: any;
  createdAt: string;
};

function generateJobId(customerId: string) {
  return `job_${customerId}_${crypto.randomBytes(6).toString("hex")}`;
}

async function createCustomerJob(customerId: string, title: string, rubric: any) {
  if (!customerId) throw new AppError("missing_customerId", 400, "missing_customerId", true);
  if (!title) throw new AppError("missing_title", 400, "missing_title", true);
  if (!rubric || typeof rubric !== "object")
    throw new AppError("invalid_rubric", 400, "invalid_rubric", true);

  const id = generateJobId(customerId);

  await pool.query(
    `INSERT INTO customer_jobs (id, customer_id, title, rubric_json)
     VALUES ($1,$2,$3,$4::jsonb)`,
    [id, customerId, title, JSON.stringify(rubric)]
  );

  return { id, customerId, title, rubric, createdAt: new Date().toISOString() };
}

async function listCustomerJobs(customerId: string): Promise<CustomerJob[]> {
  const r = await pool.query(
    `SELECT id, customer_id, title, rubric_json, created_at
     FROM customer_jobs
     WHERE customer_id=$1
     ORDER BY created_at ASC`,
    [customerId]
  );

  return r.rows.map((row: any) => ({
    id: row.id,
    customerId: row.customer_id,
    title: row.title,
    rubric: row.rubric_json,
    createdAt: row.created_at,
  }));
}

async function deleteCustomerJob(customerId: string, jobId: string) {
  await pool.query(
    `DELETE FROM customer_jobs WHERE customer_id=$1 AND id=$2`,
    [customerId, jobId]
  );
}
async function saveInboundDocToDb(args: {
  source: string;
  requestId?: string;
  toEmail?: string;
  customerId?: string;
  resolvedCustomerId?: string;
  matchFound: boolean;
  billingStatus?: string;
  blockedReason?: string;
  filename?: string;
  r2Bucket?: string;
  r2Key?: string;
  docType?: string;
  extractedChars?: number;
  textPreview?: string;
  aiScore?: number | null;
  aiJson?: any;
}) {
  try {
    devLog("DB_INBOUND_INSERT", {
      requestId: safeStr((args as any).requestId),
      source: safeStr(args.source),
      r2Key: safeStr(args.r2Key),
    });

        await pool.query(
      `
      INSERT INTO inbound_docs (
        source,
        request_id,
        to_email,
        customer_id,
        resolved_customer_id,
        match_found,
        billing_status,
        blocked_reason,
        filename,
        r2_bucket,
        r2_key,
        doc_type,
        extracted_chars,
        text_preview,
        ai_score,
        ai_json
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb
      )
      `,
      [
        safeStr(args.source) || "unknown",
        safeStr(args.requestId) || null,
        safeStr(args.toEmail) || null,
        safeStr(args.customerId) || null,
        safeStr(args.resolvedCustomerId) || null,
        !!args.matchFound,
        safeStr(args.billingStatus) || null,
        safeStr(args.blockedReason) || null,
        safeStr(args.filename) || null,
        safeStr(args.r2Bucket) || null,
        safeStr(args.r2Key) || null,
        safeStr(args.docType) || null,
        Number.isFinite(args.extractedChars as any) ? Number(args.extractedChars) : null,
        safeStr(args.textPreview) || null,
        Number.isFinite(args.aiScore as any) ? Number(args.aiScore) : null,
        JSON.stringify(args.aiJson ?? null),
      ]
    );
  } catch (e: any) {
    console.warn("⚠️ saveInboundDocToDb failed (continuing):", e?.message || e);
  }
}

// ============================
// Share-once cache (per runtime)
// ============================
const SHARED_SHEET_IDS = new Set<string>();
function sharedKey(sheetId: string, email: string) {
  return `${sheetId}::${email}`.toLowerCase();
}
function isAlreadySharedError(e: any) {
  const status = e?.code || e?.response?.status;
  const reason = e?.errors?.[0]?.reason || e?.response?.data?.error?.errors?.[0]?.reason || "";
  const msg = String(e?.message || "").toLowerCase();
  return status === 409 || reason === "alreadyExists" || msg.includes("already exists") || msg.includes("duplicate");
}
async function ensureSheetSharedOnce(sheetId: string, email: string) {
  if (!sheetId || !email) return;

  const key = sharedKey(sheetId, email);
  if (SHARED_SHEET_IDS.has(key)) return;

  try {
    const drive = google.drive({ version: "v3", auth: oauth2Client });

    await drive.permissions.create({
      fileId: sheetId,
      supportsAllDrives: true,
      requestBody: { type: "user", role: "writer", emailAddress: email },
      sendNotificationEmail: false,
    });

    SHARED_SHEET_IDS.add(key);
    console.log(`✅ SHEET_SHARED_OK: ${sheetId} -> ${email}`);
  } catch (e: any) {
    if (isAlreadySharedError(e)) {
      SHARED_SHEET_IDS.add(key);
      console.log(`✅ SHEET_ALREADY_SHARED: ${sheetId} -> ${email}`);
      return;
    }

    const status = e?.code || e?.response?.status;
    const reason = e?.response?.data?.error?.errors?.[0]?.reason;
    const message = String(e?.response?.data?.error?.message || e?.message || e);

    // Cache 403s to avoid retry loops when sheet is already usable
    if (status === 403) {
      SHARED_SHEET_IDS.add(key);
      console.log(`⚠️ SHEET_SHARE_403_CACHED: ${sheetId} -> ${email}`);
      return;
    }

    console.log(`⚠️ SHEET_SHARE_FAILED: ${sheetId} -> ${email} (status=${status}, reason=${reason}) ${message}`);
  }
}

// ============================
// Email
// ============================
async function sendAdmin(subject: string, body: string) {
  const fn = (emailSvc as any).sendAdminEmail;
  if (typeof fn === "function") {
    try {
      await fn(ADMIN_EMAIL, subject, body);
      return;
    } catch (e) {
      console.log("⚠️ sendAdminEmail failed (continuing):", e);
      return;
    }
  }
  console.log("ℹ️ emailSvc.sendAdminEmail not found; skipping email.");
}

async function sendCustomerText(to: string, subject: string, text: string) {
  const svc: any = emailSvc as any;
  const candidates = [
    svc.sendTextEmail,
    svc.sendEmailText,
    svc.sendEmail,
    svc.sendCustomerEmail,
    svc.sendCustomerTextEmail,
  ].filter((fn: any) => typeof fn === "function");

  if (!candidates.length) {
    console.log("ℹ️ No customer email function found in emailSvc; skipping.", { to, subject });
    return;
  }

  const fn = candidates[0];

  try {
    if (fn.length === 1) {
      await fn({ to, subject, text });
      return;
    }
    if (fn.length >= 3) {
      await fn(to, subject, text);
      return;
    }
    await fn({ to, subject, text });
  } catch (e: any) {
    console.log("⚠️ sendCustomerText failed (continuing):", e?.message || e);
  }
}

// ============================
// Customer utilities
// ============================
function makeSlug(companyName: string) {
  const fn = (customerUtils as any).slugifyCompanyName;
  if (typeof fn === "function") return fn(companyName);
  return companyName.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)+/g, "");
}
function makeCustomerId(slug: string) {
  const fn = (customerUtils as any).makeCustomerId;
  if (typeof fn === "function") {
    try {
      return fn.length >= 1 ? fn(slug) : fn();
    } catch {
      return `cust_${slug}_${Date.now()}`;
    }
  }
  return `cust_${slug}_${Date.now()}`;
}

// ============================
// Master Sheet model + mapping
// ============================
type CustomerRow = {
  customerId: string;
  companyName: string;
  slug: string;
  adminEmail: string;
  intakeEmail: string;
  status: string;
  trialStartsAtISO: string;
  trialEndsAtISO: string;
  tallySheetId: string;
  tallySheetUrl: string;

  stripeCustomerId?: string;
  stripeSubscriptionId?: string;
};

const MASTER_HEADERS = [
  "customerId",
  "companyName",
  "companySlug",
  "intakeEmail",
  "reportToEmail",
  "billingStatus",
  "trialStartAt",
  "trialEndAt",
  "currentSheetId",
  "currentSheetUrl",
  "currentSheetStartAt",
  "currentSheetEndAt",
  "createdAt",
  "stripeCustomerId",
  "stripeSubscriptionId",
] as const;

function mapHeaderIndexes(headers: string[]) {
  const norm = headers.map((h) => safeStr(h).toLowerCase());
  const idx = (name: string) => norm.indexOf(name.toLowerCase());
  return {
    customerId: idx("customerId"),
    companyName: idx("companyName"),
    slug: idx("companySlug"),
    adminEmail: idx("reportToEmail"),
    intakeEmail: idx("intakeEmail"),
    status: idx("billingStatus"),
    trialStartsAtISO: idx("trialStartAt"),
    trialEndsAtISO: idx("trialEndAt"),
    tallySheetId: idx("currentSheetId"),
    tallySheetUrl: idx("currentSheetUrl"),
    stripeCustomerId: idx("stripeCustomerId"),
    stripeSubscriptionId: idx("stripeSubscriptionId"),
  };
}
function columnLetter(n: number) {
  let s = "";
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

async function ensureMasterHeaders() {
  if (!MASTER_SHEET_ID) return;
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const headerRange = `${MASTER_SHEET_TAB}!1:1`;
  console.log("🔎 MASTER sheetId:", MASTER_SHEET_ID, "tab:", MASTER_SHEET_TAB);

  const headerResp = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: headerRange });
  const existing = headerResp.data.values?.[0] || [];
  const hasAny = (existing.length || 0) > 0;

  if (!hasAny) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: MASTER_SHEET_ID,
      range: headerRange,
      valueInputOption: "RAW",
      requestBody: { values: [Array.from(MASTER_HEADERS)] },
    });
    return;
  }

  const existingNorm = existing.map((h: any) => safeStr(h).toLowerCase());
  const toAdd = MASTER_HEADERS.filter((h) => !existingNorm.includes(h.toLowerCase()));
  if (!toAdd.length) return;

  const startCol = existing.length + 1;
  const startLetter = columnLetter(startCol);
  const endLetter = columnLetter(startCol + toAdd.length - 1);
  const range = `${MASTER_SHEET_TAB}!${startLetter}1:${endLetter}1`;

  await sheets.spreadsheets.values.update({
    spreadsheetId: MASTER_SHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values: [toAdd] },
  });

  console.log("✅ MASTER_HEADERS_EXTENDED:", toAdd.join(", "));
}

async function readCustomersFromMasterSheet(): Promise<CustomerRow[]> {
  if (!MASTER_SHEET_ID) return [];
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: MASTER_SHEET_ID,
    range: `${MASTER_SHEET_TAB}!A:ZZ`,
  });

  const values = resp.data.values || [];
  if (values.length < 2) return [];

  const headers = (values[0] || []).map(String);
  const col = mapHeaderIndexes(headers);
  if (col.customerId < 0) return [];

  const out: CustomerRow[] = [];
  for (let i = 1; i < values.length; i++) {
    const row: any[] = values[i] || [];
    const customerId = safeStr(row[col.customerId]);
    if (!customerId) continue;

    out.push({
      customerId,
      companyName: safeStr(row[col.companyName]),
      slug: safeStr(row[col.slug]),
      adminEmail: safeStr(row[col.adminEmail]),
      intakeEmail: safeStr(row[col.intakeEmail]),
      status: safeStr(row[col.status]),
      trialStartsAtISO: safeStr(row[col.trialStartsAtISO]),
      trialEndsAtISO: safeStr(row[col.trialEndsAtISO]),
      tallySheetId: safeStr(row[col.tallySheetId]),
      tallySheetUrl: safeStr(row[col.tallySheetUrl]),
      stripeCustomerId: col.stripeCustomerId >= 0 ? safeStr(row[col.stripeCustomerId]) : undefined,
      stripeSubscriptionId: col.stripeSubscriptionId >= 0 ? safeStr(row[col.stripeSubscriptionId]) : undefined,
    });
  }

  return out;
}

async function updateCustomerCellInMasterSheet(customerId: string, headerName: string, value: string) {
  if (!MASTER_SHEET_ID) return;

  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId: MASTER_SHEET_ID,
    range: `${MASTER_SHEET_TAB}!A:ZZ`,
  });

  const values = resp.data.values || [];
  if (values.length < 2) return;

  const headers = (values[0] || []).map(String);
  const normHeaders = headers.map((h) => safeStr(h).toLowerCase());
  const targetIdx = normHeaders.indexOf(headerName.toLowerCase());
  if (targetIdx < 0) {
    console.warn("⚠️ MASTER_HEADER_MISSING (safe no-op):", headerName);
    return;
  }

  let rowNumber = -1;
  const custIdIdx = normHeaders.indexOf("customerid");
  if (custIdIdx < 0) return;

  for (let i = 1; i < values.length; i++) {
    const row: any[] = values[i] || [];
    if (safeStr(row[custIdIdx]) === customerId) {
      rowNumber = i + 1;
      break;
    }
  }
  if (rowNumber < 0) return;

  const colLetter = columnLetter(targetIdx + 1);
  const range = `${MASTER_SHEET_TAB}!${colLetter}${rowNumber}`;

  await sheets.spreadsheets.values.update({
    spreadsheetId: MASTER_SHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values: [[value]] },
  });
}
async function updateCustomerStatusInMasterSheet(customerId: string, newStatus: string) {
  await updateCustomerCellInMasterSheet(customerId, "billingStatus", newStatus);
}

// ============================
// Customers cache (TTL)
// ============================
let customersCache: CustomerRow[] | null = null;
let customersCacheAt = 0;
async function getCustomersCached() {
  const now = Date.now();
  const ttlMs = 60_000;
  if (customersCache && now - customersCacheAt < ttlMs) return customersCache;
  customersCache = await readCustomersFromMasterSheet();
  customersCacheAt = now;
  return customersCache;
}
function invalidateCustomersCache() {
  customersCache = null;
  customersCacheAt = 0;
}

// ============================
// Billing enforcement
// ============================
function isProcessingAllowed(billingStatusRaw: string): { allowed: boolean; reason?: string } {
  const s = safeStr(billingStatusRaw).toLowerCase();

  if (!s) return { allowed: true };
  if (s === "trial" || s === "trialing") return { allowed: true };
  if (s === "active") return { allowed: true };

  if (s === "trial_ended") return { allowed: false, reason: "trial_ended" };
  if (s === "past_due") return { allowed: false, reason: "past_due" };
  if (s === "canceled") return { allowed: false, reason: "canceled" };
  if (s === "unpaid") return { allowed: false, reason: "unpaid" };

  return { allowed: true };
}

// ============================
// Text extraction
// ============================
function decodeTextSmartBuf(buf: Buffer): string {
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
    return buf.toString("utf16le").replace(/^\uFEFF/, "");
  }
  if (buf.length >= 3 && buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf) {
    return buf.toString("utf8").replace(/^\uFEFF/, "");
  }
  return buf.toString("utf8");
}
function looksGarbledText(s: string) {
  const sample = (s || "").slice(0, 2000);
  const trimmed = sample.trim();
  if (!trimmed) return true;
  const letters = (trimmed.match(/[A-Za-z]/g) || []).length;
  const ratio = letters / Math.max(trimmed.length, 1);
  return ratio < 0.05;
}

async function extractTextFromBuffer(filename: string, buf: Buffer): Promise<string> {
  const nameLower = filename.toLowerCase();

  try {
    if (nameLower.endsWith(".txt")) {
      return safeStr(decodeTextSmartBuf(buf)).replace(/\u0000/g, "");
    }

    if (nameLower.endsWith(".pdf")) {
      try {
        const parsed = await pdfParse(buf);
        const text = safeStr(parsed?.text || "");
        if (looksGarbledText(text)) {
          console.warn("⚠️ EXTRACT_GARBLED:", filename);
          return "";
        }
        return text;
      } catch (e: any) {
        console.warn("⚠️ EXTRACT_FAILED:", filename, e?.message || e);
        return "";
      }
    }

    if (nameLower.endsWith(".docx")) {
      try {
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const mammoth = require("mammoth");
        const result = await mammoth.extractRawText({ buffer: buf });
        const text = safeStr(result?.value || "");
        return text;
      } catch (e: any) {
        console.warn("⚠️ DOCX_EXTRACT_FAILED (install mammoth?):", filename, e?.message || e);
        return "";
      }
    }

    // fallback (should be blocked by allowlist before this is reached)
    return safeStr(decodeTextSmartBuf(buf)).replace(/\u0000/g, "");
  } catch (e: any) {
    console.warn("⚠️ EXTRACT_FAILED:", filename, e?.message || e);
    return "";
  }
}

// ============================
// Doc classification + tally
// ============================
function classifyDocTypeFromText(textRaw: string): "RESUME" | "NON_RESUME" {
  const text = (textRaw || "").toLowerCase();

 // quick sanity (allow short but obvious resumes)
  const letters = (text.match(/[a-z]/g) || []).length;

  // If the text is short, only treat as resume when it has strong resume markers.
  if (letters < 80) {
    const shortResumeMarkers = [
      "work experience",
      "years experience",
      "professional summary",
      "education",
      "skills",
      "certifications",
      "certification",
      "linkedin",
      "curriculum vitae",
      "resume",
    ];

    const hasMarker = shortResumeMarkers.some((k) => text.includes(k));
    if (!hasMarker) return "NON_RESUME";
  }

  const hasAny = (keys: string[]) => keys.some((k) => text.includes(k));
  const countAny = (keys: string[]) => keys.reduce((n, k) => (text.includes(k) ? n + 1 : n), 0);

  // Strong resume signals
  const resumeStrong = [
    "professional summary",
    "work experience",
    "employment history",
    "experience",
    "education",
    "skills",
    "certifications",
    "certification",
    "projects",
    "objective",
    "curriculum vitae",
    "resume",
    "linkedin",
  ];

  // Weaker but common signals
  const resumeWeak = [
    "responsibilities",
    "achievements",
    "accomplishments",
    "references",
    "technical skills",
    "core competencies",
    "profile",
    "summary",
    "high school",
    "university",
    "college",
    "bachelor",
    "diploma",
  ];

  // Strong non-resume signals (only count these; don't instantly return)
  const nonResumeStrong = [
    "invoice",
    "amount due",
    "subtotal",
    "total due",
    "payment",
    "bill to",
    "ship to",
    "purchase order",
    "po number",
    "statement of account",
    "terms and conditions",
  ];

  const resumeScore = countAny(resumeStrong) * 3 + countAny(resumeWeak);
  const nonResumeScore = countAny(nonResumeStrong) * 3;

  if (hasAny(resumeStrong) && nonResumeScore < resumeScore) return "RESUME";
  if (resumeScore >= 3 && nonResumeScore <= resumeScore) return "RESUME";
  if (nonResumeScore >= 3 && nonResumeScore > resumeScore) return "NON_RESUME";

  return "NON_RESUME";
}

function buildR2PublicUrl(r2Key: string) {
  // Hardening Step 3: only generate public URLs if explicitly enabled
  if (!R2_PUBLIC_LINKS) return "";
  if (!R2_PUBLIC_BASE_URL) return ""; // extra safety switch
  if (!r2Key) return "";

  return `${R2_PUBLIC_BASE_URL}/${encodeURIComponent(r2Key).replace(/%2F/g, "/")}`;
}

// ✅ Ensures today's row exists + returns index and current values
async function ensureTodayTallyRow(
  tallySheetId: string,
  today: string,
  customerId: string
): Promise<{ rowIndex0: number; currentCount: number; currentNotes: string }> {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });

  // Share BEFORE any reads/writes (prevents 403)
  const authedEmail = CACHED_AUTHED_EMAIL || (await logAuthedGoogleEmailFromTokens());
  if (authedEmail) {
    CACHED_AUTHED_EMAIL = authedEmail;
    await ensureSheetSharedOnce(tallySheetId, authedEmail);
  }
  if (ADMIN_EMAIL && ADMIN_EMAIL.toLowerCase() !== authedEmail?.toLowerCase()) {
    await ensureSheetSharedOnce(tallySheetId, ADMIN_EMAIL);
  }

  const existingRes = await sheets.spreadsheets.values.get({
    spreadsheetId: tallySheetId,
    range: "A2:H1000",
  });

  const rows = existingRes.data.values || [];
  const rowIndex0 = rows.findIndex((r: any[]) => safeStr(r?.[0]) === today);

  if (rowIndex0 !== -1) {
    const currentCount = Number(rows[rowIndex0]?.[1] || 0);
    const currentNotes = safeStr(rows[rowIndex0]?.[2]);
    return { rowIndex0, currentCount, currentNotes };
  }

  await sheets.spreadsheets.values.append({
    spreadsheetId: tallySheetId,
    range: "A:H",
    valueInputOption: "RAW",
    requestBody: { values: [[today, 0, "", customerId, "", "", "", ""]] },
  });

  const rereadRes = await sheets.spreadsheets.values.get({
    spreadsheetId: tallySheetId,
    range: "A2:H1000",
  });

  const rows2 = rereadRes.data.values || [];
  const idx2 = rows2.findIndex((r: any[]) => safeStr(r?.[0]) === today);

  return {
    rowIndex0: idx2 === -1 ? rows2.length - 1 : idx2,
    currentCount: 0,
    currentNotes: "",
  };
}

// ✅ Used by nightly emails: reads today’s row by header names
async function readTodayTallyRowByHeaders(
  tallySheetId: string,
  today: string
): Promise<{ count: number; lastScore: number | null; r2Key: string; r2KeysCsv: string; resumeLinkCell: string }> {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });

  // Share BEFORE read (nightly runs after restart)
  const authedEmail = CACHED_AUTHED_EMAIL || (await logAuthedGoogleEmailFromTokens());
  if (authedEmail) {
    CACHED_AUTHED_EMAIL = authedEmail;
    await ensureSheetSharedOnce(tallySheetId, authedEmail);
  }
  if (ADMIN_EMAIL && ADMIN_EMAIL.toLowerCase() !== authedEmail?.toLowerCase()) {
    await ensureSheetSharedOnce(tallySheetId, ADMIN_EMAIL);
  }

  const head = await sheets.spreadsheets.values.get({
    spreadsheetId: tallySheetId,
    range: "A1:Z1",
  });

  const headersRaw = (head.data.values?.[0] || []).map((h: any) => safeStr(h));
  const norm = (s: string) => safeStr(s).toLowerCase().replace(/[^a-z0-9]/g, "");
  const headers = headersRaw.map(norm);
  const idx = (name: string) => headers.indexOf(norm(name));

  const iDate = idx("date");
  const iCount = idx("resumesProcessed");
  const iLastScore = idx("lastScore");
  const iR2Keys = idx("r2Keys");
  const iResumeFile = idx("resumeFile");
  const iR2Key = idx("r2Key");

  if (iDate < 0 || iCount < 0) {
    return { count: 0, lastScore: null, r2Key: "", r2KeysCsv: "", resumeLinkCell: "" };
  }

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: tallySheetId,
    range: "A2:Z1000",
  });

  const rows = res.data.values || [];
  const row = rows.find((r: any[]) => safeStr(r?.[iDate]) === today);

  if (!row) return { count: 0, lastScore: null, r2Key: "", r2KeysCsv: "", resumeLinkCell: "" };

  const count = Number(row?.[iCount] || 0);
  const lastScoreNum = Number(row?.[iLastScore]);
  const lastScore = Number.isFinite(lastScoreNum) ? lastScoreNum : null;
  const r2KeysCsv = safeStr(row?.[iR2Keys]);
  const r2Key = safeStr(row?.[iR2Key]);
  const resumeLinkCell = safeStr(row?.[iResumeFile]);

  return { count, lastScore, r2Key, r2KeysCsv, resumeLinkCell };
}

async function tallyApply(
  tallySheetId: string,
  customerId: string,
  docType: "RESUME" | "NON_RESUME",
  source: string,
  r2Key?: string,
  docToken?: string,
  ai?: any
) {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const today = formatISO(toZonedTime(new Date(), TIMEZONE), { representation: "date" });

  const { rowIndex0, currentCount, currentNotes } = await ensureTodayTallyRow(tallySheetId, today, customerId);

  const cleanedNotes = stripR2FromNotes(currentNotes)
    .split(",")
    .map((s: string) => s.trim())
    .filter(Boolean)
    .filter((s: string) => s !== "inbound-file:NON_RESUME")
    .join(", ");

  const rowNumber = rowIndex0 + 2;
  const note = docType === "RESUME" ? `${source}:RESUME` : `${source}:NON_RESUME`;
  const nextNotes = appendNote(cleanedNotes, note);

  // idempotency: if docToken already recorded today, do not increment count again
  let shouldIncrement = docType === "RESUME";
  if (docType === "RESUME" && docToken) {
    try {
      const existingG = await sheets.spreadsheets.values.get({
        spreadsheetId: tallySheetId,
        range: `G${rowNumber}`,
      });
      const currentG = safeStr(existingG.data.values?.[0]?.[0]);

      const hasTokenAlready = currentG
        ? currentG
            .split(",")
            .map((s: string) => s.trim())
            .filter(Boolean)
            .includes(docToken)
        : false;

      if (hasTokenAlready) {
        shouldIncrement = false;
        console.log("🧷 TALLY_IDEMPOTENT_SKIP:", customerId, today, docToken);
        return { today, nextCount: currentCount, nextNotes, shouldIncrement: false };
      }
    } catch {
      // ignore
    }
  }

  const nextCount = shouldIncrement ? currentCount + 1 : currentCount;

  await sheets.spreadsheets.values.update({
    spreadsheetId: tallySheetId,
    range: `B${rowNumber}`,
    valueInputOption: "RAW",
    requestBody: { values: [[nextCount]] },
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: tallySheetId,
    range: `C${rowNumber}`,
    valueInputOption: "RAW",
    requestBody: { values: [[nextNotes]] },
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: tallySheetId,
    range: `D${rowNumber}`,
    valueInputOption: "RAW",
    requestBody: { values: [[customerId]] },
  });

  if (docType === "RESUME" && r2Key) {
    // E = latest r2Key
    await sheets.spreadsheets.values.update({
      spreadsheetId: tallySheetId,
      range: `E${rowNumber}`,
      valueInputOption: "RAW",
      requestBody: { values: [[r2Key]] },
    });

    // F = clickable link (USER_ENTERED so formula evaluates)
    const url = buildR2PublicUrl(r2Key);
    const fileCell = url ? `=HYPERLINK("${url}","resume file")` : "";
    await sheets.spreadsheets.values.update({
      spreadsheetId: tallySheetId,
      range: `F${rowNumber}`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [[fileCell || url]] },
    });

    // G = idempotency tokens csv
    const existingG = await sheets.spreadsheets.values.get({
      spreadsheetId: tallySheetId,
      range: `G${rowNumber}`,
    });

    const currentG = safeStr(existingG.data.values?.[0]?.[0]);
    const tokens = currentG
      ? currentG
          .split(",")
          .map((s: string) => s.trim())
          .filter(Boolean)
      : [];

    const wanted: string[] = [];
    if (docToken) wanted.push(docToken); // stable: hash:...
    if (r2Key) wanted.push(`r2:${r2Key}`); // optional trace

    for (const w of wanted) {
      if (!tokens.includes(w)) tokens.push(w);
    }

    const nextG = tokens.join(", ");

    await sheets.spreadsheets.values.update({
      spreadsheetId: tallySheetId,
      range: `G${rowNumber}`,
      valueInputOption: "RAW",
      requestBody: { values: [[nextG]] },
    });

    const scoreNum = Number(ai?.score);
    if (Number.isFinite(scoreNum)) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: tallySheetId,
        range: `H${rowNumber}`,
        valueInputOption: "RAW",
        requestBody: { values: [[scoreNum]] },
      });
    }
  }

  return { today, nextCount, nextNotes, shouldIncrement };
}
// ============================
// Resumes Tab Helpers (Sheet must have all resumes)
// ============================
// ============================
// Job-Section Sheet Engine (vertical left layout)
// ============================
// Visual markers in the sheet so we can reliably find sections
const JOB_HEADER_PREFIX = "JOB:";

// Columns for the resume rows (kept consistent everywhere)
const RESUME_COL_HEADERS = [
  "receivedAt",
  "source",
  "filename",
  "score",
  "decision",
  "summary",
  "r2Key",
  "resumeLink",
  "requestId",
];

// ✅ ONE TAB ONLY: rename first tab to "Resumes" and delete all others
async function ensureSingleTabResumes(spreadsheetId: string) {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });

  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const sheetList = meta.data.sheets || [];
  if (!sheetList.length) return;

  const first = sheetList[0];
  const firstId = first.properties?.sheetId;
  const firstTitle = safeStr(first.properties?.title);

  const requests: any[] = [];

  // Rename first tab to "Resumes"
  if (Number.isFinite(firstId) && firstTitle !== "Resumes") {
    requests.push({
      updateSheetProperties: {
        properties: { sheetId: firstId, title: "Resumes" },
        fields: "title",
      },
    });
  }

  // Delete every other tab (hard enforce "one tab")
  for (let i = 1; i < sheetList.length; i++) {
    const sid = sheetList[i]?.properties?.sheetId;
    if (Number.isFinite(sid)) {
      requests.push({ deleteSheet: { sheetId: sid } });
    }
  }

  if (!requests.length) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests },
  });

  devLog("✅ SINGLE_TAB_ENFORCED:", spreadsheetId, {
    renamedFrom: firstTitle || "(unknown)",
    deletedTabs: Math.max(sheetList.length - 1, 0),
  });
}

async function getSheetIdByTitle(spreadsheetId: string, title: string): Promise<number | null> {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const found = meta.data.sheets?.find((s: any) => safeStr(s?.properties?.title) === title);
  const id = found?.properties?.sheetId;
  return Number.isFinite(id) ? Number(id) : null;
}

async function readResumesTabValues(spreadsheetId: string): Promise<string[][]> {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const TAB = "Resumes";

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${TAB}!A1:H5000`,
  });

  const vals = (resp.data.values || []) as any[];
  return vals.map((r) => (Array.isArray(r) ? r.map((c) => safeStr(c)) : []));
}

function jobHeaderCell(jobTitle: string) {
  return `${JOB_HEADER_PREFIX} ${safeStr(jobTitle)}`.trim();
}

function isJobHeaderRow(row: string[]) {
  const a = safeStr(row?.[0]);
  return a.startsWith(JOB_HEADER_PREFIX);
}

function findJobSectionStart(values: string[][], jobTitle: string): number {
  const want = jobHeaderCell(jobTitle);
  for (let i = 0; i < values.length; i++) {
    if (safeStr(values[i]?.[0]) === want) return i; // 0-based
  }
  return -1;
}

function findJobSectionEnd(values: string[][], start0: number): number {
  // end is the first next job header OR end of sheet
  for (let i = start0 + 1; i < values.length; i++) {
    if (isJobHeaderRow(values[i])) return i; // 0-based start of next section
  }
  return values.length; // end of sheet
}

function isBlankRow(row: string[]) {
  return row.every((c) => !safeStr(c));
}

async function appendJobSectionAtBottom(spreadsheetId: string, jobTitle: string) {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const TAB = "Resumes";

  // If sheet already has content, add a blank spacer row before a new section
  const existing = await readResumesTabValues(spreadsheetId);
  const hasAny = existing.some((r) => r.some((c) => safeStr(c)));

  const sectionRows: any[] = [];

  if (hasAny) sectionRows.push(["", "", "", "", "", "", "", "", ""]); // spacer (9 cols)

sectionRows.push([jobHeaderCell(jobTitle), "", "", "", "", "", "", "", ""]); // job header row (9 cols)
sectionRows.push([...RESUME_COL_HEADERS]); // column headers row (must be 9 cols)

await sheets.spreadsheets.values.append({
  spreadsheetId,
  range: `${TAB}!A:I`,
  valueInputOption: "RAW",
  insertDataOption: "INSERT_ROWS",
  requestBody: { values: sectionRows },
});
}

async function ensureJobSectionExists(spreadsheetId: string, jobTitle: string): Promise<void> {
  await ensureResumesTab(spreadsheetId);

  const values = await readResumesTabValues(spreadsheetId);
  const start0 = findJobSectionStart(values, jobTitle);
  if (start0 !== -1) return;

  await appendJobSectionAtBottom(spreadsheetId, jobTitle);
  devLog("✅ JOB_SECTION_CREATED:", spreadsheetId, jobTitle);
}

async function appendResumeUnderJobSection(args: {
  spreadsheetId: string;
  jobTitle: string;
  row: {
    receivedAt: string;
    source: string;
    filename: string;
    score: number | null;

    // ✅ New column
    decision?: string;

    summary: string;
    r2Key: string;
    resumeLink?: string;
    requestId: string;
  };
}) {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const TAB = "Resumes";

  await ensureJobSectionExists(args.spreadsheetId, args.jobTitle);

  const values = await readResumesTabValues(args.spreadsheetId);
  const start0 = findJobSectionStart(values, args.jobTitle);

  // Safety: if still not found, just append at bottom (should not happen)
  if (start0 === -1) {
    await appendResumeRow(args.spreadsheetId, args.row);
    return;
  }

  const nextSectionStart0 = findJobSectionEnd(values, start0);

  // We want to insert AFTER:
  // start0 = job header
  // start0+1 = column headers
  // then existing resumes until blank row or next job header
  let insertAt0 = Math.min(start0 + 2, values.length);

  for (let i = start0 + 2; i < nextSectionStart0; i++) {
    const r = values[i] || [];
    // stop at blank spacer row (keeps sections separated cleanly)
    if (isBlankRow(r)) {
      insertAt0 = i;
      break;
    }
    insertAt0 = i + 1;
  }

  const sheetId = await getSheetIdByTitle(args.spreadsheetId, TAB);

  // If we can't resolve sheetId, fall back to values.append at bottom (safe)
  if (sheetId == null) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: args.spreadsheetId,
    range: `${TAB}!A:I`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: {
      values: [[
        args.row.receivedAt,
        args.row.source,
        args.row.filename,
        args.row.score ?? "",
        (args.row as any).decision ?? "",
        args.row.summary || "",
        args.row.r2Key || "",
        args.row.resumeLink || "",
        args.row.requestId || "",
      ]],
    },
  });
  return;
}
  // Insert a blank row at insertAt0 (0-based) to keep section intact
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: args.spreadsheetId,
    requestBody: {
      requests: [
        {
          insertDimension: {
            range: {
              sheetId,
              dimension: "ROWS",
              startIndex: insertAt0,      // 0-based, insert BEFORE this row
              endIndex: insertAt0 + 1,
            },
            inheritFromBefore: true,
          },
        },
      ],
    },
  });

  // Write the resume row into that inserted row
  const rowNumber = insertAt0 + 1; // 1-based for A1 notation
  await sheets.spreadsheets.values.update({
  spreadsheetId: args.spreadsheetId,
  range: `${TAB}!A${rowNumber}:I${rowNumber}`,
  valueInputOption: "RAW",
  requestBody: {
    values: [[
      args.row.receivedAt,
      args.row.source,
      args.row.filename,
      args.row.score ?? "",
      (args.row as any).decision ?? "",
      args.row.summary || "",
      args.row.r2Key || "",
      args.row.resumeLink || "",
      args.row.requestId || "",
    ]],
  },
});

  devLog("🧩 RESUME_APPENDED_UNDER_JOB:", args.spreadsheetId, args.jobTitle, { rowNumber });
}
async function ensureSheetTabExists(spreadsheetId: string, title: string) {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });

  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const existing = meta.data.sheets?.map((s: any) => s.properties?.title) || [];

  if (existing.includes(title)) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [
        {
          addSheet: {
            properties: { title },
          },
        },
      ],
    },
  });

   devLog("✅ SHEET_TAB_CREATED:", spreadsheetId, title);
}

// line above (keep whatever you already have above)

async function ensureResumesTab(spreadsheetId: string) {
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const TAB = "Resumes";

  // ✅ ONE TAB ONLY (Resumes): enforce before touching headers
  await ensureSingleTabResumes(spreadsheetId);

  const headerResp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${TAB}!A1:H1`,
  });

  const existing = headerResp.data.values?.[0] || [];
  if (existing.length > 0) {
    devLog("✅ RESUMES_TAB_INITIALIZED_SINGLE_TAB:", spreadsheetId);
    return;
  }

 await sheets.spreadsheets.values.update({
  spreadsheetId,
  range: `${TAB}!A1:I1`,
  valueInputOption: "RAW",
  requestBody: {
    values: [
      [
        "receivedAt",
        "source",
        "filename",
        "score",
        "decision",
        "summary",
        "r2Key",
        "resumeLink",
        "requestId",
      ],
    ],
  },
});

  devLog("✅ RESUMES_TAB_INITIALIZED:", spreadsheetId);
}

// line below (keep whatever you already have below)
  
// ============================
// General Submissions Helpers (ONE TAB ONLY)
// - DO NOT create a separate tab.
// - "General Submissions" is a JOB section inside the single "Resumes" tab.
// ============================

async function ensureGeneralSubmissionsTab(spreadsheetId: string) {
  // Back-compat: old name, new behavior.
  // Enforce ONE TAB ONLY and ensure the "General Submissions" section exists.
  await ensureSingleTabResumes(spreadsheetId);
  await ensureJobSectionExists(spreadsheetId, "General Submissions");
}

async function appendGeneralSubmissionRow(
  spreadsheetId: string,
  row: {
    receivedAt: string;
    source: string;
    filename: string;
    reason: string;
    r2Key: string;
    resumeLink?: string;
    requestId: string;
  }
) {
  await ensureSingleTabResumes(spreadsheetId);
  await ensureJobSectionExists(spreadsheetId, "General Submissions");

  // Append using the SAME row schema as resumes, but with score=null and summary=reason
  await appendResumeUnderJobSection({
    spreadsheetId,
    jobTitle: "General Submissions",
    row: {
      receivedAt: row.receivedAt,
      source: row.source,
      filename: row.filename,
      score: null,

      // ✅ Anything in General Submissions is an automatic NO
      decision: "NO",

      summary: safeStr(row.reason || "").slice(0, 2000),
      r2Key: row.r2Key || "",
      resumeLink: row.resumeLink || "",
      requestId: row.requestId || "",
    },
  });

  devLog("📝 GENERAL_SUBMISSION_SECTION_APPENDED:", spreadsheetId, row.requestId);
}

async function appendResumeRow(
  spreadsheetId: string,
  row: {
    receivedAt: string;
    source: string;
    filename: string;
    score: number | null;
    summary: string;
    r2Key: string;
    resumeLink?: string;
    requestId: string;
  }
) {
  // Old helper appended directly to Resumes tab.
  // New behavior: enforce single tab + append under "Unsorted" job section
  // (your routing code should call appendResumeUnderJobSection for matched jobs;
  // this preserves any legacy call sites that still call appendResumeRow).
  await ensureSingleTabResumes(spreadsheetId);
  await ensureJobSectionExists(spreadsheetId, "Unsorted");

  await appendResumeUnderJobSection({
    spreadsheetId,
    jobTitle: "Unsorted",
    row: {
      receivedAt: row.receivedAt,
      source: row.source,
      filename: row.filename,
      score: row.score ?? null,
      summary: safeStr(row.summary || "").slice(0, 5000),
      r2Key: row.r2Key || "",
      resumeLink: row.resumeLink || "",
      requestId: row.requestId || "",
    },
  });

  devLog("📝 RESUME_SECTION_APPENDED:", spreadsheetId, row.requestId);
}


// ============================
// Tally sheet creation
// ============================

async function createTallySheetForCustomer(companyName: string, customerId: string) {
  const drive = google.drive({ version: "v3", auth: oauth2Client });
  const sheets = google.sheets({ version: "v4", auth: oauth2Client });

  const parentFolderId = safeStr(process.env.TALLY_DRIVE_FOLDER_ID);

  const created = await drive.files.create({
    requestBody: {
      name: `${companyName} - Tally`,
      mimeType: "application/vnd.google-apps.spreadsheet",
      ...(parentFolderId ? { parents: [parentFolderId] } : {}),
    },
    fields: "id,webViewLink",
  });

  const spreadsheetId = safeStr(created.data.id);
  const webViewLink =
    safeStr((created.data as any).webViewLink) ||
    (spreadsheetId ? `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit` : "");

  if (!spreadsheetId) throw new Error("tally_sheet_create_failed");

  // A date, B count, C notes, D customerId, E latest r2Key, F fileLink, G r2Keys csv, H lastScore
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: "A1:H1",
    valueInputOption: "RAW",
    requestBody: {
      values: [["date", "resumesProcessed", "notes", "customerId", "r2Key", "resumeFile", "r2Keys", "lastScore"]],
    },
  });

  // Share with admin + current authed account (best effort)
  if (ADMIN_EMAIL) await ensureSheetSharedOnce(spreadsheetId, ADMIN_EMAIL);
  const authedEmail = await logAuthedGoogleEmailFromTokens();
  if (authedEmail) await ensureSheetSharedOnce(spreadsheetId, authedEmail);

  // ✅ Hard enforce: ONE TAB ONLY + baseline sections exist
  try {
    await ensureSingleTabResumes(spreadsheetId); // ✅ rename first tab to "Resumes" and delete others
    await ensureJobSectionExists(spreadsheetId, "Unsorted");
    await ensureJobSectionExists(spreadsheetId, "General Submissions"); // ✅ section, not tab
  } catch (e: any) {
    console.warn("⚠️ ENSURE_SHEET_LAYOUT_FAILED (continuing):", e?.message || e);
  }

  return { spreadsheetId, spreadsheetUrl: webViewLink };
}
// ============================
// AI scoring wrapper
// ============================
async function safeScoreResume(text: string, rubric: any | null) {
  const fn: any = scoreResume as any;
  if (typeof fn !== "function") throw new Error("scoreResume_not_a_function");

  if (fn.length >= 2) return await fn(text, rubric);

  if (rubric) {
    const rubricBlock = typeof rubric === "string" ? rubric : JSON.stringify(rubric, null, 2);
    const merged = `RUBRIC (customer scoring criteria):\n${rubricBlock}\n\n${text}`;
    return await fn(merged);
  }

  return await fn(text);
}


// ============================
// AI JOB CLASSIFIER
// ============================

// ✅ Single source of truth (define ONCE in entire file)
const JOB_MATCH_CONFIDENCE_THRESHOLD = 0.65;

// ✅ Classifies a resume to the best matching job (or null) using your existing OpenAI wrapper
async function classifyResumeToJob(
  resumeText: string,
  jobs: CustomerJob[]
): Promise<{ matchedJobId: string | null; confidence: number }> {
  if (!Array.isArray(jobs) || jobs.length === 0) return { matchedJobId: null, confidence: 0 };

  const jobPayload = jobs.map((j) => ({
    id: safeStr(j?.id),
    title: safeStr(j?.title),
    rubric: (j as any)?.rubric ?? null,
  }));

  // Keep prompt deterministic + short enough
  const prompt = `
You are an AI hiring classifier.

Given a resume and a list of job definitions,
determine which job this resume best matches.

Return STRICT JSON ONLY in this format:

{
  "matchedJobId": "job_id_here_or_null",
  "confidence": 0-1
}

Rules:
- If no strong match exists, return matchedJobId=null.
- Confidence must be a number between 0 and 1.

JOBS:
${JSON.stringify(jobPayload, null, 2)}

RESUME:
${safeStr(resumeText).slice(0, 5000)}
`;

  try {
    // ✅ Use existing wrapper for consistency (same model/settings as scoring)
    const result: any = await safeScoreResume(prompt, null);

    // Sometimes wrappers return objects; sometimes strings
    const parsed = typeof result === "string" ? tryJsonParse<any>(result) : result;

    const matchedJobId = safeStr(parsed?.matchedJobId) || null;

    const confRaw = Number(parsed?.confidence);
    const confidence = Number.isFinite(confRaw) ? Math.min(Math.max(confRaw, 0), 1) : 0;

    // If matchedJobId doesn't exist in the provided list, treat as no match
    if (matchedJobId && !jobs.some((j) => safeStr(j?.id) === matchedJobId)) {
      return { matchedJobId: null, confidence: 0 };
    }

    return { matchedJobId, confidence };
  } catch {
    return { matchedJobId: null, confidence: 0 };
  }
}

// ============================
// Inbound processing core
// ============================
type InboundDocResult = {
  ok: true;
  source: "resend" | "inbound-file" | "inbound-r2";
  savedLocal: string | null;
  deletedLocal: boolean;
  r2: { bucket: string; key: string } | null;
  r2Key: string | null;

  toEmail: string;
  customerId: string;
  resolvedCustomerId: string;
  docType: "RESUME" | "NON_RESUME";
  textPreview: string;

  billingStatus: string;
  blocked: boolean;
  blockedReason: string | null;

  tally: any;
  ai: any;
  matchFound: boolean;
};

async function processInboundDoc(args: {
  requestId: string;
  source: "resend" | "inbound-file" | "inbound-r2";
  filename: string;
  buffer: Buffer;
  extractedText?: string;
  docType?: "RESUME" | "NON_RESUME";
  customerId?: string;
  toEmail?: string;
  r2?: { bucket: string; key: string } | null;
  savedLocal?: string | null;
  deletedLocal?: boolean;
}): Promise<InboundDocResult> {
  const extractedTextRaw =
    typeof args.extractedText === "string" ? args.extractedText : await extractTextFromBuffer(args.filename, args.buffer);

  // Hardening Step 2: cap extraction size
  const extractedTooLarge = extractedTextRaw.length > MAX_EXTRACTED_CHARS;
  const extractedText = extractedTooLarge ? extractedTextRaw.slice(0, MAX_EXTRACTED_CHARS) : extractedTextRaw;

  logInfo("PROCESS_INBOUND_START", {
    requestId: args.requestId,
    source: args.source,
    filename: args.filename,
    r2Key: args.r2?.key,
  });

  if (extractedTooLarge) {
    logWarn("EXTRACT_TOO_LARGE", {
      requestId: args.requestId,
      filename: args.filename,
      extractedChars: extractedTextRaw.length,
      max: MAX_EXTRACTED_CHARS,
    });
  }

  const docType: "RESUME" | "NON_RESUME" =
    args.docType || (extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME");

  const toEmail = safeStr(args.toEmail).trim().toLowerCase();
const filenameForEmail = safeStr(args.filename);
const r2KeyForEmail = safeStr(args?.r2?.key || "");
let resolvedCustomerId = "";
let customerId = safeStr((args as any).customerId).trim();
let match: CustomerRow | null = null;

// ✅ Prefer explicit customerId when provided (inbound-file/inbound-r2 testing & future API use)
if (customerId) {
  resolvedCustomerId = customerId;
}
// ✅ If customerId is provided, resolve customer directly (no intake email required)
if (resolvedCustomerId) {
  try {
    const existingCustomers = await getCustomersCached();
    match =
      [...existingCustomers]
        .reverse()
        .find((x) => safeStr(x.customerId) === resolvedCustomerId) || null;

    if (match) {
      customerId = safeStr(match.customerId);
      resolvedCustomerId = customerId;
    } else {
      console.log("⚠️ No customer found for customerId=", resolvedCustomerId);
    }
  } catch (e: any) {
    console.warn("⚠️ CUSTOMER_LOOKUP_BY_ID_FAILED:", e?.message || e);
  }
}
if (!resolvedCustomerId && toEmail) {
  try {
    const existingCustomers = await getCustomersCached();
    match =
      [...existingCustomers]
        .reverse()
        .find((x) => safeStr(x.intakeEmail).trim().toLowerCase() === toEmail) || null;

    if (match) {
      customerId = safeStr(match.customerId);
      resolvedCustomerId = customerId;
    } else {
      console.log("⚠️ No customer resolved. toEmail=", toEmail);
    }
  } catch (e: any) {
    console.warn("⚠️ CUSTOMER_RESOLVE_SKIPPED (google auth?):", e?.message || e);
  }
}
  const billingStatus = safeStr(match?.status || "");
  const gate = isProcessingAllowed(billingStatus);
  const blocked = !!customerId && gate.allowed === false;
  const blockedReason = blocked ? gate.reason || "billing_block" : null;

  const rubric = customerId ? await getCustomerRubric(customerId) : null;

  // ----------------------------
  // AI scoring (with idempotency)
  // ----------------------------
  let ai: any = null;

  try {
    if (blocked) {
      ai = { skipped: true, reason: "billing_block", billingStatus, blockedReason };
    } else if (docType === "RESUME" && extractedText.trim().length > 40) {
      if (extractedTooLarge) {
        ai = { skipped: true, reason: "too_large", maxExtractedChars: MAX_EXTRACTED_CHARS };
      } else {
        const tallySheetId = safeStr(match?.tallySheetId);
        const today = formatISO(toZonedTime(new Date(), TIMEZONE), { representation: "date" });

        // Stable idempotency token
        const docHash = sha256Hex(args.buffer);
        const docToken = `hash:${docHash}`;

        // AI idempotency (daily): if docToken already in column G today, skip OpenAI
        let alreadyProcessedToday = false;

        try {
          if (tallySheetId && customerId && docToken) {
            const sheets = google.sheets({ version: "v4", auth: oauth2Client });

            const { rowIndex0 } = await ensureTodayTallyRow(tallySheetId, today, customerId);
            const rowNumber = rowIndex0 + 2;

            const existingG = await sheets.spreadsheets.values.get({
              spreadsheetId: tallySheetId,
              range: `G${rowNumber}`,
            });

            const currentCsv = safeStr(existingG.data.values?.[0]?.[0]);
            alreadyProcessedToday = currentCsv
              ? currentCsv
                  .split(",")
                  .map((s: string) => s.trim())
                  .filter(Boolean)
                  .includes(docToken)
              : false;
          }
        } catch (e: any) {
          console.warn("⚠️ AI_IDEMPOTENCY_CHECK_FAILED:", e?.message || e);
          alreadyProcessedToday = false;
        }

        if (extractedText && extractedText.length >= 120) {
          if (alreadyProcessedToday) {
            ai = { skipped: true, reason: "idempotent_skip" };
            console.log("🧷 AI_IDEMPOTENT_SKIP:", customerId, today, docToken);
          } else {
            const forAi = extractedText.slice(0, MAX_OPENAI_CHARS);
            ai = await safeScoreResume(forAi, rubric);
          }
        } else {
          ai = { skipped: true, reason: "too_short" };
        }
      }
    } else {
      ai = { skipped: true, reason: docType !== "RESUME" ? "non_resume" : "too_short" };
    }
  } catch (e: any) {
    ai = { error: "ai_failed", message: String(e?.message || e) };
  }

  // ----------------------------
  // Tally apply
  // ----------------------------
  let tallyResult: any = null;
  try {
    const sheetId = safeStr(match?.tallySheetId);
    const r2Key = safeStr(args?.r2?.key || "");
    const docToken = `hash:${sha256Hex(args.buffer)}`;
    const shouldSkipTally = ai?.skipped === true && ai?.reason === "idempotent_skip";

    if (blocked) {
      tallyResult = { skipped: true, reason: "billing_block", billingStatus, blockedReason };
    } else if (shouldSkipTally) {
      const today = formatISO(toZonedTime(new Date(), TIMEZONE), { representation: "date" });
      tallyResult = { skipped: true, reason: "idempotent_skip", today, shouldIncrement: false };
      console.log("🧷 TALLY_SKIP_DUPLICATE:", customerId, today, docToken);
    } else if (sheetId && customerId) {
      tallyResult = await tallyApply(sheetId, customerId, docType, args.source, r2Key || undefined, docToken, ai);

      if ((tallyResult as any)?.shouldIncrement === false) {
        logInfo("TALLY_APPLY_SKIP_OK", {
          requestId: args.requestId,
          customerId,
          date: tallyResult?.today,
          nextCount: tallyResult?.nextCount,
          shouldIncrement: false,
        });
      } else {
        logInfo("TALLY_APPLY_OK", {
          requestId: args.requestId,
          customerId,
          date: tallyResult?.today,
          nextCount: tallyResult?.nextCount,
          shouldIncrement: true,
        });
      }
    } else {
      logWarn("TALLY_SKIP_MISSING_IDS", {
        requestId: args.requestId,
        customerId,
        sheetId,
      });
    }
  } catch (e: any) {
    const status = e?.response?.status || e?.code || null;
    const data = e?.response?.data || null;
    console.error("❌ TALLY_FAILED:", e?.message || e);
    console.error("   ↳ status:", status);
    console.error("   ↳ data:", JSON.stringify(data));
    tallyResult = { error: "tally_failed", status, data, message: String(e?.message || e) };
  }

  const textPreview = String(extractedText || "").slice(0, 400);

  const aiScoreNum = Number(ai?.score);
  await saveInboundDocToDb({
  source: args.source,
  requestId: args.requestId, // 🔐 required for /r/:requestId
  toEmail,
  customerId: customerId || undefined,
  resolvedCustomerId: resolvedCustomerId || undefined,
  matchFound: !!match,
  billingStatus: billingStatus || undefined,
  blockedReason: blockedReason || undefined,
  filename: args.filename,
  r2Bucket: args.r2?.bucket,
  r2Key: args.r2?.key,
  docType,
  extractedChars: extractedText?.length || 0,
  textPreview,
  aiScore: Number.isFinite(aiScoreNum) ? aiScoreNum : null,
  aiJson: ai,
});
   // Result email (best-effort) — STRICT idempotency gate (only email when tally increments)
  try {
    const didIncrement = (tallyResult as any)?.shouldIncrement === true;

    // ✅ Sheet must have all resumes: append row ONLY when tally increments
    try {
      const sheetId = safeStr(match?.tallySheetId);
      const isResumeDoc = docType === "RESUME";
      const r2Key = safeStr(args?.r2?.key || "");
      const resumeLink = `${APP_URL}/r/${args.requestId}`;

      if (didIncrement && sheetId && customerId && isResumeDoc) {
        const scoreNum = Number(ai?.score);
        const score = Number.isFinite(scoreNum) ? scoreNum : null;

        const summary =
          safeStr(ai?.summary) ||
          safeStr(ai?.notes) ||
          safeStr(ai?.feedback) ||
          safeStr(ai?.reason) ||
          "";

        const aiSkippedLocal = !!ai?.skipped;
        const aiErrorLocal = !!ai?.error;
        const passedAiCriteria = !aiSkippedLocal && !aiErrorLocal;

        // 🔒 One tab only + baseline sections always exist
        await ensureResumesTab(sheetId);// rename first tab to "Resumes", never create extra tabs
        await ensureJobSectionExists(sheetId, "Unsorted");
        await ensureJobSectionExists(sheetId, "General Submissions");
        if (passedAiCriteria) {
          // ✅ PASS → route to Resumes section (job section / Unsorted)
          const jobs = await listCustomerJobs(customerId);

          const { matchedJobId, confidence } = await classifyResumeToJob(extractedText, jobs);

          let jobTitle = "Unsorted";

          if (matchedJobId && confidence >= JOB_MATCH_CONFIDENCE_THRESHOLD) {
          const j = jobs.find((x) => safeStr(x.id) === matchedJobId);
          if (j?.title) jobTitle = safeStr(j.title);
          }

           // ✅ Ensure the correct section exists before appending
          await ensureJobSectionExists(sheetId, jobTitle);
          await appendResumeUnderJobSection({
            spreadsheetId: sheetId,
            jobTitle,
            row: {
              receivedAt: new Date().toISOString(),
              source: args.source,
              filename: safeStr(args.filename),
              score,

              // ✅ Hiring decision (simple + explainable)
              decision:
                Number(score) >= 80 ? "CALL" : Number(score) >= 50 ? "MAYBE" : "NO",

              summary: summary.slice(0, 2000),
              r2Key,
              resumeLink,
              requestId: args.requestId,
            },
          });

          devLog("🧠 JOB_MATCH_DECISION:", {
            requestId: args.requestId,
            customerId,
            matchedJobId,
            confidence,
            jobTitle,
          });
        } else {
          // 🔵 FAIL AI criteria → route to "General Submissions" section (same sheet, same schema)
          await ensureJobSectionExists(sheetId, "General Submissions");
          const reasonText = aiErrorLocal
            ? "ai_error"
            : `ai_skipped:${safeStr(ai?.reason) || "unknown"}`;

          await appendResumeUnderJobSection({
            spreadsheetId: sheetId,
            jobTitle: "General Submissions",
            row: {
              receivedAt: new Date().toISOString(),
              source: args.source,
              filename: safeStr(args.filename),
              score: null, // ✅ no score for General Submissions
              summary: reasonText.slice(0, 2000), // ✅ store why it went here
              r2Key,
              resumeLink,
              requestId: args.requestId,
            },
          });

          devLog("🧾 ROUTED_GENERAL_SUBMISSION:", {
            requestId: args.requestId,
            customerId,
            reason: aiErrorLocal ? "ai_error" : safeStr(ai?.reason),
          });
        }
      }
    } catch (e: any) {
      console.warn("⚠️ APPEND_RESUME_ROW_FAILED (continuing):", e?.message || e);
    }

    const hasToEmail = !!toEmail;
    const matchFound = !!match;
    const isResume = docType === "RESUME";

    const aiSkipped = !!ai?.skipped;
    const aiError = !!ai?.error;

    // HARD GATE: never email if we didn't increment tally
    if (!didIncrement) {
      devLog("📧 RESULT_EMAIL_SKIP:", {
        requestId: args.requestId,
        hasToEmail,
        matchFound,
        blocked,
        docType,
        aiSkipped,
        aiError,
        tallyIncremented: didIncrement,
        reason: "idempotent_skip_no_increment",
      });

      return {
        ok: true,
        source: args.source,
        savedLocal: args.savedLocal ?? null,
        deletedLocal: !!args.deletedLocal,
        r2: args.r2 ?? null,
        r2Key: args.r2?.key || null,

        toEmail,
        customerId,
        resolvedCustomerId,
        docType,
        textPreview,

        billingStatus,
        blocked,
        blockedReason,

        tally: typeof tallyResult !== "undefined" ? tallyResult : null,
        ai,
        matchFound: !!match,
      };
    }

    const SEND_RECEIPT_IF_AI_SKIPPED = true;

    const scoredEligible =
      hasToEmail && matchFound && !blocked && isResume && !aiSkipped && !aiError;

    const receiptEligible =
      hasToEmail && matchFound && !blocked && isResume && SEND_RECEIPT_IF_AI_SKIPPED;

    if (!scoredEligible && !receiptEligible) {
      const reason =
        !hasToEmail
          ? "missing_toEmail"
          : !matchFound
          ? "no_customer_match"
          : blocked
          ? "blocked"
          : !isResume
          ? "not_resume"
          : aiError
          ? "ai_error"
          : "ai_skipped_and_receipt_disabled";

      devLog("📧 RESULT_EMAIL_SKIP:", {
        requestId: args.requestId,
        hasToEmail,
        matchFound,
        blocked,
        docType,
        aiSkipped,
        aiError,
        tallyIncremented: didIncrement,
        reason,
      });

      return {
        ok: true,
        source: args.source,
        savedLocal: args.savedLocal ?? null,
        deletedLocal: !!args.deletedLocal,
        r2: args.r2 ?? null,
        r2Key: args.r2?.key || null,

        toEmail,
        customerId,
        resolvedCustomerId,
        docType,
        textPreview,

        billingStatus,
        blocked,
        blockedReason,

        tally: typeof tallyResult !== "undefined" ? tallyResult : null,
        ai,
        matchFound: !!match,
      };
    }

    const score = typeof ai?.score !== "undefined" ? String(ai.score) : "N/A";
    const summary2 =
      safeStr(ai?.summary) ||
      safeStr(ai?.notes) ||
      safeStr(ai?.feedback) ||
      safeStr(ai?.reason) ||
      "";

    const strengths = Array.isArray(ai?.strengths) ? ai.strengths.slice(0, 4) : [];
    const weaknesses = Array.isArray(ai?.weaknesses) ? ai.weaknesses.slice(0, 4) : [];

    const isReceipt = !scoredEligible;
    const subject = isReceipt ? "Resume received" : `Resume Score: ${score}/100`;

    let text: string;

    if (isReceipt) {
      text = [
        `We received your resume and it is being processed.`,
        ``,
        `File: ${filenameForEmail}`,
        r2KeyForEmail ? `Reference ID: ${r2KeyForEmail}` : undefined,
        ``,
        `If you have any questions, reply to this email.`,
      ]
        .filter(Boolean)
        .join("\n");
    } else {
      text = [
        `Your resume has been successfully analyzed.`,
        ``,
        `Overall Score: ${score}/100`,
        ``,
        summary2 ? `Summary:` : undefined,
        summary2 ? summary2 : undefined,
        ``,
        strengths.length ? `Key Strengths:` : undefined,
        ...strengths.map((s: string) => `• ${s}`),
        ``,
        weaknesses.length ? `Areas for Improvement:` : undefined,
        ...weaknesses.map((w: string) => `• ${w}`),
        ``,
        `File: ${filenameForEmail}`,
        r2KeyForEmail ? `Reference ID: ${r2KeyForEmail}` : undefined,
        ``,
        `Thank you for using Digital Dominance Resume Scoring.`,
      ]
        .filter(Boolean)
        .join("\n");
    }

    await sendCustomerText(toEmail!, subject, text);

    console.log(isReceipt ? "📧 RESULT_EMAIL_SENT_RECEIPT" : "📧 RESULT_EMAIL_SENT_SCORED", {
      toEmail,
      customerId: resolvedCustomerId || customerId,
      docType,
      tallyIncremented: didIncrement,
      aiSkipped,
      aiError,
    });
  } catch (e: any) {
    console.log("⚠️ RESULT_EMAIL_FAILED:", String(e?.message || e));
  }

  return {
    ok: true,
    source: args.source,
    savedLocal: args.savedLocal ?? null,
    deletedLocal: !!args.deletedLocal,
    r2: args.r2 ?? null,
    r2Key: args.r2?.key || null,

    toEmail,
    customerId,
    resolvedCustomerId,
    docType,
    textPreview,

    billingStatus,
    blocked,
    blockedReason,

    tally: typeof tallyResult !== "undefined" ? tallyResult : null,
    ai,
    matchFound: !!match,
  };
} // <-- end processInboundDoc

// ============================
// Express app
// ============================
const app = express();

// ✅ CORS first
app.options(/.*/, cors(corsOptions));
app.use(cors(corsOptions));

// ✅ Then parse JSON
app.use(express.json({ limit: "2mb" }));

// ============================
// Stripe: start trial checkout (card now, bill in 30 days)
// ============================
app.post("/stripe/start-trial-checkout", async (req, res) => {
  try {
    if (!stripe) {
      return res.status(500).json({ ok: false, error: "stripe_not_configured" });
    }

    const { companyName, adminEmail, jobs, criteria } = req.body || {};

    if (!companyName || !adminEmail) {
      return res.status(400).json({ ok: false, error: "missing_company_or_email" });
    }
    const STRIPE_PRICE_ID = safeStr(process.env.STRIPE_PRICE_ID);
    if (!STRIPE_PRICE_ID) {
      return res.status(500).json({ ok: false, error: "missing_STRIPE_PRICE_ID" });
    }

    // Store full payload server-side (Stripe metadata is too small for jobs/criteria)
    const signupId = crypto.randomUUID();
    const payload = {
      companyName: safeStr(companyName),
      adminEmail: safeStr(adminEmail),
      jobs: Array.isArray(jobs) ? jobs : [],
      criteria: criteria ?? null,
    };

    await pool.query(
      `INSERT INTO pending_signups (id, payload_json) VALUES ($1, $2)`,
      [signupId, JSON.stringify(payload)]
    );

    const successUrl = `${APP_URL}/signup/success?signupId=${signupId}`;
    const cancelUrl = `${APP_URL}/signup?canceled=1`;

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      customer_email: safeStr(adminEmail),
      line_items: [{ price: STRIPE_PRICE_ID, quantity: 1 }],
      subscription_data: {
        trial_period_days: 30, // ✅ $0 today, starts billing after 30 days
        metadata: { signupId },
      },
      payment_method_collection: "always", // ✅ force card collection even if trial
      success_url: successUrl,
      cancel_url: cancelUrl,
      metadata: { signupId },
    });

    return res.json({ ok: true, url: session.url, signupId });
  } catch (err: any) {
    console.error("STRIPE_START_TRIAL_CHECKOUT_ERR", err?.message || err);
    return res.status(500).json({ ok: false, error: "stripe_checkout_failed" });
  }
});
// ✅ Handle preflight quickly
// ✅ CORS (fail-closed in prod; allow localhost in dev)
const DEV_ALLOWED_ORIGINS = ["http://localhost:3000", "http://127.0.0.1:3000"];

const corsOptions: cors.CorsOptions = {
  origin: (origin, cb) => {
    // Same-origin / server-to-server / curl (no Origin header)
    if (!origin) return cb(null, true);

    // Dev: allow localhost explicitly
    if (process.env.NODE_ENV !== "production") {
      if (DEV_ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
      return cb(new Error(`CORS blocked (dev): ${origin}`));
    }

    // Prod: fail-closed unless explicitly allowlisted
    const raw = (process.env.CORS_ALLOW_ORIGINS || "").trim();
    const allow = raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    if (allow.length === 0) return cb(new Error(`CORS blocked (prod): empty allowlist`));
    if (allow.includes(origin)) return cb(null, true);

    return cb(new Error(`CORS blocked (prod): ${origin}`));
  },
  credentials: true,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Admin-Secret", "X-Request-Id"],
};

// ✅ Handle preflight quickly (WITH the same options)
app.options(/.*/, cors(corsOptions));

// ✅ Apply CORS for all routes
app.use(cors(corsOptions));
app.options(/.*/, cors());
// --- Security headers ---
app.use((req, res, next) => {
  // ✅ Only set HSTS in production (only meaningful over HTTPS)
  if (process.env.NODE_ENV === "production") {
    res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload");
  }

  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");

  // ✅ Explicitly restrict powerful browser features (tight default)
  res.setHeader(
    "Permissions-Policy",
    "camera=(), microphone=(), geolocation=(), payment=(), usb=(), fullscreen=(), clipboard-read=(), clipboard-write=()"
  );

  res.setHeader("Cross-Origin-Resource-Policy", "same-site");
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");

  next();
});

// --- Correlation IDs + minimal request logging ---
app.use((req: Request, res: Response, next) => {
  const incoming = safeStr(req.header("X-Request-Id"));
  const requestId = incoming || genRequestId();
  (req as any).requestId = requestId;
  res.setHeader("X-Request-Id", requestId);

  // Patch res.json to always include requestId when body is an object
  const oldJson = res.json.bind(res);
  (res as any).json = (body: any) => {
    if (body && typeof body === "object" && !Array.isArray(body) && !("requestId" in body)) {
      body.requestId = requestId;
    }
    return oldJson(body);
  };

  const start = Date.now();
  res.on("finish", () => {
    // keep logs clean: only log webhooks + admin + auth by default, unless DEBUG_REQUEST_LOGS
    const p = safeStr(req.path || "");
    const shouldLog =
      truthyEnv("DEBUG_REQUEST_LOGS") || p.startsWith("/webhooks/") || p.startsWith("/admin/") || p.startsWith("/auth/");

    if (!shouldLog) return;

    const ms = Date.now() - start;
    const status = res.statusCode;

    // minimal structured-ish log
    console.log(`[REQ] ${requestId} ${req.method} ${p} ${status} ${ms}ms ip=${safeStr(getClientIp(req))}`);
  });

  next();
});

app.get("/auth/status", (_req: Request, res: Response) => {
  const creds: any = oauth2Client.credentials || {};
  return res.json({
    ok: true,
    connected: !!(creds.access_token || creds.refresh_token),
    hasRefreshToken: !!creds.refresh_token,
  });
});

/**
 * ✅ IMPORTANT:
 * Raw-body webhook routes MUST be registered BEFORE express.json()
 */

// ============================
// Resend receiving helpers
// ============================
type ResendReceivedEmail = { id: string; to?: string[] | string; text?: string };
type ResendReceivedAttachment = {
  id: string;
  filename?: string;
  size?: number;
  content_type?: string;
  download_url?: string;
};
function resendAuthHeaders() {
  return { Authorization: `Bearer ${RESEND_API_KEY}` };
}
async function resendRetrieveReceivedEmail(emailId: string): Promise<ResendReceivedEmail | null> {
  if (!RESEND_API_KEY || !emailId) return null;
  try {
    const resp = await axios.get(`https://api.resend.com/emails/receiving/${emailId}`, {
      headers: resendAuthHeaders(),
      timeout: 15_000,
    });
    const data = (resp.data as any)?.data || resp.data;
    if (!data) return null;
    return { id: safeStr(data.id || emailId), to: (data as any).to, text: (data as any).text };
  } catch (e: any) {
    console.warn("⚠️ RESEND_RETRIEVE_RECEIVED_FAILED:", e?.response?.data || e?.message || e);
    return null;
  }
}
async function resendListReceivedAttachments(emailId: string): Promise<ResendReceivedAttachment[]> {
  if (!RESEND_API_KEY || !emailId) return [];
  try {
    const resp = await axios.get(`https://api.resend.com/emails/receiving/${emailId}/attachments`, {
      headers: resendAuthHeaders(),
      timeout: 15_000,
    });
    const list = (resp.data as any)?.data || (resp.data as any)?.attachments || [];
    if (!Array.isArray(list)) return [];
    return list.map((a: any) => ({
      id: safeStr(a.id),
      filename: safeStr(a.filename) || undefined,
      size: Number(a.size) || undefined,
      content_type: safeStr(a.content_type) || undefined,
      download_url: safeStr(a.download_url) || undefined,
    }));
  } catch (e: any) {
    console.warn("⚠️ RESEND_LIST_ATTACHMENTS_FAILED:", e?.response?.data || e?.message || e);
    return [];
  }
}
async function downloadAttachmentToBuffer(downloadUrl: string): Promise<Buffer | null> {
  if (!downloadUrl) return null;
  try {
    const resp = await axios.get(downloadUrl, {
      responseType: "arraybuffer",
      timeout: 30_000,
      maxContentLength: MAX_ATTACHMENT_BYTES,
      maxBodyLength: MAX_ATTACHMENT_BYTES,
    });
    return Buffer.from(resp.data);
  } catch (e: any) {
    console.warn("⚠️ ATTACHMENT_DOWNLOAD_FAILED:", e?.message || e);
    return null;
  }
}
function pickResendTo(email: ResendReceivedEmail): string {
  const t: any = (email as any)?.to;
  if (Array.isArray(t)) return safeStr(t[0]).toLowerCase();
  if (typeof t === "string") return safeStr(t).toLowerCase();
  return "";
}

// ============================
// Resend inbound webhook (Svix VERIFIED) -> Cloud-only intake
// ============================
app.post(
  "/webhooks/resend-inbound",
  rateLimit("resend_inbound"),
  express.raw({ type: "application/json" }),
  async (req: Request, res: Response, next: any) => {
    try {
      if (!RESEND_WEBHOOK_SECRET) return res.status(500).json({ ok: false, error: "missing_resend_webhook_secret" });
      if (!RESEND_API_KEY) return res.status(500).json({ ok: false, error: "missing_resend_api_key" });

      const svixId = String(req.headers["svix-id"] || "");
      const svixTs = String(req.headers["svix-timestamp"] || "");
      const svixSig = String(req.headers["svix-signature"] || "");

      const payload = (req.body as Buffer).toString("utf8");
      const wh = new Webhook(RESEND_WEBHOOK_SECRET);
      const evt = wh.verify(payload, {
        "svix-id": svixId,
        "svix-timestamp": svixTs,
        "svix-signature": svixSig,
      }) as any;

      // ✅ DEV ONLY: allow forcing a fake attachment list for size-guard testing
      const RESEND_DEV_FAKE_ATTACHMENTS = truthyEnv("RESEND_DEV_FAKE_ATTACHMENTS");

      if (RESEND_DEV_FAKE_ATTACHMENTS) {
        const fakeEmailId = safeStr((evt as any)?.data?.id) || "dev_fake_email";
        const fakeSize = Number(process.env.RESEND_DEV_FAKE_SIZE || 5000);

        const oversized =
          fakeSize > MAX_ATTACHMENT_BYTES || fakeSize > MAX_TOTAL_ATTACH_BYTES
            ? [{ id: "dev_fake", filename: "big-test.txt", size: fakeSize, reason: "attachment_too_large" }]
            : [];

        if (oversized.length) {
          return res.status(413).json({
            ok: false,
            error: "attachments_blocked_by_size_guard",
            emailId: fakeEmailId,
            oversized,
          });
        }

        return res.json({ ok: true, emailId: fakeEmailId, devFake: true, allowed: true, size: fakeSize });
      }

      const type = safeStr(evt?.type);
      const emailId = safeStr(evt?.data?.id || evt?.data?.email_id);

      logInfo("RESEND_VERIFIED_EVENT", {
        requestId: getRequestId(req),
        type,
        createdAt: evt?.created_at,
        emailId,
        dataKeys: Object.keys(evt?.data || {}),
      });

      if (type !== "email.received" || !emailId) return res.json({ ok: true, ignored: true });

      const bucket = safeStr(process.env.CLOUDFLARE_R2_BUCKET);
      if (!bucket) return res.status(500).json({ ok: false, error: "R2_BUCKET_not_set" });

      const email = await resendRetrieveReceivedEmail(emailId);
      if (!email) return res.json({ ok: true, fetched: false });

      const toEmail = pickResendTo(email);
      if (!toEmail) return res.json({ ok: true, fetched: true, processed: false, reason: "missing_to" });

      const atts = await resendListReceivedAttachments(emailId);
      const processed: any[] = [];

      if (atts.length) {
        // Attachment guards
        const blocked: any[] = [];
        const allowed: typeof atts = [];

        let total = 0;
        for (const att of atts.slice(0, MAX_ATTACHMENTS)) {
          const original = safeStr(att?.filename) || "attachment.bin";
          const safeName = normalizeUploadName(original);

          // allowlist enforced before download
          if (!isAllowedFileByName(safeName)) {
            blocked.push({
              id: safeStr(att?.id),
              filename: safeName,
              size: Number(att?.size || 0),
              reason: "unsupported_file_type",
              allowed: Array.from(ALLOWED_EXTS),
            });
            continue;
          }

          const size = Number(att?.size || 0);

          if (!Number.isFinite(size) || size <= 0) {
            blocked.push({
              id: safeStr(att?.id),
              filename: safeName,
              size,
              reason: "missing_or_invalid_size",
            });
            continue;
          }

          if (size > MAX_ATTACHMENT_BYTES) {
            blocked.push({
              id: safeStr(att?.id),
              filename: safeName,
              size,
              reason: "attachment_too_large",
              limit: MAX_ATTACHMENT_BYTES,
            });
            continue;
          }

          if (total + size > MAX_TOTAL_ATTACH_BYTES) {
            blocked.push({
              id: safeStr(att?.id),
              filename: safeName,
              size,
              reason: "total_attachments_too_large",
              totalLimit: MAX_TOTAL_ATTACH_BYTES,
            });
            continue;
          }

          total += size;
          allowed.push(att);
        }

        if (!allowed.length) {
          const onlyUnsupported =
            blocked.length > 0 && blocked.every((x) => safeStr(x?.reason) === "unsupported_file_type");

          if (onlyUnsupported) {
            return res.status(415).json({
              ok: false,
              error: "unsupported_file_type",
              emailId,
              toEmail,
              blocked,
              allowed: Array.from(ALLOWED_EXTS),
            });
          }

          return res.status(413).json({
            ok: false,
            error: "attachments_blocked_by_size_guard",
            emailId,
            toEmail,
            blocked,
          });
        }

        // Process allowed attachments only
        for (const att of allowed) {
          const original = safeStr(att?.filename) || "attachment.bin";
          const safeName = normalizeUploadName(original);
          const url = safeStr(att?.download_url);

          if (!url) {
            processed.push({ ok: false, filename: safeName, error: "missing_download_url" });
            continue;
          }

          const buf = await downloadAttachmentToBuffer(url);
          if (!buf || !buf.length) {
            processed.push({ ok: false, filename: safeName, error: "download_failed" });
            continue;
          }

          // Enforce byte caps even after download
          if (buf.length > MAX_ATTACHMENT_BYTES || buf.length > MAX_TOTAL_ATTACH_BYTES) {
            processed.push({
              ok: false,
              filename: safeName,
              error: "payload_too_large",
              maxAttachmentBytes: MAX_ATTACHMENT_BYTES,
              maxTotalBytes: MAX_TOTAL_ATTACH_BYTES,
              size: buf.length,
            });
            continue;
          }

          const iso = new Date().toISOString();
          const r2Key = `inbound/${iso.replace(/[:.]/g, "-")}__${safeName}`;

          let r2: { bucket: string; key: string } | null = null;
          try {
            const up = await r2UploadBuffer({
              key: r2Key,
              buffer: buf,
              contentType: safeStr(att?.content_type) || "application/octet-stream",
            });
            r2 = { bucket, key: up?.key || r2Key };
            console.log("☁️ R2 UPLOAD ok (resend):", bucket, r2.key);
          } catch {
            processed.push({ ok: false, filename: safeName, error: "r2_upload_failed" });
            continue;
          }

          const extractedText = await extractTextFromBuffer(safeName, buf);
          const docType: "RESUME" | "NON_RESUME" =
            extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME";

          const result = await processInboundDoc({
            requestId: getRequestId(req),
            source: "resend",
            filename: safeName,
            buffer: buf,
            extractedText,
            docType,
            toEmail,
            r2,
            savedLocal: null,
            deletedLocal: true,
          });

          processed.push({ ok: true, filename: safeName, r2Key: r2.key, docType, result });
        }

        return res.json({ ok: true, emailId, toEmail, attachments: atts.length, processed });
      }

      // No attachments: text body. Treat as .txt (allowed)
      const bodyText = safeStr((email as any).text || "");
      if (!bodyText) return res.json({ ok: true, emailId, toEmail, attachments: 0, processed: false });

      const iso = new Date().toISOString();
      const safeName = `email_${emailId}.txt`;
      const r2Key = `inbound/${iso.replace(/[:.]/g, "-")}__${safeName}`;
      const buf = Buffer.from(bodyText, "utf8");

      const up = await r2UploadBuffer({ key: r2Key, buffer: buf, contentType: "text/plain" });
      const r2 = { bucket, key: up?.key || r2Key };

      const docType: "RESUME" | "NON_RESUME" = classifyDocTypeFromText(bodyText);

      const result = await processInboundDoc({
        requestId: getRequestId(req),
        source: "resend",
        filename: safeName,
        buffer: buf,
        extractedText: bodyText,
        docType,
        toEmail,
        r2,
        savedLocal: null,
        deletedLocal: true,
      });

      return res.json({ ok: true, emailId, toEmail, attachments: 0, processed: true, result });
    } catch (e: any) {
      console.warn("⚠️ RESEND webhook failed:", e?.message || e);
      return res.status(400).json({ ok: false, error: "invalid_webhook" });
    }
  }
);

// ============================
// Stripe webhook (raw body) -> flips billing status to active/past_due/etc
// ============================
app.post("/webhooks/stripe", express.raw({ type: "application/json" }), async (req: Request, res: Response) => {
  try {
    if (!stripe) return res.status(500).json({ ok: false, error: "stripe_not_configured" });
    if (!STRIPE_WEBHOOK_SECRET) return res.status(500).json({ ok: false, error: "missing_stripe_webhook_secret" });

    const sig = safeStr(req.headers["stripe-signature"]);
    if (!sig) return res.status(400).json({ ok: false, error: "missing_signature" });

    const event = stripe.webhooks.constructEvent(req.body as Buffer, sig, STRIPE_WEBHOOK_SECRET);
    console.log("💳 STRIPE EVENT:", event.type, { requestId: getRequestId(req) });

    const setStripeFieldsIfPossible = async (customerId: string, stripeCustomerId?: string, subId?: string) => {
      if (stripeCustomerId) await updateCustomerCellInMasterSheet(customerId, "stripeCustomerId", stripeCustomerId);
      if (subId) await updateCustomerCellInMasterSheet(customerId, "stripeSubscriptionId", subId);
      invalidateCustomersCache();
    };

    const getCustomerIdFromMeta = (obj: any) => safeStr(obj?.metadata?.customerId);

    if (event.type === "checkout.session.completed") {
      const session = event.data.object as Stripe.Checkout.Session;
      const customerId = getCustomerIdFromMeta(session);
      const stripeCustomerId = safeStr(session.customer);
      const subId = safeStr(session.subscription);

      if (customerId) {
        await setStripeFieldsIfPossible(customerId, stripeCustomerId || undefined, subId || undefined);
        await updateCustomerStatusInMasterSheet(customerId, "active");
        console.log("✅ BILLING_ACTIVE:", customerId);
      }
      return res.json({ ok: true });
    }

    if (
      event.type === "customer.subscription.created" ||
      event.type === "customer.subscription.updated" ||
      event.type === "customer.subscription.deleted"
    ) {
      const sub = event.data.object as Stripe.Subscription;
      const stripeCustomerId = safeStr(sub.customer);
      const subId = safeStr(sub.id);
      const status = safeStr(sub.status);

      let mapped = "billing_unknown";
      if (status === "active" || status === "trialing") mapped = "active";
      else if (status === "past_due") mapped = "past_due";
      else if (status === "canceled") mapped = "canceled";
      else if (status === "unpaid") mapped = "unpaid";

      const metaCustomerId = getCustomerIdFromMeta(sub);
      let customerId = metaCustomerId;

      if (!customerId && stripeCustomerId) {
        const customers = await getCustomersCached();
        const found = customers.find((c) => safeStr(c.stripeCustomerId) === stripeCustomerId);
        customerId = safeStr(found?.customerId);
      }

      if (customerId) {
        await setStripeFieldsIfPossible(customerId, stripeCustomerId || undefined, subId || undefined);
        await updateCustomerStatusInMasterSheet(customerId, mapped);
        console.log("✅ BILLING_STATUS:", customerId, mapped);
      }

      return res.json({ ok: true });
    }

    if (event.type === "invoice.paid") {
      const invoice = event.data.object as Stripe.Invoice;
      const stripeCustomerId = safeStr(invoice.customer);

      if (stripeCustomerId) {
        const customers = await getCustomersCached();
        const found = customers.find((c) => safeStr(c.stripeCustomerId) === stripeCustomerId);
        const customerId = safeStr(found?.customerId);
        if (customerId) await updateCustomerStatusInMasterSheet(customerId, "active");
      }
      return res.json({ ok: true });
    }

    if (event.type === "invoice.payment_failed") {
      const invoice = event.data.object as Stripe.Invoice;
      const stripeCustomerId = safeStr(invoice.customer);

      if (stripeCustomerId) {
        const customers = await getCustomersCached();
        const found = customers.find((c) => safeStr(c.stripeCustomerId) === stripeCustomerId);
        const customerId = safeStr(found?.customerId);
        if (customerId) await updateCustomerStatusInMasterSheet(customerId, "past_due");
      }
      return res.json({ ok: true });
    }

   return res.json({ ok: true });
} catch (e: any) {
  // Always log full error server-side
  console.error("❌ STRIPE_WEBHOOK_FAILED:", e);

  // Never leak internals to client
  return res.status(400).json({
    ok: false,
    error: "stripe_webhook_failed"
  });
}
});

// ✅ global JSON parser AFTER raw-body webhooks (Stripe/Resend)
app.use(express.json({ limit: "10mb" }));

// ✅ JSON parse error handler (prevents HTML error pages)
app.use((err: any, _req: Request, res: Response, next: any) => {
  const isBadJson =
    err?.type === "entity.parse.failed" ||
    (err instanceof SyntaxError && "body" in (err as any)) ||
    err?.status === 400;

  if (isBadJson) {
    return res.status(400).json({
      ok: false,
      error: "invalid_json",
      message: "Malformed JSON body",
    });
  }

  return next(err);
});

// ===== ROUTES =====
app.use("/upload", uploadRoutes);
app.use(intakeRoutes);

// ===== HEALTH =====
app.get("/", (_req: Request, res: Response) => res.send("✅ Resume Sorter Backend Running"));
// ============================
// Signed Resume Viewing (secure permanent links)
// GET /r/:requestId -> validates customer active -> 302 to fresh signed R2 URL
// ============================
app.get("/r/:requestId", async (req: Request, res: Response, next: any) => {
  try {
    const requestId = safeStr(req.params?.requestId);
    if (!requestId) {
      throw new AppError("Missing requestId", 400, "missing_requestId", true);
    }

    // 1) Lookup inbound doc by requestId
    const r = await pool.query(
      `
      SELECT request_id, customer_id, resolved_customer_id, r2_key, doc_type
      FROM inbound_docs
      WHERE request_id = $1
      ORDER BY created_at DESC
      LIMIT 1
      `,
      [requestId]
    );

    const row = r.rows?.[0];
    const r2Key = safeStr(row?.r2_key);
    const custId = safeStr(row?.resolved_customer_id || row?.customer_id);
    const docType = safeStr(row?.doc_type);

    if (!row || !r2Key || !custId) {
      throw new AppError("Not found", 404, "not_found", true);
    }

    // Optional safety: only allow resumes
    if (docType && docType !== "RESUME") {
      throw new AppError("Not found", 404, "not_found", true);
    }

    // 2) Validate customer active/allowed
    const customers = await getCustomersCached();
    const c = customers.find((x) => safeStr(x.customerId) === custId);

    if (!c) {
      throw new AppError("Not found", 404, "not_found", true);
    }

    const gate = isProcessingAllowed(c.status);
    if (!gate.allowed) {
      throw new AppError("Not allowed", 403, "not_allowed", true);
    }

        // 3) Generate a fresh signed URL (preferred)
    //    (robust: supports different signer export names + return shapes)
    let url = "";

    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const r2Svc: any = require("./services/r2");

      const signer =
        r2Svc?.r2GetSignedUrl ||
        r2Svc?.r2SignedUrlForKey ||
        r2Svc?.r2CreateSignedUrl ||
        r2Svc?.getSignedR2Url ||
        r2Svc?.getSignedUrl ||
        null;

      if (typeof signer === "function") {
        // Try object-arg style first, then positional
        let out: any = null;

        try {
          out = await signer({ key: r2Key, expiresInSeconds: 600 });
        } catch {
          // ignore, try positional
        }

        if (!out) {
          try {
            out = await signer(r2Key, 600);
          } catch {
            // ignore
          }
        }

        // Normalize return: string OR { url } OR { signedUrl }
        if (typeof out === "string") url = out;
        else if (out && typeof out === "object") {
          url = safeStr(out.url || out.signedUrl || out.signed_url || "");
        }
      }
    } catch {
      url = "";
    }
    // Fallback: only if you explicitly enabled public links
    if (!url) {
      const publicUrl = buildR2PublicUrl(r2Key);
      if (publicUrl) url = publicUrl;
    }

    if (!url) {
      // This means you haven't wired a signer yet and public links are off
      throw new AppError("Signed URL not configured", 500, "misconfigured_server", false);
    }

    // 4) Redirect to the fresh URL
   // Prevent caching of signed URLs
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");

    return res.redirect(302, url);
  } catch (e: any) {
    return next(e);
  }
});
// ============================
// Jobs API (multi-role hiring)
// ============================

// create job
app.post("/customers/jobs/create", async (req: Request, res: Response, next: any) => {
  try {
    const customerId = safeStr(req.body?.customerId);
    const title = safeStr(req.body?.title);
    let rubric: any = req.body?.rubric;

    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });
    if (!title) return res.status(400).json({ ok: false, error: "missing_title" });

    if (typeof rubric === "string") {
      const parsed = tryJsonParse(rubric);
      rubric = parsed ?? { text: rubric };
    }

    if (!rubric || typeof rubric !== "object") {
      return res.status(400).json({ ok: false, error: "missing_rubric" });
    }

    const job = await createCustomerJob(customerId, title, rubric);
    return res.json({ ok: true, job });
  } catch (e: any) {
    return next(e);
  }
});

// list jobs
app.get("/customers/jobs/list", async (req: Request, res: Response, next: any) => {
  try {
    const customerId = safeStr(req.query.customerId);
    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });

    const jobs = await listCustomerJobs(customerId);
    return res.json({ ok: true, jobs });
  } catch (e: any) {
    return next(e);
  }
});

// delete job
app.post("/customers/jobs/delete", async (req: Request, res: Response, next: any) => {
  try {
    const customerId = safeStr(req.body?.customerId);
    const jobId = safeStr(req.body?.jobId);

    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });
    if (!jobId) return res.status(400).json({ ok: false, error: "missing_jobId" });

    await deleteCustomerJob(customerId, jobId);
    return res.json({ ok: true });
  } catch (e: any) {
    return next(e);
  }
});
// ============================
// Rubrics API
// ============================
app.post("/customers/rubric", async (req: Request, res: Response, next: any) => {
  try {
    const customerId = safeStr(req.body?.customerId);
    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });

    let rubric: any = req.body?.rubric;

    if (typeof rubric === "string") {
      const parsed = tryJsonParse(rubric);
      rubric = parsed ?? { text: rubric };
    }

    if (!rubric || (typeof rubric !== "object" && typeof rubric !== "string")) {
      return res.status(400).json({ ok: false, error: "missing_rubric" });
    }

    await upsertCustomerRubric(customerId, rubric);
    return res.json({ ok: true, customerId });
  } catch (e: any) {
    return next(e);
  }
});

app.get("/customers/rubric", async (req: Request, res: Response, next: any) => {
  try {
    const customerId = safeStr(req.query.customerId);
    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });

    const rubric = await getCustomerRubric(customerId);
    return res.json({ ok: true, customerId, rubric });
  } catch (e: any) {
    return next(e);
  }
});

// ============================
// Customers: setup
// ============================
app.post("/customers/setup", async (req: Request, res: Response, next: any) => {
  try {
    // ✅ If coming back from Stripe success page, we pass signupId
    const signupId = safeStr(req.body?.signupId);

    let companyName = "";
    let adminEmail = "";
    let payload: any = null;

        if (signupId) {
      // ✅ Idempotency FIRST: if already completed, return the stored result immediately
      await pool.query(`
        CREATE TABLE IF NOT EXISTS completed_signups (
          signup_id TEXT PRIMARY KEY,
          intake_email TEXT,
          sheet_url TEXT,
          manage_url TEXT,
          customer_id TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
      `);

      const done = await pool.query(
        `SELECT intake_email, sheet_url, manage_url, customer_id FROM completed_signups WHERE signup_id = $1 LIMIT 1`,
        [signupId]
      );

      const d = done?.rows?.[0];
      if (d?.intake_email && d?.sheet_url && d?.manage_url) {
        return res.json({
          ok: true,
          intakeEmail: safeStr(d.intake_email),
          sheetUrl: safeStr(d.sheet_url),
          manageUrl: safeStr(d.manage_url),
          customerId: safeStr(d.customer_id),
          // nice to have for debugging
          signupId,
          idempotent: true,
        });
      }

      // Load stored signup payload (jobs/criteria too large for Stripe metadata)
      const r = await pool.query(`SELECT payload_json FROM pending_signups WHERE id = $1`, [signupId]);
      const payloadJson = safeStr(r?.rows?.[0]?.payload_json);

      if (!payloadJson) return res.status(400).json({ ok: false, error: "invalid_signupId" });

      try {
        payload = JSON.parse(payloadJson);
      } catch {
        return res.status(500).json({ ok: false, error: "pending_signup_payload_corrupt" });
      }

      companyName = safeStr(payload?.companyName);
      adminEmail = safeStr(payload?.adminEmail);
    } else {
      // Back-compat: allow direct body call (old flow)
      companyName = safeStr(req.body?.companyName);
      adminEmail = safeStr(req.body?.adminEmail) || safeStr(ADMIN_EMAIL);
      payload = req.body || null;
    }

    if (!companyName) return res.status(400).json({ ok: false, error: "missing_companyName" });
       if (!adminEmail) return res.status(400).json({ ok: false, error: "missing_adminEmail" });


    if (!MASTER_SHEET_ID) return res.status(500).json({ ok: false, error: "MASTER_CUSTOMERS_SHEET_ID not set" });
    await oauth2Client.getAccessToken();
    await ensureMasterHeaders();

    const criteria = payload?.criteria ?? null;
    const slug = makeSlug(companyName);
    const customerId = makeCustomerId(slug);
    // ✅ Persist rubric from signup payload (so scoring uses it automatically)
if (criteria && typeof criteria === "object") {
  await upsertCustomerRubric(customerId, criteria);
}
    const intakeEmail = `${slug}@goeasypaper.com`;

    const nowLocal = toZonedTime(new Date(), TIMEZONE);
    const trialStartAt = formatISO(nowLocal, { representation: "date" });
    const trialEndAt = formatISO(addDaysDfns(nowLocal, 30), { representation: "date" });

    // Pull jobs/criteria from payload (Stripe signup flow)
    const jobsFromPayload: string[] = Array.isArray(payload?.jobs)
      ? payload.jobs.map((x: any) => safeStr(x)).filter(Boolean)
      : [];

    const criteriaFromPayload: any = payload?.criteria ?? payload?.rubric ?? null;

    const tally = await createTallySheetForCustomer(companyName, customerId);
    const tallySheetId = safeStr(tally?.spreadsheetId);
    const tallySheetUrl = safeStr(tally?.spreadsheetUrl);

    if (!tallySheetId) {
      throw new Error("Missing tallySheetId from createTallySheetForCustomer");
    }

    // 🔒 Enforce single-sheet job layout (no extra tabs, jobs down left)
    await ensureResumesTab(tallySheetId);

    // Always baseline sections
    await ensureJobSectionExists(tallySheetId, "Unsorted");
    await ensureJobSectionExists(tallySheetId, "General Submissions");

    // Ensure customer job sections (if provided)
    for (const j of jobsFromPayload) {
      try {
        await ensureJobSectionExists(tallySheetId, j);
      } catch (e: any) {
        console.log("⚠️ JOB_SECTION_CREATE_FAILED (continuing):", j, e?.message || e);
      }
    }

        // ✅ Store jobs + criteria (real DB persistence)
    try {
      if (criteriaFromPayload) {
        await upsertCustomerRubric(customerId, criteriaFromPayload);
      }
    } catch (e: any) {
      console.log("⚠️ UPSERT_CUSTOMER_RUBRIC_FAILED (continuing):", e?.message || e);
    }
    const createdAtIso = new Date().toISOString();
    const row = [
      customerId,
      companyName,
      slug,  
      intakeEmail,
      adminEmail, // ✅ was ADMIN_EMAIL; now use customer email
      "trial",
      trialStartAt,
      trialEndAt,
      tallySheetId,
      tallySheetUrl,
      trialStartAt,
      trialEndAt,
      createdAtIso,
      "",
      "",
    ];

    const sheets = google.sheets({ version: "v4", auth: oauth2Client });
    await sheets.spreadsheets.values.append({
      spreadsheetId: MASTER_SHEET_ID,
      range: `${MASTER_SHEET_TAB}!A:ZZ`,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: [row] },
    });

    invalidateCustomersCache();

    const sheetUrl = tallySheetUrl || `https://docs.google.com/spreadsheets/d/${tallySheetId}`;
    const manageUrl = `${APP_URL}/manage`;

    // ✅ Send activation email (one email) to the signup/admin email
    let activationEmailSent = false;
    try {
      await (emailSvc as any).sendCustomerTextEmail({
        to: safeStr(adminEmail),
        subject: `Your trial is live — ${companyName}`,
        text:
          `You're live.\n\n` +
          `Intake email:\n${intakeEmail}\n\n` +
          `Your sheet:\n${sheetUrl}\n\n` +
          `Manage jobs:\n${manageUrl}\n\n` +
          `Send job ads + resumes to that intake email. We'll score + sort them into your sheet automatically.\n`,
      });
      activationEmailSent = true;
    } catch (e: any) {
      console.log("📧 ACTIVATION_EMAIL_FAILED (continuing):", e?.message || e);
    }

        // ✅ Mark signup complete (idempotency) + then delete pending row
    if (signupId) {
      try {
        // Save the “final answer” so success-page refresh never duplicates work
        await pool.query(
          `
          INSERT INTO completed_signups (signup_id, intake_email, sheet_url, manage_url, customer_id)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (signup_id) DO UPDATE SET
            intake_email = EXCLUDED.intake_email,
            sheet_url = EXCLUDED.sheet_url,
            manage_url = EXCLUDED.manage_url,
            customer_id = EXCLUDED.customer_id
          `,
          [signupId, safeStr(intakeEmail), safeStr(sheetUrl), safeStr(manageUrl), safeStr(customerId)]
        );
      } catch (e: any) {
        console.log("⚠️ COMPLETED_SIGNUP_WRITE_FAILED (continuing):", e?.message || e);
      }

      try {
        await pool.query(`DELETE FROM pending_signups WHERE id = $1`, [signupId]);
      } catch (e: any) {
        console.log("⚠️ PENDING_SIGNUP_DELETE_FAILED (continuing):", e?.message || e);
      }
    }

    return res.json({
      ok: true,
      intakeEmail,
      sheetUrl,
      manageUrl,

      // keep your existing extras (handy for debugging)
      companyName,
      slug,
      customerId,
      adminEmail,
      status: "trial",
      trialStartAt,
      trialEndAt,
      tallySheetId,
      tallySheetUrl,
      activationEmailSent,
    });
  } catch (e: any) {
    return next(e);
  }
});
// ============================
// Billing endpoints
// ============================
app.post("/billing/create-checkout-session", async (req: Request, res: Response, next: any) => {
  try {
    if (!stripe) return res.status(500).json({ ok: false, error: "stripe_not_configured" });
    if (!STRIPE_PRICE_ID) return res.status(500).json({ ok: false, error: "missing_STRIPE_PRICE_ID" });

    const customerId = safeStr(req.body?.customerId);
    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });

    const customers = await getCustomersCached();
    const c = customers.find((x) => x.customerId === customerId);
    if (!c) return res.status(404).json({ ok: false, error: "customer_not_found" });

    await ensureMasterHeaders();

    let stripeCustomerId = safeStr(c.stripeCustomerId || "");

    if (!stripeCustomerId) {
      const created = await stripe.customers.create({
        email: safeStr(c.adminEmail) || undefined,
        name: safeStr(c.companyName) || undefined,
        metadata: { customerId },
      });
      stripeCustomerId = created.id;
      await updateCustomerCellInMasterSheet(customerId, "stripeCustomerId", stripeCustomerId);
      invalidateCustomersCache();
    }

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      customer: stripeCustomerId,
      line_items: [{ price: STRIPE_PRICE_ID, quantity: 1 }],
      success_url: `${BILLING_SUCCESS_URL}?customerId=${encodeURIComponent(customerId)}`,
      cancel_url: `${BILLING_CANCEL_URL}?customerId=${encodeURIComponent(customerId)}`,
      allow_promotion_codes: true,
      metadata: { customerId },
      subscription_data: { metadata: { customerId } },
    });

    return res.json({ ok: true, url: session.url, sessionId: session.id });
  } catch (e: any) {
    return next(e);
  }
});

app.post("/billing/create-portal-session", async (req: Request, res: Response, next: any) => {
  try {
    if (!stripe) return res.status(500).json({ ok: false, error: "stripe_not_configured" });

    const customerId = safeStr(req.body?.customerId);
    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });

    const customers = await getCustomersCached();
    const c = customers.find((x) => x.customerId === customerId);
    if (!c) return res.status(404).json({ ok: false, error: "customer_not_found" });

    const stripeCustomerId = safeStr(c.stripeCustomerId || "");
    if (!stripeCustomerId) return res.status(400).json({ ok: false, error: "missing_stripeCustomerId" });

    const portal = await stripe.billingPortal.sessions.create({
      customer: stripeCustomerId,
      return_url: APP_URL,
    });

    return res.json({ ok: true, url: portal.url });
  } catch (e: any) {
    return next(e);
  }
});
// ============================
// Auth
// ============================
app.get("/auth", (_req: Request, res: Response) => {
  const url = oauth2Client.generateAuthUrl({ access_type: "offline", prompt: "consent", scope: SCOPES });
  return res.redirect(url);
});
app.get("/auth/google", (_req: Request, res: Response) => {
  const url = oauth2Client.generateAuthUrl({ access_type: "offline", prompt: "consent", scope: SCOPES });
  return res.redirect(url);
});
app.get("/auth/callback", async (req: Request, res: Response) => {
  try {
    const code = String(req.query.code || "");
    if (!code) return res.status(400).send("Missing ?code=");

    const { tokens } = await oauth2Client.getToken(code);

    console.log("🔐 NEW TOKENS META:", {
      token_type: tokens.token_type,
      scope: tokens.scope,
      has_access_token: !!tokens.access_token,
      has_refresh_token: !!tokens.refresh_token,
      has_id_token: !!tokens.id_token,
      access_head: tokens.access_token ? tokens.access_token.slice(0, 12) : null,
    });

    oauth2Client.setCredentials(tokens);
    saveTokensToDisk(tokens);
    await logAuthedGoogleEmailFromTokens();

    return res.send("✅ Google re-auth complete. You can close this tab.");
  } catch (err: any) {
    console.error("❌ /auth/callback error:", err?.message || err);
    return res.status(500).send(`Auth callback failed: ${err?.message || err}`);
  }
});
app.get("/auth/google/callback", async (req: Request, res: Response) => {
  try {
    const code = String(req.query.code || "");
    if (!code) return res.status(400).send("Missing ?code=");

    const { tokens } = await oauth2Client.getToken(code);
    console.log("GOOGLE_TOKENS_JSON_FRESH:", JSON.stringify(tokens));
    console.log("🔐 NEW TOKENS META:", {
      token_type: tokens.token_type,
      scope: tokens.scope,
      has_access_token: !!tokens.access_token,
      has_refresh_token: !!tokens.refresh_token,
      has_id_token: !!tokens.id_token,
      access_head: tokens.access_token ? tokens.access_token.slice(0, 12) : null,
    });

    oauth2Client.setCredentials(tokens);
    saveTokensToDisk(tokens);
    await logAuthedGoogleEmailFromTokens();

    return res.send("✅ Google re-auth complete. You can close this tab.");
  } catch (err: any) {
    console.error("❌ /auth/google/callback error:", err?.message || err);
    return res.status(500).send(`Auth callback failed: ${err?.message || err}`);
  }
});

// ============================
// Inbound uploads (dev/backdoor)
// ============================
const inboundUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: Number(process.env.MAX_UPLOAD_BYTES || 15 * 1024 * 1024) },
});

// ✅ Wrap multer so errors become JSON (no HTML)
function inboundFileMulter(req: Request, res: Response, next: any) {
  inboundUpload.single("file")(req as any, res as any, (err: any) => {
    if (!err) return next();

    const maxBytes = Number(process.env.MAX_UPLOAD_BYTES || 15 * 1024 * 1024);

    if (err?.code === "LIMIT_FILE_SIZE") {
      return res.status(413).json({
        ok: false,
        error: "payload_too_large",
        message: "File too large",
        maxBytes,
      });
    }

    return res.status(400).json({
      ok: false,
      error: "multipart_failed",
      message: String(err?.message || err),
    });
  });
}

app.post("/webhooks/inbound-file", rateLimit("inbound_file"), inboundFileMulter, async (req: Request, res: Response, next: any) => {
  try {
    if (!INBOUND_FILE_ENABLED) {
      return res.status(404).json({ ok: false, error: "not_found" });
    }

    const f: any = (req as any).file;
    if (!f) return res.status(400).json({ ok: false, error: "no_file" });

    const toEmail = safeStr((req.body as any)?.toEmail || (req.body as any)?.to).trim().toLowerCase();
if (!toEmail) return res.status(400).json({ ok: false, error: "missing_toEmail" });

const customerId = safeStr((req.query as any)?.customerId || (req.body as any)?.customerId).trim();
    const bucket = safeStr(process.env.CLOUDFLARE_R2_BUCKET);
    if (!bucket) return res.status(500).json({ ok: false, error: "R2_BUCKET_not_set" });

    const original = safeStr(f.originalname || "upload.bin");
    const safeName = normalizeUploadName(original);

    // allowlist enforced here
    if (!isAllowedFileByName(safeName)) {
      return res.status(415).json(unsupportedFileTypePayload(safeName));
    }

    // enforce attachment byte cap for inbound-file too
    const sizeBytes = Number(f.size || (f.buffer ? f.buffer.length : 0));
    if (sizeBytes > MAX_ATTACHMENT_BYTES) {
      return res.status(413).json({
        ok: false,
        error: "payload_too_large",
        message: "File too large",
        size: sizeBytes,
        maxBytes: MAX_ATTACHMENT_BYTES,
      });
    }

    const iso = new Date().toISOString();
    const r2Key = `inbound/${iso.replace(/[:.]/g, "-")}__${safeName}`;

    const shouldSaveLocal = truthyEnv("INBOUND_SAVE_LOCAL");

    let savedLocal: string | null = null;
    let deletedLocal = false;
    let r2: { bucket: string; key: string } | null = null;

    if (shouldSaveLocal) {
      const dir = path.join(process.cwd(), "inbound", "uploads");
      fs.mkdirSync(dir, { recursive: true });
      const finalPath = path.join(dir, `${iso.replace(/[:.]/g, "-")}__${safeName}`);
      await fs.promises.writeFile(finalPath, f.buffer);
      savedLocal = finalPath;
      deletedLocal = false;
      console.log("📎 FILE INBOUND saved:", finalPath, { requestId: getRequestId(req) });
    }

    try {
      const up = await r2UploadBuffer({
        key: r2Key,
        buffer: f.buffer,
        contentType: safeStr(f.mimetype) || "application/octet-stream",
      });

      r2 = { bucket, key: up?.key || r2Key };
      console.log("☁️ R2 UPLOAD ok:", bucket, r2.key, { requestId: getRequestId(req) });
    } catch (e: any) {
      console.error("❌ R2_UPLOAD_FAILED:", e?.message || e);
      return res.status(500).json({ ok: false, error: "r2_upload_failed", message: String(e?.message || e) });
    }

    const extractedText = await extractTextFromBuffer(safeName, f.buffer);
    const docType: "RESUME" | "NON_RESUME" =
      extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME";

    if (r2?.key && savedLocal) {
      try {
        await fs.promises.unlink(savedLocal);
        console.log("🧹 LOCAL_DELETE_OK:", savedLocal, { requestId: getRequestId(req) });
        deletedLocal = true;
        savedLocal = null;
      } catch (e: any) {
        console.warn("⚠️ LOCAL_DELETE_FAILED:", savedLocal, e?.message || e);
      }
    } else {
      deletedLocal = true;
    }

    const result = await processInboundDoc({
  requestId: getRequestId(req),
  source: "inbound-file",
  filename: safeName,
  buffer: f.buffer,
  extractedText,
  docType,
  toEmail,
  customerId,
  r2,
  savedLocal,
  deletedLocal,
});
    return res.json(result);
  } catch (e: any) {
    return next(e);
  }
});

// ✅ Multer error handler for inbound-file (prevents HTML error pages)
app.use("/webhooks/inbound-file", (err: any, _req: Request, res: Response, next: any) => {
  if (!err) return next();

  if (err?.code === "LIMIT_FILE_SIZE") {
    return res.status(413).json({
      ok: false,
      error: "payload_too_large",
      message: "File too large",
      maxBytes: Number(process.env.MAX_UPLOAD_BYTES || 15 * 1024 * 1024),
    });
  }

  if (err?.name === "MulterError") {
    return res.status(400).json({ ok: false, error: "multer_error", code: err.code, message: String(err.message || err) });
  }

  return next(err);
});

// ============================
// inbound-r2 (cloud intake): fail-closed secret enforcement (prod)
// ============================
app.post(
  "/webhooks/inbound-r2",
  rateLimit("inbound_r2"),
  async (req: Request, res: Response, next: any) => {
    try {
      // Accept new header name, keep old for back-compat
      
      const got = safeStr(
        req.header("X-Inbound-R2-Secret") || req.header("X-Inbound-Secret")
      );

            // Fail-closed enforcement
      if (INBOUND_SECRET_REQUIRED) {
        const ok =
          got &&
          got.length === INBOUND_WEBHOOK_SECRET.length &&
          crypto.timingSafeEqual(
            Buffer.from(got),
            Buffer.from(INBOUND_WEBHOOK_SECRET)
          );

        if (!ok) {
          throw new AppError("Unauthorized", 401, "unauthorized", true);
        }
      } else {
        // Back-compat: if secret configured, enforce even in dev
        if (INBOUND_WEBHOOK_SECRET) {
          const ok =
            got &&
            got.length === INBOUND_WEBHOOK_SECRET.length &&
            crypto.timingSafeEqual(
              Buffer.from(got),
              Buffer.from(INBOUND_WEBHOOK_SECRET)
            );

          if (!ok) {
            throw new AppError("Unauthorized", 401, "unauthorized", true);
          }
        }
      }

      // ✅ log only after secret validation (dev only)
      devLog(
        "INBOUND_R2_ENTER",
        getRequestId(req),
        "ct=",
        safeStr(req.header("content-type"))
      );

      // Body normalization (accept parsed JSON or string JSON)
      const bodyRaw = req.body;

let body: any = bodyRaw;

if (Buffer.isBuffer(bodyRaw)) {
  body = tryJsonParse(bodyRaw.toString("utf8"));
} else if (typeof bodyRaw === "string") {
  body = tryJsonParse(bodyRaw);
}

// If JSON middleware parsed successfully, bodyRaw will already be object.
// If parsing failed, the global JSON error handler would have fired.
// If parsing produced null (invalid JSON), treat as malformed.
if (body == null || typeof body !== "object") {
  throw new AppError("Malformed JSON body", 400, "invalid_json", true);
}
// If parser/middleware results in null/undefined, treat as invalid JSON.
// If it's an empty object, let missing_key/toEmail handle it.

      

      const key = safeStr(body?.key);
      const toEmail = safeStr(body?.toEmail || body?.to)
        .trim()
        .toLowerCase();

      if (!key) throw new AppError("Missing key", 400, "missing_key", true);
      if (!toEmail)
        throw new AppError("Missing toEmail", 400, "missing_toEmail", true);

      const bucket = safeStr(process.env.CLOUDFLARE_R2_BUCKET);
      if (!bucket)
        throw new AppError(
          "R2 bucket not configured",
          500,
          "misconfigured_server",
          false
        );

      const filename = safeBaseName(key).replace(
        /^\d{4}-\d{2}-\d{2}T.*?Z__/,
        ""
      );

      // allowlist enforced here
      if (!isAllowedFileByName(filename)) {
        return res.status(415).json(unsupportedFileTypePayload(filename));
      }

      const buffer = await r2DownloadToBuffer({ key });
      if (!buffer)
        throw new AppError("Object not found", 404, "object_not_found", true);

      if (buffer.length > MAX_ATTACHMENT_BYTES) {
        throw new AppError(
          "Payload too large",
          413,
          "payload_too_large",
          true
        );
      }

      const extractedText = await extractTextFromBuffer(filename, buffer);

      const docType: "RESUME" | "NON_RESUME" =
        extractedText.trim().length > 0
          ? classifyDocTypeFromText(extractedText)
          : "NON_RESUME";

      const result = await processInboundDoc({
        requestId: getRequestId(req),
        source: "inbound-r2",
        filename,
        buffer,
        extractedText,
        docType,
        toEmail,
        r2: { bucket, key },
        savedLocal: null,
        deletedLocal: true,
      });

      return res.json(result);
    } catch (e: any) {
      return next(e);
    }
  }
);
// ============================
// Nightly job (trial enforcement + customer daily reports)
// ============================
async function runNightlyJob(): Promise<{ ok: true; date: string; report: string }> {
  const nowUtc = new Date();
  const nowLocal = toZonedTime(nowUtc, TIMEZONE);
  const todayLocalISO = formatISO(nowLocal, { representation: "date" });

  const cutoffLocal = nowLocal;
  const soonLocal = addDaysDfns(nowLocal, 3);

  const cutoffUtc = fromZonedTime(cutoffLocal, TIMEZONE);
  const soonUtc = fromZonedTime(soonLocal, TIMEZONE);

  const customers = await readCustomersFromMasterSheet();
  const lines: string[] = [];
  lines.push(`Nightly run: ${todayLocalISO} (${TIMEZONE})`);
  lines.push(`Customers checked: ${customers.length}`);
  lines.push("");

  // Trial enforcement
  for (const c of customers) {
    const trialEnds = parseISODate(c.trialEndsAtISO);
    const status = (c.status || "").toLowerCase();
    if (!trialEnds) continue;

    const isTrial = status === "trial" || status === "trialing";
const isEnded = isTrial && trialEnds <= cutoffUtc;
const isEndingSoon = isTrial && trialEnds > cutoffUtc && trialEnds <= soonUtc;

if (isEnded) {
  await updateCustomerStatusInMasterSheet(c.customerId, "trial_ended");
  lines.push(`TRIAL ENDED: ${c.companyName} (${c.customerId}) -> trial_ended`);
 } else if (isEndingSoon) {
    lines.push(`TRIAL ENDING SOON: ${c.companyName} ends ${c.trialEndsAtISO}`);
  }
} // ✅ CLOSES the first: for (const c of customers) { ... }  <-- ADD THIS

     // Customer daily reports
  for (const c of customers) {
    try {
      const to = safeStr(c.adminEmail);

      const sheetId = safeStr(c.tallySheetId);
      if (!to || !sheetId) {
  devLog("🌙 NIGHTLY_SKIP_MISSING_TO_OR_SHEET:", {
    customerId: c.customerId,
    to: !!to,
    sheetId: !!sheetId,
  });
  continue;
}
      if (!to || !sheetId) continue;

      const gate = isProcessingAllowed(c.status);
      if (!gate.allowed) {
  devLog("🌙 NIGHTLY_SKIP_GATE:", {
    customerId: c.customerId,
    status: c.status,
    reason: gate.reason,
  });
  continue;
}
      if (!gate.allowed) continue;

      const sheetUrl = `https://docs.google.com/spreadsheets/d/${sheetId}`;
      const manageJobsUrl = `${BASE_URL}/manage`;

      const subject = "Your updated resume sheet is ready";

      const html = `
        <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;background:#fff;color:#111;padding:24px;">
          <div style="max-width:640px;margin:0 auto;">
            <h2 style="margin:0 0 10px 0;font-size:22px;line-height:1.2;">
              Your updated resume sheet is ready
            </h2>

            <p style="margin:0 0 18px 0;color:#333;font-size:14px;line-height:1.45;">
              Click a button:
            </p>

            <div style="display:flex;gap:12px;flex-wrap:wrap;margin:18px 0 8px 0;">
              <a href="${sheetUrl}"
                 style="display:inline-block;padding:14px 18px;border-radius:10px;text-decoration:none;font-weight:700;font-size:14px;background:#B11226;color:#fff;">
                Daily Resumes
              </a>

              <a href="${manageJobsUrl}"
                 style="display:inline-block;padding:14px 18px;border-radius:10px;text-decoration:none;font-weight:700;font-size:14px;background:#111;color:#fff;">
                Manage Jobs
              </a>
            </div>

            <p style="margin:16px 0 0 0;color:#666;font-size:12px;line-height:1.45;">
              If a button doesn’t work, copy/paste links:
              <br/>Daily Resumes: ${sheetUrl}
              <br/>Manage Jobs: ${manageJobsUrl}
            </p>

            <div style="margin-top:18px;color:#888;font-size:12px;">
              – Digital Dominance
            </div>
          </div>
        </div>
      `.trim();

      // ✅ Use the helper you already use for customer emails
      await sendCustomerText(to, subject, html);
      devLog("📨 NIGHTLY_SHEET_LINK_SENT:", c.customerId, "->", to);
    } catch (e: any) {
      devLog("⚠️ NIGHTLY_CUSTOMER_FAILED:", c.customerId, e?.message || e);
      lines.push(
        `DAILY EMAIL ERROR: ${c.companyName || c.customerId} -> ${safeStr(e?.message)}`
      );
    }
  }
  // Optionally, send an admin report
  const report = lines.join("\n");
  await sendAdmin(`Resume Sorter Nightly (${todayLocalISO})`, report);

  return { ok: true, date: todayLocalISO, report };
} // ✅ CLOSE runNightlyJob()

// --- Admin auth (fail-closed in prod) + no rate limit ---
app.use("/admin", (req: Request, res: Response, next) => {
  // Mark request so rate limiter can skip if it exists later
  (req as any)._skipRateLimit = true;

  const expected = safeStr(process.env.ADMIN_JOB_SECRET);
  const got = safeStr(req.header("X-Admin-Secret"));

  if (IS_PROD) {
    if (!expected) {
      return res.status(500).json({ ok: false, error: "missing_ADMIN_JOB_SECRET" });
    }

    const ok =
      got &&
      got.length === expected.length &&
      crypto.timingSafeEqual(Buffer.from(got), Buffer.from(expected));

    if (!ok) {
      return res.status(401).json({ ok: false, error: "unauthorized_admin" });
    }
  } else {
    // In dev: if secret is configured, enforce it; if not, allow
    if (expected) {
      const ok =
        got &&
        got.length === expected.length &&
        crypto.timingSafeEqual(Buffer.from(got), Buffer.from(expected));

      if (!ok) {
        return res.status(401).json({ ok: false, error: "unauthorized_admin" });
      }
    }
  }

  next();
});

// Manual trigger (admin)
app.get("/admin/nightly-run", async (_req: Request, res: Response, next: any) => {
  try {
    const result = await runNightlyJob();
    return res.json(result);
  } catch (e: any) {
    return next(e);
  }
});

// Cron (local-only; prod should use Render Cron / external scheduler)
const ENABLE_LOCAL_CRON = String(process.env.ENABLE_LOCAL_CRON || "").toLowerCase() === "true";

if (ENABLE_LOCAL_CRON) {
  cron.schedule(
    NIGHTLY_CRON,
    async () => {
      try {
        const nowLocal = toZonedTime(new Date(), TIMEZONE);
        console.log(`🕑 Nightly cron fired at ${formatISO(nowLocal)}`);
        const result = await runNightlyJob();
        console.log("✅ Nightly job complete");
        console.log(result.report);
      } catch (e: any) {
        console.log("❌ Nightly job failed:", e?.message || e);
      }
    },
    { timezone: TIMEZONE }
  );

  console.log(`✅ Nightly scheduler started: "${NIGHTLY_CRON}" (${TIMEZONE})`);
} else {
  console.log("⏭️ Local cron disabled (ENABLE_LOCAL_CRON != true)");
}
// ============================
// Optional debug routes
// ============================
if (DEBUG_ROUTES_ENABLED) {
  function requireAdmin(req: Request, res: Response): boolean {
    const expected = safeStr(process.env.ADMIN_JOB_SECRET);
    const got = safeStr(req.header("X-Admin-Secret"));

    if (!expected) {
      res.status(500).json({ ok: false, error: "missing_ADMIN_JOB_SECRET" });
      return false;
    }
    if (!got || got !== expected) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return false;
    }
    return true;
  }

  app.get("/debug/inbound-docs", async (req: Request, res: Response, next: any) => {
    if (!requireAdmin(req, res)) return;

    try {
      const limit = Math.min(Math.max(Number(req.query.limit || 10), 1), 50);

      const r = await pool.query(
        `
        SELECT created_at, source, to_email, customer_id, filename, r2_bucket, r2_key, doc_type
        FROM inbound_docs
        ORDER BY created_at DESC
        LIMIT $1
        `,
        [limit]
      );

      return res.json({ ok: true, limit, rows: r.rows });
    } catch (e: any) {
      return next(e);
    }
  });

  app.get("/debug/customers", async (req: Request, res: Response, next: any) => {
    if (!requireAdmin(req, res)) return;

    try {
      const customers = await getCustomersCached();
      res.json({ ok: true, count: customers.length, customers });
    } catch (e: any) {
      return next(e);
    }
  });
}

// ============================
// Vulnerability Sweep Step 3: response surface minimization
// - JSON-only 404
// - Single central error handler (no stack leaks)
// ============================


// final 404 (must be AFTER all routes, BEFORE error handler)
app.use((req: Request, res: Response) => {
  return res.status(404).json({ ok: false, error: "not_found", path: req.originalUrl });
});

// single global error handler (FINAL HARDENING)
app.use((err: any, req: Request, res: Response, next: any) => {
  const requestId = getRequestId(req) || safeStr(req.header("X-Request-Id")) || "";

  // If headers already sent, delegate to default handler
  if (res.headersSent) return next(err);

  // Log full error server-side
  logError("UNHANDLED_ERROR", {
    requestId,
    path: safeStr(req.path),
    method: safeStr(req.method),
    message: safeStr(err?.message || err),
    code: safeStr(err?.code),
    status: Number(err?.status || err?.statusCode || 500),
  });

  const statusCode = Number(err?.statusCode || err?.status || 500);
  const isApp = err instanceof AppError;

  const safeStatus = Number.isFinite(statusCode) ? Math.min(Math.max(statusCode, 400), 599) : 500;

  // In prod, never leak stack/messages unless explicitly marked safe
  const expose = isApp ? !!(err as AppError).expose : !IS_PROD;

  const code = isApp ? safeStr((err as AppError).code || "internal_error") : "internal_error";
  const message = expose ? safeStr(err?.message || "Error") : "Request failed";

  return res.status(safeStatus).json({
    ok: false,
    error: code || "internal_error",
    message,
    requestId: requestId || undefined,
  });
});

// ============================
// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});