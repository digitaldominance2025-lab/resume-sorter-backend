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
const pdfParse: any = require("pdf-parse");

dotenv.config();
console.log("SERVER FILE LOADED");

// ============================
// Helpers
// ============================
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

// ✅ FIXED: sha256Hex only hashes; no nested allowlist junk inside
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
// Config
// ============================
const PORT = Number(process.env.PORT || 3001);
const TIMEZONE = process.env.TIMEZONE || "America/Regina";

const ADMIN_EMAIL = process.env.ADMIN_EMAIL || "digitaldominance2025@gmail.com";
const MASTER_SHEET_ID = process.env.MASTER_CUSTOMERS_SHEET_ID || "";
const MASTER_SHEET_TAB = process.env.MASTER_CUSTOMERS_SHEET_TAB || "customers";
const NIGHTLY_CRON = process.env.NIGHTLY_CRON || "10 2 * * *";

const DEBUG_ROUTES_ENABLED = truthyEnv("DEBUG_ROUTES") && process.env.NODE_ENV !== "production";
const LOG_AUTHED_GOOGLE_EMAIL = truthyEnv("LOG_AUTHED_GOOGLE_EMAIL");

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
 *
 * Example:
 *   R2_PUBLIC_LINKS=true
 *   R2_PUBLIC_BASE_URL=https://your-public-domain.com
 */
const R2_PUBLIC_LINKS = truthyEnv("R2_PUBLIC_LINKS");
const R2_PUBLIC_BASE_URL = safeStr(process.env.R2_PUBLIC_BASE_URL).replace(/\/+$/, "");

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

    console.log("✅ DB tables ensured");
  } catch (e: any) {
    console.warn("⚠️ DB bootstrap failed (continuing):", e?.message || e);
  }
}
void ensureDbTables();

async function upsertCustomerRubric(customerId: string, rubric: any) {
  if (!customerId) throw new Error("missing_customerId");
  await pool.query(
    `
    INSERT INTO customer_rubrics (customer_id, rubric_json, updated_at)
    VALUES ($1, $2::jsonb, now())
    ON CONFLICT (customer_id)
    DO UPDATE SET rubric_json = EXCLUDED.rubric_json, updated_at = now()
  `,
    [customerId, JSON.stringify(rubric)]
  );
}
async function getCustomerRubric(customerId: string): Promise<any | null> {
  if (!customerId) return null;
  try {
    const r = await pool.query(`SELECT rubric_json FROM customer_rubrics WHERE customer_id=$1`, [customerId]);
    return r.rows?.[0]?.rubric_json ?? null;
  } catch {
    return null;
  }
}

async function saveInboundDocToDb(args: {
  source: string;
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
    await pool.query(
      `
      INSERT INTO inbound_docs (
        source, to_email, customer_id, resolved_customer_id, match_found,
        billing_status, blocked_reason,
        filename, r2_bucket, r2_key, doc_type, extracted_chars, text_preview,
        ai_score, ai_json
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
    `,
      [
        safeStr(args.source) || "unknown",
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
      // Optional dependency: npm i mammoth
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

  // quick sanity
  const letters = (text.match(/[a-z]/g) || []).length;
  if (letters < 80) return "NON_RESUME";

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
  if (!R2_PUBLIC_BASE_URL) return ""; // ✅ extra safety switch
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

    // G = idempotency tokens csv (store stable hash + optional r2 key)
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
    if (docToken) wanted.push(docToken); // ✅ stable: hash:...
    if (r2Key) wanted.push(`r2:${r2Key}`); // optional: keep trace of r2 keys too

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

  return { spreadsheetId, spreadsheetUrl: webViewLink };
}

// ============================
// AI scoring wrapper
// ============================
async function safeScoreResume(text: string, rubric: any | null) {
  const fn: any = scoreResume as any;
  if (typeof fn !== "function") throw new Error("scoreResume_not_a_function");

  // If your scoreResume supports rubric as 2nd arg, pass it
  if (fn.length >= 2) return await fn(text, rubric);

  // Otherwise merge rubric into the prompt text
  if (rubric) {
    const rubricBlock = typeof rubric === "string" ? rubric : JSON.stringify(rubric, null, 2);
    const merged = `RUBRIC (customer scoring criteria):\n${rubricBlock}\n\n${text}`;
    return await fn(merged);
  }

  return await fn(text);
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
  source: "resend" | "inbound-file" | "inbound-r2";
  filename: string;
  buffer: Buffer;
  extractedText?: string;
  docType?: "RESUME" | "NON_RESUME";
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

  const docType: "RESUME" | "NON_RESUME" =
    args.docType || (extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME");

  const toEmail = safeStr(args.toEmail).trim().toLowerCase();
  const filenameForEmail = safeStr(args.filename);
  const r2KeyForEmail = safeStr(args?.r2?.key || "");

  let resolvedCustomerId = "";
  let customerId = "";
  let match: CustomerRow | null = null;

  if (toEmail) {
    try {
      const existingCustomers = await getCustomersCached();
      match =
        [...existingCustomers].reverse().find((x) => safeStr(x.intakeEmail).trim().toLowerCase() === toEmail) || null;

      if (match) {
        customerId = safeStr(match.customerId);
        resolvedCustomerId = customerId;
      } else {
        console.log("⚠️ No customer resolved. toEmail=", toEmail);
      }
    } catch (e: any) {
      console.warn("⚠️ CUSTOMER_RESOLVE_SKIPPED (google auth?):", e?.message || e);
    }
  } else {
    console.log("⚠️ No customer resolved. missing toEmail");
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

        // Stable idempotency token (same resume resent => same hash)
        const docHash = sha256Hex(args.buffer);
        const docToken = `hash:${docHash}`;

        // AI idempotency (daily): if docToken already in column G today, skip OpenAI
        let alreadyProcessedToday = false;

        try {
          if (tallySheetId && customerId && docToken) {
            const sheets = google.sheets({ version: "v4", auth: oauth2Client });

            // ensure row exists (and share the sheet)
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
          // If idempotency check fails, do NOT block scoring — just proceed
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
      // ✅ duplicate today → skip all Sheets writes
      const today = formatISO(toZonedTime(new Date(), TIMEZONE), { representation: "date" });
      tallyResult = { skipped: true, reason: "idempotent_skip", today, shouldIncrement: false };
      console.log("🧷 TALLY_SKIP_DUPLICATE:", customerId, today, docToken);
    } else if (sheetId && customerId) {
      tallyResult = await tallyApply(sheetId, customerId, docType, args.source, r2Key || undefined, docToken, ai);

      if ((tallyResult as any)?.shouldIncrement === false) {
        console.log("🧷 TALLY_APPLY_SKIP_OK:", customerId, tallyResult?.today, tallyResult?.nextCount);
      } else {
        console.log("✅ TALLY_APPLY_OK:", customerId, tallyResult?.today, tallyResult?.nextCount);
      }
    } else {
      console.warn("⚠️ TALLY_SKIP: missing sheetId or customerId", { customerId, sheetId });
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

    const hasToEmail = !!toEmail;
    const matchFound = !!match;
    const isResume = docType === "RESUME";
    const aiSkipped = !!ai?.skipped;
    const aiError = !!ai?.error;

    // ✅ HARD GATE: never email if we didn't increment tally
    if (!didIncrement) {
      console.log("📧 RESULT_EMAIL_SKIP:", {
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

    // Toggle: send a receipt email even if AI skipped (prevents ghosting)
    const SEND_RECEIPT_IF_AI_SKIPPED = true;

    // Eligible for scored email only if AI ran successfully
    const scoredEligible = hasToEmail && matchFound && !blocked && isResume && !aiSkipped && !aiError;

    // Eligible for receipt if resume + matched + not blocked (and toggle enabled)
    const receiptEligible = hasToEmail && matchFound && !blocked && isResume && SEND_RECEIPT_IF_AI_SKIPPED;

    // If neither, skip and RETURN
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

      console.log("📧 RESULT_EMAIL_SKIP:", {
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

    const summary =
      safeStr(ai?.summary) || safeStr(ai?.notes) || safeStr(ai?.feedback) || safeStr(ai?.reason) || "";

    const strengths = Array.isArray(ai?.strengths) ? ai.strengths.slice(0, 4) : [];
    const weaknesses = Array.isArray(ai?.weaknesses) ? ai.weaknesses.slice(0, 4) : [];

    // If AI didn't run, send receipt; else send scored result
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
        summary ? `Summary:` : undefined,
        summary ? summary : undefined,
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

    // ✅ safer: use wrapper so you don't depend on a single function name/signature
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
}

// ============================
// Express app
// ============================
const app = express();
app.use(cors());

// ============================
// Auth status (debug)
// ============================
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
app.post("/webhooks/resend-inbound", express.raw({ type: "application/json" }), async (req: Request, res: Response) => {
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
    // Set RESEND_DEV_FAKE_ATTACHMENTS=true to bypass Resend API calls (keeps Svix verification)
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
    // (line below) ✅ continue your existing webhook response / downstream processing
    const type = safeStr(evt?.type);

    // Resend sometimes surfaces the receiving email id as data.id (common),
    // but guard for data.email_id just in case.
    const emailId = safeStr(evt?.data?.id || evt?.data?.email_id);

    console.log("📩 RESEND VERIFIED EVENT:", {
      type,
      created_at: evt?.created_at,
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
      // ===============================
      // Attachment guards (Hardening Step 2)
      // ===============================
      const oversized: any[] = [];
      const allowed: typeof atts = [];

      let total = 0;
      for (const att of atts.slice(0, MAX_ATTACHMENTS)) {
        const original = safeStr(att?.filename) || "attachment.bin";
        const safeName = normalizeUploadName(original);

        // ✅ Hardening Step 1: allowlist enforced here (before download)
        if (!isAllowedFileByName(safeName)) {
          oversized.push({
            id: safeStr(att?.id),
            filename: safeName,
            size: Number(att?.size || 0),
            reason: "unsupported_file_type",
            allowed: Array.from(ALLOWED_EXTS),
          });
          continue;
        }

        const size = Number(att?.size || 0);

        // Missing/invalid size: block (safer than downloading unknown huge files)
        if (!Number.isFinite(size) || size <= 0) {
          oversized.push({
            id: safeStr(att?.id),
            filename: safeName,
            size,
            reason: "missing_or_invalid_size",
          });
          continue;
        }

        if (size > MAX_ATTACHMENT_BYTES) {
          oversized.push({
            id: safeStr(att?.id),
            filename: safeName,
            size,
            reason: "attachment_too_large",
            limit: MAX_ATTACHMENT_BYTES,
          });
          continue;
        }

        if (total + size > MAX_TOTAL_ATTACH_BYTES) {
          oversized.push({
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
    oversized.length > 0 && oversized.every((x) => safeStr(x?.reason) === "unsupported_file_type");

  if (onlyUnsupported) {
    return res.status(415).json({
      ok: false,
      error: "unsupported_file_type",
      emailId,
      toEmail,
      blocked: oversized,
      allowed: Array.from(ALLOWED_EXTS),
    });
  }

  return res.status(413).json({
    ok: false,
    error: "attachments_blocked_by_size_guard",
    emailId,
    toEmail,
    oversized,
  });
}

      // ===============================
      // Process allowed attachments only
      // ===============================
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

        // Extra safety: enforce byte caps even after download
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
});

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
    console.log("💳 STRIPE EVENT:", event.type);

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
    console.error("❌ STRIPE_WEBHOOK_FAILED:", e?.message || e);
    return res.status(400).json({ ok: false, error: "stripe_webhook_failed", message: String(e?.message || e) });
  }
});

// ✅ global JSON parser AFTER raw-body webhooks
app.use(express.json({ limit: "10mb" }));

// ===== ROUTES =====
app.use("/upload", uploadRoutes);
app.use(intakeRoutes);

// ===== HEALTH =====
app.get("/", (_req: Request, res: Response) => res.send("✅ Resume Sorter Backend Running"));

// ============================
// Rubrics API
// ============================
app.post("/customers/rubric", async (req: Request, res: Response) => {
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
    return res.status(500).json({ ok: false, error: "rubric_save_failed", message: String(e?.message || e) });
  }
});
app.get("/customers/rubric", async (req: Request, res: Response) => {
  try {
    const customerId = safeStr(req.query.customerId);
    if (!customerId) return res.status(400).json({ ok: false, error: "missing_customerId" });

    const rubric = await getCustomerRubric(customerId);
    return res.json({ ok: true, customerId, rubric });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: "rubric_get_failed", message: String(e?.message || e) });
  }
});

// ============================
// Customers: setup
// ============================
app.post("/customers/setup", async (req: Request, res: Response) => {
  try {
    const companyName = safeStr(req.body?.companyName);
    if (!companyName) return res.status(400).json({ ok: false, error: "missing_companyName" });

    if (!MASTER_SHEET_ID) return res.status(500).json({ ok: false, error: "MASTER_CUSTOMERS_SHEET_ID not set" });

    await oauth2Client.getAccessToken();
    await ensureMasterHeaders();

    const slug = makeSlug(companyName);
    const customerId = makeCustomerId(slug);
    const intakeEmail = `${slug}@digitaldominance.ca`;

    const nowLocal = toZonedTime(new Date(), TIMEZONE);
    const trialStartAt = formatISO(nowLocal, { representation: "date" });
    const trialEndAt = formatISO(addDaysDfns(nowLocal, 30), { representation: "date" });

    const tally = await createTallySheetForCustomer(companyName, customerId);
    const tallySheetId = safeStr(tally?.spreadsheetId);
    const tallySheetUrl = safeStr(tally?.spreadsheetUrl);
    const createdAtIso = new Date().toISOString();

    const row = [
      customerId,
      companyName,
      slug,
      intakeEmail,
      ADMIN_EMAIL,
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

    return res.json({
      ok: true,
      companyName,
      slug,
      customerId,
      intakeEmail,
      adminEmail: ADMIN_EMAIL,
      status: "trial",
      trialStartAt,
      trialEndAt,
      tallySheetId,
      tallySheetUrl,
    });
  } catch (e: any) {
    console.error("❌ /customers/setup error:", e?.response?.data || e?.message || e);
    return res.status(500).json({
      ok: false,
      error: "customers_setup_failed",
      message: String(e?.message || e),
      data: e?.response?.data || null,
    });
  }
});

// ============================
// Billing endpoints
// ============================
app.post("/billing/create-checkout-session", async (req: Request, res: Response) => {
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
    console.error("❌ create-checkout-session failed:", e?.message || e);
    return res.status(500).json({ ok: false, error: "checkout_failed", message: String(e?.message || e) });
  }
});

app.post("/billing/create-portal-session", async (req: Request, res: Response) => {
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
    console.error("❌ create-portal-session failed:", e?.message || e);
    return res.status(500).json({ ok: false, error: "portal_failed", message: String(e?.message || e) });
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

app.post("/webhooks/inbound-file", inboundFileMulter, async (req: Request, res: Response) => {
  try {
    const f: any = (req as any).file;
    if (!f) return res.status(400).json({ ok: false, error: "no_file" });

    const toEmail = safeStr((req.body as any)?.toEmail || (req.body as any)?.to).trim().toLowerCase();
    if (!toEmail) return res.status(400).json({ ok: false, error: "missing_toEmail" });

    const bucket = safeStr(process.env.CLOUDFLARE_R2_BUCKET);
    if (!bucket) return res.status(500).json({ ok: false, error: "R2_BUCKET_not_set" });

    const original = safeStr(f.originalname || "upload.bin");
    const safeName = normalizeUploadName(original);

    // ✅ Hardening Step 1: allowlist enforced here
    if (!isAllowedFileByName(safeName)) {
      return res.status(415).json(unsupportedFileTypePayload(safeName));
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
      console.log("📎 FILE INBOUND saved:", finalPath);
    }

    try {
      const up = await r2UploadBuffer({
        key: r2Key,
        buffer: f.buffer,
        contentType: safeStr(f.mimetype) || "application/octet-stream",
      });

      r2 = { bucket, key: up?.key || r2Key };
      console.log("☁️ R2 UPLOAD ok:", bucket, r2.key);
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
        console.log("🧹 LOCAL_DELETE_OK:", savedLocal);
        deletedLocal = true;
        savedLocal = null;
      } catch (e: any) {
        console.warn("⚠️ LOCAL_DELETE_FAILED:", savedLocal, e?.message || e);
      }
    } else {
      deletedLocal = true;
    }

    const result = await processInboundDoc({
      source: "inbound-file",
      filename: safeName,
      buffer: f.buffer,
      extractedText,
      docType,
      toEmail,
      r2,
      savedLocal,
      deletedLocal,
    });

    return res.json(result);
  } catch (e: any) {
    console.error("❌ inbound-file error", e);
    return res.status(500).json({ ok: false, error: "inbound_file_failed", message: String(e?.message || e) });
  }
});
// ✅ Multer error handler for inbound-file (prevents HTML error pages)
app.use("/webhooks/inbound-file", (err: any, _req: Request, res: Response, next: any) => {
  if (!err) return next();

  // Multer file size limit
  if (err?.code === "LIMIT_FILE_SIZE") {
    return res.status(413).json({
      ok: false,
      error: "payload_too_large",
      message: "File too large",
      maxBytes: Number(process.env.MAX_UPLOAD_BYTES || 15 * 1024 * 1024),
    });
  }

  // Any other multer errors
  if (err?.name === "MulterError") {
    return res.status(400).json({ ok: false, error: "multer_error", code: err.code, message: String(err.message || err) });
  }

  return next(err);
});

app.post("/webhooks/inbound-r2", async (req: Request, res: Response) => {
  try {
    const secret = req.header("X-Inbound-Secret");
    if (process.env.INBOUND_WEBHOOK_SECRET) {
      if (!secret || secret !== process.env.INBOUND_WEBHOOK_SECRET) {
        return res.status(401).json({ ok: false, error: "unauthorized" });
      }
    }
    const body = typeof req.body === "string" ? tryJsonParse(req.body) : req.body;

    const key = safeStr((body as any)?.key);
    const toEmail = safeStr((body as any)?.toEmail || (body as any)?.to).trim().toLowerCase();
    if (!key) return res.status(400).json({ ok: false, error: "missing_key" });
    if (!toEmail) return res.status(400).json({ ok: false, error: "missing_toEmail" });

    const bucket = safeStr(process.env.CLOUDFLARE_R2_BUCKET);
    if (!bucket) return res.status(500).json({ ok: false, error: "R2_BUCKET_not_set" });

    const filename = safeBaseName(key).replace(/^\d{4}-\d{2}-\d{2}T.*?Z__/, "");

    // ✅ Hardening Step 1: allowlist enforced here
    if (!isAllowedFileByName(filename)) {
      return res.status(415).json(unsupportedFileTypePayload(filename));
    }

    const buffer = await r2DownloadToBuffer({ key });
    if (!buffer) return res.status(404).json({ ok: false, error: "object_not_found" });

    const extractedText = await extractTextFromBuffer(filename, buffer);
    const docType: "RESUME" | "NON_RESUME" =
      extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME";

    const result = await processInboundDoc({
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
    console.error("❌ inbound-r2 error", e);
    return res.status(500).json({ ok: false, error: "inbound_r2_failed", message: String(e?.message || e) });
  }
});

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
  }

  // Customer daily reports
  for (const c of customers) {
    try {
      const to = safeStr(c.adminEmail);
      const sheetId = safeStr(c.tallySheetId);
      if (!to || !sheetId) continue;

      const gate = isProcessingAllowed(c.status);
      if (!gate.allowed) continue;

      const t = await readTodayTallyRowByHeaders(sheetId, todayLocalISO);
      if (!t.count || t.count <= 0) continue;

      // Hardening Step 3: only include public link if explicitly enabled + base url set
      const publicUrl = t.r2Key ? buildR2PublicUrl(t.r2Key) : "";

      const avgOrLast = t.lastScore !== null ? `Last score: ${t.lastScore}` : `Last score: N/A`;

      const body = [
        `Daily Resume Report — ${safeStr(c.companyName)}`,
        ``,
        `Date: ${todayLocalISO}`,
        `Resumes processed: ${t.count}`,
        avgOrLast,
        ``,
        publicUrl ? `Latest resume (public): ${publicUrl}` : undefined,
        ``,
        t.resumeLinkCell ? `Latest resume link:` : undefined,
        t.resumeLinkCell ? `${extractUrlFromHyperlinkFormula(t.resumeLinkCell) || t.resumeLinkCell}` : undefined,
        ``,
        t.r2KeysCsv ? `R2 keys:` : undefined,
        t.r2KeysCsv ? `${t.r2KeysCsv}` : undefined,
      ]
        .filter(Boolean)
        .join("\n");

      await sendCustomerText(to, `Daily Resume Report — ${safeStr(c.companyName)} (${todayLocalISO})`, body);
      console.log("📨 NIGHTLY_CUSTOMER_SENT:", c.customerId, "->", to);
    } catch (e: any) {
      console.log("⚠️ NIGHTLY_CUSTOMER_FAILED:", c.customerId, e?.message || e);
    }
  }

  const report = lines.join("\n");
  await sendAdmin(`Resume Sorter Nightly (${todayLocalISO})`, report);

  return { ok: true, date: todayLocalISO, report };
}

// Manual trigger (admin)
app.get("/admin/nightly-run", async (req: Request, res: Response) => {
  try {
    const secret = safeStr(req.header("X-Admin-Secret"));
    const expected = safeStr(process.env.ADMIN_JOB_SECRET);

    if (!expected) {
      return res.status(500).json({ ok: false, error: "missing_ADMIN_JOB_SECRET" });
    }
    if (!secret || secret !== expected) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const result = await runNightlyJob();
    return res.json(result);
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});
// Cron
const ENABLE_LOCAL_CRON =
  String(process.env.ENABLE_LOCAL_CRON || "").toLowerCase() === "true";

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
      } catch (e) {
        console.log("❌ Nightly job failed:", e);
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
  app.get("/debug/customers", async (_req: Request, res: Response) => {
    try {
      const customers = await getCustomersCached();
      res.json({ ok: true, count: customers.length, customers });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || String(e) });
    }
  });
}

// ============================
// Start server
// ============================
app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});