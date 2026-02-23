"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// src/server.ts
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
const dotenv_1 = __importDefault(require("dotenv"));
const node_cron_1 = __importDefault(require("node-cron"));
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const multer_1 = __importDefault(require("multer"));
const date_fns_1 = require("date-fns");
const date_fns_tz_1 = require("date-fns-tz");
const googleapis_1 = require("googleapis");
const upload_1 = __importDefault(require("./routes/upload"));
const intake_1 = __importDefault(require("./routes/intake"));
const openaiScore_1 = require("./services/openaiScore");
const emailSvc = __importStar(require("./services/email"));
const customerUtils = __importStar(require("./utils/customer"));
// ✅ Use your single R2 service (do NOT also create AWS SDK client here)
const r2_1 = require("./services/r2");
// pdf-parse (classic callable function) - requires: npm i pdf-parse@1.1.1
const pdfParse = require("pdf-parse");
dotenv_1.default.config();
console.log("SERVER FILE LOADED");
// ============================
// Helpers
// ============================
function safeStr(v) {
    return (v ?? "").toString().trim();
}
function mustEnv(name) {
    const v = safeStr(process.env[name]);
    if (!v)
        throw new Error(`Missing env var: ${name}`);
    return v;
}
function parseISODate(s) {
    const d = new Date(s);
    if (Number.isNaN(d.getTime()))
        return null;
    return d;
}
async function shareSpreadsheetWithEmail(spreadsheetId, email) {
    const drive = googleapis_1.google.drive({ version: "v3", auth: oauth2Client });
    try {
        await drive.permissions.create({
            fileId: spreadsheetId,
            sendNotificationEmail: false,
            requestBody: {
                type: "user",
                role: "writer",
                emailAddress: email,
            },
        });
        console.log("✅ SHEET_SHARED_OK:", spreadsheetId, "->", email);
        return { ok: true };
    }
    catch (e) {
        console.warn("⚠️ SHEET_SHARED_FAILED:", spreadsheetId, "->", email, " :: ", e?.response?.data || e?.message || e);
        return { ok: false, error: e?.response?.data || e?.message || String(e) };
    }
}
function safeBaseName(key) {
    return key.split("/").pop() || "file.bin";
}
function truthyEnv(name) {
    const v = safeStr(process.env[name]).toLowerCase();
    return v === "1" || v === "true" || v === "yes" || v === "y";
}
// (legacy) local text file reader
function readTextFileClean(filePath) {
    const buf = fs_1.default.readFileSync(filePath);
    // Try UTF-8 first
    let text = buf.toString("utf8");
    // If it's full of null bytes, it was likely saved as UTF-16 (common from Notepad)
    const nullCount = (text.match(/\u0000/g) || []).length;
    if (nullCount > Math.max(10, text.length * 0.1)) {
        const isUtf16LE = buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe;
        const isUtf16BE = buf.length >= 2 && buf[0] === 0xfe && buf[1] === 0xff;
        if (isUtf16BE)
            text = buf.toString("utf16le").split("").reverse().join(""); // rare fallback
        else
            text = buf.toString("utf16le"); // typical Windows
    }
    // Strip UTF-8 BOM
    if (text.charCodeAt(0) === 0xfeff)
        text = text.slice(1);
    // Strip weird replacement chars
    text = text.replace(/^\uFFFD+/, "");
    // Remove embedded nulls
    text = text.replace(/\u0000/g, "");
    return text;
}
// ============================
// Config
// ============================
const PORT = Number(process.env.PORT || 3001);
const TIMEZONE = process.env.TIMEZONE || "America/Edmonton";
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || "digitaldominance2025@gmail.com";
const MASTER_SHEET_ID = process.env.MASTER_CUSTOMERS_SHEET_ID || "";
const MASTER_SHEET_TAB = process.env.MASTER_CUSTOMERS_SHEET_TAB || "customers";
const NIGHTLY_CRON = process.env.NIGHTLY_CRON || "10 2 * * *";
// Debug flags
const DEBUG_ROUTES_ENABLED = truthyEnv("DEBUG_ROUTES") && process.env.NODE_ENV !== "production";
const LOG_AUTHED_GOOGLE_EMAIL = truthyEnv("LOG_AUTHED_GOOGLE_EMAIL");
// ============================
// Google OAuth
// ============================
const oauth2Client = new googleapis_1.google.auth.OAuth2(process.env.GOOGLE_CLIENT_ID, process.env.GOOGLE_CLIENT_SECRET, process.env.GOOGLE_REDIRECT_URI);
const SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid",
];
const TOKENS_DIR = path_1.default.join(process.cwd(), ".tokens");
const TOKENS_PATH = path_1.default.join(TOKENS_DIR, "google_tokens.json");
function ensureTokensDir() {
    if (!fs_1.default.existsSync(TOKENS_DIR))
        fs_1.default.mkdirSync(TOKENS_DIR, { recursive: true });
}
function saveTokensToDisk(tokens) {
    ensureTokensDir();
    fs_1.default.writeFileSync(TOKENS_PATH, JSON.stringify(tokens, null, 2), "utf8");
}
// ============================
// Google OAuth client + token load helpers
// ============================
function decodeJwtPayload(token) {
    const parts = token.split(".");
    if (parts.length < 2)
        return null;
    const b64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
    try {
        const json = Buffer.from(padded, "base64").toString("utf8");
        return JSON.parse(json);
    }
    catch {
        return null;
    }
}
async function logAuthedGoogleEmailFromTokens() {
    try {
        // force refresh if needed
        await oauth2Client.getAccessToken();
        const oauth2 = googleapis_1.google.oauth2({
            auth: oauth2Client,
            version: "v2",
        });
        const me = await oauth2.userinfo.v2.me.get();
        const email = String(me?.data?.email || "");
        console.log("🔐 AUTHED GOOGLE EMAIL:", email || "(none)");
        return email || null;
    }
    catch (e) {
        console.warn("⚠️ logAuthedGoogleEmailFromTokens failed:", e?.message || e);
        return null;
    }
}
function loadTokensFromDisk() {
    try {
        if (!fs_1.default.existsSync(TOKENS_PATH)) {
            console.log("ℹ️ No token file found yet. Need /auth once.");
            return false;
        }
        const raw = fs_1.default.readFileSync(TOKENS_PATH, "utf8");
        const tokens = JSON.parse(raw);
        oauth2Client.setCredentials(tokens);
        console.log("✅ Loaded Google tokens from disk");
        // ✅ only do userinfo logging if explicitly enabled
        if (LOG_AUTHED_GOOGLE_EMAIL) {
            void logAuthedGoogleEmailFromTokens();
        }
        return true;
    }
    catch (e) {
        console.log("⚠️ Failed to load token file:", e?.message || e);
        return false;
    }
}
loadTokensFromDisk();
// ============================
// Email
// ============================
async function sendAdmin(subject, body) {
    const fn = emailSvc.sendAdminEmail;
    if (typeof fn === "function") {
        try {
            await fn(ADMIN_EMAIL, subject, body);
            return;
        }
        catch (e) {
            console.log("⚠️ sendAdminEmail failed (continuing):", e);
            return;
        }
    }
    console.log("ℹ️ emailSvc.sendAdminEmail not found; skipping email.");
}
// ============================
// Customer utilities
// ============================
function makeSlug(companyName) {
    const fn = customerUtils.slugifyCompanyName;
    if (typeof fn === "function")
        return fn(companyName);
    return companyName
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)+/g, "");
}
function makeCustomerId(slug) {
    const fn = customerUtils.makeCustomerId;
    if (typeof fn === "function") {
        try {
            return fn.length >= 1 ? fn(slug) : fn();
        }
        catch {
            return `cust_${slug}_${Date.now()}`;
        }
    }
    return `cust_${slug}_${Date.now()}`;
}
// Your sheet uses these header names (keep them consistent)
const MASTER_HEADERS = [
    "customerId",
    "companyName",
    "companySlug",
    "intakeEmail",
    "reportToEmail",
    "billingStatus",
    "trialStartAt", // YYYY-MM-DD (local)
    "trialEndAt", // YYYY-MM-DD (local)
    "currentSheetId",
    "currentSheetUrl",
    "currentSheetStartAt",
    "currentSheetEndAt",
    "createdAt",
];
function mapHeaderIndexes(headers) {
    const norm = headers.map((h) => safeStr(h).toLowerCase());
    const idx = (name) => norm.indexOf(name.toLowerCase());
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
    };
}
function columnLetter(n) {
    let s = "";
    while (n > 0) {
        const m = (n - 1) % 26;
        s = String.fromCharCode(65 + m) + s;
        n = Math.floor((n - 1) / 26);
    }
    return s;
}
async function ensureMasterHeaders() {
    if (!MASTER_SHEET_ID)
        return;
    const sheets = googleapis_1.google.sheets({ version: "v4", auth: oauth2Client });
    const headerRange = `${MASTER_SHEET_TAB}!1:1`;
    console.log("🔎 MASTER sheetId:", MASTER_SHEET_ID, "tab:", MASTER_SHEET_TAB);
    const headerResp = await sheets.spreadsheets.values.get({
        spreadsheetId: MASTER_SHEET_ID,
        range: headerRange,
    });
    const hasHeaders = (headerResp.data.values?.[0]?.length || 0) > 0;
    if (hasHeaders)
        return;
    await sheets.spreadsheets.values.update({
        spreadsheetId: MASTER_SHEET_ID,
        range: headerRange,
        valueInputOption: "RAW",
        requestBody: { values: [Array.from(MASTER_HEADERS)] },
    });
}
async function readCustomersFromMasterSheet() {
    if (!MASTER_SHEET_ID)
        return [];
    const sheets = googleapis_1.google.sheets({ version: "v4", auth: oauth2Client });
    const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: MASTER_SHEET_ID,
        range: `${MASTER_SHEET_TAB}!A:Z`,
    });
    const values = resp.data.values || [];
    if (values.length < 2)
        return [];
    const headers = (values[0] || []).map(String);
    const col = mapHeaderIndexes(headers);
    if (col.customerId < 0)
        return [];
    const out = [];
    for (let i = 1; i < values.length; i++) {
        const row = values[i] || [];
        const customerId = safeStr(row[col.customerId]);
        if (!customerId)
            continue;
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
        });
    }
    return out;
}
async function updateCustomerStatusInMasterSheet(customerId, newStatus) {
    if (!MASTER_SHEET_ID)
        return;
    const sheets = googleapis_1.google.sheets({ version: "v4", auth: oauth2Client });
    const resp = await sheets.spreadsheets.values.get({
        spreadsheetId: MASTER_SHEET_ID,
        range: `${MASTER_SHEET_TAB}!A:Z`,
    });
    const values = resp.data.values || [];
    if (values.length < 2)
        return;
    const headers = (values[0] || []).map(String);
    const col = mapHeaderIndexes(headers);
    if (col.customerId < 0 || col.status < 0)
        return;
    let rowNumber = -1;
    for (let i = 1; i < values.length; i++) {
        const row = values[i] || [];
        if (safeStr(row[col.customerId]) === customerId) {
            rowNumber = i + 1;
            break;
        }
    }
    if (rowNumber < 0)
        return;
    const statusColLetter = columnLetter(col.status + 1);
    const range = `${MASTER_SHEET_TAB}!${statusColLetter}${rowNumber}`;
    await sheets.spreadsheets.values.update({
        spreadsheetId: MASTER_SHEET_ID,
        range,
        valueInputOption: "RAW",
        requestBody: { values: [[newStatus]] },
    });
}
// ============================
// Customers cache (TTL)
// ============================
let customersCache = null;
let customersCacheAt = 0;
async function getCustomersCached() {
    const now = Date.now();
    const ttlMs = 60000;
    if (customersCache && now - customersCacheAt < ttlMs)
        return customersCache;
    customersCache = await readCustomersFromMasterSheet();
    customersCacheAt = now;
    return customersCache;
}
// ============================
// Doc classification + tally
// ============================
function classifyDocTypeFromText(textRaw) {
    const text = (textRaw || "").toLowerCase();
    const nonResumeHits = [
        "invoice",
        "payment",
        "amount due",
        "balance due",
        "bill to",
        "receipt",
        "purchase order",
        "statement",
        "gst",
        "hst",
    ].some((k) => text.includes(k));
    if (nonResumeHits)
        return "NON_RESUME";
    const resumeHits = [
        "experience",
        "employment",
        "work history",
        "education",
        "skills",
        "certification",
        "resume",
        "curriculum vitae",
        "@",
    ].some((k) => text.includes(k));
    return resumeHits ? "RESUME" : "NON_RESUME";
}
function appendNote(existing, note) {
    const ex = safeStr(existing);
    if (!ex)
        return note;
    const parts = ex.split(",").map((s) => s.trim());
    if (parts.includes(note))
        return ex;
    return `${ex}, ${note}`;
}
async function getOrCreateTodayRow(sheets, tallySheetId, today, customerId) {
    console.log("🔎 TALLY sheetId:", tallySheetId);
    const existing = await sheets.spreadsheets.values.get({
        spreadsheetId: tallySheetId,
        range: "A2:G1000", // ✅ include G now
    });
    const rows = existing.data.values || [];
    const rowIndex0 = rows.findIndex((r) => safeStr(r?.[0]) === today);
    if (rowIndex0 !== -1) {
        const currentCount = Number(rows[rowIndex0]?.[1] || 0);
        const currentNotes = safeStr(rows[rowIndex0]?.[2]);
        return { rowIndex0, currentCount, currentNotes };
    }
    // NOTE: 7 columns A:G now
    await sheets.spreadsheets.values.append({
        spreadsheetId: tallySheetId,
        range: "A:G",
        valueInputOption: "RAW",
        requestBody: { values: [[today, 0, "", customerId, "", "", ""]] },
    });
    const reread = await sheets.spreadsheets.values.get({
        spreadsheetId: tallySheetId,
        range: "A2:G1000",
    });
    const rows2 = reread.data.values || [];
    const idx2 = rows2.findIndex((r) => safeStr(r?.[0]) === today);
    return {
        rowIndex0: idx2 === -1 ? rows2.length - 1 : idx2,
        currentCount: 0,
        currentNotes: "",
    };
}
async function tallyApply(tallySheetId, customerId, docType, r2Key, ai) {
    const sheets = googleapis_1.google.sheets({ version: "v4", auth: oauth2Client });
    const today = (0, date_fns_1.formatISO)((0, date_fns_tz_1.toZonedTime)(new Date(), TIMEZONE), { representation: "date" }); // ✅ local YYYY-MM-DD
    const { rowIndex0, currentCount, currentNotes } = await getOrCreateTodayRow(sheets, tallySheetId, today, customerId);
    // remove old NON_RESUME notes forever
    const cleanedNotes = String(currentNotes || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .filter((s) => s !== "inbound-file:NON_RESUME")
        .join(", ");
    const rowNumber = rowIndex0 + 2;
    const note = docType === "RESUME" ? "inbound-file:RESUME" : null;
    let nextNotes = note ? appendNote(cleanedNotes, note) : cleanedNotes;
    // ✅ persist r2Key pointer for traceability (idempotent)
    if (r2Key) {
        nextNotes = appendNote(nextNotes, `r2:${r2Key}`);
    }
    // ✅ idempotency: if r2Key already recorded today, do not increment count again
    let shouldIncrement = docType === "RESUME";
    if (docType === "RESUME" && r2Key) {
        try {
            const existingF = await sheets.spreadsheets.values.get({
                spreadsheetId: tallySheetId,
                range: `F${rowNumber}`,
            });
            const current = safeStr(existingF.data.values?.[0]?.[0]);
            const hasKeyAlready = current
                ? current
                    .split(",")
                    .map((s) => s.trim())
                    .includes(r2Key)
                : false;
            if (hasKeyAlready) {
                shouldIncrement = false;
                console.log("🧷 TALLY_IDEMPOTENT_SKIP:", customerId, today, r2Key);
                return { today, nextCount: currentCount, nextNotes: cleanedNotes };
            }
        }
        catch {
            // if F read fails, we still increment like before
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
    // ✅ cost control: only store keys for RESUME
    if (docType === "RESUME" && r2Key) {
        // E = latest r2Key
        await sheets.spreadsheets.values.update({
            spreadsheetId: tallySheetId,
            range: `E${rowNumber}`,
            valueInputOption: "RAW",
            requestBody: { values: [[r2Key]] },
        });
        // F = running list of r2Keys for the day
        const existingF = await sheets.spreadsheets.values.get({
            spreadsheetId: tallySheetId,
            range: `F${rowNumber}`,
        });
        const current = safeStr(existingF.data.values?.[0]?.[0]);
        const next = current
            ? current
                .split(",")
                .map((s) => s.trim())
                .includes(r2Key)
                ? current
                : `${current}, ${r2Key}`
            : r2Key;
        await sheets.spreadsheets.values.update({
            spreadsheetId: tallySheetId,
            range: `F${rowNumber}`,
            valueInputOption: "RAW",
            requestBody: { values: [[next]] },
        });
        // G = latest AI score (only if present)
        const scoreNum = Number(ai?.score);
        if (Number.isFinite(scoreNum)) {
            await sheets.spreadsheets.values.update({
                spreadsheetId: tallySheetId,
                range: `G${rowNumber}`,
                valueInputOption: "RAW",
                requestBody: { values: [[scoreNum]] },
            });
        }
    }
    return { today, nextCount, nextNotes };
}
// ============================
// Tally sheet creation
// ============================
async function createTallySheetForCustomer(companyName, customerId) {
    const drive = googleapis_1.google.drive({ version: "v3", auth: oauth2Client });
    const sheets = googleapis_1.google.sheets({ version: "v4", auth: oauth2Client });
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
    const webViewLink = safeStr(created.data.webViewLink) ||
        (spreadsheetId ? `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit` : "");
    if (!spreadsheetId)
        throw new Error("tally_sheet_create_failed");
    await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: "A1:G1",
        valueInputOption: "RAW",
        requestBody: {
            values: [["date", "resumesProcessed", "notes", "customerId", "r2Key", "r2Keys", "lastScore"]],
        },
    });
    return { spreadsheetId, spreadsheetUrl: webViewLink };
}
// ============================
// Inbound processing core (CLOUD-READY)
// ============================
function decodeTextSmartBuf(buf) {
    // UTF-16LE BOM (Notepad often)
    if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
        return buf.toString("utf16le").replace(/^\uFEFF/, "");
    }
    // UTF-8 BOM
    if (buf.length >= 3 && buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf) {
        return buf.toString("utf8").replace(/^\uFEFF/, "");
    }
    // fallback
    return buf.toString("utf8");
}
async function extractTextFromBuffer(filename, buf) {
    const nameLower = filename.toLowerCase();
    try {
        if (nameLower.endsWith(".txt")) {
            return safeStr(decodeTextSmartBuf(buf)).replace(/\u0000/g, "");
        }
        if (nameLower.endsWith(".pdf")) {
            const parsed = await pdfParse(buf);
            return safeStr(parsed?.text || "");
        }
        // unknown -> treat as text
        return safeStr(decodeTextSmartBuf(buf)).replace(/\u0000/g, "");
    }
    catch (e) {
        console.warn("⚠️ EXTRACT_FAILED:", filename, e?.message || e);
        return "";
    }
}
async function processInboundDoc(args) {
    const extractedText = typeof args.extractedText === "string"
        ? args.extractedText
        : await extractTextFromBuffer(args.filename, args.buffer);
    const docType = args.docType ||
        (extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME");
    const toEmail = safeStr(args.toEmail).trim().toLowerCase();
    let resolvedCustomerId = "";
    let customerId = "";
    let match = null;
    // ✅ Customer resolve (never hard-fail inbound if Google is down)
    if (toEmail) {
        try {
            const existingCustomers = await getCustomersCached();
            match = [...existingCustomers]
                .reverse() // prefer newest
                .find((x) => safeStr(x.intakeEmail).trim().toLowerCase() === toEmail);
            if (match) {
                customerId = safeStr(match.customerId);
                resolvedCustomerId = customerId;
            }
            else {
                console.log("⚠️ No customer resolved. toEmail=", toEmail);
            }
        }
        catch (e) {
            console.warn("⚠️ CUSTOMER_RESOLVE_SKIPPED (google auth?):", e?.message || e);
            // leave customerId empty; inbound still succeeds
        }
    }
    else {
        console.log("⚠️ No customer resolved. missing toEmail");
    }
    // ---- AI only for RESUME (cost control)
    let ai = null;
    try {
        if (docType === "RESUME" && extractedText.trim().length > 40) {
            ai = await (0, openaiScore_1.scoreResume)(extractedText);
        }
        else {
            ai = { skipped: true, reason: docType !== "RESUME" ? "non_resume" : "too_short" };
        }
    }
    catch (e) {
        ai = { error: "ai_failed", message: String(e?.message || e) };
    }
    // ---- TALLY APPLY
    let tallyResult = null;
    try {
        const sheetId = safeStr(match?.tallySheetId);
        const r2Key = safeStr(args?.r2?.key || "");
        if (sheetId && customerId) {
            tallyResult = await tallyApply(sheetId, customerId, docType, r2Key || undefined, ai);
            console.log("✅ TALLY_APPLY_OK:", customerId, tallyResult?.today, tallyResult?.nextCount);
        }
        else {
            console.warn("⚠️ TALLY_SKIP: missing sheetId or customerId", { customerId, sheetId });
        }
    }
    catch (e) {
        const status = e?.response?.status || e?.code || null;
        const data = e?.response?.data || null;
        console.error("❌ TALLY_FAILED:", e?.message || e);
        console.error("   ↳ status:", status);
        console.error("   ↳ data:", JSON.stringify(data));
        tallyResult = { error: "tally_failed", status, data, message: String(e?.message || e) };
    }
    const textPreview = String(extractedText || "").slice(0, 400);
    return {
        ok: true,
        savedLocal: args.savedLocal ?? null,
        deletedLocal: !!args.deletedLocal,
        r2: args.r2 ?? null,
        r2Key: args.r2?.key || null,
        toEmail,
        customerId,
        resolvedCustomerId,
        docType,
        textPreview,
        tally: typeof tallyResult !== "undefined" ? tallyResult : null,
        ai,
        matchFound: !!match,
    };
}
// ============================
// Express app
// ============================
const app = (0, express_1.default)();
app.use((0, cors_1.default)());
app.use(express_1.default.json());
// ===== DEBUG ROUTES (guarded) =====
if (DEBUG_ROUTES_ENABLED) {
    app.get("/debug/whoami", async (_req, res) => {
        try {
            const tokenResp = await oauth2Client.getAccessToken();
            const accessToken = typeof tokenResp === "string" ? tokenResp : tokenResp?.token || "";
            if (!accessToken) {
                return res.status(500).json({ ok: false, error: "no_access_token" });
            }
            const info = await oauth2Client.getTokenInfo(accessToken);
            return res.json({
                ok: true,
                email: info?.email || null,
                scopes: info?.scopes || null,
            });
        }
        catch (e) {
            return res.status(500).json({ ok: false, error: e?.message || String(e) });
        }
    });
    app.get("/debug/customers", async (_req, res) => {
        try {
            if (!MASTER_SHEET_ID) {
                return res.status(500).json({ ok: false, error: "MASTER_CUSTOMERS_SHEET_ID not set" });
            }
            const sheets = googleapis_1.google.sheets({ version: "v4", auth: oauth2Client });
            const range = `${MASTER_SHEET_TAB}!A:Z`;
            const resp = await sheets.spreadsheets.values.get({
                spreadsheetId: MASTER_SHEET_ID,
                range,
            });
            const values = resp.data.values || [];
            const headers = (values[0] || []).map(String);
            const col = mapHeaderIndexes(headers);
            const customers = await readCustomersFromMasterSheet();
            return res.json({
                ok: true,
                sheetId: MASTER_SHEET_ID,
                tab: MASTER_SHEET_TAB,
                range,
                rawRows: values.slice(0, 3),
                headerMap: col,
                count: customers.length,
                sample: customers.slice(0, 5),
            });
        }
        catch (e) {
            return res.status(500).json({ ok: false, error: e?.message || String(e) });
        }
    });
    async function shareSpreadsheetWithEmail(spreadsheetId, email) {
        const drive = googleapis_1.google.drive({ version: "v3", auth: oauth2Client });
        try {
            const resp = await drive.permissions.create({
                fileId: spreadsheetId,
                sendNotificationEmail: false,
                requestBody: {
                    type: "user",
                    role: "writer",
                    emailAddress: email,
                },
            });
            const permId = resp?.data?.id || null;
            console.log("✅ SHARE_TALLY_OK:", { spreadsheetId, email, permId });
            return { ok: true, permId };
        }
        catch (e) {
            const data = e?.response?.data || null;
            const msg = e?.message || String(e);
            console.error("❌ SHARE_TALLY_FAILED:", data || msg);
            return { ok: false, error: data || msg };
        }
    }
    app.get("/debug/share-tally", async (_req, res) => {
        try {
            const tallySheetId = "1ymvx_xWG9pj6guiYaX85IuzgTmcxvTJPrhKdHS6Yl-Y";
            const email = await logAuthedGoogleEmailFromTokens();
            if (!email)
                return res.status(500).json({ ok: false, error: "no_authed_email" });
            const result = await shareSpreadsheetWithEmail(tallySheetId, email);
            return res.json({
                ok: true,
                tallySheetId,
                sharedWith: email,
                result,
            });
        }
        catch (e) {
            return res.status(500).json({ ok: false, error: e?.message || String(e) });
        }
    });
} // end DEBUG_ROUTES_ENABLED
// ===== ROUTES =====
app.use("/upload", upload_1.default);
app.use(intake_1.default);
// ===== HEALTH =====
app.get("/", (_req, res) => res.send("✅ Resume Sorter Backend Running"));
// ===== AUTH =====
app.get("/auth", (_req, res) => {
    const url = oauth2Client.generateAuthUrl({
        access_type: "offline",
        prompt: "consent",
        scope: SCOPES,
    });
    return res.redirect(url);
});
app.get("/auth/google", (_req, res) => {
    const url = oauth2Client.generateAuthUrl({
        access_type: "offline",
        prompt: "consent",
        scope: SCOPES,
    });
    return res.redirect(url);
});
app.get("/auth/callback", async (req, res) => {
    try {
        const code = String(req.query.code || "");
        if (!code)
            return res.status(400).send("Missing ?code=");
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
        return res.send("✅ Google re-auth complete. You can close this tab.");
    }
    catch (err) {
        console.error("❌ /auth/callback error:", err?.message || err);
        return res.status(500).send(`Auth callback failed: ${err?.message || err}`);
    }
});
app.get("/auth/google/callback", async (req, res) => {
    try {
        const code = String(req.query.code || "");
        if (!code)
            return res.status(400).send("Missing ?code=");
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
        return res.send("✅ Google re-auth complete. You can close this tab.");
    }
    catch (err) {
        console.error("❌ /auth/google/callback error:", err?.message || err);
        return res.status(500).send(`Auth callback failed: ${err?.message || err}`);
    }
});
// ============================
// Inbound uploads (multer)
// ============================
//
// Cloud-ready default: memoryStorage (no disk)
// Optional debug: set INBOUND_SAVE_LOCAL=1 to write a local copy, then delete it after R2 upload succeeds.
//
const inboundUpload = (0, multer_1.default)({
    storage: multer_1.default.memoryStorage(),
    limits: { fileSize: 15 * 1024 * 1024 },
});
app.post("/webhooks/inbound-file", inboundUpload.single("file"), async (req, res) => {
    try {
        const f = req.file;
        if (!f)
            return res.status(400).json({ ok: false, error: "no_file" });
        const toEmail = safeStr(req.body?.toEmail || req.body?.to)
            .trim()
            .toLowerCase();
        if (!toEmail)
            return res.status(400).json({ ok: false, error: "missing_toEmail" });
        const bucket = safeStr(process.env.CLOUDFLARE_R2_BUCKET);
        if (!bucket) {
            return res.status(500).json({ ok: false, error: "R2_BUCKET_not_set" });
        }
        const original = safeStr(f.originalname || "upload.bin");
        const safeName = original.replace(/[^a-zA-Z0-9._-]+/g, "_");
        const iso = new Date().toISOString();
        const r2Key = `inbound/${iso.replace(/[:.]/g, "-")}__${safeName}`;
        // Optional local save (debug)
        const shouldSaveLocal = safeStr(process.env.INBOUND_SAVE_LOCAL).toLowerCase() === "1" ||
            safeStr(process.env.INBOUND_SAVE_LOCAL).toLowerCase() === "true";
        let savedLocal = null;
        let deletedLocal = false;
        if (shouldSaveLocal) {
            const dir = path_1.default.join(process.cwd(), "inbound", "uploads");
            fs_1.default.mkdirSync(dir, { recursive: true });
            const finalPath = path_1.default.join(dir, `${iso.replace(/[:.]/g, "-")}__${safeName}`);
            await fs_1.default.promises.writeFile(finalPath, f.buffer);
            savedLocal = finalPath;
            deletedLocal = false;
            console.log("📎 FILE INBOUND saved:", finalPath);
        }
        // Upload to R2 (cloud source of truth)
        let r2 = null;
        try {
            const up = await (0, r2_1.r2UploadBuffer)({
                key: r2Key,
                buffer: f.buffer,
                contentType: safeStr(f.mimetype) || "application/octet-stream",
            });
            r2 = { bucket, key: up?.key || r2Key };
            console.log("☁️ R2 UPLOAD ok:", bucket, r2.key);
        }
        catch (e) {
            console.error("❌ R2_UPLOAD_FAILED:", e?.message || e);
            return res.status(500).json({
                ok: false,
                error: "r2_upload_failed",
                message: String(e?.message || e),
            });
        }
        // Extract + classify
        const extractedText = await extractTextFromBuffer(safeName, f.buffer);
        const docType = extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME";
        // ---- Delete local file after successful R2 upload ----
        if (r2?.key && savedLocal) {
            try {
                await fs_1.default.promises.unlink(savedLocal);
                console.log("🧹 LOCAL_DELETE_OK:", savedLocal);
                deletedLocal = true;
                savedLocal = null;
            }
            catch (e) {
                console.warn("⚠️ LOCAL_DELETE_FAILED:", savedLocal, e?.message || e);
            }
        }
        else {
            // If we never saved locally, treat as already "deleted"
            deletedLocal = true;
        }
        const result = await processInboundDoc({
            filename: safeName,
            buffer: f.buffer,
            extractedText,
            docType,
            toEmail,
            // cost control: only treat as "r2 doc" when it's a resume
            r2: docType === "RESUME" ? r2 : null,
            savedLocal,
            deletedLocal,
        });
        return res.json(result);
    }
    catch (e) {
        console.error("❌ inbound-file error", e);
        return res.status(500).json({
            ok: false,
            error: "inbound_file_failed",
            message: String(e?.message || e),
        });
    }
});
// Cloudflare Worker (or anything) can call this with JSON: { key, toEmail? }
app.post("/webhooks/inbound-r2", express_1.default.json(), async (req, res) => {
    try {
        // Optional: simple shared-secret auth (recommended)
        const secret = req.header("X-Inbound-Secret");
        if (process.env.INBOUND_WEBHOOK_SECRET) {
            if (!secret || secret !== process.env.INBOUND_WEBHOOK_SECRET) {
                return res.status(401).json({ ok: false, error: "unauthorized" });
            }
        }
        const key = safeStr(req.body?.key);
        const toEmail = safeStr(req.body?.toEmail || req.body?.to).trim().toLowerCase();
        if (!key)
            return res.status(400).json({ ok: false, error: "missing_key" });
        if (!toEmail)
            return res.status(400).json({ ok: false, error: "missing_toEmail" });
        if (!process.env.CLOUDFLARE_R2_BUCKET) {
            return res.status(500).json({ ok: false, error: "R2_BUCKET_not_set" });
        }
        const buffer = await (0, r2_1.r2DownloadToBuffer)({ key });
        if (!buffer)
            return res.status(404).json({ ok: false, error: "object_not_found" });
        // key example: inbound/2026-02-12T22-18-20-441Z__test.txt
        const filename = safeBaseName(key).replace(/^\d{4}-\d{2}-\d{2}T.*?Z__/, "");
        const extractedText = await extractTextFromBuffer(filename, buffer);
        const docType = extractedText.trim().length > 0 ? classifyDocTypeFromText(extractedText) : "NON_RESUME";
        const result = await processInboundDoc({
            filename,
            buffer,
            extractedText,
            docType,
            toEmail,
            // cost control: only treat as "r2 doc" when it's a resume
            r2: docType === "RESUME" ? { bucket: safeStr(process.env.CLOUDFLARE_R2_BUCKET), key } : null,
            savedLocal: null,
            deletedLocal: true, // cloud-only path
        });
        return res.json(result);
    }
    catch (e) {
        console.error("❌ inbound-r2 error", e);
        return res.status(500).json({
            ok: false,
            error: "inbound_r2_failed",
            message: String(e?.message || e),
        });
    }
});
// ============================
// Nightly job (trial status updates)
// ============================
async function runNightlyJob() {
    const nowUtc = new Date();
    const nowLocal = (0, date_fns_tz_1.toZonedTime)(nowUtc, TIMEZONE);
    const todayLocalISO = (0, date_fns_1.formatISO)(nowLocal, { representation: "date" });
    const cutoffLocal = nowLocal;
    const soonLocal = (0, date_fns_1.addDays)(nowLocal, 3);
    const cutoffUtc = (0, date_fns_tz_1.fromZonedTime)(cutoffLocal, TIMEZONE);
    const soonUtc = (0, date_fns_tz_1.fromZonedTime)(soonLocal, TIMEZONE);
    const customers = await readCustomersFromMasterSheet();
    const lines = [];
    lines.push(`Nightly run: ${todayLocalISO} (${TIMEZONE})`);
    lines.push(`Customers checked: ${customers.length}`);
    lines.push("");
    for (const c of customers) {
        const trialEnds = parseISODate(c.trialEndsAtISO);
        const status = (c.status || "").toLowerCase();
        if (!trialEnds)
            continue;
        const isTrial = status === "trial" || status === "trialing";
        const isEnded = isTrial && trialEnds <= cutoffUtc;
        const isEndingSoon = isTrial && trialEnds > cutoffUtc && trialEnds <= soonUtc;
        if (isEnded) {
            await updateCustomerStatusInMasterSheet(c.customerId, "trial_ended");
            lines.push(`TRIAL ENDED: ${c.companyName} (${c.customerId}) -> trial_ended`);
        }
        else if (isEndingSoon) {
            lines.push(`TRIAL ENDING SOON: ${c.companyName} ends ${c.trialEndsAtISO}`);
        }
    }
    const report = lines.join("\n");
    await sendAdmin(`Resume Sorter Nightly (${todayLocalISO})`, report);
    return { ok: true, date: todayLocalISO, report };
}
app.get("/admin/nightly-run", async (_req, res) => {
    try {
        const result = await runNightlyJob();
        return res.json(result);
    }
    catch (e) {
        return res.status(500).json({ ok: false, error: e?.message || String(e) });
    }
});
node_cron_1.default.schedule(NIGHTLY_CRON, async () => {
    try {
        const nowLocal = (0, date_fns_tz_1.toZonedTime)(new Date(), TIMEZONE);
        console.log(`🕑 Nightly cron fired at ${(0, date_fns_1.formatISO)(nowLocal)}`);
        const result = await runNightlyJob();
        console.log("✅ Nightly job complete");
        console.log(result.report);
    }
    catch (e) {
        console.log("❌ Nightly job failed:", e);
    }
}, { timezone: TIMEZONE });
console.log(`✅ Nightly scheduler started: "${NIGHTLY_CRON}" (${TIMEZONE})`);
// ============================
// Start server
// ============================
app.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
});
