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
const express_1 = require("express");
const fs_1 = __importDefault(require("fs"));
const db_1 = __importDefault(require("../db"));
const pdfParse = __importStar(require("pdf-parse"));
const mammoth_1 = __importDefault(require("mammoth"));
const router = (0, express_1.Router)();
// Ensure base folders exist
if (!fs_1.default.existsSync("intake")) {
    fs_1.default.mkdirSync("intake");
}
if (!fs_1.default.existsSync("intake/files")) {
    fs_1.default.mkdirSync("intake/files", { recursive: true });
}
async function extractTextFromFile(path) {
    const lower = path.toLowerCase();
    if (lower.endsWith(".txt")) {
        return fs_1.default.readFileSync(path, "utf8");
    }
    if (lower.endsWith(".pdf")) {
        const buffer = fs_1.default.readFileSync(path);
        const data = await pdfParse(buffer);
        return data.text || "";
    }
    if (lower.endsWith(".docx")) {
        const result = await mammoth_1.default.extractRawText({ path });
        return result.value || "";
    }
    return "";
}
// TEMP intake endpoint (webhook receiver)
router.post("/intake/email", async (req, res) => {
    console.log("📥 INTAKE EMAIL RECEIVED");
    console.log("TIME:", new Date().toISOString());
    // Build payload first (so we can add savedFiles to it)
    const payload = {
        receivedAt: new Date().toISOString(),
        body: req.body,
        savedFiles: [],
    };
    // Save attachments if present
    try {
        const attachments = req.body?.attachments;
        if (Array.isArray(attachments)) {
            for (const file of attachments) {
                if (!file?.filename || !file?.contentBase64)
                    continue;
                const buffer = Buffer.from(file.contentBase64, "base64");
                const safeName = `${Date.now()}-${file.filename}`;
                const relPath = `intake/files/${safeName}`;
                fs_1.default.writeFileSync(relPath, buffer);
                payload.savedFiles.push(relPath);
                console.log("Saved attachment:", safeName);
            }
        }
    }
    catch (err) {
        console.error("Attachment save failed:", err);
    }
    // Save to Postgres
    const extractedText = {};
    for (const p of payload.savedFiles) {
        extractedText[p] = await extractTextFromFile(p);
    }
    payload.extractedText = extractedText;
    await db_1.default.query("INSERT INTO intake_emails (received_at, payload) VALUES ($1, $2)", [new Date(payload.receivedAt), payload]);
    console.log("Saved intake to Postgres");
    // Save to local JSON
    fs_1.default.writeFileSync(`intake/intake-${Date.now()}.json`, JSON.stringify(payload, null, 2));
    console.log("Saved intake file");
    res.json({ ok: true, savedFiles: payload.savedFiles });
});
exports.default = router;
