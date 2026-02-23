
import { Router } from "express";
import fs from "fs";
import pool from "../db";
import * as pdfParse from "pdf-parse";
import mammoth from "mammoth";

const router = Router();

// Ensure base folders exist
if (!fs.existsSync("intake")) {
  fs.mkdirSync("intake");
}
if (!fs.existsSync("intake/files")) {
  fs.mkdirSync("intake/files", { recursive: true });
}
 async function extractTextFromFile(path: string): Promise<string> {
  const lower = path.toLowerCase();

  if (lower.endsWith(".txt")) {
    return fs.readFileSync(path, "utf8");
  }

  if (lower.endsWith(".pdf")) {
    const buffer = fs.readFileSync(path);
    const data = await (pdfParse as any)(buffer);

    return data.text || "";
  }

  if (lower.endsWith(".docx")) {
    const result = await mammoth.extractRawText({ path });
    return result.value || "";
  }

  return "";
}


// TEMP intake endpoint (webhook receiver)
router.post("/intake/email", async (req, res) => {
  console.log("📥 INTAKE EMAIL RECEIVED");
  console.log("TIME:", new Date().toISOString());

  // Build payload first (so we can add savedFiles to it)
  const payload: {
    receivedAt: string;
    body: any;
    savedFiles: string[];
  } = {
    receivedAt: new Date().toISOString(),
    body: req.body,
    savedFiles: [],
  };

  // Save attachments if present
  try {
    const attachments = req.body?.attachments;

    if (Array.isArray(attachments)) {
      for (const file of attachments) {
        if (!file?.filename || !file?.contentBase64) continue;

        const buffer = Buffer.from(file.contentBase64, "base64");
        const safeName = `${Date.now()}-${file.filename}`;
        const relPath = `intake/files/${safeName}`;

        fs.writeFileSync(relPath, buffer);
        payload.savedFiles.push(relPath);

        console.log("Saved attachment:", safeName);
      }
    }
  } catch (err) {
    console.error("Attachment save failed:", err);
  }

  // Save to Postgres
  const extractedText: Record<string, string> = {};
for (const p of payload.savedFiles) {
  extractedText[p] = await extractTextFromFile(p);
}
(payload as any).extractedText = extractedText;


  await pool.query(
    "INSERT INTO intake_emails (received_at, payload) VALUES ($1, $2)",
    [new Date(payload.receivedAt), payload]
  );

  console.log("Saved intake to Postgres");

  // Save to local JSON
  fs.writeFileSync(
    `intake/intake-${Date.now()}.json`,
    JSON.stringify(payload, null, 2)
  );
  console.log("Saved intake file");

  res.json({ ok: true, savedFiles: payload.savedFiles });
});

export default router;
