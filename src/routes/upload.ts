import pool from "../db";

import express from "express";
import multer from "multer";
import mammoth from "mammoth";
import fs from "fs";

// pdf-parse can be CommonJS; require avoids TS "not callable" issues
const pdfParse = require("pdf-parse");

const router = express.Router();

// temporary file storage
const upload = multer({ dest: "uploads/" });

router.post("/", upload.single("resume"), async (req, res) => {
  if (!req.file) {
    return res.status(400).send("No file uploaded");
  }

  const filePath = req.file.path;
  let text = "";

  try {
    // PDF resumes
    if (req.file.mimetype === "application/pdf") {
      const data = await pdfParse(fs.readFileSync(filePath));
      text = data.text;
    }
    // DOCX resumes
    else if (
      req.file.mimetype ===
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ) {
      const result = await mammoth.extractRawText({ path: filePath });
      text = result.value;
    } else {
      return res.status(400).send("Unsupported file type");
    }

    const inserted = await pool.query(
  "INSERT INTO resume_results (filename, score, resume_text) VALUES ($1, $2, $3) RETURNING *",
  [req.file.originalname, null, text]
);

res.json({ saved: inserted.rows[0] });

  } catch (error) {
    console.error(error);
    res.status(500).send("Failed to parse resume");
  } finally {
    // clean up temp file
    fs.unlinkSync(filePath);
  }
});

export default router;
