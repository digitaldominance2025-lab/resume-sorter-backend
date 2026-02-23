"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const db_1 = __importDefault(require("../db"));
const express_1 = __importDefault(require("express"));
const multer_1 = __importDefault(require("multer"));
const mammoth_1 = __importDefault(require("mammoth"));
const fs_1 = __importDefault(require("fs"));
// pdf-parse can be CommonJS; require avoids TS "not callable" issues
const pdfParse = require("pdf-parse");
const router = express_1.default.Router();
// temporary file storage
const upload = (0, multer_1.default)({ dest: "uploads/" });
router.post("/", upload.single("resume"), async (req, res) => {
    if (!req.file) {
        return res.status(400).send("No file uploaded");
    }
    const filePath = req.file.path;
    let text = "";
    try {
        // PDF resumes
        if (req.file.mimetype === "application/pdf") {
            const data = await pdfParse(fs_1.default.readFileSync(filePath));
            text = data.text;
        }
        // DOCX resumes
        else if (req.file.mimetype ===
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document") {
            const result = await mammoth_1.default.extractRawText({ path: filePath });
            text = result.value;
        }
        else {
            return res.status(400).send("Unsupported file type");
        }
        const inserted = await db_1.default.query("INSERT INTO resume_results (filename, score, resume_text) VALUES ($1, $2, $3) RETURNING *", [req.file.originalname, null, text]);
        res.json({ saved: inserted.rows[0] });
    }
    catch (error) {
        console.error(error);
        res.status(500).send("Failed to parse resume");
    }
    finally {
        // clean up temp file
        fs_1.default.unlinkSync(filePath);
    }
});
exports.default = router;
