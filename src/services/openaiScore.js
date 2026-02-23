"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.scoreResume = scoreResume;
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const openai_1 = __importDefault(require("openai"));
const client = new openai_1.default({
    apiKey: process.env.OPENAI_API_KEY,
});
async function scoreResume(text) {
    try {
        const prompt = `
You are a resume screening assistant.

Return JSON only in this format:
{
  "score": number (0-100),
  "summary": string,
  "strengths": string[],
  "weaknesses": string[]
}

Resume:
"""
${text}
"""
`;
        const resp = await client.chat.completions.create({
            model: "gpt-4.1-mini",
            messages: [{ role: "user", content: prompt }],
            temperature: 0.2,
        });
        const raw = resp.choices[0].message?.content || "{}";
        try {
            return JSON.parse(raw);
        }
        catch {
            return { error: "parse_failed", raw };
        }
    }
    catch (e) {
        const status = e?.status || e?.response?.status;
        const message = e?.message || String(e);
        if (status === 429)
            return { error: "quota_exceeded", message };
        if (status === 401)
            return { error: "bad_api_key", message };
        return { error: "openai_error", status, message };
    }
}
