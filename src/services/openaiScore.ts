import dotenv from "dotenv";
dotenv.config();

import OpenAI from "openai";

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

export async function scoreResume(text: string) {
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
    } catch {
      return { error: "parse_failed", raw };
    }
  } catch (e: any) {
    const status = e?.status || e?.response?.status;
    const message = e?.message || String(e);

    if (status === 429) return { error: "quota_exceeded", message };
    if (status === 401) return { error: "bad_api_key", message };

    return { error: "openai_error", status, message };
  }
}
