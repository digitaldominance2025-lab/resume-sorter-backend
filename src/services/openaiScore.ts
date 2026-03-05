import dotenv from "dotenv";
dotenv.config();

import OpenAI from "openai";

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

export async function scoreResume(text: string, rubric: any | null) {
  try {
    const rubricBlock =
      rubric && typeof rubric === "object" && Object.keys(rubric).length > 0
        ? `
Customer scoring rubric (use this when scoring):
${JSON.stringify(rubric, null, 2)}
`
        : "";

    const prompt = `
You are a resume screening assistant.${rubricBlock ? "\n" + rubricBlock : ""}

Return JSON only in this format:
{
  "score": number (0-100),
  "summary": string,
  "strengths": string[],
  "weaknesses": string[]
}

Scoring rules:
- If a rubric is provided, prioritize it over generic resume advice.
- Use the rubric to decide what counts as strengths/weaknesses.
- Keep the summary short and practical.

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