// src/services/email.ts
import { Resend } from "resend";

const apiKey = process.env.RESEND_API_KEY;

let resend: Resend | null = null;
if (apiKey && apiKey.startsWith("re_")) {
  resend = new Resend(apiKey);
} else {
  console.warn("⚠️ RESEND_API_KEY missing/invalid. Email sending disabled until set.");
}

const from = process.env.EMAIL_FROM || "Digital Dominance <onboarding@resend.dev>";
const admin = process.env.ADMIN_REPORT_EMAIL || process.env.ADMIN_EMAIL || "";

/** Sends an email to your admin/report address (ADMIN_REPORT_EMAIL or ADMIN_EMAIL). */
export async function sendAdminEmail(subject: string, text: string) {
  if (!resend) throw new Error("Email disabled: RESEND_API_KEY missing/invalid.");
  if (!admin) throw new Error("ADMIN_REPORT_EMAIL (or ADMIN_EMAIL) missing in .env.");

  await resend.emails.send({
    from,
    to: admin,
    subject,
    text,
  });
}

/** Sends an email to an arbitrary recipient (customer). */
export async function sendCustomerTextEmail(args: { to: string; subject: string; text: string }) {
  if (!resend) throw new Error("Email disabled: RESEND_API_KEY missing/invalid.");

  const to = (args?.to || "").trim();
  if (!to) throw new Error("Missing recipient email (to).");

  await resend.emails.send({
    from,
    to,
    subject: args.subject,
    text: args.text,
  });
}

/** Alias so server code can call a common name */
export const sendTextEmail = sendCustomerTextEmail;
