// src/services/email.ts
import { Resend } from "resend";

const apiKey = process.env.RESEND_API_KEY;

let resend: Resend | null = null;
if (apiKey && apiKey.startsWith("re_")) {
  resend = new Resend(apiKey);
} else {
  console.warn("⚠️ RESEND_API_KEY missing/invalid. Email sending disabled until set.");
}

const from = process.env.EMAIL_FROM || "EasyPaper <onboarding@resend.dev>";
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
 console.log("📧 SEND_CUSTOMER_TEXT_EMAIL_START", {
    hasResend: !!resend,
    to: (args?.to || "").trim(),
    subject: args?.subject,
    from,
    hasApiKey: !!apiKey
  });
  if (!resend) throw new Error("Email disabled: RESEND_API_KEY missing/invalid.");

  const to = (args?.to || "").trim();
  if (!to) throw new Error("Missing recipient email (to).");

const result = await resend.emails.send({
    from,
    to,
    subject: args.subject,
    text: args.text,
  });

  console.log("📧 RESEND_RESULT", result);
}

/** Alias so server code can call a common name */
export const sendTextEmail = sendCustomerTextEmail;
/** Sends an HTML email to a customer */
export async function sendCustomerHtmlEmail(args: { to: string; subject: string; html: string }) {
  console.log("📧 SEND_CUSTOMER_HTML_EMAIL_START", {
    hasResend: !!resend,
    to: (args?.to || "").trim(),
    subject: args?.subject,
    from,
    hasApiKey: !!apiKey
  });

  if (!resend) throw new Error("Email disabled: RESEND_API_KEY missing/invalid.");

  const to = (args?.to || "").trim();
  if (!to) throw new Error("Missing recipient email (to).");

  const result = await resend.emails.send({
    from,
    to,
    subject: args.subject,
    html: args.html,
  });

  console.log("📧 RESEND_HTML_RESULT", result);
}

/** Alias so server code can call a common name */
export const sendHtmlEmail = sendCustomerHtmlEmail;