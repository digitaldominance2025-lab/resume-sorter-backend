// =======================================================
// CUSTOMER UTILITIES
// =======================================================
import { getPublicR2Url } from "../services/r2";

export function safeStr(val: any): string {
  if (val === null || val === undefined) return "";
  return String(val).trim();
}

export function makeSlug(name: string): string {
  return safeStr(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)+/g, "");
}

export function makeCustomerId(slug: string): string {
  const rand = Math.random().toString(36).slice(2, 8);
  return `${slug}_${rand}`;
}

export function addDays(date: Date, days: number): Date {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

export function iso(date: Date): string {
  return date.toISOString();
}

export {};
