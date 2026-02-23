export function slugifyCompanyName(name: string): string {
  return name
    .toLowerCase()
    .trim()
    .replace(/&/g, "and")
    .replace(/['"]/g, "")
    .replace(/[^a-z0-9]+/g, "") // remove spaces/punct
    .slice(0, 32);
}

export function addDays(date: Date, days: number): Date {
  const d = new Date(date);
  if (Number.isNaN(d.getTime())) throw new Error("addDays: invalid base date");
  d.setDate(d.getDate() + days);
  return d;
}

export function iso(date: Date): string {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error("iso: invalid date");
  }
  return date.toISOString();
}

