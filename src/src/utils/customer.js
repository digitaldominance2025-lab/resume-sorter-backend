"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.slugifyCompanyName = slugifyCompanyName;
exports.addDays = addDays;
exports.iso = iso;
function slugifyCompanyName(name) {
    return name
        .toLowerCase()
        .trim()
        .replace(/&/g, "and")
        .replace(/['"]/g, "")
        .replace(/[^a-z0-9]+/g, "") // remove spaces/punct
        .slice(0, 32);
}
function addDays(date, days) {
    const d = new Date(date);
    if (Number.isNaN(d.getTime()))
        throw new Error("addDays: invalid base date");
    d.setDate(d.getDate() + days);
    return d;
}
function iso(date) {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
        throw new Error("iso: invalid date");
    }
    return date.toISOString();
}
