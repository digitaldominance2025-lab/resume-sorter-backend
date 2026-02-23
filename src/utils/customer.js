"use strict";
// =======================================================
// CUSTOMER UTILITIES
// =======================================================
Object.defineProperty(exports, "__esModule", { value: true });
exports.safeStr = safeStr;
exports.makeSlug = makeSlug;
exports.makeCustomerId = makeCustomerId;
exports.addDays = addDays;
exports.iso = iso;
function safeStr(val) {
    if (val === null || val === undefined)
        return "";
    return String(val).trim();
}
function makeSlug(name) {
    return safeStr(name)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)+/g, "");
}
function makeCustomerId(slug) {
    const rand = Math.random().toString(36).slice(2, 8);
    return `${slug}_${rand}`;
}
function addDays(date, days) {
    const d = new Date(date);
    d.setDate(d.getDate() + days);
    return d;
}
function iso(date) {
    return date.toISOString();
}
