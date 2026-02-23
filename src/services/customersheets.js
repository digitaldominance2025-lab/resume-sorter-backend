"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createCustomerTallySpreadsheet = createCustomerTallySpreadsheet;
exports.createCustomerTally = createCustomerTally;
const googleapis_1 = require("googleapis");
async function createCustomerTallySpreadsheet(oauth2Client, args) {
    console.log("DEBUG createCustomerTallySpreadsheet args:", args);
    if (!args || typeof args !== "object") {
        throw new Error("createCustomerTallySpreadsheet: args is undefined or not an object");
    }
    const companyName = String(args.companyName || "").trim();
    const slug = String(args.slug || "").trim();
    const customerId = String(args.customerId || "").trim();
    const trialEndsAtISO = String(args.trialEndsAtISO || "").trim();
    if (!companyName)
        throw new Error("createCustomerTallySpreadsheet: companyName missing");
    if (!slug)
        throw new Error("createCustomerTallySpreadsheet: slug missing");
    if (!customerId)
        throw new Error("createCustomerTallySpreadsheet: customerId missing");
    if (!trialEndsAtISO)
        throw new Error("createCustomerTallySpreadsheet: trialEndsAtISO missing");
    const sheets = googleapis_1.google.sheets({ version: "v4", auth: oauth2Client });
    const title = `Tally - ${companyName} (${slug})`;
    const createResp = await sheets.spreadsheets.create({
        requestBody: { properties: { title } },
    });
    const spreadsheetId = createResp.data.spreadsheetId;
    if (!spreadsheetId)
        throw new Error("Failed to create tally spreadsheet");
    await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: "Sheet1!A1:C1",
        valueInputOption: "RAW",
        requestBody: { values: [["date", "resumesProcessed", "notes"]] },
    });
    await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: "Sheet1!E1:F5",
        valueInputOption: "RAW",
        requestBody: {
            values: [
                ["customerId", customerId],
                ["companyName", companyName],
                ["slug", slug],
                ["trialEndsAtISO", trialEndsAtISO],
                ["createdAtISO", new Date().toISOString()],
            ],
        },
    });
    return {
        spreadsheetId,
        spreadsheetUrl: `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit`,
    };
}
async function createCustomerTally(oauth2Client, args) {
    console.log("DEBUG createCustomerTally args:", args);
    return createCustomerTallySpreadsheet(oauth2Client, args);
}
