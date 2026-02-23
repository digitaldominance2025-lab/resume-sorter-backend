import { google } from "googleapis";

type CreateTallyArgs = {
  companyName: string;
  slug: string;
  customerId: string;
  trialEndsAtISO: string;
};

export async function createCustomerTallySpreadsheet(oauth2Client: any, args: any) {
  console.log("DEBUG createCustomerTallySpreadsheet args:", args);

  if (!args || typeof args !== "object") {
    throw new Error("createCustomerTallySpreadsheet: args is undefined or not an object");
  }

  const companyName = String(args.companyName || "").trim();
  const slug = String(args.slug || "").trim();
  const customerId = String(args.customerId || "").trim();
  const trialEndsAtISO = String(args.trialEndsAtISO || "").trim();

  if (!companyName) throw new Error("createCustomerTallySpreadsheet: companyName missing");
  if (!slug) throw new Error("createCustomerTallySpreadsheet: slug missing");
  if (!customerId) throw new Error("createCustomerTallySpreadsheet: customerId missing");
  if (!trialEndsAtISO) throw new Error("createCustomerTallySpreadsheet: trialEndsAtISO missing");

  const sheets = google.sheets({ version: "v4", auth: oauth2Client });
  const title = `Tally - ${companyName} (${slug})`;

  const createResp = await sheets.spreadsheets.create({
    requestBody: { properties: { title } },
  });

  const spreadsheetId = createResp.data.spreadsheetId;
  if (!spreadsheetId) throw new Error("Failed to create tally spreadsheet");

  await sheets.spreadsheets.values.update({
  spreadsheetId,
  range: "Sheet1!A1:E1",
  valueInputOption: "RAW",
  requestBody: { values: [["date", "resumesProcessed", "notes", "r2Key", "resumeFile"]] },
});

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: "Sheet1!G1:H5",
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

export async function createCustomerTally(oauth2Client: any, args: any) {
  console.log("DEBUG createCustomerTally args:", args);
  return createCustomerTallySpreadsheet(oauth2Client, args);
}
