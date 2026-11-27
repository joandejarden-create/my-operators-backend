#!/usr/bin/env node
/**
 * Isolated non-publishing reconstruction parity: Airtable GDO → compare to STAGED.
 * Does NOT use STAGED as the reconstruction source — only as the comparison target.
 * Does NOT overwrite packages.
 *
 *   node scripts/gdi-airtable-reconstruction-parity-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const OUT = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-gold-standard-certification-v1",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadEnv() {
  for (const envPath of [
    path.join(ROOT, ".env"),
    "C:/Dev/dealality/my-operators-backend/.env",
  ]) {
    if (!fs.existsSync(envPath)) continue;
    for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
      const m = line.match(
        /^(AIRTABLE_API_KEY|AIRTABLE_PAT|AIRTABLE_BASE_ID|AIRTABLE_GDI_BASE_ID)=(.*)$/,
      );
      if (!m) continue;
      if (!process.env[m[1]]) {
        process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
      }
    }
  }
}

loadEnv();
const { listOpportunitiesForHotel } = await import(
  pathToFileURL(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/airtable-opportunity-store.js",
    ),
  ).href
);

const pointer = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL_ID,
      "STAGED.json",
    ),
    "utf8",
  ),
);
const stagedVersion = pointer.publicationVersion;
const stagedPkg = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL_ID,
      stagedVersion,
      "package.json",
    ),
    "utf8",
  ),
);
const stagedOpps = stagedPkg.opportunities || [];
const stagedById = new Map(stagedOpps.map((o) => [o.id || o.opportunityId, o]));

const airtableOpps = await listOpportunitiesForHotel(HOTEL_ID);
const atById = new Map(
  (airtableOpps || []).map((o) => [o.id || o.opportunityId, o]),
);

const stagedIds = [...stagedById.keys()];
const presentInAirtable = stagedIds.filter((id) => atById.has(id));
const missingInAirtable = stagedIds.filter((id) => !atById.has(id));

const fieldParity = [];
for (const id of presentInAirtable) {
  const s = stagedById.get(id);
  const a = atById.get(id);
  const sEmail = text(s.primaryContact?.email);
  const aEmail = text(a.primaryContactEmail || a.primaryContact?.email);
  const sName = text(s.primaryContact?.name);
  const aName = text(a.primaryContactName || a.primaryContact?.name);
  const sPhone = text(s.primaryContact?.phone);
  const aPhone = text(a.primaryContactPhone || a.primaryContact?.phone);
  fieldParity.push({
    opportunityId: id,
    nameMatch: !sName || sName.toLowerCase() === aName.toLowerCase(),
    emailMatch: !sEmail || sEmail.toLowerCase() === aEmail.toLowerCase(),
    phoneMatch: !sPhone || sPhone === aPhone,
    stagedEmail: sEmail || null,
    airtableEmail: aEmail || null,
  });
}

const emailMismatches = fieldParity.filter((r) => !r.emailMatch);
const report = {
  schema: "gdi-airtable-reconstruction-parity-v1",
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL_ID,
  comparisonTargetStagedVersion: stagedVersion,
  reconstructionSource: "AIRTABLE_GDO_listOpportunitiesForHotel",
  usedStagedAsSource: false,
  packageOverwrite: false,
  airtableOpportunityCount: airtableOpps.length,
  stagedOpportunityCount: stagedOpps.length,
  stagedIdsPresentInAirtable: presentInAirtable.length,
  stagedIdsMissingInAirtable: missingInAirtable.length,
  missingInAirtableSample: missingInAirtable.slice(0, 20),
  emailFieldMismatches: emailMismatches.length,
  emailMismatchSample: emailMismatches.slice(0, 10),
  canRebuildFullStagedInventoryFromAirtableAlone:
    missingInAirtable.length === 0 && stagedOpps.length > 0,
  status:
    missingInAirtable.length === 0 && emailMismatches.length === 0
      ? "AIRTABLE_ID_AND_EMAIL_PARITY_PASS"
      : "AIRTABLE_RECONSTRUCTION_INCOMPLETE",
  note: "Full package regeneration (drawer models, ES receipt, outlook) still requires research inventory + publication builder — this certifies GDO contact/id recoverability only.",
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "22-AIRTABLE_RECONSTRUCTION_PARITY.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
console.log(JSON.stringify(report, null, 2));
