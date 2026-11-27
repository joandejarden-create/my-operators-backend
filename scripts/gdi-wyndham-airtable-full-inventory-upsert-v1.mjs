#!/usr/bin/env node
/**
 * Upsert all Wyndham STAGED opportunities into GDO Airtable (idempotent).
 * Fills staged IDs missing from Airtable so reconstruction can start from GDO.
 *
 *   node scripts/gdi-wyndham-airtable-full-inventory-upsert-v1.mjs --dry-run
 *   node scripts/gdi-wyndham-airtable-full-inventory-upsert-v1.mjs --apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const AUTH = "FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127";
const OUT = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-gold-standard-certification-v1",
);

function flag(n) {
  return process.argv.includes(n);
}
function arg(n, fb = null) {
  const i = process.argv.indexOf(n);
  return i >= 0 ? process.argv[i + 1] || fb : fb;
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
const { listOpportunitiesForHotel, upsertOpportunitiesBatch } = await import(
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
const pkg = JSON.parse(
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
const stagedOpps = (pkg.opportunities || []).map((o) => ({
  ...o,
  hotelId: HOTEL_ID,
  id: o.id || o.opportunityId,
}));

const existing = await listOpportunitiesForHotel(HOTEL_ID);
const have = new Set(existing.map((o) => o.id || o.opportunityId));
const missing = stagedOpps.filter((o) => !have.has(o.id));
const report = {
  stagedVersion,
  stagedCount: stagedOpps.length,
  airtableBefore: existing.length,
  missingCount: missing.length,
  missingIds: missing.map((o) => o.id),
  dryRun: !flag("--apply"),
};
console.log(JSON.stringify(report, null, 2));

if (!flag("--apply")) {
  fs.writeFileSync(
    path.join(OUT, "23-AIRTABLE_FULL_INVENTORY_UPSERT_DRY_RUN.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  process.exit(0);
}
if (arg("--auth") !== AUTH) {
  console.error(`--apply requires --auth ${AUTH}`);
  process.exit(2);
}

const result = await upsertOpportunitiesBatch(HOTEL_ID, stagedOpps, {
  researchVersion: "gold-standard-full-inventory-upsert-v1",
  runId: `gold_std_full_upsert_${Date.now()}`,
  preserveSalesOwned: true,
});
const after = await listOpportunitiesForHotel(HOTEL_ID);
const afterIds = new Set(after.map((o) => o.id || o.opportunityId));
const stillMissing = stagedOpps.filter((o) => !afterIds.has(o.id)).map((o) => o.id);
const out = {
  ...report,
  dryRun: false,
  upsertResult: {
    ok: result?.ok ?? null,
    written: result?.written ?? result?.upserted ?? null,
    errors: result?.errors?.length ?? 0,
  },
  airtableAfter: after.length,
  stillMissing,
  complete: stillMissing.length === 0,
};
fs.writeFileSync(
  path.join(OUT, "23-AIRTABLE_FULL_INVENTORY_UPSERT.json"),
  `${JSON.stringify(out, null, 2)}\n`,
);
console.log(JSON.stringify(out, null, 2));
