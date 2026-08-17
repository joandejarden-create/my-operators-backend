#!/usr/bin/env node
/**
 * Steward fixes: wrong Affiliation values + Bimini (left Hilton system).
 *
 *   node scripts/apply-census-steward-fixes.mjs
 *   node scripts/apply-census-steward-fixes.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** @type {{ recordId: string, name: string, fields: Record<string, unknown>, reason: string }[]} */
const PATCHES = [
  {
    recordId: "recrVkCsIjx0hjr3K",
    name: "Dreams Rose Hall Resort & Spa",
    reason: "Dreams/Hyatt brand — not Hilton",
    fields: {
      [CENSUS_FIELDS.affiliation]: "Dreams Resorts & Spas",
      "Parent Company": "Hyatt Hotels Corporation",
    },
  },
  {
    recordId: "rechTFNeAAYgr9mms",
    name: "Hyatt Vivid Playa del Carmen",
    reason: "Hyatt Vivid brand — not Hilton",
    fields: {
      [CENSUS_FIELDS.affiliation]: "Hyatt Vivid",
      "Parent Company": "Hyatt Hotels Corporation",
    },
  },
  {
    recordId: "recx0OvNOBmyS6SUP",
    name: "Fiesta Inn Express Cancun Cumbres",
    reason: "Fiesta Inn brand — not Hampton/Hilton",
    fields: {
      [CENSUS_FIELDS.affiliation]: "Fiesta Inn",
      "Parent Company": "Grupo Posadas, S.A.B. DE C.V.",
    },
  },
  {
    recordId: "reck0OX02AXOCxrxh",
    name: "Hilton at Resorts World Bimini",
    reason: "Left Hilton system Apr 2025 — now independent Resorts World Bimini",
    fields: {
      [CENSUS_FIELDS.affiliation]: "Independent",
      "Parent Company": null,
      Website: "https://rwbimini.com/hotel/rooms-suites/",
      [CENSUS_DESCRIPTION_FIELD]:
        "Resorts World Bimini blends Bahamian island charm with modern guest rooms and suites on North Bimini. Accommodations feature hardwood floors, WiFi, and marble bathrooms, with floor-to-ceiling windows across the beachfront resort. The complex includes dining venues, Serenity Spa, pools, marina access, and casino entertainment on a 750-acre destination resort.",
    },
  },
];

async function main() {
  const apply = process.argv.includes("--apply");
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log("=== Census steward affiliation fixes ===\n");
  for (const p of PATCHES) {
    console.log(`  ${p.name}`);
    console.log(`    ${p.reason}`);
    console.log(`    → ${JSON.stringify(p.fields)}`);
  }

  if (!apply) {
    console.log("\nDry run. Run with --apply to write to Airtable.");
    return;
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const updated = await base(HOTEL_CENSUS_TABLE).update(
    PATCHES.map((p) => ({ id: p.recordId, fields: p.fields })),
    { typecast: true }
  );

  const logPath = join(__dirname, "..", "reports", "census-steward-affiliation-fixes-log.csv");
  mkdirSync(dirname(logPath), { recursive: true });
  writeFileSync(
    logPath,
    `recordId,name,reason,fields\n${PATCHES.map((p) =>
      [p.recordId, `"${p.name}"`, `"${p.reason}"`, `"${Object.keys(p.fields).join(";")}"`].join(",")
    ).join("\n")}\n`
  );

  console.log(`\nUpdated: ${updated.length}`);
  console.log("Log:", logPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
