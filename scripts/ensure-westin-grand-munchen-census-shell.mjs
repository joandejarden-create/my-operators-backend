#!/usr/bin/env node
/**
 * Ensure Hotel Property Census shell for The Westin Grand München (ALT base).
 *
 *   node scripts/ensure-westin-grand-munchen-census-shell.mjs --dry-run
 *   node scripts/ensure-westin-grand-munchen-census-shell.mjs --apply
 */
import "dotenv/config";
import Airtable from "airtable";

const APPLY = process.argv.includes("--apply");
const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (!key || !baseId) {
  console.error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
  process.exit(1);
}

const base = new Airtable({ apiKey: key }).base(baseId);
const TABLE = "Hotel Property Census";

const PAYLOAD = {
  "Property Name": "The Westin Grand München",
  "Canonical Property Name": "The Westin Grand München",
  City: "Munich",
  Country: "Germany",
  Continent: "Europe",
  Market: "Munich",
  Submarket: "Bogenhausen / Arabellapark",
  "State / Region": "Bavaria",
  "Postal Code": "81925",
  Address: "Arabellastraße 6, 81925 Munich, Germany",
  Latitude: 48.1511,
  Longitude: 11.6197,
  "Rooms / Keys": 627,
  "Rooms Confidence": "High",
  "Rooms Source Type": "Official / Brand",
  "Rooms Source URL":
    "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/rooms/",
  "Official Property URL":
    "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
  "Source URL":
    "https://www.marriott.com/en-us/hotels/mucwi-the-westin-grand-munich/overview/",
  "Current Brand": "Westin",
  "Brand Family": "Marriott International",
  "Identity Confidence": "High",
  "Production Use Status": "Census Only / Not Owner-Facing",
  "Property Identity Key": "ind_marriott_de_mucwi",
  "Source Type": "Official / Brand",
  "Source Confidence": "High",
  "Address Confidence": "High",
  "Notes for Steward":
    "ADP+GDI e2e onboarding shell 2026-10-07. Marriott code MUCWI. 627 rooms; Arabellapark / Bogenhausen. Do not confuse with Westin Grand Cayman.",
};

async function findExisting() {
  const rows = [];
  await base(TABLE)
    .select({
      maxRecords: 5,
      filterByFormula:
        "OR(FIND('Westin Grand München', {Property Name}), FIND('Westin Grand Munich', {Property Name}), {Property Identity Key}='ind_marriott_de_mucwi')",
    })
    .eachPage((r, n) => {
      rows.push(...r);
      n();
    });
  // Exclude Cayman false positive
  return rows.filter((r) => !/Cayman/i.test(r.fields["Property Name"] || ""));
}

const existing = await findExisting();
if (existing.length) {
  console.log(
    JSON.stringify(
      {
        ok: true,
        action: "REUSE",
        censusRecordId: existing[0].id,
        name: existing[0].fields["Property Name"],
        apply: APPLY,
      },
      null,
      2
    )
  );
  process.exit(0);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      action: APPLY ? "CREATE" : "DRY_RUN_CREATE",
      table: TABLE,
      baseId,
      payload: PAYLOAD,
    },
    null,
    2
  )
);

if (!APPLY) {
  console.log("Re-run with --apply to create census shell.");
  process.exit(0);
}

const created = await base(TABLE).create([{ fields: PAYLOAD }], { typecast: true });
console.log(
  JSON.stringify(
    {
      ok: true,
      action: "CREATED",
      censusRecordId: created[0].id,
      name: created[0].fields["Property Name"],
    },
    null,
    2
  )
);
