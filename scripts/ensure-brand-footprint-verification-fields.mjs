/**
 * Add explicit footprint verification fields on Brand Setup - Brand Footprint (MVP base).
 *
 * Env: AIRTABLE_API_KEY (schema.bases:write), AIRTABLE_BASE_ID
 *
 * Usage:
 *   node scripts/ensure-brand-footprint-verification-fields.mjs
 *   node scripts/ensure-brand-footprint-verification-fields.mjs --dry-run
 */
import "../load-env.js";
import { FOOTPRINT_VERIFICATION_AIRTABLE } from "../lib/brand-footprint-verification.js";

const TABLE_NAME = "Brand Setup - Brand Footprint";

const FIELDS = [
  {
    name: FOOTPRINT_VERIFICATION_AIRTABLE.status,
    type: "singleSelect",
    description:
      "Explicit footprint trust for Brand Explorer: Verified, Estimated, Placeholder, or Needs Review.",
    options: {
      choices: [
        { name: "Verified" },
        { name: "Estimated" },
        { name: "Placeholder" },
        { name: "Needs Review" },
      ],
    },
  },
  {
    name: FOOTPRINT_VERIFICATION_AIRTABLE.source,
    type: "singleLineText",
    description: "Source citation for footprint figures (text or URL).",
  },
  {
    name: FOOTPRINT_VERIFICATION_AIRTABLE.figuresAsOf,
    type: "singleLineText",
    description: "As-of date or period label for footprint figures (YYYY-MM-DD or text).",
  },
  {
    name: FOOTPRINT_VERIFICATION_AIRTABLE.notes,
    type: "multilineText",
    description: "Internal notes on footprint data quality or provenance.",
  },
];

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.name === TABLE_NAME);
  if (!table) throw new Error(`Table not found: ${TABLE_NAME}`);

  const existingNames = new Set((table.fields || []).map((f) => f.name));
  const report = { table: TABLE_NAME, tableId: table.id, created: [], alreadyPresent: [] };

  for (const field of FIELDS) {
    if (existingNames.has(field.name)) {
      report.alreadyPresent.push(field.name);
      console.log(`Field already exists: "${field.name}"`);
      continue;
    }
    if (dryRun) {
      report.created.push(field.name);
      console.log(`[dry-run] Would create: "${field.name}"`);
      continue;
    }
    const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(field),
    });
    if (!res.ok) {
      throw new Error(`Create field "${field.name}" failed ${res.status}: ${JSON.stringify(json)}`);
    }
    report.created.push(field.name);
    console.log(`Created field "${field.name}" (${json.id}).`);
  }

  if (!report.created.length && report.alreadyPresent.length === FIELDS.length) {
    console.log(`All ${FIELDS.length} footprint verification field(s) already exist on ${TABLE_NAME}.`);
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
