/**
 * Add optional single-line fields on "Brand Setup - Brand Basics" for Brand Explorer combined hero:
 *   - Explorer Hero Verification  (default UI: "Verified by brand")
 *   - Explorer Hero Data Source    (default UI: "Live Airtable / Brand Setup data")
 * Safe to re-run. Does not remove or alter existing fields.
 *
 * Env: AIRTABLE_API_KEY (schema.bases:write), AIRTABLE_BASE_ID
 *
 * Usage:
 *   node scripts/ensure-brand-basics-explorer-hero-labels.mjs
 *   node scripts/ensure-brand-basics-explorer-hero-labels.mjs --dry-run
 */
import "../load-env.js";

const TABLE_NAME = "Brand Setup - Brand Basics";

const FIELDS = [
  {
    name: "Explorer Hero Verification",
    type: "singleLineText",
    description: "Brand Explorer hero badge text (e.g. Verified by brand, Unverified by brand). Empty = default.",
  },
  {
    name: "Explorer Hero Data Source",
    type: "singleLineText",
    description: "Brand Explorer hero line after the badge (e.g. Live Airtable / Brand Setup data). Empty = default.",
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
  const toCreate = FIELDS.filter((f) => !existingNames.has(f.name));

  if (!toCreate.length) {
    console.log(`All ${FIELDS.length} explorer hero label field(s) already exist on ${TABLE_NAME}.`);
    return;
  }

  if (dryRun) {
    console.log(`Dry run — would create on ${TABLE_NAME}: ${toCreate.map((f) => f.name).join(", ")}`);
    return;
  }

  for (const field of toCreate) {
    const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(field),
    });
    if (!res.ok) {
      throw new Error(`Create field "${field.name}" failed ${res.status}: ${JSON.stringify(json)}`);
    }
    console.log(`Created field "${field.name}" (${json.id}).`);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
