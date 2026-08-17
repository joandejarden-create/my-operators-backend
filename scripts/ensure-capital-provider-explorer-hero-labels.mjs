/**
 * Add optional single-line fields on "Capital Setup - Capital Providers" for Explorer hero:
 *   - Explorer Hero Verification  (badge text)
 *   - Explorer Hero Data Source    (muted line after badge)
 * Mirrors Operator Setup - Master and Brand Setup - Brand Basics.
 *
 * Env: AIRTABLE_API_KEY (schema.bases:write), AIRTABLE_BASE_ID
 *
 * Usage:
 *   node scripts/ensure-capital-provider-explorer-hero-labels.mjs
 *   node scripts/ensure-capital-provider-explorer-hero-labels.mjs --dry-run
 */
import "../load-env.js";
import { CAPITAL_PROVIDER_EXPLORER_HERO_AIRTABLE } from "../lib/capital-provider-explorer-hero-labels.js";
import { TABLE_CAPITAL_PROVIDERS } from "../lib/capital-setup/airtable-capital-setup-fields.js";

const FIELDS = [
  {
    name: CAPITAL_PROVIDER_EXPLORER_HERO_AIRTABLE.verification,
    type: "singleLineText",
    description:
      "Capital Provider Explorer hero badge text (e.g. Verified by capital provider, Public Source — Not Capital Provider-Verified).",
  },
  {
    name: CAPITAL_PROVIDER_EXPLORER_HERO_AIRTABLE.dataSource,
    type: "singleLineText",
    description:
      "Capital Provider Explorer hero muted line after the badge (e.g. Live Airtable / Capital Setup data).",
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

  const table = (listJson.tables || []).find((t) => t.name === TABLE_CAPITAL_PROVIDERS);
  if (!table) throw new Error(`Table not found: ${TABLE_CAPITAL_PROVIDERS}`);

  const existingNames = new Set((table.fields || []).map((f) => f.name));
  const toCreate = FIELDS.filter((f) => !existingNames.has(f.name));

  if (!toCreate.length) {
    console.log(`All ${FIELDS.length} explorer hero label field(s) already exist on ${TABLE_CAPITAL_PROVIDERS}.`);
    return;
  }

  if (dryRun) {
    console.log(`Dry run — would create on ${TABLE_CAPITAL_PROVIDERS}: ${toCreate.map((f) => f.name).join(", ")}`);
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
