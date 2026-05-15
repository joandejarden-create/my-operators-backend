/**
 * Add optional Case Summary long-text fields to "Brand Setup - Brand Explorer Presentation"
 * when missing (materials.caseStudy modal in Brand Explorer combined). Safe to re-run.
 * Does not create "Summary URL" — use Body appendix or a URL-only line if you need a link.
 *
 * Env: AIRTABLE_API_KEY (schema.bases:write), AIRTABLE_BASE_ID
 *
 * Usage:
 *   node scripts/ensure-brand-explorer-presentation-case-summary-fields.mjs
 *   node scripts/ensure-brand-explorer-presentation-case-summary-fields.mjs --dry-run
 */
import "../load-env.js";

const TABLE_NAME = "Brand Setup - Brand Explorer Presentation";

const FIELDS = [
  {
    name: "Case Summary Overview",
    type: "multilineText",
    description: "Case summary modal: Property overview (see docs/brand-explorer-presentation-slots.md)",
  },
  {
    name: "Case Summary Owner Objective",
    type: "multilineText",
    description: "Case summary modal: Owner objective",
  },
  {
    name: "Case Summary Brand Relevance",
    type: "multilineText",
    description: "Case summary modal: Brand relevance",
  },
  {
    name: "Case Summary Interpretation",
    type: "multilineText",
    description: "Case summary modal: Dealality interpretation",
  },
  {
    name: "Case Summary Tags",
    type: "multilineText",
    description: "Case summary modal: Related tags (comma-separated)",
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
    console.log(`All ${FIELDS.length} Case Summary field(s) already exist on ${TABLE_NAME}.`);
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
