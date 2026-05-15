/**
 * Create Airtable table "Brand Setup - Brand Explorer Presentation" via Metadata API
 * if it does not already exist (matches api/brand-library.js + docs/brand-explorer-presentation-slots.md).
 *
 * Requirements: AIRTABLE_API_KEY with schema.bases:write, AIRTABLE_BASE_ID, base creator (or equivalent).
 *
 * Usage:
 *   node scripts/create-brand-setup-brand-explorer-presentation-table.mjs
 *   node scripts/create-brand-setup-brand-explorer-presentation-table.mjs --dry-run
 */
import "../load-env.js";

const TABLE_NAME = "Brand Setup - Brand Explorer Presentation";
const BASICS_NAME = "Brand Setup - Brand Basics";

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
  if (!token || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  }

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) {
    throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);
  }
  const tables = listJson.tables || [];
  const existing = tables.find((t) => t.name === TABLE_NAME);
  if (existing) {
    console.log(`Table already exists: ${TABLE_NAME} (${existing.id})`);
    return;
  }

  const basics = tables.find((t) => t.name === BASICS_NAME);
  if (!basics) {
    throw new Error(`Linked table not found: "${BASICS_NAME}" (cannot add Brand link).`);
  }

  const body = {
    name: TABLE_NAME,
    description: "Brand Explorer copy keyed by slot (1:n per brand). See docs/brand-explorer-presentation-slots.md",
    fields: [
      {
        name: "Slot Key",
        type: "singleLineText",
        description: "Stable id, e.g. hero.benefit_zones, overview.why_value",
      },
      {
        name: "Title",
        type: "singleLineText",
      },
      {
        name: "Body",
        type: "multilineText",
      },
      {
        name: "Case Summary Overview",
        type: "multilineText",
        description: "Optional; Case summary modal — Property overview",
      },
      {
        name: "Case Summary Owner Objective",
        type: "multilineText",
        description: "Optional; Case summary modal — Owner objective",
      },
      {
        name: "Case Summary Brand Relevance",
        type: "multilineText",
        description: "Optional; Case summary modal — Brand relevance",
      },
      {
        name: "Case Summary Interpretation",
        type: "multilineText",
        description: "Optional; Case summary modal — Dealality interpretation",
      },
      {
        name: "Case Summary Tags",
        type: "multilineText",
        description: "Optional; Case summary modal — Related tags (comma-separated)",
      },
      {
        name: "Image",
        type: "multipleAttachments",
        description: "Optional; first attachment URL used for overview scenario cards (slot overview.scenario.1–3)",
      },
      {
        name: "Sort Order",
        type: "number",
        options: { precision: 0 },
      },
      {
        name: "Active",
        type: "checkbox",
        options: { icon: "check", color: "greenBright" },
      },
      {
        name: "Brand",
        type: "multipleRecordLinks",
        options: { linkedTableId: basics.id },
      },
      {
        name: "Brand Name",
        type: "singleLineText",
        description: "Optional text fallback if link field names differ",
      },
    ],
  };

  if (dryRun) {
    console.log("Dry run — would POST:", JSON.stringify({ name: body.name, fieldCount: body.fields.length, linkTo: BASICS_NAME }, null, 2));
    return;
  }

  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`Create table failed ${res.status}: ${JSON.stringify(json)}`);
  }
  console.log(`Created table ${json.name} id=${json.id} fields=${(json.fields || []).map((f) => f.name).join(", ")}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
