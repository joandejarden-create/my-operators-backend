/**
 * Create Airtable table "Operator Setup - Explorer Materials" (Brand Explorer Presentation parity).
 *
 *   node scripts/create-operator-setup-explorer-materials-table.mjs
 *   node scripts/create-operator-setup-explorer-materials-table.mjs --dry-run
 */
import "../load-env.js";
import { NEW_BASE_MASTER_TABLE } from "../api/lib/operator-setup-new-base-read.js";

const TABLE_NAME = "Operator Setup - Explorer Materials";

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

  const master = tables.find((t) => t.name === NEW_BASE_MASTER_TABLE);
  if (!master) {
    throw new Error(`Linked table not found: "${NEW_BASE_MASTER_TABLE}" (cannot add Operator link).`);
  }

  const body = {
    name: TABLE_NAME,
    description:
      "Operator DNA Materials tab — slot-keyed file cards and gallery (parity with Brand Setup - Brand Explorer Presentation).",
    fields: [
      {
        name: "Slot Key",
        type: "singleLineText",
        description: "e.g. materials.file, materials.gallery.1",
      },
      { name: "Title", type: "singleLineText" },
      { name: "Body", type: "multilineText" },
      {
        name: "Image",
        type: "multipleAttachments",
        description: "PDF URL or gallery image; first attachment URL used in Explorer",
      },
      { name: "Sort Order", type: "number", options: { precision: 0 } },
      {
        name: "Active",
        type: "checkbox",
        options: { icon: "check", color: "greenBright" },
      },
      {
        name: "Operator",
        type: "multipleRecordLinks",
        options: { linkedTableId: master.id },
      },
      {
        name: "Company Name",
        type: "singleLineText",
        description: "Optional text fallback if link field names differ",
      },
    ],
  };

  if (dryRun) {
    console.log(
      "Dry run — would POST:",
      JSON.stringify({ name: body.name, fieldCount: body.fields.length, linkTo: NEW_BASE_MASTER_TABLE }, null, 2)
    );
    return;
  }

  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`Create table failed ${res.status}: ${JSON.stringify(json)}`);
  }
  console.log(
    `Created table ${json.name} id=${json.id} fields=${(json.fields || []).map((f) => f.name).join(", ")}`
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
