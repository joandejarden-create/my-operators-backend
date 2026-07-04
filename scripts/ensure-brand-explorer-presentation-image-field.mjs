/**
 * Add optional "Image" (multiple attachments) to Brand Setup - Brand Explorer Presentation
 * if missing. Safe to re-run.
 *
 * Env: AIRTABLE_API_KEY (schema.bases:write), AIRTABLE_BASE_ID
 *
 * Usage: node scripts/ensure-brand-explorer-presentation-image-field.mjs
 */
import "../load-env.js";

const TABLE_NAME = "Brand Setup - Brand Explorer Presentation";
const FIELD_NAME = "Image";

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
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.name === TABLE_NAME);
  if (!table) throw new Error(`Table not found: ${TABLE_NAME}`);

  const hasField = (table.fields || []).some((f) => f.name === FIELD_NAME);
  if (hasField) {
    console.log(`Field "${FIELD_NAME}" already exists on ${TABLE_NAME}.`);
    return;
  }

  const body = {
    name: FIELD_NAME,
    type: "multipleAttachments",
    description: "Optional; first file URL exposed as brandExplorer.blocks[].imageUrl",
  };

  const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Create field failed ${res.status}: ${JSON.stringify(json)}`);
  console.log(`Created field "${FIELD_NAME}" (${json.id}) on ${TABLE_NAME}.`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
