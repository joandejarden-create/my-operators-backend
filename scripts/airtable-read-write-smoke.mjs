/**
 * Smoke test: list one record (read) then PATCH identical scalar field (write, no semantic change).
 * Loads ../load-env.js (same as server).
 */
import "../load-env.js";

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
const table =
  process.env.AIRTABLE_SMOKE_TABLE ||
  process.env.AIRTABLE_TABLE_DEALS ||
  "Deals";

function enc(s) {
  return encodeURIComponent(s);
}

async function airtableFetch(url, opts = {}) {
  const res = await fetch(url, {
    ...opts,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { _raw: text };
  }
  return { res, json };
}

function pickScalarField(fields) {
  for (const [k, v] of Object.entries(fields || {})) {
    if (v == null) continue;
    const t = typeof v;
    if (t === "string" || t === "number" || t === "boolean") return [k, v];
  }
  return [null, null];
}

async function main() {
  if (!apiKey || !baseId) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID (.env / .env.local).");
    process.exit(1);
  }

  const listUrl = `https://api.airtable.com/v0/${baseId}/${enc(table)}?maxRecords=1`;
  const { res: listRes, json: listJson } = await airtableFetch(listUrl);
  if (!listRes.ok) {
    console.error("Read failed:", listRes.status, listJson);
    process.exit(1);
  }

  const rec = listJson.records?.[0];
  if (!rec) {
    console.log(`Read OK (table "${table}"): 0 rows — cannot run write no-op.`);
    process.exit(0);
  }

  const [fieldName, fieldVal] = pickScalarField(rec.fields);
  console.log(`Read OK: table="${table}" record=${rec.id} fields=${Object.keys(rec.fields || {}).length}`);

  if (!fieldName) {
    console.log("No scalar field on first record — skipping write test.");
    process.exit(0);
  }

  const patchUrl = `https://api.airtable.com/v0/${baseId}/${enc(table)}/${enc(rec.id)}`;
  const body = JSON.stringify({ fields: { [fieldName]: fieldVal } });
  const { res: patchRes, json: patchJson } = await airtableFetch(patchUrl, {
    method: "PATCH",
    body,
  });

  if (!patchRes.ok) {
    console.error("Write (no-op PATCH) failed:", patchRes.status, patchJson);
    process.exit(1);
  }

  console.log(`Write OK: PATCH same value for field "${fieldName}" on ${rec.id}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
