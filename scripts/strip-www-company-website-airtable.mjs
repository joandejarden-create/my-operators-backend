/**
 * Bulk-update Airtable "Company Website" by stripping a leading www. from the hostname.
 *
 * Read/normalize in the app does NOT write back to Airtable — run this once (or after imports)
 * to persist cleaned URLs in the Company Profile table.
 *
 * Usage:
 *   node scripts/strip-www-company-website-airtable.mjs           # dry-run (prints planned changes)
 *   node scripts/strip-www-company-website-airtable.mjs --apply # PATCH Airtable
 *
 * Requires AIRTABLE_API_KEY + AIRTABLE_BASE_ID (.env / .env.local).
 * Optional: COMPANY_PROFILE_TABLE_ID (default tblItyfH6MlOnMKZ9).
 */
import "../load-env.js";
import { stripLeadingWwwFromWebsiteUrl } from "../api/lib/strip-www-from-website-url.js";

const FIELD = "Company Website";
const TABLE_ID =
  process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9";

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;

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

async function fetchAllRecords() {
  const records = [];
  let offset;
  for (;;) {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${enc(TABLE_ID)}?${qs}`;
    const { res, json } = await airtableFetch(url);
    if (!res.ok) {
      throw new Error(`List failed ${res.status}: ${JSON.stringify(json)}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }
  return records;
}

/** Airtable batch PATCH allows up to 10 records per request. */
async function patchRecords(batch) {
  const url = `https://api.airtable.com/v0/${baseId}/${enc(TABLE_ID)}`;
  const body = JSON.stringify({
    records: batch.map(({ id, fields }) => ({ id, fields })),
  });
  const { res, json } = await airtableFetch(url, {
    method: "PATCH",
    body,
  });
  if (!res.ok) {
    throw new Error(`PATCH failed ${res.status}: ${JSON.stringify(json)}`);
  }
}

async function main() {
  const apply = process.argv.includes("--apply");

  if (!apiKey || !baseId) {
    console.error(
      "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID (.env / .env.local)."
    );
    process.exit(1);
  }

  console.log(
    `Table ${TABLE_ID} · field "${FIELD}" · mode: ${apply ? "APPLY" : "DRY-RUN"}`
  );

  const records = await fetchAllRecords();
  const updates = [];

  for (const rec of records) {
    const raw = rec.fields?.[FIELD];
    if (raw == null || raw === "") continue;
    const prev = String(raw).trim();
    if (!prev) continue;
    const next = stripLeadingWwwFromWebsiteUrl(prev);
    if (!next || next === prev) continue;
    updates.push({ id: rec.id, prev, next });
  }

  console.log(`Scanned ${records.length} rows · ${updates.length} would change.`);

  for (const u of updates.slice(0, 40)) {
    console.log(`  ${u.id}: ${u.prev} → ${u.next}`);
  }
  if (updates.length > 40) {
    console.log(`  … and ${updates.length - 40} more`);
  }

  if (!apply) {
    console.log("\nRe-run with --apply to write these changes to Airtable.");
    return;
  }

  const BATCH = 10;
  for (let i = 0; i < updates.length; i += BATCH) {
    const slice = updates.slice(i, i + BATCH);
    await patchRecords(
      slice.map((u) => ({
        id: u.id,
        fields: { [FIELD]: u.next },
      }))
    );
    console.log(`Patched ${Math.min(i + BATCH, updates.length)} / ${updates.length}`);
    await new Promise((r) => setTimeout(r, 220));
  }

  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
