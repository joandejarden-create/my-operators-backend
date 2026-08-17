#!/usr/bin/env node
/**
 * Delete stale duplicate Deal Brand Cache rows (keep highest scoreModelVersion).
 * Usage:
 *   node scripts/cleanup-duplicate-deal-brand-cache.mjs --dry-run
 *   node scripts/cleanup-duplicate-deal-brand-cache.mjs --apply
 */
import "dotenv/config";

const APPLY = process.argv.includes("--apply");
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = process.env.AIRTABLE_TABLE_DEAL_BRAND_CACHE || "Deal Brand Cache";

if (!baseId || !apiKey) {
  console.error("Missing AIRTABLE_BASE_ID / AIRTABLE_API_KEY");
  process.exit(1);
}

async function atFetchJson(url, options = {}, attempt = 1) {
  const res = await fetch(url, options);
  if (res.status === 429 && attempt < 6) {
    const waitMs = Math.min(30000, 1000 * 2 ** attempt);
    console.warn("Airtable 429 — retry in", waitMs, "ms");
    await new Promise((r) => setTimeout(r, waitMs));
    return atFetchJson(url, options, attempt + 1);
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) {
    throw new Error(data?.error?.message || `HTTP ${res.status}`);
  }
  return data;
}

async function fetchAll() {
  const records = [];
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?pageSize=100`;
    if (offset) url += `&offset=${encodeURIComponent(offset)}`;
    const data = await atFetchJson(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return records;
}

function scoreModelVersion(rec) {
  const raw = rec.fields?.["Breakdown Details By Brand"];
  if (!raw || typeof raw !== "string") return 0;
  try {
    const parsed = JSON.parse(raw);
    return Number(parsed?._meta?.scoreModelVersion) || 0;
  } catch {
    return 0;
  }
}

function lastComputedAt(rec) {
  const v = rec.fields?.["Last Computed At"];
  return v ? Date.parse(String(v)) || 0 : 0;
}

const records = await fetchAll();
/** @type {Map<string, typeof records>} */
const byDeal = new Map();
for (const rec of records) {
  const dealLink = rec.fields?.Deal;
  const dealId = Array.isArray(dealLink) && dealLink[0] ? dealLink[0] : dealLink;
  if (!dealId || typeof dealId !== "string") continue;
  if (!byDeal.has(dealId)) byDeal.set(dealId, []);
  byDeal.get(dealId).push(rec);
}

const toDelete = [];
const keep = [];
for (const [dealId, rows] of byDeal.entries()) {
  if (rows.length < 2) continue;
  rows.sort((a, b) => {
    const dv = scoreModelVersion(b) - scoreModelVersion(a);
    if (dv !== 0) return dv;
    return lastComputedAt(b) - lastComputedAt(a);
  });
  const winner = rows[0];
  keep.push({
    dealId,
    keepId: winner.id,
    keepVersion: scoreModelVersion(winner),
    deleteIds: rows.slice(1).map((r) => ({ id: r.id, version: scoreModelVersion(r) })),
  });
  for (const r of rows.slice(1)) toDelete.push(r.id);
}

console.log(
  JSON.stringify(
    {
      mode: APPLY ? "APPLY" : "DRY_RUN",
      totalRows: records.length,
      dealsWithDuplicates: keep.length,
      rowsToDelete: toDelete.length,
      plan: keep,
    },
    null,
    2
  )
);

if (!APPLY) {
  console.log("Dry run only. Re-run with --apply to delete stale duplicates.");
  process.exit(0);
}

for (const id of toDelete) {
  try {
    await atFetchJson(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${encodeURIComponent(id)}`,
      { method: "DELETE", headers: { Authorization: `Bearer ${apiKey}` } }
    );
    console.log("deleted", id);
  } catch (e) {
    console.error("Failed delete", id, e.message || e);
  }
}
console.log("Done. Deleted", toDelete.length, "duplicate cache rows.");
