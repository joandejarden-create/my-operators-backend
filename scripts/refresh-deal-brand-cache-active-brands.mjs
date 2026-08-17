#!/usr/bin/env node
/**
 * Refresh Deal Brand Cache for deals whose Preferred Brands intersect Active/Live brands.
 * Uses Match Score v2 engine via refreshDealBrandCacheForRecordId.
 *
 * Discovery:
 * 1) Deal Brand Cache rows (if present)
 * 2) Else Deals linked to Strategic Intent whose Preferred Brands hit Active/Live
 *
 * Usage:
 *   npm run refresh-deal-brand-cache-active-brands -- --dry-run
 *   npm run refresh-deal-brand-cache-active-brands -- --apply
 *   npm run refresh-deal-brand-cache-active-brands -- --apply --limit 20
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { refreshDealBrandCacheForRecordId } from "../api/my-deals.js";
import { STRATEGIC_INTENT_TABLE, STRATEGIC_INTENT_LINK_FIELD } from "../api/schemas/deal-setup-fields.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";

function parseArgs(argv) {
  const apply = argv.includes("--apply");
  let limit = Infinity;
  const li = argv.indexOf("--limit");
  if (li >= 0 && argv[li + 1]) limit = Math.max(1, parseInt(argv[li + 1], 10) || 1);
  return { apply, dryRun: !apply, limit };
}

function normBrand(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function preferredNamesFromSiFields(fields) {
  const raw = fields?.["Preferred Brands"] ?? fields?.["Preferred Brands (up to 4)"];
  if (raw == null || raw === "") return [];
  if (Array.isArray(raw)) {
    return raw
      .map((v) => (typeof v === "string" ? v : v?.name || ""))
      .map((s) => String(s).trim())
      .filter(Boolean);
  }
  if (typeof raw === "string") return raw.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean);
  return [];
}

async function atFetchAll(baseId, apiKey, table) {
  const records = [];
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?pageSize=100`;
    if (offset) url += `&offset=${encodeURIComponent(offset)}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const data = await res.json();
    if (data.error) throw new Error(`${table}: ${data.error.message || data.error.type}`);
    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return records;
}

function getSiLinkId(dealFields) {
  const raw = dealFields?.[STRATEGIC_INTENT_LINK_FIELD] ?? dealFields?.["Strategic Intent"];
  if (Array.isArray(raw) && raw[0]) return String(raw[0]);
  if (typeof raw === "string" && raw.startsWith("rec")) return raw;
  return null;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const universe = await loadActiveUniverse({ includeDetails: false });
  const activeNames = new Set(universe.brands.map((b) => normBrand(b.name)));

  fs.mkdirSync(path.join(ROOT, "reports"), { recursive: true });
  fs.writeFileSync(
    path.join(ROOT, "reports", "match-score-active-live-brand-list.json"),
    JSON.stringify(
      {
        source: universe.source,
        totalCount: universe.totalCount,
        brands: universe.brands.map((b) => ({
          recordId: b.recordId,
          name: b.name,
          slug: b.slug,
          status: b.status,
        })),
      },
      null,
      2
    ),
    "utf8"
  );

  const reasons = [];
  const dealIdSet = new Set();

  const cacheTable = process.env.AIRTABLE_TABLE_DEAL_BRAND_CACHE || "Deal Brand Cache";
  let cacheRecords = [];
  try {
    cacheRecords = await atFetchAll(baseId, apiKey, cacheTable);
  } catch (err) {
    console.warn("Deal Brand Cache fetch skipped:", err.message);
  }

  for (const rec of cacheRecords) {
    const f = rec.fields || {};
    const dealLink = f.Deal || f.deal;
    const dealId = Array.isArray(dealLink) ? dealLink[0] : dealLink;
    if (!dealId || !String(dealId).startsWith("rec")) continue;
    const preferred = String(f["Preferred Brands"] || "").trim();
    const brands = preferred ? preferred.split(/\s*,\s*/).map((s) => s.trim()).filter(Boolean) : [];
    // Empty preferred on cache still refresh (cache row exists) — scores may need v2 rewrite
    if (brands.length && !brands.some((b) => activeNames.has(normBrand(b)))) continue;
    dealIdSet.add(String(dealId));
    reasons.push({ dealId: String(dealId), source: "deal_brand_cache", preferredBrands: brands });
  }

  if (dealIdSet.size === 0) {
    console.log("No Deal Brand Cache deal links — scanning Deals + Strategic Intent…");
    const [deals, siRecords] = await Promise.all([
      atFetchAll(baseId, apiKey, DEALS_TABLE),
      atFetchAll(baseId, apiKey, STRATEGIC_INTENT_TABLE),
    ]);
    const siById = new Map(siRecords.map((r) => [r.id, r.fields || {}]));
    for (const deal of deals) {
      const siId = getSiLinkId(deal.fields || {});
      if (!siId) continue;
      const brands = preferredNamesFromSiFields(siById.get(siId) || {});
      if (!brands.length) continue;
      if (!brands.some((b) => activeNames.has(normBrand(b)))) continue;
      dealIdSet.add(deal.id);
      reasons.push({ dealId: deal.id, source: "strategic_intent", preferredBrands: brands });
    }
    console.log(`Deals=${deals.length} SI=${siRecords.length} intersecting preferred Active/Live=${dealIdSet.size}`);
  }

  const uniqueDealIds = [...dealIdSet].slice(0, opts.limit === Infinity ? undefined : opts.limit);
  const report = {
    generatedAt: new Date().toISOString(),
    dryRun: opts.dryRun,
    activeLiveCount: universe.totalCount,
    cacheRowsScanned: cacheRecords.length,
    dealsToRefresh: uniqueDealIds.length,
    dealIds: uniqueDealIds,
    sample: reasons.slice(0, 40),
    results: [],
    spotCheckNote:
      "After apply: open My Deals Matched Brands → View Details for a preferred Active/Live brand; list badge should match live breakdown (Geography Priority first when v2).",
  };

  console.log(
    `Active/Live=${universe.totalCount} dealsToRefresh=${uniqueDealIds.length} mode=${opts.dryRun ? "dry-run" : "APPLY"}`
  );

  if (opts.dryRun) {
    const out = path.join(ROOT, "reports", "match-score-deal-brand-cache-refresh-dry-run.json");
    fs.writeFileSync(out, JSON.stringify(report, null, 2), "utf8");
    console.log(`Dry-run only — wrote ${out}`);
    console.log("Pass --apply to refresh caches (Match Score v2).");
    return;
  }

  for (const dealId of uniqueDealIds) {
    try {
      const result = await refreshDealBrandCacheForRecordId(baseId, apiKey, dealId);
      report.results.push({
        dealId,
        ok: true,
        preferredScore: result.preferredScore,
        bestMatchBrand: result.bestMatchBrand,
      });
      console.log("refreshed", dealId, "preferredScore=", result.preferredScore);
    } catch (err) {
      report.results.push({ dealId, ok: false, error: err?.message || String(err) });
      console.warn("refresh failed", dealId, err?.message || err);
    }
  }

  const out = path.join(ROOT, "reports", "match-score-deal-brand-cache-refresh-apply.json");
  fs.writeFileSync(out, JSON.stringify(report, null, 2), "utf8");
  console.log(`Wrote ${out}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
