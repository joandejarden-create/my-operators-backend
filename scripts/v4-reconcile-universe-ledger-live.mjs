/**
 * Reconcile V4 universe ledger IN_PRODUCTION against live Airtable exact identity.
 * Does not write Airtable. Rewrites ledger shards + index + scorecard deltas.
 */
import fs from "node:fs";
import path from "node:path";
import "dotenv/config";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { normName } from "../lib/research-engine-v2/census-autopilot-v2/identity-dedupe.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const ROOT = path.resolve("c:/Dev/deal-capture-proxy");
const OUT = path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe");
const LEDGER_DIR = path.join(OUT, "27-universe-ledger");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function listLive(baseId, token) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of ["Property Identity Key", "Property Name", "Country", "Official Property URL"])
      params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(80);
  } while (offset);
  return out;
}

function loadLedger() {
  const files = fs
    .readdirSync(LEDGER_DIR)
    .filter((f) => f.startsWith("ledger-") && f.endsWith(".json"))
    .sort();
  const rows = [];
  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(LEDGER_DIR, f), "utf8"));
    rows.push(...(j.rows || []));
  }
  return { files, rows };
}

async function main() {
  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const live = await listLive(baseId, token);
  const keys = new Set(live.map((r) => r.fields?.["Property Identity Key"]).filter(Boolean));
  const urls = new Set(
    live
      .map((r) => String(r.fields?.["Official Property URL"] || "").trim().toLowerCase())
      .filter((u) => u.length > 8)
  );
  const nc = new Map();
  for (const r of live) {
    const k = `${normName(r.fields?.["Property Name"])}|${norm(r.fields?.Country)}`;
    if (!nc.has(k)) nc.set(k, r);
  }

  const { rows } = loadLedger();
  let movedToResearch = 0;
  let confirmedProd = 0;

  for (const r of rows) {
    const k = `${normName(r.candidate_name)}|${norm(r.country)}`;
    const url = String(r.candidate_official_url || "").trim().toLowerCase();
    const liveHit = nc.get(k) || (url && [...urls].includes(url) ? true : null);
    // Prefer exact name|country; URL alone is weak for reconcile — require nc
    const inLive = Boolean(nc.get(k));
    if (inLive) {
      confirmedProd++;
      r.universe_status = "IN_PRODUCTION";
      r.production_status = "IN_PRODUCTION";
      r.verification_status = "VERIFIED";
      r.research_status = "IN_PRODUCTION_REMEDIATION";
      r.production_airtable_id = nc.get(k).id;
      r.exclusion_reason = null;
    } else if (r.universe_status === "IN_PRODUCTION") {
      // Was fuzzy-matched; demote
      movedToResearch++;
      if (r.cvent_challenge) {
        r.universe_status = "NOT_YET_INDEPENDENTLY_REDISCOVERED";
        r.verification_status = "SOURCE_ONLY_CHALLENGE";
        r.research_status = "NEEDS_INDEPENDENT_REDISCOVERY";
      } else {
        r.universe_status = "RESEARCHABLE_UNVERIFIED";
        r.verification_status = "UNVERIFIED";
        r.research_status = "PENDING";
      }
      r.production_status = "NOT_IN_PRODUCTION";
      r.production_airtable_id = null;
    }
  }

  const statusCounts = rows.reduce((a, r) => {
    a[r.universe_status] = (a[r.universe_status] || 0) + 1;
    return a;
  }, {});

  const shardSize = 2000;
  for (let i = 0; i < rows.length; i += shardSize) {
    const chunk = rows.slice(i, i + shardSize);
    fs.writeFileSync(
      path.join(LEDGER_DIR, `ledger-${String(Math.floor(i / shardSize)).padStart(3, "0")}.json`),
      JSON.stringify({ offset: i, count: chunk.length, rows: chunk })
    );
  }

  const actionable = rows.filter((r) =>
    [
      "IN_PRODUCTION",
      "VERIFIED_READY_TO_INSERT",
      "RESEARCHABLE_UNVERIFIED",
      "NOT_YET_INDEPENDENTLY_REDISCOVERED",
      "IDENTITY_CONFLICT",
      "INSUFFICIENT_EVIDENCE",
    ].includes(r.universe_status)
  ).length;

  const index = {
    generated_at: new Date().toISOString(),
    ledger_rows: rows.length,
    live_production_count: live.length,
    live_exact_name_country_confirmed: confirmedProd,
    demoted_from_fuzzy_in_production: movedToResearch,
    status_counts: statusCounts,
    sum_statuses: Object.values(statusCounts).reduce((s, n) => s + n, 0),
    actionable_universe: actionable,
    footprint_vs_actionable_pct: Math.round((1000 * live.length) / Math.max(1, actionable)) / 10,
    footprint_vs_12846_pct: Math.round((1000 * live.length) / 12846) / 10,
    reconcile_note:
      "IN_PRODUCTION requires exact live Property Name|Country match. Fuzzy VIC matches demoted.",
  };
  fs.writeFileSync(path.join(OUT, "27-universe-ledger-index.json"), JSON.stringify(index, null, 2));
  fs.writeFileSync(path.join(OUT, "27b-ledger-live-reconcile.json"), JSON.stringify(index, null, 2));
  console.log(JSON.stringify(index, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
