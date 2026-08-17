/**
 * Rank Dealality CALA geographies for highest-value HBX TEST quota spend.
 * Does not call HBX. Pure ranking from registry + holds + prior HBX ledger + census counts.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listDealalityCalaGeographies,
  normalizeGeographyLabel,
} from "./dealality-cala-geography-registry-v1.js";
import {
  HBX_DISCOVERY_STATUS,
  initOrLoadLedger,
} from "./full-cala-hbx-geography-discovery-wave-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

const TOURISM_W = { S: 220, A: 140, B: 70, C: 30 };

const HIGH_VALUE_BOOST = new Set([
  "brazil",
  "argentina",
  "mexico",
  "chile",
  "peru",
  "jamaica",
  "bahamas",
  "puerto_rico",
  "aruba",
  "cayman_islands",
  "turks_and_caicos_islands",
  "us_virgin_islands",
  "curacao",
  "sint_maarten",
  "saint_martin",
  "barbados",
  "saint_lucia",
  "antigua_and_barbuda",
]);

function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

function holdsByCountry() {
  const holds = readJson(
    path.join(
      ROOT,
      "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
    ),
    { by_candidate_id: {} }
  );
  const by = {};
  for (const v of Object.values(holds.by_candidate_id || {})) {
    const c = String(v.country || "unknown").trim() || "unknown";
    by[c] = (by[c] || 0) + 1;
  }
  return by;
}

function censusCountsFromAudit() {
  const audit = readJson(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-geography-coverage-registry-audit.json"
    ),
    null
  );
  const by = {};
  for (const row of audit?.FULL_GEOGRAPHY_COVERAGE_MATRIX || []) {
    const name = row.geography || row.name || row.canonical_geography;
    if (!name) continue;
    by[normalizeGeographyLabel(name)] = Number(row.census_count || 0);
  }
  return by;
}

/**
 * @param {{ requestBudget?: number, pageSize?: number }} [opts]
 */
export function rankHbxPriorityGeographies(opts = {}) {
  const requestBudget = Math.max(1, Number(opts.requestBudget || 40));
  const pageSize = Math.min(1000, Math.max(100, Number(opts.pageSize || 1000)));
  const ledger = initOrLoadLedger();
  const holds = holdsByCountry();
  const censusByNorm = censusCountsFromAudit();
  const geos = listDealalityCalaGeographies({ includeScopeReview: false });

  const ranked = [];
  for (const g of geos) {
    const entry = ledger.geographies[g.geography_id] || {};
    const hbxStatus = entry.hbx_status || HBX_DISCOVERY_STATUS.NOT_STARTED;
    if (hbxStatus === HBX_DISCOVERY_STATUS.COMPLETE && entry.wave1_prior_complete) {
      // Wave1 already complete — only include if huge HOLD pool needs more depth
      if ((holds[g.name] || 0) < 800) continue;
    }
    if (hbxStatus === HBX_DISCOVERY_STATUS.COMPLETE && !entry.wave1_prior_complete) {
      continue; // already fully discovered this wave
    }

    const census =
      censusByNorm[normalizeGeographyLabel(g.name)] ??
      Number(entry.census_count || 0);
    const holdN = holds[g.name] || 0;
    const tourism = TOURISM_W[g.tourism_priority] || 40;
    const zeroBoost = census === 0 ? 500 : census < 40 ? 280 : 0;
    const unsearchedBoost = [
      HBX_DISCOVERY_STATUS.NOT_STARTED,
      HBX_DISCOVERY_STATUS.FAILED_RETRYABLE,
      HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW,
      HBX_DISCOVERY_STATUS.IN_PROGRESS,
    ].includes(hbxStatus)
      ? 320
      : 0;
    const namedBoost = HIGH_VALUE_BOOST.has(g.geography_id) ? 180 : 0;
    // Expected yield heuristic: large HOLD pools + tourism markets
    const expectedPages = Math.max(
      1,
      Math.min(
        20,
        Math.ceil((holdN > 0 ? holdN * 1.2 : tourism * 8) / pageSize)
      )
    );

    const score =
      zeroBoost +
      unsearchedBoost +
      namedBoost +
      tourism +
      holdN * 0.55 +
      Math.max(0, 200 - census) * 0.4;

    ranked.push({
      geography_id: g.geography_id,
      name: g.name,
      iso_code: g.iso_code,
      region: g.region,
      tourism_priority: g.tourism_priority,
      hbx_status_before: hbxStatus,
      census_count_approx: census,
      holds_count: holdN,
      score: Math.round(score),
      suggested_max_pages: expectedPages,
      suggested_max_hotels: expectedPages * pageSize,
    });
  }

  ranked.sort((a, b) => b.score - a.score || b.holds_count - a.holds_count);

  // Allocate request budget greedily
  let remaining = requestBudget;
  const plan = [];
  for (const row of ranked) {
    if (remaining <= 0) break;
    const pages = Math.min(row.suggested_max_pages, remaining);
    if (pages <= 0) continue;
    plan.push({
      ...row,
      allocated_pages: pages,
      allocated_hotels: pages * pageSize,
    });
    remaining -= pages;
  }

  return {
    version: "full-cala-core-identity-hbx-priority-v1",
    request_budget: requestBudget,
    page_size: pageSize,
    requests_allocated: requestBudget - remaining,
    requests_reserved: remaining,
    plan,
    ranked_all: ranked,
  };
}
