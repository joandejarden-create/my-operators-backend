/**
 * Build country coverage scorecard from live census + prior discovery stock.
 * Expected universe = max(census, cvent_stock, hbx_stock, census+holds) from audits.
 * Does not invent STR/external market sizes.
 */

import fs from "node:fs";
import path from "node:path";
import {
  COVERAGE_FLAG,
  coverageFlagFromPct,
  priorityFromFlag,
} from "./statuses.js";

export const COVERAGE_SCORECARD_VERSION = "universe-expansion-coverage-scorecard-v1";

function readJson(fp, fallback = null) {
  try {
    if (!fs.existsSync(fp)) return fallback;
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return fallback;
  }
}

/**
 * Count candidates by country from master universe files.
 */
export function countUniverseCandidatesByCountry(root = process.cwd()) {
  const dir = path.join(
    root,
    "data/research-engine-v2/census-autopilot-v2-full-universe/candidates"
  );
  const byCountry = {};
  if (!fs.existsSync(dir)) return byCountry;
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".json"))) {
    const raw = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
    const arr = Array.isArray(raw) ? raw : raw.candidates || raw.records || [];
    for (const c of arr) {
      const country = String(c.origin_country || c.country || "UNKNOWN").trim();
      byCountry[country] = (byCountry[country] || 0) + 1;
    }
  }
  return byCountry;
}

export function countHbxByCountry(root = process.cwd()) {
  const full = path.join(
    root,
    "reports/research-engine-v2/hbx-cala-full-geography-candidate-pack.json"
  );
  const wave1 = path.join(
    root,
    "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"
  );
  const fp = fs.existsSync(full) ? full : wave1;
  const j = readJson(fp, { candidates: [] });
  const byCountry = {};
  for (const c of j.candidates || []) {
    const country = String(c.country || "UNKNOWN").trim();
    byCountry[country] = (byCountry[country] || 0) + 1;
  }
  return byCountry;
}

export function countHoldsByCountry(root = process.cwd()) {
  const fp = path.join(
    root,
    "data/research-engine-v2/full-cala-15k-shell-orchestrator/holds-ledger.json"
  );
  const holds = readJson(fp, { by_candidate_id: {} });
  const byCountry = {};
  for (const h of Object.values(holds.by_candidate_id || {})) {
    const country = String(h.country || "UNKNOWN").trim();
    byCountry[country] = (byCountry[country] || 0) + 1;
  }
  return byCountry;
}

/**
 * @param {Record<string, number>} censusByCountry
 * @param {object} [opts]
 */
export function buildCoverageScorecard(censusByCountry = {}, opts = {}) {
  const root = opts.root || process.cwd();
  const cventBy = opts.cventByCountry || countUniverseCandidatesByCountry(root);
  const hbxBy = opts.hbxByCountry || countHbxByCountry(root);
  const holdsBy = opts.holdsByCountry || countHoldsByCountry(root);

  const geoAudit = readJson(
    path.join(
      root,
      "reports/research-engine-v2/full-cala-geography-coverage-registry-audit.json"
    ),
    {}
  );
  const zeroGeos = new Set(geoAudit.ZERO_RECORD_GEOGRAPHIES || []);

  const countries = new Set([
    ...Object.keys(censusByCountry || {}),
    ...Object.keys(cventBy),
    ...Object.keys(hbxBy),
    ...Object.keys(holdsBy),
    ...zeroGeos,
  ]);

  const rows = [];
  for (const country of countries) {
    if (!country || country === "UNKNOWN") continue;
    const census = Number(censusByCountry[country] || 0);
    const cvent = Number(cventBy[country] || 0);
    const hbx = Number(hbxBy[country] || 0);
    const holds = Number(holdsBy[country] || 0);
    const knownStock = Math.max(census, cvent, hbx, census + holds);
    const expected = knownStock > 0 ? knownStock : null;
    const coveragePct =
      expected && expected > 0 ? Math.round((1000 * census) / expected) / 10 : null;

    let confidence = "medium";
    if (hbx > 0 && cvent > 0) confidence = "high";
    else if (cvent === 0 && hbx === 0 && census === 0) confidence = "low";
    else if (cvent > 0 && hbx === 0) confidence = "medium";

    // Brazil etc. have huge Cvent stock but missing city → expected is upper-bound discovery pool
    if (holds > census * 2 && hbx === 0) confidence = "medium";

    const flag = coverageFlagFromPct(coveragePct, confidence);
    const gap = expected != null ? Math.max(0, expected - census) : null;
    const priority = priorityFromFlag(flag, gap || 0);

    rows.push({
      country,
      hotels_in_dealality: census,
      expected_approximate_universe: expected,
      coverage_pct: coveragePct,
      confidence,
      priority,
      flag,
      sources: {
        cvent_candidates: cvent,
        hbx_candidates: hbx,
        weak_holds: holds,
        zero_record_in_geography_audit: zeroGeos.has(country),
      },
      gap_estimate: gap,
    });
  }

  rows.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    const ga = a.gap_estimate ?? -1;
    const gb = b.gap_estimate ?? -1;
    if (gb !== ga) return gb - ga;
    return String(a.country).localeCompare(String(b.country));
  });

  const totalCensus = rows.reduce((s, r) => s + r.hotels_in_dealality, 0);
  const totalExpected = rows.reduce(
    (s, r) => s + (r.expected_approximate_universe || 0),
    0
  );

  return {
    version: COVERAGE_SCORECARD_VERSION,
    generated_at: new Date().toISOString(),
    total_hotels_in_dealality: totalCensus,
    total_expected_from_known_sources: totalExpected,
    flag_counts: Object.values(COVERAGE_FLAG).reduce((acc, f) => {
      acc[f] = rows.filter((r) => r.flag === f).length;
      return acc;
    }, {}),
    rows,
  };
}
