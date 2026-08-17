/**
 * Autopilot V1 research prioritization engine.
 * Score = BUSINESS_RELEVANCE × MATERIAL_INCOMPLETENESS × STALENESS × CROSS_TABLE_RISK × RESEARCHABILITY
 * Not alphabetical.
 */

import { PRIORITY_BAND } from "./constants.js";

const CALA_COUNTRIES = new Set([
  "Mexico",
  "Puerto Rico",
  "Dominican Republic",
  "Costa Rica",
  "Panama",
  "Colombia",
  "Guatemala",
  "Honduras",
  "El Salvador",
  "Nicaragua",
  "Belize",
  "Jamaica",
  "Bahamas",
  "Cuba",
  "Trinidad and Tobago",
  "Barbados",
  "Aruba",
  "Curacao",
  "Curaçao",
]);

/**
 * @param {object} record VIC / independent index row
 * @param {object} [ctx]
 */
export function scoreRecordPriority(record, ctx = {}) {
  const country = String(record.country || "");
  const family = String(record.family || "");
  const materialPct = Number(record.material_pct ?? 50);
  const corePct = Number(record.core_pct ?? 100);
  const status = String(record.status || record.reconstruction_status || "");

  // BUSINESS RELEVANCE (0.2–1.0)
  let business = 0.45;
  if (country === "Mexico") business += 0.25;
  else if (CALA_COUNTRIES.has(country)) business += 0.18;
  if (["IHG", "Hilton", "Choice", "Marriott"].includes(family)) business += 0.12;
  if (/pipeline|opening|future|under construction/i.test(status)) business += 0.15;
  if (ctx.activeOpportunity) business += 0.1;
  if (ctx.brandExplorerActivationValue) business += 0.08;
  business = clamp(business, 0.2, 1);

  // MATERIAL INCOMPLETENESS (0.2–1.0) — higher when more incomplete
  const incompleteness = clamp(1 - materialPct / 100, 0.15, 1);
  const coreGap = corePct < 100 ? 1.15 : 1;

  // STALENESS (0.3–1.0)
  let staleness = 0.55;
  if (record.last_verified) {
    const days = daysSince(record.last_verified);
    if (days > 365) staleness = 1;
    else if (days > 180) staleness = 0.85;
    else if (days > 90) staleness = 0.7;
    else staleness = 0.4;
  } else {
    staleness = 0.75; // never verified → treat as stale-ish
  }

  // CROSS-TABLE RISK
  let crossRisk = 0.4;
  if (record.cross_table_contradiction) crossRisk = 1;
  else if (record.reconstruction_status === "Hold — Evidence Conflict") crossRisk = 0.95;
  else if (record.page_source_state === "Blocked" || record.page_source_state === "Unavailable") {
    crossRisk = 0.7;
  } else if (materialPct < 50) crossRisk = 0.65;

  // RESEARCHABILITY — structured families score higher
  let researchability = 0.5;
  if (["IHG", "Hilton", "Choice"].includes(family)) researchability = 0.95;
  else if (family === "Marriott") researchability = 0.7;
  else if (["Accor", "Wyndham", "Hyatt"].includes(family)) researchability = 0.75;
  if (record.page_source_state === "Blocked") researchability *= 0.55;

  const raw =
    business * incompleteness * coreGap * staleness * crossRisk * researchability;

  const band = bandFromScore(raw);
  return {
    score: Number(raw.toFixed(4)),
    band,
    factors: {
      business_relevance: Number(business.toFixed(3)),
      material_incompleteness: Number(incompleteness.toFixed(3)),
      staleness: Number(staleness.toFixed(3)),
      cross_table_risk: Number(crossRisk.toFixed(3)),
      researchability: Number(researchability.toFixed(3)),
      core_gap_multiplier: coreGap,
    },
  };
}

function bandFromScore(raw) {
  if (raw >= 0.28) return PRIORITY_BAND.P0;
  if (raw >= 0.16) return PRIORITY_BAND.P1;
  if (raw >= 0.08) return PRIORITY_BAND.P2;
  return PRIORITY_BAND.P3;
}

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function daysSince(iso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return 999;
  return (Date.now() - t) / (86400 * 1000);
}

/**
 * @param {object[]} records
 * @param {object} [opts]
 */
export function prioritizeQueue(records, opts = {}) {
  const bands = opts.priorityBands || null; // e.g. ["P0 Critical","P1 High"]
  const scored = (records || []).map((r) => {
    const pri = scoreRecordPriority(r, opts.ctx || {});
    return { ...r, priority: pri };
  });
  scored.sort((a, b) => b.priority.score - a.priority.score);
  if (!bands || !bands.length) return scored;
  const set = new Set(bands.map(normalizeBand));
  return scored.filter((r) => set.has(normalizeBand(r.priority.band)));
}

function normalizeBand(b) {
  const s = String(b || "");
  if (/^p0/i.test(s)) return PRIORITY_BAND.P0;
  if (/^p1/i.test(s)) return PRIORITY_BAND.P1;
  if (/^p2/i.test(s)) return PRIORITY_BAND.P2;
  if (/^p3/i.test(s)) return PRIORITY_BAND.P3;
  return s;
}
