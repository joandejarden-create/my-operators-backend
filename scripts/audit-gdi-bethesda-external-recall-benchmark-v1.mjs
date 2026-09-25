#!/usr/bin/env node
/**
 * Bethesda external golden-benchmark recall audit — EVALUATION ONLY.
 * Does NOT seed benchmark names into production GDI.
 *
 *   node scripts/audit-gdi-bethesda-external-recall-benchmark-v1.mjs
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";

const HOTEL_ID = "recLuxvwwxID7U2B8";
const OPP_PATH = join(
  process.cwd(),
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json"
);
const SEEDS_PATH = join(
  process.cwd(),
  "fixtures/group-demand-intelligence/demand-generators/bethesda-seeds.mjs"
);

/** Evaluation-only benchmark — never imported by production discovery. */
export const BETHESDA_EXTERNAL_RECALL_BENCHMARK_V1 = Object.freeze([
  { id: "b1", event: "2027 TOPMed Annual Meeting", orgHints: ["TOPMed", "Trans-Omics"], year: 2027 },
  { id: "b2", event: "2027 CTN Annual Conference", orgHints: ["CTN", "Clinical Trials Network", "NIDA CTN"], year: 2027 },
  { id: "b3", event: "2027 NINDS CTE Summit", orgHints: ["NINDS", "CTE Summit"], year: 2027 },
  { id: "b4", event: "2027 High-Risk, High-Reward Research Symposium", orgHints: ["High-Risk", "High-Reward", "HRHR", "NIH Common Fund"], year: 2027 },
  { id: "b5", event: "AMWA 112th Annual Meeting — 2027", orgHints: ["AMWA", "American Medical Women's Association"], year: 2027, seriesHint: "annual meeting" },
  { id: "b6", event: "AMWA 113th Annual Meeting — 2028", orgHints: ["AMWA"], year: 2028, seriesHint: "annual meeting" },
  { id: "b7", event: "iCAN 2027 Summit", orgHints: ["iCAN"], year: 2027 },
  { id: "b8", event: "AAOS 2027 Combined NOLC/Fall Meeting", orgHints: ["AAOS", "NOLC"], year: 2027 },
  { id: "b9", event: "AAPA 2027 Leadership & Advocacy Summit", orgHints: ["AAPA", "Physician Assistants"], year: 2027 },
  { id: "b10", event: "AAPA Hospital Medicine Essentials 2027", orgHints: ["AAPA", "Hospital Medicine"], year: 2027 },
  { id: "b11", event: "HFA Fly-In & Advocacy Summit 2027", orgHints: ["HFA", "Healthcare Financial"], year: 2027 },
  { id: "b12", event: "Fuel Medical Symposium 2027", orgHints: ["Fuel Medical"], year: 2027 },
  { id: "b13", event: "PSC FedHealth Conference 2027", orgHints: ["FedHealth", "PSC"], year: 2027 },
  { id: "b14", event: "Federal Quantum Readiness Forum 2027", orgHints: ["Quantum Readiness", "Federal Quantum"], year: 2027 },
  { id: "b15", event: "26th Crabtown Showdown — 2027", orgHints: ["Crabtown Showdown"], year: 2027 },
  { id: "b16", event: "18th Capital Showdown — 2027", orgHints: ["Capital Showdown"], year: 2027 },
]);

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function loadOpportunities() {
  if (!existsSync(OPP_PATH)) return [];
  const doc = JSON.parse(readFileSync(OPP_PATH, "utf8"));
  const rows = doc.opportunities || doc.rows || doc.items || doc;
  return Array.isArray(rows) ? rows : [];
}

function oppText(o) {
  return [
    o.title,
    o.name,
    o.eventName,
    o.organization,
    o.orgName,
    o.demandGenerator,
    o.eventSeriesId,
    o.eventCycleId,
    o.summary,
  ]
    .map(norm)
    .join(" ");
}

function classifyMatch(bench, opps) {
  const hits = [];
  for (const o of opps) {
    const text = oppText(o);
    const orgHit = (bench.orgHints || []).some((h) => text.includes(norm(h)));
    const yearHit =
      !bench.year ||
      text.includes(String(bench.year)) ||
      String(o.eventYear || o.year || "") === String(bench.year) ||
      (o.eventCycleId && String(o.eventCycleId).includes(String(bench.year)));
    const titleBits = norm(bench.event)
      .split(" ")
      .filter((w) => w.length > 3 && !["annual", "meeting", "summit", "conference", "2027", "2028"].includes(w));
    const titleHit = titleBits.filter((w) => text.includes(w)).length >= Math.min(2, titleBits.length);
    if (orgHit && (yearHit || titleHit)) {
      hits.push(o);
    } else if (titleHit && yearHit && orgHit) {
      hits.push(o);
    }
  }
  // stricter: require org hint + (year or strong title)
  const strict = hits.filter((o) => {
    const text = oppText(o);
    return (bench.orgHints || []).some((h) => text.includes(norm(h)));
  });
  return strict.length ? strict : [];
}

function partitionOf(o) {
  return (
    o.salesPartitionV11 ||
    o.customerFacingState ||
    o.priority ||
    o.status ||
    "UNKNOWN"
  );
}

function matchState(hits) {
  if (!hits.length) return "NOT_FOUND";
  const parts = hits.map(partitionOf);
  const customerish = parts.some((p) =>
    /ACTIONABLE|WATCH|FUTURE_WATCH|TRUE|OPEN/i.test(String(p))
  );
  const closed = parts.every((p) => /CLOSED|INSUFFICIENT|REJECT/i.test(String(p)));
  if (customerish && !closed) {
    if (parts.some((p) => /ACTIONABLE/i.test(String(p)))) return "FOUND_CUSTOMER_VISIBLE";
    if (parts.some((p) => /WATCH|FUTURE_WATCH/i.test(String(p)))) return "FOUND_CUSTOMER_VISIBLE";
    return "FOUND_INTERNAL_ONLY";
  }
  return "FOUND_INTERNAL_ONLY";
}

function seedsMention(bench) {
  if (!existsSync(SEEDS_PATH)) return false;
  const raw = readFileSync(SEEDS_PATH, "utf8");
  return (bench.orgHints || []).some((h) => raw.toLowerCase().includes(h.toLowerCase()));
}

function rootCause(bench, state) {
  if (state !== "NOT_FOUND") return null;
  if (seedsMention(bench)) return "SEARCHED_BUT_EXTRACTION_FAILED_OR_CYCLE_GAP";
  // NIH-campus scientific / association / sports patterns not in 20-org seed
  if (/TOPMed|CTN|NINDS|HRHR|High-Risk/i.test(bench.event + (bench.orgHints || []).join(" "))) {
    return "TARGET_TOO_NARROW"; // NIH-adjacent scientific series beyond seeded NIH programs
  }
  if (/AAPA|AAOS|HFA|Fuel|FedHealth|Quantum|iCAN/i.test((bench.orgHints || []).join(" "))) {
    return "NO_TARGET";
  }
  if (/Crabtown|Capital Showdown/i.test(bench.event)) {
    return "NO_TARGET"; // sports tournament series beyond seeded soccer clubs
  }
  return "NOT_SEARCHED";
}

const opps = loadOpportunities();
const rows = [];
for (const b of BETHESDA_EXTERNAL_RECALL_BENCHMARK_V1) {
  const hits = classifyMatch(b, opps);
  const state = matchState(hits);
  const rc = rootCause(b, state === "NOT_FOUND" ? "NOT_FOUND" : state);
  rows.push({
    id: b.id,
    event: b.event,
    year: b.year,
    orgHints: b.orgHints,
    matchState: state === "NOT_FOUND" ? "NOT_SEARCHED_OR_MISSED" : state,
    foundTitles: hits.map((h) => h.title || h.name),
    partitions: hits.map(partitionOf),
    seedOrgPresent: seedsMention(b),
    rootCauseHypothesis: state === "NOT_FOUND" || state === "NOT_SEARCHED_OR_MISSED" ? rootCause(b, "NOT_FOUND") : null,
  });
}

// refine matchState label
for (const r of rows) {
  if (r.foundTitles.length === 0) {
    r.matchState = r.seedOrgPresent ? "SEARCHED_BUT_NOT_EXTRACTED" : "NOT_SEARCHED";
    r.rootCauseHypothesis = r.seedOrgPresent
      ? "EVENT_SERIES_GAP"
      : r.rootCauseHypothesis || "NO_TARGET";
  }
}

const found = rows.filter((r) => r.foundTitles.length > 0);
const missed = rows.filter((r) => !r.foundTitles.length);
const customerVisible = found.filter((r) => r.matchState === "FOUND_CUSTOMER_VISIBLE");

const causeDist = {};
for (const r of missed) {
  const c = r.rootCauseHypothesis || "OTHER";
  causeDist[c] = (causeDist[c] || 0) + 1;
}

const report = {
  ok: true,
  audit: "GDI_BETHESDA_EXTERNAL_RECALL_BENCHMARK_V1",
  hotelId: HOTEL_ID,
  hotelName: "Bethesda Marriott",
  evaluationOnly: true,
  doNotSeedIntoProduction: true,
  opportunityBagCount: opps.length,
  benchmarkCount: rows.length,
  // Validity deferred to founder report after public revalidation; denominator starts as all 16
  provisionalValidDenominator: rows.length,
  before: {
    foundAnywhere: found.length,
    foundCustomerVisible: customerVisible.length,
    missed: missed.length,
    recallAnywherePct: Math.round((found.length / rows.length) * 1000) / 10,
    recallCustomerVisiblePct: Math.round((customerVisible.length / rows.length) * 1000) / 10,
  },
  rootCauseDistribution: causeDist,
  rows,
  architectureNotes: {
    knownTargetMonitoring: "weekly-discovery-orchestrator over Research Targets (~50 backfilled)",
    openUniverseExists: true,
    openUniverseEntry: "open-universe-query-generator + GDI-OPEN-UNIVERSE-01 (periodic cadence)",
    eventSeriesIds: "present on bag via buildEventSeriesIdentity + GDI-FUTURE-CAL-01",
    destinationTbdEnum: "DESTINATION_TBD / HOUSING_PENDING / REGISTRATION_OPEN_HOTEL_UNANNOUNCED + FIXED_VENUE_OPEN_HOUSING",
    amwaInSeeds: true,
    nihScientificSeriesBeyondSeeds: "TOPMed/CTN/NINDS/HRHR not in demand-generator seeds — covered by fixed-venue housing query class",
  },
  timestamp: new Date().toISOString(),
};

const outDir = join(process.cwd(), "reports/group-demand-intelligence/external-recall-benchmark-v1");
mkdirSync(outDir, { recursive: true });
const out = join(outDir, "BETHESDA_EXTERNAL_RECALL_BEFORE.json");
writeFileSync(out, JSON.stringify(report, null, 2));
console.log(JSON.stringify({
  bag: opps.length,
  found: found.length,
  customerVisible: customerVisible.length,
  missed: missed.length,
  recallPct: report.before.recallAnywherePct,
  causes: causeDist,
  foundEvents: found.map((f) => f.event),
  out,
}, null, 2));
