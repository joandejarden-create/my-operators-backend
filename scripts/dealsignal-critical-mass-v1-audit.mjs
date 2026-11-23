#!/usr/bin/env node
/**
 * DealSignal Commercial Critical-Mass V1 — coverage audit (read-only).
 * Aggregates RESEARCH_CANDIDATES across DealSignal waves; no Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/dealsignal-critical-mass-v1");
fs.mkdirSync(OUT, { recursive: true });

const candidateFiles = [
  "reports/dealsignal-discovery-experiment-v2/RESEARCH_CANDIDATES.json",
  "reports/dealsignal-early-conversion-intelligence-v1/RESEARCH_CANDIDATES.json",
  "reports/dealsignal-owner-portfolio-early-warning-v2/RESEARCH_CANDIDATES.json",
  "reports/dealsignal-owner-portfolio-early-warning-v2/RESEARCH_CANDIDATES_CONTINUATION.json",
  "reports/dealsignal-global-source-cala-discovery-v1/RESEARCH_CANDIDATES.json",
  "reports/dealsignal-opportunity-discovery-v3/RESEARCH_CANDIDATES.json",
  "reports/dealsignal-opportunity-discovery-v3/RESEARCH_CANDIDATES_OWNER_DIVERSIFICATION.json",
  "reports/dealsignal-discovery-v4/RESEARCH_CANDIDATES.json",
  "reports/dealsignal-critical-mass-v1/RESEARCH_CANDIDATES.json",
];

const firstApplyLedgers = [
  "reports/dealsignal-discovery-experiment-v2/PERSISTENCE_LEDGER_FIRST_APPLY.json",
  "reports/dealsignal-early-conversion-intelligence-v1/PERSISTENCE_LEDGER_FIRST_APPLY.json",
  "reports/dealsignal-owner-portfolio-early-warning-v2/PERSISTENCE_LEDGER_FIRST_APPLY.json",
  "reports/dealsignal-owner-portfolio-early-warning-v2/PERSISTENCE_LEDGER_FIRST_APPLY_CONTINUATION.json",
  "reports/dealsignal-global-source-cala-discovery-v1/PERSISTENCE_LEDGER_FIRST_APPLY.json",
  "reports/dealsignal-opportunity-discovery-v3/PERSISTENCE_LEDGER_FIRST_APPLY.json",
  "reports/dealsignal-opportunity-discovery-v3/PERSISTENCE_LEDGER_OD_FIRST_APPLY.json",
  "reports/dealsignal-discovery-v4/PERSISTENCE_LEDGER_FIRST_APPLY.json",
  "reports/dealsignal-critical-mass-v1/PERSISTENCE_LEDGER_FIRST_APPLY.json",
];

function normOwner(o) {
  return String(o || "UNKNOWN")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function statusBucket(c) {
  const s = String(c.commercialStatus || "").toUpperCase();
  if (s.includes("VERIFIED") && !s.includes("COMMIT")) return "VERIFIED";
  if (s === "EMERGING") return "EMERGING";
  if (s.includes("EARLY") || c.recordType === "EARLY_ENGAGEMENT" || c.engagementPriority === "BUILD_RELATIONSHIP") {
    if (s === "WATCHLIST") return "WATCHLIST";
    if (s === "EMERGING") return "EMERGING";
    if (s.includes("COMMIT") || s.includes("CLOSED")) return "COMMITTED_CLOSED";
    return "EARLY_ENGAGEMENT";
  }
  if (s === "WATCHLIST") return "WATCHLIST";
  if (s.includes("COMMIT") || s.includes("CLOSED") || s === "COMMITTED") return "COMMITTED_CLOSED";
  if (c.stream === "HISTORICAL" || String(c.recordType || "").includes("HISTORICAL")) return "HISTORICAL";
  return s || "OTHER";
}

const all = [];
const byWave = {};
for (const rel of candidateFiles) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) continue;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const wave = j.wave || path.basename(path.dirname(rel));
  const cands = j.candidates || [];
  byWave[wave] = (byWave[wave] || 0) + cands.length;
  for (const c of cands) {
    all.push({
      ...c,
      _wave: wave,
      _file: rel,
      _ownerKey: normOwner(c.owner || c.decisionParty),
      _country: c.physicalLocation?.country || c.country || null,
      _findingId: c.findingId,
      _discId: c.discoveryPropertyId,
      _bucket: statusBucket(c),
    });
  }
}

const findingIds = new Set();
const discIds = new Set();
const owners = new Map();
const countries = new Map();
const buckets = {};
let withSources = 0;
let withExecs = 0;
let engageNow = 0;

for (const c of all) {
  if (c._findingId) findingIds.add(c._findingId);
  if (c._discId) discIds.add(c._discId);
  owners.set(c._ownerKey, (owners.get(c._ownerKey) || 0) + 1);
  if (c._country) countries.set(c._country, (countries.get(c._country) || 0) + 1);
  buckets[c._bucket] = (buckets[c._bucket] || 0) + 1;
  if (Array.isArray(c.sources) && c.sources.length) withSources += 1;
  if (Array.isArray(c.executives) && c.executives.length) withExecs += 1;
  if (c.engagementPriority === "ENGAGE_NOW") engageNow += 1;
}

const ownerRank = [...owners.entries()].sort((a, b) => b[1] - a[1]);
const top5Share = ownerRank.slice(0, 5).reduce((s, [, n]) => s + n, 0) / Math.max(all.length, 1);

let persistCreates = 0;
const persistByWave = {};
for (const rel of firstApplyLedgers) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) continue;
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const creates = j.summary?.creates || 0;
  persistCreates += creates;
  persistByWave[j.wave || rel] = creates;
}

const methods = {
  executableAndProven: [
    "OWNER_FIRST_IR_DISCLOSURES",
    "BRAND_OPERATOR_FIRST_ANNOUNCEMENTS",
    "DEVELOPER_FIRST_DESTINATION_PROJECTS",
    "HISTORICAL_OUTCOME_FIRST_CHRONOLOGY",
    "PORTFOLIO_FIRST_PARENT_CHILD",
    "AIRTABLE_PERSIST_DRY_APPLY_IDEMPOTENT",
    "SOURCE_REGISTRY_CALA_PRIORITY_LEXICON",
  ],
  partiallyUsed: [
    "CONTRACT_FIRST_VIA_TRACK_B_HANDOFF",
    "TRANSACTION_FIRST_ACQUISITIONS",
    "SEC_EDGAR_EXISTING_ADAPTERS",
  ],
  underutilizedOrHighFP: [
    "PUBLIC_PROCUREMENT_COLD_SEARCH",
    "CMBS_ANNEX_SWEEP",
    "BRAZIL_FII_BROADER",
    "JAMAICA_PR_BAHAMAS_OWNERS",
    "PAID_EXA_SEARCH",
  ],
  notImplementedFullAutomation: [
    "RECURRING_SCHEDULER",
    "FULL_HIF_ORCHESTRATOR",
    "SURFE_CONTACT_ENRICHMENT",
  ],
};

const prospectiveQualified =
  (buckets.EMERGING || 0) + (buckets.EARLY_ENGAGEMENT || 0);
const nearTermStrong = engageNow + Math.min(buckets.EMERGING || 0, 15);
const historicalish =
  (buckets.HISTORICAL || 0) +
  (buckets.COMMITTED_CLOSED || 0) +
  all.filter((c) => String(c.recordType || "").includes("HISTORICAL") || String(c.recordType || "").includes("RETENTION")).length;

const scorecard = {
  targets: {
    prospectiveOrEarly: "100-150",
    strongNearTerm: "25-40",
    distinctOwners: "40-60",
    historicalOrRetention: "100+",
    citationCoverageMaterialClaims: "high",
    usefulnessSample: "80%+",
    repeatableWaves: 2,
  },
  actual: {
    researchCandidateRows: all.length,
    uniqueFindingIds: findingIds.size,
    uniqueDiscoveryPropertyIds: discIds.size,
    uniqueOwnersApprox: owners.size,
    countriesRepresented: countries.size,
    buckets,
    prospectiveEmergingPlusEarly: prospectiveQualified,
    engageNow: engageNow,
    nearTermStrongProxy: nearTermStrong,
    historicalPlusCommittedProxy: historicalish,
    withSources: withSources,
    sourceCoveragePct: Math.round((withSources / Math.max(all.length, 1)) * 100),
    withExecutives: withExecs,
    top5OwnerShareOfCandidates: Math.round(top5Share * 1000) / 10,
    top5Owners: ownerRank.slice(0, 5).map(([o, n]) => ({ owner: o, n })),
    airtableFirstApplyCreatesTotal: persistCreates,
    note: "Candidate rows ≠ unique commercial decisions; portfolio parents counted once in thesis but may span hotels. Watchlist excluded from prospectiveQualified.",
  },
  readiness: {
    prospectiveOrEarly: prospectiveQualified >= 100 ? "MET" : "GAP",
    strongNearTerm: nearTermStrong >= 25 ? "MET" : "GAP",
    distinctOwners: owners.size >= 40 ? "MET" : "GAP",
    historicalOrRetention: historicalish >= 100 ? "MET" : "GAP",
    citationCoverage: withSources / Math.max(all.length, 1) >= 0.8 ? "MET" : "PARTIAL",
    repeatableWaves: "MET",
    overall: "BELOW_CRITICAL_MASS_CONTINUE_EXPANSION",
  },
};

const report = {
  asOf: new Date().toISOString(),
  wave: "DEALSIGNAL_CRITICAL_MASS_V1_AUDIT",
  methods,
  byWaveCandidateCounts: byWave,
  persistByWave,
  coverage: scorecard.actual,
  scorecard,
  ownerRank: ownerRank.slice(0, 40).map(([o, n]) => ({ owner: o, n })),
  countries: [...countries.entries()].sort((a, b) => b[1] - a[1]),
};

fs.writeFileSync(path.join(OUT, "COVERAGE_AUDIT.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify({
  candidates: all.length,
  owners: owners.size,
  countries: countries.size,
  buckets,
  prospectiveEmergingPlusEarly: prospectiveQualified,
  persistCreates,
  readiness: scorecard.readiness,
}, null, 2));
