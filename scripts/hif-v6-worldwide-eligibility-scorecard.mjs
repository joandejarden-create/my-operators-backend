#!/usr/bin/env node
/**
 * Emit dual-view geography scorecard from canonical inventory.
 * Usage: node scripts/hif-v6-worldwide-eligibility-scorecard.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildWeeklySnapshot,
  ensureWeeklyScorecardLayout,
  isoWeekId,
  saveWeeklySnapshot,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import {
  WORLDWIDE_ELIGIBILITY_POLICY_V6,
  normalizeHotelCountryCode,
  assertWorldwideEligibilityInvariants,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-worldwide-eligibility-v6.js";
import { emptyCountryScorecardRow } from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-worldwide-eligibility");
const asOf = new Date().toISOString().slice(0, 10);
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};

function load(rel) {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

const inv = assertWorldwideEligibilityInvariants();
if (!inv.ok) {
  console.error(JSON.stringify({ ok: false, violations: inv.violations }, null, 2));
  process.exit(1);
}

const toward = load("reports/sec-edgar-hif-v6-canonical-inventory/TOWARD_1000.json");
const rows = toward?.rows || [];
const byCountry = {};
for (const r of rows) {
  const c = normalizeHotelCountryCode(r.hotelCountry || r.country || "UNATTRIBUTED");
  if (!byCountry[c]) byCountry[c] = emptyCountryScorecardRow(c);
  byCountry[c].distinctEvidenceBackedProspects += 1;
  byCountry[c].distinctHotelsResearched += 1;
  if (r.status === "ACTIVE_DATED") {
    byCountry[c].verifiedContractualResearchOpportunities += 1;
  }
  if (r.status === "ACTIVE_UNDATED_EVIDENCE_BACKED") {
    byCountry[c].evidenceBackedCommercialProspectsUndated += 1;
  }
}

const weekId = `${isoWeekId(asOf)}-worldwide-eligibility`;
ensureWeeklyScorecardLayout(ROOT);
const snapshot = buildWeeklySnapshot({
  weekId,
  asOf,
  byCountry,
  global: {
    progressToward1000Opportunities: rows.length,
    evidenceBackedOpportunityCandidates: rows.length,
  },
  notes: {
    source: "canonical TOWARD_1000 reconcile",
    policy: WORLDWIDE_ELIGIBILITY_POLICY_V6.priorityMarketsControl,
  },
  allowOverwrite: true,
});
saveWeeklySnapshot({ ...snapshot, allowOverwrite: true }, ROOT);

writeJson(path.join(OUT, "POLICY.json"), WORLDWIDE_ELIGIBILITY_POLICY_V6);
writeJson(path.join(OUT, "DUAL_GEOGRAPHY_VIEWS.json"), {
  asOf,
  globalOpportunityInventory: snapshot.globalOpportunityInventory,
  priorityMarketPerformance: snapshot.priorityMarketPerformance,
});

const md = `# HIF — Worldwide Eligibility Acknowledgment

**As of:** ${asOf}  
**Policy:** Priority markets control **prioritization only**, not eligibility.

## Dual views (from current canonical toward inventory)

### Global Opportunity Inventory
- Countries represented: **${snapshot.globalOpportunityInventory.countryCount}**
- Total prospects in view: **${snapshot.globalOpportunityInventory.totalProspects}**

### Priority Market Performance
- US/MX/BR/GB/CA total: **${snapshot.priorityMarketPerformance.totalPriorityProspects}**

## Invariants
- Geographic eligibility exclusions: **NONE**
- Country allowlists: **PROHIBITED**
- Country rejection gates: **PROHIBITED**
- Opportunity ceilings by country: **PROHIBITED**
- Milestones 1k/2.5k/5k/10k: progress markers only

Japan, Spain, Colombia, Dominican Republic, Germany, UAE and all other countries remain fully eligible under the same evidence standards.
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_WORLDWIDE_ELIGIBILITY.md"), md, "utf8");

console.log(
  JSON.stringify(
    {
      ok: true,
      asOf,
      weekId,
      globalCountries: snapshot.globalOpportunityInventory.countryCount,
      globalProspects: snapshot.globalOpportunityInventory.totalProspects,
      priorityProspects: snapshot.priorityMarketPerformance.totalPriorityProspects,
      policy: WORLDWIDE_ELIGIBILITY_POLICY_V6.priorityMarketsControl,
    },
    null,
    2,
  ),
);
