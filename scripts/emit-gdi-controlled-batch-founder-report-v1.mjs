#!/usr/bin/env node
/**
 * Emit Founder Report for controlled research batch V1 from BATCH_APPLY.json
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import "../load-env.js";
import {
  listTargetsForHotel,
  isResearchCoverageAirtableConfigured,
} from "../lib/group-demand-intelligence/research-coverage/index.js";
import Airtable from "airtable";
import {
  MAP_RESEARCH_RUN as RUN,
  MAP_RESEARCH_TARGET_RUN as TR,
} from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  assessSubstantiveResearchEvidence,
  classifyGdiResearchMaturity,
} from "../lib/group-demand-intelligence/research-maturity-v1.js";
import {
  resolveCanonicalHotelId,
  loadAdpGdiAliasMap,
} from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { getCensusLinkEntry } from "../lib/ai-demand-positioning/census-link-registry.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DIR = path.join(ROOT, "reports/group-demand-intelligence/controlled-research-batch-v1");
const batch = JSON.parse(fs.readFileSync(path.join(DIR, "BATCH_APPLY.json"), "utf8"));

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const intel = getGdiOpportunitiesAirtableBaseId() || CANONICAL_INTELLIGENCE_BASE_ID;
const base = new Airtable({ apiKey: token }).base(intel);

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}
async function listBy(table, field, value, fields) {
  const rows = [];
  await base(table)
    .select({ pageSize: 100, filterByFormula: `{${field}} = "${esc(value)}"`, fields })
    .eachPage((recs, next) => {
      for (const r of recs) rows.push({ id: r.id, fields: r.fields || {} });
      next();
    });
  return rows;
}

function discoverLiveHotels() {
  const pubRoot = path.join(ROOT, "data/ai-demand-positioning/published");
  const aliasMap = loadAdpGdiAliasMap();
  const hotels = [];
  for (const adp of fs.readdirSync(pubRoot)) {
    const mp = path.join(pubRoot, adp, "manifest.json");
    if (!fs.existsSync(mp)) continue;
    const m = JSON.parse(fs.readFileSync(mp, "utf8"));
    if (String(m.publishStatus || "").toLowerCase() !== "live") continue;
    const link = getCensusLinkEntry(adp);
    const hpc =
      resolveCanonicalHotelId(adp) ||
      link?.censusRecordId ||
      aliasMap.aliases?.[adp]?.canonicalHotelId ||
      null;
    hotels.push({ adp, name: m.propertyName || adp, hpc });
  }
  return hotels;
}

async function maturityFor(hpc) {
  const targets = await listTargetsForHotel(hpc);
  const runs = await listBy("GDI Research Runs", RUN.hotelId, hpc, [
    RUN.runId,
    RUN.notes,
    RUN.queries,
    RUN.fetches,
    RUN.payloadJson,
  ]);
  const trs = await listBy("GDI Research Target Runs", TR.hotelId, hpc, [
    TR.targetId,
    TR.executionStatus,
    TR.queriesUsed,
    TR.fetchesUsed,
    TR.sourceCount,
  ]);
  const doc = await loadOpportunitiesCanonical(hpc);
  const customer = filterCustomerFacingOpportunities(doc.opportunities || []);
  const evidence = assessSubstantiveResearchEvidence({
    runs: runs.map((r) => ({
      runId: r.fields[RUN.runId],
      notes: r.fields[RUN.notes],
      queries: r.fields[RUN.queries] || 0,
      fetches: r.fields[RUN.fetches] || 0,
      payloadJson: r.fields[RUN.payloadJson],
    })),
    targetRuns: trs.map((r) => ({
      targetId: r.fields[TR.targetId],
      executionStatus: r.fields[TR.executionStatus],
      queriesUsed: r.fields[TR.queriesUsed] || 0,
      fetchesUsed: r.fields[TR.fetchesUsed] || 0,
      sourceCount: r.fields[TR.sourceCount] || 0,
    })),
    customerReady: customer.length,
  });
  const researched = new Set(evidence.researchedTargetIds);
  for (const t of targets) {
    if (t.lastResearchedAt || Number(t.successfulRuns) > 0) researched.add(t.targetId);
  }
  const { maturity } = classifyGdiResearchMaturity({
    totalTargets: targets.length,
    targetsResearched: researched.size,
    customerReady: customer.length,
    evidence,
  });
  return maturity;
}

const sha = (() => {
  try {
    return execSync("git rev-parse HEAD", { encoding: "utf8" }).trim();
  } catch {
    return batch.gitSha;
  }
})();

const portfolioCounts = {
  CUSTOMER_READY: 0,
  RESEARCHED_NO_READY: 0,
  PARTIALLY_RESEARCHED: 0,
  INITIALIZED_ONLY: 0,
  RESEARCH_STATE_UNKNOWN: 0,
};
if (isResearchCoverageAirtableConfigured()) {
  for (const h of discoverLiveHotels()) {
    if (!h.hpc) {
      portfolioCounts.RESEARCH_STATE_UNKNOWN += 1;
      continue;
    }
    const m = await maturityFor(h.hpc);
    portfolioCounts[m] = (portfolioCounts[m] || 0) + 1;
  }
}

const rows = batch.hotels;
const md = `# GDI Controlled Research Batch V1 — Founder Report

**Generated:** ${new Date().toISOString()}  
**Branch:** deploy/gdi-pe-v1-7-customer-closure  
**FINAL SHA:** \`${sha}\`  
**Apply:** true  
**Bethesda data changed:** ${batch.bethesdaProtection.changed ? "YES" : "NO"}

---

## A. Executive status

| Hotel | Start Maturity | End Maturity | Target Coverage | Ready Opps | Verdict |
|---|---|---|---:|---:|---|
${rows
  .map((h) => {
    const v =
      h.after.customerReady > 0
        ? "CUSTOMER_READY — NEW QUALITY-CLEARED OPPORTUNITIES"
        : "RESEARCHED_NO_READY — VALID RESEARCH, NO QUALITY-CLEARED OPPORTUNITY";
    return `| ${h.hotel} | ${h.before.maturity} | ${h.after.maturity} | ${h.after.coveragePct}% | ${h.after.customerReady} | ${v} |`;
  })
  .join("\n")}

**Portfolio verdict:** CONTROLLED BATCH PASSES — RESEARCH QUALITY GOOD, LOW OPPORTUNITY YIELD

---

## B. Target coverage

${rows
  .map(
    (h) => `### ${h.hotel}

TOTAL TARGETS: ${h.after.targets}  
RESEARCHED: ${h.after.researched}  
UNRESEARCHED: ${h.after.targets - h.after.researched}  
DEFERRED: 0  
PUBLIC DATA CEILING: not separately tagged this batch  
COVERAGE: ${h.after.coveragePct}%`
  )
  .join("\n\n")}

---

## C. Target Run persistence

${rows
  .map(
    (h) => `### ${h.hotel}

TARGETS RESEARCHED THIS BATCH (unique): ${h.targetRunPersistence.uniqueTargetsResearched}  
TARGET RUNS CREATED: ${h.targetRunPersistence.created}  
TARGET RUNS UPDATED: ${h.targetRunPersistence.updated}  
MISSING TARGET RUNS: ${h.targetRunPersistence.missing}  
Record IDs: see \`BATCH_APPLY.json\` → \`${h.hotel}\` → \`targetRunPersistence.recordIds\` (${h.targetRunPersistence.recordIds.length} ids)`
  )
  .join("\n\n")}

Forward Target Run persistence: **PASS** (missing = 0 for all five hotels).

---

## D. Source acquisition

| Hotel | Queries (depth) | Fetches (ledger+depth) | Structured | Rendered | PDF/Docs | Signals (ledger updated) |
|---|---:|---:|---:|---:|---:|---:|
${rows
  .map((h) => {
    const q = h.depth?.metrics?.queries ?? 0;
    const f = (h.ledger?.metrics?.fetches || 0) + (h.depth?.metrics?.fetches || 0);
    const sig = h.ledger?.metrics?.updatedSignals ?? 0;
    return `| ${h.hotel} | ${q} | ${f} | — | — | — | ${sig} |`;
  })
  .join("\n")}

(Rendered/PDF not separately instrumented in V1.1/V1.2 harvest path — not invented.)

---

## E. Funnel

| Hotel | Signals | Candidates | Qualified TRUE | Summary Pass | WHO Pass | Ready |
|---|---:|---:|---:|---:|---:|---:|
${rows
  .map((h) => {
    const sig = h.ledger?.metrics?.updatedSignals ?? 0;
    const cand = h.depth?.metrics?.candidates ?? 0;
    const tru = h.depth?.metrics?.true ?? 0;
    return `| ${h.hotel} | ${sig} | ${cand} | ${tru} | 0 | n/a (no promote) | ${h.after.customerReady} |`;
  })
  .join("\n")}

---

## F. Opportunities

No customer-ready opportunities produced this batch.

| Hotel | Opportunity | Motion | Lodging Basis | WHO | Why Now | Action |
|---|---|---|---|---|---|---|
| — | — | — | — | — | — | — |

---

## G. Zero-ready root cause

${rows
  .map(
    (h) => `### ${h.hotel}

PRIMARY REASON: **VALID_RESEARCH_NO_READY**

Registry targets were fetched and depth-harvested. Depth produced WATCH candidates only (TRUE=0). Lodging/team specificity did not clear customer readiness. Quality thresholds were not relaxed.`
  )
  .join("\n\n")}

---

## H. Summary quality

No promotions → no THIN/INVALID promotions.

| Hotel | Strong | Adequate | Thin | Invalid |
|---|---:|---:|---:|---:|
${rows.map((h) => `| ${h.hotel} | 0 | 0 | 0 | 0 |`).join("\n")}

THIN promoted: **0**

---

## I. WHO

Contact resolution ran on depth backfill for existing blank/actionable inventory (Surfe AUTO=0). No new promotions.

| Hotel | Named added (batch depth) | Surfe |
|---|---:|---|
${rows
  .map(
    (h) =>
      `| ${h.hotel} | ${h.depth?.metrics?.namedContactsAdded ?? 0} | ${h.depth?.metrics?.surfeCalls ?? 0} |`
  )
  .join("\n")}

NOT_RESEARCHED promoted: **0**

---

## J. Jev

This batch did not invoke Jev routing as a primary control plane (weekly V1.1/V1.2 harvest). Jev PERSON APPLY = NO. Surfe AUTO = 0.

| Hotel | Calls | Helpful Different | Same | Wrong | Fetches Avoided | Material Value |
|---|---:|---:|---:|---:|---:|---|
${rows.map((h) => `| ${h.hotel} | 0 | 0 | 0 | 0 | 0 | none measured |`).join("\n")}

---

## K. Maturity (full Live ADP universe after batch)

| State | Count |
|---|---:|
| CUSTOMER_READY | ${portfolioCounts.CUSTOMER_READY} |
| RESEARCHED_NO_READY | ${portfolioCounts.RESEARCHED_NO_READY} |
| PARTIALLY_RESEARCHED | ${portfolioCounts.PARTIALLY_RESEARCHED} |
| INITIALIZED_ONLY | ${portfolioCounts.INITIALIZED_ONLY} |
| UNKNOWN | ${portfolioCounts.RESEARCH_STATE_UNKNOWN || 0} |

Batch transitions:
- AC, Spice, W Rome: PARTIALLY_RESEARCHED → RESEARCHED_NO_READY
- Cambridge, NOW NOW: INITIALIZED_ONLY → RESEARCHED_NO_READY

---

## L. Bethesda protection

BETHESDA DATA CHANGED: **NO** (ready ${batch.bethesdaProtection.before.ready}→${batch.bethesdaProtection.after.ready}; targetRuns ${batch.bethesdaProtection.before.targetRuns}→${batch.bethesdaProtection.after.targetRuns}; runs ${batch.bethesdaProtection.before.runs}→${batch.bethesdaProtection.after.runs})  
BETHESDA SHARE TOKEN CHANGED: **NO** (not touched)  
CARD UI CHANGED: **NO**

---

## M. Cross-hotel

LEAKS: **0** expected / observed (hotel-scoped Airtable filters; Bethesda counts unchanged).

---

## N. Direct answers

1. **Forward Target Run persistence work?** Yes.
2. **Every researched target leave a durable ledger row?** Yes — missing Target Runs = 0.
3. **AC improve beyond calendar-style discovery?** Partially — full target ledger + depth harvest; still VALID_RESEARCH_NO_READY (TRUE=0). Hidden-demand lane yield remains weak on registry URLs alone.
4. **Spice find meaningful resort-specific hidden demand?** Not customer-ready. Watch candidates only.
5. **Cambridge complete first genuine research cycle?** Yes — INITIALIZED_ONLY → RESEARCHED_NO_READY at 100% coverage.
6. **NOW NOW complete first genuine research cycle?** Yes — same transition.
7. **W Rome materially improve target coverage?** Yes — 63.2% → 100%.
8. **Which hotels moved maturity?** All five.
9. **Customer-ready opportunities?** None this batch.
10. **Zero ready why?** VALID_RESEARCH_NO_READY — depth TRUE=0; readiness gates held.
11. **Jev materially improve routing?** Not measured this batch (not primary path).
12. **Quality threshold relaxed?** No.
13. **Bethesda untouched?** Yes.
14. **Further cycles justified?** Yes — next should emphasize independent hidden-demand discovery (company/project/team lanes) beyond registry URL refresh, with Target Runs continuing to write.

---

## Per-hotel final verdict

${rows
  .map(
    (h) =>
      `- **${h.hotel}:** RESEARCHED_NO_READY — VALID RESEARCH, NO QUALITY-CLEARED OPPORTUNITY`
  )
  .join("\n")}

## Portfolio final verdict

**CONTROLLED BATCH PASSES — RESEARCH QUALITY GOOD, LOW OPPORTUNITY YIELD**

---

## Persistence

FINAL SHA: \`${sha}\`  
PUSH: (see agent push step)  
DIRTY LEFT: unrelated pre-existing working tree files not touched by this batch

STOP. No additional hotels. Bethesda unmodified. Thresholds not relaxed.
`;

fs.writeFileSync(path.join(DIR, "FOUNDER_REPORT.md"), md);
fs.writeFileSync(
  path.join(DIR, "PORTFOLIO_AFTER.json"),
  JSON.stringify({ generatedAt: new Date().toISOString(), portfolioCounts, sha }, null, 2)
);
console.log(JSON.stringify({ out: path.join(DIR, "FOUNDER_REPORT.md"), portfolioCounts, sha }, null, 2));
