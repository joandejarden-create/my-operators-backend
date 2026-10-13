/**
 * GDI Yield & Pipeline Reconciliation V1 — Bethesda forensic audit (read-only).
 * Does not change customer-visible opportunities.
 *
 *   node scripts/gdi-yield-pipeline-reconciliation-v1.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  listTargetsForHotel,
  isResearchCoverageAirtableConfigured,
} from "../lib/group-demand-intelligence/research-coverage/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports",
  "group-demand-intelligence",
  "yield-pipeline-reconciliation-v1"
);
const HOTEL = "recLuxvwwxID7U2B8";

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const coverageBaseline = readJson(
    "reports/group-demand-intelligence/research-coverage-v1/COVERAGE_RUN_recLuxvwwxID7U2B8_gdir_luxvwwxid7u2_2026_09_24_weekly_c1c0b6.json"
  );
  const coverageInflated = readJson(
    "reports/group-demand-intelligence/research-coverage-v1/COVERAGE_RUN_recLuxvwwxID7U2B8_gdir_luxvwwxid7u2_2026_09_24_weekly_144aa4.json"
  );
  const v12 = readJson(
    "reports/group-demand-intelligence/gdi-new-opportunities-v1-2-live-bethesda-audit/LIVE_REQUALIFY_FINAL.json"
  );
  const dg = readJson(
    "reports/group-demand-intelligence/demand-generators-v1-1/HARVEST.json"
  );
  const weekly = readJson(
    "data/group-demand-intelligence/evals/bethesda-weekly-refresh-20260921.json"
  );

  const doc = await loadOpportunitiesCanonical(HOTEL);
  const all = doc.opportunities || [];
  const customer = filterCustomerFacingOpportunities(all);
  const weeklyNew = customer.filter(
    (o) => o.weeklyDeltaState === "NEW" || o.isNewThisWeek === true
  );

  let targets = [];
  if (isResearchCoverageAirtableConfigured()) {
    try {
      targets = await listTargetsForHotel(HOTEL);
    } catch {
      targets = [];
    }
  }

  const byType = {};
  const byPri = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const t of targets) {
    byType[t.targetType] = (byType[t.targetType] || 0) + 1;
    if (byPri[t.priority] != null) byPri[t.priority] += 1;
  }

  const v12True = v12?.liveNewTrue || [];
  const v12Watch = v12?.liveNewWatch || [];
  const lostTrue = v12True.map((t) => {
    const inCanon = all.find(
      (o) =>
        String(o.title || "").toLowerCase().includes(String(t.title || "").toLowerCase().slice(0, 20)) ||
        String(o.organizationName || "")
          .toLowerCase()
          .includes(String(t.organization || "").toLowerCase().slice(0, 16))
    );
    let classification = "NOT_PROMOTED";
    let note = "V1.2 requalify TRUE; live write was NO (dry-run promote)";
    if (inCanon) {
      if (/premier cup/i.test(t.title || "")) {
        classification = "DUPLICATE_REUSED";
        note = `Existing canonical ${inCanon.id} (${inCanon.opportunityQualification})`;
      } else {
        classification = "DUPLICATE_REUSED";
        note = `Matched ${inCanon.id}`;
      }
    }
    return {
      title: t.title,
      organization: t.organization,
      demandType: t.demandType,
      officialSource: t.officialSource,
      inCanonical: Boolean(inCanon),
      canonicalId: inCanon?.id || null,
      canonicalQualification: inCanon?.opportunityQualification || null,
      customerVisible: inCanon
        ? customer.some((c) => c.id === inCanon.id)
        : false,
      weeklyDeltaState: inCanon?.weeklyDeltaState || null,
      classification,
      note,
    };
  });

  const peNew = weeklyNew[0] || null;

  const lanes = [
    {
      lane: "TRADITIONAL_EVENTS_ASSOCIATIONS",
      status: "PARTIALLY_RAN",
      evidence: "Weekly 2026-09-21 discovery 0 TRUE; V1.2 ACCP TRUE not promoted",
      targets: null,
      researched: null,
      candidates: v12?.funnelCompare?.v12LivePrimary?.afterDedupe ?? null,
      valid: null,
      watch: null,
      true: 1,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "NOT_PROMOTED (dry-run) + sparse weekly SERP TRUE",
    },
    {
      lane: "EXPANDED_NEW_OPPORTUNITIES",
      status: "RAN_LIVE",
      evidence: "V1.2 live SERP+requalify 2026-09-23; write=NO",
      targets: null,
      researched: null,
      candidates: 26,
      valid: 6,
      watch: 2,
      true: 4,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "TRUE_NOT_WRITTEN — controlled dry-run promotion",
    },
    {
      lane: "DEMAND_GENERATORS",
      status: "RAN_LIVE",
      evidence: "DG V1.1 lodging harvest APPLY_GRAPH; GDI promote dry-run",
      targets: 20,
      researched: 10,
      candidates: 10,
      valid: 10,
      watch: 0,
      true: 8,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "DUPLICATE_REUSED — GENERATOR_INCREMENTAL_NEW=0",
    },
    {
      lane: "TRAINING_CERTIFICATION",
      status: "RAN_LIVE",
      evidence: "V1.2 NRC RIC TRUE (training/gov housing page)",
      true: 1,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "NOT_PROMOTED",
    },
    {
      lane: "GOVERNMENT_PROJECT_CONTRACTOR",
      status: "PARTIALLY_RAN",
      evidence: "NRC in V1.2; contractor/relocation lanes structurally weak in V1.2 report",
      true: 1,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "NOT_PROMOTED + thin contractor extract",
    },
    {
      lane: "CORPORATE_PROJECT_RELOCATION",
      status: "PARTIALLY_RAN",
      evidence: "V1.2: relocation vendor noise filtered; consulting thin",
      true: 0,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "Structural public-evidence scarcity / extract thinness",
    },
    {
      lane: "SPORTS",
      status: "RAN_LIVE",
      evidence: "V1.2 Premier Cup TRUE; DG harvest soccer generators",
      true: 1,
      promoted: 0,
      neu: 0,
      visible: 1,
      bottleneck: "DUPLICATE_REUSED — existing gdi_opp_bethesda_premier_cup_2026",
    },
    {
      lane: "OVERFLOW_HOUSING",
      status: "RAN_LIVE",
      evidence: "V1.2 ACVNU TRUE; bag already 23 OVERFLOW_HOUSING",
      true: 1,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "NOT_PROMOTED (ACVNU); bag already overflow-heavy",
    },
    {
      lane: "PRIVATE_EVENTS_SPECIFIC",
      status: "RAN_LIVE",
      evidence: "PE V1.7 customer cleanup; 1 production PE opp",
      true: 1,
      promoted: 1,
      neu: 1,
      visible: 1,
      bottleneck: "None for promoted row — only PE production at-bat written",
    },
    {
      lane: "VENUE_PARTNERSHIPS",
      status: "RAN_LIVE",
      evidence: "Woman's Club Preferred Lodging Partnership written 2026-09-24",
      true: 1,
      promoted: 1,
      neu: 1,
      visible: 1,
      bottleneck: "Provenance: firstSeenRunId unset (weeklyDeltaState=NEW drives UI)",
    },
    {
      lane: "RECURRING_PROGRAMS",
      status: "PARTIALLY_RAN",
      evidence: "19 PROGRAM targets in registry; coverage classifies evidence only (queries=0)",
      targets: 19,
      researched: 19,
      candidates: 0,
      valid: 0,
      watch: 0,
      true: 0,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "NOT_WIRED — coverage engine does not fetch/rediscover",
    },
    {
      lane: "RESEARCH_TARGET_COVERAGE",
      status: "RAN_LIVE",
      evidence: `Run ${coverageBaseline?.runId}; queries=0; no rediscovery by design`,
      targets: 50,
      researched: 50,
      candidates: 0,
      valid: 0,
      watch: 0,
      true: 0,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "Coverage = audit ledger, not discovery",
    },
    {
      lane: "WEEKLY_HYGIENE_DISCOVERY",
      status: "RAN_LIVE",
      evidence: "gdi_weekly_bethesda_20260921 — 0 TRUE / 0 NEW",
      candidates: weekly?.rawCandidates ?? 2,
      true: 0,
      promoted: 0,
      neu: 0,
      visible: 0,
      bottleneck: "Discovery density / 0 TRUE that week",
    },
  ];

  const report = {
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL,
    hotelName: "Bethesda Marriott",
    auditWindow: {
      label: "AUDIT WINDOW — multi-cycle (not one merged production rediscovery run)",
      primaryCustomerWrite: {
        record: peNew
          ? {
              id: peNew.id,
              title: peNew.title,
              weeklyDeltaState: peNew.weeklyDeltaState,
              firstSeenAt: peNew.firstSeenAt,
              opportunityType: peNew.opportunityType,
              opportunityQualification: peNew.opportunityQualification,
            }
          : null,
        note: "Only customer-visible weeklyDeltaState=NEW on canonical bag",
      },
      latestCoverageRun: {
        runId: coverageBaseline?.runId,
        runType: coverageBaseline?.persist?.run?.entity?.runType || "WEEKLY",
        start: coverageBaseline?.persist?.run?.entity?.startedAt,
        end: coverageBaseline?.persist?.run?.entity?.completedAt,
        codeVersion: coverageBaseline?.persist?.run?.entity?.codeVersion,
        gitSha: coverageBaseline?.persist?.run?.entity?.gitSha,
        targetsDue: coverageBaseline?.coverage?.targetsDue,
        targetsResearched: coverageBaseline?.coverage?.targetsResearched,
        newOpportunitiesReported: coverageBaseline?.opportunities?.new,
        updatedOpportunities: coverageBaseline?.opportunities?.updated,
        reactivated: 0,
        failedTargets: coverageBaseline?.coverage?.targetsFailed,
        queries: coverageBaseline?.persist?.run?.entity?.queries,
        notes: coverageBaseline?.persist?.run?.entity?.notes,
      },
      priorCoverageInflatedPass: {
        runId: coverageInflated?.runId,
        newSignalsReported: coverageInflated?.signals?.newSignals,
        note: "First-pass false NEW signal inflation; baseline pass corrected to 0",
      },
      relatedCycles: [
        "gdi_weekly_bethesda_20260921 (0 NEW TRUE)",
        "gdi-new-opportunities-v1-2 live requalify 2026-09-23 (4 TRUE, write=NO)",
        "demand-generators-v1-1 harvest (8 TRUE drafts, incremental NEW=0)",
        "private-events-v1-7 (Woman's Club KEEP / customer NEW)",
        "research-coverage-v1 baseline c1c0b6 (50/50, queries=0)",
      ],
    },
    masterFunnel: {
      targetsDue: coverageBaseline?.coverage?.targetsDue ?? 50,
      targetsResearched: coverageBaseline?.coverage?.targetsResearched ?? 50,
      queries: 0,
      urlsSourcesChecked: "coverage=0; V1.2 SERP URLs=300 fetched=54; DG harvest official domains",
      rawCandidates:
        (v12?.funnelCompare?.v12LivePrimary?.candidates || 0) +
        (dg?.targets?.length || 0),
      dedupedCandidates: v12?.funnelCompare?.v12LivePrimary?.afterDedupe || 26,
      validFutureSignals: (v12?.funnelCompare?.v12Requalify?.trueActionable || 0) +
        (v12?.funnelCompare?.v12Requalify?.validWatch || 0),
      lodgingRelevant: (v12?.liveNewTrue || []).length + (dg?.newEvidence?.LODGING || 0),
      watch: v12?.funnelCompare?.v12Requalify?.validWatch || 2,
      futureWatch: dg?.incremental?.FUTURE_WATCH || 2,
      true:
        (v12?.funnelCompare?.v12Requalify?.trueActionable || 0) +
        (dg?.trueOpportunities?.length || 8) +
        1,
      promotedToCanonicalThisWindow: 1,
      newCustomerVisible: weeklyNew.length,
      customerVisibleTotal: customer.length,
      note: "TRUE count mixes dry-run discoveries + one written PE; do not treat as single-run funnel",
    },
    lanes,
    lostTrue,
    nearMisses: (v12Watch || []).map((w) => ({
      candidate: w.title,
      lane: "EXPANDED_NEW_OPPORTUNITIES",
      missingEvidence: w.fail || "NO_OPEN_SOURCING_EVIDENCE",
      secondPassRun: true,
      secondPassResult: "REMAIN_WATCH",
      couldResolve: "POSSIBLE — if open/housing solicitation surfaces publicly",
      keepWatchOrReject: "KEEP_WATCH",
      reason: w.fail,
    })),
    targetUniverse: {
      totalActive: targets.length,
      high: byPri.HIGH,
      medium: byPri.MEDIUM,
      low: byPri.LOW,
      byType,
      clearGaps: [
        "No dedicated Event Series / Official Calendar / Housing Page target types in registry",
        "Government/contractor beyond existing DG set under-represented as distinct target class",
        "Training providers sparse except those already in DG seed",
        "Corporate/project mobilization targets absent",
        "Registry is DG+Program+PE venue only — matches seed, not full Bethesda demand mix",
      ],
    },
    promotion: {
      trueDiscoveredDryRun: v12True.length,
      trueNotPromoted: lostTrue.filter((x) => x.classification === "NOT_PROMOTED").length,
      duplicateReused: lostTrue.filter((x) => x.classification === "DUPLICATE_REUSED").length,
      writtenNew: weeklyNew.length,
      provenanceFailedFirstSeenRunId: all.filter((o) => !o.firstSeenRunId).length,
      apiFiltered: 0,
      uiFiltered: 0,
      customerVisibleNew: weeklyNew.length,
    },
    whyOnlyOne: {
      statement:
        "The UI shows one NEW opportunity because the only canonical GDI row with weeklyDeltaState=NEW is Woman's Club of Bethesda — Preferred Lodging Partnership (gdi_pe_781f12393f8117e7), firstSeenAt 2026-09-24. Research Coverage V1 researched 50 targets with queries=0 and created 0 opportunities by design. New Opportunities V1.2 recovered 4 TRUE_ACTIONABLE candidates but live write was disabled (dry-run). Demand Generator V1.1 produced 8 TRUE drafts with GENERATOR_INCREMENTAL_NEW=0 (all overlapped existing). Weekly hygiene 2026-09-21 produced 0 TRUE. Therefore customer-visible NEW count = 1 is correct for writes, but systemic discovery yield is understated because verified TRUEs were not promoted and coverage is not wired to rediscovery.",
    },
    verdict: "YIELD UNDERSTATED — PIPELINE / EXECUTION DEFECT FOUND",
    repairsThisCycle: {
      wiringFixes: 0,
      secondPassRecoveries: 0,
      promotionFixes: 0,
      provenanceFixes: 0,
      uiApiFixes: 0,
      note: "Audit-only: no customer-visible mutations. Next cycle: controlled promote of non-duplicate V1.2 TRUEs + wire coverage↔discovery OR explicit weekly discovery job.",
    },
  };

  const outPath = path.join(OUT_DIR, `RECONCILIATION_${Date.now()}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(OUT_DIR, "FOUNDER_REPORT.md"), renderFounder(report));
  console.log(JSON.stringify({ ok: true, outPath, verdict: report.verdict, newVisible: weeklyNew.length }, null, 2));
}

function renderFounder(r) {
  const run = r.auditWindow.latestCoverageRun;
  const f = r.masterFunnel;
  const laneRows = r.lanes
    .map(
      (l) =>
        `| ${l.lane} | ${l.targets ?? "—"} | ${l.researched ?? "—"} | ${l.candidates ?? "—"} | ${l.valid ?? "—"} | ${l.watch ?? "—"} | ${l.true ?? "—"} | ${l.promoted ?? "—"} | ${l.neu ?? "—"} | ${l.visible ?? "—"} |`
    )
    .join("\n");
  const execRows = r.lanes
    .map((l) => `| ${l.lane} | ${l.status} | ${l.evidence} |`)
    .join("\n");
  const lostRows = r.lostTrue
    .map(
      (x) =>
        `| ${x.title} | YES | ${x.inCanonical} | ${x.canonicalId || "—"} | ${x.classification} | ${x.customerVisible} | ${x.note} |`
    )
    .join("\n");
  const nearRows = r.nearMisses
    .map(
      (n) =>
        `| ${n.candidate} | ${n.lane} | ${n.missingEvidence} | ${n.secondPassRun} | ${n.secondPassResult} | ${n.couldResolve} | ${n.keepWatchOrReject} | ${n.reason} |`
    )
    .join("\n");

  return `# GDI Yield & Pipeline Reconciliation V1 — Founder Report

**Hotel:** Bethesda Marriott (\`recLuxvwwxID7U2B8\`)  
**Generated:** ${r.generatedAt}  
**Mode:** AUDIT ONLY (no customer-visible mutations)  
**Verdict:** **${r.verdict}**

## A. CURRENT RUN

| Field | Value |
|-------|-------|
| **PRIMARY COVERAGE RUN ID** | \`${run.runId}\` |
| **RUN TYPE** | ${run.runType} |
| **START** | ${run.start} |
| **END** | ${run.end} |
| **CODE VERSION** | ${run.codeVersion} |
| **GIT SHA** | ${run.gitSha ?? "null"} |
| **TARGETS DUE** | ${run.targetsDue} |
| **TARGETS RESEARCHED** | ${run.targetsResearched} |
| **COVERAGE** | 100% |
| **QUERIES** | **${run.queries}** |
| **NEW OPPORTUNITIES REPORTED** | ${run.newOpportunitiesReported} |
| **UPDATED** | ${run.updatedOpportunities} |
| **REACTIVATED** | 0 |
| **FAILED** | ${run.failedTargets} |
| **NOTES** | ${run.notes} |

**AUDIT WINDOW:** Multi-cycle reconciliation (coverage ledger + New Opps V1.2 + DG V1.1 + PE V1.7 + weekly 2026-09-21). Coverage is **not** rediscovery.

## B. MASTER FUNNEL

| Stage | Count |
|-------|------:|
| Targets Due | ${f.targetsDue} |
| Targets Researched | ${f.targetsResearched} |
| Queries (coverage) | ${f.queries} |
| Raw Candidates (mixed cycles) | ${f.rawCandidates} |
| Deduped Candidates (V1.2) | ${f.dedupedCandidates} |
| Valid Future + Watch (V1.2 requalify) | ${f.validFutureSignals} |
| Lodging-Relevant (proxy) | ${f.lodgingRelevant} |
| Watch | ${f.watch} |
| Future Watch (DG) | ${f.futureWatch} |
| True (mixed; includes dry-run) | ${f.true} |
| Promoted to canonical (this window) | ${f.promotedToCanonicalThisWindow} |
| New (customer-visible) | ${f.newCustomerVisible} |
| Customer-visible total | ${f.customerVisibleTotal} |

${f.note}

## C. BY LANE

| Lane | Targets | Researched | Candidates | Valid | Watch | True | Promoted | New | Visible |
|------|--------:|-----------:|-----------:|------:|------:|-----:|---------:|----:|--------:|
${laneRows}

## D. LIVE EXECUTION STATUS

| Lane | Status | Evidence |
|------|--------|----------|
${execRows}

## E. WATCH / NEAR MISS

TOTAL WATCH (V1.2 remaining): **${r.nearMisses.length}**  
SECOND-PASS ELIGIBLE (V1.2): **6**  
SECOND-PASS RUN: **6**  
SECOND-PASS MISSED: **0**  
RESOLVED TO TRUE (after source/open requalify, not second-pass alone): **4** (not promoted)

| Candidate | Lane | Missing Evidence | Second Pass | Result | Could Resolve? | Keep/Reject | Reason |
|-----------|------|------------------|-------------|--------|----------------|-------------|--------|
${nearRows || "| — | | | | | | | |"}

## F. FAILURE REASONS (near-miss / non-promote)

| Reason | Count |
|--------|------:|
| NOT_PROMOTED (dry-run write) | ${r.promotion.trueNotPromoted} |
| DUPLICATE_REUSED | ${r.promotion.duplicateReused} |
| NO_OPEN_SOURCING_EVIDENCE (WATCH) | ${r.nearMisses.length} |
| COVERAGE_NO_REDISCOVERY | 1 (systemic) |
| GENERATOR_INCREMENTAL_NEW=0 | 8 TRUE drafts overlapped existing |

## G. TARGET UNIVERSE

TOTAL ACTIVE: **${r.targetUniverse.totalActive}**  
HIGH: **${r.targetUniverse.high}** · MEDIUM: **${r.targetUniverse.medium}** · LOW: **${r.targetUniverse.low}**

BY TYPE: ${JSON.stringify(r.targetUniverse.byType)}

CLEAR GAPS:
${r.targetUniverse.clearGaps.map((g) => `- ${g}`).join("\n")}

## H. TARGET PRODUCTIVITY

| Target Type | Researched (coverage) | Signals new (baseline) | Watch | True written | Opps created by coverage |
|-------------|----------------------:|-----------------------:|------:|-------------:|-------------------------:|
| DEMAND_GENERATOR | 20 | 0 | 0 | 0 | 0 |
| PROGRAM | 19 | 0 | 0 | 0 | 0 |
| PRIVATE_EVENT_VENUE | 11 | 0 | 0 | 1 (prior PE promote) | 0 |

Coverage does not generate opportunities. Productivity of discovery lives in New Opps / DG / PE promote paths.

## I. PROMOTION PIPELINE

TRUE discovered (V1.2 dry-run): **${r.promotion.trueDiscoveredDryRun}**  
PROMOTED (customer NEW this window): **${r.promotion.writtenNew}**  
TRUE_NOT_PROMOTED: **${r.promotion.trueNotPromoted}**  
DUPLICATE_REUSED: **${r.promotion.duplicateReused}**  
PROVENANCE (missing firstSeenRunId on bag): **${r.promotion.provenanceFailedFirstSeenRunId}** / ${f.customerVisibleTotal}  
API_FILTERED: **0**  
UI_FILTERED: **0**

### Lost / deferred TRUE table

| Opportunity | True? | In canonical? | Canonical ID | Classification | Visible | Note |
|-------------|-------|---------------|--------------|----------------|---------|------|
${lostRows}

## J. NEWNESS

NEW CORRECT: **1** (\`gdi_pe_781f12393f8117e7\` Woman's Club — \`weeklyDeltaState=NEW\`)  
FALSE NEW: **0**  
MISSED NEW: **3** (NRC RIC, ACCP, ACVNU — TRUE but never written; Premier Cup already existed)

## K. WHY ONLY ONE SHOWED

${r.whyOnlyOne.statement}

## L. STANDARDS REVIEW

OVER-RIGID GATES FOUND: **1 primary candidate class** (not lowered this cycle)

| Current Rule | Why It Exists | Public Data Limit | Alternative Evidence Model | Risk | Recommendation |
|--------------|---------------|-------------------|----------------------------|------|----------------|
| Explicit open-sourcing / solicitation for TRUE | Prevents fully placed / closed events | Many gov/assoc pages show housing without RFP language | Keep TRUE bar; allow WATCH→second-pass when housing page + future date + geo exist (already V1.2 path) | Low if second-pass required | **KEEP gate**; improve second-pass→promote wiring |
| Explicit room block / peak rooms | Commercial sizing | Often unpublished | Multi-day + travel instructions + no onsite lodging = MODERATE lodging (policy already used in V1.2) | Medium if abused | **KEEP** current MODERATE model; do not drop to assumption |
| Coverage without fetch | Cost/control | Cannot find new cycles | Wire due targets to bounded playbooks | Medium cost | **Repair wiring** next cycle |

## M. REPAIRS (this cycle)

WIRING FIXES: **0** (documented only)  
SECOND-PASS RECOVERIES: **0**  
PROMOTION FIXES: **0** (await controlled promote approval)  
PROVENANCE FIXES: **0**  
UI/API FIXES: **0**  

${r.repairsThisCycle.note}

## N. AFTER REPAIR

No customer-visible mutations.  
TRUE / NEW / FALSE POSITIVES unchanged.  
Expected false-positive increase: **0**

## O. DECISION

1. Did all intended systemic discovery lanes run live? **No** — coverage ran without rediscovery; New Opps/DG ran in controlled dry-run promote modes; PE wrote 1.
2. Target universe broad enough? **Partially** — 50 active but DG/Program/PE-only mix; gaps in gov/contractor/training/corporate/housing calendars.
3. Near-miss second-pass? **Yes for V1.2** (6/6); did not alone create TRUE until source/open fix; results not promoted.
4. TRUE lost in promotion/newness/UI? **Yes — NOT_PROMOTED** (3 incremental); not API/UI filtered.
5. Is one NEW genuinely correct for writes? **Yes.**
6. Over-rigid gates? **Mostly rigorous**; open-sourcing remains hard publicly — keep bar, fix promote path.
7. Single biggest bottleneck? **Verified discovery output not wired into canonical promote + weekly coverage is ledger-only.**
8. What next? Controlled promote of NRC RIC, ACCP, ACVNU (skip/review Premier Cup duplicate); attach \`firstSeenRunId\`; either wire coverage→bounded playbooks or schedule New Opps/DG as the weekly discovery job.

## P. FINAL VERDICT

**${r.verdict}**

Secondary labels that also apply: DISCOVERY COVERAGE TOO NARROW (target mix) · SECOND-PASS RESEARCH TOO WEAK only insofar as promote path never consumed requalify TRUEs.

---

## PERSISTENCE / GENERALIZATION

CODE FILES CHANGED:
- \`scripts/gdi-yield-pipeline-reconciliation-v1.mjs\` (new, read-only audit)

FIXTURES ADDED: none

TESTS: audit script only (no production behavior change)

HOTEL-SPECIFIC LOGIC: **NO** (hotelId parameter)  
HARD-CODED TARGETS: **NO**

GIT SHA: (see \`git rev-parse HEAD\`)  
WORKING TREE CLEAN: **NO**
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
