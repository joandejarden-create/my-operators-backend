#!/usr/bin/env node
/**
 * HIF V6 — Commercial Opportunity Recovery orchestration.
 * Source freshness × research direction × country/timing × first-100 scorecard.
 * CUSTOMER_BLOCKED. $0. Does not weaken evidence for milestone fill.
 *
 * Usage: node scripts/hif-v6-commercial-opportunity-recovery.mjs --run
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { annotateResearchFreshness, RESEARCH_DIRECTION, SOURCE_VINTAGE } from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-direction-freshness-v6.js";
import { summarizeResearchGaps } from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-gap-visibility-v6.js";
import { buildFirst100Scorecard } from "../lib/hotel-intelligence/sec-edgar/hif-v6-first-100-milestone-v6.js";
import { TIMING_HORIZON_ORDER } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-commercial-opportunity-recovery");
const RUN = process.argv.includes("--run");
const asOf = new Date().toISOString().slice(0, 10);

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const load = (rel) => {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
};

function bump2d(matrix, a, b) {
  if (!matrix[a]) matrix[a] = {};
  matrix[a][b] = (matrix[a][b] || 0) + 1;
}

function loadAudited() {
  const j = load("reports/sec-edgar-hif-v6-commercial-timing-geo-audit/AUDITED_RECORDS.json");
  return j?.records || j?.hotels || (Array.isArray(j) ? j : []);
}

function loadRecoveryResults() {
  return load("reports/sec-edgar-hif-v6-priority-b-recovery/RECOVERY_RESULTS.json")?.results || [];
}

function loadCurrentBackward() {
  return load("reports/sec-edgar-hif-v6-current-backward-discovery/INCREMENTAL.json")?.events ||
    load("reports/sec-edgar-hif-v6-current-backward-discovery/EXTRACTED.json")?.rows ||
    [];
}

function strategyComparison() {
  const continuous = load("reports/sec-edgar-hif-v6-name-free-sec-continuous/CONTINUOUS_METRICS.json") || {};
  const continuousW3 = load("reports/sec-edgar-hif-v6-name-free-sec-continuous/CONTINUOUS_METRICS_WAVE3.json") || {};
  const mgmt = load("reports/sec-edgar-hif-v6-management-discovery/METRICS.json") || {};
  const recovery = load("reports/sec-edgar-hif-v6-priority-b-recovery/RECOVERY_METRICS.json") || {};
  const cb = load("reports/sec-edgar-hif-v6-current-backward-discovery/METRICS.json") || {};

  return {
    track1_historicalForward: {
      label: "HISTORICAL_FORWARD — CMBS / Related License name-free SEC",
      sourceDocumentsInspected:
        (continuous.documentsOk || continuous.documentsAttempted || 0) +
        (continuousW3.documentsOk || continuousW3.documentsAttempted || 0),
      incrementalContractualRecords:
        (continuous.netNewEvents || continuous.incrementalEvents || 0) +
        (continuousW3.netNewEvents || continuousW3.incrementalEvents || 0),
      nearTermDecisionCandidates: "see Priority B cohort from timing audit",
      newlyQualifiedBctMct: "primarily unresolved → research; see agreement-type-resolution",
      priorityAPromotions: 0,
      outreachReadyPromotions: 0,
      researchCostUsd: 0,
      commercialYieldNote:
        "Highest raw contractual yield; commercial yield low — expiration-only proxies dominate",
    },
    track2_currentBackward: {
      label: "CURRENT_BACKWARD — 8-K / commercial-event then contract trace",
      sourceDocumentsInspected: mgmt.documentsOk || cb.documentsOk || 0,
      incrementalSignals: mgmt.incrementalEvents || mgmt.newManagementEventEvidence || cb.incrementalEvents || 0,
      newVerifiedManagementContractualDates: mgmt.newVerifiedManagementContractualDates || 0,
      newQualifiedMct: mgmt.newQualifiedMct || 0,
      priorityAPromotions: cb.promotedPriorityA || 0,
      researchCostUsd: 0,
      commercialYieldNote:
        "Adds contemporary signals but often undated — timing UNKNOWN until contract dates linked",
    },
    track3_contractWindowTargeted: {
      label: "CONTRACT_WINDOW_TARGETED — Priority B ≤24m deep audit",
      cohortSize: recovery.cohortSize || 92,
      reviewed: recovery.reviewed || 0,
      noticeDeadlineFound: recovery.noticeDeadlineFound || 0,
      promotedPriorityA: recovery.promotedToPriorityA || 0,
      remainingPriorityB: recovery.remainingPriorityB || 0,
      rejectedOrInvalid: recovery.rejectedOrInvalid || 0,
      hold: recovery.hold || 0,
      researchCostUsd: 0,
      commercialYieldNote:
        "Directly tests near-term expiration cohort; promotions require confirmed notice/decision evidence",
    },
  };
}

function writeFounderReport(payload) {
  const m = payload.executive;
  const sc = payload.first100;
  const pb = payload.priorityBRecovery;
  const md = [];
  md.push("# HIF V6 — Commercial Opportunity Recovery & Global Research Intelligence");
  md.push("");
  md.push(`**As of:** ${asOf}  `);
  md.push("**Publication:** CUSTOMER_BLOCKED  ");
  md.push("**Research cost:** $0  ");
  md.push("");
  md.push("## 1. Executive commercial summary");
  md.push("");
  md.push(`- Canonical inventory analyzed: **${m.inventoryAnalyzed}** contractual research records`);
  md.push(`- Qualified near-term opportunities (first-100 gate): **${sc.qualifiedNearTermOpportunities}** / 100`);
  md.push(`- Priority A: **${m.priorityA}** · Priority B: **${m.priorityB}** · C: **${m.priorityC}** · D: **${m.priorityD}** · HOLD: **${m.hold}**`);
  md.push(`- Verified commercial decision windows: **${m.confirmedDecisionWindows}**`);
  md.push(`- Outreach-ready: **${m.outreachReady}**`);
  md.push(`- Qualified BCT / MCT: **${m.qualifiedBct}** / **${m.qualifiedMct}**`);
  md.push("");
  md.push("### Main commercial limitations");
  for (const lim of m.limitations) md.push(`- ${lim}`);
  md.push("");
  md.push("## 2. Country × timing matrix");
  md.push("");
  md.push("See `MATRIX_COUNTRY_TIMING.json`. Physical verification classes:");
  md.push(`- VERIFIED: ${m.geoVerified}`);
  md.push(`- REVIEW_REQUIRED: ${m.geoReviewRequired}`);
  md.push(`- UNRESOLVED: ${m.geoUnresolved}`);
  md.push("");
  md.push("### Research-gap analysis");
  md.push("");
  md.push(`- Geographic verification gap: ${payload.gaps.totals.GEOGRAPHIC_VERIFICATION_GAP}`);
  md.push(`- Timing gap: ${payload.gaps.totals.TIMING_GAP}`);
  md.push(`- Agreement classification gap: ${payload.gaps.totals.AGREEMENT_CLASSIFICATION_GAP}`);
  md.push(`- Current-status gap: ${payload.gaps.totals.CURRENT_STATUS_GAP}`);
  md.push(`- Commercial qualification gap: ${payload.gaps.totals.COMMERCIAL_QUALIFICATION_GAP}`);
  md.push(`- Coverage gap: ${payload.gaps.totals.COVERAGE_GAP}`);
  md.push("");
  md.push("## 3. Source vintage × timing matrix");
  md.push("");
  md.push("See `MATRIX_SOURCE_VINTAGE_TIMING.json`. Research direction mix:");
  for (const [k, v] of Object.entries(payload.researchDirectionPct)) {
    md.push(`- ${k}: ${v}`);
  }
  md.push("");
  md.push("### Evidence freshness");
  for (const [k, v] of Object.entries(payload.evidenceFreshnessCounts)) {
    md.push(`- ${k}: ${v}`);
  }
  md.push("");
  md.push("## 4. Research direction comparison");
  md.push("");
  for (const [k, v] of Object.entries(payload.strategyComparison)) {
    md.push(`### ${v.label || k}`);
    md.push("```json");
    md.push(JSON.stringify(v, null, 2));
    md.push("```");
    md.push("");
  }
  md.push("## 5. Priority B recovery results");
  md.push("");
  if (pb) {
    md.push(`- Reviewed: ${pb.reviewed}`);
    md.push(`- Geography verified: ${pb.geographyVerified}`);
    md.push(`- Agreement type verified (document cues): ${pb.agreementTypeVerified}`);
    md.push(`- Current status corroborated: ${pb.currentStatusCorroborated}`);
    md.push(`- Notice/termination deadline found: ${pb.noticeDeadlineFound}`);
    md.push(`- Promoted to Priority A: ${pb.promotedToPriorityA}`);
    md.push(`- Remaining Priority B: ${pb.remainingPriorityB}`);
    md.push(`- Superseded/rejected: ${(pb.historicalOrSuperseded || 0) + (pb.rejectedOrInvalid || 0)}`);
    md.push(`- HOLD: ${pb.hold}`);
  } else {
    md.push("- Recovery metrics not yet available — run `hif-v6-priority-b-recovery-audit.mjs --run`");
  }
  md.push("");
  md.push("### Hotel-specific examples");
  for (const ex of payload.examples || []) {
    md.push(`- **${ex.hotelName}** — ${ex.disposition || ex.commercialPriority}: ${ex.explanation || (ex.reasons || []).join(", ")}`);
  }
  md.push("");
  md.push("## 6. First 100 commercial milestone");
  md.push("");
  md.push(`**Qualified count: ${sc.qualifiedNearTermOpportunities} / 100**`);
  md.push("");
  md.push("Standards were not weakened. Expiration-only Priority B proxies are excluded.");
  if (sc.qualifiedNearTermOpportunities === 0) {
    md.push("");
    md.push(
      "Zero opportunities currently meet the milestone gate (verified decision window within 24 months + franchise/management relevance + identity/geo). Near-term inventory remains Priority B research candidates pending notice/EX-10/current-status evidence.",
    );
  } else {
    md.push("");
    md.push("### Distribution");
    md.push(`- By country: ${JSON.stringify(sc.byCountry)}`);
    md.push(`- By timing: ${JSON.stringify(sc.byTiming)}`);
    md.push(`- By type: ${JSON.stringify(sc.byOpportunityType)}`);
    md.push(`- By research direction: ${JSON.stringify(sc.byResearchDirection)}`);
  }
  md.push("");
  md.push("## 7. Discovery continuation");
  md.push("");
  md.push("- Name-free SEC CMBS harvest remains the primary HISTORICAL_FORWARD engine.");
  md.push("- CURRENT_BACKWARD (8-K management / conversion signals) complements with undated commercial events.");
  md.push("- CONTRACT_WINDOW_TARGETED recovery operates on the ≤24m Priority B cohort.");
  md.push("- Worldwide eligibility unchanged — priority markets are concentration only.");
  md.push("");
  md.push("## 8. Integration");
  md.push("");
  md.push("- Modules: research-direction-freshness, research-gap-visibility, first-100-milestone");
  md.push("- Scripts: commercial-opportunity-recovery, priority-b-recovery-audit, current-backward-discovery");
  md.push("- Tests: `test:hif-v6-commercial-opportunity-recovery-v6`");
  md.push("- Integration Controller owns merge; Track A/B and HE/ADP/GDI/BE dirty worktrees preserved");
  md.push("- Actual spending: **$0**");
  md.push("");
  md.push("---");
  md.push("");
  md.push("**WHERE × WHEN × HOW CURRENT** — physical geography, decision windows (not expiration proxies alone), and evidence freshness are first-class and separately measured.");
  fs.writeFileSync(path.join(OUT, "FOUNDER_COMMERCIAL_OPPORTUNITY_RECOVERY.md"), md.join("\n"), "utf8");
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, asOf }, null, 2));
    return;
  }

  const audited = loadAudited();
  const recoveryRows = loadRecoveryResults();
  const recoveryMetrics = load("reports/sec-edgar-hif-v6-priority-b-recovery/RECOVERY_METRICS.json");
  const auditSummary = load("reports/sec-edgar-hif-v6-commercial-timing-geo-audit/AUDIT_SUMMARY.json") || {};

  const annotated = audited.map((r) => {
    const fresh = annotateResearchFreshness(r, asOf);
    return { ...r, ...fresh };
  });

  const vintageTiming = {};
  const directionCounts = {};
  const freshnessCounts = {};
  const vintageCounts = {};
  for (const r of annotated) {
    bump2d(vintageTiming, r.sourceVintage || SOURCE_VINTAGE.UNKNOWN, r.timingHorizon || "UNKNOWN");
    directionCounts[r.researchDirection] = (directionCounts[r.researchDirection] || 0) + 1;
    freshnessCounts[r.evidenceFreshness] = (freshnessCounts[r.evidenceFreshness] || 0) + 1;
    vintageCounts[r.sourceVintage] = (vintageCounts[r.sourceVintage] || 0) + 1;
  }
  const n = annotated.length || 1;
  const researchDirectionPct = Object.fromEntries(
    Object.entries(directionCounts).map(([k, v]) => [k, `${((100 * v) / n).toFixed(1)}% (${v})`]),
  );
  const sourceVintagePct = Object.fromEntries(
    Object.entries(vintageCounts).map(([k, v]) => [k, `${((100 * v) / n).toFixed(1)}% (${v})`]),
  );

  // Country × timing from annotated
  const countryTiming = {};
  for (const r of annotated) {
    const c = r.physicalCountry || r.hotelCountry || "UNATTRIBUTED";
    bump2d(countryTiming, c, r.timingHorizon || "UNKNOWN");
  }

  const gaps = summarizeResearchGaps(annotated);

  // Merge recovery dispositions into first-100 candidate pool
  const byKey = new Map(annotated.map((r) => [r.key, r]));
  for (const r of recoveryRows) {
    if (!r.key) continue;
    const prev = byKey.get(r.key) || {};
    byKey.set(r.key, {
      ...prev,
      ...r,
      // Preserve promotion if recovery found notice deadline
      decisionWindowKind: r.noticeDeadline
        ? "CONFIRMED_DECISION_WINDOW"
        : r.decisionWindowKind || prev.decisionWindowKind,
      noticeDeadline: r.noticeDeadline || prev.noticeDeadline,
      researchDirection: RESEARCH_DIRECTION.CONTRACT_WINDOW_TARGETED,
    });
  }
  for (const ev of loadCurrentBackward()) {
    const key =
      ev.key ||
      `${String(ev.hotelName || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim()}|${String(ev.hotelCountry || ev.physicalCountry || "US").toUpperCase()}|${ev.expirationDate || "NODATE"}`;
    const prev = byKey.get(key) || {};
    byKey.set(key, {
      ...prev,
      ...ev,
      key,
      researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD,
      wave: ev.wave || "CURRENT_BACKWARD",
    });
  }

  const pool = [...byKey.values()];
  const first100 = buildFirst100Scorecard(pool, asOf);

  const measures = auditSummary.measures || {};
  const executive = {
    inventoryAnalyzed: annotated.length,
    priorityA: measures.priorityA ?? 0,
    priorityB: measures.priorityB ?? 0,
    priorityC: measures.priorityC ?? 0,
    priorityD: measures.priorityD ?? 0,
    hold: measures.hold ?? 0,
    confirmedDecisionWindows: measures.confirmedDecisionWindows ?? 0,
    outreachReady: measures.outreachReady ?? 0,
    qualifiedBct: measures.qualifiedBct ?? 0,
    qualifiedMct: measures.qualifiedMct ?? 0,
    geoVerified: measures.geoVerified ?? 0,
    geoReviewRequired: measures.geoReviewRequired ?? 0,
    geoUnresolved: measures.geoUnresolved ?? 0,
    qualifiedNearTermMilestone: first100.qualifiedNearTermOpportunities,
    progressToward100: first100.progressToward100,
    afterRecoveryPriorityA:
      (measures.priorityA || 0) + (recoveryMetrics?.promotedToPriorityA || 0),
    priorityBRecoveryDispositions: recoveryMetrics
      ? {
          reviewed: recoveryMetrics.reviewed,
          remainingPriorityB: recoveryMetrics.remainingPriorityB,
          hold: recoveryMetrics.hold,
          rejectedOrInvalid: recoveryMetrics.rejectedOrInvalid,
          noticeDeadlineFound: recoveryMetrics.noticeDeadlineFound,
          promotedToPriorityA: recoveryMetrics.promotedToPriorityA,
        }
      : null,
    limitations: [
      "CMBS schedules overwhelmingly yield EXPIRATION_ONLY_PROXY — not confirmed commercial decision windows",
      "Absence of renewal evidence must not be treated as proof no renewal occurred",
      "Priority B ≤24m cohort is a research queue, not an automatic commercial pipeline",
      "Physical geography often REVIEW_REQUIRED; filing jurisdiction never substitutes for hotel location",
      "First-100 milestone excludes expiration-only proxies by design",
      ...(auditSummary.limitations || []),
    ],
  };

  const examples = [
    ...(recoveryRows.filter((r) => r.promotedPriorityA).slice(0, 5)),
    ...(recoveryRows.filter((r) => r.disposition === "REJECTED_OR_INVALID").slice(0, 3)),
    ...(recoveryRows.filter((r) => r.disposition === "PRIORITY_B_RESEARCH").slice(0, 5)),
    ...(recoveryRows.filter((r) => r.disposition === "HOLD_INSUFFICIENT_EVIDENCE").slice(0, 3)),
  ];

  const strategy = strategyComparison();

  writeJson(path.join(OUT, "ANNOTATED_INVENTORY_SAMPLE.json"), {
    count: annotated.length,
    sample: annotated.slice(0, 40),
  });
  writeJson(path.join(OUT, "MATRIX_SOURCE_VINTAGE_TIMING.json"), {
    rows: vintageTiming,
    horizons: TIMING_HORIZON_ORDER,
    vintagePct: sourceVintagePct,
  });
  writeJson(path.join(OUT, "MATRIX_COUNTRY_TIMING.json"), countryTiming);
  writeJson(path.join(OUT, "RESEARCH_DIRECTION_STATS.json"), {
    counts: directionCounts,
    pct: researchDirectionPct,
  });
  writeJson(path.join(OUT, "EVIDENCE_FRESHNESS_STATS.json"), freshnessCounts);
  writeJson(path.join(OUT, "RESEARCH_GAPS.json"), gaps);
  writeJson(path.join(OUT, "STRATEGY_COMPARISON.json"), strategy);
  writeJson(path.join(OUT, "FIRST_100_SCORECARD.json"), first100);
  writeJson(path.join(OUT, "EXECUTIVE_SUMMARY.json"), executive);
  writeJson(path.join(OUT, "WEEKLY_CHANGE_HOOK.json"), {
    asOf,
    wave: "COMMERCIAL_OPPORTUNITY_RECOVERY",
    additions: recoveryMetrics?.promotedToPriorityA || 0,
    removals: (recoveryMetrics?.rejectedOrInvalid || 0) + (recoveryMetrics?.historicalOrSuperseded || 0),
    priorityChanges: recoveryMetrics?.reviewed || 0,
    note: "Canonical observations preserved; commercial interpretation may change without duplicate opportunities",
  });

  const payload = {
    executive,
    first100,
    priorityBRecovery: recoveryMetrics,
    gaps,
    researchDirectionPct,
    evidenceFreshnessCounts: freshnessCounts,
    strategyComparison: strategy,
    examples,
  };
  writeFounderReport(payload);

  // Weekly scorecard snapshot
  const snapDir = path.join(ROOT, "reports/sec-edgar-hif-v6-weekly-scorecard/snapshots");
  writeJson(path.join(snapDir, "2026-W47-commercial-opportunity-recovery.json"), {
    wave: "COMMERCIAL_OPPORTUNITY_RECOVERY",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    ...executive,
    first100: first100.progressToward100,
    researchDirectionPct,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        inventoryAnalyzed: annotated.length,
        first100: first100.progressToward100,
        researchDirectionPct,
        recoveryPromotedA: recoveryMetrics?.promotedToPriorityA ?? null,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
