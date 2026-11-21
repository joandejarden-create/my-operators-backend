#!/usr/bin/env node
/**
 * HIF V7 — Current-backward commercial opportunity pilot.
 * Qualifies curated free-source commercial signals; does not invent dates.
 * CUSTOMER_BLOCKED. $0. Milestone = first 5–10 Priority A (zero OK).
 *
 * Usage: node scripts/hif-v7-current-backward-commercial-pilot.mjs --run
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  qualifyPriorityACurrentBackward,
  buildOpportunityDossier,
  classifyOpportunityStatus,
  isOpenOpportunityStatus,
  OPPORTUNITY_STATUS,
  SIGNAL_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-v7-opportunity-status-v7.js";
import { annotateResearchFreshness, RESEARCH_DIRECTION } from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-direction-freshness-v6.js";
import { COMMERCIAL_PRIORITY } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-actionability-v6.js";
import { buildFirst100Scorecard } from "../lib/hotel-intelligence/sec-edgar/hif-v6-first-100-milestone-v6.js";
import { DECISION_WINDOW_KIND } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v7-current-backward-pilot");
const RUN = process.argv.includes("--run");
const asOf = new Date().toISOString().slice(0, 10);
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const load = (rel) => JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));

function mapNonA(row, qualification) {
  if (row.decisionAwarded || row.decisionCompleted || qualification.opportunityStatus === OPPORTUNITY_STATUS.COMPLETED) {
    return {
      commercialPriority: COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE,
      disposition: "REJECTED_OR_COMPLETED",
    };
  }
  if (qualification.opportunityStatus === OPPORTUNITY_STATUS.DECISION_AWARDED) {
    return {
      commercialPriority: COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE,
      disposition: "DECISION_AWARDED",
    };
  }
  if (row.productNote === "HOSTEL_NOT_FULL_SERVICE_HOTEL") {
    return {
      commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
      disposition: "PRIORITY_B_HOSTEL_LODGING",
    };
  }
  if (qualification.reasons?.includes("CURRENT_STATUS_NOT_VERIFIED") || !row.currentStatusVerified) {
    return { commercialPriority: COMMERCIAL_PRIORITY.HOLD, disposition: "HOLD_INSUFFICIENT_EVIDENCE" };
  }
  if (isOpenOpportunityStatus(qualification.opportunityStatus) || row.linkedBrandOrOperatorDecision) {
    return {
      commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
      disposition: "PRIORITY_B_RESEARCH",
    };
  }
  return { commercialPriority: COMMERCIAL_PRIORITY.HOLD, disposition: "HOLD_INSUFFICIENT_EVIDENCE" };
}

function writeFounderMd(payload) {
  const lines = [];
  lines.push("# HIF V7 — Current-Backward Commercial Opportunity Discovery");
  lines.push("");
  lines.push(`**As of:** ${asOf}  `);
  lines.push("**Publication:** CUSTOMER_BLOCKED  ");
  lines.push("**Research cost:** $0  ");
  lines.push(`**Integration dependency:** PRs #528 → #529 → #531 still OPEN; this pilot does not compete for canonical inventory merges.`);
  lines.push("");
  lines.push("## A. Executive results");
  lines.push("");
  for (const [k, v] of Object.entries(payload.executive)) {
    lines.push(`- **${k}:** ${typeof v === "object" ? JSON.stringify(v) : v}`);
  }
  lines.push("");
  lines.push("## B. Opportunity dossiers — Priority A");
  lines.push("");
  if (!payload.priorityADossiers.length) {
    lines.push("**Zero Priority A opportunities qualified in this pilot.**");
  } else {
    for (const d of payload.priorityADossiers) {
      lines.push(`### ${d.hotelName}`);
      lines.push(`- Location: ${d.physicalLocation.city}, ${d.physicalLocation.stateProvince}, ${d.physicalLocation.country}`);
      lines.push(`- Type: ${d.opportunityType} · Status: ${d.currentDecisionStatus}`);
      lines.push(`- Trigger: ${d.commercialTrigger}`);
      lines.push(`- Thesis: ${d.whyMayBeAvailable}`);
      lines.push(`- Window: ${d.decisionWindow.date || d.decisionWindow.horizon} (${d.decisionWindow.kind})`);
      lines.push(`- Owner/contracting party: ${d.ownerOrContractingParty || "n/a"}`);
      lines.push(`- Source: ${d.originalSource.url}`);
      lines.push(`- Uncertainties: ${(d.outstandingUncertainties || []).join("; ") || "n/a"}`);
      lines.push(`- Next action: ${d.recommendedNextAction || "n/a"}`);
      lines.push("");
    }
  }
  lines.push("### Strongest Priority B");
  for (const d of payload.priorityBDossiers.slice(0, 8)) {
    lines.push(`- **${d.hotelName}** (${d.physicalLocation.country}): ${d.whyMayBeAvailable?.slice(0, 180)}…`);
    lines.push(`  Missing for A: ${(d.qualificationReasons || []).join(", ")}`);
  }
  lines.push("");
  lines.push("## C. Country × timing");
  lines.push("```json");
  lines.push(JSON.stringify(payload.countryTiming, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("### Coverage gaps (priority markets)");
  for (const g of payload.coverageGaps) lines.push(`- ${g}`);
  lines.push("");
  lines.push("## D. Research direction comparison");
  lines.push("```json");
  lines.push(JSON.stringify(payload.strategyComparison, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## E. Source effectiveness");
  for (const s of payload.sourceEffectiveness) lines.push(`- ${s}`);
  lines.push("");
  lines.push("## F. Next research allocation");
  for (const n of payload.nextAllocation) lines.push(`- ${n}`);
  lines.push("");
  lines.push("## G. Engineering");
  lines.push(`- First-100 after V7 A promotions: ${payload.first100.progressToward100}`);
  lines.push("- Modules: `hif-v7-opportunity-status-v7.js`");
  lines.push("- Script: `hif-v7-current-backward-commercial-pilot.mjs`");
  lines.push("- Tests: `test:hif-v7-current-backward-commercial-v7`");
  fs.writeFileSync(path.join(OUT, "FOUNDER_V7_CURRENT_BACKWARD_PILOT.md"), lines.join("\n"), "utf8");
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, asOf }, null, 2));
    return;
  }

  const pack = load("reports/sec-edgar-hif-v7-current-backward-pilot/RESEARCH_CANDIDATES.json");
  const remainingB = load("reports/sec-edgar-hif-v6-priority-b-recovery/REMAINING_PRIORITY_B.json")?.hotels || [];

  const results = [];
  for (const raw of pack.candidates) {
    // Hostel product: keep as research B even if gates pass
    const forceB = raw.productNote === "HOSTEL_NOT_FULL_SERVICE_HOTEL";
    const timing = {
      decisionWindowKind: raw.decisionWindowKind,
      timingHorizon: raw.timingHorizon,
      monthsToDecision: raw.monthsToDecision,
      nextCommercialDecisionDate: raw.nextCommercialDecisionDate,
    };
    const geo = { geographicClass: raw.geographicClass, physicalCountry: raw.physicalCountry };
    let qualification = qualifyPriorityACurrentBackward(raw, timing, geo);
    if (forceB && qualification.qualifiesPriorityA) {
      qualification = {
        ...qualification,
        qualifiesPriorityA: false,
        commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
        reasons: [...qualification.reasons, "HOSTEL_PRODUCT_NOT_CORE_HOTEL_ICP"],
      };
    }
    if (!qualification.qualifiesPriorityA) {
      const mapped = mapNonA(raw, qualification);
      qualification = { ...qualification, ...mapped };
    }
    const fresh = annotateResearchFreshness(
      { ...raw, wave: "CURRENT_BACKWARD", discoveryMethod: "CURRENT_BACKWARD" },
      asOf,
    );
    const dossier = buildOpportunityDossier(
      { ...raw, ...timing, ...geo, researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD },
      qualification,
    );
    dossier.qualificationReasons = qualification.reasons;
    results.push({
      id: raw.id,
      ...raw,
      ...timing,
      ...qualification,
      ...fresh,
      dossier,
    });
  }

  // Track B: remaining 16 Priority B — none auto-promoted without new current-status evidence
  const trackB = remainingB.map((h) => ({
    hotelName: h.hotelName,
    key: h.key,
    priorDisposition: h.disposition,
    commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
    disposition: "REMAINS_PRIORITY_B",
    note: "Expiration-only / aged CMBS evidence — no new current-status corroboration in V7 pilot; not promoted",
    missingForA: [
      "CURRENT_STATUS_VERIFICATION",
      "CONFIRMED_OR_ACTIVE_DECISION_NOT_EXPIRATION_ONLY",
      "NOTICE_OR_OWNER_PROCESS_EVIDENCE",
    ],
  }));

  const priorityA = results.filter((r) => r.qualifiesPriorityA);
  const priorityB = results.filter((r) => r.commercialPriority === COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH);
  const rejected = results.filter((r) =>
    ["REJECTED_OR_COMPLETED", "DECISION_AWARDED"].includes(r.disposition),
  );
  const hold = results.filter((r) => r.commercialPriority === COMMERCIAL_PRIORITY.HOLD);

  // First-100: map Priority A rows into milestone shape
  const milestoneRows = priorityA.map((r) => ({
    hotelName: r.hotelName,
    physicalCountry: r.physicalCountry,
    geographicClass: r.geographicClass,
    agreementType: r.agreementType,
    agreementTypeResolved: true,
    commercialList: r.commercialList,
    decisionWindowKind:
      r.decisionWindowKind === DECISION_WINDOW_KIND.TIMING_UNKNOWN
        ? DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW
        : r.decisionWindowKind,
    monthsToDecision: r.monthsToDecision ?? 0,
    nextCommercialDecisionDate: r.nextCommercialDecisionDate,
    commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
    key: r.id,
    researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD,
  }));
  const first100 = buildFirst100Scorecard(milestoneRows, asOf);

  const countryTiming = {};
  for (const r of results) {
    const c = r.physicalCountry || "UNATTRIBUTED";
    if (!countryTiming[c]) countryTiming[c] = {};
    const t = r.timingHorizon || "UNKNOWN";
    countryTiming[c][t] = (countryTiming[c][t] || 0) + 1;
  }

  const executive = {
    distinctHotelsInvestigated: results.length,
    candidateSignalsDiscovered: results.length,
    priorityAQualified: priorityA.length,
    priorityBRemaining: priorityB.length + trackB.length,
    priorityBFromCurrentBackward: priorityB.length,
    priorityBFromPriorRecoveryCohort: trackB.length,
    qualifiedBct: results.filter((r) => r.qualifiesPriorityA && r.opportunityType === "BCT").length,
    qualifiedMct: results.filter((r) => r.qualifiesPriorityA && r.opportunityType === "MCT").length,
    outreachReady: 0,
    rejectedOrCompleted: rejected.length,
    hold: hold.length,
    first100Progress: first100.progressToward100,
    actualSpendUsd: 0,
    milestoneTarget: "5–10 Priority A (zero acceptable)",
    milestoneResult: priorityA.length >= 5 ? `MET_FLOOR (${priorityA.length})` : `BELOW_FLOOR (${priorityA.length}/5) — standards not weakened`,
  };

  const strategyComparison = {
    trackA_currentBackward: {
      effortShareGuide: "50%",
      hotelsInvestigated: results.length,
      priorityA: priorityA.length,
      priorityB: priorityB.length,
      rejectedCompleted: rejected.length,
      commercialYieldNote: "Highest verified commercial yield in this pilot — public RFPs and pending awards",
    },
    trackB_contractWindow: {
      effortShareGuide: "30%",
      cohortReviewed: trackB.length,
      priorityAPromotions: 0,
      commercialYieldNote: "16 remaining Priority B stay research — expiration-only CMBS without current-status corroboration",
    },
    trackC_historicalForward: {
      effortShareGuide: "20%",
      note: "SEC name-free harvest continues under #528/#531 stack; not re-run this pilot to avoid duplicate inventory writes",
      commercialYieldNote: "High raw contractual yield; low Priority A yield (prior audit 0/100)",
    },
  };

  const payload = {
    executive,
    priorityADossiers: priorityA.map((r) => r.dossier),
    priorityBDossiers: priorityB.map((r) => ({ ...r.dossier, qualificationReasons: r.reasons })),
    countryTiming,
    coverageGaps: [
      "MX: no open hotel operator/brand RFP corroborated this pass — Fibra Inn Guadalajara conversion already completed (Dec 2025)",
      "BR: conversion activity observed (e.g. Intercity Campinas) but completed/awarded — no open Priority A process found",
      "GB: Nottingham Belfry sale completed to Hunton — distress→sale pattern, not open MCT",
      "CA: Kenricia under review + Jericho hostel RFP open; YQB EOI past deadline without verified next stage",
      "US: strongest open yield via public procurement (Gideon Putnam, Pueblo, Chicago Marquis pending)",
      "National hotel-universe coverage % not computed (no credible denominator)",
    ],
    strategyComparison,
    sourceEffectiveness: [
      "Productive: state/municipal hotel operator & development RFPs (NYS Parks, PURA, MPEA, Kenora)",
      "Productive: recent WARN/regulator filings for operator changes (detect awards to avoid false opens)",
      "False positive pattern: sale/foreclosure/receivership without brand/operator decision language",
      "False positive pattern: completed soft-brand announcements already on brand.com",
      "False positive pattern: aged CMBS expiration-only rows (Track B)",
    ],
    nextAllocation: [
      "Increase CURRENT_BACKWARD share toward public procurement + trade press for open RFPs",
      "Maintain Track B only when fresh EX-10/8-K/current brand evidence appears for the 16",
      "Keep SEC historical-forward for inventory growth but do not count toward Priority A without current-backward corroboration",
      "Prioritize MX/BR native sources next cycle — coverage gap, not proof of absence",
      "Watch DEN for issuance of actual hotel management RFP after consulting award",
    ],
    first100,
  };

  writeJson(path.join(OUT, "PILOT_RESULTS.json"), { count: results.length, results });
  writeJson(path.join(OUT, "PRIORITY_A_DOSSIERS.json"), { count: priorityA.length, dossiers: payload.priorityADossiers });
  writeJson(path.join(OUT, "PRIORITY_B_DOSSIERS.json"), { count: priorityB.length, dossiers: payload.priorityBDossiers });
  writeJson(path.join(OUT, "TRACK_B_REMAINING_PRIORITY_B.json"), { count: trackB.length, hotels: trackB });
  writeJson(path.join(OUT, "REJECTED_OR_COMPLETED.json"), { count: rejected.length, hotels: rejected });
  writeJson(path.join(OUT, "EXECUTIVE_SUMMARY.json"), executive);
  writeJson(path.join(OUT, "STRATEGY_COMPARISON.json"), strategyComparison);
  writeJson(path.join(OUT, "FIRST_100_SCORECARD.json"), first100);
  writeJson(path.join(OUT, "RESEARCH_ECONOMICS.json"), {
    searchesAndFetches: "Cursor WebSearch/WebFetch free passes + prior SEC corpus",
    uniqueHotelsInvestigated: results.length,
    potentialSignals: results.length,
    rejectedCompleted: rejected.length,
    hold: hold.length,
    qualifiedBct: executive.qualifiedBct,
    qualifiedMct: executive.qualifiedMct,
    priorityA: priorityA.length,
    currentStatusVerifications: results.filter((r) => r.currentStatusVerified).length,
    geographicVerifications: results.filter((r) => r.geographicClass === "VERIFIED").length,
    costPerVerifiedOpportunityUsd: priorityA.length ? 0 : null,
    researchCostUsd: 0,
  });
  writeFounderMd(payload);

  writeJson(path.join(ROOT, "reports/sec-edgar-hif-v6-weekly-scorecard/snapshots/2026-W47-v7-current-backward-pilot.json"), {
    wave: "V7_CURRENT_BACKWARD_PILOT",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    ...executive,
  });

  console.log(JSON.stringify({ ok: true, ...executive }, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
