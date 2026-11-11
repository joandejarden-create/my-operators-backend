/**
 * Per-opportunity blocker ledger for adaptive Wave E follow-up (V6).
 * Zero-paid planning — does not execute Exa.
 */

import { NEED_DATE_V6_BLOCKER } from "./need-date-deep-discovery-v6-contract.js";

export function createEmptyBlockerLedger(opportunityId) {
  return {
    version: "gdi-need-date-blocker-ledger-v6",
    opportunityId: opportunityId || null,
    blockers: [],
    resolved: [],
    exhaustedSources: false,
    scheduleRefresh: false,
  };
}

export function addBlocker(ledger, { type, question, expectedCommercialValue, status = "OPEN" }) {
  const next = {
    ...(ledger || createEmptyBlockerLedger()),
  };
  next.blockers = [
    ...(next.blockers || []),
    {
      type: type || NEED_DATE_V6_BLOCKER.ACCESSIBILITY_UNCLASSIFIED,
      question: String(question || "").trim(),
      expectedCommercialValue: String(expectedCommercialValue || "").trim(),
      status,
      addedAt: new Date().toISOString(),
    },
  ];
  return next;
}

/**
 * Build follow-up investigation plan from open blockers.
 * Each follow-up must name a specific question — no identical query burns.
 */
export function planBlockerFollowUps(ledger, { maxFollowUps = 3, savedFindingKeys = [] } = {}) {
  const open = (ledger?.blockers || []).filter((b) => b.status === "OPEN");
  const saved = new Set(savedFindingKeys || []);
  const followUps = [];

  for (const b of open) {
    if (followUps.length >= maxFollowUps) break;
    const key = `${b.type}|${b.question}`.toLowerCase();
    if (saved.has(key)) continue;
    followUps.push({
      blockerType: b.type,
      researchQuestion: b.question,
      expectedCommercialValue: b.expectedCommercialValue,
      reuseSavedFindings: true,
      forbidIdenticalQueryReplay: true,
      paid: true,
      requiresFounderApproval: true,
    });
  }

  return {
    version: "gdi-need-date-blocker-followups-v6",
    opportunityId: ledger?.opportunityId || null,
    openCount: open.length,
    followUps,
    stopWhen:
      open.length === 0 ||
      followUps.length === 0 ||
      ledger?.exhaustedSources === true ||
      "authorized_budget_boundary",
  };
}

export function inferBlockersFromCandidateGaps(candidate = {}) {
  const ledger = createEmptyBlockerLedger(candidate.opportunityId || candidate.id);
  const gaps = [];

  if (!candidate.demandOwnerVerified && !candidate.organizationName) {
    gaps.push({
      type: NEED_DATE_V6_BLOCKER.DEMAND_OWNER_UNCLEAR,
      question: "Who is the demand-generating organization for this activity?",
      expectedCommercialValue: "Enables buyer-first outreach and deduplication.",
    });
  }
  if (!candidate.identifiableBuyer) {
    gaps.push({
      type: NEED_DATE_V6_BLOCKER.BUYER_NOT_VERIFIED,
      question: "Who owns hotel sourcing or can refer to the lodging decision-maker?",
      expectedCommercialValue: "Actionable next sales step.",
    });
  }
  if (!candidate.timingAdequateForNeedMonth) {
    gaps.push({
      type: NEED_DATE_V6_BLOCKER.DEMAND_TIMING_UNKNOWN,
      question: "When would hotel revenue occur relative to Aug/Sep/Oct 2027?",
      expectedCommercialValue: "Need-month match or Continuation labeling.",
    });
  }
  if (candidate.competingHostNamed && !candidate.overflowEvidence) {
    gaps.push({
      type: NEED_DATE_V6_BLOCKER.COMPETING_HOTEL_COMMITMENT_UNRESOLVED,
      question:
        "Is lodging fully committed to the named host/block, or is there evidenced overflow/open housing?",
      expectedCommercialValue: "Prevents speculative overflow staging.",
    });
  }
  if (!candidate.groupLodgingMechanism) {
    gaps.push({
      type: NEED_DATE_V6_BLOCKER.GROUP_LODGING_MECHANISM_UNCLEAR,
      question: "What is the guestroom / meeting / catering capture mechanism for Waterstone?",
      expectedCommercialValue: "Hotel-specific revenue thesis.",
    });
  }
  if (candidate.thinSourceOnly) {
    gaps.push({
      type: NEED_DATE_V6_BLOCKER.THIN_SOURCE_ONLY,
      question: "Can primary sources substantiate identity, timing and demand beyond a thin snippet?",
      expectedCommercialValue: "CIB-quality evidence.",
    });
  }

  let out = ledger;
  for (const g of gaps) out = addBlocker(out, g);
  return out;
}
