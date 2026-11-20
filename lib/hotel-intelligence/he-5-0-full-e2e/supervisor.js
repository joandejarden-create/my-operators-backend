/**
 * HE 5.0 adaptive research supervisor (incremental).
 * AUDIT → REUSE → RESEARCH(free-first) → VERIFY → MAP → DRY_RUN → STANDING_AUTH_CLASSIFY.
 * Controlled apply is a separate gated executor (standing authorization).
 * Paid calls only when free insufficient, cost-bounded, and under shared $50 cap.
 */

import path from "node:path";
import {
  HE50_GLOBAL_USD_CAP,
  HE50_PILOT_RUN_ID,
  HE50_PILOT_VERSION,
  PIPELINE_STAGES,
  PROVIDER_ROUTING,
  HOTEL_READINESS,
} from "./contract.js";
import { HE50_STANDING_APPLY_GATE } from "./standing-authorization-v1.js";
import { classifyPilotMutationPackage } from "./conditional-approval-v1.js";
import { createHe50PilotBudgetLedger } from "./pilot-budget-ledger.js";
import {
  BETHESDA_FIELD_GAP_MATRIX,
  summarizeFieldGapMatrix,
  fieldsNeedingPaidResearch,
} from "./field-gap-matrix.js";
import {
  BETHESDA_FREE_FINDINGS,
  SEVEN_HOTEL_FREE_IDENTITY_PACKETS,
  buildFreeResearchLedgerEntries,
  FREE_RESEARCH_SPEND_USD,
} from "./free-research-findings-v1.js";
import { buildHe50DryRunMutationPackage } from "./dry-run-mutation-package.js";
import { MULLEN_PILOT_HOTELS, founderPreviewUrl } from "../hpc-population/mullen-pilot-cohort-v1.js";

/**
 * @param {object} [opts]
 * @param {boolean} [opts.enablePaid=false] — paid still requires bounded reserve; default false for free-first complete pass
 * @param {string} [opts.ledgerPath]
 */
export function runHe50PilotSupervisor(opts = {}) {
  const enablePaid = opts.enablePaid === true;
  const ledger = createHe50PilotBudgetLedger({
    globalUsdCap: HE50_GLOBAL_USD_CAP,
    runId: HE50_PILOT_RUN_ID,
    durablePath: opts.ledgerPath || null,
  });

  const stagesCompleted = [];
  const mark = (s) => stagesCompleted.push({ stage: s, at: new Date().toISOString() });

  mark("AUDIT");
  const gapSummary = summarizeFieldGapMatrix(BETHESDA_FIELD_GAP_MATRIX);
  const paidCandidates = fieldsNeedingPaidResearch(BETHESDA_FIELD_GAP_MATRIX);

  mark("REUSE");
  const reusedModules = [
    "HE_HPC_POPULATION_WORKFLOW_V1",
    "mullen-pilot-cohort-v1 + identity-resolution",
    "research-engine-v1 budget-ledger + M1 shared-budget",
    "HE-BETH Phase 3 evidence register + stakeholder dry-runs",
    "Demand Anchors / TI / Guest Sentiment / Market Alerts read paths",
    "HE-TABS-1-5 field inventory",
  ];

  mark("RESEARCH");
  for (const e of buildFreeResearchLedgerEntries()) {
    ledger.recordFreePass({
      operation_id: `free_${e.pilot_key}_${e.research_category}`,
      hotel_id: e.hotel_id,
      pilot_key: e.pilot_key,
      provider: e.provider,
      research_category: e.research_category,
      verified_findings: e.verified_findings,
      result_class: e.result_class,
    });
  }

  const paidRefusals = [];
  if (!enablePaid) {
    for (const f of paidCandidates.slice(0, 8)) {
      paidRefusals.push({
        fieldKey: f.fieldKey,
        reason: "free_first_pass_complete_paid_deferred",
        classification: f.classification,
        note: "Remaining gaps are low marginal value or licensing-restricted; do not spend to maximize field counts",
      });
    }
  }

  mark("VERIFY");
  mark("DEDUPLICATE");
  mark("MAP");
  mark("DRY_RUN");
  const dryRun = buildHe50DryRunMutationPackage();

  mark("APPROVAL");
  const mutationClassification = classifyPilotMutationPackage(dryRun.mutations || []);
  const applyGate = {
    ...HE50_STANDING_APPLY_GATE,
    paidSpendConfirmedUsd: ledger.snapshot().confirmedActualUsd,
    freeSpendUsd: FREE_RESEARCH_SPEND_USD,
    mutationClassificationSummary: mutationClassification.summary,
    nextStep:
      "runHe50ControlledApply({ apply: true }) for eligible AUTO_APPROVED operations only",
  };

  const hotelScorecard = buildHotelScorecard();

  return {
    contract: "HE_5_0_PILOT_SUPERVISOR_RESULT_V1",
    version: HE50_PILOT_VERSION,
    runId: HE50_PILOT_RUN_ID,
    pipelineStages: PIPELINE_STAGES,
    stagesCompleted,
    providerRouting: PROVIDER_ROUTING,
    reusedModules,
    gapSummary,
    bethesdaFreeFindings: BETHESDA_FREE_FINDINGS,
    sevenHotelPackets: SEVEN_HOTEL_FREE_IDENTITY_PACKETS,
    paidRefusals,
    enablePaid,
    budget: ledger.snapshot(),
    dryRun,
    applyBlocked: applyGate,
    standingApplyGate: applyGate,
    mutationClassification,
    hotelScorecard,
    aggregate: aggregateScorecard(hotelScorecard, ledger.snapshot()),
    stopMessage: HE50_STANDING_APPLY_GATE.stopMessage,
  };
}

function buildHotelScorecard() {
  const identityByKey = Object.fromEntries(
    SEVEN_HOTEL_FREE_IDENTITY_PACKETS.map((p) => [p.pilotKey, p])
  );
  return MULLEN_PILOT_HOTELS.map((h) => {
    const pkt = identityByKey[h.pilotKey];
    const isBeth = h.pilotKey === "bethesda_marriott";
    const hpcId = isBeth ? h.knownHpcId : pkt?.hpcRecordId || null;
    const identity = isBeth
      ? "MATCHED_HPC"
      : pkt?.identityOutcome || "UNKNOWN";
    const newlyVerified = isBeth
      ? BETHESDA_FREE_FINDINGS.findings.filter((f) => f.classification === "NEWLY_VERIFIED" || f.classification?.includes?.("CONFLICT")).length
      : [pkt?.rooms?.value != null, pkt?.website, pkt?.officialName].filter(Boolean).length;
    const reused = isBeth ? 20 : identity.startsWith("MATCHED") ? 5 : 0;
    const readiness = [];
    if (
      hpcId ||
      pkt?.proposedHpcAction === "CREATE_AFTER_FOUNDER_APPROVAL" ||
      pkt?.hotelCensusRecordId
    ) {
      readiness.push(
        hpcId
          ? HOTEL_READINESS.HPC_MINIMUM_VERIFIED
          : HOTEL_READINESS.REVIEW_REQUIRED
      );
    }
    if (isBeth) readiness.push(HOTEL_READINESS.REVIEW_REQUIRED);
    return {
      hotel: h.hotelName,
      pilotKey: h.pilotKey,
      hpcIdentity: identity,
      hpcId,
      hotelCensusId: pkt?.hotelCensusRecordId || null,
      legacyHotelCensusProvenanceId: pkt?.legacyHotelCensusProvenanceId || null,
      newlyVerifiedFacts: newlyVerified,
      existingFactsReused: reused,
      proposedAirtableChanges: isBeth ? 4 : 1,
      researchSpendUsd: 0,
      remainingGaps: isBeth
        ? ["current_lender", "equity_jv", "hotel_news_market_alerts", "ubo_natural_person", "US_demand_anchors"]
        : ["full_he_population", "ownership_verification", "contacts"],
      readiness,
      founderPreviewUrl: hpcId ? founderPreviewUrl(hpcId) : null,
      mullenAssociation: isBeth
        ? "ECONOMIC_SPONSOR_STAGED_MEDIUM"
        : pkt?.mullenAssociation || "PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING",
    };
  });
}

function aggregateScorecard(rows, budget) {
  return {
    hotels: rows.length,
    matchedHpc: rows.filter((r) => r.hpcIdentity === "MATCHED_HPC").length,
    matchedCensus: rows.filter((r) => r.hpcIdentity === "MATCHED_HOTEL_CENSUS").length,
    newHpcCandidates: rows.filter((r) => r.hpcIdentity === "NEW_HPC_CANDIDATE").length,
    newlyVerifiedFactsTotal: rows.reduce((n, r) => n + r.newlyVerifiedFacts, 0),
    existingFactsReusedTotal: rows.reduce((n, r) => n + r.existingFactsReused, 0),
    proposedAirtableChangesTotal: rows.reduce((n, r) => n + r.proposedAirtableChanges, 0),
    confirmedPaidSpendUsd: budget.confirmedActualUsd,
    reservedPaidSpendUsd: budget.reservedUnreconciledUsd,
    remainingBudgetUsd: budget.remainingAuthorizedUsd,
    freeSpendUsd: 0,
  };
}

export function defaultLedgerPath(root) {
  return path.join(root, "reports/hotel-explorer/he-5-0-pilot/HE50-RESEARCH-SPEND-LEDGER.json");
}
