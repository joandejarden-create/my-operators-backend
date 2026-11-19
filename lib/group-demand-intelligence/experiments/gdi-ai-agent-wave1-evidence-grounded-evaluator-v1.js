/**
 * Evidence-grounded Wave 1 decision evaluator.
 *
 * materialLift requires independently verified incremental commercial value:
 *   - more verified named commercial contacts, and/or
 *   - more genuinely-new verified demand evidence
 *
 * Explicitly NOT material lift on their own:
 *   drawerReady, narrative/formatting, recovery overlays, fewer retrievals,
 *   rubric totals, eligibility-only changes, repackaged existing facts.
 *
 * Efficiency and correct demotions are tracked separately from materialLift.
 */

export const EVALUATOR_VERSION = "gdi-ai-agent-wave1-evidence-grounded-evaluator-v1";

export const NON_MATERIAL_LIFT_REASONS = Object.freeze([
  "drawerReady_only",
  "formatting_or_narrative_only",
  "recovery_overlay_only",
  "fewer_retrievals_only",
  "rubric_total_only",
  "eligibility_status_only",
  "repackaged_existing_facts",
]);

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function commercialOf(armOrPacket) {
  if (!armOrPacket) return {};
  return armOrPacket.commercial || armOrPacket;
}

function challengerOf(arm) {
  return arm?.independentChallenger || arm?.challenger || {};
}

function recoveredCount(arm) {
  const ch = challengerOf(arm);
  return (ch.buckets?.existingRecoveredEvidence || []).length;
}

function genuinelyNewCount(armOrPacket) {
  const packet = commercialOf(armOrPacket);
  if (packet.genuinelyNewVerifiedCount != null) return num(packet.genuinelyNewVerifiedCount);
  const ch = challengerOf(armOrPacket);
  return (ch.buckets?.genuinelyNewVerifiedEvidence || []).length;
}

function verifiedNamedCount(armOrPacket) {
  const packet = commercialOf(armOrPacket);
  if (packet.verifiedNamedCount != null) return num(packet.verifiedNamedCount);
  const ch = challengerOf(armOrPacket);
  return num(
    ch.contact?.verifiedNamedCount ??
      ch.contactRelevance?.verifiedNamedCount ??
      ch.after?.contactRelevance?.verifiedNamedCount ??
      0,
  );
}

/**
 * Evidence-grounded material commercial lift (Conclusion A).
 * @returns {{ materialLift: boolean, reasons: string[], rejectedNonMaterial: string[] }}
 */
export function evaluateMaterialCommercialLift(armA, armB) {
  const aNamed = verifiedNamedCount(armA);
  const bNamed = verifiedNamedCount(armB);
  const aNew = genuinelyNewCount(armA);
  const bNew = genuinelyNewCount(armB);
  const aRecovered = recoveredCount(armA);
  const bRecovered = recoveredCount(armB);

  const namedLift = bNamed > aNamed;
  const newEvidenceLift = bNew > aNew;

  const packetA = commercialOf(armA);
  const packetB = commercialOf(armB);
  const drawerOnly =
    packetA.drawerReady === false &&
    packetB.drawerReady === true &&
    !namedLift &&
    !newEvidenceLift;
  const recoveryOnly = bRecovered > aRecovered && !namedLift && !newEvidenceLift;
  const fewerFetchesOnly =
    num(packetB.fetchCount) < num(packetA.fetchCount) && !namedLift && !newEvidenceLift;
  const eligibilityOnly =
    String(packetA.eligibilityStatus || "") !== String(packetB.eligibilityStatus || "") &&
    !namedLift &&
    !newEvidenceLift;

  const rejectedNonMaterial = [];
  if (drawerOnly) rejectedNonMaterial.push("drawerReady_only");
  if (recoveryOnly) rejectedNonMaterial.push("recovery_overlay_only");
  if (fewerFetchesOnly) rejectedNonMaterial.push("fewer_retrievals_only");
  if (eligibilityOnly) rejectedNonMaterial.push("eligibility_status_only");

  const reasons = [];
  if (namedLift) reasons.push("verified_named_contact_gain");
  if (newEvidenceLift) reasons.push("genuinely_new_verified_demand_evidence_gain");

  return {
    materialLift: namedLift || newEvidenceLift,
    reasons,
    rejectedNonMaterial,
    verifiedNamedDelta: bNamed - aNamed,
    genuinelyNewVerifiedDelta: bNew - aNew,
    recoveredEvidenceDelta: bRecovered - aRecovered,
  };
}

/**
 * Efficiency and demotion signals — never promote these into materialLift.
 */
export function evaluateEfficiencyAndDemotions(caseRow) {
  const packetA = commercialOf(caseRow.armA);
  const packetB = commercialOf(caseRow.armB);
  const slot = caseRow.slot || "";

  const fetchDelta = num(packetB.fetchCount) - num(packetA.fetchCount);
  const durationMsDelta = num(packetB.durationMs) - num(packetA.durationMs);
  const paidUsdDelta = num(packetB.actualPaidUsd) - num(packetA.actualPaidUsd);

  const speculativeNotFalselyPromoted =
    slot !== "rejected_speculative" || packetB.salesReady !== true;
  const competitorNotFalselyPromoted =
    slot !== "competitor_cross_hotel" || packetB.salesReady !== true;
  const falseSalesReadyUnderChallenger =
    packetB.salesReady === true && packetB.challengerBlocked === true;

  return {
    efficiency: {
      fetchDelta,
      durationMsDelta,
      paidUsdDelta,
      fetchSavingsVsArmA: fetchDelta < 0,
      timeSavingsVsArmA: durationMsDelta < 0,
    },
    demotions: {
      speculativeNotFalselyPromoted,
      competitorNotFalselyPromoted,
      falseSalesReadyUnderChallenger,
      dispositionA: packetA.disposition || null,
      dispositionB: packetB.disposition || null,
      eligibilityA: packetA.eligibilityStatus || null,
      eligibilityB: packetB.eligibilityStatus || null,
    },
    nonMaterialSignals: {
      drawerReadyTransition: packetA.drawerReady === false && packetB.drawerReady === true,
      salesReadyTransition: packetA.salesReady !== true && packetB.salesReady === true,
    },
  };
}

/**
 * Build corrected deltas for a focus-case result row (live or offline rescore).
 */
export function buildEvidenceGroundedDeltas(caseRow, attribution = null) {
  const lift = evaluateMaterialCommercialLift(caseRow.armA, caseRow.armB);
  const side = evaluateEfficiencyAndDemotions(caseRow);
  const packetA = commercialOf(caseRow.armA);
  const packetB = commercialOf(caseRow.armB);
  const attr = attribution || caseRow.attribution || {};

  return {
    genuinelyNewVerifiedCount: lift.genuinelyNewVerifiedDelta,
    verifiedNamedCount: lift.verifiedNamedDelta,
    drawerReady: side.nonMaterialSignals.drawerReadyTransition,
    durationMs: side.efficiency.durationMsDelta,
    fetchCount: side.efficiency.fetchDelta,
    paidUsd: side.efficiency.paidUsdDelta,
    budgetAsymmetry: attr.tag === "BUDGET_ASYMMETRY",
    // Evidence-grounded only — drawerReady / efficiency excluded
    materialLift: lift.materialLift,
    materialLiftReasons: lift.reasons,
    rejectedNonMaterialLiftReasons: lift.rejectedNonMaterial,
    efficiency: side.efficiency,
    demotions: side.demotions,
    nonMaterialSignals: side.nonMaterialSignals,
    evaluatorVersion: EVALUATOR_VERSION,
    // Legacy drawerReady must never alone set materialLift
    legacyDrawerReadyWouldHaveLifted:
      side.nonMaterialSignals.drawerReadyTransition && !lift.materialLift,
  };
}

/**
 * Wave 1 decision: ADOPT forbidden when zero evidence-grounded material lifts.
 */
export function decideFromWave1EvidenceGrounded(results, opts = {}) {
  const adoptMeans =
    opts.adoptMeans ||
    "Further validation or selective integration only — not automatic production-wide deployment";
  const base = {
    adoptMeans,
    conclusionScope:
      opts.conclusionScope || "A_incremental_research_on_existing_opportunities",
    doesNotEstablish:
      opts.doesNotEstablish || "B_incremental_discovery_of_genuinely_new_opportunities",
    evaluatorVersion: EVALUATOR_VERSION,
  };

  if (results.some((r) => r.ok === false)) {
    return { decision: "REJECT", reason: "FOCUS_CASE_EXECUTION_FAILURE", ...base };
  }
  if (results.some((r) => r.deltas?.budgetAsymmetry)) {
    return { decision: "REJECT", reason: "BUDGET_ASYMMETRY", ...base };
  }
  if (results.some((r) => !r.armA?.activeResearch || !r.armB?.activeResearch)) {
    return { decision: "REJECT", reason: "UNFAIR_PARITY_ARM_NOT_ACTIVE", ...base };
  }
  if (
    results.some(
      (r) =>
        num(r.armA?.commercial?.actualPaidUsd) > 0 ||
        num(r.armB?.commercial?.actualPaidUsd) > 0,
    )
  ) {
    return { decision: "REJECT", reason: "PAID_SPEND_NOT_ZERO", ...base };
  }

  const falseSalesReady = results.filter(
    (r) =>
      r.armB?.commercial?.salesReady === true &&
      r.armB?.commercial?.challengerBlocked === true,
  );
  if (falseSalesReady.length) {
    return {
      decision: "REJECT",
      reason: "FALSE_SALES_READY_PRESSURE",
      falseSalesReadyIds: falseSalesReady.map((r) => r.opportunityId),
      ...base,
    };
  }

  const speculativeHeld = results
    .filter((r) => r.slot === "rejected_speculative")
    .every((r) => r.armB?.commercial?.salesReady !== true);
  const competitorNotFalselyPromoted = results
    .filter((r) => r.slot === "competitor_cross_hotel")
    .every((r) => r.armB?.commercial?.salesReady !== true);

  const materialCases = results.filter((r) => r.deltas?.materialLift === true);
  const qualityDemotion =
    speculativeHeld &&
    competitorNotFalselyPromoted &&
    results.some(
      (r) =>
        (r.slot === "rejected_speculative" || r.slot === "competitor_cross_hotel") &&
        r.armB?.commercial?.challengerBlocked === true,
    );

  // Hard gate: never ADOPT with zero evidence-grounded commercial lifts
  if (materialCases.length === 0) {
    if (qualityDemotion) {
      return {
        decision: "ITERATE",
        reason: "QUALITY_DEMOTIONS_WITHOUT_MATERIAL_LIFT",
        materialCaseIds: [],
        ...base,
      };
    }
    return {
      decision: "REJECT",
      reason: "NO_CONFIRMED_COMMERCIAL_LIFT_OR_QUALITY_GAIN",
      materialCaseIds: [],
      ...base,
    };
  }

  if (
    materialCases.length >= 2 &&
    speculativeHeld &&
    competitorNotFalselyPromoted
  ) {
    return {
      decision: "ADOPT",
      reason: "GE_2_MATERIAL_LIFTS_WITH_SAFE_GATES",
      materialCaseIds: materialCases.map((r) => r.opportunityId),
      ...base,
    };
  }

  return {
    decision: "ITERATE",
    reason: "SOME_MATERIAL_LIFT_BELOW_ADOPT_THRESHOLD",
    materialCaseIds: materialCases.map((r) => r.opportunityId),
    ...base,
  };
}

/**
 * Offline rescore of a preserved WAVE1_RESULTS.json payload (no network).
 */
export function rescoreWave1Report(report) {
  if (!report || !Array.isArray(report.results)) {
    throw new Error("WAVE1 report missing results[]");
  }
  const previousDecision = report.decision || null;
  const results = report.results.map((row) => {
    const deltas = buildEvidenceGroundedDeltas(row, row.attribution);
    return {
      ...row,
      deltas,
      previousHarnessMaterialLift: row.deltas?.materialLift === true,
      previousHarnessDrawerReadyLift: row.deltas?.drawerReady === true,
    };
  });
  const decision = decideFromWave1EvidenceGrounded(results);
  const materialCaseIds = results
    .filter((r) => r.deltas.materialLift)
    .map((r) => r.opportunityId);
  const drawerOnlyPreviouslyLifted = results
    .filter((r) => r.previousHarnessDrawerReadyLift && !r.deltas.materialLift)
    .map((r) => r.opportunityId);

  return {
    schema: "gdi-ai-agent-wave1-evidence-grounded-rescore-v1",
    evaluatorVersion: EVALUATOR_VERSION,
    rescoredAt: new Date().toISOString(),
    networkUsed: false,
    paidUsd: 0,
    previousDecision,
    decision,
    materialCaseIds,
    drawerOnlyPreviouslyLifted,
    caseCount: results.length,
    results,
    comparisonToIndependentEval: {
      expectedDecisionWhenZeroLifts: "ITERATE_OR_REJECT_NOT_ADOPT",
      materialCommercialLifts: materialCaseIds.length,
    },
  };
}

/**
 * Analyze Arm B STOP decisions vs Arm A verified-evidence yield (logs only).
 */
export function analyzeAdaptiveStopBehavior(report) {
  const rows = (report?.results || []).map((row) => {
    const aNew = genuinelyNewCount(row.armA);
    const bNew = genuinelyNewCount(row.armB);
    const aNamed = verifiedNamedCount(row.armA);
    const bNamed = verifiedNamedCount(row.armB);
    const aFetches = num(row.armA?.commercial?.fetchCount ?? row.armA?.costs?.fetchCount);
    const bFetches = num(row.armB?.commercial?.fetchCount ?? row.armB?.costs?.fetchCount);
    const stopLog = row.armB?.liveCycleSummary?.stopLog || [];
    const stopDecision = stopLog[0]?.decision || null;
    const stopJustification = stopLog[0]?.justification || null;
    const evidenceLost = aNew > bNew || aNamed > bNamed;
    const fetchSaved = bFetches < aFetches;
    let classification = "NEUTRAL";
    if (fetchSaved && !evidenceLost) classification = "BENEFICIAL_EFFICIENCY";
    else if (fetchSaved && evidenceLost) classification = "PREMATURE_STOP_EVIDENCE_LOSS";
    else if (!fetchSaved && evidenceLost) classification = "UNDERPERFORMED_WITHOUT_SAVINGS";
    else if (bNew > aNew || bNamed > aNamed) classification = "AGENT_EVIDENCE_GAIN";

    return {
      opportunityId: row.opportunityId,
      hotelName: row.hotelName,
      slot: row.slot,
      stopDecision,
      stopJustification,
      armA: { fetches: aFetches, newVerified: aNew, verifiedNamed: aNamed },
      armB: { fetches: bFetches, newVerified: bNew, verifiedNamed: bNamed },
      fetchDelta: bFetches - aFetches,
      newVerifiedDelta: bNew - aNew,
      verifiedNamedDelta: bNamed - aNamed,
      classification,
    };
  });

  return {
    schema: "gdi-ai-agent-wave1-stop-behavior-analysis-v1",
    analyzedAt: new Date().toISOString(),
    networkUsed: false,
    summary: {
      beneficialEfficiency: rows.filter((r) => r.classification === "BENEFICIAL_EFFICIENCY").length,
      prematureStopEvidenceLoss: rows.filter(
        (r) => r.classification === "PREMATURE_STOP_EVIDENCE_LOSS",
      ).length,
      agentEvidenceGain: rows.filter((r) => r.classification === "AGENT_EVIDENCE_GAIN").length,
      underperformedWithoutSavings: rows.filter(
        (r) => r.classification === "UNDERPERFORMED_WITHOUT_SAVINGS",
      ).length,
    },
    cases: rows,
  };
}
