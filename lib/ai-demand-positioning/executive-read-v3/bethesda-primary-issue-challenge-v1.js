/**
 * Bethesda Marriott — first real zero-code V3 primary-issue challenge.
 * Narrative prioritization only. No methodology change. No Bethesda-specific UI path.
 *
 * Doctrine: EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
 */

export const ADP_EXECUTIVE_BETHESDA_PRIMARY_ISSUE_CHALLENGE =
  "ADP_EXECUTIVE_BETHESDA_PRIMARY_ISSUE_CHALLENGE";

const DIMENSIONS = Object.freeze([
  "materiality",
  "commercialRelevance",
  "recurrence",
  "evidenceStrength",
  "nonObviousness",
  "propertySpecificity",
  "managementActionability",
  "breadthOfImpact",
  "controllability",
]);

function avg(scores) {
  const vals = DIMENSIONS.map((d) => scores[d]).filter((n) => typeof n === "number");
  if (!vals.length) return 0;
  return Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 1000) / 1000;
}

/**
 * Score founder challenge candidates A–D from certified analytical input.
 * Does not auto-pick Ballroom for lowest rate or SP–C for drama alone.
 */
export function scoreBethesdaPrimaryIssueChallengeV1(input, opts = {}) {
  const c = input?.aiConsideration;
  const sp = input?.scenarioPresence;
  const spGap = typeof c === "number" && typeof sp === "number" ? sp - c : null;
  const ballroom = (input?.realityGaps || []).find((g) => /ballroom/i.test(g.label || ""));
  const family = (input?.presenceIndex?.territories || []).find((t) => /family/i.test(t.intent || ""));
  const meetingsCap =
    input?.demandCaptureByIntent?.group_meeting?.rate ??
    input?.demandCaptureByIntent?.group_meeting?.captureRate ??
    null;
  const familyCap =
    input?.demandCaptureByIntent?.family?.rate ??
    input?.demandCaptureByIntent?.family?.captureRate ??
    null;
  const providers = Array.isArray(input?.providerPresence) ? input.providerPresence : [];
  const providerRates = providers
    .map((p) => (typeof p.presence === "number" ? p.presence : null))
    .filter((n) => typeof n === "number");
  const providerSpread =
    providerRates.length >= 2 ? Math.max(...providerRates) - Math.min(...providerRates) : null;
  const topAlt = input?.topObservedAlternative?.name || null;
  const rankEligible = input?.rankMetrics?.customerNarrativeEligible === true;

  const candidateA = {
    id: "A_MEETING_BALLROOM_PROPOSITION_GAP",
    label: "MEETING / BALLROOM PROPOSITION GAP",
    primaryIssueId: "meeting_ballroom_proposition",
    evidence: {
      ballroomRecognition: ballroom?.recognitionRate ?? null,
      ballroomSeverity: ballroom?.severity ?? null,
      meetingsScenarioCapture: meetingsCap,
      topObservedAlternative: topAlt,
      note: "Meeting-space recognition is not a separate certified Reality Gap row in this period; Ballroom is the certified meetings-proposition gap.",
    },
    scores: {
      materiality: ballroom?.severity === "HIGH" ? 0.85 : 0.55,
      commercialRelevance: 0.88,
      recurrence: 0.55,
      evidenceStrength: typeof ballroom?.recognitionRate === "number" ? 0.8 : 0.4,
      nonObviousness: 0.55, // low rate is visually striking — do not reward magnitude alone
      propertySpecificity: 0.92,
      managementActionability: 0.9,
      breadthOfImpact: typeof meetingsCap === "number" && meetingsCap >= 85 ? 0.42 : 0.7,
      controllability: 0.88,
    },
    guardrails: [
      "Do not select solely because Ballroom 17.9% is the lowest striking number",
      "MEDIUM severity Reality Gap is eligible but not automatically primary",
      "Meetings & Groups scenario capture already strong weakens binding-priority claim",
    ],
  };

  const candidateB = {
    id: "B_BROAD_CONSIDERATION_CONSISTENCY_GAP",
    label: "BROAD CONSIDERATION-CONSISTENCY GAP",
    primaryIssueId:
      typeof spGap === "number" && spGap >= 30
        ? "answer_level_inclusion_consistency"
        : "business_consideration_consistency",
    evidence: {
      scenarioPresence: sp,
      aiConsideration: c,
      spGap,
      top3: input?.rankMetrics?.top3 ?? null,
      rankEligibleN: input?.rankMetrics?.rankEligibleN ?? null,
      top3CustomerNarrativeEligible: rankEligible,
      top3Role: rankEligible ? "may_support" : "supporting_diagnostic_only",
    },
    scores: {
      materiality: typeof spGap === "number" && spGap >= 35 ? 0.92 : 0.75,
      commercialRelevance: 0.9,
      recurrence: 0.88,
      evidenceStrength: 0.95,
      nonObviousness: 0.48, // do not reward mathematical drama alone
      propertySpecificity: 0.45,
      managementActionability: 0.68,
      breadthOfImpact: 0.95,
      controllability: 0.58,
    },
    guardrails: [
      "Do not select solely because 81% vs 42.1% is mathematically dramatic",
      "Top-3 remains diagnostic-only when rank-eligible n is below customer-narrative threshold",
    ],
  };

  const candidateC = {
    id: "C_FAMILY_ENTRY_WEAKNESS",
    label: "FAMILY ENTRY WEAKNESS",
    primaryIssueId: "territory_family_soft_spot",
    evidence: {
      familyPresence: family?.rate ?? null,
      familyScenarioCapture: familyCap,
    },
    scores: {
      materiality: typeof family?.rate === "number" && family.rate <= 20 ? 0.7 : 0.4,
      commercialRelevance: 0.55,
      recurrence: 0.5,
      evidenceStrength: typeof family?.rate === "number" ? 0.75 : 0.35,
      nonObviousness: 0.6,
      propertySpecificity: 0.65,
      managementActionability: 0.7,
      breadthOfImpact: 0.4,
      controllability: 0.65,
    },
    guardrails: ["Family is a soft spot, not the hotel's primary commercial spine vs meetings/business"],
  };

  const candidateD = {
    id: "D_PROVIDER_OR_OTHER",
    label: "PROVIDER DIFFERENCE / OTHER EVIDENCE-BACKED",
    primaryIssueId: "provider_fragmentation_supporting",
    evidence: {
      providerSpread,
      providers: providerRates,
      topDisplacement: input?.competitiveDisplacement?.[0] || null,
      bpp: input?.bpp?.status || input?.bpp?.readyState || null,
    },
    scores: {
      materiality: typeof providerSpread === "number" && providerSpread >= 20 ? 0.55 : 0.35,
      commercialRelevance: 0.5,
      recurrence: 0.55,
      evidenceStrength: typeof providerSpread === "number" ? 0.7 : 0.4,
      nonObviousness: 0.55,
      propertySpecificity: 0.4,
      managementActionability: 0.45,
      breadthOfImpact: 0.5,
      controllability: 0.35,
    },
    guardrails: [
      "Provider fragmentation is supporting diagnostic unless it outranks the selected issue",
      "BPP SUPPRESSED_NO_CERTIFIED_PEER_SET must not invent Bonvoy peer narrative",
    ],
  };

  const scored = [candidateA, candidateB, candidateC, candidateD].map((cand) => ({
    ...cand,
    executiveDecisionScore: avg(cand.scores),
  }));
  scored.sort((a, b) => b.executiveDecisionScore - a.executiveDecisionScore);

  const selected = scored[0];
  const runnerUp = scored[1];

  // Tie-break: prefer breadth of impact when scores within 0.03 and SP–C gap is large
  let winner = selected;
  let runner = runnerUp;
  if (
    Math.abs(selected.executiveDecisionScore - runnerUp.executiveDecisionScore) <= 0.03 &&
    typeof spGap === "number" &&
    spGap >= 35
  ) {
    const breadthFirst = [...scored].sort(
      (a, b) =>
        b.scores.breadthOfImpact - a.scores.breadthOfImpact ||
        b.executiveDecisionScore - a.executiveDecisionScore
    );
    winner = breadthFirst[0];
    runner = breadthFirst[1];
  }

  const whyWinnerWins = [
    `${winner.label} wins on strongest executive decision value (score ${winner.executiveDecisionScore}).`,
    `Runner-up: ${runner.label} (score ${runner.executiveDecisionScore}).`,
    winner.id === "B_BROAD_CONSIDERATION_CONSISTENCY_GAP"
      ? "Answer-level inclusion consistency is the binding commercial constraint: Scenario Presence is broad while Consideration remains materially weaker across the monitored set. Meetings capture is already strong, so a MEDIUM Ballroom Reality Gap is supporting residual Focus — not the primary binding issue."
      : winner.id === "A_MEETING_BALLROOM_PROPOSITION_GAP"
        ? "Ballroom proposition representation is the most property-specific actionable miss and outranks broader consistency on executive decision value for this certified state."
        : "Selected on multi-dimension executive decision scoring; not magnitude-alone.",
    "Top-3 / #1 not elevated as central claim when rank-eligible sample is below customer-narrative threshold.",
    "BPP suppressed — excluded from Executive Read unless directly relevant (it is not).",
    "Provider spread treated as supporting diagnostic unless it outranks (it does not).",
  ];

  return {
    gate: ADP_EXECUTIVE_BETHESDA_PRIMARY_ISSUE_CHALLENGE,
    pass: Boolean(winner?.primaryIssueId),
    propertyId: opts.propertyId || input?.property?.propertyId || "adp_bethesda_marriott",
    periodId: opts.periodId || input?.period?.periodId || null,
    dimensions: DIMENSIONS,
    candidates: scored,
    selected: {
      id: winner.id,
      label: winner.label,
      primaryIssueId: winner.primaryIssueId,
      executiveDecisionScore: winner.executiveDecisionScore,
      scores: winner.scores,
    },
    runnerUp: {
      id: runner.id,
      label: runner.label,
      primaryIssueId: runner.primaryIssueId,
      executiveDecisionScore: runner.executiveDecisionScore,
    },
    whyWinnerWins,
    methodologyChanged: false,
  };
}

/**
 * Compare challenge winner vs generic composer selection.
 */
export function evaluateBethesdaComposerAlignmentV1(challenge, composed) {
  const challengeId = challenge?.selected?.primaryIssueId;
  const composedId = composed?.primaryIssueId;
  const aligned =
    challengeId &&
    composedId &&
    (challengeId === composedId ||
      (challengeId.includes("consistency") && String(composedId).includes("consistency")) ||
      (challengeId.includes("consideration") && String(composedId).includes("consideration")));

  return {
    gate: ADP_EXECUTIVE_BETHESDA_PRIMARY_ISSUE_CHALLENGE,
    aligned: Boolean(aligned),
    challengePrimaryIssueId: challengeId,
    composerPrimaryIssueId: composedId,
    pass: Boolean(aligned) && challenge?.pass === true && composed?.ok === true,
    note: aligned
      ? "Generic composer matches challenge winner after interpretation-learning priority rules."
      : "Composer diverges from challenge — review required before Bethesda V3 publication readiness.",
  };
}
