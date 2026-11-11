/**
 * Hospitality-demand discovery gates V2.
 * Gate 1 = verified operational activity.
 * Gate 2 = hospitality-demand potential for deep research spend.
 * Ranking prioritizes demand mechanism over funding/fame.
 */
import { hasAssociationProcurementBoilerplate } from "../../scaling-drawer-presentation-contract-v1.js";

export const EXA_AGENT_HOSPITALITY_DEMAND_GATES_V2 =
  "gdi-scaling-exa-agent-hospitality-demand-gates-v2";

export const EXA_AGENT_DISCOVERY_CONTRACT_V2 =
  "gdi-scaling-exa-agent-discovery-v2";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function companyName(c = {}) {
  return (
    text(c?.companyIdentity?.legalOrTradingName) ||
    text(c?.companyIdentity?.canonicalName) ||
    ""
  );
}

function activitySummary(c = {}) {
  return (
    text(c?.verifiedBusinessActivity) ||
    text(c?.businessTrigger?.summary) ||
    ""
  );
}

function activityDateOf(c = {}) {
  return (
    text(c?.activityDate) ||
    text(c?.activityChronology?.activityDate) ||
    text(c?.activityChronology?.sourcePublicationDate) ||
    ""
  );
}

function geographyOf(c = {}) {
  return (
    text(c?.activityGeography?.primaryLocation) ||
    text(c?.activityGeography) ||
    ""
  );
}

function demandMechanism(c = {}) {
  return (
    text(c?.travelGeneratingMechanism) ||
    text(c?.hotelDemandHypothesis?.mechanism) ||
    ""
  );
}

function evidenceLevel(c = {}) {
  return text(c?.hospitalityDemandEvidenceLevel) || "";
}

function isPublisher(name) {
  return /mynewsdesk|pr newswire|business wire|globe newswire|ein presswire/i.test(
    name,
  );
}

/**
 * Gate 1 — Verified operational activity.
 * @returns {"PASS"|"INVESTIGATE"|"REJECT"}
 */
export function assessGate1OperationalActivity(candidate = {}, opts = {}) {
  const issues = [];
  const name = companyName(candidate);
  if (!name) {
    issues.push({ code: "identity_missing", severity: "P0" });
  }
  if (isPublisher(name) || candidate?.companyIdentity?.entityType === "publisher_or_wire") {
    issues.push({ code: "publisher_as_company", severity: "P0" });
  }
  if (opts.existingDuplicate === true) {
    issues.push({ code: "duplicate_opportunity", severity: "P0" });
  }

  const summary = activitySummary(candidate);
  if (!summary) {
    issues.push({ code: "activity_missing", severity: "P0" });
  }

  const growthType = text(candidate.growthSignalType);
  if (growthType === "funding_only" || growthType === "generic_growth") {
    issues.push({ code: "growth_signal_only", severity: "P1" });
  }
  if (growthType === "hiring_only") {
    issues.push({ code: "hiring_only", severity: "P1" });
  }

  const physical = text(candidate.physicalActivityIndicator);
  if (physical === "remote_or_virtual_only" || physical === "none") {
    issues.push({ code: "no_physical_activity", severity: "P1" });
  }

  const geo = geographyOf(candidate);
  if (!geo) {
    issues.push({ code: "geography_missing", severity: "P1" });
  }

  const date = activityDateOf(candidate);
  if (!date) {
    issues.push({ code: "timeframe_weak", severity: "P1" });
  }

  const claims = [
    ...(Array.isArray(candidate.evidenceClaims) ? candidate.evidenceClaims : []),
    ...(Array.isArray(candidate.supportingEvidence)
      ? candidate.supportingEvidence
      : []),
  ];
  const hasUrl = claims.some((x) => text(x.url || x.sourceUrl));
  if (!hasUrl) {
    issues.push({ code: "evidence_urls_missing", severity: "P1" });
  }

  const p0 = issues.filter((i) => i.severity === "P0");
  const p1 = issues.filter((i) => i.severity === "P1");

  let gate1 = "PASS";
  if (p0.length) gate1 = "REJECT";
  else if (p1.length >= 2) gate1 = "INVESTIGATE";
  else if (p1.length === 1 && !date) gate1 = "INVESTIGATE";
  else if (p1.some((i) => i.code === "no_physical_activity")) gate1 = "REJECT";
  else if (p1.length) gate1 = "INVESTIGATE";

  // Agent-supplied gate1 respected only when stricter
  const agentGate1 = text(candidate.gate1OperationalActivity);
  if (agentGate1 === "REJECT") gate1 = "REJECT";
  if (agentGate1 === "INVESTIGATE" && gate1 === "PASS") gate1 = "INVESTIGATE";

  return { gate1, issues, version: EXA_AGENT_HOSPITALITY_DEMAND_GATES_V2 };
}

/**
 * Gate 2 — Hospitality-demand potential.
 * @returns disposition string
 */
export function assessGate2HospitalityDemand(candidate = {}, gate1Result = {}) {
  const issues = [];
  const gate1 = gate1Result.gate1 || assessGate1OperationalActivity(candidate).gate1;

  if (gate1 === "REJECT") {
    return {
      gate2: "REJECT",
      disposition: "REJECT_NO_HOTEL_DEMAND",
      issues: [{ code: "gate1_reject", severity: "P0" }],
      allowDeepResearchSpend: false,
      version: EXA_AGENT_HOSPITALITY_DEMAND_GATES_V2,
    };
  }

  const mech = demandMechanism(candidate);
  const level = evidenceLevel(candidate);
  const demandStatus = text(candidate?.hotelDemandHypothesis?.evidenceStatus);
  const guest = text(candidate.potentialGuestroomDemand);
  const meeting = text(candidate.potentialMeetingDemand);
  const catering = text(candidate.potentialCateringDemand);

  if (!mech && (demandStatus === "unsupported" || !demandStatus)) {
    issues.push({ code: "hotel_demand_not_credible", severity: "P0" });
  } else if (demandStatus === "unsupported" && level !== "speculative") {
    issues.push({ code: "hotel_demand_not_credible", severity: "P0" });
  }
  if (hasAssociationProcurementBoilerplate(mech)) {
    issues.push({ code: "association_boilerplate", severity: "P0" });
  }
  if (level === "no_meaningful_hospitality_connection") {
    issues.push({ code: "no_hospitality_connection", severity: "P0" });
  } else if (level === "speculative") {
    issues.push({ code: "speculative_hospitality", severity: "P1" });
  }
  if (
    guest === "none" &&
    meeting === "none" &&
    catering === "none" &&
    level !== "speculative"
  ) {
    issues.push({ code: "no_demand_channels", severity: "P0" });
  }

  const specificity = text(candidate?.hotelRelevanceHypothesis?.specificity);
  const market = text(candidate?.hotelMarketRelevance?.assessment);
  if (
    specificity === "generic" ||
    market === "irrelevant" ||
    market === "weak_or_distant"
  ) {
    issues.push({ code: "hotel_relevance_weak", severity: "P1" });
  }
  if (
    specificity === "city_only" &&
    market === "city_relevant_needs_specificity"
  ) {
    issues.push({ code: "city_only_relevance", severity: "P1" });
  }

  const physical = text(candidate.physicalActivityIndicator);
  const growthType = text(candidate.growthSignalType);
  if (
    (growthType === "funding_only" || growthType === "hiring_only") &&
    level !== "verified_physical_travel_or_gathering" &&
    level !== "strong_evidence_supported_mechanism"
  ) {
    issues.push({ code: "funding_or_hiring_without_mechanism", severity: "P0" });
  }
  if (
    growthType === "generic_growth" &&
    level !== "verified_physical_travel_or_gathering" &&
    level !== "strong_evidence_supported_mechanism"
  ) {
    issues.push({ code: "generic_growth_weak_mechanism", severity: "P1" });
  }
  if (physical === "remote_or_virtual_only") {
    issues.push({ code: "remote_only", severity: "P0" });
  }

  const p0 = issues.filter((i) => i.severity === "P0");
  const p1 = issues.filter((i) => i.severity === "P1");

  let gate2 = "MONITOR";
  let disposition = "MONITOR";

  if (p0.some((i) => i.code === "association_boilerplate" || i.code === "remote_only")) {
    gate2 = "REJECT";
    disposition = "REJECT_NO_HOTEL_DEMAND";
  } else if (
    p0.some((i) => i.code === "funding_or_hiring_without_mechanism") ||
    level === "no_meaningful_hospitality_connection"
  ) {
    gate2 = "REJECT";
    disposition = "REJECT_NO_HOTEL_DEMAND";
  } else if (
    gate1 === "PASS" &&
    p0.length === 0 &&
    p1.length === 0 &&
    (level === "verified_physical_travel_or_gathering" ||
      level === "strong_evidence_supported_mechanism") &&
    (guest === "credible" ||
      meeting === "credible" ||
      catering === "credible" ||
      guest === "possible" ||
      meeting === "possible")
  ) {
    gate2 = "ACCEPT_FOR_DEEP_RESEARCH";
    disposition = "ACCEPT_FOR_DEEP_RESEARCH";
  } else if (
    p0.length === 0 &&
    level === "plausible_unverified" &&
    (gate1 === "INVESTIGATE" ||
      guest === "possible" ||
      meeting === "possible" ||
      p1.length > 0)
  ) {
    gate2 = "TARGETED_VERIFICATION_REQUIRED";
    disposition = "TARGETED_VERIFICATION_REQUIRED";
  } else if (
    p0.length === 0 &&
    (level === "speculative" ||
      growthType === "generic_growth" ||
      guest === "unlikely" ||
      text(candidate.discoveryDisposition) === "MONITOR" ||
      text(candidate.gate2HospitalityDemand) === "MONITOR")
  ) {
    gate2 = "MONITOR";
    disposition = "MONITOR";
  } else if (
    p0.length === 0 &&
    (gate1 === "INVESTIGATE" || p1.length > 0)
  ) {
    gate2 = "TARGETED_VERIFICATION_REQUIRED";
    disposition = "TARGETED_VERIFICATION_REQUIRED";
  } else if (p0.length) {
    gate2 = "REJECT";
    disposition = "REJECT_NO_HOTEL_DEMAND";
  } else {
    gate2 = "MONITOR";
    disposition = "MONITOR";
  }

  // Honor agent TARGETED / MONITOR when independent gate would ACCEPT only on weak edge
  const agentDisp = text(candidate.discoveryDisposition);
  const agentGate2 = text(candidate.gate2HospitalityDemand);
  if (
    gate2 === "ACCEPT_FOR_DEEP_RESEARCH" &&
    (agentGate2 === "TARGETED_VERIFICATION_REQUIRED" ||
      agentDisp === "TARGETED_VERIFICATION_REQUIRED")
  ) {
    gate2 = "TARGETED_VERIFICATION_REQUIRED";
    disposition = "TARGETED_VERIFICATION_REQUIRED";
  }
  if (
    agentDisp === "HOLD_DUPLICATE_CHECK" ||
    agentDisp === "HOLD_IDENTITY_UNCLEAR"
  ) {
    disposition = agentDisp;
    gate2 = "TARGETED_VERIFICATION_REQUIRED";
  }
  if (agentDisp === "REJECT_PUBLISHER_MISCLASSIFIED") {
    gate2 = "REJECT";
    disposition = agentDisp;
  }

  return {
    gate2,
    disposition,
    issues,
    allowDeepResearchSpend: gate2 === "ACCEPT_FOR_DEEP_RESEARCH",
    version: EXA_AGENT_HOSPITALITY_DEMAND_GATES_V2,
  };
}

/**
 * Rank score 1–100 (lower = higher priority for research).
 * Funding/fame are secondary penalties, not primary boosts.
 */
export function rankHospitalityDemandCandidate(candidate = {}, assessment = {}) {
  let score = 50;
  const level = evidenceLevel(candidate);
  const levelBoost = {
    verified_physical_travel_or_gathering: -35,
    strong_evidence_supported_mechanism: -25,
    plausible_unverified: -5,
    speculative: 20,
    no_meaningful_hospitality_connection: 40,
  };
  score += levelBoost[level] ?? 10;

  const guest = text(candidate.potentialGuestroomDemand);
  const meeting = text(candidate.potentialMeetingDemand);
  if (guest === "credible") score -= 8;
  if (meeting === "credible") score -= 6;
  if (guest === "none" && meeting === "none") score += 15;

  const market = text(candidate?.hotelMarketRelevance?.assessment);
  if (market === "property_catchment_fit") score -= 12;
  if (market === "city_relevant_needs_specificity") score -= 2;
  if (market === "weak_or_distant" || market === "irrelevant") score += 20;

  const fit = text(candidate?.preliminaryPropertyFit?.fit);
  if (fit === "promising") score -= 5;
  if (fit === "poor") score += 10;

  const timing = text(candidate.likelyDemandTiming);
  if (timing === "current" || timing === "near_term") score -= 6;
  if (timing === "one_time_past") score += 12;

  const growthType = text(candidate.growthSignalType);
  if (growthType === "funding_only" || growthType === "generic_growth") score += 15;
  if (growthType === "hiring_only") score += 10;
  if (
    growthType === "operational_expansion" ||
    growthType === "project_implementation" ||
    growthType === "corporate_meetings_events" ||
    growthType === "workforce_movement"
  ) {
    score -= 8;
  }

  // Funding amount is supporting only — large rounds without mechanism penalize slightly
  const funding = Number(candidate?.businessTrigger?.fundingAmountUsd);
  if (Number.isFinite(funding) && funding > 50_000_000) {
    if (
      level !== "verified_physical_travel_or_gathering" &&
      level !== "strong_evidence_supported_mechanism"
    ) {
      score += 8;
    }
  }

  if (assessment.gate2 === "ACCEPT_FOR_DEEP_RESEARCH") score -= 5;
  if (assessment.gate2 === "MONITOR") score += 10;
  if (assessment.gate2 === "REJECT") score += 40;

  const clamped = Math.max(1, Math.min(100, Math.round(score)));
  return {
    discoveryPriority: clamped,
    researchPriority:
      assessment.gate2 === "ACCEPT_FOR_DEEP_RESEARCH"
        ? clamped <= 25
          ? "P0"
          : "P1"
        : assessment.gate2 === "TARGETED_VERIFICATION_REQUIRED"
          ? "P1"
          : assessment.gate2 === "MONITOR"
            ? "P2"
            : "SKIP",
    rankingVersion: EXA_AGENT_HOSPITALITY_DEMAND_GATES_V2,
  };
}

/**
 * Full independent assessment for one candidate.
 */
export function assessHospitalityDemandCandidate(candidate = {}, opts = {}) {
  const g1 = assessGate1OperationalActivity(candidate, opts);
  const g2 = assessGate2HospitalityDemand(candidate, g1);
  const rank = rankHospitalityDemandCandidate(candidate, g2);
  const deepQs = Array.isArray(candidate.deepResearchQuestions)
    ? candidate.deepResearchQuestions
    : Array.isArray(candidate.researchGaps)
      ? candidate.researchGaps
      : [];

  return {
    companyName: companyName(candidate) || null,
    agentDisposition: text(candidate.discoveryDisposition) || null,
    agentResearchPriority: candidate.researchPriority || null,
    gate1: g1.gate1,
    gate2: g2.gate2,
    eligibility: g2.disposition,
    disposition: g2.disposition,
    allowDeepResearchSpend: g2.allowDeepResearchSpend,
    discoveryPriority: rank.discoveryPriority,
    researchPriority: rank.researchPriority,
    deepResearchQuestions: deepQs,
    issues: [...g1.issues, ...g2.issues],
    version: EXA_AGENT_HOSPITALITY_DEMAND_GATES_V2,
  };
}

export function evaluateHospitalityDemandBatch(structured = {}, opts = {}) {
  const assessments = (structured.candidates || []).map((c, i) =>
    assessHospitalityDemandCandidate(c, {
      ...opts,
      existingDuplicate: opts.duplicateFlags?.[i] === true,
    }),
  );
  const ranked = [...assessments].sort(
    (a, b) => a.discoveryPriority - b.discoveryPriority,
  );
  return {
    assessments,
    ranked,
    acceptedForDeepResearch: assessments.filter((a) => a.allowDeepResearchSpend),
    targetedVerification: assessments.filter(
      (a) => a.gate2 === "TARGETED_VERIFICATION_REQUIRED",
    ),
    monitor: assessments.filter((a) => a.gate2 === "MONITOR"),
    rejected: assessments.filter((a) => a.gate2 === "REJECT"),
    version: EXA_AGENT_HOSPITALITY_DEMAND_GATES_V2,
  };
}
