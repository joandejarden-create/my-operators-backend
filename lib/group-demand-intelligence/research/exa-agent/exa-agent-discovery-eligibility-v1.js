/**
 * Independent discovery eligibility gate for Stage C new-company candidates.
 * Exa "High priority" labels are not commercial eligibility.
 */
import { validateDiscoveryStructured } from "./exa-agent-evidence-validation-v1.js";
import { hasAssociationProcurementBoilerplate } from "../../scaling-drawer-presentation-contract-v1.js";

export const EXA_AGENT_DISCOVERY_ELIGIBILITY_V1 =
  "gdi-scaling-exa-agent-discovery-eligibility-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Assess one discovery candidate for deep-research spend eligibility.
 */
export function assessDiscoveryCandidateEligibility(candidate = {}, opts = {}) {
  const issues = [];
  const name = text(candidate?.companyIdentity?.legalOrTradingName);
  if (!name) {
    issues.push({ severity: "P0", code: "identity_missing", message: "Company identity missing" });
  }
  if (/mynewsdesk|pr newswire|business wire/i.test(name)) {
    issues.push({
      severity: "P0",
      code: "publisher_as_company",
      message: `Publisher misclassified: ${name}`,
    });
  }

  const trigger = text(candidate?.businessTrigger?.summary);
  if (!trigger) {
    issues.push({
      severity: "P0",
      code: "activity_signal_missing",
      message: "No source-supported commercial activity summary",
    });
  }

  const geo = text(candidate?.activityGeography?.primaryLocation);
  if (!geo) {
    issues.push({
      severity: "P1",
      code: "geography_missing",
      message: "Activity geography not established",
    });
  }

  const chronoDate =
    text(candidate?.activityChronology?.activityDate) ||
    text(candidate?.activityChronology?.sourcePublicationDate);
  if (!chronoDate && candidate?.activityChronology?.isHistorical !== false) {
    issues.push({
      severity: "P1",
      code: "timeframe_weak",
      message: "No activity/publication date established",
    });
  }

  const demandStatus = text(candidate?.hotelDemandHypothesis?.evidenceStatus);
  const demandMech = text(candidate?.hotelDemandHypothesis?.mechanism);
  if (!demandMech || demandStatus === "unsupported") {
    issues.push({
      severity: "P0",
      code: "hotel_demand_not_credible",
      message: "Hotel-demand mechanism unsupported or missing",
    });
  }
  if (hasAssociationProcurementBoilerplate(demandMech)) {
    issues.push({
      severity: "P0",
      code: "association_boilerplate",
      message: "Association procurement language in Scaling demand hypothesis",
    });
  }

  const specificity = text(candidate?.hotelRelevanceHypothesis?.specificity);
  if (specificity === "generic" || !text(candidate?.hotelRelevanceHypothesis?.whyThisHotel)) {
    issues.push({
      severity: "P1",
      code: "hotel_relevance_weak",
      message: "Specific hotel relevance not established beyond generic city co-location",
    });
  }

  if (opts.existingDuplicate === true) {
    issues.push({
      severity: "P0",
      code: "duplicate_opportunity",
      message: "Candidate matches an existing opportunity",
    });
  }

  const segment = text(opts.scalingSegment || "scaling_companies");
  if (segment && !/scaling/i.test(segment)) {
    issues.push({
      severity: "P0",
      code: "wrong_segment",
      message: `Incorrect segment classification: ${segment}`,
    });
  }

  // Exa researchPriority is informational only — never a pass criterion
  if (
    candidate.researchPriority === "P0" ||
    /high/i.test(text(candidate.researchPriorityLabel))
  ) {
    // no-op acknowledgement — do not treat as eligibility
  }

  const claims = Array.isArray(candidate.evidenceClaims) ? candidate.evidenceClaims : [];
  const claimWithUrl = claims.some((c) => text(c.url || c.sourceUrl));
  if (!claimWithUrl && !text(opts.allowMissingClaimUrls)) {
    issues.push({
      severity: "P1",
      code: "evidence_claims_without_urls",
      message: "No evidence claim URLs attached to candidate",
    });
  }

  const disposition = text(candidate.discoveryDisposition);
  const p0 = issues.filter((i) => i.severity === "P0");
  const p1 = issues.filter((i) => i.severity === "P1");

  let eligibility = "REJECT";
  if (p0.length === 0 && p1.length === 0 && disposition === "ACCEPT_FOR_DEEP_RESEARCH") {
    eligibility = "ACCEPT_FOR_DEEP_RESEARCH";
  } else if (p0.length === 0 && p1.length <= 2) {
    eligibility = "HOLD_FOR_HUMAN_REVIEW";
  } else if (p0.length === 0) {
    eligibility = "HOLD_FOR_HUMAN_REVIEW";
  }

  // Independent override: agent ACCEPT without demand still held
  if (
    disposition === "ACCEPT_FOR_DEEP_RESEARCH" &&
    issues.some((i) => i.code === "hotel_demand_not_credible")
  ) {
    eligibility = "REJECT";
  }

  return {
    companyName: name || null,
    agentDisposition: disposition || null,
    agentResearchPriority: candidate.researchPriority || null,
    eligibility,
    allowDeepResearchSpend: eligibility === "ACCEPT_FOR_DEEP_RESEARCH",
    issues,
    version: EXA_AGENT_DISCOVERY_ELIGIBILITY_V1,
  };
}

/**
 * Validate full discovery structured output then score each candidate.
 */
export function evaluateDiscoveryEligibilityBatch(structured = {}, opts = {}) {
  const validation = validateDiscoveryStructured(structured);
  const assessments = (structured.candidates || []).map((c, i) =>
    assessDiscoveryCandidateEligibility(c, {
      ...opts,
      existingDuplicate: opts.duplicateFlags?.[i] === true,
    }),
  );
  return {
    validationOk: validation.ok,
    validationIssues: validation.issues,
    assessments,
    acceptedForDeepResearch: assessments.filter((a) => a.allowDeepResearchSpend),
    held: assessments.filter((a) => a.eligibility === "HOLD_FOR_HUMAN_REVIEW"),
    rejected: assessments.filter((a) => a.eligibility === "REJECT"),
    version: EXA_AGENT_DISCOVERY_ELIGIBILITY_V1,
  };
}
