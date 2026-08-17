/**
 * Deterministic Stage 2 classifier for one Acquisition Network Relationship.
 *
 * Pure function — no Airtable I/O. Does not infer Relationship Strength.
 */

import {
  CLASSIFIER_VERSION,
  TITLE_DIRECT_PROSPECT_RULES,
  TITLE_CONNECTOR_RULES,
  TITLE_DECISION_VISIBILITY_RULES,
  AMBIGUOUS_TITLE_PATTERNS,
  LOW_RELEVANCE_TITLE_PATTERNS,
  CALA_GEO_RULES,
  classifyCompanyTypeHints,
  matchOwnerTarget,
  BAND_RANK,
} from "./classification-config.js";
import {
  MAP_ACQUISITION_RELATIONSHIP as R,
  VAL_ACQUISITION_ROLE,
  VAL_PERSON_COMPANY_CLASS,
  VAL_POTENTIAL_BAND,
  VAL_CALA_RELEVANCE,
  VAL_CLASSIFICATION_CONFIDENCE,
  VAL_RESEARCH_QUEUE_ELIGIBILITY,
  VAL_CLASSIFICATION_SOURCE,
  VAL_EXISTING_OWNER_TARGET_MATCH,
  ACQUISITION_CLASSIFIER_VERSION,
} from "./field-map.js";

function scoreRules(text, rules) {
  let score = 0;
  const signals = [];
  const ids = [];
  for (const rule of rules) {
    if (rule.pattern.test(text)) {
      score += rule.weight;
      signals.push(...rule.signals);
      ids.push(rule.id);
    }
  }
  return { score, signals: [...new Set(signals)], ids };
}

function bandFromScore(score, { high = 3, medium = 2 } = {}) {
  if (score >= high) return "High";
  if (score >= medium) return "Medium";
  if (score >= 1) return "Low";
  return "Unknown";
}

function confidenceFromPoints(points) {
  if (points >= 5) return "High";
  if (points >= 3) return "Medium";
  return "Low";
}

function detectCalaRelevance(blob) {
  for (const rule of CALA_GEO_RULES) {
    if (rule.pattern.test(blob)) return rule.value;
  }
  return "Unknown";
}

function primaryPersonClass(companyHints, titleSignals) {
  if (companyHints.classes.includes("Brand")) return "Brand";
  if (companyHints.classes.includes("Operator / Management Company") && !companyHints.isOwnerDeveloperLikely) {
    return "Operator / Management Company";
  }
  if (titleSignals.includes("attorney")) return "Attorney";
  if (titleSignals.includes("broker") || companyHints.classes.includes("Broker")) return "Broker";
  if (titleSignals.includes("lender_capital") || companyHints.classes.includes("Lender")) {
    return companyHints.classes.includes("Lender") ? "Lender" : "Capital Provider";
  }
  if (titleSignals.includes("feasibility_advisory") || companyHints.classes.includes("Feasibility / Advisory")) {
    return "Feasibility / Advisory";
  }
  if (titleSignals.includes("architect_pm")) return "Architect";
  if (companyHints.classes.includes("Family Office")) return "Family Office";
  if (companyHints.classes.includes("Developer")) return "Developer";
  if (companyHints.classes.includes("Hotel Owner")) return "Hotel Owner";
  if (companyHints.classes.includes("Real Estate Investor")) return "Real Estate Investor";
  if (titleSignals.includes("asset_mgmt_title") || titleSignals.includes("asset_manager")) {
    return "Asset Manager";
  }
  if (titleSignals.includes("investor_title")) return "Real Estate Investor";
  if (!companyHints.classes.length && !titleSignals.length) return "Unclassified";
  if (companyHints.classes[0]) return companyHints.classes[0];
  return "Other";
}

function pickAcquisitionRole({
  direct,
  connector,
  decision,
  isBrand,
  isOperator,
  lowRelevance,
}) {
  if (lowRelevance && direct === "Low" && connector === "Low") return "Low Relevance";
  if (direct === "High") return "Direct Prospect";
  if (isBrand || isOperator) {
    if (connector === "High" || decision === "High") return "Decision-Signal Source";
    if (connector === "Medium") return "Owner Connector";
  }
  if (connector === "High") return "Owner Connector";
  if (decision === "High" && connector === "Medium") return "Decision-Signal Source";
  if (direct === "Medium" && connector === "Medium") return "Strategic Relationship";
  if (direct === "Medium") return "Direct Prospect";
  if (connector === "Medium") return "Owner Connector";
  if (decision === "Medium") return "Strategic Relationship";
  if (direct === "Unknown" && connector === "Unknown" && decision === "Unknown") {
    return "Unclassified";
  }
  if (direct === "Low" && connector === "Low" && decision === "Low") return "Low Relevance";
  return "Unclassified";
}

function researchQueueEligibility({ direct, connector, decision, cala, confidence, role }) {
  if (role === "Low Relevance") return "No Research Yet";
  const confOk = confidence === "High" || confidence === "Medium";
  const calaPriority = [
    "Mexico",
    "Dominican Republic",
    "Costa Rica",
    "Colombia",
    "Guatemala",
    "Wider CALA",
  ].includes(cala);

  if (
    confOk &&
    (direct === "High" ||
      connector === "High" ||
      (decision === "High" && calaPriority))
  ) {
    return "Research Priority";
  }

  if (
    direct === "Medium" ||
    connector === "Medium" ||
    decision === "Medium" ||
    (direct === "High" && !confOk) ||
    (connector === "High" && !confOk) ||
    role === "Strategic Relationship" ||
    role === "Decision-Signal Source"
  ) {
    return "Research Candidate";
  }

  return "No Research Yet";
}

/**
 * @param {{
 *   position?: string,
 *   company?: string,
 *   firstName?: string,
 *   lastName?: string,
 *   existingFields?: Record<string, unknown>,
 *   ownerIndexByKey?: Map<string, { id: string, ownerName: string }>,
 * }} input
 */
export function classifyAcquisitionRelationship(input = {}) {
  const position = String(input.position || "").trim();
  const company = String(input.company || "").trim();
  const blob = `${position} ${company}`;
  const existing = input.existingFields || {};

  const sourceExisting = String(existing[R.classificationSource] || "").trim();
  if (sourceExisting === "Manual") {
    return {
      skipped: true,
      reason: "manual_override",
      classificationSource: "Manual",
      fields: null,
      result: null,
    };
  }

  const companyHints = classifyCompanyTypeHints(company);
  const ownerMatch = matchOwnerTarget(company, input.ownerIndexByKey);

  let lowRelevance = LOW_RELEVANCE_TITLE_PATTERNS.some((p) => p.test(position));
  const ambiguousTitle =
    AMBIGUOUS_TITLE_PATTERNS.some((p) => p.test(position)) &&
    !companyHints.isOwnerDeveloperLikely &&
    !companyHints.isBrand &&
    !companyHints.isOperator &&
    !companyHints.isBrokerAdvisory;

  const directHit = scoreRules(blob, TITLE_DIRECT_PROSPECT_RULES);
  const connectorHit = scoreRules(blob, TITLE_CONNECTOR_RULES);
  const decisionHit = scoreRules(blob, TITLE_DECISION_VISIBILITY_RULES);

  // Direct prospect: require owner/developer company context for weak founder/partner signals
  let directScore = directHit.score;
  if (
    directHit.signals.includes("founder_or_principal") &&
    !companyHints.isOwnerDeveloperLikely &&
    ownerMatch.match !== "Yes"
  ) {
    directScore = Math.max(0, directScore - 1);
  }
  if (companyHints.isBrand || companyHints.isOperator) {
    // Brand/operator execs are not direct owner prospects
    directScore = Math.min(directScore, 1);
  }
  if (companyHints.isOwnerDeveloperLikely || ownerMatch.match === "Yes") {
    directScore += 1;
  }
  if (!company && directScore > 0) {
    directScore = Math.min(directScore, 1);
  }
  if (ambiguousTitle) {
    directScore = Math.min(directScore, 1);
  }
  if (lowRelevance) directScore = 0;

  let connectorScore = connectorHit.score;
  if (companyHints.isBrand || companyHints.isOperator) {
    // Development roles at brands/operators are strong connectors / decision-signal
    if (connectorHit.signals.includes("brand_or_operator_dev") || decisionHit.score > 0) {
      connectorScore = Math.max(connectorScore, 3);
    } else {
      connectorScore = Math.max(connectorScore, 2);
    }
  }
  if (companyHints.isBrokerAdvisory || companyHints.isLenderCapital) {
    connectorScore = Math.max(connectorScore, 3);
  }
  if (ownerMatch.match === "Yes" && directScore >= 2) {
    connectorScore = Math.max(connectorScore, 1);
  }
  if (lowRelevance) connectorScore = Math.min(connectorScore, 1);

  let decisionScore = decisionHit.score;
  if (connectorScore >= 3 || directScore >= 3) decisionScore = Math.max(decisionScore, 2);
  if (companyHints.isBrand && connectorHit.signals.includes("brand_or_operator_dev")) {
    decisionScore = Math.max(decisionScore, 3);
  }
  if (lowRelevance) decisionScore = 0;

  let direct = bandFromScore(directScore, { high: 3, medium: 2 });
  let connector = bandFromScore(connectorScore, { high: 3, medium: 2 });
  let decision = bandFromScore(decisionScore, { high: 3, medium: 2 });

  if (lowRelevance) {
    direct = "Low";
    connector = connector === "High" ? "Low" : connector === "Medium" ? "Low" : "Low";
    decision = "Low";
  }

  // Missing title/company → Unknown, not Low
  if (!position && !company) {
    direct = "Unknown";
    connector = "Unknown";
    decision = "Unknown";
    lowRelevance = false;
  } else if (!position && company) {
    if (!companyHints.isOwnerDeveloperLikely && !companyHints.isBrand && !companyHints.isBrokerAdvisory) {
      direct = direct === "High" ? "Medium" : direct;
    }
  }

  const cala = detectCalaRelevance(blob);

  const titleSignals = [
    ...directHit.signals,
    ...connectorHit.signals,
    ...decisionHit.signals,
  ];
  let personClass = primaryPersonClass(companyHints, titleSignals);
  // Title asset-management wins over weak company "Hotel Owner" inference
  if (
    (titleSignals.includes("asset_mgmt_title") || titleSignals.includes("asset_manager")) &&
    personClass === "Hotel Owner"
  ) {
    personClass = "Asset Manager";
  }
  if (!VAL_PERSON_COMPANY_CLASS.includes(personClass)) personClass = "Other";
  if (personClass === "Hotel Owner" && !companyHints.isOwnerDeveloperLikely && ownerMatch.match !== "Yes") {
    // Do not infer Hotel Owner from title alone
    if (!/\b(owner|asset\s+owner)\b/i.test(position)) {
      personClass = companyHints.classes.includes("Developer")
        ? "Developer"
        : companyHints.classes[0] || "Other";
    }
  }

  let confidencePoints =
    companyHints.confidenceBoost +
    (position ? 1 : 0) +
    (company ? 1 : 0) +
    (directHit.ids.length || connectorHit.ids.length ? 1 : 0) +
    (ownerMatch.match === "Yes" ? 2 : 0) -
    (ambiguousTitle ? 2 : 0) -
    (!company ? 1 : 0);
  if (lowRelevance) confidencePoints = Math.max(confidencePoints, 3);

  const confidence = confidenceFromPoints(confidencePoints);

  const role = pickAcquisitionRole({
    direct,
    connector,
    decision,
    isBrand: companyHints.isBrand,
    isOperator: companyHints.isOperator,
    lowRelevance,
  });

  const researchQueue = researchQueueEligibility({
    direct,
    connector,
    decision,
    cala,
    confidence,
    role,
  });

  /** @type {string[]} */
  const reasons = [];
  if (position) reasons.push(`Title: ${position}.`);
  if (company) reasons.push(`Company: ${company}.`);
  if (companyHints.signals.length) {
    reasons.push(`Company signals: ${companyHints.signals.slice(0, 3).join(", ")}.`);
  }
  if (ownerMatch.match === "Yes") {
    reasons.push(`Matches existing Owner Target: ${ownerMatch.ownerName}.`);
  } else if (ownerMatch.match === "Uncertain") {
    reasons.push(`Possible Owner Target match (uncertain): ${ownerMatch.ownerName}.`);
  }
  if (ambiguousTitle) {
    reasons.push("Ambiguous title without clear owner/developer company context.");
  }
  if (lowRelevance) {
    reasons.push("Title pattern indicates low deal-flow relevance.");
  }
  if (companyHints.isBrand) {
    reasons.push("Brand company — not a direct owner prospect; connector/decision-signal path.");
  }
  if (!position && !company) {
    reasons.push("Missing title and company — insufficient evidence.");
  } else if (!reasons.length) {
    reasons.push("Generic hospitality title; insufficient evidence of owner/development exposure.");
  }

  const classificationSource =
    ownerMatch.match === "Yes" && (direct === "High" || direct === "Medium")
      ? "Existing GTM Match"
      : "Automated";

  const result = {
    acquisitionRole: role,
    personCompanyClass: personClass,
    personCompanyClasses: companyHints.classes,
    directProspectPotential: direct,
    connectorPotential: connector,
    decisionVisibility: decision,
    calaRelevance: cala,
    classificationConfidence: confidence,
    scoreExplanation: reasons.join(" "),
    researchQueueEligibility: researchQueue,
    classificationSource,
    classifierVersion: ACQUISITION_CLASSIFIER_VERSION || CLASSIFIER_VERSION,
    existingOwnerTargetMatch: ownerMatch.match,
    existingOwnerTargetName: ownerMatch.ownerName || "",
    existingOwnerTargetId: ownerMatch.ownerTargetId || null,
    signals: {
      direct: directHit.ids,
      connector: connectorHit.ids,
      decision: decisionHit.ids,
      company: companyHints.signals,
    },
    scores: { directScore, connectorScore, decisionScore },
  };

  // Validate enums
  if (!VAL_ACQUISITION_ROLE.includes(result.acquisitionRole)) result.acquisitionRole = "Unclassified";
  if (!VAL_POTENTIAL_BAND.includes(result.directProspectPotential)) result.directProspectPotential = "Unknown";
  if (!VAL_POTENTIAL_BAND.includes(result.connectorPotential)) result.connectorPotential = "Unknown";
  if (!VAL_POTENTIAL_BAND.includes(result.decisionVisibility)) result.decisionVisibility = "Unknown";
  if (!VAL_CALA_RELEVANCE.includes(result.calaRelevance)) result.calaRelevance = "Unknown";
  if (!VAL_CLASSIFICATION_CONFIDENCE.includes(result.classificationConfidence)) {
    result.classificationConfidence = "Low";
  }
  if (!VAL_RESEARCH_QUEUE_ELIGIBILITY.includes(result.researchQueueEligibility)) {
    result.researchQueueEligibility = "No Research Yet";
  }
  if (!VAL_CLASSIFICATION_SOURCE.includes(result.classificationSource)) {
    result.classificationSource = "Automated";
  }
  if (!VAL_EXISTING_OWNER_TARGET_MATCH.includes(result.existingOwnerTargetMatch)) {
    result.existingOwnerTargetMatch = "No";
  }

  /** @type {Record<string, unknown>} */
  const fields = {
    [R.acquisitionRole]: result.acquisitionRole,
    [R.personCompanyClass]: result.personCompanyClass,
    [R.directProspectPotential]: result.directProspectPotential,
    [R.connectorPotential]: result.connectorPotential,
    [R.decisionVisibility]: result.decisionVisibility,
    [R.calaRelevance]: result.calaRelevance,
    [R.classificationConfidence]: result.classificationConfidence,
    [R.scoreExplanation]: result.scoreExplanation,
    [R.researchQueueEligibility]: result.researchQueueEligibility,
    [R.classificationSource]: result.classificationSource,
    [R.classifierVersion]: result.classifierVersion,
    [R.classifiedAt]: new Date().toISOString(),
    [R.existingOwnerTargetMatch]: result.existingOwnerTargetMatch,
  };
  if (result.existingOwnerTargetName) {
    fields[R.existingOwnerTargetName] = result.existingOwnerTargetName;
  }
  if (result.existingOwnerTargetId) {
    fields[R.existingOwnerTarget] = [result.existingOwnerTargetId];
  }

  return {
    skipped: false,
    reason: null,
    classificationSource: result.classificationSource,
    fields,
    result,
  };
}

/**
 * Compare two classification field patches for idempotency (ignore classifiedAt).
 */
export function classificationFieldsEqual(a, b) {
  const keys = [
    R.acquisitionRole,
    R.personCompanyClass,
    R.directProspectPotential,
    R.connectorPotential,
    R.decisionVisibility,
    R.calaRelevance,
    R.classificationConfidence,
    R.scoreExplanation,
    R.researchQueueEligibility,
    R.classificationSource,
    R.classifierVersion,
    R.existingOwnerTargetMatch,
    R.existingOwnerTargetName,
  ];
  for (const k of keys) {
    if (String(a?.[k] ?? "") !== String(b?.[k] ?? "")) return false;
  }
  return true;
}

export function bandRank(band) {
  return BAND_RANK[band] || 0;
}
