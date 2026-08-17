/**
 * Schema field specs for Decision Opportunities + Evidence (shared by ensure script + tests).
 * Link fields require live table IDs from the GTM base meta API.
 */

import {
  GTM_DECISION_OPPORTUNITIES_TABLE,
  GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE,
  MAP_DECISION_OPPORTUNITY,
  MAP_DECISION_OPPORTUNITY_EVIDENCE,
  VAL_DECISION_PROJECT_TYPE,
  VAL_DECISION_LIKELY_TYPE,
  VAL_DECISION_STAGE,
  VAL_DECISION_WINDOW,
  VAL_DECISION_BRAND_STATUS,
  VAL_DECISION_OPERATOR_STATUS,
  VAL_DECISION_EXCLUSIVITY_STATUS,
  VAL_DECISION_STILL_OPEN,
  VAL_DECISION_OPEN_CONFIDENCE,
  VAL_DECISION_TRIGGER,
  VAL_DECISION_WARM_PATH_TYPE,
  VAL_DECISION_STATUS,
  VAL_DECISION_RECOMMENDED_ACTION,
  VAL_DECISION_SCORE_BAND,
  VAL_DECISION_VISIBILITY,
  VAL_DECISION_DATA_SOURCE,
  VAL_EVIDENCE_CONFIDENCE,
  VAL_EVIDENCE_DIRECTION,
  VAL_EVIDENCE_SUPPORTS_FIELD,
  VAL_EVIDENCE_SOURCE_TYPE,
} from "./decision-opportunity-field-map.js";

function choices(names) {
  return { choices: names.map((name) => ({ name })) };
}

function singleSelect(name, optionNames, description) {
  const field = { name, type: "singleSelect", options: choices(optionNames) };
  if (description) field.description = description;
  return field;
}

function dateField(name, description) {
  const field = {
    name,
    type: "date",
    options: { dateFormat: { name: "iso" } },
  };
  if (description) field.description = description;
  return field;
}

function dateTimeField(name, description) {
  const field = {
    name,
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
  };
  if (description) field.description = description;
  return field;
}

function numberField(name, precision = 0, description) {
  const field = { name, type: "number", options: { precision } };
  if (description) field.description = description;
  return field;
}

function linkField(name, linkedTableId, description) {
  const field = {
    name,
    type: "multipleRecordLinks",
    options: { linkedTableId },
  };
  if (description) field.description = description;
  return field;
}

function checkboxField(name, description) {
  const field = {
    name,
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
  };
  if (description) field.description = description;
  return field;
}

/**
 * Non-link Opportunity fields (safe to create before/without dependent tables).
 */
export function buildDecisionOpportunityCoreFields() {
  const M = MAP_DECISION_OPPORTUNITY;
  return [
    {
      name: M.opportunityName,
      type: "singleLineText",
      description: "Primary label for founder lists. One project + one decision window.",
    },
    {
      name: M.opportunityId,
      type: "singleLineText",
      description: "Stable opportunity id (e.g. MX-PDC-OLEUM-2026-001).",
    },
    {
      name: M.projectHotelName,
      type: "singleLineText",
      description: "Hotel or project name. Required for Qualified / Founder Review.",
    },
    { name: M.country, type: "singleLineText" },
    { name: M.cityMarket, type: "singleLineText", description: "City and/or commercial market." },
    singleSelect(M.projectType, VAL_DECISION_PROJECT_TYPE),
    singleSelect(M.likelyDecisionType, VAL_DECISION_LIKELY_TYPE),
    singleSelect(
      M.decisionStage,
      VAL_DECISION_STAGE,
      "Richer than branding-decision pre/post_decision timing."
    ),
    singleSelect(M.decisionWindow, VAL_DECISION_WINDOW),
    singleSelect(
      M.decisionStillOpen,
      VAL_DECISION_STILL_OPEN,
      "Yes/No/Uncertain — not inferred solely from missing brand in public sources."
    ),
    singleSelect(
      M.decisionOpenConfidence,
      VAL_DECISION_OPEN_CONFIDENCE,
      "Confirmed|Probable|Inferred|Unknown. Separate from Opportunity Score."
    ),
    singleSelect(
      M.brandStatus,
      VAL_DECISION_BRAND_STATUS,
      "Not Publicly Identified ≠ not selected. Do not equate absence with open decision."
    ),
    singleSelect(
      M.operatorStatus,
      VAL_DECISION_OPERATOR_STATUS,
      "Same semantic rule as Brand Status."
    ),
    singleSelect(M.exclusivityStatus, VAL_DECISION_EXCLUSIVITY_STATUS),
    singleSelect(
      M.trigger,
      VAL_DECISION_TRIGGER,
      "Maps from GTM Deal Trigger where applicable; see docs/gtm-decision-radar.md."
    ),
    {
      name: M.whyNow,
      type: "multilineText",
      description: "Why this decision window matters now (founder-facing).",
    },
    {
      name: M.whyDealality,
      type: "multilineText",
      description: "Why Dealality's owner-first process fits this opportunity.",
    },
    singleSelect(
      M.warmPathType,
      VAL_DECISION_WARM_PATH_TYPE,
      "Manual only — never auto-infer relationships."
    ),
    {
      name: M.warmPathContactSource,
      type: "singleLineText",
      description: "Evidence-backed or manually entered warm-path person/source.",
    },
    { name: M.warmPathNotes, type: "multilineText" },
    singleSelect(M.status, VAL_DECISION_STATUS, "Lifecycle status. No auto Outreach Ready."),
    singleSelect(M.recommendedAction, VAL_DECISION_RECOMMENDED_ACTION),
    { name: M.founderNotes, type: "multilineText" },
    checkboxField(M.founderReviewed, "Set when Joan has reviewed this opportunity."),
    dateTimeField(M.lastReviewedAt),
    numberField(M.opportunityScore, 0, "Stage 1 contract only — do not populate speculative scores."),
    singleSelect(M.scoreBand, VAL_DECISION_SCORE_BAND, "Score engine deferred past Stage 1."),
    { name: M.scoreExplanation, type: "multilineText" },
    dateTimeField(M.scoredAt),
    {
      name: M.canonicalOpportunityKey,
      type: "singleLineText",
      description: "Future dedupe key (normalized project + decision window).",
    },
    {
      name: M.externalSourceProjectName,
      type: "singleLineText",
      description: "Project name as seen in an external source (for later matching).",
    },
    checkboxField(M.possibleDuplicate, "Flag for human dedupe review."),
    singleSelect(
      M.visibility,
      VAL_DECISION_VISIBILITY,
      "Always internal_only. Never expose via product APIs."
    ),
    singleSelect(M.dataSource, VAL_DECISION_DATA_SOURCE),
    { name: M.internalNotes, type: "multilineText" },
  ];
}

/**
 * Link fields for Decision Opportunities (need live table IDs).
 * @param {{
 *   ownerTargetsTableId?: string,
 *   propertiesTableId?: string,
 *   contactsTableId?: string,
 *   opportunitiesTableId?: string,
 * }} ids
 */
export function buildDecisionOpportunityLinkFields(ids = {}) {
  const M = MAP_DECISION_OPPORTUNITY;
  const fields = [];
  if (ids.ownerTargetsTableId) {
    fields.push(
      linkField(
        M.ownerTarget,
        ids.ownerTargetsTableId,
        "Linked GTM Owner Target (owner/developer). Optional for early discovery."
      )
    );
  }
  if (ids.propertiesTableId) {
    fields.push(
      linkField(
        M.leadProperty,
        ids.propertiesTableId,
        "Optional CoStar/GTM property. Greenfield may have no Lead Property."
      )
    );
  }
  if (ids.contactsTableId) {
    fields.push(
      linkField(
        M.decisionMakers,
        ids.contactsTableId,
        "Reuse GTM Contacts — do not duplicate contact records."
      )
    );
  }
  if (ids.opportunitiesTableId) {
    fields.push(
      linkField(
        M.duplicateOf,
        ids.opportunitiesTableId,
        "Self-link when this row is a duplicate of a canonical opportunity."
      )
    );
  }
  return fields;
}

/**
 * Evidence fields including Decision Opportunity link when opportunitiesTableId provided.
 * @param {{ opportunitiesTableId?: string }} ids
 */
export function buildDecisionOpportunityEvidenceFields(ids = {}) {
  const E = MAP_DECISION_OPPORTUNITY_EVIDENCE;
  const fields = [
    {
      name: E.evidenceId,
      type: "singleLineText",
      description: "Stable evidence id within the opportunity.",
    },
    {
      name: E.sourceName,
      type: "singleLineText",
      description: "Publisher or document title.",
    },
    { name: E.sourceUrl, type: "url" },
    singleSelect(E.sourceType, VAL_EVIDENCE_SOURCE_TYPE),
    dateField(E.publicationDate),
    dateField(E.retrievedDate, "When Dealality retrieved/captured this source."),
    {
      name: E.evidenceExcerpt,
      type: "multilineText",
      description: "Short excerpt where permitted.",
    },
    singleSelect(E.supportsField, VAL_EVIDENCE_SUPPORTS_FIELD),
    singleSelect(
      E.evidenceConfidence,
      VAL_EVIDENCE_CONFIDENCE,
      "Aligned with Partner Source Library Source Quality (High/Medium/Low)."
    ),
    singleSelect(
      E.evidenceDirection,
      VAL_EVIDENCE_DIRECTION,
      "Supports Closed / Too Late is first-class — not only open hypotheses."
    ),
    { name: E.notes, type: "multilineText" },
  ];
  if (ids.opportunitiesTableId) {
    fields.splice(
      1,
      0,
      linkField(
        E.decisionOpportunity,
        ids.opportunitiesTableId,
        "Parent Decision Opportunity."
      )
    );
  }
  return fields;
}

export function getDecisionRadarSchemaSummary() {
  return {
    opportunitiesTable: GTM_DECISION_OPPORTUNITIES_TABLE,
    evidenceTable: GTM_DECISION_OPPORTUNITY_EVIDENCE_TABLE,
    opportunityCoreFieldCount: buildDecisionOpportunityCoreFields().length,
    evidenceCoreFieldCount: buildDecisionOpportunityEvidenceFields({}).length,
    expectedLinkFields: [
      MAP_DECISION_OPPORTUNITY.ownerTarget,
      MAP_DECISION_OPPORTUNITY.leadProperty,
      MAP_DECISION_OPPORTUNITY.decisionMakers,
      MAP_DECISION_OPPORTUNITY.duplicateOf,
      MAP_DECISION_OPPORTUNITY_EVIDENCE.decisionOpportunity,
    ],
    expectedInverseEvidenceField: MAP_DECISION_OPPORTUNITY.evidence,
  };
}

/**
 * Compare desired singleSelect choices vs existing Airtable field.
 * @param {object} existingField
 * @param {string[]} desiredChoices
 */
export function diffSelectChoices(existingField, desiredChoices) {
  const existing = (existingField?.options?.choices || []).map((c) => c.name);
  const existingSet = new Set(existing);
  const missing = desiredChoices.filter((n) => !existingSet.has(n));
  return { existing, missing, ok: missing.length === 0 };
}

/**
 * Type compatibility for ensure — fail on incompatible existing fields.
 * @param {object} existingField
 * @param {object} desiredSpec
 */
export function classifyFieldEnsureAction(existingField, desiredSpec) {
  if (!existingField) {
    return { action: "create", reason: "missing" };
  }
  if (existingField.type !== desiredSpec.type) {
    return {
      action: "conflict",
      reason: `existing type ${existingField.type} != desired ${desiredSpec.type}`,
    };
  }
  if (desiredSpec.type === "singleSelect") {
    const desiredChoices = (desiredSpec.options?.choices || []).map((c) => c.name);
    const diff = diffSelectChoices(existingField, desiredChoices);
    if (!diff.ok) {
      return {
        action: "add_choices",
        reason: `missing choices: ${diff.missing.join(", ")}`,
        missingChoices: diff.missing,
        allChoices: [...new Set([...diff.existing, ...desiredChoices])],
      };
    }
  }
  if (desiredSpec.type === "multipleRecordLinks") {
    const existingLinked = existingField.options?.linkedTableId;
    const desiredLinked = desiredSpec.options?.linkedTableId;
    if (existingLinked && desiredLinked && existingLinked !== desiredLinked) {
      return {
        action: "conflict",
        reason: `linkedTableId ${existingLinked} != ${desiredLinked}`,
      };
    }
  }
  return { action: "skip", reason: "exists_compatible" };
}
