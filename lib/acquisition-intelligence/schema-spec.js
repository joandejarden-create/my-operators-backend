/**
 * Acquisition Intelligence schema specs for ensure script + tests.
 */

import {
  GTM_ACQUISITION_RELATIONSHIPS_TABLE,
  GTM_ACQUISITION_IMPORT_BATCHES_TABLE,
  MAP_ACQUISITION_RELATIONSHIP as R,
  MAP_ACQUISITION_IMPORT_BATCH as B,
  VAL_RELATIONSHIP_STRENGTH,
  VAL_ACQUISITION_ROLE,
  VAL_PERSON_COMPANY_CLASS,
  VAL_ACQUISITION_RELATIONSHIP_STATUS,
  VAL_RESEARCH_STATUS,
  VAL_POTENTIAL_BAND,
  VAL_CALA_RELEVANCE,
  VAL_CLASSIFICATION_CONFIDENCE,
  VAL_IMPORT_BATCH_STATUS,
  VAL_ACQUISITION_VISIBILITY,
  VAL_INGESTION_METHOD,
  VAL_RESEARCH_QUEUE_ELIGIBILITY,
  VAL_CLASSIFICATION_SOURCE,
  VAL_EXISTING_OWNER_TARGET_MATCH,
  SOURCE_LINKEDIN_CONNECTIONS_EXPORT,
} from "./field-map.js";

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

function emailField(name, description) {
  const field = { name, type: "email" };
  if (description) field.description = description;
  return field;
}

function urlField(name, description) {
  const field = { name, type: "url" };
  if (description) field.description = description;
  return field;
}

/**
 * Non-link relationship fields.
 */
export function buildAcquisitionRelationshipCoreFields() {
  return [
    {
      name: R.relationshipName,
      type: "singleLineText",
      description: "Primary display name for the relationship (usually person name).",
    },
    {
      name: R.sourceUserId,
      type: "singleLineText",
      description: "Dealality user/memberstack id that owns this network relationship.",
    },
    urlField(R.linkedInUrl, "LinkedIn profile URL from Connections export (identity; do not scrape)."),
    dateField(R.connectedOn, "LinkedIn Connected On date when parseable."),
    singleSelect(
      R.relationshipStrength,
      VAL_RELATIONSHIP_STRENGTH,
      "Manual only. LinkedIn connection ≠ strong relationship. Default Unknown."
    ),
    singleSelect(R.acquisitionRole, VAL_ACQUISITION_ROLE, "Primary acquisition classification."),
    singleSelect(
      R.personCompanyClass,
      VAL_PERSON_COMPANY_CLASS,
      "Person/company type — evidence-based; do not infer from title alone."
    ),
    singleSelect(
      R.importSource,
      [SOURCE_LINKEDIN_CONNECTIONS_EXPORT, "MANUAL", "LINKEDIN_API"],
      "Provenance of the relationship record."
    ),
    singleSelect(R.ingestionMethod, VAL_INGESTION_METHOD, "How the data was ingested."),
    {
      name: R.sourceFileName,
      type: "singleLineText",
    },
    dateTimeField(R.importedAt, "When this relationship was first imported for the user."),
    dateTimeField(R.lastLinkedInSyncAt, "Last time LinkedIn-derived fields were refreshed."),
    { name: R.firstName, type: "singleLineText" },
    { name: R.lastName, type: "singleLineText" },
    emailField(R.email, "Optional — LinkedIn exports often omit email."),
    { name: R.company, type: "singleLineText" },
    { name: R.position, type: "singleLineText" },
    singleSelect(R.status, VAL_ACQUISITION_RELATIONSHIP_STATUS),
    singleSelect(R.researchStatus, VAL_RESEARCH_STATUS, "Stage 3+ research queue status."),
    {
      name: R.relationshipDedupeKey,
      type: "singleLineText",
      description: "userId|li:url or userId|nc:name|company — idempotent re-import key.",
    },
    {
      name: R.notes,
      type: "multilineText",
      description: "Manual notes — never overwrite from CSV blanks.",
    },
    singleSelect(R.visibility, VAL_ACQUISITION_VISIBILITY),
    singleSelect(R.directProspectPotential, VAL_POTENTIAL_BAND, "Stage 2 classifier stub."),
    singleSelect(R.connectorPotential, VAL_POTENTIAL_BAND, "Stage 2 classifier stub."),
    singleSelect(R.decisionVisibility, VAL_POTENTIAL_BAND, "Stage 2 classifier stub."),
    singleSelect(R.calaRelevance, VAL_CALA_RELEVANCE, "Stage 2 classifier stub."),
    singleSelect(R.classificationConfidence, VAL_CLASSIFICATION_CONFIDENCE),
    {
      name: R.scoreExplanation,
      type: "multilineText",
      description: "Stage 2+ explanation of classification/scores.",
    },
    singleSelect(
      R.researchQueueEligibility,
      VAL_RESEARCH_QUEUE_ELIGIBILITY,
      "Stage 2 queue eligibility — no research executed yet."
    ),
    singleSelect(
      R.classificationSource,
      VAL_CLASSIFICATION_SOURCE,
      "Automated / Manual / Existing GTM Match. Manual blocks auto overwrite."
    ),
    {
      name: R.classifierVersion,
      type: "singleLineText",
      description: "e.g. acquisition-classify-v1",
    },
    dateTimeField(R.classifiedAt, "When automated/manual classification was last set."),
    singleSelect(
      R.existingOwnerTargetMatch,
      VAL_EXISTING_OWNER_TARGET_MATCH,
      "Whether company matched an existing GTM Owner Target."
    ),
    {
      name: R.existingOwnerTargetName,
      type: "singleLineText",
      description: "Matched Owner Target display name (when match Yes/Uncertain).",
    },
  ];
}

/**
 * @param {{ contactsTableId: string, importBatchesTableId: string, ownerTargetsTableId?: string }} ids
 */
export function buildAcquisitionRelationshipLinkFields(ids) {
  const fields = [
    linkField(R.contact, ids.contactsTableId, "Global GTM Contact identity — do not duplicate people."),
    linkField(R.importBatch, ids.importBatchesTableId, "Import batch that created/last refreshed this row."),
  ];
  if (ids.ownerTargetsTableId) {
    fields.push(
      linkField(
        R.existingOwnerTarget,
        ids.ownerTargetsTableId,
        "Optional link to matched GTM Owner Target."
      )
    );
  }
  return fields;
}

export function buildAcquisitionImportBatchCoreFields() {
  return [
    {
      name: B.batchLabel,
      type: "singleLineText",
      description: "Human-readable import batch label.",
    },
    {
      name: B.sourceUserId,
      type: "singleLineText",
      description: "User who uploaded the Connections CSV.",
    },
    { name: B.sourceFileName, type: "singleLineText" },
    singleSelect(B.importSource, [SOURCE_LINKEDIN_CONNECTIONS_EXPORT, "MANUAL", "LINKEDIN_API"]),
    singleSelect(B.ingestionMethod, VAL_INGESTION_METHOD),
    dateTimeField(B.importedAt),
    singleSelect(B.status, VAL_IMPORT_BATCH_STATUS),
    numberField(B.rowsDetected, 0),
    numberField(B.createdCount, 0),
    numberField(B.updatedCount, 0),
    numberField(B.skippedCount, 0),
    numberField(B.invalidCount, 0),
    numberField(B.duplicateCount, 0),
    numberField(B.withCompanyCount, 0),
    numberField(B.withPositionCount, 0),
    numberField(B.withLinkedInCount, 0),
    numberField(B.withEmailCount, 0),
    dateField(B.earliestConnectedOn),
    dateField(B.latestConnectedOn),
    { name: B.previewReportPath, type: "singleLineText" },
    { name: B.notes, type: "multilineText" },
    singleSelect(B.visibility, VAL_ACQUISITION_VISIBILITY),
  ];
}

/**
 * @param {{ existingType?: string, desiredType: string, name: string }} args
 */
export function classifyFieldEnsureAction(existing, spec) {
  if (!existing) return { action: "create" };
  if (existing.type === spec.type) return { action: "skip", reason: "exists_same_type" };
  // Allow url vs singleLineText soft mismatch for LinkedIn URL if already created as text
  if (
    (existing.type === "singleLineText" || existing.type === "url") &&
    (spec.type === "singleLineText" || spec.type === "url")
  ) {
    return { action: "skip", reason: "compatible_text_or_url" };
  }
  if (
    (existing.type === "singleLineText" || existing.type === "email") &&
    (spec.type === "singleLineText" || spec.type === "email")
  ) {
    return { action: "skip", reason: "compatible_text_or_email" };
  }
  return {
    action: "conflict",
    reason: `type_mismatch:${existing.type}->${spec.type}`,
  };
}

export function getAcquisitionIntelligenceSchemaSummary() {
  return {
    tables: [
      GTM_ACQUISITION_RELATIONSHIPS_TABLE,
      GTM_ACQUISITION_IMPORT_BATCHES_TABLE,
    ],
    linkedExisting: ["Contacts"],
    relationshipCoreFieldCount: buildAcquisitionRelationshipCoreFields().length,
    importBatchCoreFieldCount: buildAcquisitionImportBatchCoreFields().length,
    stage1Scope: "csv_ingestion_plus_stage2_classification",
    doesNotInclude: [
      "deep_research",
      "scoring_numeric_v2",
      "automated_outreach",
      "linkedin_scraping",
      "decision_radar_writes",
    ],
  };
}
