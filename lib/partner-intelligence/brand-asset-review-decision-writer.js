/**
 * Brand Asset Review Decision Writer v5 / v5.1.
 *
 * Applies human review decisions (approve / reject / keep-candidate) to
 * explicitly selected Brand Asset Registry records after human usage review.
 * v5.1 adds approval-state consistency audit and gated correction for records
 * where Asset Status says approved but Explorer Use Permission does not match.
 * Approval is never automatic — only record IDs passed via --approve-records
 * are touched. Does not download images, attach files, write Brand Setup media
 * fields, or promote assets into Brand Explorer.
 *
 * @see docs/data-intelligence/brand-asset-review-decision-writer-v5.md
 */
import {
  BRAND_ASSET_PILOT_CONFIG,
  MAP_BRAND_ASSET,
  BRAND_ASSET_REGISTRY_TABLE,
  normalizeRegistryAssetRecord,
} from "./brand-asset-registry-workflow.js";
import {
  VISUAL_SLOT,
  VISUAL_SLOT_DEFINITIONS,
  MAP_VISUAL_SLOT,
  mapRecordToVisualSlot,
  listRegistryRecordsRaw,
} from "./brand-explorer-visual-slot-requirements.js";

export { BRAND_ASSET_PILOT_CONFIG };

export const DECISION_VERSION = "5.1";
export const REPORT_JSON_NAME = "brand-asset-review-decision-writer.json";
export const REPORT_MD_NAME = "brand-asset-review-decision-writer.md";

const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_REVIEWER = "Joan";

export const DECISION = {
  APPROVE: "approve",
  REJECT: "reject",
  KEEP: "keep-candidate",
};

/** Full formal approval — all three fields must align (v5 decision writer). */
export const FORMALLY_APPROVED_VALUES = {
  usageReviewStatus: "Usage Review Complete",
  explorerUsePermission: "Approved For Explorer",
  assetStatus: "Approved For Explorer Use",
};

/** Workflow copy "Reviewed" maps to schema Usage Review Status option. */
export const FORMALLY_APPROVED_USAGE_STATUSES = new Set(["Reviewed", "Usage Review Complete"]);

/** v5 decision writer stamps this pattern in Review Notes on apply. */
export const V5_APPROVAL_REVIEW_NOTE_PATTERN =
  /Approved after human source\/visual review by/i;

/** Safe rollback for approval-state conflicts (v5.1). */
export const APPROVAL_STATE_CORRECTION_VALUES = {
  assetStatus: "Candidate",
  explorerUsePermission: "Candidate Only",
  usageReviewStatus: "Pending Review",
  reviewNotes:
    "Approval state corrected because Asset Status previously said Approved For Explorer Use while Explorer Use Permission remained Candidate Only. Not approved for Explorer until human review.",
};

export const APPROVAL_STATE_CONFLICT_CLASS = "Approval State Conflict";

/** Values this writer is permitted to set (approval gate — v5 only). */
export const APPROVED_VALUES = {
  usageReviewStatus: FORMALLY_APPROVED_VALUES.usageReviewStatus,
  explorerUsePermission: FORMALLY_APPROVED_VALUES.explorerUsePermission,
  assetStatus: FORMALLY_APPROVED_VALUES.assetStatus,
};

export const REJECTED_VALUES = {
  usageReviewStatus: "Reviewed",
  explorerUsePermission: "Do Not Use",
  assetStatus: "Do Not Use",
};

export const KEEP_VALUES = {
  usageReviewStatus: "Needs Review",
  explorerUsePermission: "Candidate Only",
};

const COMPANY_CONTROLLED_BASES = new Set([
  "Company Materials",
  "Marriott-Controlled Source",
  "Rendered Official Source",
  "Local Reference Material",
]);

const FILES_READ = [
  "AGENTS.md",
  "lib/partner-intelligence/brand-asset-review-decision-writer.js",
  "lib/partner-intelligence/brand-asset-human-review-readiness.js",
  "lib/partner-intelligence/tribute-visual-asset-slot-review.js",
  "lib/partner-intelligence/brand-explorer-visual-slot-requirements.js",
  "lib/partner-intelligence/brand-asset-registry-workflow.js",
  "reports/brand-asset-human-review-readiness.json",
  "reports/tribute-visual-asset-slot-review.json",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function readSlotGovernanceFromFields(f) {
  const g = (key) => nz(f[MAP_VISUAL_SLOT[key]]);
  return {
    explorerSection: g("explorerSection"),
    relatedPropertyName: g("relatedPropertyName"),
    relatedValueDriver: g("relatedValueDriver"),
    countryRegion: g("countryRegion"),
    calaRelevant: g("calaRelevant"),
    propertyConfirmed: g("propertyConfirmed"),
    brandConfirmed: g("brandConfirmed"),
    validationStatus: g("validationStatus"),
    validationNotes: g("validationNotes"),
  };
}

function extractPropertyFromAssetName(assetName) {
  const m = nz(assetName).match(/^(.+?)\s+—\s+/);
  return m ? m[1].trim() : "";
}

function normalizeRecordForDecision(rawRecord) {
  const base = normalizeRegistryAssetRecord(rawRecord);
  const f = rawRecord.fields || {};
  const slotGovernance = readSlotGovernanceFromFields(f);
  const mappedVisualSlot =
    slotGovernance.explorerSection &&
    VISUAL_SLOT_DEFINITIONS.some((d) => d.slot === slotGovernance.explorerSection)
      ? slotGovernance.explorerSection
      : mapRecordToVisualSlot(base);

  return {
    ...base,
    slotGovernance,
    mappedVisualSlot,
    explorerSection: slotGovernance.explorerSection || mappedVisualSlot,
    relatedPropertyName: slotGovernance.relatedPropertyName || extractPropertyFromAssetName(base.assetName),
    countryRegion: slotGovernance.countryRegion,
    validationStatus: slotGovernance.validationStatus,
    validationNotes: slotGovernance.validationNotes,
    reviewNotes: nz(f[MAP_BRAND_ASSET.reviewNotes]),
    companyValidated: Boolean(f[MAP_BRAND_ASSET.companyValidated]),
    companyValidationDate: nz(f[MAP_BRAND_ASSET.companyValidationDate]),
  };
}

function needsPropertyContext(slot) {
  return [VISUAL_SLOT.HERO, VISUAL_SLOT.GALLERY, VISUAL_SLOT.VALUE_DRIVER].includes(slot);
}

/**
 * Determine whether a record is safe to approve for Explorer use.
 * Returns { eligible: boolean, blockReasons: string[] }.
 */
export function evaluateApprovalEligibility(record, { allowNonPrimary = false } = {}) {
  const blockReasons = [];
  const slot = record.mappedVisualSlot;
  const validationStatus = nz(record.validationStatus);
  const explorerUse = nz(record.explorerUsePermission);
  const assetStatus = nz(record.assetStatus);

  if (!record.isPrimaryCandidate && !allowNonPrimary) {
    blockReasons.push("Not a primary candidate (pass --allow-non-primary to override)");
  }
  if (slot === VISUAL_SLOT.RECENT_OPENINGS || /recent opening/i.test(record.explorerSection)) {
    blockReasons.push("Recent Openings slot — never approve without property + PR/opening/date");
  }
  if (slot === VISUAL_SLOT.PR_LINK || /pr \/ opening link|pr \/ recent openings/i.test(record.explorerSection)) {
    blockReasons.push("PR / Opening Link — provenance only");
  }
  if (validationStatus === "Mock/Demo Guard") {
    blockReasons.push("Visual Slot Validation Status is Mock/Demo Guard");
  }
  if (validationStatus === "Provenance Only" || /pr provenance/i.test(validationStatus)) {
    blockReasons.push("Visual Slot Validation Status is PR Provenance Only");
  }
  if (explorerUse === "Do Not Use") {
    blockReasons.push("Explorer Use Permission is Do Not Use");
  }
  if (explorerUse === "Internal Only") {
    blockReasons.push("Explorer Use Permission is Internal Only (e.g. FDD reference)");
  }
  if (assetStatus === "Mock/Demo") {
    blockReasons.push("Asset Status is Mock/Demo");
  }
  if (nz(record.assetType) === "PDF / Brochure" || slot === VISUAL_SLOT.BRAND_STANDARDS) {
    blockReasons.push("Source/PDF reference — not a visual Explorer asset");
  }
  if (!nz(record.sourceUrl)) {
    blockReasons.push("Missing Source URL");
  }
  if (needsPropertyContext(slot) && !nz(record.relatedPropertyName)) {
    blockReasons.push("Missing property context for property image slot");
  }
  if (!COMPANY_CONTROLLED_BASES.has(nz(record.sourceBasis))) {
    blockReasons.push(`Source Basis not company/Marriott-controlled: ${nz(record.sourceBasis) || "(blank)"}`);
  }
  if (record.companyValidated || record.companyValidationDate) {
    blockReasons.push("Company Validated already set — writer must not touch validation fields");
  }

  return { eligible: blockReasons.length === 0, blockReasons };
}

/** Formal approval requires aligned fields plus v5 decision-writer review note. */
export function isFormallyApprovedRecord(record) {
  return (
    nz(record.explorerUsePermission) === FORMALLY_APPROVED_VALUES.explorerUsePermission &&
    FORMALLY_APPROVED_USAGE_STATUSES.has(nz(record.usageReviewStatus)) &&
    nz(record.assetStatus) === FORMALLY_APPROVED_VALUES.assetStatus &&
    V5_APPROVAL_REVIEW_NOTE_PATTERN.test(nz(record.reviewNotes))
  );
}

/**
 * Detect approval-state conflict: Asset Status says approved but fields do not match formal approval.
 * These are NOT protected approved records.
 */
export function hasApprovalStateConflict(record) {
  if (isFormallyApprovedRecord(record)) return false;

  const assetStatus = nz(record.assetStatus);
  if (assetStatus !== FORMALLY_APPROVED_VALUES.assetStatus) return false;

  const explorerUse = nz(record.explorerUsePermission);
  const usageReview = nz(record.usageReviewStatus);

  if (explorerUse !== FORMALLY_APPROVED_VALUES.explorerUsePermission) {
    return true;
  }

  if (!FORMALLY_APPROVED_USAGE_STATUSES.has(usageReview)) {
    return true;
  }

  if (!V5_APPROVAL_REVIEW_NOTE_PATTERN.test(nz(record.reviewNotes))) {
    return true;
  }

  return false;
}

export function buildApprovalStateCorrectionFields() {
  return {
    [MAP_BRAND_ASSET.assetStatus]: APPROVAL_STATE_CORRECTION_VALUES.assetStatus,
    [MAP_BRAND_ASSET.explorerUsePermission]: APPROVAL_STATE_CORRECTION_VALUES.explorerUsePermission,
    [MAP_BRAND_ASSET.usageReviewStatus]: APPROVAL_STATE_CORRECTION_VALUES.usageReviewStatus,
    [MAP_BRAND_ASSET.reviewNotes]: APPROVAL_STATE_CORRECTION_VALUES.reviewNotes,
  };
}

function fieldValuesEqual(current, proposed) {
  const cur = current == null ? "" : String(current).trim();
  const prop = proposed == null ? "" : String(proposed).trim();
  if (typeof current === "boolean" || typeof proposed === "boolean") {
    return Boolean(current) === Boolean(proposed);
  }
  return cur === prop;
}

function correctionNeedsUpdate(rawRecord, proposedFields) {
  const f = rawRecord.fields || {};
  return Object.entries(proposedFields).some(([key, value]) => !fieldValuesEqual(f[key], value));
}

export function auditApprovalStateConsistency(records, rawRecords = []) {
  const rawById = new Map(rawRecords.map((r) => [r.id, r]));
  const formalApproved = [];
  const approvalStateConflicts = [];
  const recordsUntouched = [];

  for (const record of records) {
    const snapshot = {
      recordId: record.id,
      assetName: record.assetName,
      assetStatus: record.assetStatus,
      explorerUsePermission: record.explorerUsePermission,
      usageReviewStatus: record.usageReviewStatus,
      mappedVisualSlot: record.mappedVisualSlot,
      recommendedExplorerSlot: record.recommendedExplorerSlot,
    };

    if (isFormallyApprovedRecord(record)) {
      formalApproved.push({ ...snapshot, classification: "Formal Approved" });
      recordsUntouched.push({ ...snapshot, reason: "Formally approved — protected" });
      continue;
    }

    if (hasApprovalStateConflict(record)) {
      const fields = buildApprovalStateCorrectionFields();
      const raw = rawById.get(record.id);
      approvalStateConflicts.push({
        ...snapshot,
        classification: APPROVAL_STATE_CONFLICT_CLASS,
        conflictReasons: [
          `Asset Status = ${FORMALLY_APPROVED_VALUES.assetStatus}`,
          `Explorer Use Permission = ${nz(record.explorerUsePermission) || "(blank)"} (expected ${FORMALLY_APPROVED_VALUES.explorerUsePermission} for formal approval)`,
          `Usage Review Status = ${nz(record.usageReviewStatus) || "(blank)"} (expected ${FORMALLY_APPROVED_VALUES.usageReviewStatus})`,
          ...(V5_APPROVAL_REVIEW_NOTE_PATTERN.test(nz(record.reviewNotes))
            ? []
            : ["Missing v5 decision-writer approval note in Review Notes"]),
        ],
        proposedFields: fields,
        needsUpdate: raw ? correctionNeedsUpdate(raw, fields) : true,
      });
      continue;
    }

    recordsUntouched.push({ ...snapshot, reason: "No approval-state conflict" });
  }

  const proposedCorrections = approvalStateConflicts.filter((c) => c.needsUpdate);

  return {
    formalApproved,
    approvalStateConflicts,
    proposedCorrections,
    recordsUntouched,
    recordsUntouchedCount: recordsUntouched.length,
    proposedCorrectionCount: proposedCorrections.length,
  };
}

async function runApprovalStateCorrectionsWriter(rawRecords, proposedCorrections, { apply = false } = {}) {
  const skipped = [];
  const proposed = [];
  const validationErrors = [];

  for (const correction of proposedCorrections) {
    const raw = rawRecords.find((r) => r.id === correction.recordId);
    if (!raw) continue;

    const validation = validateDecisionPayload(correction.proposedFields);
    if (!validation.valid) {
      validationErrors.push({ recordId: correction.recordId, assetName: correction.assetName, errors: validation.errors });
      continue;
    }

    if (!correction.needsUpdate) {
      skipped.push({
        recordId: correction.recordId,
        assetName: correction.assetName,
        reason: "already matches correction target",
      });
      continue;
    }

    proposed.push({
      recordId: correction.recordId,
      assetName: correction.assetName,
      fields: correction.proposedFields,
    });
  }

  let updated = [];
  if (apply && proposed.length) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    const BATCH = 10;
    for (let i = 0; i < proposed.length; i += BATCH) {
      const batch = proposed.slice(i, i + BATCH);
      const patched = await patchRegistryRecordsBatch(
        baseId,
        apiKey,
        batch.map((p) => ({ recordId: p.recordId, fields: p.fields }))
      );
      updated.push(
        ...patched.map((r) => ({
          recordId: r.id,
          assetName: nz(r.fields?.[MAP_BRAND_ASSET.assetName]),
          assetStatus: nz(r.fields?.[MAP_BRAND_ASSET.assetStatus]),
          explorerUsePermission: nz(r.fields?.[MAP_BRAND_ASSET.explorerUsePermission]),
          usageReviewStatus: nz(r.fields?.[MAP_BRAND_ASSET.usageReviewStatus]),
        }))
      );
    }
  }

  return { proposed, skipped, updated, validationErrors };
}

function buildApprovedFields() {
  return {
    [MAP_BRAND_ASSET.usageReviewStatus]: FORMALLY_APPROVED_VALUES.usageReviewStatus,
    [MAP_BRAND_ASSET.explorerUsePermission]: APPROVED_VALUES.explorerUsePermission,
    [MAP_BRAND_ASSET.assetStatus]: APPROVED_VALUES.assetStatus,
    [MAP_BRAND_ASSET.lastReviewedDate]: todayIso(),
    [MAP_BRAND_ASSET.reviewNotes]: `Approved after human source/visual review by ${DEFAULT_REVIEWER}. Marriott-controlled source. No Marriott validation implied.`,
  };
}

function buildRejectedFields() {
  return {
    [MAP_BRAND_ASSET.usageReviewStatus]: FORMALLY_APPROVED_VALUES.usageReviewStatus,
    [MAP_BRAND_ASSET.explorerUsePermission]: REJECTED_VALUES.explorerUsePermission,
    [MAP_BRAND_ASSET.assetStatus]: REJECTED_VALUES.assetStatus,
    [MAP_BRAND_ASSET.lastReviewedDate]: todayIso(),
    [MAP_BRAND_ASSET.reviewNotes]: `Rejected after human review by ${DEFAULT_REVIEWER}. Do not use for Explorer.`,
  };
}

function buildKeepCandidateFields() {
  return {
    [MAP_BRAND_ASSET.usageReviewStatus]: APPROVAL_STATE_CORRECTION_VALUES.usageReviewStatus,
    [MAP_BRAND_ASSET.explorerUsePermission]: KEEP_VALUES.explorerUsePermission,
    [MAP_BRAND_ASSET.reviewNotes]: "Kept as candidate pending further source/visual review.",
  };
}

/** Hard guard — reject any payload that would touch prohibited fields. */
export function validateDecisionPayload(fields) {
  const errors = [];
  if (Object.prototype.hasOwnProperty.call(fields, MAP_BRAND_ASSET.companyValidated)) {
    errors.push("Company Validated must not be written");
  }
  if (Object.prototype.hasOwnProperty.call(fields, MAP_BRAND_ASSET.companyValidationDate)) {
    errors.push("Company Validation Date must not be written");
  }
  if (Object.prototype.hasOwnProperty.call(fields, MAP_BRAND_ASSET.attachment)) {
    errors.push("Attachment must not be written");
  }
  if (Object.prototype.hasOwnProperty.call(fields, MAP_BRAND_ASSET.localFilePath)) {
    errors.push("Local File Path must not be written");
  }
  return { valid: errors.length === 0, errors };
}

function getRegistryTableName() {
  return process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID || BRAND_ASSET_REGISTRY_TABLE;
}

function registryDataUrl(baseId) {
  const table = encodeURIComponent(getRegistryTableName());
  return `https://api.airtable.com/v0/${baseId}/${table}`;
}

async function registryDataFetch(url, apiKey, init = {}) {
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function patchRegistryRecordsBatch(baseId, apiKey, patches) {
  const url = registryDataUrl(baseId);
  const { res, json } = await registryDataFetch(url, apiKey, {
    method: "PATCH",
    body: JSON.stringify({
      records: patches.map((p) => ({ id: p.recordId, fields: p.fields })),
      typecast: true,
    }),
  });
  if (!res.ok) {
    throw new Error(json.error?.message || `Airtable patch registry batch failed: ${res.status}`);
  }
  return json.records || [];
}

function isVisualReviewPrimary(record) {
  if (!record.isPrimaryCandidate) return false;
  if (record.mappedVisualSlot === VISUAL_SLOT.BRAND_STANDARDS) return false;
  if (nz(record.assetType) === "PDF / Brochure") return false;
  if (record.explorerUsePermission === "Internal Only") return false;
  if (record.explorerUsePermission === "Do Not Use") return false;
  return true;
}

function inferValueDriverLabel(record) {
  const property = nz(record.relatedPropertyName) || extractPropertyFromAssetName(record.assetName);
  const hay = property.toLowerCase();
  if (/resort|beach|cove|island|nizuc|holbox/i.test(hay)) return "Resort";
  if (/ermita|cartagena|heritage|colonial|conversion/i.test(hay)) return "Conversion / Adaptive Reuse";
  if (/mixed|alameda/i.test(hay)) return "Mixed-Use";
  return "Urban";
}

function slotLabel(record) {
  if (record.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER) {
    const driver = nz(record.relatedValueDriver) && record.relatedValueDriver !== "None"
      ? record.relatedValueDriver
      : inferValueDriverLabel(record);
    return `Value Driver: ${driver}`;
  }
  return `${record.mappedVisualSlot} → ${record.recommendedExplorerSlot}`;
}

export async function buildReviewDecisionReport({
  brandKey = "tribute-portfolio",
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  approveRecords = [],
  rejectRecords = [],
  keepCandidateRecords = [],
  allowNonPrimary = false,
  apply = false,
  decisionsApproved = false,
  applyApprovalStateCorrections = false,
  approvalStateCorrectionsApproved = false,
} = {}) {
  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG["tribute-portfolio"];
  const resolvedBrandId = pilot?.brandRecordId || brandRecordId;
  const decisionsApplyMode = apply && decisionsApproved;
  const correctionsApplyMode = apply && approvalStateCorrectionsApproved;
  const applyMode = decisionsApplyMode || correctionsApplyMode;

  let rawRecords = [];
  let registryReadError = null;
  try {
    rawRecords = await listRegistryRecordsRaw(resolvedBrandId);
  } catch (err) {
    registryReadError = err.message || String(err);
  }

  const records = rawRecords.map(normalizeRecordForDecision);
  const byId = new Map(records.map((r) => [r.id, r]));

  const approvalStateAudit = registryReadError
    ? {
        formalApproved: [],
        approvalStateConflicts: [],
        proposedCorrections: [],
        recordsUntouched: [],
        recordsUntouchedCount: 0,
        proposedCorrectionCount: 0,
        correctionWriter: { proposed: [], skipped: [], updated: [], validationErrors: [] },
      }
    : auditApprovalStateConsistency(records, rawRecords);

  if (!registryReadError) {
    const correctionWriter = await runApprovalStateCorrectionsWriter(
      rawRecords,
      approvalStateAudit.proposedCorrections,
      { apply: correctionsApplyMode }
    );
    approvalStateAudit.correctionWriter = correctionWriter;
  }

  const allPrimaries = records.filter((r) => r.isPrimaryCandidate);
  const primaries = records.filter(isVisualReviewPrimary);
  const excludedPrimaries = allPrimaries.filter((r) => !isVisualReviewPrimary(r));

  const primaryListing = primaries.map((r) => ({
    recordId: r.id,
    assetName: r.assetName,
    slot: slotLabel(r),
    mappedVisualSlot: r.mappedVisualSlot,
    recommendedExplorerSlot: r.recommendedExplorerSlot,
    relatedPropertyName: r.relatedPropertyName || null,
    explorerUsePermission: r.explorerUsePermission,
    assetStatus: r.assetStatus,
    eligibleForApproval: evaluateApprovalEligibility(r, { allowNonPrimary }).eligible,
  }));

  const plans = [];
  const blocked = [];

  const planDecision = (recordId, decision) => {
    const record = byId.get(recordId);
    if (!record) {
      blocked.push({ recordId, decision, reason: "Record ID not found for this brand" });
      return;
    }
    if (decision === DECISION.APPROVE) {
      const { eligible, blockReasons } = evaluateApprovalEligibility(record, { allowNonPrimary });
      if (!eligible) {
        blocked.push({ recordId, assetName: record.assetName, decision, reasons: blockReasons });
        return;
      }
      const fields = buildApprovedFields();
      const validation = validateDecisionPayload(fields);
      if (!validation.valid) {
        blocked.push({ recordId, assetName: record.assetName, decision, reasons: validation.errors });
        return;
      }
      plans.push({ recordId, assetName: record.assetName, decision, slot: slotLabel(record), fields });
    } else if (decision === DECISION.REJECT) {
      const fields = buildRejectedFields();
      plans.push({ recordId, assetName: record.assetName, decision, slot: slotLabel(record), fields });
    } else if (decision === DECISION.KEEP) {
      const fields = buildKeepCandidateFields();
      plans.push({ recordId, assetName: record.assetName, decision, slot: slotLabel(record), fields });
    }
  };

  for (const id of approveRecords) planDecision(id, DECISION.APPROVE);
  for (const id of rejectRecords) planDecision(id, DECISION.REJECT);
  for (const id of keepCandidateRecords) planDecision(id, DECISION.KEEP);

  let updated = [];
  if (decisionsApplyMode && plans.length) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    const BATCH = 10;
    for (let i = 0; i < plans.length; i += BATCH) {
      const batch = plans.slice(i, i + BATCH);
      const patched = await patchRegistryRecordsBatch(
        baseId,
        apiKey,
        batch.map((p) => ({ recordId: p.recordId, fields: p.fields }))
      );
      updated.push(
        ...patched.map((r) => ({
          recordId: r.id,
          assetName: nz(r.fields?.[MAP_BRAND_ASSET.assetName]),
          explorerUsePermission: nz(r.fields?.[MAP_BRAND_ASSET.explorerUsePermission]),
          assetStatus: nz(r.fields?.[MAP_BRAND_ASSET.assetStatus]),
        }))
      );
    }
  }

  const approvedRecordIds = new Set(
    (decisionsApplyMode
      ? updated.map((u) => u.recordId)
      : plans.filter((p) => p.decision === DECISION.APPROVE).map((p) => p.recordId))
  );
  const approvedCoverageBySlot = {};
  for (const r of records) {
    const isApproved = approvedRecordIds.has(r.id) || isFormallyApprovedRecord(r);
    if (!isApproved) continue;
    const key = r.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER ? slotLabel(r) : r.mappedVisualSlot;
    approvedCoverageBySlot[key] = approvedCoverageBySlot[key] || [];
    approvedCoverageBySlot[key].push(r.assetName);
  }

  const slotsStillMissing = [
    { slot: VISUAL_SLOT.RECENT_OPENINGS, reason: "No property + opening/PR/date candidate" },
    { slot: "Value Driver (Boutique / Lifestyle)", reason: "No primary candidate" },
    { slot: "Value Driver (Mixed-Use)", reason: "No primary candidate" },
    { slot: VISUAL_SLOT.PR_LINK, reason: "Provenance only until Rendered Source Capture v1" },
  ];

  const coveredSlotCount = Object.keys(approvedCoverageBySlot).length;
  const readyForDownload = coveredSlotCount > 0;
  const readyForExplorerPromotion = false;

  const exampleApprovalCommand = `npm run brand-asset-review-decision-writer -- --brand ${brandKey} --apply --approve-brand-asset-review-decisions --approve-records ${
    primaries
      .filter((p) => !isFormallyApprovedRecord(p) && !hasApprovalStateConflict(p))
      .slice(0, 2)
      .map((p) => p.id)
      .join(",") || "recXXXX,recYYYY"
  }`;

  const correctionApplyCommand = `npm run brand-asset-review-decision-writer -- --brand ${brandKey} --apply --approve-brand-asset-approval-state-corrections`;

  const mode = correctionsApplyMode
    ? "approval-state-corrections-apply"
    : decisionsApplyMode
      ? "decisions-apply"
      : "dry-run";

  const airtableModified = Boolean(
    (decisionsApplyMode && updated.length) ||
      (correctionsApplyMode && approvalStateAudit.correctionWriter?.updated?.length)
  );

  return {
    decisionVersion: DECISION_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified,
    brandSetupMediaUntouched: true,
    companyValidatedUntouched: true,
    brand: {
      key: brandKey,
      recordId: resolvedBrandId,
      name: pilot?.brandName || "Tribute Portfolio",
      parentCompany: pilot?.parentCompany || "Marriott International, Inc.",
    },
    textGovernanceStatus: {
      note: "Text/governance status is owned by the Tribute package pipeline; this module does not change it.",
      textGovernancePlatformReady: true,
    },
    filesRead: FILES_READ,
    registryReadError,
    totalRecordsScanned: records.length,
    approvalStateAudit,
    formalApprovedRecords: approvalStateAudit.formalApproved,
    approvalStateConflicts: approvalStateAudit.approvalStateConflicts,
    recordsProposedForCorrection: approvalStateAudit.proposedCorrections,
    recordsUntouchedCount: approvalStateAudit.recordsUntouchedCount,
    recordsUntouched: approvalStateAudit.recordsUntouched,
    correctionApplyCommand,
    nextApprovalCommandAfterCorrection: exampleApprovalCommand,
    primaryCandidateCount: primaries.length,
    allPrimaryRecords: allPrimaries.length,
    excludedFromVisualReview: excludedPrimaries.map((r) => ({
      recordId: r.id,
      assetName: r.assetName,
      eligibleForApproval: false,
      reason: "Internal/source reference — not a visual Explorer review candidate",
    })),
    primaryCandidates: primaryListing,
    requested: {
      approveRecords,
      rejectRecords,
      keepCandidateRecords,
      allowNonPrimary,
    },
    selectedForApproval: plans.filter((p) => p.decision === DECISION.APPROVE),
    selectedForRejection: plans.filter((p) => p.decision === DECISION.REJECT),
    selectedKeptAsCandidate: plans.filter((p) => p.decision === DECISION.KEEP),
    blocked,
    updated: decisionsApplyMode ? updated : [],
    correctionsUpdated: correctionsApplyMode ? approvalStateAudit.correctionWriter?.updated || [] : [],
    approvedCoverageBySlot,
    slotsStillMissing,
    readyForDownloadOrAttachment: readyForDownload,
    readyForExplorerPromotion,
    exampleApprovalCommand,
    nextCommand: `npm run brand-asset-review-decision-writer -- --brand ${brandKey} --dry-run`,
    remainingWorkBeforeDownload: [
      "Human approves selected records via --approve-records (explicit IDs only).",
      "Confirm rights/usage for each approved Marriott-controlled source URL.",
      "Build asset download + attachment writer (separate module — not this task).",
      "Do not attach files until download + rights registry exists.",
    ],
    remainingWorkBeforeExplorerPromotion: [
      "Approve a coherent slot set (logo + hero + gallery + value drivers) after human review.",
      "Capture Recent Openings with property + PR/opening/date (currently Missing).",
      "Fill Boutique/Lifestyle and Mixed-Use value-driver imagery gaps.",
      "Build governed Explorer hero/logo promotion writer (separate module).",
      "Do not replace Mock/Demo hero until governed CALA hero is approved and promoted.",
      "Rendered Source Capture v1 for Marriott newsroom PR before PR link promotion.",
    ],
  };
}

export function buildReviewDecisionMarkdown(report) {
  const lines = [];
  lines.push("# Brand Asset Review Decision Writer v5.1");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push(`Text/governance Platform Ready: **${report.textGovernanceStatus.textGovernancePlatformReady ? "yes" : "no"}**`);
  lines.push(`Brand Setup media untouched: **${report.brandSetupMediaUntouched ? "yes" : "no"}**`);
  lines.push(`Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");

  lines.push("## 1. Summary");
  lines.push("");
  lines.push(`- Total asset records scanned: **${report.totalRecordsScanned}**`);
  lines.push(`- Formal approved records: **${report.formalApprovedRecords?.length || 0}**`);
  lines.push(`- Approval-state conflicts: **${report.approvalStateConflicts?.length || 0}**`);
  lines.push(`- Records proposed for correction: **${report.recordsProposedForCorrection?.length || 0}**`);
  lines.push(`- Records untouched: **${report.recordsUntouchedCount || 0}**`);
  lines.push(`- Primary candidates: **${report.primaryCandidateCount}**`);
  lines.push(`- Selected for approval: **${report.selectedForApproval.length}**`);
  lines.push(`- Selected for rejection: **${report.selectedForRejection.length}**`);
  lines.push(`- Kept as candidate: **${report.selectedKeptAsCandidate.length}**`);
  lines.push(`- Blocked: **${report.blocked.length}**`);
  lines.push(`- Decision records updated on apply: **${report.updated.length}**`);
  lines.push(`- Correction records updated on apply: **${report.correctionsUpdated?.length || 0}**`);
  lines.push("");

  if (report.registryReadError) {
    lines.push(`> Registry read error: ${report.registryReadError}`);
    lines.push("");
  }

  lines.push("## 2. Formal approved records");
  lines.push("");
  if (!report.formalApprovedRecords?.length) {
    lines.push("None.");
  } else {
    for (const r of report.formalApprovedRecords) {
      lines.push(
        `- \`${r.recordId}\` — ${r.assetName} (${r.assetStatus} / ${r.explorerUsePermission} / ${r.usageReviewStatus})`
      );
    }
  }
  lines.push("");

  lines.push("## 3. Approval-state conflicts");
  lines.push("");
  if (!report.approvalStateConflicts?.length) {
    lines.push("None.");
  } else {
    for (const c of report.approvalStateConflicts) {
      lines.push(`- \`${c.recordId}\` — ${c.assetName} — **${c.classification}**`);
      for (const reason of c.conflictReasons || []) lines.push(`  - ${reason}`);
    }
  }
  lines.push("");

  lines.push("## 4. Records proposed for correction");
  lines.push("");
  if (!report.recordsProposedForCorrection?.length) {
    lines.push("None.");
  } else {
    for (const c of report.recordsProposedForCorrection) {
      lines.push(`- \`${c.recordId}\` — ${c.assetName}`);
      lines.push(`  - → Asset Status: Candidate, Explorer Use Permission: Candidate Only, Usage Review Status: Pending Review`);
    }
  }
  lines.push("");

  lines.push("## 5. Primary candidates (record IDs)");
  lines.push("");
  lines.push("| Record ID | Slot | Asset | Approval-eligible |");
  lines.push("|-----------|------|-------|-------------------|");
  for (const p of report.primaryCandidates) {
    lines.push(`| \`${p.recordId}\` | ${p.slot} | ${p.assetName} | ${p.eligibleForApproval ? "yes" : "no"} |`);
  }
  lines.push("");

  if (report.excludedFromVisualReview?.length) {
    lines.push("### Excluded from visual review (not approval-eligible)");
    lines.push("");
    for (const e of report.excludedFromVisualReview) {
      lines.push(`- \`${e.recordId}\` — ${e.assetName} (${e.reason})`);
    }
    lines.push("");
  }

  const printPlans = (title, list) => {
    lines.push(`## ${title}`);
    lines.push("");
    if (!list.length) {
      lines.push("None.");
      lines.push("");
      return;
    }
    for (const p of list) {
      lines.push(`- \`${p.recordId}\` — ${p.assetName} (${p.slot})`);
    }
    lines.push("");
  };

  printPlans("6. Selected for approval", report.selectedForApproval);
  printPlans("7. Selected for rejection", report.selectedForRejection);
  printPlans("8. Kept as candidate", report.selectedKeptAsCandidate);

  lines.push("## 9. Blocked records");
  lines.push("");
  if (!report.blocked.length) {
    lines.push("None.");
  } else {
    for (const b of report.blocked) {
      const reasons = b.reasons ? b.reasons.join("; ") : b.reason;
      lines.push(`- \`${b.recordId}\`${b.assetName ? ` (${b.assetName})` : ""} — ${reasons}`);
    }
  }
  lines.push("");

  lines.push("## 10. Records updated on apply");
  lines.push("");
  if (!report.updated.length && !report.correctionsUpdated?.length) {
    lines.push("None (dry-run or no eligible selections).");
  } else {
    for (const u of report.updated) {
      lines.push(`- [decision] \`${u.recordId}\` — ${u.assetName} → ${u.explorerUsePermission} / ${u.assetStatus}`);
    }
    for (const u of report.correctionsUpdated || []) {
      lines.push(`- [correction] \`${u.recordId}\` — ${u.assetName} → ${u.explorerUsePermission} / ${u.assetStatus}`);
    }
  }
  lines.push("");

  lines.push("## 11. Approved asset coverage by slot");
  lines.push("");
  if (!Object.keys(report.approvedCoverageBySlot).length) {
    lines.push("No approved assets yet.");
  } else {
    for (const [slot, names] of Object.entries(report.approvedCoverageBySlot)) {
      lines.push(`- **${slot}**: ${names.join("; ")}`);
    }
  }
  lines.push("");

  lines.push("## 12. Slots still missing");
  lines.push("");
  for (const m of report.slotsStillMissing) {
    lines.push(`- **${m.slot}** — ${m.reason}`);
  }
  lines.push("");

  lines.push("## 13. Readiness");
  lines.push("");
  lines.push(`- Ready for asset download/attachment: **${report.readyForDownloadOrAttachment ? "yes (approved assets exist)" : "no"}**`);
  lines.push(`- Ready for Explorer promotion: **${report.readyForExplorerPromotion ? "yes" : "no"}**`);
  lines.push("");

  lines.push("## 14. Correction apply command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.correctionApplyCommand);
  lines.push("```");
  lines.push("");

  lines.push("## 15. Next approval command (after correction)");
  lines.push("");
  lines.push("```bash");
  lines.push(report.nextApprovalCommandAfterCorrection || report.exampleApprovalCommand);
  lines.push("```");
  lines.push("");

  lines.push("## 16. Remaining work before download/attachment");
  lines.push("");
  for (const item of report.remainingWorkBeforeDownload) lines.push(`- ${item}`);
  lines.push("");

  lines.push("## 17. Remaining work before Explorer promotion");
  lines.push("");
  for (const item of report.remainingWorkBeforeExplorerPromotion) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
