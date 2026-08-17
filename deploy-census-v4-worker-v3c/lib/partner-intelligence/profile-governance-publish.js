/**
 * Partner Intelligence → Profile Governance publish helpers (Setup root tables).
 * @see docs/data-intelligence/partner-intelligence-profile-governance-publish-plan.md
 */
import {
  MAP_PROFILE_GOVERNANCE_AIRTABLE,
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_USAGE_PERMISSION,
  GOVERNANCE_EXTERNAL_DISPLAY,
} from "../profile-governance/profile-governance-fields.js";
import {
  extractProfileGovernanceRaw,
  normalizeProfileGovernance,
} from "../profile-governance/normalize-profile-governance.js";
import { P1_GOVERNANCE_FIELD_ALIASES } from "../brand-operator-validation-audit/p1-profile-governance-field-specs.js";
import {
  assessTargetProtection,
  classifyGovernanceChange,
  HOLD_NOTE_PATTERN,
} from "./profile-governance-publish-readiness.js";

export const NEVER_PUBLISH_API_KEYS = ["companyValidated", "companyValidationDate"];

export const PUBLISH_GOVERNANCE_API_KEYS = [
  "validationStatus",
  "usagePermission",
  "sourceType",
  "sourceRegion",
  "lastReviewedDate",
  "refreshDueDate",
  "evidenceNotes",
  "missingDataFlags",
  "reviewedBy",
  "externalDisplayStatus",
  "internalNotes",
  "confidenceLevel",
];

export const CONFIDENCE_RANK = { Low: 1, Medium: 2, High: 3, Unknown: 0 };

export const VALIDATION_STATUS_RANK = {
  [GOVERNANCE_VALIDATION_STATUS.doNotUse]: 0,
  [GOVERNANCE_VALIDATION_STATUS.needsReview]: 1,
  [GOVERNANCE_VALIDATION_STATUS.aiAssisted]: 2,
  [GOVERNANCE_VALIDATION_STATUS.sourceInformed]: 3,
  [GOVERNANCE_VALIDATION_STATUS.ownerProvided]: 3,
  [GOVERNANCE_VALIDATION_STATUS.companyPublished]: 4,
  [GOVERNANCE_VALIDATION_STATUS.companyValidated]: 5,
  [GOVERNANCE_VALIDATION_STATUS.staleRefreshNeeded]: 1,
};

export const BRAND_TABLE = "Brand Setup - Brand Basics";
export const OPERATOR_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

export const READINESS_REPORT_PATH = "reports/partner-intelligence-publish-readiness.json";
export const MAX_READINESS_REPORT_AGE_MS = 24 * 60 * 60 * 1000;

function readFieldValue(fields, columnName) {
  const raw = fields?.[columnName];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return raw.trim() || null;
  if (typeof raw === "boolean") return raw;
  if (typeof raw === "object" && raw.name) return String(raw.name).trim() || null;
  return String(raw).trim() || null;
}

export function resolveGovernanceWriteColumn(canonicalColumn, entityType, recordFields) {
  const keys = Object.keys(recordFields || {});
  if (keys.includes(canonicalColumn)) return canonicalColumn;

  const aliasConfig = P1_GOVERNANCE_FIELD_ALIASES[canonicalColumn];
  if (aliasConfig?.aliases?.length) {
    for (const alias of aliasConfig.aliases) {
      if (keys.includes(alias)) return alias;
    }
    if (entityType === "operator" && canonicalColumn === "Confidence Level") {
      return "Data Confidence Level";
    }
  }
  return canonicalColumn;
}

export function readGovernanceColumnValue(currentFields, canonicalColumn, liveColumn, entityType) {
  const live = liveColumn || canonicalColumn;
  const fromLive = readFieldValue(currentFields, live);
  if (fromLive != null) return fromLive;
  if (live !== canonicalColumn) return readFieldValue(currentFields, canonicalColumn);
  if (entityType === "operator" && canonicalColumn === "Confidence Level") {
    return readFieldValue(currentFields, "Data Confidence Level");
  }
  return null;
}

export function isSourceTypeSafeToWrite(proposedSourceType, currentSourceType) {
  const proposed = String(proposedSourceType || "").trim();
  const current = String(currentSourceType || "").trim();
  if (!proposed || proposed === "Unknown") return false;
  return true;
}

export function proposedToPublishValues(proposed) {
  const values = {};
  for (const key of PUBLISH_GOVERNANCE_API_KEYS) {
    if (NEVER_PUBLISH_API_KEYS.includes(key)) continue;
    const value = proposed?.[key];
    if (value === undefined) continue;
    if (value === null || value === "") continue;
    values[key] = value;
  }
  return values;
}

export function governanceProposedToAirtable(proposed, { entityType, recordFields } = {}) {
  const publishValues = proposedToPublishValues(proposed);
  const fields = {};
  const columnMap = {};

  for (const [apiKey, value] of Object.entries(publishValues)) {
    const canonical = MAP_PROFILE_GOVERNANCE_AIRTABLE[apiKey];
    if (!canonical) {
      throw new Error(`Missing Airtable column mapping for publish key: ${apiKey}`);
    }
    const live = resolveGovernanceWriteColumn(canonical, entityType, recordFields);
    fields[live] = value;
    columnMap[canonical] = live;
  }

  return { fields, columnMap, publishValues };
}

export function assessPublishProtection(currentFields, entityType, proposed, piReviewDate) {
  const base = assessTargetProtection(currentFields, entityType, piReviewDate);
  const reasons = [...(base.reasons || [])];
  const currentRaw = base.currentRaw || extractProfileGovernanceRaw(currentFields, { entityType });

  const changeClass = proposed
    ? classifyGovernanceChange(currentRaw, proposed, { entityType })
    : null;
  if (changeClass === "downgrade") {
    reasons.push("would_downgrade_existing_validation");
  }
  if (changeClass === "protected") {
    reasons.push("validation_status_protected");
  }

  return {
    blocked: reasons.length > 0,
    reasons: [...new Set(reasons)],
    currentRaw,
    changeClass,
  };
}

function wouldDowngradeField(canonicalColumn, currentValue, proposedValue) {
  if (canonicalColumn === MAP_PROFILE_GOVERNANCE_AIRTABLE.validationStatus) {
    const cur = VALIDATION_STATUS_RANK[currentValue] ?? 1;
    const next = VALIDATION_STATUS_RANK[proposedValue] ?? 1;
    return next < cur;
  }
  if (canonicalColumn === MAP_PROFILE_GOVERNANCE_AIRTABLE.confidenceLevel) {
    const cur = CONFIDENCE_RANK[currentValue] ?? 0;
    const next = CONFIDENCE_RANK[proposedValue] ?? 0;
    return next < cur && cur > 0;
  }
  if (canonicalColumn === MAP_PROFILE_GOVERNANCE_AIRTABLE.externalDisplayStatus) {
    if (currentValue === GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel) {
      return (
        proposedValue === GOVERNANCE_EXTERNAL_DISPLAY.hideTrustLabel ||
        proposedValue === GOVERNANCE_EXTERNAL_DISPLAY.doNotDisplay
      );
    }
  }
  if (canonicalColumn === MAP_PROFILE_GOVERNANCE_AIRTABLE.usagePermission) {
    if (currentValue === GOVERNANCE_USAGE_PERMISSION.doNotUse) return true;
  }
  return false;
}

export function diffGovernancePublish(currentFields, proposed, entityType) {
  const { fields: desiredLiveFields, columnMap, publishValues } = governanceProposedToAirtable(
    proposed,
    { entityType, recordFields: currentFields }
  );

  const wouldUpdate = [];
  const unchanged = [];
  const skipped = [];

  for (const [apiKey, canonical] of Object.entries(MAP_PROFILE_GOVERNANCE_AIRTABLE)) {
    if (NEVER_PUBLISH_API_KEYS.includes(apiKey)) {
      skipped.push({
        field: canonical,
        apiKey,
        reason: "never_written_by_pi_publish",
      });
      continue;
    }
    if (!PUBLISH_GOVERNANCE_API_KEYS.includes(apiKey)) continue;

    const live = columnMap[canonical];
    if (!live) continue;

    const desired = desiredLiveFields[live];
    if (desired === undefined) continue;

    if (apiKey === "sourceType" && !isSourceTypeSafeToWrite(desired, readGovernanceColumnValue(currentFields, canonical, live, entityType))) {
      skipped.push({
        field: canonical,
        liveColumn: live,
        apiKey,
        reason: "source_type_unknown_not_safe",
        proposed: desired,
      });
      continue;
    }

    const currentNorm = readGovernanceColumnValue(currentFields, canonical, live, entityType);
    const desiredNorm = readFieldValue({ [live]: desired }, live);

    if (wouldDowngradeField(canonical, currentNorm, desiredNorm)) {
      skipped.push({
        field: canonical,
        liveColumn: live,
        apiKey,
        reason: "downgrade_blocked",
        from: currentNorm,
        to: desiredNorm,
      });
      continue;
    }

    if (currentNorm === desiredNorm) {
      unchanged.push({ field: canonical, liveColumn: live, apiKey, value: currentNorm });
    } else {
      wouldUpdate.push({
        field: canonical,
        liveColumn: live,
        apiKey,
        from: currentNorm,
        to: desiredNorm,
      });
    }
  }

  return { wouldUpdate, unchanged, skipped, desiredLiveFields, columnMap, publishValues };
}

export function buildExpectedGovernanceAfterPublish(currentFields, proposed, entityType, sourceTable) {
  const { desiredLiveFields } = governanceProposedToAirtable(proposed, {
    entityType,
    recordFields: currentFields,
  });
  const merged = { ...currentFields, ...desiredLiveFields };
  return normalizeProfileGovernance(merged, { entityType, sourceTable });
}

export function filterReadinessPackages(report, filters = {}) {
  const {
    entityType = null,
    targetRecId = null,
    packageKey = null,
    onlyEligible = true,
  } = filters;

  const eligible = report.eligiblePackages || [];
  const blocked = onlyEligible ? [] : report.blockedPackages || [];
  let packages = [...eligible, ...blocked];

  if (packageKey) {
    packages = packages.filter((p) => p.entityKey === packageKey);
  }
  if (entityType) {
    packages = packages.filter((p) => p.entityType === entityType);
  }
  if (targetRecId) {
    packages = packages.filter((p) => p.recordId === targetRecId);
  }
  if (onlyEligible) {
    packages = packages.filter((p) => !(report.blockedPackages || []).some((b) => b.entityKey === p.entityKey));
  }

  return packages;
}

export function readinessReportAgeMs(report) {
  const ts = Date.parse(report?.generatedAt || "");
  if (Number.isNaN(ts)) return Infinity;
  return Date.now() - ts;
}

export function buildPublishPlanEntry({
  packageEntry,
  targetProfile,
  mode = "dry-run",
  applyTimestamp = null,
}) {
  const entityType = packageEntry.entityType;
  const sourceTable = entityType === "brand" ? BRAND_TABLE : OPERATOR_TABLE;
  const proposedRaw = packageEntry.proposed?.proposed || null;
  const piReviewDate = proposedRaw?.lastReviewedDate || null;

  const entry = {
    entityKey: packageEntry.entityKey,
    entityType,
    recordId: packageEntry.recordId,
    entityName: packageEntry.entityName || targetProfile?.name || null,
    sourceTable,
    eligibleInReport: !(packageEntry.blockReasons?.length),
    blockReasons: packageEntry.blockReasons || [],
    changeClass: packageEntry.changeClass || null,
    protection: null,
    currentGovernance: null,
    proposed: proposedRaw,
    fieldDiff: null,
    expectedGovernance: packageEntry.proposed?.expectedGovernance || null,
    write: null,
    errors: [],
    skippedReasons: [],
  };

  if (!packageEntry.recordId || !entityType) {
    entry.errors.push("missing_entity_link");
    entry.write = { status: "skipped", reason: "missing_entity_link" };
    return entry;
  }

  if (!targetProfile) {
    entry.errors.push("target_profile_not_found");
    entry.write = { status: "skipped", reason: "target_profile_not_found" };
    return entry;
  }

  if (!proposedRaw) {
    entry.errors.push("missing_proposed_governance");
    entry.write = { status: "skipped", reason: "missing_proposed_governance" };
    return entry;
  }

  if (packageEntry.blockReasons?.length) {
    entry.errors.push(...packageEntry.blockReasons.map((r) => `blocked_in_report:${r}`));
    entry.write = { status: "skipped", reason: "blocked_in_report" };
    return entry;
  }

  const fields = targetProfile.fields || {};
  entry.currentGovernance = extractProfileGovernanceRaw(fields, { entityType });

  const proposedForApply = {
    ...proposedRaw,
    internalNotes: applyTimestamp
      ? `PI profile-governance publish ${applyTimestamp} (${entityType}:${packageEntry.recordId}).`
      : proposedRaw.internalNotes,
  };

  entry.protection = assessPublishProtection(fields, entityType, proposedForApply, piReviewDate);
  entry.fieldDiff = diffGovernancePublish(fields, proposedForApply, entityType);
  entry.expectedGovernance =
    packageEntry.proposed?.expectedGovernance ||
    buildExpectedGovernanceAfterPublish(fields, proposedForApply, entityType, sourceTable);

  if (entry.protection.blocked) {
    entry.skippedReasons = entry.protection.reasons;
    entry.errors.push(...entry.protection.reasons.map((r) => `protected:${r}`));
    entry.write = { status: "skipped", reason: "protected_fields" };
    return entry;
  }

  if (!entry.fieldDiff.wouldUpdate.length) {
    entry.write = { status: "skipped", reason: "no_changes" };
    return entry;
  }

  entry.write = {
    status: mode === "apply" ? "pending_apply" : "dry_run",
    patch: Object.fromEntries(
      entry.fieldDiff.wouldUpdate.map((row) => [row.liveColumn || row.field, entry.fieldDiff.desiredLiveFields[row.liveColumn || row.field]])
    ),
  };

  return entry;
}

export function holdMarkerInNotes(internalNotes) {
  return Boolean(internalNotes && HOLD_NOTE_PATTERN.test(String(internalNotes)));
}
