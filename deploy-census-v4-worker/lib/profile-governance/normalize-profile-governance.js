/**
 * Normalize Brand/Operator profile governance Airtable fields for API responses.
 * Read-only — no writes, no scoring. Derives conservative Explorer displayLabel / displaySubtitle.
 */
import {
  MAP_PROFILE_GOVERNANCE_AIRTABLE,
  MAP_PROFILE_GOVERNANCE_ALIASES,
  BRAND_GOVERNANCE_LEGACY_FALLBACKS,
  OPERATOR_GOVERNANCE_LEGACY_FALLBACKS,
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_USAGE_PERMISSION,
  GOVERNANCE_EXTERNAL_DISPLAY,
  GOVERNANCE_EXTERNAL_DISPLAY_LABEL,
  GOVERNANCE_EXTERNAL_SOURCE_BASIS,
} from "./profile-governance-fields.js";

const BLOCKED_VALIDATION_EXTERNAL = new Set([
  GOVERNANCE_VALIDATION_STATUS.doNotUse,
  GOVERNANCE_VALIDATION_STATUS.needsReview,
]);

const BLOCKED_USAGE_EXTERNAL = new Set([
  GOVERNANCE_USAGE_PERMISSION.internalOnly,
  GOVERNANCE_USAGE_PERMISSION.doNotUse,
]);

const USAGE_RESTRICTIVENESS = {
  [GOVERNANCE_USAGE_PERMISSION.doNotUse]: 0,
  [GOVERNANCE_USAGE_PERMISSION.internalOnly]: 1,
  [GOVERNANCE_USAGE_PERMISSION.companyValidated]: 2,
  [GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed]: 3,
  [GOVERNANCE_USAGE_PERMISSION.scoringAllowed]: 4,
  [GOVERNANCE_USAGE_PERMISSION.externalSnapshotAllowed]: 5,
};

function trimStr(v) {
  if (v == null || v === "") return null;
  const s = String(v).trim();
  return s || null;
}

function readSelect(fields, columnName) {
  const raw = fields?.[columnName];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return trimStr(raw);
  if (typeof raw === "object" && raw.name) return trimStr(raw.name);
  return trimStr(raw);
}

function readWithAliases(fields, apiKey) {
  const primary = MAP_PROFILE_GOVERNANCE_AIRTABLE[apiKey];
  const val = readSelect(fields, primary);
  if (val) return val;
  for (const alias of MAP_PROFILE_GOVERNANCE_ALIASES[apiKey] || []) {
    const hit = readSelect(fields, alias);
    if (hit) return hit;
  }
  return null;
}

function readDate(fields, apiKey) {
  const primary = MAP_PROFILE_GOVERNANCE_AIRTABLE[apiKey];
  const aliases = MAP_PROFILE_GOVERNANCE_ALIASES[apiKey] || [];
  const raw =
    fields?.[primary] ??
    aliases.map((a) => fields?.[a]).find((v) => v != null && String(v).trim() !== "");
  const s = trimStr(raw);
  if (!s) return null;
  return s.split("T")[0];
}

function readCheckbox(fields, apiKey) {
  const col = MAP_PROFILE_GOVERNANCE_AIRTABLE[apiKey];
  return fields?.[col] === true;
}

/**
 * @param {Record<string, unknown>|null|undefined} fields
 * @param {{ entityType?: 'brand'|'operator', sourceTable?: string, fallbackFields?: Record<string, unknown>, nowDate?: string }} [options]
 */
export function extractProfileGovernanceRaw(fields, options = {}) {
  const f = fields || {};
  const entityType = options.entityType || "brand";

  let confidenceLevel = readWithAliases(f, "confidenceLevel");
  let sourceType = readWithAliases(f, "sourceType");
  let lastReviewedDate = readDate(f, "lastReviewedDate");

  const legacy = options.fallbackFields || f;

  if (entityType === "operator") {
    if (!confidenceLevel) {
      confidenceLevel = readSelect(legacy, OPERATOR_GOVERNANCE_LEGACY_FALLBACKS.dataConfidenceLevel);
    }
    if (!sourceType) {
      sourceType = readSelect(legacy, OPERATOR_GOVERNANCE_LEGACY_FALLBACKS.sourceType);
    }
  }

  if (entityType === "brand" && !sourceType) {
    const heroSource = readSelect(legacy, BRAND_GOVERNANCE_LEGACY_FALLBACKS.explorerHeroDataSource);
    if (heroSource) sourceType = heroSource;
  }

  return {
    validationStatus: readWithAliases(f, "validationStatus"),
    usagePermission: readWithAliases(f, "usagePermission"),
    sourceType,
    sourceRegion: readWithAliases(f, "sourceRegion"),
    confidenceLevel,
    lastReviewedDate,
    refreshDueDate: readDate(f, "refreshDueDate"),
    companyValidated: readCheckbox(f, "companyValidated"),
    companyValidationDate: readDate(f, "companyValidationDate"),
    externalDisplayStatus: readWithAliases(f, "externalDisplayStatus"),
    evidenceNotes: readSelect(f, MAP_PROFILE_GOVERNANCE_AIRTABLE.evidenceNotes),
    missingDataFlags: readSelect(f, MAP_PROFILE_GOVERNANCE_AIRTABLE.missingDataFlags),
    reviewedBy: readSelect(f, MAP_PROFILE_GOVERNANCE_AIRTABLE.reviewedBy),
    internalNotes: readSelect(f, MAP_PROFILE_GOVERNANCE_AIRTABLE.internalNotes),
    _legacyLastUpdatedDate:
      entityType === "operator"
        ? trimStr(legacy?.[OPERATOR_GOVERNANCE_LEGACY_FALLBACKS.lastUpdatedDate])?.split("T")[0] ||
          null
        : null,
    _legacyHeroVerification:
      entityType === "brand"
        ? readSelect(legacy, BRAND_GOVERNANCE_LEGACY_FALLBACKS.explorerHeroVerification)
        : null,
  };
}

function isCoreGovernanceBlank(raw) {
  return (
    !raw.validationStatus &&
    !raw.usagePermission &&
    !raw.externalDisplayStatus &&
    !raw.sourceType &&
    !raw.sourceRegion &&
    !raw.confidenceLevel &&
    !raw.lastReviewedDate &&
    !raw.companyValidated &&
    !raw.companyValidationDate
  );
}

function formatDisplayDate(iso) {
  if (!iso) return null;
  const d = new Date(`${iso}T12:00:00`);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function regionSubtitle(sourceRegion) {
  if (!sourceRegion) return null;
  if (sourceRegion === "CALA-Specific") return "Region: CALA-specific";
  if (sourceRegion === "Global Reference") return "Region: Global Reference";
  return `Region: ${sourceRegion}`;
}

function resolveExternalSourceBasis(validationStatus) {
  if (!validationStatus) return null;
  switch (validationStatus) {
    case GOVERNANCE_VALIDATION_STATUS.companyPublished:
      return GOVERNANCE_EXTERNAL_SOURCE_BASIS.companyPublished;
    case GOVERNANCE_VALIDATION_STATUS.sourceInformed:
      return GOVERNANCE_EXTERNAL_SOURCE_BASIS.sourceInformed;
    case GOVERNANCE_VALIDATION_STATUS.aiAssisted:
      return GOVERNANCE_EXTERNAL_SOURCE_BASIS.aiAssisted;
    default:
      return null;
  }
}

function resolveExternalDisplayLabel(validationStatus) {
  if (!validationStatus) return null;
  switch (validationStatus) {
    case GOVERNANCE_VALIDATION_STATUS.companyValidated:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyValidated;
    case GOVERNANCE_VALIDATION_STATUS.companyReviewed:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyReviewed;
    case GOVERNANCE_VALIDATION_STATUS.companyPublished:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyPublished;
    case GOVERNANCE_VALIDATION_STATUS.sourceInformed:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.sourceInformed;
    case GOVERNANCE_VALIDATION_STATUS.aiAssisted:
      return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.aiAssisted;
    default:
      return null;
  }
}

function buildDisplaySubtitle(raw, displayLabel, sourceBasis) {
  if (!displayLabel) return null;
  const parts = [];
  if (raw.lastReviewedDate) {
    const fmt = formatDisplayDate(raw.lastReviewedDate);
    if (fmt) parts.push(`Last Reviewed: ${fmt}`);
  }
  if (sourceBasis) parts.push(`Source Basis: ${sourceBasis}`);
  const region = regionSubtitle(raw.sourceRegion);
  if (region) parts.push(region);
  return parts.length ? parts.join(" · ") : null;
}

function externalDisplayAllowsLabel(raw) {
  const status = raw.externalDisplayStatus;
  if (!status || status === GOVERNANCE_EXTERNAL_DISPLAY.hideTrustLabel) return false;
  if (status === GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel) return true;
  return false;
}

function passesExternalGates(raw, warnings) {
  if (isCoreGovernanceBlank(raw)) {
    warnings.push("Governance Not Set");
    return false;
  }

  if (!externalDisplayAllowsLabel(raw)) {
    if (raw.externalDisplayStatus === GOVERNANCE_EXTERNAL_DISPLAY.needsReview) {
      warnings.push("External display status is Needs Review.");
    } else if (raw.externalDisplayStatus === GOVERNANCE_EXTERNAL_DISPLAY.doNotDisplay) {
      warnings.push("External display status is Do Not Display.");
    } else if (
      raw.externalDisplayStatus === GOVERNANCE_EXTERNAL_DISPLAY.internalOnly ||
      raw.externalDisplayStatus === GOVERNANCE_EXTERNAL_DISPLAY.hideTrustLabel
    ) {
      warnings.push("External trust label hidden by display status.");
    } else if (!raw.externalDisplayStatus) {
      warnings.push("Governance Not Set");
    }
    return false;
  }

  if (BLOCKED_USAGE_EXTERNAL.has(raw.usagePermission)) {
    warnings.push(`Usage permission blocks external display (${raw.usagePermission}).`);
    return false;
  }

  if (BLOCKED_VALIDATION_EXTERNAL.has(raw.validationStatus)) {
    warnings.push(`Validation status blocks external display (${raw.validationStatus}).`);
    return false;
  }

  if (!raw.validationStatus) {
    warnings.push("Governance Not Set");
    return false;
  }

  return true;
}

function resolveDisplayLabel(raw, warnings) {
  if (!passesExternalGates(raw, warnings)) return null;

  if (raw.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated) {
    if (!raw.companyValidated || !raw.companyValidationDate) {
      warnings.push(
        "Company Validated status requires Company Validated checkbox and Company Validation Date."
      );
      return null;
    }
    return GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyValidated;
  }

  if (raw.companyValidated && raw.validationStatus !== GOVERNANCE_VALIDATION_STATUS.companyValidated) {
    warnings.push("Company Validated checkbox is set but validation status does not match.");
  }

  if (raw.validationStatus === GOVERNANCE_VALIDATION_STATUS.companyValidated && !raw.companyValidated) {
    warnings.push("Validation status is Company Validated but checkbox is not set.");
    return null;
  }

  const externalLabel = resolveExternalDisplayLabel(raw.validationStatus);
  if (externalLabel) return externalLabel;

  warnings.push(`Validation status not eligible for external display (${raw.validationStatus}).`);
  return null;
}

/**
 * Merge secondary governance (e.g. Presentation/Materials) — more restrictive usage wins.
 * Phase 1: optional; caller may pass secondaryRaw later.
 * @param {ReturnType<typeof extractProfileGovernanceRaw>} primary
 * @param {ReturnType<typeof extractProfileGovernanceRaw>|null|undefined} secondary
 */
export function mergeProfileGovernanceRaw(primary, secondary) {
  if (!secondary || isCoreGovernanceBlank(secondary)) return primary;
  const merged = { ...primary };
  const warnings = [];

  const primaryRank = USAGE_RESTRICTIVENESS[primary.usagePermission] ?? 99;
  const secondaryRank = USAGE_RESTRICTIVENESS[secondary.usagePermission] ?? 99;
  if (secondaryRank < primaryRank) {
    merged.usagePermission = secondary.usagePermission;
    warnings.push("Secondary source has more restrictive usage permission.");
  }

  if (secondary.validationStatus === GOVERNANCE_VALIDATION_STATUS.doNotUse) {
    merged.validationStatus = GOVERNANCE_VALIDATION_STATUS.doNotUse;
    warnings.push("Secondary source marked Do Not Use.");
  }

  if (
    secondary.externalDisplayStatus &&
    secondary.externalDisplayStatus !== GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel
  ) {
    merged.externalDisplayStatus = secondary.externalDisplayStatus;
  }

  merged._mergeWarnings = warnings;
  return merged;
}

/**
 * @param {Record<string, unknown>|null|undefined} fields
 * @param {{ entityType?: 'brand'|'operator', sourceTable?: string, fallbackFields?: Record<string, unknown>, secondaryFields?: Record<string, unknown>, nowDate?: string }} [options]
 */
export function normalizeProfileGovernance(fields, options = {}) {
  const sourceTable = options.sourceTable || null;
  let raw = extractProfileGovernanceRaw(fields, options);

  if (options.secondaryFields) {
    const secondary = extractProfileGovernanceRaw(options.secondaryFields, {
      entityType: options.entityType,
      fallbackFields: options.secondaryFields,
    });
    raw = mergeProfileGovernanceRaw(raw, secondary);
  }

  const internalWarnings = [...(raw._mergeWarnings || [])];

  if (
    options.entityType === "operator" &&
    raw._legacyLastUpdatedDate &&
    !raw.lastReviewedDate
  ) {
    internalWarnings.push(
      `Profile last updated ${formatDisplayDate(raw._legacyLastUpdatedDate) || raw._legacyLastUpdatedDate} — not the same as last reviewed date.`
    );
  }

  const displayLabel = resolveDisplayLabel(raw, internalWarnings);
  const sourceBasis = displayLabel ? resolveExternalSourceBasis(raw.validationStatus) : null;
  const displaySubtitle = buildDisplaySubtitle(raw, displayLabel, sourceBasis);

  return {
    validationStatus: raw.validationStatus,
    usagePermission: raw.usagePermission,
    sourceType: raw.sourceType,
    sourceRegion: raw.sourceRegion,
    confidenceLevel: raw.confidenceLevel,
    sourceBasis,
    lastReviewedDate: raw.lastReviewedDate,
    refreshDueDate: raw.refreshDueDate,
    companyValidated: raw.companyValidated,
    companyValidationDate: raw.companyValidationDate,
    externalDisplayStatus: raw.externalDisplayStatus,
    displayLabel,
    displaySubtitle,
    internalWarnings: [...new Set(internalWarnings.filter(Boolean))],
    sources: {
      canonicalTable: sourceTable,
    },
  };
}
