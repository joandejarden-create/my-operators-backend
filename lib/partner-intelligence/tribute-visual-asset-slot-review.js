/**
 * Tribute Visual Asset Slot Review & Candidate Selection v3.
 *
 * Groups Tribute Brand Asset Registry records by Explorer slot, scores competing
 * candidates, recommends primary/alternate selections for human usage review,
 * and marks superseded weak candidates. Metadata-only — does not download images,
 * approve Explorer use, or write Brand Setup media fields.
 *
 * @see docs/data-intelligence/tribute-visual-asset-slot-review-v3.md
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
  VAL_VISUAL_SLOT_VALIDATION_STATUS,
} from "./brand-explorer-visual-slot-requirements.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export { BRAND_ASSET_PILOT_CONFIG };

export const REVIEW_VERSION = "3";
export const REPORT_JSON_NAME = "tribute-visual-asset-slot-review.json";
export const REPORT_MD_NAME = "tribute-visual-asset-slot-review.md";

const TRIBUTE_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";

/** Selection roles used in scoring and PATCH planning. */
export const SELECTION_ROLE = {
  PRIMARY: "primary",
  ALTERNATE: "alternate",
  SUPERSEDED: "superseded",
  PROTECTED: "protected",
  UNCHANGED: "unchanged",
};

/** Value-driver labels tracked for coverage. */
export const VALUE_DRIVER_LABELS = [
  "Urban",
  "Resort",
  "Conversion / Adaptive Reuse",
  "Boutique / Lifestyle",
  "Mixed-Use",
];

/** Visual slot validation status for superseded weak candidates (schema-safe fallback). */
export const VAL_SUPERSEDED_STATUS = "Not Enough Context";

export const VAL_USAGE_REVIEW_FOR_SELECTION = {
  NEEDS_REVIEW: "Pending Review",
};

const FILES_READ = [
  "AGENTS.md",
  "lib/partner-intelligence/tribute-visual-asset-slot-review.js",
  "lib/partner-intelligence/brand-explorer-visual-slot-requirements.js",
  "lib/partner-intelligence/brand-asset-registry-workflow.js",
  "lib/partner-intelligence/cala-tribute-property-visual-discovery.js",
  "reports/cala-tribute-property-visual-discovery.json",
  "reports/brand-explorer-visual-slot-requirements.json",
  "reports/brand-asset-registry-workflow.json",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function readSlotGovernanceFromFields(f) {
  const g = (key) => nz(f[MAP_VISUAL_SLOT[key]]);
  return {
    explorerSection: g("explorerSection"),
    slotPurpose: g("slotPurpose"),
    relatedValueDriver: g("relatedValueDriver"),
    relatedPropertyName: g("relatedPropertyName"),
    relatedOpeningPr: g("relatedOpeningPr"),
    countryRegion: g("countryRegion"),
    calaRelevant: g("calaRelevant"),
    propertyConfirmed: g("propertyConfirmed"),
    brandConfirmed: g("brandConfirmed"),
    sourcePageConfirmsContext: g("sourcePageConfirmsContext"),
    useCaseMatch: g("useCaseMatch"),
    validationStatus: g("validationStatus"),
    validationNotes: g("validationNotes"),
  };
}

function normalizeRecordForReview(rawRecord) {
  const base = normalizeRegistryAssetRecord(rawRecord);
  const f = rawRecord.fields || {};
  const slotGovernance = readSlotGovernanceFromFields(f);
  const mappedVisualSlot =
    slotGovernance.explorerSection && VISUAL_SLOT_DEFINITIONS.some((d) => d.slot === slotGovernance.explorerSection)
      ? slotGovernance.explorerSection
      : mapRecordToVisualSlot(base);

  return {
    ...base,
    slotGovernance,
    mappedVisualSlot,
    explorerSection: slotGovernance.explorerSection || mappedVisualSlot,
    relatedPropertyName: slotGovernance.relatedPropertyName,
    relatedValueDriver: slotGovernance.relatedValueDriver,
    relatedOpeningPr: slotGovernance.relatedOpeningPr,
    calaRelevant: slotGovernance.calaRelevant,
    propertyConfirmed: slotGovernance.propertyConfirmed,
    brandConfirmed: slotGovernance.brandConfirmed,
    sourcePageConfirmsContext: slotGovernance.sourcePageConfirmsContext,
    useCaseMatch: slotGovernance.useCaseMatch,
    validationStatus: slotGovernance.validationStatus,
    validationNotes: slotGovernance.validationNotes,
    reviewNotes: nz(f[MAP_BRAND_ASSET.reviewNotes]),
    createdTime: rawRecord.createdTime || null,
  };
}

function inferPropertySetting(propertyName) {
  const hay = nz(propertyName).toLowerCase();
  if (/resort|beach|holbox|island|cabos|riviera|caribbean|cove|turtle|nizuc/i.test(hay)) {
    return "Resort";
  }
  if (/lodge|wine|vineyard|auberge|boutique|design|tulum/i.test(hay)) {
    return "Boutique / Lifestyle";
  }
  if (/historic|heritage|ermita|cartagena|colonial|conversion|adaptive/i.test(hay)) {
    return "Conversion / Adaptive Reuse";
  }
  if (/mixed|alameda|mixed-use/i.test(hay)) return "Mixed-Use";
  if (/urban|city|lima|medellin|rumbao|recoleta|tajibos|amazonia|santa cruz/i.test(hay)) {
    return "Urban";
  }
  return "Urban";
}

function extractPropertyFromAssetName(assetName) {
  const m = nz(assetName).match(/^(.+?)\s+—\s+/);
  return m ? m[1].trim() : "";
}

function effectiveValueDriver(record) {
  const property = nz(record.relatedPropertyName) || extractPropertyFromAssetName(record.assetName);
  if (property) return inferPropertySetting(property);
  const fromField = nz(record.relatedValueDriver) || nz(record.useCaseMatch);
  return fromField && fromField !== "None" ? fromField : "Unassigned";
}

function isWeakV1GenericCandidate(record) {
  const name = nz(record.assetName);
  return (
    /property\/design image/i.test(name) ||
    /hero.*consumer property wide/i.test(name) ||
    /existing logo.*unconfirmed/i.test(name)
  );
}

function isV2CalaDiscoveryCandidate(record) {
  const page = nz(record.sourcePageUrl);
  const notes = nz(record.sourceNotes);
  const name = nz(record.assetName);
  return (
    /marriott\.com\/en-us\/hotels\/.+\/photos/i.test(page) ||
    /CALA Tribute discovery v2/i.test(notes) ||
    (/\(gallery\)|\(cover\)|\(exterior\)|\(property\)/i.test(name) &&
      nz(record.relatedPropertyName).length > 0)
  );
}

/** Formal approval only — all three fields must align (see decision writer v5.1). */
export function isApprovedProtectedRecord(record) {
  return isFormallyApprovedRecord(record);
}

function isProtectedRecord(record) {
  if (isApprovedProtectedRecord(record)) return "approved-for-explorer";
  const status = nz(record.assetStatus);
  const name = nz(record.assetName);
  if (status === "Mock/Demo" || /Mock\/Demo hero/i.test(name)) return "mock-demo";
  if (status === "Do Not Use" || /newsroom.*PR/i.test(name)) return "pr-provenance";
  if (/FDD/i.test(name) || status === "Source-Confirmed") return "fdd-reference";
  return null;
}

function isDuplicateCropUrl(url) {
  if (!url) return false;
  return /-\d+x\d+(?=\.\w+$)/.test(url);
}

function normalizeImageBase(url) {
  return nz(url).replace(/-\d+x\d+(?=\.\w+$)/, "").split("?")[0];
}

function scoreCandidateQuality(record) {
  const protectedKind = isProtectedRecord(record);
  if (protectedKind) return { score: -1000, protectedKind, breakdown: {} };

  let score = 0;
  const breakdown = {};

  const add = (key, pts, reason) => {
    score += pts;
    breakdown[key] = { points: pts, reason };
  };

  if (record.propertyConfirmed === "Yes" || nz(record.relatedPropertyName)) {
    add("namedProperty", 12, "Named property confirmed");
  }
  if (record.brandConfirmed === "Yes") add("brandConfirmed", 4, "Brand confirmed");
  if (record.calaRelevant === "Yes") add("calaRelevant", 6, "CALA relevant");
  if (record.sourceBasis === "Marriott-Controlled Source") {
    add("marriottControlled", 10, "Marriott-controlled source");
  }
  if (record.sourcePageConfirmsContext === "Yes") {
    add("sourcePageContext", 5, "Source page confirms image context");
  }
  if (/marriott\.com\/en-us\/hotels\/.+\/photos/i.test(record.sourcePageUrl)) {
    add("photosPage", 6, "Official Marriott /photos/ page source (v2 reliable path)");
  }
  if (isV2CalaDiscoveryCandidate(record)) {
    add("v2Discovery", 8, "CALA property visual discovery v2 candidate");
  }
  if (nz(record.useCaseMatch) && record.useCaseMatch !== "None") {
    const driver = effectiveValueDriver(record);
    if (record.useCaseMatch === driver || record.relatedValueDriver === driver) {
      add("valueDriverMatch", 5, `Value-driver match: ${driver}`);
    }
  } else {
    const driver = effectiveValueDriver(record);
    if (VALUE_DRIVER_LABELS.includes(driver)) {
      add("valueDriverMatch", 5, `Inferred value-driver match: ${driver}`);
    }
  }
  if (nz(record.relatedOpeningPr)) {
    add("openingRef", 8, "Opening/PR reference present");
  }

  if (isWeakV1GenericCandidate(record)) {
    add("weakV1Generic", -20, "Weak v1 generic crop — not named property");
  }
  if (isDuplicateCropUrl(record.sourceUrl) && isWeakV1GenericCandidate(record)) {
    add("duplicateCrop", -8, "Duplicate/generic crop of same source asset");
  }
  if (/tribute-portfolio\.marriott\.com\/wp-content/i.test(record.sourceUrl) && !nz(record.relatedPropertyName)) {
    add("genericBrandSite", -6, "Generic tribute-portfolio.marriott.com asset without named property");
  }
  if (/tribute-black\.svg/i.test(record.assetName)) {
    add("officialLogoSvg", 15, "Official tribute-black.svg logo candidate");
  }

  return { score, protectedKind: null, breakdown };
}

function slotGroupKey(record) {
  const slot = nz(record.recommendedExplorerSlot) || nz(record.explorerSection) || record.mappedVisualSlot;
  if (record.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER) {
    const driver = effectiveValueDriver(record);
    return `value-driver|${driver}`;
  }
  if (record.mappedVisualSlot === VISUAL_SLOT.LOGO) return "logo|Brand Setup — Logo";
  if (record.mappedVisualSlot === VISUAL_SLOT.HERO) return "hero|Brand Setup — Explorer Hero";
  if (record.mappedVisualSlot === VISUAL_SLOT.PR_LINK) return "pr-link|PR / Recent Openings";
  if (record.mappedVisualSlot === VISUAL_SLOT.BRAND_STANDARDS) return "brand-standards|Source Library Reference";
  if (record.mappedVisualSlot === VISUAL_SLOT.RECENT_OPENINGS) return "recent-openings|footprint.openings";
  return `slot|${slot}`;
}

function slotGroupLabel(key) {
  const parts = key.split("|");
  return parts.length > 1 ? parts.slice(1).join("|") : key;
}

function slotGroupVisualSlot(key, records) {
  if (key.startsWith("value-driver|")) return VISUAL_SLOT.VALUE_DRIVER;
  if (key.startsWith("logo|")) return VISUAL_SLOT.LOGO;
  if (key.startsWith("hero|")) return VISUAL_SLOT.HERO;
  if (key.startsWith("pr-link|")) return VISUAL_SLOT.PR_LINK;
  if (key.startsWith("brand-standards|")) return VISUAL_SLOT.BRAND_STANDARDS;
  if (key.startsWith("recent-openings|")) return VISUAL_SLOT.RECENT_OPENINGS;
  return records[0]?.mappedVisualSlot || VISUAL_SLOT.GALLERY;
}

export function groupRecordsBySlot(records) {
  const groups = new Map();
  for (const record of records) {
    const key = slotGroupKey(record);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(record);
  }
  return groups;
}

function buildSelectionForGroup(key, records) {
  const visualSlot = slotGroupVisualSlot(key, records);
  const scored = records.map((record) => {
    const quality = scoreCandidateQuality(record);
    return { record, ...quality };
  });

  const protectedRecords = scored.filter((s) => s.protectedKind);
  const candidates = scored.filter((s) => !s.protectedKind);
  const approvedProtected = protectedRecords.filter((s) => s.protectedKind === "approved-for-explorer");
  const hasApprovedPrimary = approvedProtected.length > 0;

  candidates.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const aV2 = isV2CalaDiscoveryCandidate(a.record) ? 1 : 0;
    const bV2 = isV2CalaDiscoveryCandidate(b.record) ? 1 : 0;
    if (bV2 !== aV2) return bV2 - aV2;
    return String(b.record.createdTime || "").localeCompare(String(a.record.createdTime || ""));
  });

  const selections = [];
  const competing = candidates.length > 1;

  let primary = null;
  let alternates = [];
  let superseded = [];

  if (visualSlot === VISUAL_SLOT.LOGO) {
    const official = candidates.find((c) => /tribute-black\.svg/i.test(c.record.assetName));
    primary = official || (candidates[0]?.score > 0 ? candidates[0] : null);
    const rest = candidates.filter((c) => c !== primary);
    alternates = rest.filter((c) => c.score > -5).slice(0, 1);
    superseded = rest.filter((c) => !alternates.includes(c));
  } else if (visualSlot === VISUAL_SLOT.HERO) {
    primary = candidates.find((c) => c.score > 5 && /Hero Image/i.test(c.record.assetName)) || null;
    alternates = candidates
      .filter((c) => c !== primary && c.score > 0 && !isWeakV1GenericCandidate(c.record))
      .slice(0, 2);
    superseded = candidates.filter(
      (c) => c !== primary && !alternates.includes(c) && (isWeakV1GenericCandidate(c.record) || c.score <= 0)
    );
  } else if (visualSlot === VISUAL_SLOT.RECENT_OPENINGS) {
    primary = candidates.find((c) => nz(c.record.relatedOpeningPr) && c.score > 10) || null;
    alternates = [];
    superseded = candidates.filter((c) => c !== primary);
  } else {
    primary = candidates[0]?.score > 0 ? candidates[0] : null;
    alternates = candidates
      .filter((c) => c !== primary && c.score > 0)
      .slice(0, 2);
    superseded = candidates.filter((c) => c !== primary && !alternates.includes(c));
  }

  if (hasApprovedPrimary && primary) {
    if (!alternates.some((a) => a.record.id === primary.record.id)) {
      alternates = [primary, ...alternates].slice(0, 2);
    }
    primary = null;
  }

  for (const p of protectedRecords) {
    selections.push({
      recordId: p.record.id,
      assetName: p.record.assetName,
      role: SELECTION_ROLE.PROTECTED,
      protectedKind: p.protectedKind,
      qualityScore: p.score,
      qualityBreakdown: p.breakdown,
      rationale: protectedRationale(p.record, p.protectedKind),
      proposedFields: buildProtectedFields(p.record, p.protectedKind),
    });
  }

  if (primary) {
    selections.push({
      recordId: primary.record.id,
      assetName: primary.record.assetName,
      role: SELECTION_ROLE.PRIMARY,
      qualityScore: primary.score,
      qualityBreakdown: primary.breakdown,
      rationale: primaryRationale(primary.record, visualSlot, competing),
      proposedFields: buildPrimaryFields(primary.record, visualSlot, competing),
    });
  }

  for (const alt of alternates) {
    selections.push({
      recordId: alt.record.id,
      assetName: alt.record.assetName,
      role: SELECTION_ROLE.ALTERNATE,
      qualityScore: alt.score,
      qualityBreakdown: alt.breakdown,
      rationale: alternateRationale(alt.record, primary?.record.assetName),
      proposedFields: buildAlternateFields(alt.record, primary?.record.assetName),
    });
  }

  for (const sup of superseded) {
    selections.push({
      recordId: sup.record.id,
      assetName: sup.record.assetName,
      role: SELECTION_ROLE.SUPERSEDED,
      qualityScore: sup.score,
      qualityBreakdown: sup.breakdown,
      rationale: supersededRationale(sup.record, primary?.record.assetName),
      proposedFields: buildSupersededFields(sup.record, primary?.record.assetName),
    });
  }

  return {
    groupKey: key,
    explorerSection: slotGroupLabel(key),
    visualSlot,
    recordCount: records.length,
    competingCandidates: competing ? candidates.map((c) => ({
      recordId: c.record.id,
      assetName: c.record.assetName,
      qualityScore: c.score,
      recommendedExplorerSlot: c.record.recommendedExplorerSlot,
      relatedPropertyName: c.record.relatedPropertyName || null,
      sourcePageUrl: c.record.sourcePageUrl || null,
      isV2Discovery: isV2CalaDiscoveryCandidate(c.record),
      isWeakV1: isWeakV1GenericCandidate(c.record),
    })) : [],
    primary: hasApprovedPrimary
      ? {
          recordId: approvedProtected[0].record.id,
          assetName: approvedProtected[0].record.assetName,
          qualityScore: approvedProtected[0].score,
          approvedLocked: true,
        }
      : primary
        ? {
            recordId: primary.record.id,
            assetName: primary.record.assetName,
            qualityScore: primary.score,
          }
        : null,
    alternates: alternates.map((a) => ({
      recordId: a.record.id,
      assetName: a.record.assetName,
      qualityScore: a.score,
    })),
    superseded: superseded.map((s) => ({
      recordId: s.record.id,
      assetName: s.record.assetName,
      qualityScore: s.score,
    })),
    selections,
    readyForHumanReview: Boolean(
      (hasApprovedPrimary && approvedProtected[0].score > -1000) || (primary && primary.score > 0)
    ),
    missing: !hasApprovedPrimary && (!primary || primary.score <= 0),
  };
}

function protectedRationale(record, kind) {
  if (kind === "approved-for-explorer") {
    return "Formally approved for Explorer use after human review — locked; slot-review v3 does not modify approved records.";
  }
  if (kind === "mock-demo") return "Mock/Demo hero guard — remain Do Not Use; never replace or promote.";
  if (kind === "pr-provenance") return "Newsroom PR placeholder — provenance only until Rendered Source Capture v1.";
  if (kind === "fdd-reference") return "FDD source reference — internal only; not a visual Explorer asset.";
  return "Protected record — selection writer does not promote.";
}

function buildProtectedFields(record, kind) {
  if (kind === "approved-for-explorer") {
    return {};
  }
  const fields = {};
  if (kind === "mock-demo") {
    fields[MAP_BRAND_ASSET.isPrimaryCandidate] = false;
    fields[MAP_BRAND_ASSET.explorerUsePermission] = "Do Not Use";
    fields[MAP_BRAND_ASSET.usageReviewStatus] = "Blocked";
    fields[MAP_VISUAL_SLOT.validationStatus] = "Mock/Demo Guard";
    fields[MAP_VISUAL_SLOT.validationNotes] =
      "Existing demo hero should not be promoted or treated as approved asset.";
  } else if (kind === "pr-provenance") {
    fields[MAP_BRAND_ASSET.isPrimaryCandidate] = false;
    fields[MAP_BRAND_ASSET.explorerUsePermission] = "Do Not Use";
    fields[MAP_BRAND_ASSET.usageReviewStatus] = "Blocked";
    fields[MAP_VISUAL_SLOT.validationStatus] = "Provenance Only";
    fields[MAP_VISUAL_SLOT.validationNotes] =
      "JS-shell/static extraction gap; do not use until Rendered Source Capture v1.";
  } else if (kind === "fdd-reference") {
    fields[MAP_BRAND_ASSET.isPrimaryCandidate] = true;
    fields[MAP_BRAND_ASSET.explorerUsePermission] = "Internal Only";
    fields[MAP_BRAND_ASSET.usageReviewStatus] = "Usage Review Complete";
    fields[MAP_VISUAL_SLOT.validationStatus] = "Source Reference Only";
    fields[MAP_VISUAL_SLOT.validationNotes] =
      "Valid as internal/source-backed reference; not a visual image asset.";
  }
  return fields;
}

function primaryRationale(record, visualSlot, competing) {
  const parts = ["Recommended primary candidate for human usage review."];
  if (isV2CalaDiscoveryCandidate(record)) {
    parts.push("CALA named-property candidate from official Marriott /photos/ source (v2 discovery).");
  }
  if (nz(record.relatedPropertyName)) parts.push(`Property: ${record.relatedPropertyName}.`);
  if (competing) parts.push("Selected over competing candidates in this slot by quality score.");
  if (visualSlot === VISUAL_SLOT.HERO) {
    parts.push("Not approved for Explorer — usage review required before any hero promotion.");
  }
  return parts.join(" ");
}

function buildPrimaryFields(record, visualSlot) {
  const notes = primaryRationale(record, visualSlot, true);
  const currentStatus = nz(record.assetStatus);
  const assetStatus =
    currentStatus === "Needs Usage Review" || !currentStatus
      ? "Candidate"
      : currentStatus === "Approved For Explorer Use"
        ? "Candidate"
        : currentStatus;
  return {
    [MAP_BRAND_ASSET.isPrimaryCandidate]: true,
    [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
    [MAP_BRAND_ASSET.usageReviewStatus]: VAL_USAGE_REVIEW_FOR_SELECTION.NEEDS_REVIEW,
    [MAP_BRAND_ASSET.assetStatus]: assetStatus,
    [MAP_VISUAL_SLOT.validationStatus]: "Needs Usage Review",
    [MAP_VISUAL_SLOT.validationNotes]: notes,
    [MAP_BRAND_ASSET.reviewNotes]:
      "Slot review v3 — primary candidate for human usage review; not approved for Explorer.",
  };
}

function alternateRationale(record, primaryName) {
  const parts = ["Alternate candidate — retain for human review if primary is not approved."];
  if (nz(record.relatedPropertyName)) parts.push(`Property: ${record.relatedPropertyName}.`);
  if (primaryName) parts.push(`Primary recommendation: ${primaryName}.`);
  return parts.join(" ");
}

function buildAlternateFields(record, primaryName) {
  return {
    [MAP_BRAND_ASSET.isPrimaryCandidate]: false,
    [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
    [MAP_BRAND_ASSET.usageReviewStatus]: VAL_USAGE_REVIEW_FOR_SELECTION.NEEDS_REVIEW,
    [MAP_VISUAL_SLOT.validationStatus]: "Needs Usage Review",
    [MAP_VISUAL_SLOT.validationNotes]: alternateRationale(record, primaryName),
    [MAP_BRAND_ASSET.reviewNotes]:
      "Slot review v3 — alternate candidate; not approved for Explorer.",
  };
}

function supersededRationale(record, primaryName) {
  const parts = ["Superseded candidate — not selected for this slot."];
  if (isWeakV1GenericCandidate(record)) {
    parts.push("Weak v1 generic crop without named property confirmation.");
  }
  if (isV2CalaDiscoveryCandidate(record) === false && /marriott\.com/.test(record.sourcePageUrl)) {
    parts.push("Older candidate superseded by higher-quality CALA property discovery.");
  }
  if (primaryName) parts.push(`Primary recommendation: ${primaryName}.`);
  parts.push("Record retained in registry — not deleted.");
  return parts.join(" ");
}

function buildSupersededFields(record, primaryName) {
  return {
    [MAP_BRAND_ASSET.isPrimaryCandidate]: false,
    [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
    [MAP_BRAND_ASSET.usageReviewStatus]: VAL_USAGE_REVIEW_FOR_SELECTION.NEEDS_REVIEW,
    [MAP_VISUAL_SLOT.validationStatus]: VAL_SUPERSEDED_STATUS,
    [MAP_VISUAL_SLOT.validationNotes]: `${supersededRationale(record, primaryName)} Visual slot status: Not Selected / Superseded Candidate.`,
    [MAP_BRAND_ASSET.reviewNotes]:
      "Slot review v3 — superseded candidate; retained for audit trail.",
  };
}

export function validateSelectionPayload(fields) {
  const errors = [];
  if (fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer") {
    errors.push("Explorer Use Permission Approved For Explorer is not allowed");
  }
  if (fields[MAP_BRAND_ASSET.assetStatus] === "Approved For Explorer Use") {
    errors.push("Asset Status Approved For Explorer Use is not allowed");
  }
  if (fields[MAP_BRAND_ASSET.companyValidated]) {
    errors.push("Company Validated must not be set");
  }
  if (fields[MAP_BRAND_ASSET.companyValidationDate]) {
    errors.push("Company Validation Date must not be set");
  }
  if (fields[MAP_BRAND_ASSET.attachment]) {
    errors.push("Attachment must not be set");
  }
  const status = fields[MAP_VISUAL_SLOT.validationStatus];
  if (status && !VAL_VISUAL_SLOT_VALIDATION_STATUS.includes(status)) {
    errors.push(`Invalid Visual Slot Validation Status: ${status}`);
  }
  return { valid: errors.length === 0, errors };
}

function fieldValuesEqual(current, proposed) {
  const cur = current == null ? "" : String(current).trim();
  const prop = proposed == null ? "" : String(proposed).trim();
  if (typeof current === "boolean" || typeof proposed === "boolean") {
    return Boolean(current) === Boolean(proposed);
  }
  return cur === prop;
}

function selectionNeedsUpdate(rawRecord, proposedFields) {
  const f = rawRecord.fields || {};
  return Object.entries(proposedFields).some(([key, value]) => !fieldValuesEqual(f[key], value));
}

function getRegistryTableName() {
  return process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID || BRAND_ASSET_REGISTRY_TABLE;
}

function registryDataUrl(baseId, recordId) {
  const table = encodeURIComponent(getRegistryTableName());
  if (recordId) {
    return `https://api.airtable.com/v0/${baseId}/${table}/${encodeURIComponent(recordId)}`;
  }
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

function assessValueDriverCoverage(slotGroups) {
  const coverage = {};
  for (const driver of VALUE_DRIVER_LABELS) {
    coverage[driver] = { status: "Missing", primary: null, alternates: [] };
  }
  for (const group of slotGroups) {
    if (group.visualSlot !== VISUAL_SLOT.VALUE_DRIVER) continue;
    const driver = group.explorerSection;
    if (!VALUE_DRIVER_LABELS.includes(driver)) continue;
    if (group.primary && group.primary.qualityScore > 0) {
      coverage[driver] = {
        status: "Ready for human usage review",
        primary: group.primary,
        alternates: group.alternates,
      };
    } else if (group.alternates.length) {
      coverage[driver] = {
        status: "Needs review — weak primary",
        primary: null,
        alternates: group.alternates,
      };
    }
  }
  return coverage;
}

function assessRecentOpeningsStatus(records, slotGroups) {
  const openingGroup = slotGroups.find((g) => g.visualSlot === VISUAL_SLOT.RECENT_OPENINGS);
  const hasOpeningEvidence = records.some((r) => nz(r.relatedOpeningPr));
  if (openingGroup?.primary && openingGroup.primary.qualityScore > 10) {
    return { status: "Ready for human usage review", note: "Opening candidate with property + PR reference." };
  }
  return {
    status: "Missing",
    note: hasOpeningEvidence
      ? "Opening reference incomplete — requires property name + opening/PR date + source URL."
      : "No property + opening/PR/date candidate in registry.",
  };
}

export async function applySlotSelections(rawRecords, allSelections, { apply = false } = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const proposed = [];
  const skipped = [];
  const validationErrors = [];

  for (const sel of allSelections) {
    const raw = rawRecords.find((r) => r.id === sel.recordId);
    if (!raw) continue;

    if (
      sel.role === SELECTION_ROLE.PROTECTED &&
      sel.protectedKind === "approved-for-explorer"
    ) {
      skipped.push({
        recordId: sel.recordId,
        assetName: sel.assetName,
        reason: "approved record locked — no updates",
      });
      continue;
    }

    if (!sel.proposedFields || Object.keys(sel.proposedFields).length === 0) {
      skipped.push({
        recordId: sel.recordId,
        assetName: sel.assetName,
        reason: "protected record — no proposed field changes",
      });
      continue;
    }

    const validation = validateSelectionPayload(sel.proposedFields);
    if (!validation.valid) {
      validationErrors.push({ assetName: sel.assetName, errors: validation.errors });
      continue;
    }
    if (!selectionNeedsUpdate(raw, sel.proposedFields)) {
      skipped.push({ recordId: sel.recordId, assetName: sel.assetName, reason: "already matches proposed selection" });
      continue;
    }
    proposed.push({
      recordId: sel.recordId,
      assetName: sel.assetName,
      role: sel.role,
      fields: sel.proposedFields,
    });
  }

  let updated = [];
  if (apply && proposed.length) {
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
        }))
      );
    }
  }

  return {
    recordsScanned: allSelections.length,
    proposed,
    skipped,
    updated: apply ? updated : [],
    validationErrors,
  };
}

export async function buildTributeVisualSlotReviewReport({
  brandRecordId = TRIBUTE_BRAND_RECORD_ID,
  apply = false,
  selectionApproved = false,
} = {}) {
  const pilot = BRAND_ASSET_PILOT_CONFIG["tribute-portfolio"];
  const generatedAt = new Date().toISOString();
  const mode = apply && selectionApproved ? "selection-apply" : "dry-run";

  let rawRecords = [];
  let registryReadError = null;
  try {
    rawRecords = await listRegistryRecordsRaw(brandRecordId);
  } catch (err) {
    registryReadError = err.message || String(err);
  }

  const records = rawRecords.map(normalizeRecordForReview);
  const groupsMap = groupRecordsBySlot(records);
  const slotGroups = [...groupsMap.entries()].map(([key, recs]) => buildSelectionForGroup(key, recs));

  const allSelections = slotGroups.flatMap((g) => g.selections);

  let selectionWriter = {
    recordsScanned: 0,
    proposed: [],
    skipped: [],
    updated: [],
    validationErrors: [],
  };
  if (!registryReadError) {
    selectionWriter = await applySlotSelections(rawRecords, allSelections, {
      apply: apply && selectionApproved,
    });
  }

  const recordsBySlot = {};
  for (const group of slotGroups) {
    const label = group.visualSlot;
    if (!recordsBySlot[label]) recordsBySlot[label] = [];
    recordsBySlot[label].push(...group.competingCandidates.map((c) => c.assetName));
    if (group.primary) {
      if (!recordsBySlot[label].includes(group.primary.assetName)) {
        recordsBySlot[label].push(group.primary.assetName);
      }
    }
  }

  const recommendedPrimary = slotGroups
    .filter((g) => g.primary)
    .map((g) => ({
      recordId: g.primary.recordId,
      assetName: g.primary.assetName,
      qualityScore: g.primary.qualityScore,
      approvedLocked: Boolean(g.primary.approvedLocked),
      rationale: g.primary.approvedLocked
        ? "Formally approved and protected; retained as primary slot coverage."
        : "Recommended primary candidate for human usage review.",
      visualSlot: g.visualSlot,
      explorerSection: g.explorerSection,
    }));

  const recommendedAlternates = allSelections
    .filter((s) => s.role === SELECTION_ROLE.ALTERNATE)
    .map((s) => ({
      recordId: s.recordId,
      assetName: s.assetName,
      qualityScore: s.qualityScore,
      rationale: s.rationale,
    }));

  const recommendedSuperseded = allSelections
    .filter((s) => s.role === SELECTION_ROLE.SUPERSEDED)
    .map((s) => ({
      recordId: s.recordId,
      assetName: s.assetName,
      qualityScore: s.qualityScore,
      rationale: s.rationale,
    }));

  const protectedApprovedRecords = allSelections
    .filter((s) => s.role === SELECTION_ROLE.PROTECTED && s.protectedKind === "approved-for-explorer")
    .map((s) => ({
      recordId: s.recordId,
      assetName: s.assetName,
      explorerUsePermission: records.find((r) => r.id === s.recordId)?.explorerUsePermission || null,
      assetStatus: records.find((r) => r.id === s.recordId)?.assetStatus || null,
      usageReviewStatus: records.find((r) => r.id === s.recordId)?.usageReviewStatus || null,
      rationale: s.rationale,
    }));

  const pendingPrimaryCandidates = recommendedPrimary.filter((p) => !p.approvedLocked);
  const approvedCoverageBySlot = {};
  for (const p of recommendedPrimary.filter((x) => x.approvedLocked)) {
    if (!approvedCoverageBySlot[p.visualSlot]) approvedCoverageBySlot[p.visualSlot] = [];
    approvedCoverageBySlot[p.visualSlot].push(p.assetName);
  }

  const slotsReadyForHumanReview = slotGroups
    .filter((g) => g.readyForHumanReview && !g.missing)
    .map((g) => ({
      visualSlot: g.visualSlot,
      explorerSection: g.explorerSection,
      primary: g.primary,
      alternates: g.alternates,
    }));

  const missingSlots = [];
  const coveredVisualSlots = new Set(slotGroups.filter((g) => g.primary).map((g) => g.visualSlot));
  const hasProtectedBrandStandards = allSelections.some(
    (s) => s.role === SELECTION_ROLE.PROTECTED && s.protectedKind === "fdd-reference"
  );
  const hasProtectedPrLink = allSelections.some(
    (s) => s.role === SELECTION_ROLE.PROTECTED && s.protectedKind === "pr-provenance"
  );

  for (const def of VISUAL_SLOT_DEFINITIONS) {
    if (def.slot === VISUAL_SLOT.RECENT_OPENINGS) {
      missingSlots.push({
        slot: def.slot,
        explorerSection: def.explorerSection,
        reason: "No property + opening/PR/date candidate in registry.",
      });
      continue;
    }
    if (def.slot === VISUAL_SLOT.VALUE_DRIVER) continue;
    if (def.slot === VISUAL_SLOT.BRAND_STANDARDS && hasProtectedBrandStandards) continue;
    if (def.slot === VISUAL_SLOT.PR_LINK && hasProtectedPrLink) {
      missingSlots.push({
        slot: def.slot,
        explorerSection: def.explorerSection,
        reason: "Provenance-only PR placeholder exists — not usable until Rendered Source Capture v1.",
      });
      continue;
    }
    if (!coveredVisualSlots.has(def.slot)) {
      missingSlots.push({
        slot: def.slot,
        explorerSection: def.explorerSection,
        reason: "No primary candidate selected for this visual slot.",
      });
    }
  }

  const valueDriverCoverage = assessValueDriverCoverage(slotGroups);
  for (const driver of VALUE_DRIVER_LABELS) {
    if (valueDriverCoverage[driver].status === "Missing") {
      missingSlots.push({
        slot: VISUAL_SLOT.VALUE_DRIVER,
        explorerSection: driver,
        reason: `No matching property/use-case image for ${driver} value driver.`,
      });
    }
  }

  const recentOpeningsStatus = assessRecentOpeningsStatus(records, slotGroups);

  const competingBySlot = slotGroups
    .filter((g) => g.competingCandidates.length > 0)
    .map((g) => ({
      visualSlot: g.visualSlot,
      explorerSection: g.explorerSection,
      candidateCount: g.competingCandidates.length,
      candidates: g.competingCandidates,
      primary: g.primary,
    }));

  return {
    reviewVersion: REVIEW_VERSION,
    generatedAt,
    mode,
    airtableModified: Boolean(apply && selectionApproved && selectionWriter.updated.length),
    approvedRecordsUntouched: !Boolean(apply && selectionApproved && selectionWriter.updated.length),
    brandSetupMediaUntouched: true,
    brand: {
      key: "tribute-portfolio",
      recordId: brandRecordId,
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
    recordsBySlot,
    competingBySlot,
    recommendedPrimary,
    recommendedAlternates,
    recommendedSuperseded,
    protectedApprovedRecords,
    approvedCoverageBySlot,
    pendingPrimaryCandidates,
    slotsReadyForHumanReview,
    missingSlots,
    recentOpeningsStatus,
    valueDriverCoverage,
    slotGroups,
    selectionWriter,
    readyForDownloadOrAttachment: protectedApprovedRecords.length > 0,
    applyCommand:
      "npm run tribute-visual-asset-slot-review -- --apply --approve-tribute-visual-slot-selection",
    nextCommand: "npm run tribute-visual-asset-slot-review -- --dry-run",
    remainingWorkToVisualParity: [
      "Human usage review on primary CALA property candidates (not auto-approved).",
      "Capture specific opening/PR with property name + date for Recent Openings.",
      "Complete value-driver imagery for Urban, Conversion, and Mixed-Use where still Missing.",
      "Resolve 4 seed properties missing overview URLs (Alameda, Ponce, Merida, Tulum).",
      "Rendered Source Capture v1 for Marriott newsroom PR (JS-shell pages).",
      "Future governed hero/logo promotion writer after usage review approval.",
      "Do not replace Mock/Demo hero until governed CALA hero is approved.",
    ],
  };
}

export function buildTributeVisualSlotReviewMarkdown(report) {
  const lines = [];
  lines.push(`# Tribute Visual Asset Slot Review & Candidate Selection v3`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push(`Text/governance Platform Ready: **${report.textGovernanceStatus.textGovernancePlatformReady ? "yes" : "no"}**`);
  lines.push(`Brand Setup media untouched: **${report.brandSetupMediaUntouched ? "yes" : "no"}**`);
  lines.push("");

  lines.push("## 1. Summary");
  lines.push("");
  lines.push(`- Total Tribute registry records scanned: **${report.totalRecordsScanned}**`);
  lines.push(`- Protected approved records: **${report.protectedApprovedRecords?.length || 0}**`);
  lines.push(`- Recommended primary candidates: **${report.recommendedPrimary.length}**`);
  lines.push(`- Pending primary candidates: **${report.pendingPrimaryCandidates?.length || 0}**`);
  lines.push(`- Recommended alternates: **${report.recommendedAlternates.length}**`);
  lines.push(`- Superseded / not selected: **${report.recommendedSuperseded.length}**`);
  lines.push(`- Slots ready for human usage review: **${report.slotsReadyForHumanReview.length}**`);
  lines.push(`- Slots still Missing: **${report.missingSlots.length}**`);
  lines.push(`- Approved records untouched: **${report.approvedRecordsUntouched ? "yes" : "no"}**`);
  lines.push(
    `- Ready for download/attachment: **${report.readyForDownloadOrAttachment ? "yes" : "no"}**`
  );
  lines.push("");

  if (report.registryReadError) {
    lines.push(`> Registry read error: ${report.registryReadError}`);
    lines.push("");
  }

  lines.push("## 2. Records by slot");
  lines.push("");
  for (const [slot, names] of Object.entries(report.recordsBySlot)) {
    lines.push(`### ${slot}`);
    for (const name of names) lines.push(`- ${name}`);
    lines.push("");
  }

  lines.push("## 3. Competing candidates by slot");
  lines.push("");
  if (!report.competingBySlot.length) {
    lines.push("No competing candidates detected.");
  } else {
    for (const group of report.competingBySlot) {
      lines.push(`### ${group.visualSlot} — ${group.explorerSection}`);
      lines.push(`**${group.candidateCount}** competing candidates`);
      lines.push("");
      lines.push("| Asset | Score | Property | v2? |");
      lines.push("|-------|-------|----------|-----|");
      for (const c of group.candidates) {
        lines.push(
          `| ${c.assetName} | ${c.qualityScore} | ${c.relatedPropertyName || "—"} | ${c.isV2Discovery ? "yes" : "no"} |`
        );
      }
      if (group.primary) {
        lines.push("");
        lines.push(`**Primary recommendation:** ${group.primary.assetName} (score ${group.primary.qualityScore})`);
      }
      lines.push("");
    }
  }

  lines.push("## 4. Protected approved records");
  lines.push("");
  if (!report.protectedApprovedRecords?.length) {
    lines.push("None.");
  } else {
    for (const p of report.protectedApprovedRecords) {
      lines.push(
        `- **${p.assetName}** (\`${p.recordId}\`) — ${p.assetStatus || "—"} / ${p.explorerUsePermission || "—"}`
      );
      lines.push(`  - ${p.rationale}`);
    }
  }
  lines.push("");

  lines.push("## 5. Recommended primary candidates");
  lines.push("");
  for (const p of report.recommendedPrimary) {
    lines.push(`- **${p.assetName}** (score ${p.qualityScore}) — ${p.rationale}`);
  }
  lines.push("");

  lines.push("## 6. Approved coverage by slot");
  lines.push("");
  if (!Object.keys(report.approvedCoverageBySlot || {}).length) {
    lines.push("No formally approved slot coverage.");
  } else {
    for (const [slot, names] of Object.entries(report.approvedCoverageBySlot)) {
      lines.push(`- **${slot}**: ${names.join("; ")}`);
    }
  }
  lines.push("");

  lines.push("## 7. Pending primary candidates");
  lines.push("");
  if (!report.pendingPrimaryCandidates?.length) {
    lines.push("None.");
  } else {
    for (const p of report.pendingPrimaryCandidates) {
      lines.push(`- \`${p.recordId}\` — ${p.assetName} (${p.visualSlot} / ${p.explorerSection})`);
    }
  }
  lines.push("");

  lines.push("## 8. Recommended alternates");
  lines.push("");
  if (!report.recommendedAlternates.length) lines.push("None.");
  for (const a of report.recommendedAlternates) {
    lines.push(`- **${a.assetName}** (score ${a.qualityScore}) — ${a.rationale}`);
  }
  lines.push("");

  lines.push("## 9. Superseded / not selected");
  lines.push("");
  if (!report.recommendedSuperseded.length) lines.push("None.");
  for (const s of report.recommendedSuperseded) {
    lines.push(`- **${s.assetName}** (score ${s.qualityScore}) — ${s.rationale}`);
  }
  lines.push("");

  lines.push("## 10. Slots ready for human usage review");
  lines.push("");
  for (const slot of report.slotsReadyForHumanReview) {
    lines.push(`- **${slot.visualSlot}** (${slot.explorerSection})`);
    if (slot.primary) lines.push(`  - Primary: ${slot.primary.assetName}`);
    if (slot.alternates?.length) {
      lines.push(`  - Alternates: ${slot.alternates.map((a) => a.assetName).join("; ")}`);
    }
  }
  lines.push("");

  lines.push("## 11. Slots still Missing");
  lines.push("");
  for (const m of report.missingSlots) {
    lines.push(`- **${m.slot}** (${m.explorerSection}) — ${m.reason}`);
  }
  lines.push("");

  lines.push("## 12. Recent Openings status");
  lines.push("");
  lines.push(`- Status: **${report.recentOpeningsStatus.status}**`);
  lines.push(`- ${report.recentOpeningsStatus.note}`);
  lines.push("");

  lines.push("## 13. Value-driver coverage");
  lines.push("");
  lines.push("| Value driver | Status | Primary |");
  lines.push("|--------------|--------|---------|");
  for (const [driver, cov] of Object.entries(report.valueDriverCoverage)) {
    lines.push(
      `| ${driver} | ${cov.status} | ${cov.primary?.assetName || "—"} |`
    );
  }
  lines.push("");

  lines.push("## 14. Selection writer");
  lines.push("");
  const sw = report.selectionWriter;
  lines.push(`- Records scanned: **${sw.recordsScanned}**`);
  lines.push(`- Proposed updates: **${sw.proposed.length}**`);
  lines.push(`- Skipped (already matches / locked): **${sw.skipped.length}**`);
  lines.push(`- Updated: **${sw.updated.length}**`);
  if (sw.validationErrors.length) {
    lines.push(`- Validation errors: **${sw.validationErrors.length}**`);
  } else {
    lines.push(`- Validation errors: **0**`);
  }
  lines.push("");

  lines.push("## 15. Apply command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.applyCommand);
  lines.push("```");
  lines.push("");

  lines.push("## 16. Remaining work before visual parity");
  lines.push("");
  for (const item of report.remainingWorkToVisualParity) {
    lines.push(`- ${item}`);
  }
  lines.push("");

  return lines.join("\n");
}
