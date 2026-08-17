/**
 * Brand Asset Human Review Readiness v4.
 *
 * Evaluates whether primary Brand Asset Registry candidates have sufficient
 * metadata and source context for human usage review. Report-only — does not
 * approve assets, download images, or write Brand Setup media fields.
 *
 * @see docs/data-intelligence/brand-asset-human-review-readiness-v4.md
 */
import {
  BRAND_ASSET_PILOT_CONFIG,
  MAP_BRAND_ASSET,
  normalizeRegistryAssetRecord,
} from "./brand-asset-registry-workflow.js";
import {
  VISUAL_SLOT,
  VISUAL_SLOT_DEFINITIONS,
  MAP_VISUAL_SLOT,
  mapRecordToVisualSlot,
  listRegistryRecordsRaw,
} from "./brand-explorer-visual-slot-requirements.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export { BRAND_ASSET_PILOT_CONFIG };

export const READINESS_VERSION = "4";
export const REPORT_JSON_NAME = "brand-asset-human-review-readiness.json";
export const REPORT_MD_NAME = "brand-asset-human-review-readiness.md";

const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";

export const READINESS_OUTCOME = {
  APPROVED: "Approved For Explorer",
  READY: "Ready For Human Review",
  NEEDS_METADATA: "Needs More Metadata",
  NEEDS_SOURCE: "Needs Source Review",
  NEEDS_VISUAL: "Needs Visual Inspection",
  NOT_READY: "Not Ready",
  MISSING: "Missing",
};

export const COMPANY_CONTROLLED_BASES = new Set([
  "Company Materials",
  "Marriott-Controlled Source",
  "Rendered Official Source",
  "Local Reference Material",
]);

export const RECOMMENDED_AIRTABLE_REVIEW_FIELDS = [
  { field: "Usage Review Status", note: "Set to Usage Review Complete only after human approves or rejects." },
  { field: "Review Notes", note: "Capture reviewer decision, concerns, and alternate preference." },
  { field: "Reviewed By", note: "Name/email of human reviewer." },
  { field: "Last Reviewed Date", note: "Date of human review." },
  { field: "Explorer Use Permission", note: "Keep Candidate Only until governed promotion writer; never auto-approve." },
  { field: "Visual Slot Validation Status", note: "Update to Valid for Slot only after human confirms slot fit." },
  { field: "Visual Slot Validation Notes", note: "Document what human verified (property name, CALA fit, value driver)." },
  { field: "Company Validated", note: "Do not set unless Marriott/brand explicitly confirms rights — not implied by this module." },
  { field: "Company Validation Date", note: "Do not set unless company validation explicitly recorded." },
];

const FILES_READ = [
  "AGENTS.md",
  "lib/partner-intelligence/brand-asset-human-review-readiness.js",
  "lib/partner-intelligence/tribute-visual-asset-slot-review.js",
  "lib/partner-intelligence/brand-explorer-visual-slot-requirements.js",
  "lib/partner-intelligence/brand-asset-registry-workflow.js",
  "reports/tribute-visual-asset-slot-review.json",
  "reports/brand-explorer-visual-slot-requirements.json",
  "reports/brand-asset-registry-workflow.json",
  "docs/data-intelligence/tribute-visual-asset-slot-review-v3.md",
];

const VAL_USAGE_OK = new Set(["Pending Review", "Needs Review", "Not Reviewed"]);

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

function extractPropertyFromAssetName(assetName) {
  const m = nz(assetName).match(/^(.+?)\s+—\s+/);
  return m ? m[1].trim() : "";
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

function effectiveValueDriver(record) {
  const property = nz(record.relatedPropertyName) || extractPropertyFromAssetName(record.assetName);
  if (property) return inferPropertySetting(property);
  const fromField = nz(record.relatedValueDriver) || nz(record.useCaseMatch);
  return fromField && fromField !== "None" ? fromField : "";
}

function normalizeRecordForReadiness(rawRecord) {
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
    relatedPropertyName: slotGovernance.relatedPropertyName,
    relatedValueDriver: slotGovernance.relatedValueDriver,
    relatedOpeningPr: slotGovernance.relatedOpeningPr,
    countryRegion: slotGovernance.countryRegion,
    calaRelevant: slotGovernance.calaRelevant,
    propertyConfirmed: slotGovernance.propertyConfirmed,
    brandConfirmed: slotGovernance.brandConfirmed,
    sourcePageConfirmsContext: slotGovernance.sourcePageConfirmsContext,
    useCaseMatch: slotGovernance.useCaseMatch,
    validationStatus: slotGovernance.validationStatus,
    validationNotes: slotGovernance.validationNotes,
    companyValidated: Boolean(f[MAP_BRAND_ASSET.companyValidated]),
    companyValidationDate: nz(f[MAP_BRAND_ASSET.companyValidationDate]),
    reviewNotes: nz(f[MAP_BRAND_ASSET.reviewNotes]),
    reviewedBy: nz(f[MAP_BRAND_ASSET.reviewedBy]),
    lastReviewedDate: nz(f[MAP_BRAND_ASSET.lastReviewedDate]),
    sourceNotes: nz(f[MAP_BRAND_ASSET.sourceNotes]),
  };
}

function needsPropertyContext(slot) {
  return [VISUAL_SLOT.HERO, VISUAL_SLOT.GALLERY, VISUAL_SLOT.VALUE_DRIVER, VISUAL_SLOT.RECENT_OPENINGS].includes(
    slot
  );
}

function checkPresent(label, value, checks) {
  const ok = Boolean(nz(value));
  checks.push({ check: label, pass: ok, value: ok ? nz(value) : null, note: ok ? "" : `Missing: ${label}` });
  return ok;
}

function checkEquals(label, value, expected, checks) {
  const ok = nz(value) === expected;
  checks.push({
    check: label,
    pass: ok,
    value: nz(value) || null,
    note: ok ? "" : `Expected ${expected}, got ${nz(value) || "(blank)"}`,
  });
  return ok;
}

function checkOneOf(label, value, allowed, checks) {
  const ok = allowed.includes(nz(value));
  checks.push({
    check: label,
    pass: ok,
    value: nz(value) || null,
    note: ok ? "" : `Expected one of [${allowed.join(", ")}], got ${nz(value) || "(blank)"}`,
  });
  return ok;
}

function checkFalseOrBlank(label, value, checks) {
  const ok = !value;
  checks.push({
    check: label,
    pass: ok,
    value: value ? String(value) : null,
    note: ok ? "" : `${label} must remain false/blank`,
  });
  return ok;
}

function isControlledSource(record) {
  return COMPANY_CONTROLLED_BASES.has(nz(record.sourceBasis));
}

function isOfficialLogoSource(record) {
  const url = nz(record.sourceUrl);
  return /tribute-portfolio\.marriott\.com/i.test(url) && /\.(svg|png|jpg|jpeg|webp)/i.test(url);
}

function runCommonReadinessChecks(record) {
  const checks = [];
  const slot = record.mappedVisualSlot;
  const propertyImage = needsPropertyContext(slot);

  checkPresent("Asset Name", record.assetName, checks);
  checkPresent("Brand Record ID", record.brandRecordId, checks);
  checkPresent("Asset Type", record.assetType, checks);
  checkPresent("Explorer Section", record.explorerSection, checks);
  checkPresent("Recommended Explorer Slot", record.recommendedExplorerSlot, checks);
  checkPresent("Source URL", record.sourceUrl, checks);
  if (propertyImage) {
    checkPresent("Source Page URL", record.sourcePageUrl, checks);
  }
  checkPresent("Source Basis", record.sourceBasis, checks);

  const controlled = isControlledSource(record);
  checks.push({
    check: "Marriott-controlled or company-controlled source",
    pass: controlled,
    value: nz(record.sourceBasis) || null,
    note: controlled ? "" : "Source Basis must be Marriott-Controlled Source or Company Materials",
  });

  if (propertyImage) {
    checkPresent("Related Property Name", record.relatedPropertyName, checks);
    checkPresent("Country / Region", record.countryRegion, checks);
    checkOneOf("CALA Relevant?", record.calaRelevant, ["Yes", "No", "Unknown"], checks);
    checkOneOf("Hotel / Property Confirmed?", record.propertyConfirmed, ["Yes", "No", "Unknown"], checks);
    checkOneOf("Brand Confirmed?", record.brandConfirmed, ["Yes", "No", "Unknown"], checks);
    checkOneOf(
      "Source Page Confirms Image Context?",
      record.sourcePageConfirmsContext,
      ["Yes", "No", "Unknown"],
      checks
    );
  }

  checkPresent("Visual Slot Validation Status", record.validationStatus, checks);
  checkPresent("Visual Slot Validation Notes", record.validationNotes, checks);
  const formallyApproved = isFormallyApprovedRecord(record);
  if (formallyApproved) {
    checkOneOf("Usage Review Status", record.usageReviewStatus, ["Usage Review Complete", "Reviewed"], checks);
    checkEquals("Explorer Use Permission", record.explorerUsePermission, "Approved For Explorer", checks);
  } else {
    checkOneOf("Usage Review Status", record.usageReviewStatus, [...VAL_USAGE_OK], checks);
    checkEquals("Explorer Use Permission", record.explorerUsePermission, "Candidate Only", checks);
  }
  checks.push({
    check: "Is Primary Candidate",
    pass: record.isPrimaryCandidate === true,
    value: record.isPrimaryCandidate ? "true" : "false",
    note: record.isPrimaryCandidate ? "" : "Is Primary Candidate must be true",
  });
  checkFalseOrBlank("Company Validated", record.companyValidated, checks);
  checkFalseOrBlank("Company Validation Date", record.companyValidationDate, checks);

  return checks;
}

function runSlotSpecificChecks(record, allPrimaries = []) {
  const checks = [];
  const slot = record.mappedVisualSlot;
  const humanVerify = [];

  if (slot === VISUAL_SLOT.LOGO) {
    const official = isOfficialLogoSource(record);
    checks.push({
      check: "Official logo source exists",
      pass: official,
      value: nz(record.sourceUrl) || null,
      note: official ? "" : "Logo should come from official tribute-portfolio.marriott.com asset URL",
    });
    humanVerify.push("Compare tribute-black.svg against current Brand Setup logo in Airtable/Webflow.");
    humanVerify.push("Confirm logo source/usage rights are acceptable before Explorer promotion.");
    humanVerify.push("Open Source URL and verify SVG renders correctly.");
  }

  if (slot === VISUAL_SLOT.HERO) {
    checks.push({
      check: "Named Tribute Portfolio property",
      pass: Boolean(nz(record.relatedPropertyName)),
      value: nz(record.relatedPropertyName) || null,
      note: "",
    });
    checks.push({
      check: "CALA-relevant",
      pass: record.calaRelevant === "Yes",
      value: record.calaRelevant || null,
      note: record.calaRelevant === "Yes" ? "" : "Hero should be CALA-relevant for CALA-focused profile",
    });
    checks.push({
      check: "Property-confirmed",
      pass: record.propertyConfirmed === "Yes",
      value: record.propertyConfirmed || null,
      note: "",
    });
    checks.push({
      check: "Suitable as primary brand-level visual (metadata)",
      pass: isControlledSource(record) && Boolean(nz(record.sourcePageUrl)),
      value: null,
      note: "",
    });
    humanVerify.push("Visually inspect Source URL — confirm real hotel/property exterior or hero shot.");
    humanVerify.push("Confirm image depicts Ermita (or named property), not generic brand creative.");
    humanVerify.push("Do not approve until usage rights and CALA fit are confirmed.");
  }

  if (slot === VISUAL_SLOT.GALLERY) {
    const property = nz(record.relatedPropertyName);
    const duplicateProperty = allPrimaries.filter(
      (r) =>
        r.id !== record.id &&
        r.mappedVisualSlot === VISUAL_SLOT.GALLERY &&
        nz(r.relatedPropertyName) === property
    );
    checks.push({
      check: "Represents a real Tribute Portfolio hotel",
      pass: Boolean(property) && record.propertyConfirmed === "Yes",
      value: property || null,
      note: "",
    });
    checks.push({
      check: "Different property from other gallery primaries",
      pass: duplicateProperty.length === 0,
      value: property || null,
      note:
        duplicateProperty.length > 0
          ? `Duplicate property also primary in slot(s): ${duplicateProperty.map((r) => r.recommendedExplorerSlot).join(", ")}`
          : "",
    });
    const galleryProperties = allPrimaries
      .filter((r) => r.mappedVisualSlot === VISUAL_SLOT.GALLERY)
      .map((r) => nz(r.relatedPropertyName))
      .filter(Boolean);
    const uniqueCount = new Set(galleryProperties).size;
    checks.push({
      check: "Gallery set includes mix of different properties",
      pass: uniqueCount >= 4,
      value: `${uniqueCount} unique properties across ${galleryProperties.length} gallery primaries`,
      note: uniqueCount < 4 ? "Prefer at least 4 different named hotels across gallery slots" : "",
    });
    humanVerify.push("Visually inspect Source URL — confirm exterior, guestroom, or public space as labeled.");
    humanVerify.push("Confirm no duplicate crop of same base image across gallery slots.");
    humanVerify.push("Confirm property name matches image context.");
  }

  if (slot === VISUAL_SLOT.VALUE_DRIVER) {
    const driverLabel = effectiveValueDriver(record);
    const property = nz(record.relatedPropertyName) || extractPropertyFromAssetName(record.assetName);
    checks.push({
      check: "Value driver identified",
      pass: Boolean(driverLabel),
      value: driverLabel || null,
      note: "",
    });
    checks.push({
      check: `Value-driver match (${driverLabel})`,
      pass: Boolean(driverLabel) && Boolean(property),
      value: `${driverLabel} (inferred from property)`,
      note: !driverLabel ? "Cannot infer value driver from property name" : "",
    });
    if (driverLabel === "Resort") {
      checks.push({
        check: "Resort/leisure property evidence",
        pass: /resort|beach|cove|island|nizuc|holbox/i.test(property),
        value: property,
        note: "",
      });
    }
    if (driverLabel === "Urban") {
      checks.push({
        check: "Urban/city property evidence",
        pass: /lima|medellin|rumbao|urban|city|recoleta|tajibos/i.test(property.toLowerCase()),
        value: property,
        note: "",
      });
    }
    if (driverLabel === "Conversion / Adaptive Reuse") {
      checks.push({
        check: "Conversion/adaptive reuse evidence",
        pass: /ermita|cartagena|heritage|historic|colonial/i.test(property.toLowerCase()),
        value: property,
        note: "",
      });
    }
    humanVerify.push(`Visually inspect Source URL — confirm image supports ${driverLabel} value driver.`);
    humanVerify.push("Reject if image is generic lifestyle or wrong property setting.");
  }

  if (slot === VISUAL_SLOT.RECENT_OPENINGS) {
    checks.push({
      check: "Recent Openings should remain Missing",
      pass: false,
      value: null,
      note: "Do not approve without property + PR/opening/date evidence",
    });
    humanVerify.push("Do not approve — requires property name, opening date, and official PR source.");
  }

  return { checks, humanVerify };
}

function buildHumanChecklist(record, commonChecks, slotResult) {
  const items = [];
  const failed = commonChecks.filter((c) => !c.pass);
  const slotFailed = slotResult.checks.filter((c) => !c.pass);

  items.push({
    category: "Metadata present",
    action: failed.length === 0 ? "All required metadata fields populated" : `Fix missing: ${failed.map((c) => c.check).join("; ")}`,
    required: true,
  });

  items.push({
    category: "Source context",
    action: isControlledSource(record)
      ? `Verify source: ${nz(record.sourceBasis)} — ${nz(record.sourcePageUrl) || nz(record.sourceUrl)}`
      : "Confirm source is official Marriott or company-controlled before approval",
    required: true,
  });

  if (needsPropertyContext(record.mappedVisualSlot)) {
    items.push({
      category: "Property identity",
      action: `Confirm image depicts: ${nz(record.relatedPropertyName)} (${nz(record.countryRegion)})`,
      required: true,
    });
  }

  for (const step of slotResult.humanVerify) {
    items.push({ category: "Human verification", action: step, required: true });
  }

  if (slotFailed.length) {
    items.push({
      category: "Slot-specific gaps",
      action: slotFailed.map((c) => c.check + (c.note ? `: ${c.note}` : "")).join("; "),
      required: true,
    });
  }

  items.push({
    category: "After review (Airtable)",
    action: "Update Review Notes, Reviewed By, Last Reviewed Date. Do not set Company Validated unless explicitly confirmed.",
    required: false,
  });

  return items;
}

function determineOutcome(record, commonChecks, slotChecks) {
  const slot = record.mappedVisualSlot;

  if (isFormallyApprovedRecord(record)) {
    return READINESS_OUTCOME.APPROVED;
  }

  if (!record.isPrimaryCandidate) {
    return READINESS_OUTCOME.NOT_READY;
  }
  if (record.explorerUsePermission !== "Candidate Only") {
    return READINESS_OUTCOME.NOT_READY;
  }
  if (record.companyValidated || record.companyValidationDate) {
    return READINESS_OUTCOME.NOT_READY;
  }
  if (slot === VISUAL_SLOT.RECENT_OPENINGS) {
    return READINESS_OUTCOME.MISSING;
  }

  const commonFailed = commonChecks.filter((c) => !c.pass);
  const slotFailed = slotChecks.filter((c) => !c.pass);
  const metadataFields = [
    "Asset Name",
    "Brand Record ID",
    "Asset Type",
    "Explorer Section",
    "Recommended Explorer Slot",
    "Source URL",
    "Source Basis",
    "Visual Slot Validation Status",
    "Visual Slot Validation Notes",
  ];
  const metadataMissing = commonFailed.some((c) => metadataFields.includes(c.check));
  if (metadataMissing || (needsPropertyContext(slot) && commonFailed.some((c) => /Property|Country|CALA|Brand|Source Page/.test(c.check)))) {
    return READINESS_OUTCOME.NEEDS_METADATA;
  }

  const sourceFailed = commonFailed.some((c) => /controlled source|Source Basis|Source Page URL/.test(c.check)) ||
    slotFailed.some((c) => /Official logo source|controlled/.test(c.check));
  if (sourceFailed) {
    return READINESS_OUTCOME.NEEDS_SOURCE;
  }

  const slotHardFail = slotFailed.some((c) =>
    /Recent Openings|Value-driver match|Property-confirmed|CALA-relevant|Named Tribute/.test(c.check)
  );
  if (slotHardFail) {
    return READINESS_OUTCOME.NOT_READY;
  }

  const valueDriverWeak = slot === VISUAL_SLOT.VALUE_DRIVER &&
    slotFailed.some((c) => /Value-driver|Resort|Urban|Conversion/.test(c.check));
  if (valueDriverWeak) {
    return READINESS_OUTCOME.NEEDS_METADATA;
  }

  if ([VISUAL_SLOT.HERO, VISUAL_SLOT.GALLERY, VISUAL_SLOT.VALUE_DRIVER].includes(slot)) {
    return READINESS_OUTCOME.NEEDS_VISUAL;
  }

  if (slot === VISUAL_SLOT.LOGO && commonFailed.length === 0 && slotFailed.length === 0) {
    return READINESS_OUTCOME.READY;
  }

  if (commonFailed.length === 0 && slotFailed.length === 0) {
    return READINESS_OUTCOME.READY;
  }

  return READINESS_OUTCOME.NOT_READY;
}

export function evaluatePrimaryCandidateReadiness(record, allPrimaries = []) {
  const commonChecks = runCommonReadinessChecks(record);
  const slotResult = runSlotSpecificChecks(record, allPrimaries);
  const allChecks = [...commonChecks, ...slotResult.checks];
  const outcome = determineOutcome(record, commonChecks, slotResult.checks);
  const checklist = buildHumanChecklist(record, commonChecks, slotResult);

  return {
    recordId: record.id,
    assetName: record.assetName,
    assetType: record.assetType,
    mappedVisualSlot: record.mappedVisualSlot,
    explorerSection: record.explorerSection,
    recommendedExplorerSlot: record.recommendedExplorerSlot,
    relatedPropertyName: record.relatedPropertyName || null,
    relatedValueDriver: effectiveValueDriver(record) || record.relatedValueDriver || null,
    sourceUrl: record.sourceUrl || null,
    sourcePageUrl: record.sourcePageUrl || null,
    sourceBasis: record.sourceBasis || null,
    outcome,
    commonChecks,
    slotChecks: slotResult.checks,
    allChecksPass: allChecks.every((c) => c.pass),
    failedChecks: allChecks.filter((c) => !c.pass).map((c) => ({ check: c.check, note: c.note })),
    humanChecklist: checklist,
    humanMustVerify: slotResult.humanVerify,
  };
}

function isVisualReviewPrimary(record) {
  if (!record.isPrimaryCandidate) return false;
  if (record.mappedVisualSlot === VISUAL_SLOT.BRAND_STANDARDS) return false;
  if (nz(record.assetType) === "PDF / Brochure") return false;
  if (record.explorerUsePermission === "Internal Only") return false;
  if (record.explorerUsePermission === "Do Not Use") return false;
  return true;
}

function assessMissingSlots(records) {
  const missing = [];
  const hasRecentOpening = records.some(
    (r) => r.isPrimaryCandidate && r.mappedVisualSlot === VISUAL_SLOT.RECENT_OPENINGS
  );
  if (!hasRecentOpening) {
    missing.push({
      slot: VISUAL_SLOT.RECENT_OPENINGS,
      explorerSection: "footprint.openings",
      outcome: READINESS_OUTCOME.MISSING,
      reason: "No primary candidate — requires property + opening/PR/date before review.",
    });
  }
  const valueDrivers = ["Boutique / Lifestyle", "Mixed-Use"];
  for (const driver of valueDrivers) {
    const hasPrimary = records.some(
      (r) =>
        r.isPrimaryCandidate &&
        r.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER &&
        effectiveValueDriver(r) === driver
    );
    if (!hasPrimary) {
      missing.push({
        slot: VISUAL_SLOT.VALUE_DRIVER,
        explorerSection: driver,
        outcome: READINESS_OUTCOME.MISSING,
        reason: `No primary candidate for ${driver} value driver.`,
      });
    }
  }
  return missing;
}

export async function buildHumanReviewReadinessReport({
  brandKey = "tribute-portfolio",
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
} = {}) {
  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG["tribute-portfolio"];
  const resolvedBrandId = pilot?.brandRecordId || brandRecordId;

  let rawRecords = [];
  let registryReadError = null;
  try {
    rawRecords = await listRegistryRecordsRaw(resolvedBrandId);
  } catch (err) {
    registryReadError = err.message || String(err);
  }

  const records = rawRecords.map(normalizeRecordForReadiness);
  const allPrimaries = records.filter((r) => r.isPrimaryCandidate);
  const primaries = records.filter(isVisualReviewPrimary);
  const excludedPrimaries = allPrimaries.filter((r) => !isVisualReviewPrimary(r));
  const evaluations = primaries.map((r) => evaluatePrimaryCandidateReadiness(r, primaries));

  const byOutcome = (outcome) => evaluations.filter((e) => e.outcome === outcome);

  const missingSlots = assessMissingSlots(records);

  return {
    readinessVersion: READINESS_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    brandSetupMediaUntouched: true,
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
    primaryCandidatesScanned: primaries.length,
    allPrimaryRecords: allPrimaries.length,
    excludedFromVisualReview: excludedPrimaries.map((r) => ({
      recordId: r.id,
      assetName: r.assetName,
      reason: "Internal/source reference — not a visual Explorer review candidate",
    })),
    approvedForExplorer: byOutcome(READINESS_OUTCOME.APPROVED),
    readyForHumanReview: byOutcome(READINESS_OUTCOME.READY),
    needsMoreMetadata: byOutcome(READINESS_OUTCOME.NEEDS_METADATA),
    needsSourceReview: byOutcome(READINESS_OUTCOME.NEEDS_SOURCE),
    needsVisualInspection: byOutcome(READINESS_OUTCOME.NEEDS_VISUAL),
    notReady: byOutcome(READINESS_OUTCOME.NOT_READY),
    pendingPrimaryCandidates: evaluations
      .filter((e) => e.outcome !== READINESS_OUTCOME.APPROVED)
      .map((e) => ({
        recordId: e.recordId,
        assetName: e.assetName,
        mappedVisualSlot: e.mappedVisualSlot,
        recommendedExplorerSlot: e.recommendedExplorerSlot,
        outcome: e.outcome,
      })),
    approvedCoverageBySlot: Object.fromEntries(
      byOutcome(READINESS_OUTCOME.APPROVED).reduce((acc, e) => {
        const key =
          e.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER
            ? `Value Driver: ${e.relatedValueDriver || "Unspecified"}`
            : e.mappedVisualSlot;
        if (!acc.has(key)) acc.set(key, []);
        acc.get(key).push(e.assetName);
        return acc;
      }, new Map())
    ),
    readyForDownloadOrAttachment: byOutcome(READINESS_OUTCOME.APPROVED).length > 0,
    missingSlots,
    evaluations,
    recommendedAirtableReviewFields: RECOMMENDED_AIRTABLE_REVIEW_FIELDS,
    nextCommand: `npm run brand-asset-human-review-readiness -- --brand ${brandKey} --dry-run`,
    remainingWorkBeforeAssetApproval: [
      "Human reviewer completes checklist for each primary candidate.",
      "Visually inspect all property image Source URLs (hero, gallery, value drivers).",
      "Compare logo SVG against Brand Setup logo before logo approval.",
      "Record Review Notes, Reviewed By, Last Reviewed Date in registry after each decision.",
      "Do not set Company Validated unless Marriott/brand explicitly confirms rights.",
      "Reject or request alternates for any candidate that fails visual or source review.",
    ],
    remainingWorkBeforeExplorerPromotion: [
      "Complete human usage review on all 11 primary candidates.",
      "Capture Recent Openings with property + PR/opening/date (currently Missing).",
      "Fill Boutique/Lifestyle and Mixed-Use value-driver imagery gaps.",
      "Build governed hero/logo promotion writer (separate module — not this task).",
      "Do not replace Mock/Demo hero until governed CALA hero is approved.",
      "Rendered Source Capture v1 for Marriott newsroom PR before PR link promotion.",
      "No Explorer Use Permission = Approved For Explorer until full visual parity review.",
    ],
  };
}

export function buildHumanReviewReadinessMarkdown(report) {
  const lines = [];
  lines.push("# Brand Asset Human Review Readiness v4");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push(`Text/governance Platform Ready: **${report.textGovernanceStatus.textGovernancePlatformReady ? "yes" : "no"}**`);
  lines.push(`Brand Setup media untouched: **${report.brandSetupMediaUntouched ? "yes" : "no"}**`);
  lines.push("");

  lines.push("## 1. Summary");
  lines.push("");
  lines.push(`- Total asset records scanned: **${report.totalRecordsScanned}**`);
  lines.push(`- Primary candidates scanned: **${report.primaryCandidatesScanned}**`);
  lines.push(`- Approved For Explorer: **${report.approvedForExplorer.length}**`);
  lines.push(`- Ready for human review: **${report.readyForHumanReview.length}**`);
  lines.push(`- Needs more metadata: **${report.needsMoreMetadata.length}**`);
  lines.push(`- Needs source review: **${report.needsSourceReview.length}**`);
  lines.push(`- Needs visual inspection: **${report.needsVisualInspection.length}**`);
  lines.push(`- Not ready: **${report.notReady.length}**`);
  lines.push(`- Pending primary candidates: **${report.pendingPrimaryCandidates.length}**`);
  lines.push(`- Missing slots: **${report.missingSlots.length}**`);
  lines.push(
    `- Ready for download/attachment: **${report.readyForDownloadOrAttachment ? "yes" : "no"}**`
  );
  lines.push("");

  const printCandidates = (title, list) => {
    lines.push(`## ${title}`);
    lines.push("");
    if (!list.length) {
      lines.push("None.");
      lines.push("");
      return;
    }
    for (const e of list) {
      lines.push(`### ${e.assetName}`);
      lines.push(`- Record: \`${e.recordId}\``);
      lines.push(`- Slot: ${e.mappedVisualSlot} → ${e.recommendedExplorerSlot}`);
      lines.push(`- Outcome: **${e.outcome}**`);
      if (e.relatedPropertyName) lines.push(`- Property: ${e.relatedPropertyName}`);
      if (e.failedChecks?.length) {
        lines.push(`- Gaps: ${e.failedChecks.map((f) => f.check).join("; ")}`);
      }
      lines.push("");
    }
  };

  printCandidates("2. Approved For Explorer", report.approvedForExplorer);
  printCandidates("3. Ready for human review", report.readyForHumanReview);
  printCandidates("4. Needs more metadata", report.needsMoreMetadata);
  printCandidates("5. Needs source review", report.needsSourceReview);
  printCandidates("6. Needs visual inspection", report.needsVisualInspection);
  printCandidates("7. Not ready", report.notReady);

  lines.push("## 8. Approved coverage by slot");
  lines.push("");
  if (!Object.keys(report.approvedCoverageBySlot || {}).length) {
    lines.push("No formally approved coverage yet.");
  } else {
    for (const [slot, names] of Object.entries(report.approvedCoverageBySlot)) {
      lines.push(`- **${slot}**: ${names.join("; ")}`);
    }
  }
  lines.push("");

  lines.push("## 9. Pending primary candidates");
  lines.push("");
  if (!report.pendingPrimaryCandidates.length) {
    lines.push("None.");
  } else {
    for (const p of report.pendingPrimaryCandidates) {
      lines.push(`- \`${p.recordId}\` — ${p.assetName} (${p.mappedVisualSlot} → ${p.recommendedExplorerSlot})`);
    }
  }
  lines.push("");

  lines.push("## 10. Missing slots");
  lines.push("");
  for (const m of report.missingSlots) {
    lines.push(`- **${m.slot}** (${m.explorerSection}) — ${m.reason}`);
  }
  lines.push("");

  lines.push("## 11. Human review checklist by candidate");
  lines.push("");
  for (const e of report.evaluations) {
    lines.push(`### ${e.assetName} (\`${e.recordId}\`)`);
    lines.push(`- Outcome: **${e.outcome}**`);
    lines.push(`- Source: [${e.sourceUrl || "—"}](${e.sourceUrl || ""})`);
    if (e.sourcePageUrl) lines.push(`- Source page: ${e.sourcePageUrl}`);
    lines.push("");
    lines.push("| Step | Action | Required |");
    lines.push("|------|--------|----------|");
    for (const item of e.humanChecklist) {
      lines.push(`| ${item.category} | ${item.action} | ${item.required ? "yes" : "no"} |`);
    }
    lines.push("");
  }

  lines.push("## 12. Recommended Airtable review fields");
  lines.push("");
  lines.push("| Field | Note |");
  lines.push("|-------|------|");
  for (const f of report.recommendedAirtableReviewFields) {
    lines.push(`| ${f.field} | ${f.note} |`);
  }
  lines.push("");

  lines.push("## 13. Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.nextCommand);
  lines.push("```");
  lines.push("");

  lines.push("## 14. Remaining work before asset approval");
  lines.push("");
  for (const item of report.remainingWorkBeforeAssetApproval) {
    lines.push(`- ${item}`);
  }
  lines.push("");

  lines.push("## 15. Remaining work before Explorer promotion");
  lines.push("");
  for (const item of report.remainingWorkBeforeExplorerPromotion) {
    lines.push(`- ${item}`);
  }
  lines.push("");

  return lines.join("\n");
}
