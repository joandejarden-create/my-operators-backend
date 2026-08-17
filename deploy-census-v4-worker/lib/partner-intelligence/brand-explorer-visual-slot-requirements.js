/**
 * Brand Explorer Visual Slot Requirements v1.
 *
 * Defines slot-specific image/evidence requirements for Brand Explorer visual
 * slots (logo, hero, gallery, recent openings, value driver, brand standards,
 * PR link), then audits the existing Brand Asset Registry records for a brand
 * against those requirements. Report-only by default.
 *
 * Does NOT download images, overwrite Brand Setup media fields, approve assets
 * for Explorer use, or write Airtable unless schema apply is explicitly gated.
 *
 * @see docs/data-intelligence/brand-explorer-visual-slot-requirements-v1.md
 */
import {
  BRAND_ASSET_PILOT_CONFIG,
  MAP_BRAND_ASSET,
  BRAND_ASSET_REGISTRY_TABLE,
  listRegistryAssetsForBrand,
} from "./brand-asset-registry-workflow.js";

export { BRAND_ASSET_PILOT_CONFIG };

export const REQUIREMENTS_VERSION = "2";
export const REPORT_JSON_NAME = "brand-explorer-visual-slot-requirements.json";
export const REPORT_MD_NAME = "brand-explorer-visual-slot-requirements.md";

/* ------------------------------------------------------------------ */
/* Visual slot model                                                   */
/* ------------------------------------------------------------------ */

export const VISUAL_SLOT = {
  LOGO: "Logo",
  HERO: "Hero Image",
  GALLERY: "Image Gallery",
  RECENT_OPENINGS: "Recent Openings",
  VALUE_DRIVER: "Where This Brand Creates the Most Value",
  BRAND_STANDARDS: "Brand Standards / Owner Considerations",
  PR_LINK: "PR / Opening Link",
};

/**
 * Validation checks — each slot lists the checks it requires. A record must
 * pass ALL required checks (given available evidence) to be Valid for Slot.
 */
export const VALIDATION_CHECK = {
  brandMatch: "brandMatch",
  propertyMatch: "propertyMatch",
  slotMatch: "slotMatch",
  geographyMatch: "geographyMatch",
  valueDriverMatch: "valueDriverMatch",
  openingMatch: "openingMatch",
  sourceControlled: "sourceControlled",
  usageReviewComplete: "usageReviewComplete",
  notMockDemo: "notMockDemo",
  notGenericLifestyle: "notGenericLifestyle",
  notAbstract: "notAbstract",
  notThirdPartyPrimary: "notThirdPartyPrimary",
  hasNamedProperty: "hasNamedProperty",
  hasRegionOrCountry: "hasRegionOrCountry",
  hasSourcePageContext: "hasSourcePageContext",
};

const C = VALIDATION_CHECK;

export const VISUAL_SLOT_DEFINITIONS = [
  {
    slot: VISUAL_SLOT.LOGO,
    explorerSection: "Brand Setup — Logo",
    purpose: "Confirms brand identity.",
    requirements: [
      "Official brand logo or existing Brand Setup logo confirmed against official source.",
      "Does not need hotel/property context.",
      "Must still pass usage/source review.",
    ],
    requiredChecks: [C.brandMatch, C.sourceControlled, C.notThirdPartyPrimary, C.usageReviewComplete],
    optionalChecks: [C.hasSourcePageContext],
    needsPropertyContext: false,
  },
  {
    slot: VISUAL_SLOT.HERO,
    explorerSection: "overview.hero / Brand Setup — Explorer Hero",
    purpose: "Primary brand-level visual.",
    requirements: [
      "Real hotel/property image.",
      "Brand-confirmed or property-confirmed as Tribute Portfolio.",
      "Prefer CALA-relevant for CALA-focused profile.",
      "Not abstract, generic lifestyle, non-hotel, or unrelated global imagery.",
      "Official Marriott-controlled source or approved local reference source.",
    ],
    requiredChecks: [
      C.brandMatch,
      C.propertyMatch,
      C.sourceControlled,
      C.notMockDemo,
      C.notGenericLifestyle,
      C.notAbstract,
      C.notThirdPartyPrimary,
      C.usageReviewComplete,
    ],
    optionalChecks: [C.geographyMatch, C.hasNamedProperty, C.hasSourcePageContext],
    needsPropertyContext: true,
    prefersCala: true,
  },
  {
    slot: VISUAL_SLOT.GALLERY,
    explorerSection: "materials.gallery.1–6",
    purpose: "Shows a collection of different real hotels under the brand.",
    requirements: [
      "Multiple different Tribute Portfolio hotels.",
      "Each image tied to a named property where possible.",
      "Prefer mix of exterior, guestroom, public space, restaurant/bar/lifestyle.",
      "Prefer CALA or region-relevant examples where possible.",
      "Avoid duplicate crops from the same image unless intentional.",
    ],
    requiredChecks: [
      C.brandMatch,
      C.propertyMatch,
      C.sourceControlled,
      C.notMockDemo,
      C.notAbstract,
      C.notThirdPartyPrimary,
      C.usageReviewComplete,
    ],
    optionalChecks: [C.hasNamedProperty, C.geographyMatch, C.hasSourcePageContext],
    needsPropertyContext: true,
    prefersCala: true,
  },
  {
    slot: VISUAL_SLOT.RECENT_OPENINGS,
    explorerSection: "footprint.openings",
    purpose: "Shows proof of current brand activity.",
    requirements: [
      "Tied to the specific hotel/opening being referenced.",
      "Image of that hotel or directly from the official opening/PR source.",
      "Requires property name, opening/announcement date, source URL, region/country.",
      "Generic brand images are not acceptable.",
    ],
    requiredChecks: [
      C.brandMatch,
      C.propertyMatch,
      C.openingMatch,
      C.hasNamedProperty,
      C.hasRegionOrCountry,
      C.hasSourcePageContext,
      C.sourceControlled,
      C.notThirdPartyPrimary,
      C.usageReviewComplete,
    ],
    optionalChecks: [C.geographyMatch],
    needsPropertyContext: true,
    prefersCala: true,
  },
  {
    slot: VISUAL_SLOT.VALUE_DRIVER,
    explorerSection: "overview.why_value",
    purpose: "Visualizes the specific value driver shown.",
    requirements: [
      "Image must match the value driver.",
      "Urban → urban hotel/property context.",
      "Resort → resort/leisure property context.",
      "Conversion/adaptive reuse → conversion, repositioning, independent, or adaptive-reuse context.",
      "Boutique/lifestyle → design-led or lifestyle hotel context.",
      "Mixed-use → mixed-use/urban lifestyle context.",
      "CALA relevance preferred where profile is CALA-focused.",
      "Generic imagery is not acceptable.",
    ],
    requiredChecks: [
      C.brandMatch,
      C.propertyMatch,
      C.valueDriverMatch,
      C.sourceControlled,
      C.notMockDemo,
      C.notGenericLifestyle,
      C.notAbstract,
      C.notThirdPartyPrimary,
      C.usageReviewComplete,
    ],
    optionalChecks: [C.geographyMatch, C.hasNamedProperty],
    needsPropertyContext: true,
    prefersCala: true,
  },
  {
    slot: VISUAL_SLOT.BRAND_STANDARDS,
    explorerSection: "materials.file / Source Library Reference",
    purpose: "Supports owner/developer understanding.",
    requirements: [
      "Can be a PDF/brochure/source attachment, diagram, standards excerpt, or official source reference.",
      "Does not necessarily require hotel photography.",
      "Must be clearly source-backed and approved for display or internal reference.",
    ],
    requiredChecks: [C.sourceControlled, C.notThirdPartyPrimary],
    optionalChecks: [C.hasSourcePageContext],
    needsPropertyContext: false,
  },
  {
    slot: VISUAL_SLOT.PR_LINK,
    explorerSection: "PR / Recent Openings",
    purpose: "Links to official news/source evidence.",
    requirements: [
      "Official brand/company source preferred.",
      "JS-shell pages provenance-only until rendered capture exists.",
      "Third-party PR context clearly marked; not used as company-materials evidence.",
    ],
    requiredChecks: [C.brandMatch, C.sourceControlled, C.notThirdPartyPrimary, C.hasSourcePageContext],
    optionalChecks: [C.hasRegionOrCountry],
    needsPropertyContext: false,
    provenanceOnlyUntilRendered: true,
  },
];

/* ------------------------------------------------------------------ */
/* Proposed registry fields for slot-specific governance               */
/* ------------------------------------------------------------------ */

export const PROPOSED_VISUAL_SLOT_FIELDS = [
  { name: "Explorer Section", type: "singleLineText", note: "Which Explorer section/slot key this asset targets." },
  { name: "Slot Purpose", type: "singleLineText", note: "Human-readable slot purpose." },
  { name: "Related Value Driver", type: "singleSelect", note: "Urban / Resort / Conversion / Boutique-Lifestyle / Mixed-Use." },
  { name: "Related Property Name", type: "singleLineText", note: "Named property this image depicts." },
  { name: "Related Opening / PR", type: "singleLineText", note: "Opening/announcement reference for footprint.openings." },
  { name: "Country / Region", type: "singleLineText", note: "Geography for the depicted property." },
  { name: "CALA Relevant?", type: "singleSelect", note: "Yes / No / Unknown." },
  { name: "Hotel / Property Confirmed?", type: "singleSelect", note: "Yes / No / Unknown." },
  { name: "Brand Confirmed?", type: "singleSelect", note: "Yes / No / Unknown." },
  { name: "Source Page Confirms Image Context?", type: "singleSelect", note: "Yes / No / Unknown." },
  { name: "Use Case Match", type: "singleSelect", note: "Matched value-driver/use-case, or None." },
  { name: "Visual Slot Validation Status", type: "singleSelect", note: "Slot-specific validation verdict." },
  { name: "Visual Slot Validation Notes", type: "multilineText", note: "Reviewer-facing validation detail." },
];

export const VAL_VISUAL_SLOT_VALIDATION_STATUS = [
  "Valid for Slot",
  "Needs Usage Review",
  "Needs Property Confirmation",
  "Needs CALA Property",
  "Wrong Slot",
  "Source Reference Only",
  "Provenance Only",
  "Mock/Demo Guard",
  "Do Not Use",
  "Not Enough Context",
];

export const VAL_CALA_RELEVANT = ["Yes", "No", "Unknown"];
export const VAL_YES_NO_UNKNOWN = ["Yes", "No", "Unknown"];

/** Central mapping for the 13 slot-governance registry fields. */
export const MAP_VISUAL_SLOT = {
  explorerSection: "Explorer Section",
  slotPurpose: "Slot Purpose",
  relatedValueDriver: "Related Value Driver",
  relatedPropertyName: "Related Property Name",
  relatedOpeningPr: "Related Opening / PR",
  countryRegion: "Country / Region",
  calaRelevant: "CALA Relevant?",
  propertyConfirmed: "Hotel / Property Confirmed?",
  brandConfirmed: "Brand Confirmed?",
  sourcePageConfirmsContext: "Source Page Confirms Image Context?",
  useCaseMatch: "Use Case Match",
  validationStatus: "Visual Slot Validation Status",
  validationNotes: "Visual Slot Validation Notes",
};

export const VAL_USAGE_REVIEW_FOR_WRITER = {
  PENDING: "Pending Review",
  NOT_REVIEWED: "Not Reviewed",
  COMPLETE: "Usage Review Complete",
  BLOCKED: "Blocked",
};

/* ------------------------------------------------------------------ */
/* Classification labels                                               */
/* ------------------------------------------------------------------ */

export const AUDIT_CLASSIFICATION = {
  VALID: "Valid for slot",
  NEEDS_USAGE_REVIEW: "Candidate but needs usage review",
  WRONG_SLOT: "Candidate but wrong slot",
  NOT_ENOUGH_CONTEXT: "Not enough context",
  NOT_PROPERTY: "Not hotel/property image",
  NOT_CALA: "Not CALA-relevant",
  DO_NOT_USE: "Do Not Use",
  MOCK_DEMO_GUARD: "Mock/Demo guard",
  PR_PROVENANCE_ONLY: "PR provenance-only",
};

/* ------------------------------------------------------------------ */
/* Heuristics (metadata-only — no image download)                      */
/* ------------------------------------------------------------------ */

const COMPANY_SOURCE_BASES = new Set([
  "Company Materials",
  "Marriott-Controlled Source",
  "Rendered Official Source",
  "Local Reference Material",
]);

// CALA country / market keywords for geography relevance detection.
const CALA_KEYWORDS = [
  "mexico", "cancun", "cabo", "merida", "monterrey", "guadalajara", "riviera",
  "puerto-rico", "puerto rico", "san-juan", "san juan", "dominican", "santo-domingo",
  "punta-cana", "jamaica", "bahamas", "aruba", "barbados", "cayman", "antigua",
  "colombia", "cartagena", "bogota", "medellin", "peru", "lima", "cusco", "chile",
  "santiago", "panama", "costa-rica", "cala", "caribbean", "latin-america",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function isTributeBrandUrl(url) {
  return /tribute-portfolio\.marriott\.com|tribute/i.test(url);
}

function looksAbstractOrIcon(url) {
  return /favicon|cropped-tributefavicon|placeholder|apple\d+x\d+|profile_placeholder|icon|logo/i.test(url);
}

function looksNamedProperty(text) {
  // Heuristic: a named property usually contains a place/hotel token beyond the generic label.
  if (!text) return false;
  if (/property\/design image|hero — consumer property wide|existing logo|Mock\/Demo|FDD|newsroom/i.test(text)) {
    return false;
  }
  return /\b(hotel|resort|inn|suites|the\s+\w+|by\s+tribute)\b/i.test(text);
}

function detectCala(...texts) {
  const hay = texts.filter(Boolean).join(" ").toLowerCase();
  return CALA_KEYWORDS.some((k) => hay.includes(k));
}

/**
 * Map an asset registry record's Recommended Explorer Slot to a visual slot.
 */
export function mapRecordToVisualSlot(record) {
  const slot = nz(record.recommendedExplorerSlot);
  const type = nz(record.assetType);
  const status = nz(record.assetStatus);

  if (/logo/i.test(slot) || type === "Logo") return VISUAL_SLOT.LOGO;
  if (status === "Mock/Demo") return VISUAL_SLOT.HERO;
  if (/explorer hero|overview\.hero/i.test(slot) || type === "Hero Image") return VISUAL_SLOT.HERO;
  if (/materials\.gallery/i.test(slot)) return VISUAL_SLOT.GALLERY;
  if (/why_value|creates the most value|value driver/i.test(slot)) return VISUAL_SLOT.VALUE_DRIVER;
  // Press/PR link records are provenance links, not opening photography — classify as PR link.
  if (type === "Press Link" || type === "Recent Opening Link" || /\bpr\b|press/i.test(slot)) {
    return VISUAL_SLOT.PR_LINK;
  }
  if (/footprint\.openings|recent opening/i.test(slot)) return VISUAL_SLOT.RECENT_OPENINGS;
  if (/source library|reference/i.test(slot) || type === "PDF / Brochure") return VISUAL_SLOT.BRAND_STANDARDS;
  return VISUAL_SLOT.GALLERY;
}

/**
 * Run all validation checks for a record against its mapped slot.
 * Returns an object of check → { value: boolean|null, note }.
 */
export function runValidationChecks(record, slotDef) {
  const url = nz(record.sourceUrl);
  const pageUrl = nz(record.sourcePageUrl);
  const name = nz(record.assetName);
  const status = nz(record.assetStatus);
  const basis = nz(record.sourceBasis);
  const usageStatus = nz(record.usageReviewStatus);

  const brandMatch = isTributeBrandUrl(url) || isTributeBrandUrl(pageUrl) || /tribute/i.test(name);
  const sourceControlled = COMPANY_SOURCE_BASES.has(basis);
  const notThirdPartyPrimary = basis !== "Third-Party Context" && basis !== "Unknown / Do Not Use";
  const notMockDemo = status !== "Mock/Demo";
  const isIconOrAbstract = url ? looksAbstractOrIcon(url) : false;
  const hasNamedProperty = looksNamedProperty(name);
  const cala = detectCala(url, pageUrl, name);
  const usageReviewComplete = usageStatus === "Usage Review Complete";
  const hasSourcePageContext = Boolean(pageUrl);

  // propertyMatch: brand image tied to a real, non-icon hotel property.
  const propertyMatch = slotDef.needsPropertyContext
    ? brandMatch && !isIconOrAbstract && hasNamedProperty
    : null;

  const checks = {
    [C.brandMatch]: { value: brandMatch },
    [C.propertyMatch]: { value: propertyMatch },
    [C.slotMatch]: { value: true, note: "Mapped to slot from Recommended Explorer Slot." },
    [C.geographyMatch]: { value: slotDef.prefersCala ? cala : null, note: cala ? "CALA keyword detected" : "No CALA geography evidence" },
    [C.valueDriverMatch]: {
      value: slotDef.slot === VISUAL_SLOT.VALUE_DRIVER ? false : null,
      note: slotDef.slot === VISUAL_SLOT.VALUE_DRIVER ? "No value-driver mapping on record yet" : "n/a",
    },
    [C.openingMatch]: {
      value: slotDef.slot === VISUAL_SLOT.RECENT_OPENINGS ? false : null,
      note: slotDef.slot === VISUAL_SLOT.RECENT_OPENINGS ? "No specific opening reference on record" : "n/a",
    },
    [C.sourceControlled]: { value: sourceControlled, note: basis },
    [C.usageReviewComplete]: { value: usageReviewComplete, note: usageStatus || "Not Reviewed" },
    [C.notMockDemo]: { value: notMockDemo },
    [C.notGenericLifestyle]: { value: !isIconOrAbstract, note: isIconOrAbstract ? "URL looks like icon/abstract" : "" },
    [C.notAbstract]: { value: !isIconOrAbstract },
    [C.notThirdPartyPrimary]: { value: notThirdPartyPrimary, note: basis },
    [C.hasNamedProperty]: { value: hasNamedProperty },
    [C.hasRegionOrCountry]: { value: false, note: "No Country / Region field populated yet" },
    [C.hasSourcePageContext]: { value: hasSourcePageContext },
  };

  return { checks, derived: { brandMatch, cala, isIconOrAbstract, hasNamedProperty } };
}

function failedRequiredChecks(checks, slotDef) {
  return slotDef.requiredChecks.filter((key) => {
    const c = checks[key];
    return c && c.value === false;
  });
}

/**
 * Classify a single record for its slot and recommend a status correction.
 */
export function auditRecord(record) {
  const slot = mapRecordToVisualSlot(record);
  const slotDef = VISUAL_SLOT_DEFINITIONS.find((d) => d.slot === slot);
  const { checks, derived } = runValidationChecks(record, slotDef);
  const failed = failedRequiredChecks(checks, slotDef);

  const status = nz(record.assetStatus);
  let classification;
  let visualSlotValidationStatus;
  let recommendedAssetStatus = status;
  let recommendedExplorerUsePermission = nz(record.explorerUsePermission) || "Candidate Only";
  let notes = "";

  if (status === "Mock/Demo") {
    classification = AUDIT_CLASSIFICATION.MOCK_DEMO_GUARD;
    visualSlotValidationStatus = "Mock/Demo Guard";
    recommendedAssetStatus = "Mock/Demo";
    recommendedExplorerUsePermission = "Do Not Use";
    notes = "Keep Mock/Demo guard — never promote as governed Explorer hero.";
  } else if (slot === VISUAL_SLOT.PR_LINK) {
    classification = AUDIT_CLASSIFICATION.PR_PROVENANCE_ONLY;
    visualSlotValidationStatus = "Provenance Only";
    recommendedAssetStatus = "Do Not Use";
    recommendedExplorerUsePermission = "Do Not Use";
    notes = "Provenance-only until Rendered Source Capture v1 renders news.marriott.com.";
  } else if (slot === VISUAL_SLOT.BRAND_STANDARDS) {
    const ok = failed.length === 0;
    classification = ok ? AUDIT_CLASSIFICATION.VALID : AUDIT_CLASSIFICATION.NOT_ENOUGH_CONTEXT;
    visualSlotValidationStatus = ok ? "Source Reference Only" : "Not Enough Context";
    recommendedAssetStatus = ok ? "Source-Confirmed" : status;
    recommendedExplorerUsePermission = "Internal Only";
    notes = "Valid source/PDF reference — not a visual image asset; internal/source reference only.";
  } else if (slot === VISUAL_SLOT.LOGO) {
    // Logo does not need property context; gate on usage review.
    if (!checks[C.usageReviewComplete].value) {
      classification = AUDIT_CLASSIFICATION.NEEDS_USAGE_REVIEW;
      visualSlotValidationStatus = "Needs Usage Review";
      recommendedAssetStatus = status === "Candidate" ? "Candidate" : "Needs Usage Review";
      notes = "Valid logo candidate — confirm against official source and complete usage review before promotion.";
    } else {
      classification = AUDIT_CLASSIFICATION.VALID;
      visualSlotValidationStatus = "Valid for Slot";
      notes = "Logo confirmed against official source and usage-reviewed.";
    }
  } else {
    // Hero / Gallery / Value driver / Recent openings — need property + CALA context.
    const missingProperty = checks[C.propertyMatch]?.value === false || !derived.hasNamedProperty;
    const missingCala = slotDef.prefersCala && !derived.cala;
    if (derived.isIconOrAbstract) {
      classification = AUDIT_CLASSIFICATION.NOT_PROPERTY;
      visualSlotValidationStatus = "Not Enough Context";
      recommendedExplorerUsePermission = "Candidate Only";
      notes = "URL resolves to an icon/favicon/abstract asset — not a usable hotel/property image.";
    } else if (missingProperty) {
      classification = AUDIT_CLASSIFICATION.NOT_ENOUGH_CONTEXT;
      visualSlotValidationStatus = "Needs Property Confirmation";
      recommendedExplorerUsePermission = "Candidate Only";
      notes = missingCala
        ? "Confirm this is a real, named Tribute hotel/property image; prefer a CALA-relevant property for CALA-focused profile."
        : "Confirm this is a real, named Tribute hotel/property image before Explorer use.";
    } else if (missingCala) {
      classification = AUDIT_CLASSIFICATION.NOT_CALA;
      visualSlotValidationStatus = "Needs CALA Property";
      recommendedExplorerUsePermission = "Candidate Only";
      notes = "Named Tribute property confirmed, but not CALA-relevant; prefer CALA property for CALA-focused profile.";
    } else if (failed.length === 0) {
      classification = AUDIT_CLASSIFICATION.VALID;
      visualSlotValidationStatus = "Valid for Slot";
      notes = "Meets slot requirements pending final human approval (auto-approve blocked).";
    } else {
      classification = AUDIT_CLASSIFICATION.NEEDS_USAGE_REVIEW;
      visualSlotValidationStatus = "Needs Usage Review";
      notes = `Outstanding checks: ${failed.join(", ")}.`;
    }
    // Never auto-approve: cap at Candidate/Needs review.
    if (recommendedExplorerUsePermission === "Approved For Explorer") {
      recommendedExplorerUsePermission = "Candidate Only";
    }
  }

  return {
    recordId: record.id || null,
    assetName: record.assetName,
    assetType: record.assetType,
    currentAssetStatus: status,
    currentExplorerUsePermission: nz(record.explorerUsePermission),
    mappedVisualSlot: slot,
    explorerSection: slotDef.explorerSection,
    classification,
    visualSlotValidationStatus,
    calaRelevant: derived.cala ? "Yes" : "Unknown",
    brandConfirmed: derived.brandMatch ? "Yes" : "Unknown",
    propertyConfirmed:
      slotDef.needsPropertyContext ? (checks[C.propertyMatch]?.value ? "Yes" : "No") : "Unknown",
    failedRequiredChecks: failed,
    recommendedAssetStatus,
    recommendedExplorerUsePermission,
    visualSlotValidationNotes: notes,
    sourceUrl: record.sourceUrl || null,
    checks,
  };
}

/* ------------------------------------------------------------------ */
/* Status correction writer v2                                         */
/* ------------------------------------------------------------------ */

function getRegistryTableName() {
  return (
    process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID ||
    BRAND_ASSET_REGISTRY_TABLE
  );
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

export async function listRegistryRecordsRaw(brandRecordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const formula = `{${MAP_BRAND_ASSET.brandRecordId}}='${brandRecordId.replace(/'/g, "\\'")}'`;
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const url = `${registryDataUrl(baseId)}?${params.toString()}`;
    const { res, json } = await registryDataFetch(url, apiKey);
    if (!res.ok) {
      throw new Error(json.error?.message || `Airtable list registry failed: ${res.status}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
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

function isOfficialTributeSource(record) {
  const basis = nz(record.sourceBasis);
  const url = nz(record.sourceUrl);
  const page = nz(record.sourcePageUrl);
  return (
    basis === "Marriott-Controlled Source" &&
    (isTributeBrandUrl(url) || isTributeBrandUrl(page))
  );
}

function isDuplicateGalleryCrop(record, allRecords) {
  const url = nz(record.sourceUrl);
  if (!url || !/property\/design image/i.test(nz(record.assetName))) return false;
  const base = url.replace(/-\d+x\d+(?=\.\w+$)/, "");
  const siblings = allRecords.filter(
    (r) =>
      r.id !== record.id &&
      /property\/design image/i.test(nz(r.assetName)) &&
      nz(r.sourceUrl).replace(/-\d+x\d+(?=\.\w+$)/, "") === base
  );
  return siblings.length > 0;
}

/**
 * Build per-record slot-governance + safe status correction payload (v2).
 */
export function buildStatusCorrectionForRecord(record, allRecords = []) {
  const name = nz(record.assetName);
  const audit = auditRecord(record);
  const slot = audit.mappedVisualSlot;
  const slotDef = VISUAL_SLOT_DEFINITIONS.find((d) => d.slot === slot);

  const slotFields = {};
  const coreFields = {};
  let ruleId = "generic";

  const setSlot = (key, value) => {
    if (value != null && value !== "") slotFields[MAP_VISUAL_SLOT[key]] = value;
  };
  const setCore = (key, value) => {
    if (value != null && value !== "") coreFields[MAP_BRAND_ASSET[key]] = value;
  };

  if (/existing logo.*unconfirmed/i.test(name)) {
    ruleId = "existing-logo-unconfirmed";
    setSlot("explorerSection", VISUAL_SLOT.LOGO);
    setSlot("slotPurpose", "Brand identity confirmation");
    setSlot("brandConfirmed", "Unknown");
    setSlot("propertyConfirmed", "Unknown");
    setSlot("calaRelevant", "Unknown");
    setSlot("sourcePageConfirmsContext", record.sourcePageUrl ? "Unknown" : "No");
    setSlot("useCaseMatch", "None");
    setSlot("validationStatus", "Needs Usage Review");
    setSlot(
      "validationNotes",
      "Existing Brand Setup logo present, but source/rights not confirmed against official Tribute source."
    );
    setCore("assetStatus", "Needs Usage Review");
    setCore("explorerUsePermission", "Candidate Only");
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.PENDING);
  } else if (/tribute-black\.svg/i.test(name)) {
    ruleId = "tribute-black-svg";
    setSlot("explorerSection", VISUAL_SLOT.LOGO);
    setSlot("slotPurpose", "Brand identity confirmation");
    setSlot("brandConfirmed", isOfficialTributeSource(record) ? "Yes" : "Unknown");
    setSlot("propertyConfirmed", "Unknown");
    setSlot("calaRelevant", "Unknown");
    setSlot("sourcePageConfirmsContext", record.sourcePageUrl ? "Yes" : "Unknown");
    setSlot("useCaseMatch", "None");
    setSlot("validationStatus", "Needs Usage Review");
    setSlot(
      "validationNotes",
      "Official logo candidate; confirm usage and match against current Brand Setup logo before Explorer approval."
    );
    setCore("assetStatus", "Needs Usage Review");
    setCore("explorerUsePermission", "Candidate Only");
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.PENDING);
  } else if (/hero.*consumer property wide/i.test(name)) {
    ruleId = "hero-candidate";
    setSlot("explorerSection", VISUAL_SLOT.HERO);
    setSlot("slotPurpose", "Primary brand-level property visual");
    setSlot("brandConfirmed", isOfficialTributeSource(record) ? "Yes" : "Unknown");
    setSlot("propertyConfirmed", "No");
    setSlot("calaRelevant", "No");
    setSlot("sourcePageConfirmsContext", "No");
    setSlot("useCaseMatch", "None");
    setSlot("validationStatus", "Not Enough Context");
    setSlot(
      "validationNotes",
      "Not approved for hero because image is not confirmed as a named Tribute Portfolio hotel/property and not CALA-relevant."
    );
    setCore("assetStatus", "Candidate");
    setCore("explorerUsePermission", "Candidate Only");
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.PENDING);
  } else if (/property\/design image/i.test(name)) {
    const dupNote = isDuplicateGalleryCrop(record, allRecords)
      ? " Image may be a duplicate/generic crop of the same source asset."
      : "";
    ruleId = `gallery-${name.match(/image (\d)/i)?.[1] || "x"}`;
    setSlot("explorerSection", VISUAL_SLOT.GALLERY);
    setSlot("slotPurpose", "Gallery of different real Tribute Portfolio hotels");
    setSlot("brandConfirmed", isOfficialTributeSource(record) ? "Yes" : "Unknown");
    setSlot("propertyConfirmed", "No");
    setSlot("calaRelevant", "No");
    setSlot("sourcePageConfirmsContext", "No");
    setSlot("useCaseMatch", "None");
    setSlot("validationStatus", "Not Enough Context");
    setSlot(
      "validationNotes",
      `Not approved for gallery because image is not confirmed as a named Tribute Portfolio hotel/property and may be duplicate/generic crop.${dupNote}`
    );
    setCore("assetStatus", "Candidate");
    setCore("explorerUsePermission", "Candidate Only");
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.PENDING);
  } else if (/FDD/i.test(name)) {
    ruleId = "fdd-reference";
    setSlot("explorerSection", VISUAL_SLOT.BRAND_STANDARDS);
    setSlot("slotPurpose", "Source/PDF reference");
    setSlot("brandConfirmed", "Yes");
    setSlot("propertyConfirmed", "Unknown");
    setSlot("calaRelevant", "Unknown");
    setSlot("sourcePageConfirmsContext", "Unknown");
    setSlot("useCaseMatch", "None");
    setSlot("validationStatus", "Source Reference Only");
    setSlot(
      "validationNotes",
      "Valid as internal/source-backed reference; not a visual image asset."
    );
    setCore("assetStatus", "Source-Confirmed");
    setCore("explorerUsePermission", "Internal Only");
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.COMPLETE);
  } else if (/newsroom.*PR|PR\/openings/i.test(name)) {
    ruleId = "newsroom-pr-placeholder";
    setSlot("explorerSection", VISUAL_SLOT.PR_LINK);
    setSlot("slotPurpose", "Official PR/opening evidence");
    setSlot("brandConfirmed", "Yes");
    setSlot("propertyConfirmed", "Unknown");
    setSlot("calaRelevant", "Unknown");
    setSlot("sourcePageConfirmsContext", "No");
    setSlot("useCaseMatch", "None");
    setSlot("validationStatus", "Provenance Only");
    setSlot(
      "validationNotes",
      "JS-shell/static extraction gap; do not use until Rendered Source Capture v1."
    );
    setCore("assetStatus", "Do Not Use");
    setCore("explorerUsePermission", "Do Not Use");
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.BLOCKED);
  } else if (/Mock\/Demo hero/i.test(name)) {
    ruleId = "mock-demo-hero-guard";
    setSlot("explorerSection", VISUAL_SLOT.HERO);
    setSlot("slotPurpose", "Prevent accidental promotion of mock/demo asset");
    setSlot("brandConfirmed", "No");
    setSlot("propertyConfirmed", "No");
    setSlot("calaRelevant", "No");
    setSlot("sourcePageConfirmsContext", "No");
    setSlot("useCaseMatch", "None");
    setSlot("validationStatus", "Mock/Demo Guard");
    setSlot(
      "validationNotes",
      "Existing demo hero should not be promoted or treated as approved asset."
    );
    setCore("assetStatus", "Mock/Demo");
    setCore("explorerUsePermission", "Do Not Use");
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.BLOCKED);
  } else {
    ruleId = "generic-audit-fallback";
    setSlot("explorerSection", slot);
    setSlot("slotPurpose", slotDef?.purpose || "");
    setSlot("validationStatus", audit.visualSlotValidationStatus);
    setSlot("validationNotes", audit.visualSlotValidationNotes);
    setSlot("brandConfirmed", audit.brandConfirmed);
    setSlot("propertyConfirmed", audit.propertyConfirmed);
    setSlot("calaRelevant", audit.calaRelevant);
    setSlot("useCaseMatch", "None");
    setCore("assetStatus", audit.recommendedAssetStatus);
    setCore("explorerUsePermission", audit.recommendedExplorerUsePermission);
    setCore("usageReviewStatus", VAL_USAGE_REVIEW_FOR_WRITER.PENDING);
  }

  const proposedFields = { ...slotFields, ...coreFields };

  return {
    recordId: record.id,
    assetName: name,
    ruleId,
    mappedVisualSlot: slot,
    classification: audit.classification,
    proposedFields,
    slotFieldNames: Object.keys(slotFields),
    coreFieldNames: Object.keys(coreFields),
    audit,
  };
}

export function validateStatusCorrectionPayload(fields) {
  const errors = [];
  if (fields[MAP_BRAND_ASSET.explorerUsePermission] === "Approved For Explorer") {
    errors.push("Explorer Use Permission Approved For Explorer is not allowed in v2 writer");
  }
  if (fields[MAP_BRAND_ASSET.assetStatus] === "Approved For Explorer Use") {
    errors.push("Asset Status Approved For Explorer Use is not allowed in v2 writer");
  }
  if (fields[MAP_BRAND_ASSET.companyValidated]) {
    errors.push("Company Validated must not be set");
  }
  if (fields[MAP_BRAND_ASSET.companyValidationDate]) {
    errors.push("Company Validation Date must not be set");
  }
  if (fields[MAP_BRAND_ASSET.attachment]) {
    errors.push("Attachment must not be set in v2 metadata-only writer");
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
  return cur === prop;
}

function correctionNeedsUpdate(rawRecord, proposedFields) {
  const f = rawRecord.fields || {};
  return Object.entries(proposedFields).some(([key, value]) => !fieldValuesEqual(f[key], value));
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

export async function applyStatusCorrections(rawRecords, { apply = false } = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const normalized = rawRecords.map((r) => ({
    id: r.id,
    ...normalizeRegistryAssetRecordForAudit(r),
  }));

  const plans = normalized.map((r) => buildStatusCorrectionForRecord(r, normalized));
  const proposed = [];
  const skipped = [];
  const validationErrors = [];

  for (const plan of plans) {
    const raw = rawRecords.find((r) => r.id === plan.recordId);
    const validation = validateStatusCorrectionPayload(plan.proposedFields);
    if (!validation.valid) {
      validationErrors.push({ assetName: plan.assetName, errors: validation.errors });
      continue;
    }
    if (!correctionNeedsUpdate(raw, plan.proposedFields)) {
      skipped.push({
        recordId: plan.recordId,
        assetName: plan.assetName,
        reason: "already matches proposed correction",
      });
      continue;
    }
    proposed.push({
      recordId: plan.recordId,
      assetName: plan.assetName,
      ruleId: plan.ruleId,
      fields: plan.proposedFields,
      slotFieldNames: plan.slotFieldNames,
      coreFieldNames: plan.coreFieldNames,
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

  const candidateOnly = plans.filter(
    (p) => p.proposedFields[MAP_BRAND_ASSET.explorerUsePermission] === "Candidate Only"
  );
  const doNotUse = plans.filter(
    (p) => p.proposedFields[MAP_BRAND_ASSET.explorerUsePermission] === "Do Not Use"
  );
  const internalOnly = plans.filter(
    (p) => p.proposedFields[MAP_BRAND_ASSET.explorerUsePermission] === "Internal Only"
  );

  return {
    recordsScanned: plans.length,
    plans,
    proposed,
    skipped,
    updated: apply ? updated : [],
    validationErrors,
    candidateOnly: candidateOnly.map((p) => p.assetName),
    doNotUse: doNotUse.map((p) => p.assetName),
    internalOnly: internalOnly.map((p) => p.assetName),
  };
}

function normalizeRegistryAssetRecordForAudit(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    assetName: nz(f[MAP_BRAND_ASSET.assetName]),
    assetType: nz(f[MAP_BRAND_ASSET.assetType]),
    assetStatus: nz(f[MAP_BRAND_ASSET.assetStatus]),
    sourceBasis: nz(f[MAP_BRAND_ASSET.sourceBasis]),
    sourceUrl: nz(f[MAP_BRAND_ASSET.sourceUrl]),
    sourcePageUrl: nz(f[MAP_BRAND_ASSET.sourcePageUrl]),
    usageReviewStatus: nz(f[MAP_BRAND_ASSET.usageReviewStatus]),
    explorerUsePermission: nz(f[MAP_BRAND_ASSET.explorerUsePermission]),
    recommendedExplorerSlot: nz(f[MAP_BRAND_ASSET.recommendedExplorerSlot]),
    slotGovernance: readSlotGovernanceFromFields(f),
  };
}

/* ------------------------------------------------------------------ */
/* Report builder                                                      */
/* ------------------------------------------------------------------ */

export async function buildVisualSlotRequirementsReport({
  brandKey = "tribute-portfolio",
  applySchema = false,
  schemaApproved = false,
  applyStatus = false,
  statusApproved = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) {
    return { error: "AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required." };
  }

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey];
  if (!pilot) {
    return { error: `Unknown brand key: ${brandKey}. Known: ${Object.keys(BRAND_ASSET_PILOT_CONFIG).join(", ")}` };
  }

  let mode = "dry-run";
  if (applySchema && schemaApproved) mode = "schema-apply";
  else if (applyStatus && statusApproved) mode = "status-apply";

  let airtableModified = false;
  let brandSetupMediaUntouched = true;

  let registryRecords = [];
  let rawRegistryRecords = [];
  let registryReadError = null;
  try {
    rawRegistryRecords = await listRegistryRecordsRaw(pilot.recordId);
    registryRecords = rawRegistryRecords.map(normalizeRegistryAssetRecordForAudit);
  } catch (err) {
    registryReadError = err.message;
    try {
      registryRecords = await listRegistryAssetsForBrand(pilot.recordId);
    } catch {
      /* keep registryReadError */
    }
  }

  // Registry field sufficiency — compare existing columns to slot-governance needs.
  const existingFields = await inspectRegistryFields(baseId, token);
  const existingFieldSet = new Set(existingFields.fields);
  const missingProposedFields = PROPOSED_VISUAL_SLOT_FIELDS.filter((f) => !existingFieldSet.has(f.name));
  const schemaSufficient = missingProposedFields.length === 0;

  let schemaApplyResult = null;
  if (applySchema && schemaApproved) {
    if (!existingFields.tableId) {
      return { error: "Brand Asset Registry table not found; run registry schema apply first." };
    }
    schemaApplyResult = await applyVisualSlotSchema(baseId, token, existingFields.tableId, existingFieldSet);
    airtableModified = schemaApplyResult.fieldsCreated.length > 0;
  }

  const audits = registryRecords.map(auditRecord);

  let statusWriter = null;
  if (rawRegistryRecords.length) {
    statusWriter = await applyStatusCorrections(rawRegistryRecords, {
      apply: applyStatus && statusApproved,
    });
    if (applyStatus && statusApproved && statusWriter.updated.length > 0) {
      airtableModified = true;
    }
  }

  // Slot coverage — which visual slots have at least one Valid record.
  const slotCoverage = VISUAL_SLOT_DEFINITIONS.map((def) => {
    const forSlot = audits.filter((a) => a.mappedVisualSlot === def.slot);
    const valid = forSlot.filter((a) => a.classification === AUDIT_CLASSIFICATION.VALID);
    const candidates = forSlot.filter((a) => a.classification !== AUDIT_CLASSIFICATION.VALID);
    let status = "Missing";
    if (valid.length) status = "Covered";
    else if (candidates.length) status = "Candidate Only";
    return {
      slot: def.slot,
      explorerSection: def.explorerSection,
      status,
      recordCount: forSlot.length,
      validCount: valid.length,
    };
  });

  const invalidAssets = audits.filter(
    (a) =>
      a.classification === AUDIT_CLASSIFICATION.NOT_ENOUGH_CONTEXT ||
      a.classification === AUDIT_CLASSIFICATION.NOT_PROPERTY ||
      a.classification === AUDIT_CLASSIFICATION.NOT_CALA ||
      a.classification === AUDIT_CLASSIFICATION.WRONG_SLOT ||
      a.classification === AUDIT_CLASSIFICATION.DO_NOT_USE
  );
  const logoOrSourceValid = audits.filter(
    (a) =>
      (a.mappedVisualSlot === VISUAL_SLOT.LOGO || a.mappedVisualSlot === VISUAL_SLOT.BRAND_STANDARDS) &&
      (a.classification === AUDIT_CLASSIFICATION.VALID ||
        a.classification === AUDIT_CLASSIFICATION.NEEDS_USAGE_REVIEW)
  );
  const needsCalaReplacement = audits.filter(
    (a) =>
      (a.mappedVisualSlot === VISUAL_SLOT.HERO ||
        a.mappedVisualSlot === VISUAL_SLOT.GALLERY ||
        a.mappedVisualSlot === VISUAL_SLOT.VALUE_DRIVER) &&
      (a.calaRelevant !== "Yes")
  );

  const missingSlots = slotCoverage.filter((s) => s.status === "Missing").map((s) => s.slot);

  const statusCorrections = audits
    .filter(
      (a) =>
        a.recommendedAssetStatus !== a.currentAssetStatus ||
        a.recommendedExplorerUsePermission !== a.currentExplorerUsePermission
    )
    .map((a) => ({
      recordId: a.recordId,
      assetName: a.assetName,
      from: { assetStatus: a.currentAssetStatus, explorerUsePermission: a.currentExplorerUsePermission },
      to: {
        assetStatus: a.recommendedAssetStatus,
        explorerUsePermission: a.recommendedExplorerUsePermission,
        visualSlotValidationStatus: a.visualSlotValidationStatus,
      },
    }));

  const schemaApplyCommand =
    "npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --apply --approve-brand-visual-slot-schema";
  const statusApplyCommand =
    "npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --apply --approve-brand-visual-slot-status";

  return {
    requirementsVersion: REQUIREMENTS_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified,
    brandSetupMediaUntouched,
    brand: { key: brandKey, recordId: pilot.recordId, name: pilot.brandName, parentCompany: pilot.parentCompany },
    textGovernanceStatus: {
      note: "Text/governance status is owned by the Tribute package pipeline; this module does not change it.",
      textGovernancePlatformReady: true,
    },
    registryReadError,
    registryRecordCount: registryRecords.length,
    statusWriter: statusWriter
      ? {
          recordsScanned: statusWriter.recordsScanned,
          recordsProposed: statusWriter.proposed.length,
          recordsUpdated: statusWriter.updated.length,
          recordsSkipped: statusWriter.skipped.length,
          recordsMarkedCandidateOnly: statusWriter.candidateOnly,
          recordsMarkedDoNotUse: statusWriter.doNotUse,
          recordsMarkedInternalOnly: statusWriter.internalOnly,
          validationErrors: statusWriter.validationErrors,
          proposed: statusWriter.proposed.map((p) => ({
            recordId: p.recordId,
            assetName: p.assetName,
            ruleId: p.ruleId,
            slotFields: p.slotFieldNames,
            coreFields: p.coreFieldNames,
            fields: p.fields,
          })),
          skipped: statusWriter.skipped,
          updated: statusWriter.updated,
        }
      : null,
    visualSlots: VISUAL_SLOT_DEFINITIONS,
    validationChecks: Object.values(VALIDATION_CHECK),
    registrySchema: {
      tableName: BRAND_ASSET_REGISTRY_TABLE,
      tableId: existingFields.tableId,
      existingFieldCount: existingFields.fields.length,
      sufficientForSlotGovernance: schemaSufficient,
      missingProposedFields: missingProposedFields.map((f) => f.name),
      proposedFields: PROPOSED_VISUAL_SLOT_FIELDS,
    },
    schemaApplyResult,
    audits,
    slotCoverage,
    missingSlots,
    invalidAssets: invalidAssets.map((a) => ({
      assetName: a.assetName,
      mappedVisualSlot: a.mappedVisualSlot,
      classification: a.classification,
      reason: a.visualSlotValidationNotes,
      failedRequiredChecks: a.failedRequiredChecks,
    })),
    logoOrSourceValid: logoOrSourceValid.map((a) => ({
      assetName: a.assetName,
      mappedVisualSlot: a.mappedVisualSlot,
      classification: a.classification,
    })),
    needsCalaHotelReplacement: needsCalaReplacement.map((a) => ({
      assetName: a.assetName,
      mappedVisualSlot: a.mappedVisualSlot,
      calaRelevant: a.calaRelevant,
    })),
    recommendedStatusCorrections: statusCorrections,
    autoApproveBlocked: true,
    doesNotDo: [
      "Download images",
      "Approve image assets for Explorer use automatically",
      "Overwrite Brand Setup logo/hero/image/media fields",
      "Replace the Mock/Demo hero",
      "Set Company Validated or Company Validation Date",
      "Imply Marriott validated the assets",
    ],
    schemaApplyCommand: schemaSufficient ? null : schemaApplyCommand,
    statusCorrectionApplyCommand:
      statusWriter && (statusWriter.proposed.length || mode === "status-apply")
        ? statusApplyCommand
        : statusCorrections.length
          ? statusApplyCommand
          : null,
    nextCommand:
      statusWriter && statusWriter.proposed.length && mode !== "status-apply"
        ? statusApplyCommand
        : "npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --dry-run",
    remainingWorkToVisualParity: [
      "Capture and confirm named Tribute hotel/property images (hero + 3–6 gallery), ideally CALA-relevant.",
      "Populate value-driver-matched imagery for 'Where This Brand Creates the Most Value'.",
      "Capture specific opening/property images + dates for Recent Openings.",
      "Rendered Source Capture v1 for news.marriott.com PR/openings.",
      "Confirm logo against official source and complete usage review.",
      "Future v3: asset download + Explorer hero/logo promotion writer with staging.",
    ],
  };
}

/* ------------------------------------------------------------------ */
/* Airtable meta helpers (schema inspect + gated apply)                */
/* ------------------------------------------------------------------ */

async function metaFetch(baseId, token, metaPath, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${metaPath}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function inspectRegistryFields(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    throw new Error(`Meta list tables failed ${res.status}: ${JSON.stringify(json)}`);
  }
  const table = (json.tables || []).find((t) => t.name === BRAND_ASSET_REGISTRY_TABLE);
  return {
    tableId: table?.id || null,
    fields: (table?.fields || []).map((f) => f.name),
  };
}

function buildFieldSpec(proposed) {
  if (proposed.type === "singleSelect") {
    let options;
    if (proposed.name === "Visual Slot Validation Status") options = VAL_VISUAL_SLOT_VALIDATION_STATUS;
    else if (proposed.name === "CALA Relevant?") options = VAL_CALA_RELEVANT;
    else if (proposed.name === "Related Value Driver")
      options = ["Urban", "Resort", "Conversion / Adaptive Reuse", "Boutique / Lifestyle", "Mixed-Use", "None"];
    else if (proposed.name === "Use Case Match")
      options = ["Urban", "Resort", "Conversion / Adaptive Reuse", "Boutique / Lifestyle", "Mixed-Use", "None"];
    else options = VAL_YES_NO_UNKNOWN;
    return {
      name: proposed.name,
      type: "singleSelect",
      options: { choices: options.map((name) => ({ name })) },
      description: proposed.note,
    };
  }
  return { name: proposed.name, type: proposed.type, description: proposed.note };
}

async function applyVisualSlotSchema(baseId, token, tableId, existingFieldSet) {
  const result = { fieldsCreated: [], fieldsSkipped: [] };
  for (const proposed of PROPOSED_VISUAL_SLOT_FIELDS) {
    if (existingFieldSet.has(proposed.name)) {
      result.fieldsSkipped.push(proposed.name);
      continue;
    }
    const spec = buildFieldSpec(proposed);
    const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
      method: "POST",
      body: JSON.stringify(spec),
    });
    if (!res.ok) throw new Error(`Create field ${proposed.name} failed: ${JSON.stringify(json)}`);
    result.fieldsCreated.push(proposed.name);
  }
  return result;
}

/* ------------------------------------------------------------------ */
/* Markdown                                                            */
/* ------------------------------------------------------------------ */

export function buildVisualSlotRequirementsMarkdown(report) {
  if (report.error) {
    return `# Brand Explorer Visual Slot Requirements v2\n\nError: ${report.error}\n`;
  }

  const lines = [
    "# Brand Explorer Visual Slot Requirements v2",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Brand: ${report.brand.name} \`${report.brand.recordId}\``,
    `Text/governance Platform Ready: **${report.textGovernanceStatus.textGovernancePlatformReady ? "yes" : "no"}** (unchanged by this module)`,
    `Brand Setup media untouched: **${report.brandSetupMediaUntouched ? "yes" : "no"}**`,
    "",
    "## 1. Brand record",
    "",
    `- Brand: ${report.brand.name} \`${report.brand.recordId}\``,
    `- Parent: ${report.brand.parentCompany}`,
    `- Registry records audited: **${report.registryRecordCount}**`,
    ...(report.registryReadError ? [`- Registry read error: ${report.registryReadError}`] : []),
    "",
    "## 2. Visual slot definitions",
    "",
    ...report.visualSlots.flatMap((s) => [
      `### ${s.slot}`,
      `- Section: \`${s.explorerSection}\``,
      `- Purpose: ${s.purpose}`,
      `- Requirements:`,
      ...s.requirements.map((r) => `  - ${r}`),
      `- Required checks: ${s.requiredChecks.join(", ")}`,
      "",
    ]),
    "## 3. Validation checks",
    "",
    ...report.validationChecks.map((c) => `- \`${c}\``),
    "",
    "## 4. Registry schema sufficiency",
    "",
    `- Table: \`${report.registrySchema.tableName}\`${report.registrySchema.tableId ? ` (\`${report.registrySchema.tableId}\`)` : ""}`,
    `- Existing fields: ${report.registrySchema.existingFieldCount}`,
    `- Sufficient for slot governance: **${report.registrySchema.sufficientForSlotGovernance ? "yes" : "no"}**`,
    ...(report.registrySchema.missingProposedFields.length
      ? [`- Missing proposed fields: ${report.registrySchema.missingProposedFields.join(", ")}`]
      : []),
    "",
    "### Proposed new fields",
    "",
    ...report.registrySchema.proposedFields.map((f) => `- **${f.name}** (${f.type}) — ${f.note}`),
    "",
    ...(report.schemaApplyResult
      ? [
          "### Schema apply result",
          "",
          `- Fields created: ${report.schemaApplyResult.fieldsCreated.join(", ") || "none"}`,
          `- Fields skipped: ${report.schemaApplyResult.fieldsSkipped.join(", ") || "none"}`,
          "",
        ]
      : []),
    ...(report.statusWriter
      ? [
          "## 5. Status correction writer v2",
          "",
          `- Records scanned: **${report.statusWriter.recordsScanned}**`,
          `- Records proposed: **${report.statusWriter.recordsProposed}**`,
          `- Records updated: **${report.statusWriter.recordsUpdated}**`,
          `- Records skipped: **${report.statusWriter.recordsSkipped}**`,
          `- Candidate Only: ${report.statusWriter.recordsMarkedCandidateOnly.join(", ") || "none"}`,
          `- Do Not Use: ${report.statusWriter.recordsMarkedDoNotUse.join(", ") || "none"}`,
          `- Internal Only: ${report.statusWriter.recordsMarkedInternalOnly.join(", ") || "none"}`,
          "",
          "### Proposed corrections",
          "",
          ...report.statusWriter.proposed.map(
            (p) =>
              `- **${p.assetName}** (\`${p.ruleId}\`) — slot fields: ${p.slotFields.join(", ") || "none"}; core: ${p.coreFields.join(", ") || "none"}`
          ),
          ...(report.statusWriter.proposed.length === 0 ? ["- None (all records already match)"] : []),
          "",
        ]
      : []),
    "## 6. Audit of existing asset records",
    "",
    "| Asset | Slot | Classification | Validation Status | CALA | Rec. Status | Rec. Explorer Use |",
    "|-------|------|----------------|-------------------|------|-------------|-------------------|",
    ...report.audits.map(
      (a) =>
        `| ${a.assetName} | ${a.mappedVisualSlot} | ${a.classification} | ${a.visualSlotValidationStatus} | ${a.calaRelevant} | ${a.recommendedAssetStatus} | ${a.recommendedExplorerUsePermission} |`
    ),
    "",
    "## 7. Invalid for Explorer visual use",
    "",
    ...(report.invalidAssets.length
      ? report.invalidAssets.map((a) => `- **${a.assetName}** (${a.mappedVisualSlot}) — ${a.classification}: ${a.reason}`)
      : ["- None"]),
    "",
    "## 8. Valid only for logo / PDF / source reference",
    "",
    ...(report.logoOrSourceValid.length
      ? report.logoOrSourceValid.map((a) => `- **${a.assetName}** (${a.mappedVisualSlot}) — ${a.classification}`)
      : ["- None"]),
    "",
    "## 9. Needs CALA hotel/property replacement",
    "",
    ...(report.needsCalaHotelReplacement.length
      ? report.needsCalaHotelReplacement.map((a) => `- **${a.assetName}** (${a.mappedVisualSlot}) — CALA relevant: ${a.calaRelevant}`)
      : ["- None"]),
    "",
    "## 10. Slot coverage",
    "",
    "| Slot | Status | Records | Valid |",
    "|------|--------|---------|-------|",
    ...report.slotCoverage.map(
      (s) => `| ${s.slot} | ${s.status} | ${s.recordCount} | ${s.validCount} |`
    ),
    "",
    `**Missing slots:** ${report.missingSlots.length ? report.missingSlots.join(", ") : "none"}`,
    "",
    "## 11. Recommended status corrections",
    "",
    ...(report.recommendedStatusCorrections.length
      ? report.recommendedStatusCorrections.map(
          (c) =>
            `- **${c.assetName}**: ${c.from.assetStatus} → ${c.to.assetStatus}; Explorer ${c.from.explorerUsePermission || "—"} → ${c.to.explorerUsePermission} (${c.to.visualSlotValidationStatus})`
        )
      : ["- None"]),
    "",
    "## 12. Airtable modified",
    "",
    `**${report.airtableModified ? "Yes" : "No"}**`,
    "",
    "## 13. Schema apply command",
    "",
    report.schemaApplyCommand
      ? `\`\`\`bash\n${report.schemaApplyCommand}\n\`\`\``
      : "_Registry schema already sufficient for slot governance._",
    "",
    "## 14. Status-correction apply command",
    "",
    report.statusCorrectionApplyCommand
      ? `\`\`\`bash\n${report.statusCorrectionApplyCommand}\n\`\`\``
      : "_No status corrections proposed._",
    "",
    "## 15. Remaining work to visual parity (Kimpton / Radisson Blu)",
    "",
    ...report.remainingWorkToVisualParity.map((w) => `- ${w}`),
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
  ];

  return `${lines.join("\n")}\n`;
}
