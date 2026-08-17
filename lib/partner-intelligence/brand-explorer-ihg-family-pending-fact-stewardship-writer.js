/**
 * Brand Explorer IHG-Family Pending Fact Stewardship Writer v30B.
 *
 * Triages Kimpton Hotels pending Explorer facts after v30A / v30A-R1 visual repairs.
 * Approves only simple source-backed identity/loyalty labels; rejects/archives thin,
 * duplicated, FDD-sensitive, and presentation-superseded extracts as Internal Only.
 *
 * @see docs/data-intelligence/brand-explorer-ihg-family-pending-fact-stewardship-writer-v30B.md
 */
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { TARGET_BRANDS } from "./brand-explorer-ihg-family-active-profile-repair-writer.js";
import {
  MAP_BRAND_RESIDENCES,
  buildResidencesApiShape,
} from "../brand-explorer/brand-residences-api-shape.js";
import { classifyBrandResidencesMigration } from "./brand-residences-legacy-migration-writer.js";
import Airtable from "airtable";

export const WRITER_VERSION = "30B";
export const REPORT_JSON_NAME = "brand-explorer-ihg-family-pending-fact-stewardship-writer.json";
export const REPORT_MD_NAME = "brand-explorer-ihg-family-pending-fact-stewardship-writer.md";
export const DOC_MD_NAME = "brand-explorer-ihg-family-pending-fact-stewardship-writer-v30B.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v30B-ihg-family-pending-fact-stewardship";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-kimpton-fact-governance";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const STEWARDSHIP_TAG = "v30B-ihg-family-pending-fact-stewardship";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "ascend",
  "radisson",
  "radisson-blu",
  ...WAVE1_EXPANSION_SLUGS,
]);

const PROJECT_FIT_TABLE = "Brand Setup - Project Fit";
const LEGACY_RESIDENCES_FIELD = "Branded Residences Allowed";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-ihg-family-active-profile-repair-writer.md",
  "reports/brand-explorer-ihg-family-active-profile-repair-writer.json",
  "reports/brand-explorer-kimpton-context-sort-idempotency-repair-writer.md",
  "reports/brand-explorer-kimpton-context-sort-idempotency-repair-writer.json",
  "reports/brand-explorer-complete-build-kimpton.md",
  "reports/brand-explorer-complete-build-kimpton.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-radisson-pending-fact-stewardship-writer.md",
  "reports/brand-explorer-choice-family-pending-fact-stewardship-writer.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/airtable-facts.js",
  "lib/partner-intelligence/airtable-source.js",
  "live Kimpton Partner Facts",
  "live Kimpton Source Library records",
  "live Kimpton Brand Explorer Presentation rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-ihg-family-pending-fact-stewardship-writer.js",
  "scripts/brand-explorer-ihg-family-pending-fact-stewardship-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by ihg|approved by ihg|brand approved|company-approved|company approved|official sign-off/i;

const INTERNAL_SURFACE_RE =
  /\b(source capture|internal extraction|paste into airtable|franchise disclosure document|item\s*19|fdd\b)\b/i;

const FDD_FRAGMENT_RE =
  /\b(ota|gso|loyalty direct|direct connect|gross room revenues|item\s*19|distribution agreements)\b/i;

const SIMPLE_LOYALTY_PROGRAM_RE = /^ihg one rewards(?:®|™)?$/i;
const SIMPLE_BRAND_NAME_RE = /^kimpton hotels$/i;
const SIMPLE_PARENT_COMPANY_RE = /^ihg hotels & resorts$/i;

/** Presentation slots that already render owner-facing copy for each fact key. */
const SUPERSEDED_BY_PRESENTATION = {
  "be.footprint.geoIntro": "footprint.geo_intro",
  "be.overview.whyValue": "overview.why_value",
  "be.overview.typicalUseCase": "overview.typical_use_case",
  "be.overview.scenarios": "valueOwners.scenario.1",
  "be.commercial.intro": "commercial.theme",
  "be.positioning.summary": "overview.positioning",
  "be.positioning.history": "overview.history",
  "be.positioning.guestPromise": "overview.guest_promise",
  "be.positioning.tagline": "overview.tagline",
  "be.loyalty.programName": "loyalty.hero_title",
  "be.overview.developmentModel": "overview.development_model",
};

const NUMERIC_FOOTPRINT_FIELDS = new Set([
  "be.footprint.globalHotels",
  "be.footprint.globalRooms",
  "be.footprint.globalPipeline",
  "be.footprint.americasHotels",
]);

const FDD_METRIC_FIELDS = new Set([
  "be.economics.royaltyPct",
  "be.economics.initialFranchiseFee",
  "be.loyalty.memberCount",
  "be.loyalty.roomContributionPct",
  "be.loyalty.enterpriseBookingPct",
]);

const META_INTERNAL_FIELDS = new Set([
  "be.meta.overallSourceConfidence",
  "be.meta.lastReviewedDate",
]);

const APPROVE_ONCE_FIELDS = new Set([
  "be.identity.brandName",
  "be.identity.parentCompany",
  "be.loyalty.programName",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

async function fetchBrandApiShape(brandId) {
  const req = { query: { brandId, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status() {
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function presentationBodyForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  const row = blocks.find((b) => nz(b.slotKey) === slotKey);
  return nz(row?.body) || nz(row?.title);
}

function scenarioPresentationReady(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return [1, 2, 3, 4].every((i) => {
    const row = blocks.find((b) => nz(b.slotKey) === `valueOwners.scenario.${i}`);
    return wordCount(nz(row?.body)) >= 15;
  });
}

export function resolveTargetBrands(brandsArg) {
  const raw = nz(brandsArg || TARGET_BRANDS.map((b) => b.slug).join(","));
  const slugs = raw
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const resolved = [];
  for (const slug of slugs) {
    if (PROTECTED_BRAND_SLUGS.includes(slug)) {
      throw new Error(`Brand ${slug} is protected and cannot be modified by v30B`);
    }
    const meta = TARGET_BRANDS.find((b) => b.slug === slug);
    if (!meta) {
      throw new Error(`v30B supports Kimpton only; got: ${slug}`);
    }
    if (!resolved.some((b) => b.slug === meta.slug)) resolved.push(meta);
  }
  if (!resolved.length) throw new Error("No target brands resolved");
  return resolved;
}

async function fetchAllFacts(brandRecordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function isExplorerFact(fact) {
  return nz(fact.explorerType) === "Brand Explorer" || nz(fact.fieldName).startsWith("be.");
}

function factValue(fact) {
  return nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
}

function sourceStewardship(source, brandRecordId) {
  if (!source) return { sufficient: false, reason: "missing_source" };
  if (!isApprovedExplorerSource(source)) {
    return { sufficient: false, reason: "source_not_approved_for_explorer" };
  }
  if (source.brandId && source.brandId !== brandRecordId) {
    return { sufficient: false, reason: "source_wrong_brand" };
  }
  return { sufficient: true, reason: "approved_explorer_source", sourceUrl: nz(source.sourceUrl) };
}

async function fetchProjectFitForBrand(recordId, brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return null;
  const base = new Airtable({ apiKey }).base(baseId);
  const linkFieldNames = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];
  for (const linkField of linkFieldNames) {
    try {
      const formula = `FIND("${recordId}", ARRAYJOIN({${linkField}})) > 0`;
      const records = await base(PROJECT_FIT_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).all();
      if (records.length > 0) return { id: records[0].id, fields: records[0].fields || {} };
    } catch {
      continue;
    }
  }
  if (brandName) {
    const escaped = brandName.replace(/"/g, '\\"');
    const records = await base(PROJECT_FIT_TABLE)
      .select({ filterByFormula: `{Brand Name} = "${escaped}"`, maxRecords: 1 })
      .all();
    if (records.length > 0) return { id: records[0].id, fields: records[0].fields || {} };
  }
  return null;
}

async function inspectResidencesConflict(brandBasics, brandName, recordId) {
  const basicsFields = brandBasics?.fields || {};
  const apiShape = buildResidencesApiShape(basicsFields);
  const projectFit = await fetchProjectFitForBrand(recordId, brandName);
  const migration = classifyBrandResidencesMigration({
    basicsFields,
    projectFitFields: projectFit?.fields || {},
  });
  const legacyRaw = nz(projectFit?.fields?.[LEGACY_RESIDENCES_FIELD]);
  const conflict = migration.classification === "conflict_requires_review";
  return {
    projectFitRecordId: projectFit?.id || null,
    legacyProjectFitValue: legacyRaw || null,
    brandBasicsStatus: basicsFields[MAP_BRAND_RESIDENCES.status] || apiShape.status,
    brandBasicsReviewStatus: basicsFields[MAP_BRAND_RESIDENCES.reviewStatus] || apiShape.reviewStatus,
    apiShape,
    migrationClassification: migration.classification,
    migrationBlockers: migration.blockers || [],
    classification: conflict ? "hold_for_founder_review" : "no_fact_level_conflict",
    rationale: conflict
      ? `Legacy Project Fit "${legacyRaw}" conflicts with Brand Basics status "${apiShape.status}" — do not resolve by guessing in v30B.`
      : "No Explorer fact governs residences; Brand Basics / Project Fit alignment is separate from fact queue.",
    remainsBlockerAfterV30B: conflict,
    relatedPendingFactField: null,
  };
}

export function classifyIhgKimptonPendingFact(
  fact,
  {
    brandSlug,
    brandRecordId,
    presentationBodies = {},
    scenariosReady = false,
    source = null,
    fieldAlreadyApproved = false,
    duplicateFieldPendingCount = 1,
  } = {}
) {
  const fieldKey = nz(fact.fieldName);
  const value = factValue(fact);
  const status = nz(fact.humanReviewStatus);
  const supersededSlot = SUPERSEDED_BY_PRESENTATION[fieldKey];
  const presentationBody = supersededSlot ? nz(presentationBodies[supersededSlot]) : "";
  const presentationHasBody = wordCount(presentationBody) >= 12;
  const src = sourceStewardship(source, brandRecordId);

  if (status !== "Pending") {
    return {
      fieldKey,
      classification: "not_pending",
      proposedAction: "none",
      approveReady: false,
      rationale: `Review status is ${status || "unknown"} — idempotent skip`,
    };
  }

  if (nz(fact.brandId) && fact.brandId !== brandRecordId) {
    return {
      fieldKey,
      classification: "reject_archive",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: "Fact linked to wrong brand — reject as Internal Only.",
      sourceSupport: src,
    };
  }

  if (COMPANY_VALIDATION_BLOCK_RE.test(value) || INTERNAL_SURFACE_RE.test(value)) {
    return {
      fieldKey,
      classification: "reject_archive",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: "Blocked validation or internal/source-capture/FDD language.",
      sourceSupport: src,
    };
  }

  if (fieldAlreadyApproved && APPROVE_ONCE_FIELDS.has(fieldKey)) {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: `Duplicate pending ${fieldKey} extract — primary fact already approved in this batch.`,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.identity.brandName") {
    if (!SIMPLE_BRAND_NAME_RE.test(value)) {
      return {
        fieldKey,
        classification: "needs_founder_review",
        proposedAction: "hold_pending",
        approveReady: false,
        rationale: "Brand name extract is not the simple Kimpton Hotels label.",
        sourceSupport: src,
      };
    }
    if (!src.sufficient) {
      return {
        fieldKey,
        classification: "needs_source_confirmation",
        proposedAction: "hold_pending",
        approveReady: false,
        rationale: "Kimpton Hotels label needs approved Explorer source before approval.",
        sourceSupport: src,
      };
    }
    return {
      fieldKey,
      classification: "approve_ready",
      proposedAction: "approve",
      approveReady: true,
      rationale: "Simple source-backed brand identity label (Kimpton Hotels).",
      approvedValue: "Kimpton Hotels",
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.identity.parentCompany") {
    if (!SIMPLE_PARENT_COMPANY_RE.test(value)) {
      return {
        fieldKey,
        classification: "needs_founder_review",
        proposedAction: "hold_pending",
        approveReady: false,
        rationale: "Parent company extract is not the simple IHG Hotels & Resorts label.",
        sourceSupport: src,
      };
    }
    if (!src.sufficient) {
      return {
        fieldKey,
        classification: "needs_source_confirmation",
        proposedAction: "hold_pending",
        approveReady: false,
        rationale: "IHG parent label needs approved Explorer source before approval.",
        sourceSupport: src,
      };
    }
    return {
      fieldKey,
      classification: "approve_ready",
      proposedAction: "approve",
      approveReady: true,
      rationale: "Simple source-backed parent company label (IHG Hotels & Resorts) — factual, not company validation.",
      approvedValue: "IHG Hotels & Resorts",
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.loyalty.programName") {
    const normalized = value.replace(/[®™]/g, "").trim();
    if (!SIMPLE_LOYALTY_PROGRAM_RE.test(normalized) && !SIMPLE_LOYALTY_PROGRAM_RE.test(value)) {
      return {
        fieldKey,
        classification: "needs_founder_review",
        proposedAction: "reject_archive",
        approveReady: false,
        rationale: "Program name is not a simple IHG One Rewards label — reject rather than expand loyalty claims.",
        sourceSupport: src,
      };
    }
    if (!src.sufficient) {
      return {
        fieldKey,
        classification: "needs_source_confirmation",
        proposedAction: "hold_pending",
        approveReady: false,
        rationale: "IHG One Rewards label needs approved Explorer source before approval.",
        sourceSupport: src,
      };
    }
    if (duplicateFieldPendingCount > 1 && fieldAlreadyApproved) {
      return {
        fieldKey,
        classification: "keep_internal",
        proposedAction: "reject_archive",
        approveReady: false,
        rationale: "Duplicate IHG One Rewards pending fact — reject after primary approval.",
        sourceSupport: src,
      };
    }
    return {
      fieldKey,
      classification: "approve_ready",
      proposedAction: "approve",
      approveReady: true,
      rationale:
        "Simple source-backed loyalty program name (IHG One Rewards) — approve for governance clearance; do not expand into unsupported One Rewards claims.",
      approvedValue: "IHG One Rewards",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
    };
  }

  if (NUMERIC_FOOTPRINT_FIELDS.has(fieldKey)) {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale:
        "Thin numeric footprint fragment without presentation context — presentation footprint slots are authoritative; count may be dated.",
      supersededByPresentationSlot: SUPERSEDED_BY_PRESENTATION["be.footprint.geoIntro"],
      presentationAuthoritative: wordCount(presentationBodies["footprint.geo_intro"]) >= 12,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.footprint.geoIntro") {
    return {
      fieldKey,
      classification: "reject_archive",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: FDD_FRAGMENT_RE.test(value)
        ? "FDD distribution-chart fragment — not safe owner-facing geography intro; presentation footprint.geo_intro is authoritative."
        : "Geography intro extract superseded by presentation footprint.geo_intro.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.overview.whyValue" || fieldKey === "be.overview.typicalUseCase") {
    return {
      fieldKey,
      classification: presentationHasBody ? "keep_internal" : "needs_founder_review",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: presentationHasBody
        ? "Marketing overview extract superseded by presentation owner-facing copy — reject/archive as Internal Only."
        : "Overview marketing extract needs founder rewrite — not approve-ready.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.overview.scenarios") {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: scenariosReady
        ? "Thin scenario phrase superseded by valueOwners.scenario.1–4 presentation cards."
        : "Scenario fragment too thin — keep Internal Only until presentation scenarios are authoritative.",
      supersededByPresentationSlot: "valueOwners.scenario.1",
      presentationAuthoritative: scenariosReady,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.overview.developmentModel") {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale:
        "Thin development-model phrase — presentation overview.development_model and deal terms are authoritative.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: wordCount(presentationBodies["overview.development_model"]) >= 8,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.commercial.intro") {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: "Thin commercial marketing fragment — commercial presentation themes cover F&B positioning.",
      sourceSupport: src,
    };
  }

  if (
    fieldKey === "be.positioning.guestPromise" ||
    fieldKey === "be.positioning.history" ||
    fieldKey === "be.positioning.tagline" ||
    fieldKey === "be.positioning.summary"
  ) {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale:
        "Positioning/marketing extract — presentation overview slots are authoritative; not safe to surface raw brochure copy.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
    };
  }

  if (FDD_METRIC_FIELDS.has(fieldKey)) {
    const notConfirmed = /not confirmed|unavailable|tbd/i.test(value);
    return {
      fieldKey,
      classification: "reject_archive",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: notConfirmed
        ? "Economics/loyalty metric not confirmed in source — keep Internal Only."
        : fieldKey === "be.loyalty.memberCount" && /^100$/.test(value)
          ? "Member count extract is a truncated/invalid fragment (100) — reject."
          : "FDD-sensitive economics/loyalty metric — presentation and deal-terms paths are authoritative; not for public fact surfacing.",
      sourceSupport: src,
    };
  }

  if (META_INTERNAL_FIELDS.has(fieldKey)) {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: "Internal stewardship/meta field — not owner-facing Explorer content.",
      sourceSupport: src,
    };
  }

  return {
    fieldKey,
    classification: "needs_source_confirmation",
    proposedAction: "hold_pending",
    approveReady: false,
    rationale: `Unhandled pending ${fieldKey} for ${brandSlug} — hold for founder/source review.`,
    sourceSupport: src,
  };
}

export function buildIhgFactStewardshipPatch(fact, diagnosis) {
  const stamp = new Date().toISOString().slice(0, 10);
  const prior = nz(fact.reviewerNotes);
  const note = [
    STEWARDSHIP_TAG,
    diagnosis.rationale,
    diagnosis.supersededByPresentationSlot
      ? `Superseded by presentation ${diagnosis.supersededByPresentationSlot}.`
      : "",
    "Not company validation.",
  ]
    .filter(Boolean)
    .join(" ");
  const reviewerNotes = prior.includes(STEWARDSHIP_TAG) ? prior : prior ? `${prior}\n${note}` : note;

  if (diagnosis.proposedAction === "approve") {
    if (!diagnosis.approveReady) {
      return { patch: null, skipped: ["approve_not_ready"] };
    }
    const approvedValue = nz(diagnosis.approvedValue) || factValue(fact);
    if (INTERNAL_SURFACE_RE.test(approvedValue) || COMPANY_VALIDATION_BLOCK_RE.test(approvedValue)) {
      return { patch: null, skipped: ["unsafe_approved_value"] };
    }
    const fields = {
      [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
      [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
      [MAP_PARTNER_FACT.lastUpdated]: stamp,
      [MAP_PARTNER_FACT.dataGap]: "No",
    };
    if (!nz(fact.approvedValue)) {
      fields[MAP_PARTNER_FACT.approvedValue] = approvedValue;
    }
    if (VAL_PARTNER_FACT_SELECTS.publicVisibility.includes("Public")) {
      fields[MAP_PARTNER_FACT.publicVisibility] = "Public";
    }
    return { patch: fields, skipped: [] };
  }

  if (diagnosis.proposedAction === "reject_archive") {
    const fields = {
      [MAP_PARTNER_FACT.humanReviewStatus]: "Rejected",
      [MAP_PARTNER_FACT.publicVisibility]: "Internal Only",
      [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
      [MAP_PARTNER_FACT.dataGap]: "Yes",
      [MAP_PARTNER_FACT.lastUpdated]: stamp,
    };
    return { patch: fields, skipped: [] };
  }

  return { patch: null, skipped: [`action_${diagnosis.proposedAction}`] };
}

export function buildApplyCommand({ brands = "kimpton" } = {}) {
  return [
    "npm run brand-explorer-ihg-family-pending-fact-stewardship-writer --",
    `--brands ${brands}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

async function buildPerBrandReport(target, options) {
  const { apply, approveBatch, founderReviewed, noValidationClaim } = options;

  const [brandBasicsBefore, liveState, brandApi, allFacts] = await Promise.all([
    fetchBrandBasics(target.recordId),
    fetchLiveState(target.recordId),
    fetchBrandApiShape(target.recordId),
    fetchAllFacts(target.recordId),
  ]);

  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const explorerFacts = allFacts.filter(isExplorerFact);
  const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const pendingSorted = [...pendingFacts].sort((a, b) => {
    const fk = nz(a.fieldName).localeCompare(nz(b.fieldName));
    return fk !== 0 ? fk : nz(a.id).localeCompare(nz(b.id));
  });

  const presentationBodies = {};
  for (const slot of new Set(Object.values(SUPERSEDED_BY_PRESENTATION))) {
    presentationBodies[slot] = presentationBodyForSlot(brandApi, slot);
  }
  presentationBodies["footprint.geo_intro"] = presentationBodyForSlot(brandApi, "footprint.geo_intro");
  presentationBodies["overview.development_model"] = presentationBodyForSlot(
    brandApi,
    "overview.development_model"
  );

  const scenariosReady = scenarioPresentationReady(brandApi);
  const residencesConflict = await inspectResidencesConflict(
    brandBasicsBefore,
    target.name,
    target.recordId
  );

  const pendingByField = new Map();
  for (const f of pendingSorted) {
    const k = nz(f.fieldName);
    pendingByField.set(k, (pendingByField.get(k) || 0) + 1);
  }

  const approvedFields = new Set();
  const factDiagnosis = [];
  const factsToApprove = [];
  const factsToReject = [];
  const factsToHold = [];
  const applyPatches = [];
  const applyBlockers = [];

  for (const fact of pendingSorted) {
    const fieldKey = nz(fact.fieldName);
    if (nz(fact.brandId) && fact.brandId !== target.recordId) {
      applyBlockers.push(`wrong_brand:${fact.id}`);
      continue;
    }

    const source = fact.sourceRecordId
      ? await getPartnerSourceById(fact.sourceRecordId).catch(() => null)
      : null;

    const diagnosis = classifyIhgKimptonPendingFact(fact, {
      brandSlug: target.slug,
      brandRecordId: target.recordId,
      presentationBodies,
      scenariosReady,
      source,
      fieldAlreadyApproved: approvedFields.has(fieldKey),
      duplicateFieldPendingCount: pendingByField.get(fieldKey) || 1,
    });

    const { patch, skipped } = buildIhgFactStewardshipPatch(fact, diagnosis);

    const row = {
      factId: fact.id,
      fieldKey,
      currentValue: factValue(fact),
      currentHumanReviewStatus: nz(fact.humanReviewStatus),
      currentPublicVisibility: nz(fact.publicVisibility),
      sourceRecordId: fact.sourceRecordId || null,
      sourceStatus: source ? nz(source.reviewStatus || source.status) : null,
      sourceApprovedForExplorer: source ? isApprovedExplorerSource(source) : false,
      sourceUrl: nz(source?.sourceUrl).slice(0, 160),
      sourceSupport: diagnosis.sourceSupport || sourceStewardship(source, target.recordId),
      ...diagnosis,
      patchPreview: patch,
      patchSkipped: skipped,
    };
    factDiagnosis.push(row);

    if (diagnosis.proposedAction === "approve" && patch) {
      factsToApprove.push(row);
      applyPatches.push({ factId: fact.id, fieldKey, patch, diagnosis, action: "approve" });
      approvedFields.add(fieldKey);
    } else if (diagnosis.proposedAction === "reject_archive" && patch) {
      factsToReject.push(row);
      applyPatches.push({ factId: fact.id, fieldKey, patch, diagnosis, action: "reject" });
    } else if (diagnosis.proposedAction === "hold_pending") {
      factsToHold.push(row);
      applyBlockers.push(`hold:${fieldKey}:${fact.id}`);
    } else if (diagnosis.proposedAction === "none") {
      // idempotent
    } else {
      applyBlockers.push(`unhandled_action:${fieldKey}:${diagnosis.proposedAction}`);
      if (diagnosis.proposedAction === "reject_archive" && !patch) {
        applyBlockers.push(`reject_patch_blocked:${fieldKey}:${skipped.join(",")}`);
      }
    }
  }

  const actionsByCategory = {
    approve_ready: factsToApprove.length,
    reject_archive: factsToReject.filter((f) => f.classification === "reject_archive").length,
    keep_internal: factsToReject.filter((f) => f.classification === "keep_internal").length,
    needs_founder_review: factsToHold.filter((f) => f.classification === "needs_founder_review").length,
    needs_source_confirmation: factsToHold.filter((f) => f.classification === "needs_source_confirmation")
      .length,
    hold_pending: factsToHold.length,
  };

  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasWork = applyPatches.length > 0;
  const allPendingResolved =
    applyPatches.length === pendingFacts.length &&
    factsToHold.length === 0 &&
    pendingFacts.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork && allPendingResolved;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const errors = [];
    for (const item of applyPatches) {
      try {
        const result = await patchPartnerFact(item.factId, item.patch);
        updated.push({
          factId: result.id,
          fieldKey: item.fieldKey,
          action: item.action,
          status: result.humanReviewStatus,
        });
      } catch (err) {
        errors.push({ factId: item.factId, fieldKey: item.fieldKey, message: err.message });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length === applyPatches.length && errors.length === 0;
    applyResults = { updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const projectedPendingAfter =
    canApply || (applyBlockers.length === 0 && allPendingResolved)
      ? Math.max(0, pendingFacts.length - applyPatches.length)
      : pendingFacts.length + factsToHold.length;

  const dryRunClean =
    applyBlockers.length === 0 && hasWork && allPendingResolved && factsToHold.length === 0;

  const projectedSourceGovernanceScore =
    projectedPendingAfter === 0 ? 100 : Math.max(0, 100 - projectedPendingAfter * 3);
  const projectedFinalQaNumeric = Math.min(
    99,
    projectedPendingAfter === 0 ? 92 : 85 - Math.min(10, factsToHold.length * 2)
  );
  const projectedReadiness =
    projectedPendingAfter === 0 && !residencesConflict.remainsBlockerAfterV30B
      ? "ready"
      : projectedPendingAfter === 0
        ? "almost_ready"
        : "almost_ready";

  return {
    brand: {
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    },
    pendingFactSummary: {
      totalExplorerFacts: explorerFacts.length,
      pendingBefore: pendingFacts.length,
      pendingAfterProjected: projectedPendingAfter,
      approvedBefore: explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Approved").length,
    },
    factDiagnosis,
    actionsByCategory,
    factsToApprove,
    factsToReject,
    factsToHold,
    applyPatches: applyPatches.map((p) => ({
      factId: p.factId,
      fieldKey: p.fieldKey,
      action: p.action,
      classification: p.diagnosis.classification,
      proposedAction: p.diagnosis.proposedAction,
      sourceSupport: p.diagnosis.sourceSupport,
      patch: p.patch,
    })),
    residencesConflict,
    applyBlockers,
    dryRunClean,
    canApply,
    allPendingResolved,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    ihgValidationImplied: false,
    presentationRowsModified: false,
    airtableModified,
    applyResults,
    projectedGovernance: {
      pendingFactsAfter: projectedPendingAfter,
      sourceGovernanceScoreProjected: projectedSourceGovernanceScore,
      governedPlatformReadyProjected: projectedPendingAfter === 0,
      factApprovalNeeded: projectedPendingAfter > 0,
    },
    expectedFinalQaAfterApply: {
      overallNumeric: projectedFinalQaNumeric,
      overallActiveProfileReadiness: projectedReadiness,
      sourceGovernanceScore: projectedSourceGovernanceScore,
    },
    expectedActiveProfileAfterApply:
      projectedPendingAfter === 0 && !residencesConflict.remainsBlockerAfterV30B,
    expectedBlockersRemainingAfterApply: [
      projectedPendingAfter > 0 ? `${projectedPendingAfter} pending facts remain` : null,
      residencesConflict.remainsBlockerAfterV30B
        ? "Branded residences legacy Case-by-case vs Brand Basics No — founder review required (separate from fact queue)"
        : null,
      factsToHold.length > 0 ? `${factsToHold.length} facts held for founder/source review` : null,
    ].filter(Boolean),
    exactDryRunCommand: `npm run brand-explorer-ihg-family-pending-fact-stewardship-writer -- --brands ${target.slug} --dry-run`,
  };
}

export async function buildBrandExplorerIhgFamilyPendingFactStewardshipWriterReport({
  brandsArg = TARGET_BRANDS.map((b) => b.slug).join(","),
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
  stopOnCritical = false,
} = {}) {
  const targets = resolveTargetBrands(brandsArg);
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  }

  const brandReports = [];
  let halted = false;
  let haltReason = "";

  for (const target of targets) {
    if (halted) {
      brandReports.push({ brand: target, skipped: true, skipReason: haltReason });
      continue;
    }
    const report = await buildPerBrandReport(target, {
      apply,
      approveBatch,
      founderReviewed,
      noValidationClaim,
    });
    brandReports.push(report);
    await new Promise((r) => setTimeout(r, 1500));

    if (stopOnCritical && apply && report.applyBlockers?.length > 0) {
      halted = true;
      haltReason = `Stopped after ${target.slug}: apply blockers remain`;
    }
  }

  const dryRunClean = brandReports.every((b) => b.skipped || b.dryRunClean);
  const airtableModified = brandReports.some((b) => b.airtableModified);

  return {
    writerVersion: WRITER_VERSION,
    v30BWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    brandsRequested: targets.map((b) => b.slug),
    protectedBrandsUntouched: PROTECTED_BRAND_SLUGS,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    brandReports,
    companyValidatedUntouched: brandReports.every((b) => b.skipped || b.companyValidatedUntouched),
    summary: {
      brandsProcessed: brandReports.filter((b) => !b.skipped).length,
      pendingFactsBefore: brandReports.reduce((n, b) => n + (b.pendingFactSummary?.pendingBefore || 0), 0),
      pendingFactsAfterProjected: brandReports.reduce(
        (n, b) => n + (b.pendingFactSummary?.pendingAfterProjected || 0),
        0
      ),
      factsToApprove: brandReports.reduce((n, b) => n + (b.factsToApprove?.length || 0), 0),
      factsToReject: brandReports.reduce((n, b) => n + (b.factsToReject?.length || 0), 0),
      factsToHold: brandReports.reduce((n, b) => n + (b.factsToHold?.length || 0), 0),
      dryRunClean,
      airtableModified,
    },
    exactDryRunCommand: `npm run brand-explorer-ihg-family-pending-fact-stewardship-writer -- --brands ${targets
      .map((b) => b.slug)
      .join(",")} --dry-run`,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brands: targets.map((b) => b.slug).join(",") }) : null,
    markdown: "",
  };
}

export function buildBrandExplorerIhgFamilyPendingFactStewardshipWriterMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer IHG-Family Pending Fact Stewardship Writer v${report.writerVersion}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brands: **${report.brandsRequested.join(", ")}**`);
  lines.push(`- v30B exists: **${report.v30BWriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.summary.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`
  );
  lines.push("");

  for (const br of report.brandReports) {
    if (br.skipped) {
      lines.push(`## ${br.brand?.slug} — SKIPPED`);
      lines.push(br.skipReason || "");
      continue;
    }
    lines.push(`## ${br.brand.name} (\`${br.brand.slug}\`)`);
    lines.push("");
    lines.push("### Pending fact summary");
    lines.push(`- Pending before: **${br.pendingFactSummary.pendingBefore}**`);
    lines.push(`- Pending after (projected): **${br.pendingFactSummary.pendingAfterProjected}**`);
    lines.push(`- Approve: **${br.factsToApprove.length}**`);
    lines.push(`- Reject / Internal Only: **${br.factsToReject.length}**`);
    lines.push(`- Hold: **${br.factsToHold.length}**`);
    lines.push("");

    lines.push("### Residences conflict");
    const rc = br.residencesConflict;
    lines.push(`- Legacy Project Fit: **${rc.legacyProjectFitValue || "—"}**`);
    lines.push(`- Brand Basics status: **${rc.brandBasicsStatus}**`);
    lines.push(`- Classification: **${rc.classification}**`);
    lines.push(`- Remains blocker after v30B: **${rc.remainsBlockerAfterV30B ? "yes" : "no"}**`);
    lines.push(`- ${rc.rationale}`);
    lines.push("");

    lines.push("### Expected after apply");
    lines.push(
      `- Final QA: **~${br.expectedFinalQaAfterApply.overallNumeric}** (${br.expectedFinalQaAfterApply.overallActiveProfileReadiness})`
    );
    lines.push(`- Source governance score: **${br.expectedFinalQaAfterApply.sourceGovernanceScore}**`);
    lines.push("");

    if (br.applyBlockers?.length) {
      lines.push("### Apply blockers");
      for (const b of br.applyBlockers) lines.push(`- ${b}`);
      lines.push("");
    }

    lines.push("### Fact actions (sample)");
    for (const row of br.factDiagnosis.slice(0, 8)) {
      lines.push(
        `- \`${row.factId}\` **${row.fieldKey}** → ${row.classification} / ${row.proposedAction}`
      );
    }
    if (br.factDiagnosis.length > 8) {
      lines.push(`- … and ${br.factDiagnosis.length - 8} more (see JSON)`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command");
  lines.push(report.exactApplyCommand ? `\`${report.exactApplyCommand}\`` : "(none — dry-run not clean)");
  return lines.join("\n");
}
