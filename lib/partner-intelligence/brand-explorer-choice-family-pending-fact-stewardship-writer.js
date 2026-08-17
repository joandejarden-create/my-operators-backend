/**
 * Brand Explorer Choice-Family Pending Fact Stewardship Writer v29B.
 *
 * Reviews pending Explorer facts for Ascend Hotel Collection and Radisson Blu by Choice
 * after v29A visual/copy repairs. Default: dry-run. Approves only simple source-backed
 * facts; rejects/archives thin superseded extracts as Internal Only.
 *
 * @see docs/data-intelligence/brand-explorer-choice-family-pending-fact-stewardship-writer-v29B.md
 */
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { WAVE1_EXPANSION_SLUGS } from "./brand-explorer-next-brand-selection-audit.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { TARGET_BRANDS } from "./brand-explorer-choice-family-active-profile-repair-writer.js";

export const WRITER_VERSION = "29B";
export const REPORT_JSON_NAME = "brand-explorer-choice-family-pending-fact-stewardship-writer.json";
export const REPORT_MD_NAME = "brand-explorer-choice-family-pending-fact-stewardship-writer.md";
export const DOC_MD_NAME = "brand-explorer-choice-family-pending-fact-stewardship-writer-v29B.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v29B-choice-family-pending-fact-stewardship";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-choice-family-fact-governance";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const STEWARDSHIP_TAG = "v29B-choice-family-pending-fact-stewardship";

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "radisson",
  "curio-collection",
  "kimpton",
  ...WAVE1_EXPANSION_SLUGS,
]);

/** Scoped pending facts per brand (fact IDs from v29A diagnosis). */
export const TARGET_PENDING_BY_BRAND = Object.freeze({
  ascend: {
    slug: "ascend",
    recordId: "reclkgOzvAcBheUSo",
    name: "Ascend Hotel Collection",
    facts: Object.freeze([
      { factId: "rec4BrtEotr82mlZq", fieldKey: "be.footprint.geoIntro" },
    ]),
  },
  "radisson-blu": {
    slug: "radisson-blu",
    recordId: "recWPEvxBQxVVzSq3",
    name: "Radisson Blu by Choice",
    facts: Object.freeze([
      { factId: "recEog0ehiJXRLYpo", fieldKey: "be.loyalty.programName" },
      { factId: "recPGX8dxsNBmfuUE", fieldKey: "be.overview.whyValue" },
      { factId: "recYI7VbRqpoQiXaT", fieldKey: "be.footprint.americasHotels" },
    ]),
  },
});

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-choice-family-active-profile-repair-writer.md",
  "reports/brand-explorer-choice-family-active-profile-repair-writer.json",
  "reports/brand-explorer-complete-build-ascend.md",
  "reports/brand-explorer-complete-build-radisson-blu.md",
  "reports/brand-explorer-radisson-pending-fact-stewardship-writer.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/airtable-facts.js",
  "lib/partner-intelligence/airtable-source.js",
  "live Ascend / Radisson Blu Partner Facts",
  "live Ascend / Radisson Blu Source Library records",
  "live Ascend / Radisson Blu Brand Explorer Presentation rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-choice-family-pending-fact-stewardship-writer.js",
  "scripts/brand-explorer-choice-family-pending-fact-stewardship-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by choice|validated by radisson|approved by radisson|brand approved|company-approved|official sign-off/i;

const INTERNAL_SURFACE_RE =
  /\b(source capture|internal extraction|paste into airtable|franchise disclosure document|item\s*19|fdd\b)\b/i;

const SIMPLE_LOYALTY_PROGRAM_NAME_RE = /^choice privileges(?:®|™)?$/i;

/** Presentation slots that already render owner-facing copy for each fact key. */
const SUPERSEDED_BY_PRESENTATION = {
  "be.footprint.geoIntro": "footprint.geo_intro",
  "be.overview.whyValue": "overview.why_value",
  "be.footprint.americasHotels": "footprint.region.americas",
  "be.loyalty.programName": "loyalty.hero_title",
};

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

export function resolveTargetBrands(brandsArg) {
  const raw = nz(brandsArg || TARGET_BRANDS.map((b) => b.slug).join(","));
  const slugs = raw
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const resolved = [];
  for (const slug of slugs) {
    if (PROTECTED_BRAND_SLUGS.includes(slug)) {
      throw new Error(`Brand ${slug} is protected and cannot be modified by v29B`);
    }
    const config = TARGET_PENDING_BY_BRAND[slug];
    if (!config) {
      throw new Error(`v29B supports Ascend and Radisson Blu only; got: ${slug}`);
    }
    if (!resolved.some((b) => b.slug === slug)) resolved.push(config);
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
  return { sufficient: true, reason: "approved_explorer_source" };
}

export function classifyChoiceFamilyPendingFact(
  fact,
  { brandSlug, brandRecordId, presentationBody = "", source = null, expectedFactId = null } = {}
) {
  const fieldKey = nz(fact.fieldName);
  const value = factValue(fact);
  const status = nz(fact.humanReviewStatus);
  const supersededSlot = SUPERSEDED_BY_PRESENTATION[fieldKey];
  const presentationHasBody = wordCount(presentationBody) >= 12;
  const src = sourceStewardship(source, brandRecordId);

  if (expectedFactId && fact.id !== expectedFactId) {
    return {
      fieldKey,
      classification: "out_of_scope",
      proposedAction: "none",
      approveReady: false,
      rationale: `Fact ID mismatch — expected ${expectedFactId}, got ${fact.id}`,
    };
  }

  if (status !== "Pending") {
    return {
      fieldKey,
      classification: "not_pending",
      proposedAction: "none",
      approveReady: false,
      rationale: `Review status is ${status || "unknown"} — idempotent skip`,
    };
  }

  if (COMPANY_VALIDATION_BLOCK_RE.test(value) || INTERNAL_SURFACE_RE.test(value)) {
    return {
      fieldKey,
      classification: "reject_archive",
      proposedAction: "reject_internal",
      approveReady: false,
      rationale: "Blocked validation or internal/source-capture/FDD language",
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.footprint.geoIntro") {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale:
        "Thin geography fragment (“Americas regions by Choice Hotels.”) — presentation footprint.geo_intro is authoritative; not safe to surface externally.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.loyalty.programName") {
    const simpleName = SIMPLE_LOYALTY_PROGRAM_NAME_RE.test(value.replace(/[®™]/g, "").trim()) ||
      SIMPLE_LOYALTY_PROGRAM_NAME_RE.test(value);
    const presentationAlreadyNamesProgram = /choice privileges/i.test(presentationBody);
    if (!simpleName) {
      return {
        fieldKey,
        classification: "needs_founder_review",
        proposedAction: "reject_archive",
        approveReady: false,
        rationale: "Program name extract is not a simple approved label — reject rather than expand loyalty claims.",
        sourceSupport: src,
      };
    }
    if (!src.sufficient) {
      return {
        fieldKey,
        classification: "needs_source_confirmation",
        proposedAction: "hold_pending",
        approveReady: false,
        rationale: "Choice Privileges label needs approved Explorer source before approval.",
        sourceSupport: src,
      };
    }
    if (presentationAlreadyNamesProgram && wordCount(presentationBody) >= 8) {
      return {
        fieldKey,
        classification: "approve_ready",
        proposedAction: "approve",
        approveReady: true,
        rationale:
          "Simple source-backed program name (Choice Privileges) — approve for governance queue clearance; presentation loyalty.hero_title already names the program.",
        approvedValue: "Choice Privileges",
        supersededByPresentationSlot: supersededSlot,
        presentationAuthoritative: true,
        sourceSupport: src,
      };
    }
    return {
      fieldKey,
      classification: "approve_ready",
      proposedAction: "approve",
      approveReady: true,
      rationale: "Simple source-backed program name (Choice Privileges) — safe factual approval.",
      approvedValue: "Choice Privileges",
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.overview.whyValue") {
    return {
      fieldKey,
      classification: presentationHasBody ? "keep_internal" : "needs_founder_review",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale: presentationHasBody
        ? "Partial why-value marketing extract superseded by presentation overview.why_value — reject/archive as Internal Only."
        : "Why-value extract needs founder rewrite with source support — not approve-ready.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
    };
  }

  if (fieldKey === "be.footprint.americasHotels") {
    const hasNumericFootprint = /\d+\s+hotels?/i.test(value);
    if (!src.sufficient || !hasNumericFootprint) {
      return {
        fieldKey,
        classification: "needs_source_confirmation",
        proposedAction: "reject_archive",
        approveReady: false,
        rationale:
          "Americas hotel count extract needs stronger source confirmation — keep Internal Only rather than surface a potentially dated count.",
        supersededByPresentationSlot: supersededSlot,
        presentationAuthoritative: presentationHasBody,
        sourceSupport: src,
      };
    }
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale:
        "Footprint count fragment may be dated — presentation footprint regions are authoritative for Explorer UI.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
    };
  }

  return {
    fieldKey,
    classification: "needs_source_confirmation",
    proposedAction: "hold_pending",
    approveReady: false,
    rationale: `Unhandled pending fact for ${brandSlug} — keep in stewardship queue`,
    sourceSupport: src,
  };
}

export function buildChoiceFamilyFactStewardshipPatch(fact, diagnosis) {
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
    if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Approved")) {
      return { patch: null, skipped: ["unknown_select_option:Approved"] };
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
    if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Rejected")) {
      return { patch: null, skipped: ["unknown_select_option:Rejected"] };
    }
    if (!VAL_PARTNER_FACT_SELECTS.publicVisibility.includes("Internal Only")) {
      return { patch: null, skipped: ["unknown_select_option:Internal Only"] };
    }
    return {
      patch: {
        [MAP_PARTNER_FACT.humanReviewStatus]: "Rejected",
        [MAP_PARTNER_FACT.publicVisibility]: "Internal Only",
        [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
        [MAP_PARTNER_FACT.dataGap]: "Yes",
        [MAP_PARTNER_FACT.lastUpdated]: stamp,
      },
      skipped: [],
    };
  }

  return { patch: null, skipped: [`action_${diagnosis.proposedAction}`] };
}

export function buildApplyCommand({ brands = "ascend,radisson-blu" } = {}) {
  return [
    "npm run brand-explorer-choice-family-pending-fact-stewardship-writer --",
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

  const factDiagnosis = [];
  const factsToApprove = [];
  const factsToReject = [];
  const factsToHold = [];
  const applyPatches = [];
  const applyBlockers = [];

  for (const spec of target.facts) {
    const fact =
      allFacts.find((f) => f.id === spec.factId) ||
      pendingFacts.find((f) => nz(f.fieldName) === spec.fieldKey);
    if (!fact) {
      applyBlockers.push(`missing_fact:${spec.fieldKey}:${spec.factId}`);
      factDiagnosis.push({
        fieldKey: spec.fieldKey,
        factId: spec.factId,
        error: "fact_not_found",
      });
      continue;
    }
    if (fact.id !== spec.factId) {
      applyBlockers.push(`fact_id_mismatch:${spec.fieldKey}:${spec.factId}:${fact.id}`);
    }
    if (nz(fact.brandId) && fact.brandId !== target.recordId) {
      applyBlockers.push(`wrong_brand:${fact.id}`);
      continue;
    }
    if (nz(fact.fieldName) !== spec.fieldKey) {
      applyBlockers.push(`field_key_mismatch:${fact.id}:${fact.fieldName}`);
    }

    const presentationSlot = SUPERSEDED_BY_PRESENTATION[spec.fieldKey];
    const presentationBody = presentationBodyForSlot(brandApi, presentationSlot);
    const source = fact.sourceRecordId
      ? await getPartnerSourceById(fact.sourceRecordId).catch(() => null)
      : null;

    const diagnosis = classifyChoiceFamilyPendingFact(fact, {
      brandSlug: target.slug,
      brandRecordId: target.recordId,
      presentationBody,
      source,
      expectedFactId: spec.factId,
    });
    const { patch, skipped } = buildChoiceFamilyFactStewardshipPatch(fact, diagnosis);

    const row = {
      factId: fact.id,
      expectedFactId: spec.factId,
      fieldName: spec.fieldKey,
      currentValue: factValue(fact),
      currentStatus: nz(fact.humanReviewStatus),
      evidenceText: nz(fact.evidenceText).slice(0, 200),
      sourceRecordId: fact.sourceRecordId || null,
      sourceApprovedForExplorer: source ? isApprovedExplorerSource(source) : false,
      sourceUrl: nz(source?.sourceUrl).slice(0, 120),
      sourceSupport: diagnosis.sourceSupport || sourceStewardship(source, target.recordId),
      presentationSlot,
      presentationExcerpt: presentationBody.slice(0, 200),
      ...diagnosis,
      patchPreview: patch,
      patchSkipped: skipped,
    };
    factDiagnosis.push(row);

    if (diagnosis.proposedAction === "approve" && patch) {
      factsToApprove.push(row);
      applyPatches.push({ factId: fact.id, fieldName: spec.fieldKey, patch, diagnosis, action: "approve" });
    } else if (diagnosis.proposedAction === "reject_archive" && patch) {
      factsToReject.push(row);
      applyPatches.push({ factId: fact.id, fieldName: spec.fieldKey, patch, diagnosis, action: "reject" });
    } else if (diagnosis.proposedAction === "hold_pending") {
      factsToHold.push(row);
      applyBlockers.push(`hold:${spec.fieldKey}`);
      if (diagnosis.proposedAction === "reject_archive" && !patch) {
        applyBlockers.push(`reject_patch_blocked:${spec.fieldKey}:${skipped.join(",")}`);
      }
    } else if (diagnosis.proposedAction === "none") {
      // idempotent skip for already processed facts
    } else {
      applyBlockers.push(`unhandled_action:${spec.fieldKey}:${diagnosis.proposedAction}`);
    }
  }

  const scopedFieldKeys = target.facts.map((f) => f.fieldKey);
  const outOfScopePending = pendingFacts.filter((f) => !scopedFieldKeys.includes(nz(f.fieldName)));
  if (outOfScopePending.length) {
    applyBlockers.push(
      `unexpected_pending_facts:${outOfScopePending.map((f) => f.fieldName).join(",")}`
    );
  }

  const expectedPatchCount = target.facts.length;
  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasWork = applyPatches.length > 0;
  const allScopedResolved =
    applyPatches.length === expectedPatchCount &&
    factsToHold.length === 0 &&
    factDiagnosis.every((d) => !d.error);
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork && allScopedResolved;

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
          fieldName: item.fieldName,
          action: item.action,
          status: result.humanReviewStatus,
        });
      } catch (err) {
        errors.push({ factId: item.factId, fieldName: item.fieldName, message: err.message });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length === applyPatches.length && errors.length === 0;
    applyResults = { updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const dryRunClean =
    applyBlockers.length === 0 && hasWork && allScopedResolved && factsToHold.length === 0;
  const resolvedPatchCount = applyPatches.length;
  const projectedPendingAfter =
    dryRunClean || canApply
      ? Math.max(0, pendingFacts.length - resolvedPatchCount)
      : pendingFacts.length;

  return {
    brand: {
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    },
    targetPendingFacts: target.facts,
    factDiagnosis,
    factsToApprove,
    factsToReject,
    factsToHold,
    applyPatches: applyPatches.map((p) => ({
      factId: p.factId,
      fieldName: p.fieldName,
      action: p.action,
      patch: p.patch,
      proposedAction: p.diagnosis.proposedAction,
      classification: p.diagnosis.classification,
    })),
    applyBlockers,
    dryRunClean,
    canApply,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    choiceValidationImplied: false,
    presentationRowsModified: false,
    airtableModified,
    applyResults,
    projectedGovernance: {
      pendingFactsBefore: pendingFacts.length,
      pendingFactsAfter: projectedPendingAfter,
      factApprovalNeeded: projectedPendingAfter > 0,
      governedPlatformReady: true,
      sourceCount: (liveState.sources || []).length,
      approvedExplorerSources: (liveState.sources || []).filter((s) => isApprovedExplorerSource(s)).length,
    },
    expectedActiveProfileAfterApply: projectedPendingAfter === 0,
    exactDryRunCommand: `npm run brand-explorer-choice-family-pending-fact-stewardship-writer -- --brands ${target.slug} --dry-run`,
  };
}

export async function buildBrandExplorerChoiceFamilyPendingFactStewardshipWriterReport({
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
    await new Promise((r) => setTimeout(r, 1200));

    if (stopOnCritical && apply && report.applyBlockers?.length > 0) {
      halted = true;
      haltReason = `Stopped after ${target.slug}: apply blockers remain`;
    }
  }

  const brandsApplyReady = brandReports.filter((b) => !b.skipped && b.dryRunClean);
  const dryRunClean = brandReports.every((b) => b.skipped || b.dryRunClean);
  const airtableModified = brandReports.some((b) => b.airtableModified);

  const report = {
    writerVersion: WRITER_VERSION,
    v29BWriterExists: true,
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
      factsToApprove: brandReports.reduce((n, b) => n + (b.factsToApprove?.length || 0), 0),
      factsToReject: brandReports.reduce((n, b) => n + (b.factsToReject?.length || 0), 0),
      factsToHold: brandReports.reduce((n, b) => n + (b.factsToHold?.length || 0), 0),
      dryRunClean,
      airtableModified,
      expectedActiveProfileReadyBrands: brandReports
        .filter((b) => b.expectedActiveProfileAfterApply)
        .map((b) => b.brand?.slug),
    },
    halted,
    haltReason,
    airtableModified,
    exactDryRunCommand: `npm run brand-explorer-choice-family-pending-fact-stewardship-writer -- --brands ${targets
      .map((b) => b.slug)
      .join(",")} --dry-run`,
    exactApplyCommand:
      dryRunClean && brandsApplyReady.length
        ? buildApplyCommand({ brands: targets.map((b) => b.slug).join(",") })
        : null,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Choice-Family Pending Fact Stewardship Writer v${WRITER_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brands: **${report.brandsRequested.join(", ")}**`);
  lines.push(`- v29B exists: **${report.v29BWriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Dry-run clean: **${report.summary.dryRunClean ? "yes" : "no"}**`);
  lines.push("");

  for (const br of report.brandReports) {
    if (br.skipped) {
      lines.push(`## ${br.brand?.slug} — SKIPPED`);
      lines.push(br.skipReason || "");
      lines.push("");
      continue;
    }
    lines.push(`## ${br.brand.name} (\`${br.brand.recordId}\`)`);
    lines.push("");
    lines.push(
      `- Pending facts before: **${br.projectedGovernance.pendingFactsBefore}** → after: **${br.projectedGovernance.pendingFactsAfter}**`
    );
    lines.push(`- Expected active-profile after apply: **${br.expectedActiveProfileAfterApply ? "yes" : "no"}**`);
    lines.push(`- Approve: **${br.factsToApprove.length}**; Reject/internal: **${br.factsToReject.length}**; Hold: **${br.factsToHold.length}**`);
    lines.push("");
    for (const row of br.factDiagnosis) {
      lines.push(`### ${row.fieldName} (\`${row.factId || "missing"}\`)`);
      lines.push(`- Classification: **${row.classification}**`);
      lines.push(`- Proposed action: **${row.proposedAction}**`);
      lines.push(`- Rationale: ${row.rationale}`);
      lines.push(`- Current value: \`${row.currentValue}\``);
      lines.push(
        `- Source: \`${row.sourceRecordId || "none"}\` (${row.sourceSupport?.reason || "n/a"}; Explorer-approved: ${row.sourceApprovedForExplorer ? "yes" : "no"})`
      );
      if (row.presentationSlot) {
        lines.push(
          `- Presentation: \`${row.presentationSlot}\` (${row.presentationAuthoritative ? "authoritative" : "thin/missing"})`
        );
        lines.push(`- Presentation excerpt: ${row.presentationExcerpt || "(empty)"}`);
      }
    }
    if (br.applyBlockers?.length) {
      lines.push("");
      lines.push("### Apply blockers");
      for (const b of br.applyBlockers) lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command");
  lines.push("```bash");
  lines.push(report.exactApplyCommand || "(dry-run not clean)");
  lines.push("```");
  return lines.join("\n");
}

export function buildBrandExplorerChoiceFamilyPendingFactStewardshipWriterMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}
