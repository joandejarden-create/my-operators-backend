/**
 * Brand Governance Status for Hotel Property Census Autopilot.
 *
 * Discovery / coverage may include all official parent-company inventory brands
 * in the selected region — not only Brand Setup Active/Live.
 *
 * Owner-facing / public / Dealality product use remains Active/Live (or
 * explicitly approved promotions). Brand Setup / Brand Explorer are never written.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  CENSUS_OFFICIAL_BRANDS,
  isCensusOfficialBrand,
  isOpaqueBrandCode,
  getCensusOfficialEntry,
} from "./census-official-brand-registry.js";
import { buildActiveBrandSetupControlList } from "./census-autopilot-active-brand-scope.js";
import { resolveExtractorFamily } from "./census-family-extractor-registry.js";
import { listCountriesWithDiscoveryAdapter } from "./production-census-cala-region-config.js";
import { canonicalizeParentCompany } from "./census-parent-company-normalization.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

function classifyDirtyPartnerLabelLocal(brand, propertyName = "", sourceUrl = "") {
  const b = String(brand || "").trim();
  const n = b.toLowerCase().replace(/\s+/g, " ");
  const hay = `${propertyName} ${sourceUrl}`.toLowerCase();
  if (/^sam$/i.test(b)) {
    return { dirty: true, reason: "accor_managed_by_sam_code" };
  }
  if (/marriott bonvoy\s*[—-]\s*brand unconfirmed/i.test(b) || n.includes("brand unconfirmed")) {
    return { dirty: true, reason: "marriott_brand_unconfirmed" };
  }
  if (/ihg partner/i.test(b) || /^spnd$/i.test(b) || /\/spnd\//i.test(sourceUrl)) {
    return { dirty: true, reason: "ihg_partner_spnd_artifact" };
  }
  if (n === "choice hotels" || n === "choice") {
    return { dirty: true, reason: "generic_choice_partner_label" };
  }
  if (/managed by accor|by accor\b/i.test(hay) && isOpaqueBrandCode(b)) {
    return { dirty: true, reason: "accor_managed_opaque" };
  }
  return { dirty: false };
}

export const BRAND_GOVERNANCE_VERSION = "census-brand-governance-v1";

export const BRAND_GOVERNANCE_STATUS = Object.freeze({
  ACTIVE_BRAND_SETUP: "active_brand_setup",
  EVIDENCE_BACKED_NON_ACTIVE: "evidence_backed_non_active_brand",
  PROMOTION_CANDIDATE: "brand_setup_promotion_candidate",
  DIRTY_PARTNER_LABEL: "dirty_partner_label",
  BRAND_CODE_UNRESOLVED: "brand_code_unresolved",
  UNSUPPORTED_OR_AMBIGUOUS: "unsupported_or_ambiguous",
});

export const CENSUS_ONLY_PRODUCTION_USE_STATUS = "Census Only / Not Owner-Facing";
export const OWNER_FACING_PRODUCTION_USE_STATUS = "Owner-Facing Eligible";

/** Review reason categories (reporting + Radar Display Reason encoding). */
export const REVIEW_REASON = Object.freeze({
  GOVERNANCE_REVIEW_REQUIRED: "governance_review_required",
  DATA_QUALITY_REVIEW_REQUIRED: "data_quality_review_required",
  PUBLIC_APPROVAL_REQUIRED: "public_approval_required",
  STEWARD_REVIEW_REQUIRED: "steward_review_required",
});

export const PROMOTION_PACK_JSON = path.join(
  ROOT,
  "reports/research-engine-v2/production-census-brand-setup-promotion-candidates.json"
);
export const PROMOTION_PACK_MD = path.join(
  ROOT,
  "reports/research-engine-v2/production-census-brand-setup-promotion-candidates.md"
);

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function slugifyBrand(name) {
  return String(name || "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * Build Active/Live name+slug index (read-only Brand Setup).
 * @param {object} [opts]
 */
export function buildActiveBrandIndex(opts = {}) {
  const control = opts.controlList || buildActiveBrandSetupControlList(opts);
  /** @type {Map<string, object>} */
  const byNorm = new Map();
  /** @type {Map<string, object>} */
  const bySlug = new Map();
  for (const b of control.brands || []) {
    const name = String(b.brand_name || "").trim();
    const slug = String(b.brand_slug || "").trim();
    if (name) byNorm.set(norm(name), b);
    if (slug) bySlug.set(slug, b);
  }
  return {
    control,
    by_norm: byNorm,
    by_slug: bySlug,
    active_count: (control.brands || []).length,
  };
}

/**
 * True when brand is eligible for owner-facing / public / product surfaces.
 * @param {string} brand
 * @param {object} [opts]
 */
export function isOwnerFacingBrandEligible(brand, opts = {}) {
  const b = String(brand || "").trim();
  if (!b) return false;
  if (opts.explicitlyApproved === true) return true;
  const index = opts.activeIndex || buildActiveBrandIndex(opts);
  if (index.by_norm.has(norm(b))) return true;
  const slug = String(opts.brand_slug || "").trim();
  if (slug && index.by_slug.has(slug)) return true;
  return false;
}

/**
 * Classify Brand Governance Status for a Census or discovery row.
 * @param {{
 *   brand?: string,
 *   brand_slug?: string,
 *   property_name?: string,
 *   source_url?: string,
 *   official_property_url?: string,
 *   parent_company?: string,
 *   source_family?: string,
 *   human_review?: boolean,
 *   production_use_status?: string,
 *   fields?: Record<string, unknown>,
 * }} input
 * @param {object} [opts]
 */
export function classifyBrandGovernanceStatus(input = {}, opts = {}) {
  const fields = input.fields || {};
  const brand = String(
    input.brand || fields[MAP_FIRST_PASS.currentBrand] || fields["Current Brand"] || ""
  ).trim();
  const propertyName = String(
    input.property_name || fields[MAP_FIRST_PASS.propertyName] || fields["Property Name"] || ""
  ).trim();
  const sourceUrl = String(
    input.official_property_url ||
      input.source_url ||
      fields[MAP_FIRST_PASS.officialUrl] ||
      fields["Official Property URL"] ||
      fields[MAP_FIRST_PASS.sourceUrl] ||
      fields["Source URL"] ||
      ""
  ).trim();
  const brandSlug = String(
    input.brand_slug || fields["Brand Explorer Slug if mapped"] || ""
  ).trim();
  const parent = String(
    input.parent_company ||
      fields[MAP_FIRST_PASS.brandFamily] ||
      fields["Brand Family"] ||
      input.source_family ||
      ""
  ).trim();

  const index = opts.activeIndex || buildActiveBrandIndex(opts);
  const reasons = [];

  if (!brand) {
    return {
      status: BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS,
      owner_facing_eligible: false,
      census_save_allowed: false,
      clean_core_eligible_if_identity_clean: false,
      in_active_brand_setup: false,
      in_official_registry: false,
      reasons: ["brand_blank"],
      brand: null,
      parent_company: parent || null,
    };
  }

  if (isOpaqueBrandCode(brand)) {
    return {
      status: BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED,
      owner_facing_eligible: false,
      census_save_allowed: false,
      clean_core_eligible_if_identity_clean: false,
      in_active_brand_setup: false,
      in_official_registry: false,
      reasons: ["brand_code_unresolved"],
      brand,
      parent_company: parent || null,
    };
  }

  const dirty = classifyDirtyPartnerLabelLocal(brand, propertyName, sourceUrl);
  if (dirty.dirty) {
    return {
      status: BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL,
      owner_facing_eligible: false,
      census_save_allowed: false,
      clean_core_eligible_if_identity_clean: false,
      in_active_brand_setup: false,
      in_official_registry: false,
      reasons: [dirty.reason || "dirty_partner_label"],
      brand,
      parent_company: parent || null,
    };
  }

  const inActive =
    index.by_norm.has(norm(brand)) || (brandSlug && index.by_slug.has(brandSlug));
  const official = isCensusOfficialBrand(brand);
  const entry = getCensusOfficialEntry(brand);

  if (inActive) {
    return {
      status: BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP,
      owner_facing_eligible: true,
      census_save_allowed: true,
      clean_core_eligible_if_identity_clean: true,
      in_active_brand_setup: true,
      in_official_registry: official,
      reasons: ["active_live_brand_setup"],
      brand,
      parent_company:
        canonicalizeParentCompany(parent || entry?.parent || "") ||
        parent ||
        entry?.parent ||
        null,
      soft_brand: Boolean(entry?.soft),
    };
  }

  if (official) {
    const hasOfficialUrl = /^https?:\/\//i.test(sourceUrl);
    reasons.push("official_census_registry");
    if (hasOfficialUrl) reasons.push("official_source_url_present");
    return {
      status: hasOfficialUrl
        ? BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE
        : BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE,
      owner_facing_eligible: false,
      census_save_allowed: true,
      clean_core_eligible_if_identity_clean: hasOfficialUrl,
      in_active_brand_setup: false,
      in_official_registry: true,
      brand_setup_promotion_candidate: true,
      reasons,
      brand,
      parent_company:
        canonicalizeParentCompany(parent || entry?.parent || "") ||
        parent ||
        entry?.parent ||
        null,
      soft_brand: Boolean(entry?.soft),
    };
  }

  // Recognizable non-registry / ambiguous
  return {
    status: BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS,
    owner_facing_eligible: false,
    census_save_allowed: false,
    clean_core_eligible_if_identity_clean: false,
    in_active_brand_setup: false,
    in_official_registry: false,
    reasons: ["unsupported_or_ambiguous_brand"],
    brand,
    parent_company: parent || null,
  };
}

/**
 * Census-only governance fields for evidence-backed non-active brands.
 * Public/Radar Hold + reason codes — does NOT set Human Review Required
 * (governance approval ≠ data-quality dirtiness).
 * @param {object} governance — classifyBrandGovernanceStatus result
 * @param {{ explicitly_approved?: boolean, force_human_review?: boolean }} [opts]
 */
export function buildNonActiveCensusGovernanceFields(governance, opts = {}) {
  if (!governance || governance.status === BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP) {
    return {};
  }
  if (
    governance.status !== BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE &&
    governance.status !== BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE
  ) {
    return {};
  }
  const reason = [
    REVIEW_REASON.GOVERNANCE_REVIEW_REQUIRED,
    REVIEW_REASON.PUBLIC_APPROVAL_REQUIRED,
    governance.status,
  ].join("|");
  /** @type {Record<string, unknown>} */
  const fields = {
    "Production Use Status": CENSUS_ONLY_PRODUCTION_USE_STATUS,
    "Public Display Review Status": "Hold",
    "Radar Display Status": "Hold",
    "Radar Display Reason": reason,
    "Last Reviewed Date": todayIsoDate(),
  };
  // Governance-only: do not set HR unless explicitly forced (legacy / steward override)
  if (opts.force_human_review === true && opts.explicitly_approved !== true) {
    fields["Human Review Required"] = true;
    fields["Enrichment Priority"] = "High";
  } else if (opts.explicitly_approved !== true) {
    fields["Enrichment Priority"] = "Medium";
  }
  return fields;
}

/**
 * Clean Core eligibility for evidence-backed non-active brands.
 * Active/Live brands are always brand-governance-eligible (other Clean Core gates still apply).
 * @param {object} record
 * @param {object} [opts]
 */
export function evaluateNonActiveCleanCoreEligibility(record, opts = {}) {
  const fields = record?.fields || {};
  const gov = classifyBrandGovernanceStatus({ fields }, opts);
  if (gov.status === BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP) {
    return { eligible: true, governance: gov, reasons: ["active_brand_setup"] };
  }
  if (gov.status !== BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE) {
    return {
      eligible: false,
      governance: gov,
      reasons: [`governance_${gov.status}`],
    };
  }

  const blockers = [];
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const brandFamily = String(
    fields[MAP_FIRST_PASS.brandFamily] ||
      fields["Brand Family"] ||
      fields[MAP_FIRST_PASS.family] ||
      fields["Family / Source Family"] ||
      ""
  ).trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.officialUrl] ||
      fields["Official Property URL"] ||
      fields[MAP_FIRST_PASS.sourceUrl] ||
      fields["Source URL"] ||
      ""
  ).trim();
  const useStatus = String(fields["Production Use Status"] || "").trim();
  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const entry = getCensusOfficialEntry(brand);
  const familyCanonical =
    canonicalizeParentCompany(brandFamily) ||
    canonicalizeParentCompany(entry?.parent || "") ||
    null;

  if (!isCensusOfficialBrand(brand)) blockers.push("brand_not_official_registry");
  if (!familyCanonical) blockers.push("brand_family_not_canonical");
  if (!propertyName) blockers.push("property_identity_missing");
  if (!/^https?:\/\//i.test(sourceUrl)) blockers.push("missing_official_source_url");
  if (useStatus !== CENSUS_ONLY_PRODUCTION_USE_STATUS) {
    blockers.push("must_be_census_only_not_owner_facing");
  }

  return {
    eligible: blockers.length === 0,
    governance: gov,
    reasons: blockers.length ? blockers : ["evidence_backed_non_active_clean_core_ok"],
    family_canonical: familyCanonical,
  };
}

/**
 * Encode review reason tags for Radar Display Reason / reports.
 * @param {string[]} tags
 */
export function encodeReviewReasonTags(tags = []) {
  return [...new Set(tags.filter(Boolean).map((t) => String(t).trim()))].join("|");
}

/**
 * Parse Radar Display Reason into tag set.
 * @param {string} raw
 */
export function parseReviewReasonTags(raw) {
  return String(raw || "")
    .split("|")
    .map((s) => s.trim())
    .filter(Boolean);
}

/**
 * Classify governance vs data-quality review for a Census record.
 * Governance holds do not imply dirty data.
 * @param {{ fields?: Record<string, unknown>, brand?: string }} input
 * @param {object} [opts]
 */
export function classifyCensusReviewReasons(input = {}, opts = {}) {
  const fields = input.fields || {};
  const gov = classifyBrandGovernanceStatus(input, opts);
  const tags = [];
  let governance_review_required = false;
  let data_quality_review_required = false;
  let public_approval_required = false;
  let steward_review_required = false;

  if (
    gov.status === BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL ||
    gov.status === BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED ||
    gov.status === BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS
  ) {
    data_quality_review_required = true;
    steward_review_required = true;
    tags.push(REVIEW_REASON.DATA_QUALITY_REVIEW_REQUIRED);
    tags.push(REVIEW_REASON.STEWARD_REVIEW_REQUIRED);
    tags.push(gov.status);
  }

  if (
    gov.status === BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE ||
    gov.status === BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE
  ) {
    governance_review_required = true;
    public_approval_required = true;
    tags.push(REVIEW_REASON.GOVERNANCE_REVIEW_REQUIRED);
    tags.push(REVIEW_REASON.PUBLIC_APPROVAL_REQUIRED);
    tags.push(gov.status);
  }

  const useStatus = String(fields["Production Use Status"] || "").trim();
  const publicHold = String(fields["Public Display Review Status"] || "").trim() === "Hold";
  const radarHold = String(fields["Radar Display Status"] || "").trim() === "Hold";
  if (
    !gov.owner_facing_eligible &&
    (useStatus === CENSUS_ONLY_PRODUCTION_USE_STATUS || publicHold || radarHold)
  ) {
    public_approval_required = true;
    if (!tags.includes(REVIEW_REASON.PUBLIC_APPROVAL_REQUIRED)) {
      tags.push(REVIEW_REASON.PUBLIC_APPROVAL_REQUIRED);
    }
  }

  // Existing data-quality reason tags on the record
  const existing = parseReviewReasonTags(fields["Radar Display Reason"]);
  if (existing.includes(REVIEW_REASON.DATA_QUALITY_REVIEW_REQUIRED)) {
    data_quality_review_required = true;
  }
  if (
    existing.some((t) =>
      /source_conflict|duplicate|city_|brand_blank|parent_mismatch|missing_steward/i.test(t)
    )
  ) {
    data_quality_review_required = true;
    steward_review_required = true;
  }

  return {
    governance: gov,
    governance_review_required,
    data_quality_review_required,
    public_approval_required,
    steward_review_required,
    governance_only:
      (governance_review_required || public_approval_required) && !data_quality_review_required,
    tags: [...new Set(tags)],
    reason_encoded: encodeReviewReasonTags(tags),
  };
}

/**
 * Level 2 eligibility — Clean Core + no data-quality blockers.
 * Governance Holds / Census Only / evidence-backed non-active are allowed.
 * @param {object} record
 * @param {object} [opts]
 */
export function evaluateLevel2Eligibility(record, opts = {}) {
  const fields = record?.fields || {};
  const review = classifyCensusReviewReasons({ fields }, opts);
  const reasons = [];

  if (review.data_quality_review_required) {
    reasons.push("data_quality_review_required");
  }
  if (
    review.governance?.status === BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL ||
    review.governance?.status === BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED ||
    review.governance?.status === BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS
  ) {
    reasons.push(`governance_${review.governance.status}`);
  }
  if (opts.duplicateRisk === true) reasons.push("duplicate_risk");

  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.officialUrl] ||
      fields["Official Property URL"] ||
      fields[MAP_FIRST_PASS.sourceUrl] ||
      fields["Source URL"] ||
      ""
  ).trim();
  if (!brand) reasons.push("missing_brand");
  if (!country) reasons.push("missing_country");
  if (!/^https?:\/\//i.test(sourceUrl)) reasons.push("missing_official_source_url");

  const clean =
    opts.cleanCoreResult ||
    (typeof opts.evaluateCleanCore === "function" ? opts.evaluateCleanCore(record, opts) : null);

  let cleanOk = opts.cleanCorePass === true;
  if (clean) {
    const blockersSansGovHr = (clean.blockers || []).filter((b) => {
      if (!review.governance_only) return true;
      if (b === "human_review_required") return false;
      if (String(b).startsWith("canonical_steward")) return false;
      return true;
    });
    const autofillOnly =
      opts.allowAutofillableCleanCoreGaps === true &&
      (clean.missing || []).every((m) =>
        ["Canonical Property Name", "Source Family", "Data Confidence Tier"].includes(m)
      ) &&
      blockersSansGovHr.every((b) =>
        String(b).startsWith("canonical_blank") || String(b).startsWith("canonical_dirty")
      );
    cleanOk =
      clean.pass === true ||
      (blockersSansGovHr.length === 0 && (clean.missing || []).length === 0) ||
      autofillOnly;
  } else if (opts.cleanCorePass === false) {
    cleanOk = false;
  } else if (opts.cleanCorePass == null && !clean && opts.requireCleanCore !== false) {
    reasons.push("clean_core_unknown");
  }

  if (!cleanOk) reasons.push("clean_core_not_pass");

  const unique = [...new Set(reasons)];
  return {
    eligible: unique.length === 0,
    review,
    reasons: unique,
    governance_only_hold: Boolean(review.governance_only),
    clean_core: cleanOk,
  };
}

/**
 * Build patch to separate governance-only HR from data-quality HR.
 * Clears HR when only governance/public approval; keeps Holds.
 * @param {object} record
 * @param {object} [opts]
 */
export function buildReviewReclassificationPatch(record, opts = {}) {
  const fields = record?.fields || {};
  const review = classifyCensusReviewReasons({ fields }, opts);
  /** @type {Record<string, unknown>} */
  const patch = {};
  const hr = fields[MAP_FIRST_PASS.humanReview] === true;

  if (review.data_quality_review_required) {
    if (!hr) patch["Human Review Required"] = true;
    patch["Radar Display Reason"] = encodeReviewReasonTags([
      ...review.tags,
      ...(parseReviewReasonTags(fields["Radar Display Reason"]) || []),
    ]);
    patch["Enrichment Priority"] = "High";
    patch["Last Reviewed Date"] = todayIsoDate();
    return {
      record_id: record.id,
      reason: "data_quality_review",
      patch,
      review,
    };
  }

  if (review.governance_only || review.governance_review_required || review.public_approval_required) {
    // Keep Census Only + Public/Radar Hold; clear governance-only HR
    if (hr) patch["Human Review Required"] = false;
    if (String(fields["Production Use Status"] || "") !== CENSUS_ONLY_PRODUCTION_USE_STATUS) {
      if (!review.governance?.owner_facing_eligible) {
        patch["Production Use Status"] = CENSUS_ONLY_PRODUCTION_USE_STATUS;
      }
    }
    if (String(fields["Public Display Review Status"] || "") !== "Hold") {
      if (!review.governance?.owner_facing_eligible) {
        patch["Public Display Review Status"] = "Hold";
      }
    }
    if (String(fields["Radar Display Status"] || "") !== "Hold") {
      if (!review.governance?.owner_facing_eligible) {
        patch["Radar Display Status"] = "Hold";
      }
    }
    patch["Radar Display Reason"] = review.reason_encoded || encodeReviewReasonTags(review.tags);
    patch["Last Reviewed Date"] = todayIsoDate();
    if (!Object.keys(patch).length) {
      return { record_id: record.id, reason: "noop", patch: {}, review };
    }
    return {
      record_id: record.id,
      reason: "governance_review_reclassify",
      patch,
      review,
    };
  }

  // HR set but no classified reason — leave for steward; tag steward
  if (hr) {
    const existing = parseReviewReasonTags(fields["Radar Display Reason"]);
    if (!existing.includes(REVIEW_REASON.STEWARD_REVIEW_REQUIRED) && !existing.includes(REVIEW_REASON.DATA_QUALITY_REVIEW_REQUIRED)) {
      patch["Radar Display Reason"] = encodeReviewReasonTags([
        ...existing,
        REVIEW_REASON.STEWARD_REVIEW_REQUIRED,
        "unclassified_human_review",
      ]);
      patch["Last Reviewed Date"] = todayIsoDate();
      return {
        record_id: record.id,
        reason: "unclassified_hr_tag",
        patch,
        review,
      };
    }
  }

  return { record_id: record.id, reason: "noop", patch: {}, review };
}

/**
 * Official parent-inventory discovery control list.
 * Includes Active/Live + all CENSUS_OFFICIAL_BRANDS for ready parent families.
 * Brand Setup / Brand Explorer read-only.
 * @param {object} [opts]
 */
export function buildOfficialParentInventoryDiscoveryControlList(opts = {}) {
  const active =
    opts.controlList ||
    opts.activeControlList ||
    buildActiveBrandSetupControlList(opts);
  const activeIndex = buildActiveBrandIndex({ controlList: active, ...opts });

  /** @type {Map<string, object>} */
  const byKey = new Map();

  for (const b of active.brands || []) {
    const key = norm(b.brand_name) || b.brand_slug;
    byKey.set(key, {
      ...b,
      governance_status: BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP,
      owner_facing_eligible: true,
      in_active_brand_setup: true,
      in_official_registry: isCensusOfficialBrand(b.brand_name),
    });
  }

  for (const [brandName, meta] of Object.entries(CENSUS_OFFICIAL_BRANDS)) {
    const key = norm(brandName);
    if (byKey.has(key)) continue;
    const parentRaw = meta.parent || null;
    const family =
      resolveExtractorFamily(parentRaw || brandName).family ||
      canonicalizeParentCompany(parentRaw) ||
      parentRaw ||
      "generic";
    const routingFamily =
      family === "generic"
        ? String(parentRaw || "")
            .replace(/ International.*/i, "")
            .replace(/ Hotels.*/i, "")
            .trim() || "generic"
        : family;
    // Map canonical parents to extractor families
    const extractorFamily = (() => {
      const p = String(parentRaw || "").toLowerCase();
      if (p.includes("marriott")) return "Marriott";
      if (p.includes("hilton") || p === "slh") return "Hilton";
      if (p.includes("ihg") || p.includes("intercontinental")) return "IHG";
      if (p.includes("choice")) return "Choice";
      if (p.includes("accor")) return "Accor";
      if (p.includes("wyndham")) return "Wyndham";
      if (p.includes("preferred")) return "Preferred";
      if (p.includes("bwh") || p.includes("best western")) return "BWH Hotels";
      return routingFamily;
    })();

    const readyCountries = listCountriesWithDiscoveryAdapter(extractorFamily);
    byKey.set(key, {
      brand_name: brandName,
      brand_slug: slugifyBrand(brandName),
      parent_company: canonicalizeParentCompany(parentRaw) || parentRaw,
      brand_family: extractorFamily,
      extractor_family: extractorFamily,
      brand_setup_record_id: null,
      brand_status: "Official Inventory (not Active/Live)",
      governance_status: BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE,
      owner_facing_eligible: false,
      in_active_brand_setup: false,
      in_official_registry: true,
      soft_brand_collection: Boolean(meta.soft),
      brand_setup_promotion_candidate: true,
      discovery_adapter_available: readyCountries.length > 0,
      discovery_adapter_countries: readyCountries,
      census_matching_aliases: [],
    });
  }

  const brands = [...byKey.values()];
  const parents = [
    ...new Set(brands.map((b) => b.parent_company || b.brand_family).filter(Boolean)),
  ].sort();

  return {
    version: BRAND_GOVERNANCE_VERSION,
    purpose: "official_parent_inventory_discovery",
    brand_setup_read_only: true,
    brand_explorer_untouched: true,
    discover_all_official_parents: true,
    require_brand_match_default: false,
    active_brands_in_scope: activeIndex.active_count,
    official_registry_brands_added: brands.filter((b) => !b.in_active_brand_setup).length,
    brands_in_scope: brands.length,
    parent_companies_in_scope: parents,
    brands,
    active_control_list: {
      version: active.version,
      active_brands_in_scope: active.active_brands_in_scope,
    },
    governance_status_values: Object.values(BRAND_GOVERNANCE_STATUS),
    owner_facing_rule:
      "Owner-facing / public / Dealality product use limited to Active/Live Brand Setup or explicitly approved promotions",
    census_only_rule:
      "Evidence-backed non-active brands may be saved as Census Only / Not Owner-Facing with Public/Radar Hold and Human Review Required",
  };
}

/**
 * Upsert Brand Setup promotion decision pack (read-only vs Brand Setup).
 * @param {object[]} candidates
 * @param {object} [meta]
 */
export function writeBrandSetupPromotionDecisionPack(candidates = [], meta = {}) {
  const payload = {
    generated_at: new Date().toISOString(),
    version: BRAND_GOVERNANCE_VERSION,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    note: "Decision pack only — do not auto-create or modify Brand Setup / Brand Explorer",
    ...meta,
    candidates: candidates.map((c) => ({
      proposed_brand_name: c.proposed_brand_name || c.brand,
      parent_company: c.parent_company || null,
      governance_status:
        c.governance_status || BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE,
      official_source_evidence: Boolean(c.official_source_evidence),
      appears_in_official_parent_inventory: Boolean(
        c.appears_in_official_parent_inventory ?? c.in_official_parent_inventory
      ),
      in_active_brand_setup: Boolean(c.in_active_brand_setup),
      census_records_affected: c.census_records_affected || c.count || 0,
      countries_affected: [...(c.countries_affected || [])],
      source_url_examples: c.source_url_examples || [],
      property_examples: c.property_examples || c.examples || [],
      recommended_action: c.recommended_action || "steward_review_for_brand_setup_promotion",
    })),
  };

  fs.mkdirSync(path.dirname(PROMOTION_PACK_JSON), { recursive: true });
  fs.writeFileSync(PROMOTION_PACK_JSON, `${JSON.stringify(payload, null, 2)}\n`, "utf8");

  const lines = [
    `# Brand Setup Promotion Decision Pack`,
    ``,
    `**Brand Setup writes:** false`,
    `**Brand Explorer writes:** false`,
    `**Generated:** ${payload.generated_at}`,
    `**Candidates:** ${payload.candidates.length}`,
    ``,
    `| Brand | Parent | Census records | Official inventory | Active/Live | Action |`,
    `| --- | --- | ---: | --- | --- | --- |`,
  ];
  for (const c of payload.candidates) {
    lines.push(
      `| ${c.proposed_brand_name} | ${c.parent_company || "—"} | ${c.census_records_affected} | ${c.appears_in_official_parent_inventory ? "yes" : "no"} | ${c.in_active_brand_setup ? "yes" : "no"} | ${c.recommended_action} |`
    );
  }
  lines.push(
    ``,
    `## Rules`,
    ``,
    `- Do not modify Brand Setup automatically`,
    `- Do not modify Brand Explorer`,
    `- Do not force non-active brands into an existing Active brand`,
    `- Do not drop official hotels because the brand is not currently Active`,
    `- Owner-facing remains Active/Live (or explicitly approved promotions) only`,
    ``
  );
  fs.writeFileSync(PROMOTION_PACK_MD, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath: PROMOTION_PACK_JSON, mdPath: PROMOTION_PACK_MD, payload };
}
