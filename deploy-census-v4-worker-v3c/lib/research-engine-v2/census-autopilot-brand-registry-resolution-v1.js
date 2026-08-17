/**
 * Brand Registry Resolution Mission v1.
 *
 * Resolves remaining Census brand blocks (opaque codes, unknown/not-in-registry,
 * promotion candidates, source conflicts, Human Review due to Brand).
 * Applies High-confidence Brand remaps to Hotel Property Census only.
 * Emits Brand Setup promotion decision pack (read-only — no Brand Setup writes).
 * Chains to source-confirmed-census-v2 for Clean Core recompute.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { fetchAccorCatalogByIds } from "../accor-catalog-api.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  checkAutopilotApplyEnv,
  applyPreflight,
  parseAutopilotArgs,
} from "./census-autopilot-apply-guard.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import { writeBrandSetupPromotionDecisionPack } from "./census-brand-governance.js";
import {
  ACCOR_BRAND_CODE_TO_NAME,
  accorBrandNameFromCode,
} from "./census-autopilot-accor-cala-discovery-adapter.js";
import {
  resolveCensusOfficialBrand,
  isCensusOfficialBrand,
  getCensusOfficialEntry,
  isOpaqueBrandCode,
  decodeBrandFromOfficialUrl,
  CENSUS_OFFICIAL_BRAND_REGISTRY_VERSION,
} from "./census-official-brand-registry.js";
import { runSourceConfirmedCensusV2Mission } from "./census-autopilot-source-confirmed-census-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE = "brand-registry-resolution-v1";
export const BRAND_REGISTRY_RESOLUTION_V1_VERSION = "brand-registry-resolution-v1";

export const BRAND_REGISTRY_RESOLUTION_STATUS = Object.freeze({
  COMPLETE: "production_census_brand_registry_resolution_v1_complete",
  PARTIAL: "production_census_brand_registry_resolution_v1_partial_remaining",
  NO_SAFE_WRITES:
    "production_census_brand_registry_resolution_v1_no_safe_writes_remaining",
  BLOCKED: "production_census_brand_registry_resolution_v1_blocked",
});

export const RESOLUTION_CLASS = Object.freeze({
  HIGH_CONFIDENCE_BRAND_REMAP: "high_confidence_brand_remap",
  EVIDENCE_BACKED_NON_ACTIVE_BRAND: "evidence_backed_non_active_brand",
  SOURCE_CODE_DECODED: "source_code_decoded",
  BRAND_CODE_UNRESOLVED: "brand_code_unresolved",
  BRAND_SOURCE_CONFLICT: "brand_source_conflict",
  DIRTY_PARTNER_LABEL: "dirty_partner_label",
  ALREADY_RESOLVED: "already_resolved",
  OUT_OF_SCOPE: "out_of_scope",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Address",
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Owner Name",
  "Developer Name",
  "Developer",
  "Operator / Management Company",
  "Opening Date",
  "Renovation / Conversion Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
]);

const ALLOWED_WRITE_FIELDS = Object.freeze([
  MAP_FIRST_PASS.currentBrand,
  MAP_FIRST_PASS.canonicalPropertyName,
  MAP_FIRST_PASS.brandFamily,
  MAP_FIRST_PASS.humanReview,
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  MAP_FIRST_PASS.publicDisplayReviewStatus,
  MAP_FIRST_PASS.radarDisplayStatus,
  MAP_FIRST_PASS.radarDisplayReason,
  "Last Reviewed Date",
]);

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Human Review Required",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
  "Production Use Status",
];

const TARGET_REASON_CODES = new Set([
  "brand_code_unresolved",
  "brand_unknown_not_in_registry",
  "brand_not_in_active_setup",
  "brand_setup_promotion_candidate",
  "brand_source_conflict",
  "brand_name_conflict",
]);

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

function normBrand(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * Extract Accor property id from official URL or identity key.
 * @param {string} url
 * @param {string} [identityKey]
 */
export function extractAccorPropertyId(url, identityKey = "") {
  const u = String(url || "");
  const m = u.match(/all\.accor\.com\/hotel\/([A-Za-z0-9]+)/i);
  if (m) return m[1].toUpperCase();
  const ik = String(identityKey || "");
  const m2 = ik.match(/ind_accor_[a-z]{2}_([a-z0-9]+)/i);
  if (m2) return m2[1].toUpperCase();
  return null;
}

/**
 * Dirty partner / source artifacts — never invent a brand from these.
 * @param {string} brand
 * @param {string} [propertyName]
 * @param {string} [sourceUrl]
 */
export function classifyDirtyPartnerLabel(brand, propertyName = "", sourceUrl = "") {
  const b = String(brand || "").trim();
  const n = normBrand(b);
  const hay = `${propertyName} ${sourceUrl}`.toLowerCase();

  if (/^sam$/i.test(b)) {
    return {
      dirty: true,
      reason: "accor_managed_by_sam_code",
      note: "Accor catalog brand SAM = managed by Accor / By Accor — not a product brand",
    };
  }
  if (/marriott bonvoy\s*[—-]\s*brand unconfirmed/i.test(b) || n.includes("brand unconfirmed")) {
    return {
      dirty: true,
      reason: "marriott_brand_unconfirmed",
      note: "Partner listing without confirmed Marriott brand code",
    };
  }
  if (/ihg partner/i.test(b) || /^spnd$/i.test(b) || /\/spnd\//i.test(sourceUrl)) {
    if (/ihg partner|spnd/i.test(b) || /\/spnd\//i.test(sourceUrl)) {
      return {
        dirty: true,
        reason: "ihg_partner_spnd_artifact",
        note: "IHG partner / SPND path is not a Census brand",
      };
    }
  }
  if (n === "choice hotels" || n === "choice") {
    return {
      dirty: true,
      reason: "generic_choice_partner_label",
      note: "Generic Choice Hotels label without Choice brand slug",
    };
  }
  if (/managed by accor|by accor\b/i.test(hay) && isOpaqueBrandCode(b)) {
    return {
      dirty: true,
      reason: "accor_managed_opaque",
      note: "Opaque Accor managed-by label without decodeable brand code",
    };
  }
  return { dirty: false };
}

/**
 * @param {object} dictionary
 * @param {string} brandName
 */
export function isInActiveBrandSetup(dictionary, brandName) {
  const b = String(brandName || "").trim();
  if (!b || !dictionary?.by_canonical_norm) return false;
  const n = normBrand(b);
  const c = n.replace(/[^a-z0-9]/g, "");
  return Boolean(
    dictionary.by_canonical_norm.get(n) || dictionary.by_canonical_norm.get(c)
  );
}

/**
 * Recommend Brand Setup promotion action (decision pack only).
 * @param {object} cand
 */
export function recommendPromotionAction(cand) {
  const brand = String(cand.proposed_brand_name || cand.brand || "").trim();
  const n = normBrand(brand);
  const count = cand.census_records_affected || 0;
  if (
    /unconfirmed|partner \/ spnd|choice hotels|^sam$/i.test(brand) ||
    cand.dirty_partner
  ) {
    return "steward_review_required";
  }
  if (cand.merge_target) return "merge_to_existing_brand";

  // Soft / collection brands — Census OK; Brand Setup/Explorer is a separate wave
  if (
    /handwritten|preferred|ascend|tapestry|autograph|design hotels|small luxury|registry collection|trademark collection/i.test(
      n
    )
  ) {
    return "keep_census_only";
  }

  // Newly proven Accor lifestyle / niche parent brands → promote
  if (
    /^(banyan tree|angsana|hyde|mondrian|sls|mama shelter|tribe|joia iberostar|garner|apartments by marriott bonvoy|hyde)$/i.test(
      n
    )
  ) {
    return "promote_to_brand_setup";
  }

  // Large already-Census major brands missing Active/Live — keep Census-only until Brand Explorer wave
  if (
    count >= 5 &&
    /holiday inn|jw marriott|intercontinental|crowne plaza|four points|fairfield|staybridge|iberostar|wyndham garden|sleep inn|city express|sofitel|waldorf|st\.?\s*regis|luxury collection|renaissance|ramada|candlewood/i.test(
      n
    )
  ) {
    return "keep_census_only";
  }

  if (cand.in_official_parent_inventory && count >= 1) {
    return "promote_to_brand_setup";
  }
  if (count >= 1 && cand.official_source_evidence) {
    return "promote_to_brand_setup";
  }
  return "steward_review_required";
}

function metaPatch(extra = {}) {
  return {
    "Data Confidence Tier": "High",
    "Enrichment Status": "In Progress",
    "Enrichment Priority": "High",
    "Last Reviewed Date": todayIsoDate(),
    ...extra,
  };
}

function stewardDirtyPatch(reason) {
  return {
    [MAP_FIRST_PASS.humanReview]: true,
    "Enrichment Priority": "High",
    "Last Reviewed Date": todayIsoDate(),
    [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Needs Review",
    [MAP_FIRST_PASS.radarDisplayStatus]: "Hold",
    [MAP_FIRST_PASS.radarDisplayReason]: `Brand registry: ${reason}`,
  };
}

function sanitizePatch(patch) {
  const out = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (FORBIDDEN_WRITE_FIELDS.includes(k)) continue;
    if (!ALLOWED_WRITE_FIELDS.includes(k)) continue;
    out[k] = v;
  }
  return out;
}

/**
 * Classify one Census row for brand-registry-resolution-v1.
 * @param {object} record
 * @param {{ dictionary?: object, accorCatalogById?: Map<string, object> }} [opts]
 */
export function classifyBrandRegistryResolutionRow(record, opts = {}) {
  const fields = record?.fields || {};
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const brandFamily = String(fields[MAP_FIRST_PASS.brandFamily] || "").trim();
  const sourceFamily = String(fields[MAP_FIRST_PASS.family] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || ""
  ).trim();
  const identityKey = String(fields[MAP_FIRST_PASS.identityKey] || "").trim();
  const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
  const humanReview = fields[MAP_FIRST_PASS.humanReview] === true;
  const dictionary = opts.dictionary || null;
  const accorCatalogById = opts.accorCatalogById || null;

  const base = {
    record_id: record.id,
    identity_key: identityKey || null,
    property_name: propertyName,
    brand_before: brand,
    human_review_before: humanReview,
    source_family: sourceFamily,
    brand_family: brandFamily,
    source_url: sourceUrl,
    country,
  };

  // Dirty partner first — never invent brand
  const dirty = classifyDirtyPartnerLabel(brand, propertyName, sourceUrl);
  if (dirty.dirty) {
    const needsSteward =
      !humanReview ||
      String(fields[MAP_FIRST_PASS.radarDisplayStatus] || "") !== "Hold" ||
      !String(fields[MAP_FIRST_PASS.radarDisplayReason] || "").includes(dirty.reason);
    return {
      ...base,
      class: RESOLUTION_CLASS.DIRTY_PARTNER_LABEL,
      action: needsSteward ? "flag_dirty_partner" : "none",
      high_patch: needsSteward
        ? sanitizePatch(stewardDirtyPatch(dirty.reason))
        : null,
      reason_code: dirty.reason,
      brand_after: brand,
      target: true,
      exclude_from_clean_core: true,
    };
  }

  // Accor catalog hydrate for opaque codes still unresolved by static map
  let catalogHint = null;
  if (
    (isOpaqueBrandCode(brand) || /^[A-Za-z]{2,3}$/.test(brand)) &&
    /accor/i.test(sourceFamily || sourceUrl)
  ) {
    const pid = extractAccorPropertyId(sourceUrl, identityKey);
    if (pid && accorCatalogById?.has(pid)) {
      const hotel = accorCatalogById.get(pid);
      const code = String(hotel?.brand || "").trim().toUpperCase();
      const mapped = ACCOR_BRAND_CODE_TO_NAME[code] || accorBrandNameFromCode(code);
      if (mapped && getCensusOfficialEntry(mapped) && !/^SAM$/i.test(code)) {
        catalogHint = {
          ok: true,
          canonical: getCensusOfficialEntry(mapped).canonical,
          parent: getCensusOfficialEntry(mapped).parent || "Accor",
          method: "accor_catalog_brand_code",
          confidence: "High",
          was_opaque_code: true,
          catalog_code: code,
          property_id: pid,
        };
      } else if (/^SAM$/i.test(code) || /^SAM$/i.test(brand)) {
        const needsSteward =
          !humanReview ||
          String(fields[MAP_FIRST_PASS.radarDisplayStatus] || "") !== "Hold";
        return {
          ...base,
          class: RESOLUTION_CLASS.DIRTY_PARTNER_LABEL,
          action: needsSteward ? "flag_dirty_partner" : "none",
          high_patch: needsSteward
            ? sanitizePatch(stewardDirtyPatch("accor_managed_by_sam_code"))
            : null,
          reason_code: "accor_managed_by_sam_code",
          brand_after: brand,
          target: true,
          exclude_from_clean_core: true,
          catalog_code: code || "SAM",
        };
      }
    }
  }

  const resolved =
    catalogHint ||
    resolveCensusOfficialBrand(brand, {
      propertyName,
      sourceUrl,
      sourceFamily,
    });

  // High remap / decoded code
  if (resolved.ok && resolved.confidence === "High" && !resolved.already_canonical) {
    const entry = getCensusOfficialEntry(resolved.canonical);
    const inActive = isInActiveBrandSetup(dictionary, resolved.canonical);
    const alreadySame = normBrand(resolved.canonical) === normBrand(brand);
    if (alreadySame && !humanReview) {
      // Code map resolved to same display brand — no write
      return {
        ...base,
        class: inActive
          ? RESOLUTION_CLASS.ALREADY_RESOLVED
          : RESOLUTION_CLASS.EVIDENCE_BACKED_NON_ACTIVE_BRAND,
        action: "none",
        reason_code: inActive
          ? "already_official_active"
          : "evidence_backed_non_active_brand",
        brand_after: resolved.canonical,
        target: !inActive,
        brand_setup_promotion_candidate: !inActive,
        in_active_brand_setup: inActive,
        official_source_evidence: true,
      };
    }
    const wasOpaque =
      Boolean(resolved.was_opaque_code) ||
      isOpaqueBrandCode(brand) ||
      /^pt\s*br$/i.test(brand) ||
      Boolean(catalogHint);
    const className = wasOpaque
      ? RESOLUTION_CLASS.SOURCE_CODE_DECODED
      : inActive
        ? RESOLUTION_CLASS.HIGH_CONFIDENCE_BRAND_REMAP
        : RESOLUTION_CLASS.EVIDENCE_BACKED_NON_ACTIVE_BRAND;

    const patch = sanitizePatch({
      [MAP_FIRST_PASS.currentBrand]: resolved.canonical,
      ...(resolved.parent || entry?.parent
        ? { [MAP_FIRST_PASS.brandFamily]: resolved.parent || entry.parent }
        : {}),
      ...(humanReview ? { [MAP_FIRST_PASS.humanReview]: false } : {}),
      ...metaPatch(),
    });

    return {
      ...base,
      class: className,
      action: "update_brand",
      high_patch: patch,
      reason_code: resolved.method || className,
      brand_after: resolved.canonical,
      method: resolved.method,
      target: true,
      brand_setup_promotion_candidate: !inActive,
      in_active_brand_setup: inActive,
      official_source_evidence: true,
    };
  }

  // Already canonical official
  if (resolved.ok && resolved.already_canonical) {
    const inActive = isInActiveBrandSetup(dictionary, resolved.canonical);
    if (!inActive) {
      return {
        ...base,
        class: RESOLUTION_CLASS.EVIDENCE_BACKED_NON_ACTIVE_BRAND,
        action: humanReview ? "clear_human_review_if_confirmed" : "none",
        high_patch: humanReview
          ? sanitizePatch({
              [MAP_FIRST_PASS.humanReview]: false,
              ...metaPatch(),
            })
          : null,
        reason_code: "evidence_backed_non_active_brand",
        brand_after: resolved.canonical,
        target: true,
        brand_setup_promotion_candidate: true,
        in_active_brand_setup: false,
        official_source_evidence: true,
      };
    }
    if (humanReview) {
      const urlDecoded = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
      if (
        urlDecoded.ok &&
        normBrand(urlDecoded.canonical) === normBrand(resolved.canonical)
      ) {
        return {
          ...base,
          class: RESOLUTION_CLASS.HIGH_CONFIDENCE_BRAND_REMAP,
          action: "clear_human_review",
          high_patch: sanitizePatch({
            [MAP_FIRST_PASS.humanReview]: false,
            ...metaPatch(),
          }),
          reason_code: "human_review_cleared_brand_confirmed",
          brand_after: resolved.canonical,
          target: true,
        };
      }
      return {
        ...base,
        class: RESOLUTION_CLASS.BRAND_SOURCE_CONFLICT,
        action: "none",
        reason_code: "brand_source_conflict",
        brand_after: brand,
        target: true,
      };
    }
    return {
      ...base,
      class: RESOLUTION_CLASS.ALREADY_RESOLVED,
      action: "none",
      reason_code: "already_official_active",
      brand_after: brand,
      target: false,
    };
  }

  // Opaque unresolved
  if (
    resolved.steward_code === "brand_code_unresolved" ||
    isOpaqueBrandCode(brand)
  ) {
    const needsSteward = !humanReview;
    return {
      ...base,
      class: RESOLUTION_CLASS.BRAND_CODE_UNRESOLVED,
      action: needsSteward ? "flag_human_review" : "none",
      high_patch: needsSteward
        ? sanitizePatch(stewardDirtyPatch("brand_code_unresolved"))
        : null,
      reason_code: "brand_code_unresolved",
      brand_after: brand,
      target: true,
      exclude_from_clean_core: true,
    };
  }

  if (
    resolved.steward_code === "brand_setup_promotion_candidate" ||
    resolved.steward_code === "brand_unknown_not_in_registry"
  ) {
    // URL may still prove a registry brand
    const fromUrl = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
    if (fromUrl.ok && getCensusOfficialEntry(fromUrl.canonical)) {
      const entry = getCensusOfficialEntry(fromUrl.canonical);
      const inActive = isInActiveBrandSetup(dictionary, fromUrl.canonical);
      const className = inActive
        ? RESOLUTION_CLASS.HIGH_CONFIDENCE_BRAND_REMAP
        : RESOLUTION_CLASS.EVIDENCE_BACKED_NON_ACTIVE_BRAND;
      return {
        ...base,
        class: className,
        action: "update_brand",
        high_patch: sanitizePatch({
          [MAP_FIRST_PASS.currentBrand]: fromUrl.canonical,
          ...(entry?.parent ? { [MAP_FIRST_PASS.brandFamily]: entry.parent } : {}),
          ...(humanReview ? { [MAP_FIRST_PASS.humanReview]: false } : {}),
          ...metaPatch(),
        }),
        reason_code: fromUrl.method || "url_decode_remap",
        brand_after: fromUrl.canonical,
        target: true,
        brand_setup_promotion_candidate: !inActive,
        in_active_brand_setup: inActive,
        official_source_evidence: true,
      };
    }

    const looksReal =
      brand.length >= 4 &&
      !isOpaqueBrandCode(brand) &&
      !/unconfirmed|partner/i.test(brand);
    return {
      ...base,
      class: looksReal
        ? RESOLUTION_CLASS.EVIDENCE_BACKED_NON_ACTIVE_BRAND
        : RESOLUTION_CLASS.BRAND_CODE_UNRESOLVED,
      action: "none",
      reason_code: resolved.steward_code,
      brand_after: brand,
      target: true,
      brand_setup_promotion_candidate: looksReal,
      exclude_from_clean_core: !looksReal || !isCensusOfficialBrand(brand),
    };
  }

  if (humanReview) {
    return {
      ...base,
      class: RESOLUTION_CLASS.BRAND_SOURCE_CONFLICT,
      action: "none",
      reason_code: "human_review_brand_unresolved",
      brand_after: brand,
      target: true,
    };
  }

  return {
    ...base,
    class: RESOLUTION_CLASS.OUT_OF_SCOPE,
    action: "none",
    reason_code: "out_of_scope",
    brand_after: brand,
    target: false,
  };
}

/**
 * @param {object[]} censusRecords
 * @param {{ dictionary?: object, accorCatalogById?: Map }} [opts]
 */
export function buildBrandRegistryResolutionPlan(censusRecords = [], opts = {}) {
  const dictionary = opts.dictionary || null;
  const accorCatalogById = opts.accorCatalogById || null;
  const rows = [];
  const proposals = [];
  const steward = [];
  const examples = [];
  const promotionMap = new Map();
  const counters = {
    records_scanned: 0,
    targets: 0,
    high_confidence_brand_remap: 0,
    evidence_backed_non_active_brand: 0,
    source_code_decoded: 0,
    brand_code_unresolved: 0,
    brand_source_conflict: 0,
    dirty_partner_label: 0,
    already_resolved: 0,
    out_of_scope: 0,
    high_writes_planned: 0,
  };

  for (const rec of censusRecords) {
    counters.records_scanned += 1;
    const row = classifyBrandRegistryResolutionRow(rec, {
      dictionary,
      accorCatalogById,
    });
    rows.push(row);
    if (counters[row.class] != null) counters[row.class] += 1;
    if (row.target) counters.targets += 1;

    if (row.brand_setup_promotion_candidate && row.brand_after && !row.in_active_brand_setup) {
      // Decision pack: remaps / decoded codes only — not every Active-gap already-canonical brand
      const packWorthy =
        row.class === RESOLUTION_CLASS.SOURCE_CODE_DECODED ||
        row.action === "update_brand" ||
        (row.class === RESOLUTION_CLASS.EVIDENCE_BACKED_NON_ACTIVE_BRAND &&
          row.reason_code === "evidence_backed_non_active_brand" &&
          /^(banyan tree|angsana|hyde|mondrian|sls|handwritten collection|mama shelter|tribe|joia iberostar|garner|apartments by marriott bonvoy)$/i.test(
            normBrand(row.brand_after)
          ));
      if (packWorthy) {
      const key = row.brand_after;
      if (!promotionMap.has(key)) {
        promotionMap.set(key, {
          proposed_brand_name: key,
          parent_company: row.brand_family || row.source_family || null,
          source_url_examples: [],
          official_source_evidence: Boolean(row.official_source_evidence),
          census_records_affected: 0,
          countries_affected: new Set(),
          in_official_parent_inventory: Boolean(row.official_source_evidence),
          in_active_brand_setup: Boolean(row.in_active_brand_setup),
          property_examples: [],
          record_ids: [],
        });
      }
      const p = promotionMap.get(key);
      p.census_records_affected += 1;
      if (row.country) p.countries_affected.add(row.country);
      if (row.source_url && p.source_url_examples.length < 5) {
        p.source_url_examples.push(row.source_url);
      }
      if (row.property_name && p.property_examples.length < 5) {
        p.property_examples.push(row.property_name);
      }
      if (p.record_ids.length < 20) p.record_ids.push(row.record_id);
      if (!p.parent_company) p.parent_company = row.source_family || null;
      p.official_source_evidence =
        p.official_source_evidence || Boolean(row.official_source_evidence);
      p.in_official_parent_inventory =
        p.in_official_parent_inventory || Boolean(row.official_source_evidence);
      }
    }

    if (row.target && row.high_patch && Object.keys(row.high_patch).length) {
      proposals.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        queue: "brand_normalization",
        confidence: "High",
        action: "update",
        patch: row.high_patch,
        fields: row.high_patch,
        brand_before: row.brand_before,
        brand_after: row.brand_after,
        classification: row.class,
        method: row.method || row.reason_code,
        allow_normalization_overwrite: true,
      });
      counters.high_writes_planned += 1;
      if (examples.length < 40) {
        examples.push({
          record_id: row.record_id,
          property_name: row.property_name,
          before: row.brand_before,
          after: row.brand_after,
          class: row.class,
          reason: row.reason_code,
        });
      }
    }

    if (
      row.target &&
      (row.class === RESOLUTION_CLASS.BRAND_CODE_UNRESOLVED ||
        row.class === RESOLUTION_CLASS.DIRTY_PARTNER_LABEL ||
        row.class === RESOLUTION_CLASS.BRAND_SOURCE_CONFLICT)
    ) {
      steward.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        brand: row.brand_before,
        reason_code: row.reason_code,
        class: row.class,
        source_family: row.source_family,
        source_url: row.source_url,
        country: row.country,
      });
    }

    void TARGET_REASON_CODES;
  }

  const promotion_candidates = [...promotionMap.values()]
    .map((p) => {
      const countries = [...p.countries_affected].sort();
      const cand = {
        proposed_brand_name: p.proposed_brand_name,
        parent_company: p.parent_company,
        source_url_examples: p.source_url_examples,
        official_source_evidence: p.official_source_evidence,
        census_records_affected: p.census_records_affected,
        countries_affected: countries,
        appears_in_official_parent_inventory: p.in_official_parent_inventory,
        in_active_brand_setup: p.in_active_brand_setup,
        property_examples: p.property_examples,
        record_ids: p.record_ids,
      };
      cand.recommended_action = recommendPromotionAction(cand);
      return cand;
    })
    .sort((a, b) => b.census_records_affected - a.census_records_affected);

  return {
    version: BRAND_REGISTRY_RESOLUTION_V1_VERSION,
    objective: BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE,
    registry_version: CENSUS_OFFICIAL_BRAND_REGISTRY_VERSION,
    counters,
    proposals,
    steward_cases: steward,
    promotion_candidates,
    examples_before_after: examples,
    rows,
  };
}

async function listCensus(baseId, token, tableId) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

function countCleanCore(records, dictionary) {
  let n = 0;
  for (const r of records) {
    try {
      const ev = evaluateCleanCorePass(r, { dictionary, skipBrandSourceOfTruth: false });
      if (ev?.pass) n += 1;
    } catch {
      /* ignore */
    }
  }
  return n;
}

function countUnknown(records) {
  return records.filter((r) => {
    const b = String(r.fields?.[MAP_FIRST_PASS.currentBrand] || "").trim();
    return b && !isCensusOfficialBrand(b);
  }).length;
}

function countHrBrand(records) {
  return records.filter((r) => r.fields?.[MAP_FIRST_PASS.humanReview] === true).length;
}

function countUnresolvedCodes(records, dictionary, accorCatalogById) {
  let n = 0;
  for (const r of records) {
    const row = classifyBrandRegistryResolutionRow(r, { dictionary, accorCatalogById });
    if (row.class === RESOLUTION_CLASS.BRAND_CODE_UNRESOLVED) n += 1;
  }
  return n;
}

/**
 * Prefetch Accor catalog for opaque Accor steward rows.
 * @param {object[]} censusRecords
 */
export async function hydrateAccorCatalogForOpaqueCodes(censusRecords = [], opts = {}) {
  const ids = [];
  for (const rec of censusRecords) {
    const fields = rec.fields || {};
    const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
    const sourceFamily = String(fields[MAP_FIRST_PASS.family] || "");
    const sourceUrl = String(
      fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || ""
    );
    if (!/accor/i.test(sourceFamily || sourceUrl)) continue;
    if (!(isOpaqueBrandCode(brand) || /^[A-Za-z]{2,3}$/.test(brand))) continue;
    const pid = extractAccorPropertyId(
      sourceUrl,
      fields[MAP_FIRST_PASS.identityKey]
    );
    if (pid) ids.push(pid);
  }
  /** @type {Map<string, object>} */
  const byId = new Map();
  const unique = [...new Set(ids)];
  if (!unique.length) return byId;

  const log = opts.log || (() => {});
  log(`[brand-registry-v1] Accor catalog hydrate for ${unique.length} property ids…`);
  for (let i = 0; i < unique.length; i += 40) {
    const chunk = unique.slice(i, i + 40);
    try {
      const res = await fetchAccorCatalogByIds(chunk, opts);
      for (const h of res.hotels || []) {
        if (h?.propertyId) byId.set(String(h.propertyId).toUpperCase(), h);
      }
    } catch (err) {
      log(`[brand-registry-v1] Accor catalog chunk failed: ${err?.message || err}`);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  return byId;
}

function renderPromotionMd(candidates) {
  const lines = [
    `# Brand Setup Promotion Candidates — Census Brand Registry Resolution v1`,
    ``,
    `Read-only decision pack. **Brand Setup was not modified.**`,
    ``,
    `| Brand | Parent | Records | Countries | In parent inventory | Recommended action |`,
    `| --- | --- | ---: | --- | --- | --- |`,
  ];
  for (const c of candidates || []) {
    lines.push(
      `| ${c.proposed_brand_name} | ${c.parent_company || "—"} | ${c.census_records_affected} | ${(c.countries_affected || []).join(", ") || "—"} | ${c.appears_in_official_parent_inventory ? "yes" : "no"} | \`${c.recommended_action}\` |`
    );
  }
  if (!(candidates || []).length) lines.push(`| _None_ | | | | | |`);

  lines.push(``, `## Details`, ``);
  for (const c of candidates || []) {
    lines.push(`### ${c.proposed_brand_name}`);
    lines.push(`- Parent: ${c.parent_company || "—"}`);
    lines.push(`- Census records: ${c.census_records_affected}`);
    lines.push(`- Countries: ${(c.countries_affected || []).join(", ") || "—"}`);
    lines.push(`- Recommended: \`${c.recommended_action}\``);
    lines.push(`- Evidence: ${c.official_source_evidence ? "yes" : "no"}`);
    for (const u of c.source_url_examples || []) lines.push(`  - ${u}`);
    lines.push(``);
  }
  return lines.join("\n");
}

function renderMd(report) {
  const c = report.counters || {};
  const lines = [
    `# Brand Registry Resolution v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE}\``,
    `**Write target:** Hotel Property Census (\`${report.write_target?.table_id}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup writes:** false`,
    `**Brand Explorer writes:** false`,
    ``,
    `## Before / After`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Unknown brands (not in official census registry) | ${report.before?.unknown ?? "—"} | ${report.after?.unknown ?? "—"} |`,
    `| Human Review (brand-related) | ${report.before?.human_review ?? "—"} | ${report.after?.human_review ?? "—"} |`,
    `| brand_code_unresolved | ${report.before?.brand_code_unresolved ?? "—"} | ${report.after?.brand_code_unresolved ?? "—"} |`,
    `| Clean Core pass (approx) | ${report.before?.clean_core ?? "—"} | ${report.after?.clean_core ?? "—"} |`,
    `| Excluded from Clean Core | ${report.before?.excluded_from_clean_core ?? "—"} | ${report.after?.excluded_from_clean_core ?? "—"} |`,
    `| High brand remaps applied | — | ${report.high_brand_remaps_applied ?? 0} |`,
    `| Dirty partner labels | — | ${c.dirty_partner_label ?? 0} |`,
    `| Evidence-backed non-active brands | — | ${c.evidence_backed_non_active_brand ?? 0} |`,
    `| Brand Setup promotion candidates | — | ${(report.promotion_candidates || []).length} |`,
    ``,
    `## Classification counters`,
    ``,
    `| Class | Count |`,
    `| --- | ---: |`,
    `| Scanned | ${c.records_scanned ?? 0} |`,
    `| Targets | ${c.targets ?? 0} |`,
    `| high_confidence_brand_remap | ${c.high_confidence_brand_remap ?? 0} |`,
    `| evidence_backed_non_active_brand | ${c.evidence_backed_non_active_brand ?? 0} |`,
    `| source_code_decoded | ${c.source_code_decoded ?? 0} |`,
    `| brand_code_unresolved | ${c.brand_code_unresolved ?? 0} |`,
    `| brand_source_conflict | ${c.brand_source_conflict ?? 0} |`,
    `| dirty_partner_label | ${c.dirty_partner_label ?? 0} |`,
    ``,
    `## Examples before / after`,
    ``,
  ];
  for (const ex of report.examples_before_after || []) {
    lines.push(
      `- \`${ex.before}\` → \`${ex.after}\` (${ex.class} / ${ex.reason}) — ${ex.property_name || ex.record_id}`
    );
  }
  if (!(report.examples_before_after || []).length) lines.push(`_None_`);

  lines.push(``, `## Unresolved steward`, ``);
  for (const s of (report.steward_cases || []).slice(0, 50)) {
    lines.push(
      `- \`${s.brand}\` — ${s.reason_code} / ${s.class} — ${s.property_name || s.record_id}`
    );
  }
  if (!(report.steward_cases || []).length) lines.push(`_None_`);

  lines.push(
    ``,
    `## Chained source-confirmed-census-v2`,
    ``,
    `- Ran: ${report.chained_source_confirmed?.ran ? "yes" : "no"}`,
    `- Status: \`${report.chained_source_confirmed?.status || "—"}\``,
    `- Updates: ${report.chained_source_confirmed?.updates_applied ?? "—"}`,
    `- Clean Core after chain: ${report.chained_source_confirmed?.clean_core_after ?? "—"}`,
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- Brand Setup / Brand Explorer untouched`,
    `- No address / coords / phone / rooms`,
    `- No owner/operator/date writes`,
    `- No opaque code guessing / no hotel-name-only brand inference`,
    `- Evidence-backed non-active brands reported in promotion pack, not forced into Active dictionary`,
    `- Unresolved codes excluded from Clean Core`,
    ``
  );
  return lines.join("\n");
}

export function writeBrandRegistryResolutionReports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-brand-registry-resolution-v1.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-brand-registry-resolution-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-brand-registry-resolution-v1.md"
  );
  const promoJson = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-brand-setup-promotion-candidates.json"
  );
  const promoMd = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-brand-setup-promotion-candidates.md"
  );
  const md = renderMd(report);
  writeJson(jsonPath, report);
  writeText(mdPath, md);
  writeText(docsPath, md);
  const promo = writeBrandSetupPromotionDecisionPack(report.promotion_candidates || [], {
    source: "brand_registry_resolution_v1",
    brand_registry_version: BRAND_REGISTRY_RESOLUTION_V1_VERSION,
  });
  return {
    jsonPath,
    mdPath,
    docsPath,
    promoJson: promo.jsonPath || promoJson,
    promoMd: promo.mdPath || promoMd,
  };
}

async function applyPatches(proposals, { baseId, token, tableId, batchSize, log }) {
  let updatesApplied = 0;
  const writtenIds = [];
  const writeErrors = [];
  const size = Math.min(100, Math.max(1, batchSize || 100));
  for (let i = 0; i < proposals.length; i += size) {
    const chunk = proposals.slice(i, i + size);
    const updates = chunk.map((p) => ({ id: p.record_id, fields: p.patch }));
    for (let j = 0; j < updates.length; j += 10) {
      const records = updates.slice(j, j + 10);
      try {
        const res = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
          {
            method: "PATCH",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ records, typecast: true }),
          }
        );
        const json = await res.json().catch(() => ({}));
        if (!res.ok) {
          writeErrors.push({
            status: res.status,
            error: json.error || json,
            ids: records.map((r) => r.id),
          });
        } else {
          for (const r of json.records || []) {
            writtenIds.push(r.id);
            updatesApplied += 1;
          }
        }
      } catch (err) {
        writeErrors.push({ error: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
    log(
      `[brand-registry-v1] batch ${Math.floor(i / size) + 1}: written=${updatesApplied} errors=${writeErrors.length}`
    );
  }
  return { updatesApplied, writtenIds, writeErrors };
}

/**
 * Mission entrypoint. Optionally chains source-confirmed-census-v2.
 */
export async function runBrandRegistryResolutionV1Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(m));
  const started = Date.now();
  const chainSourceConfirmed = opts.chainSourceConfirmed !== false;

  const envCheck = checkAutopilotApplyEnv(env);
  const preflight = applyPreflight(args, envCheck);
  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms &&
      envCheck.allOk &&
      preflight.ok
  );

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    const blocked = {
      ok: false,
      status: BRAND_REGISTRY_RESOLUTION_STATUS.BLOCKED,
      objective: BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE,
      blocked_reason: writeTarget.reason || "wrong_census_target",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
    };
    writeBrandRegistryResolutionReports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: BRAND_REGISTRY_RESOLUTION_STATUS.BLOCKED,
      objective: BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
    };
    writeBrandRegistryResolutionReports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: BRAND_REGISTRY_RESOLUTION_STATUS.BLOCKED,
      objective: BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
    };
    writeBrandRegistryResolutionReports(blocked);
    return blocked;
  }

  const region = args.region || "CALA";
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-brand-registry-resolution-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  log(`[brand-registry-v1] listing Hotel Property Census…`);
  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const dictionary = buildCanonicalBrandDictionary({ region });
  const accorCatalogById = await hydrateAccorCatalogForOpaqueCodes(census, { log });

  const before = {
    records: census.length,
    unknown: countUnknown(census),
    human_review: countHrBrand(census),
    brand_code_unresolved: countUnresolvedCodes(census, dictionary, accorCatalogById),
    clean_core: countCleanCore(census, dictionary),
  };
  before.excluded_from_clean_core = census.length - before.clean_core;
  log(
    `[brand-registry-v1] before unknown=${before.unknown} hr=${before.human_review} unresolved_codes=${before.brand_code_unresolved} clean_core≈${before.clean_core}`
  );

  const maxPasses = Math.min(6, Math.max(1, args.maxPasses || 6));
  let updatesApplied = 0;
  const writtenIds = [];
  const writeErrors = [];
  let plan = buildBrandRegistryResolutionPlan(census, {
    dictionary,
    accorCatalogById,
  });

  for (let pass = 1; pass <= maxPasses; pass += 1) {
    writeJson(path.join(runDir, `pass-${pass}-classification.json`), {
      counters: plan.counters,
      proposals: plan.proposals.length,
      steward: plan.steward_cases.length,
      promotion_candidates: plan.promotion_candidates,
    });

    if (!enableWrites || !plan.proposals.length) {
      log(
        `[brand-registry-v1] pass ${pass}: ${plan.proposals.length} proposals (${enableWrites ? "none left" : "dry"})`
      );
      break;
    }

    log(`[brand-registry-v1] pass ${pass}: applying ${plan.proposals.length} High patches…`);
    const applied = await applyPatches(plan.proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      batchSize: args.batchSize || 100,
      log,
    });
    updatesApplied += applied.updatesApplied;
    writtenIds.push(...applied.writtenIds);
    writeErrors.push(...applied.writeErrors);

    if (applied.updatesApplied === 0) break;

    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
    plan = buildBrandRegistryResolutionPlan(census, {
      dictionary,
      accorCatalogById,
    });
    // Stop when remaining proposals are only idempotent steward re-flags
    const brandWriteCount = plan.proposals.filter(
      (p) => p.patch && Object.prototype.hasOwnProperty.call(p.patch, MAP_FIRST_PASS.currentBrand)
    ).length;
    if (!plan.proposals.length) break;
    if (brandWriteCount === 0 && pass >= 1) {
      log(
        `[brand-registry-v1] pass ${pass}: only steward re-flags remain (${plan.proposals.length}) — stopping`
      );
      break;
    }
  }

  if (enableWrites && updatesApplied > 0) {
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }
  const afterPlan = buildBrandRegistryResolutionPlan(census, {
    dictionary,
    accorCatalogById,
  });
  const after = {
    records: census.length,
    unknown: countUnknown(census),
    human_review: countHrBrand(census),
    brand_code_unresolved: countUnresolvedCodes(census, dictionary, accorCatalogById),
    clean_core: countCleanCore(census, dictionary),
  };
  after.excluded_from_clean_core = census.length - after.clean_core;

  let chained = { ran: false };
  if (chainSourceConfirmed && args.mode === "mission") {
    log(`[brand-registry-v1] chaining source-confirmed-census-v2…`);
    try {
      const sc = await runSourceConfirmedCensusV2Mission({
        argv,
        args,
        env,
        enableProductionWrites: enableWrites,
        token,
        bases,
        log,
      });
      chained = {
        ran: true,
        status: sc.status,
        updates_applied: sc.updates_applied,
        before: sc.before,
        after: sc.after,
        clean_core_after: sc.after?.clean_core,
        unknown_after: sc.after?.unknown,
      };
      // Refresh census metrics from chained after if present
      if (sc.after) {
        after.unknown = sc.after.unknown ?? after.unknown;
        after.human_review = sc.after.human_review ?? after.human_review;
        after.clean_core = sc.after.clean_core ?? after.clean_core;
        after.excluded_from_clean_core =
          sc.after.excluded_from_clean_core ??
          after.records - after.clean_core;
      }
    } catch (err) {
      log(`[brand-registry-v1] chained source-confirmed failed: ${err?.message || err}`);
      chained = { ran: true, error: err?.message || String(err) };
    }
  }

  let status = BRAND_REGISTRY_RESOLUTION_STATUS.PARTIAL;
  if (writeErrors.length && updatesApplied === 0 && enableWrites && plan.proposals.length) {
    status = BRAND_REGISTRY_RESOLUTION_STATUS.BLOCKED;
  } else if (
    !enableWrites ||
    (updatesApplied === 0 &&
      afterPlan.counters.high_writes_planned === 0 &&
      (afterPlan.counters.brand_code_unresolved > 0 ||
        afterPlan.counters.dirty_partner_label > 0 ||
        afterPlan.counters.brand_source_conflict > 0))
  ) {
    if (enableWrites && updatesApplied === 0) {
      status = BRAND_REGISTRY_RESOLUTION_STATUS.NO_SAFE_WRITES;
    } else if (!enableWrites) {
      status = BRAND_REGISTRY_RESOLUTION_STATUS.PARTIAL;
    }
  }

  if (
    after.brand_code_unresolved === 0 &&
    afterPlan.counters.dirty_partner_label === 0 &&
    afterPlan.counters.brand_source_conflict === 0 &&
    after.unknown === 0
  ) {
    status = BRAND_REGISTRY_RESOLUTION_STATUS.COMPLETE;
  } else if (
    updatesApplied > 0 &&
    (after.brand_code_unresolved > 0 ||
      afterPlan.counters.dirty_partner_label > 0 ||
      after.unknown > 0)
  ) {
    status = BRAND_REGISTRY_RESOLUTION_STATUS.PARTIAL;
  } else if (
    enableWrites &&
    updatesApplied === 0 &&
    afterPlan.counters.high_writes_planned === 0
  ) {
    status = BRAND_REGISTRY_RESOLUTION_STATUS.NO_SAFE_WRITES;
  }

  const highRemaps =
    (afterPlan.counters.high_confidence_brand_remap || 0) +
    (afterPlan.counters.source_code_decoded || 0);

  const report = {
    ok: status !== BRAND_REGISTRY_RESOLUTION_STATUS.BLOCKED,
    status,
    objective: BRAND_REGISTRY_RESOLUTION_V1_OBJECTIVE,
    version: BRAND_REGISTRY_RESOLUTION_V1_VERSION,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
    airtable_writes: enableWrites && updatesApplied > 0,
    before,
    after,
    counters: afterPlan.counters,
    high_brand_remaps_applied: updatesApplied,
    proposals_planned: plan.proposals.length,
    steward_cases: afterPlan.steward_cases,
    promotion_candidates: afterPlan.promotion_candidates,
    examples_before_after: plan.examples_before_after,
    write_errors: writeErrors.slice(0, 20),
    written_ids: writtenIds,
    run_dir: runDir,
    runtime_ms: Date.now() - started,
    chained_source_confirmed: chained,
    classification_note:
      "Categories A–F: high_confidence_brand_remap | evidence_backed_non_active_brand | source_code_decoded | brand_code_unresolved | brand_source_conflict | dirty_partner_label",
    next_recommended_action:
      status === BRAND_REGISTRY_RESOLUTION_STATUS.COMPLETE
        ? "Brand identity complete enough for Level 2 enrichment planning"
        : "Steward remaining opaque/dirty labels; promote Brand Setup candidates via separate founder decision; do not guess codes",
  };

  void highRemaps;
  writeJson(path.join(runDir, "final-summary.json"), report);
  writeText(path.join(runDir, "final-summary.md"), renderMd(report));
  writeBrandRegistryResolutionReports(report);
  log(
    `[brand-registry-v1] status=${status} updates=${updatesApplied} unknown ${before.unknown}→${after.unknown} clean_core ${before.clean_core}→${after.clean_core}`
  );
  return report;
}
