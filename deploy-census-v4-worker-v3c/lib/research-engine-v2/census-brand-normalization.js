/**
 * Brand Normalization + Brand Source-of-Truth Gate for Hotel Property Census.
 *
 * Queue: brand_normalization
 * Writes Current Brand (+ Brand Family when High parent consistent).
 * Never Brand Setup / Brand Explorer / enrichment coords/phone/rooms.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  buildCanonicalBrandDictionary,
  lookupCanonicalBrand,
  familyFromOfficialUrl,
  SOFT_BRAND_COLLECTION_SLUGS,
} from "./census-brand-canonical-dictionary.js";
import {
  isCensusOfficialBrand,
  isOpaqueBrandCode,
} from "./census-official-brand-registry.js";
import { isOwnerFacingBrandEligible } from "./census-brand-governance.js";
import { CENSUS_ONLY_PRODUCTION_USE_STATUS } from "./census-brand-governance.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";
import { canonicalizeParentCompany } from "./census-parent-company-normalization.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const BRAND_NORMALIZATION_QUEUE_ID = "brand_normalization";
export const BRAND_NORMALIZATION_VERSION = "census-brand-normalization-v1";

export const BRAND_NORMALIZATION_STATUS = Object.freeze({
  APPLIED_CLEAN: "production_census_brand_normalization_applied_clean",
  PARTIAL: "production_census_brand_normalization_partial_steward_remaining",
  READY_NEEDS_MISSION: "production_census_brand_normalization_ready_needs_mission",
  BLOCKED: "production_census_brand_normalization_blocked",
});

export const BRAND_CLASS = Object.freeze({
  VALID: "brand_valid",
  BLANK: "brand_blank",
  MISSPELLED: "brand_misspelled",
  ALIAS_NORMALIZABLE: "brand_alias_normalizable",
  PARENT_MISMATCH: "brand_parent_mismatch",
  SOURCE_MISMATCH: "brand_source_mismatch",
  PROPERTY_NAME_MISMATCH: "brand_property_name_mismatch",
  SOFT_BRAND_CONFLICT: "soft_brand_collection_conflict",
  UNKNOWN: "brand_unknown_not_in_dictionary",
  STEWARD: "steward_review_required",
});

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

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

function familiesCompatible(a, b) {
  if (!a || !b) return true;
  const na = norm(a);
  const nb = norm(b);
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  // Marriott International ↔ Marriott
  if (/marriott/i.test(a) && /marriott/i.test(b)) return true;
  if (/hilton/i.test(a) && /hilton/i.test(b)) return true;
  if (/ihg|intercontinental/i.test(a) && /ihg|intercontinental/i.test(b)) return true;
  if (/choice|radisson/i.test(a) && /choice|radisson/i.test(b)) return true;
  if (/accor/i.test(a) && /accor/i.test(b)) return true;
  if (/wyndham/i.test(a) && /wyndham/i.test(b)) return true;
  if (/preferred/i.test(a) && /preferred/i.test(b)) return true;
  if (/slh|small luxury/i.test(a) && /slh|small luxury/i.test(b)) return true;
  return false;
}

/**
 * Detect brand token evidence in official property name (support only — not sole SoT).
 */
function propertyNameSupportsBrand(propertyName, brandCanonical) {
  const name = norm(propertyName);
  const brand = norm(brandCanonical);
  if (!name || !brand) return false;
  if (name.includes(brand)) return true;
  // Four Points / Hampton / etc. short tokens
  const tokens = brand.split(" ").filter((t) => t.length >= 4);
  if (tokens.length && tokens.every((t) => name.includes(t))) return true;
  return false;
}

/**
 * Brand appears to be the entire property name (wrong).
 */
function brandEqualsPropertyName(brand, propertyName) {
  return norm(brand) === norm(propertyName) && Boolean(brand);
}

/**
 * Audit + classify one census record's Brand.
 */
export function classifyCensusBrand(record, dictionary, opts = {}) {
  const fields = record?.fields || {};
  const brandRaw = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const canonicalName = String(fields[MAP_FIRST_PASS.canonicalPropertyName] || "").trim();
  const brandFamily = String(fields[MAP_FIRST_PASS.brandFamily] || "").trim();
  const sourceFamily = String(fields[MAP_FIRST_PASS.family] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || ""
  ).trim();
  const urlFamily = familyFromOfficialUrl(sourceUrl);

  const result = {
    record_id: record.id,
    identity_key: fields[MAP_FIRST_PASS.identityKey] || null,
    property_name: propertyName,
    brand_before: brandRaw,
    brand_family: brandFamily,
    source_family: sourceFamily,
    url_family: urlFamily,
    classification: null,
    canonical_brand: null,
    high_fix: null,
    steward: false,
    reasons: [],
    sources: [],
  };

  if (!brandRaw) {
    result.classification = BRAND_CLASS.BLANK;
    result.steward = true;
    result.reasons.push("brand_blank");
    return result;
  }

  if (brandEqualsPropertyName(brandRaw, propertyName)) {
    result.classification = BRAND_CLASS.STEWARD;
    result.steward = true;
    result.reasons.push("brand_equals_property_name");
    return result;
  }

  const lookup = lookupCanonicalBrand(brandRaw, dictionary, {
    propertyName,
    sourceUrl,
  });

  // Exact canonical
  if (
    lookup.ok &&
    (lookup.match === "exact_canonical" || lookup.match === "exact_canonical_compact")
  ) {
    result.canonical_brand = lookup.canonical;
    const entry = lookup.entry;
    // Parent / source family checks
    if (
      urlFamily &&
      entry &&
      !familiesCompatible(urlFamily, entry.brand_family) &&
      !familiesCompatible(urlFamily, entry.parent_company)
    ) {
      result.classification = BRAND_CLASS.SOURCE_MISMATCH;
      result.steward = true;
      result.reasons.push("official_url_family_conflicts_brand");
      return result;
    }
    if (
      sourceFamily &&
      entry &&
      !familiesCompatible(sourceFamily, entry.brand_family) &&
      !familiesCompatible(sourceFamily, entry.parent_company)
    ) {
      result.classification = BRAND_CLASS.SOURCE_MISMATCH;
      result.steward = true;
      result.reasons.push("source_family_conflicts_brand");
      return result;
    }
    if (
      brandFamily &&
      entry?.brand_family &&
      !familiesCompatible(brandFamily, entry.brand_family) &&
      !familiesCompatible(brandFamily, entry.parent_company)
    ) {
      result.classification = BRAND_CLASS.PARENT_MISMATCH;
      // High fix Brand Family when URL supports brand family
      if (urlFamily && familiesCompatible(urlFamily, entry.brand_family)) {
        const wantFamily =
          canonicalizeParentCompany(entry.parent_company || entry.brand_family) ||
          entry.parent_company ||
          entry.brand_family;
        result.high_fix = {
          [MAP_FIRST_PASS.brandFamily]: wantFamily,
          "Data Confidence Tier": "High",
          "Enrichment Status": "In Progress",
          "Enrichment Priority": "High",
          "Last Reviewed Date": todayIsoDate(),
        };
        result.sources.push({ field: "Brand Family", method: "url_family_parent_align", confidence: "High" });
        result.classification = BRAND_CLASS.PARENT_MISMATCH;
        result.steward = false;
        result.reasons.push("parent_mismatch_high_fixable");
        return result;
      }
      result.steward = true;
      result.reasons.push("brand_family_conflicts_dictionary_parent");
      return result;
    }

    // Soft brand: name should not be ONLY the collection with no hotel identity
    if (entry?.soft_brand_collection) {
      const nameOk =
        propertyNameSupportsBrand(propertyName, entry.canonical_brand_name) ||
        (canonicalName && canonicalName !== entry.canonical_brand_name);
      if (!nameOk && !urlFamily) {
        result.classification = BRAND_CLASS.SOFT_BRAND_CONFLICT;
        result.steward = true;
        result.reasons.push("soft_brand_without_name_or_url_support");
        return result;
      }
    }

    // Casing-only normalize
    if (brandRaw !== lookup.canonical) {
      result.classification = BRAND_CLASS.ALIAS_NORMALIZABLE;
      result.high_fix = {
        [MAP_FIRST_PASS.currentBrand]: lookup.canonical,
        "Data Confidence Tier": "High",
        "Enrichment Status": "In Progress",
        "Enrichment Priority": "High",
        "Last Reviewed Date": todayIsoDate(),
      };
      result.sources.push({ field: "Current Brand", method: "canonical_casing", confidence: "High" });
      result.reasons.push("casing_or_punctuation_normalize");
      return result;
    }

    result.classification = BRAND_CLASS.VALID;
    return result;
  }

  // Alias / misspelling → High normalize when in Active dictionary
  if (lookup.ok && (lookup.match === "alias" || lookup.match === "alias_compact" || lookup.match === "misspelling")) {
    const canonical = lookup.canonical;
    const entry = lookup.entry;
    if (!entry && !lookup.in_active_dictionary) {
      // Alias points to name not in Active list — steward
      result.classification = BRAND_CLASS.STEWARD;
      result.steward = true;
      result.canonical_brand = canonical;
      result.reasons.push("alias_target_not_in_active_dictionary");
      return result;
    }

    // Source conflict: do not overwrite if URL family conflicts with target
    if (
      urlFamily &&
      entry &&
      !familiesCompatible(urlFamily, entry.brand_family) &&
      !familiesCompatible(urlFamily, entry.parent_company)
    ) {
      result.classification = BRAND_CLASS.SOURCE_MISMATCH;
      result.steward = true;
      result.reasons.push("alias_normalize_blocked_by_url_family");
      return result;
    }

    // hampton alone requires hilton URL or name support
    if (norm(brandRaw) === "hampton") {
      const supported =
        /hilton/i.test(urlFamily || "") ||
        /hilton\.com/i.test(sourceUrl) ||
        /hampton/i.test(propertyName);
      if (!supported) {
        result.classification = BRAND_CLASS.STEWARD;
        result.steward = true;
        result.reasons.push("hampton_requires_hilton_source_or_name");
        return result;
      }
    }

    // Property name strongly indicates a different brand (Four Points vs Sheraton)
    if (
      /four\s*points/i.test(propertyName) &&
      /^sheraton$/i.test(canonical) &&
      !/four\s*points/i.test(brandRaw)
    ) {
      result.classification = BRAND_CLASS.PROPERTY_NAME_MISMATCH;
      result.steward = true;
      result.reasons.push("property_name_suggests_four_points_not_sheraton");
      return result;
    }

    result.canonical_brand = entry?.canonical_brand_name || canonical;
    result.classification =
      lookup.match === "misspelling" ? BRAND_CLASS.MISSPELLED : BRAND_CLASS.ALIAS_NORMALIZABLE;
    result.high_fix = {
      [MAP_FIRST_PASS.currentBrand]: result.canonical_brand,
      "Data Confidence Tier": "High",
      "Enrichment Status": "In Progress",
      "Enrichment Priority": "High",
      "Last Reviewed Date": todayIsoDate(),
    };
    if (entry?.parent_company || entry?.brand_family) {
      const wantFamily =
        canonicalizeParentCompany(entry.parent_company || entry.brand_family) ||
        entry.parent_company ||
        entry.brand_family;
      if (!brandFamily || !familiesCompatible(brandFamily, wantFamily)) {
        result.high_fix[MAP_FIRST_PASS.brandFamily] = wantFamily;
      }
    }
    result.sources.push({
      field: "Current Brand",
      method: lookup.match,
      confidence: "High",
      from: brandRaw,
      to: result.canonical_brand,
    });
    return result;
  }

  // Unknown — try URL-supported brand from family only? Never infer brand from parent alone.
  result.classification = BRAND_CLASS.UNKNOWN;
  result.steward = true;
  result.reasons.push(lookup.reason || "unknown_not_in_dictionary");
  return result;
}

/**
 * Brand Source-of-Truth gate for Clean Core.
 * Active/Live brands use Brand Setup dictionary.
 * Evidence-backed non-active official brands may pass only when Census Only /
 * Not Owner-Facing (see evaluateNonActiveCleanCoreEligibility).
 */
export function evaluateBrandSourceOfTruth(record, dictionary, opts = {}) {
  const fields = record?.fields || {};
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const humanReview = fields[MAP_FIRST_PASS.humanReview] === true;
  const useStatus = String(fields["Production Use Status"] || "").trim();

  // Opaque codes never qualify for Clean Core
  if (brand && isOpaqueBrandCode(brand)) {
    return {
      pass: false,
      classification: BRAND_CLASS.STEWARD,
      steward: true,
      reasons: ["brand_code_unresolved"],
      canonical_brand: null,
      row: null,
    };
  }

  // Active/Live Brand Setup brands (owner-facing eligible)
  if (brand && isOwnerFacingBrandEligible(brand, opts)) {
    return {
      pass: true,
      classification: BRAND_CLASS.VALID,
      steward: false,
      reasons: ["active_brand_setup"],
      canonical_brand: brand,
      row: null,
      brand_governance_status: "active_brand_setup",
    };
  }

  // Official census registry brands outside Active/Live — Census Only path only
  // (checked before Active dictionary classify so non-active inventory is not blocked)
  if (
    brand &&
    isCensusOfficialBrand(brand) &&
    opts.requireActiveBrandSetup !== true &&
    useStatus === CENSUS_ONLY_PRODUCTION_USE_STATUS
  ) {
    return {
      pass: true,
      classification: BRAND_CLASS.VALID,
      steward: false,
      reasons: ["census_official_registry_census_only"],
      canonical_brand: brand,
      row: null,
      brand_governance_status: "evidence_backed_non_active_brand",
    };
  }

  const row = classifyCensusBrand(record, dictionary, opts);
  if (row.classification === BRAND_CLASS.VALID) {
    return {
      pass: true,
      classification: BRAND_CLASS.VALID,
      steward: false,
      reasons: row.reasons?.length ? row.reasons : ["active_brand_dictionary"],
      canonical_brand: row.canonical_brand || brand,
      row,
    };
  }

  // Legacy path: official registry without Active requirement (opt-in)
  if (
    brand &&
    isCensusOfficialBrand(brand) &&
    !humanReview &&
    opts.allowOfficialWithoutCensusOnly === true &&
    opts.requireActiveBrandSetup !== true
  ) {
    return {
      pass: true,
      classification: BRAND_CLASS.VALID,
      steward: false,
      reasons: ["census_official_registry"],
      canonical_brand: brand,
      row: null,
    };
  }

  return {
    pass: false,
    classification: row.classification,
    steward: row.steward,
    reasons: row.reasons,
    canonical_brand: row.canonical_brand,
    row,
  };
}

/**
 * Build High proposals + steward queue for brand_normalization.
 */
export function buildBrandNormalizationProposals(censusRecords = [], opts = {}) {
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary(opts);
  const proposals = [];
  const stewardCases = [];
  const examples = [];
  const counters = {
    records_scanned: 0,
    brand_valid: 0,
    brand_blank: 0,
    brand_misspelled: 0,
    brand_alias_normalizable: 0,
    brand_parent_mismatch: 0,
    brand_source_mismatch: 0,
    brand_property_name_mismatch: 0,
    soft_brand_collection_conflict: 0,
    brand_unknown_not_in_dictionary: 0,
    steward_review_required: 0,
    high_proposals: 0,
  };

  for (const rec of censusRecords) {
    counters.records_scanned += 1;
    if (rec.fields?.[MAP_FIRST_PASS.humanReview] === true) {
      stewardCases.push({
        record_id: rec.id,
        reason: "human_review_required",
        classification: BRAND_CLASS.STEWARD,
      });
      counters.steward_review_required += 1;
      continue;
    }

    const row = classifyCensusBrand(rec, dictionary, opts);
    const key = row.classification;
    if (counters[key] != null) counters[key] += 1;
    else if (key === BRAND_CLASS.STEWARD) counters.steward_review_required += 1;

    if (row.high_fix && Object.keys(row.high_fix).length && !row.steward) {
      proposals.push({
        record_id: rec.id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        queue: BRAND_NORMALIZATION_QUEUE_ID,
        confidence: "High",
        action: "update",
        patch: row.high_fix,
        fields: row.high_fix,
        brand_before: row.brand_before,
        brand_after: row.high_fix[MAP_FIRST_PASS.currentBrand] || row.brand_before,
        classification: row.classification,
        sources: row.sources,
        method: "brand_normalization",
      });
      counters.high_proposals += 1;
      if (examples.length < 25) {
        examples.push({
          record_id: rec.id,
          property_name: row.property_name,
          before: row.brand_before,
          after: row.high_fix[MAP_FIRST_PASS.currentBrand] || row.brand_before,
          classification: row.classification,
        });
      }
    } else if (row.steward || key === BRAND_CLASS.BLANK || key === BRAND_CLASS.UNKNOWN) {
      stewardCases.push({
        record_id: rec.id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        brand: row.brand_before,
        classification: row.classification,
        reasons: row.reasons,
      });
      if (key !== BRAND_CLASS.STEWARD && key !== BRAND_CLASS.BLANK && key !== BRAND_CLASS.UNKNOWN) {
        counters.steward_review_required += 1;
      }
      // Flag Human Review only for conflict / blank — not every Active-dictionary outsider
      const flagHr = [
        BRAND_CLASS.BLANK,
        BRAND_CLASS.SOURCE_MISMATCH,
        BRAND_CLASS.SOFT_BRAND_CONFLICT,
        BRAND_CLASS.PROPERTY_NAME_MISMATCH,
        BRAND_CLASS.PARENT_MISMATCH,
      ].includes(key);
      const alreadyHr = rec.fields?.[MAP_FIRST_PASS.humanReview] === true;
      if (flagHr && !alreadyHr) {
        const stewardPatch = {
          [MAP_FIRST_PASS.humanReview]: true,
          "Enrichment Status": "In Progress",
          "Enrichment Priority": "High",
          "Last Reviewed Date": todayIsoDate(),
        };
        proposals.push({
          record_id: rec.id,
          identity_key: row.identity_key,
          property_name: row.property_name,
          queue: BRAND_NORMALIZATION_QUEUE_ID,
          confidence: "High",
          action: "update",
          patch: stewardPatch,
          fields: stewardPatch,
          brand_before: row.brand_before,
          brand_after: row.brand_before,
          classification: row.classification,
          method: "brand_normalization_steward_flag",
          steward_only: true,
        });
        counters.high_proposals += 1;
      }
    }
  }

  let status = BRAND_NORMALIZATION_STATUS.READY_NEEDS_MISSION;
  if (proposals.length === 0 && stewardCases.length === 0) {
    status = BRAND_NORMALIZATION_STATUS.APPLIED_CLEAN;
  } else if (proposals.length === 0 && stewardCases.length > 0) {
    status = BRAND_NORMALIZATION_STATUS.PARTIAL;
  }

  return {
    version: BRAND_NORMALIZATION_VERSION,
    queue: BRAND_NORMALIZATION_QUEUE_ID,
    status,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    brand_setup_read_only: true,
    brand_explorer_writes: false,
    dictionary_active_brand_count: dictionary.active_brand_count,
    counters,
    proposals,
    steward_cases: stewardCases,
    examples_before_after: examples,
  };
}

export function runBrandNormalizationQueue(opts = {}) {
  return buildBrandNormalizationProposals(opts.censusRecords || [], opts);
}

function renderBrandNormMd(report, opts = {}) {
  const c = report.counters || {};
  const lines = [
    `# Production Census Brand Normalization`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Queue:** \`${report.queue}\``,
    `**Write target:** ${report.write_target?.table} (\`${report.write_target?.table_id}\`)`,
    `**Airtable writes:** ${opts.airtable_writes ? "yes" : "no (controlled)"}`,
    `**Dictionary Active brands:** ${report.dictionary_active_brand_count}`,
    ``,
    `## Audit counters`,
    ``,
    `| Class | Count |`,
    `| --- | ---: |`,
    `| Scanned | ${c.records_scanned ?? 0} |`,
    `| brand_valid | ${c.brand_valid ?? 0} |`,
    `| brand_blank | ${c.brand_blank ?? 0} |`,
    `| brand_misspelled | ${c.brand_misspelled ?? 0} |`,
    `| brand_alias_normalizable | ${c.brand_alias_normalizable ?? 0} |`,
    `| brand_parent_mismatch | ${c.brand_parent_mismatch ?? 0} |`,
    `| brand_source_mismatch | ${c.brand_source_mismatch ?? 0} |`,
    `| brand_property_name_mismatch | ${c.brand_property_name_mismatch ?? 0} |`,
    `| soft_brand_collection_conflict | ${c.soft_brand_collection_conflict ?? 0} |`,
    `| brand_unknown_not_in_dictionary | ${c.brand_unknown_not_in_dictionary ?? 0} |`,
    `| steward_review_required | ${c.steward_review_required ?? 0} |`,
    `| High proposals | ${c.high_proposals ?? 0} |`,
    `| Updates applied | ${opts.updates_applied ?? 0} |`,
    ``,
    `## Clean Core`,
    ``,
    `- Before: ${opts.clean_core_before ?? "—"}`,
    `- After: ${opts.clean_core_after ?? "—"}`,
    ``,
    `## Examples before / after`,
    ``,
  ];
  for (const ex of report.examples_before_after || []) {
    lines.push(
      `- \`${ex.before}\` → \`${ex.after}\` (${ex.classification}) — ${ex.property_name || ex.record_id}`
    );
  }
  if (!(report.examples_before_after || []).length) lines.push(`_None_`);
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- Brand Setup read-only; Brand Explorer untouched`,
    `- No address / coords / phone / rooms`,
    `- No weak brand inference from hotel name or parent alone`,
    `- Source-conflicting overwrites stewarded`,
    ``
  );
  return lines.join("\n");
}

/**
 * Persist public reports.
 */
export function writeBrandNormalizationReports(report, opts = {}) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-brand-normalization.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-brand-normalization.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-brand-normalization.md"
  );
  const payload = { ...report, ...opts, generated_at: new Date().toISOString() };
  writeJson(jsonPath, payload);
  const md = renderBrandNormMd(report, opts);
  writeText(mdPath, md);
  writeText(docsPath, md);
  return { jsonPath, mdPath, docsPath };
}

export { renderBrandNormMd };
