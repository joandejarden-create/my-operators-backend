/**
 * Brand Family / Parent Company normalization for Hotel Property Census.
 *
 * Canonical Census field: Brand Family (no separate Parent Company column).
 * Queue: parent_company_normalization
 * Write target: Hotel Property Census only — never Brand Setup / Brand Explorer.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  buildCanonicalBrandDictionary,
  familyFromOfficialUrl,
  lookupCanonicalBrand,
} from "./census-brand-canonical-dictionary.js";
import {
  inferParentCompanyForAutopilot,
  PARENT_BY_SLUG,
} from "./census-autopilot-parent-inference.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const PARENT_COMPANY_NORMALIZATION_QUEUE_ID = "parent_company_normalization";
export const PARENT_COMPANY_NORMALIZATION_VERSION =
  "census-parent-company-normalization-v1";

export const PARENT_COMPANY_NORMALIZATION_STATUS = Object.freeze({
  APPLIED_CLEAN: "production_census_parent_company_normalization_applied_clean",
  PARTIAL: "production_census_parent_company_normalization_partial_steward_remaining",
  READY_NEEDS_MISSION:
    "production_census_parent_company_normalization_ready_needs_mission",
  BLOCKED: "production_census_parent_company_normalization_blocked",
});

/** Canonical Brand Family values written to Hotel Property Census. */
export const CANONICAL_PARENT_COMPANIES = Object.freeze([
  "Marriott International",
  "Hilton",
  "IHG",
  "Choice Hotels International",
  "Accor",
  "Wyndham Hotels & Resorts",
  "Preferred Hotels & Resorts",
  "BWH Hotels",
  "Small Luxury Hotels of the World",
  "Bunkhouse",
]);

export const PARENT_CLASS = Object.freeze({
  VALID: "parent_valid",
  BLANK: "parent_blank",
  ALIAS_NORMALIZABLE: "parent_alias_normalizable",
  BRAND_MISMATCH: "parent_brand_mismatch",
  SOURCE_MISMATCH: "parent_source_mismatch",
  UNKNOWN: "parent_unknown",
  STEWARD: "steward_review_required",
});

/** Alias / short form → canonical Brand Family. */
export const PARENT_ALIAS_TO_CANONICAL = Object.freeze({
  // Marriott
  marriott: "Marriott International",
  "marriott international": "Marriott International",
  "marriott international, inc": "Marriott International",
  "marriott international, inc.": "Marriott International",
  "marriott intl": "Marriott International",
  "marriott bonvoy": "Marriott International",
  "marriott hotels": "Marriott International",
  "marriott hotels & resorts": "Marriott International",
  "marriott hotels and resorts": "Marriott International",
  // Hilton
  hilton: "Hilton",
  "hilton worldwide": "Hilton",
  "hilton worldwide holdings": "Hilton",
  "hilton hotels": "Hilton",
  "hilton hotels & resorts": "Hilton",
  "hilton hotels and resorts": "Hilton",
  // IHG
  ihg: "IHG",
  "ihg hotels": "IHG",
  "ihg hotels & resorts": "IHG",
  "ihg hotels and resorts": "IHG",
  "intercontinental hotels group": "IHG",
  "intercontinental hotels group plc": "IHG",
  // Choice
  choice: "Choice Hotels International",
  "choice hotels": "Choice Hotels International",
  "choice hotels international": "Choice Hotels International",
  "choice hotels international, inc": "Choice Hotels International",
  "choice hotels international, inc.": "Choice Hotels International",
  "choice hotels international inc": "Choice Hotels International",
  "radisson hotel group": "Choice Hotels International",
  // Accor
  accor: "Accor",
  "accor hotels": "Accor",
  accorhotels: "Accor",
  "accor live limitless": "Accor",
  // Wyndham
  wyndham: "Wyndham Hotels & Resorts",
  "wyndham hotels": "Wyndham Hotels & Resorts",
  "wyndham hotels & resorts": "Wyndham Hotels & Resorts",
  "wyndham hotels and resorts": "Wyndham Hotels & Resorts",
  // Preferred
  preferred: "Preferred Hotels & Resorts",
  "preferred hotels": "Preferred Hotels & Resorts",
  "preferred hotels & resorts": "Preferred Hotels & Resorts",
  "preferred hotels and resorts": "Preferred Hotels & Resorts",
  // BWH / SLH / Bunkhouse
  bwh: "BWH Hotels",
  "bwh hotels": "BWH Hotels",
  "best western": "BWH Hotels",
  "best western hotels & resorts": "BWH Hotels",
  slh: "Small Luxury Hotels of the World",
  "small luxury hotels": "Small Luxury Hotels of the World",
  "small luxury hotels of the world": "Small Luxury Hotels of the World",
  bunkhouse: "Bunkhouse",
  "bunkhouse hotels": "Bunkhouse",
});

const CANONICAL_SET = new Set(CANONICAL_PARENT_COMPANIES);

function normKey(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[.,]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

function isBlank(v) {
  return v == null || !String(v).trim();
}

/**
 * Resolve alias / short form to canonical Brand Family, or null if unknown.
 * @param {string} raw
 */
export function canonicalizeParentCompany(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  if (CANONICAL_SET.has(s)) return s;
  const hit = PARENT_ALIAS_TO_CANONICAL[normKey(s)];
  if (hit) return hit;
  // Soft: trailing Inc / PLC
  const stripped = s.replace(/,?\s*(inc\.?|llc\.?|plc\.?|ltd\.?)\s*$/i, "").trim();
  if (stripped !== s) {
    if (CANONICAL_SET.has(stripped)) return stripped;
    const h2 = PARENT_ALIAS_TO_CANONICAL[normKey(stripped)];
    if (h2) return h2;
  }
  return null;
}

/**
 * True when Brand Family is an exact canonical parent value.
 * @param {string} raw
 */
export function isCanonicalParentCompany(raw) {
  return CANONICAL_SET.has(String(raw || "").trim());
}

/**
 * Short routing family used by adapters (Marriott / Hilton / Choice / …).
 * @param {string} canonicalOrRaw
 */
export function canonicalParentToRoutingFamily(canonicalOrRaw) {
  const c = canonicalizeParentCompany(canonicalOrRaw) || String(canonicalOrRaw || "").trim();
  if (/marriott/i.test(c)) return "Marriott";
  if (/hilton/i.test(c)) return "Hilton";
  if (/^ihg$/i.test(c) || /intercontinental hotels group/i.test(c)) return "IHG";
  if (/choice/i.test(c)) return "Choice";
  if (/accor/i.test(c)) return "Accor";
  if (/wyndham/i.test(c)) return "Wyndham";
  if (/preferred/i.test(c)) return "Preferred";
  if (/bwh|best western/i.test(c)) return "BWH Hotels";
  if (/small luxury|slh/i.test(c)) return "SLH";
  if (/bunkhouse/i.test(c)) return "Bunkhouse";
  return c || null;
}

/**
 * Two parent labels refer to the same family (after canonicalization).
 * @param {string} a
 * @param {string} b
 */
export function parentsCompatible(a, b) {
  if (!a || !b) return true;
  const ca = canonicalizeParentCompany(a);
  const cb = canonicalizeParentCompany(b);
  if (ca && cb) return ca === cb;
  const ra = canonicalParentToRoutingFamily(a);
  const rb = canonicalParentToRoutingFamily(b);
  if (ra && rb && ra === rb) return true;
  return normKey(a) === normKey(b);
}

/**
 * Infer expected canonical parent from brand / URL / slug (High when dictionary or URL).
 * @param {object} record
 * @param {object} [dictionary]
 */
export function inferExpectedCanonicalParent(record, dictionary = null) {
  const fields = record?.fields || {};
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || fields["Current Brand"] || "").trim();
  const url = String(
    fields[MAP_FIRST_PASS.officialUrl] ||
      fields["Official Property URL"] ||
      fields[MAP_FIRST_PASS.sourceUrl] ||
      fields["Source URL"] ||
      ""
  ).trim();
  const slug = String(fields[MAP_FIRST_PASS.brandSlug] || fields["Brand Explorer Slug if mapped"] || "").trim();
  const sourceFamily = String(fields[MAP_FIRST_PASS.family] || fields["Family / Source Family"] || "").trim();

  const dict = dictionary || buildCanonicalBrandDictionary();
  /** @type {{ canonical: string|null, confidence: 'High'|'Medium'|'Low', sources: string[] }} */
  const out = { canonical: null, confidence: "Low", sources: [] };

  if (brand) {
    const hit = lookupCanonicalBrand(brand, dict);
    if (hit?.entry) {
      const p = canonicalizeParentCompany(
        hit.entry.parent_company || hit.entry.brand_family || ""
      );
      if (p) {
        out.canonical = p;
        out.confidence = "High";
        out.sources.push("brand_dictionary_parent");
      }
    }
  }

  if (slug && PARENT_BY_SLUG[slug]) {
    const p = canonicalizeParentCompany(PARENT_BY_SLUG[slug]);
    if (p) {
      if (!out.canonical) {
        out.canonical = p;
        out.confidence = "High";
        out.sources.push("parent_by_slug");
      } else if (out.canonical !== p) {
        out.sources.push("slug_parent_conflict");
      } else {
        out.sources.push("parent_by_slug");
      }
    }
  }

  const urlFamily = familyFromOfficialUrl(url);
  if (urlFamily) {
    const p = canonicalizeParentCompany(urlFamily);
    if (p) {
      if (!out.canonical) {
        out.canonical = p;
        out.confidence = "High";
        out.sources.push("official_url_family");
      } else if (out.canonical !== p) {
        out.sources.push("url_parent_conflict");
        // Prefer URL+brand agreement; if conflict keep brand dict and flag
      } else {
        out.sources.push("official_url_family");
      }
    }
  }

  if (sourceFamily) {
    const p = canonicalizeParentCompany(sourceFamily);
    if (p) {
      if (!out.canonical) {
        out.canonical = p;
        out.confidence = out.confidence === "Low" ? "Medium" : out.confidence;
        out.sources.push("source_family");
      } else if (out.canonical !== p) {
        out.sources.push("source_family_conflict");
      } else {
        out.sources.push("source_family");
      }
    }
  }

  // Slug inference fallback (Medium) — never sole High write without brand/URL
  if (!out.canonical && (slug || brand)) {
    const inferred = inferParentCompanyForAutopilot({
      brand_slug: slug,
      slug,
      parent_company: null,
      brand_name: brand,
    });
    const p = canonicalizeParentCompany(inferred?.parent_company);
    if (p && inferred?.inference_confidence === "High") {
      out.canonical = p;
      out.confidence = "Medium";
      out.sources.push("slug_token_inference");
    }
  }

  return out;
}

/**
 * Classify one Census record's Brand Family / parent posture.
 * @param {object} record
 * @param {object} [opts]
 */
export function classifyCensusParentCompany(record, opts = {}) {
  const fields = record?.fields || {};
  const identityKey = fields[MAP_FIRST_PASS.identityKey] || fields["Property Identity Key"] || "";
  const propertyName = fields[MAP_FIRST_PASS.propertyName] || fields["Property Name"] || "";
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || fields["Current Brand"] || "").trim();
  const rawParent = String(fields[MAP_FIRST_PASS.brandFamily] || fields["Brand Family"] || "").trim();
  const url = String(
    fields[MAP_FIRST_PASS.officialUrl] ||
      fields["Official Property URL"] ||
      fields[MAP_FIRST_PASS.sourceUrl] ||
      fields["Source URL"] ||
      ""
  ).trim();

  const dictionary = opts.dictionary || null;
  const expected = inferExpectedCanonicalParent(record, dictionary);
  const canonicalFromRaw = canonicalizeParentCompany(rawParent);
  const urlFamily = familyFromOfficialUrl(url);
  const urlCanonical = canonicalizeParentCompany(urlFamily);

  /** @type {object} */
  const row = {
    record_id: record.id,
    identity_key: identityKey,
    property_name: propertyName,
    brand,
    parent_before: rawParent || null,
    parent_canonical_from_raw: canonicalFromRaw,
    expected_parent: expected.canonical,
    expected_confidence: expected.confidence,
    expected_sources: expected.sources,
    classification: PARENT_CLASS.VALID,
    steward: false,
    reasons: [],
    high_fix: null,
    sources: [],
  };

  if (fields[MAP_FIRST_PASS.humanReview] === true) {
    // Still allow High alias normalization of Brand Family (identity hygiene).
    // Do not invent parents or resolve mismatches while held.
    if (!isBlank(rawParent) && canonicalizeParentCompany(rawParent) && !isCanonicalParentCompany(rawParent)) {
      const c = canonicalizeParentCompany(rawParent);
      if (c && (!urlCanonical || urlCanonical === c)) {
        row.classification = PARENT_CLASS.ALIAS_NORMALIZABLE;
        row.high_fix = {
          [MAP_FIRST_PASS.brandFamily]: c,
          "Last Reviewed Date": todayIsoDate(),
        };
        row.sources.push({
          field: "Brand Family",
          method: "parent_alias_normalization_while_held",
          confidence: "High",
          from: rawParent,
          to: c,
        });
        row.reasons.push(`alias_to_canonical_while_held:${rawParent}→${c}`);
        return row;
      }
    }
    row.classification = PARENT_CLASS.STEWARD;
    row.steward = true;
    row.reasons.push("human_review_required");
    return row;
  }

  // --- Blank ---
  if (isBlank(rawParent)) {
    row.classification = PARENT_CLASS.BLANK;
    row.reasons.push("brand_family_blank");
    if (expected.canonical && expected.confidence === "High") {
      row.high_fix = {
        [MAP_FIRST_PASS.brandFamily]: expected.canonical,
        "Data Confidence Tier": "High",
        "Enrichment Status": "In Progress",
        "Enrichment Priority": "High",
        "Last Reviewed Date": todayIsoDate(),
      };
      row.sources.push({
        field: "Brand Family",
        method: "parent_blank_fill_from_brand_or_url",
        confidence: "High",
        expected_sources: expected.sources,
      });
    } else {
      row.steward = true;
      row.reasons.push("blank_without_high_expected_parent");
    }
    return row;
  }

  // --- Already canonical ---
  if (isCanonicalParentCompany(rawParent)) {
    // Check brand / source consistency
    if (expected.canonical && expected.canonical !== rawParent && expected.confidence === "High") {
      const urlAgrees =
        !urlCanonical || urlCanonical === expected.canonical;
      const noUrlConflict = !expected.sources.includes("url_parent_conflict");
      if (urlAgrees && noUrlConflict && !expected.sources.includes("slug_parent_conflict")) {
        // Brand+URL say different parent than current canonical → steward (do not overwrite conflicting populated)
        row.classification = PARENT_CLASS.BRAND_MISMATCH;
        row.steward = true;
        row.reasons.push(
          `brand_family_canonical_but_mismatches_expected:${rawParent}≠${expected.canonical}`
        );
        return row;
      }
    }
    if (urlCanonical && urlCanonical !== rawParent) {
      row.classification = PARENT_CLASS.SOURCE_MISMATCH;
      row.steward = true;
      row.reasons.push(`source_url_parent_conflict:${urlCanonical}≠${rawParent}`);
      return row;
    }
    row.classification = PARENT_CLASS.VALID;
    row.reasons.push("brand_family_canonical");
    return row;
  }

  // --- Alias normalizable ---
  if (canonicalFromRaw) {
    // Alias → canonical High write when expected agrees or expected blank/compatible
    const expectedOk =
      !expected.canonical ||
      expected.canonical === canonicalFromRaw ||
      expected.confidence !== "High";
    if (urlCanonical && urlCanonical !== canonicalFromRaw) {
      row.classification = PARENT_CLASS.SOURCE_MISMATCH;
      row.steward = true;
      row.reasons.push(
        `alias_normalizable_but_url_conflicts:${canonicalFromRaw}≠${urlCanonical}`
      );
      return row;
    }
    if (
      expected.canonical &&
      expected.canonical !== canonicalFromRaw &&
      expected.confidence === "High"
    ) {
      // Alias of wrong family vs High brand expectation
      row.classification = PARENT_CLASS.BRAND_MISMATCH;
      if (!expected.sources.includes("url_parent_conflict")) {
        // High repair to expected when brand+URL aligned
        const urlSupports = !urlCanonical || urlCanonical === expected.canonical;
        if (urlSupports) {
          row.high_fix = {
            [MAP_FIRST_PASS.brandFamily]: expected.canonical,
            "Data Confidence Tier": "High",
            "Enrichment Status": "In Progress",
            "Enrichment Priority": "High",
            "Last Reviewed Date": todayIsoDate(),
          };
          row.sources.push({
            field: "Brand Family",
            method: "parent_brand_mismatch_repair",
            confidence: "High",
            from: rawParent,
            to: expected.canonical,
          });
          row.steward = false;
          row.reasons.push(
            `alias_of_wrong_parent_repaired:${rawParent}→${expected.canonical}`
          );
          return row;
        }
      }
      row.steward = true;
      row.reasons.push(
        `alias_conflicts_expected_parent:${canonicalFromRaw}≠${expected.canonical}`
      );
      return row;
    }

    if (expectedOk) {
      row.classification = PARENT_CLASS.ALIAS_NORMALIZABLE;
      row.high_fix = {
        [MAP_FIRST_PASS.brandFamily]: canonicalFromRaw,
        "Data Confidence Tier": "High",
        "Enrichment Status": "In Progress",
        "Enrichment Priority": "Medium",
        "Last Reviewed Date": todayIsoDate(),
      };
      row.sources.push({
        field: "Brand Family",
        method: "parent_alias_normalization",
        confidence: "High",
        from: rawParent,
        to: canonicalFromRaw,
      });
      row.reasons.push(`alias_to_canonical:${rawParent}→${canonicalFromRaw}`);
      return row;
    }
  }

  // --- Unknown non-canonical (regional operators etc.) ---
  row.classification = PARENT_CLASS.UNKNOWN;
  row.reasons.push("parent_not_in_canonical_dictionary");
  if (expected.canonical && expected.confidence === "High") {
    // Brand maps to major parent but Brand Family is regional/unknown → steward
    row.classification = PARENT_CLASS.BRAND_MISMATCH;
    row.steward = true;
    row.reasons.push(
      `unknown_parent_conflicts_brand_expected:${rawParent}≠${expected.canonical}`
    );
  }
  // Do not invent Independent / Other
  return row;
}

/**
 * Build High proposals + steward cases for parent_company_normalization.
 * @param {object[]} censusRecords
 * @param {object} [opts]
 */
export function buildParentCompanyNormalizationProposals(censusRecords = [], opts = {}) {
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary(opts);
  const proposals = [];
  const stewardCases = [];
  const examples = [];
  const counters = {
    records_scanned: 0,
    parent_valid: 0,
    parent_blank: 0,
    parent_alias_normalizable: 0,
    parent_brand_mismatch: 0,
    parent_source_mismatch: 0,
    parent_unknown: 0,
    steward_review_required: 0,
    high_proposals: 0,
  };

  for (const rec of censusRecords) {
    counters.records_scanned += 1;
    const row = classifyCensusParentCompany(rec, { ...opts, dictionary });
    const key = row.classification;
    if (counters[key] != null) counters[key] += 1;
    if (row.steward) counters.steward_review_required += 1;

    if (row.high_fix && Object.keys(row.high_fix).length && !row.steward) {
      proposals.push({
        record_id: rec.id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        queue: PARENT_COMPANY_NORMALIZATION_QUEUE_ID,
        confidence: "High",
        action: "update",
        patch: row.high_fix,
        fields: row.high_fix,
        parent_before: row.parent_before,
        parent_after: row.high_fix[MAP_FIRST_PASS.brandFamily],
        classification: row.classification,
        sources: row.sources,
        method: "parent_company_normalization",
      });
      counters.high_proposals += 1;
      if (examples.length < 30) {
        examples.push({
          record_id: rec.id,
          property_name: row.property_name,
          brand: row.brand,
          before: row.parent_before,
          after: row.high_fix[MAP_FIRST_PASS.brandFamily],
          classification: row.classification,
        });
      }
    } else if (row.steward) {
      stewardCases.push({
        record_id: rec.id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        brand: row.brand,
        parent: row.parent_before,
        expected_parent: row.expected_parent,
        classification: row.classification,
        reasons: row.reasons,
      });
      const alreadyHr = rec.fields?.[MAP_FIRST_PASS.humanReview] === true;
      const flagHr = [
        PARENT_CLASS.BRAND_MISMATCH,
        PARENT_CLASS.SOURCE_MISMATCH,
      ].includes(key);
      if (flagHr && !alreadyHr && opts.proposeStewardFlags !== false) {
        proposals.push({
          record_id: rec.id,
          identity_key: row.identity_key,
          property_name: row.property_name,
          queue: PARENT_COMPANY_NORMALIZATION_QUEUE_ID,
          confidence: "High",
          action: "steward_flag",
          patch: {
            [MAP_FIRST_PASS.humanReview]: true,
            "Enrichment Status": "In Progress",
            "Enrichment Priority": "High",
            "Public Display Review Status": "Needs Review",
            "Last Reviewed Date": todayIsoDate(),
          },
          fields: {
            [MAP_FIRST_PASS.humanReview]: true,
            "Enrichment Status": "In Progress",
            "Enrichment Priority": "High",
            "Public Display Review Status": "Needs Review",
            "Last Reviewed Date": todayIsoDate(),
          },
          classification: row.classification,
          method: "parent_company_normalization_steward_flag",
          steward: true,
        });
      }
    }
  }

  let status = PARENT_COMPANY_NORMALIZATION_STATUS.READY_NEEDS_MISSION;
  if (counters.high_proposals === 0 && counters.steward_review_required === 0) {
    status = PARENT_COMPANY_NORMALIZATION_STATUS.APPLIED_CLEAN;
  } else if (counters.steward_review_required > 0 && counters.high_proposals === 0) {
    status = PARENT_COMPANY_NORMALIZATION_STATUS.PARTIAL;
  }

  return {
    version: PARENT_COMPANY_NORMALIZATION_VERSION,
    queue: PARENT_COMPANY_NORMALIZATION_QUEUE_ID,
    canonical_field: "Brand Family",
    schema_note:
      "Hotel Property Census has Brand Family (canonical parent rollup) and Family / Source Family (source lineage). No Parent Company column.",
    canonical_parents: [...CANONICAL_PARENT_COMPANIES],
    status,
    counters,
    proposals,
    steward_cases: stewardCases,
    examples_before_after: examples,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
  };
}

/**
 * Controlled / orchestrator entry — proposals only, no Airtable writes.
 * @param {object} [opts]
 */
export function runParentCompanyNormalizationQueue(opts = {}) {
  return buildParentCompanyNormalizationProposals(opts.censusRecords || [], opts);
}

/**
 * Evaluate parent gate for Clean Core (canonical + consistent).
 * @param {object} record
 * @param {object} [opts]
 */
export function evaluateParentCompanyCleanCoreGate(record, opts = {}) {
  const fields = record?.fields || {};
  const raw = String(fields[MAP_FIRST_PASS.brandFamily] || fields["Brand Family"] || "").trim();
  if (!raw) {
    return {
      pass: false,
      classification: PARENT_CLASS.BLANK,
      blocker: "parent_normalization_needed",
      reason: "brand_family_blank",
    };
  }
  if (!isCanonicalParentCompany(raw)) {
    const alias = canonicalizeParentCompany(raw);
    return {
      pass: false,
      classification: alias
        ? PARENT_CLASS.ALIAS_NORMALIZABLE
        : PARENT_CLASS.UNKNOWN,
      blocker: alias ? "parent_normalization_needed" : "parent_unknown",
      reason: alias
        ? `brand_family_alias_needs_normalize:${raw}`
        : `brand_family_not_canonical:${raw}`,
      canonical_candidate: alias,
    };
  }

  if (opts.skipConsistencyCheck === true) {
    return { pass: true, classification: PARENT_CLASS.VALID, blocker: null };
  }

  const expected = inferExpectedCanonicalParent(record, opts.dictionary);
  if (
    expected.canonical &&
    expected.confidence === "High" &&
    expected.canonical !== raw &&
    !expected.sources.includes("url_parent_conflict")
  ) {
    return {
      pass: false,
      classification: PARENT_CLASS.BRAND_MISMATCH,
      blocker: "parent_brand_mismatch",
      reason: `brand_family_mismatches_expected:${raw}≠${expected.canonical}`,
      expected: expected.canonical,
    };
  }

  const url = String(
    fields[MAP_FIRST_PASS.officialUrl] ||
      fields["Official Property URL"] ||
      fields[MAP_FIRST_PASS.sourceUrl] ||
      fields["Source URL"] ||
      ""
  ).trim();
  const urlCanonical = canonicalizeParentCompany(familyFromOfficialUrl(url));
  if (urlCanonical && urlCanonical !== raw) {
    return {
      pass: false,
      classification: PARENT_CLASS.SOURCE_MISMATCH,
      blocker: "parent_source_conflict",
      reason: `brand_family_mismatches_url:${raw}≠${urlCanonical}`,
      expected: urlCanonical,
    };
  }

  return { pass: true, classification: PARENT_CLASS.VALID, blocker: null };
}

export function writeParentCompanyNormalizationReports(report, opts = {}) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-parent-company-normalization.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-parent-company-normalization.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-parent-company-normalization.md"
  );

  const payload = { ...report, ...opts, generated_at: new Date().toISOString() };
  const c = payload.counters || {};
  const b = payload.before || {};
  const a = payload.after || {};
  const lines = [
    `# Parent Company / Brand Family Normalization`,
    ``,
    `**Status:** \`${payload.status}\``,
    `**Queue:** \`${PARENT_COMPANY_NORMALIZATION_QUEUE_ID}\``,
    `**Canonical field:** Brand Family`,
    `**Write target:** Hotel Property Census (\`${productionHotelPropertyCensus.tableId}\`)`,
    `**Airtable writes:** ${payload.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup writes:** false`,
    `**Brand Explorer writes:** false`,
    ``,
    `## Schema`,
    ``,
    `- **Brand Family** — canonical parent rollup field (Autopilot SoT)`,
    `- **Family / Source Family** — source lineage (not overwritten by this queue)`,
    `- No \`Parent Company\` column on Hotel Property Census`,
    ``,
    `## Canonical parents`,
    ``,
    ...CANONICAL_PARENT_COMPANIES.map((p) => `- ${p}`),
    ``,
    `## Counters`,
    ``,
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Records scanned | ${c.records_scanned ?? "—"} |`,
    `| Parent valid | ${c.parent_valid ?? "—"} |`,
    `| Parent blank | ${c.parent_blank ?? "—"} |`,
    `| Alias normalizable | ${c.parent_alias_normalizable ?? "—"} |`,
    `| Brand/parent mismatch | ${c.parent_brand_mismatch ?? "—"} |`,
    `| Source/parent mismatch | ${c.parent_source_mismatch ?? "—"} |`,
    `| Parent unknown | ${c.parent_unknown ?? "—"} |`,
    `| High parent fixes proposed | ${c.high_proposals ?? "—"} |`,
    `| Steward cases | ${(payload.steward_cases || []).length} |`,
    `| Records written | ${payload.records_written ?? 0} |`,
    ``,
    `## Clean Core before → after`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Clean Core | ${b.clean_core ?? "—"} | ${a.clean_core ?? "—"} |`,
    `| Parent valid | ${b.parent_valid ?? c.parent_valid ?? "—"} | ${a.parent_valid ?? "—"} |`,
    `| Parent blank | ${b.parent_blank ?? c.parent_blank ?? "—"} | ${a.parent_blank ?? "—"} |`,
    ``,
    `## Fields written`,
    ``,
    `${(payload.fields_written || []).join(", ") || "(none)"}`,
    ``,
    `## Examples before → after`,
    ``,
  ];
  for (const ex of (payload.examples_before_after || []).slice(0, 20)) {
    lines.push(
      `- **${ex.property_name}** (${ex.brand || "—"}): \`${ex.before || "(blank)"}\` → \`${ex.after}\` [${ex.classification}]`
    );
  }
  lines.push(``, `## Unresolved steward cases`, ``);
  for (const s of (payload.steward_cases || []).slice(0, 25)) {
    lines.push(
      `- **${s.property_name}**: parent=\`${s.parent || "(blank)"}\` expected=\`${s.expected_parent || "—"}\` — ${s.classification}; ${(s.reasons || []).join("; ")}`
    );
  }
  lines.push(
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- No Brand Setup / Brand Explorer writes`,
    `- No address / coords / phone / rooms / owner / operator / date writes`,
    `- No weak parent inference; source conflicts stewarded`,
    ``
  );
  const md = lines.join("\n");
  writeJson(jsonPath, payload);
  writeText(mdPath, md);
  writeText(docsPath, md);
  return { jsonPath, mdPath, docsPath };
}
