/**
 * Brand mapping gap repair — deterministic aliases only.
 * Does not invent brands. Does not write Brand Setup / Brand Explorer.
 */
import {
  buildCanonicalBrandDictionary,
  lookupCanonicalBrand,
  HIGH_BRAND_ALIAS_TO_CANONICAL,
} from "./census-brand-canonical-dictionary.js";
import {
  resolveCensusOfficialBrand,
  getCensusOfficialEntry,
} from "./census-official-brand-registry.js";
import { MAP_BRAND } from "./master-brand-portfolio-validation-v1.js";
import { canonicalizeParentCompany } from "./census-parent-company-normalization.js";
import { AFFILIATION_STATUS_OPTIONS } from "./production-census-schema-create.js";

export const BRAND_MAPPING_GAP_REPAIR_VERSION = "brand-mapping-gap-repair-v1";

const PARENT_SUFFIX_RE =
  /\s+(?:by\s+)?(?:ihg|hilton|marriott|sheraton|choice(?:\s+hotels)?|wyndham|accor|hyatt|radisson)\s*$/i;
const HOTELS_RESORTS_RE = /\s+(?:hotels?\s*(?:&\s*|and\s+)?resorts?|hotels\s*&\s*resorts)\s*$/i;

export const DETERMINISTIC_ALIAS_EXAMPLES = Object.freeze({
  "Hotel Indigo by IHG": "Hotel Indigo",
  "Hampton Inn": "Hampton by Hilton",
  "Four Points": "Four Points by Sheraton",
});

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

export function stripBrandParentSuffix(raw) {
  let s = String(raw || "").trim();
  if (!s) return s;
  s = s.replace(PARENT_SUFFIX_RE, "").trim();
  s = s.replace(HOTELS_RESORTS_RE, "").trim();
  return s;
}

/**
 * Resolve a raw/validated brand string to a canonical Census brand.
 * Candidate Brand Text must not self-validate — this only maps names.
 */
export function resolveBrandMappingAlias(brandRaw, opts = {}) {
  const raw = String(brandRaw || "").trim();
  if (!raw) return { ok: false, reason: "blank" };

  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({});
  const attempts = [raw, stripBrandParentSuffix(raw)].filter(
    (v, i, a) => v && a.indexOf(v) === i
  );

  for (const attempt of attempts) {
    const alias =
      HIGH_BRAND_ALIAS_TO_CANONICAL[norm(attempt)] ||
      HIGH_BRAND_ALIAS_TO_CANONICAL[norm(attempt).replace(/[^a-z0-9]/g, "")];
    if (alias) {
      const entry = getCensusOfficialEntry(alias);
      return {
        ok: true,
        canonical: alias,
        parent: entry?.parent || null,
        method: "high_alias",
        soft: Boolean(entry?.soft),
      };
    }

    const official = resolveCensusOfficialBrand(attempt, {
      propertyName: opts.propertyName,
      sourceUrl: opts.sourceUrl,
      sourceFamily: opts.sourceFamily,
    });
    if (official.ok && official.canonical) {
      return {
        ok: true,
        canonical: official.canonical,
        parent: official.parent || null,
        method: official.method || "census_official_registry",
        soft: Boolean(official.soft),
        in_active_dictionary: false,
      };
    }

    const lookup = lookupCanonicalBrand(attempt, dictionary, {
      propertyName: opts.propertyName,
      sourceUrl: opts.sourceUrl,
    });
    if (lookup.ok && lookup.canonical) {
      return {
        ok: true,
        canonical: lookup.canonical,
        parent:
          canonicalizeParentCompany(
            lookup.entry?.parent_company || lookup.entry?.brand_family
          ) ||
          lookup.entry?.parent_company ||
          lookup.entry?.brand_family ||
          null,
        method: lookup.match || "dictionary",
        soft: Boolean(lookup.entry?.soft_brand_collection),
        in_active_dictionary: lookup.in_active_dictionary !== false,
      };
    }
  }

  return {
    ok: false,
    reason: "BRAND_MAPPING_GAP",
    brand: raw,
    class: "BRAND_MAPPING_GAP",
  };
}

export function isDeterministicAliasRepair(existing, canonical) {
  const a = norm(existing);
  const b = norm(canonical);
  if (!a || !b || a === b) return a === b;
  const stripped = norm(stripBrandParentSuffix(existing));
  if (stripped === b) return true;
  const resolved = resolveBrandMappingAlias(existing);
  return resolved.ok && norm(resolved.canonical) === b;
}

/**
 * Candidate Brand Text cannot itself validate Current Brand.
 */
export function candidateBrandCannotSelfValidate(fields = {}) {
  const candidate = String(fields[MAP_BRAND.candidateBrand] || "").trim();
  if (!candidate) return { ok: true, reason: "no_candidate" };
  return {
    ok: false,
    reason: "candidate_brand_is_research_hint_only",
    candidate,
    may_validate: false,
  };
}

export function affiliationStatusForValidatedBrand(resolved) {
  if (!resolved?.ok) return null;
  const soft = resolved.soft === true;
  const wanted = soft ? "Soft-Branded / Collection" : "Branded";
  return AFFILIATION_STATUS_OPTIONS.includes(wanted) ? wanted : null;
}

/**
 * Build NULL_FILL / alias-canonicalization patches for mapping gaps.
 * Alias rewrite of the same brand is allowed; different-brand overwrite is not.
 */
export function buildBrandMappingRepairPatch(fields, opts = {}) {
  const dictionary = opts.dictionary;
  const existing = String(fields[MAP_BRAND.currentBrand] || "").trim();
  const hint =
    existing ||
    String(opts.portfolioBrand || fields[MAP_BRAND.candidateBrand] || "").trim();
  if (!hint) return { ok: false, reason: "no_brand_hint" };

  const resolved = resolveBrandMappingAlias(hint, {
    dictionary,
    propertyName: fields[MAP_BRAND.propertyName],
    sourceUrl: fields[MAP_BRAND.officialUrl],
  });
  if (!resolved.ok) {
    return { ok: false, reason: "BRAND_MAPPING_GAP", brand: hint, class: "BRAND_MAPPING_GAP" };
  }

  /** @type {Record<string, unknown>} */
  const patch = {};
  if (existing) {
    if (norm(existing) === norm(resolved.canonical)) {
      // already canonical
    } else if (isDeterministicAliasRepair(existing, resolved.canonical)) {
      patch[MAP_BRAND.currentBrand] = resolved.canonical;
    } else {
      return {
        ok: false,
        reason: "existing_current_brand_not_alias",
        existing,
        canonical: resolved.canonical,
      };
    }
  } else if (opts.allowWriteCurrentBrand === true && opts.identityHigh === true) {
    // Only when a HIGH identity match already happened (portfolio / live directory)
    patch[MAP_BRAND.currentBrand] = resolved.canonical;
  } else if (!existing) {
    return { ok: false, reason: "current_brand_blank_needs_identity_match" };
  }

  if (isBlank(fields[MAP_BRAND.brandFamily]) && resolved.parent) {
    patch[MAP_BRAND.brandFamily] = resolved.parent;
  }
  if (isBlank(fields[MAP_BRAND.familySourceFamily]) && resolved.parent) {
    patch[MAP_BRAND.familySourceFamily] = resolved.parent;
  }
  const aff = affiliationStatusForValidatedBrand(resolved);
  if (aff && isBlank(fields[MAP_BRAND.affiliationStatus || "Affiliation Status"])) {
    const currentAff = fields["Affiliation Status"];
    if (isBlank(currentAff)) patch["Affiliation Status"] = aff;
  }

  if (!Object.keys(patch).length) {
    return { ok: false, reason: "nothing_to_repair", canonical: resolved.canonical };
  }
  patch[MAP_BRAND.lastReviewed] = todayIsoDate();
  patch[MAP_BRAND.enrichmentStatus] = "Partial";
  return {
    ok: true,
    class: existing ? "BRAND_ALIAS_REPAIR" : "BRAND_VALIDATED_HIGH",
    patch,
    canonical: resolved.canonical,
    method: resolved.method,
  };
}

/**
 * Repair mapping for Census records that already have Current Brand / gap hints.
 */
export function repairBrandMappingGaps(censusRecords, opts = {}) {
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary({});
  const proposals = [];
  const remainingGaps = [];
  let repairs = 0;
  let familyFills = 0;

  for (const rec of censusRecords || []) {
    const fields = rec.fields || {};
    const built = buildBrandMappingRepairPatch(fields, { dictionary });
    if (built.ok) {
      proposals.push({ id: rec.id, fields: built.patch, class: built.class });
      repairs += 1;
      if (built.patch[MAP_BRAND.brandFamily] || built.patch[MAP_BRAND.familySourceFamily]) {
        familyFills += 1;
      }
    } else if (built.reason === "BRAND_MAPPING_GAP") {
      remainingGaps.push({
        id: rec.id,
        brand: built.brand,
        name: fields[MAP_BRAND.propertyName] || null,
      });
    }
  }

  return {
    ok: true,
    version: BRAND_MAPPING_GAP_REPAIR_VERSION,
    repairs,
    family_fills: familyFills,
    remaining_gaps: remainingGaps.length,
    remaining_gap_samples: remainingGaps.slice(0, 40),
    proposals,
  };
}
