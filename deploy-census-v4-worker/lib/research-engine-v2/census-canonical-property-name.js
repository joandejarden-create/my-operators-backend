/**
 * Canonical Property Name — High-confidence autofill / exact-suffix cleanup
 * for Hotel Property Census only (tbl9aY5ijiuIzzWam).
 *
 * Never Brand Setup / Brand Explorer / VIC / old Census.
 * Never overwrite materially different populated values.
 */

import {
  classifyPropertyNameProblems,
  normalizeHotelName,
} from "./production-census-property-name-cleanup-extractor.js";

export const CANONICAL_PROPERTY_NAME_FIELD = "Canonical Property Name";
export const CANONICAL_NAME_VERSION = "census-canonical-property-name-v1";

export const CANONICAL_NAME_STATUS = Object.freeze({
  COMPLETE_CLEAN: "complete_clean",
  BLANK_CAN_AUTOFILL: "blank_can_autofill",
  DIRTY_CAN_CLEAN: "dirty_can_clean",
  POPULATED_CONFLICT_NEEDS_REVIEW: "populated_conflict_needs_review",
  MISSING_SOURCE_SUPPORT: "missing_source_support",
  STEWARD_REVIEW_REQUIRED: "steward_review_required",
  INTENTIONALLY_BLANK: "intentionally_blank",
  FIELD_MISSING: "canonical_property_name_field_missing",
});

export const CANONICAL_COMPLETION_STATUS = Object.freeze({
  APPLIED_CLEAN:
    "production_census_canonical_property_name_completion_applied_clean",
  PARTIAL_STEWARD:
    "production_census_canonical_property_name_completion_partial_steward_remaining",
  READY_NEEDS_PRODUCTION_CYCLE:
    "production_census_canonical_property_name_completion_ready_needs_production_cycle",
  BLOCKED: "production_census_canonical_property_name_completion_blocked",
  FIELD_MISSING: "canonical_property_name_field_missing",
});

/**
 * Exact, safe membership / soft-collection trailing suffixes only.
 * Do not strip brand-as-identity ("JOIA … by Iberostar") or ambiguous tails.
 */
export const SAFE_MEMBERSHIP_SUFFIX_PATTERNS = Object.freeze([
  /,\s*a\s+member\s+of\s+radisson\s+individuals\.?\s*$/i,
  /\s+a\s+member\s+of\s+radisson\s+individuals\.?\s*$/i,
  /,\s*a\s+member\s+of\s+preferred\s+hotels\s*(?:&\s*|and\s+)?resorts\.?\s*$/i,
  /\s+a\s+member\s+of\s+preferred\s+hotels\s*(?:&\s*|and\s+)?resorts\.?\s*$/i,
  /,\s*part\s+of\s+radisson\s+individuals\.?\s*$/i,
  /\s+part\s+of\s+radisson\s+individuals\.?\s*$/i,
  /,\s*part\s+of\s+preferred\s+hotels\s*(?:&\s*|and\s+)?resorts\.?\s*$/i,
  /\s+part\s+of\s+preferred\s+hotels\s*(?:&\s*|and\s+)?resorts\.?\s*$/i,
]);

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !v.trim()) return true;
  return false;
}

/**
 * Normalize for equivalence / duplicate keys (punctuation + spacing).
 */
export function normalizeCanonicalCompareKey(name) {
  return String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function namesAreEquivalent(a, b) {
  const na = normalizeCanonicalCompareKey(a);
  const nb = normalizeCanonicalCompareKey(b);
  if (!na || !nb) return false;
  return na === nb;
}

/**
 * Strip only exact safe membership/collection suffixes.
 * Returns { cleaned, stripped, patterns_matched }.
 */
export function stripSafeMembershipSuffixes(name) {
  let cleaned = String(name || "").replace(/\s+/g, " ").trim();
  const matched = [];
  let changed = true;
  while (changed) {
    changed = false;
    for (const re of SAFE_MEMBERSHIP_SUFFIX_PATTERNS) {
      const next = cleaned.replace(re, "").replace(/\s+/g, " ").trim().replace(/,\s*$/, "").trim();
      if (next !== cleaned && next.length >= 3) {
        matched.push(re.source);
        cleaned = next;
        changed = true;
      }
    }
  }
  return {
    cleaned,
    stripped: matched.length > 0 && cleaned !== String(name || "").replace(/\s+/g, " ").trim(),
    patterns_matched: matched,
  };
}

export function hasSafeDirtyMembershipSuffix(name) {
  const s = String(name || "").trim();
  if (!s) return false;
  return SAFE_MEMBERSHIP_SUFFIX_PATTERNS.some((re) => re.test(s));
}

/**
 * Derive High-confidence canonical candidate from Property Name.
 * Does not invent names; only cleans exact safe suffixes or copies clean chain names.
 */
export function deriveCanonicalPropertyNameCandidate(fields = {}) {
  const propertyName = String(fields["Property Name"] || "").trim();
  const brand = String(fields["Current Brand"] || "").trim();
  const city = String(fields.City || "").trim();
  const country = String(fields.Country || "").trim();
  const sourceUrl = String(fields["Source URL"] || fields["Official Property URL"] || "").trim();
  const humanReview = fields["Human Review Required"] === true;

  if (!propertyName || /^unknown$/i.test(propertyName)) {
    return {
      ok: false,
      reason: "property_name_missing_or_unknown",
      candidate: null,
      confidence: null,
    };
  }
  if (!brand) {
    return { ok: false, reason: "brand_missing", candidate: null, confidence: null };
  }
  if (!city || /^unknown$/i.test(city)) {
    return { ok: false, reason: "city_missing_or_unknown", candidate: null, confidence: null };
  }
  if (!country) {
    return { ok: false, reason: "country_missing", candidate: null, confidence: null };
  }
  if (!sourceUrl) {
    return { ok: false, reason: "source_url_missing", candidate: null, confidence: null };
  }
  if (humanReview) {
    return { ok: false, reason: "human_review_required", candidate: null, confidence: null };
  }

  const problems = classifyPropertyNameProblems(propertyName);
  if (problems.malformed && problems.severity === "high") {
    return {
      ok: false,
      reason: "property_name_malformed_route_cleanup",
      candidate: null,
      confidence: null,
      problems,
      route: "property_name_cleanup",
    };
  }
  if (problems.malformed && problems.reasons.includes("generic_only")) {
    return {
      ok: false,
      reason: "property_name_not_property_specific",
      candidate: null,
      confidence: null,
      problems,
    };
  }

  const stripped = stripSafeMembershipSuffixes(propertyName);
  const candidate = normalizeHotelName(stripped.cleaned).replace(/\s+/g, " ").trim();
  if (!candidate || candidate.length < 3) {
    return { ok: false, reason: "clean_candidate_too_short", candidate: null, confidence: null };
  }
  if (/^unknown$/i.test(candidate)) {
    return { ok: false, reason: "candidate_unknown", candidate: null, confidence: null };
  }

  // After strip, reject if still marketing-heavy
  const afterProblems = classifyPropertyNameProblems(candidate);
  if (afterProblems.malformed && afterProblems.severity === "high") {
    return {
      ok: false,
      reason: "clean_candidate_still_malformed",
      candidate: null,
      confidence: null,
      route: "property_name_cleanup",
    };
  }

  return {
    ok: true,
    reason: stripped.stripped
      ? "exact_membership_suffix_stripped"
      : "clean_property_name_copy",
    candidate,
    confidence: "High",
    stripped: stripped.stripped,
    patterns_matched: stripped.patterns_matched,
  };
}

/**
 * Classify Canonical Property Name for one Census record.
 */
export function classifyCanonicalPropertyName(fields = {}, opts = {}) {
  const existing = String(fields[CANONICAL_PROPERTY_NAME_FIELD] || "").trim();
  const derived = deriveCanonicalPropertyNameCandidate(fields);

  if (opts.fieldExists === false) {
    return {
      status: CANONICAL_NAME_STATUS.FIELD_MISSING,
      reason: "canonical_property_name_field_missing",
      existing: existing || null,
      candidate: null,
      write_allowed: false,
    };
  }

  // Intentionally blank: held / steward hold
  if (fields["Human Review Required"] === true && isBlank(existing)) {
    return {
      status: CANONICAL_NAME_STATUS.INTENTIONALLY_BLANK,
      reason: "human_review_required",
      existing: null,
      candidate: derived.candidate,
      write_allowed: false,
    };
  }

  if (!derived.ok) {
    if (derived.route === "property_name_cleanup") {
      return {
        status: CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED,
        reason: derived.reason,
        existing: existing || null,
        candidate: null,
        write_allowed: false,
        route: "property_name_cleanup",
      };
    }
    if (isBlank(existing)) {
      return {
        status: CANONICAL_NAME_STATUS.MISSING_SOURCE_SUPPORT,
        reason: derived.reason,
        existing: null,
        candidate: null,
        write_allowed: false,
      };
    }
    // Existing populated but we can't derive a safe compare candidate
    if (hasSafeDirtyMembershipSuffix(existing)) {
      const strippedExisting = stripSafeMembershipSuffixes(existing);
      if (strippedExisting.stripped && strippedExisting.cleaned.length >= 3) {
        return {
          status: CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN,
          reason: "existing_canonical_membership_suffix",
          existing,
          candidate: strippedExisting.cleaned,
          write_allowed: true,
          confidence: "High",
        };
      }
    }
    return {
      status: CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED,
      reason: derived.reason || "cannot_validate_existing_canonical",
      existing,
      candidate: null,
      write_allowed: false,
    };
  }

  const candidate = derived.candidate;

  if (isBlank(existing)) {
    return {
      status: CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL,
      reason: derived.reason,
      existing: null,
      candidate,
      write_allowed: true,
      confidence: "High",
      stripped: derived.stripped,
    };
  }

  if (namesAreEquivalent(existing, candidate)) {
    if (hasSafeDirtyMembershipSuffix(existing) && existing !== candidate) {
      return {
        status: CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN,
        reason: "existing_canonical_has_safe_suffix",
        existing,
        candidate,
        write_allowed: true,
        confidence: "High",
      };
    }
    return {
      status: CANONICAL_NAME_STATUS.COMPLETE_CLEAN,
      reason: "matches_clean_candidate",
      existing,
      candidate,
      write_allowed: false,
    };
  }

  if (hasSafeDirtyMembershipSuffix(existing)) {
    const strippedExisting = stripSafeMembershipSuffixes(existing);
    if (
      strippedExisting.stripped &&
      namesAreEquivalent(strippedExisting.cleaned, candidate)
    ) {
      return {
        status: CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN,
        reason: "existing_canonical_membership_suffix",
        existing,
        candidate: strippedExisting.cleaned,
        write_allowed: true,
        confidence: "High",
      };
    }
  }

  return {
    status: CANONICAL_NAME_STATUS.POPULATED_CONFLICT_NEEDS_REVIEW,
    reason: "existing_differs_materially",
    existing,
    candidate,
    write_allowed: false,
  };
}

/**
 * Property-level URL only — shared sitemap/directory Source URLs must not
 * block Canonical autofill (Marriott mexico-hotel-sitemap, Preferred directory, etc.).
 */
function propertyLevelUrlKey(url, isPropertyLevelUrlFn) {
  const raw = String(url || "").trim();
  if (!raw) return null;
  if (typeof isPropertyLevelUrlFn === "function" && !isPropertyLevelUrlFn(raw)) {
    return null;
  }
  // Heuristic fallback when detector not injected (directory / sitemap / regional hubs)
  const s = raw.toLowerCase();
  if (
    /sitemap|\/locations\/|\/regional-hotels|\/directory|hotel-sitemap|\/destination\/continent\//i.test(
      s
    )
  ) {
    return null;
  }
  return s;
}

/**
 * Build duplicate-risk index over Hotel Property Census records.
 * @param {object[]} censusRecords
 * @param {{ isPropertyLevelUrl?: (url: string) => boolean }} [opts]
 */
export function buildCanonicalDuplicateIndex(censusRecords = [], opts = {}) {
  const bySourceUrl = new Map();
  const byIdentityKey = new Map();
  const byCanonicalIdentity = new Map();
  const byNameIdentity = new Map();
  const byAddress = new Map();
  const isPropertyLevelUrlFn = opts.isPropertyLevelUrl || null;

  const add = (map, key, recId) => {
    if (!key || !recId) return;
    if (!map.has(key)) map.set(key, new Set());
    map.get(key).add(recId);
  };

  for (const rec of censusRecords) {
    const id = rec.id;
    const f = rec.fields || {};
    const brand = normalizeCanonicalCompareKey(f["Current Brand"]);
    const city = normalizeCanonicalCompareKey(f.City);
    const country = normalizeCanonicalCompareKey(f.Country);
    const geo = `${brand}|${city}|${country}`;

    const identityKey = String(f["Property Identity Key"] || "").trim().toLowerCase();
    if (identityKey) add(byIdentityKey, identityKey, id);

    const sourceUrl = propertyLevelUrlKey(f["Source URL"], isPropertyLevelUrlFn);
    if (sourceUrl) add(bySourceUrl, sourceUrl, id);
    const official = propertyLevelUrlKey(f["Official Property URL"], isPropertyLevelUrlFn);
    if (official) add(bySourceUrl, official, id);

    const canonical = normalizeCanonicalCompareKey(f[CANONICAL_PROPERTY_NAME_FIELD]);
    if (canonical && brand && city && country) {
      add(byCanonicalIdentity, `${canonical}|${geo}`, id);
    }
    const pname = normalizeCanonicalCompareKey(f["Property Name"]);
    if (pname && brand && city && country) {
      add(byNameIdentity, `${pname}|${geo}`, id);
    }
    const address = normalizeCanonicalCompareKey(f.Address);
    if (address && address.length >= 12 && brand && country) {
      add(byAddress, `${address}|${brand}|${country}`, id);
    }
  }

  return { bySourceUrl, byIdentityKey, byCanonicalIdentity, byNameIdentity, byAddress };
}

/**
 * Check whether writing candidate canonical for recordId creates duplicate risk.
 * Shared directory/sitemap Source URLs are ignored (not property-level identity).
 * @param {object} record
 * @param {string} candidate
 * @param {ReturnType<typeof buildCanonicalDuplicateIndex>} index
 * @param {{ isPropertyLevelUrl?: (url: string) => boolean }} [opts]
 */
export function assessCanonicalDuplicateRisk(record, candidate, index, opts = {}) {
  const id = record?.id;
  const f = record?.fields || {};
  const brand = normalizeCanonicalCompareKey(f["Current Brand"]);
  const city = normalizeCanonicalCompareKey(f.City);
  const country = normalizeCanonicalCompareKey(f.Country);
  const geo = `${brand}|${city}|${country}`;
  const candKey = normalizeCanonicalCompareKey(candidate);
  const isPropertyLevelUrlFn = opts.isPropertyLevelUrl || null;

  const others = (map, key) => {
    const set = map?.get?.(key);
    if (!set) return [];
    return [...set].filter((rid) => rid !== id);
  };

  const hits = [];
  if (candKey && brand && city && country) {
    const key = `${candKey}|${geo}`;
    for (const rid of others(index.byCanonicalIdentity, key)) {
      hits.push({ type: "canonical_brand_city_country", other_record_id: rid });
    }
    for (const rid of others(index.byNameIdentity, key)) {
      hits.push({ type: "property_name_brand_city_country", other_record_id: rid });
    }
  }

  // Only property-level Source / Official URLs participate in URL collision checks
  const sourceUrl = propertyLevelUrlKey(f["Source URL"], isPropertyLevelUrlFn);
  if (sourceUrl) {
    for (const rid of others(index.bySourceUrl, sourceUrl)) {
      hits.push({ type: "source_url", other_record_id: rid });
    }
  }
  const official = propertyLevelUrlKey(f["Official Property URL"], isPropertyLevelUrlFn);
  if (official && official !== sourceUrl) {
    for (const rid of others(index.bySourceUrl, official)) {
      hits.push({ type: "official_property_url", other_record_id: rid });
    }
  }

  if (hits.length) {
    return {
      duplicate_risk: true,
      reason: "canonical_write_would_collide",
      hits,
    };
  }
  return { duplicate_risk: false, reason: null, hits: [] };
}

/**
 * Propose High write for Canonical Property Name (blank autofill or dirty cleanup).
 */
export function proposeCanonicalPropertyNameWrite(record, index, opts = {}) {
  const fields = record?.fields || {};
  const classified = classifyCanonicalPropertyName(fields, opts);

  if (
    classified.status !== CANONICAL_NAME_STATUS.BLANK_CAN_AUTOFILL &&
    classified.status !== CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN
  ) {
    return {
      action: "skip",
      classified,
      patch: null,
    };
  }

  const candidate = classified.candidate;
  if (!candidate || classified.confidence !== "High") {
    return { action: "skip", classified, patch: null };
  }

  const dup = assessCanonicalDuplicateRisk(record, candidate, index, opts);
  if (dup.duplicate_risk) {
    return {
      action: "steward",
      classified: {
        ...classified,
        status: CANONICAL_NAME_STATUS.STEWARD_REVIEW_REQUIRED,
        reason: "duplicate_risk",
        duplicate: dup,
      },
      patch: null,
    };
  }

  // Idempotent: skip if already equal after normalize
  const existing = String(fields[CANONICAL_PROPERTY_NAME_FIELD] || "").trim();
  if (existing && namesAreEquivalent(existing, candidate) && existing === candidate) {
    return {
      action: "skip_identical",
      classified: { ...classified, status: CANONICAL_NAME_STATUS.COMPLETE_CLEAN },
      patch: null,
    };
  }

  return {
    action: classified.status === CANONICAL_NAME_STATUS.DIRTY_CAN_CLEAN ? "cleanup" : "autofill",
    classified,
    patch: { [CANONICAL_PROPERTY_NAME_FIELD]: candidate },
    before: existing || null,
    after: candidate,
  };
}
