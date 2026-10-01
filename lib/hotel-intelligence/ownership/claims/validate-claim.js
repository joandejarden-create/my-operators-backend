/**
 * Packet 2.5A — Candidate → Accepted / Observation / Rejected gate.
 * Local entailment + role + temporal + entity-quality + semantic dedupe.
 * Derived facts are never labeled as source claims.
 */

import {
  CLAIM_ACCEPTANCE_STATUSES,
  CLAIM_ASSERTION_KINDS,
  FALSE_POSITIVE_CATEGORIES,
} from "./fp-taxonomy.js";
import {
  CLAIM_TYPE_PRECONDITIONS,
  MODALITY_RE,
  NEGATION_RE,
  TEMPORAL_ANNOUNCED_RE,
  TEMPORAL_FORMER_RE,
  TEMPORAL_CURRENT_RE,
  OWNERSHIP_PREDICATE_RE,
  OPERATOR_PREDICATE_RE,
} from "./role-lexicon.js";
import { getSourceProfile, profileForDocument } from "./source-profiles.js";
import { normalizeEntityName } from "../ids.js";

export const CLAIM_VALIDATE_VERSION = "claim-validate-v1";

const LOOKS_LIKE_ENTITY = null; // replaced by looksLikeEntityName()

const GARBAGE_OBJECT =
  /\b(appears|dispute|participation interest|corporate materials|owner language)\b/i;

function looksLikeEntityName(name) {
  const s = String(name || "").trim();
  if (s.length < 3 || s.length > 90) return false;
  if (GARBAGE_OBJECT.test(s)) return false;
  if (/^(the|a|an|in|of|interest|stake|participation|and)$/i.test(s)) return false;
  if (/^in\s+/i.test(s)) return false;
  if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(s)) return false;
  // Must start with letter/digit (allow One&Only)
  if (!/^[A-ZÁÉÍÓÚÑ0-9]/i.test(s)) return false;
  return true;
}

/**
 * Validate one extracted candidate.
 * @param {object} candidate — IntelligenceClaim-shaped
 * @param {{ document?: object, hotel_name?: string }} [ctx]
 * @returns {{ status: string, reason: string, fp_category: string|null, claim: object, observation: object|null }}
 */
export function validateClaimCandidate(candidate, ctx = {}) {
  const span = String(candidate.evidence_excerpt || "").trim();
  const doc = ctx.document || {};
  const profile = getSourceProfile(doc);
  const hotelName = ctx.hotel_name || doc.hotel_name || candidate.subject_raw;
  const notes = [...(candidate.derivation_notes || [])];

  let claim = {
    ...candidate,
    acceptance_status: "EXTRACTED_CANDIDATE",
    assertion_kind: "ASSERTED",
    source_profile: profileForDocument(doc),
  };

  // --- Negation ---
  if (NEGATION_RE.test(span)) {
    // Explicit "operator is not ownership" near OPERATES is OK; near OWNED_BY reject
    if (
      ["ENTITY_IS_OWNER_OF_PROPERTY", "ENTITY_HOLDS_TITLE"].includes(
        claim.claim_type
      )
    ) {
      return reject(claim, "NEGATION_IGNORED", "negation_blocks_ownership");
    }
    if (
      /no\s+longer\s+operates|formerly\s+managed|agreement\s+terminated/i.test(
        span
      ) &&
      ["ENTITY_OPERATES_HOTEL", "ENTITY_MANAGES_HOTEL"].includes(claim.claim_type)
    ) {
      claim = {
        ...claim,
        temporal_status: "FORMER",
      };
      notes.push("negation_or_termination_→_former");
    }
  }

  // --- Signing authority never from title alone ---
  if (claim.claim_type === "PERSON_HAS_SIGNING_AUTHORITY") {
    const pre = CLAIM_TYPE_PRECONDITIONS.PERSON_HAS_SIGNING_AUTHORITY;
    if (!pre.must.test(span)) {
      return reject(
        { ...claim, claim_type: "PERSON_HAS_TITLE" },
        "PERSON_TITLE_AS_AUTHORITY",
        "title_without_signing_language"
      );
    }
  }

  // --- Preconditions by claim type ---
  const pre = CLAIM_TYPE_PRECONDITIONS[claim.claim_type];
  if (pre?.must && !pre.must.test(span) && !pre.must.test(claim.predicate_raw || "")) {
    // Soft fail → observation for useful but under-specified roles
    if (
      ["ENTITY_SPONSORED_PROJECT", "ENTITY_DEVELOPED_PROPERTY"].includes(
        claim.claim_type
      )
    ) {
      return observe(
        claim,
        "ROLE_OVERINFERENCE",
        "precondition_not_met_observation",
        notes
      );
    }
    if (claim.claim_type === "HOTEL_CURRENT_BRAND") {
      return reject(
        claim,
        "UNSUPPORTED_CANONICALIZATION",
        "brand_claim_without_brand_language"
      );
    }
    return reject(claim, "ROLE_OVERINFERENCE", "claim_precondition_failed");
  }
  if (pre?.must_not && pre.must_not.test(span) && OWNERSHIP_PREDICATE_RE.test(span) === false) {
    if (claim.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY") {
      if (OPERATOR_PREDICATE_RE.test(span) && !OWNERSHIP_PREDICATE_RE.test(span)) {
        return reject(claim, "OPERATOR_AS_OWNER", "operator_language_not_ownership");
      }
    }
  }

  // --- Modality ---
  if (MODALITY_RE.test(span) || TEMPORAL_ANNOUNCED_RE.test(span)) {
    if (
      ["ENTITY_IS_OWNER_OF_PROPERTY", "ENTITY_OPERATES_HOTEL", "HOTEL_CURRENT_BRAND"].includes(
        claim.claim_type
      ) &&
      claim.temporal_status === "CURRENT"
    ) {
      if (claim.claim_type === "HOTEL_CURRENT_BRAND" || /breathless|announced|set\s+to\s+open/i.test(span)) {
        claim = {
          ...claim,
          claim_type:
            claim.claim_type === "HOTEL_CURRENT_BRAND"
              ? "HOTEL_ANNOUNCED_BRAND"
              : claim.claim_type,
          temporal_status: "ANNOUNCED",
        };
        notes.push("modality_→_announced");
      } else {
        return observe(
          claim,
          "MODALITY_IGNORED",
          "modal_language_not_current_fact",
          notes
        );
      }
    }
  }

  // --- Temporal attach ---
  if (TEMPORAL_FORMER_RE.test(span) && claim.temporal_status === "CURRENT") {
    // Only flip if former language is about THIS claim's object/role
    if (
      claim.claim_type === "HOTEL_FORMER_BRAND" ||
      /formerly|previously|former/i.test(span)
    ) {
      claim = { ...claim, temporal_status: "FORMER" };
      notes.push("temporal_former_from_span");
    }
  }
  if (
    TEMPORAL_CURRENT_RE.test(span) &&
    claim.temporal_status === "UNKNOWN" &&
    !TEMPORAL_FORMER_RE.test(span)
  ) {
    claim = { ...claim, temporal_status: "CURRENT" };
  }

  // --- Portfolio listing ---
  if (
    claim.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY" &&
    /owned\s+portfolio\s+listing|portfolio\s+listing/i.test(span)
  ) {
    return observe(
      { ...claim, temporal_status: "CURRENT_UNVERIFIED" },
      "PORTFOLIO_LISTING_AS_OWNERSHIP",
      "portfolio_listing_observation_only",
      notes
    );
  }

  // --- Local entailment: subject / object appear in span (or hotel context) ---
  const entail = checkLocalEntailment(claim, span, hotelName);
  if (!entail.ok) {
    if (entail.soft) {
      return observe(claim, entail.fp_category, entail.reason, notes);
    }
    return reject(claim, entail.fp_category, entail.reason);
  }

  // --- Entity quality ---
  const quality = checkEntityQuality(claim, hotelName);
  if (!quality.ok) {
    return reject(claim, quality.fp_category, quality.reason);
  }

  // --- Brand: object must not equal full hotel name unless brand language distinguishes ---
  if (
    claim.claim_type === "HOTEL_CURRENT_BRAND" &&
    normalizeEntityName(claim.object_raw) === normalizeEntityName(hotelName)
  ) {
    const brandCue =
      /\b(current\s+brand|brand\s+identity)\s*:/i.test(span) ||
      /\bcurrent\s+trading\s+name\b/i.test(span);
    if (!brandCue) {
      return reject(
        claim,
        "UNSUPPORTED_CANONICALIZATION",
        "hotel_identity_is_not_brand_claim"
      );
    }
    const family = brandFamilyOrNull(claim.object_raw, hotelName);
    if (family) {
      claim = { ...claim, object_raw: family, object_entity_candidate: family };
      notes.push("normalized_brand_family_from_hotel_name");
    } else if (
      normalizeEntityName(claim.object_raw) === normalizeEntityName(hotelName)
    ) {
      return observe(
        claim,
        "UNSUPPORTED_CANONICALIZATION",
        "trading_name_equals_hotel_identity",
        notes
      );
    }
  }

  // --- Adjacent self ---
  if (
    claim.claim_type === "PROPERTY_IS_ADJACENT_TO" &&
    normalizeEntityName(claim.subject_raw) === normalizeEntityName(claim.object_raw)
  ) {
    return reject(
      claim,
      "PROPERTY_CONTEXT_BLEED",
      "adjacent_self_reference"
    );
  }

  // --- Percentage scope ---
  if (claim.claim_type === "ENTITY_OWNS_PERCENT_OF_ENTITY") {
    if (
      claim.ownership_percentage == null ||
      claim.ownership_percentage <= 0 ||
      claim.ownership_percentage > 100
    ) {
      return reject(claim, "PERCENTAGE_SCOPE_ERROR", "invalid_percentage");
    }
    if (
      normalizeEntityName(claim.subject_raw) === normalizeEntityName(hotelName) &&
      normalizeEntityName(claim.object_raw) === normalizeEntityName(hotelName)
    ) {
      return reject(
        claim,
        "PERCENTAGE_SCOPE_ERROR",
        "percentage_subject_object_both_hotel"
      );
    }
    const obj = claim.object_raw || claim.subject_raw;
    if (!obj || GARBAGE_OBJECT.test(obj) || !looksLikeEntityName(obj)) {
      return reject(
        claim,
        "PERCENTAGE_SCOPE_ERROR",
        "percentage_object_not_entity"
      );
    }
    if (!looksLikeEntityName(claim.subject_raw || "") && claim.subject_raw) {
      // Allow hotel context as subject when object is the % holder... prefer reject bad subjects
      if (/^in\s+/i.test(claim.subject_raw) || GARBAGE_OBJECT.test(claim.subject_raw)) {
        return reject(
          claim,
          "PERCENTAGE_SCOPE_ERROR",
          "percentage_subject_not_entity"
        );
      }
    }
    // Subject should be the owning entity when pattern is "X acquires N%"
    if (/^stake\s+in\b/i.test(claim.subject_raw || "")) {
      return reject(
        claim,
        "PERCENTAGE_SCOPE_ERROR",
        "percentage_subject_not_entity"
      );
    }
  }

  // --- Cross-sentence ownership overreach heuristic ---
  // If span contains both operate and own about different clauses, keep only local match
  if (
    claim.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY" &&
    /\.\s+[A-Z]/.test(span) &&
    OPERATOR_PREDICATE_RE.test(span)
  ) {
    // Allow "owned by X and operated by X" same sentence
    if (!/owned\s+by.{0,80}operated\s+by|operado/i.test(span)) {
      notes.push("multi_clause_span_reviewed");
    }
  }

  // --- Source profile demotions ---
  if ((profile.demote || []).includes(claim.claim_type)) {
    if (claim.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY") {
      return observe(
        claim,
        "ROLE_OVERINFERENCE",
        `demoted_by_source_profile:${claim.source_profile}`,
        notes
      );
    }
  }

  // --- Calibration: ambiguous role → lower extraction confidence ---
  let extractionConfidence = Number(claim.extraction_confidence || 0.7);
  if (claim.claim_type === "ENTITY_PARTICIPATES_IN_JV" && !/\bjv\b|joint\s+venture|fideicomiso/i.test(span)) {
    extractionConfidence = Math.min(extractionConfidence, 0.55);
  }
  if (!claim.object_raw || String(claim.object_raw).length < 3) {
    return reject(claim, "ENTITY_CONTEXT_BLEED", "missing_object");
  }

  const accepted = {
    ...claim,
    derivation_notes: notes,
    extraction_confidence: extractionConfidence,
    acceptance_status: "ACCEPTED_CLAIM",
    assertion_kind: "ASSERTED",
    validation_reason: "entailed_local_span",
    evidence_excerpt: tightenSpan(span, claim),
  };

  return {
    status: "ACCEPTED_CLAIM",
    reason: "accepted",
    fp_category: null,
    claim: accepted,
    observation: null,
  };
}

/**
 * Validate + dedupe a batch of candidates.
 * @param {object[]} candidates
 * @param {{ documentsById?: Map|object, hotel_name?: string }} [ctx]
 */
export function acceptClaimsFromCandidates(candidates, ctx = {}) {
  const accepted = [];
  const observations = [];
  const rejected = [];
  const seen = new Map();

  for (const cand of candidates || []) {
    const doc =
      ctx.documentsById?.[cand.source_id] ||
      ctx.documentsById?.get?.(cand.source_id) ||
      null;
    const result = validateClaimCandidate(cand, {
      document: doc || { authority: cand.source_type, hotel_name: ctx.hotel_name },
      hotel_name: ctx.hotel_name,
    });

    if (result.status === "REJECTED_EXTRACTION") {
      rejected.push(result);
      continue;
    }
    if (result.status === "OBSERVATION_ONLY") {
      observations.push(result);
      continue;
    }

    const key = semanticDedupeKey(result.claim);
    if (seen.has(key)) {
      rejected.push({
        status: "REJECTED_EXTRACTION",
        reason: "duplicate_semantic_claim",
        fp_category: "DUPLICATE_SEMANTIC_CLAIM",
        claim: {
          ...result.claim,
          acceptance_status: "REJECTED_EXTRACTION",
          validation_reason: "duplicate_semantic_claim",
        },
        observation: null,
        duplicate_of: seen.get(key),
      });
      continue;
    }
    seen.set(key, result.claim.claim_id);
    accepted.push(result.claim);
  }

  return {
    accepted_claims: accepted,
    observations: observations.map((o) => o.observation || toObservation(o.claim, o.reason)),
    rejected_extractions: rejected.map((r) => ({
      claim_id: r.claim?.claim_id,
      claim_type: r.claim?.claim_type,
      reason: r.reason,
      fp_category: r.fp_category,
      evidence_excerpt: r.claim?.evidence_excerpt,
      subject_raw: r.claim?.subject_raw,
      object_raw: r.claim?.object_raw,
    })),
    counts: {
      candidates: (candidates || []).length,
      accepted: accepted.length,
      observations: observations.length,
      rejected: rejected.length,
    },
  };
}

function checkLocalEntailment(claim, span, hotelName) {
  const s = span.toLowerCase();
  const obj = String(claim.object_raw || "").trim();
  const sub = String(claim.subject_raw || "").trim();

  if (!span || span.length < 8) {
    return {
      ok: false,
      soft: false,
      fp_category: "OTHER",
      reason: "evidence_span_too_short",
    };
  }

  // Object must appear in span (or alias abbreviation)
  if (obj) {
    const objNorm = normalizeEntityName(obj);
    const spanNorm = normalizeEntityName(span);
    const objTokens = objNorm.split(" ").filter((t) => t.length > 2);
    const hit =
      spanNorm.includes(objNorm) ||
      (objTokens.length && objTokens.every((t) => spanNorm.includes(t))) ||
      (obj.length <= 6 && new RegExp(`\\b${escapeRe(obj)}\\b`, "i").test(span));
    if (!hit) {
      return {
        ok: false,
        soft: false,
        fp_category: "ENTITY_CONTEXT_BLEED",
        reason: "object_not_in_evidence_span",
      };
    }
  }

  // Subject: hotel context OR appears in span
  if (sub) {
    const subNorm = normalizeEntityName(sub);
    const hotelNorm = normalizeEntityName(hotelName);
    const spanNorm = normalizeEntityName(span);
    const subjectOk =
      spanNorm.includes(subNorm) ||
      (hotelNorm && subNorm === hotelNorm) ||
      (hotelNorm && spanNorm.includes(hotelNorm));
    if (!subjectOk && claim.claim_type.startsWith("PERSON_")) {
      // person name must be in span
      if (!spanNorm.includes(subNorm.split(" ")[0] || "")) {
        return {
          ok: false,
          soft: false,
          fp_category: "ENTITY_CONTEXT_BLEED",
          reason: "person_not_in_span",
        };
      }
    } else if (!subjectOk && !claim.claim_type.startsWith("HOTEL_")) {
      // Allow org-as-subject percent claims when subject in span
      if (!spanNorm.includes(subNorm.split(" ").slice(0, 2).join(" "))) {
        return {
          ok: false,
          soft: true,
          fp_category: "PROPERTY_CONTEXT_BLEED",
          reason: "weak_subject_hotel_context",
        };
      }
    }
  }

  return { ok: true };
}

function checkEntityQuality(claim, hotelName) {
  const obj = String(claim.object_raw || "").trim();
  if (!obj) {
    return { ok: false, fp_category: "ENTITY_CONTEXT_BLEED", reason: "empty_object" };
  }
  if (!looksLikeEntityName(obj)) {
    return {
      ok: false,
      fp_category: "PERCENTAGE_SCOPE_ERROR",
      reason: "garbage_object_span",
    };
  }
  if (obj.length > 80) {
    return {
      ok: false,
      fp_category: "ENTITY_CONTEXT_BLEED",
      reason: "object_span_too_long",
    };
  }
  void hotelName;
  return { ok: true };
}

function semanticDedupeKey(claim) {
  const type = claim.claim_type;
  const family =
    type === "ENTITY_MANAGES_HOTEL" ? "ENTITY_OPERATES_HOTEL" : type;
  const obj = normalizeEntityName(
    stripLegalSuffix(claim.object_raw || claim.canonical_object_id || "")
  );
  const sub = normalizeEntityName(
    stripLegalSuffix(claim.subject_raw || "")
  );
  const pct =
    claim.ownership_percentage != null
      ? String(claim.ownership_percentage)
      : "";
  const temp =
    claim.temporal_status === "CURRENT_UNVERIFIED"
      ? "CURRENT"
      : claim.temporal_status || "";
  const objKey = shortenOrgKey(obj);
  const subKey = shortenOrgKey(sub);
  // Percent / JV / ownership edges must not collapse distinct subjects
  if (
    [
      "ENTITY_OWNS_PERCENT_OF_ENTITY",
      "ENTITY_IS_OWNER_OF_PROPERTY",
      "ENTITY_HOLDS_TITLE",
      "ENTITY_PARTICIPATES_IN_JV",
      "ENTITY_SPONSORED_PROJECT",
    ].includes(family)
  ) {
    return `${family}|${subKey}|${objKey}|${pct}|${temp}|${claim.hotel_context_id || ""}`;
  }
  return `${family}|${objKey}|${pct}|${temp}|${claim.hotel_context_id || ""}`;
}

function shortenOrgKey(norm) {
  if (!norm) return "";
  if (norm.includes("inmobiliaria") || norm === "ihvsf") return "ihvsf";
  if (norm.includes("grupo hotelero santa fe") || norm === "gsf") return "gsf";
  if (norm.includes("chartwell")) return "chartwell";
  if (norm.includes("nakheel")) return "nakheel";
  if (norm.includes("walton")) return "walton";
  if (norm.includes("fideicomiso mahekal") || norm.includes("mahekal holding") || norm === "mhkl") {
    return "mahekal_jv";
  }
  if (norm.includes("krystal resort")) return "krystal_resort_pv";
  if (norm.includes("breathless")) return "breathless";
  if (norm.includes("hilton")) return "hilton";
  if (norm.includes("krystal grand") && !norm.includes("puerto")) return "krystal_grand";
  return norm;
}

function stripLegalSuffix(name) {
  return String(name || "")
    .replace(
      /,?\s*S\.?\s*A\.?\s*B\.?\s*de\s*C\.?\s*V\.?/gi,
      ""
    )
    .replace(
      /,?\s*S\.?\s*de\s*R\.?\s*L\.?\s*de\s*C\.?\s*V\.?/gi,
      ""
    )
    .trim();
}

function stripGeoSuffix(name) {
  return String(name || "")
    .replace(
      /\s+(Puerto\s+Vallarta|Los\s+Cabos|Canc[uú]n|Riviera\s+Maya|Yucat[aá]n|San\s+Jose\s+Del\s+Cabo)$/i,
      ""
    )
    .trim();
}

/** Avoid stripping brand names that ARE the geography-qualified brand (Chablé Yucatán). */
function brandFamilyOrNull(name, hotelName) {
  const family = stripGeoSuffix(name);
  if (!family || normalizeEntityName(family) === normalizeEntityName(name)) {
    return null;
  }
  // If family is a single token equal to brand stem of hotel, keep only when hotel had geo suffix
  if (
    normalizeEntityName(name) === normalizeEntityName(hotelName) &&
    family.split(/\s+/).length === 1 &&
    !/grand|resort|collection|hotels?/i.test(family)
  ) {
    return null; // e.g. Chablé from Chablé Yucatán — observe identity instead
  }
  return family;
}

function tightenSpan(span, claim) {
  // Prefer sentence containing object
  const obj = String(claim.object_raw || "").split(/\s+/).slice(0, 3).join(" ");
  if (obj && span.length > 200) {
    const idx = span.toLowerCase().indexOf(obj.toLowerCase().slice(0, 12));
    if (idx >= 0) {
      const start = Math.max(0, span.lastIndexOf(".", idx - 1) + 1);
      const end = span.indexOf(".", idx + obj.length);
      const sliced = span.slice(start, end > 0 ? end + 1 : Math.min(span.length, idx + 120)).trim();
      if (sliced.length >= 20) return sliced.slice(0, 320);
    }
  }
  return span.slice(0, 320);
}

function toObservation(claim, reason) {
  return {
    schema_version: "intelligence-observation-from-claim-v1",
    observation_type: "unresolved_candidate",
    claim_type: claim.claim_type,
    subject_raw: claim.subject_raw,
    object_raw: claim.object_raw,
    evidence_excerpt: claim.evidence_excerpt,
    source_id: claim.source_id,
    reason,
    hotel_context_id: claim.hotel_context_id,
    created_at: new Date().toISOString(),
  };
}

function reject(claim, fpCategory, reason) {
  return {
    status: "REJECTED_EXTRACTION",
    reason,
    fp_category: FALSE_POSITIVE_CATEGORIES.includes(fpCategory)
      ? fpCategory
      : "OTHER",
    claim: {
      ...claim,
      acceptance_status: "REJECTED_EXTRACTION",
      validation_reason: reason,
      fp_category: fpCategory,
    },
    observation: null,
  };
}

function observe(claim, fpCategory, reason, notes = []) {
  const c = {
    ...claim,
    acceptance_status: "OBSERVATION_ONLY",
    validation_reason: reason,
    fp_category: fpCategory,
    derivation_notes: [...(claim.derivation_notes || []), ...notes],
  };
  return {
    status: "OBSERVATION_ONLY",
    reason,
    fp_category: fpCategory,
    claim: c,
    observation: toObservation(c, reason),
  };
}

function escapeRe(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export {
  CLAIM_ACCEPTANCE_STATUSES,
  CLAIM_ASSERTION_KINDS,
  semanticDedupeKey,
};
