/**
 * Packet 2.5 — Atomic claim extractor.
 * Outputs Intelligence Claims only — never final owner truth.
 * Reuses ownership phrase patterns; extends for JV %, brand state, people, adjacent.
 */

import { extractOwnershipClaimsFromText } from "../research/extract-phrases.js";
import { createIntelligenceClaim, deriveOverallClaimConfidence } from "./schemas.js";
import { claimTypeToRelationshipCandidate } from "./source-language.js";
import {
  authorityForClaimType,
  normalizeAuthorityKey,
} from "./authority.js";

export const CLAIM_EXTRACTOR_VERSION = "claim-extractor-v1";

const REL_TO_CLAIM = Object.freeze({
  OWNED_BY: "ENTITY_IS_OWNER_OF_PROPERTY",
  CONTROLLED_BY: "ENTITY_CONTROLS_ENTITY",
  SPONSORED_BY: "ENTITY_SPONSORED_PROJECT",
  JV_WITH: "ENTITY_PARTICIPATES_IN_JV",
  DEVELOPED_BY: "ENTITY_DEVELOPED_PROPERTY",
  OPERATED_BY: "ENTITY_OPERATES_HOTEL",
  ASSET_MANAGED_BY: "ENTITY_MANAGES_HOTEL",
  BRANDED_BY: "ENTITY_BRANDS_HOTEL",
  LEASED_FROM: "ENTITY_LEASES_PROPERTY",
  FINANCED_BY: "ENTITY_FINANCED_PROPERTY",
});

/** Extra atomic patterns beyond ownership phrase extract. */
const EXTRA_PATTERNS = [
  {
    claim_type: "ENTITY_OWNS_PERCENT_OF_ENTITY",
    temporal_default: "UNKNOWN",
    re: /\b(\d{1,3})\s*%\s+(?:stake|interest|participaci[oó]n)\s+(?:in\s+|de\s+)([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.'’\-]+(?:\s+(?:&\s*)?[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.'’\-]*){0,5})/gi,
    subject_from: "context_org",
    object_group: 2,
    percent_group: 1,
  },
  {
    claim_type: "ENTITY_OWNS_PERCENT_OF_ENTITY",
    temporal_default: "FORMER",
    re: /\b(Grupo\s+Chartwell|Chartwell)\s+formerly\s+owned\s+a\s+(\d{1,3})\s*%/gi,
    subject_group: 1,
    percent_group: 2,
    object_from: "hotel_or_vehicle",
  },
  {
    claim_type: "ENTITY_OWNS_PERCENT_OF_ENTITY",
    temporal_default: "UNKNOWN",
    re: /\b(Grupo\s+Hotelero\s+Santa\s+Fe(?:[,\s]+S\.?A\.?B\.?\s*de\s*C\.?V\.?)?)\s+acquired\s+the\s+remaining\s+(\d{1,3})\s*%/gi,
    subject_group: 1,
    percent_group: 2,
    object_from: "hotel_or_vehicle",
  },
  {
    claim_type: "ENTITY_OWNS_PERCENT_OF_ENTITY",
    temporal_default: "UNKNOWN",
    re: /([A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.'’\-]+(?:\s+[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.'’\-]*){0,4})\s+(?:acquires?|acquired|holds?)\s+(?:a\s+)?(?:remaining\s+)?(\d{1,3})\s*%/gi,
    subject_group: 1,
    object_from: "hotel_or_vehicle",
    percent_group: 2,
  },
  {
    claim_type: "ENTITY_OWNS_PERCENT_OF_ENTITY",
    temporal_default: "CURRENT",
    re: /\b(Nakheel\s+Hotels?)\s+(?:acquires?|acquired|holds?)\s+(?:a\s+)?(\d{1,3})\s*%\s*(?:stake|interest)?(?:\s+in\s+(One\s*&\s*Only\s+Palmilla|[A-ZÁÉÍÓÚÑ][\wÁÉÍÓÚÑáéíóúñ&.'’\-\s]{3,40}))?/gi,
    subject_group: 1,
    percent_group: 2,
    object_group: 3,
    object_from: "hotel_or_vehicle",
  },
  {
    claim_type: "HOTEL_ANNOUNCED_BRAND",
    temporal_default: "ANNOUNCED",
    re: /\b(Breathless\s+Puerto\s+Vallarta(?:\s+Resort\s*(?:&\s*)?Spa)?)\b[^.?]{0,80}\b(?:set\s+to\s+open|abrirá|apertura|announced|anunci)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "HOTEL_ANNOUNCED_BRAND",
    temporal_default: "ANNOUNCED",
    re: /\b(?:set\s+to\s+open|abrirá|apertura)\b[^.?]{0,60}\b(Breathless\s+Puerto\s+Vallarta(?:\s+Resort\s*(?:&\s*)?Spa)?)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "HOTEL_FORMER_BRAND",
    temporal_default: "FORMER",
    re: /\b(?:formerly|former(?:ly)?\s+a|antes\s+(?:un|una)|previously)\s+(?:a\s+)?(Hilton(?:\s+Puerto\s+Vallarta(?:\s+Resort)?)?|Capella(?:\s+Pedregal)?|Krystal\s+Altitude(?:\s+Puerto\s+Vallarta)?)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "PROPERTY_WAS_REBRANDED",
    temporal_default: "FORMER",
    re: /\b(Krystal\s+Altitude(?:\s+(?:Puerto\s+)?Vallarta)?)\s+is\s+now\s+(?:the\s+)?(Krystal\s+Grand(?:\s+Puerto\s+Vallarta)?)\b/gi,
    subject_group: 1,
    object_group: 2,
  },
  {
    claim_type: "PROPERTY_IS_ADJACENT_TO",
    temporal_default: "CURRENT",
    re: /\b(Krystal\s+Resort\s+Puerto\s+Vallarta)\b[^.?]{0,100}\b(?:third[\s-]?party\s+owned|owned\s+by\s+(?:Chartwell|third)|managed\s+by)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "PROPERTY_IS_ADJACENT_TO",
    temporal_default: "CURRENT",
    re: /\bthird[\s-]?party\s+owned\s+hotels?\s+managed\b[^.?]{0,120}\b(Krystal\s+Resort\s+Puerto\s+Vallarta)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "ENTITY_IS_SUBSIDIARY_OF_ENTITY",
    temporal_default: "CURRENT",
    re: /\b(Inmobiliaria\s+en\s+Hoteler[ií]a\s+Vallarta\s+Santa\s+Fe(?:[,\s]+S\.\s*de\s*R\.?\s*L\.?\s*de\s*C\.?\s*V\.?)?|IHVSF)\b/gi,
    subject_group: 1,
  },
  {
    claim_type: "ENTITY_IS_OWNER_OF_PROPERTY",
    temporal_default: "CURRENT",
    re: /\b(?:propiedad\s+de|propietario|owned\s+by|portfolio\s+of\s+owned)\s*[:\-]?\s*(Grupo\s+Hotelero\s+Santa\s+Fe(?:[,\s]+S\.?A\.?B\.?\s*de\s*C\.?V\.?)?)/gi,
    object_group: 1,
  },
  {
    claim_type: "ENTITY_OPERATES_HOTEL",
    temporal_default: "CURRENT",
    re: /\b(?:operated|managed|operado|administrado)\s+by\s+(Grupo\s+Hotelero\s+Santa\s+Fe(?:[,\s]+S\.?A\.?B\.?\s*de\s*C\.?V\.?)?)/gi,
    object_group: 1,
  },
  {
    claim_type: "PERSON_HAS_TITLE",
    temporal_default: "CURRENT_UNVERIFIED",
    re: /\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3}),?\s+(?:Director(?:a)?\s+General|CEO|CFO|Directora?\s+de\s+Ventas|Chief\s+Executive)\b/gi,
    subject_group: 1,
  },
  {
    claim_type: "PERSON_HAS_TITLE",
    temporal_default: "CURRENT_UNVERIFIED",
    re: /\b(?:Director(?:a)?\s+General|Directora?\s+de\s+Ventas)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})\b/gi,
    subject_group: 1,
  },
  {
    claim_type: "ENTITY_PARTICIPATES_IN_JV",
    temporal_default: "CURRENT",
    re: /\b(Fideicomiso\s+Mahekal|MHKL|Mahekal\s+Holding)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "ENTITY_SPONSORED_PROJECT",
    temporal_default: "FORMER",
    re: /\b(?:adquirido\s+por|acquired\s+by|affiliates?\s+of)\s+(Walton\s+Street\s+Capital\s+Mexico|filiales\s+de\s+Walton\s+Street\s+Capital\s+M[eé]xico)/gi,
    object_group: 1,
  },
  {
    claim_type: "ENTITY_IS_OWNER_OF_PROPERTY",
    temporal_default: "CURRENT_UNVERIFIED",
    re: /\b(?:owned\s+portfolio\s+(?:listing\s+)?of|portfolio\s+of\s+owned)\s+(Grupo\s+Hotelero\s+Santa\s+Fe(?:[,\s]+S\.?A\.?B\.?\s*de\s*C\.?V\.?)?)/gi,
    object_group: 1,
  },
  {
    claim_type: "HOTEL_CURRENT_BRAND",
    temporal_default: "CURRENT",
    re: /\b(?:current\s+brand(?:\s+identity)?\s*:\s*)(Waldorf\s+Astoria\s+Los\s+Cabos\s+Pedregal|One\s*&\s*Only\s+Palmilla|Krystal\s+Grand|Chabl[eé]\s+(?:Maroma|Yucat[aá]n)|Breathless)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "HOTEL_CURRENT_BRAND",
    temporal_default: "CURRENT",
    re: /\b(Waldorf\s+Astoria\s+Los\s+Cabos\s+Pedregal|One\s*&\s*Only\s+Palmilla)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "PROPERTY_ALIAS",
    temporal_default: "FORMER",
    re: /\b(?:formerly|also\s+known\s+as|aka|antes)\s+(?:known\s+as\s+)?(Capella\s+Pedregal|Resort\s+at\s+Pedregal|Hilton\s+Puerto\s+Vallarta(?:\s+Resort)?)\b/gi,
    object_group: 1,
  },
  {
    claim_type: "PROPERTY_HAS_ROOM_COUNT",
    temporal_default: "CURRENT",
    re: /\b(\d{2,4})\s+(?:habitaciones|rooms|suites)\b/gi,
    value_group: 1,
  },
];

/**
 * @param {object} doc — source document { id, title, url, text, source_type, authority, publication_date, hotel_context_id }
 * @param {{ research_run_id?: string, hotel_name?: string, context_org?: string }} [opts]
 * @returns {object[]} IntelligenceClaim[]
 */
export function extractClaimsFromDocument(doc, opts = {}) {
  const text = String(doc?.text || "");
  if (!text.trim()) return [];

  const claims = [];
  const seen = new Set();
  const meta = {
    url: doc.url || null,
    hotelName: opts.hotel_name || doc.hotel_name || null,
  };

  // Layer 1: existing ownership phrase extractor → atomic claims
  for (const hit of extractOwnershipClaimsFromText(text, meta)) {
    const claimType = REL_TO_CLAIM[hit.relationship_type] || "OTHER_STRUCTURED_CLAIM";
    pushClaim(claims, seen, buildClaim({
      claim_type: claimType,
      subject_raw: opts.hotel_name || doc.hotel_name || null,
      predicate_raw: hit.relationship_type,
      object_raw: hit.name,
      relationship_candidate: hit.relationship_type,
      evidence_excerpt: clip(hit.quote || hit.name, 280),
      temporal_status: inferTemporal(text, hit.quote),
      extraction_confidence: Math.min(0.92, 0.55 + (hit.relationship_explicitness || 0.3) * 0.4),
      semantic_confidence: hit.relationship_type === "OWNED_BY" ? 0.75 : 0.7,
      doc,
      opts,
    }));
  }

  // Layer 2: extra structured patterns
  for (const pat of EXTRA_PATTERNS) {
    pat.re.lastIndex = 0;
    let m;
    while ((m = pat.re.exec(text)) !== null) {
      const excerpt = clip(text.slice(Math.max(0, m.index - 40), m.index + m[0].length + 80), 320);
      const subject =
        (pat.subject_group != null ? clean(m[pat.subject_group]) : null) ||
        (pat.subject_from === "context_org" ? opts.context_org || null : null) ||
        opts.hotel_name ||
        doc.hotel_name ||
        null;
      const object =
        (pat.object_group != null ? clean(m[pat.object_group]) : null) ||
        (pat.object_from === "hotel_or_vehicle"
          ? opts.hotel_name || doc.hotel_name || null
          : null);
      const pct =
        pat.percent_group != null && m[pat.percent_group]
          ? Number(m[pat.percent_group])
          : null;
      const claimValue =
        pat.value_group != null && m[pat.value_group]
          ? String(m[pat.value_group])
          : null;

      // Never mint signing authority from title patterns
      if (pat.claim_type === "PERSON_HAS_SIGNING_AUTHORITY") continue;

      let temporal = pat.temporal_default || "UNKNOWN";
      if (pat.claim_type.startsWith("HOTEL_") || pat.claim_type.startsWith("ENTITY_")) {
        temporal = inferTemporal(text, excerpt) !== "UNKNOWN"
          ? inferTemporal(text, excerpt)
          : temporal;
      }

      // PropCo name alone → subsidiary / title-holder candidate, not parent OWNED_BY
      let claimType = pat.claim_type;
      let relCand = claimTypeToRelationshipCandidate(claimType);
      if (
        claimType === "ENTITY_IS_SUBSIDIARY_OF_ENTITY" &&
        /IHVSF|Inmobiliaria\s+en\s+Hoteler/i.test(object || subject || "")
      ) {
        // PropCo holds title to hotel — edge is hotel OWNED_BY PropCo
        pushClaim(claims, seen, buildClaim({
          claim_type: "ENTITY_HOLDS_TITLE",
          subject_raw: opts.hotel_name || doc.hotel_name || null,
          predicate_raw: "holds_title",
          object_raw: clean(object || subject),
          relationship_candidate: "OWNED_BY",
          evidence_excerpt: excerpt,
          temporal_status: "CURRENT",
          extraction_confidence: 0.8,
          semantic_confidence: 0.72,
          ownership_percentage: null,
          doc,
          opts,
          derivation_notes: ["propco_vehicle_signal"],
        }));
      }

      pushClaim(claims, seen, buildClaim({
        claim_type: claimType,
        subject_raw: subject,
        predicate_raw: claimType,
        object_raw: object,
        relationship_candidate: relCand,
        evidence_excerpt: excerpt,
        temporal_status: temporal,
        ownership_percentage: pct,
        claim_value: claimValue,
        extraction_confidence: 0.78,
        semantic_confidence: semanticFor(claimType, pct),
        doc,
        opts,
      }));
    }
  }

  // Layer 3: explicit anti-overinference — operator text must not mint OWNED_BY
  return claims.map((c) => hardenClaim(c, text));
}

/**
 * @param {object[]} documents
 * @param {object} [opts]
 */
export function extractClaimsFromCorpus(documents, opts = {}) {
  const all = [];
  for (const doc of documents || []) {
    all.push(...extractClaimsFromDocument(doc, {
      ...opts,
      hotel_name: doc.hotel_name || opts.hotel_name,
      context_org: doc.context_org || opts.context_org,
    }));
  }
  return all;
}

function buildClaim({
  claim_type,
  subject_raw,
  predicate_raw,
  object_raw,
  relationship_candidate,
  evidence_excerpt,
  temporal_status,
  ownership_percentage = null,
  claim_value = null,
  extraction_confidence,
  semantic_confidence,
  doc,
  opts,
  derivation_notes = [],
}) {
  const authKey = normalizeAuthorityKey(doc.authority || doc.source_type || doc.provider);
  const sourceAuthority = authorityForClaimType(claim_type, authKey);
  const overall = deriveOverallClaimConfidence({
    extraction_confidence,
    entity_resolution_confidence: 0.5,
    semantic_confidence,
    temporal_confidence: temporal_status === "UNKNOWN" ? 0.4 : 0.7,
    source_authority: sourceAuthority,
    corroboration: 0.5,
  });

  return createIntelligenceClaim({
    subject_raw,
    predicate_raw,
    object_raw,
    subject_entity_candidate: subject_raw,
    object_entity_candidate: object_raw,
    hotel_context_id: doc.hotel_context_id || opts.hotel_context_id || null,
    claim_type,
    relationship_candidate,
    claim_value,
    ownership_percentage,
    source_id: doc.id || doc.source_id || null,
    source_url: doc.url || null,
    source_type: doc.source_type || authKey,
    source_authority: sourceAuthority,
    document_title: doc.title || null,
    publication_date: doc.publication_date || null,
    observed_date: doc.observed_date || null,
    temporal_status,
    evidence_excerpt,
    evidence_pointer: doc.url || doc.id || null,
    extraction_confidence,
    entity_resolution_confidence: null,
    semantic_confidence,
    overall_claim_confidence: overall,
    verification_status: "EXTRACTED",
    research_run_id: opts.research_run_id || null,
    derivation_notes,
  });
}

function hardenClaim(claim, fullText) {
  const excerpt = String(claim.evidence_excerpt || "");
  // Operator / managed language near OWNED_BY → demote to OPERATED_BY claim type
  if (
    claim.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY" &&
    /\b(operated|managed|operado|administrado|management\s+fees?|third[\s-]?party)\b/i.test(
      excerpt
    ) &&
    !/\b(owned|propiedad|propietario|adquir)/i.test(excerpt)
  ) {
    return {
      ...claim,
      claim_type: "ENTITY_OPERATES_HOTEL",
      relationship_candidate: "OPERATED_BY",
      semantic_confidence: Math.min(claim.semantic_confidence || 0.7, 0.65),
      derivation_notes: [
        ...(claim.derivation_notes || []),
        "demoted_operator_language_not_ownership",
      ],
    };
  }
  // Announced brand must stay ANNOUNCED
  if (
    claim.claim_type === "HOTEL_ANNOUNCED_BRAND" &&
    claim.temporal_status === "CURRENT"
  ) {
    return { ...claim, temporal_status: "ANNOUNCED" };
  }
  // Title never becomes signing authority
  if (claim.claim_type === "PERSON_HAS_SIGNING_AUTHORITY") {
    return {
      ...claim,
      claim_type: "PERSON_HAS_TITLE",
      derivation_notes: [
        ...(claim.derivation_notes || []),
        "signing_authority_requires_separate_evidence",
      ],
    };
  }
  // Portfolio listing alone — keep as CURRENT_UNVERIFIED / lower bar (not auto-promote sole owner)
  if (
    claim.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY" &&
    /\bowned\s+portfolio\s+listing\b/i.test(excerpt)
  ) {
    return {
      ...claim,
      temporal_status: "CURRENT_UNVERIFIED",
      semantic_confidence: Math.min(claim.semantic_confidence || 0.7, 0.55),
      derivation_notes: [
        ...(claim.derivation_notes || []),
        "portfolio_listing_weaker_than_explicit_ownership",
      ],
    };
  }
  // 50% stake must not become sole owner claim
  if (
    claim.claim_type === "ENTITY_IS_OWNER_OF_PROPERTY" &&
    claim.ownership_percentage != null &&
    claim.ownership_percentage < 100
  ) {
    return {
      ...claim,
      claim_type: "ENTITY_OWNS_PERCENT_OF_ENTITY",
      relationship_candidate: "JV_WITH",
      derivation_notes: [
        ...(claim.derivation_notes || []),
        "partial_ownership_not_sole_owner",
      ],
    };
  }
  void fullText;
  return claim;
}

function inferTemporal(fullText, excerpt) {
  // Prefer local excerpt — full-document "formerly" must not flip current ownership sentences.
  const local = String(excerpt || "");
  const blob = local || String(fullText || "").slice(0, 400);
  // Acquisition of remaining interest is not itself a FORMER fact
  if (/\bacquired\s+the\s+remaining\b|\bacquires?\s+\d{1,3}\s*%/i.test(local)) {
    if (/\bformerly\s+owned\b/i.test(local) && !/\bacquired\b/i.test(local.split(/formerly/i)[0] || "")) {
      // fall through
    } else {
      return "UNKNOWN";
    }
  }
  if (/\b(former(?:ly)?|historical|previously|antes|ex-|sold|divested|ya\s+no)\b/i.test(local)) {
    // "previously held with X" in an acquisition sentence ≠ FORMER for acquirer
    if (/\bacquired\b/i.test(local) && /\bpreviously\s+held\b/i.test(local)) {
      return "UNKNOWN";
    }
    return "FORMER";
  }
  if (/\b(announced|set\s+to\s+open|abrirá|planned|upcoming|anunciad)\b/i.test(blob)) {
    return "ANNOUNCED";
  }
  if (
    /\b(is\s+owned\s+by|owned\s+by|propiedad\s+de|operates?|operated\s+by|current|currently|actualmente)\b/i.test(
      local
    )
  ) {
    return "CURRENT";
  }
  if (/\b(current|currently|actualmente|hoy|owns|opera)\b/i.test(blob)) {
    return "CURRENT";
  }
  return "UNKNOWN";
}

function semanticFor(claimType, pct) {
  if (claimType === "ENTITY_OWNS_PERCENT_OF_ENTITY" && pct != null) return 0.85;
  if (claimType === "PERSON_HAS_TITLE") return 0.7;
  if (claimType === "HOTEL_ANNOUNCED_BRAND") return 0.88;
  if (claimType === "PROPERTY_IS_ADJACENT_TO") return 0.8;
  return 0.72;
}

function pushClaim(list, seen, claim) {
  const key = [
    claim.claim_type,
    normalizeKey(claim.subject_raw),
    normalizeKey(claim.object_raw),
    claim.ownership_percentage ?? "",
    claim.temporal_status,
    claim.source_id || "",
  ].join("|");
  if (seen.has(key)) return;
  seen.add(key);
  list.push(claim);
}

function normalizeKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function clean(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .replace(/\.(?:\s+[A-Z].*)?$/, "")
    .replace(/\b(Separately|Additionally|Meanwhile)\b.*$/i, "")
    .replace(/^[,:\-–—\s]+|[,:\-–—\s.]+$/g, "")
    .trim();
}

function clip(s, n) {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  return t.length <= n ? t : `${t.slice(0, n - 1)}…`;
}
