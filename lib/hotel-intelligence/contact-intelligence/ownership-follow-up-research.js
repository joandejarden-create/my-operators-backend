/**
 * Bounded ownership follow-up path — merges deterministic + structured-model
 * candidates, repairs interpretation failures, and plans question-dependent
 * next searches inside the existing Context.dev / ownership handoff stack.
 *
 * Combining candidates ≠ accepting ownership. Never authorizes enrichment,
 * publication, or canonical writes. Discovery staging only.
 */
import { createHash } from "node:crypto";
import { evidenceAppearsInSource } from "../../partner-intelligence/merge-extraction-candidates.js";
import {
  classifyOwnershipLead,
  hotelCoreName,
  mentions,
  mentionsHotel,
  ownershipPassageSupport,
} from "./research-evidence.js";
import {
  ownershipCandidates,
  ownershipDocumentPassages,
} from "./ownership-research-planning.js";
import {
  admitOwnershipClaim,
  validateOwnershipDocumentClaims,
  proposeFollowUpQueries,
} from "./ownership-document-structured-extract.js";

export const OWNERSHIP_FOLLOW_UP_VERSION = "ownership-follow-up-research-v1";

function nz(v) {
  return String(v == null ? "" : v).trim();
}

function findSpanOffsets(sourceText, span) {
  const raw = String(sourceText || "");
  const s = nz(span);
  if (!s) return { found: false, start: -1, end: -1 };
  const start = raw.indexOf(s);
  if (start >= 0) return { found: true, start, end: start + s.length };
  return { found: false, start: -1, end: -1 };
}

function normKey(s) {
  return nz(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/** Phrases that are not entity names (observed: subject="Still family-owned"). */
const NON_ENTITY_SUBJECT_RE =
  /^(?:still\s+)?family[- ]owned$|^family[- ]owned(?:\s+on\s+.+)?$|^unnamed(?:\s+family)?$|^unknown$/i;

/**
 * Validate acquisition direction from evidence span.
 * Returns { buyer, acquired, direction: BUYER_ACQUIRED_TARGET | TARGET_ACQUIRED_BY_BUYER | AMBIGUOUS }
 */
/**
 * Repair inverted OWNS / OWNED_BY fields when evidence is passive
 * ("hotel owned by Person" mis-modeled as subject=hotel, relationship=OWNS, object=Person).
 * Does not invent parties; only swaps when the span clearly uses owned-by/passive voice.
 */
export function resolveOwnsDirection(evidenceSpan, subject, object, hotelName = "") {
  const spanRaw = nz(evidenceSpan);
  const sub = nz(subject);
  const obj = nz(object);
  const core = hotelCoreName(hotelName);

  const ownedBy =
    /\b(?:owned|operated)\s+by\s+(?:one\s+|the\s+)?((?:[A-Z][\w'’.\-]+(?:\s+(?:&|and|[A-Z][\w'’.\-]+)){0,4}))\b/i.exec(
      spanRaw
    ) ||
    /\bpropiedad\s+de\s+((?:[A-ZÁÉÍÓÚÑ][\w'’.\-]+(?:\s+(?:&|y|and|[A-ZÁÉÍÓÚÑ][\w'’.\-]+)){0,4}))\b/i.exec(
      spanRaw
    );

  if (!ownedBy) {
    return {
      owner: null,
      asset: null,
      direction: "AMBIGUOUS",
      fields_inverted_vs_evidence: false,
      corrected_subject: sub || null,
      corrected_object: obj || null,
      explanation: "No passive owned-by pattern to repair.",
    };
  }

  const owner = nz(ownedBy[1]).replace(/,$/, "");
  const hotelInSpan = mentionsHotel(spanRaw, hotelName) || (core && mentions(spanRaw, core));
  const subLooksLikeHotel =
    (core && mentions(sub, core)) ||
    mentionsHotel(sub, hotelName) ||
    /\bhotel\b|\bresort\b|\bvillas?\b/i.test(sub);
  const objLooksLikePerson =
    Boolean(obj) &&
    !/\b(hotel|resort|company|inc|llc|ltd|corp|group|holdings)\b/i.test(obj) &&
    /^[A-Z]/.test(obj);

  // Inverted: subject is hotel/asset, object is the person named after "owned by"
  if (
    owner &&
    subLooksLikeHotel &&
    (normKey(obj) === normKey(owner) || mentions(owner, obj) || mentions(obj, owner) || objLooksLikePerson)
  ) {
    return {
      owner,
      asset: core || hotelName || sub,
      direction: "PERSON_OWNS_HOTEL",
      fields_inverted_vs_evidence: true,
      corrected_subject: owner,
      corrected_object: core || hotelName || sub,
      explanation: `Evidence is passive («owned by ${owner}»); subject/object were inverted.`,
    };
  }

  // Already correct: subject is owner named in owned-by
  if (owner && (normKey(sub) === normKey(owner) || mentions(owner, sub))) {
    return {
      owner,
      asset: hotelInSpan ? core || hotelName : obj || null,
      direction: "PERSON_OWNS_HOTEL",
      fields_inverted_vs_evidence: false,
      corrected_subject: sub || owner,
      corrected_object: obj || core || hotelName || null,
      explanation: `Evidence supports ${owner} as owner of ${core || hotelName || "property"}.`,
    };
  }

  return {
    owner,
    asset: hotelInSpan ? core || hotelName : null,
    direction: owner ? "PERSON_OWNS_HOTEL" : "AMBIGUOUS",
    fields_inverted_vs_evidence: false,
    corrected_subject: sub || owner || null,
    corrected_object: obj || core || null,
    explanation: "Owned-by pattern present; field alignment uncertain.",
  };
}

/**
 * Publication / source_date is not an ownership "as-of" date.
 * Demote false CURRENT_AS_OF_STATED_DATE when only an old source_date exists
 * and the span lacks present-tense ownership language.
 * Past acquisitions (ACQUIRED + old event_date) never stay CURRENT without a present cue.
 */
export function repairOwnershipCurrentness(claim = {}) {
  const cur = nz(claim.currentness).toUpperCase();
  const sourceDate = nz(claim.source_date);
  const eventDate = nz(claim.event_date);
  const span = nz(claim.evidence_span || claim.excerpt);
  const rel = nz(claim.relationship).toUpperCase();

  const presentCue = /\b(?:currently|still owned|today|as of\s+20(?:1[8-9]|2[0-9])|remains?\s+(?:the\s+)?owner|present(?:ly)?\s+own)/i.test(
    span
  );
  const yearFrom = (s) => {
    const m = nz(s).match(/\b(19\d{2}|20\d{2})\b/);
    return m ? Number(m[1]) : null;
  };
  const sourceYear = yearFrom(sourceDate);
  const eventYear = yearFrom(eventDate);
  const staleSource = sourceYear != null && sourceYear < 2018;
  const staleEvent = eventYear != null && eventYear < 2018;
  const hasEvidencedDate = Boolean(eventDate || sourceDate);

  // CURRENT_AS_OF_STATED_DATE requires an evidenced date — null dates cannot satisfy the label.
  if (cur === "CURRENT_AS_OF_STATED_DATE" && !hasEvidencedDate) {
    return {
      ...claim,
      currentness: "UNRESOLVED",
      currentness_repaired: "CURRENT_REQUIRES_EVIDENCED_DATE",
      historical_vs_current: "CURRENTNESS_UNRESOLVED_NO_STATED_DATE",
    };
  }

  // Historical acquisition language must not remain CURRENT without present-tense ownership cue.
  if (
    rel === "ACQUIRED" &&
    (cur === "CURRENT_AS_OF_STATED_DATE" || cur === "CURRENT" || !cur) &&
    !presentCue &&
    (staleEvent || staleSource || /acquir|purchas|bought/i.test(span))
  ) {
    return {
      ...claim,
      currentness: "HISTORICAL",
      currentness_repaired: "ACQUISITION_IS_NOT_CURRENT_OWNERSHIP",
      historical_vs_current: "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK",
    };
  }

  if (cur === "CURRENT_AS_OF_STATED_DATE" && !eventDate && staleSource && !presentCue) {
    return {
      ...claim,
      currentness: "HISTORICAL",
      currentness_repaired: "SOURCE_DATE_IS_PUBLICATION_NOT_OWNERSHIP_AS_OF",
      historical_vs_current: "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK",
    };
  }

  if (
    (rel === "OWNS" || rel === "OWNED_BY") &&
    /owned\s+by/i.test(span) &&
    !presentCue &&
    (staleSource || /original owners?|earliest|founded|erected|historic/i.test(span))
  ) {
    if (cur === "CURRENT_AS_OF_STATED_DATE" || cur === "CURRENT" || !cur) {
      return {
        ...claim,
        currentness: "HISTORICAL",
        currentness_repaired: "PASSIVE_OWNED_BY_WITHOUT_PRESENT_CUE",
        historical_vs_current: "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK",
      };
    }
  }

  return claim;
}

export function resolveAcquisitionDirection(evidenceSpan, subject, object, hotelName = "") {
  const spanRaw = nz(evidenceSpan);
  const span = spanRaw.replace(/\b(?:by the time|when|after|before|once)\s+/gi, "");
  const sub = nz(subject);
  const obj = nz(object);
  const core = hotelCoreName(hotelName);

  const buyerAcquired =
    /\b((?:[A-Z][\w&.\-]+(?:\s+(?:&|and|[A-Z][\w&.\-]+)){0,5}))\s+(?:has\s+|have\s+|today\s+)?(?:acquired|purchased|bought)\b/.exec(
      span
    );
  const acquiredBy =
    /\b(?:acquired|purchased|bought)\s+by\s+((?:[A-Z][\w&.\-]+(?:\s+(?:&|and|[A-Z][\w&.\-]+)){0,5}))\b/.exec(span) ||
    /\bacquisition\s+by\s+((?:[A-Z][\w&.\-]+(?:\s+(?:&|and|[A-Z][\w&.\-]+)){0,5}))\b/.exec(span);
  const dealToAcquire =
    /\b((?:[A-Z][\w&.\-]+(?:\s+(?:&|and|[A-Z][\w&.\-]+)){0,4}))\s+(?:announced it had\s+)?(?:reached a deal to|agreed to)\s+acquire\s+((?:[A-Z][\w&.\-]+(?:\s+(?:&|and|[A-Z][\w&.\-]+)){0,5}))\b/.exec(
      span
    );

  function cleanParty(name) {
    return nz(name).replace(/,$/, "").trim();
  }

  let buyer = null;
  let acquired = null;
  let direction = "AMBIGUOUS";

  if (dealToAcquire) {
    buyer = cleanParty(dealToAcquire[1]);
    acquired = cleanParty(dealToAcquire[2]);
    direction = "BUYER_ACQUIRED_TARGET";
  } else if (acquiredBy) {
    buyer = cleanParty(acquiredBy[1]);
    if (mentions(spanRaw, obj) || mentionsHotel(spanRaw, hotelName) || mentions(spanRaw, core)) {
      acquired = mentionsHotel(spanRaw, hotelName) || mentions(spanRaw, core) ? core || hotelName : obj;
    } else {
      acquired = obj || null;
    }
    direction = buyer ? "BUYER_ACQUIRED_TARGET" : "AMBIGUOUS";
  } else if (buyerAcquired) {
    buyer = cleanParty(buyerAcquired[1]);
    if (/\b(?:the\s+)?(?:property|hotel|resort)\b/i.test(spanRaw)) {
      acquired = mentions(spanRaw, obj) ? obj : core || obj || "property";
    } else if (mentions(spanRaw, obj)) {
      acquired = obj;
    }
    direction = buyer ? "BUYER_ACQUIRED_TARGET" : "AMBIGUOUS";
  }

  // Detect inverted subject/object vs evidence (LVMH acquires Belmond but fields flipped)
  if (buyer && acquired && sub && obj) {
    const subIsAcquired = normKey(sub) === normKey(acquired) || mentions(acquired, sub);
    const objIsBuyer = normKey(obj) === normKey(buyer) || mentions(buyer, obj);
    if (subIsAcquired && objIsBuyer) {
      return {
        buyer,
        acquired,
        direction: "BUYER_ACQUIRED_TARGET",
        fields_inverted_vs_evidence: true,
        corrected_subject: buyer,
        corrected_object: acquired,
        explanation: `Evidence shows ${buyer} acquiring ${acquired}; model/fields had subject/object inverted.`,
      };
    }
  }

  if (!buyer && !acquired) {
    return {
      buyer: null,
      acquired: null,
      direction: "AMBIGUOUS",
      fields_inverted_vs_evidence: false,
      corrected_subject: sub || null,
      corrected_object: obj || null,
      explanation: "Evidence does not resolve acquisition direction.",
    };
  }

  return {
    buyer,
    acquired,
    direction,
    fields_inverted_vs_evidence: false,
    corrected_subject: buyer || sub || null,
    corrected_object: acquired || obj || null,
    explanation:
      direction === "AMBIGUOUS"
        ? "Partial acquisition language; direction not fully resolved."
        : `${buyer || "?"} acquired ${acquired || "?"}.`,
  };
}

/**
 * Normalize unnamed-family representation: empty named parties; never an entity
 * named "Still family-owned".
 */
export function normalizeUnnamedFamilyClaim(claim = {}) {
  const rel = nz(claim.relationship) || nz(claim.claim_type);
  const subject = nz(claim.subject || claim.name);
  const isFamily =
    rel === "FAMILY_OWNS_UNNAMED" ||
    rel === "UNNAMED_FAMILY" ||
    (claim.classification === "FAMILY_OWNED_UNNAMED_LEAD");

  if (!isFamily && !NON_ENTITY_SUBJECT_RE.test(subject)) {
    return { ...claim, normalized: false };
  }

  const badSubject = NON_ENTITY_SUBJECT_RE.test(subject) || !subject;
  return {
    ...claim,
    relationship: "FAMILY_OWNS_UNNAMED",
    subject: "",
    name: null,
    named_parties: [],
    object: nz(claim.object) || "",
    normalized: true,
    cleared_non_entity_subject: badSubject ? subject || true : false,
    classification: claim.classification || "FAMILY_OWNED_UNNAMED_LEAD",
  };
}

/**
 * Contextual hotel linking: separate exact spans for hotel identity vs relationship.
 * Rejects when multiple distinct property names compete without a clear link span.
 */
export function resolveContextualHotelLink({
  sourceText,
  hotelName,
  relationshipSpan,
  hotelIdentitySpan = null,
  otherPropertyNames = [],
} = {}) {
  const text = String(sourceText || "");
  const rel = nz(relationshipSpan);
  const idSpan = nz(hotelIdentitySpan);
  const core = hotelCoreName(hotelName);

  const relOk = rel && evidenceAppearsInSource(rel, text);
  const idOk = !idSpan || evidenceAppearsInSource(idSpan, text);
  if (!relOk) {
    return {
      status: "REJECTED",
      establishes_relationship_to_target: false,
      explanation: "Relationship span missing or not exact in source.",
      hotel_identity_span: idSpan || null,
      relationship_span: rel || null,
    };
  }

  const hotelInRel = mentionsHotel(rel, hotelName);
  const hotelInId = idSpan ? mentionsHotel(idSpan, hotelName) || mentions(idSpan, core) : false;
  const hotelLinked = hotelInRel || hotelInId;

  const competitors = (otherPropertyNames || [])
    .map(nz)
    .filter(Boolean)
    .filter((n) => normKey(n) !== normKey(hotelName) && normKey(n) !== normKey(core));
  const competingInRel = competitors.filter((n) => mentions(rel, n));
  const competingInId = idSpan ? competitors.filter((n) => mentions(idSpan, n)) : [];
  if (competingInRel.length + competingInId.length > 0 && !hotelInRel) {
    return {
      status: "AMBIGUOUS_MULTI_PROPERTY",
      establishes_relationship_to_target: false,
      explanation: `Multiple properties mentioned without clear exclusive link to ${core || hotelName}: ${[...competingInRel, ...competingInId].join(", ")}`,
      hotel_identity_span: idSpan || null,
      relationship_span: rel,
      competing_properties: [...new Set([...competingInRel, ...competingInId])],
    };
  }

  if (!hotelLinked) {
    return {
      status: "NONE",
      establishes_relationship_to_target: false,
      explanation: "No exact hotel-identity span binds this relationship to the target hotel.",
      hotel_identity_span: idSpan || null,
      relationship_span: rel,
    };
  }

  if (hotelInRel) {
    return {
      status: "EXPLICIT_IN_RELATIONSHIP_SPAN",
      establishes_relationship_to_target: true,
      explanation: "Hotel (or core name) appears in the relationship evidence span.",
      hotel_identity_span: null,
      relationship_span: rel,
      offsets: findSpanOffsets(text, rel),
    };
  }

  // Contextual: hotel named in a separate exact identity span
  return {
    status: "CONTEXTUAL_TWO_SPAN",
    establishes_relationship_to_target: true,
    explanation: `Hotel identified in separate exact span; relationship expressed in another exact span. Identity: «${idSpan.slice(0, 80)}…»; relationship: «${rel.slice(0, 80)}…».`,
    hotel_identity_span: idSpan,
    relationship_span: rel,
    hotel_identity_offsets: findSpanOffsets(text, idSpan),
    relationship_offsets: findSpanOffsets(text, rel),
  };
}

function deterministicToUnified(candidate, hotelName, sourceMeta = {}) {
  const family = candidate.classification === "FAMILY_OWNED_UNNAMED_LEAD";
  const norm = family
    ? normalizeUnnamedFamilyClaim(candidate)
    : candidate;
  const excerpt = nz(norm.excerpt || candidate.excerpt);
  const name = family ? null : nz(norm.name || candidate.name) || null;
  let direction = null;
  if (name && /acquir|purchas|bought/i.test(excerpt)) {
    direction = resolveAcquisitionDirection(excerpt, name, hotelCoreName(hotelName) || hotelName, hotelName);
  }
  return {
    method: "DETERMINISTIC",
    provenance: {
      method: "DETERMINISTIC",
      source_url: sourceMeta.source_url || null,
      doc_id: sourceMeta.doc_id || null,
    },
    subject: family ? "" : name,
    relationship: family
      ? "FAMILY_OWNS_UNNAMED"
      : candidate.lead_reasons?.includes("TRANSACTION_ACQUISITION_LANGUAGE")
        ? "ACQUIRED"
        : "OTHER",
    object: hotelCoreName(hotelName) || hotelName,
    scope: family ? "HOTEL_ASSET" : "HOTEL_ASSET",
    named_parties: family ? [] : name ? [name] : [],
    event_date: null,
    source_date: null,
    currentness: "UNRESOLVED",
    evidence_span: excerpt,
    classification: candidate.classification,
    supported_transaction_claim: Boolean(candidate.supported_transaction_claim),
    evidence_grounded: Boolean(excerpt),
    acquisition_direction: direction,
    target_hotel_link: {
      status: mentionsHotel(excerpt, hotelName) ? "EXPLICIT_IN_RELATIONSHIP_SPAN" : "NONE",
      establishes_relationship_to_target_validated: mentionsHotel(excerpt, hotelName),
      hotel_identity_span: null,
      relationship_span: excerpt,
    },
    phrase_rule_flags: candidate.lead_reasons || [],
    accepted_as_ownership: false,
    enrichment_authorized: false,
    publication_authorized: false,
  };
}

function modelToUnified(claim, hotelName, sourceMeta = {}) {
  let c = { ...claim };
  if (
    c.relationship === "FAMILY_OWNS_UNNAMED" ||
    c.relationship === "UNNAMED_FAMILY" ||
    NON_ENTITY_SUBJECT_RE.test(nz(c.subject))
  ) {
    c = normalizeUnnamedFamilyClaim(c);
  }

  let direction = null;
  if (c.relationship === "ACQUIRED" || c.relationship === "PARENT_ACQUIRED") {
    direction = resolveAcquisitionDirection(c.evidence_span, c.subject, c.object, hotelName);
    if (direction.fields_inverted_vs_evidence) {
      c = {
        ...c,
        subject: direction.corrected_subject,
        object: direction.corrected_object,
        direction_repaired: true,
      };
    }
  } else if (c.relationship === "OWNS" || c.relationship === "OWNED_BY") {
    direction = resolveOwnsDirection(c.evidence_span, c.subject, c.object, hotelName);
    if (direction.fields_inverted_vs_evidence) {
      c = {
        ...c,
        subject: direction.corrected_subject,
        object: direction.corrected_object,
        relationship: "OWNS",
        direction_repaired: true,
        named_parties: direction.corrected_subject ? [direction.corrected_subject] : c.named_parties,
      };
    }
  }

  c = repairOwnershipCurrentness(c);

  const link = c.target_hotel_link || {};
  return {
    method: "STRUCTURED_MODEL",
    provenance: {
      method: "STRUCTURED_MODEL",
      source_url: sourceMeta.source_url || null,
      doc_id: sourceMeta.doc_id || null,
    },
    subject: nz(c.subject),
    relationship: nz(c.relationship),
    object: nz(c.object),
    scope: nz(c.scope) || "HOTEL_ASSET",
    named_parties: Array.isArray(c.named_parties)
      ? c.named_parties
      : c.subject
        ? [c.subject]
        : [],
    event_date: c.event_date ?? null,
    source_date: c.source_date ?? null,
    currentness: nz(c.currentness) || "UNRESOLVED",
    currentness_repaired: c.currentness_repaired || null,
    historical_vs_current: c.historical_vs_current || null,
    evidence_span: nz(c.evidence_span),
    // Preserve quotation grounding from validateOwnershipDocumentClaims — required for role mapping / follow-ups
    evidence_grounded: Boolean(c.evidence_grounded),
    classification: c.relationship === "FAMILY_OWNS_UNNAMED" ? "FAMILY_OWNED_UNNAMED_LEAD" : "OWNER_CANDIDATE",
    supported_transaction_claim: Boolean(c.deterministic_transaction_support || c.supported_transaction_claim),
    acquisition_direction: direction,
    owns_direction: c.relationship === "OWNS" || c.relationship === "OWNED_BY" ? direction : null,
    direction_repaired: Boolean(c.direction_repaired),
    target_hotel_link: {
      status: link.status || "NONE",
      establishes_relationship_to_target_validated: Boolean(
        link.establishes_relationship_to_target_validated
      ),
      hotel_identity_span: link.hotel_identity_span || link.supporting_passage || null,
      relationship_span: nz(c.evidence_span),
      model_flag: Boolean(link.establishes_relationship_to_target),
    },
    phrase_rule_flags: c.phrase_rule_flags || [],
    admission_reasons: c.admission_reasons || [],
    retained_reason: sourceMeta.retained_reason || null,
    accepted_as_ownership: false,
    enrichment_authorized: false,
    publication_authorized: false,
  };
}

function claimDedupeKey(c) {
  return [
    normKey(c.subject),
    normKey(c.relationship),
    normKey(c.object),
    normKey(c.scope),
    normKey((c.evidence_span || "").slice(0, 80)),
  ].join("|");
}

/**
 * Merge deterministic + model claims without silently dropping either arm.
 * Model omission cannot remove a deterministic AJ Capital-style acquisition lead.
 */
export function mergeOwnershipClaimCandidates({
  hotelName,
  deterministicCandidates = [],
  modelGrounded = [],
  modelRetained = [],
  sourceMeta = {},
} = {}) {
  const unified = [];
  for (const d of deterministicCandidates) {
    if (d.classification === "REJECTED_NEGATIVE_CONTROL") {
      unified.push({
        ...deterministicToUnified(d, hotelName, sourceMeta),
        classification: "REJECTED_NEGATIVE_CONTROL",
        keep_visible: true,
      });
      continue;
    }
    unified.push(deterministicToUnified(d, hotelName, sourceMeta));
  }
  for (const m of modelGrounded) {
    unified.push(modelToUnified(m, hotelName, sourceMeta));
  }
  for (const r of modelRetained) {
    const claim = r.claim || r;
    unified.push(
      modelToUnified(claim, hotelName, {
        ...sourceMeta,
        retained_reason: r.reason || "RETAINED_FOR_REVIEW",
      })
    );
  }

  // Deduplicate equivalents but keep multi-method provenance
  const byKey = new Map();
  const conflicts = [];
  for (const c of unified) {
    const key = claimDedupeKey(c);
    if (!byKey.has(key)) {
      byKey.set(key, { ...c, methods: [c.method], provenance_list: [c.provenance] });
      continue;
    }
    const prev = byKey.get(key);
    if (!prev.methods.includes(c.method)) {
      prev.methods.push(c.method);
      prev.provenance_list.push(c.provenance);
    }
    // Conflict: same parties but disagree on currentness / direction / hotel link
    if (
      prev.currentness !== c.currentness ||
      prev.acquisition_direction?.direction !== c.acquisition_direction?.direction ||
      prev.target_hotel_link?.establishes_relationship_to_target_validated !==
        c.target_hotel_link?.establishes_relationship_to_target_validated
    ) {
      conflicts.push({
        key,
        a: {
          method: prev.method,
          currentness: prev.currentness,
          direction: prev.acquisition_direction?.direction,
          hotel_link: prev.target_hotel_link?.establishes_relationship_to_target_validated,
        },
        b: {
          method: c.method,
          currentness: c.currentness,
          direction: c.acquisition_direction?.direction,
          hotel_link: c.target_hotel_link?.establishes_relationship_to_target_validated,
        },
      });
      prev.conflicts_with = prev.conflicts_with || [];
      prev.conflicts_with.push(c.method);
    }
  }

  const merged = [...byKey.values()];
  // Explicit: deterministic acquisition leads always survive even if model omitted them
  const deterministicAcquisitions = unified.filter(
    (c) =>
      c.method === "DETERMINISTIC" &&
      c.subject &&
      (c.relationship === "ACQUIRED" || c.supported_transaction_claim)
  );
  for (const d of deterministicAcquisitions) {
    const present = merged.some(
      (m) => normKey(m.subject) === normKey(d.subject) && m.relationship === "ACQUIRED"
    );
    if (!present) {
      merged.push({
        ...d,
        methods: ["DETERMINISTIC"],
        preserved_despite_model_omission: true,
        note: "Deterministic acquisition lead retained after model omission.",
      });
    }
  }

  return {
    version: OWNERSHIP_FOLLOW_UP_VERSION,
    hotel_name: hotelName,
    hotel_core: hotelCoreName(hotelName),
    candidates: merged,
    conflicts,
    accepted_as_ownership_count: 0,
    enrichment_authorized: false,
    publication_authorized: false,
  };
}

/**
 * Build document-level merge from saved text + optional saved model output.
 */
export function combineSavedDocumentClaims({
  sourceText,
  hotelName,
  modelParsed = null,
  sourceMeta = {},
} = {}) {
  const document = ownershipDocumentPassages(sourceText, { hotel_name: hotelName });
  const deterministic = ownershipCandidates(document, { hotel_name: hotelName });
  let grounded = [];
  let retained = [];
  if (modelParsed) {
    const v = validateOwnershipDocumentClaims(modelParsed, sourceText, hotelName);
    grounded = v.grounded_claims;
    retained = v.retained_for_review;
  }
  const merged = mergeOwnershipClaimCandidates({
    hotelName,
    deterministicCandidates: deterministic,
    modelGrounded: grounded,
    modelRetained: retained,
    sourceMeta,
  });
  return {
    ...merged,
    document_sha256: document.sha256,
    deterministic_count: deterministic.length,
    model_grounded_count: grounded.length,
    model_retained_count: retained.length,
  };
}

/**
 * Question-dependent follow-up queries — not broad "who owns this hotel"
 * when a specific transaction/entity lead exists.
 */
export function buildQuestionDependentFollowUpPlan(merged, hotel = {}) {
  const hotelName = hotel.hotel_name || merged.hotel_name || "";
  const core = hotelCoreName(hotelName);
  const plans = [];

  for (const c of merged.candidates || []) {
    if (c.classification === "REJECTED_NEGATIVE_CONTROL") continue;
    const subject = nz(c.subject);
    const dir = c.acquisition_direction;

    if (c.relationship === "ACQUIRED" && subject && c.currentness !== "CURRENT_AS_OF_STATED_DATE") {
      const buyer = dir?.corrected_subject || dir?.buyer || subject;
      plans.push({
        unresolved_question: "Has the historical acquirer subsequently sold this hotel, or is current ownership still supported as of a stated date?",
        hotel_name: hotelName,
        lead: { subject: buyer, relationship: "ACQUIRED", event_date: c.event_date, evidence_span: c.evidence_span },
        priority_query_types: ["transaction_announcement", "owner_disclosure", "filing", "property_history"],
        proposed_queries: [
          `"${core}" (sold OR sale OR "acquired by" OR divested OR disposed) ${buyer}`,
          `"${core}" "${buyer}" (owner OR ownership OR "real estate" OR filing OR prospectus)`,
          `"${buyer}" "${core}" (2018 OR 2019 OR 2020 OR 2021 OR 2022 OR 2023 OR 2024 OR 2025 OR 2026)`,
        ],
        avoid_queries: [`who owns ${core}`, `who owns "${hotelName}"`],
        stop_conditions: [
          "Evidence answers subsequent sale or states current ownership as of a date",
          "Concrete access block",
          "Case budget exhausted",
        ],
        absence_of_later_sale_is_not_proof_of_current_ownership: true,
        distinguish: {
          historical_owner: buyer,
          current_owner_requires: "positive evidence as of a stated date",
        },
        enrichment_authorized: false,
        publication_authorized: false,
      });
    }

    // Historical / undated person-owner lead (e.g. "owned by Jeremiah Gumbs" in a 1996 article).
    // Keep as research lead — do not drop because it is ineligible for current-owner enrichment.
    const ownsDir = c.owns_direction || c.acquisition_direction;
    const ownerName =
      ownsDir?.corrected_subject ||
      (c.direction_repaired ? subject : null) ||
      (c.party_role === "HISTORICAL_OWNER" ? subject : null) ||
      subject;
    if (
      (c.relationship === "OWNS" || c.relationship === "OWNED_BY") &&
      ownerName &&
      c.currentness !== "CURRENT_AS_OF_STATED_DATE" &&
      c.classification !== "REJECTED_NEGATIVE_CONTROL"
    ) {
      plans.push({
        unresolved_question: `Is ${ownerName} still the owner of this hotel as of a stated recent date, or has ownership transferred?`,
        hotel_name: hotelName,
        lead: {
          subject: ownerName,
          relationship: "OWNS",
          currentness: c.currentness,
          source_date: c.source_date,
          evidence_span: c.evidence_span,
          staging_only: true,
        },
        priority_query_types: ["owner_disclosure", "property_history", "transaction_announcement"],
        proposed_queries: [
          `"${core}" (owner OR "owned by" OR proprietor OR "family-owned") (2020 OR 2021 OR 2022 OR 2023 OR 2024 OR 2025 OR 2026)`,
          `"${core}" "${ownerName}" (sold OR sale OR owner OR ownership OR "no longer")`,
          `"${core}" (owner OR "owned by") -tripadvisor -booking`,
        ],
        avoid_queries: [`who owns ${core}`],
        absence_of_later_sale_is_not_proof_of_current_ownership: true,
        distinguish: {
          historical_owner: ownerName,
          current_owner_requires: "positive evidence as of a stated date",
          not_enrichment_eligible_until_current: true,
        },
        enrichment_authorized: false,
        publication_authorized: false,
      });
    }

    if (c.relationship === "PARENT_ACQUIRED" || c.scope === "COMPANY") {
      const buyer = dir?.corrected_subject || dir?.buyer || subject;
      const acquiredCo = dir?.corrected_object || dir?.acquired || c.object;
      plans.push({
        unresolved_question: "After the parent-company transaction, did this hotel remain with the same owner group or was the asset sold separately?",
        hotel_name: hotelName,
        lead: { subject: buyer, object: acquiredCo, relationship: "PARENT_ACQUIRED", evidence_span: c.evidence_span },
        priority_query_types: ["transaction_announcement", "owner_disclosure", "property_history"],
        proposed_queries: [
          `"${core}" (sold OR "asset sale" OR ownership) after "${acquiredCo}"`,
          `"${core}" (owner OR owned by) -tripadvisor -booking`,
        ],
        avoid_queries: [`who owns ${core}`],
        absence_of_later_sale_is_not_proof_of_current_ownership: true,
        enrichment_authorized: false,
        publication_authorized: false,
      });
    }

    if (c.scope === "GENERAL_PORTFOLIO" || (c.relationship === "OPERATES" && !c.target_hotel_link?.establishes_relationship_to_target_validated)) {
      plans.push({
        unresolved_question: "Who is the named asset owner behind this operator/brand for the target hotel?",
        hotel_name: hotelName,
        lead: { subject, relationship: c.relationship, scope: c.scope, evidence_span: c.evidence_span },
        priority_query_types: ["owner_disclosure", "transaction_announcement", "filing"],
        proposed_queries: [
          `"${core}" (acquired OR acquisition OR "owned by" OR owner OR investor OR proprietor)`,
          `"${core}" (sale OR sold OR "real estate")`,
        ],
        avoid_queries: subject ? [`"${subject}" owner and operator`] : [],
        keep_management_separate: true,
        enrichment_authorized: false,
        publication_authorized: false,
      });
    }

    if (c.relationship === "FAMILY_OWNS_UNNAMED") {
      plans.push({
        unresolved_question: "Which named family, person, or company owns this property, with evidence connecting that party to ownership?",
        hotel_name: hotelName,
        lead: { subject: "", relationship: "FAMILY_OWNS_UNNAMED", evidence_span: c.evidence_span },
        priority_query_types: ["property_history", "owner_disclosure", "filing"],
        proposed_queries: [
          `"${core}" (owned by OR owner OR proprietor OR "family trust" OR company)`,
          `"${core}" (deed OR registry OR "title" OR investor)`,
        ],
        avoid_queries: [`"${core}" manager`, `"${core}" general manager`],
        do_not_infer_from_manager_or_surname_alone: true,
        enrichment_authorized: false,
        publication_authorized: false,
      });
    }
  }

  // Dedupe by unresolved_question + first query
  const seen = new Set();
  return plans.filter((p) => {
    const k = `${p.unresolved_question}|${p.proposed_queries?.[0] || ""}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

/** Caps for prepared DEVELOPMENT continuation (not executed here). */
export const DEVELOPMENT_CONTINUATION_CAPS = Object.freeze({
  label: "DEVELOPMENT_CONTINUATION",
  not_unseen_generalization_test: true,
  context_dev_credits_total: 30,
  context_dev_credits_max_per_hotel: 10,
  model_reader_usd_total: 0.25,
  contact_enrichment_credits: 0,
  other_paid_providers: 0,
  automatic_retries: 0,
  held_out_access: false,
  publication: false,
  canonical_writes: false,
  enrichment: false,
});

/**
 * Prepare (do not run) a bounded live continuation for one hotel case.
 */
export function prepareDevelopmentContinuationCase({
  hotel,
  merged,
  follow_up_plans,
  already_spent_context = 0,
  already_spent_model_usd = 0,
} = {}) {
  const caps = DEVELOPMENT_CONTINUATION_CAPS;
  const hotelBudget = Math.min(
    caps.context_dev_credits_max_per_hotel,
    caps.context_dev_credits_total
  );
  return {
    version: OWNERSHIP_FOLLOW_UP_VERSION,
    run_label: "DEVELOPMENT_CONTINUATION",
    human_validation: "PENDING",
    hotel: {
      hotel_id: hotel.hotel_id,
      hotel_name: hotel.hotel_name,
    },
    caps,
    ledger: {
      context_dev: {
        budget: hotelBudget,
        already_spent: already_spent_context,
        remaining: Math.max(0, hotelBudget - already_spent_context),
      },
      model_reader_usd: {
        budget_shared: caps.model_reader_usd_total,
        already_spent: already_spent_model_usd,
      },
      retries: 0,
    },
    merged_candidate_count: (merged.candidates || []).length,
    follow_up_plans,
    search_policy: {
      use_existing: ["contextDevSearch", "contextDevScrapeMarkdown", "structured reader", "ownershipQueries rankOwnershipSources"],
      prioritize: ["transaction_announcement", "owner_disclosure", "filing", "property_history"],
      follow_named_parties: true,
      no_broad_who_owns_after_specific_lead: true,
      absence_of_sale_not_proof_of_current_ownership: true,
      stop_when: ["question_answered", "access_block", "budget_exhausted"],
    },
    gates: {
      enrichment_authorized: false,
      publication_authorized: false,
      canonical_writes: false,
      contact_enrichment: false,
    },
    executed: false,
  };
}

/**
 * Row shape for the required output table (staging; not ownership acceptance).
 */
export function toFollowUpOutputRow(candidate, hotel, extras = {}) {
  const dir = candidate.acquisition_direction;
  return {
    hotel: hotel.hotel_name || hotel,
    candidate_owner: candidate.subject || (candidate.relationship === "FAMILY_OWNS_UNNAMED" ? "(unnamed family)" : null),
    relationship: candidate.relationship,
    scope: candidate.scope,
    supporting_passages: {
      relationship: candidate.evidence_span || candidate.target_hotel_link?.relationship_span || null,
      hotel_identity: candidate.target_hotel_link?.hotel_identity_span || null,
    },
    transaction_date: candidate.event_date,
    source_date: candidate.source_date,
    acquisition_direction: dir?.direction || null,
    direction_explanation: dir?.explanation || null,
    contrary_evidence: extras.contrary_evidence || [],
    currentness: candidate.currentness,
    remaining_gap: extras.remaining_gap || candidate.next_evidence_needed || null,
    methods: candidate.methods || [candidate.method],
    accepted_as_ownership: false,
    enrichment_authorized: false,
    publication_authorized: false,
  };
}
