/**
 * Native adjudication of retrieved ownership documents — reuses existing
 * planning/follow-up/claim-contract modules. Does not rewrite the document reader.
 * Enrichment is always disabled here.
 */
import { ownershipDocumentPassages, ownershipCandidates } from "./ownership-research-planning.js";
import {
  mergeOwnershipClaimCandidates,
  repairOwnershipCurrentness,
  buildQuestionDependentFollowUpPlan,
} from "./ownership-follow-up-research.js";
import {
  normalizeOwnershipClaimContract,
  historicalClaimFollowUpQuestions,
  inferEarliestOwnershipFailureStage,
  pushStageEvent,
  OWNERSHIP_RESEARCH_FAILURE_STAGE,
} from "./ownership-research-stage-trace.js";
import { mentionsHotel } from "./research-evidence.js";
import { validateOwnerPersonEnrichmentSubmission } from "./owner-person-enrichment-gate.js";

export const OWNERSHIP_RETRIEVAL_ADJUDICATION_VERSION = "ownership-retrieval-adjudication-v1";

const OWNER_RELS = new Set(["OWNS", "OWNED_BY", "SPONSORS"]);
const OWNER_ROLES = new Set(["PROPERTY_OWNER", "ECONOMIC_OWNER", "ECONOMIC_OWNER_OR_SPONSOR", "ECONOMIC_SPONSOR"]);

/**
 * Adjudicate discovery documents through native ownership contract.
 */
export function adjudicateOwnershipDiscovery(discovery = {}, hotel = {}) {
  const h = hotel.hotel_name ? hotel : discovery.hotel || {};
  const claims = [];
  const passages = [];
  const follow_ups = [];
  const enrichment_gate_attempts = [];

  for (const doc of discovery.documents || []) {
    if (!doc?.ok && !(doc.markdown || doc.passage)) continue;
    const text = String(doc.markdown || doc.passage || "");
    if (!text) continue;
    const pack = ownershipDocumentPassages(text, h);
    for (const p of pack.passages || []) {
      const hotelHit = mentionsHotel(p.excerpt, h.hotel_name);
      passages.push({
        url: doc.url,
        excerpt: String(p.excerpt || "").slice(0, 400),
        hotel_identity_in_passage: hotelHit,
        characters: (p.excerpt || "").length,
      });
    }
    const candidates = ownershipCandidates(
      { ...pack, hotel_name: h.hotel_name, source_url: doc.url },
      h
    );
    const merged = mergeOwnershipClaimCandidates({
      hotelName: h.hotel_name,
      deterministicCandidates: candidates,
      modelGrounded: [],
      modelRetained: [],
      sourceMeta: { source_url: doc.url, url: doc.url },
    });
    for (const c of merged.candidates || []) {
      let working = repairOwnershipCurrentness({
        ...c,
        source_url: doc.url,
        evidence_grounded: Boolean(c.evidence_span || c.excerpt),
      });
      const contract = normalizeOwnershipClaimContract(working, h);
      const row = {
        ...working,
        ...contract,
        provider: discovery.provider,
        source_url: doc.url,
      };
      claims.push(row);
      if (discovery.stage_trace) {
        pushStageEvent(discovery.stage_trace, {
          kind: "claim",
          stage: "OWNERSHIP_RELATIONSHIP_CLASSIFICATION",
          claim: {
            subject: row.subject,
            relationship: row.relationship,
            currentness: row.currentness,
            party_kind: row.party_kind,
            evidence_grounded: row.evidence_grounded,
            source_url: row.source_url,
          },
          provider: discovery.provider,
        });
      }
      if (
        row.currentness === "HISTORICAL" ||
        row.party_kind === "HISTORICAL_ACQUIRER" ||
        (row.relationship === "ACQUIRED" && row.currentness !== "CURRENT_AS_OF_STATED_DATE")
      ) {
        for (const q of historicalClaimFollowUpQuestions(row, h)) {
          follow_ups.push({ question: q, based_on: row.subject, provider: discovery.provider });
        }
      }
    }
  }

  // Parallel may also propose ownership claims directly — still must pass native contract.
  if (discovery.parallel_artifact?.ownership_claims) {
    for (const pc of discovery.parallel_artifact.ownership_claims) {
      const working = repairOwnershipCurrentness({
        subject: pc.named_entity_or_person || pc.subject || pc.name,
        relationship: mapParallelRelationship(pc.relationship),
        object: h.hotel_name,
        evidence_span: pc.supporting_passage || pc.excerpt,
        source_url: pc.source_url || pc.url,
        source_date: pc.publication_date || pc.source_date,
        event_date: pc.transaction_or_event_date || pc.event_date,
        currentness: pc.currentness || "UNRESOLVED",
        evidence_grounded: Boolean(pc.supporting_passage || pc.excerpt),
        target_hotel_link_validated: mentionsHotel(
          pc.supporting_passage || pc.excerpt || "",
          h.hotel_name
        ),
      });
      const contract = normalizeOwnershipClaimContract(working, h);
      claims.push({
        ...working,
        ...contract,
        provider: discovery.provider,
        from_parallel_structured_claim: true,
      });
    }
  }

  const plans = buildQuestionDependentFollowUpPlan({ candidates: claims, hotel_name: h.hotel_name }, h);
  for (const p of plans) {
    follow_ups.push({
      question: p.unresolved_question,
      proposed_queries: p.proposed_queries,
      provider: discovery.provider,
    });
  }

  const score = scoreAdjudicatedClaims(claims, h);

  // Enrichment gate — always disabled; prove provider output cannot bypass.
  const enrichment_blocked = true;
  if (score.supported_current_owner_claims[0]) {
    const gate = validateOwnerPersonEnrichmentSubmission({
      hotel_name: h.hotel_name,
      hotel_to_owner: {
        relationship_class: "PROPERTY_OWNER",
        supported: true,
        evidence_refs: [
          {
            url: score.supported_current_owner_claims[0].source_url,
            excerpt: score.supported_current_owner_claims[0].evidence_span,
          },
        ],
      },
      organization: { name: score.supported_current_owner_claims[0].subject },
      person: { display_name: "Placeholder", identity_supported: false },
      affiliation_corroboration: { source_class: "NONE", evidence_refs: [] },
      provider: "surfe",
    });
    enrichment_gate_attempts.push({
      attempted: false,
      enrichment_authorized: false,
      gate_ok: gate.ok,
      note: "Comparison path never enables enrichment; gate shown for bypass-proof only",
    });
  }

  const earliest = inferEarliestOwnershipFailureStage({
    claims,
    searches: discovery.stage_trace?.searches || [
      { raw_result_count: discovery.raw_result_count || 0 },
    ],
    fetches: (discovery.documents || []).map((d) => ({
      ok: d.ok,
      characters: d.characters,
      attempted: true,
    })),
    rankedEmptyBecauseFiltered:
      (discovery.raw_result_count || 0) > 0 && !(discovery.ranked || []).length,
    confirmedDomain: null,
    evidencedPeople: [],
    hasAttributableContact: false,
    stopReasons: [discovery.stop_reason].filter(Boolean),
    objectiveSupported: score.supported_current_owner_evidence,
  });

  return {
    version: OWNERSHIP_RETRIEVAL_ADJUDICATION_VERSION,
    provider: discovery.provider,
    hotel: {
      hotel_id: h.hotel_id || null,
      hotel_name: h.hotel_name || null,
    },
    passages,
    claims,
    follow_ups: follow_ups.slice(0, 12),
    score,
    earliest_failure_stage: earliest,
    enrichment_authorized: false,
    enrichment_blocked,
    enrichment_gate_attempts,
    publication_authorized: false,
    canonical_writes: false,
    cost: discovery.cost || null,
    stop_reason: discovery.stop_reason || null,
  };
}

function mapParallelRelationship(rel) {
  const r = String(rel || "").toUpperCase();
  if (["PROPERTY_OWNER", "ECONOMIC_OWNER", "OWNS", "OWNED_BY"].includes(r)) return "OWNS";
  if (["ECONOMIC_SPONSOR", "SPONSOR", "SPONSORS"].includes(r)) return "SPONSORS";
  if (["HISTORICAL_OWNER", "HISTORICAL"].includes(r)) return "ACQUIRED";
  if (["OPERATOR", "OPERATES"].includes(r)) return "OPERATES";
  if (["BRAND", "BRANDS"].includes(r)) return "BRANDS";
  return r || "OTHER";
}

export function scoreAdjudicatedClaims(claims = [], hotel = {}) {
  const hotelSpecificPassages = claims.filter(
    (c) => c.evidence_grounded && (c.target_hotel_link_validated || mentionsHotel(c.evidence_span || "", hotel.hotel_name))
  );
  const supportedCurrent = claims.filter((c) => isSupportedCurrentOwnerClaim(c, hotel));
  const historical = claims.filter(
    (c) =>
      c.currentness === "HISTORICAL" ||
      c.party_kind === "HISTORICAL_ACQUIRER" ||
      (c.relationship === "ACQUIRED" && c.currentness !== "CURRENT_AS_OF_STATED_DATE")
  );
  const ambiguous = claims.filter(
    (c) =>
      c.evidence_grounded &&
      !isSupportedCurrentOwnerClaim(c, hotel) &&
      ["OTHER", "FAMILY_OWNS_UNNAMED", "OPERATES", "BRANDS"].includes(String(c.relationship || "").toUpperCase())
  );
  const incorrect =
    claims.filter((c) => c.promotes_to_current_ownership === true).length > 0
      ? claims.filter((c) => c.promotes_to_current_ownership)
      : [];

  return {
    hotel_specific_ownership_passage_retrieved: hotelSpecificPassages.length > 0,
    supported_current_owner_evidence: supportedCurrent.length > 0,
    supported_current_owner_claims: supportedCurrent.map((c) => ({
      subject: c.subject,
      relationship: c.relationship,
      currentness: c.currentness,
      source_url: c.source_url,
      evidence_span: String(c.evidence_span || "").slice(0, 240),
      provider: c.provider,
    })),
    historical_owner_evidence: historical.length > 0,
    historical_claims: historical.map((c) => ({
      subject: c.subject,
      relationship: c.relationship,
      currentness: c.currentness,
      source_url: c.source_url,
    })),
    ambiguous_relationship: ambiguous.length > 0 && supportedCurrent.length === 0,
    confirmed_owner_domain: false,
    relevant_person: false,
    attributable_email: false,
    attributable_phone: false,
    complete_chain: false,
    incorrect_owner_assignments: incorrect.length,
    claim_counts: {
      total: claims.length,
      current_owner: supportedCurrent.length,
      historical: historical.length,
      ambiguous: ambiguous.length,
    },
  };
}

/**
 * Strict current-owner contract — company name alone is insufficient.
 */
export function isSupportedCurrentOwnerClaim(claim = {}, hotel = {}) {
  if (!claim.evidence_grounded) return false;
  const rel = String(claim.relationship || "").toUpperCase();
  const role = String(claim.party_role || claim.party_kind || "").toUpperCase();
  const cur = String(claim.currentness || "").toUpperCase();
  const ownerRel = OWNER_RELS.has(rel) || OWNER_ROLES.has(role) || role === "SPONSOR";
  if (!ownerRel) return false;
  if (!(cur === "CURRENT_AS_OF_STATED_DATE" || cur === "CURRENT")) return false;
  if (claim.promotes_to_current_ownership === true && !claim.evidence_grounded) return false;
  const span = claim.evidence_span || claim.excerpt || "";
  const hotelOk =
    claim.target_hotel_link_validated === true || mentionsHotel(span, hotel.hotel_name);
  if (!hotelOk) return false;
  if (!nz(claim.subject)) return false;
  // Reject self-hotel / generic pages as owner org
  if (String(claim.subject).toLowerCase() === String(hotel.hotel_name || "").toLowerCase()) return false;
  if (/investor\.gov|owner\.com|merriam-webster|dictionary|wikipedia/i.test(claim.source_url || "")) {
    return false;
  }
  return true;
}

function nz(v) {
  return String(v == null ? "" : v).trim();
}

export { OWNERSHIP_RESEARCH_FAILURE_STAGE };
