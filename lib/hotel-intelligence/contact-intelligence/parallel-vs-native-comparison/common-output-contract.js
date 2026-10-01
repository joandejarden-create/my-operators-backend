/**
 * Common review output contract for Parallel vs Dealality-native comparison.
 * Normalizes both arms without promoting weaker relationships into ownership.
 */

import { RELATIONSHIP_ENUM } from "./comparison-config.js";

export const OUTPUT_CONTRACT_VERSION = "parallel-vs-native-common-output-v1";

const OWNERSHIP_LIKE = new Set(["PROPERTY_OWNER", "ECONOMIC_SPONSOR"]);
const WEAKER = new Set(["OPERATOR", "BRAND", "REGISTERED_BUSINESS", "HISTORICAL_OWNER"]);

function emptyHotelBlock(shared = {}) {
  return {
    hotel_id: shared.hotel_id || null,
    hotel_name: shared.hotel_name || null,
    resolved_property_identity: null,
    identity_uncertainty: "UNRESOLVED",
    identity_evidence: [],
  };
}

export function createEmptyReviewRow({ arm, hotel_id, hotel_name, shared_input = null } = {}) {
  return {
    contract_version: OUTPUT_CONTRACT_VERSION,
    arm: arm || null,
    hotel: emptyHotelBlock(shared_input || { hotel_id, hotel_name }),
    ownership_claims: [],
    organization: {
      candidate_official_domain: null,
      domain_org_evidence: [],
      domain_status: "ABSENT",
    },
    people: [],
    public_contact_details: [],
    contrary_evidence: [],
    missing_links: [],
    abstention: null,
    failure: null,
    budget: {
      spent_usd: null,
      spent_credits: null,
      exhausted: false,
    },
    wall_clock_ms: null,
    human_intervention: "NONE",
    raw_ref: null,
    review_flags: {
      ownership_promoted_from_weaker_relationship: false,
      historical_treated_as_current: false,
      brand_or_operator_as_owner: false,
    },
  };
}

function mapNativeRelationship(classification) {
  const c = String(classification || "").toUpperCase();
  if (/PROPERTY_OWNER|OWNED_BY|OWNER\b/.test(c) && !/OPERATOR|BRAND/.test(c)) return "PROPERTY_OWNER";
  if (/ECONOMIC|SPONSOR/.test(c)) return "ECONOMIC_SPONSOR";
  if (/OPERATOR|MANAGED/.test(c)) return "OPERATOR";
  if (/BRAND|AFFILIAT/.test(c)) return "BRAND";
  if (/REGISTERED|PROPCO|LEGAL/.test(c)) return "REGISTERED_BUSINESS";
  if (/HISTORICAL|FORMER|ACQUIRED/.test(c)) return "HISTORICAL_OWNER";
  if (/UNRESOLVED|UNKNOWN|STAGED/.test(c)) return "UNRESOLVED";
  return "UNRESOLVED";
}

function mapParallelRelationship(rel) {
  const r = String(rel || "").toUpperCase();
  if (RELATIONSHIP_ENUM.includes(r)) return r;
  if (/OWNED_BY|PROPERTY_OWNER|OWNER/.test(r) && !/OPERATOR|BRAND/.test(r)) return "PROPERTY_OWNER";
  if (/SPONSOR|ECONOMIC/.test(r)) return "ECONOMIC_SPONSOR";
  if (/OPERAT/.test(r)) return "OPERATOR";
  if (/BRAND/.test(r)) return "BRAND";
  if (/PROPCO|REGISTERED|LEGAL/.test(r)) return "REGISTERED_BUSINESS";
  if (/FORMER|HISTORICAL|PRIOR/.test(r)) return "HISTORICAL_OWNER";
  return "UNRESOLVED";
}

/**
 * Guard: never rewrite OPERATOR/BRAND into PROPERTY_OWNER during normalization.
 */
export function assertNoOwnershipPromotion(claim) {
  const rel = claim?.relationship;
  const flags = {
    ownership_promoted_from_weaker_relationship: false,
    brand_or_operator_as_owner: false,
    historical_treated_as_current: false,
  };
  if (WEAKER.has(rel) && claim?.treated_as_current_owner === true) {
    flags.ownership_promoted_from_weaker_relationship = true;
  }
  if ((rel === "OPERATOR" || rel === "BRAND") && claim?.scored_as_owner === true) {
    flags.brand_or_operator_as_owner = true;
  }
  if (rel === "HISTORICAL_OWNER" && claim?.currentness === "CURRENT") {
    flags.historical_treated_as_current = true;
  }
  return flags;
}

export function normalizeNativeResearchToContract(research, sharedInput = {}) {
  const row = createEmptyReviewRow({
    arm: "DEALALITY_NATIVE",
    shared_input: sharedInput,
  });
  row.hotel.hotel_id = research?.hotel_id || sharedInput.hotel_id;
  row.hotel.hotel_name = research?.hotel_name || sharedInput.hotel_name;
  row.hotel.resolved_property_identity = research?.hotel_name || sharedInput.hotel_name;
  row.hotel.identity_uncertainty = research?.hotel_id ? "ASSUMED_FROM_SEED" : "UNRESOLVED";
  row.wall_clock_ms = research?.elapsed_ms ?? null;
  row.budget.spent_credits = research?.budgets?.context_dev?.spent ?? null;
  row.budget.exhausted = (research?.unresolved_reasons || []).some((x) =>
    /BUDGET_EXHAUSTED/i.test(x)
  );
  row.raw_ref = { kind: "native_handoff", version: research?.version || null };

  const ownership = research?.ownership || {};
  const name = ownership.owner_display_name || ownership.name || null;
  if (name) {
    const relationship = mapNativeRelationship(
      ownership.classification || ownership.owner_role || ownership.relationship_primary
    );
    const claim = {
      named_entity_or_person: name,
      relationship,
      supporting_passage: ownership.evidence_excerpt || ownership.excerpt || null,
      source_url: ownership.source_url || (research?.sources || [])[0]?.url || null,
      publication_date: ownership.publication_date || null,
      transaction_or_event_date: ownership.transaction_date || ownership.event_date || null,
      currentness: ownership.currentness || (relationship === "HISTORICAL_OWNER" ? "HISTORICAL" : "UNKNOWN"),
      contrary_evidence: ownership.contrary_evidence || [],
      missing_links: ownership.missing_links || [],
      treated_as_current_owner: OWNERSHIP_LIKE.has(relationship),
      scored_as_owner: OWNERSHIP_LIKE.has(relationship),
    };
    row.ownership_claims.push(claim);
    Object.assign(row.review_flags, assertNoOwnershipPromotion(claim));
  } else if ((research?.unresolved_reasons || []).length) {
    row.abstention = {
      reason: "OWNER_STILL_UNRESOLVED_OR_NO_NAMED_CLAIM",
      unresolved_reasons: research.unresolved_reasons,
    };
  }

  const domain = research?.confirmed_company_domain_host || research?.confirmed_company_domain || null;
  if (domain) {
    row.organization.candidate_official_domain = String(domain).replace(/^https?:\/\//, "").split("/")[0];
    row.organization.domain_status = "CANDIDATE";
    row.organization.domain_org_evidence = research?.newly_researched?.domain?.evidence
      ? [research.newly_researched.domain.evidence]
      : [];
  }

  for (const p of research?.people || []) {
    row.people.push({
      name: p.display_name || p.name || null,
      role: p.title || p.role || null,
      organization: p.organization_name || ownership.owner_display_name || null,
      affiliation_evidence: (p.evidence || []).slice(0, 3),
      relevance_to_ownership_development_acquisitions_or_am:
        p.why_relevant || p.publication_label || null,
      source_url: (p.evidence || [])[0]?.source_url || null,
      source_date: (p.evidence || [])[0]?.observed_at || null,
      current_versus_former_uncertainty: p.role_currency || p.provenance?.role_currency || "UNKNOWN",
      publication_label: p.publication_label || null,
    });
    for (const ch of p.channels || []) {
      row.public_contact_details.push({
        route_kind: /LINKEDIN/i.test(ch.kind) ? "NAMED_PERSON" : "CORPORATE_OR_PERSON",
        channel_kind: ch.kind,
        value: ch.value,
        source: ch.display_label || null,
        attribution: ch.attribution || null,
        purpose: "discovery_only",
        deliverability_verified: false,
      });
    }
  }

  for (const ch of research?.organization_contact_route?.channels || []) {
    row.public_contact_details.push({
      route_kind: /hotel/i.test(ch.display_label || "") ? "HOTEL" : "CORPORATE",
      channel_kind: ch.kind,
      value: ch.value,
      source: ch.display_label || null,
      attribution: ch.attribution || null,
      purpose: "public_route",
      deliverability_verified: false,
    });
  }

  row.missing_links = [...(research?.unresolved_reasons || [])];
  if (!row.ownership_claims.length && !row.abstention) {
    row.abstention = { reason: "NO_OWNERSHIP_CLAIM" };
  }
  return row;
}

export function normalizeParallelArtifactToContract(artifact, sharedInput = {}) {
  const row = createEmptyReviewRow({
    arm: "PARALLEL",
    shared_input: sharedInput,
  });
  const blocks = artifact?.blocks || artifact?.structured_blocks || artifact?.content || {};
  const identity = blocks.SUBJECT_IDENTITY || artifact?.subject_identity || {};
  row.hotel.resolved_property_identity =
    identity.resolved_name || sharedInput.hotel_name || null;
  row.hotel.identity_uncertainty =
    identity.identity_match === "TRUE"
      ? "MATCHED"
      : identity.identity_match === "FALSE"
        ? "MISMATCH"
        : "UNRESOLVED";
  row.hotel.identity_evidence = identity.identity_evidence || [];
  row.wall_clock_ms = artifact?.runtime_ms ?? artifact?.latency_ms ?? null;
  row.budget.spent_usd = artifact?.provider_cost_usd ?? null;
  row.raw_ref = {
    kind: "parallel_task",
    run_id: artifact?.provider_run_id || artifact?.run_id || null,
    processor: artifact?.processor || null,
  };

  if (row.hotel.identity_uncertainty === "MISMATCH" || row.hotel.identity_uncertainty === "UNRESOLVED") {
    row.abstention = {
      reason: "IDENTITY_NOT_ESTABLISHED",
      identity_match: identity.identity_match || null,
    };
    // Do not promote ownership findings when identity fails
    row.missing_links.push("IDENTITY_GATE");
    return row;
  }

  const ownershipChain =
    blocks.OWNERSHIP_CLAIMS || blocks.OWNERSHIP_CHAIN || blocks.OWNERSHIP || [];
  const chainArr = Array.isArray(ownershipChain) ? ownershipChain : [ownershipChain].filter(Boolean);
  for (const node of chainArr) {
    const relationship = mapParallelRelationship(
      node.relationship || node.role || node.relationship_type
    );
    const claim = {
      named_entity_or_person:
        node.named_entity_or_person || node.name || node.entity || node.party || null,
      relationship,
      supporting_passage:
        node.supporting_passage || node.evidence_excerpt || node.excerpt || node.passage || null,
      source_url: node.source_url || node.url || null,
      publication_date: node.publication_date || null,
      transaction_or_event_date:
        node.transaction_or_event_date || node.transaction_date || node.event_date || null,
      currentness: node.currentness || node.temporal_status || "UNKNOWN",
      contrary_evidence: node.contrary_evidence_note
        ? [node.contrary_evidence_note]
        : node.contrary_evidence || [],
      missing_links: node.missing_links || [],
      treated_as_current_owner: OWNERSHIP_LIKE.has(relationship) && node.currentness !== "HISTORICAL",
      scored_as_owner: OWNERSHIP_LIKE.has(relationship),
    };
    if (claim.named_entity_or_person) row.ownership_claims.push(claim);
    Object.assign(row.review_flags, assertNoOwnershipPromotion(claim));
  }

  const org = blocks.ORGANIZATION || blocks.OWNER_ORGANIZATION || {};
  if (org.candidate_official_domain || org.domain || org.official_domain) {
    row.organization.candidate_official_domain =
      org.candidate_official_domain || org.domain || org.official_domain;
    row.organization.domain_status = org.domain_status || "CANDIDATE";
    row.organization.domain_org_evidence = org.domain_org_evidence_passage
      ? [{ passage: org.domain_org_evidence_passage, url: org.domain_source_url || null }]
      : org.domain_evidence || org.evidence || [];
  }

  for (const p of blocks.PEOPLE || blocks.KEY_PEOPLE || []) {
    row.people.push({
      name: p.name || p.person_name || null,
      role: p.role || p.title || null,
      organization: p.organization || null,
      affiliation_evidence: p.affiliation_passage
        ? [{ passage: p.affiliation_passage, url: p.source_url || null }]
        : p.evidence || [],
      relevance_to_ownership_development_acquisitions_or_am:
        p.relevance || p.relationship_to_hotel || null,
      source_url: p.source_url || null,
      source_date: p.source_date || p.last_seen || p.date || null,
      current_versus_former_uncertainty:
        p.current_versus_former || p.role_currentness || "UNKNOWN",
    });
  }

  for (const c of blocks.PUBLIC_CONTACTS || blocks.CONTACTS || artifact?.contacts || []) {
    row.public_contact_details.push({
      route_kind:
        c.route_kind ||
        (c.subject_type === "HOTEL" ? "HOTEL" : c.person_name ? "NAMED_PERSON" : "CORPORATE"),
      channel_kind: c.email ? "EMAIL" : c.phone ? "PHONE" : c.profile_url ? "PROFILE" : "OTHER",
      value: c.value || c.email || c.phone || c.profile_url || null,
      source: c.source_url || null,
      attribution: c.attribution || c.source_type || null,
      purpose: c.purpose || "discovery_only",
      deliverability_verified: false,
      email_status: c.email_status || null,
      note: "Not independently deliverability-verified.",
    });
  }

  row.contrary_evidence = blocks.CONTRARY_EVIDENCE || [];
  row.missing_links = blocks.OPEN_QUESTIONS || blocks.MISSING_LINKS || [];
  if (!row.ownership_claims.length && !row.abstention) {
    row.abstention = { reason: "NO_OWNERSHIP_CLAIM_IN_PARALLEL_OUTPUT" };
  }
  return row;
}

export function scoreboardMetrics(reviewRows = [], { denominator = 10 } = {}) {
  const byHotel = new Map();
  for (const r of reviewRows) {
    const id = r.hotel?.hotel_id;
    if (!id) continue;
    if (!byHotel.has(id)) byHotel.set(id, []);
    byHotel.get(id).push(r);
  }

  function armMetrics(arm) {
    const rows = reviewRows.filter((r) => r.arm === arm);
    const hotels = new Set(rows.map((r) => r.hotel?.hotel_id).filter(Boolean));
    let supportedOwnership = 0;
    let currentEnough = 0;
    let ownerDomain = 0;
    let qualifiedPerson = 0;
    let completeChain = 0;
    let unsupportedAssignments = 0;
    let abstentions = 0;
    let failures = 0;
    let budgetExhaustion = 0;
    let costUsd = 0;
    let costCredits = 0;
    let wallMs = 0;

    for (const r of rows) {
      const own = (r.ownership_claims || []).filter((c) => OWNERSHIP_LIKE.has(c.relationship));
      const weakAsOwner = (r.ownership_claims || []).filter(
        (c) => WEAKER.has(c.relationship) && c.scored_as_owner
      );
      if (own.length) supportedOwnership += 1;
      if (own.some((c) => c.currentness === "CURRENT" || c.currentness === "AS_OF_STATED_DATE")) {
        currentEnough += 1;
      }
      if (r.organization?.candidate_official_domain) ownerDomain += 1;
      const people = (r.people || []).filter((p) => p.name && !/FORMER/i.test(p.current_versus_former_uncertainty || ""));
      if (people.length) qualifiedPerson += 1;
      if (own.length && r.organization?.candidate_official_domain && people.length) completeChain += 1;
      unsupportedAssignments += weakAsOwner.length;
      if (r.abstention) abstentions += 1;
      if (r.failure) failures += 1;
      if (r.budget?.exhausted) budgetExhaustion += 1;
      costUsd += Number(r.budget?.spent_usd || 0);
      costCredits += Number(r.budget?.spent_credits || 0);
      wallMs += Number(r.wall_clock_ms || 0);
    }

    return {
      arm,
      hotels_in_arm: hotels.size,
      denominator,
      hotels_with_supported_dated_ownership_or_sponsor: supportedOwnership,
      hotels_with_sufficiently_current_owner_sponsor_evidence: currentEnough,
      hotels_with_evidenced_owner_domain: ownerDomain,
      hotels_with_qualified_relevant_person: qualifiedPerson,
      hotels_with_complete_owner_domain_person_chain: completeChain,
      unsupported_ownership_person_assignments: unsupportedAssignments,
      abstentions,
      failures,
      budget_exhaustion: budgetExhaustion,
      cost_usd_total: Number(costUsd.toFixed(4)),
      cost_credits_total: costCredits,
      cost_per_supported_ownership_result_usd:
        supportedOwnership > 0 ? Number((costUsd / supportedOwnership).toFixed(4)) : null,
      cost_per_complete_chain_usd:
        completeChain > 0 ? Number((costUsd / completeChain).toFixed(4)) : null,
      wall_clock_ms_total: wallMs,
      human_intervention: rows.some((r) => r.human_intervention && r.human_intervention !== "NONE")
        ? "PRESENT"
        : "NONE",
      recall: "NOT_REPORTED_NO_REFERENCE_SET",
    };
  }

  return {
    denominator,
    metrics_note: "All ten hotels remain in the denominator. Do not report recall without a reference set.",
    DEALALITY_NATIVE: armMetrics("DEALALITY_NATIVE"),
    PARALLEL: armMetrics("PARALLEL"),
  };
}
