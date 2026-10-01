/**
 * Contact Intelligence V1.1 — single canonical owner-target resolver.
 *
 * Authority: ownership-surface hotelOwnerAnchor → OCG / HOTEL_TO_OWNER.
 * Contact Intelligence does NOT invent ownership.
 * If unresolved → OWNER_TARGET_UNRESOLVED (no guess).
 */

import { getDefaultOwnershipSurface } from "../ownership/ownership-surface-v1.js";
import { getOwnerPortfolioProfile } from "../ownership/owner-control/portfolio-store.js";
import {
  createContactTarget,
  inferRoleCategoryFromTitle,
  rankContactTargetRelevance,
} from "./contact-target.js";
import { ROLE_CATEGORY, CONTACT_SUBJECT_KIND, UNRESOLVED_REASON } from "./vocabulary.js";

export const OWNER_TARGET_RESOLVER_VERSION = "ci-owner-target-resolver-v1.1";

export const OWNER_TARGET_STATUS = Object.freeze({
  RESOLVED: "RESOLVED",
  PARTIAL: "PARTIAL",
  OWNER_TARGET_UNRESOLVED: "OWNER_TARGET_UNRESOLVED",
});

/**
 * Normalize owner organization into ContactOrganizationTarget.
 */
export function createContactOrganizationTarget(partial = {}) {
  return {
    target_kind: CONTACT_SUBJECT_KIND.ORGANIZATION,
    organization_id: partial.organization_id || null,
    canonical_name: partial.canonical_name || null,
    aliases: Array.isArray(partial.aliases) ? partial.aliases : [],
    legal_name: partial.legal_name || null,
    trade_name: partial.trade_name || null,
    country: partial.country || null,
    domain: partial.domain || null,
    domain_confidence: partial.domain_confidence || null,
    parent_relationship: partial.parent_relationship || null,
    owner_relevance: partial.owner_relevance || "ECONOMIC_OWNER",
    portfolio_hotel_ids: Array.isArray(partial.portfolio_hotel_ids)
      ? partial.portfolio_hotel_ids
      : [],
    why_relevant: partial.why_relevant || "Resolved owner organization for hotel contact path",
    source: partial.source || "ownership_surface",
  };
}

/**
 * Bounded decision-maker targets (max 5). Hypotheses only until evidenced live.
 */
export function buildPersonTargetsFromHypotheses(orgTarget, hypotheses = [], { hotel_id = null } = {}) {
  const out = [];
  for (const h of hypotheses) {
    if (out.length >= 5) break;
    if (h.former_affiliation || h.deceased) continue;
    const role = inferRoleCategoryFromTitle(h.title) || ROLE_CATEGORY.OTHER;
    if (role === ROLE_CATEGORY.OPERATOR_SIDE || role === ROLE_CATEGORY.OTHER) {
      // Keep OWNER_PRINCIPAL / exec hypotheses; skip weak OTHER unless marked
      if (!h.force_include && role === ROLE_CATEGORY.OPERATOR_SIDE) continue;
    }
    const target = createContactTarget({
      target_type: CONTACT_SUBJECT_KIND.PERSON,
      organization_id: orgTarget?.organization_id || null,
      hotel_id,
      role_category: role === ROLE_CATEGORY.OTHER ? ROLE_CATEGORY.OWNER_PRINCIPAL : role,
      display_name: h.display_name || h.name || null,
      title: h.title || null,
      why_relevant: h.why_relevant || h.why_relevant_hypothesis || null,
      current_role_required: true,
      source_context: h.source || "hypothesis",
    });
    const rank = rankContactTargetRelevance(target);
    if (rank.rank === "REJECTED") continue;
    out.push({
      ...target,
      relevance_rank: rank.rank,
      relevance_score: rank.score,
      current_role_evidence: h.role_observed_at || h.evidence || null,
      organization_match: true,
      portfolio_relevance: Boolean(orgTarget?.portfolio_hotel_ids?.length),
      hypothesis_only: true,
    });
  }
  return out.slice(0, 5);
}

/**
 * Canonical Contact Intelligence owner-target resolution.
 *
 * @param {string} hotelId
 * @param {object} [opts]
 * @param {object} [opts.cohortCase] — CI12 case row (seed fallback NEVER invents OCG)
 * @param {object} [opts.ownershipSurface]
 * @param {object[]} [opts.person_hypotheses]
 */
export function resolveContactOwnerTarget(hotelId, opts = {}) {
  const surface = opts.ownershipSurface || getDefaultOwnershipSurface();
  const cohort = opts.cohortCase || null;
  const hotel_id = String(hotelId || cohort?.hotel_id || "").trim();

  const anchorResult = hotel_id ? surface.hotelOwnerAnchor(hotel_id) : { ok: false };
  const anchor = anchorResult?.anchor || null;
  const mappedOwnerId = anchor?.primary_owner_entity_id || null;

  // Explicit OWNER_TRUTH_UNKNOWN — do not guess
  if (cohort?.owner_truth_status === "OWNER_TRUTH_UNKNOWN" && !mappedOwnerId) {
    return {
      version: OWNER_TARGET_RESOLVER_VERSION,
      status: OWNER_TARGET_STATUS.OWNER_TARGET_UNRESOLVED,
      hotel_id,
      unresolved_reason: UNRESOLVED_REASON.OWNER_UNRESOLVED,
      unresolved_code: "OWNER_TARGET_UNRESOLVED",
      owner_truth_status: "OWNER_TRUTH_UNKNOWN",
      organization_target: null,
      person_targets: [],
      authority: "ownership_surface",
      invent_ownership: false,
      notes: [
        "Economic owner intentionally unresolved for this case; Contact Intelligence must abstain.",
      ],
    };
  }

  if (!anchorResult?.ok || !mappedOwnerId) {
    // Cohort seed is NOT OCG authority — only annotate partial when tagged PARTIAL
    if (cohort?.owner_entity_id && cohort?.owner_truth_status === "PARTIAL") {
      const orgTarget = createContactOrganizationTarget({
        organization_id: cohort.owner_entity_id,
        canonical_name: cohort.owner_hint || null,
        country: cohort.country || null,
        domain: cohort.seedHints?.owner_website || null,
        portfolio_hotel_ids: [],
        owner_relevance: "HYPOTHESIS_PARTIAL",
        why_relevant: "Cohort PARTIAL owner hypothesis — not OCG-confirmed",
        source: "ci12_cohort_partial",
      });
      return {
        version: OWNER_TARGET_RESOLVER_VERSION,
        status: OWNER_TARGET_STATUS.PARTIAL,
        hotel_id,
        owner_entity_id: cohort.owner_entity_id,
        organization_target: orgTarget,
        person_targets: buildPersonTargetsFromHypotheses(
          orgTarget,
          opts.person_hypotheses || [],
          { hotel_id }
        ),
        authority: "ci12_cohort_partial_not_ocg",
        invent_ownership: false,
        ownership_surface_ok: false,
        notes: [
          "OCG/surface unresolved; using PARTIAL cohort hypothesis for contact targeting only.",
          "Must not write to OCG or Census ownership.",
        ],
      };
    }

    return {
      version: OWNER_TARGET_RESOLVER_VERSION,
      status: OWNER_TARGET_STATUS.OWNER_TARGET_UNRESOLVED,
      hotel_id,
      unresolved_reason: UNRESOLVED_REASON.OWNER_UNRESOLVED,
      unresolved_code: "OWNER_TARGET_UNRESOLVED",
      organization_target: null,
      person_targets: [],
      authority: "ownership_surface",
      invent_ownership: false,
      notes: ["hotelOwnerAnchor failed; Contact Intelligence will not invent owner."],
    };
  }

  const profile = getOwnerPortfolioProfile(mappedOwnerId) || null;
  const portfolioIds = Array.isArray(profile?.hotels)
    ? profile.hotels.map((h) => h.hotel_id || h.airtable_record_id).filter(Boolean)
    : Array.isArray(profile?.portfolio_hotel_ids)
      ? profile.portfolio_hotel_ids
      : [];

  const orgTarget = createContactOrganizationTarget({
    organization_id: mappedOwnerId,
    canonical_name:
      anchor.owner_display_name || profile?.display_name || profile?.name || mappedOwnerId,
    aliases: profile?.aliases || [],
    legal_name: profile?.legal_name || null,
    trade_name: profile?.trade_name || null,
    country: profile?.country || cohort?.country || null,
    domain: profile?.primary_domain || cohort?.seedHints?.owner_website || null,
    domain_confidence: profile?.primary_domain ? "PROFILE" : null,
    parent_relationship: anchor.owner_role || null,
    owner_relevance: "ECONOMIC_OWNER",
    portfolio_hotel_ids: portfolioIds,
    why_relevant: "Canonical owner from ownership surface / OCG",
    source: "ownership_surface.hotelOwnerAnchor",
  });

  return {
    version: OWNER_TARGET_RESOLVER_VERSION,
    status: OWNER_TARGET_STATUS.RESOLVED,
    hotel_id,
    owner_entity_id: mappedOwnerId,
    owner_display_name: orgTarget.canonical_name,
    confidence: anchor.confidence || null,
    organization_target: orgTarget,
    person_targets: buildPersonTargetsFromHypotheses(
      orgTarget,
      opts.person_hypotheses || [],
      { hotel_id }
    ),
    authority: "ownership_surface",
    invent_ownership: false,
    ownership_surface_ok: true,
    anchor,
  };
}

/** Document dual-path consolidation decision for audits. */
export const OWNER_RESOLVER_CONSOLIDATION = Object.freeze({
  OWNER_RESOLVER_A: {
    name: "ownership-surface hotelOwnerAnchor",
    path: "lib/hotel-intelligence/ownership/ownership-surface-v1.js",
    role: "CANONICAL for Contact Intelligence hotel→owner",
  },
  OWNER_RESOLVER_B: {
    name: "ownership-contact-research-handoff researchHotelOwnershipContactPath",
    path: "lib/hotel-intelligence/contact-intelligence/ownership-contact-research-handoff.js",
    role: "EVALUATION / staging research only — must NOT invent OCG",
  },
  OWNER_RESOLVER_C: {
    name: "owner-contact-resolution-v2 resolve_owner_contacts",
    path: "lib/hotel-intelligence/contact-intelligence/owner-contact-resolution-v2/",
    role: "Owner→contact enrichment after org target resolved — not hotel→owner authority",
  },
  canonical_ci_call: "resolveContactOwnerTarget(hotel_id)",
});
