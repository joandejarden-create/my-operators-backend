/**
 * Contact Intelligence V1 — ContactTarget (who are we trying to reach?).
 */

import { ROLE_CATEGORY, RELEVANCE_RANK, CONTACT_SUBJECT_KIND } from "./vocabulary.js";

export const CONTACT_TARGET_VERSION = "contact-target-v1";

const ROLE_PRIORITY = Object.freeze({
  [ROLE_CATEGORY.OWNER_PRINCIPAL]: 100,
  [ROLE_CATEGORY.CHAIRMAN_CEO]: 95,
  [ROLE_CATEGORY.DEVELOPMENT_EXECUTIVE]: 90,
  [ROLE_CATEGORY.ACQUISITIONS_EXECUTIVE]: 88,
  [ROLE_CATEGORY.INVESTMENT_EXECUTIVE]: 85,
  [ROLE_CATEGORY.ASSET_MANAGEMENT_EXECUTIVE]: 80,
  [ROLE_CATEGORY.PORTFOLIO_EXECUTIVE]: 78,
  [ROLE_CATEGORY.REAL_ESTATE_EXECUTIVE]: 75,
  [ROLE_CATEGORY.OWNER_RELATIONS]: 70,
  [ROLE_CATEGORY.DEVELOPMENT_CONTACT]: 65,
  [ROLE_CATEGORY.OPERATOR_SIDE]: 20,
  [ROLE_CATEGORY.OTHER]: 10,
});

/**
 * @param {object} partial
 */
export function createContactTarget(partial = {}) {
  const target_type = String(partial.target_type || CONTACT_SUBJECT_KIND.PERSON).toUpperCase();
  const role_category = String(partial.role_category || ROLE_CATEGORY.OTHER).toUpperCase();
  const priority =
    partial.priority != null
      ? Number(partial.priority)
      : ROLE_PRIORITY[role_category] ?? ROLE_PRIORITY[ROLE_CATEGORY.OTHER];

  return {
    target_version: CONTACT_TARGET_VERSION,
    target_type,
    organization_id: partial.organization_id || null,
    person_id: partial.person_id || null,
    hotel_id: partial.hotel_id || null,
    role_category,
    priority,
    why_relevant: partial.why_relevant || null,
    current_role_required: partial.current_role_required !== false,
    source_context: partial.source_context || null,
    display_name: partial.display_name || null,
    title: partial.title || null,
  };
}

/**
 * Deterministic relevance rank for owner/development targets.
 * Rejects HR/marketing/admin-only people unless explicitly allowed.
 */
export function rankContactTargetRelevance(target, { allowOperatorSide = false } = {}) {
  const role = String(target?.role_category || ROLE_CATEGORY.OTHER).toUpperCase();
  const title = String(target?.title || "").toLowerCase();

  if (/hr\b|human resources|marketing|recrui|talent|admin\b|receptionist|concierge/.test(title)) {
    return {
      rank: RELEVANCE_RANK.REJECTED,
      reason: "PERSON_NOT_DECISION_RELEVANT",
      score: 0,
    };
  }

  if (role === ROLE_CATEGORY.OPERATOR_SIDE && !allowOperatorSide) {
    return {
      rank: RELEVANCE_RANK.REJECTED,
      reason: "OPERATOR_PERSON_NOT_OWNER_PERSON",
      score: ROLE_PRIORITY[ROLE_CATEGORY.OPERATOR_SIDE],
    };
  }

  const score = ROLE_PRIORITY[role] ?? ROLE_PRIORITY[ROLE_CATEGORY.OTHER];
  if (score >= 90) return { rank: RELEVANCE_RANK.PRIMARY_CONTACT, reason: null, score };
  if (score >= 65) return { rank: RELEVANCE_RANK.SECONDARY_CONTACT, reason: null, score };
  if (target?.target_type === CONTACT_SUBJECT_KIND.ORGANIZATION) {
    return { rank: RELEVANCE_RANK.ORGANIZATION_FALLBACK, reason: null, score: 50 };
  }
  return { rank: RELEVANCE_RANK.ORGANIZATION_FALLBACK, reason: null, score };
}

/**
 * Infer role category from title heuristics (AI may refine later; never invents contacts).
 */
export function inferRoleCategoryFromTitle(title) {
  const t = String(title || "").toLowerCase();
  if (!t) return ROLE_CATEGORY.OTHER;
  if (/chairman|presidente del consejo|board chair/.test(t)) return ROLE_CATEGORY.CHAIRMAN_CEO;
  if (/\bceo\b|chief executive|director general|managing director/.test(t)) return ROLE_CATEGORY.CHAIRMAN_CEO;
  if (/owner|principal|founder|propietario/.test(t)) return ROLE_CATEGORY.OWNER_PRINCIPAL;
  if (/develop|desarrollo|expansion|new build/.test(t)) return ROLE_CATEGORY.DEVELOPMENT_EXECUTIVE;
  if (/acquisit|compra|m&a|transaction/.test(t)) return ROLE_CATEGORY.ACQUISITIONS_EXECUTIVE;
  if (/invest|capital|private equity|fondo/.test(t)) return ROLE_CATEGORY.INVESTMENT_EXECUTIVE;
  if (/asset manag|gesti[oó]n de activos/.test(t)) return ROLE_CATEGORY.ASSET_MANAGEMENT_EXECUTIVE;
  if (/portfolio|portafolio/.test(t)) return ROLE_CATEGORY.PORTFOLIO_EXECUTIVE;
  if (/real estate|bienes ra[ií]ces|inmobiliaria/.test(t)) return ROLE_CATEGORY.REAL_ESTATE_EXECUTIVE;
  if (/owner relation|franchisee|franchise/.test(t)) return ROLE_CATEGORY.OWNER_RELATIONS;
  if (/general manager|gm\b|hotel manager|operations|ops\b/.test(t)) return ROLE_CATEGORY.OPERATOR_SIDE;
  return ROLE_CATEGORY.OTHER;
}

export { ROLE_PRIORITY };
