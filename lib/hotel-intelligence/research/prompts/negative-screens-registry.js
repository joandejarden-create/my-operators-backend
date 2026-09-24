/**
 * Packet 2.8A — unified Hotel Intelligence research negative screens.
 * Maps template/playbook/planner vocab into one SCREAMING_SNAKE registry.
 */

import { NEGATIVE_SCREENS as PLAYBOOK_NEGATIVE_SCREENS } from "../../research-methods/playbook-schema.js";

export const HI_NEGATIVE_SCREENS_REGISTRY_VERSION = "hi-negative-screens-v1";

/** Canonical screens for Webhound / native HI research prompts. */
export const HI_NEGATIVE_SCREENS = Object.freeze([
  ...PLAYBOOK_NEGATIVE_SCREENS,
  "BRAND_NOT_OPERATOR",
  "SUPERSEDED_EVIDENCE",
  "SPONSOR_NOT_DEED_UBO",
  "ORG_CAPABILITY_NOT_PROPERTY_RELATIONSHIP",
  "LEGACY_LEGAL_NAME_NOT_CURRENT_BRAND_SIGNAL",
  "BRAND_EVENT_NOT_OWNER_SPECIFIC",
  "RESIDUAL_PORTFOLIO_NOT_CURRENT_OPERATOR_PROOF",
  "SEARCH_SNIPPET_NOT_DECISIVE_EVIDENCE",
  "GUEST_PATTERN_NOT_STRUCTURAL_PROOF",
  "ORDER_APPROVAL_IS_NOT_PHYSICAL_COMPLETION",
  "DO_NOT_TREAT_DISSOLVED_ENTITY_AS_EXACT_LEGAL_SELLER_WITHOUT_QUALIFICATION",
  // Development & Project Intelligence screens
  "WRONG_SITE",
  "ADJACENT_PROJECT",
  "SAME_DEVELOPER_DIFFERENT_PROJECT",
  "SIMILAR_PROJECT_NAME",
  "FORMER_PROJECT_NOT_CURRENT",
  "MASTERPLAN_COMPONENT_NOT_EXACT_SITE",
  "BRAND_RUMOR_NOT_AGREEMENT",
  "DEVELOPER_NOT_OWNER",
  "SPONSOR_NOT_DEED_OWNER",
  "APPROVED_NOT_CONSTRUCTING",
  "PERMIT_NOT_PROJECT_COMPLETION",
  "OLD_RENDERING_NOT_CURRENT_PLAN",
  "MASTERPLAN_NOT_EXACT_COMPONENT",
  "ARTICLE_SPECULATION_NOT_DECISIVE",
]);

const ALIASES = Object.freeze({
  wrong_property: "WRONG_PROPERTY",
  adjacent_hotel: "ADJACENT_ASSET",
  adjacent_asset: "ADJACENT_ASSET",
  operator_not_owner: "OPERATOR_NOT_OWNER",
  historical_not_current: "HISTORICAL_NOT_CURRENT",
  announced_not_current: "ANNOUNCED_NOT_CURRENT",
  announced_renovation_assumed_completed: "ANNOUNCED_NOT_CURRENT",
  pre_renovation_as_current: "ANNOUNCED_NOT_CURRENT",
  similar_name_collision: "SIMILAR_NAME_COLLISION",
  title_as_signing_authority: "TITLE_NOT_AUTHORITY",
  title_not_authority: "TITLE_NOT_AUTHORITY",
  source_not_decisive: "SOURCE_NOT_DECISIVE",
  search_snippet_not_decisive_evidence: "SEARCH_SNIPPET_NOT_DECISIVE_EVIDENCE",
  single_review_as_trend: "GUEST_PATTERN_NOT_STRUCTURAL_PROOF",
  brand_level_as_property: "BRAND_EVENT_NOT_OWNER_SPECIFIC",
  operator_level_as_hotel: "ORG_CAPABILITY_NOT_PROPERTY_RELATIONSHIP",
  neighbor_construction: "ADJACENT_ASSET",
  sponsor_control_is_not_deed_ubo: "SPONSOR_NOT_DEED_UBO",
  org_capability_is_not_property_relationship: "ORG_CAPABILITY_NOT_PROPERTY_RELATIONSHIP",
  wrong_site: "WRONG_SITE",
  adjacent_project: "ADJACENT_PROJECT",
  same_developer_different_project: "SAME_DEVELOPER_DIFFERENT_PROJECT",
  brand_rumor_not_agreement: "BRAND_RUMOR_NOT_AGREEMENT",
  developer_not_owner: "DEVELOPER_NOT_OWNER",
  sponsor_not_deed_owner: "SPONSOR_NOT_DEED_OWNER",
  approved_not_constructing: "APPROVED_NOT_CONSTRUCTING",
});

export function normalizeNegativeScreen(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  if (HI_NEGATIVE_SCREENS.includes(s)) return s;
  const upper = s.toUpperCase().replace(/\s+/g, "_");
  if (HI_NEGATIVE_SCREENS.includes(upper)) return upper;
  const aliased = ALIASES[s.toLowerCase()] || ALIASES[upper.toLowerCase()];
  return aliased || upper;
}

export function normalizeNegativeScreens(list = []) {
  const out = [];
  const seen = new Set();
  for (const item of list) {
    const n = normalizeNegativeScreen(item);
    if (!n || seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
}

/** Shared screens injected into every Hotel Intelligence Webhound investigation. */
export const COMMON_HI_NEGATIVE_SCREENS = Object.freeze([
  "WRONG_PROPERTY",
  "ADJACENT_ASSET",
  "SIMILAR_NAME_COLLISION",
  "OPERATOR_NOT_OWNER",
  "BRAND_NOT_OPERATOR",
  "HISTORICAL_NOT_CURRENT",
  "ANNOUNCED_NOT_CURRENT",
  "SUPERSEDED_EVIDENCE",
  "INSUFFICIENT_IDENTITY_MATCH",
  "TITLE_NOT_AUTHORITY",
  "SPONSOR_NOT_DEED_UBO",
  "ORG_CAPABILITY_NOT_PROPERTY_RELATIONSHIP",
  "RESIDUAL_PORTFOLIO_NOT_CURRENT_OPERATOR_PROOF",
  "SEARCH_SNIPPET_NOT_DECISIVE_EVIDENCE",
  "GUEST_PATTERN_NOT_STRUCTURAL_PROOF",
]);
