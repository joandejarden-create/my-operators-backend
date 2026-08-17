/**
 * Material unknowns that could change eligibility / rank / confidence / alignment.
 * Deterministic heuristics from adapted operator + project — does not invent answers.
 */

import { listValue, scalarValue, isKnownPositive } from "./adapters/field-state.js";
import { establishesCurrentGeographicEligibility } from "../operator-intelligence/market-presence.js";

/**
 * @returns {Array<{
 *   id: string,
 *   question: string,
 *   impact: 'eligibility'|'rank'|'confidence'|'alignment',
 *   direction: 'could_improve'|'could_reduce'|'unknown_direction',
 *   phase: 'before_shortlist'|'before_outreach'|'during_outreach'|'before_proposal'|'before_final',
 *   criticality: 'required_before_outreach'|'required_before_final'|'useful_not_critical',
 * }>}
 */
export function listRankingChangeValidations(project, operator) {
  const out = [];
  const country = scalarValue(project?.geography?.country);
  const presence = operator?.geography?.marketPresence || operator?.geography?.presenceRecords || [];
  const strongHere = presence.some(
    (p) =>
      p?.country &&
      country &&
      String(p.country).toLowerCase().includes(String(country).toLowerCase()) &&
      establishesCurrentGeographicEligibility(p.presenceType)
  );

  if (country && !strongHere) {
    out.push({
      id: "confirm_market_presence",
      question: `Confirm qualifying current Market Presence in ${country} (Current Managed / Operating / Regional Office).`,
      impact: "eligibility",
      direction: "could_improve",
      phase: "before_outreach",
      criticality: "required_before_outreach",
    });
  }

  const structures = listValue(operator?.operatingStructures);
  if (!structures.length) {
    out.push({
      id: "confirm_management_structure",
      question: "Confirm supported management structures for this project.",
      impact: "eligibility",
      direction: "could_improve",
      phase: "before_outreach",
      criticality: "required_before_outreach",
    });
  }

  const brands = listValue(operator?.brandsOperated);
  const preferred = listValue(project?.preferredBrands || project?.brandPreferences);
  if (preferred.length && !brands.length) {
    out.push({
      id: "confirm_brand_relationship",
      question: "Verify current brand–operator relationship and approval geography (property-scoped; not global inference).",
      impact: "alignment",
      direction: "could_improve",
      phase: "during_outreach",
      criticality: "required_before_final",
    });
  } else if (brands.length) {
    out.push({
      id: "confirm_project_brand_approval",
      question: "Confirm project-specific brand approval / availability for this asset.",
      impact: "rank",
      direction: "could_improve",
      phase: "during_outreach",
      criticality: "required_before_final",
    });
  }

  const comps = listValue(operator?.comparables);
  if (!comps.length) {
    out.push({
      id: "obtain_comparables",
      question: "Obtain verified comparable assignments for this project type / segment.",
      impact: "confidence",
      direction: "could_improve",
      phase: "before_outreach",
      criticality: "useful_not_critical",
    });
  }

  if (!isKnownPositive(operator?.specialistExperience?.conversion)) {
    out.push({
      id: "verify_conversion_experience",
      question: "Verify conversion / reflag / repositioning experience with evidence.",
      impact: "alignment",
      direction: "could_improve",
      phase: "during_outreach",
      criticality: "useful_not_critical",
    });
  }

  out.push({
    id: "confirm_operator_interest",
    question: "Confirm operator interest and capacity for this specific project.",
    impact: "eligibility",
    direction: "could_reduce",
    phase: "during_outreach",
    criticality: "required_before_outreach",
  });

  out.push({
    id: "project_specific_fees",
    question: "Obtain project-specific fee / commercial proposal (never auto-published from general research).",
    impact: "rank",
    direction: "unknown_direction",
    phase: "before_proposal",
    criticality: "required_before_final",
  });

  out.push({
    id: "regional_team",
    question: "Confirm regional team / in-market operating resources.",
    impact: "confidence",
    direction: "could_improve",
    phase: "during_outreach",
    criticality: "useful_not_critical",
  });

  return out;
}
