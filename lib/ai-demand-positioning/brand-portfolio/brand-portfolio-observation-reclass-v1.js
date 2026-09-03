/**
 * Classify historical non-neutral observations for Brand & Portfolio reuse eligibility.
 * Does NOT silently relabel poor prompts.
 */

import { PORTFOLIO_TYPES } from "./brand-portfolio-position-contract-v1.js";
import { getPortfolioMapping } from "./brand-portfolio-affiliation-mapping-v1.js";
import { tokenPresent } from "../measurement-assurance/prompt-bias-detection-v1.js";

export const RECLASS_BUCKETS = Object.freeze({
  VALID_BRAND_PORTFOLIO_DEMAND: "VALID_BRAND_PORTFOLIO_DEMAND",
  INVALID_AFFILIATION: "INVALID_AFFILIATION",
  PROPERTY_SPECIFIC_OTHER: "PROPERTY_SPECIFIC_OTHER",
  NEEDS_RERUN_GOVERNED_TEMPLATE: "NEEDS_RERUN_GOVERNED_TEMPLATE",
});

/**
 * Deterministic eligibility for BRAND_PORTFOLIO_PROMPT_ELIGIBILITY_INTEGRITY (historical).
 */
export function classifyHistoricalPromptForBrandPortfolio({
  propertyId,
  scenarioId,
  exactPrompt,
  scenarioSource,
}) {
  const mapping = getPortfolioMapping(propertyId);
  const prompt = String(exactPrompt || "");
  const defects = [];

  if (!mapping) {
    return {
      bucket: RECLASS_BUCKETS.PROPERTY_SPECIFIC_OTHER,
      defects: ["no_portfolio_mapping"],
      proposedLensId: null,
      proposedPortfolioType: null,
    };
  }

  // NOHO / wrong Hyatt — never reusable
  if (propertyId === "adp_now_now_noho") {
    if (tokenPresent(prompt, "hyatt") || tokenPresent(prompt, "world of hyatt")) {
      return {
        bucket: RECLASS_BUCKETS.INVALID_AFFILIATION,
        defects: ["PROFILE_AFFILIATION_CONTAMINATION", "hyatt_constraint_on_independent"],
        proposedLensId: null,
        proposedPortfolioType: null,
        goldCase: "noho_hyatt",
      };
    }
  }

  // Explicit brand/collection/loyalty constraints matching governed affiliation
  const checks = [];

  if (propertyId === "adp_waterstone_boca_raton" || propertyId === "adp_hotel_phillips_kansas_city") {
    if (tokenPresent(prompt, "curio collection")) {
      checks.push({
        bucket: RECLASS_BUCKETS.VALID_BRAND_PORTFOLIO_DEMAND,
        proposedLensId: "curio_collection",
        proposedPortfolioType: PORTFOLIO_TYPES.COLLECTION_PORTFOLIO,
      });
    } else if (
      tokenPresent(prompt, "hilton honors") ||
      tokenPresent(prompt, "honors points") ||
      (tokenPresent(prompt, "hilton") && tokenPresent(prompt, "honors"))
    ) {
      checks.push({
        bucket: RECLASS_BUCKETS.VALID_BRAND_PORTFOLIO_DEMAND,
        proposedLensId: "hilton_honors",
        proposedPortfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
      });
    } else if (tokenPresent(prompt, "hilton") && !tokenPresent(prompt, "curio")) {
      // Soft Hilton mention without clear collection vs loyalty lens
      checks.push({
        bucket: RECLASS_BUCKETS.NEEDS_RERUN_GOVERNED_TEMPLATE,
        proposedLensId: null,
        proposedPortfolioType: null,
        defects: ["ambiguous_hilton_constraint_needs_lens_split"],
      });
    }
  }

  if (propertyId === "adp_renaissance_times_square") {
    const hasRenaissance = tokenPresent(prompt, "renaissance");
    const hasBonvoy =
      tokenPresent(prompt, "bonvoy") ||
      tokenPresent(prompt, "marriott bonvoy") ||
      tokenPresent(prompt, "marriott-affiliated") ||
      tokenPresent(prompt, "marriott");
    if (hasRenaissance && hasBonvoy) {
      checks.push({
        bucket: RECLASS_BUCKETS.NEEDS_RERUN_GOVERNED_TEMPLATE,
        defects: ["mixed_hard_brand_and_loyalty_in_one_prompt"],
        proposedLensId: null,
        proposedPortfolioType: null,
      });
    } else if (hasRenaissance) {
      checks.push({
        bucket: RECLASS_BUCKETS.VALID_BRAND_PORTFOLIO_DEMAND,
        proposedLensId: "renaissance",
        proposedPortfolioType: PORTFOLIO_TYPES.HARD_BRAND_PORTFOLIO,
      });
    } else if (hasBonvoy) {
      checks.push({
        bucket: RECLASS_BUCKETS.VALID_BRAND_PORTFOLIO_DEMAND,
        proposedLensId: "marriott_bonvoy",
        proposedPortfolioType: PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
      });
    }
  }

  if (
    propertyId === "adp_cambridge_beaches_bermuda" ||
    propertyId === "adp_now_now_noho"
  ) {
    if (
      tokenPresent(prompt, "independent") &&
      (tokenPresent(prompt, "not affiliated") || tokenPresent(prompt, "independent"))
    ) {
      checks.push({
        bucket: RECLASS_BUCKETS.NEEDS_RERUN_GOVERNED_TEMPLATE,
        defects: ["independent_constraint_present_but_independent_methodology_pending"],
        proposedLensId: "independent_positioning",
        proposedPortfolioType: PORTFOLIO_TYPES.INDEPENDENT_POSITIONING,
        notes: "Candidate Independent Positioning — methodology not yet founded; do not migrate to production section yet",
      });
    }
  }

  if (checks.length) {
    const best = checks[0];
    return {
      bucket: best.bucket,
      defects: best.defects || defects,
      proposedLensId: best.proposedLensId,
      proposedPortfolioType: best.proposedPortfolioType,
      notes: best.notes || null,
      scenarioId,
      scenarioSource,
    };
  }

  // Property-specific without portfolio constraint
  if (scenarioSource === "property_specific") {
    return {
      bucket: RECLASS_BUCKETS.PROPERTY_SPECIFIC_OTHER,
      defects: ["no_portfolio_constraint_in_prompt"],
      proposedLensId: null,
      proposedPortfolioType: null,
      scenarioId,
      scenarioSource,
    };
  }

  return {
    bucket: RECLASS_BUCKETS.PROPERTY_SPECIFIC_OTHER,
    defects: ["unclassified_non_portfolio"],
    proposedLensId: null,
    proposedPortfolioType: null,
    scenarioId,
    scenarioSource,
  };
}
