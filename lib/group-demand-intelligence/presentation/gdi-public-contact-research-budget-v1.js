/**
 * Bounded public-contact research budget (founder V2).
 *
 * Meaningful attempts = distinct research strategies / authoritative page fetches,
 * not arbitrary query spam.
 */

export const GDI_PUBLIC_CONTACT_RESEARCH_BUDGET_V1 =
  "gdi-public-contact-research-budget-v1";

export const DEFAULT_PUBLIC_CONTACT_RESEARCH_BUDGET = Object.freeze({
  minAttempts: 2,
  maxAttempts: 6,
  softMaxAttempts: 8,
  stopWhenDirectEmailAndPhone: true,
  stopWhenDirectEmail: false,
  stopWhenUnproductiveConsecutive: 2,
});

/**
 * @param {object} [overrides]
 */
export function resolvePublicContactResearchBudget(overrides = {}) {
  const minAttempts = Math.max(
    1,
    Number(overrides.minAttempts ?? DEFAULT_PUBLIC_CONTACT_RESEARCH_BUDGET.minAttempts),
  );
  const maxAttempts = Math.max(
    minAttempts,
    Number(
      overrides.maxAttempts ??
        overrides.maxQueries ??
        DEFAULT_PUBLIC_CONTACT_RESEARCH_BUDGET.maxAttempts,
    ),
  );
  const softMax = Math.max(
    maxAttempts,
    Number(
      overrides.softMaxAttempts ??
        DEFAULT_PUBLIC_CONTACT_RESEARCH_BUDGET.softMaxAttempts,
    ),
  );
  return {
    version: GDI_PUBLIC_CONTACT_RESEARCH_BUDGET_V1,
    minAttempts,
    maxAttempts,
    softMaxAttempts: softMax,
    stopWhenDirectEmailAndPhone:
      overrides.stopWhenDirectEmailAndPhone !== false,
    stopWhenDirectEmail: overrides.stopWhenDirectEmail === true,
    stopWhenUnproductiveConsecutive: Math.max(
      1,
      Number(
        overrides.stopWhenUnproductiveConsecutive ??
          DEFAULT_PUBLIC_CONTACT_RESEARCH_BUDGET.stopWhenUnproductiveConsecutive,
      ),
    ),
  };
}

/**
 * Decide whether another attempt is allowed.
 */
export function shouldContinuePublicContactResearch({
  budget,
  attemptsUsed = 0,
  consecutiveUnproductive = 0,
  hasDirectEmail = false,
  hasDirectPhone = false,
  exceptionalAuthorize = false,
} = {}) {
  const b = resolvePublicContactResearchBudget(budget || {});
  if (b.stopWhenDirectEmailAndPhone && hasDirectEmail && hasDirectPhone) {
    return { continue: false, reason: "CHANNELS_COMPLETE" };
  }
  if (b.stopWhenDirectEmail && hasDirectEmail) {
    return { continue: false, reason: "DIRECT_EMAIL_FOUND" };
  }
  if (
    consecutiveUnproductive >= b.stopWhenUnproductiveConsecutive &&
    attemptsUsed >= b.minAttempts
  ) {
    return { continue: false, reason: "UNPRODUCTIVE" };
  }
  if (attemptsUsed >= b.maxAttempts) {
    if (exceptionalAuthorize && attemptsUsed < b.softMaxAttempts) {
      return { continue: true, reason: "EXCEPTIONAL_SOFT_MAX" };
    }
    return { continue: false, reason: "MAX_ATTEMPTS" };
  }
  return {
    continue: true,
    reason:
      attemptsUsed < b.minAttempts ? "BELOW_MINIMUM" : "WITHIN_BUDGET",
  };
}

export default {
  GDI_PUBLIC_CONTACT_RESEARCH_BUDGET_V1,
  DEFAULT_PUBLIC_CONTACT_RESEARCH_BUDGET,
  resolvePublicContactResearchBudget,
  shouldContinuePublicContactResearch,
};
