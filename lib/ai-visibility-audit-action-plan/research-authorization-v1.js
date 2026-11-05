/**
 * AVA research authorization gate.
 * Payload fields alone (paidProviderUsed / paidResearchUsd) are not authority.
 */
export const AVA_RESEARCH_AUTHORIZATION_CONTRACT =
  "AVA_RESEARCH_AUTHORIZATION_V1";

const PAID_PROVIDER_NAMES = Object.freeze([
  "ahrefs",
  "semrush",
  "majestic",
  "moz",
  "similarweb",
]);

/**
 * Resolve whether paid backlink/research providers may be used.
 * Defaults to denied unless explicit founder authorization env/meta is set.
 */
export function resolveAvaResearchAuthorization(options = {}) {
  const envFlag =
    String(process.env.AVA_PAID_RESEARCH_AUTHORIZED || "").trim() === "1" ||
    String(process.env.AVA_PAID_RESEARCH_AUTHORIZED || "")
      .trim()
      .toLowerCase() === "true";
  const metaFlag = options.metaPaidResearchAuthorized === true;
  const founderFlag = options.founderPaidResearchAuthorized === true;
  const authorized = envFlag || metaFlag || founderFlag;
  const budgetUsd = Number(
    options.authorizedBudgetUsd ??
      process.env.AVA_PAID_RESEARCH_BUDGET_USD ??
      0
  );
  return {
    contract: AVA_RESEARCH_AUTHORIZATION_CONTRACT,
    paidResearchAuthorized: authorized === true,
    authorizedBudgetUsd: Number.isFinite(budgetUsd) ? Math.max(0, budgetUsd) : 0,
    spentUsd: Number(options.spentUsd || 0) || 0,
    allowedProviders: authorized ? [...PAID_PROVIDER_NAMES] : [],
    deniedProviders: authorized ? [] : [...PAID_PROVIDER_NAMES],
    reason: authorized
      ? "Founder-authorized paid research enabled for this run."
      : "Paid backlink providers (Ahrefs/Semrush/Majestic/etc.) are not authorized. Public/partial research only.",
  };
}

export function assertPaidProviderAllowed(providerName, auth) {
  const name = String(providerName || "").toLowerCase();
  if (!PAID_PROVIDER_NAMES.includes(name)) {
    return { ok: true, paid: false };
  }
  if (!auth?.paidResearchAuthorized) {
    const err = new Error(`AVA_PAID_RESEARCH_NOT_AUTHORIZED:${name}`);
    err.code = "AVA_PAID_RESEARCH_NOT_AUTHORIZED";
    throw err;
  }
  if ((auth.spentUsd || 0) >= (auth.authorizedBudgetUsd || 0)) {
    const err = new Error(`AVA_PAID_RESEARCH_BUDGET_EXCEEDED:${name}`);
    err.code = "AVA_PAID_RESEARCH_BUDGET_EXCEEDED";
    throw err;
  }
  return { ok: true, paid: true };
}

export function applyResearchAuthorizationToDigitalAuthority(block, auth) {
  const authorized = auth?.paidResearchAuthorized === true;
  return {
    ...block,
    paidProviderUsed: authorized && block?.paidProviderUsed === true,
    paidResearchUsd: authorized ? Number(block?.paidResearchUsd || 0) || 0 : 0,
    researchMode: authorized
      ? block?.researchMode || "PAID_AUTHORIZED"
      : "PUBLIC_PARTIAL",
    researchAuthorization: {
      paidResearchAuthorized: authorized,
      reason: auth?.reason,
      authorizedBudgetUsd: auth?.authorizedBudgetUsd || 0,
      spentUsd: auth?.spentUsd || 0,
    },
  };
}

export { PAID_PROVIDER_NAMES };
