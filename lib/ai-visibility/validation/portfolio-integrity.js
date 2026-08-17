/**
 * Portfolio integrity checks for showcase companies.
 * Peer set = comparison universe only; portfolio subjects = entitled brands only.
 */

import {
  loadShowcaseCompaniesConfig,
  listShowcaseCompanyKeys,
  getShowcaseCompany,
  getShowcasePortfolioBrandIds,
} from "../brand-ai-showcase-companies.js";
import { loadPeerSetConfig } from "../peer-sets.js";

export const PORTFOLIO_INTEGRITY_VERSION = "ai_intelligence_portfolio_integrity_v1";

const SHOWCASE_KEYS = Object.freeze(["marriott", "hilton", "ihg", "choice"]);

function peerEntityIds(peerSetId) {
  if (!peerSetId) return new Set();
  const cfg = loadPeerSetConfig();
  const set = (cfg.peerSets || []).find((p) => p.peerSetId === peerSetId);
  return new Set((set?.entityIds || []).map(String));
}

/**
 * @param {object} [options]
 */
export function runPortfolioIntegrityGate(options = {}) {
  const showcase = options.showcase || loadShowcaseCompaniesConfig();
  const peerSetId = showcase.sharedPeerSetId;
  const peerIds = peerEntityIds(peerSetId);

  const companies = {};
  const failures = [];
  /** @type {Map<string, string>} */
  const allPortfolioIds = new Map();

  for (const key of SHOWCASE_KEYS) {
    const company = getShowcaseCompany(key, showcase);
    if (!company.ok) {
      companies[key.toUpperCase()] = { status: "FAIL", detail: company.error };
      failures.push({ type: "portfolio_company_missing", company: key });
      continue;
    }
    const portfolio = getShowcasePortfolioBrandIds(key, showcase);
    const brandIds = portfolio.ok ? portfolio.brandIds : [];
    const issues = [];

    for (const id of brandIds) {
      if (allPortfolioIds.has(id) && allPortfolioIds.get(id) !== key) {
        issues.push(`cross_portfolio_brand:${id}`);
        failures.push({
          type: "cross_portfolio_leakage",
          company: key,
          brandId: id,
          alsoIn: allPortfolioIds.get(id),
        });
      }
      allPortfolioIds.set(id, key);
    }

    // Portfolio subjects must not equal the full peer set (peer is comparison universe)
    if (peerIds.size && brandIds.length && brandIds.every((id) => peerIds.has(id))) {
      const peerOnlyCompetitors = [...peerIds].filter((id) => !brandIds.includes(id));
      if (peerOnlyCompetitors.length === 0 && brandIds.length === peerIds.size) {
        issues.push("portfolio_equals_peer_universe");
        failures.push({
          type: "portfolio_peer_collapse",
          company: key,
          detail: "Portfolio brands identical to full peer set",
        });
      }
    }

    companies[key.toUpperCase()] = {
      status: issues.length ? "FAIL" : "PASS",
      brandCount: brandIds.length,
      brandIds,
      peerSetId: peerSetId || null,
      issues,
    };
  }

  const keys = listShowcaseCompanyKeys(showcase);
  for (const key of SHOWCASE_KEYS) {
    if (!keys.includes(key)) {
      // already flagged via getShowcaseCompany
    }
  }

  return {
    status: failures.length === 0 ? "PASS" : "FAIL",
    failures: failures.length,
    companies,
    crossPortfolioLeakage: failures.some((f) => f.type === "cross_portfolio_leakage"),
    peerSetId: peerSetId || null,
    peerEntityCount: peerIds.size,
    version: PORTFOLIO_INTEGRITY_VERSION,
  };
}
