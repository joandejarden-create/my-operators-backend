/**
 * Showcase company portfolio config (Phase 3A.7).
 * NOT an authorization bypass — demo/Company Profile still gates deep access.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { normalizeParentCompany } from "./parent-company-normalize.js";

export const SHOWCASE_COMPANIES_CONFIG_ID = "brand_ai_showcase_companies_v1";
export const SHOWCASE_COMPANIES_VERSION = "1.1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATH = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "brand-ai-showcase-companies-v1.json"
);

export function loadShowcaseCompaniesConfig(filePath = DEFAULT_PATH) {
  const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return {
    ...raw,
    id: raw.id || SHOWCASE_COMPANIES_CONFIG_ID,
    version: String(raw.version || SHOWCASE_COMPANIES_VERSION),
  };
}

export function listShowcaseCompanyKeys(config) {
  const cfg = config || loadShowcaseCompaniesConfig();
  return (cfg.companies || []).map((c) => c.companyKey);
}

export function getShowcaseCompany(companyKey, config) {
  const cfg = config || loadShowcaseCompaniesConfig();
  const key = String(companyKey || "").trim().toLowerCase();
  const company = (cfg.companies || []).find((c) => c.companyKey === key);
  if (!company) {
    return { ok: false, error: `showcase_company_not_found:${companyKey}` };
  }
  return {
    ok: true,
    ...company,
    sharedPeerSetId: cfg.sharedPeerSetId,
    configId: cfg.id,
    configVersion: cfg.version,
  };
}

/**
 * Deep brand IDs for a showcase company key. Does not grant access by itself.
 */
export function getShowcasePortfolioBrandIds(companyKey, config) {
  const company = getShowcaseCompany(companyKey, config);
  if (!company.ok) return company;
  return {
    ok: true,
    companyKey: company.companyKey,
    canonicalCompanyName: company.canonicalCompanyName,
    brandIds: [...(company.brandIds || [])],
    AUTHORIZATION_BYPASS: false,
  };
}

/**
 * Assert every portfolio brand belongs to the expected canonical parent family
 * using provided live brand rows ({ id, parentCompany }).
 */
export function assertShowcasePortfolioParentPurity(companyKey, liveBrandsById, config) {
  const company = getShowcaseCompany(companyKey, config);
  if (!company.ok) return company;
  const expected = normalizeParentCompany(company.canonicalCompanyName).canonical;
  const violations = [];
  for (const brandId of company.brandIds || []) {
    const live = liveBrandsById?.[brandId];
    if (!live) {
      violations.push({ brandId, reason: "brand_not_in_live_map" });
      continue;
    }
    const got = normalizeParentCompany(live.parentCompany).canonical;
    if (got !== expected) {
      violations.push({
        brandId,
        brandName: live.name || live.brandName || null,
        expectedParent: expected,
        observedParent: live.parentCompany || null,
        canonicalObserved: got,
        reason: "parent_mismatch",
      });
    }
  }
  return {
    ok: violations.length === 0,
    companyKey: company.companyKey,
    expectedParent: expected,
    violations,
  };
}
