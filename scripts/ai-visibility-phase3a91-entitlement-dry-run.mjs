#!/usr/bin/env node
/**
 * Phase 3A.9.1 — dry-run only demo entitlement plan including IHG.
 * Does NOT write Airtable. Does NOT touch production client companies.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
  listShowcaseCompanyKeys,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import {
  DEMO_BRAND_PORTFOLIO,
  MAP_DEMO_STAKEHOLDER_COMPANIES,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import { MAP_CP_BRANDS_AIRTABLE } from "../lib/company-profile-brands-backfill.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const outPath = path.join(
  __dirname,
  "..",
  "data",
  "ai-visibility",
  "phase3a91-ihg-showcase-entitlement-dry-run.json"
);

const cfg = loadShowcaseCompaniesConfig();
const field = MAP_CP_BRANDS_AIRTABLE.brandLinkField;
const brandDemoCompanyId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;

const proposed = listShowcaseCompanyKeys(cfg).map((key) => {
  const company = getShowcaseCompany(key, cfg);
  return {
    TABLE: "Company Profile",
    RECORD: `FUTURE_SHOWCASE_DEMO_${key.toUpperCase()}_OR_CONTEXT`,
    FIELD: field,
    BEFORE: "(no showcase-specific Company Profile yet)",
    AFTER: company.brandIds,
    SHOWCASE_ONLY: true,
    PRODUCTION_CLIENT_IMPACT: "NONE",
    canonicalCompanyName: company.canonicalCompanyName,
    brandCount: (company.brandIds || []).length,
    note: "Do not apply to production client profiles. Prefer new demo CP rows or demo-only context switch.",
  };
});

const report = {
  generatedAt: new Date().toISOString(),
  IHG_DEMO_ENTITLEMENT_PLAN: {
    architecture:
      "One centrally monitored peer-v2 dataset + separate deep Brand entitlements per showcase company (Marriott/Hilton/Choice/IHG) via Phase 2F. Showcase config is not an auth bypass.",
    ihgDeepBrandIds: getShowcaseCompany("ihg", cfg).brandIds,
    peerBenchmarkSetId: cfg.sharedPeerSetId,
    duplicateProviderRunsRequired: false,
    productionClientWrites: false,
  },
  RECOMMENDED_DEMO_ENTITLEMENT_ARCHITECTURE:
    "Same monitored peer v2 cohort + separate deep Brand entitlements per showcase company (Phase 2F). Prefer dedicated Dealality showcase-demo Company Profiles (or demo-only context) linking only that company's portfolio brand IDs. Do not write production client Company Profiles.",
  APPLIED: false,
  AIRTABLE_WRITES: 0,
  PRODUCTION_CLIENT_ENTITLEMENT_WRITES: 0,
  currentDemoPortfolio: {
    TABLE: "Company Profile",
    RECORD: brandDemoCompanyId,
    FIELD: field,
    BEFORE: DEMO_BRAND_PORTFOLIO.map((b) => b.brandId),
    AFTER: "UNCHANGED — cross-parent validation portfolio retained",
    SHOWCASE_ONLY: true,
    PRODUCTION_CLIENT_IMPACT: "NONE",
  },
  proposedShowcaseEntitlements: proposed,
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
console.log(JSON.stringify({ companies: listShowcaseCompanyKeys(cfg), APPLIED: false, AIRTABLE_WRITES: 0 }, null, 2));
console.log(`Wrote ${outPath}`);
