#!/usr/bin/env node
/**
 * Phase 3A.7 — dry-run only demo entitlement plan for Marriott/Hilton/Choice showcase.
 * Does NOT write Airtable. Does NOT touch production client companies.
 *
 * Recommended architecture: separate showcase-demo Company Profiles (future) OR
 * governed demo constellation contexts that swap deep brand link sets from
 * brand_ai_showcase_companies_v1 — still via Phase 2F entitlement resolution.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
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
  "phase3a7-showcase-entitlement-dry-run.json"
);

const cfg = loadShowcaseCompaniesConfig();
const field = MAP_CP_BRANDS_AIRTABLE.brandLinkField;
const brandDemoCompanyId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;

const proposed = ["marriott", "hilton", "choice"].map((key) => {
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
    note: "Do not apply to production client profiles. Prefer new demo CP rows or demo-only context switch.",
  };
});

const currentDemo = {
  TABLE: "Company Profile",
  RECORD: brandDemoCompanyId,
  FIELD: field,
  BEFORE: DEMO_BRAND_PORTFOLIO.map((b) => b.brandId),
  AFTER: "UNCHANGED in Phase 3A.7 — cross-parent validation portfolio retained",
  SHOWCASE_ONLY: true,
  PRODUCTION_CLIENT_IMPACT: "NONE",
  note: "Dealality Brand Demo stays multi-parent for functional QA; not Marriott/Hilton/Choice sales framing.",
};

const report = {
  generatedAt: new Date().toISOString(),
  RECOMMENDED_DEMO_ENTITLEMENT_ARCHITECTURE:
    "Same monitored peer v2 cohort + separate deep Brand entitlements per showcase company (Phase 2F). Showcase config is not an auth bypass. Prefer dedicated Dealality showcase-demo Company Profiles (or demo-only context) linking only that company's portfolio brand IDs. Do not write production client Company Profiles.",
  APPLIED: false,
  AIRTABLE_WRITES: 0,
  PRODUCTION_CLIENT_ENTITLEMENT_WRITES: 0,
  currentDemoPortfolio: currentDemo,
  proposedShowcaseEntitlements: proposed,
  UI_NOTE:
    "No product-client company impersonation control. Future admin/demo control may select showcase company key among governed demo contexts only.",
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
console.log(JSON.stringify(report, null, 2));
