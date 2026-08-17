#!/usr/bin/env node
/**
 * Link the governed Dealality Brand Demo portfolio on Company Profile.
 *
 *   node scripts/link-dealality-brand-demo-portfolio.mjs --dry-run
 *   node scripts/link-dealality-brand-demo-portfolio.mjs --apply --replace
 *
 * Default --apply merges DEMO_BRAND_PORTFOLIO IDs into the current link field.
 * --replace sets the Brand Demo link field exactly to DEMO_BRAND_PORTFOLIO
 * (demo-only; removes Comfort / Radisson RED from Brand Demo if present).
 *
 * Does not invent brands. Does not touch production client companies.
 */
import "../load-env.js";
import Airtable from "airtable";
import {
  DEMO_BRAND_PORTFOLIO,
  DEMO_BRAND_PORTFOLIO_PHASE3A_PRIOR,
  MAP_DEMO_STAKEHOLDER_COMPANIES,
} from "../lib/dealality/demo-stakeholder-workspace.js";
import { MAP_CP_BRANDS_AIRTABLE } from "../lib/company-profile-brands-backfill.js";

const apply = process.argv.includes("--apply");
const replace = process.argv.includes("--replace");
const dryRun = !apply || process.argv.includes("--dry-run");

const companyId = MAP_DEMO_STAKEHOLDER_COMPANIES.Brand.companyId;
const brandIds = DEMO_BRAND_PORTFOLIO.map((b) => b.brandId);
const field = MAP_CP_BRANDS_AIRTABLE.brandLinkField;

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY / AIRTABLE_BASE_ID");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);
const rec = await base("Company Profile").find(companyId);
const current = Array.isArray(rec.fields[field]) ? [...rec.fields[field]] : [];
const next = replace ? [...brandIds] : [...new Set([...current, ...brandIds])];
const added = next.filter((id) => !current.includes(id));
const removed = current.filter((id) => !next.includes(id));

console.log(
  JSON.stringify(
    {
      mode: apply && !dryRun ? "apply" : "dry-run",
      replace,
      companyId,
      companyName: rec.fields["Company Name"],
      field,
      demoPortfolio: DEMO_BRAND_PORTFOLIO,
      priorPhase3aPortfolio: DEMO_BRAND_PORTFOLIO_PHASE3A_PRIOR,
      current,
      next,
      added,
      removed,
      PRODUCTION_CLIENT_WRITES: 0,
      note: "Dealality Brand Demo only",
    },
    null,
    2
  )
);

if (!apply || dryRun) {
  console.log("Dry-run only. Re-run with --apply [--replace] to write.");
  process.exit(0);
}

if (!added.length && !removed.length) {
  console.log("No Airtable write needed — demo portfolio already matches.");
  process.exit(0);
}

await base("Company Profile").update(companyId, { [field]: next });
console.log("Applied. Brands You Operate / Support updated on Dealality Brand Demo.");
