#!/usr/bin/env node
/**
 * QA: dump companies snapshot for sample deal (requires .env Airtable credentials).
 *   node scripts/qa-oas-companies-sample-deal.mjs [dealId]
 */
import { config } from "dotenv";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { fetchDealScoringContext } from "../api/my-deals.js";
import { buildOperatorAlignmentCompaniesSnapshot } from "../lib/operator-alignment-company-utils.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
config({ path: join(__dirname, "..", ".env") });

const dealId = process.argv[2] || "recIeGRZP21udmTnt";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;

if (!baseId || !apiKey) {
  console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY in .env");
  process.exit(1);
}

const ctx = await fetchDealScoringContext(baseId, apiKey, dealId);
if (!ctx) {
  console.error("Deal not found:", dealId);
  process.exit(1);
}

const payload = await buildOperatorAlignmentCompaniesSnapshot(dealId, {
  dealFields: ctx.dealFields,
  locationData: ctx.locationData,
  mpData: ctx.mpData,
  siData: ctx.siData,
});

const summary = {
  dealId,
  mode: payload.mode,
  companiesAvailable: payload.companiesAvailable,
  gatingReason: payload.gatingReason || null,
  companiesCount: (payload.companiesForConsideration || []).length,
  dataCompletenessSummary: payload.dataCompletenessSummary,
  dataGapsCount: (payload.dataGaps || []).length,
  suggestedWorkflowActionsCount: (payload.suggestedWorkflowActions || []).length,
  sampleCompanyNames: (payload.companiesForConsideration || []).slice(0, 5).map((c) => c.operatorName),
};

console.log(JSON.stringify(summary, null, 2));
