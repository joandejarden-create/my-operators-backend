/**
 * GDI readiness/visibility convergence V1 tests.
 *   node scripts/test-gdi-readiness-visibility-convergence-v1.mjs
 *   node scripts/test-gdi-readiness-visibility-convergence-v1.mjs --live
 */
import "../load-env.js";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  filterCustomerFacingOpportunities,
  isCustomerFacingOpportunity,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  applyGdiSummaryEnrichment,
  evaluateGdiSummaryQuality,
} from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import {
  stampLegacyVisibilityPreserved,
  clearLegacyVisibilityPreserved,
  isLegacyVisibilityCompatible,
} from "../lib/group-demand-intelligence/legacy-visibility-compatibility-v1.js";
import { clearLegacyVisibilityPreserved as clearViaEnrichImport } from "../lib/group-demand-intelligence/legacy-visibility-compatibility-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/readiness-visibility-convergence-v1"
);
const LIVE = process.argv.includes("--live");

// Summary enrichment upgrades THIN title-dup when facts exist
{
  const thin = {
    title: "76th Annual Meeting",
    organizationName: "Society for the Study of Social Problems",
    eventStartDate: "2027-08-01",
    eventEndDate: "2027-08-31",
    destinationStatus: "New York City / Times Square",
    opportunityType: "OVERFLOW_HOUSING",
    lodgingEvidence: { overflowMentioned: true, roomBlockMentioned: true },
    summaryWhat: "76th Annual Meeting",
  };
  assert.equal(evaluateGdiSummaryQuality(thin).quality, "THIN");
  const pkt = applyGdiSummaryEnrichment(thin, { force: true });
  assert.ok(
    pkt.result.summaryQuality === "STRONG" ||
      pkt.result.summaryQuality === "ADEQUATE",
    pkt.result.summaryQuality
  );
}

// Legacy stamp is temporary and clearable; new path clears it
{
  const o = stampLegacyVisibilityPreserved(
    { id: "x", title: "t" },
    { reason: "test" }
  );
  assert.equal(isLegacyVisibilityCompatible(o), true);
  assert.equal(isLegacyVisibilityCompatible(clearLegacyVisibilityPreserved(o)), false);
  assert.equal(isLegacyVisibilityCompatible(clearViaEnrichImport(o)), false);
}

// Hilton DQ cannot be re-exposed via legacy stamp
{
  const hilton = stampLegacyVisibilityPreserved(
    {
      id: "gdi_opp_hilton_noise",
      title: "How To Manage The Paper — EXHIBITOR BLOCK",
      organizationName: "Noise Org",
      priority: "WATCHLIST",
    },
    { reason: "should_not_expose" }
  );
  assert.equal(
    isCustomerFacingOpportunity(hilton, { nowDate: "2026-09-28" }),
    false
  );
}

// Artifact freeze expectations
{
  const resultsPath = path.join(OUT, "HOTEL_RESULTS.json");
  const classPath = path.join(OUT, "SIXTY_THREE_CLASSIFICATION.json");
  assert.ok(fs.existsSync(resultsPath), "HOTEL_RESULTS missing — run convergence script");
  assert.ok(fs.existsSync(path.join(OUT, "VISIBLE_CORPUS_BEFORE.json")));
  const results = JSON.parse(fs.readFileSync(resultsPath, "utf8"));
  const cls = JSON.parse(fs.readFileSync(classPath, "utf8"));
  assert.equal(results.RENAISSANCE.finalVisible, 11);
  assert.equal(results.RENAISSANCE.strictAfter, 11);
  assert.equal(results.WATERSTONE.finalVisible, 15);
  assert.equal(results.WATERSTONE.strictAfter, 15);
  assert.equal(results.BETHESDA.finalVisible, 37);
  assert.equal(results.BETHESDA.strictAfter, 37);
  assert.equal(results.HILTON.finalVisible, 0);
  assert.equal(cls.byClass.STRICT_READY, 63);
  assert.equal(cls.total, 63);
  assert.equal(results.RENAISSANCE.legacyPreserved, 0);
  assert.equal(results.RENAISSANCE.priorityCounts.HIGH_PRIORITY, 0);
  assert.equal(results.RENAISSANCE.priorityCounts.MEDIUM_PRIORITY, 7);
  assert.equal(results.RENAISSANCE.priorityCounts.WATCHLIST, 4);
}

console.log("unit convergence OK");

if (!LIVE) {
  console.log("PASS (unit; use --live for Airtable reload)");
  process.exit(0);
}

const { loadOpportunitiesCanonical } = await import(
  "../lib/group-demand-intelligence/opportunity-persistence.js"
);
const { filterSalespersonView } = await import(
  "../lib/group-demand-intelligence/opportunity-factory.js"
);
const { applyLiveCommercialQuality } = await import(
  "../lib/group-demand-intelligence/live-commercial-quality-v1.js"
);

const now = new Date().toISOString().slice(0, 10);
const expect = {
  BETHESDA: ["recLuxvwwxID7U2B8", 37],
  RENAISSANCE: ["recG66DQJKP2c0UNh", 11],
  WATERSTONE: ["recgMYovrrZDJMqzX", 15],
  HILTON: ["rec35fExUxCClpOP6", 0],
};
for (const [key, [hpc, n]] of Object.entries(expect)) {
  const doc = await loadOpportunitiesCanonical(hpc);
  const api = filterCustomerFacingOpportunities(
    filterSalespersonView(
      (doc.opportunities || []).map((o) =>
        applyLiveCommercialQuality(o, { nowDate: now })
      )
    ),
    { nowDate: now }
  );
  assert.equal(api.length, n, `${key} api=${api.length}`);
  const strict = api.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: now }).ok)
    .length;
  assert.equal(strict, n, `${key} strict=${strict}`);
  assert.equal(api.filter((o) => o.legacyVisibilityPreserved).length, 0);
  console.log(`[live] ${key} api=${api.length} strict=${strict}`);
}

console.log("PASS (live)");
