#!/usr/bin/env node
/**
 * AI Demand Positioning — Execute monitoring period for a property.
 *
 * Usage:
 *   node scripts/run-ai-demand-positioning.mjs --property adp_waterstone_boca_raton --dry-run
 *   node scripts/run-ai-demand-positioning.mjs --property adp_waterstone_boca_raton --apply
 */

import { readFileSync, readdirSync } from "fs";
import { join } from "path";
import { config } from "dotenv";
config();

const args = process.argv.slice(2);
const propertyId = args.find((a, i) => args[i - 1] === "--property") || "adp_waterstone_boca_raton";
const dryRun = !args.includes("--apply");
const providersArg = args.find((a, i) => args[i - 1] === "--providers");
const selectedProviders = providersArg ? providersArg.split(",") : null;

const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
const runtimeDir = join(process.cwd(), "data/ai-demand-positioning/runtime");

// Load property profile
function loadProfile(pid) {
  const files = readdirSync(fixturesDir).filter((f) => f.endsWith(".json"));
  for (const file of files) {
    const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf-8"));
    if (data.propertyId === pid) return data;
  }
  return null;
}

// Dynamic import to handle ESM modules
async function main() {
  const profile = loadProfile(propertyId);
  if (!profile) {
    console.error(`Property not found: ${propertyId}`);
    process.exit(1);
  }

  console.log(`\n=== AI Demand Positioning — Monitoring Period ===`);
  console.log(`Property: ${profile.name} (${profile.city}, ${profile.state})`);
  console.log(`Affiliation: ${profile.affiliation}`);
  console.log(`Mode: ${dryRun ? "DRY RUN" : "LIVE EXECUTION"}`);
  console.log("");

  // Build scenario universe
  const { buildScenarioUniverse } = await import("../lib/ai-demand-positioning/prompt-universe/scenario-registry.js");
  const scenarios = buildScenarioUniverse(profile);
  console.log(`Scenario Universe: ${scenarios.length} scenarios`);
  console.log(`  Standard: ${scenarios.filter((s) => s.source === "standard").length}`);
  console.log(`  Property-specific: ${scenarios.filter((s) => s.source === "property_specific").length}`);
  console.log("");

  // Estimate cost
  const { estimateCost } = await import("../lib/ai-demand-positioning/execution/multi-provider-runner.js");
  const cost = estimateCost(scenarios.length);
  console.log(`Cost Estimate: $${cost.total}`);
  console.log(`  Per provider: ${JSON.stringify(cost.byProvider)}`);
  console.log(`  Total calls: ${cost.scenarioCount * cost.providerCount}`);
  console.log("");

  // Execute
  const { executeMonitoringPeriod } = await import("../lib/ai-demand-positioning/execution/multi-provider-runner.js");
  console.log(`Executing...`);
  const availableProviders = selectedProviders || ["openai", "gemini", "perplexity", "claude"].filter((p) => {
    if (p === "openai") return !!(process.env.OPENAI_API_KEY || process.env.FDD_OPENAI_API_KEY);
    if (p === "gemini") return !!(process.env.GEMINI_API_KEY || process.env.GOOGLE_GENAI_API_KEY || process.env.GOOGLE_GENERATIVE_AI_API_KEY || process.env.FDD_GEMINI_API_KEY);
    if (p === "perplexity") return !!(process.env.PERPLEXITY_API_KEY || process.env.PPLX_API_KEY);
    if (p === "claude") return !!(process.env.ANTHROPIC_API_KEY || process.env.CLAUDE_API_KEY || process.env.FDD_ANTHROPIC_API_KEY);
    return false;
  });
  console.log(`Providers: ${availableProviders.join(", ")}`);
  console.log("");

  const period = await executeMonitoringPeriod({
    propertyId,
    scenarios,
    dryRun,
    providers: availableProviders,
    onProgress: (completed, total) => {
      if (completed % 50 === 0 || completed === total) {
        process.stdout.write(`  Progress: ${completed}/${total}\r`);
      }
    },
  });
  console.log(`\nPeriod: ${period.periodId}`);
  console.log(`Status: ${period.status}`);
  console.log(`Observations: ${period.observations.length}`);

  if (!dryRun) {
    // Parse responses
    const { parsePeriodObservations } = await import("../lib/ai-demand-positioning/execution/response-parser.js");
    const parsed = parsePeriodObservations(period, profile);

    // Compute intelligence
    const { buildOwnerPayload } = await import("../lib/ai-demand-positioning/customer/owner-payload.js");
    const payload = buildOwnerPayload(parsed, scenarios, profile);

    if (payload.ok) {
      console.log(`\n=== Intelligence Summary ===`);
      console.log(`Demand Capture: ${payload.demandCapture.display}`);
      console.log(`Lost Demand: ${payload.lostDemand.totalLost} scenarios`);
      console.log(`Competitive Set: ${payload.competitiveSet.observedCount} observed`);
      console.log(`Reality Gap: ${payload.realityGap.display}`);
      console.log(`White Space: ${payload.whiteSpace.totalOpportunities} opportunities`);
      console.log(`Actions: ${payload.actions.length} recommended`);
    } else {
      console.log(`\nPayload build failed: ${payload.error}`);
    }

    // Save updated period
    const { savePeriod } = await import("../lib/ai-demand-positioning/data-model.js");
    const path = savePeriod(parsed);
    console.log(`\nSaved to: ${path}`);

    if (payload.ok) {
      const {
        buildPublishedSnapshotBundle,
        savePublishedSnapshotBundle,
      } = await import("../lib/ai-demand-positioning/published-snapshot.js");
      const publishedBundle = buildPublishedSnapshotBundle({ period: parsed, profile });
      if (publishedBundle.ok) {
        const saved = savePublishedSnapshotBundle(publishedBundle, { seed: false });
        console.log(`Published snapshot: ${saved.manifestFile}`);
        if (process.env.ADP_AIRTABLE_PUBLISH_APPLY === "true") {
          const { upsertPublishedReportToAirtable } = await import("../lib/ai-demand-positioning/airtable-published-report.js");
          const airtableResult = await upsertPublishedReportToAirtable(publishedBundle, {
            payloadStoreRef: `published/${propertyId}/${publishedBundle.manifest.reportFile}`,
          });
          console.log(`Airtable Live row: ${airtableResult.recordId}`);
        }
      }
    }
  } else {
    console.log(`\n[DRY RUN] No API calls made. Period saved for structure validation.`);
    console.log(`Saved to: data/ai-demand-positioning/runtime/${period.periodId}.json`);
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
