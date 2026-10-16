#!/usr/bin/env node
/**
 * AI Demand Positioning — Execute monitoring period for a property.
 *
 * Usage:
 *   node scripts/run-ai-demand-positioning.mjs --property adp_waterstone_boca_raton --dry-run
 *   node scripts/run-ai-demand-positioning.mjs --property adp_waterstone_boca_raton --apply
 *   node scripts/run-ai-demand-positioning.mjs --property adp_renaissance_times_square --apply --providers openai,gemini,perplexity,claude
 *
 * Live --apply reserves against the ADP authorization ledger (fail-closed if missing),
 * then certifies + publishes via publishExistingHotelAdpSnapshot when certification is required.
 */

import { readFileSync, readdirSync } from "fs";
import { join } from "path";
import { config } from "dotenv";
config();

const args = process.argv.slice(2);
const propertyId = args.find((a, i) => args[i - 1] === "--property") || "adp_waterstone_boca_raton";
const dryRun = !args.includes("--apply");
const providersArg = args.find((a, i) => args[i - 1] === "--providers");
const selectedProviders = providersArg ? providersArg.split(",").map((s) => s.trim()).filter(Boolean) : null;
const skipBudget = args.includes("--skip-budget-gate");
const resumeArg = args.find((a, i) => args[i - 1] === "--resume");
const autoResume = args.includes("--resume") && !resumeArg;
const resumePeriodId = resumeArg && !resumeArg.startsWith("--") ? resumeArg : null;

const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");

function loadProfile(pid) {
  const files = readdirSync(fixturesDir).filter((f) => f.endsWith(".json"));
  for (const file of files) {
    const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf-8"));
    if (data.propertyId === pid) return data;
  }
  return null;
}

function resolveAvailableProviders(selected) {
  const candidates = selected || ["openai", "gemini", "perplexity", "claude"];
  return candidates.filter((p) => {
    if (p === "openai") return !!(process.env.OPENAI_API_KEY || process.env.FDD_OPENAI_API_KEY);
    if (p === "gemini") {
      return !!(
        process.env.GEMINI_API_KEY ||
        process.env.GOOGLE_GENAI_API_KEY ||
        process.env.GOOGLE_GENERATIVE_AI_API_KEY ||
        process.env.FDD_GEMINI_API_KEY
      );
    }
    if (p === "perplexity") return !!(process.env.PERPLEXITY_API_KEY || process.env.PPLX_API_KEY);
    if (p === "claude") {
      return !!(
        process.env.ANTHROPIC_API_KEY ||
        process.env.CLAUDE_API_KEY ||
        process.env.FDD_ANTHROPIC_API_KEY
      );
    }
    return false;
  });
}

async function main() {
  const profile = loadProfile(propertyId);
  if (!profile) {
    console.error(`Property not found: ${propertyId}`);
    process.exit(1);
  }

  console.log(`\n=== AI Demand Positioning — Monitoring Period ===`);
  console.log(`Property: ${profile.name} (${profile.city}, ${profile.state})`);
  console.log(`Affiliation: ${profile.affiliation}`);
  console.log(`Census / HPC: ${profile.censusRecordId || "n/a"}`);
  console.log(`Mode: ${dryRun ? "DRY RUN" : "LIVE EXECUTION"}`);
  console.log("");

  const { buildScenarioUniverse } = await import(
    "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js"
  );
  const scenarios = buildScenarioUniverse(profile);
  console.log(`Scenario Universe: ${scenarios.length} scenarios`);
  console.log(`  Standard: ${scenarios.filter((s) => s.source === "standard").length}`);
  console.log(`  Property-specific: ${scenarios.filter((s) => s.source === "property_specific").length}`);
  console.log("");

  const availableProviders = resolveAvailableProviders(selectedProviders);
  if (!availableProviders.length) {
    console.error("No provider API keys available.");
    process.exit(1);
  }

  const { estimateCost, executeMonitoringPeriod } = await import(
    "../lib/ai-demand-positioning/execution/multi-provider-runner.js"
  );
  const cost = estimateCost(scenarios.length, availableProviders);
  console.log(`Cost Estimate: $${cost.total}`);
  console.log(`  Per provider: ${JSON.stringify(cost.byProvider)}`);
  console.log(`  Total calls: ${cost.scenarioCount * cost.providerCount}`);
  console.log(`Providers: ${availableProviders.join(", ")}`);
  console.log("");

  let reservationId = null;
  let budgetModule = null;
  if (!dryRun && !skipBudget) {
    budgetModule = await import(
      "../lib/ai-demand-positioning/budget/authorization-ledger-v1.js"
    );
    const ledgerPath = budgetModule.adpAuthorizationLedgerPath();
    console.log(`ADP authorization ledger: ${ledgerPath}`);
    const before = budgetModule.readAdpBudgetLedger();
    console.log(
      `  Budget $${before.budgetUsd} · reserved $${before.reservedUsd} · spent(est) $${before.estimatedSpentUsd} · remaining $${budgetModule.remainingAdpBudgetUsd(before).toFixed(2)}`
    );
    const reserved = budgetModule.reserveAdpBudget({
      label: `adp_monitoring:${propertyId}`,
      estimatedUsd: cost.total,
      jobKey: `adp_apply:${propertyId}:${scenarios.length}:${availableProviders.join("+")}`,
      hotelId: profile.censusRecordId || null,
      propertyId,
    });
    reservationId = reserved.reservationId;
    console.log(
      `Reserved $${reserved.estimatedUsd} as ${reservationId}${reserved.idempotent ? " (idempotent)" : ""}`
    );
    console.log("");
  }

  let resumePeriod = null;
  if (!dryRun && (resumePeriodId || autoResume || args.includes("--resume"))) {
    const { loadPeriod, loadLatestPeriod } = await import(
      "../lib/ai-demand-positioning/data-model.js"
    );
    if (resumePeriodId) {
      resumePeriod = loadPeriod(resumePeriodId);
    } else {
      const latest = loadLatestPeriod(propertyId, { includeTargeted: true });
      if (latest && String(latest.status || "").includes("IN_PROGRESS")) {
        resumePeriod = latest;
      } else if (latest && String(latest.status || "") === "EXECUTION_IN_PROGRESS") {
        resumePeriod = latest;
      } else if (resumePeriodId === null && args.includes("--resume") && latest) {
        // Explicit --resume: allow continuing the newest incomplete period
        const obs = latest.observations || [];
        const expected = scenarios.length * availableProviders.length;
        if (obs.length > 0 && obs.length < expected && latest.status !== "EXECUTION_COMPLETE") {
          resumePeriod = latest;
        }
      }
    }
    if (resumePeriod) {
      console.log(
        `Resuming period ${resumePeriod.periodId} with ${(resumePeriod.observations || []).length} existing observations (no duplicate submits for completed pairs).`
      );
    } else {
      console.log(`--resume requested but no in-progress period found; starting fresh.`);
    }
  }

  console.log(`Executing...`);
  let period;
  try {
    period = await executeMonitoringPeriod({
      propertyId,
      scenarios,
      dryRun,
      providers: availableProviders,
      resumePeriod,
      checkpointEvery: 10,
      onProgress: (completed, total, meta = {}) => {
        if (meta.skipped) return;
        if (completed % 10 === 0 || completed === total) {
          console.log(
            `  Progress: ${completed}/${total}${meta.provider ? ` [${meta.provider}]` : ""}`
          );
        }
      },
    });
  } catch (err) {
    if (reservationId && budgetModule) {
      budgetModule.releaseAdpBudgetReservation(reservationId, {
        reason: `execution_failed:${err?.message || err}`,
      });
    }
    throw err;
  }

  console.log(`\nPeriod: ${period.periodId}`);
  console.log(`Status: ${period.status}`);
  console.log(`Observations: ${period.observations.length}`);
  const failedObs = (period.observations || []).filter((o) => o.error).length;
  const okObs = (period.observations || []).length - failedObs;
  console.log(`  Successful: ${okObs} · Failed: ${failedObs}`);

  if (!dryRun) {
    const { parsePeriodObservations } = await import(
      "../lib/ai-demand-positioning/execution/response-parser.js"
    );
    const parsed = parsePeriodObservations(period, profile);

    const { buildOwnerPayload } = await import(
      "../lib/ai-demand-positioning/customer/owner-payload.js"
    );
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

    const { savePeriod } = await import("../lib/ai-demand-positioning/data-model.js");
    const savedPath = savePeriod(parsed);
    console.log(`\nSaved period to: ${savedPath}`);

    if (payload.ok) {
      const {
        buildPublishedSnapshotBundle,
        publishExistingHotelAdpSnapshot,
        savePublishedSnapshotBundle,
      } = await import("../lib/ai-demand-positioning/published-snapshot.js");
      const publishedBundle = buildPublishedSnapshotBundle({ period: parsed, profile });
      if (!publishedBundle.ok) {
        console.error(`Published bundle failed: ${publishedBundle.message || publishedBundle.error}`);
        if (reservationId && budgetModule) {
          budgetModule.releaseAdpBudgetReservation(reservationId, {
            reason: "published_bundle_failed",
          });
        }
        process.exit(1);
      }

      const requireCert =
        process.env.ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH === "1";
      let saved;
      if (requireCert) {
        console.log("\nCertifying + publishing via publishExistingHotelAdpSnapshot...");
        saved = await publishExistingHotelAdpSnapshot(publishedBundle, null, {
          seed: false,
          officialCustomerPublish: true,
        });
        if (!saved?.ok) {
          console.error("Publish/certify failed:", saved);
          if (reservationId && budgetModule) {
            // Keep reservation spent attribution only if research ran; commit estimated for executed research
            budgetModule.commitAdpBudgetReservation(reservationId, {
              periodId: period.periodId,
            });
            console.error(
              "Research cost committed (estimated); customer publish blocked — see certification output."
            );
          }
          process.exit(1);
        }
        console.log(`Published snapshot: ${saved.manifestFile || saved.dir}`);
        console.log(`Certification: ${saved.certificationStatus || "n/a"}`);
      } else {
        saved = savePublishedSnapshotBundle(publishedBundle, { seed: false });
        console.log(`Published snapshot: ${saved.manifestFile}`);
      }

      if (process.env.ADP_AIRTABLE_PUBLISH_APPLY === "true") {
        const { upsertPublishedReportToAirtable } = await import(
          "../lib/ai-demand-positioning/airtable-published-report.js"
        );
        const airtableResult = await upsertPublishedReportToAirtable(publishedBundle, {
          payloadStoreRef: `published/${propertyId}/${publishedBundle.manifest.reportFile}`,
        });
        console.log(`Airtable Live row: ${airtableResult.recordId}`);
      }
    }

    if (reservationId && budgetModule) {
      budgetModule.commitAdpBudgetReservation(reservationId, {
        periodId: period.periodId,
      });
      const after = budgetModule.readAdpBudgetLedger();
      console.log(
        `\nBudget after commit: spent(est) $${after.estimatedSpentUsd} · reserved $${after.reservedUsd} · remaining $${budgetModule.remainingAdpBudgetUsd(after).toFixed(2)} · actualBilled ${after.actualBilledUsd == null ? "UNKNOWN" : "$" + after.actualBilledUsd}`
      );
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
