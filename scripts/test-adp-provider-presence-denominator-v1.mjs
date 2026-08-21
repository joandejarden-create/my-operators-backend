#!/usr/bin/env node
/**
 * Provider presence denominator regressions (ADP_MEASUREMENT_CONTRACT_V1).
 *   npm run test:adp-provider-presence-denominator-v1
 */
import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import { buildProviderPresenceRows } from "../lib/ai-demand-positioning/metrics/provider-presence-v1.js";
import { computeConsiderationMetrics } from "../lib/ai-demand-positioning/metrics/consideration-rate.js";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedManifest, loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";

function obs({ provider, scenarioId, mentioned, error, parsed = true }) {
  return {
    provider,
    scenarioId,
    mentioned: Boolean(mentioned),
    error: error || undefined,
    parsed,
    rawResponse: parsed ? "x" : undefined,
  };
}

function main() {
  // COMPLETE PROVIDER — all scheduled succeeded
  const complete = buildProviderPresenceRows([
    obs({ provider: "openai", scenarioId: "s1", mentioned: true }),
    obs({ provider: "openai", scenarioId: "s2", mentioned: false }),
    obs({ provider: "openai", scenarioId: "s3", mentioned: true }),
  ]);
  const c = complete.providers.find((p) => p.provider === "openai");
  assert.equal(c.scenariosScheduled, 3);
  assert.equal(c.comparable, 3);
  assert.equal(c.total, 3);
  assert.equal(c.mentioned, 2);
  assert.equal(c.presence, 66.7);
  assert.equal(c.incompleteCoverage, false);
  assert.equal(c.coverageNote, null);
  assert.equal(c.presenceUnavailable, false);

  // PARTIAL PROVIDER — some failed; omitted from denom
  const partial = buildProviderPresenceRows([
    obs({ provider: "gemini", scenarioId: "s1", mentioned: true }),
    obs({ provider: "gemini", scenarioId: "s2", mentioned: true }),
    obs({ provider: "gemini", scenarioId: "s3", mentioned: false }),
    obs({ provider: "gemini", scenarioId: "s4", mentioned: false, error: "Gemini 500" }),
    obs({ provider: "gemini", scenarioId: "s5", mentioned: false, error: "timeout" }),
  ]);
  const g = partial.providers.find((p) => p.provider === "gemini");
  assert.equal(g.scenariosScheduled, 5);
  assert.equal(g.comparable, 3);
  assert.equal(g.excludedFromMetric, 2);
  assert.equal(g.mentioned, 2);
  assert.equal(g.presence, 66.7); // 2/3 — NOT 2/5=40%
  assert.equal(g.total, 3); // never scheduled-as-denom
  assert.equal(g.incompleteCoverage, true);
  assert.equal(g.coverageNote, "3 of 5 observations captured");
  assert.notEqual(g.presence, 40);

  // ZERO SUCCESS — unavailable/null, not 0%
  const zero = buildProviderPresenceRows([
    obs({ provider: "claude", scenarioId: "s1", mentioned: false, error: "fail" }),
    obs({ provider: "claude", scenarioId: "s2", mentioned: false, error: "fail" }),
  ]);
  const z = zero.providers.find((p) => p.provider === "claude");
  assert.equal(z.comparable, 0);
  assert.equal(z.presence, null);
  assert.equal(z.presenceUnavailable, true);
  assert.notEqual(z.presence, 0);

  // MIXED PROVIDER COVERAGE — equal-provider aggregation still uses each provider's comparable rate
  const mixed = buildProviderPresenceRows([
    obs({ provider: "openai", scenarioId: "s1", mentioned: true }),
    obs({ provider: "openai", scenarioId: "s2", mentioned: true }),
    obs({ provider: "gemini", scenarioId: "s1", mentioned: true }),
    obs({ provider: "gemini", scenarioId: "s2", mentioned: false, error: "fail" }),
  ]);
  const mo = mixed.providers.find((p) => p.provider === "openai");
  const mg = mixed.providers.find((p) => p.provider === "gemini");
  assert.equal(mo.presence, 100);
  assert.equal(mg.presence, 100); // 1/1 comparable, not 1/2
  const mean = (mo.presence + mg.presence) / 2;
  assert.equal(mean, 100);

  // Aggregate Consideration still omits failed (same grain rule)
  const profile = loadPropertyProfile("adp_hotel_phillips_kansas_city");
  const man = loadPublishedManifest("adp_hotel_phillips_kansas_city");
  const period = loadAllPeriods("adp_hotel_phillips_kansas_city").find((p) => p.periodId === man.latestPeriodId);
  const scenarios = buildScenarioUniverse(profile);
  const cons = computeConsiderationMetrics(period.observations, scenarios, profile);
  const payload = buildOwnerPayload(period, scenarios, profile);
  const gem = payload.evidence.providers.find((p) => p.provider === "gemini");
  assert.equal(gem.scenariosScheduled, 63);
  assert.equal(gem.comparable, 55);
  assert.equal(gem.excludedFromMetric, 8);
  assert.equal(gem.presence, 49.1);
  assert.equal(gem.total, 55);
  // Consideration rate unchanged by provider display fix (same comparable grain)
  // Aggregate Consideration omits failed Gemini (comparable grain) — do not assert exact
  // dual-path equality vs demandCapture; only that Consideration stays on comparable grain.
  assert.ok(cons?.observationConsiderationRate != null);
  assert.ok(payload.executiveMetrics?.considerationRate?.rate != null);
  assert.equal(payload.executiveMetrics.considerationRate.comparableObservations, cons.comparableObservations);

  const published = loadPublishedReport("adp_hotel_phillips_kansas_city");
  assert.equal(published.evidence.providers.find((p) => p.provider === "gemini").presence, 49.1);

  const ui = readFileSync(join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js"), "utf8");
  assert.ok(ui.includes("presenceUnavailable") || ui.includes("observations captured"));
  assert.ok(ui.includes("Unavailable"));

  console.log("test:adp-provider-presence-denominator-v1 OK");
}

main();
