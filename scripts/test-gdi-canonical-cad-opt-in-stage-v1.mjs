#!/usr/bin/env node
/**
 * $0 — Canonical Full E2E CAD opt-in enrichment stage.
 * Classifies Research SoT; never auto-promotes FQO / persistence / predictive claims.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveCanonicalCadOptIn,
  runCanonicalCadOptInStage,
  applyCadEnrichmentToResearchRows,
  CAD_CLASS,
  CAD_SOURCING_WINDOW_STATE,
  GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-cad-opt-in-stage-v1.js";
import { runCanonicalFullE2eResearch } from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(ROOT, "reports/gdi-canonical-cad-opt-in-stage-v1");
fs.mkdirSync(REPORT, { recursive: true });

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}
async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const historicalOpp = {
  id: "cad_hist_1",
  title: "Past Annual Meeting 2024",
  organizationName: "Example Society",
  likelyCompetitor: "Hilton San Diego Bayfront",
  eventStartDate: "2024-05-01",
  lodgingEvidence:
    "Official room block at Hilton San Diego Bayfront for attendees.",
  lodgingEvidenceUrl: "https://example.org/housing",
  sourceUrls: ["https://example.org/housing"],
  whyThisHotel: "Previously hosted nearby; no current open pathway evidenced.",
  recommendedNextStep: "Archive learnings.",
  futureDemandPathway: false,
};

const emergingOpp = {
  id: "cad_emerg_1",
  title: "BIO International Convention 2028 lodging/overflow",
  organizationName: "BIO",
  likelyCompetitor: "Marriott Marquis San Diego Marina",
  competitiveContext: {
    incumbentHotel: "Marriott Marquis San Diego Marina",
    pathway: "OVERFLOW_OR_FUTURE_CYCLE",
  },
  eventStartDate: "2028-06-01",
  lodgingEvidence:
    "Official housing partner pages list Marriott Marquis as a primary hotel.",
  sourceUrls: ["https://example.org/bio-housing"],
  whyThisHotel:
    "This hotel has no verified current host or overflow role for BIO yet.",
  recommendedNextStep: "",
  primaryContact: { name: "BIO International Convention" },
};

check("default_cad_opt_in_off", () => {
  const r = resolveCanonicalCadOptIn({});
  assert.equal(r.enabled, false);
});

check("explicit_flags_enable_cad", () => {
  assert.equal(resolveCanonicalCadOptIn({ enableCadStage: true }).enabled, true);
  assert.equal(
    resolveCanonicalCadOptIn({ enableCompetitiveAccountDiscovery: true }).enabled,
    true,
  );
});

check("stage_classifies_and_never_auto_fqo", () => {
  const stage = runCanonicalCadOptInStage({
    enableCadStage: true,
    opportunities: [historicalOpp, emergingOpp, { id: "plain", title: "No competitive" }],
  });
  assert.equal(stage.contract, GDI_CANONICAL_CAD_OPT_IN_STAGE_V1);
  assert.equal(stage.enabled, true);
  assert.equal(stage.status, "CLASSIFIED");
  assert.equal(stage.fqoAutoPromoted, false);
  assert.equal(stage.persistenceAuthorized, false);
  assert.equal(stage.predictiveEngineClaim, false);
  assert.equal(stage.classifiedCount, 3);
  assert.ok(stage.preserves.HISTORICAL_COMPETITIVE_INTEL);
  assert.ok(stage.preserves.EMERGING_COMPETITIVE_OPPORTUNITY);
  assert.ok(stage.preserves.FQO_CANDIDATE);
  assert.deepEqual(stage.preserves.sourcingWindows, [
    "CONFIRMED",
    "INFERRED",
    "UNKNOWN",
  ]);
  const hist = stage.enriched.find((e) => e.opportunityId === "cad_hist_1");
  assert.equal(hist.cadClass, CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL);
  assert.ok(
    Object.values(CAD_SOURCING_WINDOW_STATE).includes(hist.sourcingWindowState),
  );
  assert.equal(hist.fqoPass, false);
  assert.equal(hist.persistenceAuthorized, false);
  const emerg = stage.enriched.find((e) => e.opportunityId === "cad_emerg_1");
  assert.ok(
    emerg.cadClass === CAD_CLASS.EMERGING_COMPETITIVE_OPPORTUNITY ||
      emerg.cadClass === CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL,
  );
  assert.notEqual(emerg.cadClass, CAD_CLASS.FQO_CANDIDATE);
});

check("apply_enrichment_does_not_flip_fqo", () => {
  const stage = runCanonicalCadOptInStage({
    enableCadStage: true,
    opportunities: [historicalOpp],
  });
  const rows = applyCadEnrichmentToResearchRows(
    [{ id: "cad_hist_1", title: "x", finalizedQualifiedOpportunity: false, salesReady: false }],
    stage,
  );
  assert.equal(rows[0].finalizedQualifiedOpportunity, false);
  assert.equal(rows[0].salesReady, false);
  assert.ok(rows[0].competitiveAccountDiscovery);
});

await checkAsync("canonical_entry_skips_cad_by_default", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    outDir: path.join(REPORT, "canonical-default-no-cad"),
  });
  assert.equal(r.cadOptIn?.enabled, false);
  assert.equal(r.cadOptIn?.status, "SKIPPED_OPT_IN_OFF");
  assert.equal(r.cadOptIn?.fqoAutoPromoted, false);
  assert.equal(r.cadOptIn?.predictiveEngineClaim, false);
});

await checkAsync("canonical_entry_runs_cad_when_enabled", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    enableCadStage: true,
    seedOpportunities: [historicalOpp, emergingOpp],
    outDir: path.join(REPORT, "canonical-cad-opt-in"),
  });
  assert.equal(r.cadOptIn?.enabled, true);
  assert.equal(r.cadOptIn?.status, "CLASSIFIED");
  assert.ok(r.cadOptIn?.classifiedCount >= 2);
  assert.equal(r.cadOptIn?.fqoAutoPromoted, false);
  assert.equal(r.cadOptIn?.persistenceAuthorized, false);
  assert.equal(r.cadOptIn?.predictiveEngineClaim, false);
  const summary = JSON.parse(fs.readFileSync(r.summaryPath, "utf8"));
  assert.equal(summary.cadOptIn?.enabled, true);
  assert.equal(summary.cadOptIn?.fqoAutoPromoted, false);
  fs.writeFileSync(
    path.join(REPORT, "CAD_STAGE_RESULT.json"),
    `${JSON.stringify(r.cadOptIn, null, 2)}\n`,
  );
});

const failed = results.filter((x) => !x.ok);
const out = {
  contract: GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
  ok: failed.length === 0,
  passed: results.filter((x) => x.ok).length,
  failed: failed.length,
  results,
};
fs.writeFileSync(
  path.join(REPORT, "TEST_RESULTS.json"),
  `${JSON.stringify(out, null, 2)}\n`,
);
console.log(JSON.stringify(out, null, 2));
if (failed.length) process.exit(1);
