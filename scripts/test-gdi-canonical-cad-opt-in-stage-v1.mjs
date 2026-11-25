#!/usr/bin/env node
/**
 * $0 — Canonical Full E2E CAD STANDARD stage + predictive sourcing assessment.
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

check("default_cad_standard_stage_on", () => {
  const r = resolveCanonicalCadOptIn({});
  assert.equal(r.enabled, true);
  assert.equal(r.standardStage, true);
});

check("explicit_disable_only", () => {
  assert.equal(resolveCanonicalCadOptIn({ disableCadStage: true }).enabled, false);
});

check("stage_classifies_predictive_and_never_auto_fqo", () => {
  const stage = runCanonicalCadOptInStage({
    opportunities: [historicalOpp, emergingOpp, { id: "plain", title: "No competitive" }],
  });
  assert.equal(stage.enabled, true);
  assert.equal(stage.standardStage, true);
  assert.equal(stage.status, "CLASSIFIED");
  assert.equal(stage.fqoAutoPromoted, false);
  assert.equal(stage.persistenceAuthorized, false);
  assert.equal(stage.predictiveEngineClaim, false);
  assert.equal(stage.classifiedCount, 3);
  assert.ok(stage.predictiveSourcing);
  assert.equal(stage.predictiveSourcing.fullPredictiveEngineClaimed, false);
  const hist = stage.enriched.find((e) => e.opportunityId === "cad_hist_1");
  assert.equal(hist.cadClass, CAD_CLASS.HISTORICAL_COMPETITIVE_INTEL);
  assert.ok(hist.predictiveSourcingAssessment);
  assert.equal(
    hist.predictiveSourcingAssessment.fullPredictiveEngineClaimed,
    false,
  );
  assert.equal(
    hist.predictiveSourcingAssessment.estimatedWindowIsNotOpenContract,
    true,
  );
  assert.ok(
    Object.values(CAD_SOURCING_WINDOW_STATE).includes(hist.sourcingWindowState),
  );
  const emerg = stage.enriched.find((e) => e.opportunityId === "cad_emerg_1");
  assert.notEqual(emerg.cadClass, CAD_CLASS.FQO_CANDIDATE);
});

check("apply_enrichment_does_not_flip_fqo", () => {
  const stage = runCanonicalCadOptInStage({
    opportunities: [historicalOpp],
  });
  const rows = applyCadEnrichmentToResearchRows(
    [{ id: "cad_hist_1", title: "x", finalizedQualifiedOpportunity: false, salesReady: false }],
    stage,
  );
  assert.equal(rows[0].finalizedQualifiedOpportunity, false);
  assert.equal(rows[0].salesReady, false);
  assert.ok(rows[0].competitiveAccountDiscovery);
  assert.ok(rows[0].predictiveSourcingAssessment);
});

await checkAsync("canonical_entry_runs_cad_by_default", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    disableLeisureStandardStage: true,
    seedOpportunities: [historicalOpp, emergingOpp],
    outDir: path.join(REPORT, "canonical-default-cad"),
  });
  assert.equal(r.cadOptIn?.enabled, true);
  assert.equal(r.cadOptIn?.standardStage, true);
  assert.equal(r.cadOptIn?.status, "CLASSIFIED");
  assert.ok(r.cadOptIn?.classifiedCount >= 2);
  assert.equal(r.cadOptIn?.fqoAutoPromoted, false);
  assert.equal(r.cadOptIn?.predictiveEngineClaim, false);
  const summary = JSON.parse(fs.readFileSync(r.summaryPath, "utf8"));
  assert.equal(summary.cadOptIn?.enabled, true);
  fs.writeFileSync(
    path.join(REPORT, "CAD_STAGE_RESULT.json"),
    `${JSON.stringify(r.cadOptIn, null, 2)}\n`,
  );
});

await checkAsync("canonical_entry_can_explicitly_disable_cad", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    disableLeisureStandardStage: true,
    disableCadStage: true,
    outDir: path.join(REPORT, "canonical-cad-disabled"),
  });
  assert.equal(r.cadOptIn?.enabled, false);
  assert.equal(r.cadOptIn?.status, "SKIPPED_EXPLICITLY_DISABLED");
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
