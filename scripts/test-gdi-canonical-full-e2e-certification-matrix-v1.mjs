#!/usr/bin/env node
/**
 * $0 fixture/mock certification of the ACTUAL canonical Full E2E entry
 * with leisure + CAD as standard stages (no enable flags required).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runCanonicalFullE2eResearch } from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js";
import {
  buildCanonicalCertificationMatrixFromRun,
  GDI_CANONICAL_FULL_E2E_CERTIFICATION_MATRIX_V1,
  CERT_LABEL,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-certification-matrix-v1.js";
import {
  CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID,
  HILTON_NY_TIMES_SQUARE_HOTEL_ID,
  WYNDHAM_HOTEL_ID_RECONCILIATION,
} from "../lib/group-demand-intelligence/workflow/gdi-canonical-leisure-applicability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-certification-matrix-v1",
);
fs.mkdirSync(REPORT, { recursive: true });

const results = [];
async function check(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
    console.log(`PASS ${name}`);
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
    console.error(`FAIL ${name}: ${e.message || e}`);
  }
}

const seedCad = {
  id: "cert_cad_hist",
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

await check("wyndham_hilton_ids_are_separate_hotels", () => {
  assert.equal(CERTIFIED_WYNDHAM_SAN_DIEGO_BAYSIDE_HOTEL_ID, "recnPB3Q02NO8Ma1o");
  assert.equal(HILTON_NY_TIMES_SQUARE_HOTEL_ID, "rec35fExUxCClpOP6");
  assert.equal(
    WYNDHAM_HOTEL_ID_RECONCILIATION.relationship,
    "SEPARATE_HOTELS_NOT_ALIAS",
  );
});

await check("canonical_entry_default_includes_leisure_and_cad", async () => {
  const outDir = path.join(REPORT, "laluna-standard-run");
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    // NO enableLeisureModules / enableCadStage flags
    seedOpportunities: [seedCad],
    leisureModules: [
      "leisure_distribution",
      "leisure_attractions",
      "leisure_events",
    ],
    outDir,
  });

  assert.equal(r.leisureOptIn?.enabled, true);
  assert.equal(r.leisureOptIn?.standardStage, true);
  assert.notEqual(r.leisureOptIn?.status, "SKIPPED_OPT_IN_OFF");
  assert.equal(r.cadOptIn?.enabled, true);
  assert.equal(r.cadOptIn?.standardStage, true);
  assert.equal(r.cadOptIn?.fqoAutoPromoted, false);
  assert.equal(r.cadOptIn?.predictiveEngineClaim, false);
  assert.equal(Number(r.leisureOptIn?.spentUsd || 0), 0);

  const matrix = buildCanonicalCertificationMatrixFromRun(r, {
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
  });
  assert.equal(matrix.contract, GDI_CANONICAL_FULL_E2E_CERTIFICATION_MATRIX_V1);
  assert.equal(matrix.paidLiveCertified, false);
  assert.equal(matrix.certificationScope, "FIXTURE_MOCK_CODE_PHASE");
  assert.equal(matrix.leisureDefaultOffRemoved, true);
  assert.equal(matrix.cadDefaultOffRemoved, true);
  assert.ok(matrix.rows.length >= 18);

  const predictive = matrix.rows.find(
    (row) => row.capability === "predictive_sourcing_minimum_workflow",
  );
  assert.equal(predictive.fullPredictiveEngineClaimed, false);
  assert.equal(predictive.certificationLabel, CERT_LABEL.GAP);

  const cadRow = matrix.rows.find(
    (row) => row.capability === "competitive_account_discovery",
  );
  assert.equal(cadRow.certificationLabel, CERT_LABEL.EXECUTED_ZERO_PAID);

  fs.writeFileSync(
    path.join(REPORT, "CERTIFICATION_MATRIX.json"),
    `${JSON.stringify(matrix, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(REPORT, "WYNDHAM_HOTEL_ID_RECONCILIATION.json"),
    `${JSON.stringify(WYNDHAM_HOTEL_ID_RECONCILIATION, null, 2)}\n`,
  );
});

await check("omitting_enable_flags_cannot_silently_skip", async () => {
  const r = await runCanonicalFullE2eResearch({
    hotelId: "recaokE8psIbuVeuH",
    mode: "fixture",
    skipHpc: true,
    skipHotelWideFinalization: true,
    allowIsolatedTestSkipHotelWideFinalization: true,
    isolatedTestHarness: true,
    outDir: path.join(REPORT, "no-flags-guard"),
  });
  assert.notEqual(r.leisureOptIn?.status, "SKIPPED_OPT_IN_OFF");
  assert.notEqual(r.cadOptIn?.status, "SKIPPED_OPT_IN_OFF");
  assert.equal(r.leisureOptIn?.enabled, true);
  assert.equal(r.cadOptIn?.enabled, true);
});

const failed = results.filter((r) => !r.ok);
const out = {
  contract: GDI_CANONICAL_FULL_E2E_CERTIFICATION_MATRIX_V1,
  ok: failed.length === 0,
  passed: results.filter((r) => r.ok).length,
  failed: failed.length,
  results,
  paidLiveCertified: false,
  certificationScope: "FIXTURE_MOCK_CODE_PHASE",
};
fs.writeFileSync(
  path.join(REPORT, "TEST_RESULTS.json"),
  `${JSON.stringify(out, null, 2)}\n`,
);
console.log(JSON.stringify(out, null, 2));
if (failed.length) process.exit(1);
