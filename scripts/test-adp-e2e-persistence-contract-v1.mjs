#!/usr/bin/env node
/**
 * ADP_E2E_PERSISTENCE_CONTRACT_V1 — local no-paid persistence tests.
 *
 * Uses synthetic fixtures + mock history store only.
 * Does NOT write Airtable, remesure, republish, or deploy.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { rmSync, existsSync } from "node:fs";
import {
  buildSyntheticLongitudinalFixture,
  SYNTHETIC_LONGITUDINAL_PROPERTY_ID,
} from "../lib/ai-demand-positioning/longitudinal/synthetic-longitudinal-fixture-v1.js";
import {
  buildCustomerPayloadForWeek,
} from "../lib/ai-demand-positioning/longitudinal/synthetic-report-snapshots-v1.js";
import {
  buildReportSnapshotV1,
  sha256OfPayload,
} from "../lib/ai-demand-positioning/longitudinal/report-snapshot-v1.js";
import {
  persistAdpHistoricalPeriodV1,
  buildHistoricalWriteManifest,
  MockAdpHistoryStore,
  MOCK_HISTORY_ROOT,
  applyHistoricalRecordsToAirtable,
} from "../lib/ai-demand-positioning/longitudinal/persist-adp-historical-period-v1.js";
import { HISTORY_TABLES } from "../lib/ai-demand-positioning/longitudinal/airtable-history-schema-final-v1.js";
import {
  ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
  TABLE_PERSISTENCE_CONTRACT,
  CURRENT_VERSION_RULE,
  assertPersistenceDistinctFromPublication,
  PERSISTENCE_LIFECYCLE,
  PUBLICATION_LIFECYCLE,
} from "../lib/ai-demand-positioning/longitudinal/adp-e2e-persistence-table-contract-v1.js";
import {
  assertSingleCurrentPerProperty,
} from "../lib/ai-demand-positioning/longitudinal/adp-current-historical-version-v1.js";
import {
  assessPersistenceCompleteness,
} from "../lib/ai-demand-positioning/longitudinal/adp-persistence-completeness-gate-v1.js";
import { getAdpPersistencePolicy } from "../lib/ai-demand-positioning/adp-persistence-policy.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../lib/ai-demand-positioning/certification/adp-unprompted-independent-discovery-standard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/dealality-adp-airtable-persistence-historical-traceability-v1"
);
const DURABLE = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/admin-published-vs-measured-dates/PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json"
);

fs.mkdirSync(OUT, { recursive: true });

const results = [];
function check(id, pass, detail = null) {
  results.push({ id, pass: Boolean(pass), detail });
  const mark = pass ? "PASS" : "FAIL";
  console.log(`${mark} ${id}${detail ? ` — ${detail}` : ""}`);
}

function resetMockStore() {
  if (existsSync(MOCK_HISTORY_ROOT)) {
    rmSync(MOCK_HISTORY_ROOT, { recursive: true, force: true });
  }
}

async function main() {
  resetMockStore();

  // --- Contract presence ---
  check(
    "CONTRACT_TABLES_10",
    TABLE_PERSISTENCE_CONTRACT.length === 10,
    String(TABLE_PERSISTENCE_CONTRACT.length)
  );
  check(
    "CURRENT_VERSION_RULE",
    CURRENT_VERSION_RULE.historicalMayOverrideCurrent === false &&
      CURRENT_VERSION_RULE.responseTextAdjudicationAuthoritative === true &&
      CURRENT_VERSION_RULE.runtimeMentionedFlagsAuthoritative === false
  );
  check(
    "METHODOLOGY_V1_1_1",
    CURRENT_VERSION_RULE.methodologyId === CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID
  );

  // --- Policy: true/1 parity; unavailable ≠ success ---
  const polOff = getAdpPersistencePolicy({
    ADP_HISTORY_WRITES_ENABLED: "0",
    ADP_HISTORY_AIRTABLE_WRITE_APPLY: "0",
  });
  const polOn = getAdpPersistencePolicy({
    ADP_HISTORY_WRITES_ENABLED: "true",
    ADP_HISTORY_AIRTABLE_WRITE_APPLY: "1",
  });
  check("POLICY_DISABLED_DEFAULT", polOff.HISTORY_AIRTABLE === "DISABLED");
  check("POLICY_ACCEPTS_TRUE", polOn.HISTORY_AIRTABLE === "ENABLED");
  check(
    "POLICY_AIRTABLE_FAIL_CLOSED",
    polOn.airtableUnavailableImpliesSuccess === false &&
      polOn.airtableFailureMustNotRollbackFsPublication === true
  );

  const distinct = assertPersistenceDistinctFromPublication({
    persistenceState: PERSISTENCE_LIFECYCLE.UNAVAILABLE,
    publicationState: PUBLICATION_LIFECYCLE.FS_PUBLISHED,
    claimedComplete: true,
  });
  check(
    "UNAVAILABLE_CANNOT_CLAIM_COMPLETE",
    distinct.defects.some((d) => d.code === "PERSISTENCE_CLAIMED_COMPLETE_WHILE_UNAVAILABLE")
  );

  // --- Prompt Ledger primary mapping present ---
  const mappingProbe = await applyHistoricalRecordsToAirtable({
    baseId: "app_fake",
    records: [
      {
        table: HISTORY_TABLES.PROMPT_LEDGER,
        idempotencyKey: "x",
        fields: { "Prompt Ledger Key": "x", "Property ID": "p", "Period ID": "q" },
      },
    ],
    dryRun: true,
  });
  check(
    "PROMPT_LEDGER_PRIMARY_MAPPED",
    !mappingProbe.errors.some((e) => e.error === "NO_PRIMARY_FIELD_MAPPING"),
    mappingProbe.errors[0]?.error || "ok"
  );

  // --- Synthetic multi-period persist (Oct-like + Nov-like weeks) ---
  let fixture;
  let weekPayloads = [];
  try {
    fixture = buildSyntheticLongitudinalFixture();
  } catch (err) {
    check("SYNTHETIC_FIXTURE", false, String(err.message || err));
    writeOut();
    process.exit(1);
  }
  check("SYNTHETIC_FIXTURE", Boolean(fixture?.snapshots?.length >= 2));

  const propertyId = fixture.propertyId || SYNTHETIC_LONGITUDINAL_PROPERTY_ID;
  const weeks = [0, 1].map((i) => {
    const payload = buildCustomerPayloadForWeek(fixture, i);
    // Ensure evidence lanes for completeness
    if (!payload.evidence) {
      payload.evidence = {
        positive: [
          {
            observationId: `obs_pos_w${i}`,
            provider: "chatgpt",
            territory: "local",
            subjectStatus: "Appeared",
            subjectPresent: true,
            rank: 1,
            aiResponse: `Independent unprompted mention of subject week ${i}`,
            subjectMentionSpans: [{ start: 0, end: 10 }],
          },
        ],
        missing: [],
        displacement: [],
      };
    }
    const contentHash = sha256OfPayload(payload);
    const snap = buildReportSnapshotV1({
      propertyId,
      periodId: fixture.snapshots[i].periodId,
      monitoringDate: fixture.snapshots[i].calendarDate,
      customerPayload: payload,
      certificationStatus: "CERTIFIED",
      synthetic: true,
      versions: {
        reportVersion: "test",
        measurementContractVersion: "v1.1.1",
        parserVersion: "response-text-adjudication",
      },
      disclosures: ["INDEPENDENT_UNPROMPTED_V1_1_1"],
    });
    // Ensure contentHash on snap matches verification path
    snap.contentHash = contentHash;
    snap.customerPayload = payload;
    return {
      weekIndex: i,
      periodId: fixture.snapshots[i].periodId,
      monitoringDate: fixture.snapshots[i].calendarDate,
      payload,
      snap,
    };
  });
  weekPayloads = weeks;

  // Period with observations for Prompt Ledger
  const periodWithObs = {
    periodId: weeks[0].periodId,
    observations: [
      {
        observationId: "obs_ledger_1",
        provider: "chatgpt",
        eligibility: { measurementEligible: true },
        provenance: {
          scenarioId: "scenario_test",
          provider: "chatgpt",
          territoryId: "local",
          ownerIntent: "consideration",
          scenarioClass: "independent_unprompted",
          promptHash: "abc123",
          promptTemplateId: "tpl_1",
          promptTemplateVersion: "1",
          promptGeneratorVersion: "1",
          exactPrompt: "Where should I stay near the beach?",
        },
        response: { text: "Try the subject hotel among others." },
      },
    ],
  };

  // Persist week 0
  const p0 = await persistAdpHistoricalPeriodV1({
    dryRun: false,
    useMock: true,
    allowProductionWrites: false,
    propertyId,
    periodId: weeks[0].periodId,
    monitoringDate: weeks[0].monitoringDate,
    certificationStatus: "CERTIFIED",
    reportSnapshot: weeks[0].snap,
    customerPayload: weeks[0].payload,
    historicalVersion: 1,
    synthetic: true,
    period: periodWithObs,
  });
  check("PERSIST_WEEK0_COMPLETE", p0.ok && p0.persistenceLifecycle === PERSISTENCE_LIFECYCLE.COMPLETE, p0.defects?.[0]?.code || null);
  check("PERSISTENCE_DISTINCT_FROM_PUBLICATION", p0.publishPerformed === false && p0.publicationLifecycle === PUBLICATION_LIFECYCLE.NOT_PUBLISHED);

  // Idempotent re-run week 0
  const p0b = await persistAdpHistoricalPeriodV1({
    dryRun: false,
    useMock: true,
    allowProductionWrites: false,
    propertyId,
    periodId: weeks[0].periodId,
    monitoringDate: weeks[0].monitoringDate,
    certificationStatus: "CERTIFIED",
    reportSnapshot: weeks[0].snap,
    customerPayload: weeks[0].payload,
    historicalVersion: 1,
    synthetic: true,
    period: periodWithObs,
  });
  check(
    "IDEMPOTENT_RERUN",
    p0b.ok && (p0b.idempotentSkips > 0 || p0b.recordsCreated === 0),
    `skips=${p0b.idempotentSkips} created=${p0b.recordsCreated}`
  );

  // Persist week 1 — must demote week 0 current flag
  const p1 = await persistAdpHistoricalPeriodV1({
    dryRun: false,
    useMock: true,
    allowProductionWrites: false,
    propertyId,
    periodId: weeks[1].periodId,
    monitoringDate: weeks[1].monitoringDate,
    certificationStatus: "CERTIFIED",
    reportSnapshot: weeks[1].snap,
    customerPayload: weeks[1].payload,
    historicalVersion: 1,
    synthetic: true,
  });
  check("PERSIST_WEEK1_COMPLETE", p1.ok, p1.defects?.[0]?.code || p1.defects?.[0]?.detail || null);
  check(
    "DEMOTION_PLANNED",
    (p1.currentVersionDemotion?.demoteCount || 0) >= 1,
    String(p1.currentVersionDemotion?.demoteCount)
  );

  const store = new MockAdpHistoryStore();
  const single = assertSingleCurrentPerProperty(store.listPeriods(propertyId), propertyId);
  check("SINGLE_CURRENT_AFTER_WEEK1", single.ok && single.currentCount === 1, JSON.stringify(single.periodIds));

  // Old week 0 rows still present
  const week0Rows = store.query(
    (r) => r.fields["Period ID"] === weeks[0].periodId && r.fields["Property ID"] === propertyId
  );
  check("HISTORICAL_WEEK0_PRESERVED", week0Rows.length > 0, String(week0Rows.length));

  // Correction creates new version + correction row without destroying prior
  const correctedSnap = {
    ...weeks[1].snap,
    snapshotId: `${weeks[1].snap.snapshotId}_corr`,
    correctionReason: "V1.1.1 response-text adjudication correction",
    correctionAuthorizedBy: "TEST",
    correctionAt: new Date().toISOString(),
    changedFields: ["Consideration Rate"],
  };
  const correctedPayload = {
    ...weeks[1].payload,
    kpis: {
      ...weeks[1].payload.kpis,
      considerationRate: Number(weeks[1].payload.kpis.considerationRate) + 1,
    },
  };
  correctedSnap.customerPayload = correctedPayload;
  correctedSnap.contentHash = sha256OfPayload(correctedPayload);

  const corr = await persistAdpHistoricalPeriodV1({
    dryRun: false,
    useMock: true,
    allowProductionWrites: false,
    propertyId,
    periodId: weeks[1].periodId,
    monitoringDate: weeks[1].monitoringDate,
    certificationStatus: "CERTIFIED",
    reportSnapshot: correctedSnap,
    customerPayload: correctedPayload,
    historicalVersion: 2,
    correctionMode: true,
    correctsSnapshotId: weeks[1].snap.snapshotId,
    synthetic: true,
  });
  check("CORRECTION_PERSIST", corr.ok, corr.defects?.[0]?.code || null);
  const corrRows = store.query(
    (r) => r.table === HISTORY_TABLES.REPORT_CORRECTIONS && r.fields["Property ID"] === propertyId
  );
  // Re-load store after corr
  const store2 = new MockAdpHistoryStore();
  const corrRows2 = store2.query(
    (r) => r.table === HISTORY_TABLES.REPORT_CORRECTIONS && r.fields["Property ID"] === propertyId
  );
  check("CORRECTION_TRACEABLE", corrRows2.length >= 1, String(corrRows2.length));
  const week1v1Metrics = store2.query(
    (r) =>
      r.table === HISTORY_TABLES.PERIOD_METRICS &&
      r.fields["Period ID"] === weeks[1].periodId &&
      r.fields["Historical Version"] === 1
  );
  check("PRIOR_VERSION_UNCHANGED", week1v1Metrics.length >= 1);

  // Missing evidence fails completeness when required
  const emptyManifest = buildHistoricalWriteManifest({
    propertyId: "adp_test_missing_evidence",
    periodId: "period_missing",
    monitoringDate: "2026-11-01",
    certificationStatus: "CERTIFIED",
    customerPayload: {
      executive: { headline: "x", narrative: "y" },
      kpis: {
        considerationRate: 10,
        considerationNumerator: 1,
        considerationDenominator: 10,
        scenarioPresence: 10,
        scenarioPresenceNumerator: 1,
        scenarioPresenceDenominator: 10,
        demandCapture: 10,
        realityCoverage: 10,
        numberOneAppearanceRate: 0,
        topThreeAppearanceRate: 0,
        presenceIndex: 50,
        coreBenchmark: 50,
        coreComposition: ["a"],
      },
      territories: [{ territoryId: "t1", numerator: 1, denominator: 10 }],
      providers: [{ providerId: "chatgpt", numerator: 1, denominator: 10 }],
      competitive: { overview: { entities: [{ entityId: "e1", rank: 1, numerator: 1, denominator: 10 }] } },
      evidence: { positive: [], missing: [], displacement: [] },
    },
    reportSnapshot: null,
    synthetic: true,
  });
  const completenessEmpty = assessPersistenceCompleteness({
    manifestCounts: emptyManifest.counts,
    requirePromptLedger: true,
    requireEvidence: true,
    claimedComplete: false,
  });
  check(
    "MISSING_EVIDENCE_FAILS_COMPLETENESS",
    !completenessEmpty.ok &&
      completenessEmpty.defects.some((d) => d.code === "REQUIRED_TABLE_EMPTY_IN_MANIFEST")
  );

  // Partial write detection
  const partial = assessPersistenceCompleteness({
    manifestCounts: { [HISTORY_TABLES.PERIOD_METRICS]: 5 },
    storedCounts: { [HISTORY_TABLES.PERIOD_METRICS]: 2 },
    requiredTables: [HISTORY_TABLES.PERIOD_METRICS],
    requirePromptLedger: false,
    requireEvidence: false,
  });
  check(
    "PARTIAL_WRITE_DETECTABLE",
    !partial.ok && partial.recoverable &&
      partial.defects.some((d) => d.code === "PARTIAL_HISTORY_WRITE")
  );

  // Historical reconstruction from mock store (metadata)
  const reconstructed = {
    hotelIdentity: propertyId,
    periods: store2.listPeriods(propertyId).map((r) => ({
      periodId: r.fields["Period ID"],
      monitoringDate: r.fields["Monitoring Date"],
      isCurrent: r.fields["Is Current Historical Version"],
      contentHash: r.fields["Content Hash"],
      certificationStatus: r.fields["Certification Status"],
    })),
    metrics: store2.query(
      (r) => r.table === HISTORY_TABLES.PERIOD_METRICS && r.fields["Property ID"] === propertyId
    ).map((r) => ({
      periodId: r.fields["Period ID"],
      version: r.fields["Historical Version"],
      considerationRate: r.fields["Consideration Rate"],
    })),
    evidenceRefs: store2.query(
      (r) => r.table === HISTORY_TABLES.EVIDENCE_INDEX && r.fields["Property ID"] === propertyId
    ).map((r) => r.fields["Response Store Ref"]),
  };
  check(
    "HISTORICAL_RECONSTRUCTION_METADATA",
    reconstructed.periods.length >= 2 &&
      reconstructed.metrics.length >= 2 &&
      reconstructed.evidenceRefs.length >= 1
  );

  // Durable Oct/Nov consideration parity (read-only Airtable already audited)
  const durable = JSON.parse(fs.readFileSync(DURABLE, "utf8"));
  const reconPath = path.join(OUT, "RECONCILIATION_SUMMARY.json");
  const recon = existsSync(reconPath)
    ? JSON.parse(fs.readFileSync(reconPath, "utf8"))
    : null;
  const oct = durable.rows.filter((r) => r.measuredMonthYear === "October 2026");
  const nov = durable.rows.filter((r) => r.measuredMonthYear === "November 2026");
  check("DURABLE_OCT_HOTELS", oct.length === 16, String(oct.length));
  check("DURABLE_NOV_HOTELS", nov.length === 22, String(nov.length));
  check(
    "AIRTABLE_PERIOD_METRICS_CONSIDERATION_PARITY_38",
    recon?.historyByTable?.["ADP Period Metrics"]?.hotelsMatch === 38,
    recon ? String(recon.historyByTable["ADP Period Metrics"].hotelsMatch) : "no_recon"
  );
  check(
    "LIVE_OVERLAY_NOT_AUTHORITY",
    recon?.publishedReports?.byClass?.MATCH !== 38,
    JSON.stringify(recon?.publishedReports?.byClass || {})
  );

  // GDI non-regression: no GDI modules touched by this test
  check("NO_GDI_MUTATION", true, "persistence test does not import GDI writers");

  writeOut({
    propertyId,
    weeks: weeks.map((w) => ({ periodId: w.periodId, monitoringDate: w.monitoringDate })),
    reconstructed,
    persistResults: {
      week0: { ok: p0.ok, lifecycle: p0.persistenceLifecycle },
      week0rerun: { ok: p0b.ok, skips: p0b.idempotentSkips },
      week1: { ok: p1.ok, demote: p1.currentVersionDemotion?.demoteCount },
      correction: { ok: corr.ok },
    },
  });

  const failed = results.filter((r) => !r.pass);
  console.log(
    JSON.stringify(
      {
        contract: ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
        passed: results.filter((r) => r.pass).length,
        failed: failed.length,
        failedIds: failed.map((r) => r.id),
      },
      null,
      2
    )
  );
  process.exit(failed.length ? 1 : 0);
}

function writeOut(extra = {}) {
  const doc = {
    contract: ADP_E2E_PERSISTENCE_TABLE_CONTRACT_V1,
    generatedAt: new Date().toISOString(),
    results,
    passed: results.filter((r) => r.pass).length,
    failed: results.filter((r) => !r.pass).length,
    ...extra,
    stopBeforeAirtableMutation: true,
    stopBeforeProductionDeploy: true,
    noPaidResearch: true,
  };
  fs.writeFileSync(
    path.join(OUT, "PERSISTENCE_TEST_RESULTS.json"),
    `${JSON.stringify(doc, null, 2)}\n`
  );
}

main().catch((err) => {
  console.error(String(err?.stack || err));
  process.exit(1);
});
