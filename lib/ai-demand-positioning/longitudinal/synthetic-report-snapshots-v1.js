/**
 * Build synthetic customer report payloads + immutable snapshots for 6 weeks.
 * Isolated under fixtures — never production published/.
 */

import { writeFileSync, rmSync, existsSync } from "fs";
import { join } from "path";
import {
  buildSyntheticLongitudinalFixture,
  SYNTHETIC_LONGITUDINAL_PROPERTY_ID,
  WEEK_DATES,
  ENTITY,
  assertNoSyntheticLeakInProductionHistory,
} from "./synthetic-longitudinal-fixture-v1.js";
import {
  buildReportSnapshotV1,
  persistReportSnapshot,
  createCorrectedSnapshot,
  SYNTHETIC_SNAPSHOT_ROOT,
  PRODUCTION_PUBLISHED_ROOT,
  loadHistoricalReportView,
  assertHistoricalReproduction,
  evaluateReportHistoryPersistence,
  sha256OfPayload,
  listReportSnapshots,
} from "./report-snapshot-v1.js";
import { auditReportSnapshotCompleteness } from "./customer-surface-persistence-registry-v1.js";
import {
  resolveComparisonWindowV1,
  COMPARISON_MODES,
} from "./comparison-window-v1.js";
import { buildCompetitiveMovementPackForComparison } from "./movement-for-comparison-v1.js";
import { buildTrendSeriesFromResolution } from "./trend-window-v1.js";

function baseKpis(metrics, weekIndex) {
  return {
    realityCoverage: metrics.realityCoverage,
    scenarioPresence: metrics.scenarioPresence,
    considerationRate: metrics.considerationRate,
    considerationNumerator: 40 + weekIndex,
    considerationDenominator: 100,
    demandCapture: 30 + weekIndex,
    scenarioPresenceNumerator: 20 + weekIndex,
    scenarioPresenceDenominator: 40,
    numberOneAppearanceRate: 10 + weekIndex,
    topThreeAppearanceRate: 25 + weekIndex,
    presenceIndex: 100 + weekIndex * 3,
    coreBenchmark: 50,
    coreComposition: ["core_a", "core_b", "core_c"],
  };
}

export function buildCustomerPayloadForWeek(fixture, weekIndex) {
  const snap = fixture.snapshots[weekIndex];
  const metrics = snap.certifiedMetrics;
  const historyThrough = fixture.snapshots.slice(0, weekIndex + 1);
  const periods = historyThrough.map((s) => ({
    periodId: s.periodId,
    executionDate: s.executionDate,
    calendarDate: s.calendarDate,
    finalized: s.finalized,
    certificationStatus: s.certificationStatus,
    longitudinalComparable: s.longitudinalComparable,
  }));

  const comparisonResolution = resolveComparisonWindowV1({
    propertyId: fixture.propertyId,
    currentPeriodId: snap.periodId,
    comparisonMode: COMPARISON_MODES.PRIOR_RUN,
    periods,
    scope: "overall",
  });

  const metricsByPeriodId = Object.fromEntries(
    historyThrough.map((s) => [s.periodId, { ...s.certifiedMetrics }])
  );

  // Historical knowledge: trend points only through this week's date
  const trendSeries = buildTrendSeriesFromResolution({
    comparisonResolution: {
      ...comparisonResolution,
      comparisonMode: COMPARISON_MODES.LAST_30_DAYS,
      periodsInTrendWindow: historyThrough,
    },
    metricsByPeriodId,
  });
  const points = (trendSeries.points || []).filter(
    (p) => String(p.calendarDate) <= String(snap.calendarDate)
  );

  const movement = buildCompetitiveMovementPackForComparison({
    currentSnapshot: snap,
    historySnapshots: historyThrough,
    comparisonResolution,
  });

  const overviewEntities = snap.byScope.overall.entities.map((e) => ({
    entityId: e.entityId,
    displayName: e.displayName,
    rank: e.rank,
    numerator: e.numerator,
    denominator: e.denominator,
    presencePct: e.aiPresencePct,
    tieState: e.tieState,
  }));

  return {
    identity: {
      propertyId: fixture.propertyId,
      propertyName: "Cambridge Beaches Resort & Spa (SYNTH)",
      periodId: snap.periodId,
      monitoringDate: snap.calendarDate,
    },
    executive: {
      headline: `Synthetic executive read for ${snap.calendarDate}`,
      narrative: `Certified synthetic positioning as of ${snap.calendarDate}.`,
    },
    kpis: baseKpis(metrics, weekIndex),
    territories: Object.keys(snap.byScope)
      .filter((k) => k !== "overall")
      .map((territoryId) => ({
        territoryId,
        name: territoryId,
        numerator: 10 + weekIndex,
        denominator: 40,
        aiPresence: 20 + weekIndex,
        coreBenchmark: 50,
        presenceIndex: 90 + weekIndex,
        rank: snap.byScope[territoryId]?.entities?.find((e) => e.isSubject)?.rank ?? null,
        missingEvidenceCount: 5,
        positiveEvidenceEligibleCount: 3,
        narrative: `${territoryId} narrative ${snap.calendarDate}`,
      })),
    providers: ["openai", "claude", "gemini", "perplexity"].map((providerId, i) => ({
      providerId,
      name: providerId,
      numerator: 8 + i,
      denominator: 15,
      presenceRate: 50 + i + weekIndex,
      missingCount: 2,
      completeness: "COMPLETE",
    })),
    competitive: {
      overview: {
        entities: overviewEntities,
        visibleSubset: overviewEntities.filter((e) => e.rank != null && e.rank <= 10),
        visibleOrdering: overviewEntities
          .filter((e) => e.rank != null && e.rank <= 10)
          .map((e) => e.entityId),
      },
      context: {
        topAlternative:
          overviewEntities.find((e) => e.entityId !== ENTITY.SUBJECT && e.rank === 2)?.displayName ||
          "Alt",
        displacement: { count: 3 + (weekIndex % 3), scenarioIds: [`synth_disp_${weekIndex}`] },
        narratives: [`Competitive context ${snap.calendarDate}`],
        actions: [`Action ${weekIndex + 1}`],
      },
    },
    realityGaps: [
      {
        findingId: `gap_${weekIndex}`,
        category: "ATTRIBUTE",
        status: "OPEN",
        wording: `Reality gap finding week ${weekIndex + 1}`,
        evidenceIds: [],
      },
    ],
    sources: [
      {
        url: `https://example.invalid/synth/${snap.calendarDate}`,
        title: "Synthetic source",
        order: 1,
      },
    ],
    evidence: {
      positive: [
        {
          observationId: `obs_pos_${snap.periodId}`,
          provider: "openai",
          periodId: snap.periodId,
          territory: "leisure",
          subjectStatus: "Appeared",
          rank: 2,
          aiResponse: `Exact synthetic LLM response for ${snap.calendarDate}. Cambridge Beaches mentioned here.`,
          subjectMentionSpans: [{ start: 48, end: 65, text: "Cambridge Beaches" }],
          competitors: ["Grotto Bay"],
          citations: [],
        },
      ],
      missing: [
        {
          observationId: `obs_miss_${snap.periodId}`,
          provider: "gemini",
          periodId: snap.periodId,
          territory: "business",
          subjectStatus: "Missing",
          rank: null,
          aiResponse: `Exact missing synthetic LLM response for ${snap.calendarDate} listing competitors only.`,
          subjectMentionSpans: [],
          competitors: ["Other Hotel"],
          citations: [],
        },
      ],
      displacement: [
        {
          observationId: `obs_disp_${snap.periodId}`,
          provider: "claude",
          periodId: snap.periodId,
          territory: "overall",
          subjectStatus: "Displaced",
          aiResponse: `Displacement evidence response ${snap.calendarDate}`,
          competitors: ["Displacer Hotel"],
          citations: [],
        },
      ],
    },
    trends: {
      comparisonMode: COMPARISON_MODES.LAST_30_DAYS,
      baselinePeriodId: comparisonResolution.comparisonPeriodId,
      points,
    },
    rankMovement: {
      comparisonMode: comparisonResolution.comparisonMode,
      comparisonPeriodId: comparisonResolution.comparisonPeriodId,
      deltaColumnLabel: comparisonResolution.deltaColumnLabel,
      byScope: Object.fromEntries(
        Object.entries(movement.byScope || {}).map(([k, v]) => [
          k,
          {
            comparable: v.comparable,
            rows: (v.rows || []).slice(0, 15).map((r) => ({
              entityId: r.entityId,
              currentRank: r.currentRank,
              priorRank: r.priorRank,
              rankDelta: r.rankDelta,
              state: r.state,
            })),
          },
        ])
      ),
    },
    actions: [{ id: `act_${weekIndex}`, label: `Review action ${weekIndex + 1}` }],
  };
}

export function buildAndPersistSyntheticReportSnapshots({ reset = true } = {}) {
  const fixture = buildSyntheticLongitudinalFixture();
  const propertyId = SYNTHETIC_LONGITUDINAL_PROPERTY_ID;
  const dir = join(SYNTHETIC_SNAPSHOT_ROOT, propertyId);
  if (reset && existsSync(dir)) {
    rmSync(dir, { recursive: true, force: true });
  }

  const snapshots = [];
  const completenessResults = [];
  for (let i = 0; i < WEEK_DATES.length; i++) {
    const payload = buildCustomerPayloadForWeek(fixture, i);
    const completeness = auditReportSnapshotCompleteness(payload);
    completenessResults.push(completeness);
    const snap = buildReportSnapshotV1({
      propertyId,
      periodId: fixture.snapshots[i].periodId,
      monitoringDate: WEEK_DATES[i],
      customerPayload: payload,
      synthetic: true,
      snapshotId: `adp_snap_synth_${WEEK_DATES[i].replace(/-/g, "")}_v1`,
      versions: { publicationCommit: "SYNTHETIC_NO_COMMIT" },
    });
    persistReportSnapshot(snap);
    snapshots.push(snap);
  }

  const originalW2 = snapshots[1];
  const correctedPayload = structuredClone(originalW2.customerPayload);
  correctedPayload.kpis.considerationRate = originalW2.customerPayload.kpis.considerationRate + 1;
  correctedPayload.executive.narrative += " [CORRECTED]";
  const { corrected } = createCorrectedSnapshot({
    originalSnapshot: originalW2,
    correctedCustomerPayload: correctedPayload,
    correctionReason: "SYNTHETIC_KPI_CORRECTION_DEMO",
  });
  persistReportSnapshot(corrected);
  writeFileSync(
    join(dir, "corrections-week2.json"),
    JSON.stringify(
      {
        originalSnapshotId: originalW2.snapshotId,
        correctedSnapshotId: corrected.snapshotId,
        reason: "SYNTHETIC_KPI_CORRECTION_DEMO",
        changedFields: corrected.changedFields,
        originalContentHash: originalW2.contentHash,
        correctedContentHash: corrected.contentHash,
        originalPreservedOnDisk: true,
        currentHistoricalVersion: corrected.snapshotId,
        trendPolicy: "USE_CURRENT_HISTORICAL_VERSION_WHEN_COMPARABLE",
      },
      null,
      2
    ) + "\n"
  );

  const persistence = evaluateReportHistoryPersistence({
    snapshotPersisted: true,
    structuredHistoryPersisted: true,
    snapshotHashVerified: snapshots.every(
      (s) => s.contentHash === sha256OfPayload(s.customerPayload)
    ),
    certificationPassed: true,
  });

  const week3 = snapshots[2];
  const hist = loadHistoricalReportView({ propertyId, snapshotId: week3.snapshotId });
  const reproduction = assertHistoricalReproduction(week3, hist);

  const leak = assertNoSyntheticLeakInProductionHistory();
  const publishedLeak =
    existsSync(join(PRODUCTION_PUBLISHED_ROOT, propertyId)) ||
    listReportSnapshots(propertyId).some((s) => !s.synthetic);

  return {
    fixture,
    snapshots,
    correctedWeek2: corrected,
    week2Correction: {
      originalSnapshotId: originalW2.snapshotId,
      correctedSnapshotId: corrected.snapshotId,
      changedFields: corrected.changedFields,
      originalHash: originalW2.contentHash,
      correctedHash: corrected.contentHash,
    },
    week3Historical: hist,
    week3ReproductionDefects: reproduction,
    completenessResults,
    persistence,
    propertyId,
    hotelXEntityId: ENTITY.HOTEL_X,
    isolation: { historyLeak: leak, publishedLeak },
  };
}
