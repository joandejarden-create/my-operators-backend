/**
 * SYNTHETIC longitudinal history fixture — CONTROLLED VALIDATION ONLY.
 *
 * Isolation:
 *   - propertyId: adp_synth_cambridge_longitudinal_v1
 *   - periodIds: adp_synth_period_*
 *   - fixtures/ai-demand-positioning/longitudinal-synthetic/ only
 *   - NEVER under data/ai-demand-positioning/competitive-history/
 *   - never returned by production published-read
 *   - never mutates ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1
 *
 * Dates (~7 days): 2026-07-17 … 2026-08-21
 */

import { writeFileSync, mkdirSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import { MOVEMENT_STATE } from "../competitive-history/rank-history-ledger-v1.js";

export const SYNTHETIC_LONGITUDINAL_PROPERTY_ID = "adp_synth_cambridge_longitudinal_v1";
export const SYNTHETIC_FIXTURE_VERSION = "adp_synth_longitudinal_fixture_v1";
export const SYNTHETIC_ROOT = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/longitudinal-synthetic",
  SYNTHETIC_LONGITUDINAL_PROPERTY_ID
);
export const PRODUCTION_HISTORY_ROOT = join(
  process.cwd(),
  "data/ai-demand-positioning/competitive-history"
);

export const WEEK_DATES = Object.freeze([
  "2026-07-17",
  "2026-07-24",
  "2026-07-31",
  "2026-08-07",
  "2026-08-14",
  "2026-08-21",
]);

export const ENTITY = Object.freeze({
  SUBJECT: "__subject__",
  HOTEL_X: "synth_hotel_x",
  MOVED_UP: "synth_moved_up",
  MOVED_DOWN: "synth_moved_down",
  UNCHANGED: "synth_unchanged",
  NEW_ONLY: "synth_new_only",
  RETURNED: "synth_returned",
  EXITED: "synth_exited",
  OUTSIDE_IN: "synth_outside_in",
  INSIDE_OUT: "synth_inside_out",
  TIE_A: "synth_tie_a",
  TIE_B: "synth_tie_b",
  ALIAS_STABLE: "synth_alias_entity",
  BUSINESS_ONLY: "synth_business_mover",
  LEISURE_FLAT: "synth_leisure_flat",
  FAMILY_NEW: "synth_family_new",
});

const SCOPES = [
  "overall",
  "business",
  "leisure",
  "couples",
  "family",
  "group_meeting",
  "wellness",
  "adventure",
  "celebration",
];

const TREND_METRICS = [
  { realityCoverage: 92, scenarioPresence: 64, considerationRate: 41 },
  { realityCoverage: 94, scenarioPresence: 66, considerationRate: 43 },
  { realityCoverage: 93, scenarioPresence: 63, considerationRate: 42 },
  { realityCoverage: 96, scenarioPresence: 69, considerationRate: 47 },
  { realityCoverage: 97, scenarioPresence: 71, considerationRate: 49 },
  { realityCoverage: 98, scenarioPresence: 74, considerationRate: 52 },
];

function periodIdForDate(date) {
  return `adp_synth_period_${date.replace(/-/g, "")}_cambridge_long_v1`;
}

function entity(entityId, displayName, rank, tieState = "UNIQUE") {
  return {
    entityId,
    displayName,
    isSubject: entityId === ENTITY.SUBJECT,
    appearances: Math.max(1, 100 - rank),
    numerator: Math.max(1, 100 - rank),
    denominator: 100,
    aiPresenceRate: Math.max(1, 100 - rank) / 100,
    aiPresencePct: Math.max(1, 100 - rank),
    tieState,
    rank,
  };
}

/** Authoritative overall ranking tables — unique ranks within each week. */
const OVERALL_TABLES = [
  // Jul 17
  [
    [ENTITY.SUBJECT, "Cambridge Beaches Resort & Spa (SYNTH)", 1],
    [ENTITY.MOVED_DOWN, "Moved Down Hotel", 2],
    [ENTITY.EXITED, "Exited Hotel", 3],
    [ENTITY.UNCHANGED, "Unchanged Hotel", 4],
    [ENTITY.MOVED_UP, "Moved Up Hotel", 5],
    [ENTITY.ALIAS_STABLE, "Alias Hotel Old Name", 6],
    [ENTITY.RETURNED, "Returned Hotel", 7],
    [ENTITY.INSIDE_OUT, "Inside Out Hotel", 8],
    [ENTITY.HOTEL_X, "Hotel X", 9],
    [ENTITY.TIE_A, "Tie Alpha", 10],
    [ENTITY.TIE_B, "Tie Beta", 11],
    [ENTITY.OUTSIDE_IN, "Outside In Hotel", 13],
  ],
  // Jul 24 — Hotel X #8 (≈30-day baseline for Aug 21)
  [
    [ENTITY.SUBJECT, "Cambridge Beaches Resort & Spa (SYNTH)", 1],
    [ENTITY.MOVED_DOWN, "Moved Down Hotel", 2],
    [ENTITY.EXITED, "Exited Hotel", 3],
    [ENTITY.UNCHANGED, "Unchanged Hotel", 4],
    [ENTITY.MOVED_UP, "Moved Up Hotel", 5],
    [ENTITY.ALIAS_STABLE, "Alias Hotel Old Name", 6],
    [ENTITY.RETURNED, "Returned Hotel", 7],
    [ENTITY.HOTEL_X, "Hotel X", 8],
    [ENTITY.INSIDE_OUT, "Inside Out Hotel", 9],
    [ENTITY.TIE_A, "Tie Alpha", 10],
    [ENTITY.TIE_B, "Tie Beta", 11],
    [ENTITY.OUTSIDE_IN, "Outside In Hotel", 12],
  ],
  // Jul 31 — RETURNED absent
  [
    [ENTITY.SUBJECT, "Cambridge Beaches Resort & Spa (SYNTH)", 1],
    [ENTITY.MOVED_DOWN, "Moved Down Hotel", 2],
    [ENTITY.EXITED, "Exited Hotel", 3],
    [ENTITY.UNCHANGED, "Unchanged Hotel", 4],
    [ENTITY.MOVED_UP, "Moved Up Hotel", 5],
    [ENTITY.ALIAS_STABLE, "Alias Hotel Old Name", 6],
    [ENTITY.HOTEL_X, "Hotel X", 7],
    [ENTITY.INSIDE_OUT, "Inside Out Hotel", 8],
    [ENTITY.TIE_A, "Tie Alpha", 9],
    [ENTITY.TIE_B, "Tie Beta", 10],
    [ENTITY.OUTSIDE_IN, "Outside In Hotel", 11],
  ],
  // Aug 7 — month start; Hotel X #6; NEW appears; alias rename
  [
    [ENTITY.SUBJECT, "Cambridge Beaches Resort & Spa (SYNTH)", 1],
    [ENTITY.MOVED_DOWN, "Moved Down Hotel", 2],
    [ENTITY.EXITED, "Exited Hotel", 3],
    [ENTITY.UNCHANGED, "Unchanged Hotel", 4],
    [ENTITY.MOVED_UP, "Moved Up Hotel", 5],
    [ENTITY.HOTEL_X, "Hotel X", 6],
    [ENTITY.ALIAS_STABLE, "Alias Hotel New Name", 7],
    [ENTITY.INSIDE_OUT, "Inside Out Hotel", 8],
    [ENTITY.TIE_A, "Tie Alpha", 9],
    [ENTITY.TIE_B, "Tie Beta", 10],
    [ENTITY.OUTSIDE_IN, "Outside In Hotel", 11],
    [ENTITY.NEW_ONLY, "Brand New Hotel", 12],
  ],
  // Aug 14 — prior run; Hotel X #5; RETURNED back; MOVED_UP #7
  [
    [ENTITY.SUBJECT, "Cambridge Beaches Resort & Spa (SYNTH)", 1],
    [ENTITY.MOVED_DOWN, "Moved Down Hotel", 2],
    [ENTITY.EXITED, "Exited Hotel", 3],
    [ENTITY.UNCHANGED, "Unchanged Hotel", 4],
    [ENTITY.HOTEL_X, "Hotel X", 5],
    [ENTITY.ALIAS_STABLE, "Alias Hotel New Name", 6],
    [ENTITY.MOVED_UP, "Moved Up Hotel", 7],
    [ENTITY.RETURNED, "Returned Hotel", 8],
    [ENTITY.INSIDE_OUT, "Inside Out Hotel", 9],
    [ENTITY.NEW_ONLY, "Brand New Hotel", 10],
    [ENTITY.TIE_A, "Tie Alpha", 11],
    [ENTITY.TIE_B, "Tie Beta", 12],
    [ENTITY.OUTSIDE_IN, "Outside In Hotel", 13],
  ],
  // Aug 21 — current
  [
    [ENTITY.SUBJECT, "Cambridge Beaches Resort & Spa (SYNTH)", 1],
    [ENTITY.HOTEL_X, "Hotel X", 3],
    [ENTITY.UNCHANGED, "Unchanged Hotel", 4],
    [ENTITY.MOVED_UP, "Moved Up Hotel", 5],
    [ENTITY.MOVED_DOWN, "Moved Down Hotel", 6],
    [ENTITY.ALIAS_STABLE, "Alias Hotel New Name", 7],
    [ENTITY.OUTSIDE_IN, "Outside In Hotel", 8],
    [ENTITY.RETURNED, "Returned Hotel", 9],
    [ENTITY.NEW_ONLY, "Brand New Hotel", 10],
    [ENTITY.TIE_A, "Tie Alpha", 11],
    [ENTITY.TIE_B, "Tie Beta", 12],
    [ENTITY.INSIDE_OUT, "Inside Out Hotel", 14],
  ],
];

function overallEntities(weekIndex) {
  return OVERALL_TABLES[weekIndex].map(([id, name, rank]) =>
    entity(
      id,
      name,
      rank,
      id === ENTITY.TIE_A || id === ENTITY.TIE_B ? "TIED_APPEARANCES_ALPHA_BREAK" : "UNIQUE"
    )
  );
}

function territoryEntities(weekIndex, intent) {
  if (intent === "business") {
    const ranks = [5, 5, 5, 4, 3, 4]; // Aug14 #3 → Aug21 #4 = ↓1
    return [
      entity(ENTITY.SUBJECT, "Subject", 1),
      entity(ENTITY.BUSINESS_ONLY, "Business Mover", 2),
      entity(ENTITY.HOTEL_X, "Hotel X", ranks[weekIndex]),
    ];
  }
  if (intent === "leisure") {
    return [
      entity(ENTITY.SUBJECT, "Subject", 1),
      entity(ENTITY.LEISURE_FLAT, "Leisure Flat", 2),
      entity(ENTITY.UNCHANGED, "Unchanged Hotel", 3),
      entity(ENTITY.HOTEL_X, "Hotel X", 4),
    ];
  }
  if (intent === "family") {
    const rows = [entity(ENTITY.SUBJECT, "Subject", 1), entity(ENTITY.HOTEL_X, "Hotel X", 2)];
    if (weekIndex >= 5) rows.push(entity(ENTITY.FAMILY_NEW, "Family New Entrant", 3));
    return rows;
  }
  return [
    entity(ENTITY.SUBJECT, "Subject", 1),
    entity(ENTITY.HOTEL_X, "Hotel X", [9, 8, 7, 6, 5, 3][weekIndex]),
  ];
}

function buildSnapshot(weekIndex) {
  const date = WEEK_DATES[weekIndex];
  const periodId = periodIdForDate(date);
  const byScope = {};
  for (const scopeKey of SCOPES) {
    const entities =
      scopeKey === "overall" ? overallEntities(weekIndex) : territoryEntities(weekIndex, scopeKey);
    byScope[scopeKey] = {
      scopeKey,
      scopeLabel: scopeKey,
      comparableN: 100,
      periodId,
      propertyId: SYNTHETIC_LONGITUDINAL_PROPERTY_ID,
      certificationStatus: "SYNTHETIC_CERTIFIED",
      executionDate: `${date}T12:00:00.000Z`,
      entities: [...entities].sort((a, b) => a.rank - b.rank),
    };
  }
  return {
    version: "adp_competitive_rank_history_v1",
    synthetic: true,
    syntheticFixtureVersion: SYNTHETIC_FIXTURE_VERSION,
    propertyId: SYNTHETIC_LONGITUDINAL_PROPERTY_ID,
    periodId,
    calendarDate: date,
    executionDate: `${date}T12:00:00.000Z`,
    certificationStatus: "SYNTHETIC_CERTIFIED",
    finalized: true,
    longitudinalComparable: true,
    certifiedMetrics: { ...TREND_METRICS[weekIndex] },
    byScope,
    generatedAt: new Date().toISOString(),
  };
}

/** Non-comparable decoy period (same dates family) for RANK_CHANGE_NOT_COMPARABLE tests. */
export function buildNonComparableDecoySnapshot() {
  const date = "2026-08-10";
  const periodId = periodIdForDate(date);
  return {
    version: "adp_competitive_rank_history_v1",
    synthetic: true,
    propertyId: SYNTHETIC_LONGITUDINAL_PROPERTY_ID,
    periodId,
    calendarDate: date,
    executionDate: `${date}T12:00:00.000Z`,
    certificationStatus: "NOT_CERTIFIED",
    finalized: false,
    longitudinalComparable: false,
    certifiedMetrics: { realityCoverage: 50, scenarioPresence: 50, considerationRate: 50 },
    byScope: {
      overall: {
        scopeKey: "overall",
        entities: [entity(ENTITY.SUBJECT, "Subject", 1), entity(ENTITY.HOTEL_X, "Hotel X", 2)],
      },
    },
  };
}

export function buildSyntheticLongitudinalFixture() {
  const snapshots = WEEK_DATES.map((_, i) => buildSnapshot(i));
  const metricsByPeriodId = Object.fromEntries(
    snapshots.map((s) => [s.periodId, { ...s.certifiedMetrics }])
  );

  return {
    version: SYNTHETIC_FIXTURE_VERSION,
    propertyId: SYNTHETIC_LONGITUDINAL_PROPERTY_ID,
    isolation: {
      productionHistoryRoot: PRODUCTION_HISTORY_ROOT,
      fixtureRoot: SYNTHETIC_ROOT,
      neverPublish: true,
      neverLoadInCustomerApi: true,
    },
    weeks: [...WEEK_DATES],
    periodIds: snapshots.map((s) => s.periodId),
    snapshots,
    metricsByPeriodId,
    samePeriodRecoveryNote: {
      rule: "PROVIDER_RECOVERY_SAME_PERIOD_CHANGE_IS_NOT_POP_MOVEMENT",
      periodId: snapshots[5].periodId,
    },
    expectedHotelX: {
      ranks: [9, 8, 7, 6, 5, 3],
      atCurrent: {
        PRIOR_RUN: {
          priorRank: 5,
          currentRank: 3,
          rankDelta: 2,
          state: MOVEMENT_STATE.MOVED,
          baselineDate: "2026-08-14",
        },
        LAST_30_DAYS: {
          priorRank: 8,
          currentRank: 3,
          rankDelta: 5,
          state: MOVEMENT_STATE.MOVED,
          baselineDate: "2026-07-24",
          requestedAnchor: "2026-07-22",
        },
        MONTH_TO_DATE: {
          priorRank: 6,
          currentRank: 3,
          rankDelta: 3,
          state: MOVEMENT_STATE.MOVED,
          baselineDate: "2026-08-07",
        },
      },
    },
  };
}

export function writeSyntheticLongitudinalFixture() {
  const fixture = buildSyntheticLongitudinalFixture();
  if (SYNTHETIC_ROOT.startsWith(PRODUCTION_HISTORY_ROOT)) {
    throw new Error("REFUSING_TO_WRITE_SYNTHETIC_INTO_PRODUCTION_HISTORY");
  }
  mkdirSync(SYNTHETIC_ROOT, { recursive: true });
  writeFileSync(
    join(SYNTHETIC_ROOT, "manifest.json"),
    JSON.stringify(
      {
        version: fixture.version,
        propertyId: fixture.propertyId,
        weeks: fixture.weeks,
        periodIds: fixture.periodIds,
        isolation: fixture.isolation,
        expectedHotelX: fixture.expectedHotelX,
        metricsByPeriodId: fixture.metricsByPeriodId,
      },
      null,
      2
    ) + "\n"
  );
  for (const snap of fixture.snapshots) {
    writeFileSync(join(SYNTHETIC_ROOT, `${snap.periodId}.json`), JSON.stringify(snap, null, 2) + "\n");
  }
  return { fixture, dir: SYNTHETIC_ROOT };
}

export function assertNoSyntheticLeakInProductionHistory() {
  const prodDir = join(PRODUCTION_HISTORY_ROOT, SYNTHETIC_LONGITUDINAL_PROPERTY_ID);
  if (existsSync(prodDir)) {
    return { ok: false, detail: `synth property dir under production: ${prodDir}` };
  }
  const cam = join(PRODUCTION_HISTORY_ROOT, "adp_cambridge_beaches_bermuda");
  let cambridgeHistoryFileCount = 0;
  if (existsSync(cam)) {
    const files = readdirSync(cam);
    cambridgeHistoryFileCount = files.length;
    if (files.some((f) => String(f).includes("adp_synth_"))) {
      return { ok: false, detail: "synth period under cambridge production history" };
    }
  }
  return { ok: true, cambridgeHistoryFileCount };
}
