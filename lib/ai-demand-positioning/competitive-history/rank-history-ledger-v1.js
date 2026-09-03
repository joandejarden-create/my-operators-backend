/**
 * Competitive Rank History Ledger V1
 *
 * Stores FULL governed competitive rankings per scope (Overall + each Demand Territory)
 * for certified comparable periods. Top-10 display is presentation-only.
 *
 * Movement vs immediately prior COMPARABLE CERTIFIED period:
 *   rankDelta = priorRank - currentRank  (+ means improved / moved up)
 *
 * Do NOT finalize snapshots until PROVIDER_COVERAGE_RECOVERY_COMPLETE
 * (or governed acceptable residual). Same-period recovery is not PoP movement.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import { countCanonicalPresenceAppearances } from "../customer/canonical-presence-per-observation-v1.js";
import { hotelById } from "../metrics/presence-benchmark-v1.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { roundAdpPercent } from "../format-percent.js";
import {
  OVERALL_RANKING_KEY,
  TERRITORY_INTENT_ORDER,
  COMPETITIVE_RANKING_OVERALL_VIEW_VERSION,
} from "../customer/competitive-ranking-overall-view-v1.js";
import { COMPETITIVE_RANKING_CORE_TRANSPARENCY_VERSION } from "../customer/competitive-ranking-core-transparency-v1.js";
import { arePeriodsComparable, selectPriorComparablePeriod } from "../metrics/longitudinal-comparability.js";
import { MEASUREMENT_CONTRACT_VERSION } from "../contracts/adp-measurement-contract-v1.js";
import {
  buildProviderCoverageGapLedger,
  evaluateProviderCoverageRecoveryGate,
} from "../measurement-assurance/provider-coverage-recovery-v1.js";

export const COMPETITIVE_RANK_HISTORY_VERSION = "adp_competitive_rank_history_v1";
export const RANKING_ENTITY_RESOLVER_VERSION = "adp_canonical_presence_per_observation_v1";

export const MOVEMENT_STATE = Object.freeze({
  INITIAL: "INITIAL",
  MOVED: "MOVED",
  UNCHANGED: "UNCHANGED",
  NEW_TO_RANKING: "NEW_TO_RANKING",
  RETURNED: "RETURNED",
  EXITED: "EXITED",
  RANK_CHANGE_NOT_COMPARABLE: "RANK_CHANGE_NOT_COMPARABLE",
});

export const DEFECT_INCORRECT_RANK_DELTA = "INCORRECT_RANK_DELTA";
export const DEFECT_FALSE_NEW_TO_RANKING = "FALSE_NEW_TO_RANKING";
export const DEFECT_FALSE_RETURNED = "FALSE_RETURNED";
export const DEFECT_ENTITY_HISTORY_BREAK = "ENTITY_HISTORY_BREAK";
export const DEFECT_NON_COMPARABLE_PERIOD_MOVEMENT = "NON_COMPARABLE_PERIOD_MOVEMENT";
export const DEFECT_STALE_COMPETITIVE_HISTORY = "STALE_COMPETITIVE_HISTORY";

const HISTORY_ROOT = join(process.cwd(), "data/ai-demand-positioning/competitive-history");

/** Tie rule (documented, unchanged): higher appearances first; then alphabetical name. Dense ranks 1..N. */
export const TIE_HANDLING_RULE =
  "SORT_BY_APPEARANCES_DESC_THEN_NAME_ASC; dense observedRank = index+1 after sort; equal appearances ⇒ stable alpha order, distinct consecutive ranks (no shared rank number).";

function entityDisplayName(entityId, propertyProfile) {
  const hotel = hotelById(entityId, propertyProfile);
  return hotel?.canonical || entityId.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function territoryObservations(observations, scenarios, intent) {
  if (!intent || intent === OVERALL_RANKING_KEY) {
    return filterComparableObservations(observations || []);
  }
  const ids = new Set((scenarios || []).filter((s) => s.intent === intent).map((s) => s.scenarioId));
  return filterComparableObservations(observations || []).filter((o) => ids.has(o.scenarioId));
}

/**
 * Full ranked universe for one scope (not Top-10 truncated).
 */
export function buildFullScopeRanking({
  observations,
  scenarios,
  propertyProfile,
  scopeKey,
  period,
  certificationStatus = null,
  providerCompleteness = null,
}) {
  const scoped = territoryObservations(observations, scenarios, scopeKey);
  const n = scoped.length;
  const { counts, subjectKey } = countCanonicalPresenceAppearances(scoped, propertyProfile);
  const rows = [];

  for (const [entityId, appearances] of Object.entries(counts)) {
    const isSubject = entityId === subjectKey;
    rows.push({
      entityId,
      displayName: isSubject
        ? propertyProfile?.name || "Your Property"
        : entityDisplayName(entityId, propertyProfile),
      isSubject,
      appearances,
      numerator: appearances,
      denominator: n,
      aiPresenceRate: n ? appearances / n : null,
      aiPresencePct: n ? roundAdpPercent((appearances / n) * 100) : null,
      tieState: null,
    });
  }

  rows.sort((a, b) => b.appearances - a.appearances || a.displayName.localeCompare(b.displayName));

  // Annotate ties (same appearances as neighbor) without changing dense ranks
  for (let i = 0; i < rows.length; i++) {
    const prev = rows[i - 1];
    const next = rows[i + 1];
    const tied =
      (prev && prev.appearances === rows[i].appearances) ||
      (next && next.appearances === rows[i].appearances);
    rows[i].tieState = tied ? "TIED_APPEARANCES_ALPHA_BREAK" : "UNIQUE";
    rows[i].rank = i + 1;
  }

  return {
    scopeKey,
    scopeLabel:
      scopeKey === OVERALL_RANKING_KEY ? "Overall" : territoryLabelForIntent(scopeKey) || scopeKey,
    comparableN: n,
    rankingVersion: COMPETITIVE_RANKING_OVERALL_VIEW_VERSION,
    coreTransparencyVersion: COMPETITIVE_RANKING_CORE_TRANSPARENCY_VERSION,
    entityResolverVersion: RANKING_ENTITY_RESOLVER_VERSION,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    tieHandlingRule: TIE_HANDLING_RULE,
    periodId: period?.periodId || null,
    propertyId: period?.propertyId || propertyProfile?.propertyId || null,
    certificationStatus,
    providerCompleteness,
    timestamp: new Date().toISOString(),
    executionDate: period?.executionDate || null,
    entities: rows,
  };
}

export function buildFullCompetitiveRankingSnapshot({
  period,
  scenarios,
  propertyProfile,
  certificationStatus = null,
}) {
  const ledger = buildProviderCoverageGapLedger({
    propertyId: period.propertyId,
    period,
    scenarios,
    propertyProfile,
  });
  const gate = evaluateProviderCoverageRecoveryGate(ledger);
  const providerCompleteness = {
    gateStatus: gate.status,
    blocksFullCertification: gate.blocksFullCertification,
    residualCoverage: ledger.residualCoverage?.class || null,
    missingObservations: ledger.missingObservations?.length || 0,
  };

  const scopes = [OVERALL_RANKING_KEY, ...TERRITORY_INTENT_ORDER];
  const byScope = {};
  for (const scopeKey of scopes) {
    const ranking = buildFullScopeRanking({
      observations: period.observations,
      scenarios,
      propertyProfile,
      scopeKey,
      period,
      certificationStatus,
      providerCompleteness,
    });
    if (ranking.comparableN > 0 || scopeKey === OVERALL_RANKING_KEY) {
      byScope[scopeKey] = ranking;
    }
  }

  return {
    version: COMPETITIVE_RANK_HISTORY_VERSION,
    propertyId: period.propertyId,
    periodId: period.periodId,
    executionDate: period.executionDate,
    certificationStatus,
    finalized: false,
    finalizeBlockedReason: gate.blocksFullCertification
      ? "PROVIDER_COVERAGE_RECOVERY_INCOMPLETE"
      : null,
    providerCoverageGate: gate,
    providerCompleteness,
    byScope,
    generatedAt: new Date().toISOString(),
  };
}

/**
 * Only finalize when provider coverage recovery gate allows certification decision.
 */
export function canFinalizeRankSnapshot(snapshot) {
  if (!snapshot) return { ok: false, reason: "missing_snapshot" };
  if (snapshot.providerCoverageGate?.blocksFullCertification) {
    return { ok: false, reason: "PROVIDER_COVERAGE_RECOVERY_INCOMPLETE" };
  }
  return { ok: true, reason: null };
}

export function finalizeRankSnapshot(snapshot, { certificationStatus }) {
  const gate = canFinalizeRankSnapshot(snapshot);
  if (!gate.ok) {
    return { ok: false, reason: gate.reason, snapshot };
  }
  return {
    ok: true,
    snapshot: {
      ...snapshot,
      finalized: true,
      finalizeBlockedReason: null,
      certificationStatus: certificationStatus || snapshot.certificationStatus,
      finalizedAt: new Date().toISOString(),
    },
  };
}

export function historyDir(propertyId) {
  return join(HISTORY_ROOT, propertyId);
}

export function historyPath(propertyId, periodId) {
  return join(historyDir(propertyId), `${periodId}.json`);
}

export function saveCompetitiveHistorySnapshot(snapshot) {
  const dir = historyDir(snapshot.propertyId);
  mkdirSync(dir, { recursive: true });
  const path = historyPath(snapshot.propertyId, snapshot.periodId);
  writeFileSync(path, JSON.stringify(snapshot, null, 2) + "\n");
  return path;
}

export function loadCompetitiveHistorySnapshot(propertyId, periodId) {
  const path = historyPath(propertyId, periodId);
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf8"));
}

export function listCompetitiveHistorySnapshots(propertyId) {
  const dir = historyDir(propertyId);
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => {
      try {
        return JSON.parse(readFileSync(join(dir, f), "utf8"));
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => String(a.executionDate || "").localeCompare(String(b.executionDate || "")));
}

function entityMap(scopeRanking) {
  const map = Object.create(null);
  for (const e of scopeRanking?.entities || []) {
    map[e.entityId] = e;
  }
  return map;
}

/**
 * Compute movement between current and prior finalized snapshots for one scope.
 * Inspects earlier history for RETURNED vs NEW_TO_RANKING.
 */
export function computeScopeMovement({
  currentScope,
  priorScope,
  comparable,
  earlierScopes = [],
}) {
  if (!comparable) {
    return {
      comparable: false,
      reason: "RANK_CHANGE_NOT_COMPARABLE",
      rows: [],
    };
  }

  const cur = entityMap(currentScope);
  const pri = entityMap(priorScope);
  const earlierHad = new Set();
  for (const sc of earlierScopes) {
    for (const e of sc?.entities || []) earlierHad.add(e.entityId);
  }

  const rows = [];
  const allIds = new Set([...Object.keys(cur), ...Object.keys(pri)]);

  for (const entityId of allIds) {
    const c = cur[entityId];
    const p = pri[entityId];
    let state;
    let priorRank = p?.rank ?? null;
    let currentRank = c?.rank ?? null;
    let rankDelta = null;

    if (c && p) {
      rankDelta = priorRank - currentRank;
      state = rankDelta === 0 ? MOVEMENT_STATE.UNCHANGED : MOVEMENT_STATE.MOVED;
    } else if (c && !p) {
      state = earlierHad.has(entityId) ? MOVEMENT_STATE.RETURNED : MOVEMENT_STATE.NEW_TO_RANKING;
      rankDelta = null;
    } else if (!c && p) {
      state = MOVEMENT_STATE.EXITED;
      rankDelta = null;
    }

    rows.push({
      entityId,
      displayName: c?.displayName || p?.displayName || entityId,
      isSubject: Boolean(c?.isSubject || p?.isSubject),
      aiPresencePct: c?.aiPresencePct ?? null,
      priorAiPresencePct: p?.aiPresencePct ?? null,
      currentRank,
      priorRank,
      rankDelta,
      state,
      numerator: c?.numerator ?? null,
      denominator: c?.denominator ?? null,
    });
  }

  rows.sort((a, b) => {
    if (a.currentRank == null && b.currentRank == null) return a.displayName.localeCompare(b.displayName);
    if (a.currentRank == null) return 1;
    if (b.currentRank == null) return -1;
    return a.currentRank - b.currentRank;
  });

  return { comparable: true, reason: null, rows };
}

/**
 * Build movement pack for all scopes given current snapshot + history list.
 */
export function buildCompetitiveMovementPack({
  currentSnapshot,
  historySnapshots = [],
  scenarios,
  currentPeriod,
  priorPeriod,
}) {
  const comparability = priorPeriod
    ? arePeriodsComparable(currentPeriod, priorPeriod, scenarios)
    : { comparable: false, reason: "no_prior_period" };

  const priorSnap = priorPeriod
    ? historySnapshots.find((h) => h.periodId === priorPeriod.periodId) || null
    : null;

  const earlier = historySnapshots.filter((h) => {
    if (h.periodId === currentSnapshot.periodId) return false;
    if (priorSnap && h.periodId === priorSnap.periodId) return false;
    const d = String(h.executionDate || "");
    const priorDate = String(priorSnap?.executionDate || "");
    // RETURNED = seen before the selected baseline; NEW = first appearance after baseline gap
    if (priorDate && d >= priorDate) return false;
    return d < String(currentSnapshot.executionDate || "");
  });

  const byScope = {};
  for (const scopeKey of Object.keys(currentSnapshot.byScope || {})) {
    const currentScope = currentSnapshot.byScope[scopeKey];
    const priorScope = priorSnap?.byScope?.[scopeKey] || null;
    const earlierScopes = earlier.map((h) => h.byScope?.[scopeKey]).filter(Boolean);

    if (!priorSnap || !comparability.comparable || !priorSnap.finalized || !currentSnapshot.finalized) {
      byScope[scopeKey] = {
        scopeKey,
        comparable: false,
        reason:
          !priorSnap
            ? "NO_PRIOR_SNAPSHOT"
            : !comparability.comparable
              ? comparability.reason || "RANK_CHANGE_NOT_COMPARABLE"
              : !priorSnap.finalized || !currentSnapshot.finalized
                ? "SNAPSHOT_NOT_FINALIZED"
                : "RANK_CHANGE_NOT_COMPARABLE",
        rows: (currentScope.entities || []).map((e) => ({
          entityId: e.entityId,
          displayName: e.displayName,
          isSubject: e.isSubject,
          aiPresencePct: e.aiPresencePct,
          currentRank: e.rank,
          priorRank: null,
          rankDelta: null,
          state: MOVEMENT_STATE.INITIAL,
          numerator: e.numerator,
          denominator: e.denominator,
        })),
      };
      continue;
    }

    byScope[scopeKey] = {
      scopeKey,
      ...computeScopeMovement({
        currentScope,
        priorScope,
        comparable: true,
        earlierScopes,
      }),
      priorPeriodId: priorSnap.periodId,
    };
  }

  return {
    version: COMPETITIVE_RANK_HISTORY_VERSION,
    propertyId: currentSnapshot.propertyId,
    currentPeriodId: currentSnapshot.periodId,
    priorPeriodId: priorSnap?.periodId || null,
    comparability,
    byScope,
    // Future UI fields — not rendered in production until 2+ comparable certified periods
    customerPresentationReady: Boolean(
      priorSnap?.finalized && currentSnapshot.finalized && comparability.comparable
    ),
  };
}

/**
 * Future customer payload attachment (optional, not auto-published).
 */
export function buildCustomerRankHistoryPreview(movementPack, { topN = 10 } = {}) {
  if (!movementPack) return null;
  const byScope = {};
  for (const [scopeKey, scope] of Object.entries(movementPack.byScope || {})) {
    const visible = (scope.rows || [])
      .filter((r) => r.currentRank != null)
      .slice(0, topN)
      .map((r) => ({
        entityId: r.entityId,
        name: r.displayName,
        rank: r.currentRank,
        priorRank: r.priorRank,
        rankDelta: r.rankDelta,
        state: r.state,
        // Presentation tokens for future UI — not styled yet
        rankDisplayHint:
          r.state === MOVEMENT_STATE.NEW_TO_RANKING
            ? `#${r.currentRank} NEW`
            : r.state === MOVEMENT_STATE.RETURNED
              ? `#${r.currentRank} RETURNED`
              : r.rankDelta == null
                ? `#${r.currentRank}`
                : r.rankDelta === 0
                  ? `#${r.currentRank}`
                  : r.rankDelta > 0
                    ? `#${r.currentRank} ↑${r.rankDelta}`
                    : `#${r.currentRank} ↓${Math.abs(r.rankDelta)}`,
      }));
    byScope[scopeKey] = {
      comparable: scope.comparable,
      reason: scope.reason || null,
      rows: visible,
    };
  }
  return {
    version: COMPETITIVE_RANK_HISTORY_VERSION,
    presentation: "DEFERRED_UNTIL_SECOND_COMPARABLE_CERTIFIED_PERIOD",
    customerPresentationReady: movementPack.customerPresentationReady,
    byScope,
  };
}

export { selectPriorComparablePeriod, arePeriodsComparable };
