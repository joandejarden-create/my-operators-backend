/**
 * Attach governed row-level Prior Run comparisons to customer payloads.
 *
 * Source priority (no provider rerun, no fabrication):
 * 1. Competitive-history finalized snapshots (period-specific ranks / presence)
 * 2. Published report JSON for intentPresenceIndex subjectRatePct
 * 3. BPP structured history + customer pack presence displays
 *
 * Certified P1↔P2 customer lineage may exist even when live arePeriodsComparable
 * rejects V1 vs V1.1 (CORRECTED_V1_1). Prefer published currentVsPrior /
 * monitoring registry prior IDs + finalized history snapshots.
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  listCompetitiveHistorySnapshots,
  loadCompetitiveHistorySnapshot,
  computeScopeMovement,
  MOVEMENT_STATE,
} from "../competitive-history/rank-history-ledger-v1.js";
import { resolvePriorCertifiedCorePeriodId } from "../monitoring/core-monitoring-period-registry-v1.js";
import {
  resolveRowLevelPriorComparisonV1,
  formatGovernedDeltaDisplay,
  formatRankWithMovement,
  resolveRankDirection,
  DELTA_UNIT,
  ROW_MEMBERSHIP_STATE,
} from "./resolve-row-level-prior-comparison-v1.js";
import {
  PERIOD_1_ID as BPP_PERIOD_1_ID,
  PERIOD_2_ID as BPP_PERIOD_2_ID,
  formatPpDelta as bppFormatPpDelta,
} from "../brand-portfolio/bpp-period2-longitudinal-v1.js";

const PUBLISHED_DIR = join(process.cwd(), "data/ai-demand-positioning/published");
const BPP_HISTORY_ROOT = join(
  process.cwd(),
  "data/ai-demand-positioning/brand-portfolio-history/periods"
);

function readJson(path) {
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

export function loadPublishedPayloadForPeriod(propertyId, periodId) {
  if (!propertyId || !periodId) return null;
  const report = readJson(join(PUBLISHED_DIR, propertyId, `report-${periodId}.json`));
  return report?.payload || null;
}

function resolveCorePriorPeriodId(propertyId, payload) {
  return (
    payload?.executiveMetrics?.currentVsPrior?.priorComparablePeriodId ||
    payload?.period?.priorComparablePeriodId ||
    resolvePriorCertifiedCorePeriodId(propertyId) ||
    null
  );
}

function resolveCoreCurrentPeriodId(payload) {
  return payload?.period?.periodId || payload?.periodId || null;
}

function mapLedgerStateToMembership(state) {
  if (state === MOVEMENT_STATE.NEW_TO_RANKING) return ROW_MEMBERSHIP_STATE.NEW;
  if (state === MOVEMENT_STATE.EXITED) return ROW_MEMBERSHIP_STATE.EXITED;
  if (state === MOVEMENT_STATE.RETURNED) return ROW_MEMBERSHIP_STATE.RETURNED;
  if (state === MOVEMENT_STATE.MOVED || state === MOVEMENT_STATE.UNCHANGED) {
    return ROW_MEMBERSHIP_STATE.SAME;
  }
  return ROW_MEMBERSHIP_STATE.NOT_COMPARABLE;
}

/**
 * Build competitive movement by scope from finalized history using certified prior id.
 * Does NOT re-rank historical universes; uses ranks already stored on each snapshot.
 */
export function buildCoreCompetitiveMovementFromCertifiedHistory(
  propertyId,
  currentPeriodId,
  priorPeriodId
) {
  if (!propertyId || !currentPeriodId || !priorPeriodId) {
    return { comparable: false, reason: "missing_period_ids", byScope: {} };
  }
  const currentSnap =
    loadCompetitiveHistorySnapshot(propertyId, currentPeriodId) ||
    listCompetitiveHistorySnapshots(propertyId).find((h) => h.periodId === currentPeriodId);
  const priorSnap =
    loadCompetitiveHistorySnapshot(propertyId, priorPeriodId) ||
    listCompetitiveHistorySnapshots(propertyId).find((h) => h.periodId === priorPeriodId);

  if (!currentSnap?.finalized || !priorSnap?.finalized) {
    return {
      comparable: false,
      reason: !currentSnap || !priorSnap ? "missing_history_snapshot" : "snapshot_not_finalized",
      byScope: {},
      currentPeriodId,
      priorPeriodId,
    };
  }

  const history = listCompetitiveHistorySnapshots(propertyId);
  const earlier = history.filter((h) => {
    if (h.periodId === currentPeriodId || h.periodId === priorPeriodId) return false;
    const d = String(h.executionDate || "");
    return d < String(priorSnap.executionDate || "");
  });

  const byScope = {};
  for (const scopeKey of Object.keys(currentSnap.byScope || {})) {
    const earlierScopes = earlier.map((h) => h.byScope?.[scopeKey]).filter(Boolean);
    byScope[scopeKey] = {
      scopeKey,
      ...computeScopeMovement({
        currentScope: currentSnap.byScope[scopeKey],
        priorScope: priorSnap.byScope?.[scopeKey] || null,
        comparable: true,
        earlierScopes,
      }),
      priorPeriodId,
      currentPeriodId,
    };
  }

  return {
    comparable: true,
    reason: null,
    byScope,
    currentPeriodId,
    priorPeriodId,
    gate: "HISTORICAL_RANK_USES_PERIOD_SPECIFIC_UNIVERSE",
  };
}

function attachCompetitiveDisplayRows(displayRows, scopeMovement, meta) {
  if (!Array.isArray(displayRows) || !scopeMovement?.comparable) return displayRows;
  const byId = new Map((scopeMovement.rows || []).map((r) => [r.entityId, r]));

  return displayRows.map((row) => {
    const entityId = row.entityId || row.canonicalEntityId;
    if (!entityId) return row;
    const m = byId.get(entityId);
    if (!m) {
      // Visible presentation row with no history match — do not invent
      return {
        ...row,
        priorValue: null,
        delta: null,
        deltaDisplay: "—",
        priorRank: null,
        rankDelta: null,
        rankDirection: "UNAVAILABLE",
        movementState: ROW_MEMBERSHIP_STATE.NOT_COMPARABLE,
        rankDisplay: formatRankWithMovement({
          currentRank: row.displayRank === "—" ? null : row.displayRank,
        }),
      };
    }

    const comparison = resolveRowLevelPriorComparisonV1({
      measurementFamily: "CORE",
      propertyId: meta.propertyId,
      currentPeriodId: meta.currentPeriodId,
      priorPeriodId: meta.priorPeriodId,
      scopeType: "competitive",
      scopeKey: meta.scopeKey,
      canonicalRowId: entityId,
      metric: "aiPresencePct",
      currentValue: m.aiPresencePct,
      priorValue: m.priorAiPresencePct,
      currentRank: m.currentRank,
      priorRank: m.priorRank,
      currentExists: m.currentRank != null,
      priorExists: m.priorRank != null || m.priorAiPresencePct != null,
      earlierHadEntity: m.state === MOVEMENT_STATE.RETURNED,
      comparable: true,
      deltaUnit: DELTA_UNIT.PP,
    });

    // Prefer ledger membership when present
    const membership = mapLedgerStateToMembership(m.state);
    const deltaDisplay =
      membership === ROW_MEMBERSHIP_STATE.NEW
        ? "NEW"
        : membership === ROW_MEMBERSHIP_STATE.EXITED
          ? "EXITED"
          : membership === ROW_MEMBERSHIP_STATE.RETURNED && comparison.delta == null
            ? "RETURNED"
            : formatGovernedDeltaDisplay({
                delta: comparison.delta,
                deltaUnit: DELTA_UNIT.PP,
                membershipState: membership,
              }) || "—";

    const rankDelta =
      m.rankDelta != null
        ? m.rankDelta
        : comparison.rankDelta;
    const displayRank = row.displayRank === "—" ? null : row.displayRank ?? m.currentRank;

    return {
      ...row,
      priorValue: m.priorAiPresencePct,
      priorAiPresencePct: m.priorAiPresencePct,
      delta: comparison.delta,
      deltaDisplay,
      priorRank: m.priorRank,
      rankDelta,
      rankDirection: resolveRankDirection(rankDelta),
      movementState: membership,
      rankDisplay: formatRankWithMovement({
        currentRank: displayRank,
        priorRank: m.priorRank,
        rankDelta,
        membershipState: membership,
      }),
      priorRunComparison: comparison,
    };
  });
}

function attachIntentTerritoryDeltas(payload, priorPayload, meta) {
  if (!payload?.intentPresenceIndex || !priorPayload?.intentPresenceIndex) return payload;

  const nextIndex = { ...payload.intentPresenceIndex };
  for (const [intentKey, cur] of Object.entries(nextIndex)) {
    const prior = priorPayload.intentPresenceIndex[intentKey];
    const currentPct = cur?.subjectRatePct;
    const priorPct = prior?.subjectRatePct;
    const comparison = resolveRowLevelPriorComparisonV1({
      measurementFamily: "CORE",
      propertyId: meta.propertyId,
      currentPeriodId: meta.currentPeriodId,
      priorPeriodId: meta.priorPeriodId,
      scopeType: "intent_territory",
      scopeKey: intentKey,
      canonicalRowId: intentKey,
      metric: "subjectRatePct",
      currentValue: currentPct,
      priorValue: priorPct,
      currentExists: currentPct != null,
      priorExists: priorPct != null,
      comparable: true,
      deltaUnit: DELTA_UNIT.PP,
    });

    nextIndex[intentKey] = {
      ...cur,
      priorValue: priorPct ?? null,
      priorSubjectRatePct: priorPct ?? null,
      delta: comparison.delta,
      deltaDisplay: comparison.deltaDisplay,
      movementState: comparison.movementState,
      priorRunComparison: comparison,
    };
  }

  // Also mirror onto demandCapture.byIntent for consumers that read rate grain
  let nextDc = payload.demandCapture;
  if (payload.demandCapture?.byIntent && priorPayload.demandCapture?.byIntent) {
    const byIntent = { ...payload.demandCapture.byIntent };
    for (const [intentKey, cur] of Object.entries(byIntent)) {
      const prior = priorPayload.demandCapture.byIntent[intentKey];
      const comparison = resolveRowLevelPriorComparisonV1({
        measurementFamily: "CORE",
        propertyId: meta.propertyId,
        currentPeriodId: meta.currentPeriodId,
        priorPeriodId: meta.priorPeriodId,
        scopeType: "demand_capture_intent",
        scopeKey: intentKey,
        canonicalRowId: intentKey,
        metric: "rate",
        currentValue: cur?.rate,
        priorValue: prior?.rate,
        currentExists: cur?.rate != null,
        priorExists: prior?.rate != null,
        comparable: true,
        deltaUnit: DELTA_UNIT.PP,
      });
      byIntent[intentKey] = {
        ...cur,
        priorValue: prior?.rate ?? null,
        priorRate: prior?.rate ?? null,
        delta: comparison.delta,
        deltaDisplay: comparison.deltaDisplay,
        movementState: comparison.movementState,
      };
    }
    nextDc = { ...payload.demandCapture, byIntent };
  }

  return {
    ...payload,
    intentPresenceIndex: nextIndex,
    demandCapture: nextDc,
  };
}

/**
 * Enrich Core published payload with row-level Prior Run fields.
 */
export function attachCoreRowLevelPriorComparisons(propertyId, payload) {
  if (!payload || !propertyId) return payload;
  try {
    const currentPeriodId = resolveCoreCurrentPeriodId(payload);
    const priorPeriodId = resolveCorePriorPeriodId(propertyId, payload);
    if (!currentPeriodId || !priorPeriodId) return payload;

    const movement = buildCoreCompetitiveMovementFromCertifiedHistory(
      propertyId,
      currentPeriodId,
      priorPeriodId
    );

    let next = { ...payload };
    next.rowLevelPriorRun = {
      version: "adp_row_level_prior_run_v1",
      currentPeriodId,
      priorPeriodId,
      competitiveHistoryComparable: movement.comparable,
      competitiveHistoryReason: movement.reason,
      gates: {
        SAME_ROW_COMPARISON_SAME_CANONICAL_RESOLVER: true,
        HISTORICAL_RANK_USES_PERIOD_SPECIFIC_UNIVERSE: true,
      },
    };

    if (movement.comparable && next.competitiveRankingByTerritory?.byTerritory) {
      const byTerritory = { ...next.competitiveRankingByTerritory.byTerritory };
      for (const [scopeKey, block] of Object.entries(byTerritory)) {
        const scopeMovement = movement.byScope[scopeKey];
        if (!scopeMovement) continue;
        byTerritory[scopeKey] = {
          ...block,
          displayRows: attachCompetitiveDisplayRows(block.displayRows || [], scopeMovement, {
            propertyId,
            currentPeriodId,
            priorPeriodId,
            scopeKey,
          }),
          priorPeriodId,
          hasPriorPeriod: true,
        };
      }
      next.competitiveRankingByTerritory = {
        ...next.competitiveRankingByTerritory,
        byTerritory,
        priorPeriodId,
        hasPriorPeriod: true,
      };
    }

    const priorPayload = loadPublishedPayloadForPeriod(propertyId, priorPeriodId);
    if (priorPayload) {
      next = attachIntentTerritoryDeltas(next, priorPayload, {
        propertyId,
        currentPeriodId,
        priorPeriodId,
      });
    }

    return next;
  } catch (err) {
    console.error("[ADP read] row-level prior attach failed:", err.message);
    return payload;
  }
}

function parsePresencePp(row) {
  if (row == null) return null;
  if (typeof row === "number" && Number.isFinite(row)) {
    return row <= 1 ? Math.round(row * 1000) / 10 : Math.round(row * 10) / 10;
  }
  if (row.presencePct != null && Number.isFinite(Number(row.presencePct))) {
    return Math.round(Number(row.presencePct) * 10) / 10;
  }
  if (row.presenceRate != null && Number.isFinite(Number(row.presenceRate))) {
    const r = Number(row.presenceRate);
    return r <= 1 ? Math.round(r * 1000) / 10 : Math.round(r * 10) / 10;
  }
  if (row.appearanceRate != null && Number.isFinite(Number(row.appearanceRate))) {
    const r = Number(row.appearanceRate);
    return r <= 1 ? Math.round(r * 1000) / 10 : Math.round(r * 10) / 10;
  }
  if (typeof row.presenceDisplay === "string") {
    const m = row.presenceDisplay.match(/([0-9]+(?:\.[0-9]+)?)\s*%/);
    if (m) return Math.round(Number(m[1]) * 10) / 10;
  }
  return null;
}

function loadBppStructured(periodId, propertyId) {
  return readJson(join(BPP_HISTORY_ROOT, periodId, propertyId, "structured.json"));
}

/**
 * Repair BPP ranking Δ vs Prior Run from P1 structured presence + current pack rows.
 * Preserves existing priorRank / rankMovementState; fills presence deltaDisplay.
 */
export function attachBppRowLevelPriorComparisons(propertyId, bppPayload) {
  if (!bppPayload || !propertyId) return bppPayload;
  try {
    if (bppPayload.ranking?.hasPriorPeriod === false) return bppPayload;
    const priorPeriodId = bppPayload.priorPeriodId || BPP_PERIOD_1_ID;
    const currentPeriodId = bppPayload.periodId || BPP_PERIOD_2_ID;
    const priorStructured = loadBppStructured(priorPeriodId, propertyId);
    if (!priorStructured?.rankingOverall) return bppPayload;

    const priorById = new Map(
      (priorStructured.rankingOverall || []).map((r) => [r.canonicalEntityId, r])
    );

    const enrichRows = (rows) => {
      if (!Array.isArray(rows)) return rows;
      return rows.map((row) => {
        const id = row.canonicalEntityId;
        if (!id) return row;
        const prior = priorById.get(id);
        const currentPp = parsePresencePp(row);
        const priorPp =
          prior?.presencePct != null
            ? Math.round(Number(prior.presencePct) * 10) / 10
            : prior?.presenceRate != null
              ? Math.round(Number(prior.presenceRate) * 1000) / 10
              : null;
        const priorRank = row.priorRank != null ? row.priorRank : prior?.rank ?? null;
        const currentRank = row.rank ?? null;
        const comparison = resolveRowLevelPriorComparisonV1({
          measurementFamily: "BPP",
          propertyId,
          currentPeriodId,
          priorPeriodId,
          scopeType: "bpp_competitive",
          scopeKey: "overall",
          canonicalRowId: id,
          metric: "presencePct",
          currentValue: currentPp,
          priorValue: priorPp,
          currentRank,
          priorRank,
          currentExists: currentRank != null || currentPp != null,
          priorExists: prior != null,
          comparable: true,
          deltaUnit: DELTA_UNIT.PP,
        });

        const membership =
          row.rankMovementState === "NEW_TO_RANKING"
            ? ROW_MEMBERSHIP_STATE.NEW
            : row.rankMovementState === "EXITED"
              ? ROW_MEMBERSHIP_STATE.EXITED
              : row.rankMovementState === "RETURNED"
                ? ROW_MEMBERSHIP_STATE.RETURNED
                : comparison.movementState;

        const deltaDisplay =
          membership === ROW_MEMBERSHIP_STATE.NEW
            ? "NEW"
            : membership === ROW_MEMBERSHIP_STATE.EXITED
              ? "EXITED"
              : comparison.delta != null
                ? bppFormatPpDelta(comparison.delta)
                : row.deltaDisplay && row.deltaDisplay !== "—"
                  ? row.deltaDisplay
                  : "—";

        const rankDelta =
          priorRank != null && currentRank != null ? priorRank - currentRank : null;

        return {
          ...row,
          priorValue: priorPp,
          priorPresencePp: priorPp,
          delta: comparison.delta,
          deltaDisplay,
          priorRank,
          rankDelta,
          rankDirection: resolveRankDirection(rankDelta),
          movementState: membership,
          rankLabel: formatRankWithMovement({
            currentRank,
            priorRank,
            rankDelta,
            membershipState: membership,
          }),
          priorRunComparison: comparison,
        };
      });
    };

    const next = { ...bppPayload };
    if (next.ranking?.rows) {
      next.ranking = {
        ...next.ranking,
        hasPriorPeriod: true,
        rows: enrichRows(next.ranking.rows),
      };
    }

    // Provider movement already on longitudinal; ensure display pp when rates exist
    if (next.providerPresence?.rows && priorStructured.providers) {
      const priorProv = new Map(
        (priorStructured.providers || []).map((p) => [p.providerId || p.provider, p])
      );
      next.providerPresence = {
        ...next.providerPresence,
        rows: next.providerPresence.rows.map((row) => {
          const id = row.provider || row.providerId;
          const prior = priorProv.get(id);
          const currentPp = parsePresencePp(row);
          const priorPp =
            prior?.presenceRate != null
              ? Math.round(Number(prior.presenceRate) * 1000) / 10
              : null;
          const comparison = resolveRowLevelPriorComparisonV1({
            measurementFamily: "BPP",
            propertyId,
            currentPeriodId,
            priorPeriodId,
            scopeType: "bpp_provider",
            scopeKey: id,
            canonicalRowId: id,
            metric: "presenceRate",
            currentValue: currentPp,
            priorValue: priorPp,
            comparable: true,
            deltaUnit: DELTA_UNIT.PP,
          });
          return {
            ...row,
            priorValue: priorPp,
            delta: comparison.delta,
            deltaDisplay: comparison.deltaDisplay,
            movementState: comparison.movementState,
          };
        }),
      };
    }

    return next;
  } catch (err) {
    console.error("[ADP BPP] row-level prior attach failed:", err.message);
    return bppPayload;
  }
}
