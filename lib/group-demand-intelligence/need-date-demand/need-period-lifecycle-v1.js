/**
 * Dynamic need-period lifecycle + append-only change history (V2).
 * Never deletes periods. Opportunities stay independent of need-period status.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V2,
  NEED_PERIOD_STATUS,
  NEED_PERIOD_CHANGE_TYPE,
  NEED_PERIOD_RESEARCH_IMPLICATION,
} from "./need-date-demand-contract-v1.js";
import { normalizeNeedPeriod } from "./need-period-normalize-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function clonePeriod(p) {
  return JSON.parse(JSON.stringify(p));
}

function nextVersion(period) {
  const v = Number(period.currentVersion || period.versionNumber || 1);
  return Number.isFinite(v) && v > 0 ? v + 1 : 2;
}

export function ensureVersionedNeedPeriod(raw = {}, opts = {}) {
  const base = normalizeNeedPeriod(raw, opts);
  return {
    ...base,
    currentVersion: Number(raw.currentVersion || raw.versionNumber || 1) || 1,
    createdAt: raw.createdAt || nowIso(),
    updatedAt: raw.updatedAt || nowIso(),
    lastConfirmedByHotelAt: raw.lastConfirmedByHotelAt || null,
    changeReason: text(raw.changeReason) || null,
    supersededByNeedPeriodId: raw.supersededByNeedPeriodId || null,
    relatedNeedPeriodIds: Array.isArray(raw.relatedNeedPeriodIds)
      ? raw.relatedNeedPeriodIds.map(text).filter(Boolean)
      : [],
    linkedOpportunityIds: Array.isArray(raw.linkedOpportunityIds)
      ? raw.linkedOpportunityIds.map(text).filter(Boolean)
      : [],
    researchHistory: Array.isArray(raw.researchHistory)
      ? raw.researchHistory
      : [],
    isTestScenario:
      base.status === NEED_PERIOD_STATUS.TEST_SCENARIO ||
      raw.isTestScenario === true ||
      text(raw.source) === "fixture_test_scenario",
    preferenceOverrides: raw.preferenceOverrides ?? base.preferenceOverrides ?? null,
  };
}

export function createEmptyNeedPeriodChangeHistory(hotelId) {
  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    hotelId: text(hotelId),
    changes: [],
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
}

function appendChange(history, entry) {
  const h = history || createEmptyNeedPeriodChangeHistory(entry.hotelId);
  const change = {
    changeId:
      entry.changeId ||
      `ndch_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`,
    needPeriodId: entry.needPeriodId,
    hotelId: entry.hotelId,
    changeType: entry.changeType,
    previousValues: entry.previousValues || null,
    newValues: entry.newValues || null,
    timestamp: entry.timestamp || nowIso(),
    actorSource: entry.actorSource || "system",
    reason: text(entry.reason) || null,
    impactOnExistingMatches: entry.impactOnExistingMatches || null,
    researchImplication: entry.researchImplication || null,
    childNeedPeriodIds: entry.childNeedPeriodIds || null,
    supersededNeedPeriodId: entry.supersededNeedPeriodId || null,
  };
  h.changes.push(change);
  h.updatedAt = change.timestamp;
  return { history: h, change };
}

export function researchImplicationForChange(changeType) {
  switch (changeType) {
    case NEED_PERIOD_CHANGE_TYPE.ADD:
      return NEED_PERIOD_RESEARCH_IMPLICATION.MATCH_EXISTING_THEN_GAP;
    case NEED_PERIOD_CHANGE_TYPE.FILLED:
    case NEED_PERIOD_CHANGE_TYPE.CLOSED:
    case NEED_PERIOD_CHANGE_TYPE.CANCEL:
      return NEED_PERIOD_RESEARCH_IMPLICATION.STOP_PERIOD_DISCOVERY;
    case NEED_PERIOD_CHANGE_TYPE.PARTIALLY_FILLED:
    case NEED_PERIOD_CHANGE_TYPE.MODIFY:
    case NEED_PERIOD_CHANGE_TYPE.PRIORITY_CHANGED:
    case NEED_PERIOD_CHANGE_TYPE.REVENUE_OBJECTIVE_CHANGED:
      return NEED_PERIOD_RESEARCH_IMPLICATION.REPRIORITIZE_GAPS;
    case NEED_PERIOD_CHANGE_TYPE.REOPEN:
      return NEED_PERIOD_RESEARCH_IMPLICATION.REACTIVATE_AND_REMATCH;
    case NEED_PERIOD_CHANGE_TYPE.REPLACE:
      return NEED_PERIOD_RESEARCH_IMPLICATION.SUPERSEDE_AND_ACTIVATE_SUCCESSOR;
    case NEED_PERIOD_CHANGE_TYPE.SPLIT:
      return NEED_PERIOD_RESEARCH_IMPLICATION.MATCH_EXISTING_THEN_GAP;
    default:
      return NEED_PERIOD_RESEARCH_IMPLICATION.PRESERVE_ONLY;
  }
}

/** Statuses that still drive active need-date matching / discovery priority. */
export function isNeedPeriodCommerciallyOpen(period = {}) {
  const s = text(period.status);
  return (
    s === NEED_PERIOD_STATUS.ACTIVE ||
    s === NEED_PERIOD_STATUS.RESEARCHING ||
    s === NEED_PERIOD_STATUS.REVIEW ||
    s === NEED_PERIOD_STATUS.PARTIALLY_FILLED ||
    s === NEED_PERIOD_STATUS.DRAFT ||
    s === NEED_PERIOD_STATUS.TEST_SCENARIO
  );
}

/** Still an opportunity in GDI, but not an active need-date prospect for this period. */
export function isNeedPeriodInactiveForProspecting(period = {}) {
  const s = text(period.status);
  return (
    s === NEED_PERIOD_STATUS.FILLED ||
    s === NEED_PERIOD_STATUS.CLOSED ||
    s === NEED_PERIOD_STATUS.COMPLETED ||
    s === NEED_PERIOD_STATUS.CANCELLED ||
    s === NEED_PERIOD_STATUS.REPLACED
  );
}

/**
 * Apply a lifecycle operation. Returns updated periods array + change history entry.
 * Never deletes periods from the collection.
 */
export function applyNeedPeriodChange({
  periods = [],
  history = null,
  changeType,
  needPeriodId = null,
  patch = {},
  actorSource = "founder_test",
  reason = null,
  hotelId = null,
} = {}) {
  const list = periods.map((p) => ensureVersionedNeedPeriod(p));
  let hist =
    history ||
    createEmptyNeedPeriodChangeHistory(hotelId || list[0]?.hotelId || "");
  const implication = researchImplicationForChange(changeType);

  if (changeType === NEED_PERIOD_CHANGE_TYPE.ADD) {
    const added = ensureVersionedNeedPeriod({
      ...patch,
      hotelId: patch.hotelId || hotelId,
      status: patch.status || NEED_PERIOD_STATUS.ACTIVE,
      currentVersion: 1,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    });
    if (!added.valid && added.errors?.length) {
      // still allow TEST_SCENARIO fixtures that normalize validly
    }
    list.push(added);
    const { history: h, change } = appendChange(hist, {
      needPeriodId: added.needPeriodId,
      hotelId: added.hotelId,
      changeType,
      previousValues: null,
      newValues: snapshotPeriod(added),
      actorSource,
      reason,
      researchImplication: implication,
      impactOnExistingMatches: "NEW_PERIOD_REQUIRES_INVENTORY_MATCH",
    });
    return { periods: list, history: h, change, researchImplication: implication };
  }

  const idx = list.findIndex((p) => p.needPeriodId === needPeriodId);
  if (idx < 0) {
    const err = new Error(`need_period_not_found:${needPeriodId}`);
    err.code = "NEED_PERIOD_NOT_FOUND";
    throw err;
  }
  const prev = clonePeriod(list[idx]);

  if (changeType === NEED_PERIOD_CHANGE_TYPE.SPLIT) {
    const children = Array.isArray(patch.childPeriods) ? patch.childPeriods : [];
    if (!children.length) {
      throw new Error("split_requires_childPeriods");
    }
    const parent = list[idx];
    parent.status = NEED_PERIOD_STATUS.REPLACED;
    parent.currentVersion = nextVersion(parent);
    parent.updatedAt = nowIso();
    parent.changeReason = text(reason) || "SPLIT";
    const childNorms = [];
    for (const child of children) {
      const c = ensureVersionedNeedPeriod({
        ...child,
        hotelId: parent.hotelId,
        relatedNeedPeriodIds: [
          ...(child.relatedNeedPeriodIds || []),
          parent.needPeriodId,
        ],
        status: child.status || NEED_PERIOD_STATUS.ACTIVE,
      });
      parent.relatedNeedPeriodIds = [
        ...new Set([...(parent.relatedNeedPeriodIds || []), c.needPeriodId]),
      ];
      list.push(c);
      childNorms.push(c.needPeriodId);
    }
    parent.supersededByNeedPeriodId = childNorms[0] || null;
    const { history: h, change } = appendChange(hist, {
      needPeriodId: parent.needPeriodId,
      hotelId: parent.hotelId,
      changeType,
      previousValues: snapshotPeriod(prev),
      newValues: snapshotPeriod(parent),
      actorSource,
      reason,
      researchImplication: implication,
      childNeedPeriodIds: childNorms,
      impactOnExistingMatches: "REMATCH_CHILDREN_PRESERVE_PARENT_HISTORY",
    });
    return { periods: list, history: h, change, researchImplication: implication };
  }

  if (changeType === NEED_PERIOD_CHANGE_TYPE.REPLACE) {
    const successorRaw = patch.successorPeriod;
    if (!successorRaw) throw new Error("replace_requires_successorPeriod");
    const successor = ensureVersionedNeedPeriod({
      ...successorRaw,
      hotelId: prev.hotelId,
      relatedNeedPeriodIds: [
        ...(successorRaw.relatedNeedPeriodIds || []),
        prev.needPeriodId,
      ],
      status: successorRaw.status || NEED_PERIOD_STATUS.ACTIVE,
    });
    const current = list[idx];
    current.status = NEED_PERIOD_STATUS.REPLACED;
    current.supersededByNeedPeriodId = successor.needPeriodId;
    current.currentVersion = nextVersion(current);
    current.updatedAt = nowIso();
    current.changeReason = text(reason) || "REPLACE";
    current.relatedNeedPeriodIds = [
      ...new Set([...(current.relatedNeedPeriodIds || []), successor.needPeriodId]),
    ];
    list.push(successor);
    const { history: h, change } = appendChange(hist, {
      needPeriodId: current.needPeriodId,
      hotelId: current.hotelId,
      changeType,
      previousValues: snapshotPeriod(prev),
      newValues: snapshotPeriod(current),
      actorSource,
      reason,
      researchImplication: implication,
      supersededNeedPeriodId: current.needPeriodId,
      childNeedPeriodIds: [successor.needPeriodId],
      impactOnExistingMatches:
        "SUPERSEDE_PRIOR_KEEP_OPPORTUNITIES_ACTIVATE_SUCCESSOR",
    });
    return {
      periods: list,
      history: h,
      change,
      researchImplication: implication,
      successor,
    };
  }

  const current = list[idx];
  const statusMap = {
    [NEED_PERIOD_CHANGE_TYPE.PARTIALLY_FILLED]: NEED_PERIOD_STATUS.PARTIALLY_FILLED,
    [NEED_PERIOD_CHANGE_TYPE.FILLED]: NEED_PERIOD_STATUS.FILLED,
    [NEED_PERIOD_CHANGE_TYPE.CLOSED]: NEED_PERIOD_STATUS.CLOSED,
    [NEED_PERIOD_CHANGE_TYPE.CANCEL]: NEED_PERIOD_STATUS.CANCELLED,
    [NEED_PERIOD_CHANGE_TYPE.REOPEN]: NEED_PERIOD_STATUS.ACTIVE,
  };

  if (statusMap[changeType]) {
    current.status = statusMap[changeType];
  }
  if (changeType === NEED_PERIOD_CHANGE_TYPE.MODIFY || patch) {
    for (const [k, v] of Object.entries(patch || {})) {
      if (k === "childPeriods" || k === "successorPeriod") continue;
      if (v !== undefined) current[k] = v;
    }
    // Re-normalize dates if label/dates changed
    const renorm = normalizeNeedPeriod(current, { hotelId: current.hotelId });
    Object.assign(current, renorm, {
      currentVersion: current.currentVersion,
      createdAt: current.createdAt,
      relatedNeedPeriodIds: current.relatedNeedPeriodIds,
      linkedOpportunityIds: current.linkedOpportunityIds,
      researchHistory: current.researchHistory,
      supersededByNeedPeriodId: current.supersededByNeedPeriodId,
      isTestScenario: current.isTestScenario,
      preferenceOverrides: current.preferenceOverrides ?? null,
      airtableRecordId: current.airtableRecordId,
      calendarYear: current.calendarYear,
    });
  }
  if (changeType === NEED_PERIOD_CHANGE_TYPE.PRIORITY_CHANGED && patch.priority) {
    current.priority = patch.priority;
  }
  if (
    changeType === NEED_PERIOD_CHANGE_TYPE.REVENUE_OBJECTIVE_CHANGED &&
    patch.revenueNeed
  ) {
    current.revenueNeed = patch.revenueNeed;
  }

  current.currentVersion = nextVersion(prev);
  current.updatedAt = nowIso();
  current.changeReason = text(reason) || changeType;
  if (actorSource === "hotel_sales") {
    current.lastConfirmedByHotelAt = nowIso();
  }

  const impact =
    changeType === NEED_PERIOD_CHANGE_TYPE.FILLED ||
    changeType === NEED_PERIOD_CHANGE_TYPE.CLOSED ||
    changeType === NEED_PERIOD_CHANGE_TYPE.CANCEL
      ? "KEEP_OPPORTUNITIES_DROP_ACTIVE_NEED_DATE_PROSPECT_FLAG"
      : changeType === NEED_PERIOD_CHANGE_TYPE.REOPEN
        ? "REMATCH_EXISTING_THEN_GAP"
        : "RECALCULATE_ASSOCIATIONS_PRESERVE_HISTORY";

  const { history: h, change } = appendChange(hist, {
    needPeriodId: current.needPeriodId,
    hotelId: current.hotelId,
    changeType,
    previousValues: snapshotPeriod(prev),
    newValues: snapshotPeriod(current),
    actorSource,
    reason,
    researchImplication: implication,
    impactOnExistingMatches: impact,
  });

  return { periods: list, history: h, change, researchImplication: implication };
}

function snapshotPeriod(p) {
  return {
    needPeriodId: p.needPeriodId,
    hotelId: p.hotelId,
    periodLabel: p.periodLabel,
    startDate: p.startDate,
    endDate: p.endDate,
    revenueNeed: p.revenueNeed,
    priority: p.priority,
    status: p.status,
    currentVersion: p.currentVersion,
    supersededByNeedPeriodId: p.supersededByNeedPeriodId || null,
    targetVolume: p.targetVolume ?? null,
    targetRevenue: p.targetRevenue ?? null,
    preferredWeekpart: p.preferredWeekpart ?? null,
    preferenceOverrides: p.preferenceOverrides ?? null,
  };
}

/**
 * Diff two period collections for Preview "changes since prior run".
 */
export function diffNeedPeriodCollections(prior = [], current = []) {
  const priorMap = new Map(
    (prior || []).map((p) => [p.needPeriodId, ensureVersionedNeedPeriod(p)]),
  );
  const currMap = new Map(
    (current || []).map((p) => [p.needPeriodId, ensureVersionedNeedPeriod(p)]),
  );
  const added = [];
  const removed = [];
  const modified = [];
  for (const [id, cur] of currMap) {
    const prev = priorMap.get(id);
    if (!prev) {
      added.push(cur);
      continue;
    }
    const a = snapshotPeriod(prev);
    const b = snapshotPeriod(cur);
    if (JSON.stringify(a) !== JSON.stringify(b)) {
      modified.push({ needPeriodId: id, previous: a, current: b });
    }
  }
  for (const [id, prev] of priorMap) {
    if (!currMap.has(id)) removed.push(prev);
  }
  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    added,
    modified,
    removedNote:
      "Periods are never deleted in lifecycle ops; 'removed' only if prior snapshot omitted them.",
    removed,
  };
}

/**
 * Recalculate which matches remain active need-date prospects after status changes.
 * Does not delete associations or opportunities.
 */
export function reconcileNeedDateAssociations({
  periods = [],
  matches = [],
  historyChanges = [],
} = {}) {
  const byId = new Map(
    periods.map((p) => [p.needPeriodId, ensureVersionedNeedPeriod(p)]),
  );
  const active = [];
  const inactivePreserved = [];
  for (const m of matches || []) {
    const period = byId.get(m.needPeriodId);
    if (!period) {
      inactivePreserved.push({
        ...m,
        needDateProspectActive: false,
        inactiveReason: "period_missing_from_collection",
      });
      continue;
    }
    if (isNeedPeriodCommerciallyOpen(period)) {
      active.push({
        ...m,
        needDateProspectActive: true,
        periodStatus: period.status,
      });
    } else {
      inactivePreserved.push({
        ...m,
        needDateProspectActive: false,
        periodStatus: period.status,
        inactiveReason:
          "period_filled_cancelled_or_replaced_opportunity_preserved",
      });
    }
  }
  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    activeNeedDateProspects: active,
    preservedInactiveAssociations: inactivePreserved,
    recentChanges: historyChanges.slice(-10),
    note: "Closing a need period never deletes GDI opportunities.",
  };
}
