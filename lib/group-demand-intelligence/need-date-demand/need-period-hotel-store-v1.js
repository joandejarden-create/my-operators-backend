/**
 * Hotel Need Periods local persistence (portal V1).
 * Canonical path: data/group-demand-intelligence/hotels/{hotelId}/need-periods.json
 * Isolated portal test: .../need-dates-portal-shadow/{hotelId}/need-periods.json
 * Never mutates Airtable. Never auto-triggers paid Exa.
 */

import fs from "node:fs";
import path from "node:path";
import { getGdiDataRoot, hotelDir } from "../repository.js";
import {
  NEED_DATE_DEMAND_INTELLIGENCE_V2,
  NEED_PERIOD_STATUS,
} from "./need-date-demand-contract-v1.js";
import {
  ensureVersionedNeedPeriod,
  createEmptyNeedPeriodChangeHistory,
  applyNeedPeriodChange,
} from "./need-period-lifecycle-v1.js";

export const NEED_DATES_PORTAL_ISOLATION_NAMESPACE = "portal_v1_test";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function readJson(p, fallback) {
  if (!fs.existsSync(p)) return fallback;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, doc) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
}

export function resolveNeedPeriodsStorePaths(hotelId, { isolation = null } = {}) {
  const id = text(hotelId);
  if (!id) throw new Error("hotel_id_required");
  if (isolation === NEED_DATES_PORTAL_ISOLATION_NAMESPACE || isolation === true) {
    const root = path.join(
      getGdiDataRoot(),
      "need-dates-portal-shadow",
      id,
    );
    return {
      hotelId: id,
      isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
      dir: root,
      periodsPath: path.join(root, "need-periods.json"),
      historyPath: path.join(root, "need-period-change-history.json"),
      customerVisible: false,
    };
  }
  const dir = hotelDir(id);
  return {
    hotelId: id,
    isolation: null,
    dir,
    periodsPath: path.join(dir, "need-periods.json"),
    historyPath: path.join(dir, "need-period-change-history.json"),
    customerVisible: true,
  };
}

export function loadNeedPeriodStore(hotelId, opts = {}) {
  const paths = resolveNeedPeriodsStorePaths(hotelId, opts);
  const periodsDoc = readJson(paths.periodsPath, null);
  const historyDoc = readJson(paths.historyPath, null);
  return {
    paths,
    periods: Array.isArray(periodsDoc?.periods)
      ? periodsDoc.periods.map((p) => ensureVersionedNeedPeriod(p))
      : [],
    history:
      historyDoc ||
      createEmptyNeedPeriodChangeHistory(paths.hotelId),
    meta: {
      version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
      hotelId: paths.hotelId,
      isolation: paths.isolation,
      customerVisible: paths.customerVisible,
      updatedAt: periodsDoc?.updatedAt || null,
      createdAt: periodsDoc?.createdAt || null,
    },
  };
}

function persistStore(store) {
  const { paths, periods, history } = store;
  const now = nowIso();
  writeJson(paths.periodsPath, {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V2,
    hotelId: paths.hotelId,
    isolation: paths.isolation,
    customerVisible: paths.customerVisible,
    createdAt: store.meta.createdAt || now,
    updatedAt: now,
    periods,
  });
  writeJson(paths.historyPath, {
    ...history,
    hotelId: paths.hotelId,
    isolation: paths.isolation,
    updatedAt: now,
  });
  store.meta.updatedAt = now;
  if (!store.meta.createdAt) store.meta.createdAt = now;
  return store;
}

/**
 * Optimistic concurrency: expectedVersion must match currentVersion when provided.
 */
export function saveNeedPeriodChange({
  hotelId,
  isolation = null,
  changeType,
  needPeriodId = null,
  patch = {},
  actorSource = "hotel_sales",
  actorUser = null,
  reason = null,
  expectedVersion = null,
} = {}) {
  const store = loadNeedPeriodStore(hotelId, { isolation });
  if (needPeriodId && expectedVersion != null) {
    const current = store.periods.find((p) => p.needPeriodId === needPeriodId);
    if (!current) {
      const err = new Error(`need_period_not_found:${needPeriodId}`);
      err.code = "NEED_PERIOD_NOT_FOUND";
      err.status = 404;
      throw err;
    }
    if (Number(current.currentVersion) !== Number(expectedVersion)) {
      const err = new Error("need_period_version_conflict");
      err.code = "NEED_PERIOD_VERSION_CONFLICT";
      err.status = 409;
      err.currentVersion = current.currentVersion;
      err.expectedVersion = expectedVersion;
      throw err;
    }
  }

  const patchWithId = { ...patch };
  if (
    changeType === "ADD" &&
    !text(patchWithId.needPeriodId) &&
    !text(needPeriodId)
  ) {
    patchWithId.needPeriodId = `ndp_${store.paths.hotelId.slice(0, 8)}_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  } else if (changeType === "ADD" && needPeriodId && !patchWithId.needPeriodId) {
    patchWithId.needPeriodId = needPeriodId;
  }

  const result = applyNeedPeriodChange({
    periods: store.periods,
    history: store.history,
    changeType,
    needPeriodId,
    patch: patchWithId,
    actorSource,
    reason,
    hotelId: store.paths.hotelId,
  });

  if (actorUser) {
    result.change.actorUser = {
      id: actorUser.id || null,
      email: actorUser.email || null,
      name: actorUser.name || null,
    };
  }

  // Mark research state: hotel edits never auto-run paid research
  const touchedIds = new Set(
    [
      result.change?.needPeriodId,
      ...(result.change?.childNeedPeriodIds || []),
      result.successor?.needPeriodId,
    ].filter(Boolean),
  );
  for (const p of result.periods) {
    if (touchedIds.has(p.needPeriodId)) {
      p.researchState = p.researchState || "MATCHING_EXISTING";
      if (
        p.status === NEED_PERIOD_STATUS.FILLED ||
        p.status === NEED_PERIOD_STATUS.CANCELLED ||
        p.status === NEED_PERIOD_STATUS.REPLACED ||
        p.status === NEED_PERIOD_STATUS.CLOSED
      ) {
        // keep prior research artifacts; discovery no longer prioritized
        if (p.researchState === "RESEARCHING") p.researchState = "RESULTS_AVAILABLE";
      } else if (
        !p.researchState ||
        p.researchState === "NOT_RESEARCHED" ||
        p.researchState === "RESULTS_AVAILABLE"
      ) {
        p.researchState = "MATCHING_EXISTING";
      }
      p.updatedAt = nowIso();
    }
  }

  store.periods = result.periods;
  store.history = result.history;
  persistStore(store);

  return {
    ok: true,
    hotelId: store.paths.hotelId,
    isolation: store.paths.isolation,
    customerVisible: store.paths.customerVisible,
    change: result.change,
    periods: store.periods,
    history: store.history,
    successor: result.successor || null,
    meta: store.meta,
  };
}

export function listNeedPeriodsForHotel(hotelId, opts = {}) {
  const store = loadNeedPeriodStore(hotelId, opts);
  return {
    ok: true,
    hotelId: store.paths.hotelId,
    isolation: store.paths.isolation,
    customerVisible: store.paths.customerVisible,
    periods: store.periods,
    history: store.history,
    meta: store.meta,
  };
}

export function getNeedPeriodById(hotelId, needPeriodId, opts = {}) {
  const store = loadNeedPeriodStore(hotelId, opts);
  const period = store.periods.find((p) => p.needPeriodId === needPeriodId);
  if (!period) return null;
  const changes = (store.history.changes || []).filter(
    (c) => c.needPeriodId === needPeriodId,
  );
  return { period, changes, meta: store.meta, isolation: store.paths.isolation };
}

export function clearIsolatedNeedPeriodStore(hotelId) {
  const paths = resolveNeedPeriodsStorePaths(hotelId, {
    isolation: NEED_DATES_PORTAL_ISOLATION_NAMESPACE,
  });
  if (fs.existsSync(paths.periodsPath)) fs.unlinkSync(paths.periodsPath);
  if (fs.existsSync(paths.historyPath)) fs.unlinkSync(paths.historyPath);
  return { ok: true, cleared: true, isolation: paths.isolation, hotelId: paths.hotelId };
}
