/**
 * Airtable persistence for Need-Date Demand Intelligence V3.
 * Authoritative store: AI Demand Positioning base.
 * Never exposes credentials to the browser. Never auto-triggers paid Exa.
 */

import Airtable from "airtable";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
} from "../../decision-outcomes/airtable-base.js";
import {
  TABLE_GDI_HOTEL_NEED_PERIODS,
  TABLE_GDI_NEED_PERIOD_CHANGE_HISTORY,
  TABLE_GDI_NEED_DATE_CAMPAIGN_RUNS,
  TABLE_GDI_NEED_PERIOD_OPPORTUNITY_MATCHES,
  F_NEED_PERIOD as NP,
  F_CHANGE_HISTORY as CH,
  F_CAMPAIGN_RUN as CR,
  F_MATCH as M,
  NEED_DATE_SCHEMA_VERSION,
  toAirtablePriority,
  fromAirtablePriority,
  toAirtableStatus,
  fromAirtableStatus,
  toAirtableRevenueNeed,
  fromAirtableRevenueNeed,
  toAirtableResearchStatus,
} from "./need-date-airtable-field-map-v3.js";
import {
  NEED_PERIOD_CHANGE_TYPE,
  NEED_PERIOD_STATUS,
  WATERSTONE_NEED_DATE_PILOT_HOTEL_ID,
} from "./need-date-demand-contract-v1.js";
import {
  ensureVersionedNeedPeriod,
  applyNeedPeriodChange,
  createEmptyNeedPeriodChangeHistory,
  researchImplicationForChange,
} from "./need-period-lifecycle-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function nowIso() {
  return new Date().toISOString();
}

export function resolveNeedDateAirtableBaseId(env = process.env) {
  return (
    env.ADP_AIRTABLE_BASE_ID ||
    env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    env.AIRTABLE_GDI_BASE_ID ||
    CANONICAL_INTELLIGENCE_BASE_ID
  ).trim();
}

export function isNeedDateAirtableLiveEnabled(env = process.env) {
  const v = String(env.GDI_NEED_DATES_AIRTABLE_LIVE || "").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes";
}

function token(env = process.env) {
  return env.AIRTABLE_API_KEY || env.AIRTABLE_PAT || "";
}

function getBase(env = process.env) {
  const apiKey = token(env);
  const baseId = resolveNeedDateAirtableBaseId(env);
  if (!apiKey || !baseId) {
    const err = new Error("need_date_airtable_not_configured");
    err.code = "AIRTABLE_NOT_CONFIGURED";
    err.status = 503;
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "need_date_v3" });
  Airtable.configure({ apiKey });
  return { base: new Airtable({ apiKey }).base(baseId), baseId };
}

function escapeFormula(v) {
  return String(v || "").replace(/'/g, "\\'");
}

function recordToPeriod(rec) {
  const f = rec.fields || {};
  const startDate = f[NP.startDate] || null;
  const endDate = f[NP.endDate] || null;
  const year =
    f[NP.calendarYear] ||
    (startDate ? Number(String(startDate).slice(0, 4)) : null);
  return ensureVersionedNeedPeriod({
    needPeriodId: f[NP.needPeriodId] || rec.id,
    hotelId: f[NP.hotelId],
    periodLabel: f[NP.periodName],
    startDate,
    endDate,
    revenueNeed: fromAirtableRevenueNeed(f[NP.revenueNeed] || []),
    priority: fromAirtablePriority(f[NP.priority]),
    status: fromAirtableStatus(f[NP.status]),
    targetVolume: f[NP.targetRoomNights] ?? null,
    targetRevenue: f[NP.targetRevenue] ?? null,
    minGroupSize: f[NP.minGroupSize] ?? null,
    maxGroupSize: f[NP.maxGroupSize] ?? null,
    preferredWeekpart: f[NP.preferredDays] || null,
    commercialRestrictions: f[NP.restrictions] || null,
    hotelSalesNotes: f[NP.salesNotes] || null,
    currentVersion: Number(f[NP.currentVersion] || 1),
    changeReason: f[NP.changeReason] || null,
    lastConfirmedByHotelAt: f[NP.lastConfirmedByHotel] || null,
    researchState: reverseResearchStatus(f[NP.researchStatus]),
    isTestScenario: Boolean(f[NP.isTestScenario]),
    source: f[NP.source] || "airtable",
    airtableRecordId: rec.id,
    calendarYear: year,
    hotelCommercialProfileIds: f[NP.hotelCommercialProfile] || [],
    supersededByNeedPeriodId: null,
    relatedNeedPeriodIds: [],
    idempotencyKey: f[NP.idempotencyKey] || null,
    preferenceOverrides: (() => {
      const raw = f[NP.periodPreferenceOverrides];
      if (!raw) return null;
      if (typeof raw === "object") return raw;
      try {
        return JSON.parse(String(raw));
      } catch {
        return null;
      }
    })(),
  });
}

function reverseResearchStatus(v) {
  const map = {
    "Not Researched": "NOT_RESEARCHED",
    "Matching Existing Opportunities": "MATCHING_EXISTING",
    "Research Recommended": "RESEARCH_RECOMMENDED",
    "Awaiting Authorization": "AWAITING_AUTHORIZATION",
    Researching: "RESEARCHING",
    "Results Available": "RESULTS_AVAILABLE",
    "Research Blocked / Failed": "RESEARCH_BLOCKED",
  };
  return map[String(v || "")] || "NOT_RESEARCHED";
}

function periodToFields(period, { commercialProfileRecordId = null } = {}) {
  const start = period.startDate;
  const year = start ? Number(String(start).slice(0, 4)) : null;
  const fields = {
    [NP.needPeriodId]: period.needPeriodId,
    [NP.hotelId]: period.hotelId,
    [NP.periodName]: period.periodLabel || period.needPeriodId,
    [NP.startDate]: period.startDate,
    [NP.endDate]: period.endDate,
    [NP.calendarYear]: year,
    [NP.revenueNeed]: toAirtableRevenueNeed(period.revenueNeed || []),
    [NP.priority]: toAirtablePriority(period.priority),
    [NP.status]: toAirtableStatus(period.status),
    [NP.currentVersion]: Number(period.currentVersion || 1),
    [NP.researchStatus]: toAirtableResearchStatus(
      period.researchState || "MATCHING_EXISTING",
    ),
    [NP.isTestScenario]:
      period.isTestScenario === true ||
      period.status === NEED_PERIOD_STATUS.TEST_SCENARIO,
    [NP.source]: period.source || "hotel_portal",
    [NP.schemaVersion]: NEED_DATE_SCHEMA_VERSION,
  };
  if (period.targetVolume != null) fields[NP.targetRoomNights] = period.targetVolume;
  if (period.targetRevenue != null) fields[NP.targetRevenue] = period.targetRevenue;
  if (period.minGroupSize != null) fields[NP.minGroupSize] = period.minGroupSize;
  if (period.maxGroupSize != null) fields[NP.maxGroupSize] = period.maxGroupSize;
  if (period.preferredWeekpart) fields[NP.preferredDays] = period.preferredWeekpart;
  if (period.commercialRestrictions) fields[NP.restrictions] = period.commercialRestrictions;
  if (period.hotelSalesNotes) fields[NP.salesNotes] = period.hotelSalesNotes;
  if (period.changeReason) fields[NP.changeReason] = period.changeReason;
  if (period.lastConfirmedByHotelAt) {
    fields[NP.lastConfirmedByHotel] = period.lastConfirmedByHotelAt;
  }
  if (period.idempotencyKey) fields[NP.idempotencyKey] = period.idempotencyKey;
  if (period.preferenceOverrides && typeof period.preferenceOverrides === "object") {
    fields[NP.periodPreferenceOverrides] = JSON.stringify(period.preferenceOverrides);
  }
  if (commercialProfileRecordId) {
    fields[NP.hotelCommercialProfile] = [commercialProfileRecordId];
  }
  return fields;
}

async function findCommercialProfileId(base, hotelId) {
  const rows = await base("Hotel Commercial Profiles")
    .select({
      filterByFormula: `{HPC Hotel ID}='${escapeFormula(hotelId)}'`,
      maxRecords: 1,
    })
    .firstPage();
  return rows[0]?.id || null;
}

export async function listNeedPeriodsFromAirtable(hotelId, opts = {}) {
  const { base, baseId } = getBase(opts.env);
  const formulaParts = [`{${NP.hotelId}}='${escapeFormula(hotelId)}'`];
  if (opts.testScenarioOnly) {
    formulaParts.push(`{${NP.isTestScenario}}=TRUE()`);
  }
  if (opts.excludeWithdrawn) {
    formulaParts.push(`{${NP.status}}!='Withdrawn'`);
  }
  const formula =
    formulaParts.length === 1
      ? formulaParts[0]
      : `AND(${formulaParts.join(",")})`;
  const rows = await base(TABLE_GDI_HOTEL_NEED_PERIODS)
    .select({ filterByFormula: formula, pageSize: 100 })
    .all();
  return {
    ok: true,
    baseId,
    hotelId,
    source: "airtable",
    customerVisible: true,
    isolation: null,
    periods: rows.map(recordToPeriod),
    meta: {
      updatedAt: nowIso(),
      count: rows.length,
      schemaVersion: NEED_DATE_SCHEMA_VERSION,
    },
  };
}

/**
 * Read-only cross-hotel inventory of GDI Hotel Need Periods (Airtable).
 * Used by universal Need-Date icon eligibility audits — never writes.
 */
export async function listAllNeedPeriodsFromAirtable(opts = {}) {
  const { base, baseId } = getBase(opts.env);
  const rows = await base(TABLE_GDI_HOTEL_NEED_PERIODS)
    .select({ pageSize: 100 })
    .all();
  const periods = rows.map(recordToPeriod);
  const byHotelId = new Map();
  for (const p of periods) {
    const hid = String(p.hotelId || "").trim() || "UNKNOWN";
    const list = byHotelId.get(hid) || [];
    list.push(p);
    byHotelId.set(hid, list);
  }
  return {
    ok: true,
    baseId,
    source: "airtable",
    periods,
    byHotelId: Object.fromEntries(byHotelId),
    hotelIds: [...byHotelId.keys()].sort(),
    meta: {
      updatedAt: nowIso(),
      count: periods.length,
      hotelCount: byHotelId.size,
      schemaVersion: NEED_DATE_SCHEMA_VERSION,
    },
  };
}

/**
 * Hotel-scoped append-only need-period change history (Airtable).
 */
export async function listHotelNeedPeriodChangeHistoryFromAirtable(
  hotelId,
  opts = {},
) {
  const { base, baseId } = getBase(opts.env);
  const rows = await base(TABLE_GDI_NEED_PERIOD_CHANGE_HISTORY)
    .select({
      filterByFormula: `{${CH.hotelId}}='${escapeFormula(hotelId)}'`,
      pageSize: 100,
      sort: [{ field: CH.changedAt, direction: "desc" }],
    })
    .all();
  const changes = rows.map((r) => {
    const prev = safeJson(r.get(CH.previousValues));
    const next = safeJson(r.get(CH.newValues));
    return {
      changeId: r.get(CH.changeId),
      hotelId,
      needPeriodId:
        next?.needPeriodId ||
        prev?.needPeriodId ||
        null,
      changeType: r.get(CH.changeType),
      previousVersion: r.get(CH.previousVersion),
      newVersion: r.get(CH.newVersion),
      previousValues: prev,
      newValues: next,
      timestamp: r.get(CH.changedAt),
      actorSource: r.get(CH.changedBy),
      reason: r.get(CH.changeReason),
      researchImplication: r.get(CH.researchImpact),
      correlationId: r.get(CH.correlationId),
      airtableRecordId: r.id,
      linkedNeedPeriodIds: r.get(CH.needPeriod) || [],
      replacement:
        String(r.get(CH.changeType) || "").toUpperCase().includes("REPLACE") ||
        String(r.get(CH.changeType) || "") === "Replaced"
          ? {
              from: prev?.periodLabel || prev?.needPeriodId || null,
              to: next?.periodLabel || next?.needPeriodId || null,
            }
          : null,
    };
  });
  return {
    ok: true,
    baseId,
    hotelId,
    changes,
    meta: { count: changes.length, schemaVersion: NEED_DATE_SCHEMA_VERSION },
  };
}

export async function getNeedPeriodFromAirtable(hotelId, needPeriodId, opts = {}) {
  const { base } = getBase(opts.env);
  const rows = await base(TABLE_GDI_HOTEL_NEED_PERIODS)
    .select({
      filterByFormula: `AND({${NP.hotelId}}='${escapeFormula(hotelId)}',{${NP.needPeriodId}}='${escapeFormula(needPeriodId)}')`,
      maxRecords: 1,
    })
    .firstPage();
  if (!rows[0]) return null;
  const period = recordToPeriod(rows[0]);
  const changes = await base(TABLE_GDI_NEED_PERIOD_CHANGE_HISTORY)
    .select({
      filterByFormula: `{${CH.hotelId}}='${escapeFormula(hotelId)}'`,
      pageSize: 100,
      sort: [{ field: CH.changedAt, direction: "desc" }],
    })
    .all();
  const filtered = changes
    .map((r) => ({
      changeId: r.get(CH.changeId),
      needPeriodId,
      changeType: r.get(CH.changeType),
      previousVersion: r.get(CH.previousVersion),
      newVersion: r.get(CH.newVersion),
      previousValues: safeJson(r.get(CH.previousValues)),
      newValues: safeJson(r.get(CH.newValues)),
      timestamp: r.get(CH.changedAt),
      actorSource: r.get(CH.changedBy),
      reason: r.get(CH.changeReason),
      researchImplication: r.get(CH.researchImpact),
      correlationId: r.get(CH.correlationId),
      airtableRecordId: r.id,
      linkedNeedPeriodIds: r.get(CH.needPeriod) || [],
    }))
    .filter(
      (c) =>
        (c.linkedNeedPeriodIds || []).includes(rows[0].id) ||
        c.newValues?.needPeriodId === needPeriodId ||
        c.previousValues?.needPeriodId === needPeriodId,
    );
  return { period, changes: filtered, airtableRecordId: rows[0].id };
}

function safeJson(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return { raw: String(raw) };
  }
}

async function appendChangeHistory(base, {
  change,
  needPeriodRecordId,
  hotelId,
  correlationId = null,
}) {
  // Idempotent: skip if Change ID already exists
  const existing = await base(TABLE_GDI_NEED_PERIOD_CHANGE_HISTORY)
    .select({
      filterByFormula: `{${CH.changeId}}='${escapeFormula(change.changeId)}'`,
      maxRecords: 1,
    })
    .firstPage();
  if (existing[0]) {
    return { skippedDuplicate: true, recordId: existing[0].id };
  }
  const fields = {
    [CH.changeId]: change.changeId,
    [CH.hotelId]: hotelId,
    [CH.previousVersion]: change.previousValues?.currentVersion ?? null,
    [CH.newVersion]: change.newValues?.currentVersion ?? null,
    [CH.changeType]: mapChangeType(change.changeType),
    [CH.previousValues]: JSON.stringify(change.previousValues || null),
    [CH.newValues]: JSON.stringify(change.newValues || null),
    [CH.changedBy]:
      change.actorUser?.email ||
      change.actorSource ||
      "system",
    [CH.changedAt]: change.timestamp || nowIso(),
    [CH.changeReason]: change.reason || null,
    [CH.researchImpact]:
      change.researchImplication ||
      change.impactOnExistingMatches ||
      null,
    [CH.correlationId]: correlationId || change.changeId,
    [CH.schemaVersion]: NEED_DATE_SCHEMA_VERSION,
  };
  if (needPeriodRecordId) fields[CH.needPeriod] = [needPeriodRecordId];
  const created = await base(TABLE_GDI_NEED_PERIOD_CHANGE_HISTORY).create(
    [{ fields }],
    { typecast: true },
  );
  return { skippedDuplicate: false, recordId: created[0].id };
}

function mapChangeType(t) {
  const s = String(t || "").toUpperCase();
  if (s === "PARTIALLY_FILLED") return "PARTIAL";
  if (s === "CLOSED") return "FILLED";
  return s;
}

/**
 * Create or update need periods + append-only history with optimistic concurrency.
 */
export async function saveNeedPeriodChangeToAirtable({
  hotelId,
  changeType,
  needPeriodId = null,
  patch = {},
  actorSource = "hotel_sales",
  actorUser = null,
  reason = null,
  expectedVersion = null,
  correlationId = null,
  env = process.env,
} = {}) {
  const { base, baseId } = getBase(env);
  const listed = await listNeedPeriodsFromAirtable(hotelId, { env });
  const commercialProfileRecordId = await findCommercialProfileId(base, hotelId);

  // Airtable ADD must always carry a stable Need Period ID. Empty IDs make
  // `{Need Period ID}=''` match the first blank row and overwrite it.
  const patchWithId = { ...patch, hotelId: patch.hotelId || hotelId };
  if (
    changeType === NEED_PERIOD_CHANGE_TYPE.ADD &&
    !String(patchWithId.needPeriodId || "").trim() &&
    !String(needPeriodId || "").trim()
  ) {
    patchWithId.needPeriodId = `ndp_${String(hotelId).slice(0, 8)}_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  } else if (
    changeType === NEED_PERIOD_CHANGE_TYPE.ADD &&
    needPeriodId &&
    !String(patchWithId.needPeriodId || "").trim()
  ) {
    patchWithId.needPeriodId = needPeriodId;
  }

  let history = createEmptyNeedPeriodChangeHistory(hotelId);
  const result = applyNeedPeriodChange({
    periods: listed.periods,
    history,
    changeType,
    needPeriodId,
    patch: patchWithId,
    actorSource,
    reason,
    hotelId,
  });
  if (actorUser) {
    result.change.actorUser = {
      id: actorUser.id || null,
      email: actorUser.email || null,
      name: actorUser.name || null,
    };
  }

  // Optimistic concurrency for existing period
  const targetId = result.change.needPeriodId;
  const existingHit = await getNeedPeriodFromAirtable(hotelId, targetId, { env });
  if (
    changeType !== NEED_PERIOD_CHANGE_TYPE.ADD &&
    expectedVersion != null &&
    existingHit &&
    Number(existingHit.period.currentVersion) !== Number(expectedVersion)
  ) {
    const err = new Error("need_period_version_conflict");
    err.code = "NEED_PERIOD_VERSION_CONFLICT";
    err.status = 409;
    err.currentVersion = existingHit.period.currentVersion;
    err.expectedVersion = expectedVersion;
    throw err;
  }

  const upserted = [];
  const errors = [];

  // Persist periods touched by this change
  const touched = new Set(
    [
      result.change.needPeriodId,
      ...(result.change.childNeedPeriodIds || []),
      result.successor?.needPeriodId,
    ].filter(Boolean),
  );

  for (const period of result.periods) {
    if (!touched.has(period.needPeriodId) && changeType !== NEED_PERIOD_CHANGE_TYPE.ADD) {
      // Still persist status/version for modified parent
      if (period.needPeriodId !== needPeriodId) continue;
    }
    if (!touched.has(period.needPeriodId) && period.needPeriodId !== needPeriodId) continue;

    try {
      const fields = periodToFields(period, { commercialProfileRecordId });
      // Never look up by blank Need Period ID — that collapses all ADDs onto one row.
      const existing =
        period.needPeriodId && String(period.needPeriodId).trim()
          ? await base(TABLE_GDI_HOTEL_NEED_PERIODS)
              .select({
                filterByFormula: `{${NP.needPeriodId}}='${escapeFormula(period.needPeriodId)}'`,
                maxRecords: 1,
              })
              .firstPage()
          : [];
      let recordId;
      if (existing[0]) {
        if (
          expectedVersion != null &&
          period.needPeriodId === needPeriodId &&
          Number(existing[0].get(NP.currentVersion) || 1) !== Number(expectedVersion) &&
          changeType !== NEED_PERIOD_CHANGE_TYPE.ADD
        ) {
          const err = new Error("need_period_version_conflict");
          err.code = "NEED_PERIOD_VERSION_CONFLICT";
          err.status = 409;
          err.currentVersion = existing[0].get(NP.currentVersion);
          throw err;
        }
        const updated = await base(TABLE_GDI_HOTEL_NEED_PERIODS).update(
          [{ id: existing[0].id, fields }],
          { typecast: true },
        );
        recordId = updated[0].id;
      } else {
        // Idempotent create by Need Period ID
        const created = await base(TABLE_GDI_HOTEL_NEED_PERIODS).create(
          [{ fields }],
          { typecast: true },
        );
        recordId = created[0].id;
      }
      upserted.push({ needPeriodId: period.needPeriodId, recordId });
      period.airtableRecordId = recordId;
    } catch (e) {
      errors.push({ needPeriodId: period.needPeriodId, error: e.message, code: e.code });
      if (e.code === "NEED_PERIOD_VERSION_CONFLICT") throw e;
    }
  }

  if (errors.length && !upserted.length) {
    const err = new Error("need_period_airtable_write_failed");
    err.code = "AIRTABLE_WRITE_FAILED";
    err.status = 502;
    err.details = errors;
    throw err;
  }

  const primaryRecordId =
    upserted.find((u) => u.needPeriodId === result.change.needPeriodId)
      ?.recordId || upserted[0]?.recordId;

  // Wire superseded-by / related links when possible
  if (result.successor?.needPeriodId && primaryRecordId) {
    const succ = upserted.find(
      (u) => u.needPeriodId === result.successor.needPeriodId,
    );
    if (succ) {
      await base(TABLE_GDI_HOTEL_NEED_PERIODS).update(
        [
          {
            id: primaryRecordId,
            fields: { [NP.supersededBy]: [succ.recordId] },
          },
        ],
        { typecast: true },
      );
    }
  }

  const hist = await appendChangeHistory(base, {
    change: result.change,
    needPeriodRecordId: primaryRecordId,
    hotelId,
    correlationId,
  });

  if (errors.length) {
    return {
      ok: false,
      partial: true,
      baseId,
      hotelId,
      change: result.change,
      periods: result.periods,
      upserted,
      errors,
      historyWrite: hist,
      message:
        "Partial Airtable write — some records failed; history may be incomplete. Retry is idempotent by Need Period ID / Change ID.",
    };
  }

  return {
    ok: true,
    baseId,
    hotelId,
    source: "airtable",
    change: result.change,
    periods: result.periods,
    successor: result.successor || null,
    upserted,
    historyWrite: hist,
    researchImplication: researchImplicationForChange(changeType),
  };
}

export async function upsertNeedPeriodOpportunityMatch({
  hotelId,
  needPeriodId,
  opportunityId,
  matchingEvidence = null,
  timingOverlap = null,
  revenueRelevance = null,
  matchClass = "Existing Match",
  matchStatus = "Active",
  env = process.env,
} = {}) {
  const { base } = getBase(env);
  const matchKey = `${needPeriodId}__${opportunityId}`;
  const periodHit = await getNeedPeriodFromAirtable(hotelId, needPeriodId, { env });
  const fields = {
    [M.matchKey]: matchKey,
    [M.needPeriodId]: needPeriodId,
    [M.opportunityId]: opportunityId,
    [M.matchingEvidence]:
      typeof matchingEvidence === "string"
        ? matchingEvidence
        : JSON.stringify(matchingEvidence || {}),
    [M.timingOverlap]: timingOverlap,
    [M.revenueRelevance]:
      typeof revenueRelevance === "string"
        ? revenueRelevance
        : JSON.stringify(revenueRelevance || {}),
    [M.matchClass]: matchClass,
    [M.matchStatus]: matchStatus,
    [M.lastVerifiedAt]: nowIso(),
    [M.schemaVersion]: NEED_DATE_SCHEMA_VERSION,
  };
  if (periodHit?.airtableRecordId) {
    fields[M.needPeriod] = [periodHit.airtableRecordId];
  }
  const existing = await base(TABLE_GDI_NEED_PERIOD_OPPORTUNITY_MATCHES)
    .select({
      filterByFormula: `{${M.matchKey}}='${escapeFormula(matchKey)}'`,
      maxRecords: 1,
    })
    .firstPage();
  if (existing[0]) {
    await base(TABLE_GDI_NEED_PERIOD_OPPORTUNITY_MATCHES).update(
      [{ id: existing[0].id, fields }],
      { typecast: true },
    );
    return { ok: true, matchKey, updated: true, recordId: existing[0].id };
  }
  const created = await base(TABLE_GDI_NEED_PERIOD_OPPORTUNITY_MATCHES).create(
    [{ fields }],
    { typecast: true },
  );
  return { ok: true, matchKey, created: true, recordId: created[0].id };
}

export async function createNeedDateCampaignRunDraft(input = {}, env = process.env) {
  const { base } = getBase(env);
  const campaignId =
    input.campaignId ||
    `ndc_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  const fields = {
    [CR.campaignId]: campaignId,
    [CR.hotelId]: input.hotelId,
    [CR.researchMode]: input.researchMode || "PREVIEW",
    [CR.selectedStrategies]: Array.isArray(input.selectedStrategies)
      ? input.selectedStrategies.join(", ")
      : String(input.selectedStrategies || ""),
    [CR.researchStatus]: "Awaiting Authorization",
    [CR.approvedBudget]: input.approvedBudget ?? null,
    [CR.actualSpend]: 0,
    [CR.existingMatches]: input.existingMatches ?? 0,
    [CR.enrichedExisting]: 0,
    [CR.newQualified]: 0,
    [CR.researchContinuation]: 0,
    [CR.duplicatesRejected]: 0,
    [CR.certificationResults]: JSON.stringify(input.certificationResults || {}),
    [CR.publicationStatus]: "Not Published",
    [CR.startedAt]: nowIso(),
    [CR.schemaVersion]: NEED_DATE_SCHEMA_VERSION,
  };
  if (Array.isArray(input.linkedNeedPeriodRecordIds) && input.linkedNeedPeriodRecordIds.length) {
    fields[CR.linkedNeedPeriods] = input.linkedNeedPeriodRecordIds;
  }
  const created = await base(TABLE_GDI_NEED_DATE_CAMPAIGN_RUNS).create(
    [{ fields }],
    { typecast: true },
  );
  return { ok: true, campaignId, recordId: created[0].id, paidSpendBlocked: true };
}

/**
 * Seed Waterstone Aug/Sep/Oct 2027 TEST_SCENARIO records (idempotent by Need Period ID).
 */
export async function seedWaterstoneAirtableTestPeriods(env = process.env) {
  const hotelId = WATERSTONE_NEED_DATE_PILOT_HOTEL_ID;
  const months = [
    {
      needPeriodId: "ndp_airtable_waterstone_aug_2027",
      periodLabel: "August 2027",
      startDate: "2027-08-01",
      endDate: "2027-08-31",
    },
    {
      needPeriodId: "ndp_airtable_waterstone_sep_2027",
      periodLabel: "September 2027",
      startDate: "2027-09-01",
      endDate: "2027-09-30",
    },
    {
      needPeriodId: "ndp_airtable_waterstone_oct_2027",
      periodLabel: "October 2027",
      startDate: "2027-10-01",
      endDate: "2027-10-31",
    },
  ];
  const created = [];
  for (const m of months) {
    const r = await saveNeedPeriodChangeToAirtable({
      hotelId,
      changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
      patch: {
        needPeriodId: m.needPeriodId,
        hotelId,
        periodLabel: m.periodLabel,
        startDate: m.startDate,
        endDate: m.endDate,
        revenueNeed: [
          "GROUP_GUESTROOMS",
          "MEETING_SPACE",
          "BANQUETS",
          "LOCAL_CATERING",
        ],
        priority: "HIGH",
        status: NEED_PERIOD_STATUS.TEST_SCENARIO,
        isTestScenario: true,
        source: "founder_test_scenario_v3",
        researchState: "MATCHING_EXISTING",
        hotelSalesNotes:
          "Founder-defined RESEARCH TEST PERIOD — not a hotel-submitted occupancy deficiency. No invented room-night or revenue targets.",
        idempotencyKey: `seed:${m.needPeriodId}`,
      },
      actorSource: "founder_test",
      actorUser: { id: "founder", email: "founder@dealality.test" },
      reason: "V3 Waterstone TEST_SCENARIO seed",
      correlationId: `seed-${m.needPeriodId}`,
      env,
    });
    // If already exists, ADD may duplicate — handle by checking first
    created.push(r);
  }
  return { ok: true, hotelId, results: created };
}

/**
 * Safer seed: upsert by Need Period ID without double ADD when present.
 */
export async function ensureWaterstoneAirtableTestPeriods(env = process.env) {
  const hotelId = WATERSTONE_NEED_DATE_PILOT_HOTEL_ID;
  const listed = await listNeedPeriodsFromAirtable(hotelId, { env });
  const byId = new Map(listed.periods.map((p) => [p.needPeriodId, p]));
  const wanted = [
    "ndp_airtable_waterstone_aug_2027",
    "ndp_airtable_waterstone_sep_2027",
    "ndp_airtable_waterstone_oct_2027",
  ];
  const missing = wanted.filter((id) => !byId.has(id));
  const { base } = getBase(env);
  const commercialProfileRecordId = await findCommercialProfileId(base, hotelId);
  const specs = {
    ndp_airtable_waterstone_aug_2027: {
      label: "August 2027",
      start: "2027-08-01",
      end: "2027-08-31",
    },
    ndp_airtable_waterstone_sep_2027: {
      label: "September 2027",
      start: "2027-09-01",
      end: "2027-09-30",
    },
    ndp_airtable_waterstone_oct_2027: {
      label: "October 2027",
      start: "2027-10-01",
      end: "2027-10-31",
    },
  };
  const restored = [];
  for (const id of wanted) {
    const existing = byId.get(id);
    if (!existing) continue;
    if (
      existing.status === NEED_PERIOD_STATUS.TEST_SCENARIO &&
      existing.isTestScenario !== false
    ) {
      continue;
    }
    await saveNeedPeriodChangeToAirtable({
      hotelId,
      changeType: NEED_PERIOD_CHANGE_TYPE.REOPEN,
      needPeriodId: id,
      expectedVersion: existing.currentVersion,
      patch: {
        status: NEED_PERIOD_STATUS.TEST_SCENARIO,
        isTestScenario: true,
        researchState: "MATCHING_EXISTING",
        hotelSalesNotes:
          "Founder-defined RESEARCH TEST PERIOD — not a hotel-submitted occupancy deficiency.",
        startDate: specs[id].start,
        endDate: specs[id].end,
        periodLabel: specs[id].label,
        idempotencyKey: `restore-test-scenario:${id}:v4`,
      },
      actorSource: "founder_test",
      reason:
        "V4 restore Aug/Sep/Oct research baseline to TEST_SCENARIO after lifecycle isolation",
      env,
    });
    restored.push(id);
  }
  for (const id of missing) {
    await saveNeedPeriodChangeToAirtable({
      hotelId,
      changeType: NEED_PERIOD_CHANGE_TYPE.ADD,
      patch: {
        needPeriodId: id,
        hotelId,
        periodLabel: specs[id].label,
        startDate: specs[id].start,
        endDate: specs[id].end,
        revenueNeed: [
          "GROUP_GUESTROOMS",
          "MEETING_SPACE",
          "BANQUETS",
          "LOCAL_CATERING",
        ],
        priority: "HIGH",
        status: NEED_PERIOD_STATUS.TEST_SCENARIO,
        isTestScenario: true,
        source: "founder_test_scenario_v3",
        researchState: "MATCHING_EXISTING",
        hotelSalesNotes:
          "Founder-defined RESEARCH TEST PERIOD — not a hotel-submitted occupancy deficiency.",
        idempotencyKey: `seed:${id}`,
      },
      actorSource: "founder_test",
      reason: "V3 Waterstone TEST_SCENARIO seed",
      env,
    });
  }
  // Keep November lifecycle isolate out of research baseline
  const nov = byId.get("ndp_airtable_waterstone_nov_2027_lifecycle");
  if (
    nov &&
    nov.status !== NEED_PERIOD_STATUS.CANCELLED &&
    nov.status !== "Cancelled" &&
    nov.status !== "Withdrawn"
  ) {
    await saveNeedPeriodChangeToAirtable({
      hotelId,
      changeType: NEED_PERIOD_CHANGE_TYPE.CANCEL,
      needPeriodId: nov.needPeriodId,
      expectedVersion: nov.currentVersion,
      patch: {
        status: NEED_PERIOD_STATUS.CANCELLED,
        idempotencyKey: `isolate-nov:${nov.needPeriodId}:v4`,
      },
      actorSource: "founder_test",
      reason: "V4 isolate November lifecycle record from live research baseline",
      env,
    });
  }
  const again = await listNeedPeriodsFromAirtable(hotelId, { env });
  return {
    ok: true,
    hotelId,
    created: missing,
    restored,
    commercialProfileRecordId,
    periods: again.periods.filter((p) => wanted.includes(p.needPeriodId)),
  };
}
