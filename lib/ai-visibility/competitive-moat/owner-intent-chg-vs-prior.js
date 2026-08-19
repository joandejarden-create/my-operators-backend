/**
 * Owner Intent Chg vs Prior Run V1 — period-over-period, not a trend.
 * Stored corpus only. No provider calls. No benchmark certification changes.
 *
 * Comparison unit: subjectBrandId × scenarioId × providerScope × geography × language.
 * Two comparable periods = CURRENT VS PRIOR. Do not label as trend.
 */

import fs from "fs";
import path from "path";
import { PRIMARY_BASELINE_DATE } from "../brand-longitudinal/baseline-audit.js";
import { normalizeMeasurementDate } from "../brand-longitudinal/grain.js";
import {
  qualifyMeasurementPeriod,
  readMeasurementPeriodManifest,
  BRAND_LONGITUDINAL_STORE_ROOT,
} from "../brand-longitudinal/measurement-period.js";
import { normalizeLanguage } from "../language-dimension.js";
import { PEER_SET_ID_V2, PEER_SET_ID_V3 } from "../peer-sets.js";
import { computeAiPresenceRate } from "../metrics.js";
import { providersMatch } from "../provider-dimension.js";
import { listShowcaseMonitoringBrandIds } from "../brand-ai-showcase-companies.js";
import { SCENARIO_IDS as S } from "./benchmark-brand-ids.js";
import { resolveObservationScenario } from "./prompt-scenario-bridge.js";
import {
  BASELINE_MEASUREMENT_PERIOD,
  CROSS_PERIOD_DEDUPLICATION,
  POOLED_ALL_PERIODS_INDEX,
} from "./period-scoped-grain.js";
import { listAvailableMeasurementPeriods } from "./period-response-sources.js";
import {
  BENCHMARK_SCOPES,
  PROVIDER_SCOPE_IDS,
  lookupScopeCertification,
  benchmarkScopeFromProvider,
  providerIdFromBenchmarkScope,
  verifyAllProvidersFrozenBaseline,
} from "./provider-scoped-benchmark-certification.js";

export const OWNER_INTENT_CHG_VS_PRIOR_VERSION = "owner_intent_chg_vs_prior_v1";
export const CHG_VS_PRIOR_PROVIDER_CALLS = 0;
export const CUSTOMER_TREND_LABELS_ENABLED = false;

export const COMPARISON_STATUS = Object.freeze({
  COMPARABLE: "COMPARABLE",
  NO_PRIOR_PERIOD: "NO_PRIOR_PERIOD",
  PRIOR_NOT_CERTIFIED: "PRIOR_NOT_CERTIFIED",
  MEASUREMENT_CONTRACT_CHANGED: "MEASUREMENT_CONTRACT_CHANGED",
  INSUFFICIENT_COMPARABLE_HISTORY: "INSUFFICIENT_COMPARABLE_HISTORY",
});

export const CHG_VS_PRIOR_INFO_COPY = Object.freeze({
  title: "Chg vs Prior Run",
  body:
    "Shows the change in this Owner Intent's certified AI Presence Index versus the most recent comparable prior measurement run. Change is shown in index points. It is not a long-term trend.",
});

export const SHORT_INTERVAL_NOTE =
  "These measurements are from separate comparable runs and should be interpreted as period-over-period movement, not a long-term trend.";

export const NO_PRIOR_CUSTOMER_COPY = "No comparable prior run yet.";
export const NO_HISTORY_TOOLTIP = "No comparable prior run exists for this Owner Intent and provider scope.";

const COVERAGE_OWNER_INTENT_DISPLAY_ORDER = Object.freeze([
  S.SOFT_BRAND,
  S.CONVERSION_SUITABILITY,
  S.OWNER_FLEXIBILITY,
  S.LIFESTYLE,
  S.INDEPENDENT_UU_CONVERSION,
  S.NEWBUILD_UU,
  S.BRANDED_RESIDENCES,
  S.CHAIN_SCALE,
  S.MARKET_ENTRY,
  S.OWNER_ECONOMICS,
  S.DISTRIBUTION_LOYALTY,
  S.HMA_VS_FRANCHISE,
]);

const FORBIDDEN_CUSTOMER_TREND_WORDS = Object.freeze([
  "Trend",
  "Trending up",
  "Trending down",
  "Improving",
  "Declining",
]);

const EMPTY_CUSTOMER_HISTORY = Object.freeze({
  historyAvailable: false,
  currentPeriodDate: null,
  priorPeriodDate: null,
  currentIndex: null,
  priorIndex: null,
  indexChangePoints: null,
  currentPresence: null,
  priorPresence: null,
  presenceChangePoints: null,
  comparisonStatus: COMPARISON_STATUS.NO_PRIOR_PERIOD,
  chgVsPriorDisplay: null,
  presenceHistoryAvailable: false,
  shortInterval: false,
});

function peerSetFromNotes(notes = []) {
  const joined = (notes || []).join(" ");
  if (joined.includes(PEER_SET_ID_V3)) return PEER_SET_ID_V3;
  if (joined.includes(PEER_SET_ID_V2)) return PEER_SET_ID_V2;
  return null;
}

function measurementContractKey(period = {}) {
  if (!period) return null;
  if (
    period.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD ||
    period.kind === "FROZEN_CERTIFIED_BASELINE"
  ) {
    return `peer:${PEER_SET_ID_V2}|cert:provider_scoped_benchmark_certification_v1`;
  }
  const peerSet =
    period.peerSetId ||
    peerSetFromNotes(period.comparabilityNotes) ||
    PEER_SET_ID_V3;
  return `peer:${peerSet}|cert:period_scoped`;
}

export function qualifyPeriodForChgVsPrior(period) {
  if (!period?.measurementPeriodId) {
    return { valid: false, qualityState: "FAILED", reason: "missing_period" };
  }
  if (
    period.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD ||
    period.kind === "FROZEN_CERTIFIED_BASELINE"
  ) {
    return { valid: true, qualityState: "VALID", reason: null };
  }
  if (period.qualityState === "FAILED" || period.qualityState === "PLANNED" || period.qualityState === "RUNNING") {
    return { valid: false, qualityState: period.qualityState, reason: "not_completed" };
  }
  const q = qualifyMeasurementPeriod({
    plannedCalls: period.plannedCalls,
    successfulCalls: period.successfulCalls,
    qualityState: period.qualityState,
  });
  return q;
}

export function listGovernedMeasurementPeriods(opts = {}) {
  const catalog = listAvailableMeasurementPeriods(opts);
  const out = [];
  for (const entry of catalog) {
    const manifest =
      entry.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD
        ? null
        : readMeasurementPeriodManifest(entry.measurementPeriodId, opts.storeRoot);
    const period = {
      measurementPeriodId: entry.measurementPeriodId,
      source: entry.source,
      kind: entry.kind,
      qualityState: manifest?.qualityState || entry.qualityState || "VALID",
      completedAt:
        manifest?.completedAt ||
        entry.completedAt ||
        (entry.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD ? `${PRIMARY_BASELINE_DATE}T00:00:00.000Z` : null),
      startedAt: manifest?.startedAt || null,
      geography: manifest?.geography || { key: "CALA", geographyScope: "Region", commercialRegion: "CALA" },
      language: manifest?.language || "en",
      plannedCalls: manifest?.plannedCalls ?? (entry.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD ? 1 : 0),
      successfulCalls:
        manifest?.successfulCalls ?? (entry.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD ? 1 : 0),
      comparabilityNotes: manifest?.comparabilityNotes || [],
      peerSetId: peerSetFromNotes(manifest?.comparabilityNotes) ||
        (entry.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD ? PEER_SET_ID_V2 : null),
      datasetNamespace: manifest?.datasetNamespace || null,
    };
    period.measurementDate = normalizeMeasurementDate(period.completedAt) ||
      (period.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD ? PRIMARY_BASELINE_DATE : null);
    period.qualification = qualifyPeriodForChgVsPrior(period);
    period.measurementContractKey = measurementContractKey(period);
    out.push(period);
  }
  return out.sort((a, b) => String(a.measurementDate || "").localeCompare(String(b.measurementDate || "")));
}

export function comparisonUnitKey({
  subjectBrandId,
  scenarioId,
  providerScope,
  geography = "CALA",
  language = "en",
} = {}) {
  return [
    subjectBrandId || "",
    scenarioId || "",
    providerScope || "",
    String(geography || "CALA").toUpperCase(),
    normalizeLanguage(language) || "en",
  ].join("|");
}

export function inferCurrentPeriodId(observations = [], fallback = BASELINE_MEASUREMENT_PERIOD) {
  const ids = new Set();
  for (const o of observations || []) {
    const id = o.measurementPeriodId || o.periodId || null;
    if (id) ids.add(String(id));
    const batch = String(o.batchId || "");
    if (batch.startsWith("aiv_brand_longitudinal_period_")) ids.add(batch);
  }
  if (ids.size === 1) return [...ids][0];
  if (ids.size > 1) {
    const governed = listGovernedMeasurementPeriods();
    const ranked = governed
      .filter((p) => ids.has(p.measurementPeriodId) && p.qualification.valid)
      .sort((a, b) => String(b.measurementDate || "").localeCompare(String(a.measurementDate || "")));
    return ranked[0]?.measurementPeriodId || fallback;
  }
  return fallback;
}

function sameGeo(periodGeo, want) {
  const wantKey = String(want || "CALA").toUpperCase();
  const key = String(
    periodGeo?.key || periodGeo?.commercialRegion || periodGeo || "CALA"
  ).toUpperCase();
  return key === wantKey || key.includes(wantKey) || wantKey.includes(key);
}

/**
 * CURRENT = latest valid completed period matching context among candidates,
 * or the explicit currentPeriodId (live read path).
 * PRIOR = most recent earlier valid comparable period. Never a later period.
 */
export function selectCurrentAndPriorPeriods(opts = {}) {
  const geography = opts.geography || "CALA";
  const periods = (opts.periods || listGovernedMeasurementPeriods(opts)).filter((p) => {
    if (!p.qualification?.valid) return false;
    if (!sameGeo(p.geography, geography)) return false;
    return true;
  });

  const byId = new Map(periods.map((p) => [p.measurementPeriodId, p]));
  let current =
    (opts.currentPeriodId && byId.get(opts.currentPeriodId)) ||
    periods[periods.length - 1] ||
    null;

  if (opts.anchorToLiveCurrent !== false && opts.currentPeriodId && byId.get(opts.currentPeriodId)) {
    current = byId.get(opts.currentPeriodId);
  } else if (opts.anchorToLiveCurrent && opts.currentPeriodId) {
    current = byId.get(opts.currentPeriodId) || current;
  }

  if (!current) {
    return { currentPeriod: null, priorPeriod: null, periods };
  }

  const earlier = periods.filter((p) => {
    if (p.measurementPeriodId === current.measurementPeriodId) return false;
    const cd = current.measurementDate || "";
    const pd = p.measurementDate || "";
    if (!cd || !pd) return false;
    if (pd === cd) return false;
    return pd < cd;
  });
  const prior = earlier.length ? earlier[earlier.length - 1] : null;
  return { currentPeriod: current, priorPeriod: prior, periods };
}

export function formatCustomerDate(isoOrDate) {
  const d = normalizeMeasurementDate(isoOrDate);
  if (!d) return null;
  const [y, m, day] = d.split("-").map((n) => Number(n));
  const dt = new Date(Date.UTC(y, m - 1, day));
  return dt.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  });
}

export function formatIndexChangeDisplay(indexChangePoints) {
  if (indexChangePoints == null || !Number.isFinite(indexChangePoints)) return null;
  if (indexChangePoints === 0) return "No change";
  const rounded = Math.round(indexChangePoints);
  return `${rounded > 0 ? "+" : ""}${rounded} pts`;
}

export function formatPresenceChangePts(presenceChangePoints) {
  if (presenceChangePoints == null || !Number.isFinite(presenceChangePoints)) return null;
  if (presenceChangePoints === 0) return "No change";
  const rounded = Math.round(presenceChangePoints);
  return `${rounded > 0 ? "+" : ""}${rounded} pts`;
}

function pctLabel(rate) {
  if (rate == null || !Number.isFinite(rate)) return null;
  return `${Math.round(rate * 100)}%`;
}

function daysBetween(a, b) {
  const da = normalizeMeasurementDate(a);
  const db = normalizeMeasurementDate(b);
  if (!da || !db) return null;
  const ms = Math.abs(new Date(`${db}T00:00:00Z`) - new Date(`${da}T00:00:00Z`));
  return Math.round(ms / 86400000);
}

export function isScopeCertifiedForPeriod(subjectBrandId, scenarioId, providerScope, periodId, opts = {}) {
  const rec = lookupScopeCertification(subjectBrandId, scenarioId, providerScope, opts);
  if (!rec) return { certified: false, index: null, record: null };
  const status = String(rec.certificationStatus || "");
  const certified =
    (status === "PRODUCTION_VALIDATED" || status.startsWith("PRODUCTION_VALIDATED")) &&
    typeof rec.certifiedIndex === "number";
  const recPeriod = rec.measurementPeriod || BASELINE_MEASUREMENT_PERIOD;
  const periodMatch = !periodId || recPeriod === periodId;
  return {
    certified: certified && periodMatch,
    index: certified && periodMatch ? rec.certifiedIndex : null,
    record: rec,
    certificationPeriod: recPeriod,
  };
}

/**
 * Pure comparison for one exact scope. Used by production attach + unit tests.
 * Cross-scope / cross-scenario / cross-geography / cross-language callers must fail closed.
 */
export function buildOwnerIntentChgVsPrior(input = {}) {
  const unitCurrent = comparisonUnitKey(input.currentUnit || input);
  const unitPrior = comparisonUnitKey(input.priorUnit || input.currentUnit || input);
  if (unitCurrent !== unitPrior) {
    return {
      ...EMPTY_CUSTOMER_HISTORY,
      comparisonStatus: COMPARISON_STATUS.INSUFFICIENT_COMPARABLE_HISTORY,
      _internal: { CROSS_SCOPE_COLLISION: true, unitCurrent, unitPrior },
    };
  }

  const currentPeriod = input.currentPeriod || null;
  const priorPeriod = input.priorPeriod || null;
  const currentDate =
    input.currentPeriodDate ||
    currentPeriod?.measurementDate ||
    normalizeMeasurementDate(currentPeriod?.completedAt);
  const priorDate =
    input.priorPeriodDate ||
    priorPeriod?.measurementDate ||
    normalizeMeasurementDate(priorPeriod?.completedAt);

  const currentPresence = typeof input.currentPresence === "number" ? input.currentPresence : null;
  const priorPresence = typeof input.priorPresence === "number" ? input.priorPresence : null;
  const currentCertified = input.currentCertified === true;
  const priorCertified = input.priorCertified === true;
  const currentIndex = currentCertified && typeof input.currentIndex === "number" ? input.currentIndex : null;
  const priorIndex = priorCertified && typeof input.priorIndex === "number" ? input.priorIndex : null;
  const contractsMatch = input.measurementContractCompatible !== false;

  if (!priorPeriod && !priorDate) {
    return {
      ...EMPTY_CUSTOMER_HISTORY,
      currentPeriodDate: currentDate || null,
      currentIndex,
      currentPresence,
      comparisonStatus: COMPARISON_STATUS.NO_PRIOR_PERIOD,
    };
  }

  if (currentDate && priorDate && currentDate === priorDate) {
    return {
      ...EMPTY_CUSTOMER_HISTORY,
      currentPeriodDate: currentDate,
      currentIndex,
      currentPresence,
      comparisonStatus: COMPARISON_STATUS.INSUFFICIENT_COMPARABLE_HISTORY,
      _internal: { SAME_PERIOD_DUPLICATE: true },
    };
  }

  const presenceHistoryAvailable =
    currentPresence != null && priorPresence != null && Number.isFinite(currentPresence) && Number.isFinite(priorPresence);
  const presenceChangePoints = presenceHistoryAvailable
    ? Math.round((currentPresence - priorPresence) * 100)
    : null;

  let comparisonStatus = COMPARISON_STATUS.INSUFFICIENT_COMPARABLE_HISTORY;
  let indexChangePoints = null;

  if (!contractsMatch) {
    comparisonStatus = COMPARISON_STATUS.MEASUREMENT_CONTRACT_CHANGED;
  } else if (currentCertified && !priorCertified) {
    comparisonStatus = COMPARISON_STATUS.PRIOR_NOT_CERTIFIED;
  } else if (currentCertified && priorCertified && currentIndex != null && priorIndex != null) {
    comparisonStatus = COMPARISON_STATUS.COMPARABLE;
    indexChangePoints = Math.round(currentIndex - priorIndex);
  } else if (presenceHistoryAvailable) {
    comparisonStatus = COMPARISON_STATUS.INSUFFICIENT_COMPARABLE_HISTORY;
  } else {
    comparisonStatus = COMPARISON_STATUS.INSUFFICIENT_COMPARABLE_HISTORY;
  }

  const shortInterval = daysBetween(currentDate, priorDate) != null && daysBetween(currentDate, priorDate) <= 14;

  return {
    historyAvailable: true,
    currentPeriodDate: currentDate || null,
    priorPeriodDate: priorDate || null,
    currentIndex,
    priorIndex,
    indexChangePoints,
    currentPresence,
    priorPresence,
    presenceChangePoints,
    comparisonStatus,
    chgVsPriorDisplay: formatIndexChangeDisplay(indexChangePoints),
    presenceHistoryAvailable,
    shortInterval,
    shortIntervalNote: shortInterval ? SHORT_INTERVAL_NOTE : null,
  };
}

export function toCustomerSafeChgVsPrior(history = EMPTY_CUSTOMER_HISTORY) {
  return {
    historyAvailable: history.historyAvailable === true,
    currentPeriodDate: history.currentPeriodDate || null,
    priorPeriodDate: history.priorPeriodDate || null,
    currentIndex: history.currentIndex ?? null,
    priorIndex: history.priorIndex ?? null,
    indexChangePoints: history.indexChangePoints ?? null,
    currentPresence: history.currentPresence ?? null,
    priorPresence: history.priorPresence ?? null,
    presenceChangePoints: history.presenceChangePoints ?? null,
    comparisonStatus: history.comparisonStatus || COMPARISON_STATUS.NO_PRIOR_PERIOD,
    chgVsPriorDisplay: history.chgVsPriorDisplay || null,
    presenceHistoryAvailable: history.presenceHistoryAvailable === true,
    shortInterval: history.shortInterval === true,
    shortIntervalNote: history.shortIntervalNote || null,
  };
}

export function filterObservationsForComparisonUnit(observations = [], opts = {}) {
  const { providerScope, language = "en", geography = "CALA" } = opts;
  const providerId = providerIdFromBenchmarkScope(providerScope);
  const wantLang = normalizeLanguage(language) || "en";
  const wantGeo = String(geography || "CALA").toUpperCase();
  return (observations || []).filter((o) => {
    if (!o || o.success === false) return false;
    const lang = normalizeLanguage(o.language) || "en";
    if (lang !== wantLang) return false;
    const geo = String(o.geography || o.geographyKey || o.commercialRegion || "CALA").toUpperCase();
    if (geo && wantGeo && geo !== wantGeo && !geo.includes(wantGeo) && !wantGeo.includes(geo)) return false;
    if (providerScope && providerScope !== BENCHMARK_SCOPES.ALL_PROVIDERS) {
      if (!providerId || !providersMatch(o.provider, providerId)) return false;
    }
    return true;
  });
}

export function loadLongitudinalPeriodObservations(periodId, storeRoot = BRAND_LONGITUDINAL_STORE_ROOT) {
  if (!periodId || periodId === BASELINE_MEASUREMENT_PERIOD) return [];
  const responsesDir = path.join(storeRoot, periodId, "responses");
  const mentionsDir = path.join(storeRoot, periodId, "mentions");
  if (!fs.existsSync(responsesDir)) return [];
  const out = [];
  for (const file of fs.readdirSync(responsesDir)) {
    if (!file.endsWith(".json")) continue;
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(path.join(responsesDir, file), "utf8"));
    } catch {
      continue;
    }
    if (!raw || raw.status === "failed") continue;
    let mentions = [];
    const mentionPath = path.join(mentionsDir, `${raw.responseId || file.replace(/\.json$/, "")}.json`);
    if (fs.existsSync(mentionPath)) {
      try {
        const packed = JSON.parse(fs.readFileSync(mentionPath, "utf8"));
        mentions = packed.mentions || (Array.isArray(packed) ? packed : []);
      } catch {
        mentions = [];
      }
    }
    const presentEntityIds = [
      ...new Set((mentions || []).map((m) => m.canonicalEntityId || m.resolvedEntityId || m.entityId).filter(Boolean)),
    ];
    out.push({
      observationId: raw.responseId || file.replace(/\.json$/, ""),
      promptId: raw.promptId || null,
      promptVersion: raw.promptVersion || "1",
      provider: String(raw.provider || "").toLowerCase(),
      language: raw.language || "en",
      geography: raw.geographyKey || raw.geography || "CALA",
      success: true,
      presentEntityIds,
      mentions,
      measurementPeriodId: periodId,
      measurementDate: normalizeMeasurementDate(raw.createdAt || raw.timestamp),
      batchId: raw.batchId || periodId,
    });
  }
  return out;
}

function presenceForScenario(brandId, scenarioId, observations, scopeOpts) {
  const scoped = filterObservationsForComparisonUnit(observations, scopeOpts);
  const comparable = [];
  for (const obs of scoped) {
    const resolved = resolveObservationScenario(obs, {});
    if (resolved.scenarioId === scenarioId) comparable.push(obs);
  }
  if (!comparable.length) {
    return { scenarioId, subjectPresence: null, measurable: false, comparableObservationCount: 0 };
  }
  const rate = computeAiPresenceRate(comparable, brandId);
  const subjectPresence =
    typeof rate.value === "number" ? Math.round(rate.value * 10000) / 10000 : null;
  return {
    scenarioId,
    subjectPresence,
    measurable: typeof rate.denominator === "number" && rate.denominator > 0,
    comparableObservationCount: rate.denominator ?? comparable.length,
  };
}

/**
 * Attach customer-safe Chg vs Prior fields onto unified Owner Intent rows.
 * Does not mutate indexValue / presence from the live current payload.
 */
export function attachChgVsPriorToCoverageRows(rows = [], opts = {}) {
  const brandId = opts.brandId;
  const providerScope =
    opts.providerScope ||
    benchmarkScopeFromProvider(opts.provider, opts.allProvidersMode !== false);
  const geography = opts.geography || "CALA";
  const language = opts.language || "en";
  const currentObservations = opts.currentObservations || [];
  const currentPeriodId = opts.currentPeriodId || inferCurrentPeriodId(currentObservations);
  const { currentPeriod, priorPeriod } = selectCurrentAndPriorPeriods({
    currentPeriodId,
    geography,
    language,
    anchorToLiveCurrent: true,
    periods: opts.periods,
  });

  let priorObservations = opts.priorObservations || null;
  if (!priorObservations && priorPeriod && priorPeriod.measurementPeriodId !== BASELINE_MEASUREMENT_PERIOD) {
    priorObservations = loadLongitudinalPeriodObservations(priorPeriod.measurementPeriodId, opts.storeRoot);
  }

  const contractsMatch =
    currentPeriod && priorPeriod
      ? currentPeriod.measurementContractKey === priorPeriod.measurementContractKey
      : true;

  return (rows || []).map((row) => {
    const scenarioId = row.scenarioId;
    const currentCert = isScopeCertifiedForPeriod(brandId, scenarioId, providerScope, currentPeriod?.measurementPeriodId);
    const priorCert = priorPeriod
      ? isScopeCertifiedForPeriod(brandId, scenarioId, providerScope, priorPeriod.measurementPeriodId)
      : { certified: false, index: null };

    const currentPresence =
      typeof row.subjectPresence === "number"
        ? row.subjectPresence
        : presenceForScenario(brandId, scenarioId, currentObservations, {
            providerScope,
            language,
            geography,
          }).subjectPresence;

    let priorPresence = null;
    if (priorPeriod && priorObservations) {
      const priorAgg = presenceForScenario(brandId, scenarioId, priorObservations, {
        providerScope,
        language,
        geography,
      });
      priorPresence = priorAgg.measurable ? priorAgg.subjectPresence : null;
    }

    const history = buildOwnerIntentChgVsPrior({
      currentUnit: { subjectBrandId: brandId, scenarioId, providerScope, geography, language },
      priorUnit: { subjectBrandId: brandId, scenarioId, providerScope, geography, language },
      currentPeriod,
      priorPeriod,
      currentPresence,
      priorPresence,
      currentCertified: currentCert.certified,
      priorCertified: priorCert.certified,
      currentIndex: currentCert.index,
      priorIndex: priorCert.index,
      measurementContractCompatible: contractsMatch,
    });

    const customer = toCustomerSafeChgVsPrior(history);
    return {
      ...row,
      ...customer,
      chgVsPrior: customer,
    };
  });
}

export function auditPeriodIntegrity(opts = {}) {
  const periods = listGovernedMeasurementPeriods(opts);
  const valid = periods.filter((p) => p.qualification.valid);
  const dates = valid.map((p) => p.measurementDate).filter(Boolean);
  const dateCounts = dates.reduce((acc, d) => {
    acc[d] = (acc[d] || 0) + 1;
    return acc;
  }, {});
  const duplicateDates = Object.entries(dateCounts).filter(([, n]) => n > 1);
  const ids = valid.map((p) => p.measurementPeriodId);
  const idCounts = ids.reduce((acc, id) => {
    acc[id] = (acc[id] || 0) + 1;
    return acc;
  }, {});
  const duplicateIds = Object.entries(idCounts).filter(([, n]) => n > 1);

  const pairs = [];
  const incompatible = [];
  for (let i = 1; i < valid.length; i += 1) {
    const current = valid[i];
    const prior = valid[i - 1];
    const pair = {
      currentPeriodId: current.measurementPeriodId,
      priorPeriodId: prior.measurementPeriodId,
      currentDate: current.measurementDate,
      priorDate: prior.measurementDate,
      contractsMatch: current.measurementContractKey === prior.measurementContractKey,
    };
    if (!pair.contractsMatch) incompatible.push(pair);
    else pairs.push(pair);
  }

  return {
    version: OWNER_INTENT_CHG_VS_PRIOR_VERSION,
    PERIODS_FOUND: periods.length,
    VALID_PERIODS: valid.length,
    COMPARABLE_PERIOD_PAIRS: pairs.length,
    INCOMPATIBLE_PERIOD_PAIRS: incompatible.length,
    DUPLICATE_PERIODS: duplicateIds.length,
    DUPLICATE_PERIOD_COMPARISONS: duplicateDates.length,
    CROSS_SCOPE_COLLISIONS: 0,
    CROSS_PERIOD_DEDUPLICATION,
    POOLED_ALL_PERIODS_INDEX,
    PROVIDER_CALLS: CHG_VS_PRIOR_PROVIDER_CALLS,
    incompatiblePairs: incompatible,
    comparablePairs: pairs,
    periods,
  };
}

export function forbiddenCustomerTrendWords() {
  return FORBIDDEN_CUSTOMER_TREND_WORDS;
}

export function emptyChgVsPriorCustomer() {
  return { ...EMPTY_CUSTOMER_HISTORY };
}

/**
 * Corpus audit across customer-visible brands × coverage scenarios × provider scopes.
 * Does not change live certified values. Counts stored comparable history.
 */
export function auditOwnerIntentChgVsPriorUniverse(opts = {}) {
  const integrity = auditPeriodIntegrity(opts);
  const brandIds = opts.brandIds || listShowcaseMonitoringBrandIds();
  const scopes = opts.scopes || [BENCHMARK_SCOPES.ALL_PROVIDERS, ...PROVIDER_SCOPE_IDS];
  const geography = opts.geography || "CALA";
  const language = opts.language || "en";
  const currentObservations = opts.currentObservations || [];
  const currentPeriodId = opts.currentPeriodId || inferCurrentPeriodId(currentObservations);
  const { currentPeriod, priorPeriod } = selectCurrentAndPriorPeriods({
    currentPeriodId,
    geography,
    language,
    anchorToLiveCurrent: true,
    periods: integrity.periods,
  });

  let storedLatestVsPrior = { currentPeriod: null, priorPeriod: null };
  {
    const allValid = integrity.periods.filter((p) => p.qualification.valid);
    storedLatestVsPrior = {
      currentPeriod: allValid[allValid.length - 1] || null,
      priorPeriod: allValid.length >= 2 ? allValid[allValid.length - 2] : null,
    };
  }

  let priorObservations = opts.priorObservations || null;
  if (!priorObservations && priorPeriod?.measurementPeriodId && priorPeriod.measurementPeriodId !== BASELINE_MEASUREMENT_PERIOD) {
    priorObservations = loadLongitudinalPeriodObservations(priorPeriod.measurementPeriodId, opts.storeRoot);
  }

  let storedPriorObs = opts.storedPriorObservations || null;
  let storedCurrentObs = opts.storedCurrentObservations || null;
  if (storedLatestVsPrior.priorPeriod?.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD) {
    storedPriorObs = storedPriorObs || currentObservations;
  } else if (storedLatestVsPrior.priorPeriod) {
    storedPriorObs =
      storedPriorObs || loadLongitudinalPeriodObservations(storedLatestVsPrior.priorPeriod.measurementPeriodId, opts.storeRoot);
  }
  if (storedLatestVsPrior.currentPeriod?.measurementPeriodId === BASELINE_MEASUREMENT_PERIOD) {
    storedCurrentObs = storedCurrentObs || currentObservations;
  } else if (storedLatestVsPrior.currentPeriod) {
    storedCurrentObs =
      storedCurrentObs ||
      loadLongitudinalPeriodObservations(storedLatestVsPrior.currentPeriod.measurementPeriodId, opts.storeRoot);
  }

  const brandsWithTwoPlus = new Set();
  if (storedLatestVsPrior.currentPeriod && storedLatestVsPrior.priorPeriod) {
    for (const id of brandIds) brandsWithTwoPlus.add(id);
  }

  let comparablePresenceRows = 0;
  let comparableCertifiedIndexRows = 0;
  let rowsEligibleForChangeNow = 0;
  let rowsWithValidChangeButNotRenderable = 0;
  let rowsWithoutValidPriorShowingNumericChange = 0;
  const autograph = {};

  for (const brandId of brandIds) {
    for (const scenarioId of COVERAGE_OWNER_INTENT_DISPLAY_ORDER) {
      for (const providerScope of scopes) {
        const storedCurrentAgg = storedCurrentObs
          ? presenceForScenario(brandId, scenarioId, storedCurrentObs, { providerScope, language, geography })
          : { subjectPresence: null, measurable: false };
        const storedPriorAgg = storedPriorObs
          ? presenceForScenario(brandId, scenarioId, storedPriorObs, { providerScope, language, geography })
          : { subjectPresence: null, measurable: false };
        if (storedCurrentAgg.measurable && storedPriorAgg.measurable) {
          comparablePresenceRows += 1;
        }
        const storedCurrentCert = isScopeCertifiedForPeriod(
          brandId,
          scenarioId,
          providerScope,
          storedLatestVsPrior.currentPeriod?.measurementPeriodId
        );
        const storedPriorCert = isScopeCertifiedForPeriod(
          brandId,
          scenarioId,
          providerScope,
          storedLatestVsPrior.priorPeriod?.measurementPeriodId
        );
        if (storedCurrentCert.certified && storedPriorCert.certified) {
          comparableCertifiedIndexRows += 1;
        }

        const liveCurrentCert = isScopeCertifiedForPeriod(
          brandId,
          scenarioId,
          providerScope,
          currentPeriod?.measurementPeriodId
        );
        const livePriorCert = priorPeriod
          ? isScopeCertifiedForPeriod(brandId, scenarioId, providerScope, priorPeriod.measurementPeriodId)
          : { certified: false, index: null };
        const liveHistory = buildOwnerIntentChgVsPrior({
          currentUnit: { subjectBrandId: brandId, scenarioId, providerScope, geography, language },
          priorUnit: { subjectBrandId: brandId, scenarioId, providerScope, geography, language },
          currentPeriod,
          priorPeriod,
          currentPresence: presenceForScenario(brandId, scenarioId, currentObservations, {
            providerScope,
            language,
            geography,
          }).subjectPresence,
          priorPresence:
            priorPeriod && priorObservations
              ? presenceForScenario(brandId, scenarioId, priorObservations, {
                  providerScope,
                  language,
                  geography,
                }).subjectPresence
              : null,
          currentCertified: liveCurrentCert.certified,
          priorCertified: livePriorCert.certified,
          currentIndex: liveCurrentCert.index,
          priorIndex: livePriorCert.index,
          measurementContractCompatible:
            !currentPeriod ||
            !priorPeriod ||
            currentPeriod.measurementContractKey === priorPeriod.measurementContractKey,
        });

        const numericRenderable = liveHistory.chgVsPriorDisplay != null;
        const validIndexChange = liveHistory.comparisonStatus === COMPARISON_STATUS.COMPARABLE && liveHistory.indexChangePoints != null;
        if (validIndexChange) rowsEligibleForChangeNow += 1;
        if (validIndexChange && !numericRenderable) rowsWithValidChangeButNotRenderable += 1;
        if (!priorPeriod && numericRenderable) rowsWithoutValidPriorShowingNumericChange += 1;
      }
    }
  }

  return {
    integrity,
    TOTAL_MEASUREMENT_PERIODS: integrity.PERIODS_FOUND,
    BRANDS_WITH_2_PLUS_VALID_PERIODS: brandsWithTwoPlus.size,
    COMPARABLE_PRESENCE_ROWS: comparablePresenceRows,
    COMPARABLE_CERTIFIED_INDEX_ROWS: comparableCertifiedIndexRows,
    ROWS_ELIGIBLE_FOR_CHANGE_NOW: rowsEligibleForChangeNow,
    ROWS_WITH_VALID_CHANGE_BUT_NOT_RENDERABLE: rowsWithValidChangeButNotRenderable,
    ROWS_WITHOUT_VALID_PRIOR_SHOWING_NUMERIC_CHANGE: rowsWithoutValidPriorShowingNumericChange,
    liveCurrentPeriodId: currentPeriod?.measurementPeriodId || null,
    livePriorPeriodId: priorPeriod?.measurementPeriodId || null,
    storedCurrentPeriodId: storedLatestVsPrior.currentPeriod?.measurementPeriodId || null,
    storedPriorPeriodId: storedLatestVsPrior.priorPeriod?.measurementPeriodId || null,
    frozenBaseline: verifyAllProvidersFrozenBaseline(opts),
    autograph,
    CUSTOMER_TREND_LABELS_ENABLED,
    PROVIDER_CALLS: CHG_VS_PRIOR_PROVIDER_CALLS,
  };
}

export function smokeAutographChgVsPrior(opts = {}) {
  const brandId = opts.brandId;
  const scenarioId = opts.scenarioId;
  const currentObservations = opts.currentObservations || [];
  const geography = opts.geography || "CALA";
  const language = opts.language || "en";
  const scopes = [BENCHMARK_SCOPES.ALL_PROVIDERS, ...PROVIDER_SCOPE_IDS];
  const currentPeriodId = opts.currentPeriodId || inferCurrentPeriodId(currentObservations);
  const { currentPeriod, priorPeriod } = selectCurrentAndPriorPeriods({
    currentPeriodId,
    geography,
    language,
    anchorToLiveCurrent: true,
  });
  const out = {};
  for (const providerScope of scopes) {
    const cert = isScopeCertifiedForPeriod(brandId, scenarioId, providerScope, currentPeriod?.measurementPeriodId);
    const priorCert = priorPeriod
      ? isScopeCertifiedForPeriod(brandId, scenarioId, providerScope, priorPeriod.measurementPeriodId)
      : { certified: false, index: null };
    const currentAgg = presenceForScenario(brandId, scenarioId, currentObservations, {
      providerScope,
      language,
      geography,
    });
    const history = buildOwnerIntentChgVsPrior({
      currentUnit: { subjectBrandId: brandId, scenarioId, providerScope, geography, language },
      priorUnit: { subjectBrandId: brandId, scenarioId, providerScope, geography, language },
      currentPeriod,
      priorPeriod,
      currentPresence: currentAgg.subjectPresence,
      priorPresence: null,
      currentCertified: cert.certified,
      priorCertified: priorCert.certified,
      currentIndex: cert.index,
      priorIndex: priorCert.index,
      measurementContractCompatible: true,
    });
    out[providerScope] = {
      CURRENT_PERIOD: currentPeriod?.measurementPeriodId || null,
      PRIOR_PERIOD: priorPeriod?.measurementPeriodId || null,
      CURRENT_PRESENCE: currentAgg.subjectPresence,
      PRIOR_PRESENCE: history.priorPresence,
      CURRENT_INDEX: cert.certified ? cert.index : null,
      PRIOR_INDEX: history.priorIndex,
      CHG_VS_PRIOR: history.chgVsPriorDisplay,
      DISPLAY_STATUS: history.comparisonStatus,
    };
  }
  return out;
}
