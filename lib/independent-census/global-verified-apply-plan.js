/**
 * Limited global Verified apply preview (report-only, no Airtable writes).
 */

import { readFileSync } from "fs";
import {
  buildBackwardsVerifiedFields,
  PROMOTION_ELIGIBILITY,
  PROMOTION_RECOMMENDATION,
} from "./backwards-census-match.js";
import { normalizeCountry, normalizeKey } from "./match-current-census.js";
import { VERIFIED_FIELDS } from "./fields.js";

export const APPLY_PLAN_COUNTRY_ORDER = [
  "mexico",
  "colombia",
  "argentina",
  "brazil",
  "panama",
  "chile",
  "costa rica",
  "ecuador",
];

/** Default 250-record first-batch country-balanced caps (sum = 250). */
export const DEFAULT_COUNTRY_BALANCED_ALLOCATIONS = {
  mexico: 50,
  colombia: 40,
  argentina: 40,
  brazil: 40,
  panama: 25,
  chile: 25,
  "costa rica": 20,
  ecuador: 10,
};

export const ALLOCATION_MODE = {
  PRIORITY_FILL: "priority-fill",
  COUNTRY_BALANCED: "country-balanced",
};

const DEFAULT_MAX_RECORDS = 250;

export function loadBackwardsMatchReportForApplyPlan(filePath) {
  const data = JSON.parse(readFileSync(filePath, "utf8"));
  const rows = Array.isArray(data.reportRows) ? data.reportRows : [];
  return { data, rows };
}

export function parseCountryPriorityList(priorityStr) {
  const raw = String(priorityStr || "").trim();
  if (!raw) return [...APPLY_PLAN_COUNTRY_ORDER];
  return raw
    .split(",")
    .map((s) => normalizeCountry(s.trim()))
    .filter(Boolean);
}

/**
 * Parse {"mexico":50,...} or mexico:50,colombia:40
 */
export function parseCountryAllocations(allocStr, countryOrder) {
  const raw = String(allocStr || "").trim();
  if (!raw) {
    return { ...DEFAULT_COUNTRY_BALANCED_ALLOCATIONS };
  }

  if (raw.startsWith("{")) {
    const parsed = JSON.parse(raw);
    const out = {};
    for (const [k, v] of Object.entries(parsed)) {
      out[normalizeCountry(k)] = Number(v) || 0;
    }
    return out;
  }

  const out = {};
  for (const part of raw.split(",")) {
    const [k, v] = part.split(":").map((s) => s.trim());
    if (k && v != null) out[normalizeCountry(k)] = parseInt(v, 10) || 0;
  }
  if (Object.keys(out).length) return out;

  return { ...DEFAULT_COUNTRY_BALANCED_ALLOCATIONS };
}

function countryPriority(countryRaw, countryOrder) {
  const norm = normalizeCountry(countryRaw);
  const idx = countryOrder.indexOf(norm);
  return idx === -1 ? countryOrder.length + 1 : idx;
}

function isApplyEligible(row) {
  if (row.matchConfidence !== "high") return false;
  if (row.promotionEligibility !== PROMOTION_ELIGIBILITY.ELIGIBLE) return false;
  if (
    row.promotionRecommendation !== PROMOTION_RECOMMENDATION.PROMOTE_AFTER_REVIEW
  ) {
    return false;
  }
  return true;
}

function summarizeSkipped(rows) {
  let skippedAlreadyVerified = 0;
  let skippedDuplicateHold = 0;
  let skippedOther = 0;

  for (const r of rows) {
    if (r.promotionRecommendation === PROMOTION_RECOMMENDATION.ALREADY_VERIFIED) {
      skippedAlreadyVerified++;
    } else if (
      r.promotionRecommendation === PROMOTION_RECOMMENDATION.HOLD_DUPLICATE
    ) {
      skippedDuplicateHold++;
    } else if (!isApplyEligible(r)) {
      skippedOther++;
    }
  }

  return { skippedAlreadyVerified, skippedDuplicateHold, skippedOther };
}

function groupEligibleByCountry(eligible, countryOrder) {
  const pools = new Map();
  for (const co of countryOrder) {
    pools.set(co, []);
  }
  pools.set("(other)", []);

  for (const row of eligible) {
    const co = normalizeCountry(row.osmCountry) || "(other)";
    if (!pools.has(co)) pools.set(co, []);
    pools.get(co).push(row);
  }

  for (const [, pool] of pools) {
    pool.sort((a, b) => (b.matchScore || 0) - (a.matchScore || 0));
  }
  return pools;
}

/**
 * Country-balanced: fill per-country caps; shortfalls cascade to later priority countries.
 */
export function selectCountryBalancedRows(eligible, opts) {
  const {
    countryOrder,
    allocations,
    maxPerCountry = null,
  } = opts;

  const pools = groupEligibleByCountry(eligible, countryOrder);
  const taken = {};
  const targetByCountry = {};
  const selected = [];

  for (const co of countryOrder) {
    taken[co] = 0;
    targetByCountry[co] = allocations[co] || 0;
  }

  let pendingRedistribution = 0;

  for (let i = 0; i < countryOrder.length; i++) {
    const co = countryOrder[i];
    let target = allocations[co] || 0;
    if (maxPerCountry != null) target = Math.min(target, maxPerCountry);

    const pool = pools.get(co) || [];
    const take = Math.min(target, pool.length);
    selected.push(...pool.slice(0, take));
    taken[co] = take;

    const shortfall = target - take;
    if (shortfall > 0) {
      pendingRedistribution += shortfall;
      for (let j = i + 1; j < countryOrder.length && pendingRedistribution > 0; j++) {
        const co2 = countryOrder[j];
        const pool2 = pools.get(co2) || [];
        const already = taken[co2] || 0;
        let room = pool2.length - already;
        if (maxPerCountry != null) {
          room = Math.min(room, Math.max(0, maxPerCountry - already));
        }
        const add = Math.min(pendingRedistribution, room);
        if (add > 0) {
          selected.push(...pool2.slice(already, already + add));
          taken[co2] = already + add;
          pendingRedistribution -= add;
        }
      }
    }
  }

  if (pendingRedistribution > 0) {
    for (const co of countryOrder) {
      if (pendingRedistribution <= 0) break;
      const pool = pools.get(co) || [];
      const already = taken[co] || 0;
      let room = pool.length - already;
      if (maxPerCountry != null) {
        room = Math.min(room, Math.max(0, maxPerCountry - already));
      }
      const add = Math.min(pendingRedistribution, room);
      if (add > 0) {
        selected.push(...pool.slice(already, already + add));
        taken[co] = already + add;
        pendingRedistribution -= add;
      }
    }
  }

  selected.sort((a, b) => {
    const cp =
      countryPriority(a.osmCountry, countryOrder) -
      countryPriority(b.osmCountry, countryOrder);
    if (cp !== 0) return cp;
    return (b.matchScore || 0) - (a.matchScore || 0);
  });

  return { selected, taken, targetByCountry, pendingRedistribution };
}

function selectPriorityFillRows(eligible, countryOrder, maxRecords) {
  const sorted = [...eligible].sort((a, b) => {
    const cp =
      countryPriority(a.osmCountry, countryOrder) -
      countryPriority(b.osmCountry, countryOrder);
    if (cp !== 0) return cp;
    return (b.matchScore || 0) - (a.matchScore || 0);
  });
  return sorted.slice(0, maxRecords);
}

function candidateFromMatchRow(row) {
  return {
    airtableRecordId: row.osmCandidateRecordId,
    sourceType: "osm",
    sourceRecordId: row.osmSourceRecordId || "",
    rawHotelName: row.osmName,
    rawCity: row.osmCity || "",
    rawCountry: row.osmCountry,
    rawLatitude: row.osmLatitude,
    rawLongitude: row.osmLongitude,
    rawWebsite: row.osmWebsite || "",
    rawPhone: row.rawPhone || "",
    rawBrand: row.rawBrand || "",
  };
}

function buildPromotionRiskNote(row) {
  const parts = [];
  if (row.inDuplicateCluster) {
    parts.push("Retention duplicate cluster flagged.");
  }
  if (row.promotionRecommendation === PROMOTION_RECOMMENDATION.HOLD_DUPLICATE) {
    parts.push("Legacy duplicate cluster — excluded from apply plan.");
  }
  if (row.notes) parts.push(row.notes);
  if (!parts.length) {
    parts.push("High-confidence legacy match with strict promotion signals.");
  }
  return parts.join(" ");
}

function proposedVerifiedFieldsSummary(mapped) {
  const f = mapped.fields;
  return {
    verifiedHotelName: f[VERIFIED_FIELDS.verifiedHotelName] || "",
    verifiedCity: f[VERIFIED_FIELDS.verifiedCity] || "",
    verifiedCountry: f[VERIFIED_FIELDS.verifiedCountry] || "",
    verifiedLatitude: f[VERIFIED_FIELDS.verifiedLatitude] ?? "",
    verifiedLongitude: f[VERIFIED_FIELDS.verifiedLongitude] ?? "",
    verifiedWebsite: f[VERIFIED_FIELDS.verifiedWebsite] || "",
    verifiedPhone: f[VERIFIED_FIELDS.verifiedPhone] || "",
    verifiedBrandLabel: f[VERIFIED_FIELDS.verifiedBrandLabel] || "",
    verifiedDedupeKey: mapped.verifiedDedupeKey,
    censusReconciliationStatus:
      f[VERIFIED_FIELDS.censusReconciliationStatus] || "",
    primarySourceCandidate: mapped.candidateAirtableRecordId,
  };
}

function buildPlanRows(selected, batchId, approvedByPlaceholder) {
  return selected.map((row, index) => {
    const candidate = candidateFromMatchRow(row);
    const mapped = buildBackwardsVerifiedFields(candidate, row, {
      approvedBy: approvedByPlaceholder,
      batchId,
      approvedAt: new Date().toISOString(),
    });

    return {
      applyRank: index + 1,
      candidateRecordId: row.osmCandidateRecordId,
      osmName: row.osmName,
      osmCity: row.osmCity,
      osmCountry: row.osmCountry,
      osmLatitude: row.osmLatitude,
      osmLongitude: row.osmLongitude,
      matchedLegacyRecordId: row.matchedLegacyRecordId,
      matchedLegacyName: row.matchedLegacyName,
      distanceMeters: row.distanceMeters,
      matchScore: row.matchScore,
      matchReason: row.matchReason,
      proposedVerifiedFields: proposedVerifiedFieldsSummary(mapped),
      promotionRiskNote: buildPromotionRiskNote(row),
      verifiedDedupeKey: row.verifiedDedupeKey,
      retentionRecommendation: row.retentionRecommendation,
      countryNormalized: normalizeCountry(row.osmCountry) || "",
    };
  });
}

/**
 * @param {object} opts
 */
export function buildGlobalVerifiedApplyPlan(opts) {
  const maxRecords = opts.maxRecords ?? DEFAULT_MAX_RECORDS;
  const allocationMode =
    opts.allocationMode || ALLOCATION_MODE.PRIORITY_FILL;
  const countryOrder = parseCountryPriorityList(opts.countryPriority);
  const allocations = parseCountryAllocations(
    opts.countryAllocations,
    countryOrder
  );
  const maxPerCountry =
    opts.maxPerCountry != null ? Number(opts.maxPerCountry) : null;

  const { rows } = loadBackwardsMatchReportForApplyPlan(opts.backwardsMatchReportPath);
  const batchId = opts.applyPlanBatchId || "global-verified-apply-plan-001-2026-05-20";
  const approvedByPlaceholder = opts.approvedByPlaceholder || "(pending human approval)";

  const skippedSummary = summarizeSkipped(rows);
  const eligible = rows.filter(isApplyEligible);

  const eligibleByCountry = {};
  for (const r of eligible) {
    const co = normalizeCountry(r.osmCountry) || "(unknown)";
    eligibleByCountry[co] = (eligibleByCountry[co] || 0) + 1;
  }

  let selected = [];
  let allocationDetail = null;

  if (allocationMode === ALLOCATION_MODE.COUNTRY_BALANCED) {
    const allocSum = countryOrder.reduce(
      (s, co) => s + (allocations[co] || 0),
      0
    );
    const effectiveMax = Math.min(maxRecords, allocSum);

    const balanced = selectCountryBalancedRows(eligible, {
      countryOrder,
      allocations,
      maxPerCountry,
    });
    selected = balanced.selected.slice(0, effectiveMax);
    allocationDetail = {
      mode: ALLOCATION_MODE.COUNTRY_BALANCED,
      targetByCountry: balanced.targetByCountry,
      selectedByCountry: balanced.taken,
      pendingRedistributionAfterCascade: balanced.pendingRedistribution,
      allocationTargets: allocations,
      allocationTargetSum: allocSum,
      effectiveMaxRecords: effectiveMax,
    };
  } else {
    selected = selectPriorityFillRows(eligible, countryOrder, maxRecords);
    allocationDetail = {
      mode: ALLOCATION_MODE.PRIORITY_FILL,
      effectiveMaxRecords: maxRecords,
    };
  }

  const planRows = buildPlanRows(selected, batchId, approvedByPlaceholder);

  const byCountry = {};
  for (const r of planRows) {
    const co = r.countryNormalized || "(unknown)";
    byCountry[co] = (byCountry[co] || 0) + 1;
  }

  return {
    batchId,
    maxRecords,
    allocationMode,
    allocationDetail,
    sourceBackwardsMatchReport: opts.backwardsMatchReportPath,
    totalReportRows: rows.length,
    eligibleBeforeCap: eligible.length,
    eligibleByCountry,
    applyPlanCount: planRows.length,
    byCountry,
    countryOrder,
    skippedAlreadyVerified: skippedSummary.skippedAlreadyVerified,
    skippedDuplicateHold: skippedSummary.skippedDuplicateHold,
    skippedOtherIneligible: skippedSummary.skippedOther,
    planRows,
    dryRun: true,
    airtableWrites: false,
    verifiedTableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
  };
}

export function applyPlanRowToCsv(row) {
  const p = row.proposedVerifiedFields || {};
  return {
    applyRank: row.applyRank,
    candidateRecordId: row.candidateRecordId,
    osmName: row.osmName,
    osmCity: row.osmCity,
    osmCountry: row.osmCountry,
    osmLatitude: row.osmLatitude,
    osmLongitude: row.osmLongitude,
    matchedLegacyRecordId: row.matchedLegacyRecordId,
    matchedLegacyName: row.matchedLegacyName,
    distanceMeters: row.distanceMeters ?? "",
    matchScore: row.matchScore,
    matchReason: row.matchReason,
    verifiedHotelName: p.verifiedHotelName,
    verifiedCity: p.verifiedCity,
    verifiedCountry: p.verifiedCountry,
    verifiedLatitude: p.verifiedLatitude,
    verifiedLongitude: p.verifiedLongitude,
    verifiedDedupeKey: p.verifiedDedupeKey,
    promotionRiskNote: row.promotionRiskNote,
  };
}

export const APPLY_PLAN_CSV_COLUMNS = [
  "applyRank",
  "candidateRecordId",
  "osmName",
  "osmCity",
  "osmCountry",
  "osmLatitude",
  "osmLongitude",
  "matchedLegacyRecordId",
  "matchedLegacyName",
  "distanceMeters",
  "matchScore",
  "matchReason",
  "verifiedHotelName",
  "verifiedCity",
  "verifiedCountry",
  "verifiedLatitude",
  "verifiedLongitude",
  "verifiedDedupeKey",
  "promotionRiskNote",
];

export function resolveApplyPlanOutputSlug(batchId, allocationMode) {
  const id = String(batchId || "001-2026-05-20").trim();
  if (allocationMode === ALLOCATION_MODE.COUNTRY_BALANCED) {
    return id.startsWith("balanced-")
      ? `independent-census-global-verified-apply-plan-${id}`
      : `independent-census-global-verified-apply-plan-balanced-${id}`;
  }
  return id.startsWith("global-verified-apply-plan-")
    ? `independent-census-${id}`
    : `independent-census-global-verified-apply-plan-${id}`;
}
