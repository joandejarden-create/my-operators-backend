/**
 * GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1
 *
 * Hotel-level need-date configuration status for every eligible GDI hotel.
 * Segregates HOTEL_CONFIRMED periods from MARKET_ESTIMATED tentative periods.
 * Never promotes an estimate to a verified hotel need-date match.
 * No Airtable writes. No paid research. No customer publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listGdiSelectableHotels } from "../repository.js";
import {
  NEED_DATE_DEMAND_INTELLIGENCE_V2,
  NEED_PERIOD_STATUS,
  NEED_REVENUE_TYPE,
} from "./need-date-demand-contract-v1.js";
import { listNeedPeriodsForHotel } from "./need-period-hotel-store-v1.js";
import { isNeedPeriodCommerciallyOpen } from "./need-period-lifecycle-v1.js";
import {
  filterCustomerFacingNeedPeriodsForTileIcons,
  NEED_DATE_TILE_INDICATOR,
} from "./opportunity-need-date-tile-preview-v1.js";

export const GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1 =
  "GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1";

/** Hotel-level configuration status (independent of opportunity matches). */
export const HOTEL_NEED_DATE_CONFIG_STATUS = Object.freeze({
  HOTEL_CONFIRMED: "HOTEL_CONFIRMED",
  MARKET_ESTIMATED: "MARKET_ESTIMATED",
  NOT_ESTABLISHED: "NOT_ESTABLISHED",
});

export const HOTEL_NEED_DATE_CONFIG_STATUS_LABEL = Object.freeze({
  [HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED]:
    "Hotel-confirmed need periods",
  [HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED]:
    "Estimated — not hotel confirmed",
  [HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED]: "Need dates not established",
});

export const NEED_PERIOD_PROVENANCE = Object.freeze({
  HOTEL_CONFIRMED: "HOTEL_CONFIRMED",
  MARKET_ESTIMATED: "MARKET_ESTIMATED",
});

export const MARKET_ESTIMATE_LIFECYCLE = Object.freeze({
  ACTIVE: "ACTIVE",
  CONFIRMED_PROMOTED: "CONFIRMED_PROMOTED",
  REJECTED: "REJECTED",
  SUPERSEDED: "SUPERSEDED",
  EDITED: "EDITED",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function nowIso() {
  return new Date().toISOString();
}

function marketEstimateStorePath(hotelId) {
  return path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    text(hotelId),
    "market-estimated-need-periods.json"
  );
}

function isHotelConfirmedProvenance(period = {}) {
  const provenance = upper(
    period.provenance || period.needPeriodProvenance || ""
  );
  if (provenance === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED) return false;
  if (provenance === NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED) return true;

  const source = upper(period.source);
  const scenario = upper(
    period.preferenceOverrides?.scenarioClassification ||
      period.scenarioClassification ||
      ""
  );
  const status = text(period.status);
  if (
    period.isTestScenario === true ||
    status === NEED_PERIOD_STATUS.TEST_SCENARIO ||
    /SYNTHETIC|FIXTURE|FOUNDER_TEST|RESEARCH_BENCHMARK/i.test(source)
  ) {
    return false;
  }
  return (
    !source ||
    /HOTEL_REPORTED|HOTEL_SALES|HOTEL_PORTAL|AIRTABLE|HOTEL_CONFIRMED/i.test(
      source
    ) ||
    scenario === "ACTUAL_HOTEL_INPUT" ||
    scenario === "HOTEL_REPORTED" ||
    period.hotelConfirmed === true
  );
}

function isCommerciallyOpenConfirmed(period = {}) {
  if (!period?.needPeriodId && !period?.id) return false;
  if (!isNeedPeriodCommerciallyOpen(period) && period.status) {
    // isNeedPeriodCommerciallyOpen expects status — still allow Active without helper quirks
  }
  try {
    if (!isNeedPeriodCommerciallyOpen(period)) return false;
  } catch {
    if (!/Active|Researching|Review|Partially Filled/i.test(text(period.status))) {
      return false;
    }
  }
  return isHotelConfirmedProvenance(period);
}

/**
 * Load market-estimated periods for a hotel (filesystem only; segregated store).
 */
export function loadMarketEstimatedNeedPeriods(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return { hotelId: null, periods: [], path: null };
  const p = opts.path || marketEstimateStorePath(id);
  if (!fs.existsSync(p)) {
    return {
      hotelId: id,
      periods: [],
      path: p,
      version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
    };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const periods = (Array.isArray(doc.periods) ? doc.periods : []).map((row) => ({
    ...row,
    provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
    hotelConfirmationStatus: row.hotelConfirmationStatus || "PENDING",
  }));
  return {
    hotelId: id,
    periods,
    path: p,
    version: doc.version || GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
    updatedAt: doc.updatedAt || null,
  };
}

/**
 * Persist market estimates locally. Never writes hotel-confirmed need-periods.json.
 */
export function saveMarketEstimatedNeedPeriods(hotelId, periods, opts = {}) {
  const id = text(hotelId);
  if (!id) throw new Error("hotel_id_required");
  const p = opts.path || marketEstimateStorePath(id);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const doc = {
    version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
    hotelId: id,
    provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
    note: "Tentative market estimates — not hotel-confirmed occupancy gaps. Promotion requires authorized confirmation.",
    updatedAt: nowIso(),
    periods: (periods || []).map((row) => ({
      ...row,
      hotelId: id,
      provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
      hotelConfirmationStatus: row.hotelConfirmationStatus || "PENDING",
    })),
  };
  fs.writeFileSync(p, `${JSON.stringify(doc, null, 2)}\n`, "utf8");
  return doc;
}

/**
 * Confirm / edit / reject lifecycle for a market estimate (local only).
 * Confirm marks estimate CONFIRMED_PROMOTED but does NOT auto-write hotel calendar.
 */
export function applyMarketEstimateLifecycleAction({
  hotelId,
  estimateId,
  action,
  patch = {},
  actor = "authorized_user",
} = {}) {
  const store = loadMarketEstimatedNeedPeriods(hotelId);
  const idx = store.periods.findIndex(
    (p) => text(p.estimateId || p.needPeriodId || p.id) === text(estimateId)
  );
  if (idx < 0) {
    return { ok: false, reason: "estimate_not_found", hotelId, estimateId };
  }
  const current = { ...store.periods[idx] };
  const act = upper(action);
  const history = Array.isArray(current.lifecycleHistory)
    ? current.lifecycleHistory.slice(-20)
    : [];
  history.push({ at: nowIso(), action: act, actor, patchKeys: Object.keys(patch || {}) });

  if (act === "CONFIRM" || act === "PROMOTE") {
    current.hotelConfirmationStatus = "CONFIRMED_PENDING_CALENDAR_WRITE";
    current.lifecycleStatus = MARKET_ESTIMATE_LIFECYCLE.CONFIRMED_PROMOTED;
    current.confirmedAt = nowIso();
    current.confirmedBy = actor;
    current.note =
      "Authorized confirmation recorded. Hotel-confirmed calendar write requires separate authorized workflow — not auto-applied.";
  } else if (act === "REJECT") {
    current.hotelConfirmationStatus = "REJECTED";
    current.lifecycleStatus = MARKET_ESTIMATE_LIFECYCLE.REJECTED;
    current.rejectedAt = nowIso();
    current.rejectedBy = actor;
    current.rejectionReason = text(patch.reason) || "Rejected by authorized user";
  } else if (act === "EDIT") {
    Object.assign(current, patch, {
      hotelConfirmationStatus: "PENDING",
      lifecycleStatus: MARKET_ESTIMATE_LIFECYCLE.EDITED,
      lastEditedAt: nowIso(),
      lastEditedBy: actor,
      provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
    });
  } else {
    return { ok: false, reason: "unknown_action", action };
  }
  current.lifecycleHistory = history;
  current.dateLastReviewed = nowIso().slice(0, 10);
  store.periods[idx] = current;
  saveMarketEstimatedNeedPeriods(hotelId, store.periods);
  return {
    ok: true,
    action: act,
    estimate: current,
    wroteHotelConfirmedCalendar: false,
  };
}

/**
 * Build a market estimate only when evidence is sufficient.
 * Returns null when evidence is weak — retain NOT_ESTABLISHED.
 */
export function buildEvidenceBasedMarketEstimate(hotel = {}, evidence = {}) {
  const hotelId = text(hotel.hotelId || hotel.id);
  const sources = Array.isArray(evidence.sources) ? evidence.sources.filter(Boolean) : [];
  const evidenceText = text(evidence.evidenceSummary || evidence.summary);
  const start = text(evidence.startDate || evidence.monthStart).slice(0, 10);
  const end = text(evidence.endDate || evidence.monthEnd || start).slice(0, 10);
  const revenueNeedType =
    text(evidence.revenueNeedType) || NEED_REVENUE_TYPE.GROUP_GUESTROOMS;
  const confidence = Number(evidence.confidence);
  const confOk = Number.isFinite(confidence) && confidence >= 0.45;

  if (!hotelId || !start || sources.length < 1 || evidenceText.length < 40 || !confOk) {
    return null;
  }
  // Refuse fabricated occupancy-gap claims without support language.
  if (
    /occupancy gap|exact weak dates|guaranteed soft period/i.test(evidenceText) &&
    !/supported|evidenced|seasonality|event calendar|weekday|weekend|historically/i.test(
      evidenceText
    )
  ) {
    return null;
  }

  return {
    estimateId: text(evidence.estimateId) || `mkt_est_${hotelId}_${start.replace(/-/g, "")}`,
    needPeriodId: text(evidence.estimateId) || `mkt_est_${hotelId}_${start.replace(/-/g, "")}`,
    hotelId,
    startDate: start,
    endDate: end,
    periodLabel: text(evidence.periodLabel) || `${start} – ${end}`,
    granularity: text(evidence.granularity) || (start === end ? "Month" : "Date Range"),
    revenueNeedType,
    provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
    evidenceSummary: evidenceText.slice(0, 600),
    sources: sources.slice(0, 8),
    confidence: Math.min(1, Math.max(0, confidence)),
    uncertainty: text(evidence.uncertainty) || "Market estimate — hotel has not confirmed.",
    dateLastReviewed: text(evidence.dateLastReviewed) || nowIso().slice(0, 10),
    hotelConfirmationStatus: "PENDING",
    lifecycleStatus: MARKET_ESTIMATE_LIFECYCLE.ACTIVE,
    status: "Active",
    customerVisible: false,
    publishable: false,
  };
}

/**
 * Derive hotel-level configuration status from confirmed + estimated periods.
 * Mixed periods: hotel status is HOTEL_CONFIRMED if any confirmed; estimates remain
 * labeled MARKET_ESTIMATED at period level and must not be relabeled confirmed.
 */
export function deriveHotelNeedDateConfigStatus({
  hotelId = null,
  hotelConfirmedPeriods = [],
  marketEstimatedPeriods = [],
} = {}) {
  const confirmed = (hotelConfirmedPeriods || []).filter(isCommerciallyOpenConfirmed);
  const estimates = (marketEstimatedPeriods || []).filter((p) => {
    if (upper(p.lifecycleStatus) === MARKET_ESTIMATE_LIFECYCLE.REJECTED) return false;
    if (upper(p.hotelConfirmationStatus) === "REJECTED") return false;
    return upper(p.provenance || NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED) ===
      NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED;
  });

  let status = HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED;
  if (confirmed.length > 0) status = HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED;
  else if (estimates.length > 0) status = HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED;

  return {
    version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
    hotelId: hotelId || null,
    status,
    statusLabel: HOTEL_NEED_DATE_CONFIG_STATUS_LABEL[status],
    accessibleLabel:
      status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
        ? "Hotel-confirmed need periods exist"
        : status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
          ? "Estimated need periods — not hotel confirmed"
          : "Need dates not established",
    icon:
      status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
        ? "calendar-check"
        : status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
          ? "calendar-dashed"
          : "calendar-minus",
    colorClass:
      status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
        ? "gdi-hotel-need-date-status--confirmed"
        : status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
          ? "gdi-hotel-need-date-status--estimated"
          : "gdi-hotel-need-date-status--not-established",
    counts: {
      hotelConfirmedPeriods: confirmed.length,
      marketEstimatedPeriods: estimates.length,
    },
    periods: [
      ...confirmed.map((p) => ({
        id: p.needPeriodId || p.id,
        provenance: NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED,
        periodLabel: p.periodLabel || p.label || null,
        startDate: p.startDate || null,
        endDate: p.endDate || null,
        revenueNeedType: p.revenueNeedType || p.primaryRevenueType || null,
      })),
      ...estimates.map((p) => ({
        id: p.estimateId || p.needPeriodId || p.id,
        provenance: NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED,
        periodLabel: p.periodLabel || null,
        startDate: p.startDate || null,
        endDate: p.endDate || null,
        revenueNeedType: p.revenueNeedType || null,
        confidence: p.confidence ?? null,
        hotelConfirmationStatus: p.hotelConfirmationStatus || "PENDING",
      })),
    ],
  };
}

/**
 * Match presentation provenance guard:
 * MARKET_ESTIMATED periods must never produce unqualified VERIFIED_DATE_MATCH.
 */
export function applyNeedDateMatchProvenanceGuard(preview = {}, periodProvenance = null) {
  const out = { ...(preview || {}) };
  const provenance = upper(
    periodProvenance ||
      preview.periodProvenance ||
      preview.needPeriodProvenance ||
      ""
  );
  out.periodProvenance = provenance || null;
  out.timingDoesNotImplyAccessibility = true;

  if (provenance === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED) {
    if (out.indicatorState === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH) {
      out.indicatorState = NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE;
      out.icon = "calendar-clock";
      out.colorClass = "gdi-need-date-icon--flexible";
      out.demotedFromVerifiedBecause = "MARKET_ESTIMATED_PERIOD";
    }
    out.estimatedLabel = "Estimated hotel need period — not hotel confirmed";
    out.tooltipText = out.tooltipText
      ? `${out.tooltipText} (estimated — not hotel confirmed)`
      : "Estimated need-date alignment — not hotel confirmed";
    out.accessibleLabel = out.accessibleLabel
      ? `${out.accessibleLabel}; estimated, not hotel confirmed`
      : "Estimated need-date alignment; not hotel confirmed";
    out.verifiedHotelNeedDateMatch = false;
  } else if (provenance === NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED) {
    out.verifiedHotelNeedDateMatch =
      out.indicatorState === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH;
  } else {
    out.verifiedHotelNeedDateMatch =
      out.indicatorState === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH;
  }
  return out;
}

/**
 * Customer/share-safe hotel status DTO — excludes unpublished estimates unless allowed.
 */
export function toCustomerSafeHotelNeedDateStatus(statusDoc = {}, opts = {}) {
  const allowEstimates = opts.includeUnpublishedEstimates === true;
  const status = statusDoc.status || HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED;
  const periods = (statusDoc.periods || []).filter((p) => {
    if (p.provenance === NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED) return true;
    return allowEstimates && p.provenance === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED;
  });

  // Share/auth default: if only unpublished estimates exist, show NOT_ESTABLISHED
  // unless explicitly allowed (internal authenticated management surfaces).
  let safeStatus = status;
  if (
    !allowEstimates &&
    status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
  ) {
    safeStatus = HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED;
  }

  return {
    version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
    hotelId: statusDoc.hotelId || null,
    status: safeStatus,
    statusLabel: HOTEL_NEED_DATE_CONFIG_STATUS_LABEL[safeStatus],
    accessibleLabel:
      safeStatus === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
        ? "Hotel-confirmed need periods exist"
        : safeStatus === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
          ? "Estimated need periods — not hotel confirmed"
          : "Need dates not established",
    icon:
      safeStatus === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
        ? "calendar-check"
        : safeStatus === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
          ? "calendar-dashed"
          : "calendar-minus",
    colorClass:
      safeStatus === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
        ? "gdi-hotel-need-date-status--confirmed"
        : safeStatus === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
          ? "gdi-hotel-need-date-status--estimated"
          : "gdi-hotel-need-date-status--not-established",
    confirmedPeriodCount: periods.filter(
      (p) => p.provenance === NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED
    ).length,
    estimatedPeriodCount: allowEstimates
      ? periods.filter((p) => p.provenance === NEED_PERIOD_PROVENANCE.MARKET_ESTIMATED)
          .length
      : 0,
    // Never expose internal research notes on share
    internalOnly: false,
  };
}

/**
 * Airtable-authoritative local mirror (read-only sync artifact).
 * Precedence: airtable mirror > local need-periods.json (when not a demo stub).
 */
export function loadAirtableNeedPeriodMirror(hotelId) {
  const id = text(hotelId);
  const filePath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    id,
    "need-periods.airtable-mirror.json"
  );
  if (!fs.existsSync(filePath)) return { path: filePath, periods: [], present: false };
  try {
    const doc = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const periods = Array.isArray(doc?.periods) ? doc.periods : [];
    return {
      path: filePath,
      present: true,
      recoveredAt: doc.recoveredAt || null,
      baseId: doc.baseId || null,
      table: doc.table || "GDI Hotel Need Periods",
      periods,
      excludedNonAuthoritative: Array.isArray(doc.excludedNonAuthoritative)
        ? doc.excludedNonAuthoritative
        : [],
      authority: "AIRTABLE_GDI_HOTEL_NEED_PERIODS",
    };
  } catch {
    return { path: filePath, periods: [], present: false };
  }
}

function isDemoLocalNeedPeriodStub(doc = {}) {
  const note = text(doc.note);
  return /demo periods for universal|UI verification|Not an Airtable write/i.test(
    note
  );
}

/**
 * Resolve full hotel need-date configuration for one hotel.
 * Precedence for hotel-confirmed:
 *  1. Airtable mirror (HOTEL_REPORTED Active; excludes TEST_SCENARIO)
 *  2. Local need-periods.json (non-demo)
 * Market estimates remain segregated and never auto-promoted.
 */
export function resolveHotelNeedDateConfiguration(hotelId, opts = {}) {
  const id = text(hotelId);
  let hotelConfirmedPeriods = [];
  const mirror = loadAirtableNeedPeriodMirror(id);
  if (mirror.present && mirror.periods.length) {
    hotelConfirmedPeriods = mirror.periods.map((p) => ({
      ...p,
      provenance: NEED_PERIOD_PROVENANCE.HOTEL_CONFIRMED,
      hotelConfirmed: true,
    }));
  } else {
    try {
      const listed = listNeedPeriodsForHotel(id, opts);
      const docPeriods = Array.isArray(listed)
        ? listed
        : Array.isArray(listed?.periods)
          ? listed.periods
          : [];
      // Ignore intentional local demo stubs when Airtable authority exists elsewhere.
      if (!isDemoLocalNeedPeriodStub(listed) || opts.allowDemoLocalStub === true) {
        hotelConfirmedPeriods = docPeriods;
      }
    } catch {
      hotelConfirmedPeriods = [];
    }
  }

  // Prefer customer-facing filter for confirmed display, but keep provenance check.
  const confirmedOpen = filterCustomerFacingNeedPeriodsForTileIcons(
    hotelConfirmedPeriods
  ).filter(isHotelConfirmedProvenance);

  const market = loadMarketEstimatedNeedPeriods(id, opts);
  return deriveHotelNeedDateConfigStatus({
    hotelId: id,
    hotelConfirmedPeriods: confirmedOpen.length
      ? confirmedOpen
      : hotelConfirmedPeriods.filter(isCommerciallyOpenConfirmed),
    marketEstimatedPeriods: market.periods,
  });
}

/**
 * Coverage audit — defaults to GDI-selectable research set for backward compatibility.
 * Prefer auditHotelLayerModelV2() for canonical inventory coverage.
 */
export function auditUniversalNeedDateHotelCoverage(opts = {}) {
  const hotels = opts.hotels || listGdiSelectableHotels();
  const rows = [];
  for (const h of hotels) {
    const hotelId = text(h.hotelId);
    if (!hotelId) continue;
    const cfg = resolveHotelNeedDateConfiguration(hotelId, opts);
    const name =
      text(h.displayName) ||
      text(h.hotelName) ||
      text(h.optionLabel) ||
      text(h.config?.displayName) ||
      text(h.profile?.identity?.hotelName) ||
      text(h.profile?.name) ||
      text(h.profile?.hotelName) ||
      hotelId;
    rows.push({
      hotelId,
      hotelName: name,
      status: cfg.status,
      statusLabel: cfg.statusLabel,
      hotelConfirmedPeriods: cfg.counts.hotelConfirmedPeriods,
      marketEstimatedPeriods: cfg.counts.marketEstimatedPeriods,
      missingNeedDates: cfg.status === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED,
      estimateUnsupportedReason:
        cfg.status === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
          ? "Insufficient evidence-backed market seasonality/event support to create a tentative period without fabricating occupancy gaps"
          : null,
    });
  }

  const counts = {
    totalHotels: rows.length,
    HOTEL_CONFIRMED: rows.filter(
      (r) => r.status === HOTEL_NEED_DATE_CONFIG_STATUS.HOTEL_CONFIRMED
    ).length,
    MARKET_ESTIMATED: rows.filter(
      (r) => r.status === HOTEL_NEED_DATE_CONFIG_STATUS.MARKET_ESTIMATED
    ).length,
    NOT_ESTABLISHED: rows.filter(
      (r) => r.status === HOTEL_NEED_DATE_CONFIG_STATUS.NOT_ESTABLISHED
    ).length,
  };

  return {
    version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
    generatedAt: nowIso(),
    counts,
    missingHotels: rows.filter((r) => r.missingNeedDates),
    hotels: rows,
    contractNotes: [
      "MARKET_ESTIMATED never auto-writes hotel-confirmed calendar",
      "Share surfaces hide unpublished estimates unless publication contract allows",
      "No need-date config must not suppress GDI opportunity inventory",
    ],
  };
}

export const NEED_DATE_INFORMATION_LEGEND_V1 = Object.freeze({
  version: GDI_UNIVERSAL_NEED_DATE_INTELLIGENCE_V1,
  title: "Need-Date Intelligence legend",
  sections: [
    {
      id: "revenue_streams",
      title: "Revenue streams",
      items: [
        {
          icon: "bed",
          label: "Bed = potential guestroom revenue. Gold = applicable; muted = not currently established.",
        },
        {
          icon: "utensils",
          label:
            "Utensils = potential meetings, events, or catering revenue. Gold = applicable; muted = not currently established.",
        },
      ],
    },
    {
      id: "opportunity_timing",
      title: "Opportunity timing",
      items: [
        {
          icon: "calendar-check",
          label: "Green calendar-check = verified match to a confirmed hotel need period.",
        },
        {
          icon: "calendar-clock",
          label: "Gold calendar-clock = potentially flexible timing.",
        },
        {
          icon: "calendar-search",
          label: "Purple calendar-search = timing requires further research.",
        },
        {
          icon: "none",
          label:
            "No opportunity calendar icon = no meaningful need-date match shown; it does not mean the hotel calendar is unconfigured.",
        },
      ],
    },
    {
      id: "hotel_configuration",
      title: "Hotel configuration",
      items: [
        {
          icon: "calendar-check",
          label: "Green calendar = hotel-confirmed need periods exist.",
        },
        {
          icon: "calendar-dashed",
          label:
            "Gold estimated calendar = market-based tentative periods; hotel confirmation pending.",
        },
        {
          icon: "calendar-minus",
          label: "Gray calendar-minus = need dates not established.",
        },
      ],
    },
  ],
});
