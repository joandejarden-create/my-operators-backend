/**
 * Universal Need-Date tile icon eligibility — cross-hotel discovery.
 *
 * Any hotel with commercially open, non-test hotel need periods is automatically
 * eligible for icons via attachHotelNeedDateTilePreviews (API already hotel-scoped).
 * This module audits eligibility; it does not invent periods or mutate inventory.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listRegisteredHotels } from "../repository.js";
import {
  filterCustomerFacingNeedPeriodsForTileIcons,
  attachHotelNeedDateTilePreviewsSync,
  NEED_DATE_TILE_INDICATOR,
} from "./opportunity-need-date-tile-preview-v1.js";
import { NEED_PERIOD_STATUS } from "./need-date-demand-contract-v1.js";
import { listNeedPeriodsForHotel } from "./need-period-hotel-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const KNOWN_RESEARCH_HOTELS = Object.freeze([
  {
    hotelId: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    researchCompleted: true,
    researchNotes: "V17 Need-Date live research + commercial need periods 2026–2027",
  },
  {
    hotelId: "recgMYovrrZDJMqzX",
    name: "Waterstone Resort & Marina",
    researchCompleted: true,
    researchNotes:
      "Need-Date V1–V8 / deep discovery against founder TEST_SCENARIO periods — not hotel-reported operational needs",
  },
  {
    hotelId: "recterKmNPdntCm0v",
    name: "Grand Fiesta Americana Chapultepec",
    researchCompleted: false,
    researchNotes: "Fixture need-period only (synthetic) — not operational",
  },
  {
    hotelId: "recIEcSWuhSCoNLFN",
    name: "Fiesta Americana Condesa Cancún All Inclusive",
    researchCompleted: false,
    researchNotes: "Fixture need-period only (synthetic) — not operational",
  },
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function classifyPeriodProvenance(p) {
  const status = text(p.status);
  const source = text(p.source).toUpperCase();
  const isTest =
    p.isTestScenario === true ||
    status === NEED_PERIOD_STATUS.TEST_SCENARIO ||
    /TEST_SCENARIO/i.test(status) ||
    /FOUNDER_TEST|SYNTHETIC|FIXTURE|FOUNDER_LIFECYCLE/i.test(source);
  const withdrawn = /WITHDRAWN|ARCHIVED|INACTIVE|SUPERSEDED/i.test(status);
  const hotelReported =
    !isTest &&
    !withdrawn &&
    (/HOTEL_REPORTED|HOTEL_SALES|HOTEL_PORTAL|AIRTABLE/i.test(source) ||
      text(p.preferenceOverrides?.scenarioClassification).toUpperCase() ===
        "ACTUAL_HOTEL_INPUT");
  return {
    isTest,
    withdrawn,
    hotelReported,
    operationalCandidate: hotelReported && !isTest && !withdrawn,
  };
}

/**
 * Discover hotels with need periods from Airtable (when live) + local stores +
 * known research history. Read-only.
 */
export async function auditNeedDateTileIconEligibility(opts = {}) {
  const env = opts.env || process.env;
  const registered = listRegisteredHotels();
  const nameById = new Map();
  for (const h of registered) {
    const n = text(h.name || h.hotelName || h.displayName);
    if (h.hotelId) nameById.set(h.hotelId, n || h.hotelId);
  }
  for (const k of KNOWN_RESEARCH_HOTELS) {
    const existing = text(nameById.get(k.hotelId));
    if (!existing || existing === k.hotelId) nameById.set(k.hotelId, k.name);
  }

  let airtableByHotel = {};
  let airtableMeta = { available: false };
  try {
    const {
      isNeedDateAirtableLiveEnabled,
      listAllNeedPeriodsFromAirtable,
    } = await import("./need-date-airtable-store-v3.js");
    if (
      opts.useAirtable !== false &&
      (opts.useAirtable === true || isNeedDateAirtableLiveEnabled(env))
    ) {
      const listed = await listAllNeedPeriodsFromAirtable({ env });
      airtableByHotel = listed.byHotelId || {};
      airtableMeta = {
        available: true,
        baseId: listed.baseId,
        totalPeriods: listed.meta?.count || 0,
        hotelCount: listed.meta?.hotelCount || 0,
      };
    }
  } catch (err) {
    airtableMeta = {
      available: false,
      error: String(err?.message || err),
    };
  }

  const hotelIds = new Set([
    ...Object.keys(airtableByHotel),
    ...KNOWN_RESEARCH_HOTELS.map((h) => h.hotelId),
    ...registered.map((h) => h.hotelId),
  ]);

  // Local store scan for hotels/{id}/need-periods.json
  const hotelsRoot = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
  );
  if (fs.existsSync(hotelsRoot)) {
    for (const name of fs.readdirSync(hotelsRoot)) {
      const p = path.join(hotelsRoot, name, "need-periods.json");
      if (fs.existsSync(p)) hotelIds.add(name);
    }
  }

  const rows = [];
  for (const hotelId of [...hotelIds].sort()) {
    if (/^rec(TEST|DISC|PAR|PUB|WAVE)/i.test(hotelId) && hotelId.length < 20) {
      // skip obvious fixture stubs unless they have Airtable periods
      if (!airtableByHotel[hotelId]?.length) continue;
    }

    const known = KNOWN_RESEARCH_HOTELS.find((h) => h.hotelId === hotelId);
    const airtablePeriods = airtableByHotel[hotelId] || [];
    let localPeriods = [];
    try {
      localPeriods = listNeedPeriodsForHotel(hotelId)?.periods || [];
    } catch {
      localPeriods = [];
    }

    const allRaw = [...airtablePeriods, ...localPeriods];
    // Dedupe by needPeriodId preferring Airtable
    const byId = new Map();
    for (const p of allRaw) {
      if (!p?.needPeriodId) continue;
      if (!byId.has(p.needPeriodId)) byId.set(p.needPeriodId, p);
    }
    const periods = [...byId.values()];
    const provenance = periods.map((p) => ({
      needPeriodId: p.needPeriodId,
      periodLabel: p.periodLabel || null,
      status: p.status || null,
      source: p.source || null,
      isTestScenario: p.isTestScenario === true,
      ...classifyPeriodProvenance(p),
    }));

    const operationalRaw = periods.filter(
      (p) => classifyPeriodProvenance(p).operationalCandidate,
    );
    const operational = filterCustomerFacingNeedPeriodsForTileIcons(operationalRaw);
    const testOnly = provenance.filter((p) => p.isTest && !p.withdrawn);
    const researchCompleted =
      known?.researchCompleted === true ||
      periods.length > 0 ||
      operational.length > 0;

    let eligibility = "NOT_ELIGIBLE";
    if (operational.length > 0) {
      eligibility = "OPERATIONAL_ICONS_ELIGIBLE";
    } else if (testOnly.length > 0 || periods.some((p) => classifyPeriodProvenance(p).isTest)) {
      eligibility = "TEST_SCENARIO_ONLY";
    } else if (researchCompleted && periods.length === 0) {
      eligibility = "RESEARCH_HISTORY_NO_PERIODS";
    } else if (periods.length > 0) {
      eligibility = "PERIODS_PRESENT_NOT_OPERATIONAL";
    }

    rows.push({
      hotelId,
      hotelName: nameById.get(hotelId) || hotelId,
      researchCompleted: Boolean(researchCompleted),
      researchNotes: known?.researchNotes || null,
      eligibility,
      operationalIconEligible: eligibility === "OPERATIONAL_ICONS_ELIGIBLE",
      testScenarioOnly: eligibility === "TEST_SCENARIO_ONLY",
      needPeriodCounts: {
        totalRaw: periods.length,
        airtable: airtablePeriods.length,
        local: localPeriods.length,
        operationalCustomerFacing: operational.length,
        testScenario: testOnly.length,
        withdrawn: provenance.filter((p) => p.withdrawn).length,
      },
      periodSourceTypes: [...new Set(provenance.map((p) => p.source || "unknown"))],
      periods: provenance,
      operationalPeriods: operational.map((p) => ({
        needPeriodId: p.needPeriodId,
        periodLabel: p.periodLabel,
        startDate: p.startDate,
        endDate: p.endDate,
        source: p.source,
        status: p.status,
      })),
    });
  }

  return {
    version: "gdi-need-date-tile-icon-eligibility-v1",
    generatedAt: new Date().toISOString(),
    airtable: airtableMeta,
    hotelsAudited: rows.length,
    hotelsWithCompletedNeedDateResearch: rows.filter((r) => r.researchCompleted)
      .length,
    hotelsOperationalIconEligible: rows.filter((r) => r.operationalIconEligible)
      .length,
    hotelsTestScenarioOnly: rows.filter((r) => r.testScenarioOnly).length,
    rows,
  };
}

/**
 * Classify customer-facing opportunities for one hotel against its operational
 * need periods. Pure / sync when periods provided.
 */
export function classifyHotelNeedDateIcons(hotelId, opportunities, needPeriods, opts = {}) {
  const periods = filterCustomerFacingNeedPeriodsForTileIcons(needPeriods || []);
  const stamped = attachHotelNeedDateTilePreviewsSync(hotelId, opportunities || [], {
    needPeriods: periods,
    hotelName: opts.hotelName || null,
  });
  const counts = {
    evaluated: stamped.length,
    verified: 0,
    flexible: 0,
    timingResearch: 0,
    none: 0,
  };
  for (const o of stamped) {
    const s = o.tileNeedDatePreview?.indicatorState;
    if (s === NEED_DATE_TILE_INDICATOR.VERIFIED_DATE_MATCH) counts.verified += 1;
    else if (s === NEED_DATE_TILE_INDICATOR.POTENTIALLY_FLEXIBLE) counts.flexible += 1;
    else if (s === NEED_DATE_TILE_INDICATOR.TIMING_RESEARCH_REQUIRED)
      counts.timingResearch += 1;
    else counts.none += 1;
  }
  return { stamped, counts, operationalPeriodCount: periods.length };
}

export { KNOWN_RESEARCH_HOTELS };
