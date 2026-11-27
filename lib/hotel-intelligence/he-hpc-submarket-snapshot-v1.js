/**
 * Hotel Explorer Submarket Snapshot — canonical HPC cohort (read-only).
 *
 * HPC (appCCUsuGsE1ifoLk / tbl9aY5ijiuIzzWam) is the source of truth for hotel
 * identity, rooms, status, brand, and market/submarket classifications.
 * Scout / Hotel Census may enrich but must never exclude a valid HPC hotel.
 *
 * No writes. No invented geography.
 */

import Airtable from "airtable";
import {
  formatHotelPropertyCensusAsBrandPresenceHotel,
  HOTEL_PROPERTY_CENSUS_TABLE,
  HPC_SELECT_FIELDS,
} from "./radar-internal-preview-hotels-v1.js";
import {
  HPC_TABLE_ID,
  GRAPH_BASE_ID_CANONICAL,
} from "./research-engine-v1/airtable-graph-contract-v1.js";

export const HE_HPC_SUBMARKET_SNAPSHOT_V1 = "he-hpc-submarket-snapshot-v1";

const REC_ID_RE = /^rec[A-Za-z0-9]{14}$/;
const VISIBLE_STATUS = new Set(["Open", "Pipeline"]);

function escapeFormula(value) {
  return String(value || "")
    .replace(/\\/g, "\\\\")
    .replace(/'/g, "\\'");
}

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function resolveHpcBaseId() {
  return (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    GRAPH_BASE_ID_CANONICAL
  );
}

function createHpcBase() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = resolveHpcBaseId();
  if (!key || !baseId) return null;
  return new Airtable({ apiKey: key }).base(baseId);
}

/**
 * Format HPC record for Submarket Snapshot.
 * Unknown room counts stay null (not zero).
 */
export function formatHpcHotelForSubmarketSnapshot(record) {
  const dto = formatHotelPropertyCensusAsBrandPresenceHotel(record);
  const fields = record?.fields || {};
  const roomsRaw = fields["Rooms / Keys"] ?? fields.rooms ?? fields.Rooms;
  let rooms = null;
  if (roomsRaw != null && roomsRaw !== "") {
    const n = Number(roomsRaw);
    if (Number.isFinite(n) && n > 0) rooms = n;
  }
  return {
    ...dto,
    rooms,
    roomsKnown: rooms != null,
    recordId: record.id,
    censusSourceTable: HOTEL_PROPERTY_CENSUS_TABLE,
    hpcTableId: HPC_TABLE_ID,
    source: "hpc",
  };
}

export function dedupeHpcHotelsByIdentity(hotels = []) {
  const seen = new Set();
  const out = [];
  for (const h of hotels || []) {
    if (!h || typeof h !== "object") continue;
    const id = text(h.id || h.recordId);
    const key =
      id ||
      [
        String(h.name || "")
          .toLowerCase()
          .trim(),
        String(h.city || "")
          .toLowerCase()
          .trim(),
        String(h.brand || "")
          .toLowerCase()
          .trim(),
      ].join("|");
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(h);
  }
  return out;
}

export function classifySubmarketGrouping(selected) {
  const submarket = text(selected?.submarket);
  const market = text(selected?.market);
  const city = text(selected?.city);
  const country = text(selected?.country);

  if (submarket && !/^unknown$/i.test(submarket) && !/\bregional$/i.test(submarket)) {
    return {
      mode: "submarket",
      label: submarket,
      market,
      city,
      country,
      researchNeeded: false,
      note: "Verified HPC Submarket classification.",
    };
  }
  if (market && !/^unknown$/i.test(market)) {
    return {
      mode: "market",
      label: market,
      market,
      city,
      country,
      researchNeeded: Boolean(!submarket),
      note: submarket
        ? "Using HPC Market — Submarket present but not used for cohort match."
        : "HPC Market assigned; Submarket not verified — market-level cohort only.",
    };
  }
  if (city) {
    return {
      mode: "city",
      label: city,
      market: null,
      city,
      country,
      researchNeeded: true,
      note: "No verified HPC Market/Submarket — city fallback for partial coverage only.",
    };
  }
  return {
    mode: "selected_only",
    label: null,
    market: null,
    city: null,
    country,
    researchNeeded: true,
    note: "No verified geographic classification — showing selected HPC property only.",
  };
}

function buildCohortFormula(grouping) {
  const parts = [];
  // Prefer Open|Pipeline; selected hotel is always merged separately.
  parts.push(`OR({Radar Status}='Open',{Radar Status}='Pipeline')`);
  if (grouping.country) {
    parts.push(`{Country}='${escapeFormula(grouping.country)}'`);
  }
  if (grouping.mode === "submarket" && grouping.label) {
    parts.push(`{Submarket}='${escapeFormula(grouping.label)}'`);
  } else if (grouping.mode === "market" && grouping.label) {
    parts.push(
      `OR({Market}='${escapeFormula(grouping.label)}',{Market / Submarket}='${escapeFormula(
        grouping.label
      )}')`
    );
  } else if (grouping.mode === "city" && grouping.city) {
    parts.push(`{City}='${escapeFormula(grouping.city)}'`);
  } else {
    return null;
  }
  return `AND(${parts.join(",")})`;
}

export function computeHpcSubmarketMetrics(hotels = []) {
  const list = hotels || [];
  const open = list.filter((h) => String(h.status || h.radarStatus || "") === "Open");
  const pipeline = list.filter((h) => String(h.status || h.radarStatus || "") === "Pipeline");
  const sumKnownRooms = (arr) =>
    arr.reduce((sum, h) => {
      const n = Number(h.rooms);
      return Number.isFinite(n) && n > 0 ? sum + n : sum;
    }, 0);
  const countKnownRooms = (arr) =>
    arr.filter((h) => Number.isFinite(Number(h.rooms)) && Number(h.rooms) > 0).length;

  const openRooms = sumKnownRooms(open);
  const pipelineRooms = sumKnownRooms(pipeline);
  const roomsKnownHotelCount = countKnownRooms(list);
  const roomsUnknownHotelCount = list.length - roomsKnownHotelCount;
  const avgKeys =
    roomsKnownHotelCount > 0
      ? Math.round((openRooms + pipelineRooms) / roomsKnownHotelCount)
      : null;

  return {
    totalHotels: list.length,
    openHotels: open.length,
    openRooms,
    pipelineHotels: pipeline.length,
    pipelineRooms,
    totalRooms: openRooms + pipelineRooms,
    roomsKnownHotelCount,
    roomsUnknownHotelCount,
    avgKeys,
    // Incomplete coverage: do not treat known-room sum as full market rooms.
    roomsCoverageComplete: roomsUnknownHotelCount === 0 && list.length > 0,
  };
}

async function fetchRecordById(base, hotelId) {
  try {
    const rec = await base(HOTEL_PROPERTY_CENSUS_TABLE).find(hotelId);
    return { record: rec, error: null };
  } catch (err) {
    return {
      record: null,
      error: {
        reason: "hpc_selected_hotel_not_found",
        message: String(err?.message || err).slice(0, 240),
      },
    };
  }
}

async function fetchCohortRecords(base, formula, { maxRecords = 200 } = {}) {
  if (!formula) return { records: [], error: null };
  const records = [];
  try {
    await base(HOTEL_PROPERTY_CENSUS_TABLE)
      .select({
        filterByFormula: formula,
        pageSize: 100,
        fields: [...HPC_SELECT_FIELDS],
        maxRecords,
      })
      .eachPage((page, next) => {
        for (const rec of page || []) records.push(rec);
        next();
      });
    return { records, error: null };
  } catch (err) {
    return {
      records: [],
      error: {
        reason: "hpc_cohort_fetch_failed",
        message: String(err?.message || err).slice(0, 240),
      },
    };
  }
}

/**
 * Build Submarket Snapshot payload from canonical HPC.
 */
export async function buildHpcSubmarketSnapshot({
  hotelId,
  airtableBase = null,
  maxRecords = 200,
} = {}) {
  const id = String(hotelId || "").trim();
  if (!REC_ID_RE.test(id)) {
    return {
      ok: false,
      version: HE_HPC_SUBMARKET_SNAPSHOT_V1,
      error: "invalid_hotel_id",
      writeOnGet: false,
      applyAuthorized: false,
    };
  }

  const base = airtableBase || createHpcBase();
  if (!base) {
    return {
      ok: false,
      version: HE_HPC_SUBMARKET_SNAPSHOT_V1,
      error: "airtable_not_configured",
      writeOnGet: false,
      applyAuthorized: false,
      baseId: resolveHpcBaseId(),
      tableId: HPC_TABLE_ID,
      tableName: HOTEL_PROPERTY_CENSUS_TABLE,
    };
  }

  const selectedFetch = await fetchRecordById(base, id);
  if (!selectedFetch.record) {
    return {
      ok: false,
      version: HE_HPC_SUBMARKET_SNAPSHOT_V1,
      error: selectedFetch.error?.reason || "hpc_selected_hotel_not_found",
      details: selectedFetch.error,
      writeOnGet: false,
      applyAuthorized: false,
      baseId: resolveHpcBaseId(),
      tableId: HPC_TABLE_ID,
      tableName: HOTEL_PROPERTY_CENSUS_TABLE,
      hotels: [],
    };
  }

  const selected = formatHpcHotelForSubmarketSnapshot(selectedFetch.record);
  const grouping = classifySubmarketGrouping(selected);
  const formula = buildCohortFormula(grouping);
  const cohortFetch =
    grouping.mode === "selected_only"
      ? { records: [], error: null }
      : await fetchCohortRecords(base, formula, { maxRecords });

  const cohortHotels = (cohortFetch.records || []).map(formatHpcHotelForSubmarketSnapshot);
  // Always include selected HPC hotel — Scout emptiness must never hide it.
  const matchedHotels = dedupeHpcHotelsByIdentity([selected, ...cohortHotels]);
  const matchedMetrics = computeHpcSubmarketMetrics(matchedHotels);

  // Geographic inventory: market-level HPC cohort (distinct from exact submarket match).
  // Never treat a 1-hotel submarket match as the entire competitive market.
  const marketGrouping =
    text(selected.market) && !/^unknown$/i.test(selected.market)
      ? {
          mode: "market",
          label: selected.market,
          market: selected.market,
          city: selected.city,
          country: selected.country,
        }
      : null;
  const marketFormula = marketGrouping ? buildCohortFormula(marketGrouping) : null;
  const skipMarketFetch =
    !marketFormula ||
    (grouping.mode === "market" && grouping.label === marketGrouping.label);
  const marketFetch = skipMarketFetch
    ? { records: [], error: null }
    : await fetchCohortRecords(base, marketFormula, { maxRecords });
  const geographicHotels = dedupeHpcHotelsByIdentity([
    selected,
    ...(marketFetch.records || []).map(formatHpcHotelForSubmarketSnapshot),
  ]);
  const geographicMetrics = computeHpcSubmarketMetrics(geographicHotels);

  // Competitive set starts as geographic inventory; callers may enrich with Area Hotels.
  // Do not fabricate complete market totals or market-share percentages.
  const competitiveHotels = geographicHotels;
  const competitiveMetrics = geographicMetrics;

  const hotels = matchedHotels;
  const metrics = matchedMetrics;

  const selectedInCohort = hotels.some((h) => h.id === selected.id);
  const matchedThin = matchedHotels.length <= 1;
  const partialCoverage =
    grouping.researchNeeded ||
    grouping.mode === "selected_only" ||
    grouping.mode === "city" ||
    (grouping.mode === "submarket" && matchedThin) ||
    !matchedMetrics.roomsCoverageComplete;

  const researchNeeded = Boolean(
    grouping.researchNeeded ||
      matchedThin ||
      geographicHotels.length <= 1 ||
      !geographicMetrics.roomsCoverageComplete
  );

  const coverage = {
    complete: !partialCoverage && matchedHotels.length > 1,
    partial: partialCoverage,
    label:
      grouping.mode === "selected_only"
        ? "Selected HPC property only — market/submarket assignment unresolved"
        : matchedThin
          ? `Partial HPC coverage — ${matchedHotels.length} matched hotel(s) for ${
              grouping.label || "this geography"
            }. Not a complete submarket census.`
          : `HPC cohort for ${grouping.label} (${matchedHotels.length} hotels). Coverage may still be incomplete.`,
    researchNeeded,
    inventGeography: false,
    disclosed: true,
    marketShareFabricated: false,
    cohorts: {
      matchedHpcInventory: matchedHotels.length,
      geographicSubmarketInventory: geographicHotels.length,
      competitiveSet: competitiveHotels.length,
    },
  };

  const cohorts = {
    matchedHpcInventory: {
      id: "matched_hpc_inventory",
      label: "Verified matched HPC inventory",
      description:
        grouping.mode === "submarket"
          ? `Exact HPC Submarket match for ${grouping.label}`
          : `Primary HPC grouping (${grouping.mode}) for ${grouping.label || "selected hotel"}`,
      grouping,
      formula: formula || null,
      hotels: matchedHotels,
      metrics: matchedMetrics,
    },
    geographicSubmarketInventory: {
      id: "geographic_submarket_inventory",
      label: "Verified geographic market inventory",
      description: marketGrouping
        ? `HPC Market cohort for ${marketGrouping.label} (not a fabricated complete census)`
        : "No verified HPC Market — geographic inventory limited to selected hotel",
      grouping: marketGrouping || { mode: "selected_only", label: null },
      formula: marketFormula,
      hotels: geographicHotels,
      metrics: geographicMetrics,
    },
    competitiveSet: {
      id: "property_relevant_competitive_set",
      label: "Property-relevant competitive set",
      description:
        "Starts from verified geographic HPC inventory. Expand via Area Hotels / discovery research when authorized — never invent peers.",
      hotels: competitiveHotels,
      metrics: competitiveMetrics,
      researchNeeded: competitiveHotels.length <= 1,
    },
  };

  return {
    ok: true,
    version: HE_HPC_SUBMARKET_SNAPSHOT_V1,
    success: true,
    writeOnGet: false,
    applyAuthorized: false,
    persisted: false,
    source: "hpc",
    sourceOfTruth: "Hotel Property Census",
    scoutRole: "supporting_enrichment_only",
    legacyHotelCensus: "PROHIBITED",
    baseId: resolveHpcBaseId(),
    tableId: HPC_TABLE_ID,
    tableName: HOTEL_PROPERTY_CENSUS_TABLE,
    selectedHotel: selected,
    selectedHotelIncluded: selectedInCohort,
    grouping,
    formula: formula || null,
    hotels,
    metrics,
    coverage,
    cohorts,
    errors: [cohortFetch.error, marketFetch.error].filter(Boolean),
  };
}

export function mergeHpcWithSupportingPool(hpcPayload, supportingHotels = []) {
  const hpcHotels = hpcPayload?.hotels || [];
  const support = (supportingHotels || []).map((h) => ({
    ...h,
    source: h.source || "scout_or_radar",
  }));
  return dedupeHpcHotelsByIdentity([...hpcHotels, ...support]);
}
