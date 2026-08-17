/**
 * Census Autopilot V1.3 — Gap closure: Rooms + Address + Coordinates.
 * No Webhound. No Airtable writes. Autonomous multi-pass.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import "dotenv/config";

import { buildGoldenFieldMap } from "../golden/golden-enrichment.js";
import {
  scoreHotelGoldenCompleteness,
  aggregatePortfolioScores,
  buildFieldMissingness,
  hasSupportedValue,
  VALUE_STATUS,
} from "../golden/golden-completeness.js";
import { priorityFields } from "../golden/golden-schema.js";
import { resolveFamilyRooms } from "./rooms-family-resolvers.js";
import {
  resolvePropertyAddress,
  resolvePropertyCoordinates,
  resolveHiltonGraphQLAddressCoords,
  warmFamilyDirectoryCaches,
  resolveGeocodingProvider,
  estimateGeocodeCostUsd,
  ADDR_GEO_VERSION,
} from "./address-coordinate-resolvers.js";
import { sleep } from "../../adapters/adapter-utils.js";
import { writeGapClosureArtifacts } from "./artifact-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../../../..");
const V12_DIR = path.join(ROOT, "data/research-engine-v2/census-autopilot-v1-2-golden-95");
const VIC_PATH = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/01_combined_4family_index.json"
);
const ARTIFACT_DIR = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v1-3-gap-closure"
);

const GAP_FIELDS = new Set(["Rooms / Keys", "Address", "Latitude", "Longitude"]);

function loadV12Freeze() {
  const perHotel = JSON.parse(
    fs.readFileSync(path.join(V12_DIR, "21-per-hotel-completeness.json"), "utf8")
  );
  const missingness = JSON.parse(
    fs.readFileSync(path.join(V12_DIR, "08-field-missingness-matrix.json"), "utf8")
  );
  const finalPass = JSON.parse(
    fs.readFileSync(path.join(V12_DIR, "12-live-research-pass-final.json"), "utf8")
  );
  return {
    version: "census-autopilot-v1.2-freeze",
    hotels: perHotel.hotels,
    buckets: perHotel.buckets,
    portfolio: finalPass.portfolio,
    missingness: missingness.final,
    rooms_pct: missingness.final.find((m) => m.field === "Rooms / Keys")?.completion_pct,
    address_pct: missingness.final.find((m) => m.field === "Address")?.completion_pct,
    lat_pct: missingness.final.find((m) => m.field === "Latitude")?.completion_pct,
    lng_pct: missingness.final.find((m) => m.field === "Longitude")?.completion_pct,
  };
}

function loadRecords() {
  const vic = JSON.parse(fs.readFileSync(VIC_PATH, "utf8"));
  return (vic.records || []).filter(
    (r) => r.country === "Mexico" && ["IHG", "Hilton", "Choice"].includes(r.family)
  );
}

function cell(value, opts = {}) {
  return {
    value: hasSupportedValue(value) ? value : null,
    status: opts.status || (hasSupportedValue(value) ? VALUE_STATUS.SUPPORTED : VALUE_STATUS.UNKNOWN),
    source: opts.source || null,
    method: opts.method || null,
    confidence: opts.confidence || null,
    cvent_used: false,
    legacy_used: false,
    derived: Boolean(opts.derived),
  };
}

/**
 * Rebuild field map: preserve V1.2 non-gap supported fields; seed gap fields from V1.2 when present.
 */
function rebuildFieldMapFromV12(record, v12Hotel) {
  const { fieldMap, geo } = buildGoldenFieldMap(record, null);
  const unknown = new Set(v12Hotel?.unknown_fields || []);

  // Preserve V1.2 supported non-gap fields (freeze evidence status)
  for (const entry of priorityFields()) {
    const f = entry.field;
    if (GAP_FIELDS.has(f)) continue;
    if (unknown.has(f)) continue;
    if (!hasSupportedValue(fieldMap[f]?.value)) {
      fieldMap[f] = cell(`v12_supported:${f}`, {
        status: VALUE_STATUS.SUPPORTED,
        source: "v1.2_freeze_preserved",
        method: "freeze_non_gap_supported_status",
        confidence: "Medium",
      });
    }
  }

  // Seed rooms from V1.2 if present
  if (v12Hotel?.rooms != null) {
    fieldMap["Rooms / Keys"] = cell(v12Hotel.rooms, {
      source: "v1.2_freeze",
      method: "v12_rooms_seed",
      confidence: "Medium",
    });
  } else if (!unknown.has("Rooms / Keys") && hasSupportedValue(fieldMap["Rooms / Keys"]?.value)) {
    // keep enrichment
  } else {
    fieldMap["Rooms / Keys"] = cell(null, { status: VALUE_STATUS.UNKNOWN });
  }

  // Gap fields that V1.2 already had: mark for re-confirmation (will fill in passes)
  for (const f of ["Address", "Latitude", "Longitude"]) {
    if (!unknown.has(f)) {
      // Was supported in V1.2 — preserve until replaced with fresh official value
      if (!hasSupportedValue(fieldMap[f]?.value)) {
        fieldMap[f] = cell(`v12_supported:${f}`, {
          source: "v1.2_freeze_preserved",
          method: "freeze_gap_field_was_supported",
          confidence: "Medium",
        });
      }
    } else {
      fieldMap[f] = cell(null, { status: VALUE_STATUS.UNKNOWN });
    }
  }

  return { fieldMap, geo };
}

function scoreAll(records, fieldMaps, contexts) {
  const hotelScores = [];
  const perHotel = [];
  records.forEach((record, i) => {
    const score = scoreHotelGoldenCompleteness(fieldMaps[i], contexts[i]);
    hotelScores.push(score);
    perHotel.push({
      independent_record_id: record.independent_record_id,
      name: record.name,
      family: record.family,
      brand: record.brand,
      city: record.city,
      ...score,
      rooms: fieldMaps[i]["Rooms / Keys"]?.value ?? null,
      address: fieldMaps[i].Address?.value ?? null,
      latitude: fieldMaps[i].Latitude?.value ?? null,
      longitude: fieldMaps[i].Longitude?.value ?? null,
      unknown_fields: score.unknown_fields,
    });
  });
  return {
    portfolio: aggregatePortfolioScores(hotelScores),
    missingness: buildFieldMissingness(fieldMaps, contexts),
    perHotel,
  };
}

function fieldPct(missingness, field) {
  return missingness.find((m) => m.field === field)?.completion_pct ?? null;
}

async function mapPool(items, concurrency, fn) {
  const out = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, concurrency) }, () => worker()));
  return out;
}

/**
 * @param {object} opts
 */
export async function runGapClosureV13(opts = {}) {
  const started = Date.now();
  const log = opts.log || console.log;
  fs.mkdirSync(ARTIFACT_DIR, { recursive: true });
  const runId =
    opts.runId ||
    `cav13_${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${randomUUID().slice(0, 6)}`;
  const runDir = path.join(ARTIFACT_DIR, "runs", runId);
  fs.mkdirSync(runDir, { recursive: true });

  const freeze = loadV12Freeze();
  let records = loadRecords();
  if (opts.maxRecords) records = records.slice(0, opts.maxRecords);

  const v12ById = new Map(freeze.hotels.map((h) => [h.independent_record_id, h]));
  log(`[v1.3] Gap closure — ${records.length} hotels; freeze avg=${freeze.portfolio.average_raw_priority_completeness_pct}%`);

  const providerInfo = resolveGeocodingProvider();
  const providerStatus =
    providerInfo.provider === "mapbox" && providerInfo.permanent_storage_enabled && providerInfo.credentials_ok
      ? "PROVIDER_READY"
      : providerInfo.provider === "mapbox" && providerInfo.credentials_ok && !providerInfo.permanent_storage_enabled
        ? "PROVIDER_CONFIGURED_BUT_TERMS_REVIEW_REQUIRED"
        : providerInfo.provider === "none"
          ? "NO_PROVIDER"
          : "PROVIDER_BLOCKED";

  // Rebuild maps from freeze
  const fieldMaps = [];
  const contexts = [];
  for (const record of records) {
    const v12 = v12ById.get(record.independent_record_id);
    const { fieldMap, geo } = rebuildFieldMapFromV12(record, v12);
    fieldMaps.push(fieldMap);
    contexts.push({
      market: geo.Market,
      Market: geo.Market,
      family: record.family,
      brand: record.brand,
      property_id: record.property_ids?.[0],
    });
  }

  const baseline = scoreAll(records, fieldMaps, contexts);
  log(`[v1.3] rebuilt baseline avg=${baseline.portfolio.average_raw_priority_completeness_pct}%`);

  const delayMs = opts.delayMs ?? 200;
  const concurrency = opts.concurrency ?? 3;
  const timeoutMs = opts.timeoutMs ?? 25000;

  const roomsResults = { IHG: [], Hilton: [], Choice: [] };
  const addressResults = [];
  const coordResults = [];
  const learning = { hilton_graphql_address_coords: false, ihg_mexico_filtered_address: false };
  let geocodeCalls = 0;
  let geocodeCost = 0;

  log(`[v1.3] warming directories…`);
  try {
    await warmFamilyDirectoryCaches({ delayMs: 100 });
  } catch (err) {
    log(`[v1.3] warm warn: ${err?.message || err}`);
  }

  // —— PASS 1: Structured parent sources (Hilton GraphQL + directory)
  log(`[v1.3] PASS 1 — structured parent sources`);
  await mapPool(records, concurrency, async (record, idx) => {
    const fm = fieldMaps[idx];
    const needAddr = !hasSupportedValue(fm.Address?.value) || String(fm.Address?.value).startsWith("v12_supported:");
    const needCoord =
      !hasSupportedValue(fm.Latitude?.value) ||
      String(fm.Latitude?.value).startsWith("v12_supported:") ||
      !hasSupportedValue(fm.Longitude?.value) ||
      String(fm.Longitude?.value).startsWith("v12_supported:");

    // Always refresh Hilton from GraphQL when gap or preserved
    if (record.family === "Hilton" && (needAddr || needCoord || true)) {
      try {
        const gql = await resolveHiltonGraphQLAddressCoords(record, { delayMs });
        if (gql.ok) {
          if (gql.address) {
            fm.Address = cell(gql.address, {
              source: gql.source_url,
              method: gql.method,
              confidence: gql.confidence,
            });
            learning.hilton_graphql_address_coords = true;
            addressResults.push({ id: record.independent_record_id, family: "Hilton", method: gql.method, ok: true });
          }
          if (gql.latitude != null && gql.longitude != null) {
            fm.Latitude = cell(gql.latitude, {
              source: gql.source_url,
              method: gql.method,
              confidence: "High",
            });
            fm.Longitude = cell(gql.longitude, {
              source: gql.source_url,
              method: gql.method,
              confidence: "High",
            });
            coordResults.push({ id: record.independent_record_id, family: "Hilton", method: gql.method, ok: true });
          }
        }
      } catch (err) {
        addressResults.push({
          id: record.independent_record_id,
          family: "Hilton",
          ok: false,
          error: err?.message || String(err),
        });
      }
    } else if (needAddr) {
      const addr = await resolvePropertyAddress(record, { delayMs, timeoutMs });
      if (addr.ok && addr.claim) {
        fm.Address = cell(addr.claim.address, {
          source: addr.claim.source,
          method: addr.claim.method,
          confidence: addr.claim.confidence,
        });
        addressResults.push({ id: record.independent_record_id, family: record.family, method: addr.claim.method, ok: true });
        if (addr.claim.method?.includes("mexico_filtered")) learning.ihg_mexico_filtered_address = true;
      }
    }

    if ((idx + 1) % 50 === 0) log(`[v1.3] pass1 ${idx + 1}/${records.length}`);
  });
  const afterPass1 = scoreAll(records, fieldMaps, contexts);
  log(`[v1.3] after pass1 avg=${afterPass1.portfolio.average_raw_priority_completeness_pct}%`);

  // —— PASS 2: Official property pages — rooms + remaining address/coords
  log(`[v1.3] PASS 2 — official property pages (rooms + remaining address/coords)`);
  await mapPool(records, concurrency, async (record, idx) => {
    const fm = fieldMaps[idx];
    const needRooms = !hasSupportedValue(fm["Rooms / Keys"]?.value);
    const needAddr =
      !hasSupportedValue(fm.Address?.value) || String(fm.Address?.value).startsWith("v12_supported:");
    const needCoord =
      !hasSupportedValue(fm.Latitude?.value) ||
      String(fm.Latitude?.value).startsWith("v12_supported:") ||
      !hasSupportedValue(fm.Longitude?.value);

    if (needRooms) {
      const rooms = await resolveFamilyRooms(record, { delayMs, timeoutMs });
      roomsResults[record.family]?.push({
        id: record.independent_record_id,
        ok: rooms.ok,
        rooms: rooms.claim?.rooms ?? null,
        method: rooms.claim?.method ?? null,
        reason: rooms.reason ?? null,
      });
      if (rooms.ok && rooms.claim) {
        fm["Rooms / Keys"] = cell(rooms.claim.rooms, {
          source: rooms.claim.source,
          method: rooms.claim.method,
          confidence: rooms.claim.confidence,
        });
      }
    }

    if (needAddr) {
      const addr = await resolvePropertyAddress(record, { delayMs, timeoutMs });
      if (addr.ok && addr.claim) {
        fm.Address = cell(addr.claim.address, {
          source: addr.claim.source,
          method: addr.claim.method,
          confidence: addr.claim.confidence,
        });
        if (addr.claim.method?.includes("mexico_filtered")) learning.ihg_mexico_filtered_address = true;
      }
    }

    if (needCoord && hasSupportedValue(fm.Address?.value) && !String(fm.Address.value).startsWith("v12_supported:")) {
      const coords = await resolvePropertyCoordinates(
        record,
        { address: fm.Address.value, source: fm.Address.source },
        { delayMs, timeoutMs, skipGeocode: true }
      );
      if (coords.ok && coords.claim) {
        fm.Latitude = cell(coords.claim.latitude, {
          source: coords.claim.source,
          method: coords.claim.method,
          confidence: coords.claim.confidence,
        });
        fm.Longitude = cell(coords.claim.longitude, {
          source: coords.claim.source,
          method: coords.claim.method,
          confidence: coords.claim.confidence,
        });
      }
    }

    if ((idx + 1) % 50 === 0) log(`[v1.3] pass2 ${idx + 1}/${records.length}`);
  });
  const afterPass2 = scoreAll(records, fieldMaps, contexts);
  log(`[v1.3] after pass2 avg=${afterPass2.portfolio.average_raw_priority_completeness_pct}%`);

  // —— PASS 3: lightweight retry rooms still missing (same ladder — diminishing)
  log(`[v1.3] PASS 3 — rooms retry / owner-operator ladder (native only)`);
  const stillNeedRooms = records
    .map((r, i) => ({ r, i }))
    .filter(({ i }) => !hasSupportedValue(fieldMaps[i]["Rooms / Keys"]?.value));
  // Cap effort — second attempt only for IHG where page OK previously may help with delay
  await mapPool(stillNeedRooms.slice(0, opts.pass3Limit ?? 80), Math.min(2, concurrency), async ({ r, i }) => {
    const rooms = await resolveFamilyRooms(r, { delayMs: delayMs + 150, timeoutMs: 35000 });
    if (rooms.ok && rooms.claim) {
      fieldMaps[i]["Rooms / Keys"] = cell(rooms.claim.rooms, {
        source: rooms.claim.source,
        method: rooms.claim.method + "+pass3",
        confidence: rooms.claim.confidence,
      });
    }
  });
  const afterPass3 = scoreAll(records, fieldMaps, contexts);
  log(`[v1.3] after pass3 avg=${afterPass3.portfolio.average_raw_priority_completeness_pct}%`);

  // —— PASS 4: Approved geocoding
  log(`[v1.3] PASS 4 — geocode cascade (provider=${providerStatus})`);
  const geoTargets = records
    .map((r, i) => ({ r, i }))
    .filter(({ i }) => {
      const fm = fieldMaps[i];
      const hasAddr =
        hasSupportedValue(fm.Address?.value) && !String(fm.Address.value).startsWith("v12_supported:");
      const missCoord =
        !hasSupportedValue(fm.Latitude?.value) ||
        String(fm.Latitude?.value).startsWith("v12_supported:") ||
        !hasSupportedValue(fm.Longitude?.value);
      return hasAddr && missCoord;
    });

  if (providerStatus === "PROVIDER_READY" && !opts.skipGeocode) {
    await mapPool(geoTargets, Math.min(2, concurrency), async ({ r, i }) => {
      const fm = fieldMaps[i];
      const coords = await resolvePropertyCoordinates(
        r,
        { address: fm.Address.value, source: fm.Address.source },
        { delayMs: delayMs + 100, skipGeocode: false }
      );
      if (coords.ok && coords.claim) {
        geocodeCalls += 1;
        geocodeCost += coords.geocode_cost_estimate_usd?.estimated_usd || 0;
        fm.Latitude = cell(coords.claim.latitude, {
          source: coords.claim.source,
          method: coords.claim.method,
          confidence: coords.claim.confidence,
        });
        fm.Longitude = cell(coords.claim.longitude, {
          source: coords.claim.source,
          method: coords.claim.method,
          confidence: coords.claim.confidence,
        });
        coordResults.push({
          id: r.independent_record_id,
          family: r.family,
          method: coords.claim.method,
          ok: true,
          geocoded: true,
        });
      } else {
        coordResults.push({
          id: r.independent_record_id,
          family: r.family,
          ok: false,
          reason: coords.reason,
          provider_status: coords.provider_status || providerStatus,
        });
      }
    });
  } else {
    log(`[v1.3] geocode skipped — ${providerStatus}`);
  }
  const afterPass4 = scoreAll(records, fieldMaps, contexts);
  log(`[v1.3] after pass4 avg=${afterPass4.portfolio.average_raw_priority_completeness_pct}%`);

  // —— PASS 5: First-party / escalation classification
  log(`[v1.3] PASS 5 — first-party gap packs`);
  const final = afterPass4;
  const firstParty = buildFirstPartyPacks(records, fieldMaps, final.perHotel);
  const blockers = buildRemainingBlockers(records, fieldMaps, final, providerStatus);

  const roomsByFamily = {};
  for (const fam of ["IHG", "Hilton", "Choice"]) {
    const subset = final.perHotel.filter((h) => h.family === fam);
    const withRooms = subset.filter((h) => hasSupportedValue(h.rooms) && !String(h.rooms).startsWith("v12_supported:")).length;
    roomsByFamily[fam] = {
      hotels: subset.length,
      with_rooms: withRooms,
      pct: subset.length ? Math.round((1000 * withRooms) / subset.length) / 10 : 0,
    };
  }

  const result = {
    run_id: runId,
    version: "census-autopilot-v1.3-gap-closure",
    elapsed_ms: Date.now() - started,
    external_cost_usd: Math.round(geocodeCost * 10000) / 10000,
    webhound_calls: 0,
    airtable_writes: 0,
    freeze_v12: {
      avg: freeze.portfolio.average_raw_priority_completeness_pct,
      hotels_ge_95: freeze.portfolio.hotels_at_or_above_95_share_pct,
      rooms_pct: freeze.rooms_pct,
      address_pct: freeze.address_pct,
      lat_pct: freeze.lat_pct,
    },
    passes: {
      rebuilt_baseline: baseline.portfolio.average_raw_priority_completeness_pct,
      pass1_structured: afterPass1.portfolio.average_raw_priority_completeness_pct,
      pass2_official_pages: afterPass2.portfolio.average_raw_priority_completeness_pct,
      pass3_rooms_retry: afterPass3.portfolio.average_raw_priority_completeness_pct,
      pass4_geocode: afterPass4.portfolio.average_raw_priority_completeness_pct,
    },
    final: final.portfolio,
    rooms_pct: fieldPct(final.missingness, "Rooms / Keys"),
    address_pct: fieldPct(final.missingness, "Address"),
    lat_pct: fieldPct(final.missingness, "Latitude"),
    lng_pct: fieldPct(final.missingness, "Longitude"),
    rooms_by_family: roomsByFamily,
    provider_status: providerStatus,
    geocode_calls: geocodeCalls,
    learning,
    firewall: {
      cvent: false,
      legacy: false,
      unsupported: 0,
    },
  };

  fs.writeFileSync(
    path.join(runDir, "final-snapshot.json"),
    JSON.stringify({ result, perHotel: final.perHotel, missingness: final.missingness }, null, 2)
  );

  await writeGapClosureArtifacts({
    artifactRoot: ARTIFACT_DIR,
    freeze,
    result,
    baseline,
    afterPass1,
    afterPass2,
    afterPass3,
    afterPass4: final,
    roomsResults,
    addressResults,
    coordResults,
    firstParty,
    blockers,
    learning,
    providerInfo,
    providerStatus,
    roomsByFamily,
  });

  log(
    `[v1.3] DONE avg=${result.final.average_raw_priority_completeness_pct}% rooms=${result.rooms_pct}% addr=${result.address_pct}% lat=${result.lat_pct}%`
  );
  return result;
}

function buildFirstPartyPacks(records, fieldMaps, perHotel) {
  const byFamily = { IHG: [], Hilton: [], Choice: [] };
  records.forEach((r, i) => {
    const fm = fieldMaps[i];
    const gaps = [];
    if (!hasSupportedValue(fm["Rooms / Keys"]?.value)) gaps.push("Rooms / Keys");
    if (!hasSupportedValue(fm.Address?.value) || String(fm.Address.value).startsWith("v12_supported:")) {
      gaps.push("Address");
    }
    if (!hasSupportedValue(fm.Latitude?.value) || String(fm.Latitude.value).startsWith("v12_supported:")) {
      gaps.push("Latitude/Longitude");
    }
    if (!gaps.length) return;
    byFamily[r.family]?.push({
      independent_record_id: r.independent_record_id,
      name: r.name,
      brand: r.brand,
      city: r.city,
      website: r.website,
      property_ids: r.property_ids,
      gaps,
      completeness_pct: perHotel[i]?.raw_priority_completeness_pct,
    });
  });
  return {
    note: "NOT SENT — unresolved gaps only",
    packs: byFamily,
    rooms_only: {
      IHG: byFamily.IHG.filter((x) => x.gaps.includes("Rooms / Keys")).map((x) => ({
        name: x.name,
        website: x.website,
        property_ids: x.property_ids,
      })),
      Hilton: byFamily.Hilton.filter((x) => x.gaps.includes("Rooms / Keys")).map((x) => ({
        name: x.name,
        website: x.website,
        property_ids: x.property_ids,
      })),
      Choice: byFamily.Choice.filter((x) => x.gaps.includes("Rooms / Keys")).map((x) => ({
        name: x.name,
        website: x.website,
        property_ids: x.property_ids,
      })),
    },
  };
}

function buildRemainingBlockers(records, fieldMaps, final, providerStatus) {
  let roomsMissing = 0;
  let addrMissing = 0;
  let coordMissing = 0;
  let coordProviderBlocked = 0;
  const byFamilyRooms = { IHG: 0, Hilton: 0, Choice: 0 };
  records.forEach((r, i) => {
    const fm = fieldMaps[i];
    if (!hasSupportedValue(fm["Rooms / Keys"]?.value)) {
      roomsMissing += 1;
      byFamilyRooms[r.family] = (byFamilyRooms[r.family] || 0) + 1;
    }
    if (!hasSupportedValue(fm.Address?.value) || String(fm.Address?.value).startsWith("v12_supported:")) {
      addrMissing += 1;
    }
    if (!hasSupportedValue(fm.Latitude?.value) || String(fm.Latitude?.value).startsWith("v12_supported:")) {
      coordMissing += 1;
      if (
        hasSupportedValue(fm.Address?.value) &&
        !String(fm.Address.value).startsWith("v12_supported:") &&
        providerStatus !== "PROVIDER_READY"
      ) {
        coordProviderBlocked += 1;
      }
    }
  });
  return {
    rooms_missing: roomsMissing,
    rooms_missing_by_family: byFamilyRooms,
    address_missing: addrMissing,
    coordinates_missing: coordMissing,
    coordinates_provider_blocked: coordProviderBlocked,
    hotels_below_95: final.perHotel.filter((h) => !h.meets_95).length,
    cheapest_path: [
      "First-party Rooms / Keys validation packs per family (bulk spreadsheet)",
      addrMissing ? "IHG address page retries + steward review for remaining" : null,
      coordMissing && providerStatus === "PROVIDER_READY"
        ? "Continue Mapbox geocode for address-confirmed residuals"
        : null,
    ].filter(Boolean),
  };
}
