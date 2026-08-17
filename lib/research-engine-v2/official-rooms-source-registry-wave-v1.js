/**
 * Official Rooms Source Registry Wave — matrix + Tier A production NULL_FILL.
 *
 * Integrates:
 * - Colombia RNT (existing) + medium corroboration promotion
 * - Peru MINCETUR HABI
 * - Brazil CADASTUR Unidade Habitacionais
 * - Barbados BTPA Number of Bedrooms (when HTML parse yields rows)
 *
 * Never: HBX rooms[], Cvent-only, Benchmark Census rooms, destructive overwrite.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { createLiveHotelPropertyCensusAdapter } from "./census-autopilot-batch-engine.js";
import {
  buildOfficialRoomsSourceMatrix,
  listTierASources,
  OFFICIAL_ROOMS_SOURCE_REGISTRY_VERSION,
  SOURCE_TIER,
} from "./cala-official-rooms-source-registry-v1.js";
import { fetchColombiaRntLodgingRows } from "./colombia-rnt-open-data-adapter.js";
import {
  fetchPeruMinceturCsv,
  isPeruMinceturHotelRow,
  mapPeruMinceturRowToCensusCandidate,
  MAP_PERU_MINCETUR,
  normalizePeruText,
  titleCasePeruPlace,
} from "./peru-mincetur-open-data-adapter.js";
import { fetchBrazilCadasturLodgingRows } from "./brazil-cadastur-open-data-adapter.js";
import { fetchBarbadosBtpaDirectoryRows } from "./barbados-btpa-directory-adapter.js";
import {
  matchCensusToColombiaRntRooms,
  matchCensusToPeruMinceturRooms,
  matchCensusToBrazilCadasturRooms,
  matchCensusToBarbadosBtpaRooms,
  promoteColombiaRntMediumWithCorroboration,
  buildSecondaryRoomsPatch,
  indexNormalizedRegistryByCity,
} from "./census-rooms-secondary-match.js";
import { resolveSecondaryHotelDataPolicy } from "./census-secondary-hotel-data-policy.js";
import {
  POSTAL_CODE_FIELD,
  extractPostalFromAddress,
  normalizePostalCode,
  isValidPostalForCountry,
} from "./census-postal-code-v1.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");
const STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/official-rooms-source-registry"
);
const REPORT_JSON = path.join(
  ROOT,
  "reports/research-engine-v2/official-rooms-source-registry-final.json"
);
const REPORT_MD = path.join(
  ROOT,
  "reports/research-engine-v2/official-rooms-source-registry-final.md"
);
const MATRIX_JSON = path.join(
  ROOT,
  "reports/research-engine-v2/cala-official-rooms-source-matrix.json"
);
const MATRIX_MD = path.join(
  ROOT,
  "reports/research-engine-v2/cala-official-rooms-source-matrix.md"
);

const READ_FIELDS = [
  "Property Name",
  "Canonical Property Name",
  "Country",
  "City",
  "State / Region",
  "Address",
  POSTAL_CODE_FIELD,
  "Phone",
  "Official Property URL",
  MAP_ROOMS.roomsKeys,
  MAP_ROOMS.confidenceExisting,
  MAP_ROOMS.sourceUrlExisting,
  MAP_ROOMS.sourceTypePlanned,
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2));
}

function writeMd(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text);
}

async function listCensusRecords(baseId, token, fields) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || ""}`
      );
    }
    records.push(...(json.records || []));
    offset = json.offset;
    await sleep(90);
  } while (offset);
  return records;
}

function countRooms(records) {
  let n = 0;
  /** @type {Record<string, number>} */
  const byCountry = {};
  for (const r of records) {
    const f = r.fields || {};
    if (!isBlank(f[MAP_ROOMS.roomsKeys])) {
      n += 1;
      const c = String(f.Country || "?");
      byCountry[c] = (byCountry[c] || 0) + 1;
    }
  }
  return { rooms_populated: n, by_country: byCountry };
}

function nullFillFundamentals(fields, fundamentals = {}) {
  /** @type {Record<string, unknown>} */
  const patch = {};
  if (fundamentals.address && isBlank(fields.Address)) {
    patch.Address = fundamentals.address;
  }
  if (fundamentals.phone && isBlank(fields.Phone)) {
    patch.Phone = fundamentals.phone;
  }
  if (
    fundamentals.website &&
    isBlank(fields["Official Property URL"]) &&
    /^https?:\/\//i.test(String(fundamentals.website))
  ) {
    patch["Official Property URL"] = fundamentals.website;
  }
  if (fundamentals.city && isBlank(fields.City)) {
    patch.City = fundamentals.city;
  }
  if (fundamentals.state_region && isBlank(fields["State / Region"])) {
    patch["State / Region"] = fundamentals.state_region;
  }
  if (fundamentals.postal_code && isBlank(fields[POSTAL_CODE_FIELD])) {
    const country = fields.Country;
    const n = normalizePostalCode(fundamentals.postal_code, country);
    if (n && isValidPostalForCountry(n, country)) {
      patch[POSTAL_CODE_FIELD] = n;
    }
  } else if (
    fundamentals.address &&
    isBlank(fields[POSTAL_CODE_FIELD]) &&
    fields.Country
  ) {
    const fromAddr = extractPostalFromAddress(
      fundamentals.address,
      fields.Country
    );
    if (fromAddr.ok) patch[POSTAL_CODE_FIELD] = fromAddr.postal_code;
  }
  return patch;
}

function bumpPerf(perf, source, key, n = 1) {
  if (!perf[source]) {
    perf[source] = {
      geography: "",
      records_available: 0,
      records_processed: 0,
      census_matches_high: 0,
      rooms_populated: 0,
      rooms_candidate: 0,
      conflicts: 0,
      address_patches: 0,
      postal_patches: 0,
      phone_patches: 0,
      website_patches: 0,
      city_patches: 0,
      state_patches: 0,
      requests: 0,
      errors: 0,
      yield_pct: 0,
    };
  }
  perf[source][key] = (perf[source][key] || 0) + n;
}

function peruRawToNormalized(rawRows) {
  return rawRows
    .filter((r) => isPeruMinceturHotelRow(r))
    .map((row) => {
      const cand = mapPeruMinceturRowToCensusCandidate(row, { dryRun: true });
      const f = cand.fields || {};
      const distrito = titleCasePeruPlace(row?.[MAP_PERU_MINCETUR.distrito]);
      const provincia = titleCasePeruPlace(row?.PROVINCIA || "");
      const depto = titleCasePeruPlace(row?.[MAP_PERU_MINCETUR.departamento]);
      return {
        property_name: f[MAP_PERU_MINCETUR.propertyName],
        commercial_name: normalizePeruText(
          row?.[MAP_PERU_MINCETUR.nombreComercial]
        ),
        legal_name: normalizePeruText(row?.[MAP_PERU_MINCETUR.razonSocial]),
        city: distrito,
        match_cities: [distrito, provincia, depto].filter(Boolean),
        state_region: depto,
        country: "Peru",
        address: f[MAP_PERU_MINCETUR.address] || null,
        postal_code: null,
        phone: f[MAP_PERU_MINCETUR.phone] || null,
        website: f[MAP_PERU_MINCETUR.officialPropertyUrl] || null,
        rooms: f[MAP_PERU_MINCETUR.roomsKeys] ?? null,
        identity_key: cand.identity_key,
        source_url: MAP_PERU_MINCETUR.sourceDatasetUrl,
      };
    })
    .filter((r) => r.property_name && r.rooms != null);
}

/**
 * @param {{
 *   mode?: 'dry-run'|'run',
 *   enableProductionWrites?: boolean,
 *   maxPerSource?: number,
 *   skipBarbados?: boolean,
 *   log?: Function,
 * }} [opts]
 */
export async function runOfficialRoomsSourceRegistryWave(opts = {}) {
  const mode = opts.mode || "dry-run";
  const enableWrites =
    Boolean(opts.enableProductionWrites) && mode === "run";
  const log = opts.log || console.log;
  const maxPerSource = Number(opts.maxPerSource || 5000);
  const generated_at = new Date().toISOString();
  fs.mkdirSync(STATE_DIR, { recursive: true });

  const matrix = buildOfficialRoomsSourceMatrix();
  writeJson(MATRIX_JSON, matrix);
  writeMd(
    MATRIX_MD,
    [
      `# CALA Official Rooms Source Matrix`,
      ``,
      `Version: \`${matrix.version}\``,
      `Geographies: ${matrix.GEOGRAPHIES_ASSESSED_LABEL}`,
      ``,
      `## Tier A`,
      ...(matrix.TIER_A_SOURCES_FOUND || []).map((s) => `- \`${s}\``),
      ``,
      `## Geographies without scalable rooms source`,
      `Count: ${(matrix.GEOGRAPHIES_WITH_NO_SCALABLE_ROOM_SOURCE || []).length}`,
      ``,
    ].join("\n")
  );
  log(
    `[ors] matrix ${matrix.GEOGRAPHIES_ASSESSED_LABEL}; Tier A=${matrix.TIER_A_SOURCES_FOUND.length}`
  );

  const policy = resolveSecondaryHotelDataPolicy();
  if (!policy.enable_secondary_rooms_sources && enableWrites) {
    return {
      ok: false,
      OFFICIAL_ROOMS_SOURCE_REGISTRY_STATUS: "blocked_secondary_rooms_policy",
      message:
        "Set ENABLE_SECONDARY_HOTEL_DATA_SOURCES=1 ENABLE_SECONDARY_ROOMS_SOURCES=1",
    };
  }

  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;
  assertProductionCensusWriteTarget({
    baseId,
    tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });

  log(`[ors] listing Hotel Property Census…`);
  const records = await listCensusRecords(baseId, token, READ_FIELDS);
  const before = countRooms(records);
  log(
    `[ors] n=${records.length} rooms_before=${before.rooms_populated}`
  );

  /** @type {Map<string, { id: string, patch: Record<string, unknown> }>} */
  const patchMap = new Map();
  const sourcePerf = {};
  const roomsBySource = {};
  const roomsByCountryWritten = {};
  let roomsWritten = 0;
  let roomsCandidates = 0;
  let roomsConflicts = 0;
  let colombiaPromoted = 0;
  let addressPatches = 0;
  let postalPatches = 0;
  let phonePatches = 0;
  let websitePatches = 0;
  let cityPatches = 0;
  let statePatches = 0;
  let errors = 0;
  const integrated = [];
  const sampleYield = {};

  function applyMatch(rec, match, sourceId, geography) {
    bumpPerf(sourcePerf, sourceId, "records_processed");
    sourcePerf[sourceId].geography = geography;
    if (!match?.ok) return;
    if (match.confidence === "High") {
      bumpPerf(sourcePerf, sourceId, "census_matches_high");
      const built = buildSecondaryRoomsPatch(rec.fields || {}, match);
      if (built.conflict) {
        roomsConflicts += 1;
        bumpPerf(sourcePerf, sourceId, "conflicts");
        return;
      }
      if (!built.ok || !built.write_rooms_value) {
        if (built.reason === "rooms_already_populated") return;
        return;
      }
      const fund = nullFillFundamentals(
        { ...(rec.fields || {}), ...(patchMap.get(rec.id)?.patch || {}) },
        match.fundamentals || {}
      );
      const prev = patchMap.get(rec.id) || { id: rec.id, patch: {} };
      Object.assign(prev.patch, built.patch, fund);
      patchMap.set(rec.id, prev);
      roomsWritten += 1;
      bumpPerf(sourcePerf, sourceId, "rooms_populated");
      roomsBySource[sourceId] = (roomsBySource[sourceId] || 0) + 1;
      roomsByCountryWritten[geography] =
        (roomsByCountryWritten[geography] || 0) + 1;
      if (fund.Address) {
        addressPatches += 1;
        bumpPerf(sourcePerf, sourceId, "address_patches");
      }
      if (fund[POSTAL_CODE_FIELD]) {
        postalPatches += 1;
        bumpPerf(sourcePerf, sourceId, "postal_patches");
      }
      if (fund.Phone) {
        phonePatches += 1;
        bumpPerf(sourcePerf, sourceId, "phone_patches");
      }
      if (fund["Official Property URL"]) {
        websitePatches += 1;
        bumpPerf(sourcePerf, sourceId, "website_patches");
      }
      if (fund.City) {
        cityPatches += 1;
        bumpPerf(sourcePerf, sourceId, "city_patches");
      }
      if (fund["State / Region"]) {
        statePatches += 1;
        bumpPerf(sourcePerf, sourceId, "state_patches");
      }
      if (match.promoted) colombiaPromoted += 1;
    } else if (match.confidence === "Medium") {
      roomsCandidates += 1;
      bumpPerf(sourcePerf, sourceId, "rooms_candidate");
    }
  }

  // —— Brazil CADASTUR ——
  try {
    log(`[ors] Brazil CADASTUR fetch…`);
    bumpPerf(sourcePerf, "brazil_cadastur", "requests");
    const br = await fetchBrazilCadasturLodgingRows({ hotelsOnly: true });
    if (br.ok) {
      integrated.push("brazil_cadastur_meios");
      sourcePerf.brazil_cadastur.records_available = br.rows.length;
      log(`[ors] Brazil CADASTUR rows=${br.rows.length}`);
      const brIndex = indexNormalizedRegistryByCity(br.rows);
      const missing = records.filter(
        (r) =>
          /^brazil$/i.test(String(r.fields?.Country || "")) &&
          isBlank(r.fields?.[MAP_ROOMS.roomsKeys]) &&
          !isBlank(r.fields?.City)
      );
      let attempted = 0;
      let high = 0;
      for (const rec of missing) {
        if (attempted >= maxPerSource) break;
        attempted += 1;
        const match = matchCensusToBrazilCadasturRooms(
          rec.fields || {},
          br.rows,
          { cityIndex: brIndex }
        );
        if (match.ok && match.confidence === "High") high += 1;
        applyMatch(rec, match, "brazil_cadastur", "Brazil");
      }
      sampleYield.brazil_cadastur = {
        missing_rooms: missing.length,
        attempted,
        rooms_high: high,
        PROPERTY_MATCH_RATE:
          attempted > 0 ? Number(((100 * high) / attempted).toFixed(1)) : 0,
        ROOM_FIELD_AVAILABILITY: 100,
        ROOM_SEMANTICS_CONFIDENCE: "HIGH_UH_not_leitos",
      };
      log(
        `[ors] Brazil attempted=${attempted} high=${high}`
      );
    } else {
      errors += 1;
      bumpPerf(sourcePerf, "brazil_cadastur", "errors");
      log(`[ors] Brazil CADASTUR failed: ${br.message}`);
    }
  } catch (err) {
    errors += 1;
    log(`[ors] Brazil error: ${String(err?.message || err).slice(0, 160)}`);
  }

  // —— Peru MINCETUR ——
  try {
    log(`[ors] Peru MINCETUR fetch…`);
    bumpPerf(sourcePerf, "peru_mincetur", "requests");
    const peFetch = await fetchPeruMinceturCsv({});
    if (peFetch.ok) {
      integrated.push("peru_mincetur_hospedaje");
      const peRows = peruRawToNormalized(peFetch.rows || []);
      sourcePerf.peru_mincetur.records_available = peRows.length;
      log(`[ors] Peru MINCETUR hotel rows=${peRows.length}`);
      const peIndex = indexNormalizedRegistryByCity(peRows);
      const missing = records.filter(
        (r) =>
          /^peru$/i.test(String(r.fields?.Country || "")) &&
          isBlank(r.fields?.[MAP_ROOMS.roomsKeys]) &&
          !isBlank(r.fields?.City)
      );
      let attempted = 0;
      let high = 0;
      for (const rec of missing) {
        if (attempted >= maxPerSource) break;
        attempted += 1;
        const match = matchCensusToPeruMinceturRooms(rec.fields || {}, peRows, {
          cityIndex: peIndex,
        });
        if (match.ok && match.confidence === "High") high += 1;
        applyMatch(rec, match, "peru_mincetur", "Peru");
      }
      sampleYield.peru_mincetur = {
        missing_rooms: missing.length,
        attempted,
        rooms_high: high,
        PROPERTY_MATCH_RATE:
          attempted > 0 ? Number(((100 * high) / attempted).toFixed(1)) : 0,
        ROOM_FIELD_AVAILABILITY: 100,
        ROOM_SEMANTICS_CONFIDENCE: "HIGH_HABI_not_CAMA",
      };
      log(`[ors] Peru attempted=${attempted} high=${high}`);
    } else {
      errors += 1;
      bumpPerf(sourcePerf, "peru_mincetur", "errors");
      log(`[ors] Peru fetch failed: ${peFetch.message || peFetch.error_kind}`);
    }
  } catch (err) {
    errors += 1;
    log(`[ors] Peru error: ${String(err?.message || err).slice(0, 160)}`);
  }

  // —— Colombia RNT remainder + medium promotion ——
  try {
    log(`[ors] Colombia RNT fetch…`);
    bumpPerf(sourcePerf, "colombia_rnt", "requests");
    const co = await fetchColombiaRntLodgingRows({
      maxRows: 25000,
      pageSize: 5000,
      year: 2026,
      hotelsOnly: true,
    });
    if (co.ok && (co.rows || []).length) {
      if (!integrated.includes("colombia_rnt")) integrated.push("colombia_rnt");
      sourcePerf.colombia_rnt.records_available = co.rows.length;
      const missing = records.filter(
        (r) =>
          /^colombia$/i.test(String(r.fields?.Country || "")) &&
          isBlank(r.fields?.[MAP_ROOMS.roomsKeys]) &&
          !isBlank(r.fields?.City)
      );
      let attempted = 0;
      let high = 0;
      let promoted = 0;
      for (const rec of missing) {
        if (attempted >= maxPerSource) break;
        attempted += 1;
        let match = matchCensusToColombiaRntRooms(rec.fields || {}, co.rows, {
          fuzzy: false,
        });
        if (match.ok && match.confidence === "Medium") {
          const promo = promoteColombiaRntMediumWithCorroboration(
            rec.fields || {},
            match
          );
          if (promo.ok && promo.promoted) {
            match = promo;
            promoted += 1;
          }
        }
        if (match.ok && match.confidence === "High") high += 1;
        applyMatch(rec, match, "colombia_rnt", "Colombia");
      }
      sampleYield.colombia_rnt = {
        missing_rooms: missing.length,
        attempted,
        rooms_high: high,
        medium_promoted: promoted,
        PROPERTY_MATCH_RATE:
          attempted > 0 ? Number(((100 * high) / attempted).toFixed(1)) : 0,
        ROOM_FIELD_AVAILABILITY: 100,
        ROOM_SEMANTICS_CONFIDENCE: "HIGH_habitaciones_not_camas",
      };
      log(
        `[ors] Colombia attempted=${attempted} high=${high} promoted=${promoted}`
      );
    } else {
      log(`[ors] Colombia RNT skipped: ${co.message || co.error_kind}`);
    }
  } catch (err) {
    errors += 1;
    log(`[ors] Colombia error: ${String(err?.message || err).slice(0, 160)}`);
  }

  // —— Barbados BTPA ——
  if (!opts.skipBarbados) {
    try {
      log(`[ors] Barbados BTPA fetch…`);
      bumpPerf(sourcePerf, "barbados_btpa", "requests");
      const bb = await fetchBarbadosBtpaDirectoryRows({});
      if (bb.ok && bb.rows.length) {
        integrated.push("barbados_btpa_directory");
        sourcePerf.barbados_btpa.records_available = bb.rows.length;
        const missing = records.filter(
          (r) =>
            /^barbados$/i.test(String(r.fields?.Country || "")) &&
            isBlank(r.fields?.[MAP_ROOMS.roomsKeys])
        );
        let attempted = 0;
        let high = 0;
        for (const rec of missing) {
          if (attempted >= maxPerSource) break;
          attempted += 1;
          const match = matchCensusToBarbadosBtpaRooms(
            rec.fields || {},
            bb.rows
          );
          if (match.ok && match.confidence === "High") high += 1;
          applyMatch(rec, match, "barbados_btpa", "Barbados");
        }
        sampleYield.barbados_btpa = {
          missing_rooms: missing.length,
          attempted,
          rooms_high: high,
          PROPERTY_MATCH_RATE:
            attempted > 0 ? Number(((100 * high) / attempted).toFixed(1)) : 0,
          ROOM_FIELD_AVAILABILITY: 100,
          ROOM_SEMANTICS_CONFIDENCE: "HIGH_number_of_bedrooms",
        };
        log(`[ors] Barbados attempted=${attempted} high=${high}`);
      } else {
        log(
          `[ors] Barbados BTPA no rows: ${bb.message || "empty"} — held as Tier A pending parser`
        );
        sampleYield.barbados_btpa = {
          ACCESS_STABILITY: "html_parse_unstable_or_empty",
          rows: bb.rows?.length || 0,
        };
      }
    } catch (err) {
      errors += 1;
      log(`[ors] Barbados error: ${String(err?.message || err).slice(0, 160)}`);
    }
  }

  for (const [k, row] of Object.entries(sourcePerf)) {
    row.yield_pct =
      row.records_processed > 0
        ? Number(
            ((100 * row.rooms_populated) / row.records_processed).toFixed(1)
          )
        : 0;
    void k;
  }

  const uniqPatches = [...patchMap.values()].filter(
    (p) => Object.keys(p.patch).length
  );
  const totalFieldWrites = uniqPatches.reduce(
    (a, p) => a + Object.keys(p.patch).length,
    0
  );

  let written = 0;
  if (enableWrites && uniqPatches.length) {
    const adapter = createLiveHotelPropertyCensusAdapter({
      token,
      baseId,
      tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
    });
    log(`[ors] writing ${uniqPatches.length} census patches…`);
    const res = await adapter.patchRecords(
      uniqPatches.map((p) => ({ id: p.id, fields: p.patch }))
    );
    written = res.updated || 0;
    if (res.errors?.length) {
      errors += res.errors.length;
      log(`[ors] patch errors: ${res.errors.length}`);
    }
  }

  let afterRecords = records;
  if (enableWrites && written) {
    afterRecords = await listCensusRecords(baseId, token, READ_FIELDS);
  } else if (!enableWrites) {
    afterRecords = records.map((r) => {
      const p = patchMap.get(r.id);
      if (!p) return r;
      return { ...r, fields: { ...r.fields, ...p.patch } };
    });
  }
  const after = countRooms(afterRecords);

  const topYield = Object.entries(sourcePerf)
    .map(([source, row]) => ({ source, ...row }))
    .sort((a, b) => b.rooms_populated - a.rooms_populated)
    .slice(0, 10);

  const nextOpportunities = [
    ...listTierASources()
      .filter((s) => !integrated.includes(s.source_id))
      .map((s) => ({
        source_id: s.source_id,
        geography: s.geography,
        action: s.recommended_action,
        tier: s.tier,
      })),
    {
      source_id: "uruguay_mintur_refresh",
      geography: "Uruguay",
      action: "recheck_habitaciones_column_population",
      tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    },
    {
      source_id: "ecuador_mintur_capacity_if_property_export",
      geography: "Ecuador",
      action: "watch_for_property_level_habitaciones_export",
      tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    },
    {
      source_id: "mexico_sectur_rnt_bulk",
      geography: "Mexico",
      action: "build_if_bulk_habitaciones_becomes_available",
      tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    },
    {
      source_id: "chile_sernatur_rooms_export",
      geography: "Chile",
      action: "seek_current_property_rooms_export",
      tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    },
    {
      source_id: "puerto_rico_roomtax",
      geography: "Puerto Rico",
      action: "USAGE_REVIEW_before_any_write",
      tier: SOURCE_TIER.USAGE_REVIEW,
    },
  ].slice(0, 10);

  const final = {
    ok: true,
    OFFICIAL_ROOMS_SOURCE_REGISTRY_STATUS: enableWrites
      ? "official_rooms_source_registry_wave_complete"
      : "official_rooms_source_registry_dry_run_complete",
    version: OFFICIAL_ROOMS_SOURCE_REGISTRY_VERSION,
    WAVE: "cala_official_rooms_source_registry_v1",
    mode,
    production_writes: enableWrites,
    GEOGRAPHIES_ASSESSED: matrix.GEOGRAPHIES_ASSESSED_LABEL,
    TIER_A_SOURCES_FOUND: matrix.TIER_A_SOURCES_FOUND,
    TIER_B_SOURCES_FOUND: matrix.TIER_B_SOURCES_FOUND,
    USAGE_REVIEW_SOURCES: matrix.USAGE_REVIEW_SOURCES,
    TIER_A_SOURCES_INTEGRATED: integrated,
    CENSUS_COUNT: afterRecords.length,
    ROOMS_POPULATED_BEFORE: before.rooms_populated,
    ROOMS_POPULATED_AFTER: after.rooms_populated,
    ROOMS_COMPLETENESS: Math.round(
      (100 * after.rooms_populated) / Math.max(afterRecords.length, 1)
    ),
    ROOMS_WRITTEN_THIS_RUN: roomsWritten,
    ROOMS_CANDIDATES_HELD: roomsCandidates,
    ROOMS_CONFLICTS: roomsConflicts,
    ROOMS_BY_COUNTRY: roomsByCountryWritten,
    ROOMS_BY_SOURCE: roomsBySource,
    COLOMBIA_RNT_MEDIUM_PROMOTED: colombiaPromoted,
    ADDRESS_PATCHES: addressPatches,
    POSTAL_CODE_PATCHES: postalPatches,
    PHONE_PATCHES: phonePatches,
    WEBSITE_PATCHES: websitePatches,
    CITY_PATCHES: cityPatches,
    STATE_REGION_PATCHES: statePatches,
    TOTAL_PRODUCTION_FIELD_WRITES: enableWrites ? totalFieldWrites : 0,
    TOTAL_PROPOSED_FIELD_WRITES: totalFieldWrites,
    RECORDS_PATCHED: enableWrites ? written : uniqPatches.length,
    SAMPLE_YIELD_TESTS: sampleYield,
    TOP_10_HIGHEST_YIELD_ROOM_SOURCES: topYield,
    TOP_10_NEXT_SOURCE_OPPORTUNITIES: nextOpportunities,
    GEOGRAPHIES_WITH_NO_SCALABLE_ROOM_SOURCE:
      matrix.GEOGRAPHIES_WITH_NO_SCALABLE_ROOM_SOURCE,
    SOURCE_PERFORMANCE: sourcePerf,
    HBX_ROOMS_ARRAY_WRITES: 0,
    CVENT_ONLY_ROOM_VALIDATIONS: 0,
    BENCHMARK_ROOM_WRITES: 0,
    DESTRUCTIVE_OVERWRITES: 0,
    WRONG_TABLE_WRITES: 0,
    ERRORS: errors,
    FOUNDER_DECISION_REQUIRED:
      (matrix.USAGE_REVIEW_SOURCES || []).length > 0 ? "YES" : "NO",
    FOUNDER_DECISION_ITEMS: [
      {
        item: "puerto_rico_roomtax",
        reason: "Likely property rooms exist but portal is login/tax — usage/storage review before writes",
      },
      {
        item: "trinidad_tobago_tttic",
        reason: "Certified listing exists; rooms field not verified as open bulk",
      },
    ],
    NEXT_RECOMMENDED_ACTION:
      "Continue Tier A remainder (Barbados parser hardening if empty) + hunt Mexico/Chile/DR bulk rooms exports; property-page research only for geographies without scalable registry",
    MATRIX_PATH: MATRIX_JSON,
    generated_at,
  };

  writeJson(REPORT_JSON, final);
  writeJson(path.join(STATE_DIR, "source-performance.json"), sourcePerf);
  writeJson(path.join(STATE_DIR, "checkpoint.json"), {
    updated_at: generated_at,
    integrated,
    rooms_written: roomsWritten,
    mode,
  });
  writeMd(
    REPORT_MD,
    [
      `# Official Rooms Source Registry Wave`,
      ``,
      `Status: \`${final.OFFICIAL_ROOMS_SOURCE_REGISTRY_STATUS}\``,
      `Geographies: ${final.GEOGRAPHIES_ASSESSED}`,
      ``,
      `| Metric | Value |`,
      `| --- | ---: |`,
      `| Rooms before | ${final.ROOMS_POPULATED_BEFORE} |`,
      `| Rooms after | ${final.ROOMS_POPULATED_AFTER} |`,
      `| Rooms written | ${final.ROOMS_WRITTEN_THIS_RUN} |`,
      `| Colombia medium promoted | ${final.COLOMBIA_RNT_MEDIUM_PROMOTED} |`,
      ``,
      `Tier A integrated: ${(integrated || []).join(", ")}`,
      `HBX rooms[] writes: 0 · Cvent-only: 0 · Benchmark: 0`,
    ].join("\n")
  );

  return final;
}
