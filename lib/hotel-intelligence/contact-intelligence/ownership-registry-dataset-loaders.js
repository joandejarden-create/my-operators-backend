/**
 * Default local loaders for ownership native method router.
 * Prefer saved official datasets on disk — no network by default.
 * Dependency injection still overrides these in tests.
 */

import fs from "node:fs";
import path from "node:path";
import {
  fetchBrazilCadasturLodgingRows,
  BRAZIL_CADASTUR_ADAPTER_VERSION,
  MAP_BRAZIL_CADASTUR,
} from "../../research-engine-v2/brazil-cadastur-open-data-adapter.js";
import {
  DENUE_CLIENT_VERSION,
  DENUE_ENTIDAD,
  DENUE_STATE_CSV_URL,
  loadDenueHotelRowsFromCsvDir,
} from "../../inegi-denue/client.js";

export const OWNERSHIP_REGISTRY_DATASET_LOADER_VERSION = "ownership-registry-dataset-loader-v1";

export const REGISTRY_DATASET_STATUS = Object.freeze({
  LOADED: "LOADED",
  DATASET_UNAVAILABLE: "DATASET_UNAVAILABLE",
});

const CADASTUR_CANDIDATE_PATHS = [
  "data/brazil-cadastur/meio-de-hospedagem-1-trimestre-2026.xlsx",
  "data/research-engine-v2/official-rooms-sources/brazil-cadastur-q1-2026.xlsx",
];

const DENUE_DATA_DIR = "data/inegi-denue";

/** Map hotel geography → DENUE federal entity code for saved indexes. */
export function resolveDenueEntidadCode(hotel = {}) {
  if (hotel.denue_entidad) return String(hotel.denue_entidad).padStart(2, "0");
  const blob = [
    hotel.state_region,
    hotel.state,
    hotel.entidad,
    hotel.market,
    hotel.city,
    hotel.submarket,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

  if (/quintana|cancun|cancún|riviera maya|playa mujeres|tulum|cozumel|isla mujeres/.test(blob)) {
    return DENUE_ENTIDAD.QUINTANA_ROO;
  }
  if (/ciudad de mexico|cdmx|mexico city|df\b|coyoacan|polanco|condesa|roma norte/.test(blob)) {
    return DENUE_ENTIDAD.CDMX;
  }
  if (/jalisco|guadalajara|zapopan|puerto vallarta/.test(blob)) {
    return DENUE_ENTIDAD.JALISCO;
  }
  if (/baja california sur|los cabos|cabo san lucas|san jose del cabo|la paz/.test(blob)) {
    return DENUE_ENTIDAD.BCS;
  }
  return null;
}

function resolveRoot(root) {
  return root || process.cwd();
}

function findFirstExisting(root, relatives) {
  for (const rel of relatives) {
    const abs = path.isAbsolute(rel) ? rel : path.join(root, rel);
    if (fs.existsSync(abs)) return abs;
  }
  return null;
}

/**
 * Load Cadastur lodging rows from local XLSX only (forceDownload=false, no fetch if missing).
 */
export async function loadCadasturDatasetForHotel(hotel = {}, opts = {}) {
  const root = resolveRoot(opts.root);
  const xlsxPath =
    opts.xlsxPath ||
    findFirstExisting(root, opts.candidatePaths || CADASTUR_CANDIDATE_PATHS);

  if (!xlsxPath) {
    return {
      ok: false,
      status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
      reason: "CADASTUR_XLSX_NOT_FOUND_LOCALLY",
      rows: [],
      provenance: {
        adapter: BRAZIL_CADASTUR_ADAPTER_VERSION,
        source_family: MAP_BRAZIL_CADASTUR.familySourceFamily,
        source_dataset_url: MAP_BRAZIL_CADASTUR.sourceDatasetUrl,
        searched_paths: (opts.candidatePaths || CADASTUR_CANDIDATE_PATHS).map((p) =>
          path.isAbsolute(p) ? p : path.join(root, p)
        ),
        local_only: true,
        network: false,
      },
    };
  }

  const loaded = await fetchBrazilCadasturLodgingRows({
    xlsxPath,
    forceDownload: false,
    hotelsOnly: true,
    // Never download when file missing — fetchBrazilCadasturLodgingRows downloads if missing;
    // we only call when existsSync already verified.
    maxRows: opts.maxRows,
  });

  if (!loaded?.ok) {
    return {
      ok: false,
      status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
      reason: loaded?.error_kind || loaded?.message || "CADASTUR_LOAD_FAILED",
      rows: [],
      provenance: {
        adapter: BRAZIL_CADASTUR_ADAPTER_VERSION,
        cache_path: xlsxPath,
        local_only: true,
        network: false,
      },
    };
  }

  const stat = fs.statSync(xlsxPath);
  return {
    ok: true,
    status: REGISTRY_DATASET_STATUS.LOADED,
    rows: loaded.rows || [],
    provenance: {
      adapter: BRAZIL_CADASTUR_ADAPTER_VERSION,
      source_family: MAP_BRAZIL_CADASTUR.familySourceFamily,
      source_type: MAP_BRAZIL_CADASTUR.sourceType,
      source_dataset_url: MAP_BRAZIL_CADASTUR.sourceDatasetUrl,
      dataset_label: "Meios de Hospedagem 1º trimestre 2026",
      dataset_date: "2026-Q1",
      cache_path: loaded.cache_path || xlsxPath,
      file_bytes: stat.size,
      file_mtime: stat.mtime.toISOString(),
      raw_count: loaded.raw_count,
      row_count: (loaded.rows || []).length,
      local_only: true,
      network: false,
      hotel_hint: {
        hotel_id: hotel.hotel_id || hotel.census_id || null,
        hotel_name: hotel.hotel_name || hotel.property_name || null,
        city: hotel.city || null,
        country: hotel.country || null,
      },
    },
  };
}

/**
 * Load DENUE hotel index for hotel's entidad from local cache/extract only.
 * Does not download zips or call INEGI API.
 */
export async function loadDenueDatasetForHotel(hotel = {}, opts = {}) {
  const root = resolveRoot(opts.root);
  const dataDir = opts.dataDir || path.join(root, DENUE_DATA_DIR);
  const code = resolveDenueEntidadCode(hotel);

  if (!code) {
    return {
      ok: false,
      status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
      reason: "DENUE_ENTIDAD_UNRESOLVED",
      missing_prerequisite: "hotel.state_region|city|market|denue_entidad for saved indexes (03/09/14/23)",
      further_research_could_resolve: true,
      rows: [],
      provenance: {
        adapter: DENUE_CLIENT_VERSION,
        data_dir: dataDir,
        local_only: true,
        network: false,
      },
    };
  }

  const indexPath = path.join(dataDir, `denue_${code}_hotels.json`);
  if (fs.existsSync(indexPath)) {
    const cached = JSON.parse(fs.readFileSync(indexPath, "utf8"));
    const rows = cached.rows || [];
    const stat = fs.statSync(indexPath);
    return {
      ok: true,
      status: REGISTRY_DATASET_STATUS.LOADED,
      rows,
      provenance: {
        adapter: DENUE_CLIENT_VERSION,
        cve_ent: code,
        source_mode: "local_hotels_json_cache",
        source_url: cached.meta?.source_url || DENUE_STATE_CSV_URL(code),
        source_zip: cached.meta?.source_zip || null,
        dataset_retrieved_at: cached.meta?.retrieved_at || null,
        dataset_date: cached.meta?.retrieved_at || null,
        index_path: indexPath,
        file_bytes: stat.size,
        file_mtime: stat.mtime.toISOString(),
        row_count: rows.length,
        hotel_filter: cached.meta?.hotel_filter || null,
        note: cached.meta?.note || null,
        local_only: true,
        network: false,
        hotel_hint: {
          hotel_id: hotel.hotel_id || hotel.census_id || null,
          hotel_name: hotel.hotel_name || hotel.property_name || null,
          city: hotel.city || null,
          state_region: hotel.state_region || hotel.state || null,
          country: hotel.country || null,
        },
      },
    };
  }

  const extractDir = path.join(dataDir, `denue_${code}_extract`);
  if (fs.existsSync(extractDir)) {
    const retrievedAt = new Date().toISOString();
    const rows = await loadDenueHotelRowsFromCsvDir(extractDir, { retrievedAt });
    return {
      ok: true,
      status: REGISTRY_DATASET_STATUS.LOADED,
      rows,
      provenance: {
        adapter: DENUE_CLIENT_VERSION,
        cve_ent: code,
        source_mode: "local_state_csv_extract",
        source_url: DENUE_STATE_CSV_URL(code),
        extract_dir: extractDir,
        dataset_retrieved_at: retrievedAt,
        row_count: rows.length,
        local_only: true,
        network: false,
      },
    };
  }

  return {
    ok: false,
    status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
    reason: "DENUE_LOCAL_INDEX_OR_EXTRACT_MISSING",
    missing_prerequisite: `denue_${code}_hotels.json or denue_${code}_extract under ${dataDir}`,
    further_research_could_resolve: true,
    rows: [],
    provenance: {
      adapter: DENUE_CLIENT_VERSION,
      cve_ent: code,
      expected_index: indexPath,
      expected_extract: extractDir,
      source_url: DENUE_STATE_CSV_URL(code),
      local_only: true,
      network: false,
      note: "Won't download INEGI zip from default loader (local-only policy)",
    },
  };
}

/**
 * Default deps for opt-in router — real loaders, still overridable.
 */
export function createDefaultRegistryDatasetDeps(opts = {}) {
  return {
    fetchCadasturRows: async (hotel) => {
      const loaded = await loadCadasturDatasetForHotel(hotel, opts);
      return loaded;
    },
    fetchDenueRows: async (hotel) => {
      const loaded = await loadDenueDatasetForHotel(hotel, opts);
      return loaded;
    },
  };
}
