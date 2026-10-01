/**
 * Brazil Cadastur ownership evidence lane (P1B).
 * Emits registered lodging PJ + CNPJ for entity resolution — NOT automatic OWNED_BY.
 */

import {
  fetchBrazilCadasturLodgingRows,
  MAP_BRAZIL_CADASTUR,
} from "../../../../research-engine-v2/brazil-cadastur-open-data-adapter.js";
import { createOwnershipEvidenceCandidate } from "../evidence-provider.js";
import { FIELD_SEMANTICS } from "../source-semantics.js";
import {
  normalizeMatchText,
  scoreHotelRegistryMatchCityGated,
} from "./match-utils.js";

export const BRAZIL_OWNERSHIP_ADAPTER_VERSION = "ownership-adapter-brazil-cadastur-v2";

let _cache = null;
let _index = null;

/**
 * @param {object[]} rows
 */
export function indexCadasturRowsByCity(rows = []) {
  const byCity = new Map();
  const byUf = new Map();
  for (const row of rows) {
    const cityK = normalizeMatchText(row.city);
    if (cityK) {
      if (!byCity.has(cityK)) byCity.set(cityK, []);
      byCity.get(cityK).push(row);
    }
    const ufK = String(row.state_uf || "").trim().toUpperCase();
    if (ufK) {
      if (!byUf.has(ufK)) byUf.set(ufK, []);
      byUf.get(ufK).push(row);
    }
  }
  return { byCity, byUf, row_count: rows.length };
}

/**
 * City-first Cadastur match. Does not scan the full national file per hotel.
 * @param {object} hotel
 * @param {object[]} rows
 * @param {{ index?: object, threshold?: number }} [opts]
 */
export function matchCadasturRowsForHotel(hotel, rows, opts = {}) {
  const name = hotel?.hotel_name || hotel?.name || "";
  const city = hotel?.city || "";
  const threshold = opts.threshold ?? 0.72;
  if (!name) {
    return { matches: [], notes: ["missing_hotel_name"], pool_size: 0 };
  }
  const cityK = normalizeMatchText(city);
  if (!cityK) {
    return { matches: [], notes: ["missing_hotel_city"], pool_size: 0 };
  }
  const index = opts.index || indexCadasturRowsByCity(rows);
  let pool = index.byCity.get(cityK) || [];
  let notes = [];
  if (!pool.length) {
    notes.push("cadastur_city_index_empty");
    const uf = String(hotel?.state_uf || hotel?.state_region || "")
      .trim()
      .toUpperCase()
      .slice(0, 2);
    if (uf && index.byUf.has(uf)) {
      pool = index.byUf.get(uf);
      notes.push(`cadastur_uf_fallback:${uf}`);
    }
  }
  const scored = [];
  for (const row of pool) {
    const gated = [
      scoreHotelRegistryMatchCityGated(name, row.property_name, city, row.city, {
        threshold,
      }),
      scoreHotelRegistryMatchCityGated(
        name,
        row.commercial_name || "",
        city,
        row.city,
        { threshold }
      ),
      scoreHotelRegistryMatchCityGated(
        name,
        row.legal_name || "",
        city,
        row.city,
        { threshold }
      ),
    ].sort((a, b) => b.score - a.score)[0];
    if (!gated || !gated.eligible) continue;
    scored.push({ row, score: gated.score, city_ok: gated.city_ok });
  }
  scored.sort((a, b) => b.score - a.score);
  return {
    matches: scored.slice(0, 3),
    notes,
    pool_size: pool.length,
  };
}

async function loadRows(opts = {}) {
  if (_cache?.ok && !opts.forceReload) return _cache;
  _cache = await fetchBrazilCadasturLodgingRows({
    hotelsOnly: true,
    ...opts,
  });
  _index = _cache?.ok ? indexCadasturRowsByCity(_cache.rows || []) : null;
  return _cache;
}

/**
 * @param {object} hotel
 * @param {{ env?: object }} [ctx]
 */
export async function lookupBrazilCadasturOwnership(hotel, ctx = {}) {
  const started = Date.now();
  const metrics = {
    provider: "brazil_cadastur",
    records_queried: 0,
    hotel_matches: 0,
    entity_candidates: 0,
    relationships_produced: 0,
    latency_ms: 0,
    access_ok: false,
    error: null,
  };

  const name = hotel?.hotel_name || hotel?.name || "";
  const city = hotel?.city || "";
  if (!name) {
    metrics.latency_ms = Date.now() - started;
    return { ok: false, candidates: [], metrics, notes: ["missing_hotel_name"] };
  }

  let fetched;
  try {
    fetched = await loadRows({ env: ctx.env });
  } catch (err) {
    metrics.error = String(err?.message || err).slice(0, 120);
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`cadastur_exception:${metrics.error}`],
      failure_type: "adapter_failed",
    };
  }

  if (!fetched?.ok) {
    metrics.error = fetched?.message || fetched?.error_kind || "cadastur_unavailable";
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`cadastur_load_failed:${metrics.error}`],
      failure_type: "source_inaccessible",
    };
  }

  const normalized = fetched.rows || [];
  metrics.records_queried = normalized.length;
  metrics.access_ok = true;

  const notes = [];
  const matched = matchCadasturRowsForHotel(
    { hotel_name: name, city, state_region: hotel?.state_region },
    normalized,
    { index: _index || indexCadasturRowsByCity(normalized) }
  );
  const scored = matched.matches;
  notes.push(...(matched.notes || []));

  metrics.hotel_matches = scored.length;
  metrics.city_pool_size = matched.pool_size;
  const candidates = [];

  for (const { row, score } of scored) {
    const legal = row.legal_name || row.property_name || row.commercial_name || "";
    if (!legal) continue;
    const cnpj = String(row.cnpj || "").replace(/\D/g, "");
    const semName = FIELD_SEMANTICS["brazil_cadastur.nome_pessoa_juridica"];
    candidates.push(
      createOwnershipEvidenceCandidate({
        hotel_id: hotel.hotel_id,
        source_provider: "brazil_cadastur",
        source_country: "Brazil",
        source_url: row.source_url || MAP_BRAZIL_CADASTUR.sourceDatasetUrl,
        source_record_id: row.identity_key || cnpj || null,
        entity_candidate: {
          legal_name: legal,
          display_name: row.commercial_name || legal,
          jurisdiction: "BR",
          identifiers: cnpj
            ? [{ kind: "cnpj", value: cnpj, country: "Brazil" }]
            : [],
        },
        supported_relationship: null,
        source_semantics: semName.notes,
        extracted_claim: `Cadastur match (score=${score.toFixed(2)}): ${legal}${cnpj ? ` CNPJ ${cnpj}` : ""}`,
        source_authority: 0.9,
        max_verification: "none",
        adapter_notes: [
          "entity_resolution_only",
          `match_score:${score.toFixed(2)}`,
          "not_automatic_owned_by",
        ],
      })
    );
    metrics.entity_candidates += 1;
    notes.push(`cadastur_match:${legal.slice(0, 60)}`);
  }

  if (!candidates.length) notes.push("cadastur_no_confident_match");

  metrics.latency_ms = Date.now() - started;
  return {
    ok: true,
    candidates,
    metrics,
    notes,
    failure_type: candidates.length ? null : "company_identifier_unavailable",
  };
}

export function buildBrazilCadasturOwnershipSignal(row) {
  return {
    country: "Brazil",
    tax_id_type: "CNPJ",
    tax_id: String(row?.cnpj || "").replace(/\D/g, "") || null,
    legal_name_from_registry: row?.legal_name || null,
    commercial_name: row?.commercial_name || null,
    source_url: row?.source_url || MAP_BRAZIL_CADASTUR.sourceDatasetUrl,
    lane: "ownership_enrichment_blocked",
    note: "Cadastur PJ/CNPJ = registered lodging entity — not Census Owner Name / not auto PropCo",
  };
}
