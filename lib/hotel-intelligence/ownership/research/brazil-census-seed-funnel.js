/**
 * Brazil Census → Cadastur → CNPJ seed funnel (P1.7A).
 * Read-only measurement. No Census ownership writes.
 */

import { MAP_CENSUS_FIELDS } from "../../map_hotel_intelligence_fields.js";
import {
  indexCadasturRowsByCity,
  matchCadasturRowsForHotel,
} from "./adapters/brazil-cadastur.js";
import { companySeedFromRegistryRow } from "../company-seed.js";
import { normalizeCnpjDigits, matrizCnpjFromAny } from "./brazil-cnpj-client.js";

export const BRAZIL_SEED_FUNNEL_VERSION = "ownership-brazil-seed-funnel-v1";

function isBrazil(country) {
  return /brazil|brasil/i.test(String(country || ""));
}

/**
 * @param {object} rec — Airtable census record { id, fields }
 */
export function censusRecordToFunnelHotel(rec) {
  const f = rec?.fields || rec || {};
  return {
    hotel_id: rec.hotel_id || null,
    census_record_id: rec.id || null,
    name: f[MAP_CENSUS_FIELDS.officialName] || f[MAP_CENSUS_FIELDS.propertyName] || f.name,
    hotel_name: f[MAP_CENSUS_FIELDS.officialName] || f[MAP_CENSUS_FIELDS.propertyName] || f.name,
    city: f[MAP_CENSUS_FIELDS.city] || f.city || null,
    state_region: f[MAP_CENSUS_FIELDS.stateRegion] || null,
    country: f[MAP_CENSUS_FIELDS.country] || f.country || "Brazil",
    address: f[MAP_CENSUS_FIELDS.address] || null,
    website: f[MAP_CENSUS_FIELDS.website] || null,
    brand: f[MAP_CENSUS_FIELDS.brandName] || null,
  };
}

/**
 * Identity funnel against Cadastur (local, no CNPJ API).
 * @param {object[]} censusHotels
 * @param {object[]} cadasturRows
 */
export function measureBrazilCadasturIdentityFunnel(censusHotels = [], cadasturRows = []) {
  const index = indexCadasturRowsByCity(cadasturRows);
  const n = censusHotels.length;
  const rows = [];
  let cadasturMatch = 0;
  let companyName = 0;
  let cnpjPresent = 0;
  const dropReasons = {
    missing_name: 0,
    missing_city: 0,
    city_index_empty: 0,
    name_below_threshold: 0,
    match_no_company: 0,
    match_no_cnpj: 0,
  };

  for (const hotel of censusHotels) {
    const name = hotel.hotel_name || hotel.name;
    if (!name) {
      dropReasons.missing_name += 1;
      rows.push({ hotel, drop: "missing_name" });
      continue;
    }
    const matched = matchCadasturRowsForHotel(hotel, cadasturRows, { index });
    if (matched.notes?.includes("missing_hotel_city")) {
      dropReasons.missing_city += 1;
      rows.push({ hotel, drop: "missing_city", pool_size: 0 });
      continue;
    }
    if (!matched.matches.length) {
      if (matched.notes?.includes("cadastur_city_index_empty") && matched.pool_size === 0) {
        dropReasons.city_index_empty += 1;
        rows.push({ hotel, drop: "city_index_empty", pool_size: 0 });
      } else {
        dropReasons.name_below_threshold += 1;
        rows.push({
          hotel,
          drop: "name_below_threshold",
          pool_size: matched.pool_size,
        });
      }
      continue;
    }
    cadasturMatch += 1;
    const top = matched.matches[0];
    const legal = top.row.legal_name || top.row.property_name || top.row.commercial_name;
    const cnpj = normalizeCnpjDigits(top.row.cnpj);
    if (!legal) {
      dropReasons.match_no_company += 1;
      rows.push({ hotel, drop: "match_no_company", cadastur: top.row, score: top.score });
      continue;
    }
    companyName += 1;
    const seed = companySeedFromRegistryRow(hotel, top.row, null, {
      source: "brazil_cadastur",
      discovery_method: "tourism_registry_identity",
      source_semantics:
        "Cadastur registered lodging PJ — identity only, not PropCo OWNED_BY",
    });
    if (!cnpj) {
      dropReasons.match_no_cnpj += 1;
      rows.push({
        hotel,
        drop: "match_no_cnpj",
        seed,
        score: top.score,
      });
      continue;
    }
    cnpjPresent += 1;
    rows.push({
      hotel,
      drop: null,
      seed,
      score: top.score,
      cnpj,
      legal,
    });
  }

  const pct = (count) => (n ? Math.round((count / n) * 1000) / 10 : 0);
  const largestDrop = Object.entries(dropReasons).sort((a, b) => b[1] - a[1])[0];

  return {
    version: BRAZIL_SEED_FUNNEL_VERSION,
    n,
    funnel: {
      brazil_census_hotels: n,
      cadastur_match: cadasturMatch,
      cadastur_match_pct: pct(cadasturMatch),
      company_name: companyName,
      company_name_pct: pct(companyName),
      cnpj: cnpjPresent,
      cnpj_pct: pct(cnpjPresent),
    },
    drop_reasons: dropReasons,
    largest_drop: largestDrop
      ? { reason: largestDrop[0], count: largestDrop[1], pct: pct(largestDrop[1]) }
      : null,
    cadastur_index: {
      cities: index.byCity.size,
      rows: index.row_count,
    },
    rows,
  };
}

/**
 * Enrich identity-funnel rows that have CNPJ with registry validation / matrix / QSA.
 * Caps live lookups. Mutates copies; does not write Census.
 * @param {object} identityFunnel
 * @param {{ fetchCnpj?: Function, maxLookups?: number }} opts
 */
export async function enrichBrazilSeedFunnelWithRegistry(identityFunnel, opts = {}) {
  const fetchCnpj = opts.fetchCnpj;
  const maxLookups = Number(opts.maxLookups || 80);
  if (typeof fetchCnpj !== "function") {
    return {
      ...identityFunnel,
      registry_enrichment: { skipped: true, reason: "no_fetch_fn" },
    };
  }

  let lookups = 0;
  let validated = 0;
  let probable = 0;
  let matrixResolved = 0;
  let qsaAvailable = 0;
  const enrichedRows = [];

  for (const row of identityFunnel.rows || []) {
    if (!row.cnpj || lookups >= maxLookups) {
      enrichedRows.push(row);
      continue;
    }
    lookups += 1;
    const res = await fetchCnpj(row.cnpj);
    if (!res?.ok || !res.record) {
      enrichedRows.push({ ...row, registry_ok: false });
      continue;
    }
    const rec = res.record;
    const { validateCnpjSeed } = await import("./brazil-cnpj-seed-validation.js");
    const validation = validateCnpjSeed(
      row.hotel,
      {
        legal_name: rec.razao_social,
        cnpj: row.cnpj,
        seed_source: "tourism_registry_identity",
      },
      rec
    );
    const seed = companySeedFromRegistryRow(row.hotel, {
      ...row.seed?.entity_candidate,
      legal_name: rec.razao_social,
      city: rec.municipio,
      cnpj: row.cnpj,
    }, validation, {
      source: "brazil_cadastur",
      discovery_method: "cnpj_validation",
    });
    if (validation.status === "VALIDATED") validated += 1;
    if (validation.status === "PROBABLE") probable += 1;

    const matriz = matrizCnpjFromAny(row.cnpj);
    const isBranch = matriz && matriz !== row.cnpj;
    if (!isBranch || (res.matrix_record || rec.descricao_identificador_matriz_filial)) {
      matrixResolved += 1;
    }
    const qsa = rec.qsa || rec.qsa_partners || [];
    if (Array.isArray(qsa) && qsa.length) qsaAvailable += 1;

    enrichedRows.push({
      ...row,
      seed,
      validation,
      registry_ok: true,
      is_branch: Boolean(isBranch),
      matrix_cnpj: matriz,
      qsa_count: Array.isArray(qsa) ? qsa.length : 0,
    });
  }

  const n = identityFunnel.n || 1;
  const pct = (count) => Math.round((count / n) * 1000) / 10;
  const lookupPct = (count) =>
    lookups ? Math.round((count / lookups) * 1000) / 10 : 0;

  return {
    ...identityFunnel,
    rows: enrichedRows,
    funnel: {
      ...identityFunnel.funnel,
      validated_cnpj: validated,
      validated_cnpj_pct: lookupPct(validated),
      probable_cnpj: probable,
      matrix_resolved: matrixResolved,
      matrix_resolved_pct: lookupPct(matrixResolved),
      qsa_available: qsaAvailable,
      qsa_available_pct: lookupPct(qsaAvailable),
      registry_lookups: lookups,
    },
    registry_enrichment: {
      lookups,
      max_lookups: maxLookups,
      validated,
      probable,
      matrix_resolved: matrixResolved,
      qsa_available: qsaAvailable,
    },
  };
}

export { isBrazil };
