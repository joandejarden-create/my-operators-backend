/**
 * Ecuador MINTUR ownership evidence lane (P1C).
 * Nombre comercial = identity only — no OWNED_BY.
 */

import {
  fetchEcuadorMinturLodgingRows,
} from "../../../../research-engine-v2/ecuador-mintur-catastro-adapter.js";
import { createOwnershipEvidenceCandidate } from "../evidence-provider.js";
import { FIELD_SEMANTICS } from "../source-semantics.js";
import { scoreHotelRegistryMatch } from "./match-utils.js";

export const ECUADOR_OWNERSHIP_ADAPTER_VERSION =
  "ownership-adapter-ecuador-mintur-v1";

let _cache = null;

export async function lookupEcuadorMinturOwnership(hotel, ctx = {}) {
  const started = Date.now();
  const metrics = {
    provider: "ecuador_mintur",
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

  try {
    if (!_cache) {
      _cache = await fetchEcuadorMinturLodgingRows({ env: ctx.env });
    }
  } catch (err) {
    metrics.error = String(err?.message || err).slice(0, 120);
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`ecuador_exception:${metrics.error}`],
      failure_type: "adapter_failed",
    };
  }

  if (!_cache?.ok) {
    metrics.error = _cache?.message || _cache?.error_kind || "ecuador_unavailable";
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`ecuador_load_failed:${metrics.error}`],
      failure_type: "source_inaccessible",
    };
  }

  const rows = _cache.rows || _cache.normalized || [];
  metrics.records_queried = rows.length;
  metrics.access_ok = true;
  const sem = FIELD_SEMANTICS["ecuador_mintur.nombre_comercial"];

  const scored = rows
    .map((row) => {
      const regName = row.property_name || row.nombre_comercial || row.commercial_name || "";
      const regCity = row.city || row.canton || "";
      return {
        row,
        score: scoreHotelRegistryMatch(name, regName, city, regCity),
        regName,
      };
    })
    .filter((x) => x.score >= 0.72)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  metrics.hotel_matches = scored.length;
  const candidates = [];
  for (const { row, score, regName } of scored) {
    if (!regName) continue;
    candidates.push(
      createOwnershipEvidenceCandidate({
        hotel_id: hotel.hotel_id,
        source_provider: "ecuador_mintur",
        source_country: "Ecuador",
        source_url: row.source_url || null,
        source_record_id: row.identity_key || null,
        entity_candidate: {
          legal_name: regName,
          jurisdiction: "EC",
          identifiers: [],
        },
        supported_relationship: null,
        source_semantics: sem.notes,
        extracted_claim: `Ecuador MINTUR match (score=${score.toFixed(2)}): ${regName}`,
        source_authority: 0.85,
        max_verification: "none",
        adapter_notes: ["entity_resolution_only"],
      })
    );
    metrics.entity_candidates += 1;
  }

  metrics.latency_ms = Date.now() - started;
  return {
    ok: true,
    candidates,
    metrics,
    notes: candidates.length ? [] : ["ecuador_no_confident_match"],
    failure_type: candidates.length
      ? null
      : "government_source_lacks_ownership_semantics",
  };
}
