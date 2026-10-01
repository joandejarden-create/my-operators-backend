/**
 * Guatemala INGUAT ownership evidence lane (P1C).
 * propietario field → weak OWNED_BY (needs_review max) when present.
 */

import {
  fetchGuatemalaInguatLodgingRows,
  MAP_GUATEMALA_INGUAT,
} from "../../../../research-engine-v2/guatemala-inguat-open-data-adapter.js";
import { createOwnershipEvidenceCandidate } from "../evidence-provider.js";
import { FIELD_SEMANTICS } from "../source-semantics.js";
import { scoreHotelRegistryMatch } from "./match-utils.js";
import { isPlausibleLegalEntityName } from "../entity-name-guard.js";

export const GUATEMALA_OWNERSHIP_ADAPTER_VERSION =
  "ownership-adapter-guatemala-inguat-v1";

let _cache = null;

export async function lookupGuatemalaInguatOwnership(hotel, ctx = {}) {
  const started = Date.now();
  const metrics = {
    provider: "guatemala_inguat",
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
      _cache = await fetchGuatemalaInguatLodgingRows({
        hotelsOnly: true,
        env: ctx.env,
      });
    }
  } catch (err) {
    metrics.error = String(err?.message || err).slice(0, 120);
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`inguat_exception:${metrics.error}`],
      failure_type: "adapter_failed",
    };
  }

  if (!_cache?.ok) {
    metrics.error = _cache?.message || _cache?.error_kind || "inguat_unavailable";
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`inguat_load_failed:${metrics.error}`],
      failure_type:
        _cache?.error_kind === "missing_local_xlsx"
          ? "source_inaccessible"
          : "adapter_failed",
    };
  }

  const rows = _cache.rows || [];
  metrics.records_queried = rows.length;
  metrics.access_ok = true;

  const scored = rows
    .map((row) => ({
      row,
      score: scoreHotelRegistryMatch(
        name,
        row.property_name,
        city,
        row.city || ""
      ),
    }))
    .filter((x) => x.score >= 0.72)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  metrics.hotel_matches = scored.length;
  const candidates = [];
  const notes = [];
  const sem = FIELD_SEMANTICS["guatemala_inguat.propietario"];

  for (const { row, score } of scored) {
    const ownerRaw = String(row.ownership_signal?.raw || "").trim();
    if (!ownerRaw) {
      notes.push(`inguat_match_no_propietario:${row.property_name}`);
      // Still emit registered establishment name for entity resolution only
      if (row.property_name) {
        candidates.push(
          createOwnershipEvidenceCandidate({
            hotel_id: hotel.hotel_id,
            source_provider: "guatemala_inguat",
            source_country: "Guatemala",
            source_url: row.source_url || MAP_GUATEMALA_INGUAT.sourceDatasetUrl,
            source_record_id: row.identity_key || row.registry_code || null,
            entity_candidate: {
              legal_name: row.property_name,
              jurisdiction: "GT",
              identifiers: [],
            },
            supported_relationship: null,
            source_semantics:
              "INGUAT establishment listing without propietario column — identity only",
            extracted_claim: `INGUAT match (score=${score.toFixed(2)}): ${row.property_name}`,
            source_authority: 0.85,
            max_verification: "none",
            adapter_notes: ["entity_resolution_only"],
          })
        );
        metrics.entity_candidates += 1;
      }
      continue;
    }
    if (!isPlausibleLegalEntityName(ownerRaw)) {
      notes.push(`inguat_propietario_implausible:${ownerRaw.slice(0, 40)}`);
      continue;
    }
    candidates.push(
      createOwnershipEvidenceCandidate({
        hotel_id: hotel.hotel_id,
        source_provider: "guatemala_inguat",
        source_country: "Guatemala",
        source_url: row.source_url || MAP_GUATEMALA_INGUAT.sourceDatasetUrl,
        source_record_id: row.identity_key || row.registry_code || null,
        entity_candidate: {
          legal_name: ownerRaw,
          jurisdiction: "GT",
          identifiers: [],
        },
        supported_relationship: "OWNED_BY",
        source_semantics: sem.notes,
        extracted_claim: `INGUAT propietario (score=${score.toFixed(2)}): ${ownerRaw}`,
        source_authority: 0.8,
        max_verification: "needs_review",
        adapter_notes: [`match_score:${score.toFixed(2)}`, "weak_propietario_claim"],
      })
    );
    metrics.entity_candidates += 1;
    metrics.relationships_produced += 1;
  }

  metrics.latency_ms = Date.now() - started;
  return {
    ok: true,
    candidates,
    metrics,
    notes: notes.length ? notes : ["inguat_no_confident_match"],
    failure_type: candidates.length ? null : "no_public_ownership_evidence",
  };
}
