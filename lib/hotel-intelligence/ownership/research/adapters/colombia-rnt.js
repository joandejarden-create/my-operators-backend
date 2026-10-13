/**
 * Colombia RNT ownership evidence lane (P1C).
 * NIT + razón social = registered lodging entity — NOT PropCo OWNED_BY.
 */

import {
  fetchColombiaRntLodgingRows,
  mapColombiaRntRowToCensusCandidate,
  MAP_COLOMBIA_RNT,
} from "../../../../research-engine-v2/colombia-rnt-open-data-adapter.js";
import { createOwnershipEvidenceCandidate } from "../evidence-provider.js";
import { FIELD_SEMANTICS } from "../source-semantics.js";
import { scoreHotelRegistryMatch } from "./match-utils.js";

export const COLOMBIA_OWNERSHIP_ADAPTER_VERSION = "ownership-adapter-colombia-rnt-v1";

let _cache = null;

/**
 * @param {object} hotel
 * @param {{ env?: object }} [ctx]
 */
export async function lookupColombiaRntOwnership(hotel, ctx = {}) {
  const started = Date.now();
  const metrics = {
    provider: "colombia_rnt",
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
    return { ok: false, candidates: [], metrics, notes: ["missing_hotel_name"] };
  }

  try {
    if (!_cache?.ok) {
      const raw = await fetchColombiaRntLodgingRows({
        activeOnly: true,
        maxRows: 8000,
        pageSize: 2000,
        env: ctx.env,
      });
      if (!raw.ok) {
        _cache = raw;
      } else {
        _cache = {
          ok: true,
          candidates: (raw.rows || []).map((row) =>
            mapColombiaRntRowToCensusCandidate(row, { dryRun: true })
          ),
        };
      }
    }
  } catch (err) {
    metrics.error = String(err?.message || err).slice(0, 120);
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`colombia_rnt_exception:${metrics.error}`],
      failure_type: "adapter_failed",
    };
  }

  if (!_cache?.ok) {
    metrics.error = _cache?.message || "colombia_rnt_unavailable";
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`colombia_rnt_load_failed:${metrics.error}`],
      failure_type: "source_inaccessible",
    };
  }

  const rows = _cache.candidates || [];
  metrics.records_queried = rows.length;
  metrics.access_ok = true;

  const scored = rows
    .map((c) => {
      const regName =
        c.fields?.[MAP_COLOMBIA_RNT.propertyName] ||
        c.ownership_signal?.legal_name_from_registry ||
        "";
      const regCity = c.fields?.[MAP_COLOMBIA_RNT.city] || "";
      return {
        c,
        score: scoreHotelRegistryMatch(name, regName, city, regCity),
        regName,
      };
    })
    .filter((x) => x.score >= 0.72)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3);

  metrics.hotel_matches = scored.length;
  const candidates = [];
  const notes = [];
  const sem = FIELD_SEMANTICS["colombia_rnt.razon_social_establecimiento"];

  for (const { c, score, regName } of scored) {
    const legal =
      c.ownership_signal?.legal_name_from_registry || regName || "";
    const nit = String(c.ownership_signal?.tax_id || "").replace(/\D/g, "");
    if (!legal) continue;
    candidates.push(
      createOwnershipEvidenceCandidate({
        hotel_id: hotel.hotel_id,
        source_provider: "colombia_rnt",
        source_country: "Colombia",
        source_url:
          c.ownership_signal?.source_url || MAP_COLOMBIA_RNT.sourceDatasetUrl,
        source_record_id: c.codigo_rnt || c.identity_key || null,
        entity_candidate: {
          legal_name: legal,
          jurisdiction: "CO",
          identifiers: nit
            ? [{ kind: "nit", value: nit, country: "Colombia" }]
            : [],
        },
        supported_relationship: null,
        source_semantics: sem.notes,
        extracted_claim: `Colombia RNT match (score=${score.toFixed(2)}): ${legal}${nit ? ` NIT ${nit}` : ""}`,
        source_authority: 0.92,
        max_verification: "none",
        adapter_notes: ["entity_resolution_only", `match_score:${score.toFixed(2)}`],
      })
    );
    metrics.entity_candidates += 1;
    notes.push(`colombia_rnt_match:${legal.slice(0, 60)}`);
  }

  metrics.latency_ms = Date.now() - started;
  return {
    ok: true,
    candidates,
    metrics,
    notes: notes.length ? notes : ["colombia_rnt_no_confident_match"],
    failure_type: candidates.length
      ? null
      : "government_source_lacks_ownership_semantics",
  };
}
