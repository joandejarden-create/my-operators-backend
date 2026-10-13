/**
 * Peru MINCETUR ownership evidence lane (P1C).
 * RUC + razón social = registered lodging entity. REP_LEGAL ≠ owner.
 */

import {
  runPeruMinceturDryRun,
  MAP_PERU_MINCETUR,
} from "../../../../research-engine-v2/peru-mincetur-open-data-adapter.js";
import { createOwnershipEvidenceCandidate } from "../evidence-provider.js";
import { FIELD_SEMANTICS } from "../source-semantics.js";
import { scoreHotelRegistryMatch } from "./match-utils.js";

export const PERU_OWNERSHIP_ADAPTER_VERSION = "ownership-adapter-peru-mincetur-v1";

let _cache = null;

export async function lookupPeruMinceturOwnership(hotel, ctx = {}) {
  const started = Date.now();
  const metrics = {
    provider: "peru_mincetur",
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
      _cache = await runPeruMinceturDryRun({
        hotelsOnly: true,
        maxRows: 15000,
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
      notes: [`peru_exception:${metrics.error}`],
      failure_type: "adapter_failed",
    };
  }

  if (!_cache?.ok) {
    metrics.error = _cache?.message || "peru_unavailable";
    metrics.latency_ms = Date.now() - started;
    return {
      ok: false,
      candidates: [],
      metrics,
      notes: [`peru_load_failed:${metrics.error}`],
      failure_type: "source_inaccessible",
    };
  }

  const rows = _cache.candidates || [];
  metrics.records_queried = rows.length;
  metrics.access_ok = true;

  const scored = rows
    .map((c) => {
      const regName =
        c.fields?.[MAP_PERU_MINCETUR.propertyName] ||
        c.ownership_signal?.commercial_name ||
        c.ownership_signal?.legal_name_from_registry ||
        "";
      const regCity = c.fields?.[MAP_PERU_MINCETUR.city] || "";
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
  const sem = FIELD_SEMANTICS["peru_mincetur.razon_social"];

  for (const { c, score } of scored) {
    const legal =
      c.ownership_signal?.legal_name_from_registry ||
      c.ownership_signal?.commercial_name ||
      "";
    const ruc = String(c.ownership_signal?.tax_id || "").replace(/\D/g, "");
    if (!legal) continue;
    if (c.ownership_signal?.legal_representative) {
      notes.push("peru_rep_legal_ignored_as_owner");
    }
    candidates.push(
      createOwnershipEvidenceCandidate({
        hotel_id: hotel.hotel_id,
        source_provider: "peru_mincetur",
        source_country: "Peru",
        source_url:
          c.ownership_signal?.source_url || MAP_PERU_MINCETUR.sourceDatasetUrl,
        source_record_id: c.identity_key || null,
        entity_candidate: {
          legal_name: legal,
          jurisdiction: "PE",
          identifiers: ruc
            ? [{ kind: "ruc", value: ruc, country: "Peru" }]
            : [],
        },
        supported_relationship: null,
        source_semantics: sem.notes,
        extracted_claim: `Peru MINCETUR match (score=${score.toFixed(2)}): ${legal}${ruc ? ` RUC ${ruc}` : ""}`,
        source_authority: 0.9,
        max_verification: "none",
        adapter_notes: ["entity_resolution_only", `match_score:${score.toFixed(2)}`],
      })
    );
    metrics.entity_candidates += 1;
  }

  metrics.latency_ms = Date.now() - started;
  return {
    ok: true,
    candidates,
    metrics,
    notes: notes.length ? notes : ["peru_no_confident_match"],
    failure_type: candidates.length
      ? null
      : "government_source_lacks_ownership_semantics",
  };
}
