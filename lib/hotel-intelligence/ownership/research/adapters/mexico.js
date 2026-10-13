/**
 * Mexico authoritative ownership lane (P1A).
 *
 * Reality check (investigation):
 * - SECTUR RNT = interactive consulta; bulk automation gated / often unavailable.
 * - SIGER/RPC = manual browser workflow; no bulk API in-repo.
 * - CoStar True Owner = FORBIDDEN as graph evidence.
 *
 * This adapter:
 * 1) Builds structured RNT/SIGER research plans (documented attempts)
 * 2) Optionally uses MX_RNT_SEARCH_URL when enabled
 * 3) Emits entity-resolution candidates only when a defensible razón social is obtained
 * 4) Never copies CoStar into ownership evidence
 */

import {
  buildMxRntSearchPlan,
  inferMxRntState,
  fetchMxRntHospedajeSearch,
} from "../../../../gtm-owner-target/adapters/mx-rnt-hospedaje.js";
import { buildMxSigerSearchPlan } from "../../../../gtm-owner-target/adapters/mx-siger-registry.js";
import { lookupMexicoRntViaBrowser } from "../mx-browser-lane.js";
import { lookupMexicoCompanyIdentity } from "../mexico-company-identity.js";
import { createOwnershipEvidenceCandidate } from "../evidence-provider.js";
import { FIELD_SEMANTICS } from "../source-semantics.js";

export const MEXICO_OWNERSHIP_ADAPTER_VERSION = "ownership-adapter-mexico-v1";

/**
 * @param {object} hotel
 * @param {{ env?: object }} [ctx]
 */
export async function lookupMexicoOwnership(hotel, ctx = {}) {
  const started = Date.now();
  const env = ctx.env || process.env;
  const metrics = {
    provider: "mexico_rnt_siger",
    records_queried: 0,
    hotel_matches: 0,
    entity_candidates: 0,
    relationships_produced: 0,
    latency_ms: 0,
    access_ok: false,
    error: null,
    rnt_enabled: false,
    siger_automated: false,
  };
  const notes = [];
  const candidates = [];

  const name = hotel?.hotel_name || hotel?.name || "";
  const city = hotel?.city || "";
  const state = inferMxRntState(city, hotel?.submarket, hotel?.market);

  try {
    const identity = await lookupMexicoCompanyIdentity(hotel, { env });
    notes.push(...(identity.notes || []));
    if (identity.method_attempt) metrics.company_identity = identity.metrics;
    for (const cand of identity.candidates || []) {
      candidates.push(cand);
      metrics.entity_candidates += 1;
    }
    if (identity.seed) metrics.company_seed = identity.seed;
  } catch (err) {
    notes.push(`mexico_company_identity_exception:${String(err?.message || err).slice(0, 80)}`);
  }

  const queueItem = {
    ownerName: name,
    entitySearchName: name,
    id: hotel?.hotel_id || null,
  };
  const property = {
    buildingName: name,
    city,
    country: "Mexico",
    state,
  };

  const rntPlan = buildMxRntSearchPlan(queueItem, property);
  notes.push("mexico_rnt_search_plan_built");
  let sigerPlan = null;
  try {
    sigerPlan = buildMxSigerSearchPlan(queueItem, property);
    notes.push("mexico_siger_search_plan_built");
  } catch {
    notes.push("mexico_siger_plan_unavailable");
  }

  const rntEnabled =
    String(env.MX_RNT_LOOKUP_ENABLED || "").trim() === "1" &&
    Boolean(String(env.MX_RNT_SEARCH_URL || "").trim());
  metrics.rnt_enabled = rntEnabled;

  if (rntEnabled) {
    try {
      const res = await fetchMxRntHospedajeSearch({
        commercialName: name,
        state,
        city,
      });
      metrics.records_queried = Array.isArray(res?.hits) ? res.hits.length : 0;
      metrics.access_ok = Boolean(res?.ok);
      if (res?.ok && Array.isArray(res.hits)) {
        const sem = FIELD_SEMANTICS["mexico_rnt.razon_social"];
        for (const hit of res.hits.slice(0, 3)) {
          const legal = hit.razonSocial || hit.nombreComercial;
          if (!legal) continue;
          metrics.hotel_matches += 1;
          candidates.push(
            createOwnershipEvidenceCandidate({
              hotel_id: hotel.hotel_id,
              source_provider: "mexico_rnt",
              source_country: "Mexico",
              source_url: hit.verificationUrl,
              source_record_id: hit.certificateNumber || null,
              entity_candidate: {
                legal_name: legal,
                display_name: hit.nombreComercial || legal,
                jurisdiction: "MX",
                identifiers: [],
              },
              supported_relationship: null,
              source_semantics: sem.notes,
              extracted_claim: `Mexico RNT hit: ${legal}`,
              source_authority: 0.88,
              max_verification: "none",
              adapter_notes: ["entity_resolution_only", "rnt_registered_provider"],
            })
          );
          metrics.entity_candidates += 1;
        }
      } else {
        notes.push(`mexico_rnt_fetch_failed:${res?.error || res?.reason || "unknown"}`);
      }
    } catch (err) {
      notes.push(`mexico_rnt_exception:${String(err?.message || err).slice(0, 80)}`);
      metrics.error = String(err?.message || err).slice(0, 80);
    }
  } else {
    notes.push("mexico_rnt_automation_disabled_use_plan");
    notes.push("mexico_siger_requires_browser_interaction");

    const browserEnabled =
      String(env.OWNERSHIP_MX_BROWSER_ENABLE || "0").trim() === "1";
    if (browserEnabled) {
      try {
        const browserRes = await lookupMexicoRntViaBrowser(hotel, env);
        notes.push(...(browserRes.notes || []));
        if (browserRes.probe) {
          metrics.browser_probe_ms = browserRes.probe.time_ms;
          metrics.browser_reliability = browserRes.probe.automation_reliability;
          metrics.access_ok = browserRes.probe.ok || metrics.access_ok;
        }
        for (const cand of browserRes.candidates || []) {
          metrics.entity_candidates += 1;
          metrics.hotel_matches += 1;
          candidates.push(
            createOwnershipEvidenceCandidate({
              hotel_id: hotel.hotel_id,
              source_provider: cand.source_provider || "mx_rnt_browser",
              source_country: "Mexico",
              source_url: cand.source_url,
              entity_candidate: cand.entity_candidate,
              supported_relationship: null,
              source_semantics: cand.source_semantics,
              extracted_claim: cand.extracted_claim,
              source_authority: cand.source_authority || 0.75,
              max_verification: "needs_review",
              adapter_notes: ["entity_resolution_only", "mx_browser_lane"],
            })
          );
        }
        if (browserRes.probe?.captcha_detected) {
          notes.push("mx_siger_rpc_not_automated:captcha_or_bot_protection");
        }
      } catch (err) {
        notes.push(`mx_browser_lane_exception:${String(err?.message || err).slice(0, 80)}`);
      }
    }
  }

  metrics.latency_ms = Date.now() - started;
  return {
    ok: true,
    candidates,
    metrics,
    notes,
    plans: {
      rnt: rntPlan,
      siger: sigerPlan,
    },
    failure_type: candidates.length
      ? null
      : rntEnabled
        ? "no_public_ownership_evidence"
        : "browser_interaction_required",
  };
}
