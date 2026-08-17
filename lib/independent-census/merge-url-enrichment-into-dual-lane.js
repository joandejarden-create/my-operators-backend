/**
 * Merge known-chain / Google Places Official URL proposals into dual-lane
 * intake payloads (report-only patch; no Airtable writes).
 *
 * Default: high-confidence brand-domain proposals only.
 */

import {
  sanitizeOfficialUrlCandidate,
} from "./known-chain-official-url-enrichment.js";
import { isDeniedWebsite } from "./intake-autopilot-gates.js";

export const URL_ENRICHMENT_MERGE_VERSION = "census-intake-url-enrichment-merge-v1";

export const DEFAULT_MERGE_POLICY = Object.freeze({
  require_high_confidence: true,
  require_brand_domain: true,
  require_apply_candidate: true,
  overwrite_existing_official_url: false,
});

/**
 * @param {object} enrichment — row from known-chain enrichment report
 * @param {object} [policy]
 */
export function isMergeEligibleEnrichment(enrichment, policy = DEFAULT_MERGE_POLICY) {
  if (!enrichment) return false;
  const cityOnly =
    enrichment.proposal_source === "catalog_city_only" &&
    String(enrichment.proposed_city || "").trim();
  if (cityOnly) return true;
  if (policy.require_apply_candidate && !enrichment.apply_as_official_url_candidate) {
    return false;
  }
  const url = sanitizeOfficialUrlCandidate(
    enrichment.proposed_official_property_url || ""
  );
  if (!url || isDeniedWebsite(url)) return false;
  if (policy.require_high_confidence && enrichment.proposal_confidence !== "high") {
    return false;
  }
  if (
    policy.require_brand_domain &&
    enrichment.proposal_source !== "google_places_website_on_brand_domain" &&
    enrichment.proposal_source !== "catalog_official_url"
  ) {
    return false;
  }
  return true;
}

/**
 * Apply eligible URL proposals onto dual-lane payloads.
 * @param {object} dualLanePlan
 * @param {object[]} enrichments
 * @param {object} [opts]
 */
export function mergeUrlEnrichmentIntoDualLane(dualLanePlan, enrichments, opts = {}) {
  const policy = { ...DEFAULT_MERGE_POLICY, ...(opts.policy || {}) };
  const byId = new Map();
  for (const e of enrichments || []) {
    if (!isMergeEligibleEnrichment(e, policy)) continue;
    const sid = String(e.source_record_id || "");
    if (!sid) continue;
    byId.set(sid, {
      ...e,
      proposed_official_property_url: sanitizeOfficialUrlCandidate(
        e.proposed_official_property_url
      ),
    });
  }

  const applied = [];
  const skipped_existing = [];
  const not_found = [];

  const patchList = (list) =>
    (list || []).map((row) => {
      const sid = String(row.source_record_id || row.sourceRecordId || "");
      const enrichment = byId.get(sid);
      if (!enrichment) return row;

      const preview = { ...(row.sanitized_payload_preview || {}) };
      const existing = String(preview["Official Property URL"] || "").trim();
      const proposedUrl = sanitizeOfficialUrlCandidate(
        enrichment.proposed_official_property_url || ""
      );
      const catalogCity = String(enrichment.proposed_city || "").trim();
      const existingCity = String(preview.City || "").trim();
      const cityOnly = enrichment.proposal_source === "catalog_city_only";

      if (proposedUrl && existing && !policy.overwrite_existing_official_url && !cityOnly) {
        // Still allow catalog city fill when Official URL already present.
        if (
          catalogCity &&
          (!existingCity || /^unknown$/i.test(existingCity))
        ) {
          preview.City = catalogCity;
          applied.push({
            source_record_id: sid,
            property_name: enrichment.property_name,
            current_brand: enrichment.current_brand,
            proposed_official_property_url: existing,
            proposed_city: catalogCity,
            proposal_source: enrichment.proposal_source + "_city_fill",
            proposal_confidence: enrichment.proposal_confidence,
            google_place_id: enrichment.google_place_id || "",
          });
          byId.delete(sid);
          return {
            ...row,
            city: catalogCity,
            sanitized_payload_preview: preview,
            url_enrichment: {
              version: URL_ENRICHMENT_MERGE_VERSION,
              applied: true,
              proposal_source: enrichment.proposal_source + "_city_fill",
              proposal_confidence: enrichment.proposal_confidence,
              google_place_id: enrichment.google_place_id || "",
              proposed_city: catalogCity,
              policy_note: "city_fill_with_existing_official_url",
            },
          };
        }
        skipped_existing.push({
          source_record_id: sid,
          property_name: enrichment.property_name,
          existing_url: existing,
        });
        byId.delete(sid);
        return row;
      }

      if (proposedUrl && (!existing || policy.overwrite_existing_official_url)) {
        preview["Official Property URL"] = proposedUrl;
      }
      if (
        catalogCity &&
        (!existingCity || /^unknown$/i.test(existingCity))
      ) {
        preview.City = catalogCity;
      }
      // Keep Source URL as OSM discovery; Official Property URL is brand site.
      applied.push({
        source_record_id: sid,
        property_name: enrichment.property_name,
        current_brand: enrichment.current_brand,
        proposed_official_property_url: proposedUrl || existing || null,
        proposed_city: catalogCity || null,
        proposal_source: enrichment.proposal_source,
        proposal_confidence: enrichment.proposal_confidence,
        google_place_id: enrichment.google_place_id || "",
      });
      byId.delete(sid);

      return {
        ...row,
        city: preview.City || row.city,
        sanitized_payload_preview: preview,
        url_enrichment: {
          version: URL_ENRICHMENT_MERGE_VERSION,
          applied: true,
          proposal_source: enrichment.proposal_source,
          proposal_confidence: enrichment.proposal_confidence,
          google_place_id: enrichment.google_place_id || "",
          proposed_city: catalogCity || null,
          policy_note:
            "google_places restricted_refresh_required; brand-domain corroborated",
        },
      };
    });

  const independent_payloads = patchList(dualLanePlan.independent_payloads);
  const known_brand_payloads = patchList(dualLanePlan.known_brand_payloads);

  for (const [sid, e] of byId.entries()) {
    not_found.push({
      source_record_id: sid,
      property_name: e.property_name,
    });
  }

  const batchSuffix = opts.batchSuffix || "url-enriched";
  const baseBatch = dualLanePlan.batch_id || "dual-lane";
  const batch_id = opts.batchId || `${baseBatch}-${batchSuffix}`;

  return {
    dual_lane: {
      ...dualLanePlan,
      batch_id,
      generated_at: new Date().toISOString(),
      independent_payloads,
      known_brand_payloads,
      url_enrichment_merge: {
        version: URL_ENRICHMENT_MERGE_VERSION,
        policy,
        applied_count: applied.length,
        skipped_existing_count: skipped_existing.length,
        not_found_count: not_found.length,
        applied,
        skipped_existing,
        not_found,
        source_enrichment_report: opts.enrichmentReportPath || null,
        airtable_writes: false,
      },
      airtable_writes: false,
      hotel_property_census_writes: false,
    },
    summary: {
      version: URL_ENRICHMENT_MERGE_VERSION,
      batch_id,
      eligible_input: applied.length + skipped_existing.length + not_found.length,
      applied_count: applied.length,
      skipped_existing_count: skipped_existing.length,
      not_found_count: not_found.length,
      policy,
      applied,
      airtable_writes: false,
    },
  };
}
