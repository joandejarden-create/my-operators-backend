/**
 * Unit tests — merge URL enrichment into dual-lane payloads.
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  isMergeEligibleEnrichment,
  mergeUrlEnrichmentIntoDualLane,
} from "../lib/independent-census/merge-url-enrichment-into-dual-lane.js";

test("isMergeEligibleEnrichment requires high brand-domain by default", () => {
  assert.equal(
    isMergeEligibleEnrichment({
      apply_as_official_url_candidate: true,
      proposal_confidence: "high",
      proposal_source: "google_places_website_on_brand_domain",
      proposed_official_property_url: "https://www.riu.com/en/hotel/x",
    }),
    true
  );
  assert.equal(
    isMergeEligibleEnrichment({
      apply_as_official_url_candidate: true,
      proposal_confidence: "medium",
      proposal_source: "google_places_website_on_brand_domain",
      proposed_official_property_url: "https://www.riu.com/en/hotel/x",
    }),
    false
  );
  assert.equal(
    isMergeEligibleEnrichment({
      apply_as_official_url_candidate: true,
      proposal_confidence: "high",
      proposal_source: "google_places_website_off_brand_domain",
      proposed_official_property_url: "https://www.example-resort.com/",
    }),
    false
  );
});

test("mergeUrlEnrichmentIntoDualLane patches Official Property URL", () => {
  const dual = {
    batch_id: "batch-a",
    independent_payloads: [],
    known_brand_payloads: [
      {
        source_record_id: "node/1",
        sanitized_payload_preview: {
          "Property Name": "RIU Palace",
          "Official Property URL": "",
          Country: "Dominican Republic",
        },
      },
    ],
  };
  const { dual_lane, summary } = mergeUrlEnrichmentIntoDualLane(
    dual,
    [
      {
        source_record_id: "node/1",
        property_name: "RIU Palace",
        current_brand: "RIU",
        apply_as_official_url_candidate: true,
        proposal_confidence: "high",
        proposal_source: "google_places_website_on_brand_domain",
        proposed_official_property_url:
          "https://www.riu.com/en/hotel/x?utm_source=google",
        google_place_id: "places/abc",
      },
    ],
    { batchSuffix: "url-enriched" }
  );
  assert.equal(summary.applied_count, 1);
  assert.equal(
    dual_lane.known_brand_payloads[0].sanitized_payload_preview[
      "Official Property URL"
    ],
    "https://www.riu.com/en/hotel/x"
  );
  assert.equal(dual_lane.batch_id, "batch-a-url-enriched");
  assert.equal(dual_lane.url_enrichment_merge.applied_count, 1);
});
