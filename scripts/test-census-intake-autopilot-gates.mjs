/**
 * Unit tests — census intake Autopilot gates (no Airtable).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  INTAKE_DECISIONS,
  evaluateIntakeAutopilotGate,
} from "../lib/independent-census/intake-autopilot-gates.js";

function baseIndependent(over = {}) {
  return {
    lane: "independent_unaffiliated",
    intake_class: "independent_l1_promote",
    hpc_recommended_action: "likely_new_candidate",
    quality_score: 100,
    sanitized_payload_preview: {
      "Property Name": "Hotel Europa",
      Country: "Dominican Republic",
      City: "Sosua",
      "Official Property URL": "http://www.hotelplazaeuropa.com",
      "Current Brand": "Independent",
      "Affiliation Status": "Independent",
      ...over.fields,
    },
    ...over,
  };
}

test("independent High → auto_insert", () => {
  const g = evaluateIntakeAutopilotGate(baseIndependent());
  assert.equal(g.decision, INTAKE_DECISIONS.AUTO_INSERT);
  assert.equal(g.identity_confidence, "High");
  assert.equal(g.human_review_required, false);
  assert.equal(g.production_writable_insert, true);
});

test("Instagram URL → reject", () => {
  const g = evaluateIntakeAutopilotGate(
    baseIndependent({
      fields: {
        "Official Property URL": "https://www.instagram.com/leisysgarden/",
      },
    })
  );
  assert.equal(g.decision, INTAKE_DECISIONS.REJECT);
  assert.ok(g.reasons.includes("official_url_denylisted_ota_or_social"));
});

test("HPC likely_existing → auto_enrich_only", () => {
  const g = evaluateIntakeAutopilotGate(
    baseIndependent({ hpc_recommended_action: "likely_existing" })
  );
  assert.equal(g.decision, INTAKE_DECISIONS.AUTO_ENRICH_ONLY);
});

test("HPC possible_duplicate → steward_hold", () => {
  const g = evaluateIntakeAutopilotGate(
    baseIndependent({ hpc_recommended_action: "possible_duplicate_review" })
  );
  assert.equal(g.decision, INTAKE_DECISIONS.STEWARD_HOLD);
});

test("independent missing city → steward_hold", () => {
  const g = evaluateIntakeAutopilotGate(
    baseIndependent({
      fields: { City: "Unknown" },
    })
  );
  assert.equal(g.decision, INTAKE_DECISIONS.STEWARD_HOLD);
  assert.ok(g.reasons.includes("independent_missing_city"));
});

test("known chain backlog with city → auto_insert HR", () => {
  const g = evaluateIntakeAutopilotGate({
    lane: "known_brand_census_intake",
    intake_class: "known_chain_census_backlog_not_active_setup",
    hpc_recommended_action: "likely_new_candidate",
    quality_score: 70,
    sanitized_payload_preview: {
      "Property Name": "Hotel RIU Mambo",
      Country: "Dominican Republic",
      City: "Maimón",
      "Official Property URL": "https://www.riu.com/hotel",
      "Current Brand": "RIU",
      "Affiliation Status": "Branded",
    },
  });
  assert.equal(g.decision, INTAKE_DECISIONS.AUTO_INSERT);
  assert.equal(g.human_review_required, true);
});

test("active soft brand → auto_insert no HR + enrichment queue", () => {
  const g = evaluateIntakeAutopilotGate({
    lane: "known_brand_census_intake",
    intake_class: "active_or_soft_brand_census_plus_autopilot",
    hpc_recommended_action: "likely_new_candidate",
    quality_score: 80,
    sanitized_payload_preview: {
      "Property Name": "Hilton La Romana",
      Country: "Dominican Republic",
      City: "La Romana",
      "Official Property URL": "https://www.hilton.com/en/hotels/example/",
      "Current Brand": "Hilton Hotels & Resorts",
      "Affiliation Status": "Branded",
    },
  });
  assert.equal(g.decision, INTAKE_DECISIONS.AUTO_INSERT);
  assert.equal(g.human_review_required, false);
  assert.equal(g.queue_autopilot_enrichment, true);
});

test("steward brand tag → steward_hold", () => {
  const g = evaluateIntakeAutopilotGate({
    lane: "known_brand_census_intake",
    intake_class: "steward_brand_tag_review",
    hpc_recommended_action: "likely_new_candidate",
    sanitized_payload_preview: {
      "Property Name": "White Sands B&B",
      Country: "Dominican Republic",
      City: "Cabarete",
      "Official Property URL": "https://example.com",
      "Current Brand": "Lance & Marianna",
      "Affiliation Status": "Brand-Unconfirmed",
    },
  });
  assert.equal(g.decision, INTAKE_DECISIONS.STEWARD_HOLD);
});
