/**
 * Unit tests — intake Autopilot controlled dry-run builder.
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  buildIntakeInsertFields,
  validateIntakeInsertProposal,
  buildIntakeControlledDryRun,
} from "../lib/independent-census/intake-autopilot-controlled.js";
import { INTAKE_DECISIONS } from "../lib/independent-census/intake-autopilot-gates.js";

test("buildIntakeInsertFields sets required governance fields", () => {
  const { fields } = buildIntakeInsertFields({
    human_review_required: false,
    identity_confidence: "High",
    payload: {
      "Property Name": "Hotel Europa",
      "Property Identity Key": "osm_do_node_1",
      "Current Brand": "Independent",
      "Affiliation Status": "Independent",
      Country: "Dominican Republic",
      City: "Sosua",
      "Official Property URL": "http://www.hotelplazaeuropa.com",
      "Source URL": "https://www.openstreetmap.org/node/1",
      "Family / Source Family": "independent_open_sources",
      "VIC Freeze Hash": "independent_census_dr_osm_2026-08-07",
      "Independent Hotel Flag": true,
    },
  });
  assert.equal(fields["Production Use Status"], "Census Only / Not Owner-Facing");
  assert.equal(fields["Human Review Required"], false);
  assert.equal(fields["Identity Confidence"], "High");
  const v = validateIntakeInsertProposal(fields, {
    lane: "independent_unaffiliated",
    intake_class: "independent_l1_promote",
    quality_score: 100,
  });
  assert.equal(v.pass, true, v.failures.join(","));
});

test("denylisted URL fails validation", () => {
  const { fields } = buildIntakeInsertFields({
    human_review_required: false,
    payload: {
      "Property Name": "X",
      "Property Identity Key": "osm_do_node_2",
      "Current Brand": "Independent",
      "Affiliation Status": "Independent",
      Country: "Dominican Republic",
      City: "Sosua",
      "Official Property URL": "https://www.instagram.com/x",
      "Source URL": "https://www.openstreetmap.org/node/2",
      "Family / Source Family": "independent_open_sources",
      "VIC Freeze Hash": "test",
    },
  });
  const v = validateIntakeInsertProposal(fields, {
    lane: "independent_unaffiliated",
    intake_class: "independent_l1_promote",
    quality_score: 100,
  });
  assert.equal(v.pass, false);
  assert.ok(v.failures.some((f) => f.includes("denylist") || f.includes("regate")));
});

test("controlled dry-run filters auto_insert only", () => {
  const plan = {
    rows: [
      {
        source_record_id: "a",
        decision: INTAKE_DECISIONS.AUTO_INSERT,
        production_writable_insert: true,
        human_review_required: false,
        identity_confidence: "High",
        lane: "independent_unaffiliated",
        intake_class: "independent_l1_promote",
        payload: {
          "Property Name": "Hotel Europa",
          "Property Identity Key": "osm_do_node_a",
          "Current Brand": "Independent",
          "Affiliation Status": "Independent",
          Country: "Dominican Republic",
          City: "Sosua",
          "Official Property URL": "http://www.hotelplazaeuropa.com",
          "Source URL": "https://www.openstreetmap.org/node/a",
          "Family / Source Family": "independent_open_sources",
          "VIC Freeze Hash": "test",
          "Independent Hotel Flag": true,
        },
      },
      {
        source_record_id: "b",
        decision: INTAKE_DECISIONS.STEWARD_HOLD,
        production_writable_insert: false,
        payload: { "Property Name": "Hold me" },
      },
    ],
  };
  const dry = buildIntakeControlledDryRun(plan, { cohort: "no_hr" });
  assert.equal(dry.counts.proposals, 1);
  assert.equal(dry.airtable_writes, false);
});
