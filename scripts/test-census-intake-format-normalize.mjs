/**
 * Unit tests — intake census field normalize + hostel reject.
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  isHostelOrHostalProperty,
  normalizeIntakeCensusFamilyFields,
  resolveIntakeParentFamily,
  buildIntakeCensusFormatRemediationPatch,
} from "../lib/independent-census/intake-census-field-normalize.js";
import { evaluateIntakeAutopilotGate, INTAKE_DECISIONS } from "../lib/independent-census/intake-autopilot-gates.js";
import { buildIntakeInsertFields } from "../lib/independent-census/intake-autopilot-controlled.js";

test("isHostelOrHostalProperty detects hostal/hostel", () => {
  assert.equal(isHostelOrHostalProperty("Hostal Ganesh"), true);
  assert.equal(isHostelOrHostalProperty("Ganesh Hostel"), true);
  assert.equal(isHostelOrHostalProperty("Hotal San Francisco de Asis"), true);
  assert.equal(isHostelOrHostalProperty("Hotel Europa"), false);
  assert.equal(isHostelOrHostalProperty("Santo Domingo Bed & Breakfast"), false);
});

test("resolveIntakeParentFamily maps brands to census parents", () => {
  assert.deepEqual(resolveIntakeParentFamily("Four Points by Sheraton"), {
    sourceFamily: "Marriott",
    brandFamily: "Marriott International",
  });
  assert.deepEqual(resolveIntakeParentFamily("Hilton Hotels & Resorts"), {
    sourceFamily: "Hilton",
    brandFamily: "Hilton",
  });
  assert.deepEqual(resolveIntakeParentFamily("Radisson by Choice"), {
    sourceFamily: "Choice",
    brandFamily: "Choice Hotels International, Inc.",
  });
  assert.deepEqual(resolveIntakeParentFamily("The Luxury Collection"), {
    sourceFamily: "Marriott",
    brandFamily: "Marriott International",
  });
  assert.deepEqual(resolveIntakeParentFamily("Independent"), {
    sourceFamily: "",
    brandFamily: "",
  });
  assert.deepEqual(resolveIntakeParentFamily("RIU"), {
    sourceFamily: "RIU",
    brandFamily: "RIU",
  });
  assert.deepEqual(resolveIntakeParentFamily("Occidental"), {
    sourceFamily: "Barceló",
    brandFamily: "Barceló",
  });
  assert.deepEqual(
    resolveIntakeParentFamily("Dreams (Hyatt Inclusive Collection)"),
    { sourceFamily: "Hyatt", brandFamily: "Hyatt" }
  );
  assert.deepEqual(resolveIntakeParentFamily("Starfish Resorts"), {
    sourceFamily: "Karisma Hotels",
    brandFamily: "Karisma Hotels",
  });
  assert.deepEqual(resolveIntakeParentFamily("Holiday Inn"), {
    sourceFamily: "IHG",
    brandFamily: "IHG Hotels & Resorts",
  });
  assert.deepEqual(resolveIntakeParentFamily("Club Med"), {
    sourceFamily: "Club Med",
    brandFamily: "Club Med",
  });
  assert.deepEqual(resolveIntakeParentFamily("Breezes (SuperClubs)"), {
    sourceFamily: "SuperClubs",
    brandFamily: "SuperClubs",
  });
  assert.deepEqual(resolveIntakeParentFamily("Breezes"), {
    sourceFamily: "SuperClubs",
    brandFamily: "SuperClubs",
  });
});

test("canonicalizeBrandFamilyDisplay maps short aliases to census forms", async () => {
  const {
    canonicalizeBrandFamilyDisplay,
  } = await import("../lib/independent-census/intake-census-field-normalize.js");
  assert.equal(canonicalizeBrandFamilyDisplay("IHG"), "IHG Hotels & Resorts");
  assert.equal(
    canonicalizeBrandFamilyDisplay("InterContinental Hotels Group"),
    "IHG Hotels & Resorts"
  );
  assert.equal(
    canonicalizeBrandFamilyDisplay("Marriott"),
    "Marriott International"
  );
  assert.equal(
    canonicalizeBrandFamilyDisplay("Choice"),
    "Choice Hotels International, Inc."
  );
});

test("normalize clears Unknown City and fills Brand Family display", () => {
  const n = normalizeIntakeCensusFamilyFields({
    "Property Name": "Catalonia Punta Cana",
    "Current Brand": "Catalonia",
    "Affiliation Status": "Branded",
    "Family / Source Family": "Other",
    "Brand Family": "Catalonia",
    City: "Unknown",
    Country: "Dominican Republic",
    "Official Property URL":
      "https://www.cataloniahotels.com/en/hotel/catalonia-punta-cana",
  });
  assert.equal(n["Family / Source Family"], "Catalonia");
  assert.equal(n["Brand Family"], "Catalonia");
  assert.equal(n.City, "Punta Cana");
  assert.equal(n["State / Region"], "La Altagracia");
});

test("normalizeIntakeCensusFamilyFields clears invented independents + Unknown state", () => {
  const ind = normalizeIntakeCensusFamilyFields({
    "Property Name": "Hotel Europa",
    "Current Brand": "Independent",
    "Affiliation Status": "Independent",
    "Family / Source Family": "independent_open_sources",
    "Brand Family": "",
    "State / Region": "Unknown",
  });
  assert.equal(ind["Family / Source Family"], undefined);
  assert.equal(ind["Brand Family"], undefined);
  assert.equal(ind["State / Region"], undefined);

  const branded = normalizeIntakeCensusFamilyFields({
    "Property Name": "Four Points by Sheraton",
    "Current Brand": "Four Points by Sheraton",
    "Affiliation Status": "Branded",
    "Family / Source Family": "Four Points by Sheraton",
    "Brand Family": "Four Points by Sheraton",
    "State / Region": "Unknown",
  });
  assert.equal(branded["Family / Source Family"], "Marriott");
  assert.equal(branded["Brand Family"], "Marriott International");
  assert.equal(branded["State / Region"], undefined);
});

test("gates reject hostal", () => {
  const g = evaluateIntakeAutopilotGate({
    lane: "independent_unaffiliated",
    intake_class: "independent_l1_promote",
    hpc_recommended_action: "likely_new_candidate",
    quality_score: 100,
    sanitized_payload_preview: {
      "Property Name": "Hostal Ganesh",
      Country: "Dominican Republic",
      City: "El Valle, Samana",
      "Official Property URL": "https://ganeshhostel.com/",
      "Current Brand": "Independent",
      "Affiliation Status": "Independent",
    },
  });
  assert.equal(g.decision, INTAKE_DECISIONS.REJECT);
  assert.ok(g.reasons.includes("hostel_or_hostal_out_of_scope"));
});

test("buildIntakeInsertFields applies parent family for branded", () => {
  const { fields } = buildIntakeInsertFields({
    human_review_required: false,
    identity_confidence: "High",
    payload: {
      "Property Name": "Four Points by Sheraton",
      "Property Identity Key": "osm_do_node_x",
      "Current Brand": "Four Points by Sheraton",
      "Brand Family": "Four Points by Sheraton",
      "Affiliation Status": "Branded",
      Country: "Dominican Republic",
      City: "Punta Cana",
      "State / Region": "Unknown",
      "Official Property URL": "https://www.marriott.com/x",
      "Source URL": "https://www.openstreetmap.org/node/x",
      "Family / Source Family": "Four Points by Sheraton",
      "VIC Freeze Hash": "independent_census_dr_osm_2026-08-07",
    },
  });
  assert.equal(fields["Family / Source Family"], "Marriott");
  assert.equal(fields["Brand Family"], "Marriott International");
  assert.equal(fields["State / Region"], "La Altagracia");
});

test("remediation patch clears Unknown and maps family", () => {
  const r = buildIntakeCensusFormatRemediationPatch({
    "Property Name": "Hostal Ganesh",
    "Current Brand": "Independent",
    "Affiliation Status": "Independent",
    "Family / Source Family": "independent_open_sources",
    "State / Region": "Unknown",
  });
  assert.equal(r.delete_record, true);
  assert.equal(r.patch["Family / Source Family"], null);
  assert.equal(r.patch["State / Region"], null);
});
