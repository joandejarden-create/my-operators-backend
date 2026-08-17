import test from "node:test";
import assert from "node:assert/strict";
import {
  buildContinentSubContinentPatch,
  buildBrandFamilyDerivePatch,
  evaluateStrictCurrentBrandFromOfficialUrl,
  computeMasterCompleteness,
  MAP_MASTER,
  MASTER_SCHEMA_NOTES,
} from "../lib/research-engine-v2/master-census-enrichment-v1.js";
import { resolveContinentSubContinentFromCountry } from "../lib/research-engine-v2/census-region-market-map.js";
import { buildCanonicalBrandDictionary } from "../lib/research-engine-v2/census-brand-canonical-dictionary.js";

test("continent map covers Saint Lucia and Bermuda", () => {
  assert.deepEqual(resolveContinentSubContinentFromCountry("Saint Lucia"), {
    continent: "North America",
    subContinent: "Caribbean",
  });
  assert.deepEqual(resolveContinentSubContinentFromCountry("Bermuda"), {
    continent: "North America",
    subContinent: "Caribbean",
  });
  assert.deepEqual(resolveContinentSubContinentFromCountry("French Guiana"), {
    continent: "South America",
    subContinent: "South America",
  });
});

test("continent NULL_FILL does not overwrite", () => {
  const blank = buildContinentSubContinentPatch({
    Country: "Jamaica",
  });
  assert.equal(blank.ok, true);
  assert.equal(blank.patch[MAP_MASTER.continent], "North America");
  assert.equal(blank.patch[MAP_MASTER.subContinent], "Caribbean");

  const filled = buildContinentSubContinentPatch({
    Country: "Jamaica",
    Continent: "North America",
    "Sub-Continent": "Caribbean",
  });
  assert.equal(filled.ok, false);
});

test("schema notes: Family / Source Family is single field", () => {
  assert.equal(MASTER_SCHEMA_NOTES.family_field, "Family / Source Family");
  assert.equal(MASTER_SCHEMA_NOTES.website_field, "Official Property URL");
});

test("brand family derive requires Current Brand and mapping", () => {
  const dictionary = buildCanonicalBrandDictionary({});
  const noBrand = buildBrandFamilyDerivePatch({}, dictionary);
  assert.equal(noBrand.ok, false);

  const withBrand = buildBrandFamilyDerivePatch(
    { "Current Brand": "Hotel Indigo" },
    dictionary
  );
  // May be ok or mapping gap depending on dictionary contents
  assert.ok(withBrand.ok === true || withBrand.reason === "BRAND_MAPPING_GAP");
});

test("strict brand rejects OTA website and requires unique name hit", () => {
  const dictionary = buildCanonicalBrandDictionary({});
  const ota = evaluateStrictCurrentBrandFromOfficialUrl(
    {
      "Official Property URL": "https://www.booking.com/hotel/x",
      "Property Name": "Hotel Indigo Cancun",
    },
    dictionary
  );
  assert.equal(ota.ok, false);

  const noName = evaluateStrictCurrentBrandFromOfficialUrl(
    {
      "Official Property URL": "https://www.ihg.com/hotelindigo/hotels/us/en/cancun",
      "Property Name": "Beach Resort Cancun",
    },
    dictionary
  );
  assert.ok(["BRAND_CANDIDATE", "BRAND_UNRESOLVED", "BRAND_CONFLICT"].includes(noName.class));
});

test("completeness dashboard shape", () => {
  const dash = computeMasterCompleteness([
    {
      id: "1",
      fields: {
        Continent: "North America",
        "Sub-Continent": "Caribbean",
        City: "Kingston",
      },
    },
  ]);
  assert.equal(dash.n, 1);
  assert.equal(dash.continent.populated, 1);
  assert.equal(dash.rooms.populated, 0);
});
