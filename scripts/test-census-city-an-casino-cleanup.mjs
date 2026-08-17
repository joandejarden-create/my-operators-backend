import test from "node:test";
import assert from "node:assert/strict";
import {
  extractCityFromAutographStyleName,
  extractCityFromOfficialUrl,
  buildAnCasinoCityCleanupProposal,
} from "../lib/independent-census/census-city-an-casino-cleanup.js";

test("extracts Punta Cana / Bavaro from Autograph names", () => {
  assert.equal(
    extractCityFromAutographStyleName(
      "Royalton Punta Cana, An Autograph Collection All-Inclusive Resort & Casino"
    ),
    "Punta Cana"
  );
  assert.equal(
    extractCityFromAutographStyleName(
      "Royalton Bavaro, An Autograph Collection All-Inclusive Resort & Casino"
    ),
    "Bávaro"
  );
  assert.equal(
    extractCityFromAutographStyleName(
      "Emotions All Inclusive Puerto Plata, an Ascend Collection Hotel"
    ),
    "Puerto Plata"
  );
});

test("extracts city from Marriott URL", () => {
  assert.equal(
    extractCityFromOfficialUrl(
      "https://www.marriott.com/en-us/hotels/pujrb-royalton-bavaro-an-autograph-collection-all-inclusive-resort-and-casino/overview"
    ),
    "Bávaro"
  );
});

test("builds High cleanup patch for An & Casino", () => {
  const p = buildAnCasinoCityCleanupProposal({
    Country: "Dominican Republic",
    City: "An & Casino",
    "Property Name":
      "Royalton Splash Punta Cana, An Autograph Collection All-Inclusive Resort & Casino",
    "Official Property URL":
      "https://www.marriott.com/en-us/hotels/pujrs-royalton-splash-punta-cana-an-autograph-collection-all-inclusive-resort-and-casino/overview",
  });
  assert.equal(p.ok, true);
  assert.equal(p.patch.City, "Punta Cana");
  assert.equal(p.patch["State / Region"], "La Altagracia");
});
