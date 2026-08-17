/**
 * City Resolver V4 unit checks.
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  resolveCityV4,
  parseBrazilAddress,
  parseArgentinaAddress,
  extractCityFromOfficialUrl,
  isPostalAsCity,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";

test("Brazil CEP is postal-as-city", () => {
  assert.equal(isPostalAsCity("95170-272", "Brazil"), true);
});

test("IHG URL recovers Farroupilha — not title", () => {
  const r = extractCityFromOfficialUrl(
    "https://www.ihg.com/holidayinnexpress/hotels/us/en/farroupilha/cxjfa/hoteldetail",
    "Brazil"
  );
  assert.equal(r.ok, true);
  assert.equal(r.city, "Farroupilha");
});

test("resolveCityV4 prefers IHG URL over CEP city", () => {
  const r = resolveCityV4({
    country: "Brazil",
    city: "01419-001",
    official_url: "https://www.ihg.com/intercontinental/hotels/us/en/sao-paulo/saoha/hoteldetail",
  });
  assert.equal(r.ok, true);
  assert.equal(r.city, "São Paulo");
  assert.equal(r.production_eligible, true);
});

test("Brazil address parser extracts UF", () => {
  const p = parseBrazilAddress("Av. Paulista 1000, São Paulo - SP, 01310-100");
  assert.equal(p.state, "São Paulo");
});

test("Argentina research address yields Mendoza", () => {
  const p = parseArgentinaAddress("Primitivo de la Reta 989, M5500 Mendoza, Argentina");
  assert.ok(p.city === "Mendoza" || p.state === "Mendoza");
});

test("Never accepts Country as City", () => {
  const r = resolveCityV4({ country: "Mexico", city: "Mexico" });
  assert.equal(r.current_class.bucket, "COUNTRY_AS_CITY");
});

test("Street line is not accepted as City", () => {
  const r = resolveCityV4({
    country: "Mexico",
    city: "Unknown",
    address: "Avenida. Miguel Aleman 737 Nte., Monterrey, NL",
  });
  if (r.city) assert.equal(/avenida|miguel aleman/i.test(r.city), false);
});
