import test from "node:test";
import assert from "node:assert/strict";

import {
  parseOfficialCubaHotelHtml,
  CUBANACAN_CURATED_ROWS,
} from "../lib/research-engine-v2/cuba-independent-hotel-discovery-v1.js";
import {
  isStateRegionApplicable,
  resolveCubaProvinceFromCity,
  buildCalaAdminGeographyLibrarySnapshot,
} from "../lib/research-engine-v2/cala-admin-geography-library-v1.js";
import { resolveStateRegionFromCity } from "../lib/research-engine-v2/census-city-to-state-map.js";
import { listDealalityCalaGeographies } from "../lib/research-engine-v2/dealality-cala-geography-registry-v1.js";

test("admin library covers 52 geos + Cuba provinces", () => {
  const geos = listDealalityCalaGeographies();
  assert.equal(geos.length, 52);
  const snap = buildCalaAdminGeographyLibrarySnapshot();
  assert.equal(snap.geography_count, 52);
  assert.equal(isStateRegionApplicable("Cuba"), true);
  assert.equal(isStateRegionApplicable("Aruba"), false);
  assert.equal(resolveCubaProvinceFromCity("Varadero"), "Matanzas");
  assert.equal(resolveCubaProvinceFromCity("Havana"), "La Habana");
});

test("Cuba city→state map resolves", () => {
  const r = resolveStateRegionFromCity({
    city: "Varadero",
    country: "Cuba",
  });
  assert.equal(r.ok, true);
  assert.equal(r.state, "Matanzas");
});

test("parse Gaviota-style HTML yields lodging candidates", () => {
  const html = `
    <h2>HOTEL PLAYA CAYO SANTA MARÍA *****</h2>
    <h2>Hotel Ambos Mundos</h2>
    <h2>Hotels</h2>
    <h2>Servicio Premium Playa Pesquero</h2>
  `;
  const rows = parseOfficialCubaHotelHtml(html, {
    source_id: "test",
    source_url: "https://example.test",
    group: "Gaviota Hotels",
  });
  assert.ok(rows.some((r) => /ambos mundos/i.test(r.property_name)));
  assert.ok(rows.some((r) => /cayo santa/i.test(r.property_name)));
  assert.ok(!rows.some((r) => /^hotels$/i.test(r.property_name)));
  assert.ok(CUBANACAN_CURATED_ROWS.length >= 30);
});
