/**
 * Unit tests — DR city → State / Region map.
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  resolveDominicanRepublicStateRegion,
} from "../lib/independent-census/dominican-republic-state-region.js";

test("maps Punta Cana / Sosúa / Santo Domingo", () => {
  assert.equal(
    resolveDominicanRepublicStateRegion("Punta Cana").province,
    "La Altagracia"
  );
  assert.equal(
    resolveDominicanRepublicStateRegion("Sosúa").province,
    "Puerto Plata"
  );
  assert.equal(
    resolveDominicanRepublicStateRegion("Santo Domingo").province,
    "Distrito Nacional"
  );
  assert.equal(
    resolveDominicanRepublicStateRegion("Boca Chica").province,
    "Santo Domingo"
  );
  assert.equal(
    resolveDominicanRepublicStateRegion("Las Terrenas").province,
    "Samaná"
  );
  assert.equal(
    resolveDominicanRepublicStateRegion("Jarabacoa").province,
    "La Vega"
  );
});

test("rejects Unknown / blank / unmapped", () => {
  assert.equal(resolveDominicanRepublicStateRegion("Unknown").ok, false);
  assert.equal(resolveDominicanRepublicStateRegion("").ok, false);
  assert.equal(resolveDominicanRepublicStateRegion("An & Casino").ok, false);
});

test("extracts province from mixed city field", () => {
  const r = resolveDominicanRepublicStateRegion("Bavaro - La Altagracia");
  assert.equal(r.ok, true);
  assert.equal(r.province, "La Altagracia");
  assert.ok(r.suggest_city_cleanup);
});
