/**
 * Unit tests — Colombia RNT open-data adapter (no network / no Airtable).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  MAP_COLOMBIA_RNT,
  buildColombiaRntIdentityKey,
  buildColombiaRntWhereClause,
  dedupeColombiaRntByCodigo,
  isColombiaRntLodgingHotelRow,
  mapColombiaRntRowToCensusCandidate,
  parseColombiaRntRooms,
  validateColombiaRntCensusPatch,
} from "../lib/research-engine-v2/colombia-rnt-open-data-adapter.js";

const sampleHotel = {
  codigo_rnt: "43",
  estado_rnt: "ACTIVO",
  razon_social_establecimiento: "HOTEL LOS BALCONES",
  departamento: "CAUCA",
  municipio: "POPAYAN",
  nit: "891501824",
  categoria: "ESTABLECIMIENTOS DE ALOJAMIENTO TURÍSTICO",
  sub_categoria: "HOTEL",
  habitaciones: "8",
  camas: "16",
  ano: "2019",
};

test("identity key is stable gov_co_rnt_*", () => {
  assert.equal(buildColombiaRntIdentityKey("43"), "gov_co_rnt_43");
});

test("lodging filter keeps HOTEL and drops travel agencies", () => {
  assert.equal(isColombiaRntLodgingHotelRow(sampleHotel), true);
  assert.equal(
    isColombiaRntLodgingHotelRow({
      ...sampleHotel,
      categoria: "AGENCIAS DE VIAJES",
      sub_categoria: "AGENCIAS DE VIAJES Y DE TURISMO",
    }),
    false
  );
});

test("hostels excluded by default", () => {
  assert.equal(
    isColombiaRntLodgingHotelRow({
      ...sampleHotel,
      sub_categoria: "HOSTAL",
    }),
    false
  );
});

test("dedupe keeps newest ano per codigo_rnt", () => {
  const rows = [
    { ...sampleHotel, ano: "2023", nit: "1" },
    { ...sampleHotel, ano: "2025", nit: "2" },
    { ...sampleHotel, codigo_rnt: "99", ano: "2024", nit: "3" },
  ];
  const out = dedupeColombiaRntByCodigo(rows);
  assert.equal(out.length, 2);
  const fortyThree = out.find((r) => r.codigo_rnt === "43");
  assert.equal(fortyThree.nit, "2");
});

test("rooms sanity holds absurd counts", () => {
  const bad = parseColombiaRntRooms("5113");
  assert.equal(bad.ok, false);
  assert.equal(bad.hold, true);
  const good = parseColombiaRntRooms("120");
  assert.equal(good.ok, true);
  assert.equal(good.rooms, 120);
});

test("map candidate never sets Owner Name; NIT stays in ownership_signal", () => {
  const c = mapColombiaRntRowToCensusCandidate(sampleHotel);
  assert.equal(c.validation.ok, true);
  assert.equal(c.fields.Country, "Colombia");
  assert.equal(c.fields["Property Name"], "HOTEL LOS BALCONES");
  assert.equal(c.fields["Rooms / Keys"], 8);
  assert.equal(c.fields["Owner Name"], undefined);
  assert.equal(c.ownership_signal.tax_id, "891501824");
  assert.equal(c.ownership_signal.lane, "ownership_enrichment_blocked");
});

test("validation fails when Owner Name sneaks into patch", () => {
  const c = mapColombiaRntRowToCensusCandidate(sampleHotel);
  const patch = { ...c.fields, "Owner Name": "Evil Corp" };
  const v = validateColombiaRntCensusPatch(patch, { ownershipSignal: c.ownership_signal });
  assert.equal(v.ok, false);
  assert.ok(v.failed.some((f) => f.startsWith("forbidden_field:Owner Name")));
});

test("SoQL where includes lodging + hotel subcategory", () => {
  const w = buildColombiaRntWhereClause({ year: 2026, hotelsOnly: false });
  assert.match(w, /ALOJAMIENTO/);
  assert.match(w, /ACTIVO/);
  assert.match(w, /HOTEL/);
  assert.match(w, /ano='2026'/);
});

test("MAP_COLOMBIA_RNT is the central mapping object", () => {
  assert.equal(MAP_COLOMBIA_RNT.sourceDatasetId, "thwd-ivmp");
  assert.equal(MAP_COLOMBIA_RNT.propertyName, "Property Name");
  assert.equal(MAP_COLOMBIA_RNT.nit, "nit");
});
