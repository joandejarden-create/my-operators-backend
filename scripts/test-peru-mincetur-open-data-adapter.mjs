/**
 * Unit tests — Peru MINCETUR open-data adapter (no network).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  MAP_PERU_MINCETUR,
  buildPeruMinceturIdentityKey,
  isPeruMinceturHotelRow,
  mapPeruMinceturRowToCensusCandidate,
  normalizePeruWebsite,
  parsePeruMinceturCsv,
  parsePeruMinceturRooms,
  validatePeruMinceturCensusPatch,
} from "../lib/research-engine-v2/peru-mincetur-open-data-adapter.js";

const sample = {
  FECHA_CORTE: "20260806",
  CAMA: "40",
  HABI: "32",
  RUC: "20608180070",
  RAZON_SOCIAL: "Peruvian Spots S.A.C.",
  NOMBRE_COMERCIAL: "AVA SPOTS",
  DEPARTAMENTO: "CUSCO",
  PROVINCIA: "URUBAMBA",
  DISTRITO: "URUBAMBA",
  PAGINA_WEB: "www.avaspots.com",
  CLASE: "Hotel",
  CATEGORIA: "3 Estrellas",
  NRO_CERTIFICADO: "CERT-123",
  REP_LEGAL: "Jane Doe",
  VIA: "Calle",
  DES_VIA: "Principal",
  NUMERO: "10",
  TELEF1: "984000000",
};

test("identity prefers certificate number", () => {
  assert.equal(
    buildPeruMinceturIdentityKey("CERT-123", "20608180070", "AVA SPOTS", "Urubamba"),
    "gov_pe_mincetur_CERT-123"
  );
});

test("hotel filter keeps Hotel, drops Hostal", () => {
  assert.equal(isPeruMinceturHotelRow(sample), true);
  assert.equal(isPeruMinceturHotelRow({ ...sample, CLASE: "Hostal" }), false);
});

test("website normalization", () => {
  assert.equal(normalizePeruWebsite("www.avaspots.com"), "https://www.avaspots.com");
  assert.equal(normalizePeruWebsite(""), null);
});

test("map uses commercial name + Official Property URL; no Owner Name", () => {
  const c = mapPeruMinceturRowToCensusCandidate(sample);
  assert.equal(c.validation.ok, true);
  assert.equal(c.fields["Property Name"], "AVA SPOTS");
  assert.equal(c.fields.Country, "Peru");
  assert.equal(c.fields["Official Property URL"], "https://www.avaspots.com");
  assert.equal(c.fields["Rooms / Keys"], 32);
  assert.equal(c.fields["Owner Name"], undefined);
  assert.equal(c.ownership_signal.tax_id, "20608180070");
  assert.equal(c.ownership_signal.lane, "ownership_enrichment_blocked");
});

test("Owner Name injection fails validation", () => {
  const c = mapPeruMinceturRowToCensusCandidate(sample);
  const v = validatePeruMinceturCensusPatch(
    { ...c.fields, "Owner Name": "Nope" },
    { ownershipSignal: c.ownership_signal }
  );
  assert.equal(v.ok, false);
});

test("CSV parse + clase URL repair", () => {
  const csv = `FECHA_CORTE;CAMA;HABI;RUC;RAZON_SOCIAL;NOMBRE_COMERCIAL;TELEF1;TELEF2;TELEF3;TELEF4;VIA;DES_VIA;NUMERO;INTERIOR;OBS_DOMICILIO;ZONA;DES_ZONA;UBIGEO;DEPARTAMENTO;PROVINCIA;DISTRITO;E_MAIL;PAGINA_WEB;CLASE;CATEGORIA;NRO_CERTIFICADO;FECHA_EXPEDICION;REP_LEGAL
20260806;10;5;123;Legal;Trade;;;;;;;;;;;;010101;LIMA;LIMA;MIRAFLORES;;www.example.com;Hotel;3 Estrellas;C1;;Rep
20260806;10;5;124;Legal2;Trade2;;;;;;;;;;;;010101;LIMA;LIMA;MIRAFLORES;;;www.shifted.com;3 Estrellas;;;Rep`;
  const parsed = parsePeruMinceturCsv(csv);
  assert.equal(parsed.rows.length, 2);
  assert.equal(parsed.rows[0].CLASE, "Hotel");
  assert.equal(parsed.rows[1].CLASE, "");
  assert.equal(parsed.rows[1].PAGINA_WEB, "www.shifted.com");
});

test("rooms sanity", () => {
  assert.equal(parsePeruMinceturRooms("20").ok, true);
  assert.equal(parsePeruMinceturRooms("5000").hold, true);
});

test("MAP_PERU_MINCETUR central mapping", () => {
  assert.equal(MAP_PERU_MINCETUR.country, "Peru");
  assert.equal(MAP_PERU_MINCETUR.ruc, "RUC");
});
