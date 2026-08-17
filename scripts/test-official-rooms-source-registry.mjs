import test from "node:test";
import assert from "node:assert/strict";
import {
  buildOfficialRoomsSourceMatrix,
  SOURCE_TIER,
  listTierASources,
} from "../lib/research-engine-v2/cala-official-rooms-source-registry-v1.js";
import {
  parseBrazilCadasturRooms,
  isBrazilCadasturHotelRow,
  mapBrazilCadasturRowToNormalized,
  MAP_BRAZIL_CADASTUR,
} from "../lib/research-engine-v2/brazil-cadastur-open-data-adapter.js";
import {
  matchCensusToBrazilCadasturRooms,
  promoteColombiaRntMediumWithCorroboration,
} from "../lib/research-engine-v2/census-rooms-secondary-match.js";
import { parseBarbadosBedroomCount } from "../lib/research-engine-v2/barbados-btpa-directory-adapter.js";

test("52-geography official rooms matrix builds", () => {
  const m = buildOfficialRoomsSourceMatrix();
  assert.equal(m.GEOGRAPHIES_ASSESSED, 52);
  assert.ok(m.TIER_A_SOURCES_FOUND.includes("brazil_cadastur_meios"));
  assert.ok(m.TIER_A_SOURCES_FOUND.includes("peru_mincetur_hospedaje"));
  assert.ok(m.TIER_A_SOURCES_FOUND.includes("colombia_rnt"));
  assert.ok(listTierASources().length >= 3);
});

test("Brazil UH parses; Leitos never used as rooms", () => {
  const uh = parseBrazilCadasturRooms(180);
  assert.equal(uh.ok, true);
  assert.equal(uh.rooms, 180);
  const row = {
    [MAP_BRAZIL_CADASTUR.nomeFantasia]: "Hotel Teste",
    [MAP_BRAZIL_CADASTUR.municipio]: "São Paulo",
    [MAP_BRAZIL_CADASTUR.uf]: "SP",
    [MAP_BRAZIL_CADASTUR.tipoHospedagem]: "Hotel",
    [MAP_BRAZIL_CADASTUR.situacaoAtividade]: "Operação",
    [MAP_BRAZIL_CADASTUR.unidadeHabitacionais]: 100,
    [MAP_BRAZIL_CADASTUR.leitos]: 220,
    [MAP_BRAZIL_CADASTUR.enderecoComercial]: "Rua X 1 CEP: 01310-100 SP",
    [MAP_BRAZIL_CADASTUR.cnpj]: "123",
  };
  assert.equal(isBrazilCadasturHotelRow(row), true);
  const n = mapBrazilCadasturRowToNormalized(row);
  assert.equal(n.rooms, 100);
  assert.equal(n.leitos, 220);
  assert.equal(n.rooms_field_used, "Unidade Habitacionais");
  assert.notEqual(n.rooms, n.leitos);
});

test("Brazil HIGH match requires city + strong name", () => {
  const sourceRows = [
    {
      property_name: "Hotel Alles Blau",
      city: "Pelotas",
      state_region: "Rio Grande do Sul",
      state_uf: "RS",
      rooms: 42,
      source_url: "https://dados.turismo.gov.br/dataset/meios-de-hospedagem",
    },
  ];
  const high = matchCensusToBrazilCadasturRooms(
    {
      Country: "Brazil",
      City: "Pelotas",
      "State / Region": "RS",
      "Property Name": "Hotel Alles Blau",
    },
    sourceRows
  );
  assert.equal(high.ok, true);
  assert.equal(high.confidence, "High");
  assert.equal(high.rooms, 42);
});

test("Barbados bedrooms parser rejects zero", () => {
  assert.equal(parseBarbadosBedroomCount("0").ok, false);
  assert.equal(parseBarbadosBedroomCount("224").rooms, 224);
});

test("Colombia medium promotion needs corroboration signals", () => {
  const medium = {
    ok: true,
    rooms: 80,
    confidence: "Medium",
    identity_match_high: false,
    match_sim: 0.82,
    matched_source_name: "HOTEL SOL CARTAGENA",
    category: "tourism_board_convention_bureau_destination_authority",
    source_url: "https://example.test",
  };
  const no = promoteColombiaRntMediumWithCorroboration(
    { Country: "Colombia", City: "Cartagena", "Property Name": "Hotel Sol" },
    medium
  );
  assert.equal(no.ok, false);
  const yes = promoteColombiaRntMediumWithCorroboration(
    {
      Country: "Colombia",
      City: "Cartagena",
      "Property Name": "Hotel Sol",
      Address: "Calle Cartagena 12",
      Phone: "+573001112233",
      "Official Property URL": "https://hotelsol.com",
      "Postal Code": "130001",
    },
    medium
  );
  assert.equal(yes.ok, true);
  assert.equal(yes.promoted, true);
  assert.equal(yes.confidence, "High");
});

test("matrix never marks DataTur as Tier A", () => {
  const m = buildOfficialRoomsSourceMatrix();
  const datatur = m.flat_source_rows.find((r) =>
    /datatur/i.test(String(r["Source Name"] || r.source_id || ""))
  );
  assert.ok(datatur);
  assert.equal(datatur.Tier, SOURCE_TIER.TIER_D_AGGREGATE_ONLY);
});
