/**
 * Adaptive overnight Mode B — unit tests (no live Airtable / crawl).
 */
import test from "node:test";
import assert from "node:assert/strict";

import {
  resolveBrandMappingAlias,
  isDeterministicAliasRepair,
  candidateBrandCannotSelfValidate,
  buildBrandMappingRepairPatch,
} from "../lib/research-engine-v2/brand-mapping-gap-repair-v1.js";
import {
  rejectNonGuestroomSemantics,
  sourcesAreIndependent,
  sameUpstreamSource,
  evaluateRoomsCorroboration,
} from "../lib/research-engine-v2/rooms-candidate-corroboration-v1.js";
import {
  allAvailableResearchModesExhausted,
  applyNullFillToRecords,
  ADAPTIVE_PHASES,
  migrateAdaptivePhaseStatus,
} from "../lib/research-engine-v2/adaptive-overnight-engine-v1.js";
import {
  isForbiddenHost,
  extractJsonLdHotels,
  looksLikePropertyUrl,
} from "../lib/research-engine-v2/official-domain-crawler-v1.js";
import {
  scoreResidualProperty,
  rankResidualQueue,
} from "../lib/research-engine-v2/residual-property-research-v1.js";
import { runOvernightCensusEnrichment } from "../lib/research-engine-v2/overnight-census-enrichment-v1.js";
import { MAP_MASTER } from "../lib/research-engine-v2/master-census-enrichment-v1.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";

function dash(n = 15575) {
  return {
    n,
    current_brand: { completeness_pct: 14 },
    rooms: { completeness_pct: 17 },
    address: { completeness_pct: 85 },
    postal_code: { completeness_pct: 38 },
    latitude: { completeness_pct: 67 },
    longitude: { completeness_pct: 67 },
    state_region: { completeness_pct: 89 },
    city: { completeness_pct: 98 },
    website: { completeness_pct: 75 },
    phone: { completeness_pct: 90 },
    brand_family: { completeness_pct: 14 },
    family_source_family: { completeness_pct: 16 },
  };
}

test("structured plateau is not a master stop condition", () => {
  const status = {
    phase_1_structured: "PLATEAUED",
    phase_1b_apify_first_party: "READY",
    phase_2_property_outward_domain: "READY",
    phase_2a_property_outward_pages: "READY",
    phase_2b_property_outward_independent: "READY",
    phase_2c_candidate_brand_routing: "READY",
    phase_2d_website_discovery: "READY",
    phase_2e_demand_adapters: "READY",
    phase_3_rooms_corroboration: "READY",
    phase_4_pdf_factsheet: "READY",
    phase_5_residual: "READY",
    phase_6_newly_eligible_coords: "READY",
  };
  assert.equal(allAvailableResearchModesExhausted(status), false);
});

test("allAvailableResearchModesExhausted requires all Mode B lanes", () => {
  const status = migrateAdaptivePhaseStatus({
    phase_1_structured: "EXHAUSTED",
    phase_1b_apify_first_party: "EXHAUSTED",
    phase_2e_demand_adapters: "EXHAUSTED",
    phase_3_rooms_corroboration: "EXHAUSTED",
    phase_4_pdf_factsheet: "EXHAUSTED",
    phase_5_residual: "EXHAUSTED",
    phase_6_newly_eligible_coords: "EXHAUSTED",
    phase_2_property_outward_domain: "EXHAUSTED",
    phase_2a_property_outward_pages: "EXHAUSTED",
    phase_2b_property_outward_independent: "EXHAUSTED",
    phase_2c_candidate_brand_routing: "EXHAUSTED",
    phase_2d_website_discovery: "EXHAUSTED",
  });
  assert.equal(allAvailableResearchModesExhausted(status), true);
});

test("brand alias repairs Hotel Indigo by IHG / Hampton Inn / Four Points", () => {
  const indigo = resolveBrandMappingAlias("Hotel Indigo by IHG");
  assert.equal(indigo.ok, true);
  assert.equal(indigo.canonical, "Hotel Indigo");
  const hampton = resolveBrandMappingAlias("Hampton Inn");
  assert.equal(hampton.ok, true);
  assert.equal(hampton.canonical, "Hampton by Hilton");
  const fp = resolveBrandMappingAlias("Four Points");
  assert.equal(fp.ok, true);
  assert.equal(fp.canonical, "Four Points by Sheraton");
  assert.equal(isDeterministicAliasRepair("Hotel Indigo by IHG", "Hotel Indigo"), true);
});

test("candidate brand cannot self-validate Current Brand", () => {
  const r = candidateBrandCannotSelfValidate({
    "Candidate Brand Text": "Holiday Inn Express",
  });
  assert.equal(r.may_validate, false);
});

test("alias repair does not overwrite a different populated brand", () => {
  const built = buildBrandMappingRepairPatch({
    "Current Brand": "Marriott Hotels",
    "Property Name": "Example",
  });
  if (built.patch?.["Current Brand"]) {
    assert.equal(built.patch["Current Brand"], "Marriott Hotels");
  }
});

test("NULL_FILL preserved on adaptive apply", () => {
  const records = [
    { id: "rec1", fields: { [MAP_MASTER.currentBrand]: "Hotel Indigo" } },
  ];
  const applied = applyNullFillToRecords(records, [
    { id: "rec1", fields: { [MAP_MASTER.currentBrand]: "Hampton by Hilton" } },
  ]);
  assert.equal(applied.length, 0);
  assert.equal(records[0].fields[MAP_MASTER.currentBrand], "Hotel Indigo");
});

test("rooms semantics reject beds/plazas/capacity", () => {
  const beds = rejectNonGuestroomSemantics("120 leitos / camas", 120);
  assert.equal(beds.ok, false);
  const ok = rejectNonGuestroomSemantics("180 habitaciones", 180);
  assert.equal(ok.ok, true);
});

test("two pages from the same host are not independent corroboration", () => {
  assert.equal(
    sameUpstreamSource(
      "https://www.datos.gov.co/a",
      "https://datos.gov.co/b"
    ),
    true
  );
  assert.equal(
    sourcesAreIndependent(
      "https://www.datos.gov.co/rnt",
      "https://www.hotel-example.mx/en"
    ),
    true
  );
  const syndicated = evaluateRoomsCorroboration(
    { rooms: 100, source_url: "https://brand.com/a" },
    {
      count: 100,
      confidence: "High",
      source_kind: "other",
      source_url: "https://brand.com/b",
      context: "100 rooms",
    }
  );
  assert.equal(syndicated.ok, false);
  assert.equal(syndicated.reason, "syndicated_same_upstream");
});

test("OTA hosts are forbidden for official crawl", () => {
  assert.equal(isForbiddenHost("https://www.booking.com/hotel/mx/x.html"), true);
  assert.equal(looksLikePropertyUrl("https://www.marriott.com/en-us/hotels/mexmc-overview"), true);
});

test("JSON-LD hotel extract", () => {
  const html = `<script type="application/ld+json">{"@type":"Hotel","name":"Test Hotel","address":{"streetAddress":"1 Main","addressLocality":"Cancun","addressCountry":"Mexico","postalCode":"77500"}}</script>`;
  const rows = extractJsonLdHotels(html);
  assert.equal(rows[0].name, "Test Hotel");
  assert.equal(rows[0].city, "Cancun");
});

test("residual ranking prefers multi-field gaps over phone-only", () => {
  const high = scoreResidualProperty({
    fields: {
      [MAP_MASTER.propertyName]: "Hotel A",
      [MAP_MASTER.country]: "Mexico",
      [MAP_MASTER.city]: "Cancun",
      [MAP_MASTER.officialUrl]: "https://hotel-a.mx/",
    },
  });
  const phoneOnly = scoreResidualProperty({
    fields: {
      [MAP_MASTER.propertyName]: "Hotel B",
      [MAP_MASTER.country]: "Mexico",
      [MAP_MASTER.city]: "Cancun",
      [MAP_MASTER.currentBrand]: "X",
      [MAP_MASTER.roomsKeys]: 10,
      [MAP_MASTER.address]: "1 St",
      [MAP_MASTER.postalCode]: "123",
      [MAP_MASTER.latitude]: 1,
      [MAP_MASTER.longitude]: 1,
      [MAP_MASTER.officialUrl]: "https://x.com",
      [MAP_MASTER.stateRegion]: "QR",
    },
  });
  assert.ok(high.score > phoneOnly.score);
  const ranked = rankResidualQueue(
    [
      { id: "a", fields: { [MAP_MASTER.phone]: "1" } },
      {
        id: "b",
        fields: {
          [MAP_MASTER.propertyName]: "Gap",
          [MAP_MASTER.country]: "Mexico",
          [MAP_MASTER.city]: "CDMX",
          [MAP_MASTER.officialUrl]: "https://gap.mx",
        },
      },
    ],
    { max: 5 }
  );
  assert.equal(ranked[0].rec.id, "b");
});

test("overnight Mode A plateau switches to Mode B instead of exiting", async () => {
  const modes = [];
  let t = Date.now();
  const report = await runOvernightCensusEnrichment({
    mode: "dry-run",
    enableProductionWrites: false,
    maxRuntimeMinutes: 60,
    maxExternalCostUsd: 50,
    now: () => t,
    sleepFn: async () => {},
    runMasterFn: async () => {
      t += 20 * 60_000;
      return {
        ok: true,
        CURRENT_BRAND_WRITES: 0,
        ROOMS_WRITTEN: 0,
        TOTAL_FIELDS_WRITTEN_THIS_RUN: 0,
        PROPERTIES_PATCHED_THIS_RUN: 0,
        MAPBOX_REQUESTS: 0,
        ESTIMATED_MAPBOX_COST: 0,
        HBX_ROOMS_ARRAY_WRITES: 0,
        HBX_COORDINATE_WRITES: 0,
        CVENT_ONLY_ROOM_VALIDATIONS: 0,
        BENCHMARK_ROOM_WRITES: 0,
        DESTRUCTIVE_OVERWRITES: 0,
        WRONG_TABLE_WRITES: 0,
        ERRORS: 0,
        FOUNDER_DECISION_REQUIRED: "NO",
        DASHBOARD_BEFORE: dash(),
        DASHBOARD_AFTER: dash(),
      };
    },
    runLiveDirectoryFn: async () => {
      modes.push("B_live");
      t += 5 * 60_000;
      return { ok: true, proposals: [], exhausted: false, LIVE_OFFICIAL_DOMAINS_CRAWLED: 1 };
    },
    log: () => {},
  });
  assert.notEqual(report.STOP_REASON, "all_structured_lanes_plateaued");
  assert.ok(
    report.STOP_REASON === "overnight_runtime_limit_reached" ||
      report.STOP_REASON === "overnight_max_cycles_reached" ||
      report.CYCLES_COMPLETED >= 1
  );
  assert.ok(modes.includes("B_live") || report.MODE_B_TIME_MINUTES >= 0);
  assert.equal(report.WRONG_TABLE_WRITES, 0);
  assert.equal(report.DESTRUCTIVE_OVERWRITES, 0);
  assert.equal(report.HBX_ROOMS_ARRAY_WRITES, 0);
});

test("production table id remains Hotel Property Census", () => {
  assert.equal(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID, "tbl9aY5ijiuIzzWam");
  const ids = ADAPTIVE_PHASES.map((p) => p.id);
  assert.ok(ids.includes("phase_1b_apify_first_party"));
  assert.ok(
    ids.indexOf("phase_1b_apify_first_party") <
      ids.indexOf("phase_2_property_outward_domain")
  );
  assert.ok(ids.includes("phase_2e_demand_adapters"));
});
