#!/usr/bin/env node
/**
 * Multi-provider census validation synthesis.
 *
 * Read-only: combines existing CALA / StayingAPI / SerpApi validation artifacts.
 * Does NOT call external APIs or write Airtable.
 *
 * Usage: node scripts/hotel-intelligence-multi-provider-synthesis.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/multi-provider-validation-v1"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function pct(n, d) {
  if (!d) return 0;
  return Math.round((n / d) * 1000) / 10;
}
function present(k) {
  return Boolean(String(process.env[k] || "").trim());
}

const cala = readJson(
  path.join(ROOT, "reports/hotel-intelligence/cala-validation-v1/12-summary.json")
);
const staying = readJson(
  path.join(ROOT, "reports/hotel-intelligence/stayingapi-validation-v1/05-summary.json")
);
const serp = readJson(
  path.join(ROOT, "reports/hotel-intelligence/serpapi-validation-v1/05-summary.json")
);

const LIVE = {
  hotels: 5956,
  rooms_missing: 5765,
  brand_missing: 3856,
  coords_missing: 5120,
  website_missing: 2011,
  phone_missing: 2553,
  hbx_linked: 3016,
};

const SAMPLE_GAPS = {
  address: 299,
  coordinates: 351,
  rooms: 375,
  brand: 104,
  parent: 95,
  website: 38,
  phone: 295,
};

const serpAddrRecovered = SAMPLE_GAPS.address - (serp.field_recovery_attempted_subset?.address_line_1?.still_missing ?? SAMPLE_GAPS.address);
const serpCoordRecovered = SAMPLE_GAPS.coordinates - (serp.field_recovery_attempted_subset?.latitude?.still_missing ?? SAMPLE_GAPS.coordinates);
const serpPhoneRecovered = SAMPLE_GAPS.phone - (serp.field_recovery_attempted_subset?.phone?.still_missing ?? SAMPLE_GAPS.phone);
const serpWebRecovered = SAMPLE_GAPS.website - (serp.field_recovery_attempted_subset?.website?.still_missing ?? SAMPLE_GAPS.website);

const serpAddrRate = serpAddrRecovered / SAMPLE_GAPS.address;
const serpCoordRate = serpCoordRecovered / SAMPLE_GAPS.coordinates;
const serpPhoneRate = serpPhoneRecovered / SAMPLE_GAPS.phone;
const serpWebRate = serpWebRecovered / SAMPLE_GAPS.website;

const audit = {
  marker: "HOTEL_INTELLIGENCE_PROVIDER_AUDIT_COMPLETE",
  table: [
    {
      provider: "Hotelbeds",
      existing_code: true,
      path: "lib/hotel-intelligence/providers/hotelbeds.js + lib/research-engine-v2/hbx-*",
      credentials_configured: present("API_KEY") || present("HOTELBEDS_API_KEY") || present("HBX_API_KEY") || present("APIKEY"),
      mcp_connection: "via Hotel Intelligence MCP (hotelbeds provider)",
      used_by_hotel_intelligence: true,
      tests: "test:hotel-intelligence; hbx inventory scripts",
      reachable_at_last_cala_run: "quota_exhausted (TEST_DAILY_QUOTA_EXHAUSTED)",
    },
    {
      provider: "StayingAPI",
      existing_code: true,
      path: "lib/research-engine-v2/providers/staying-api/* + lib/hotel-intelligence/providers/stayingapi.js",
      credentials_configured: present("STAYINGAPI_KEY"),
      mcp_connection: "via Hotel Intelligence MCP (stayingapi provider); no Cursor StayingAPI MCP",
      used_by_hotel_intelligence: true,
      tests: "test:hotel-intelligence-stayingapi; stayingapi:benchmark-v1",
      reachable_at_last_run: "yes (Free/Sandbox; rate limits + weak identity)",
    },
    {
      provider: "SerpApi",
      existing_code: true,
      path: "lib/research-engine-v2/providers/serpapi-google-hotels/* + lib/hotel-intelligence/providers/serpapi.js",
      credentials_configured: present("SERPAPI_KEY") || present("SERPAPI_API_KEY"),
      mcp_connection: "via Hotel Intelligence MCP (serpapi provider); no Cursor SerpApi MCP",
      used_by_hotel_intelligence: true,
      tests: "test:hotel-intelligence-serpapi; serpapi:benchmark-v1; test:serpapi-key-env-canonical",
      reachable_at_last_run: "yes (Production Plan)",
    },
    {
      provider: "HotelAPI.co",
      existing_code: false,
      path: null,
      credentials_configured: false,
      mcp_connection: "none",
      used_by_hotel_intelligence: false,
      tests: "none",
      reachable_at_last_run: "docs-only audit",
    },
  ],
};

const capabilityMatrix = {
  note: "CONFIRMED from existing code/docs/observed runs; UNKNOWN where not evidenced",
  fields: {
    hotel_name: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "CONFIRMED_SUPPORTED",
      serpapi: "CONFIRMED_SUPPORTED",
      hotelapi_co_free: "CONFIRMED_SUPPORTED",
    },
    address: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "CONFIRMED_SUPPORTED",
      serpapi: "CONFIRMED_SUPPORTED",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    city_state_postal_country: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "CONFIRMED_SUPPORTED",
      serpapi: "SUPPORTED_INDIRECTLY",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    latitude_longitude: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "CONFIRMED_SUPPORTED",
      serpapi: "CONFIRMED_SUPPORTED",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    TOTAL_PROPERTY_ROOM_COUNT: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "NOT_SUPPORTED",
      serpapi: "NOT_SUPPORTED",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    room_types: {
      dealality_census: "UNKNOWN",
      hotelbeds: "SUPPORTED_INDIRECTLY",
      stayingapi: "SUPPORTED_INDIRECTLY",
      serpapi: "SUPPORTED_INDIRECTLY",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    brand: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "SUPPORTED_INDIRECTLY",
      stayingapi: "NOT_SUPPORTED",
      serpapi: "NOT_SUPPORTED",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    parent_company: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "SUPPORTED_INDIRECTLY",
      stayingapi: "NOT_SUPPORTED",
      serpapi: "NOT_SUPPORTED",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    website: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "SUPPORTED_INDIRECTLY",
      serpapi: "SUPPORTED_INDIRECTLY",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    phone: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "NOT_SUPPORTED",
      serpapi: "CONFIRMED_SUPPORTED",
      hotelapi_co_free: "NOT_SUPPORTED",
    },
    rates: {
      dealality_census: "NOT_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "SUPPORTED_INDIRECTLY",
      serpapi: "CONFIRMED_SUPPORTED",
      hotelapi_co_free: "CONFIRMED_SUPPORTED",
    },
    persistent_provider_id: {
      dealality_census: "CONFIRMED_SUPPORTED",
      hotelbeds: "CONFIRMED_SUPPORTED",
      stayingapi: "CONFIRMED_SUPPORTED",
      serpapi: "CONFIRMED_SUPPORTED",
      hotelapi_co_free: "CONFIRMED_SUPPORTED",
    },
    booking_com_id: {
      dealality_census: "NOT_SUPPORTED",
      hotelbeds: "UNKNOWN",
      stayingapi: "CONFIRMED_SUPPORTED",
      serpapi: "NOT_SUPPORTED",
      hotelapi_co_free: "UNKNOWN",
    },
    google_place_id_cid_data_id: {
      dealality_census: "NOT_SUPPORTED",
      hotelbeds: "NOT_SUPPORTED",
      stayingapi: "NOT_SUPPORTED",
      serpapi: "NOT_SUPPORTED",
      hotelapi_co_free: "NOT_SUPPORTED",
      note: "SerpApi Maps docs support these; HI adapter uses google_hotels only (property_token)",
    },
    google_hotels_property_token: {
      serpapi: "CONFIRMED_SUPPORTED",
      others: "NOT_SUPPORTED",
    },
  },
  room_count_verdicts: {
    HOTELBEDS_TOTAL_PROPERTY_ROOM_COUNT: "SUPPORTED",
    STAYINGAPI_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    SERPAPI_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    HOTELAPI_CO_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    sample_measurement: "ROOM_COUNT_PROVIDER_GAP_CONFIRMED for frozen CALA run (HBX quota_exhausted; Staying/Serp return 0 keys)",
  },
};

const independentYield = {
  hotelbeds: {
    hotels_attempted: 0,
    hotels_matched: 0,
    api_calls: 0,
    status: "quota_exhausted",
    message: "TEST_DAILY_QUOTA_EXHAUSTED",
    fields_returned: {},
    note: "Identity validated; content enrich not measured on frozen sample",
    census_hbx_codes_present_live: LIVE.hbx_linked,
  },
  stayingapi: {
    hotels_attempted: staying.frozen_400?.enrich_attempted ?? 15,
    hotels_matched_exact_high: 0,
    api_calls: staying.efficiency?.total_stayingapi_calls ?? 15,
    fields_recovered: {
      address: 0,
      coordinates: 0,
      phone: 0,
      website: 0,
      rooms: 0,
      brand: 0,
    },
    recommendation: staying.recommendation,
  },
  serpapi: {
    hotels_attempted: 400,
    hotels_matched_exact_high: serp.frozen_400?.enrich_matched_exact_or_high ?? 129,
    api_calls: serp.efficiency?.total_serpapi_calls ?? 404,
    account_delta_searches: serp.credits?.account_delta ?? 380,
    fields_recovered: {
      address: serpAddrRecovered,
      coordinates: serpCoordRecovered,
      phone: serpPhoneRecovered,
      website: serpWebRecovered,
      rooms: 0,
      brand: 0,
    },
    high_confidence: {
      address: serp.field_recovery_attempted_subset?.address_line_1?.high_confidence ?? 83,
      coordinates: serp.coordinate_recovery?.coords_high_confidence ?? 106,
      phone: serp.field_recovery_attempted_subset?.phone?.high_confidence ?? 4,
    },
    property_tokens: serp.external_ids?.property_tokens ?? 129,
    recommendation: serp.recommendation,
  },
  hotelapi_co_free: {
    hotels_attempted: 0,
    note: "Docs-only; not integrated",
    HOTELAPI_CO_FREE_CENSUS_VALUE: "LOW",
  },
};

const combined = {
  missing_before: SAMPLE_GAPS,
  recovered_by_hotelbeds_only: {
    address: 0,
    coordinates: 0,
    phone: 0,
    website: 0,
    rooms: 0,
    brand: 0,
    note: "HBX enrich not measured (quota)",
  },
  recovered_by_stayingapi_only: {
    address: 0,
    coordinates: 0,
    phone: 0,
    website: 0,
    rooms: 0,
    brand: 0,
  },
  recovered_by_serpapi_only: {
    address: serpAddrRecovered,
    coordinates: serpCoordRecovered,
    phone: serpPhoneRecovered,
    website: serpWebRecovered,
    rooms: 0,
    brand: 0,
  },
  recovered_by_multiple_agreeing: {
    note: "Not jointly measured this sprint (providers run independently; HBX unavailable)",
    rooms: 0,
    coordinates: 0,
  },
  conflicting_providers: {
    note: "Intra-SerpApi conflicts only (address 25, phone 16, website 36 vs existing census)",
    serpapi_address_conflicts: 25,
    serpapi_phone_conflicts: 16,
    serpapi_website_conflicts: 36,
  },
  still_missing_after_serpapi: {
    address: SAMPLE_GAPS.address - serpAddrRecovered,
    coordinates: SAMPLE_GAPS.coordinates - serpCoordRecovered,
    phone: SAMPLE_GAPS.phone - serpPhoneRecovered,
    website: SAMPLE_GAPS.website - serpWebRecovered,
    rooms: SAMPLE_GAPS.rooms,
    brand: SAMPLE_GAPS.brand,
    parent: SAMPLE_GAPS.parent,
  },
};

const projection5956 = {
  method:
    "Extrapolate SerpApi sample recovery rates × live missing counts; HBX rooms unknown until LIVE quota; StayingAPI yield ≈ 0",
  coordinates_potentially_recoverable: Math.round(LIVE.coords_missing * serpCoordRate),
  addresses_potentially_recoverable: Math.round(
    // live address missing not given; use sample rate as proxy on sparse addresses — report as sample-rate applied to coords-like gap caution
    SAMPLE_GAPS.address * (LIVE.hotels / 400) * serpAddrRate
  ),
  phones_potentially_recoverable: Math.round(LIVE.phone_missing * serpPhoneRate),
  websites_potentially_recoverable: Math.round(LIVE.website_missing * serpWebRate),
  brands_potentially_recoverable: 0,
  room_counts_potentially_recoverable_from_current_providers:
    "UNKNOWN until Hotelbeds LIVE content measured; Staying/Serp = 0",
  api_calls_serpapi_estimate: Math.round(LIVE.hotels * 0.95),
  human_review_cases_estimate: Math.round(
    LIVE.hotels * ((129 / 400) * 0.2)
  ), // ~20% of Exact/High may still need review for conflicts
  still_missing_rooms_estimate: LIVE.rooms_missing,
  caveat:
    "Linear extrapolation; Exact/High match rate 32% on CALA sample — actual recovery only on matched hotels",
};

const projection10000 = {
  coordinates_potentially_recoverable: Math.round(5120 * (10000 / 5956) * serpCoordRate),
  phones_potentially_recoverable: Math.round(2553 * (10000 / 5956) * serpPhoneRate),
  serpapi_searches_estimate: Math.round(10000 * 0.95),
  rooms_from_current_stack: "UNKNOWN / likely still gap without Hotelbeds LIVE or new rooms provider",
  caveat: "Extrapolation from 5,956 baseline gaps scaled to 10k universe size",
};

const summary = {
  marker: "DEALALITY_MULTI_PROVIDER_CENSUS_VALIDATION_COMPLETE",
  safety: {
    airtable_writes: 0,
    census_writes: 0,
    brand_explorer_writes: 0,
    automatic_merges: 0,
    schema_changes: 0,
    migrations: 0,
    secrets_exposed: false,
    ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
    synthesis_only_no_new_provider_api_calls: true,
  },
  audit,
  capability_matrix: capabilityMatrix,
  room_count: {
    HOTELBEDS_TOTAL_PROPERTY_ROOM_COUNT: "SUPPORTED",
    STAYINGAPI_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    SERPAPI_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    HOTELAPI_CO_TOTAL_PROPERTY_ROOM_COUNT: "NOT_SUPPORTED",
    frozen_sample_verdict: "ROOM_COUNT_PROVIDER_GAP_CONFIRMED",
    reason:
      "Hotelbeds is the only configured provider that can return total keys, but TEST quota was exhausted so 0 rooms recovered on frozen sample; StayingAPI/SerpApi firewalled as NOT_SUPPORTED",
  },
  integrations_reused: [
    "lib/research-engine-v2/hbx-content-api-client.js → hotelbeds provider",
    "lib/research-engine-v2/providers/staying-api/* → stayingapi provider",
    "lib/research-engine-v2/providers/serpapi-google-hotels/* → serpapi provider",
    "Frozen sample hotel-intelligence-cala-validation-v1",
  ],
  mcp_changes: {
    providers: ["dealality_census", "hotelbeds", "stayingapi", "serpapi"],
    adapters: [
      "lib/hotel-intelligence/providers/hotelbeds.js",
      "lib/hotel-intelligence/providers/stayingapi.js",
      "lib/hotel-intelligence/providers/serpapi.js",
    ],
    orchestration: "hotel_enrich isolates provider failures; multi-provider evidence staging",
  },
  controlled_10: {
    stayingapi: staying.controlled,
    serpapi: serp.controlled,
    hotelbeds: { status: "quota_exhausted", not_run: true },
  },
  frozen_400: {
    identity: {
      exact: 338,
      strong: 27,
      exact_plus_strong_pct: 91.3,
      self_match_pct: 100,
    },
    independent_yield: independentYield,
    combined,
  },
  field_recovery_table: [
    {
      field: "address",
      missing_before: 299,
      hbx: 0,
      stayingapi: 0,
      serpapi: serpAddrRecovered,
      combined: serpAddrRecovered,
      still_missing: 299 - serpAddrRecovered,
      recovery_pct: pct(serpAddrRecovered, 299),
    },
    {
      field: "coordinates",
      missing_before: 351,
      hbx: 0,
      stayingapi: 0,
      serpapi: serpCoordRecovered,
      combined: serpCoordRecovered,
      still_missing: 351 - serpCoordRecovered,
      recovery_pct: pct(serpCoordRecovered, 351),
    },
    {
      field: "phone",
      missing_before: 295,
      hbx: 0,
      stayingapi: 0,
      serpapi: serpPhoneRecovered,
      combined: serpPhoneRecovered,
      still_missing: 295 - serpPhoneRecovered,
      recovery_pct: pct(serpPhoneRecovered, 295),
    },
    {
      field: "website",
      missing_before: 38,
      hbx: 0,
      stayingapi: 0,
      serpapi: serpWebRecovered,
      combined: serpWebRecovered,
      still_missing: 38 - serpWebRecovered,
      recovery_pct: pct(serpWebRecovered, 38),
    },
    {
      field: "room_count",
      missing_before: 375,
      hbx: 0,
      stayingapi: 0,
      serpapi: 0,
      combined: 0,
      still_missing: 375,
      recovery_pct: 0,
    },
    {
      field: "brand",
      missing_before: 104,
      hbx: 0,
      stayingapi: 0,
      serpapi: 0,
      combined: 0,
      still_missing: 104,
      recovery_pct: 0,
    },
    {
      field: "parent_company",
      missing_before: 95,
      hbx: 0,
      stayingapi: 0,
      serpapi: 0,
      combined: 0,
      still_missing: 95,
      recovery_pct: 0,
    },
  ],
  coordinate_recovery: {
    sample_missing: 351,
    serpapi_recovered: serpCoordRecovered,
    serpapi_high_confidence: serp.coordinate_recovery?.coords_high_confidence ?? 106,
    multi_provider_supported: 0,
    conflicts: serp.coordinate_recovery?.coords_conflicts ?? 0,
    still_missing: 351 - serpCoordRecovered,
    distance_when_both_present: serp.coordinate_recovery?.distance_when_both_present || {},
  },
  external_identity_graph: {
    "Dealality ↔ Hotelbeds": LIVE.hbx_linked + " live codes; sample enrich not measured",
    "Dealality ↔ StayingAPI": 0,
    "Dealality ↔ Booking.com": 0,
    "Dealality ↔ Google Place": 0,
    "Dealality ↔ Google Hotels property_token": serp.external_ids?.property_tokens ?? 129,
    "Hotelbeds ↔ StayingAPI": 0,
    "Hotelbeds ↔ SerpApi": serp.cross_provider_linkage?.hotelbeds_serpapi ?? 39,
    "StayingAPI ↔ Google": 0,
  },
  api_efficiency: {
    serpapi: {
      calls: serp.efficiency?.total_serpapi_calls ?? 404,
      account_delta: serp.credits?.account_delta ?? 380,
      hotels_matched: 129,
      useful_missing_fields_approx:
        serpAddrRecovered + serpCoordRecovered + serpPhoneRecovered + serpWebRecovered,
      useful_fields_per_call: pct(
        serpAddrRecovered + serpCoordRecovered + serpPhoneRecovered + serpWebRecovered,
        serp.efficiency?.total_serpapi_calls || 404
      ) / 100,
      calls_per_enriched_hotel: pct(404, 129) / 100,
    },
    stayingapi: {
      calls: 15,
      hotels_matched: 0,
      useful_fields_per_call: 0,
    },
    hotelbeds: {
      calls: 0,
      status: "quota_exhausted",
    },
  },
  best_provider_by_field: {
    hotel_identity: {
      primary: "dealality_census",
      fallback: "serpapi",
      confidence: "high",
      reason: "91.3% Exact+Strong self-resolve; SerpApi corroborates 32%",
    },
    address: {
      primary: "serpapi",
      fallback: "hotelbeds",
      confidence: "high_when_exact_high_match",
      reason: "27.8% missing-address recovery on sample; HBX unmeasured",
    },
    coordinates: {
      primary: "serpapi",
      fallback: "hotelbeds",
      confidence: "high_when_exact_high_match",
      reason: "30.2% missing-coord recovery; agreement <500m strong",
    },
    room_count: {
      primary: "hotelbeds",
      fallback: "official_site / future rooms provider",
      confidence: "supported_in_adapter_unmeasured_due_to_quota",
      reason: "Only configured provider with total-keys field; Staying/Serp NOT_SUPPORTED",
    },
    brand: {
      primary: "dealality_census",
      fallback: "brand_directory",
      confidence: "high",
      reason: "External OTAs/Google Hotels not Brand Explorer SoT",
    },
    parent_company: {
      primary: "dealality_census",
      fallback: "brand_directory",
      confidence: "high",
      reason: "Same as brand",
    },
    phone: {
      primary: "serpapi",
      fallback: "hotelbeds",
      confidence: "probable_to_high",
      reason: "26.8% missing-phone recovery on sample",
    },
    website: {
      primary: "dealality_census",
      fallback: "serpapi",
      confidence: "probable",
      reason: "Classify Serp links; avoid OTA-as-official",
    },
    property_type: {
      primary: "dealality_census",
      fallback: "hotelbeds",
      confidence: "medium",
      reason: "SerpApi type is useful input, not auto-map",
    },
    star_rating: {
      primary: "serpapi",
      fallback: "hotelbeds",
      confidence: "useful_input_not_str_scale",
      reason: "hotel_class raw only; no STR Chain Scale auto-map",
    },
    rates: {
      primary: "hotelbeds",
      fallback: "serpapi",
      confidence: "n/a_for_census",
      reason: "Rates are not a census fill priority",
    },
  },
  recommended_waterfall: [
    "1. Load Dealality census evidence + dhl_ mapping (no external call)",
    "2. If HBX Hotel Code present and Hotelbeds LIVE → content for rooms/address/geo/phone",
    "3. For remaining missing coords/address/phone → SerpApi Google Hotels search (1 call; details only if thin)",
    "4. Skip StayingAPI for bulk census (0 Exact/High on CALA validation)",
    "5. Do not call SerpApi when target fields already present at high confidence unless validation requested",
    "6. Stage evidence only; human review for conflicts / >500m coord deltas / ambiguous identity",
    "7. Rooms still missing after HBX → dedicated rooms-provider search (gap confirmed)",
  ],
  projection_5956: projection5956,
  projection_10000: projection10000,
  hotelapi_co_decision: "DO_NOT_INTEGRATE_HOTELAPI_CO",
  overall_decision: "PROVIDER_STACK_READY_EXCEPT_ROOM_COUNT",
  highest_value_next_step:
    "Restore LIVE Hotelbeds content access and measure rooms/keys yield on the same frozen 400 sample — that is the only current path to close the Rooms/Keys gap.",
  source_artifacts: [
    "reports/hotel-intelligence/cala-validation-v1/",
    "reports/hotel-intelligence/stayingapi-validation-v1/",
    "reports/hotel-intelligence/serpapi-validation-v1/",
  ],
};

writeJson(path.join(OUT_DIR, "00-provider-audit.json"), audit);
writeJson(path.join(OUT_DIR, "01-capability-matrix.json"), capabilityMatrix);
writeJson(path.join(OUT_DIR, "02-combined-yield.json"), {
  independentYield,
  combined,
  field_recovery_table: summary.field_recovery_table,
});
writeJson(path.join(OUT_DIR, "03-summary.json"), summary);

const md = `# Multi-Provider Census Validation

\`DEALALITY_MULTI_PROVIDER_CENSUS_VALIDATION_COMPLETE\`

## Overall decision
**PROVIDER_STACK_READY_EXCEPT_ROOM_COUNT**

## Room count
\`ROOM_COUNT_PROVIDER_GAP_CONFIRMED\` (frozen sample)  
Hotelbeds = SUPPORTED in adapter but quota_exhausted · StayingAPI/SerpApi/HotelAPI.co = NOT_SUPPORTED

## Field recovery (400)

| Field | Missing Before | HBX | StayingAPI | SerpApi | Combined | Still Missing | Recovery % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
${summary.field_recovery_table
  .map(
    (r) =>
      `| ${r.field} | ${r.missing_before} | ${r.hbx} | ${r.stayingapi} | ${r.serpapi} | ${r.combined} | ${r.still_missing} | ${r.recovery_pct}% |`
  )
  .join("\n")}

## HotelAPI.co
**DO_NOT_INTEGRATE_HOTELAPI_CO** (LOW census value)

## Highest-value next step
${summary.highest_value_next_step}

## Safety
Airtable/Census/Brand Explorer writes: 0 · Secrets exposed: no · Synthesis from prior read-only runs
`;

fs.writeFileSync(path.join(OUT_DIR, "MULTI_PROVIDER_VALIDATION_REPORT.md"), md, "utf8");

console.log("HOTEL_INTELLIGENCE_PROVIDER_AUDIT_COMPLETE");
console.log("DEALALITY_MULTI_PROVIDER_CENSUS_VALIDATION_COMPLETE");
console.log(
  JSON.stringify(
    {
      overall_decision: summary.overall_decision,
      room_gap: summary.room_count.frozen_sample_verdict,
      hotelapi: summary.hotelapi_co_decision,
      serpapi_exact_high: 129,
      coord_recovery_pct: pct(serpCoordRecovered, 351),
      next: summary.highest_value_next_step,
    },
    null,
    2
  )
);
