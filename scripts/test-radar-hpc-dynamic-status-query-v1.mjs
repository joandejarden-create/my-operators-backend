#!/usr/bin/env node
/**
 * HE-001: dynamic HPC Radar Status query — fixtures only, no live Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import {
  buildHpcRadarStatusFilterFormula,
  buildHotelIdentityKeys,
  clearRadarHpcStatusCache,
  fetchHpcRadarStatusEligibleRecords,
  formatHotelPropertyCensusAsBrandPresenceHotel,
  HPC_SELECT_FIELDS,
  isAuthorizedForRadarInternalPreviewHotels,
  loadAuthorizedInternalPreviewHotels,
  mergeInternalPreviewHotelsIntoBrandPresenceList,
  RADAR_HPC_STATUS_CACHE_POLICY,
  RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
  resolveRadarHpcStatusCacheTtlMs,
  resolveRadarStatusFromHpcFields,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const selectFixture = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "fixtures/hotel-census/hpc-radar-select-fields-v1.json"),
    "utf8"
  )
);
assert.deepEqual(
  [...HPC_SELECT_FIELDS],
  selectFixture.selectFields,
  "HPC_SELECT_FIELDS must match verified live HPC schema field names"
);
for (const alias of selectFixture.rejectedAliasSelectFields) {
  assert.equal(
    HPC_SELECT_FIELDS.includes(alias),
    false,
    `alias must not be selected: ${alias}`
  );
}

const BETHESDA = "recLuxvwwxID7U2B8";
const WATERSTONE = "recgMYovrrZDJMqzX";
const MAYFLOWER = "rec0eTEnCqlsGJJt9";

function fixtureRecord(id, name, status, coords, extra = {}) {
  return {
    id,
    fields: {
      "Property Name": name,
      City: extra.city || "Test City",
      Country: extra.country || "United States",
      "Current Brand": extra.brand || "Test Brand",
      "Parent Company": extra.parentCompany || "Test Parent",
      Latitude: coords?.lat,
      Longitude: coords?.lng,
      "Rooms / Keys": extra.rooms || 100,
      "Radar Status": status,
      "Production Use Status":
        extra.productionUseStatus || "Census Only / Not Owner-Facing",
      "Short Property Summary": extra.description || `${name} summary`,
      Amenities: extra.amenities || "Pool",
      "Total Meeting Space": extra.meeting || 5000,
      Address: extra.address || "1 Main St",
      ...extra.fields,
    },
  };
}

const FIXTURES = {
  bethesdaOpen: fixtureRecord(
    BETHESDA,
    "Bethesda Marriott",
    "Open",
    { lat: 39.016519, lng: -77.103463 },
    { city: "Bethesda", brand: "Marriott Hotels", rooms: 407 }
  ),
  waterstoneOpen: fixtureRecord(
    WATERSTONE,
    "Waterstone Resort & Marina",
    "Open",
    { lat: 26.358, lng: -80.077 },
    { city: "Boca Raton", brand: "Independent", rooms: 139 }
  ),
  mayflowerPipeline: fixtureRecord(
    MAYFLOWER,
    "The Mayflower Hotel",
    "Pipeline",
    { lat: 38.904, lng: -77.04 },
    { city: "Washington", brand: "Autograph Collection", rooms: 657 }
  ),
  mayflowerOpen: fixtureRecord(
    MAYFLOWER,
    "The Mayflower Hotel",
    "Open",
    { lat: 38.904, lng: -77.04 },
    { city: "Washington", brand: "Autograph Collection", rooms: 657 }
  ),
  mayflowerHidden: fixtureRecord(
    MAYFLOWER,
    "The Mayflower Hotel",
    "Hidden",
    { lat: 38.904, lng: -77.04 },
    { city: "Washington" }
  ),
  mayflowerBlank: {
    id: MAYFLOWER,
    fields: {
      "Property Name": "The Mayflower Hotel",
      Latitude: 38.904,
      Longitude: -77.04,
    },
  },
  missingCoords: fixtureRecord(
    "recMISS1111111111",
    "No Coord Hotel",
    "Open",
    { lat: null, lng: null }
  ),
  missingIdentity: {
    id: "not-a-rec",
    fields: {
      "Property Name": "Bad Id Hotel",
      "Radar Status": "Open",
      Latitude: 1,
      Longitude: 2,
    },
  },
};

clearRadarHpcStatusCache();

assert.equal(RADAR_INTERNAL_PREVIEW_HOTELS_VERSION, "radar_hpc_dynamic_status_v1");
assert.equal(RADAR_HPC_STATUS_CACHE_POLICY.ttlMsDefault, 45_000);
assert.equal(resolveRadarHpcStatusCacheTtlMs({}), 45_000);
assert.equal(resolveRadarHpcStatusCacheTtlMs({ RADAR_HPC_STATUS_CACHE_TTL_MS: "12000" }), 12000);
assert.match(buildHpcRadarStatusFilterFormula(), /OR\(\{Radar Status\}='Open'/);

// --- Auth ---
assert.equal(isAuthorizedForRadarInternalPreviewHotels({ isAdmin: false }, {}), false);
assert.equal(isAuthorizedForRadarInternalPreviewHotels({ isAdmin: true }, {}), true);
assert.equal(
  isAuthorizedForRadarInternalPreviewHotels(
    { isAdmin: false, workspaceAccess: ["Owner", "Operator"] },
    {}
  ),
  true,
  "Owner/Operator workspace principal may load HPC inventory"
);
const denied = await loadAuthorizedInternalPreviewHotels({
  dealalityUser: { isAdmin: false },
  records: [FIXTURES.bethesdaOpen],
  bypassCache: true,
});
assert.equal(denied.unauthorized, true);
assert.equal(denied.hotels.length, 0);

// --- Core trio + Pipeline distinguishable ---
const loaded = await loadAuthorizedInternalPreviewHotels({
  dealalityUser: { isAdmin: true },
  records: [
    FIXTURES.bethesdaOpen,
    FIXTURES.waterstoneOpen,
    FIXTURES.mayflowerPipeline,
    FIXTURES.mayflowerHidden,
    FIXTURES.mayflowerBlank,
    FIXTURES.missingCoords,
    FIXTURES.missingIdentity,
  ],
  bypassCache: true,
});
assert.equal(loaded.ok, true);
assert.equal(loaded.diagnostics.openCount, 3, "Bethesda+Waterstone+missingCoords counted Open");
assert.equal(loaded.diagnostics.pipelineCount, 1);
assert.ok(loaded.diagnostics.missingCoordinates.some((m) => m.id === "recMISS1111111111"));
assert.ok(loaded.diagnostics.missingIdentity.some((m) => m.reason === "invalid_or_missing_record_id"));
assert.ok(loaded.hotels.every((h) => h.status === "Open" || h.status === "Pipeline"));
assert.ok(loaded.hotels.every((h) => h.internalPreviewOnly === true));
assert.ok(!loaded.hotels.some((h) => h.id === MAYFLOWER && h.status === "Hidden"));
assert.ok(loaded.hotels.some((h) => h.id === BETHESDA && h.status === "Open"));
assert.ok(loaded.hotels.some((h) => h.id === WATERSTONE && h.status === "Open"));
assert.ok(loaded.hotels.some((h) => h.id === MAYFLOWER && h.status === "Pipeline"));
assert.ok(
  !loaded.hotels.some((h) => h.id === "recMISS1111111111"),
  "missing coords excluded from map hotels"
);

const pipelineHotel = loaded.hotels.find((h) => h.id === MAYFLOWER);
assert.equal(pipelineHotel.status, "Pipeline");
assert.equal(pipelineHotel.radarStatus, "Pipeline");
assert.notEqual(pipelineHotel.status, "Open", "Pipeline visually/status-distinct from Open");

// --- Status transitions (fixtures only) ---
for (const [label, rec, expectVisible] of [
  ["open", FIXTURES.mayflowerOpen, true],
  ["pipeline", FIXTURES.mayflowerPipeline, true],
  ["hidden", FIXTURES.mayflowerHidden, false],
  ["blank", FIXTURES.mayflowerBlank, false],
]) {
  const result = await loadAuthorizedInternalPreviewHotels({
    dealalityUser: { isAdmin: true },
    records: [rec],
    bypassCache: true,
  });
  const visible = result.hotels.some((h) => h.id === MAYFLOWER);
  assert.equal(visible, expectVisible, `transition ${label}`);
  assert.equal(
    resolveRadarStatusFromHpcFields(rec.fields),
    expectVisible ? (label === "pipeline" ? "Pipeline" : "Open") : null
  );
}

// --- Duplicate identity: same id ---
const idDup = mergeInternalPreviewHotelsIntoBrandPresenceList(
  [
    {
      id: WATERSTONE,
      name: "Waterstone from Hotel Census",
      status: "Open",
      lat: 26.358,
      lng: -80.077,
    },
  ],
  [formatHotelPropertyCensusAsBrandPresenceHotel(FIXTURES.waterstoneOpen)]
);
assert.equal(idDup.hotels.length, 1);
assert.equal(idDup.addedIds.length, 0);
assert.ok(idDup.duplicatesReconciled.length >= 1);

// --- Duplicate identity: geo+name fingerprint across different ids ---
const geoDup = mergeInternalPreviewHotelsIntoBrandPresenceList(
  [
    {
      id: "recLEGACY11111111",
      name: "Bethesda Marriott",
      status: "Open",
      lat: 39.016519,
      lng: -77.103463,
    },
  ],
  [formatHotelPropertyCensusAsBrandPresenceHotel(FIXTURES.bethesdaOpen)]
);
assert.equal(geoDup.hotels.length, 1, "legacy census preserved; HPC skipped");
assert.equal(geoDup.hotels[0].id, "recLEGACY11111111");
assert.ok(geoDup.duplicatesReconciled.some((d) => d.hpcId === BETHESDA));

const keys = buildHotelIdentityKeys(
  formatHotelPropertyCensusAsBrandPresenceHotel(FIXTURES.bethesdaOpen)
);
assert.ok([...keys].some((k) => k.startsWith("id:")));
assert.ok([...keys].some((k) => k.startsWith("geo:")));

// --- Legacy Hotel Census continuity ---
const legacy = [
  {
    id: "recCENSUSCONTINU01",
    name: "Casas Del XVI",
    status: "Open",
    lat: 18.47,
    lng: -69.9,
    brand: "Independent",
  },
];
const cont = mergeInternalPreviewHotelsIntoBrandPresenceList(legacy, loaded.hotels);
assert.ok(cont.hotels.some((h) => h.id === "recCENSUSCONTINU01"));
assert.ok(cont.hotels.some((h) => h.id === BETHESDA));
assert.equal(
  cont.hotels.find((h) => h.id === "recCENSUSCONTINU01").name,
  "Casas Del XVI"
);

// --- Pagination completeness via mock Airtable base ---
function mockBase(pages, { failTimes = 0, rateLimit = false } = {}) {
  let failsLeft = failTimes;
  return (tableName) => {
    assert.equal(tableName, "Hotel Property Census");
    return {
      select(opts) {
        assert.match(String(opts.filterByFormula || ""), /Open/);
        assert.ok(opts.pageSize <= 100);
        return {
          async eachPage(cb) {
            if (failsLeft > 0) {
              failsLeft -= 1;
              const err = new Error(rateLimit ? "429 rate limit" : "boom");
              err.statusCode = rateLimit ? 429 : 500;
              throw err;
            }
            for (let i = 0; i < pages.length; i++) {
              await new Promise((resolve, reject) => {
                try {
                  cb(pages[i], () => resolve());
                } catch (e) {
                  reject(e);
                }
              });
            }
          },
        };
      },
    };
  };
}

const paged = await fetchHpcRadarStatusEligibleRecords({
  airtableBase: mockBase([
    [FIXTURES.bethesdaOpen],
    [FIXTURES.waterstoneOpen, FIXTURES.mayflowerPipeline],
  ]),
});
assert.equal(paged.records.length, 3);
assert.equal(paged.diagnostics.paginationComplete, true);
assert.equal(paged.diagnostics.pageCount, 2);

const rateLimitedThenOk = await fetchHpcRadarStatusEligibleRecords({
  airtableBase: mockBase([[FIXTURES.bethesdaOpen]], {
    failTimes: 1,
    rateLimit: true,
  }),
  maxRetries: 3,
});
assert.equal(rateLimitedThenOk.records.length, 1);
assert.equal(rateLimitedThenOk.diagnostics.paginationComplete, true);

const hardFail = await fetchHpcRadarStatusEligibleRecords({
  airtableBase: mockBase([[FIXTURES.bethesdaOpen]], {
    failTimes: 5,
    rateLimit: false,
  }),
  maxRetries: 1,
});
assert.equal(hardFail.records.length, 0);
assert.equal(hardFail.diagnostics.paginationComplete, false);
assert.ok(hardFail.errors.some((e) => e.reason === "hpc_formula_fetch_failed"));

function mockUnknownFieldBase() {
  return () => ({
    select() {
      return {
        async eachPage() {
          const err = new Error(
            "UNKNOWN_FIELD_NAME: Field \"Total Meeting Space\" does not exist"
          );
          err.statusCode = 422;
          throw err;
        },
      };
    },
  });
}
const unknownField = await fetchHpcRadarStatusEligibleRecords({
  airtableBase: mockUnknownFieldBase(),
  maxRetries: 0,
});
assert.equal(unknownField.records.length, 0);
assert.equal(unknownField.diagnostics.paginationComplete, false);
assert.ok(
  unknownField.errors.some((e) => e.reason === "hpc_select_unknown_field"),
  "unknown select fields must surface clearly"
);

const failedLoad = await loadAuthorizedInternalPreviewHotels({
  dealalityUser: { isAdmin: true },
  airtableBase: mockUnknownFieldBase(),
  bypassCache: true,
});
assert.equal(failedLoad.ok, false);
assert.equal(failedLoad.hotels.length, 0);
assert.ok((failedLoad.errors || []).length > 0);

const apiSrc = fs.readFileSync(
  path.join(ROOT, "api/brand-presence-internal-preview.js"),
  "utf8"
);
assert.match(apiSrc, /hpc_retrieval_failed/);
assert.match(apiSrc, /status\(502\)/);
assert.match(
  apiSrc,
  /Do not treat empty hotels as an authoritative inventory/
);

// --- Share-mode exclusion (client source contract) ---
const mapping = fs.readFileSync(
  path.join(ROOT, "public/brand-presence-mapping.js"),
  "utf8"
);
assert.match(mapping, /isOpportunityRadarShareMode\(\)/);
assert.match(mapping, /skipped:\s*'share_mode'/);
assert.match(mapping, /customerVisible === true/);
assert.match(mapping, /buildRadarHotelIdentityKeys/);
assert.match(mapping, /__radarHpcInternalDiagnostics/);
assert.doesNotMatch(
  mapping,
  /INTERNAL_HE_PREVIEW_CENSUS_IDS|recLuxvwwxID7U2B8/,
  "client must not hardcode Bethesda allowlist for merge"
);

const host = fs.readFileSync(
  path.join(ROOT, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8"
);
assert.match(host, /brand-presence-mapping\.js\?v=radar-hpc-workspace-principal-1/);

const api = fs.readFileSync(
  path.join(ROOT, "api/brand-presence-internal-preview.js"),
  "utf8"
);
assert.match(api, /customerVisible:\s*false/);
assert.match(api, /diagnostics:/);
assert.match(api, /Cache-Control/);

// Overview enrichment module still present (PR #380 regression surface)
assert.ok(
  fs.existsSync(
    path.join(ROOT, "lib/hotel-intelligence/hotel-explorer-overview-enrichment-v1.js")
  )
);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:radar-hpc-dynamic-status-query-v1",
    version: RADAR_INTERNAL_PREVIEW_HOTELS_VERSION,
    hotels: {
      bethesda: BETHESDA,
      waterstone: WATERSTONE,
      mayflower: MAYFLOWER,
    },
    openPipelineHiddenBlank: true,
    duplicateIdentity: true,
    authAndShareExclusion: true,
    paginationAndRateLimit: true,
  })
);
