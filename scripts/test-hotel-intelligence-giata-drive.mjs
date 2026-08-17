#!/usr/bin/env node
/**
 * Unit tests for GIATA Drive Hotel Intelligence adapter (no live API / no secrets).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  normalizeGiataDriveProperty,
  giataIdFromUrl,
  GIATA_DRIVE_ROOMS_CAPABILITY,
  GIATA_DRIVE_SUPPLIER_MAPPING,
  createGiataDriveClient,
} from "../lib/research-engine-v2/providers/giata-drive/index.js";
import {
  createGiataDriveProvider,
  normalizeGiataDriveHotel,
  PROVIDER_ID,
} from "../lib/hotel-intelligence/providers/giata-drive.js";
import {
  createGiataDriveSyncStore,
  GIATA_OPEN_CONTENT_REMOVED,
} from "../lib/hotel-intelligence/providers/giata-drive-sync.js";
import { createProviderRegistry } from "../lib/hotel-intelligence/providers/registry.js";
import { createHotelIntelligenceService } from "../lib/hotel-intelligence/orchestration/service.js";
import { createLocalStore } from "../lib/hotel-intelligence/local-store.js";
import { createExternalIdRegistry } from "../lib/hotel-intelligence/external-ids.js";
import { scoreFieldConfidence } from "../lib/hotel-intelligence/confidence.js";
import { MAP_PROVIDER_IDS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";

assert.equal(MAP_PROVIDER_IDS.giata_drive, "giata_drive");
assert.equal(PROVIDER_ID, "giata_drive");
assert.equal(GIATA_DRIVE_ROOMS_CAPABILITY.maps_room_types_to_room_count, false);
assert.equal(GIATA_DRIVE_SUPPLIER_MAPPING.status, "NOT_ENTITLED");
assert.equal(
  giataIdFromUrl("https://giatadrive.com/api/v1/properties/12345"),
  "12345"
);

const sampleRaw = {
  giataId: 98765,
  names: [{ locale: "en", value: "Sample Beach Resort", isDefault: true }],
  city: { names: [{ locale: "en", value: "Providenciales" }] },
  destination: { names: [{ locale: "en", value: "Turks and Caicos" }] },
  country: { code: "TC", names: [{ locale: "en", value: "Turks and Caicos Islands" }] },
  addresses: [
    {
      street: "Grace Bay Road",
      streetNum: "1",
      zip: "TKCA 1ZZ",
      cityName: "Providenciales",
    },
  ],
  geoCodes: [{ latitude: 21.78, longitude: -72.23, accuracy: "address" }],
  chains: [{ names: [{ locale: "en", value: "Independent", isDefault: true }] }],
  ratings: [{ value: "4", isDefault: true }],
  urls: [{ url: "https://example-resort.test/" }],
  phones: [{ tech: "phone", phone: "+1-649-555-0100" }],
  texts: { en: "Oceanfront resort on Grace Bay." },
  facts: { pool: true, spa: true },
  images: [{ url: "https://cdn.example/img1.jpg" }],
  roomTypes: [
    { code: "DLX", name: "Deluxe", units: 12 },
    { code: "STE", name: "Suite", units: 4 },
  ],
};

const normalized = normalizeGiataDriveProperty(sampleRaw);
assert.ok(normalized);
assert.equal(normalized.giata_id, "98765");
assert.equal(normalized.name, "Sample Beach Resort");
assert.equal(normalized.city, "Providenciales");
assert.equal(normalized.country_code, "TC");
assert.ok(String(normalized.address).includes("Grace Bay"));
assert.equal(normalized.postal_code, "TKCA 1ZZ");
assert.equal(normalized.latitude, 21.78);
assert.equal(normalized.longitude, -72.23);
assert.equal(normalized.brand_name, "Independent");
assert.equal(normalized.website, "https://example-resort.test/");
assert.equal(normalized.phone, "+1-649-555-0100");
assert.equal(normalized.room_types_count, 2);
assert.equal(normalized.room_count, null, "roomTypes must never become room_count");
assert.equal(normalized.hotelbeds_id, null);
assert.equal(normalized.booking_id, null);
assert.equal(normalized.expedia_id, null);
assert.equal(normalized.supplier_ids, null);

const cand = normalizeGiataDriveHotel(normalized);
assert.equal(cand.provider, "giata_drive");
assert.equal(cand.external_id, "98765");
assert.equal(cand.room_count, null);
assert.equal(cand.raw_safe.maps_room_types_to_room_count, false);
assert.equal(cand.raw_safe.hotelbeds_id, null);
assert.equal(cand.raw_safe.booking_id, null);

// Confidence model
const roomsConf = scoreFieldConfidence("room_count", "giata_drive");
assert.equal(roomsConf.confidence, 0);
assert.equal(roomsConf.auto_accept, false);
const geoConf = scoreFieldConfidence("latitude", "giata_drive");
assert.ok(geoConf.confidence >= 0.85);
const addrConf = scoreFieldConfidence("address_line_1", "giata_drive");
assert.ok(addrConf.confidence >= 0.85);
const brandConf = scoreFieldConfidence("brand_name", "giata_drive");
assert.ok(brandConf.confidence >= 0.8);

// Auth / unavailable without key
{
  const client = createGiataDriveClient({ env: {} });
  assert.equal(client.hasCredentials(), false);
  const provider = createGiataDriveProvider({
    env: { HOTEL_INTELLIGENCE_GIATA_DRIVE: "1" },
    client,
  });
  const status = await provider.getAvailabilityStatus();
  assert.equal(status.status, "unavailable");
  assert.match(status.message, /GIATA_DRIVE_API_KEY_missing/);
}

// Disabled when flag off (even with key present in env object)
{
  const provider = createGiataDriveProvider({
    env: {
      HOTEL_INTELLIGENCE_GIATA_DRIVE: "0",
      GIATA_DRIVE_API_KEY: "test-key-not-real",
    },
  });
  const status = await provider.getAvailabilityStatus();
  assert.equal(status.status, "disabled");
}

// Mock detail + country listing
{
  const calls = [];
  const fetchImpl = async (url) => {
    calls.push(String(url));
    const u = String(url);
    if (u.includes("/properties?") || /\/properties$/.test(u.split("?")[0])) {
      return {
        ok: true,
        status: 200,
        text: async () =>
          JSON.stringify({
            urls: [
              "https://giatadrive.com/api/v1/properties/98765",
              "https://giatadrive.com/api/v1/properties/11111",
            ],
            deletedUrls: ["https://giatadrive.com/api/v1/properties/99999"],
            latestRevision: "42",
          }),
      };
    }
    if (u.includes("/properties/98765")) {
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify(sampleRaw),
      };
    }
    if (u.includes("/properties/11111")) {
      return {
        ok: true,
        status: 200,
        text: async () =>
          JSON.stringify({
            ...sampleRaw,
            giataId: 11111,
            names: [{ locale: "en", value: "Other Hotel", isDefault: true }],
            roomTypes: [{ code: "STD", units: 99 }],
          }),
      };
    }
    return { ok: false, status: 404, text: async () => "{}" };
  };

  const provider = createGiataDriveProvider({
    env: {
      HOTEL_INTELLIGENCE_GIATA_DRIVE: "1",
      GIATA_DRIVE_API_KEY: "test-key-not-real",
    },
    fetchImpl,
  });

  const avail = await provider.getAvailabilityStatus();
  assert.equal(avail.status, "ok");

  const listed = await provider.searchHotels({
    countryCode: "TC",
    limit: 2,
    fetch_details: true,
  });
  assert.equal(listed.provider_status.status, "ok");
  assert.equal(listed.hotels.length, 2);
  assert.ok(listed.hotels.every((h) => h.room_count === null));
  assert.equal(listed.index.latest_revision, "42");
  assert.equal(listed.index.deleted_url_count, 1);

  const got = await provider.getHotel("98765");
  assert.equal(got.provider_status.status, "ok");
  assert.equal(got.hotel.external_id, "98765");
  assert.equal(got.hotel.room_count, null);
  assert.ok(!String(JSON.stringify(got)).includes("test-key-not-real"));
}

// Incremental after cursor + deleted URL handling
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "giata-sync-"));
  const sync = createGiataDriveSyncStore({ root: tmp });
  const first = sync.applyIndexSnapshot({
    urls: ["https://giatadrive.com/api/v1/properties/1"],
    deletedUrls: [],
    latestRevision: "10",
  });
  assert.equal(first.new, 1);
  assert.equal(first.changed, 0);
  const second = sync.applyIndexSnapshot({
    urls: ["https://giatadrive.com/api/v1/properties/1"],
    deletedUrls: ["https://giatadrive.com/api/v1/properties/2"],
    latestRevision: "11",
  });
  assert.equal(second.deleted_open_content_urls, 1);
  assert.equal(second.hotel_status_auto_changed, false);
  const delEvent = second.events.find((e) => e.type === GIATA_OPEN_CONTENT_REMOVED);
  assert.ok(delEvent);
  assert.equal(delEvent.hotel_status_changed, false);
  const state = sync.load();
  assert.equal(state.latest_giata_revision, "11");
  assert.ok(state.last_giata_sync_at);
}

// Provider failure isolation — registry still lists others
{
  const registry = createProviderRegistry({
    env: {
      HOTEL_INTELLIGENCE_GIATA_DRIVE: "1",
      // no API key
    },
    forceCensus: true,
  });
  assert.ok(registry.list().includes("giata_drive"));
  const availability = await registry.availability();
  assert.equal(availability.giata_drive.status, "unavailable");
  assert.ok(availability.dealality_census);
}

// hotel_enrich with GIATA (mocked) — stages evidence, no Airtable write
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "giata-enrich-"));
  const store = createLocalStore({ root: tmp });
  const idRegistry = createExternalIdRegistry(store);
  const hotelId = idRegistry.ensureHotelIdForAirtable("recTESTGIATA001");
  idRegistry.linkExternalId(hotelId, "giata_drive", "98765");

  const fetchImpl = async (url) => {
    if (String(url).includes("/properties/98765")) {
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify(sampleRaw),
      };
    }
    return { ok: false, status: 404, text: async () => "{}" };
  };

  const providers = createProviderRegistry({
    env: {
      HOTEL_INTELLIGENCE_GIATA_DRIVE: "1",
      GIATA_DRIVE_API_KEY: "test-key-not-real",
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
    },
    giataDriveOpts: { fetchImpl },
  });

  const service = createHotelIntelligenceService({
    store,
    idRegistry,
    providers,
    env: { ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0" },
    censusRecords: [],
  });

  const result = await service.hotelEnrich({
    hotel_id: hotelId,
    providers: ["giata_drive"],
    fields: ["address_line_1", "latitude", "longitude", "phone", "website", "brand_name", "room_count"],
  });
  assert.equal(result.ok, true);
  assert.equal(result.airtable_writes_enabled, false);
  assert.ok(result.fields_found.includes("address_line_1"));
  assert.ok(result.fields_found.includes("latitude"));
  assert.ok(!result.fields_found.includes("room_count"), "room_count must not be staged from GIATA");
  assert.ok(result.fields_updated.every((u) => u.airtable_written === false));
}

console.log("ok - giata_drive adapter auth/unavailable/normalize/firewall/sync/enrich");
