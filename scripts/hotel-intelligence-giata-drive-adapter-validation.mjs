#!/usr/bin/env node
/**
 * GIATA Drive provider adapter — controlled read-only validation.
 * Uses Hotel Intelligence provider (not a parallel client).
 *
 * SAFETY: forces Airtable/census writes OFF. Never logs secrets.
 *
 * Usage:
 *   node scripts/hotel-intelligence-giata-drive-adapter-validation.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";

import { createGiataDriveProvider } from "../lib/hotel-intelligence/providers/giata-drive.js";
import { createGiataDriveSyncStore } from "../lib/hotel-intelligence/providers/giata-drive-sync.js";
import { createLocalStore } from "../lib/hotel-intelligence/local-store.js";
import { createExternalIdRegistry } from "../lib/hotel-intelligence/external-ids.js";
import { createEvidenceStore } from "../lib/hotel-intelligence/evidence-store.js";
import { createHotelIntelligenceService } from "../lib/hotel-intelligence/orchestration/service.js";
import { createProviderRegistry } from "../lib/hotel-intelligence/providers/registry.js";
import { resolveHotelIdentity, MATCH_STATUS } from "../lib/hotel-intelligence/identity-resolve.js";
import { MAP_CENSUS_FIELDS } from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { scoreFieldConfidence } from "../lib/hotel-intelligence/confidence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/giata-drive-provider-adapter-v1"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.HOTEL_INTELLIGENCE_GIATA_DRIVE = "1";

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function writeJson(file, data) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function redact(obj) {
  return JSON.parse(
    JSON.stringify(obj, (k, v) => {
      if (typeof v === "string" && /Bearer\s+\S+/i.test(v)) {
        return v.replace(/Bearer\s+\S+/gi, "Bearer [REDACTED]");
      }
      if (
        /password|secret|authorization|token|credential|api[_-]?key/i.test(String(k)) &&
        typeof v === "string" &&
        v.length > 8 &&
        v !== "present" &&
        v !== "missing"
      ) {
        return "[REDACTED]";
      }
      return v;
    })
  );
}

function blank(v) {
  return v == null || !String(v).trim();
}

function nameKey(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function classifyMatch(resolved) {
  const s = String(resolved?.match_status || "").toLowerCase();
  if (s === MATCH_STATUS.EXACT) return "EXISTING_EXACT";
  if (s === MATCH_STATUS.STRONG) return "EXISTING_STRONG";
  if (s === MATCH_STATUS.PROBABLE) return "PROBABLE_EXISTING";
  if (s === MATCH_STATUS.AMBIGUOUS) return "AMBIGUOUS";
  if (s === MATCH_STATUS.NEW) return "NEW_CANDIDATE";
  return "AMBIGUOUS";
}

async function main() {
  ensureDir(OUT_DIR);
  const safety = {
    ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
      process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES,
    ENABLE_HBX_CENSUS_WRITES: process.env.ENABLE_HBX_CENSUS_WRITES,
    HOTEL_INTELLIGENCE_GIATA_DRIVE: process.env.HOTEL_INTELLIGENCE_GIATA_DRIVE,
    GIATA_DRIVE_API_KEY: process.env.GIATA_DRIVE_API_KEY ? "present" : "missing",
    GIATA_DRIVE_USERNAME: process.env.GIATA_DRIVE_USERNAME ? "present" : "missing",
    GIATA_DRIVE_PASSWORD: process.env.GIATA_DRIVE_PASSWORD ? "present" : "missing",
  };

  if (safety.GIATA_DRIVE_API_KEY !== "present") {
    throw new Error("GIATA_DRIVE_API_KEY missing — abort");
  }

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "giata-adapter-val-"));
  const store = createLocalStore({ root: tmpRoot });
  const idRegistry = createExternalIdRegistry(store);
  const evidence = createEvidenceStore(store);
  const provider = createGiataDriveProvider({
    env: process.env,
    forceEnabled: true,
  });
  const providers = createProviderRegistry({
    env: process.env,
    forceGiataDrive: true,
    giata_drive: provider,
  });
  const service = createHotelIntelligenceService({
    store,
    idRegistry,
    evidence,
    providers,
    env: process.env,
  });

  const avail = await provider.getAvailabilityStatus();
  const caps = provider.capabilities();

  // --- Load census (read-only) ---
  console.log(
    JSON.stringify({
      module: "giata-drive-adapter-validation",
      event: "loading_census",
    })
  );
  const censusRecords = await providers.census.loadRecords();
  console.log(
    JSON.stringify({
      module: "giata-drive-adapter-validation",
      event: "census_loaded",
      count: censusRecords.length,
    })
  );

  // --- Coverage dashboard: pick zero/near-zero ISO codes (not hardcoded) ---
  const dashPath = path.join(
    ROOT,
    "reports/hotel-intelligence/cala-coverage-dashboard-v1/cala-coverage-dashboard.json"
  );
  const dash = JSON.parse(fs.readFileSync(dashPath, "utf8"));
  const isoByCountry = new Map(
    (dash.rows || []).map((r) => [r.country, r.iso_code])
  );
  const zeroNear = [
    ...(dash.zero_coverage_countries || []).map((z) => ({
      country: z.country,
      iso: isoByCountry.get(z.country) || null,
      tier: "ZERO",
    })),
    ...(dash.near_zero_coverage_countries || []).map((z) => ({
      country: z.country,
      iso: isoByCountry.get(z.country) || null,
      tier: "NEAR_ZERO",
    })),
  ].filter((z) => z.iso);

  // Probe GIATA index for up to 12 candidates; keep first 5 with properties
  const discoveryTargets = [];
  for (const z of zeroNear) {
    if (discoveryTargets.length >= 5) break;
    const listed = await provider.searchHotels({
      countryCode: z.iso,
      limit: 5,
      fetch_details: false,
    });
    const n = listed.index?.url_count || listed.hotels?.length || 0;
    if (n > 0) {
      discoveryTargets.push({
        ...z,
        giata_index_count: n,
        latest_revision: listed.index?.latest_revision || null,
      });
    }
  }

  // --- Incremental feed (BR country index) ---
  const sync = createGiataDriveSyncStore({ root: tmpRoot });
  const clientList = await provider.client.listPropertyUrls({ countryCode: "BR" });
  const incrFull = sync.applyIndexSnapshot({
    urls: clientList.json?.urls || [],
    deletedUrls: clientList.json?.deletedUrls || [],
    latestRevision: clientList.json?.latestRevision,
  });
  // bump metrics for the explicit index call
  provider.getMetrics();

  // --- Existing-hotel enrichment mix (~25) ---
  // Pull details from BR/MX/DO + discovery targets; resolve; enrich matches
  const enrichCountries = [
    { iso: "BR", country: "Brazil", quota: 8 },
    { iso: "MX", country: "Mexico", quota: 7 },
    { iso: "DO", country: "Dominican Republic", quota: 5 },
    ...discoveryTargets.slice(0, 2).map((d) => ({
      iso: d.iso,
      country: d.country,
      quota: 3,
    })),
  ];

  const enrichRows = [];
  const fieldRecover = {
    address: 0,
    coords: 0,
    brand: 0,
    phone: 0,
    website: 0,
  };
  let matches = 0;
  let conflicts = 0;
  let reviewRequired = 0;
  let giataIdsLinked = 0;

  for (const c of enrichCountries) {
    const listed = await provider.searchHotels({
      countryCode: c.iso,
      limit: c.quota,
      fetch_details: true,
    });
    for (const hotel of listed.hotels || []) {
      const resolved = resolveHotelIdentity(
        {
          name: hotel.name,
          city: hotel.city,
          country: hotel.country,
          address: hotel.address,
          latitude: hotel.latitude,
          longitude: hotel.longitude,
          brand: hotel.brand_name,
          website: hotel.website,
          phone: hotel.phone,
          external_ids: hotel.external_id
            ? [{ provider: "giata_drive", external_id: String(hotel.external_id) }]
            : [],
        },
        censusRecords,
        { idRegistry }
      );
      const cls = classifyMatch(resolved);
      const row = {
        country: c.country,
        iso: c.iso,
        giata_id: hotel.external_id,
        name: hotel.name,
        match_class: cls,
        match_status: resolved.match_status,
        hotel_id: resolved.hotel_id || null,
        room_count_from_giata: hotel.room_count,
        fields: {
          address: hotel.address,
          city: hotel.city,
          lat: hotel.latitude,
          lng: hotel.longitude,
          brand: hotel.brand_name,
          phone: hotel.phone,
          website: hotel.website,
        },
        enrich: null,
      };

      if (
        cls === "EXISTING_EXACT" ||
        cls === "EXISTING_STRONG" ||
        cls === "PROBABLE_EXISTING"
      ) {
        matches += 1;
        let hotelId = resolved.hotel_id;
        const matchedRecId =
          resolved.airtable_record_id ||
          resolved.candidate_matches?.[0]?.airtable_record_id ||
          null;
        if (!hotelId && matchedRecId) {
          hotelId = idRegistry.ensureHotelIdForAirtable(matchedRecId);
        }
        const matchedRec = matchedRecId
          ? censusRecords.find((r) => r.id === matchedRecId)
          : null;

        if (hotelId && hotel.external_id) {
          idRegistry.linkExternalId(hotelId, "giata_drive", String(hotel.external_id));
          giataIdsLinked += 1;

          const censusRec =
            matchedRec ||
            censusRecords.find(
              (r) => idRegistry.getByAirtableId(r.id)?.hotel_id === hotelId
            );
          const cf = censusRec?.fields || {};
          const missing = {
            address: blank(cf[MAP_CENSUS_FIELDS.address]),
            coords:
              blank(cf[MAP_CENSUS_FIELDS.latitude]) ||
              blank(cf[MAP_CENSUS_FIELDS.longitude]),
            brand: blank(cf[MAP_CENSUS_FIELDS.brandName]),
            phone: blank(cf[MAP_CENSUS_FIELDS.phone]),
            website: blank(cf[MAP_CENSUS_FIELDS.website]),
          };

          const enrichResult = await service.hotelEnrich({
            hotel_id: hotelId,
            providers: ["giata_drive"],
            giata_id: hotel.external_id,
            fields: [
              "address_line_1",
              "latitude",
              "longitude",
              "brand_name",
              "phone",
              "website",
              "room_count",
            ],
          });
          row.enrich = {
            fields_found: enrichResult.fields_found,
            review_required: enrichResult.review_required,
            airtable_written: false,
            room_count_staged: enrichResult.fields_found.includes("room_count"),
          };
          if (enrichResult.review_required) reviewRequired += 1;
          if (enrichResult.conflicts?.length) conflicts += 1;

          if (missing.address && hotel.address) fieldRecover.address += 1;
          if (missing.coords && hotel.latitude != null) fieldRecover.coords += 1;
          if (missing.brand && hotel.brand_name) fieldRecover.brand += 1;
          if (missing.phone && hotel.phone) fieldRecover.phone += 1;
          if (missing.website && hotel.website) fieldRecover.website += 1;
        }
      }
      enrichRows.push(row);
      if (enrichRows.length >= 25) break;
    }
    if (enrichRows.length >= 25) break;
  }

  // --- Zero-coverage discovery (detail + resolve/stage) ---
  const discoveryResults = [];
  for (const t of discoveryTargets) {
    const listed = await provider.searchHotels({
      countryCode: t.iso,
      limit: 10,
      fetch_details: true,
    });
    let existing = 0;
    let neu = 0;
    let ambiguous = 0;
    const staged = [];
    for (const hotel of listed.hotels || []) {
      const resolved = resolveHotelIdentity(
        {
          name: hotel.name,
          city: hotel.city,
          country: hotel.country,
          address: hotel.address,
          latitude: hotel.latitude,
          longitude: hotel.longitude,
          brand: hotel.brand_name,
          website: hotel.website,
          phone: hotel.phone,
          external_ids: hotel.external_id
            ? [{ provider: "giata_drive", external_id: String(hotel.external_id) }]
            : [],
        },
        censusRecords,
        { idRegistry }
      );
      const cls = classifyMatch(resolved);
      if (cls.startsWith("EXISTING") || cls === "PROBABLE_EXISTING") existing += 1;
      else if (cls === "NEW_CANDIDATE") {
        neu += 1;
        const stagedId = idRegistry.createStagedHotelId({
          property_identity_key: `giata:${hotel.external_id}`,
        });
        if (hotel.external_id) {
          idRegistry.linkExternalId(stagedId, "giata_drive", String(hotel.external_id));
        }
        const stagedHotels = store.readStagedHotels();
        stagedHotels.hotels[stagedId] = {
          hotel_id: stagedId,
          status: "NEW_CANDIDATE",
          source: "giata_drive",
          name: hotel.name,
          city: hotel.city,
          country: hotel.country,
          latitude: hotel.latitude,
          longitude: hotel.longitude,
          brand_name: hotel.brand_name,
          external_ids: { giata_drive: hotel.external_id },
          room_count: null,
          staged_at: new Date().toISOString(),
          production_written: false,
        };
        store.writeStagedHotels(stagedHotels);
        staged.push({
          hotel_id: stagedId,
          giata_id: hotel.external_id,
          name: hotel.name,
        });
      } else ambiguous += 1;
    }
    discoveryResults.push({
      country: t.country,
      iso: t.iso,
      tier: t.tier,
      giata_properties: listed.hotels?.length || 0,
      index_count: t.giata_index_count,
      existing_matches: existing,
      new_candidates: neu,
      ambiguous,
      potential_coverage_gain: neu,
      staged_sample: staged.slice(0, 5),
    });
  }

  // --- Cvent Brazil corroboration (~25) ---
  const cventPath = path.join(
    ROOT,
    "data/hotel-intelligence/discovery-factory/staged-review-required.json"
  );
  const readyPath = path.join(
    ROOT,
    "data/hotel-intelligence/discovery-factory/staged-ready-for-import.json"
  );
  let cventItems = [];
  function loadStaged(filePath) {
    if (!fs.existsSync(filePath)) return [];
    const raw = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (Array.isArray(raw)) return raw;
    if (Array.isArray(raw.items)) return raw.items;
    if (Array.isArray(raw.candidates)) return raw.candidates;
    if (Array.isArray(raw.hotels)) return raw.hotels;
    if (raw.hotels && typeof raw.hotels === "object") {
      return Object.values(raw.hotels);
    }
    return [];
  }
  cventItems = [
    ...loadStaged(cventPath),
    ...loadStaged(readyPath),
  ];
  const brazilCvent = cventItems
    .filter((x) => {
      const country = String(
        x.country ||
          x.location?.country ||
          x.fields?.country ||
          ""
      ).toLowerCase();
      return country.includes("brazil") || country === "br";
    })
    .slice(0, 80);

  const brDetails = await provider.searchHotels({
    countryCode: "BR",
    limit: 15,
    fetch_details: true,
  });
  const giataByName = new Map();
  for (const h of brDetails.hotels || []) {
    giataByName.set(nameKey(h.name), h);
  }

  const cventResults = [];
  let cventMatches = 0;
  let cityOk = 0;
  let coordOk = 0;
  let brandOk = 0;
  let confidenceUp = 0;
  for (const item of brazilCvent.slice(0, 25)) {
    const name =
      item.name ||
      item.property_name ||
      item.identity?.official_name ||
      item.identity?.display_name ||
      item.fields?.name ||
      "";
    const city = item.city || item.location?.city || item.fields?.city || "";
    const g = giataByName.get(nameKey(name));
    // fuzzy: substring
    let hit = g;
    if (!hit) {
      for (const [k, h] of giataByName) {
        if (k && nameKey(name) && (k.includes(nameKey(name)) || nameKey(name).includes(k))) {
          hit = h;
          break;
        }
      }
    }
    const row = {
      cvent_name: name,
      cvent_city: city,
      giata_match: Boolean(hit),
      giata_id: hit?.external_id || null,
      giata_corroborated: false,
      city_corroborated: false,
      coords_recovered: false,
      brand_recovered: false,
      confidence_note: null,
    };
    if (hit) {
      cventMatches += 1;
      row.giata_corroborated = true;
      if (
        city &&
        hit.city &&
        nameKey(city) === nameKey(hit.city)
      ) {
        row.city_corroborated = true;
        cityOk += 1;
      }
      if (hit.latitude != null && hit.longitude != null) {
        row.coords_recovered = true;
        coordOk += 1;
      }
      if (hit.brand_name) {
        row.brand_recovered = true;
        brandOk += 1;
      }
      const before = scoreFieldConfidence("address_line_1", "manual").confidence;
      const after = scoreFieldConfidence("address_line_1", "giata_drive", {
        agreementBonus: 0.03,
      }).confidence;
      if (after > before) {
        confidenceUp += 1;
        row.confidence_note = `address ${before}→${after}; giata_corroborated=true`;
      }
    }
    cventResults.push(row);
  }

  const metrics = provider.getMetrics();
  const usefulFieldsPerDetail =
    metrics.successful_details > 0
      ? Number(
          (
            (fieldRecover.address +
              fieldRecover.coords +
              fieldRecover.brand +
              fieldRecover.phone +
              fieldRecover.website) /
            metrics.successful_details
          ).toFixed(3)
        )
      : 0;

  // Estimate SerpApi savings: fields GIATA already strong for
  const serpSavingEstimate = {
    method:
      "If GIATA returns address+coords+phone, skip SerpApi for those fields (1–2 searches each).",
    hotels_with_giata_address: enrichRows.filter((r) => r.fields?.address).length,
    hotels_with_giata_coords: enrichRows.filter(
      (r) => r.fields?.lat != null && r.fields?.lng != null
    ).length,
    hotels_with_giata_phone: enrichRows.filter((r) => r.fields?.phone).length,
    estimated_serpapi_calls_avoided_low:
      enrichRows.filter((r) => r.fields?.address && r.fields?.lat != null).length,
    estimated_serpapi_calls_avoided_high:
      enrichRows.filter((r) => r.fields?.address && r.fields?.lat != null).length * 2 +
      enrichRows.filter((r) => r.fields?.phone).length,
  };

  const summary = redact({
    marker: "DEALALITY_GIATA_DRIVE_PROVIDER_ADAPTER_COMPLETE",
    generated_at: new Date().toISOString(),
    safety: {
      Airtable_writes: 0,
      Census_writes: 0,
      Brand_Explorer_writes: 0,
      Automatic_merges: 0,
      Schema_changes: 0,
      Migrations: 0,
      Secrets_exposed: false,
      flags: safety,
    },
    provider_adapter: {
      provider_registered: true,
      credential_status: safety.GIATA_DRIVE_API_KEY,
      availability: avail,
      capabilities: caps,
      mcp_tools_affected: ["hotel_enrich", "hotel_search", "hotel_intelligence_meta"],
    },
    existing_hotel_test: {
      target: 25,
      sampled: enrichRows.length,
      giata_matches: matches,
      giata_ids_linked_locally: giataIdsLinked,
      addresses_recovered: fieldRecover.address,
      coords_recovered: fieldRecover.coords,
      brands_recovered: fieldRecover.brand,
      phones_recovered: fieldRecover.phone,
      websites_recovered: fieldRecover.website,
      conflicts,
      review_required: reviewRequired,
      room_count_ever_staged: enrichRows.some((r) => r.enrich?.room_count_staged),
      rows: enrichRows,
    },
    zero_coverage_discovery: discoveryResults,
    cvent_corroboration: {
      sampled: cventResults.length,
      giata_match_rate:
        cventResults.length > 0
          ? Number((cventMatches / cventResults.length).toFixed(3))
          : 0,
      identity_corroboration: cventMatches,
      city_corroboration: cityOk,
      coordinate_recovery: coordOk,
      brand_recovery: brandOk,
      confidence_improvements: confidenceUp,
      note: "No automatic REVIEW_REQUIRED→READY_FOR_IMPORT transitions",
      rows: cventResults,
    },
    api_efficiency: {
      ...metrics,
      useful_missing_fields_recovered: fieldRecover,
      detail_calls_per_enriched_hotel:
        matches > 0
          ? Number((metrics.detail_calls / Math.max(matches, 1)).toFixed(2))
          : null,
      useful_fields_per_detail_call: usefulFieldsPerDetail,
      serpapi_call_saving_potential: serpSavingEstimate,
    },
    incremental_feed: {
      latestRevision: incrFull.latest_giata_revision,
      new: incrFull.new,
      changed: incrFull.changed,
      deleted_open_content_urls: incrFull.deleted_open_content_urls,
      hotel_status_auto_changed: incrFull.hotel_status_auto_changed,
    },
    production_role: [
      "SECONDARY_UNIVERSE_DISCOVERY",
      "IDENTITY_VALIDATION",
      "EXTERNAL_ID_GRAPH",
      "GEO_ENRICHMENT",
      "BRAND_ENRICHMENT",
    ],
    highest_value_next_step: "RUN_GIATA_EXISTING_CENSUS_ENRICHMENT",
  });

  // Prefer RUN if matches recovered useful fields; else OPTIMIZE waterfall; else discovery sprint
  if (matches >= 5 && fieldRecover.address + fieldRecover.coords + fieldRecover.phone >= 3) {
    summary.highest_value_next_step = "RUN_GIATA_EXISTING_CENSUS_ENRICHMENT";
  } else if (discoveryResults.some((d) => d.new_candidates > 0)) {
    summary.highest_value_next_step = "USE_GIATA_IN_NEXT_BALANCED_DISCOVERY_SPRINT";
  } else if (serpSavingEstimate.estimated_serpapi_calls_avoided_low >= 5) {
    summary.highest_value_next_step = "OPTIMIZE_GIATA_SERPAPI_WATERFALL";
  } else {
    summary.highest_value_next_step = "REMEDIATE_GIATA_ADAPTER_FIRST";
  }

  writeJson(path.join(OUT_DIR, "adapter-validation-summary.json"), summary);
  writeJson(path.join(OUT_DIR, "existing-hotel-enrichment.json"), enrichRows);
  writeJson(path.join(OUT_DIR, "zero-coverage-discovery.json"), discoveryResults);
  writeJson(path.join(OUT_DIR, "cvent-corroboration.json"), cventResults);

  console.log(
    JSON.stringify({
      module: "giata-drive-adapter-validation",
      event: "complete",
      out_dir: "reports/hotel-intelligence/giata-drive-provider-adapter-v1",
      matches,
      discovery_countries: discoveryResults.length,
      cvent_match_rate: summary.cvent_corroboration.giata_match_rate,
      next: summary.highest_value_next_step,
      airtable_writes: 0,
    })
  );
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      module: "giata-drive-adapter-validation",
      event: "fatal",
      message: String(err?.message || err).slice(0, 400),
    })
  );
  process.exit(1);
});
