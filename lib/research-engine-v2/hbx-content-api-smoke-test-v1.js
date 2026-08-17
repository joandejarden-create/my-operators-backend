/**
 * HBX Content API smoke test v1 — read-only, no Airtable writes.
 *
 * Objective: hbx-content-api-smoke-test-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolveHbxConfig,
  hbxFetchJson,
  contentUrl,
  apiUrl,
  HBX_CONTENT_API_CLIENT_VERSION,
} from "./hbx-content-api-client.js";
import { productionHotelPropertyCensus } from "./production-census-source-of-truth.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const HBX_SMOKE_OBJECTIVE = "hbx-content-api-smoke-test-v1";
export const HBX_SMOKE_VERSION = "hbx-content-api-smoke-test-v1";

export const HBX_SMOKE_STATUS = Object.freeze({
  COMPLETE: "production_census_hbx_content_api_smoke_test_v1_complete",
  PARTIAL_AUTH_FAILED:
    "production_census_hbx_content_api_smoke_test_v1_partial_auth_failed",
  PARTIAL_SCHEMA_UNKNOWN:
    "production_census_hbx_content_api_smoke_test_v1_partial_schema_unknown",
  BLOCKED: "production_census_hbx_content_api_smoke_test_v1_blocked",
});

/** Field probes: census-relevant paths on a hotel content object. */
const FIELD_PROBES = Object.freeze([
  {
    field: "Hotelbeds hotel code / ID",
    paths: ["code", "hotelCode"],
    recommendation_if_present: "write_safe_high",
    note: "Stable external identity key for dedupe / source linkage",
  },
  {
    field: "Hotel name",
    paths: ["name.content", "name"],
    recommendation_if_present: "write_safe_high",
    note: "Canonical display name candidate (normalize before write)",
  },
  {
    field: "Accommodation type",
    paths: ["accommodationTypeCode", "accommodationType.code", "accommodationType"],
    recommendation_if_present: "write_medium_internal",
    note: "Map to Dealality lodging class; filter non-hotel",
  },
  {
    field: "Category / star rating",
    paths: ["categoryCode", "category.code", "category", "categoryGroupCode"],
    recommendation_if_present: "write_medium_internal",
    note: "HBX category codes ≠ Dealality brand tier; keep provenance",
  },
  {
    field: "Chain / brand",
    paths: ["chainCode", "chain.code", "chain", "brandCode", "brand"],
    recommendation_if_present: "candidate_only",
    note: "Chain codes need Brand Setup mapping before Census Current Brand writes",
  },
  {
    field: "Country",
    paths: ["countryCode", "country.code", "country.isocode", "country"],
    recommendation_if_present: "write_safe_high",
    note: "ISO country for identity + geography",
  },
  {
    field: "State / Region",
    paths: ["stateCode", "state.code", "state.name", "state"],
    recommendation_if_present: "write_medium_internal",
    note: "Often destination/zone derived; validate vs Dealality State / Region",
  },
  {
    field: "Destination",
    paths: ["destinationCode", "destination.code", "destination.name.content", "destination"],
    recommendation_if_present: "candidate_only",
    note: "HBX destination ≠ Dealality Market; use as geography hint only",
  },
  {
    field: "Zone",
    paths: ["zoneCode", "zone.code", "zone.name.content", "zone"],
    recommendation_if_present: "candidate_only",
    note: "Possible Submarket hint; mapping required",
  },
  {
    field: "City",
    paths: ["city.content", "city"],
    recommendation_if_present: "write_safe_high",
    note: "Subject to City semantics + Proper Case",
  },
  {
    field: "Address",
    paths: ["address.content", "address", "address.street", "address.content"],
    recommendation_if_present: "write_medium_internal",
    note: "Prefer street-level; Medium until steward/official corroboration",
  },
  {
    field: "Postal code",
    paths: ["postalCode", "postalcode", "address.postalCode"],
    recommendation_if_present: "write_medium_internal",
    note: "Useful address completeness; not City",
  },
  {
    field: "Latitude",
    paths: ["coordinates.latitude", "latitude"],
    recommendation_if_present: "license_policy_needed",
    note: "Confirm HBX license allows permanent coordinate storage before Census write",
  },
  {
    field: "Longitude",
    paths: ["coordinates.longitude", "longitude"],
    recommendation_if_present: "license_policy_needed",
    note: "Prefer Mapbox-after-validated-address if license unclear",
  },
  {
    field: "Phone",
    paths: ["phones", "phone", "phones[0].phoneNumber", "phones[0].number"],
    recommendation_if_present: "write_medium_internal",
    note: "phones[] includes phoneType values e.g. PHONEHOTEL / PHONEBOOKING / PHONEMANAGEMENT — prefer PHONEHOTEL; reject PHONEBOOKING as central/reservation",
  },
  {
    field: "Phone (property PHONEHOTEL)",
    paths: ["phones"],
    recommendation_if_present: "write_medium_internal",
    note: "Detected via phones[].phoneType === PHONEHOTEL when present",
    custom: "phonehotel",
  },
  {
    field: "Email",
    paths: ["email", "emails", "emails[0]"],
    recommendation_if_present: "candidate_only",
    note: "Often reservations@; not a Census required field",
  },
  {
    field: "Website / web URL",
    paths: ["web", "website", "url", "websiteUrl"],
    recommendation_if_present: "write_medium_internal",
    note: "Validate hotel-official vs OTA/affiliate before Official Property URL",
  },
  {
    field: "Description",
    paths: ["description.content", "description"],
    recommendation_if_present: "license_policy_needed",
    note: "Marketing copy — likely internal-only unless license allows storage",
  },
  {
    field: "Facilities / amenities",
    paths: ["facilities", "facilityGroups"],
    recommendation_if_present: "candidate_only",
    note: "Useful enrichment; map carefully; not core identity",
  },
  {
    field: "Images",
    paths: ["images", "image"],
    recommendation_if_present: "license_policy_needed",
    note: "Do not publish/store without content license review",
  },
  {
    field: "Rooms / room types",
    paths: ["rooms", "roomTypes"],
    recommendation_if_present: "unsupported",
    note: "Usually room-type catalog, NOT total keys — do not write to Rooms / Keys from array length",
  },
  {
    field: "Total rooms / keys",
    paths: ["roomsNumber", "roomCount", "numberOfRooms", "totalRooms", "hotelRooms"],
    recommendation_if_present: "write_medium_internal",
    note: "Only if explicit total-count field present with property match — never use rooms[] length or giataCode",
  },
  {
    field: "Segments / tags",
    paths: ["segmentCodes", "segments"],
    recommendation_if_present: "candidate_only",
    note: "Classification aid only",
  },
  {
    field: "Terminals / interest points",
    paths: ["terminals", "interestPoints", "pointsOfInterest"],
    recommendation_if_present: "candidate_only",
    note: "Context only",
  },
  {
    field: "Last update date",
    paths: ["lastUpdate", "S2C", "lastUpdateDate"],
    recommendation_if_present: "write_medium_internal",
    note: "Provenance / freshness metadata",
  },
  {
    field: "Active / issues",
    paths: ["issues", "ranking", "exclusiveDeal"],
    recommendation_if_present: "candidate_only",
    note: "Operational flags — review before product exposure",
  },
]);

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}

function getPath(obj, dotted) {
  if (!obj || !dotted) return undefined;
  // support phones[0].phoneNumber
  const parts = String(dotted)
    .replace(/\[(\d+)\]/g, ".$1")
    .split(".")
    .filter(Boolean);
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function firstPresent(obj, paths) {
  for (const p of paths) {
    const v = getPath(obj, p);
    if (v == null || v === "") continue;
    if (Array.isArray(v) && v.length === 0) continue;
    return { path: p, value: v };
  }
  return null;
}

function sampleValue(v, max = 120) {
  if (v == null) return null;
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
    return String(v).slice(0, max);
  }
  if (Array.isArray(v)) {
    return `array(len=${v.length})`;
  }
  if (typeof v === "object") {
    const keys = Object.keys(v).slice(0, 8).join(",");
    return `object{${keys}${Object.keys(v).length > 8 ? ",…" : ""}}`;
  }
  return String(v).slice(0, max);
}

function hotelCodeOf(h) {
  return h?.code ?? h?.hotelCode ?? null;
}

function hotelNameOf(h) {
  return h?.name?.content || h?.name || null;
}

function findPhoneHotel(hotel) {
  const phones = hotel?.phones;
  if (!Array.isArray(phones)) return null;
  const hit = phones.find(
    (p) =>
      String(p?.phoneType || p?.phoneTypeCode || p?.type || "")
        .toUpperCase()
        .includes("PHONEHOTEL") ||
      String(p?.phoneType || "").toUpperCase() === "HOTEL"
  );
  if (!hit) return null;
  return {
    path: "phones[phoneType=PHONEHOTEL].phoneNumber",
    value: hit.phoneNumber || hit.number || hit,
  };
}

function buildFieldMatrix(hotels = []) {
  const matrix = [];
  for (const probe of FIELD_PROBES) {
    let present = false;
    let responsePath = null;
    let sample = null;
    let presentCount = 0;
    for (const h of hotels) {
      let hit = null;
      if (probe.custom === "phonehotel") {
        hit = findPhoneHotel(h);
      } else {
        hit = firstPresent(h, probe.paths);
      }
      if (!hit) continue;
      presentCount += 1;
      if (!present) {
        present = true;
        responsePath = hit.path;
        sample = sampleValue(hit.value);
      }
    }
    let recommendation = present
      ? probe.recommendation_if_present
      : "unsupported";
    // Refine rooms: if rooms is array of room types, flag unsupported for Rooms/Keys
    if (probe.field === "Rooms / room types" && present) {
      recommendation = "unsupported";
    }
    if (probe.field === "Total rooms / keys" && !present) {
      recommendation = "unsupported";
    }
    matrix.push({
      field: probe.field,
      present_in_hbx: present,
      present_in_sample_count: presentCount,
      sample_size: hotels.length,
      response_path: responsePath,
      sample_value: sample,
      census_use_recommendation: recommendation,
      note: probe.note,
    });
  }
  return matrix;
}

function summarizeCapabilities(matrix) {
  const byField = Object.fromEntries(matrix.map((m) => [m.field, m]));
  return {
    has_address: Boolean(byField.Address?.present_in_hbx),
    has_coordinates: Boolean(
      byField.Latitude?.present_in_hbx && byField.Longitude?.present_in_hbx
    ),
    has_phone: Boolean(byField.Phone?.present_in_hbx),
    has_website: Boolean(byField["Website / web URL"]?.present_in_hbx),
    has_rooms_keys_total: Boolean(byField["Total rooms / keys"]?.present_in_hbx),
    has_room_types_only: Boolean(byField["Rooms / room types"]?.present_in_hbx),
    has_brand_chain: Boolean(byField["Chain / brand"]?.present_in_hbx),
    has_description: Boolean(byField.Description?.present_in_hbx),
    has_facilities: Boolean(byField["Facilities / amenities"]?.present_in_hbx),
    has_images: Boolean(byField.Images?.present_in_hbx),
    has_property_phone_hotel: Boolean(
      byField["Phone (property PHONEHOTEL)"]?.present_in_hbx
    ),
  };
}

function recommendWritePolicy(caps, authOk) {
  if (!authOk) {
    return {
      posture: "blocked_until_auth",
      summary: "Do not enable HBX Census writes until auth succeeds on test endpoint.",
      flags: {
        ENABLE_HBX_CENSUS_WRITES: "0",
        ENABLE_HBX_INSERTS: "0",
      },
    };
  }
  return {
    posture: "read_only_discovery_lane_candidate",
    summary:
      "HBX is promising as a discovery/enrichment source. Keep ENABLE_HBX_CENSUS_WRITES=0 until dry-run ingest + license review for coordinates/images/descriptions. Prefer identity + address/city/country as Medium internal; prefer phones[].phoneType=PHONEHOTEL (reject PHONEBOOKING); Mapbox for coords after validated address unless HBX coordinate storage is licensed; Rooms/Keys NOT supported from rooms[] (room-type catalog only — no roomsNumber in sample).",
    flags: {
      ENABLE_HBX_CENSUS_WRITES: "0",
      ENABLE_HBX_INSERTS: "0",
    },
    next_dry_run_scope: [
      "CALA countries only (filter countryCode)",
      "from/to pagination batches of 100–500",
      "dedupe vs Hotel Property Census by name|country + Hotelbeds code key",
      "candidate review pack only — no apply",
      "phone filter: PHONEHOTEL only; drop PHONEBOOKING",
      "license review: coordinates, images, descriptions storage rights",
      "rooms: do not derive Rooms/Keys from rooms[] length; seek alternate approved sources",
    ],
    capabilities: caps,
  };
}

function renderMarkdown(report) {
  const m = report.field_availability_matrix || [];
  const rows = m
    .map(
      (r) =>
        `| ${r.field} | ${r.present_in_hbx ? "yes" : "no"} | ${r.response_path || "—"} | ${String(r.sample_value ?? "—").replace(/\|/g, "/")} | \`${r.census_use_recommendation}\` |`
    )
    .join("\n");
  const caps = report.capabilities || {};
  return `# HBX Content API Smoke Test v1

**Status:** \`${report.status}\`
**Objective:** \`${HBX_SMOKE_OBJECTIVE}\`
**Generated:** ${report.generated_at}
**Client:** \`${HBX_CONTENT_API_CLIENT_VERSION}\`
**HBX env:** \`${report.hbx_env}\`
**Content base:** \`${report.content_base}\`

## No-write confirmation

- Airtable writes: **${report.airtable_writes}**
- Hotel Property Census writes: **${report.census_writes}**
- Brand Explorer writes: **${report.brand_explorer_writes}**
- Brand Setup writes: **${report.brand_setup_writes}**
- \`ENABLE_HBX_CENSUS_WRITES\`: **${report.flags.ENABLE_HBX_CENSUS_WRITES}**
- \`ENABLE_HBX_INSERTS\`: **${report.flags.ENABLE_HBX_INSERTS}**
- Future write target (not used): ${productionHotelPropertyCensus.tableName} (\`${productionHotelPropertyCensus.tableId}\`)

## Auth

- Success: **${report.auth_success}**
- API key fingerprint: \`${report.api_key_fingerprint || "n/a"}\`
- Signature fingerprint (truncated): \`${report.signature_fingerprint || "n/a"}\`
- Secrets logged: **false**

## Endpoints tested

${(report.endpoints || [])
  .map(
    (e) =>
      `- **${e.name}** \`${e.method} ${e.path}\` → status **${e.status}** ok=${e.ok} (${e.elapsed_ms}ms)`
  )
  .join("\n")}

## Sample hotels

- Count: **${report.sample_hotel_count ?? 0}**
- Codes: ${(report.sample_hotel_codes || []).join(", ") || "(none)"}
- Names (truncated): ${(report.sample_hotel_names || []).map((n) => `\`${n}\``).join(", ") || "(none)"}

## Capability snapshot

| Capability | Present? |
|------------|----------|
| Address | ${caps.has_address ? "yes" : "no"} |
| Coordinates | ${caps.has_coordinates ? "yes" : "no"} |
| Phone | ${caps.has_phone ? "yes" : "no"} |
| Website | ${caps.has_website ? "yes" : "no"} |
| Rooms/Keys (total count) | ${caps.has_rooms_keys_total ? "yes" : "no"} |
| Room types array only | ${caps.has_room_types_only ? "yes" : "no"} |
| Property phone (PHONEHOTEL) | ${caps.has_property_phone_hotel ? "yes" : "no"} |
| Brand/chain | ${caps.has_brand_chain ? "yes" : "no"} |
| Descriptions | ${caps.has_description ? "yes" : "no"} |
| Facilities | ${caps.has_facilities ? "yes" : "no"} |
| Images | ${caps.has_images ? "yes" : "no"} |

## Field availability matrix

| Field | Present in HBX? | Response path | Sample value | Census use recommendation |
|-------|-----------------|---------------|--------------|---------------------------|
${rows}

## Recommended Census write policy

**Posture:** \`${report.write_policy?.posture}\`

${report.write_policy?.summary || ""}

### Keep flags

\`\`\`
ENABLE_HBX_CENSUS_WRITES=0
ENABLE_HBX_INSERTS=0
\`\`\`

### Next dry-run ingest scope

${(report.write_policy?.next_dry_run_scope || []).map((x) => `- ${x}`).join("\n") || "- (n/a)"}

## Notes

${(report.notes || []).map((n) => `- ${n}`).join("\n") || "- (none)"}
`;
}

/**
 * @param {{ env?: NodeJS.ProcessEnv, log?: Function }} [opts]
 */
export async function runHbxContentApiSmokeTestV1(opts = {}) {
  const log = opts.log || console.log;
  const env = { ...(opts.env || process.env) };

  // Hard no-write for this mission.
  env.ENABLE_HBX_CENSUS_WRITES = "0";
  env.ENABLE_HBX_INSERTS = "0";

  const cfg = resolveHbxConfig(env);
  const notes = [];
  const endpoints = [];
  let authSuccess = false;
  let signatureFingerprint = null;
  let status = HBX_SMOKE_STATUS.BLOCKED;

  if (!cfg.ok) {
    const report = {
      ok: false,
      status: HBX_SMOKE_STATUS.BLOCKED,
      objective: HBX_SMOKE_OBJECTIVE,
      version: HBX_SMOKE_VERSION,
      reason: "missing_hbx_credentials",
      missing: cfg.missing,
      airtable_writes: 0,
      census_writes: 0,
      brand_explorer_writes: 0,
      brand_setup_writes: 0,
      flags: {
        ENABLE_HBX_CENSUS_WRITES: "0",
        ENABLE_HBX_INSERTS: "0",
      },
      generated_at: new Date().toISOString(),
    };
    persistReports(report);
    return report;
  }

  if (cfg.writesEnabled || cfg.insertsEnabled) {
    notes.push(
      "Env had HBX write flags enabled; smoke test forced them to 0 and performed no writes."
    );
  }

  log(
    `[hbx-smoke] env=${cfg.hbxEnv} contentBase=${cfg.contentBase} key=${cfg.apiKeyFingerprint} writes=0`
  );

  // Test A — status / auth
  const statusRes = await hbxFetchJson(
    apiUrl(cfg, "hotel-api/1.0/status"),
    cfg
  );
  signatureFingerprint = statusRes.auth_meta?.signature_fingerprint || null;
  endpoints.push({
    name: "A_status_auth",
    method: "GET",
    path: "/hotel-api/1.0/status",
    status: statusRes.status,
    ok: statusRes.ok,
    elapsed_ms: statusRes.elapsed_ms,
    error_code: statusRes.error_code || null,
  });
  authSuccess = Boolean(statusRes.ok);
  if (!authSuccess) {
    notes.push(
      `Status endpoint auth failed status=${statusRes.status} err=${statusRes.error_code || statusRes.error_message || "n/a"} — will still try Content API hotels sample.`
    );
  }

  // Test B — hotels sample
  const hotelsPath =
    "hotels?fields=all&language=ENG&from=1&to=10&useSecondaryLanguage=false";
  const hotelsRes = await hbxFetchJson(contentUrl(cfg, hotelsPath), cfg);
  if (!signatureFingerprint) {
    signatureFingerprint = hotelsRes.auth_meta?.signature_fingerprint || null;
  }
  endpoints.push({
    name: "B_hotels_sample",
    method: "GET",
    path: `/hotel-content-api/1.0/${hotelsPath}`,
    status: hotelsRes.status,
    ok: hotelsRes.ok,
    elapsed_ms: hotelsRes.elapsed_ms,
    error_code: hotelsRes.error_code || null,
  });
  if (hotelsRes.ok) authSuccess = true;

  const hotels = Array.isArray(hotelsRes.body?.hotels)
    ? hotelsRes.body.hotels
    : Array.isArray(hotelsRes.body)
      ? hotelsRes.body
      : [];
  const sampleCodes = hotels.map(hotelCodeOf).filter((c) => c != null);
  const sampleNames = hotels
    .map(hotelNameOf)
    .filter(Boolean)
    .map((n) => String(n).slice(0, 80));

  // Test C — locations / types (tiny pages)
  const typeEndpoints = [
    {
      name: "C_countries",
      path: "locations/countries?fields=all&language=ENG&from=1&to=5",
    },
    {
      name: "C_destinations",
      path: "locations/destinations?fields=all&language=ENG&from=1&to=5",
    },
    {
      name: "C_categories",
      path: "types/categories?fields=all&language=ENG&from=1&to=5",
    },
    {
      name: "C_chains",
      path: "types/chains?fields=all&language=ENG&from=1&to=5",
    },
    {
      name: "C_accommodations",
      path: "types/accommodations?fields=all&language=ENG&from=1&to=5",
    },
    {
      name: "C_facilities",
      path: "types/facilities?fields=all&language=ENG&from=1&to=5",
    },
  ];
  const typeResults = {};
  for (const te of typeEndpoints) {
    const res = await hbxFetchJson(contentUrl(cfg, te.path), cfg);
    endpoints.push({
      name: te.name,
      method: "GET",
      path: `/hotel-content-api/1.0/${te.path}`,
      status: res.status,
      ok: res.ok,
      elapsed_ms: res.elapsed_ms,
      error_code: res.error_code || null,
      top_keys: res.body && typeof res.body === "object" ? Object.keys(res.body).slice(0, 12) : [],
    });
    typeResults[te.name] = {
      ok: res.ok,
      status: res.status,
      top_keys: res.body && typeof res.body === "object" ? Object.keys(res.body).slice(0, 12) : [],
      count_hint:
        res.body?.countries?.length ||
        res.body?.destinations?.length ||
        res.body?.categories?.length ||
        res.body?.chains?.length ||
        res.body?.accommodations?.length ||
        res.body?.facilities?.length ||
        null,
    };
    // gentle pacing
    await new Promise((r) => setTimeout(r, 120));
  }

  // Test D — one hotel details
  let detailsRes = null;
  const detailCode = sampleCodes[0];
  if (detailCode != null) {
    const detailPath = `hotels/${encodeURIComponent(detailCode)}/details?fields=all&language=ENG&useSecondaryLanguage=false`;
    detailsRes = await hbxFetchJson(contentUrl(cfg, detailPath), cfg);
    endpoints.push({
      name: "D_hotel_details",
      method: "GET",
      path: `/hotel-content-api/1.0/${detailPath}`,
      status: detailsRes.status,
      ok: detailsRes.ok,
      elapsed_ms: detailsRes.elapsed_ms,
      error_code: detailsRes.error_code || null,
      hotel_code: detailCode,
    });
    // Some APIs return hotel or hotels[0]
    const detailHotel =
      detailsRes.body?.hotel ||
      detailsRes.body?.hotels?.[0] ||
      (detailsRes.ok ? detailsRes.body : null);
    if (detailHotel && typeof detailHotel === "object" && !Array.isArray(detailHotel)) {
      // Prefer details object for matrix if richer
      if (!hotels.length) hotels.push(detailHotel);
      else {
        // merge missing keys into first sample for inspection
        hotels[0] = { ...hotels[0], ...detailHotel };
      }
    } else if (!detailsRes.ok) {
      notes.push(
        `Hotel details endpoint returned ${detailsRes.status}; list payload used for field matrix.`
      );
    }
  } else {
    notes.push("No hotel codes from Test B — skipped Test D details.");
  }

  const inspectionHotels = hotels.slice(0, 10);
  let matrix = buildFieldMatrix(inspectionHotels);
  let caps = summarizeCapabilities(matrix);
  let schemaUnknown = false;

  if (authSuccess && inspectionHotels.length === 0) {
    schemaUnknown = true;
    notes.push("Auth appeared OK but hotel sample empty — schema/pagination unknown.");
  }
  if (authSuccess && inspectionHotels.length > 0) {
    const presentCount = matrix.filter((m) => m.present_in_hbx).length;
    if (presentCount < 3) {
      schemaUnknown = true;
      notes.push(
        `Hotel objects returned but few expected fields matched probes (${presentCount}/${matrix.length}).`
      );
    }
  }

  if (!authSuccess && !hotelsRes.ok && !statusRes.ok) {
    status = HBX_SMOKE_STATUS.PARTIAL_AUTH_FAILED;
  } else if (schemaUnknown) {
    status = HBX_SMOKE_STATUS.PARTIAL_SCHEMA_UNKNOWN;
  } else if (authSuccess && hotelsRes.ok) {
    status = HBX_SMOKE_STATUS.COMPLETE;
  } else if (authSuccess) {
    status = HBX_SMOKE_STATUS.PARTIAL_SCHEMA_UNKNOWN;
  } else {
    status = HBX_SMOKE_STATUS.PARTIAL_AUTH_FAILED;
  }

  const writePolicy = recommendWritePolicy(caps, authSuccess);

  // Safe sample hotel keys (no secrets)
  const sampleTopKeys = inspectionHotels[0]
    ? Object.keys(inspectionHotels[0]).slice(0, 40)
    : [];

  const report = {
    ok: status === HBX_SMOKE_STATUS.COMPLETE || status === HBX_SMOKE_STATUS.PARTIAL_SCHEMA_UNKNOWN,
    status,
    objective: HBX_SMOKE_OBJECTIVE,
    version: HBX_SMOKE_VERSION,
    client_version: HBX_CONTENT_API_CLIENT_VERSION,
    generated_at: new Date().toISOString(),
    hbx_env: cfg.hbxEnv,
    api_base: cfg.apiBase,
    content_base: cfg.contentBase,
    api_key_fingerprint: cfg.apiKeyFingerprint,
    signature_fingerprint: signatureFingerprint,
    auth_success: authSuccess,
    endpoints,
    type_results: typeResults,
    sample_hotel_count: inspectionHotels.length,
    sample_hotel_codes: sampleCodes.slice(0, 10),
    sample_hotel_names: sampleNames.slice(0, 10),
    sample_hotel_top_keys: sampleTopKeys,
    hotel_details_tested: Boolean(detailCode),
    hotel_details_code: detailCode ?? null,
    hotel_details_ok: detailsRes ? Boolean(detailsRes.ok) : null,
    field_availability_matrix: matrix,
    capabilities: caps,
    write_policy: writePolicy,
    recommended_census_write_policy: writePolicy,
    next_dry_run_ingest_scope: writePolicy.next_dry_run_scope || [],
    airtable_writes: 0,
    census_writes: 0,
    brand_explorer_writes: 0,
    brand_setup_writes: 0,
    secrets_logged: false,
    flags: {
      ENABLE_HBX_CENSUS_WRITES: "0",
      ENABLE_HBX_INSERTS: "0",
    },
    future_write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: productionHotelPropertyCensus.tableId,
      used_this_run: false,
    },
    notes,
  };

  persistReports(report);
  log(
    `[hbx-smoke] done status=${status} auth=${authSuccess} hotels=${inspectionHotels.length} airtable_writes=0`
  );
  return report;
}

function persistReports(report) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(path.join(reportsDir, "hbx-content-api-smoke-test-v1.json"), report);
  const md = renderMarkdown(report);
  writeMd(path.join(reportsDir, "hbx-content-api-smoke-test-v1.md"), md);
  writeMd(path.join(docsDir, "hbx-content-api-smoke-test-v1.md"), md);
}
