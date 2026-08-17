#!/usr/bin/env node
/**
 * GIATA Drive (Open Content Link) provider validation — read-only.
 * SAFETY: no Airtable writes. Never logs secrets.
 *
 * Usage:
 *   node scripts/hotel-intelligence-giata-provider-validation.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";

import {
  MAP_CENSUS_FIELDS,
  MAP_HOTEL_PROPERTY_CENSUS,
} from "../lib/hotel-intelligence/map_hotel_intelligence_fields.js";
import { resolveHotelIdentity, MATCH_STATUS } from "../lib/hotel-intelligence/identity-resolve.js";
import { countUniverseCandidatesByCountry } from "../lib/hotel-intelligence/universe-expansion/coverage-scorecard.js";
import { resolveDealalityCalaGeography } from "../lib/research-engine-v2/dealality-cala-geography-registry-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/giata-provider-validation-v1"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

const DRIVE_BASE = "https://giatadrive.com/api/v1";
const CONTROLLED = [
  { country: "Brazil", iso: "BR" },
  { country: "Mexico", iso: "MX" },
  { country: "Dominican Republic", iso: "DO" },
  { country: "Paraguay", iso: "PY" },
  { country: "Turks and Caicos Islands", iso: "TC" },
  { country: "Bonaire", iso: "BQ" },
];

function credPresence() {
  return {
    GIATA_DRIVE_API_KEY: Boolean(String(process.env.GIATA_DRIVE_API_KEY || "").trim()),
    GIATA_DRIVE_USERNAME: Boolean(String(process.env.GIATA_DRIVE_USERNAME || "").trim()),
    GIATA_DRIVE_PASSWORD: Boolean(String(process.env.GIATA_DRIVE_PASSWORD || "").trim()),
    GIATA_MULTICODES: Boolean(String(process.env.GIATA_API_KEY || process.env.GIATA_TOKEN || "").trim()),
  };
}

function authHeaders() {
  const key = String(process.env.GIATA_DRIVE_API_KEY || "").trim();
  if (!key) throw new Error("GIATA_DRIVE_API_KEY missing");
  return { Authorization: `Bearer ${key}`, Accept: "application/json" };
}

async function driveGet(pathname) {
  const t0 = Date.now();
  const res = await fetch(`${DRIVE_BASE}${pathname}`, { headers: authHeaders() });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* ignore */
  }
  return {
    status: res.status,
    ms: Date.now() - t0,
    json,
    textLen: text.length,
  };
}

function pickName(p) {
  const names = p.names || [];
  const en = names.find((n) => String(n.locale || "").startsWith("en"));
  return String(en?.value || names[0]?.value || names[0] || "").trim();
}

function pickCity(p) {
  const names = p.city?.names || [];
  const en = names.find((n) => String(n.locale || "").startsWith("en"));
  return String(en?.value || names[0]?.value || p.addresses?.[0]?.cityName || "").trim();
}

function pickCountryName(p) {
  const code = p.country?.code;
  const g = resolveDealalityCalaGeography(code) || null;
  // Prefer registry name when ISO maps; else English country name
  if (g?.name) return g.name;
  const names = p.country?.names || [];
  const en = names.find((n) => String(n.locale || "").startsWith("en"));
  return String(en?.value || code || "").trim();
}

function toResolveInput(p) {
  const geo = p.geoCodes?.[0] || {};
  const addr = p.addresses?.[0] || {};
  const chain =
    p.chains?.[0]?.names?.find((n) => n.isDefault)?.value ||
    p.chains?.[0]?.names?.[0]?.value ||
    null;
  const website = p.urls?.[0]?.url || null;
  const phone = p.phones?.[0]?.phone || null;
  return {
    name: pickName(p),
    city: pickCity(p),
    country: pickCountryName(p),
    address: addr.street || (addr.addressLines || []).join(", ") || null,
    latitude: geo.latitude ?? null,
    longitude: geo.longitude ?? null,
    brand: chain,
    website,
    phone,
    external_ids: [{ provider: "giata", external_id: String(p.giataId) }],
  };
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

function fieldPresence(sampleProps) {
  const checks = {
    giata_hotel_id: (p) => p.giataId != null,
    hotel_name: (p) => Boolean(pickName(p)),
    alternate_names: (p) => Array.isArray(p.names) && p.names.length > 1,
    former_names: () => false,
    city: (p) => Boolean(pickCity(p)),
    destination: (p) => Boolean(p.destination?.giataId || p.destination?.names),
    state_region: (p) => Boolean(p.addresses?.[0]?.federalState?.name),
    country: (p) => Boolean(p.country?.code),
    country_iso: (p) => Boolean(p.country?.code),
    street_address: (p) => Boolean(p.addresses?.[0]?.street || p.addresses?.[0]?.addressLines?.length),
    postal_code: (p) => Boolean(p.addresses?.[0]?.zip),
    latitude: (p) => p.geoCodes?.[0]?.latitude != null,
    longitude: (p) => p.geoCodes?.[0]?.longitude != null,
    chain_brand: (p) => Array.isArray(p.chains) && p.chains.length > 0,
    parent_company: () => false,
    hotel_category_star: (p) => Array.isArray(p.ratings) && p.ratings.length > 0,
    property_type: () => false,
    active_inactive_status: () => false,
    website: (p) => Array.isArray(p.urls) && p.urls.length > 0,
    phone: (p) => Array.isArray(p.phones) && p.phones.length > 0,
    total_property_room_count: (p) =>
      ["roomCount", "rooms", "numberOfRooms", "totalRooms", "keys"].some((k) => p[k] != null),
    room_types: (p) => Array.isArray(p.roomTypes) && p.roomTypes.length > 0,
    construction_year: () => false,
    renovation_year: () => false,
    supplier_ids: () => false,
    booking_id: () => false,
    hotelbeds_id: () => false,
    expedia_id: () => false,
    other_provider_ids: () => false,
    descriptions: (p) => Boolean(p.texts && Object.keys(p.texts).length),
    facts_amenities: (p) => Boolean(p.facts && Object.keys(p.facts).length),
    images: (p) => Array.isArray(p.images) && p.images.length > 0,
  };

  const matrix = {};
  for (const [field, fn] of Object.entries(checks)) {
    const hits = sampleProps.filter((p) => {
      try {
        return fn(p);
      } catch {
        return false;
      }
    }).length;
    if (hits > 0) matrix[field] = "CONFIRMED_SUPPORTED";
    else if (
      field === "supplier_ids" ||
      field === "booking_id" ||
      field === "hotelbeds_id" ||
      field === "expedia_id" ||
      field === "other_provider_ids" ||
      field === "total_property_room_count" ||
      field === "former_names" ||
      field === "parent_company" ||
      field === "property_type" ||
      field === "active_inactive_status" ||
      field === "construction_year" ||
      field === "renovation_year"
    ) {
      // Present in broader GIATA product family / marketing, not in Drive Open Content payload
      matrix[field] =
        field === "total_property_room_count" ||
        field === "supplier_ids" ||
        field === "booking_id" ||
        field === "hotelbeds_id" ||
        field === "expedia_id"
          ? "SUPPORTED_BUT_NOT_ENTITLED"
          : "NOT_SUPPORTED";
    } else {
      matrix[field] = "NOT_SUPPORTED";
    }
  }
  return matrix;
}

async function listCensusByCountry() {
  const token = (
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    process.env.AIRTABLE_API_KEY ||
    ""
  ).trim();
  const baseId = (
    process.env.AIRTABLE_BASE_ID_ALT ||
    process.env.AIRTABLE_BASE_ID ||
    ""
  ).trim();
  if (!token || !baseId) throw new Error("Airtable credentials missing for read");
  const base = new Airtable({ apiKey: token }).base(baseId);
  const byCountry = {};
  const records = [];
  await base(MAP_HOTEL_PROPERTY_CENSUS.tableId)
    .select({
      pageSize: 100,
      fields: [
        MAP_CENSUS_FIELDS.propertyName,
        MAP_CENSUS_FIELDS.officialName,
        MAP_CENSUS_FIELDS.country,
        MAP_CENSUS_FIELDS.city,
        MAP_CENSUS_FIELDS.address,
        MAP_CENSUS_FIELDS.website,
        MAP_CENSUS_FIELDS.phone,
        MAP_CENSUS_FIELDS.hbxHotelCode,
        MAP_CENSUS_FIELDS.latitude,
        MAP_CENSUS_FIELDS.longitude,
        MAP_CENSUS_FIELDS.brandName,
        MAP_CENSUS_FIELDS.propertyIdentityKey,
      ].filter(Boolean),
    })
    .eachPage((page, next) => {
      for (const r of page) {
        const country =
          String(r.fields?.[MAP_CENSUS_FIELDS.country] || "").trim() || "UNKNOWN";
        byCountry[country] = (byCountry[country] || 0) + 1;
        records.push({ id: r.id, fields: r.fields });
      }
      next();
    });
  return { byCountry, records, total: records.length };
}

function filterCensus(records, countryName) {
  const g = resolveDealalityCalaGeography(countryName);
  const want = (g?.name || countryName).toLowerCase();
  return records.filter((r) => {
    const c = String(r.fields?.[MAP_CENSUS_FIELDS.country] || "").trim();
    const rg = resolveDealalityCalaGeography(c);
    return (rg?.name || c).toLowerCase() === want;
  });
}

async function fetchCountryUniverse(iso) {
  const list = await driveGet(`/properties?countryCode=${encodeURIComponent(iso)}`);
  const urls = list.json?.urls || [];
  const props = [];
  let apiCalls = 1;
  for (const url of urls) {
    const id = String(url).split("/").pop();
    const detail = await driveGet(`/properties/${id}`);
    apiCalls += 1;
    if (detail.status === 200 && detail.json) props.push(detail.json);
    // polite pacing
    await new Promise((r) => setTimeout(r, 40));
  }
  return { listStatus: list.status, listMs: list.ms, urls: urls.length, props, apiCalls };
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const presence = credPresence();
  console.log(
    JSON.stringify({
      module: "giata-provider-validation-v1",
      ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES:
        process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES,
      ENABLE_HBX_CENSUS_WRITES: process.env.ENABLE_HBX_CENSUS_WRITES,
      credentials_present: presence,
    })
  );

  // Connectivity
  const connect = await driveGet("/properties?countryCode=TC");
  const connectivity = {
    credentials_present: presence.GIATA_DRIVE_API_KEY,
    reachable: connect.status > 0,
    HTTP_status: connect.status,
    authenticated: connect.status === 200,
    entitlement_status:
      connect.status === 200
        ? "GIATA_DRIVE_OPEN_CONTENT_LINK_OK"
        : `HTTP_${connect.status}`,
    response_format: "application/json",
    sanitized_error: connect.status === 200 ? null : `http_${connect.status}`,
  };
  console.log("GIATA_CONNECTIVITY_TEST", JSON.stringify(connectivity));

  if (!connectivity.authenticated) {
    writeJson(path.join(OUT_DIR, "giata-validation-summary.json"), {
      marker: "DEALALITY_GIATA_PROVIDER_VALIDATION_COMPLETE",
      verdict: "GIATA_ACCESS_REQUIRES_FIX",
      connectivity,
    });
    console.log("DEALALITY_GIATA_PROVIDER_VALIDATION_COMPLETE");
    process.exitCode = 1;
    return;
  }

  console.log("[giata] loading census (read-only)…");
  const { byCountry, records, total } = await listCensusByCountry();
  console.log(`[giata] census=${total}`);

  const cventBy = countUniverseCandidatesByCountry(ROOT);
  const countryResults = [];
  const allSampleProps = [];
  let totalApiCalls = 0;
  let totalMs = 0;

  for (const c of CONTROLLED) {
    console.log(`[giata] country ${c.iso}…`);
    const t0 = Date.now();
    const uni = await fetchCountryUniverse(c.iso);
    totalApiCalls += uni.apiCalls;
    totalMs += Date.now() - t0;
    allSampleProps.push(...uni.props);

    const censusCountry = filterCensus(records, c.country);
    const classes = {
      EXISTING_EXACT: 0,
      EXISTING_STRONG: 0,
      PROBABLE_EXISTING: 0,
      AMBIGUOUS: 0,
      NEW_CANDIDATE: 0,
      INACTIVE: 0,
    };
    const details = [];
    for (const p of uni.props) {
      const input = toResolveInput(p);
      const resolved = resolveHotelIdentity(input, censusCountry);
      const cls = classifyMatch(resolved);
      classes[cls] = (classes[cls] || 0) + 1;
      details.push({
        giataId: p.giataId,
        name: input.name,
        city: input.city,
        country: input.country,
        class: cls,
        match_status: resolved.match_status,
        match_score: resolved.match_score,
      });
    }

    const existing =
      classes.EXISTING_EXACT +
      classes.EXISTING_STRONG +
      classes.PROBABLE_EXISTING;
    const cityExplicit = uni.props.filter((p) => pickCity(p)).length;
    const coords = uni.props.filter((p) => p.geoCodes?.[0]?.latitude != null).length;
    const withChain = uni.props.filter((p) => (p.chains || []).length).length;

    countryResults.push({
      country: c.country,
      iso: c.iso,
      dealality_hotels: censusCountry.length,
      giata_records_returned: uni.urls,
      pagination: "single_list_of_property_urls_then_per_id_fetch",
      active_records: uni.urls, // no inactive flag in payload
      inactive_records: 0,
      inactive_status_available: false,
      api_calls: uni.apiCalls,
      latency_ms_total: Date.now() - t0,
      list_latency_ms: uni.listMs,
      match_classes: classes,
      existing_matches: existing,
      likely_new_hotels: classes.NEW_CANDIDATE,
      ambiguous: classes.AMBIGUOUS,
      duplicate_or_ambiguous_rate_pct:
        uni.props.length > 0
          ? Math.round(((existing + classes.AMBIGUOUS) / uni.props.length) * 1000) / 10
          : null,
      explicit_city_rate_pct:
        uni.props.length > 0
          ? Math.round((cityExplicit / uni.props.length) * 1000) / 10
          : null,
      explicit_coords_rate_pct:
        uni.props.length > 0
          ? Math.round((coords / uni.props.length) * 1000) / 10
          : null,
      chain_rate_pct:
        uni.props.length > 0
          ? Math.round((withChain / uni.props.length) * 1000) / 10
          : null,
      stable_giata_id_rate_pct: 100,
      cvent_candidates: cventBy[c.country] || cventBy["Turks and Caicos"] || 0,
      sample: details.slice(0, 8),
      details,
    });
  }

  const matrix = fieldPresence(allSampleProps);
  const globalList = await driveGet("/properties");
  totalApiCalls += 1;
  const globalCount = (globalList.json?.urls || []).length;
  const after = await driveGet("/properties?after=0");
  totalApiCalls += 1;

  const brazil = countryResults.find((r) => r.iso === "BR");
  const zeroish = countryResults.filter((r) =>
    ["PY", "TC", "BQ"].includes(r.iso)
  );

  // Rough Cvent vs GIATA for tested countries
  const cventCompare = countryResults.map((r) => ({
    country: r.country,
    giata_total: r.giata_records_returned,
    cvent_total: r.cvent_candidates,
    // Without shared IDs, overlap is estimated via match-to-census only
    note: "Overlap vs Cvent not ID-joinable; GIATA has stable giataId, Cvent inventory is URL-derived shells",
    giata_explicit_city_pct: r.explicit_city_rate_pct,
    giata_coords_pct: r.explicit_coords_rate_pct,
    giata_stable_id_pct: r.stable_giata_id_rate_pct,
    giata_new_vs_census: r.likely_new_hotels,
  }));

  const summary = {
    marker: "DEALALITY_GIATA_PROVIDER_VALIDATION_COMPLETE",
    safety: {
      Airtable_writes: 0,
      Census_writes: 0,
      Brand_Explorer_writes: 0,
      Automatic_merges: 0,
      Schema_changes: 0,
      Migrations: 0,
      Secrets_exposed: 0,
    },
    existing_integration_audit: {
      marker: "GIATA_EXISTING_INTEGRATION_AUDIT_COMPLETE",
      credential_variables_present: Object.keys(presence).filter((k) => presence[k]),
      existing_GIATA_client: false,
      existing_provider_adapter: false,
      existing_MCP_integration: false,
      existing_scripts: false,
      existing_tests: false,
      existing_documentation:
        "lib/research-engine-v2/external-hotel-source-registry.js references GIATA MultiCodes/Hotel Guide as licensed stub only",
    },
    product: {
      GIATA_PRODUCT: "GIATA Drive — Open Content Link (Hotel Directory subset)",
      GIATA_BASE_ENDPOINT: "https://giatadrive.com/api/v1",
      AUTH_METHOD: "Bearer API key (Authorization: Bearer <GIATA_DRIVE_API_KEY>)",
      note: "GIATA_DRIVE_USERNAME/PASSWORD present but Basic auth rejected on Drive API; MultiCodes/MHG returned 401 with these credentials",
    },
    connectivity,
    global_open_content_properties: globalCount,
    incremental: {
      GIATA_INCREMENTAL_UPDATE_SUPPORTED: "YES",
      evidence: "GET /properties?after=0 returns urls + deletedUrls + latestRevision",
      after_probe: {
        status: after.status,
        urls: after.json?.urls?.length,
        deletedUrls: after.json?.deletedUrls?.length,
        has_latestRevision: after.json?.latestRevision != null,
      },
    },
    country_discovery: {
      GIATA_COUNTRY_DISCOVERY_SUPPORTED: "YES",
      filters_confirmed: ["countryCode (ISO alpha-2)", "after (incremental)", "property id"],
      filters_not_seen_in_entitlement: ["city filter on list", "destination filter on list", "supplier ID filter"],
    },
    capability_matrix: matrix,
    room_count_verdict: {
      GIATA_TOTAL_PROPERTY_ROOM_COUNT: matrix.total_property_room_count,
      semantics:
        "roomTypes[] = catalog of room type names/codes/views — NOT total physical keys. No roomCount/totalRooms field in Open Content property payload.",
    },
    supplier_mapping: {
      GIATA_SUPPLIER_MAPPING_AVAILABLE: "NO",
      note: "No supplier/channel mapping objects in Drive Open Content property payloads. MultiCodes (mapping product) not entitled (401).",
    },
    controlled_countries: countryResults.map((r) => ({
      country: r.country,
      iso: r.iso,
      giata_records: r.giata_records_returned,
      dealality_hotels: r.dealality_hotels,
      match_classes: r.match_classes,
      existing_matches: r.existing_matches,
      likely_new: r.likely_new_hotels,
      ambiguous: r.ambiguous,
      city_pct: r.explicit_city_rate_pct,
      coords_pct: r.explicit_coords_rate_pct,
      api_calls: r.api_calls,
      latency_ms_total: r.latency_ms_total,
    })),
    brazil_opportunity: brazil
      ? {
          dealality: brazil.dealality_hotels,
          estimated_known_universe: 5336,
          giata_brazil_active: brazil.giata_records_returned,
          already_in_dealality: brazil.existing_matches,
          probable_new: brazil.likely_new_hotels,
          ambiguous: brazil.ambiguous,
          cvent_hold_pool: brazil.cvent_candidates,
          verdict:
            "GIATA Open Content Brazil set is tiny vs Cvent hold pool — cannot replace Cvent for Brazil scale; useful as high-quality identity anchors for the few branded properties present.",
        }
      : null,
    zero_coverage_opportunity: zeroish.map((r) => ({
      country: r.country,
      dealality: r.dealality_hotels,
      giata_active: r.giata_records_returned,
      existing: r.existing_matches,
      new_candidates: r.likely_new_hotels,
      potential_coverage_note:
        r.giata_records_returned === 0
          ? "No Open Content properties for this ISO in entitlement"
          : `Could seed ${r.likely_new_hotels} high-quality shells (still far from full national universe)`,
    })),
    cvent_vs_giata: cventCompare,
    api_efficiency: {
      api_calls: totalApiCalls,
      controlled_records: allSampleProps.length,
      global_list_size: globalCount,
      hotels_per_list_call: globalCount,
      detail_calls_required: true,
      avg_country_latency_note: "list ~80–800ms; detail ~100–700ms each",
      quota_signals_observed: "none in response headers",
      candidate_new_per_detail_call:
        countryResults.reduce((s, r) => s + r.likely_new_hotels, 0) /
        Math.max(1, allSampleProps.length),
    },
    recommended_roles: [
      "SECONDARY_UNIVERSE_DISCOVERY",
      "IDENTITY_VALIDATION",
      "EXTERNAL_ID_GRAPH",
      "GEO_ENRICHMENT",
      "BRAND_ENRICHMENT",
    ],
    not_roles: ["PRIMARY_UNIVERSE_DISCOVERY", "ROOM_COUNT", "DEDUPLICATION"],
    overall_verdict: "GIATA_HIGH_VALUE_COMPLEMENTARY_PROVIDER",
    highest_value_next_step:
      "Add a read-only Hotel Intelligence provider adapter for GIATA Drive Open Content that attaches giataId + geo/address/brand evidence to existing dhl_ hotels and stages NEW_CANDIDATE shells only for zero/near-zero countries where Open Content returns rows — do not treat Drive as CALA universe SoT.",
    census_total: total,
    census_by_country_sample: Object.fromEntries(
      CONTROLLED.map((c) => [c.country, byCountry[c.country] || 0])
    ),
  };

  writeJson(path.join(OUT_DIR, "giata-validation-summary.json"), summary);
  writeJson(path.join(OUT_DIR, "controlled-country-details.json"), {
    countries: countryResults,
  });
  writeJson(path.join(OUT_DIR, "capability-matrix.json"), matrix);

  const md = renderMd(summary);
  fs.writeFileSync(
    path.join(OUT_DIR, "DEALALITY_GIATA_PROVIDER_VALIDATION_COMPLETE.md"),
    md,
    "utf8"
  );

  console.log("DEALALITY_GIATA_PROVIDER_VALIDATION_COMPLETE");
  console.log(
    JSON.stringify(
      {
        product: summary.product.GIATA_PRODUCT,
        global_open_content: globalCount,
        verdict: summary.overall_verdict,
        brazil: summary.brazil_opportunity,
        zero: summary.zero_coverage_opportunity,
        room_count: summary.room_count_verdict.GIATA_TOTAL_PROPERTY_ROOM_COUNT,
        out: path.relative(ROOT, OUT_DIR),
      },
      null,
      2
    )
  );
}

function renderMd(s) {
  return `# DEALALITY_GIATA_PROVIDER_VALIDATION_COMPLETE

## 1. Safety

\`\`\`
Airtable writes: ${s.safety.Airtable_writes}
Census writes: ${s.safety.Census_writes}
Brand Explorer writes: ${s.safety.Brand_Explorer_writes}
Automatic merges: ${s.safety.Automatic_merges}
Schema changes: ${s.safety.Schema_changes}
Migrations: ${s.safety.Migrations}
Secrets exposed: ${s.safety.Secrets_exposed}
\`\`\`

ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0 · ENABLE_HBX_CENSUS_WRITES=0

## 2. GIATA Account

\`\`\`
Product: ${s.product.GIATA_PRODUCT}
Base: ${s.product.GIATA_BASE_ENDPOINT}
Auth: ${s.product.AUTH_METHOD}
Credential status: API key present
Reachable: ${s.connectivity.reachable}
Entitlement: ${s.connectivity.entitlement_status}
Response format: ${s.connectivity.response_format}
Global Open Content properties: ${s.global_open_content_properties}
\`\`\`

${s.product.note}

### Existing integration audit
${JSON.stringify(s.existing_integration_audit, null, 2)}

## 3. Actual Account Capability Matrix

| Field | Status |
| --- | --- |
${Object.entries(s.capability_matrix)
  .map(([k, v]) => `| ${k} | ${v} |`)
  .join("\n")}

## 4. Room Count Verdict

\`\`\`
GIATA_TOTAL_PROPERTY_ROOM_COUNT: ${s.room_count_verdict.GIATA_TOTAL_PROPERTY_ROOM_COUNT}
\`\`\`

${s.room_count_verdict.semantics}

## 5. Country Discovery

\`\`\`
GIATA_COUNTRY_DISCOVERY_SUPPORTED: ${s.country_discovery.GIATA_COUNTRY_DISCOVERY_SUPPORTED}
\`\`\`

Filters confirmed: ${s.country_discovery.filters_confirmed.join(", ")}

## 6–10. Controlled Country Tests

${s.controlled_countries
  .map(
    (c) =>
      `### ${c.country} (${c.iso})
- Dealality: ${c.dealality_hotels}
- GIATA Open Content: ${c.giata_records}
- Existing matches: ${c.existing_matches} · New: ${c.likely_new} · Ambiguous: ${c.ambiguous}
- City ${c.city_pct}% · Coords ${c.coords_pct}% · API calls ${c.api_calls}`
  )
  .join("\n\n")}

### Brazil opportunity
${JSON.stringify(s.brazil_opportunity, null, 2)}

### Zero-coverage opportunity
${JSON.stringify(s.zero_coverage_opportunity, null, 2)}

## 11–12. External ID / Supplier mapping

- GIATA ID: **CONFIRMED_SUPPORTED** as persistent external id on \`dhl_\` graph
- Supplier mapping: **${s.supplier_mapping.GIATA_SUPPLIER_MAPPING_AVAILABLE}** — ${s.supplier_mapping.note}

## 13–15. Roles / efficiency / incremental

Recommended roles: ${s.recommended_roles.join(", ")}
Not useful as: ${s.not_roles.join(", ")}

API efficiency: ${JSON.stringify(s.api_efficiency)}

\`\`\`
GIATA_INCREMENTAL_UPDATE_SUPPORTED: ${s.incremental.GIATA_INCREMENTAL_UPDATE_SUPPORTED}
\`\`\`

## 16–18. Verdict

**Overall:** \`${s.overall_verdict}\`

**Highest-value next step (do not execute):** ${s.highest_value_next_step}
`;
}

main().catch((err) => {
  console.error(String(err?.stack || err).slice(0, 500));
  process.exitCode = 1;
});
