#!/usr/bin/env node
/**
 * Founder-authorized geocode staging for HPC coordinate completeness.
 *
 * - Reads the allowlisted HPC records only.
 * - Reuses existing verified coords when present.
 * - Otherwise calls the configured GEOCODING_PROVIDER (Mapbox/Google).
 * - Writes staging evidence JSON/MD — NEVER writes Airtable.
 *
 * Usage:
 *   node scripts/stage-hpc-coordinate-completeness-geocode-v1.mjs \
 *     --ids recH9h3McgMNFQEla,recHXCkkKD5Xwue3p \
 *     --founder-auth FOUNDER_AUTH_HPC_COORD_GEOCODE_LOOKUP_V1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
  HPC_RADAR_STATUS_FIELD,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import {
  HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
  validateHpcCoordinates,
  buildProposedCoordinatePatch,
  detectCityCenterProximity,
  readHpcCoordinates,
} from "../lib/hotel-census/hpc-coordinate-completeness-gate-v1.js";
import {
  resolveGeocodingProvider,
  geocodeOfficialAddress,
  isStreetLevelAddress,
  buildOfficialGeocodeQuery,
  placeMatchesCensus,
} from "../lib/research-engine-v2/production-census-geocoding-providers.js";
import { isValidCoordPair } from "../lib/research-engine-v2/production-census-coordinate-extractor.js";

/**
 * Staging-only recovery when the shared provider returns multiple_divergent_candidates
 * but the top Mapbox feature is a high-relevance street address that matches Census.
 * Still reject city-center / non-address / place mismatch. Never writes Airtable.
 */
async function recoverTopAddressCandidate(query, census, propertyName) {
  const token = String(
    process.env.MAPBOX_ACCESS_TOKEN || process.env.MAPBOX_TOKEN || ""
  ).trim();
  if (!token || !query) return { ok: false, reason: "recovery_unavailable" };
  const permanent = String(process.env.MAPBOX_PERMANENT_GEOCODING || "").trim() === "1";
  const url =
    `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(query)}.json` +
    `?access_token=${encodeURIComponent(token)}` +
    `&limit=5&types=address,poi&language=en` +
    (permanent ? "&permanent=true" : "");
  const res = await fetch(url);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, reason: `recovery_http_${res.status}` };
  const best = (json.features || [])[0];
  if (!best) return { ok: false, reason: "recovery_zero_results" };
  const [lng, lat] = best.center || [];
  if (!isValidCoordPair(Number(lat), Number(lng))) {
    return { ok: false, reason: "recovery_invalid_coords" };
  }
  const placeTypes = best.place_type || [];
  if (!placeTypes.includes("address") && !placeTypes.includes("poi")) {
    return { ok: false, reason: "recovery_not_address_or_poi", placeTypes };
  }
  const placeName = best.place_name || "";
  const match = placeMatchesCensus(placeName, census);
  if (!match.ok) {
    return { ok: false, reason: "recovery_place_mismatch", failures: match.failures, placeName };
  }
  const relevance = Number(best.relevance || 0);
  if (relevance < 0.8) {
    return { ok: false, reason: "recovery_low_relevance", relevance, placeName };
  }
  const cityHit = detectCityCenterProximity({
    lat: Number(lat),
    lng: Number(lng),
    city: census.city,
    country: census.country,
    confirmedFallback: placeTypes.some((t) => /place|locality|region/.test(String(t))),
  });
  if (cityHit.confirmed) {
    return { ok: false, reason: "recovery_confirmed_city_center", cityHit };
  }
  return {
    ok: true,
    lat: Number(lat),
    lng: Number(lng),
    provider: "mapbox",
    method: permanent ? "mapbox_permanent_geocode" : "mapbox_temporary_geocode",
    confidence: "Medium",
    query,
    formatted_address: placeName,
    relevance,
    permanent,
    place_types: placeTypes,
    recovery: "top_address_after_divergent_candidates",
    propertyName,
    softFlags: cityHit.soft
      ? ["POTENTIAL_CITY_CENTER_UNCONFIRMED", "INCOMPLETE_ADDRESS_MATCH"]
      : ["INCOMPLETE_ADDRESS_MATCH"],
  };
}

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_JSON = path.join(
  ROOT,
  "reports/hotel-explorer/hpc-coordinate-completeness-geocode-staged-v1.json"
);
const OUT_MD = path.join(
  ROOT,
  "reports/hotel-explorer/hpc-coordinate-completeness-geocode-staged-v1.md"
);

/** Founder-authorized lookup set for this run only. */
const AUTHORIZED_IDS = Object.freeze([
  "recH9h3McgMNFQEla",
  "recHXCkkKD5Xwue3p",
]);

const FOUNDATION_AUTH = "FOUNDER_AUTH_HPC_COORD_GEOCODE_LOOKUP_V1";

function loadEnv(file) {
  const out = {};
  if (!fs.existsSync(file)) return out;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const i = t.indexOf("=");
    if (i < 0) continue;
    const k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    out[k] = v;
  }
  return out;
}

function argValue(argv, flag) {
  const i = argv.indexOf(flag);
  if (i < 0 || i + 1 >= argv.length) return null;
  return argv[i + 1];
}

const env = {
  ...loadEnv(path.join("C:/Dev/dealality/my-operators-backend", ".env")),
  ...loadEnv(path.join(ROOT, ".env")),
};
for (const [k, v] of Object.entries(env)) {
  if (process.env[k] == null || process.env[k] === "") process.env[k] = v;
}

const argv = process.argv.slice(2);
const auth = argValue(argv, "--founder-auth");
if (auth !== FOUNDATION_AUTH) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "founder_auth_required",
      expected: FOUNDATION_AUTH,
    })
  );
  process.exit(2);
}

const idsRaw = argValue(argv, "--ids") || AUTHORIZED_IDS.join(",");
const requestedIds = idsRaw
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

for (const id of requestedIds) {
  if (!AUTHORIZED_IDS.includes(id)) {
    console.error(
      JSON.stringify({
        ok: false,
        error: "id_not_in_founder_authorized_set",
        id,
        authorized: AUTHORIZED_IDS,
      })
    );
    process.exit(2);
  }
}

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (!key || !baseId) {
  console.error(JSON.stringify({ ok: false, error: "missing_airtable_config" }));
  process.exit(1);
}

const providerInfo = resolveGeocodingProvider(process.env.GEOCODING_PROVIDER);
const base = new Airtable({ apiKey: key }).base(baseId);
const verifiedAt = new Date().toISOString().slice(0, 10);
const results = [];
let geocodeCalls = 0;

for (const id of requestedIds) {
  const rec = await base(HOTEL_PROPERTY_CENSUS_TABLE).find(id);
  const f = rec.fields || {};
  const name =
    String(f["Property Name"] || f["Canonical Property Name"] || "").trim() || null;
  const address = String(f.Address || "").trim() || null;
  const city = String(f.City || "").trim() || null;
  const country = String(f.Country || "").trim() || null;
  const state = String(f["State / Region"] || "").trim() || null;
  const postal = String(f["Postal Code"] || "").trim() || null;
  const status = String(f[HPC_RADAR_STATUS_FIELD] || "").trim() || null;
  const { lat: existingLat, lng: existingLng } = readHpcCoordinates(f);

  const beforeValidation = validateHpcCoordinates({
    lat: existingLat,
    lng: existingLng,
    city,
    country,
    address,
    coordinateConfidence: f["Coordinate Confidence"],
    coordinateSourceType: f["Coordinate Source Type"],
    geocodeProvider: f["Geocode Provider"],
  });

  let resolution = {
    mode: null,
    geocode: null,
    rejected: null,
    patch: null,
  };

  if (beforeValidation.mapEligible && beforeValidation.ok) {
    resolution.mode = "REUSE_EXISTING_VERIFIED_COORDINATES";
    resolution.patch = buildProposedCoordinatePatch({
      before: f,
      geocode: {
        lat: existingLat,
        lng: existingLng,
        provider: String(f["Geocode Provider"] || "existing").toLowerCase(),
        method: f["Geocode Method"] || "existing_source",
        confidence: f["Coordinate Confidence"] || "High",
        formatted_address: address,
        query: null,
        permanent: true,
      },
      verifiedAt,
    });
  } else {
    if (!isStreetLevelAddress(address)) {
      resolution.mode = "BLOCKED_ADDRESS_NOT_STREET_LEVEL";
    } else if (!providerInfo.credentials_ok || providerInfo.provider === "none") {
      resolution.mode = "BLOCKED_GEOCODER_UNAVAILABLE";
      resolution.rejected = { reason: "geocoder_unavailable", providerInfo };
    } else {
      const query = buildOfficialGeocodeQuery({
        name,
        address,
        city,
        state,
        country,
      });
      geocodeCalls += 1;
      let geo = await geocodeOfficialAddress(
        { name, address, city, state, country },
        providerInfo
      );
      if (
        !geo.ok &&
        geo.reason === "multiple_divergent_candidates" &&
        providerInfo.provider === "mapbox"
      ) {
        geocodeCalls += 1;
        const recovered = await recoverTopAddressCandidate(
          query,
          { city, state, country },
          name
        );
        if (recovered.ok) {
          geo = recovered;
          resolution.recoveryApplied = true;
        }
      }
      resolution.geocode = {
        ok: geo.ok === true,
        reason: geo.reason || null,
        lat: geo.lat ?? null,
        lng: geo.lng ?? null,
        provider: geo.provider || providerInfo.provider,
        method: geo.method || null,
        confidence: geo.confidence || null,
        formatted_address: geo.formatted_address || null,
        query: geo.query || query,
        relevance: geo.relevance ?? null,
        location_type: geo.location_type ?? null,
        permanent: geo.permanent ?? null,
        failures: geo.failures || null,
        recovery: geo.recovery || null,
        softFlags: geo.softFlags || null,
      };

      if (!geo.ok) {
        resolution.mode = "GEOCODE_FAILED";
        resolution.rejected = { reason: geo.reason, detail: geo };
      } else {
        const cityHit = detectCityCenterProximity({
          lat: geo.lat,
          lng: geo.lng,
          city,
          country,
          confirmedFallback:
            Array.isArray(geo.place_types) &&
            geo.place_types.some((t) => /place|locality|region/.test(String(t))),
        });
        const post = validateHpcCoordinates({
          lat: geo.lat,
          lng: geo.lng,
          city,
          country,
          address,
          coordinateConfidence: geo.confidence,
          confirmedCityCenterFallback: cityHit.confirmed,
          incompleteAddressMatch: Boolean(geo.failures?.length),
          requireProvenance: true,
          coordinateSourceType: "official_address_geocode",
          geocodeProvider: geo.provider,
        });

        if (cityHit.confirmed || post.hardFail) {
          resolution.mode = "GEOCODE_REJECTED_BY_GATE";
          resolution.rejected = {
            reason: cityHit.confirmed
              ? "confirmed_city_center_fallback"
              : post.hardIssues[0],
            cityHit,
            validation: post,
          };
        } else {
          resolution.mode = "STAGED_OFFICIAL_ADDRESS_GEOCODE";
          resolution.patch = buildProposedCoordinatePatch({
            before: f,
            geocode: geo,
            verifiedAt,
          });
          resolution.postValidation = post;
          const soft = new Set([
            ...(geo.softFlags || []),
            ...(cityHit.soft ? ["POTENTIAL_CITY_CENTER_UNCONFIRMED"] : []),
            ...(post.softIssues || []),
          ]);
          if (soft.size) resolution.softFlags = [...soft];
          if (cityHit.label) resolution.cityCenterLabel = cityHit.label;
        }
      }
    }
  }

  results.push({
    hpcRecordId: id,
    propertyName: name,
    radarStatus: status,
    address,
    city,
    country,
    postalCode: postal,
    before: {
      Latitude: existingLat,
      Longitude: existingLng,
      "Coordinate Source Type": f["Coordinate Source Type"] || null,
      "Coordinate Confidence": f["Coordinate Confidence"] || null,
      "Geocode Provider": f["Geocode Provider"] || null,
      "Geocode Method": f["Geocode Method"] || null,
      "Geocode Reviewed Date": f["Geocode Reviewed Date"] || null,
    },
    beforeValidation,
    resolution,
    airtableWrite: false,
    statusPreserved: true,
  });
}

const report = {
  ok: true,
  contract: HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
  founderAuth: FOUNDATION_AUTH,
  airtableWrite: false,
  paidGeocodeCalls: geocodeCalls,
  geocodeCallCap: AUTHORIZED_IDS.length,
  provider: {
    provider: providerInfo.provider,
    credentials_ok: providerInfo.credentials_ok,
    permanent_storage_enabled: providerInfo.permanent_storage_enabled ?? null,
    storage_terms_reviewed: providerInfo.storage_terms_reviewed ?? null,
  },
  generatedAt: new Date().toISOString(),
  verifiedAt,
  results,
  proposedAirtableChanges: results
    .filter((r) => r.resolution?.patch?.after)
    .map((r) => ({
      id: r.hpcRecordId,
      fields: r.resolution.patch.after,
      evidence: r.resolution.patch.evidence,
      mode: r.resolution.mode,
    })),
};

fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

const md = [];
md.push("# HPC Coordinate Completeness — Geocode Staging v1");
md.push("");
md.push(`**Contract:** \`${HPC_COORDINATE_COMPLETENESS_GATE_VERSION}\``);
md.push(`**Generated:** ${report.generatedAt}`);
md.push(`**Airtable writes:** none`);
md.push(`**Geocode API calls:** ${geocodeCalls} (cap ${AUTHORIZED_IDS.length})`);
md.push(`**Provider:** ${providerInfo.provider}`);
md.push("");
for (const r of results) {
  md.push(`## ${r.propertyName} (\`${r.hpcRecordId}\`)`);
  md.push("");
  md.push(`- Status (preserved): **${r.radarStatus}**`);
  md.push(`- Resolution: \`${r.resolution.mode}\``);
  md.push(`- Address: ${r.address}, ${r.city}, ${r.country} ${r.postalCode || ""}`);
  if (r.resolution.patch) {
    md.push("");
    md.push("| Field | Before | After (proposed) |");
    md.push("| --- | --- | --- |");
    for (const field of Object.keys(r.resolution.patch.after)) {
      md.push(
        `| ${field} | ${r.resolution.patch.before[field] ?? "null"} | ${r.resolution.patch.after[field] ?? "null"} |`
      );
    }
    md.push("");
    md.push(`- Formatted match: ${r.resolution.patch.evidence.formattedAddress || "—"}`);
    md.push(`- Confidence: ${r.resolution.patch.evidence.confidence}`);
    md.push(`- Method: ${r.resolution.patch.evidence.method}`);
  } else if (r.resolution.rejected) {
    md.push(`- Rejected: \`${r.resolution.rejected.reason}\``);
  }
  md.push("");
}
md.push(`Machine-readable: \`${path.relative(ROOT, OUT_JSON).replace(/\\\\/g, "/")}\``);
fs.writeFileSync(OUT_MD, md.join("\n"));

console.log(
  JSON.stringify(
    {
      ok: true,
      outJson: OUT_JSON,
      outMd: OUT_MD,
      geocodeCalls,
      modes: results.map((r) => ({ id: r.hpcRecordId, mode: r.resolution.mode })),
      proposedCount: report.proposedAirtableChanges.length,
    },
    null,
    2
  )
);
