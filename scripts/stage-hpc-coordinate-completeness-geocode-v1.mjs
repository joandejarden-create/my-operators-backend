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
  createGeocodeRequestBudget,
  recoverTopCandidateFromDivergentGeocode,
} from "../lib/hotel-census/hpc-coordinate-completeness-gate-v1.js";
import {
  resolveGeocodingProvider,
  geocodeOfficialAddress,
  isStreetLevelAddress,
  buildOfficialGeocodeQuery,
} from "../lib/research-engine-v2/production-census-geocoding-providers.js";

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
const maxPaid = Number(argValue(argv, "--max-paid-requests") || requestedIds.length);
const budget = createGeocodeRequestBudget({
  maxPaidRequests: maxPaid,
  authorizationLabel: FOUNDATION_AUTH,
});
const offline = argv.includes("--offline-from-prior");

if (offline) {
  if (!fs.existsSync(OUT_JSON)) {
    console.error(JSON.stringify({ ok: false, error: "missing_prior_staging_json" }));
    process.exit(1);
  }
  const prior = JSON.parse(fs.readFileSync(OUT_JSON, "utf8"));
  const revised = {
    ...prior,
    regeneratedAt: new Date().toISOString(),
    offlineFromPrior: true,
    additionalPaidGeocodeCallsThisRun: 0,
    budgetEnforcement: {
      maxPaidRequests: maxPaid,
      historicalPaidCallsBeforeBudgetFix: prior.paidGeocodeCalls ?? prior.geocodeCalls ?? null,
      note: "No new paid calls. Divergent-candidate recovery must reuse the same response (0 extra paid calls).",
    },
    provider: {
      ...(prior.provider || {}),
      permanent_storage_enabled:
        prior.provider?.permanent_storage_enabled ??
        providerInfo.permanent_storage_enabled ??
        null,
      permanentStoragePolicy:
        "MAPBOX_PERMANENT_GEOCODING=1 required for permanent Census storage; temporary geocodes must not be written.",
    },
  };
  for (const row of revised.results || []) {
    if (row.resolution?.patch?.evidence) {
      row.resolution.patch.evidence.officialWebsiteSuppliedCoordinates = false;
      row.resolution.patch.evidence.coordinateSourceAttribution =
        "Mapbox permanent geocode of official street address — not hotel website coordinates";
    }
  }
  fs.writeFileSync(OUT_JSON, JSON.stringify(revised, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: "offline_from_prior",
        additionalPaidGeocodeCallsThisRun: 0,
        outJson: OUT_JSON,
        proposedCount: (revised.proposedAirtableChanges || []).length,
      },
      null,
      2
    )
  );
  process.exit(0);
}

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
      const consume = budget.tryConsume(1, `geocode:${id}`);
      if (!consume.ok) {
        resolution.mode = "BLOCKED_GEOCODE_BUDGET_EXCEEDED";
        resolution.rejected = consume;
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
        continue;
      }
      let geo = await geocodeOfficialAddress(
        { name, address, city, state, country },
        providerInfo
      );
      // Zero extra paid calls: recover top address from the same response payload.
      if (!geo.ok && geo.reason === "multiple_divergent_candidates") {
        const recovered = recoverTopCandidateFromDivergentGeocode(geo, {
          city,
          state,
          country,
        });
        if (recovered.ok) {
          geo = recovered;
          resolution.recoveryApplied = true;
          resolution.recoveryPaidCallsAdded = 0;
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

const budgetSnap = budget.snapshot();
const report = {
  ok: true,
  contract: HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
  founderAuth: FOUNDATION_AUTH,
  airtableWrite: false,
  paidGeocodeCalls: budgetSnap.used,
  geocodeCallCap: budgetSnap.maxPaidRequests,
  budget: budgetSnap,
  budgetExceeded: results.some((r) => r.resolution?.mode === "BLOCKED_GEOCODE_BUDGET_EXCEEDED"),
  provider: {
    provider: providerInfo.provider,
    credentials_ok: providerInfo.credentials_ok,
    permanent_storage_enabled: providerInfo.permanent_storage_enabled ?? null,
    storage_terms_reviewed: providerInfo.storage_terms_reviewed ?? null,
    permanentStoragePolicy:
      "MAPBOX_PERMANENT_GEOCODING=1 required for permanent Census storage; temporary geocodes must not be written.",
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
md.push(
  `**Geocode API calls:** ${budgetSnap.used} (cap ${budgetSnap.maxPaidRequests}; recovery adds 0 paid calls)`
);
md.push(`**Provider:** ${providerInfo.provider}`);
md.push(
  `**Permanent storage flag:** ${providerInfo.permanent_storage_enabled === true ? "MAPBOX_PERMANENT_GEOCODING=1" : "not enabled"}`
);
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
      paidGeocodeCalls: budgetSnap.used,
      geocodeCallCap: budgetSnap.maxPaidRequests,
      budgetExceeded: report.budgetExceeded,
      modes: results.map((r) => ({ id: r.hpcRecordId, mode: r.resolution.mode })),
      proposedCount: report.proposedAirtableChanges.length,
    },
    null,
    2
  )
);
