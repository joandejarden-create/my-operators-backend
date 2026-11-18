#!/usr/bin/env node
/**
 * Read-only HPC Open|Pipeline coordinate completeness audit.
 * No Airtable writes. No paid geocoding. No secrets printed.
 *
 * Contract: HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1 (audit phase)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
  HPC_RADAR_STATUS_FIELD,
  buildHpcRadarStatusFilterFormula,
} from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_JSON = path.join(
  ROOT,
  "reports/hotel-explorer/hpc-open-pipeline-coordinate-completeness-audit-v1.json"
);
const OUT_MD = path.join(
  ROOT,
  "reports/hotel-explorer/hpc-open-pipeline-coordinate-completeness-audit-v1.md"
);

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

const env = {
  ...loadEnv(path.join("C:/Dev/dealality/my-operators-backend", ".env")),
  ...loadEnv(path.join(ROOT, ".env")),
};
for (const [k, v] of Object.entries(env)) {
  if (process.env[k] == null || process.env[k] === "") process.env[k] = v;
}

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (!key || !baseId) {
  console.error(JSON.stringify({ ok: false, error: "missing_airtable_config" }));
  process.exit(1);
}

/** Approximate country centroid / capital-ish traps (not exhaustive). */
const COUNTRY_CENTER_TRAPS = [
  { country: /united states|usa/i, lat: 39.8283, lng: -98.5795, tol: 0.15 },
  { country: /canada/i, lat: 56.1304, lng: -106.3468, tol: 0.2 },
  { country: /mexico/i, lat: 23.6345, lng: -102.5528, tol: 0.2 },
  { country: /spain/i, lat: 40.4637, lng: -3.7492, tol: 0.15 },
  { country: /united kingdom|uk|england/i, lat: 55.3781, lng: -3.436, tol: 0.2 },
];

/** Well-known city centers used as last-resort geocodes (suspicious if exact). */
const CITY_CENTER_TRAPS = [
  { city: /toronto/i, country: /canada/i, lat: 43.6532, lng: -79.3832, tol: 0.002 },
  { city: /london/i, country: /united kingdom|uk/i, lat: 51.5074, lng: -0.1278, tol: 0.002 },
  { city: /madrid/i, country: /spain/i, lat: 40.4168, lng: -3.7038, tol: 0.002 },
  { city: /mexico city|ciudad de m[eé]xico/i, country: /mexico/i, lat: 19.4326, lng: -99.1332, tol: 0.002 },
  { city: /washington/i, country: /united states|usa/i, lat: 38.9072, lng: -77.0369, tol: 0.002 },
  { city: /san diego/i, country: /united states|usa/i, lat: 32.7157, lng: -117.1611, tol: 0.002 },
  { city: /bethesda/i, country: /united states|usa/i, lat: 38.9847, lng: -77.0947, tol: 0.002 },
  { city: /palma/i, country: /spain/i, lat: 39.5696, lng: 2.6502, tol: 0.002 },
];

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function text(v) {
  return v == null ? "" : String(v).trim();
}

function near(a, b, tol) {
  return Math.abs(a - b) <= tol;
}

function classifyCoords(fields) {
  const lat = num(fields.Latitude);
  const lng = num(fields.Longitude);
  const city = text(fields.City);
  const country = text(fields.Country);
  const address = text(fields.Address);
  const issues = [];

  if (lat == null || lng == null) {
    issues.push("MISSING_LATITUDE_OR_LONGITUDE");
    return {
      lat,
      lng,
      issues,
      mapEligible: false,
      hardFail: true,
      softFlags: [],
    };
  }
  if (lat === 0 && lng === 0) {
    issues.push("ZERO_ZERO_COORDINATES");
  }
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    issues.push("OUT_OF_RANGE");
  }
  if (!address) {
    issues.push("MISSING_ADDRESS_FOR_VERIFICATION");
  }

  for (const trap of COUNTRY_CENTER_TRAPS) {
    if (trap.country.test(country) && near(lat, trap.lat, trap.tol) && near(lng, trap.lng, trap.tol)) {
      issues.push("SUSPICIOUS_COUNTRY_CENTER");
    }
  }
  for (const trap of CITY_CENTER_TRAPS) {
    if (
      trap.city.test(city) &&
      trap.country.test(country) &&
      near(lat, trap.lat, trap.tol) &&
      near(lng, trap.lng, trap.tol)
    ) {
      issues.push("SUSPICIOUS_CITY_CENTER");
    }
  }

  // Hemisphere / region sanity (coarse): Canada lat should be ~41–70, lng west.
  if (/canada/i.test(country)) {
    if (lat < 41 || lat > 84 || lng > -50 || lng < -145) {
      issues.push("COUNTRY_BOUNDS_MISMATCH");
    }
  }
  if (/united states|usa/i.test(country)) {
    if (lat < 18 || lat > 72 || lng > -65 || lng < -180) {
      // Hawaii/Alaska edge cases still mostly within; flag extreme only.
      if (lng > -50 || lat < 10 || lat > 75) issues.push("COUNTRY_BOUNDS_MISMATCH");
    }
  }
  if (/spain/i.test(country)) {
    if (lat < 27 || lat > 44 || lng < -19 || lng > 5) {
      issues.push("COUNTRY_BOUNDS_MISMATCH");
    }
  }
  if (/united kingdom|uk|england|scotland|wales/i.test(country)) {
    if (lat < 49 || lat > 61 || lng < -9 || lng > 2) {
      issues.push("COUNTRY_BOUNDS_MISMATCH");
    }
  }

  const hardFail = issues.some((i) =>
    [
      "MISSING_LATITUDE_OR_LONGITUDE",
      "ZERO_ZERO_COORDINATES",
      "OUT_OF_RANGE",
      "COUNTRY_BOUNDS_MISMATCH",
    ].includes(i)
  );

  return {
    lat,
    lng,
    issues,
    mapEligible: !hardFail && lat != null && lng != null && !(lat === 0 && lng === 0),
    hardFail,
    softFlags: issues.filter((i) =>
      [
        "SUSPICIOUS_CITY_CENTER",
        "SUSPICIOUS_COUNTRY_CENTER",
        "MISSING_ADDRESS_FOR_VERIFICATION",
      ].includes(i)
    ),
  };
}

function loadAdpRadissonCoords() {
  const pub = path.join(
    ROOT,
    "data/ai-demand-positioning/published/adp_radisson_blu_toronto_downtown/report-adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b.json"
  );
  const out = { found: false, path: pub, lat: null, lng: null, censusRecordId: null };
  if (!fs.existsSync(pub)) return out;
  const raw = JSON.parse(fs.readFileSync(pub, "utf8"));
  out.censusRecordId =
    raw?.censusRecordId ||
    raw?.hotel?.censusRecordId ||
    raw?.meta?.censusRecordId ||
    raw?.identity?.censusRecordId ||
    null;
  const candidates = [
    raw?.hotel?.lat,
    raw?.hotel?.latitude,
    raw?.property?.lat,
    raw?.property?.latitude,
    raw?.location?.lat,
    raw?.location?.latitude,
    raw?.coordinates?.lat,
    raw?.coordinates?.latitude,
  ];
  const lngs = [
    raw?.hotel?.lng,
    raw?.hotel?.longitude,
    raw?.property?.lng,
    raw?.property?.longitude,
    raw?.location?.lng,
    raw?.location?.longitude,
    raw?.coordinates?.lng,
    raw?.coordinates?.longitude,
  ];
  for (let i = 0; i < candidates.length; i++) {
    const la = num(candidates[i]);
    const ln = num(lngs[i]);
    if (la != null && ln != null) {
      out.found = true;
      out.lat = la;
      out.lng = ln;
      break;
    }
  }
  // Deep scan keys (bounded)
  const blob = JSON.stringify(raw);
  if (!out.found && /249\s*Queens\s*Quay/i.test(blob)) {
    out.addressMentioned = true;
  }
  return out;
}

const base = new Airtable({ apiKey: key }).base(baseId);
const formula = buildHpcRadarStatusFilterFormula();
const records = [];

const CORE_FIELDS = [
  "Property Name",
  "Canonical Property Name",
  "Address",
  "City",
  "Country",
  "Postal Code",
  "State / Region",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
  "Official Property URL",
  "Current Brand",
  HPC_RADAR_STATUS_FIELD,
];
const PROVENANCE_FIELDS = [
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
];

async function fetchOpenPipeline(fields) {
  const out = [];
  await base(HOTEL_PROPERTY_CENSUS_TABLE)
    .select({
      filterByFormula: formula,
      fields,
      pageSize: 100,
    })
    .eachPage((page, next) => {
      for (const r of page) out.push(r);
      next();
    });
  return out;
}

let selectedFields = [...CORE_FIELDS, ...PROVENANCE_FIELDS];
let provenanceAvailable = true;
try {
  records.push(...(await fetchOpenPipeline(selectedFields)));
} catch (err) {
  const msg = String(err?.message || err);
  if (/UNKNOWN_FIELD_NAME|Unknown field/i.test(msg)) {
    provenanceAvailable = false;
    selectedFields = CORE_FIELDS;
    records.push(...(await fetchOpenPipeline(selectedFields)));
  } else {
    throw err;
  }
}

const rows = records.map((r) => {
  const f = r.fields || {};
  const classified = classifyCoords(f);
  return {
    id: r.id,
    name: text(f["Property Name"]) || text(f["Canonical Property Name"]) || null,
    radarStatus: text(f[HPC_RADAR_STATUS_FIELD]) || null,
    address: text(f.Address) || null,
    city: text(f.City) || null,
    country: text(f.Country) || null,
    postalCode: text(f["Postal Code"]) || null,
    market: text(f.Market) || null,
    submarket: text(f.Submarket) || null,
    brand: text(f["Current Brand"]) || null,
    officialUrl: text(f["Official Property URL"]) || null,
    latitude: classified.lat,
    longitude: classified.lng,
    coordinateSourceType: text(f["Coordinate Source Type"]) || null,
    coordinateConfidence: text(f["Coordinate Confidence"]) || null,
    geocodeProvider: text(f["Geocode Provider"]) || null,
    geocodeMethod: text(f["Geocode Method"]) || null,
    geocodeReviewedDate: text(f["Geocode Reviewed Date"]) || null,
    issues: classified.issues,
    hardFail: classified.hardFail,
    softFlags: classified.softFlags,
    mapEligible: classified.mapEligible,
  };
});

const exceptions = rows.filter((r) => r.issues.length > 0);
const hardExceptions = exceptions.filter((r) => r.hardFail);
const softExceptions = exceptions.filter((r) => !r.hardFail && r.softFlags.length > 0);
const openCount = rows.filter((r) => r.radarStatus === "Open").length;
const pipelineCount = rows.filter((r) => r.radarStatus === "Pipeline").length;
const mapEligible = rows.filter((r) => r.mapEligible).length;

const adp = loadAdpRadissonCoords();
const radisson = rows.find((r) => r.id === "recH9h3McgMNFQEla") || null;

const proposedCorrections = [];
if (radisson && radisson.issues.includes("MISSING_LATITUDE_OR_LONGITUDE")) {
  proposedCorrections.push({
    hpcRecordId: "recH9h3McgMNFQEla",
    propertyName: radisson.name,
    radarStatus: radisson.radarStatus,
    resolutionStatus: "STAGED_THIRD_PARTY_CONSENSUS_PENDING_OFFICIAL_GEOCODE",
    before: {
      Latitude: radisson.latitude,
      Longitude: radisson.longitude,
      "Coordinate Source Type": radisson.coordinateSourceType,
      "Coordinate Confidence": radisson.coordinateConfidence,
    },
    after: {
      Latitude: 43.6387,
      Longitude: -79.3855,
      "Coordinate Source Type": "third_party_map_consensus_pending_official_geocode",
      "Coordinate Confidence": "Medium",
      "Geocode Provider": null,
      "Geocode Method": null,
      "Geocode Reviewed Date": null,
    },
    evidence: {
      address: radisson.address,
      city: radisson.city,
      country: radisson.country,
      postalCode: radisson.postalCode,
      submarket: radisson.submarket,
      officialUrl: radisson.officialUrl,
      adpPublishedHasCoords: adp.found,
      adpCensusRecordId: adp.censusRecordId,
      adpAddressMentioned: adp.addressMentioned || false,
      stagedSourceClass: "THIRD_PARTY_MAP_CONSENSUS",
      confidence: "UNVERIFIED_PENDING_FOUNDER_GEOCODE_AUTH",
      note: "Prefer official permanent geocode of 249 Queens Quay West, Toronto, ON M5J 2N5 before Airtable write. Do not use Toronto city-center (43.6532, -79.3832).",
      rejectedCityCenter: { lat: 43.6532, lng: -79.3832 },
      proposedVsCityCenterDistanceDegApprox: Math.hypot(43.6387 - 43.6532, -79.3855 - -79.3832),
    },
    airtableWrite: false,
  });
}

const umusic = rows.find((r) => r.id === "recHXCkkKD5Xwue3p") || null;
if (umusic && umusic.issues.includes("MISSING_LATITUDE_OR_LONGITUDE")) {
  proposedCorrections.push({
    hpcRecordId: "recHXCkkKD5Xwue3p",
    propertyName: umusic.name,
    radarStatus: umusic.radarStatus,
    resolutionStatus: "PENDING_AUTHORIZED_OFFICIAL_GEOCODE",
    before: {
      Latitude: umusic.latitude,
      Longitude: umusic.longitude,
      "Coordinate Source Type": umusic.coordinateSourceType,
      "Coordinate Confidence": umusic.coordinateConfidence,
    },
    after: {
      Latitude: null,
      Longitude: null,
      note: "No coordinates invented. Full street address present — ready for authorized permanent geocode only.",
    },
    evidence: {
      address: umusic.address,
      city: umusic.city,
      country: umusic.country,
      postalCode: umusic.postalCode,
      officialUrl: umusic.officialUrl,
      confidence: "NO_COORDINATE_PROPOSAL_WITHOUT_GEOCODE_AUTH",
      note: "Do not use Madrid city-center (40.4168, -3.7038). Authorize Mapbox permanent / Google storage-terms geocode of Calle de la Paz 11, 28012 Madrid, Spain.",
      rejectedCityCenter: { lat: 40.4168, lng: -3.7038 },
    },
    airtableWrite: false,
  });
}

const audit = {
  ok: true,
  contract: "HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1",
  phase: "audit_and_proposed_corrections",
  airtableWrite: false,
  paidGeocoding: false,
  generatedAt: new Date().toISOString(),
  formula,
  table: HOTEL_PROPERTY_CENSUS_TABLE,
  selectedFields,
  provenanceFieldsAvailable: provenanceAvailable,
  totals: {
    openPipeline: rows.length,
    open: openCount,
    pipeline: pipelineCount,
    mapEligible,
    hardExceptions: hardExceptions.length,
    softExceptions: softExceptions.length,
    clean: rows.filter((r) => r.issues.length === 0).length,
  },
  hardExceptions,
  softExceptions,
  proposedCorrections,
  radissonLookup: {
    hpc: radisson,
    adpPublishedCoords: adp,
  },
  governanceNotes: {
    openPipelineRequiresValidVerifiedCoords: true,
    hiddenBlankMayLackCoords: true,
    noSilentInvention: true,
    preferCanonicalIdMatch: true,
    noCityCenterAsHotelCoords: true,
  },
};

fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
fs.writeFileSync(OUT_JSON, JSON.stringify(audit, null, 2));

const md = [];
md.push("# HPC Open/Pipeline Coordinate Completeness Audit v1");
md.push("");
md.push(`**Contract:** \`HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1\``);
md.push(`**Generated:** ${audit.generatedAt}`);
md.push(`**Airtable writes:** none`);
md.push(`**Paid geocoding:** none`);
md.push("");
md.push("## Totals");
md.push("");
md.push("| Metric | Count |");
md.push("| --- | ---: |");
md.push(`| Open + Pipeline | ${audit.totals.openPipeline} |`);
md.push(`| Open | ${audit.totals.open} |`);
md.push(`| Pipeline | ${audit.totals.pipeline} |`);
md.push(`| Map-eligible (hard-pass) | ${audit.totals.mapEligible} |`);
md.push(`| Hard exceptions | ${audit.totals.hardExceptions} |`);
md.push(`| Soft flags | ${audit.totals.softExceptions} |`);
md.push(`| Clean (no issues) | ${audit.totals.clean} |`);
md.push("");
md.push("## Hard exceptions");
md.push("");
if (!hardExceptions.length) {
  md.push("_None._");
} else {
  md.push("| HPC id | Name | Status | Issues | Lat | Lng | Address |");
  md.push("| --- | --- | --- | --- | --- | --- | --- |");
  for (const r of hardExceptions) {
    md.push(
      `| \`${r.id}\` | ${r.name || "—"} | ${r.radarStatus || "—"} | ${r.issues.join(", ")} | ${r.latitude ?? "null"} | ${r.longitude ?? "null"} | ${r.address || "—"} |`
    );
  }
}
md.push("");
md.push("## Soft flags (review)");
md.push("");
if (!softExceptions.length) {
  md.push("_None._");
} else {
  md.push("| HPC id | Name | Flags | Lat | Lng |");
  md.push("| --- | --- | --- | --- | --- |");
  for (const r of softExceptions) {
    md.push(
      `| \`${r.id}\` | ${r.name || "—"} | ${r.softFlags.join(", ")} | ${r.latitude ?? "null"} | ${r.longitude ?? "null"} |`
    );
  }
}
md.push("");
md.push("## Proposed corrections (staged — no write)");
md.push("");
for (const p of proposedCorrections) {
  md.push(`### ${p.propertyName} (\`${p.hpcRecordId}\`)`);
  md.push("");
  md.push("| Field | Before | After |");
  md.push("| --- | --- | --- |");
  for (const field of Object.keys(p.after)) {
    md.push(`| ${field} | ${p.before[field] ?? "null"} | ${p.after[field] ?? "null"} |`);
  }
  md.push("");
  md.push(`- Address evidence: ${p.evidence.address}, ${p.evidence.city}, ${p.evidence.country} ${p.evidence.postalCode || ""}`);
  md.push(`- ADP published coords reusable: **${p.evidence.adpPublishedHasCoords}**`);
  md.push(`- Confidence: \`${p.evidence.confidence}\``);
  md.push(`- ${p.evidence.note}`);
  md.push("");
}
md.push("## Machine-readable");
md.push("");
md.push(`\`${path.relative(ROOT, OUT_JSON).replace(/\\\\/g, "/")}\``);
md.push("");

fs.writeFileSync(OUT_MD, md.join("\n"));
console.log(
  JSON.stringify(
    {
      ok: true,
      outJson: OUT_JSON,
      outMd: OUT_MD,
      totals: audit.totals,
      hardExceptionIds: hardExceptions.map((r) => r.id),
      proposedCorrectionIds: proposedCorrections.map((p) => p.hpcRecordId),
    },
    null,
    2
  )
);
