/**
 * HPC Open/Pipeline coordinate completeness gate (permanent contract).
 *
 * Contract: HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1
 *
 * - Open/Pipeline activation requires valid property-level Latitude/Longitude.
 * - Hidden/blank may lack coordinates.
 * - Existing Open/Pipeline exceptions are flagged — statuses are not auto-changed.
 * - No hotel-ID special cases; no silent city-center invention.
 */

import { isValidCoordPair } from "../research-engine-v2/production-census-coordinate-extractor.js";

export const HPC_COORDINATE_COMPLETENESS_GATE_VERSION =
  "HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1";

export const HPC_RADAR_STATUS_OPEN_PIPELINE = Object.freeze(["Open", "Pipeline"]);

/** Hard-block codes — prevent new Open/Pipeline activation. */
export const HARD_ISSUE_CODES = Object.freeze([
  "MISSING_LATITUDE_OR_LONGITUDE",
  "ZERO_ZERO_COORDINATES",
  "OUT_OF_RANGE",
  "COUNTRY_BOUNDS_MISMATCH",
  "ADDRESS_UNRELATED_COORDINATES",
  "CONFIRMED_CITY_CENTER_FALLBACK",
]);

/** Soft / manual-review codes — do not hard-block solely for proximity to a city center. */
export const SOFT_ISSUE_CODES = Object.freeze([
  "LOW_CONFIDENCE_COORDINATES",
  "INCOMPLETE_ADDRESS_MATCH",
  "POTENTIAL_CITY_CENTER_UNCONFIRMED",
  "MISSING_ADDRESS_FOR_VERIFICATION",
  "MISSING_COORDINATE_PROVENANCE",
]);

/**
 * Known city-center traps. Exact/near hits are soft unless `confirmedFallback` is set
 * by a geocode path that intentionally returned a locality/admin result.
 */
export const CITY_CENTER_TRAPS = Object.freeze([
  { city: /toronto/i, country: /canada/i, lat: 43.6532, lng: -79.3832, tol: 0.0015, label: "Toronto city center" },
  { city: /madrid/i, country: /spain/i, lat: 40.4168, lng: -3.7038, tol: 0.0015, label: "Madrid city center" },
  { city: /london/i, country: /united kingdom|uk|england/i, lat: 51.5074, lng: -0.1278, tol: 0.0015, label: "London city center" },
  { city: /mexico city|ciudad de m[eé]xico|cdmx/i, country: /mexico/i, lat: 19.4326, lng: -99.1332, tol: 0.0015, label: "Mexico City center" },
  { city: /washington/i, country: /united states|usa/i, lat: 38.9072, lng: -77.0369, tol: 0.0015, label: "Washington DC center" },
  { city: /san diego/i, country: /united states|usa/i, lat: 32.7157, lng: -117.1611, tol: 0.0015, label: "San Diego city center" },
  { city: /bethesda/i, country: /united states|usa/i, lat: 38.9847, lng: -77.0947, tol: 0.0015, label: "Bethesda city center" },
  { city: /palma/i, country: /spain/i, lat: 39.5696, lng: 2.6502, tol: 0.0015, label: "Palma city center" },
]);

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function text(v) {
  return v == null ? "" : String(v).trim();
}

function near(a, b, tol) {
  return Math.abs(Number(a) - Number(b)) <= tol;
}

export function readHpcCoordinates(fields = {}) {
  const lat = num(fields.Latitude ?? fields.latitude ?? fields.lat);
  const lng = num(fields.Longitude ?? fields.longitude ?? fields.lng);
  return { lat, lng };
}

export function isOpenOrPipelineStatus(status) {
  return HPC_RADAR_STATUS_OPEN_PIPELINE.includes(String(status || "").trim());
}

/**
 * Coarse country bounding boxes — hard fail only on clear mismatches.
 */
export function checkCountryBounds(lat, lng, country) {
  const c = text(country);
  if (!c || lat == null || lng == null) return null;
  if (/canada/i.test(c)) {
    if (lat < 41 || lat > 84 || lng > -50 || lng < -145) return "COUNTRY_BOUNDS_MISMATCH";
  }
  if (/united states|usa/i.test(c)) {
    if (lng > -50 || lat < 10 || lat > 75) return "COUNTRY_BOUNDS_MISMATCH";
  }
  if (/spain/i.test(c)) {
    if (lat < 27 || lat > 44 || lng < -19 || lng > 5) return "COUNTRY_BOUNDS_MISMATCH";
  }
  if (/united kingdom|uk|england|scotland|wales/i.test(c)) {
    if (lat < 49 || lat > 61 || lng < -9 || lng > 2) return "COUNTRY_BOUNDS_MISMATCH";
  }
  return null;
}

/**
 * Detect proximity to a known city center.
 * @returns {{ confirmed: boolean, label: string|null, soft: boolean }}
 */
export function detectCityCenterProximity({ lat, lng, city, country, confirmedFallback = false }) {
  if (lat == null || lng == null) return { confirmed: false, label: null, soft: false };
  for (const trap of CITY_CENTER_TRAPS) {
    if (
      trap.city.test(text(city)) &&
      trap.country.test(text(country)) &&
      near(lat, trap.lat, trap.tol) &&
      near(lng, trap.lng, trap.tol)
    ) {
      if (confirmedFallback) {
        return { confirmed: true, label: trap.label, soft: false };
      }
      // Plausible hotel can sit near a center — warn only, do not hard-block.
      return { confirmed: false, label: trap.label, soft: true };
    }
  }
  return { confirmed: false, label: null, soft: false };
}

/**
 * Validate a coordinate pair + identity context.
 *
 * @param {object} input
 * @param {number|null} [input.lat]
 * @param {number|null} [input.lng]
 * @param {string} [input.city]
 * @param {string} [input.country]
 * @param {string} [input.address]
 * @param {string} [input.coordinateConfidence]
 * @param {boolean} [input.confirmedCityCenterFallback]
 * @param {boolean} [input.addressUnrelated]
 * @param {boolean} [input.incompleteAddressMatch]
 * @returns {{
 *   ok: boolean,
 *   hardFail: boolean,
 *   issues: string[],
 *   hardIssues: string[],
 *   softIssues: string[],
 *   mapEligible: boolean
 * }}
 */
export function validateHpcCoordinates(input = {}) {
  const lat = num(input.lat);
  const lng = num(input.lng);
  const issues = [];

  if (lat == null || lng == null) {
    issues.push("MISSING_LATITUDE_OR_LONGITUDE");
  } else if (lat === 0 && lng === 0) {
    issues.push("ZERO_ZERO_COORDINATES");
  } else if (!isValidCoordPair(lat, lng)) {
    issues.push("OUT_OF_RANGE");
  } else {
    const bounds = checkCountryBounds(lat, lng, input.country);
    if (bounds) issues.push(bounds);

    const cityHit = detectCityCenterProximity({
      lat,
      lng,
      city: input.city,
      country: input.country,
      confirmedFallback: input.confirmedCityCenterFallback === true,
    });
    if (cityHit.confirmed) issues.push("CONFIRMED_CITY_CENTER_FALLBACK");
    else if (cityHit.soft) issues.push("POTENTIAL_CITY_CENTER_UNCONFIRMED");
  }

  if (!text(input.address)) {
    issues.push("MISSING_ADDRESS_FOR_VERIFICATION");
  }
  if (input.addressUnrelated === true) {
    issues.push("ADDRESS_UNRELATED_COORDINATES");
  }
  if (input.incompleteAddressMatch === true) {
    issues.push("INCOMPLETE_ADDRESS_MATCH");
  }
  const confidence = text(input.coordinateConfidence);
  if (confidence && /^(low|hold)$/i.test(confidence)) {
    issues.push("LOW_CONFIDENCE_COORDINATES");
  }
  // Provenance is required for new activation / geocode applies — not a blanket
  // soft-fail on every legacy Open row that predates provenance fields.
  if (
    input.requireProvenance === true &&
    lat != null &&
    lng != null &&
    !text(input.coordinateSourceType) &&
    !text(input.geocodeProvider)
  ) {
    issues.push("MISSING_COORDINATE_PROVENANCE");
  }

  const hardIssues = issues.filter((c) => HARD_ISSUE_CODES.includes(c));
  const softIssues = issues.filter((c) => SOFT_ISSUE_CODES.includes(c));
  const hardFail = hardIssues.length > 0;
  const mapEligible =
    !hardFail &&
    lat != null &&
    lng != null &&
    !(lat === 0 && lng === 0) &&
    isValidCoordPair(lat, lng);

  return {
    ok: !hardFail,
    hardFail,
    issues,
    hardIssues,
    softIssues,
    mapEligible,
    lat,
    lng,
  };
}

/**
 * Evaluate whether a record may be activated to Open/Pipeline.
 * Does not mutate status. Existing Open/Pipeline with failures are flagged only.
 */
export function evaluateOpenPipelineActivation(fields = {}, opts = {}) {
  const status = text(fields["Radar Status"] ?? fields.radarStatus ?? fields.status);
  const { lat, lng } = readHpcCoordinates(fields);
  const targetingOpenPipeline =
    opts.targetStatus != null
      ? isOpenOrPipelineStatus(opts.targetStatus)
      : isOpenOrPipelineStatus(status);

  // New activations should carry provenance; existing inventory is flagged hard-only.
  const requireProvenance =
    opts.requireProvenance === true ||
    (opts.isNewActivation === true && targetingOpenPipeline);

  const validation = validateHpcCoordinates({
    lat,
    lng,
    city: fields.City ?? fields.city,
    country: fields.Country ?? fields.country,
    address: fields.Address ?? fields.address,
    coordinateConfidence: fields["Coordinate Confidence"] ?? fields.coordinateConfidence,
    coordinateSourceType: fields["Coordinate Source Type"] ?? fields.coordinateSourceType,
    geocodeProvider: fields["Geocode Provider"] ?? fields.geocodeProvider,
    confirmedCityCenterFallback: opts.confirmedCityCenterFallback === true,
    addressUnrelated: opts.addressUnrelated === true,
    incompleteAddressMatch: opts.incompleteAddressMatch === true,
    requireProvenance,
  });

  const currentlyOpenPipeline = isOpenOrPipelineStatus(status);
  const activationBlocked = targetingOpenPipeline && validation.hardFail;

  return {
    gate: HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
    status: status || null,
    currentlyOpenPipeline,
    targetingOpenPipeline,
    activationBlocked,
    /** Existing exceptions: flag only — never auto-demote status. */
    existingExceptionFlag: currentlyOpenPipeline && validation.hardFail,
    preserveStatus: true,
    validation,
  };
}

/**
 * Build admin/Radar diagnostic rows from formatted hotels or Airtable fields.
 */
export function buildCoordinateCompletenessDiagnostics(records = []) {
  const missingCoordinates = [];
  const hardExceptions = [];
  const softExceptions = [];

  for (const row of records) {
    const fields = row?.fields || row || {};
    const id = text(row?.id || fields.id);
    const name = text(fields.name || fields["Property Name"] || fields["Canonical Property Name"]);
    const status = text(fields.status || fields.radarStatus || fields["Radar Status"]);
    if (!isOpenOrPipelineStatus(status)) continue;

    const { lat, lng } = readHpcCoordinates(fields);
    const validation = validateHpcCoordinates({
      lat: lat ?? fields.lat,
      lng: lng ?? fields.lng,
      city: fields.city || fields.City,
      country: fields.country || fields.Country,
      address: fields.address || fields.Address || fields.address1,
      coordinateConfidence: fields.coordinateConfidence || fields["Coordinate Confidence"],
      coordinateSourceType: fields.coordinateSourceType || fields["Coordinate Source Type"],
      geocodeProvider: fields.geocodeProvider || fields["Geocode Provider"],
    });

    if (!validation.issues.length) continue;

    const entry = {
      id: id || null,
      name: name || null,
      status: status || null,
      issues: validation.issues.slice(),
      hardIssues: validation.hardIssues.slice(),
      softIssues: validation.softIssues.slice(),
      lat: validation.lat,
      lng: validation.lng,
    };

    if (validation.hardIssues.includes("MISSING_LATITUDE_OR_LONGITUDE") ||
        validation.hardIssues.includes("ZERO_ZERO_COORDINATES")) {
      missingCoordinates.push({
        id: entry.id,
        name: entry.name,
        reason: validation.hardIssues[0] || "missing_or_zero_coordinates",
      });
    }
    if (validation.hardFail) hardExceptions.push(entry);
    else softExceptions.push(entry);
  }

  return {
    gate: HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
    missingCoordinates,
    hardExceptions,
    softExceptions,
    hardExceptionCount: hardExceptions.length,
    softExceptionCount: softExceptions.length,
  };
}

/**
 * Map a successful geocode lookup into proposed HPC Airtable fields (no write).
 */
export function buildProposedCoordinatePatch({
  before = {},
  geocode = {},
  verifiedAt = null,
} = {}) {
  const lat = num(geocode.lat);
  const lng = num(geocode.lng);
  const providerLabel =
    geocode.provider === "mapbox"
      ? "Mapbox"
      : geocode.provider === "google"
        ? "Google"
        : geocode.provider || null;

  const sourceType =
    geocode.method && /permanent|official_address/i.test(String(geocode.method))
      ? "official_address_geocode"
      : "official_address_geocode";

  const after = {
    Latitude: lat,
    Longitude: lng,
    "Coordinate Source Type": sourceType,
    "Coordinate Confidence": geocode.confidence || "Medium",
    "Geocode Provider": providerLabel,
    "Geocode Method": geocode.method || "official_address_geocode",
    "Geocode Reviewed Date": verifiedAt || new Date().toISOString().slice(0, 10),
  };

  return {
    airtableWrite: false,
    before: {
      Latitude: before.Latitude ?? before.latitude ?? null,
      Longitude: before.Longitude ?? before.longitude ?? null,
      "Coordinate Source Type": before["Coordinate Source Type"] ?? null,
      "Coordinate Confidence": before["Coordinate Confidence"] ?? null,
      "Geocode Provider": before["Geocode Provider"] ?? null,
      "Geocode Method": before["Geocode Method"] ?? null,
      "Geocode Reviewed Date": before["Geocode Reviewed Date"] ?? null,
    },
    after,
    evidence: {
      formattedAddress: geocode.formatted_address || null,
      query: geocode.query || null,
      relevance: geocode.relevance ?? null,
      locationType: geocode.location_type ?? null,
      permanent: geocode.permanent ?? null,
      provider: geocode.provider || null,
      method: geocode.method || null,
      confidence: geocode.confidence || null,
      verifiedAt: after["Geocode Reviewed Date"],
    },
  };
}
