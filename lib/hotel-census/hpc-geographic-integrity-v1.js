/**
 * HPC geographic integrity — coordinates must be property-specific and valid.
 * Numeric non-null is NOT sufficient (rejects 0/0, out-of-range, wrong-country).
 */

export const HPC_GEOGRAPHIC_INTEGRITY_V1 = "HPC_GEOGRAPHIC_INTEGRITY_V1";

/** Approximate country bounding boxes for ADP hotel countries */
export const HPC_COUNTRY_BBOX = Object.freeze({
  "United States": { minLat: 24.5, maxLat: 49.5, minLng: -125, maxLng: -66.5 },
  USA: { minLat: 24.5, maxLat: 49.5, minLng: -125, maxLng: -66.5 },
  "United Kingdom": { minLat: 49.8, maxLat: 61.0, minLng: -8.7, maxLng: 2.0 },
  UK: { minLat: 49.8, maxLat: 61.0, minLng: -8.7, maxLng: 2.0 },
  Spain: { minLat: 27.5, maxLat: 44.0, minLng: -18.5, maxLng: 4.5 },
  Germany: { minLat: 47.2, maxLat: 55.1, minLng: 5.8, maxLng: 15.1 },
  Switzerland: { minLat: 45.8, maxLat: 47.9, minLng: 5.9, maxLng: 10.6 },
  Italy: { minLat: 36.6, maxLat: 47.1, minLng: 6.6, maxLng: 18.6 },
  France: { minLat: 41.3, maxLat: 51.2, minLng: -5.2, maxLng: 9.7 },
  Mexico: { minLat: 14.5, maxLat: 32.8, minLng: -118.5, maxLng: -86.5 },
  Colombia: { minLat: -4.3, maxLat: 13.5, minLng: -79.1, maxLng: -66.8 },
  "Dominican Republic": { minLat: 17.4, maxLat: 20.1, minLng: -72.1, maxLng: -68.2 },
  Bermuda: { minLat: 32.2, maxLat: 32.45, minLng: -65.0, maxLng: -64.6 },
  Grenada: { minLat: 11.95, maxLat: 12.35, minLng: -61.85, maxLng: -61.55 },
  Jamaica: { minLat: 17.7, maxLat: 18.6, minLng: -78.4, maxLng: -76.15 },
});

/** Soft city centers for near-city checks (km). Not used as fill values. */
export const HPC_CITY_ANCHORS = Object.freeze({
  Charlotte: { lat: 35.2271, lng: -80.8431, maxKm: 25 },
  "Falls Church": { lat: 38.8823, lng: -77.1711, maxKm: 20 },
  Arlington: { lat: 38.8499, lng: -77.0528, maxKm: 20 },
  Tampa: { lat: 27.9506, lng: -82.4572, maxKm: 30 },
  London: { lat: 51.5074, lng: -0.1278, maxKm: 25 },
  Founex: { lat: 46.333, lng: 6.183, maxKm: 20 },
  Geneva: { lat: 46.2044, lng: 6.1432, maxKm: 40 },
});

export const COORD_ISSUE = Object.freeze({
  MISSING: "MISSING",
  ZERO_ZERO: "ZERO_ZERO",
  OUT_OF_RANGE: "OUT_OF_RANGE",
  WRONG_COUNTRY_BBOX: "WRONG_COUNTRY_BBOX",
  FAR_FROM_CITY_ANCHOR: "FAR_FROM_CITY_ANCHOR",
  DUPLICATE_UNRELATED: "DUPLICATE_UNRELATED",
  SWAP_SUSPECTED: "SWAP_SUSPECTED",
  VALID: "VALID",
});

export function parseCoord(value) {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function haversineKm(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export function isInCountryBbox(country, lat, lng) {
  const key = String(country || "").trim();
  const box = HPC_COUNTRY_BBOX[key];
  if (!box) return { ok: true, checked: false };
  const ok =
    lat >= box.minLat && lat <= box.maxLat && lng >= box.minLng && lng <= box.maxLng;
  return { ok, checked: true, box };
}

/**
 * True only when coordinates are geographically usable for maps / ADP.
 * Rejects null, NaN, and Null Island (0,0).
 */
export function hasValidHpcCoordinates(latRaw, lngRaw) {
  const lat = parseCoord(latRaw);
  const lng = parseCoord(lngRaw);
  if (lat == null || lng == null) return false;
  if (lat === 0 && lng === 0) return false;
  if (Math.abs(lat) < 1e-6 && Math.abs(lng) < 1e-6) return false;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
  return true;
}

/**
 * Audit one hotel's lat/lng (+ optional city/country consistency).
 */
export function auditHotelCoordinates({
  propertyId,
  hotelName,
  latitude,
  longitude,
  country,
  city,
} = {}) {
  const lat = parseCoord(latitude);
  const lng = parseCoord(longitude);
  const issues = [];

  if (lat == null || lng == null) {
    issues.push(COORD_ISSUE.MISSING);
  } else if (lat === 0 && lng === 0) {
    issues.push(COORD_ISSUE.ZERO_ZERO);
  } else if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    issues.push(COORD_ISSUE.OUT_OF_RANGE);
  } else {
    const bbox = isInCountryBbox(country, lat, lng);
    if (bbox.checked && !bbox.ok) {
      // Check swap
      const swapped = isInCountryBbox(country, lng, lat);
      if (swapped.checked && swapped.ok) {
        issues.push(COORD_ISSUE.SWAP_SUSPECTED);
      } else {
        issues.push(COORD_ISSUE.WRONG_COUNTRY_BBOX);
      }
    }
    const anchor = HPC_CITY_ANCHORS[String(city || "").trim()];
    if (anchor && issues.length === 0) {
      const km = haversineKm(lat, lng, anchor.lat, anchor.lng);
      if (km > anchor.maxKm) issues.push(COORD_ISSUE.FAR_FROM_CITY_ANCHOR);
    }
  }

  const valid = issues.length === 0 && hasValidHpcCoordinates(lat, lng);
  return {
    propertyId,
    hotelName,
    latitude: lat,
    longitude: lng,
    country: country || "",
    city: city || "",
    issues,
    valid,
    primaryIssue: valid ? COORD_ISSUE.VALID : issues[0] || COORD_ISSUE.MISSING,
  };
}

/**
 * Detect identical coords on unrelated hotels (same lat/lng to 5 decimals).
 */
export function findDuplicateCoordinates(audits) {
  const byKey = new Map();
  for (const a of audits) {
    if (!a.valid) continue;
    const key = `${Number(a.latitude).toFixed(5)}|${Number(a.longitude).toFixed(5)}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(a);
  }
  const dups = [];
  for (const [key, rows] of byKey.entries()) {
    if (rows.length < 2) continue;
    dups.push({ key, hotels: rows.map((r) => r.propertyId) });
    for (const r of rows) {
      if (!r.issues.includes(COORD_ISSUE.DUPLICATE_UNRELATED)) {
        r.issues.push(COORD_ISSUE.DUPLICATE_UNRELATED);
        r.valid = false;
        r.primaryIssue = COORD_ISSUE.DUPLICATE_UNRELATED;
      }
    }
  }
  return dups;
}

/**
 * Geographic field consistency (non-coordinate columns).
 */
export function auditGeographicFieldConsistency(fields = {}) {
  const rows = [];
  const country = String(fields.Country || "").trim();
  const continent = String(fields.Continent || "").trim();
  const subContinent = String(fields["Sub-Continent"] || "").trim();
  const market = String(fields.Market || "").trim();
  const submarket = String(fields.Submarket || "").trim();
  const composite = String(fields["Market / Submarket"] || "").trim();
  const city = String(fields.City || "").trim();
  const address = String(fields.Address || "").trim();
  const postal = String(fields["Postal Code"] || "").trim();

  function push(field, status, note) {
    rows.push({ field, status, note, value: fields[field] ?? "" });
  }

  push("Address", address ? "OK" : "EMPTY", address ? "" : "Missing address");
  push("City", city ? "OK" : "EMPTY", city ? "" : "Missing city");
  push("Country", country ? "OK" : "EMPTY", country ? "" : "Missing country");
  push("Continent", continent ? "OK" : "EMPTY", "");
  push(
    "Sub-Continent",
    subContinent ? "OK" : "EMPTY_OR_SCHEMA_BLOCKED",
    subContinent ? "" : "Empty — may be schema-blocked (e.g. Western Europe)"
  );
  push("Market", market ? "OK" : "EMPTY", "");
  push("Submarket", submarket ? "OK" : "EMPTY", "");
  push("Market / Submarket", composite ? "OK" : "EMPTY", "");
  push("Postal Code", postal ? "OK" : "EMPTY", "");

  if (country && continent) {
    const europe = /united kingdom|uk|germany|switzerland|spain|italy|france/i.test(country);
    const na = /united states|usa|mexico|bermuda|dominican|jamaica|grenada|canada/i.test(
      country
    );
    if (europe && continent !== "Europe") {
      push("Continent", "INCONSISTENT", `Country ${country} vs Continent ${continent}`);
    }
    if (na && continent !== "North America" && !/colombia|south america/i.test(country)) {
      // Colombia is South America
      if (!/colombia/i.test(country)) {
        push("Continent", "INCONSISTENT", `Country ${country} vs Continent ${continent}`);
      }
    }
  }
  if (/colombia/i.test(country) && continent && continent !== "South America") {
    push("Continent", "INCONSISTENT", `Colombia should be South America`);
  }

  if (composite && market && !composite.toLowerCase().includes(market.toLowerCase().slice(0, 8))) {
    push(
      "Market / Submarket",
      "WEAK_MATCH",
      "Composite may not include Market label"
    );
  }

  return rows;
}

/**
 * Certification gate: coordinates must pass geographic integrity.
 */
export function certifyHpcGeographicIntegrity(fields = {}, opts = {}) {
  const audit = auditHotelCoordinates({
    propertyId: opts.propertyId,
    hotelName: fields["Property Name"],
    latitude: fields.Latitude,
    longitude: fields.Longitude,
    country: fields.Country,
    city: fields.City,
  });
  const geoRows = auditGeographicFieldConsistency(fields);
  const blockers = [];
  if (!audit.valid) {
    blockers.push({
      code: `COORD_${audit.primaryIssue}`,
      detail: {
        latitude: audit.latitude,
        longitude: audit.longitude,
        issues: audit.issues,
      },
    });
  }
  for (const r of geoRows) {
    if (r.status === "INCONSISTENT") {
      blockers.push({ code: `GEO_${r.field}_INCONSISTENT`, detail: r.note });
    }
  }
  return {
    ok: blockers.length === 0,
    contractVersion: HPC_GEOGRAPHIC_INTEGRITY_V1,
    coordinateValid: audit.valid,
    coordinateAudit: audit,
    geographicFieldRows: geoRows,
    blockers,
    assessedAt: new Date().toISOString(),
  };
}

/** Whether an existing lat/lng value must be corrected (overwrite allowed). */
export function requiresCoordinateCorrection(latRaw, lngRaw) {
  return !hasValidHpcCoordinates(latRaw, lngRaw);
}

export function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

const COUNTRY_TO_NOMINATIM_CC = {
  "United States": "us",
  USA: "us",
  "United Kingdom": "gb",
  UK: "gb",
  Spain: "es",
  Germany: "de",
  Switzerland: "ch",
  Italy: "it",
  France: "fr",
  Mexico: "mx",
  Colombia: "co",
  "Dominican Republic": "do",
  Bermuda: "bm",
  Grenada: "gd",
  Jamaica: "jm",
};

/**
 * Geocode an official address via Nominatim (OSM). Rate-limit externally.
 */
export async function geocodeOfficialAddressNominatim({
  address,
  city,
  country,
  hotelName,
  userAgent = "DealalityHPC/1.0 (geographic-integrity; contact=ops@dealality.com)",
} = {}) {
  const cc = COUNTRY_TO_NOMINATIM_CC[String(country || "").trim()] || "";
  const queries = [
    [hotelName, address, city, country].filter(Boolean).join(", "),
    [address, city, country].filter(Boolean).join(", "),
    [address, country].filter(Boolean).join(", "),
  ].filter(Boolean);

  for (const q of queries) {
    const params = new URLSearchParams({
      q,
      format: "json",
      limit: "3",
    });
    if (cc) params.set("countrycodes", cc);
    const url = `https://nominatim.openstreetmap.org/search?${params}`;
    const res = await fetch(url, {
      headers: { "User-Agent": userAgent, Accept: "application/json" },
    });
    if (!res.ok) continue;
    const hits = await res.json();
    for (const hit of hits) {
      const lat = Number(hit.lat);
      const lng = Number(hit.lon);
      if (!hasValidHpcCoordinates(lat, lng)) continue;
      const bbox = isInCountryBbox(country, lat, lng);
      if (bbox.checked && !bbox.ok) continue;
      return {
        ok: true,
        latitude: lat,
        longitude: lng,
        displayName: hit.display_name || "",
        query: q,
        osmType: hit.type || hit.class || "",
        source: "nominatim_osm_official_address",
        geocodeProvider: "Manual Review",
        geocodeMethod: "official_address_geocode",
        coordinateSourceType: "official_address_geocode",
        coordinateConfidence: /hotel|resort|motel/i.test(
          `${hit.type || ""} ${hit.class || ""} ${hit.display_name || ""}`
        )
          ? "High"
          : "Medium",
      };
    }
  }
  return { ok: false, reason: "NO_NOMINATIM_HIT" };
}
