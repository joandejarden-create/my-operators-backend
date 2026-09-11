/**
 * Read-only hotel matching for leak audits.
 * Uses ADP property profile fixtures only — never updates matched records or Census.
 */

import { readdirSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

function normalizeName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractDomain(urlOrHost) {
  const raw = String(urlOrHost || "").trim().toLowerCase();
  if (!raw) return "";
  try {
    const withProto = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    const host = new URL(withProto).hostname.replace(/^www\./, "");
    return host;
  } catch {
    return raw.replace(/^www\./, "").split("/")[0];
  }
}

function loadAllProfilesIncludingHidden() {
  const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
  if (!existsSync(fixturesDir)) return [];
  const files = readdirSync(fixturesDir).filter((f) => f.endsWith("-property-profile.json"));
  const profiles = [];
  for (const file of files) {
    try {
      const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf8"));
      if (data.propertyId && data.name) profiles.push(data);
    } catch (err) {
      console.error("[leak-audit] profile read failed:", file, err.message);
    }
  }
  return profiles;
}

/**
 * Attempt match for read-only context. Does not write to any hotel record.
 * @returns {{ matched: boolean, matchedHotelId: string|null, matchMethod: string|null, confidence: string }}
 */
export function matchHotelReadOnly(intake) {
  const profiles = loadAllProfilesIncludingHidden();
  const name = normalizeName(intake.hotelName);
  const domain = extractDomain(intake.hotelWebsite);
  const city = normalizeName(intake.city);
  const country = normalizeName(intake.country);

  // 1) Exact name
  for (const p of profiles) {
    if (normalizeName(p.name) === name) {
      return {
        matched: true,
        matchedHotelId: p.propertyId,
        matchMethod: "exact_name",
        confidence: "high",
        matchedName: p.name,
        readOnly: true,
      };
    }
  }

  // 2) Website domain
  if (domain) {
    for (const p of profiles) {
      const pDomain = extractDomain(p.website || p.officialWebsite || p.url || "");
      if (pDomain && pDomain === domain) {
        return {
          matched: true,
          matchedHotelId: p.propertyId,
          matchMethod: "website_domain",
          confidence: "high",
          matchedName: p.name,
          readOnly: true,
        };
      }
    }
  }

  // 3) Fuzzy name + city/country
  for (const p of profiles) {
    const pName = normalizeName(p.name);
    const nameOverlap =
      name.length >= 6 && (pName.includes(name) || name.includes(pName));
    if (!nameOverlap) continue;
    const pCity = normalizeName(p.city);
    const pCountry = normalizeName(p.country);
    const geoOk =
      (!city || !pCity || pCity === city) && (!country || !pCountry || pCountry === country);
    if (geoOk) {
      return {
        matched: true,
        matchedHotelId: p.propertyId,
        matchMethod: "fuzzy_name_geo",
        confidence: "medium",
        matchedName: p.name,
        readOnly: true,
      };
    }
  }

  return {
    matched: false,
    matchedHotelId: null,
    matchMethod: null,
    confidence: "none",
    matchedName: null,
    readOnly: true,
  };
}
