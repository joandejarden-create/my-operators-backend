/**
 * Universal Hotel Record Resolver — record-level field completion.
 *
 * Official sources first. Secondary only when ENABLE_SECONDARY_HOTEL_DATA_SOURCES=1.
 * Webhound never Census SoT. Mapbox only for coordinates after High Address.
 */

import {
  inspectHotelRecord,
  prioritizeIncompleteRecords,
} from "./universal-hotel-record-inspector.js";
import {
  routeHotelRecordSources,
  SOURCE_STRATEGY,
} from "./universal-record-source-router.js";
import { resolveChoicePropertyRecord } from "./choice-property-record-resolver.js";
import {
  familyFromIdentity,
  warmFamilyDirectoryCaches,
  lookupHiltonDirectoryRow,
  lookupMarriottSitemapRow,
  lookupAccorCatalogRow,
  resolveDirectoryAddressCandidate,
  resolveDirectoryPhoneCandidate,
} from "./census-autopilot-family-directory-adapters.js";
import { resolveStateRegionFromCity, isDirtyStateRegionValue, resolveStateFromChoiceOfficialUrl } from "./census-city-to-state-map.js";
import {
  resolveMarketFromCity,
  resolveSubmarketHighOnly,
  resolveContinentSubContinentFromCountry,
} from "./census-region-market-map.js";
import {
  extractOfficialAddressFromHtml,
  extractOfficialRoomsFromHtml,
} from "./census-level-2-parent-extractors.js";
import { extractOfficialPhoneFromHtml } from "./census-phone-number-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { isFalsePositiveRoomCount } from "./production-census-rooms-keys-extractor.js";
import {
  evaluateCoordinateCompletionEligibility,
  buildCoordinateCompletionPatch,
} from "./census-coordinate-completion.js";
import { resolveMapboxCoordinates } from "./census-mapbox-coordinate-provider.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  discoverAndExtractMarriottDamFactsheet,
  buildDamFactsheetCensusPatch,
} from "./marriott-dam-factsheet-discovery.js";
import { extractMarshaCode } from "./marriott-hqv-coordinate-client.js";
import { isIncorrectCanonicalPropertyName } from "./universal-hotel-record-inspector.js";

export const UNIVERSAL_HOTEL_RECORD_RESOLVER_VERSION =
  "universal-hotel-record-resolver-v1";

function isBlank(v) {
  return v == null || !String(v).trim();
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function secondaryEnabled(env = process.env) {
  return String(env.ENABLE_SECONDARY_HOTEL_DATA_SOURCES || "0") === "1";
}

const ALLOWED_RESOLVER_FIELDS = new Set([
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Official Property URL",
  "Source URL",
  "Family / Source Family",
  "City",
  "State / Region",
  "Country",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Review Status",
  "Rooms Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
  "Human Review Required",
  "Data Quality Review Required",
  "Last Reviewed Date",
]);

/**
 * Sanitize patch — strip forbidden / disallowed fields.
 */
export function sanitizeResolverPatch(patch = {}) {
  /** @type {Record<string, unknown>} */
  const out = {};
  for (const [k, v] of Object.entries(patch || {})) {
    if (isForbiddenAutopilotField(k)) continue;
    if (!ALLOWED_RESOLVER_FIELDS.has(k)) continue;
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return out;
}

async function fetchOfficialHtml(url, opts = {}) {
  const target = String(url || "").trim();
  if (!/^https?:\/\//i.test(target)) return { ok: false, reason: "bad_url" };
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), opts.timeoutMs || 25000);
  try {
    const res = await fetch(target, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "user-agent":
          "Mozilla/5.0 (compatible; DealalityUniversalResolver/1.0; +https://dealality.com)",
        accept: "text/html,application/xhtml+xml",
      },
    });
    const html = await res.text();
    if (!res.ok) {
      return {
        ok: false,
        reason: `http_${res.status}`,
        blocked: res.status === 403 || res.status === 429,
        url: target,
      };
    }
    return { ok: true, html, url: target };
  } catch (err) {
    return {
      ok: false,
      reason: err?.name === "AbortError" ? "timeout" : "network_error",
      url: target,
    };
  } finally {
    clearTimeout(t);
  }
}

function applyGeographyMaps(fields, patch, blockers) {
  const city = patch.City || fields.City;
  const country = patch.Country || fields.Country;
  const existingState = fields["State / Region"];
  const needsState =
    (isBlank(existingState) || isDirtyStateRegionValue(existingState)) &&
    !patch["State / Region"];
  if (needsState && city) {
    const st = resolveStateRegionFromCity({
      city,
      country,
      state: existingState,
    });
    if (st.ok && st.state) patch["State / Region"] = st.state;
    else if (!isDirtyStateRegionValue(existingState)) {
      blockers.push({
        field: "State / Region",
        reason: st.reason || "state_mapping_missing",
      });
    }
  }
  // Choice Mexico: URL state slug when city map miss / blank
  if (
    (isBlank(fields["State / Region"]) || isDirtyStateRegionValue(fields["State / Region"])) &&
    !patch["State / Region"]
  ) {
    const fromUrl = resolveStateFromChoiceOfficialUrl(
      fields["Official Property URL"] || fields["Source URL"]
    );
    if (fromUrl.ok && fromUrl.state) {
      patch["State / Region"] = fromUrl.state;
    }
  }
  if (isBlank(fields.Market) && !patch.Market && city) {
    const m = resolveMarketFromCity({ city, country });
    if (m.ok && m.market) patch.Market = m.market;
    else blockers.push({ field: "Market", reason: m.reason || "market_mapping_missing" });
  }
  if (isBlank(fields.Submarket) && !patch.Submarket && (patch.Market || fields.Market)) {
    const sub = resolveSubmarketHighOnly({
      market: patch.Market || fields.Market,
      city,
      address: patch.Address || fields.Address,
      propertyName:
        patch["Canonical Property Name"] ||
        fields["Canonical Property Name"] ||
        fields["Property Name"],
    });
    if (sub.ok && sub.submarket) patch.Submarket = sub.submarket;
    else
      blockers.push({
        field: "Submarket",
        reason: sub.reason || "submarket_mapping_missing",
      });
  }
  if (country && (isBlank(fields.Continent) || isBlank(fields["Sub-Continent"]))) {
    const geo = resolveContinentSubContinentFromCountry(country);
    if (geo?.continent && isBlank(fields.Continent) && !patch.Continent) {
      patch.Continent = geo.continent;
    }
    if (geo?.subContinent && isBlank(fields["Sub-Continent"]) && !patch["Sub-Continent"]) {
      patch["Sub-Continent"] = geo.subContinent;
    }
  }
}

async function extractFromOfficialUrl(fields, patch, blockers, secondaryOps) {
  const url = String(
    patch["Official Property URL"] ||
      fields["Official Property URL"] ||
      fields["Source URL"] ||
      ""
  ).trim();
  if (!url) {
    blockers.push({ field: "Official Property URL", reason: "hotel_url_missing" });
    return;
  }
  const fetched = await fetchOfficialHtml(url);
  if (!fetched.ok) {
    blockers.push({
      field: "property_page",
      reason: fetched.blocked ? "bot_blocked" : fetched.reason,
      url,
    });
    return;
  }
  if (isBlank(fields.Address) && !patch.Address) {
    const addr = extractOfficialAddressFromHtml(fetched.html, fetched.url);
    if (addr.ok && isStreetLevelAddress(addr.address)) {
      patch.Address = addr.address;
      patch["Address Confidence"] = "High";
      patch["Address Source URL"] = fetched.url;
    } else {
      blockers.push({ field: "Address", reason: "address_missing_from_official_source" });
      secondaryOps.push({ field: "Address", reason: "official_page_lacks_street_address" });
    }
  }
  if (isBlank(fields.Phone) && !patch.Phone) {
    const phone = extractOfficialPhoneFromHtml(fetched.html, fetched.url);
    if (phone.ok && phone.phone) patch.Phone = phone.phone;
    else {
      blockers.push({ field: "Phone", reason: "phone_missing_from_official_source" });
      secondaryOps.push({ field: "Phone", reason: "official_page_lacks_phone" });
    }
  }
  if (isBlank(fields["Rooms / Keys"]) && patch["Rooms / Keys"] == null) {
    const rooms = extractOfficialRoomsFromHtml(fetched.html, fetched.url);
    const count = rooms.rooms ?? rooms.count;
    if (
      rooms.ok &&
      count != null &&
      !isFalsePositiveRoomCount(rooms.note || "", count, "official_rooms")
    ) {
      patch["Rooms / Keys"] = count;
      patch["Rooms Confidence"] = "High";
      patch["Rooms Source URL"] = fetched.url;
      patch["Rooms Source Type"] = "official_property_page";
      patch["Rooms Evidence Tier"] = "official_html";
      patch["Rooms Reviewed Date"] = todayIso();
      patch["Rooms Review Status"] = "Autopilot High — official property page";
    } else {
      blockers.push({ field: "Rooms / Keys", reason: "rooms_missing_from_official_source" });
      secondaryOps.push({ field: "Rooms / Keys", reason: "official_page_lacks_exact_rooms" });
    }
  }
}

/**
 * Resolve one incomplete Hotel Property Census record.
 */
export async function resolveUniversalHotelRecord(record, opts = {}) {
  const env = opts.env || process.env;
  const fields = record?.fields || {};
  const inspection = inspectHotelRecord(record);
  const route = routeHotelRecordSources(fields, {
    propertyCode: opts.propertyCode,
    identityKey: fields["Property Identity Key"],
  });

  /** @type {Record<string, unknown>} */
  let patch = {};
  /** @type {object[]} */
  const blockers = [];
  /** @type {object[]} */
  const secondaryOpportunities = [];
  /** @type {object[]} */
  const provenance = [];
  let secondaryWrites = 0;

  if (!inspection.incomplete && !opts.force) {
    return {
      ok: true,
      status: "already_complete",
      record_id: record.id,
      inspection,
      route,
      patch: {},
      blockers: [],
      secondary_opportunities: [],
      webhound_as_sot: false,
      version: UNIVERSAL_HOTEL_RECORD_RESOLVER_VERSION,
    };
  }

  // Strategy: Choice
  if (route.strategies.includes(SOURCE_STRATEGY.CHOICE_PROPERTY_ID)) {
    const choice = await resolveChoicePropertyRecord(record, {
      propertyCode: opts.propertyCode || route.codes.choice_property_id,
      log: opts.log,
      skipWarmCache: opts.skipWarmCache,
    });
    Object.assign(patch, choice.patch || {});
    blockers.push(...(choice.blockers || []));
    secondaryOpportunities.push(...(choice.secondary_opportunities || []));
    provenance.push(...(choice.provenance || []));
  }

  // Marriott: sitemap URL + DAM factsheet
  if (route.strategies.includes(SOURCE_STRATEGY.MARRIOTT_MARSHA_OFFICIAL)) {
    const marsha =
      route.codes.marsha ||
      extractMarshaCode(fields["Official Property URL"] || fields["Source URL"] || "");
    if (marsha && isBlank(fields["Official Property URL"])) {
      const sit = await lookupMarriottSitemapRow(fields, fields["Property Identity Key"]);
      if (sit.ok && sit.row?.propertyUrl) {
        patch["Official Property URL"] = sit.row.propertyUrl;
        patch["Source URL"] = sit.row.propertyUrl;
        provenance.push({ field: "Official Property URL", source: "marriott_sitemap" });
        if (sit.row.title && isIncorrectCanonicalPropertyName(fields).incorrect) {
          patch["Canonical Property Name"] = sit.row.title;
        }
      }
    }
    if (isBlank(fields["Rooms / Keys"]) || isBlank(fields.Address) || isBlank(fields.Phone)) {
      try {
        const dam = await discoverAndExtractMarriottDamFactsheet(record, {
          marsha,
        });
        if (dam.ok) {
          const damPatch = buildDamFactsheetCensusPatch(dam, { ...fields, ...patch });
          Object.assign(patch, damPatch.patch || {});
          provenance.push({ field: "dam_factsheet", url: dam.url });
        }
      } catch {
        /* non-fatal */
      }
    }
  }

  // Hilton directory
  if (route.strategies.includes(SOURCE_STRATEGY.HILTON_DIRECTORY_PROPERTY)) {
    const hit = await lookupHiltonDirectoryRow(fields, fields["Property Identity Key"]);
    if (hit.ok && hit.row) {
      if (isBlank(fields["Official Property URL"]) && hit.row.propertyUrl) {
        patch["Official Property URL"] = hit.row.propertyUrl;
      }
      if (isIncorrectCanonicalPropertyName(fields).incorrect && hit.row.name) {
        patch["Canonical Property Name"] = hit.row.name;
        if (isBlank(fields["Property Name"])) patch["Property Name"] = hit.row.name;
      }
      if (isBlank(fields.Address)) {
        const addr = await resolveDirectoryAddressCandidate({
          fields,
          identityKey: fields["Property Identity Key"],
          family: "Hilton",
        });
        if (addr.ok) {
          patch.Address = addr.address;
          patch["Address Confidence"] = "High";
          patch["Address Source URL"] = addr.source_url;
        }
      }
      if (isBlank(fields.Phone)) {
        const ph = await resolveDirectoryPhoneCandidate({
          fields,
          identityKey: fields["Property Identity Key"],
          family: "Hilton",
        });
        if (ph.ok) patch.Phone = ph.phone;
      }
    }
  }

  // Accor catalog
  if (route.strategies.includes(SOURCE_STRATEGY.ACCOR_CATALOG)) {
    const hit = await lookupAccorCatalogRow(fields, fields["Property Identity Key"]);
    if (hit.ok && hit.row) {
      if (isBlank(fields.Address) && hit.row.address && isStreetLevelAddress(hit.row.address)) {
        patch.Address = hit.row.address;
        patch["Address Confidence"] = "High";
        patch["Address Source URL"] = hit.row.source_url || hit.row.url || null;
      }
      if (isBlank(fields.Phone) && hit.row.phone) patch.Phone = hit.row.phone;
      if (isBlank(fields["Official Property URL"]) && (hit.row.url || hit.row.propertyUrl)) {
        patch["Official Property URL"] = hit.row.url || hit.row.propertyUrl;
      }
    }
  }

  // Generic official URL page extract when still missing Level 2
  if (
    route.strategies.includes(SOURCE_STRATEGY.OFFICIAL_PROPERTY_URL) ||
    route.strategies.includes(SOURCE_STRATEGY.IHG_PROPERTY_PAGE) ||
    route.strategies.includes(SOURCE_STRATEGY.WYNDHAM_PROPERTY_PAGE) ||
    route.strategies.includes(SOURCE_STRATEGY.PREFERRED_COLLECTION_PAGE)
  ) {
    const stillNeed =
      (isBlank(fields.Address) && !patch.Address) ||
      (isBlank(fields.Phone) && !patch.Phone) ||
      (isBlank(fields["Rooms / Keys"]) && patch["Rooms / Keys"] == null);
    if (stillNeed) {
      await extractFromOfficialUrl(fields, patch, blockers, secondaryOpportunities);
    }
  }

  applyGeographyMaps(fields, patch, blockers);

  // Mapbox coordinates only after High Address
  const mergedForGeo = { ...fields, ...patch };
  if (
    (isBlank(fields.Latitude) || isBlank(fields.Longitude)) &&
    mergedForGeo.Address &&
    mergedForGeo["Address Confidence"] === "High" &&
    mergedForGeo["Address Source URL"] &&
    opts.enableMapbox !== false
  ) {
    const elig = evaluateCoordinateCompletionEligibility(
      { id: record.id, fields: mergedForGeo },
      {}
    );
    if (elig.ok || elig.eligible) {
      try {
        const geo = await resolveMapboxCoordinates(
          {
            address: mergedForGeo.Address,
            city: mergedForGeo.City,
            country: mergedForGeo.Country,
            state: mergedForGeo["State / Region"],
          },
          { env }
        );
        if (geo?.ok && geo.latitude != null && geo.longitude != null) {
          const cPatch = buildCoordinateCompletionPatch(geo, {});
          Object.assign(patch, cPatch || {});
          if (!patch.Latitude) {
            patch.Latitude = geo.latitude;
            patch.Longitude = geo.longitude;
            patch["Coordinate Source Type"] = "mapbox_permanent";
            patch["Coordinate Confidence"] = "High";
            patch["Geocode Provider"] = "Mapbox";
            patch["Geocode Method"] = geo.method || "permanent_forward";
            patch["Geocode Reviewed Date"] = todayIso();
          }
        } else {
          blockers.push({
            field: "Latitude/Longitude",
            reason: geo?.reason || "mapbox_geocode_failed",
          });
        }
      } catch (err) {
        blockers.push({
          field: "Latitude/Longitude",
          reason: "mapbox_error",
          error: err?.message || String(err),
        });
      }
    } else {
      blockers.push({
        field: "Latitude/Longitude",
        reason: elig.reason || "coordinate_eligibility_failed",
      });
    }
  } else if (isBlank(fields.Latitude) || isBlank(fields.Longitude)) {
    if (!mergedForGeo.Address) {
      blockers.push({
        field: "Latitude/Longitude",
        reason: "mapbox_waiting_for_high_address",
      });
    }
  }

  // Secondary sources: report only unless enabled
  if (!secondaryEnabled(env) && secondaryOpportunities.length) {
    // no writes
  } else if (secondaryEnabled(env) && secondaryOpportunities.length) {
    // Policy gate: do not auto-write secondary in v1 without explicit per-field handlers.
    // Keep opportunities listed; founder must approve source policy first.
    blockers.push({
      field: "*",
      reason: "secondary_enabled_but_no_approved_writer_v1",
    });
  }

  patch = sanitizeResolverPatch(patch);
  if (Object.keys(patch).length) {
    patch["Last Reviewed Date"] = todayIso();
    if (!patch["Enrichment Status"]) {
      patch["Enrichment Status"] = `Universal record resolver — ${route.family}`;
    }
  }

  const unresolved = inspection.missing_keys.filter((key) => {
    const fieldMap = {
      hotel_url: "Official Property URL",
      address: "Address",
      phone: "Phone",
      rooms: "Rooms / Keys",
      state_region: "State / Region",
      market: "Market",
      submarket: "Submarket",
      canonical_property_name: "Canonical Property Name",
      coordinates: "Latitude",
    };
    const f = fieldMap[key];
    if (!f) return true;
    if (key === "coordinates") return patch.Latitude == null;
    return patch[f] == null && isBlank(fields[f]);
  });

  return {
    ok: Object.keys(patch).length > 0,
    status: Object.keys(patch).length
      ? unresolved.length
        ? "partial"
        : "resolved"
      : "unresolved",
    record_id: record.id,
    family: route.family,
    inspection,
    route,
    patch,
    provenance,
    blockers,
    secondary_opportunities: secondaryOpportunities,
    secondary_writes: secondaryWrites,
    secondary_enabled: secondaryEnabled(env),
    unresolved_keys: unresolved,
    webhound_as_sot: false,
    version: UNIVERSAL_HOTEL_RECORD_RESOLVER_VERSION,
  };
}

export {
  inspectHotelRecord,
  prioritizeIncompleteRecords,
  routeHotelRecordSources,
  SOURCE_STRATEGY,
  warmFamilyDirectoryCaches,
  familyFromIdentity,
};
