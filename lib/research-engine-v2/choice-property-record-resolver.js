/**
 * Choice property-id record resolver (e.g. MX043).
 * Official Choice directory + property page only. Never Webhound as SoT.
 */

import {
  warmFamilyDirectoryCaches,
  lookupChoiceRegionalRow,
  extractChoicePropertyId,
  resolveDirectoryAddressCandidate,
  resolveDirectoryPhoneCandidate,
} from "./census-autopilot-family-directory-adapters.js";
import {
  canonicalChoicePropertyUrl,
  CHOICE_FETCH_HEADERS,
} from "../choice-regional-directory-extract.js";
import {
  extractOfficialAddressFromHtml,
  extractOfficialRoomsFromHtml,
} from "./census-level-2-parent-extractors.js";
import { extractOfficialPhoneFromHtml } from "./census-phone-number-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { isFalsePositiveRoomCount } from "./production-census-rooms-keys-extractor.js";
import { resolveStateRegionFromCity, resolveStateFromChoiceOfficialUrl, isDirtyStateRegionValue } from "./census-city-to-state-map.js";
import {
  resolveMarketFromCity,
  resolveSubmarketHighOnly,
  resolveContinentSubContinentFromCountry,
} from "./census-region-market-map.js";
import { isIncorrectCanonicalPropertyName } from "./universal-hotel-record-inspector.js";

export const CHOICE_PROPERTY_RESOLVER_VERSION = "choice-property-record-resolver-v1";

function isBlank(v) {
  return v == null || !String(v).trim();
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Normalize Choice property code (MX043).
 */
export function normalizeChoicePropertyCode(raw) {
  const s = String(raw || "")
    .trim()
    .toUpperCase();
  if (/^[A-Z]{2}\d{2,4}$/.test(s)) return s;
  const m = s.match(/\b([A-Z]{2}\d{2,4})\b/);
  return m ? m[1] : null;
}

/** Official Choice URL brand-slug → display label (High identity only). */
const CHOICE_URL_BRAND_LABELS = [
  { re: /grand-fiesta-americana/i, label: "Grand Fiesta Americana", key: "grand_fiesta" },
  { re: /fiesta-americana/i, label: "Fiesta Americana", key: "fiesta" },
  { re: /comfort-inn|comfort-suites/i, label: "Comfort Inn", key: "comfort" },
  { re: /quality-inn|quality-hotel/i, label: "Quality Inn", key: "quality" },
  { re: /sleep-inn/i, label: "Sleep Inn", key: "sleep" },
  { re: /econo-lodge/i, label: "Econo Lodge", key: "econo" },
  { re: /rodeway/i, label: "Rodeway Inn", key: "rodeway" },
  { re: /clarion/i, label: "Clarion", key: "clarion" },
  { re: /cambria/i, label: "Cambria Hotels", key: "cambria" },
  { re: /park-inn/i, label: "Park Inn", key: "park_inn" },
  { re: /ascend/i, label: "Ascend Hotel Collection", key: "ascend" },
  { re: /radisson/i, label: "Radisson", key: "radisson" },
];

function brandKeyFromLabel(label) {
  const s = String(label || "").toLowerCase();
  if (/grand\s*fiesta/.test(s)) return "grand_fiesta";
  if (/fiesta\s*americana/.test(s)) return "fiesta";
  if (/comfort/.test(s)) return "comfort";
  if (/quality/.test(s)) return "quality";
  if (/sleep/.test(s)) return "sleep";
  if (/econo/.test(s)) return "econo";
  if (/rodeway/.test(s)) return "rodeway";
  if (/clarion/.test(s)) return "clarion";
  if (/cambria/.test(s)) return "cambria";
  if (/park\s*inn/.test(s)) return "park_inn";
  if (/ascend/.test(s)) return "ascend";
  if (/radisson/.test(s)) return "radisson";
  return null;
}

/**
 * Derive Choice brand label from official property URL slug.
 * @param {string} url
 * @returns {{ label: string, key: string }|null}
 */
export function deriveChoiceBrandLabelFromOfficialUrl(url) {
  const u = String(url || "").trim();
  if (!/choicehotels\.com/i.test(u)) return null;
  for (const rule of CHOICE_URL_BRAND_LABELS) {
    if (rule.re.test(u)) return { label: rule.label, key: rule.key };
  }
  return null;
}

/**
 * Pick High brand label for stub name compose (prefer clean Current Brand; else URL slug).
 */
export function resolveChoiceBrandLabelForCompose(fields = {}) {
  const current = String(fields["Current Brand"] || "").trim();
  const url =
    fields["Official Property URL"] || fields["Source URL"] || fields.HotelURL || "";
  const fromUrl = deriveChoiceBrandLabelFromOfficialUrl(url);
  const weakCurrent = !current || /^(choice(\s+hotels?)?|unknown|n\/?a)$/i.test(current);
  if (weakCurrent && fromUrl) {
    return { label: fromUrl.label, method: "choice_url_brand_slug", key: fromUrl.key };
  }
  if (fromUrl) {
    const currentKey = brandKeyFromLabel(current);
    // Official URL brand family conflicts with Current Brand → trust URL slug
    if (currentKey && currentKey !== fromUrl.key) {
      return { label: fromUrl.label, method: "choice_url_brand_overrides_conflict", key: fromUrl.key };
    }
  }
  if (current && !weakCurrent) {
    return { label: current, method: "current_brand", key: brandKeyFromLabel(current) };
  }
  if (fromUrl) {
    return { label: fromUrl.label, method: "choice_url_brand_slug", key: fromUrl.key };
  }
  return null;
}

/**
 * True when name is a parent "… property CODE" stub (e.g. Choice property MX043).
 */
export function isParentPropertyCodeStubName(name) {
  return /^(choice|hilton|marriott|ihg|accor|wyndham|preferred)\s+property\s+[a-z0-9]+$/i.test(
    String(name || "").trim()
  );
}

/**
 * Build Choice official URL candidates from directory row / known patterns.
 */
export function buildChoiceOfficialUrlCandidates(input = {}) {
  /** @type {string[]} */
  const out = [];
  const push = (u) => {
    const c = canonicalChoicePropertyUrl(u);
    if (c && /choicehotels\.com/i.test(c) && !out.includes(c)) out.push(c);
  };
  if (input.directoryUrl) push(input.directoryUrl);
  if (input.officialUrl) push(input.officialUrl);
  if (input.sourceUrl) push(input.sourceUrl);
  // Pattern only when city + brand slug known — never invent city
  const code = normalizeChoicePropertyCode(input.propertyCode);
  const citySlug = String(input.citySlug || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-");
  const brandSlug = String(input.brandSlug || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-");
  const region = String(input.regionSlug || "mexico")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-");
  if (code && citySlug && brandSlug) {
    push(`https://www.choicehotels.com/${region}/${citySlug}/${brandSlug}/${code.toLowerCase()}`);
  }
  return out;
}

async function fetchChoicePage(url, opts = {}) {
  const target = canonicalChoicePropertyUrl(url);
  if (!target) return { ok: false, reason: "blank_url" };
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), opts.timeoutMs || 25000);
  try {
    const res = await fetch(target, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        ...CHOICE_FETCH_HEADERS,
        Accept: "text/html,application/xhtml+xml",
      },
    });
    const html = await res.text();
    if (!res.ok) {
      return {
        ok: false,
        reason: `http_${res.status}`,
        status: res.status,
        url: target,
        blocked: res.status === 403 || res.status === 429,
      };
    }
    return { ok: true, html, url: target, status: res.status };
  } catch (err) {
    return {
      ok: false,
      reason: err?.name === "AbortError" ? "timeout" : "network_error",
      error: err?.message || String(err),
      url: target,
    };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Resolve one Choice Census record (or synthetic fields + property code).
 */
export async function resolveChoicePropertyRecord(record, opts = {}) {
  const fields = { ...(record?.fields || {}) };
  if (opts.propertyCode) {
    fields["Brand Property Code"] = normalizeChoicePropertyCode(opts.propertyCode);
  }
  const identityKey = fields["Property Identity Key"] || "";
  const propertyId =
    normalizeChoicePropertyCode(opts.propertyCode) ||
    extractChoicePropertyId(fields, identityKey);

  const blockers = [];
  const secondaryOpportunities = [];
  /** @type {Record<string, unknown>} */
  const patch = {};
  const provenance = [];

  if (!opts.skipWarmCache) {
    await warmFamilyDirectoryCaches({
      log: opts.log,
      includeMarriott: false,
      includeAccor: false,
    });
  }

  const lookup = await lookupChoiceRegionalRow(fields, identityKey, {
    force: opts.forceCache,
  });

  let directoryRow = lookup.ok ? lookup.row : null;
  let match = lookup.match || null;

  // Direct cache get by code when fields lack URL/identity
  if (!directoryRow && propertyId && opts.choiceCache?.get) {
    directoryRow = opts.choiceCache.get(propertyId) || null;
    if (directoryRow) match = "property_id_direct";
  }

  if (!directoryRow && propertyId) {
    // Re-warm and try ID only via synthetic fields
    const retry = await lookupChoiceRegionalRow(
      { ...fields, "Brand Property Code": propertyId },
      identityKey.includes(propertyId.toLowerCase())
        ? identityKey
        : `ind_choice_mx_${propertyId.toLowerCase()}`,
      {}
    );
    if (retry.ok) {
      directoryRow = retry.row;
      match = retry.match || "property_id_retry";
    }
  }

  if (!directoryRow) {
    blockers.push({
      field: "directory",
      reason: lookup.reason || "choice_directory_miss",
      property_id: propertyId,
    });
    // Continue with Official Property URL when present — directory is not required
  }

  const dirUrl = directoryRow
    ? canonicalChoicePropertyUrl(directoryRow.propertyUrl)
    : null;
  const urlCandidates = buildChoiceOfficialUrlCandidates({
    directoryUrl: dirUrl,
    officialUrl: fields["Official Property URL"],
    sourceUrl: /regional-hotels/i.test(String(fields["Source URL"] || ""))
      ? null
      : fields["Source URL"],
    propertyCode: propertyId,
  });

  if (!urlCandidates.length && !directoryRow) {
    secondaryOpportunities.push({
      field: "Address/Phone/Rooms",
      reason: "official_choice_directory_and_url_miss",
      note: "Enable secondary only after founder policy approval",
    });
    return {
      ok: false,
      version: CHOICE_PROPERTY_RESOLVER_VERSION,
      property_id: propertyId,
      match: null,
      patch: {},
      blockers,
      secondary_opportunities: secondaryOpportunities,
      webhound_as_sot: false,
    };
  }

  // Hotel URL
  if (dirUrl && isBlank(fields["Official Property URL"])) {
    patch["Official Property URL"] = dirUrl;
    patch["Source URL"] = dirUrl;
    patch["Family / Source Family"] =
      fields["Family / Source Family"] || "Choice Hotels International";
    provenance.push({ field: "Official Property URL", source_url: dirUrl, match });
  }

  // Canonical / Property Name from directory
  const dirName = String(directoryRow?.name || "").trim();
  const canonCheck = isIncorrectCanonicalPropertyName(fields);
  const nameCheck = isIncorrectCanonicalPropertyName({
    ...fields,
    "Canonical Property Name": fields["Property Name"],
  });
  if (dirName && dirName.length >= 4) {
    if (isBlank(fields["Property Name"]) || nameCheck.incorrect) {
      patch["Property Name"] = dirName;
      provenance.push({
        field: "Property Name",
        source_url: dirUrl,
        match,
        prior_incorrect: nameCheck.incorrect ? nameCheck.reason : null,
      });
    }
    if (isBlank(fields["Canonical Property Name"]) || canonCheck.incorrect) {
      patch["Canonical Property Name"] = dirName;
      provenance.push({
        field: "Canonical Property Name",
        source_url: dirUrl,
        match,
        prior_incorrect: canonCheck.incorrect ? canonCheck.reason : null,
      });
    }
  }

  // City from directory if available
  const dirCity =
    directoryRow?.city ||
    directoryRow?.cityName ||
    (directoryRow?.citySlug
      ? String(directoryRow.citySlug).replace(/-/g, " ")
      : null);
  if (dirCity && isBlank(fields.City)) {
    const cityLabel = String(dirCity).replace(/\b\w/g, (c) => c.toUpperCase());
    if (cityLabel.length > 2) {
      patch.City = cityLabel;
      provenance.push({ field: "City", source_url: dirUrl, match });
    }
  }

  // Directory address candidate
  if (directoryRow && isBlank(fields.Address)) {
    const addrHit = await resolveDirectoryAddressCandidate({
      fields: {
        ...fields,
        "Official Property URL": dirUrl || fields["Official Property URL"],
        "Brand Property Code": propertyId,
      },
      identityKey,
      family: "Choice",
    });
    if (addrHit?.ok && addrHit.address && isStreetLevelAddress(addrHit.address)) {
      patch.Address = addrHit.address;
      patch["Address Confidence"] = "High";
      patch["Address Source URL"] = addrHit.source_url || dirUrl;
      provenance.push({
        field: "Address",
        source_url: patch["Address Source URL"],
        method: "choice_directory",
      });
    }
  }

  // Property page extraction (JSON-LD / HTML) — primary when directory misses
  let pageHtml = null;
  let pageUrl = null;
  for (const cand of urlCandidates.slice(0, 3)) {
    const fetched = await fetchChoicePage(cand, {
      ...opts,
      timeoutMs: opts.timeoutMs || 45000,
    });
    if (fetched.ok) {
      pageHtml = fetched.html;
      pageUrl = fetched.url;
      break;
    }
    if (fetched.blocked) {
      blockers.push({ field: "property_page", reason: "bot_blocked", url: cand });
    } else {
      blockers.push({
        field: "property_page",
        reason: fetched.reason || "fetch_failed",
        url: cand,
      });
    }
  }

  if (pageHtml && pageUrl) {
    // Name from JSON-LD / <title> when stub
    if (canonCheck.incorrect || nameCheck.incorrect || isBlank(fields["Canonical Property Name"])) {
      const ldName =
        pageHtml.match(/"@type"\s*:\s*"Hotel"[^]*?"name"\s*:\s*"([^"]+)"/i) ||
        pageHtml.match(/propertyName["']?\s*[:=]\s*["']([^"']+)["']/i);
      const title = pageHtml.match(/<title[^>]*>([^<]+)<\/title>/i);
      const pageName = String(ldName?.[1] || "")
        .trim()
        .replace(/\s*\|\s*Choice Hotels.*$/i, "")
        .trim();
      const titleName = String(title?.[1] || "")
        .split("|")[0]
        .trim()
        .replace(/\s*-\s*Choice Hotels.*$/i, "")
        .trim();
      const resolvedName = pageName || titleName;
      if (
        resolvedName &&
        resolvedName.length >= 6 &&
        !/choice property/i.test(resolvedName) &&
        !/^[A-Z]{2}\d{2,4}$/i.test(resolvedName)
      ) {
        if (isBlank(fields["Property Name"]) || nameCheck.incorrect) {
          patch["Property Name"] = resolvedName;
          provenance.push({ field: "Property Name", source_url: pageUrl, method: "property_page" });
        }
        if (isBlank(fields["Canonical Property Name"]) || canonCheck.incorrect) {
          patch["Canonical Property Name"] = resolvedName;
          provenance.push({
            field: "Canonical Property Name",
            source_url: pageUrl,
            method: "property_page",
          });
        }
      }
    }

    if (isBlank(fields.Address) && !patch.Address) {
      const addr = extractOfficialAddressFromHtml(pageHtml, pageUrl);
      if (addr.ok && isStreetLevelAddress(addr.address)) {
        patch.Address = addr.address;
        patch["Address Confidence"] = "High";
        patch["Address Source URL"] = pageUrl;
        provenance.push({ field: "Address", source_url: pageUrl, method: "official_json_ld_or_html" });
      }
    }
    if (isBlank(fields.Phone) && isBlank(fields["Phone Number"])) {
      const phone = extractOfficialPhoneFromHtml(pageHtml, pageUrl);
      if (phone.ok && phone.phone) {
        patch.Phone = phone.phone;
        provenance.push({ field: "Phone", source_url: pageUrl, method: "official_phone" });
      } else {
        blockers.push({ field: "Phone", reason: "phone_missing_from_official_source" });
        secondaryOpportunities.push({
          field: "Phone",
          reason: "official_choice_page_lacks_phone",
        });
      }
    }
    if (isBlank(fields["Rooms / Keys"])) {
      const rooms = extractOfficialRoomsFromHtml(pageHtml, pageUrl);
      const count = rooms.rooms ?? rooms.count;
      if (
        rooms.ok &&
        count != null &&
        !isFalsePositiveRoomCount(rooms.note || "", count, "official_rooms")
      ) {
        patch["Rooms / Keys"] = count;
        patch["Rooms Confidence"] = "High";
        patch["Rooms Source URL"] = pageUrl;
        patch["Rooms Source Type"] = "official_property_page";
        patch["Rooms Evidence Tier"] = "official_choice_html";
        patch["Rooms Reviewed Date"] = todayIso();
        patch["Rooms Review Status"] = "Autopilot High — Choice official page";
        provenance.push({ field: "Rooms / Keys", source_url: pageUrl });
      } else {
        blockers.push({ field: "Rooms / Keys", reason: "rooms_missing_from_official_source" });
        secondaryOpportunities.push({
          field: "Rooms / Keys",
          reason: "official_choice_page_lacks_exact_rooms",
        });
      }
    }
  } else if (!pageHtml) {
    blockers.push({ field: "property_page", reason: "choice_property_page_unavailable" });
  }

  // When directory/page blocked but stub name + Brand (or URL slug) + City → High name
  const city = String(patch.City || fields.City || "").trim();
  const brandHit = resolveChoiceBrandLabelForCompose({
    ...fields,
    "Official Property URL":
      patch["Official Property URL"] || fields["Official Property URL"],
  });
  if (
    brandHit?.label &&
    city &&
    (canonCheck.incorrect || nameCheck.incorrect)
  ) {
    const composed = `${brandHit.label} ${city}`.replace(/\s+/g, " ").trim();
    if (composed.length >= 8 && !patch["Canonical Property Name"]) {
      patch["Canonical Property Name"] = composed;
      provenance.push({
        field: "Canonical Property Name",
        method: `brand_city_compose_${brandHit.method}`,
        source_url: fields["Official Property URL"] || null,
      });
    }
    if (
      (isBlank(fields["Property Name"]) || nameCheck.incorrect) &&
      !patch["Property Name"]
    ) {
      patch["Property Name"] = composed;
      provenance.push({
        field: "Property Name",
        method: `brand_city_compose_${brandHit.method}`,
        source_url: fields["Official Property URL"] || null,
      });
    }
  }

  // Geography maps (Dealality commercial — not inventing)
  const cityForGeo = patch.City || fields.City;
  const country = fields.Country || "Mexico";
  const existingState = fields["State / Region"];
  if (
    (isBlank(existingState) || isDirtyStateRegionValue(existingState)) &&
    !patch["State / Region"]
  ) {
    const st = resolveStateRegionFromCity({
      city: cityForGeo,
      country,
      state: existingState,
    });
    if (st.ok && st.state) {
      patch["State / Region"] = st.state;
      provenance.push({
        field: "State / Region",
        method: st.method || "approved_city_state_map",
      });
    } else {
      const fromUrl = resolveStateFromChoiceOfficialUrl(
        fields["Official Property URL"] || dirUrl || fields["Source URL"]
      );
      if (fromUrl.ok && fromUrl.state) {
        patch["State / Region"] = fromUrl.state;
        provenance.push({
          field: "State / Region",
          method: fromUrl.method,
          source_url: fields["Official Property URL"] || dirUrl,
        });
      } else {
        blockers.push({
          field: "State / Region",
          reason: st.reason || fromUrl.reason || "state_mapping_missing",
        });
      }
    }
  }
  if (isBlank(fields.Market) && cityForGeo) {
    const mkt = resolveMarketFromCity({ city: cityForGeo, country });
    if (mkt.ok && mkt.market) {
      patch.Market = mkt.market;
      provenance.push({ field: "Market", method: mkt.method });
    } else {
      blockers.push({ field: "Market", reason: mkt.reason || "market_mapping_missing" });
    }
  }
  if (isBlank(fields.Submarket) && (patch.Market || fields.Market) && cityForGeo) {
    const sub = resolveSubmarketHighOnly({
      market: patch.Market || fields.Market,
      city: cityForGeo,
      address: patch.Address || fields.Address,
      propertyName: patch["Canonical Property Name"] || fields["Canonical Property Name"],
    });
    if (sub.ok && sub.submarket) {
      patch.Submarket = sub.submarket;
      provenance.push({ field: "Submarket", method: sub.method });
    } else {
      blockers.push({ field: "Submarket", reason: sub.reason || "submarket_mapping_missing" });
    }
  }

  // Official Choice regional geo (prefer over Mapbox when present)
  if (
    directoryRow &&
    (isBlank(fields.Latitude) || isBlank(fields.Longitude)) &&
    directoryRow.latitude != null &&
    directoryRow.longitude != null
  ) {
    const lat = Number(directoryRow.latitude);
    const lng = Number(directoryRow.longitude);
    if (
      Number.isFinite(lat) &&
      Number.isFinite(lng) &&
      !(lat === 0 && lng === 0) &&
      Math.abs(lat) <= 90 &&
      Math.abs(lng) <= 180
    ) {
      patch.Latitude = lat;
      patch.Longitude = lng;
      patch["Coordinate Source Type"] = "official_brand_directory";
      patch["Coordinate Confidence"] = "High";
      patch["Geocode Provider"] = "Choice regional directory";
      patch["Geocode Method"] = "choice_regional_geoLocation";
      patch["Geocode Reviewed Date"] = todayIso();
      provenance.push({
        field: "Latitude/Longitude",
        method: "choice_regional_geoLocation",
        source_url: dirUrl || directoryRow.propertyUrl || directoryRow.source_url,
      });
    }
  }
  // Continent / Sub-Continent only when maps resolve — omit if schema field missing at write time
  if (isBlank(fields.Continent) || isBlank(fields["Sub-Continent"])) {
    const geo = resolveContinentSubContinentFromCountry(country);
    if (geo?.continent && isBlank(fields.Continent)) patch.Continent = geo.continent;
    if (geo?.subContinent && isBlank(fields["Sub-Continent"])) {
      patch["Sub-Continent"] = geo.subContinent;
    }
  }

  if (Object.keys(patch).length) {
    patch["Last Reviewed Date"] = todayIso();
    patch["Enrichment Status"] = "Universal record resolver — Choice official";
  }

  return {
    ok: Object.keys(patch).length > 0,
    version: CHOICE_PROPERTY_RESOLVER_VERSION,
    property_id: propertyId || directoryRow?.propertyId || null,
    match,
    directory_url: dirUrl,
    page_url: pageUrl,
    patch,
    provenance,
    blockers,
    secondary_opportunities: secondaryOpportunities,
    webhound_as_sot: false,
    source_class: "official_choice",
  };
}
