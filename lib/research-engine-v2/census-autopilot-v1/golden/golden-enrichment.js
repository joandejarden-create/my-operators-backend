/**
 * Map live Autopilot research + Dealality derivation → Golden Census field cells.
 * Never uses Cvent or legacy as production evidence.
 */

import { VALUE_STATUS, hasSupportedValue } from "./golden-completeness.js";
import { assignDealalityGeography } from "./golden-geography.js";
import { FIELD_RESOLUTION_STATUS } from "../constants.js";

const RESOLVED = new Set([
  FIELD_RESOLUTION_STATUS.VERIFIED,
  FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING,
  FIELD_RESOLUTION_STATUS.MISSING_FOUND,
  FIELD_RESOLUTION_STATUS.DERIVED,
]);

function cell(value, opts = {}) {
  const supported = hasSupportedValue(value);
  return {
    value: supported ? value : null,
    status: opts.status || (supported ? VALUE_STATUS.SUPPORTED : VALUE_STATUS.UNKNOWN),
    source: opts.source || null,
    source_family: opts.source_family || null,
    evidence_url: opts.evidence_url || null,
    derived: Boolean(opts.derived),
    confidence: opts.confidence || null,
    cvent_used: false,
    legacy_used: false,
  };
}

function fromLiveFields(fieldResults) {
  const map = {};
  for (const fr of fieldResults || []) {
    if (!fr?.field) continue;
    const st = fr.resolution_status || fr.status;
    const val = fr.researched_value ?? fr.value;
    if (RESOLVED.has(st) && hasSupportedValue(val)) {
      map[fr.field] = {
        value: val,
        status: st === FIELD_RESOLUTION_STATUS.DERIVED ? VALUE_STATUS.DERIVED : VALUE_STATUS.SUPPORTED,
        source: fr.source || fr.source_method || null,
        evidence_url: fr.evidence_url || null,
        confidence: fr.confidence || null,
        cvent_used: Boolean(fr.cvent_used_as_source),
        legacy_used: Boolean(fr.legacy_used_as_source),
      };
    }
  }
  return map;
}

const AMENITY_PATTERNS = Object.freeze({
  Pool: /\bpool|alberca|piscina\b/i,
  Spa: /\bspa\b/i,
  Fitness: /\bfitness|gym|gimnasio|health club\b/i,
  Golf: /\bgolf\b/i,
  "Beach / Beachfront": /\bbeach|playa|beachfront|oceanfront\b/i,
  "Beach Club": /\bbeach club\b/i,
  Casino: /\bcasino\b/i,
  "Kids Club": /\bkids?\s*club|club infantil|children.?s club\b/i,
  "Club Lounge": /\bclub lounge|executive lounge|concierge lounge\b/i,
  "All-Inclusive": /\ball[\s-]?inclusive|todo incluido\b/i,
  Parking: /\bparking|estacionamiento|valet\b/i,
  "Airport Shuttle": /\bairport\s*shuttle|traslado\s*aeropuerto\b/i,
  Ski: /\bski\b/i,
  "Residences Amenity": /\bresidences?\b|branded residences/i,
});

/**
 * Extract amenity Yes flags from source text/tags. Never set No from absence.
 */
export function extractAmenityFlags(sourceText, tags = []) {
  const hay = `${sourceText || ""} ${(tags || []).join(" ")}`;
  const out = {};
  for (const [field, re] of Object.entries(AMENITY_PATTERNS)) {
    if (re.test(hay)) {
      out[field] = cell("Yes", {
        status: VALUE_STATUS.SUPPORTED,
        source: "official_amenity_text_explicit",
        confidence: "Medium",
      });
    }
  }
  return out;
}

/**
 * F&B / meetings soft signals from amenity/description text.
 */
export function extractFbMeetingsSignals(text) {
  const t = String(text || "");
  const out = {};
  if (/\brestaurant|dining|comida|cocina\b/i.test(t)) {
    out["F&B Flag"] = cell("Yes", {
      source: "official_text_explicit_dining",
      confidence: "Medium",
    });
  }
  if (/\bbar|lounge|rooftop bar\b/i.test(t)) {
    out["Bars / Lounges"] = cell("Yes", {
      source: "official_text_explicit_bar",
      confidence: "Low",
    });
  }
  if (/\brooftop\b/i.test(t) && /\b(bar|restaurant|dining|lounge)\b/i.test(t)) {
    out["Rooftop F&B"] = cell("Yes", {
      source: "official_text_explicit_rooftop_fb",
      confidence: "Low",
    });
  }
  if (/\broom service|in-room dining\b/i.test(t)) {
    out["Room Service"] = cell("Yes", {
      source: "official_text_explicit_room_service",
      confidence: "Medium",
    });
  }
  if (/\bmeeting|event space|sal[oó]n|ballroom|convention|banquet\b/i.test(t)) {
    out["Meeting / Event Space"] = cell("Yes", {
      source: "official_text_explicit_meetings",
      confidence: "Medium",
    });
  }
  if (/\bballroom\b/i.test(t)) {
    out["Ballroom"] = cell("Yes", {
      source: "official_text_explicit_ballroom",
      confidence: "Medium",
    });
  }
  if (/\bconvention\b/i.test(t)) {
    out["Convention Hotel"] = cell("Yes", {
      source: "official_text_explicit_convention",
      confidence: "Low",
    });
  }
  // Defensible restaurant count only when explicit
  const countMatch = t.match(/(\d{1,2})\s+(?:restaurants?|on-?site restaurants?)/i);
  if (countMatch) {
    const n = Number(countMatch[1]);
    if (n >= 1 && n <= 40) {
      out["Restaurant Count"] = cell(n, {
        source: "official_text_explicit_restaurant_count",
        confidence: "Medium",
      });
    }
  }
  return out;
}

/**
 * Dealality classification derivation (documented, reproducible).
 */
export function deriveDealalityClassification(record, geo, amenityText) {
  const brand = String(record.brand || "").toLowerCase();
  const market = geo.Market;
  const coastal = MEXICO_COASTAL_CHECK(market);
  let resortUrban = coastal ? "Resort" : "Urban";
  if (/airport/i.test(`${record.city || ""} ${record.name || ""}`)) resortUrban = "Urban";

  let propertyType = "Hotel";
  if (/resort/i.test(`${record.name || ""} ${brand}`)) propertyType = "Resort";
  if (/suites/i.test(brand)) propertyType = "All-Suite Hotel";

  let segment = "Upper Midscale / Upscale (family-derived)";
  if (/intercontinental|kimpton|vignette|regent|six senses|waldorf|conrad|luxury|joia|iberostar/i.test(brand)) {
    segment = "Upper Upscale / Luxury (family-derived)";
  } else if (/holiday inn express|hampton|tru|spark|rodeway|econo|suburban/i.test(brand)) {
    segment = "Midscale / Economy (family-derived)";
  } else if (/crowne plaza|hilton garden|doubletree|homewood|staybridge|candlewood|cambria|ascend|quality|comfort/i.test(brand)) {
    segment = "Upscale / Upper Midscale (family-derived)";
  }

  let assetContext = coastal ? "Leisure / Resort" : "Urban / Commercial";
  if (/airport/i.test(`${record.city || ""} ${record.name || ""}`)) assetContext = "Airport / Transit";

  return {
    "Property Type": cell(propertyType, {
      status: VALUE_STATUS.DERIVED,
      source: "dealality_property_type_rules_v1",
      derived: true,
      confidence: "Medium",
    }),
    "Resort / Urban": cell(resortUrban, {
      status: VALUE_STATUS.DERIVED,
      source: "dealality_resort_urban_from_market_v1",
      derived: true,
      confidence: "Medium",
    }),
    "Asset Context": cell(assetContext, {
      status: VALUE_STATUS.DERIVED,
      source: "dealality_asset_context_rules_v1",
      derived: true,
      confidence: "Medium",
    }),
    "Dealality Segment / Positioning": cell(segment, {
      status: VALUE_STATUS.DERIVED,
      source: "dealality_segment_from_brand_family_v1",
      derived: true,
      confidence: "Low",
    }),
  };
}

function MEXICO_COASTAL_CHECK(market) {
  return [
    "Cancún / Riviera Maya",
    "Los Cabos",
    "Puerto Vallarta / Riviera Nayarit",
    "Acapulco",
    "Mazatlán",
    "Veracruz",
  ].includes(market);
}

/**
 * Short Dealality-original AI summary from verified facts (no copyrighted long copy).
 */
export function buildDealalityAiSummary(record, geo, fieldMap) {
  const parts = [];
  const name = record.name || fieldMap["Property Name"]?.value;
  const brand = record.brand || fieldMap["Current Brand"]?.value;
  const city = geo.City || record.city;
  const market = geo.Market;
  const rooms = fieldMap["Rooms / Keys"]?.value;
  const resort = fieldMap["Resort / Urban"]?.value;
  if (!name || !brand) return null;
  parts.push(`${name} is a ${brand} hotel`);
  if (city) parts.push(`in ${city}`);
  if (market) parts.push(`(${market})`);
  parts.push(".");
  if (resort) parts.push(` Dealality classifies the asset as ${resort}.`);
  if (rooms) parts.push(` Independently supported room inventory: ${rooms} keys.`);
  const am = [];
  for (const f of ["Pool", "Spa", "Fitness", "Meeting / Event Space", "F&B Flag"]) {
    if (fieldMap[f]?.value === "Yes") am.push(f.replace(" Flag", "").replace(" / Event Space", " space"));
  }
  if (am.length) parts.push(` Official sources support: ${am.join(", ")}.`);
  return parts.join("").replace(/\s+/g, " ").trim();
}

/**
 * Boost rooms extraction from HTML (stronger patterns; still no Cvent).
 * @param {string} html
 */
export function extractRoomsBoost(html) {
  if (!html) return null;
  const patterns = [
    /"numberOfRooms"\s*:\s*(\d{2,4})/i,
    /"totalRooms"\s*:\s*(\d{2,4})/i,
    /"roomCount"\s*:\s*(\d{2,4})/i,
    /"rooms"\s*:\s*(\d{2,4})/i,
    /data-room-count=["'](\d{2,4})["']/i,
    /(\d{2,4})\s+(?:guest\s+)?rooms?\b/i,
    /\b(?:features?|offers?|with)\s+(\d{2,4})\s+rooms?\b/i,
    /total\s+(?:of\s+)?(\d{2,4})\s+(?:guest\s+)?rooms/i,
    /(\d{2,4})\s+llaves\b/i,
    /(\d{2,4})\s+habitaciones\b/i,
  ];
  for (const re of patterns) {
    const m = html.match(re);
    if (!m) continue;
    const n = Number(m[1]);
    // Reject VIC false-positive-ish tiny numbers and absurd sizes
    if (n >= 20 && n <= 2500) return n;
  }
  return null;
}

/**
 * Build golden field map for one hotel from VIC record + optional live research result.
 */
export function buildGoldenFieldMap(record, liveResult = null) {
  const fieldMap = {};
  const liveMap = fromLiveFields(liveResult?.fields || []);

  // Identity from VIC (independent discovery — not legacy production)
  fieldMap["Property Name"] = cell(record.name, {
    source: "vic_independent_index",
    source_family: record.family,
    evidence_url: record.discovery_source || record.website,
  });
  fieldMap["Current Brand"] = cell(record.brand, {
    source: "vic_independent_index",
    source_family: record.family,
  });
  fieldMap["Brand Family"] = cell(record.parent || record.family, {
    source: "vic_independent_index",
    status: VALUE_STATUS.DERIVED,
    derived: true,
  });
  const pid = (record.property_ids && record.property_ids[0]) || record.property_id;
  if (pid) {
    fieldMap["Official Property ID"] = cell(String(pid).toUpperCase(), {
      source: "vic_brand_property_code",
      source_family: record.family,
    });
  }
  fieldMap["Official Property URL"] = cell(record.website, {
    source: "vic_independent_index",
    evidence_url: record.website,
  });
  fieldMap.Country = cell(record.country || "Mexico", {
    source: "vic_independent_index",
  });
  fieldMap.City = cell(record.city, {
    source: "vic_independent_index",
  });

  // Overlay live research (authoritative when present; reject cvent/legacy)
  for (const [k, v] of Object.entries(liveMap)) {
    if (v.cvent_used || v.legacy_used) continue;
    fieldMap[k] = { ...v, cvent_used: false, legacy_used: false };
  }

  // Alias live amenity flag names
  const flagAlias = {
    "Pool Flag": "Pool",
    "Spa Flag": "Spa",
    "Fitness Flag": "Fitness",
    "Meeting Space Flag": "Meeting / Event Space",
  };
  for (const [from, to] of Object.entries(flagAlias)) {
    if (liveMap[from] && !fieldMap[to]) fieldMap[to] = liveMap[from];
  }

  // Geography (Dealality-derived)
  const geo = assignDealalityGeography({
    country: record.country,
    city: fieldMap.City?.value || record.city,
    name: record.name,
  });
  fieldMap.Continent = cell(geo.Continent, {
    status: VALUE_STATUS.DERIVED,
    source: geo.geography_provenance.continent_source,
    derived: true,
    confidence: "High",
  });
  fieldMap["Sub-Continent"] = cell(geo["Sub-Continent"], {
    status: VALUE_STATUS.DERIVED,
    source: geo.geography_provenance.continent_source,
    derived: true,
    confidence: "High",
  });
  fieldMap["State / Region"] = cell(geo["State / Region"], {
    status: VALUE_STATUS.DERIVED,
    source: "dealality_market_to_state_map",
    derived: true,
    confidence: geo.Market === "Other Mexico" ? "Low" : "Medium",
  });
  fieldMap.Market = cell(geo.Market, {
    status: VALUE_STATUS.DERIVED,
    source: geo.geography_provenance.market_source,
    derived: true,
    confidence: geo.geography_provenance.market_confidence,
  });
  fieldMap.Submarket = cell(geo.Submarket, {
    status: geo.Submarket ? VALUE_STATUS.DERIVED : VALUE_STATUS.UNKNOWN,
    source: geo.geography_provenance.submarket_source,
    derived: true,
    confidence: geo.geography_provenance.submarket_confidence,
  });

  // Address / phone / coords from live if present (already overlaid)
  // Rooms boost from page HTML if live left rooms empty
  if (!hasSupportedValue(fieldMap["Rooms / Keys"]?.value) && liveResult?.page_html_snippet) {
    const rooms = extractRoomsBoost(liveResult.page_html_snippet);
    if (rooms != null) {
      fieldMap["Rooms / Keys"] = cell(rooms, {
        source: "official_page_rooms_boost_v12",
        evidence_url: record.website,
        confidence: "Medium",
      });
    }
  }

  const amenityText =
    fieldMap["Amenities - Source Text"]?.value ||
    fieldMap["Hotel Description - Source Text"]?.value ||
    "";
  const tags = String(fieldMap["Amenities - Structured Tags"]?.value || "")
    .split(/[;|,]/)
    .map((s) => s.trim())
    .filter(Boolean);

  const amenFlags = extractAmenityFlags(amenityText, tags);
  for (const [k, v] of Object.entries(amenFlags)) {
    if (!fieldMap[k]) fieldMap[k] = v;
  }

  const fbMeet = extractFbMeetingsSignals(
    `${amenityText} ${fieldMap["Hotel Description - Source Text"]?.value || ""}`
  );
  for (const [k, v] of Object.entries(fbMeet)) {
    if (!fieldMap[k]) fieldMap[k] = v;
  }

  // If we have amenity source text, structured tags may be derived from flags
  if (hasSupportedValue(amenityText) && !hasSupportedValue(fieldMap["Amenities - Structured Tags"]?.value)) {
    const derivedTags = Object.keys(amenFlags);
    if (derivedTags.length) {
      fieldMap["Amenities - Structured Tags"] = cell(derivedTags.join("; "), {
        status: VALUE_STATUS.DERIVED,
        source: "dealality_amenity_tag_normalize_v1",
        derived: true,
        confidence: "Medium",
      });
    }
  }

  const classification = deriveDealalityClassification(record, geo, amenityText);
  for (const [k, v] of Object.entries(classification)) {
    if (!fieldMap[k]) fieldMap[k] = v;
  }

  // Content AI summary (Dealality-original)
  if (
    hasSupportedValue(fieldMap["Hotel Description - Source Text"]?.value) ||
    hasSupportedValue(fieldMap["Property Name"]?.value)
  ) {
    const summary = buildDealalityAiSummary(record, geo, fieldMap);
    if (summary) {
      fieldMap["Hotel Description - AI Summary"] = cell(summary, {
        status: VALUE_STATUS.DERIVED,
        source: "dealality_ai_summary_from_verified_facts_v1",
        derived: true,
        confidence: "Medium",
      });
    }
  }

  // If description missing but we have name/brand/city, still allow thin AI summary
  if (!fieldMap["Hotel Description - Source Text"]?.value && record.name) {
    const thin = `${record.name} (${record.brand}) — ${record.city}, ${record.country}. Identity confirmed via independent brand-directory discovery.`;
    fieldMap["Hotel Description - Source Text"] = cell(thin, {
      status: VALUE_STATUS.DERIVED,
      source: "dealality_identity_fact_line_v1",
      derived: true,
      confidence: "Low",
    });
  }

  // Lifecycle opportunistic (separate track — still populate when live found)
  if (liveMap["Opening Date"]) fieldMap["Opening Date"] = liveMap["Opening Date"];
  if (liveMap["Affiliation Status"] || record.status) {
    fieldMap["Affiliation Status"] = cell(record.status || liveMap["Affiliation Status"]?.value, {
      source: "vic_independent_index",
    });
  }
  if (liveMap["Owner Name"]) fieldMap["Owner Name"] = liveMap["Owner Name"];
  if (liveMap["Operator / Management Company"]) {
    fieldMap["Operator / Management Company"] = liveMap["Operator / Management Company"];
  }

  // Governance stubs (not in priority score)
  fieldMap["Source URL"] = cell(record.website || record.discovery_source, {
    source: "vic_independent_index",
  });
  fieldMap["Source Type"] = cell("official_brand_directory_or_property", {
    source: "autopilot_v12",
  });
  fieldMap["Data Confidence Tier"] = cell("Medium", { source: "autopilot_v12" });
  fieldMap["Production Use Status"] = cell("staging_not_written", {
    source: "autopilot_v12_no_writes",
  });
  fieldMap["Last Verified"] = cell(new Date().toISOString().slice(0, 10), {
    source: "autopilot_v12",
  });

  return { fieldMap, geo };
}
