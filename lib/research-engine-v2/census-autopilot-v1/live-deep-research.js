/**
 * Autopilot V1.1 — Live Lane A + Lane B deep research (no credits / no Webhound / no writes).
 * Reuses family directory adapters + official page fetch + deep signal extraction.
 */

import { fetchText, sleep, DEFAULT_FETCH_HEADERS } from "../adapters/adapter-utils.js";
import { IHG_FETCH_HEADERS } from "../../ihg-brand-directory-extract.js";
import { HILTON_FETCH_HEADERS } from "../../hilton-brand-directory-extract.js";
import { CHOICE_FETCH_HEADERS } from "../../choice-regional-directory-extract.js";
import {
  extractIhgAmenitiesFromHtml,
  formatIhgAmenitiesText,
  ihgHoteldetailLooksBlocked,
} from "../../ihg-hotel-amenities-extract.js";
import { extractDeepOfficialPageSignals } from "../clean-census/field-research.js";
import {
  resolveFamilyDirectorySignals,
  warmFamilyDirectoryCaches,
  familyFromIdentity,
  extractHiltonCtyhocn,
} from "../census-autopilot-family-directory-adapters.js";
import { fetchHiltonHotelObservation } from "../adapters/hilton.js";
import { FIELD_RESOLUTION_STATUS, SOURCE_LANE } from "./constants.js";
import { buildFieldRoutingRegistry } from "./field-routing.js";

export const LIVE_DEEP_VERSION = "census-autopilot-v1.1-live-deep-research";

/**
 * Stronger rooms patterns for official HTML (no Cvent). Returns null if none.
 * @param {string} html
 */
export function extractRoomsBoostFromHtml(html) {
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
    const m = String(html).match(re);
    if (!m) continue;
    const n = Number(m[1]);
    if (n >= 20 && n <= 2500) return n;
  }
  return null;
}

/** Material fields that drive incompleteness / remediation. */
export const MATERIAL_HARD_FIELDS = Object.freeze([
  "Rooms / Keys",
  "Opening Date",
  "Operator / Management Company",
  "Owner Name",
  "Latitude",
  "Longitude",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "F&B Flag",
  "Meeting Space Flag",
  "Spa Flag",
  "Pool Flag",
  "Fitness Flag",
  "Phone",
  "Address",
  "Official Property URL",
  "Hotel Description - Source Text",
]);

const RESOLVED_STATUSES = new Set([
  FIELD_RESOLUTION_STATUS.VERIFIED,
  FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING,
  FIELD_RESOLUTION_STATUS.MISSING_FOUND,
  FIELD_RESOLUTION_STATUS.DERIVED,
]);

/**
 * @param {object} record VIC index row
 */
export function recordToAdapterFields(record) {
  const ids = record.property_ids || [];
  const code = ids[0] || record.property_id || null;
  return {
    "Property Name": record.name,
    name: record.name,
    "Current Brand": record.brand,
    Affiliation: record.brand,
    "Brand Family": record.family,
    "Parent Company": record.parent,
    Country: record.country,
    country: record.country,
    City: record.city,
    city: record.city,
    "Affiliation Status": record.status,
    status: record.status,
    "Official Property URL": record.website,
    "Official URL": record.website,
    "Source URL": record.discovery_source || record.website,
    Website: record.website,
    "Property Identity Key": record.independent_record_id,
    "Brand Property Code": code,
    "Family / Source Family": record.family,
  };
}

function headersForFamily(family, url) {
  const f = String(family || "").toLowerCase();
  if (f === "ihg" || /ihg\.com/i.test(url || "")) return IHG_FETCH_HEADERS;
  if (f === "hilton" || /hilton\.com/i.test(url || "")) return HILTON_FETCH_HEADERS;
  if (f === "choice" || /choicehotels\.com/i.test(url || "")) return CHOICE_FETCH_HEADERS;
  return DEFAULT_FETCH_HEADERS;
}

/**
 * @param {string} url
 * @param {string} family
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function fetchOfficialPage(url, family, opts = {}) {
  if (!url || !/^https?:\/\//i.test(url)) {
    return { ok: false, status: 0, url: url || null, text: "", reason: "missing_url" };
  }
  try {
    const page = await fetchText(url, {
      headers: headersForFamily(family, url),
      timeoutMs: opts.timeoutMs ?? 25000,
    });
    const amenityRich =
      /amenity-title|hotel-amenities|cmp-card__title|numberOfRooms|hoteldetail/i.test(
        page.text || ""
      );
    const blocked =
      page.status === 403 ||
      page.status === 429 ||
      ((/access denied|robot check|attention required|please enable javascript/i.test(
        page.text || ""
      ) ||
        (/captcha|akamai/i.test(page.text || "") && (page.text || "").length < 50000)) &&
        !amenityRich);
    const ihgBlocked =
      String(family).toLowerCase() === "ihg" &&
      !amenityRich &&
      ihgHoteldetailLooksBlocked(page.text || "", page.url || url);
    if ((blocked || ihgBlocked) && !amenityRich) {
      return {
        ok: false,
        status: page.status,
        url: page.url || url,
        text: page.text || "",
        reason: ihgBlocked ? "ihg_interstitial" : `blocked_http_${page.status}`,
        retrievedAt: page.retrievedAt,
      };
    }
    return {
      ok: page.ok || amenityRich,
      status: page.status,
      url: page.url || url,
      text: page.text || "",
      reason: page.ok || amenityRich ? null : `http_${page.status}`,
      retrievedAt: page.retrievedAt,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      text: "",
      reason: err?.name === "AbortError" ? "timeout" : err?.message || String(err),
    };
  }
}

/**
 * Extract secondary official hotel website from brand page (Lane B seed).
 * @param {string} html
 * @param {string} pageUrl
 */
export function extractStandaloneWebsiteCandidate(html, pageUrl) {
  const text = String(html || "");
  // Common patterns: "Visit hotel website", external links that aren't brand domains
  const brandHosts = /ihg\.com|hilton\.com|choicehotels\.com|marriott\.com|hyatt\.com|accor\.com/i;
  const candidates = [];
  for (const m of text.matchAll(
    /href=["'](https?:\/\/(?!www\.(?:ihg|hilton|choicehotels|marriott)\.com)[^"']+)["'][^>]*>[\s\S]{0,80}(?:hotel website|official site|visit (?:the )?website|property website)/gi
  )) {
    candidates.push(m[1]);
  }
  for (const m of text.matchAll(
    /"(?:hotelWebsite|propertyWebsite|externalWebsite|websiteUrl)"\s*:\s*"(https?:\/\/[^"]+)"/gi
  )) {
    candidates.push(m[1]);
  }
  for (const c of candidates) {
    try {
      const u = new URL(c);
      if (brandHosts.test(u.hostname)) continue;
      if (/facebook|instagram|twitter|youtube|linkedin|google\./i.test(u.hostname)) continue;
      return c;
    } catch {
      /* skip */
    }
  }
  // JSON-LD sameAs / url that is non-brand
  for (const block of text.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi)) {
    try {
      const json = JSON.parse(block[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        const urls = []
          .concat(obj?.sameAs || [])
          .concat(obj?.url ? [obj.url] : []);
        for (const raw of urls) {
          try {
            const u = new URL(String(raw));
            if (!brandHosts.test(u.hostname) && u.hostname !== new URL(pageUrl || "https://x").hostname) {
              if (!/facebook|instagram|twitter|youtube/i.test(u.hostname)) return u.toString();
            }
          } catch {
            /* skip */
          }
        }
      }
    } catch {
      /* skip */
    }
  }
  return null;
}

/**
 * Build claim bag from directory + deep page signals.
 * @param {object} record
 * @param {object} dirSignals
 * @param {object|null} deep
 * @param {object} pageMeta
 */
export function collectLiveClaims(record, dirSignals, deep, pageMeta = {}) {
  /** @type {Map<string, object>} */
  const claims = new Map();

  const put = (field, value, meta = {}) => {
    if (value == null || value === "") return;
    const prev = claims.get(field);
    const confRank = { Exact: 5, High: 4, Medium: 3, Low: 2, Unknown: 1 };
    const nextRank = confRank[meta.confidence] || 2;
    const prevRank = confRank[prev?.confidence] || 0;
    if (prev && prevRank > nextRank) return;
    claims.set(field, {
      field,
      researched_value: value,
      normalized_value: value,
      confidence: meta.confidence || "Medium",
      source: meta.source || null,
      source_type: meta.source_type || null,
      evidence_url: meta.evidence_url || null,
      evidence_date: meta.evidence_date || null,
      last_verified: meta.last_verified || new Date().toISOString(),
      resolution_level: meta.resolution_level ?? 1,
      lane: meta.lane || SOURCE_LANE.A_STRUCTURED_OFFICIAL,
      corroborating_source: meta.corroborating_source || null,
      contradiction_found: meta.contradiction_found || false,
      temporal_status: meta.temporal_status || "current",
      open_date_kind: meta.open_date_kind || null,
    });
  };

  // Baseline identity from independent freeze (Level 0) — already independently discovered
  put("Property Name", record.name, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    source_type: "Independent Freeze",
    evidence_url: record.website || record.discovery_source,
    resolution_level: 0,
  });
  put("Canonical Property Name", record.name, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    source_type: "Independent Freeze",
    resolution_level: 0,
  });
  put("Current Brand", record.brand, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    source_type: "Independent Freeze",
    resolution_level: 0,
    temporal_status: "current",
  });
  put("Brand Family", record.family, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    resolution_level: 0,
  });
  put("Country", record.country, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    resolution_level: 0,
  });
  put("City", record.city, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    resolution_level: 0,
  });
  put("Affiliation Status", record.status, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    resolution_level: 0,
    temporal_status: "current",
  });
  put("Official Property URL", record.website, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    resolution_level: 0,
  });
  put("Family / Source Family", record.family, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    resolution_level: 0,
  });
  put("Source URL", record.discovery_source || record.website, {
    confidence: "High",
    source: "verified_independent_census_freeze",
    resolution_level: 0,
  });
  put(
    "Property Identity Key",
    (record.property_ids && record.property_ids[0]) || record.independent_record_id,
    {
      confidence: "High",
      source: "verified_independent_census_freeze",
      resolution_level: 0,
    }
  );

  // Lane A directory
  if (dirSignals?.address?.ok) {
    const addr =
      dirSignals.address.address1 ||
      dirSignals.address.address ||
      dirSignals.address.formatted ||
      dirSignals.address.source_text;
    put("Address", addr, {
      confidence: dirSignals.address.confidence || "High",
      source: dirSignals.address.method,
      source_type: "Official Parent Company Directory",
      evidence_url: dirSignals.address.source_url,
      resolution_level: 1,
      lane: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
    });
    if (dirSignals.address.state || dirSignals.address.region) {
      put("State / Region", dirSignals.address.state || dirSignals.address.region, {
        confidence: "High",
        source: dirSignals.address.method,
        source_type: "Official Parent Company Directory",
        evidence_url: dirSignals.address.source_url,
        resolution_level: 1,
      });
    }
  }
  if (dirSignals?.phone?.ok) {
    put("Phone", dirSignals.phone.phone || dirSignals.phone.value, {
      confidence: dirSignals.phone.confidence || "High",
      source: dirSignals.phone.method,
      source_type: "Official Parent Company Directory",
      evidence_url: dirSignals.phone.source_url,
      resolution_level: 1,
    });
  }
  if (dirSignals?.coordinates?.ok) {
    put("Latitude", dirSignals.coordinates.lat, {
      confidence: dirSignals.coordinates.confidence || "High",
      source: dirSignals.coordinates.method,
      source_type: "Official Parent Company Directory",
      evidence_url: dirSignals.coordinates.source_url,
      resolution_level: 1,
    });
    put("Longitude", dirSignals.coordinates.lng, {
      confidence: dirSignals.coordinates.confidence || "High",
      source: dirSignals.coordinates.method,
      source_type: "Official Parent Company Directory",
      evidence_url: dirSignals.coordinates.source_url,
      resolution_level: 1,
    });
  }
  if (dirSignals?.amenities?.ok) {
    put("Amenities - Source Text", dirSignals.amenities.source_text, {
      confidence: dirSignals.amenities.confidence || "Medium",
      source: dirSignals.amenities.method,
      source_type: "Official Parent Company Directory",
      evidence_url: dirSignals.amenities.source_url,
      resolution_level: 1,
    });
    if (dirSignals.amenities.tags?.length) {
      put("Amenities - Structured Tags", dirSignals.amenities.tags.join("; "), {
        confidence: "Medium",
        source: dirSignals.amenities.method,
        evidence_url: dirSignals.amenities.source_url,
        resolution_level: 1,
      });
      applyAmenityFlags(put, dirSignals.amenities.tags, {
        source: dirSignals.amenities.method,
        evidence_url: dirSignals.amenities.source_url,
        resolution_level: 1,
      });
    }
  }
  if (dirSignals?.description?.ok) {
    put(
      "Hotel Description - Source Text",
      dirSignals.description.text || dirSignals.description.source_text,
      {
        confidence: dirSignals.description.confidence || "Medium",
        source: dirSignals.description.method,
        source_type: "Official Parent Company Directory",
        evidence_url: dirSignals.description.source_url,
        resolution_level: 1,
      }
    );
  }

  // Deep official page (Level 2)
  if (deep) {
    const url = pageMeta.url || deep.sourceUrl || record.website;
    const lane = pageMeta.lane || SOURCE_LANE.A_STRUCTURED_OFFICIAL;
    const level = pageMeta.resolution_level ?? 2;

    if (deep.rooms != null) {
      put("Rooms / Keys", deep.rooms, {
        confidence: "Medium",
        source: "official_property_page_explicit_room_count",
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: level,
        lane,
      });
    }
    if (deep.phone) {
      put("Phone", deep.phone, {
        confidence: "Medium",
        source: "official_property_page_tel",
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: level,
        lane,
      });
    }
    if (deep.openDateHint) {
      put("Opening Date", deep.openDateHint, {
        confidence:
          deep.openDateKind === "actual" || deep.openDateKind === "now_open" ? "Medium" : "Low",
        source: `official_property_opening_language_${deep.openDateKind || "unspecified"}`,
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: level,
        lane,
        open_date_kind: deep.openDateKind,
        temporal_status: deep.openDateKind || "current",
      });
      if (deep.openDateKind === "expected" || deep.openDateKind === "announced") {
        put("Future Opening Flag", true, {
          confidence: "Medium",
          source: "official_property_opening_language",
          evidence_url: url,
          resolution_level: level,
        });
      }
    }
    if (deep.latitude != null && deep.longitude != null) {
      put("Latitude", deep.latitude, {
        confidence: "High",
        source: "official_property_embedded_coordinates",
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: level,
        lane,
      });
      put("Longitude", deep.longitude, {
        confidence: "High",
        source: "official_property_embedded_coordinates",
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: level,
        lane,
      });
    }
    if (deep.amenitiesMentioned?.length) {
      put("Amenities - Source Text", deep.amenitiesMentioned.join("; "), {
        confidence: "Medium",
        source: "official_property_explicit_amenity_mentions",
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: level,
        lane,
      });
      applyAmenityFlags(put, deep.amenitiesMentioned, {
        source: "official_property_explicit_amenity_mentions",
        evidence_url: url,
        resolution_level: level,
        lane,
      });
    }
    if (deep.fbMentioned) {
      put("F&B Flag", "Yes", {
        confidence: "Medium",
        source: "official_property_fb_mention",
        evidence_url: url,
        resolution_level: level,
      });
    }
    if (deep.meetingFacilities === "Yes") {
      put("Meeting Space Flag", "Yes", {
        confidence: "Medium",
        source: "official_property_meeting_mention",
        evidence_url: url,
        resolution_level: level,
      });
    }
    if (deep.spaWellness) {
      put("Spa Flag", "Yes", {
        confidence: "Medium",
        source: "official_property_spa_mention",
        evidence_url: url,
        resolution_level: level,
      });
    }
    if (deep.poolMentioned) {
      put("Pool Flag", "Yes", {
        confidence: "Medium",
        source: "official_property_pool_mention",
        evidence_url: url,
        resolution_level: level,
      });
    }
    if (deep.managementHint) {
      put("Operator / Management Company", deep.managementHint, {
        confidence: "Medium",
        source: "official_property_managed_operated_by_language",
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: Math.max(level, 2),
        lane,
        temporal_status: "current",
      });
    }
    if (deep.ownerHint) {
      put("Owner Name", deep.ownerHint, {
        confidence: "Low",
        source: "official_property_owned_by_language",
        source_type: "Official Property Page",
        evidence_url: url,
        resolution_level: Math.max(level, 3),
        lane,
        temporal_status: "current",
      });
    }
  }

  // IHG-specific amenity extract (richer)
  if (pageMeta.ihgAmenitiesText) {
    put("Amenities - Source Text", pageMeta.ihgAmenitiesText, {
      confidence: "High",
      source: "ihg_hoteldetail_amenity_title_extract",
      source_type: "Official Property Page",
      evidence_url: pageMeta.url,
      resolution_level: 2,
    });
  }

  return claims;
}

function applyAmenityFlags(put, tags, meta) {
  const joined = (tags || []).join(" | ").toLowerCase();
  const rules = [
    ["F&B Flag", /restaurant|dining|bistro|cafe|café|bar\b|lounge|f&b/i],
    ["Meeting Space Flag", /meeting|conference|event space|banquet|ballroom/i],
    ["Spa Flag", /spa|wellness|sauna/i],
    ["Pool Flag", /pool|piscina|swimming/i],
    ["Fitness Flag", /fitness|gym|fitness center|fitness centre/i],
    ["Resort / Leisure Flag", /resort|beach|all.?inclusive/i],
    ["Extended Stay Flag", /extended stay|kitchenette|suite stay/i],
  ];
  for (const [field, re] of rules) {
    if (re.test(joined)) {
      put(field, "Yes", { ...meta, confidence: "Medium", source_type: meta.source_type || "Official Property Page" });
    }
  }
}

/**
 * Map claim bag → full researchable field results with ladder / stop / escalation.
 * @param {object} record
 * @param {Map<string, object>} claims
 * @param {object[]} researchable
 * @param {object} researchMeta
 */
export function buildFieldResultsFromClaims(record, claims, researchable, researchMeta = {}) {
  const sourceBlocked = researchMeta.source_blocked === true;
  const fields = [];

  for (const route of researchable) {
    const claim = claims.get(route.field);
    const prior = INDEX_BASELINE(record, route.field);
    let resolution_status;
    let researched_value = claim?.researched_value ?? null;
    let confidence = claim?.confidence || null;
    let escalation_status = "none";
    let proposed_action = "none";
    let contradiction_found = false;
    let resolution_level = claim?.resolution_level ?? null;
    let source_attempt_count = researchMeta.source_attempt_count || 0;
    let research_effort_score = researchMeta.research_effort_score || 0;

    if (claim) {
      if (prior != null && String(prior) !== String(researched_value) && isMaterialComparable(route.field)) {
        contradiction_found = true;
        // Prefer live official over freeze when both present — flag for steward if brand/status
        if (["Current Brand", "Affiliation Status"].includes(route.field)) {
          resolution_status = FIELD_RESOLUTION_STATUS.CONFLICTING_EVIDENCE;
          escalation_status = "human_specialist";
          proposed_action = "steward_review_temporal_change";
        } else {
          resolution_status = FIELD_RESOLUTION_STATUS.SUPERSEDED;
          proposed_action = "adopt_live_official_value";
        }
      } else if (prior != null && String(prior) === String(researched_value)) {
        resolution_status = FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING;
        proposed_action = "retain";
      } else if (prior == null) {
        resolution_status = FIELD_RESOLUTION_STATUS.MISSING_FOUND;
        proposed_action = "stage_independent_claim";
      } else {
        resolution_status = FIELD_RESOLUTION_STATUS.VERIFIED;
        proposed_action = "retain";
      }
      if (resolution_level === 0 && !contradiction_found) {
        resolution_status = FIELD_RESOLUTION_STATUS.VERIFIED;
      }
    } else if (sourceBlocked && !claim) {
      // Page blocked: only mark SOURCE_BLOCKED when directory also failed to provide a path
      if (
        MATERIAL_HARD_FIELDS.includes(route.field) &&
        (researchMeta.resolution_level_max || 0) < 1
      ) {
        resolution_status = FIELD_RESOLUTION_STATUS.SOURCE_BLOCKED;
        escalation_status = "retry_later";
        proposed_action = "retry_official_fetch";
      } else if (route.field === "Owner Name") {
        resolution_status = FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED;
        escalation_status = classifyOwnerEscalation(researchMeta);
        proposed_action = "accept_unknown_or_first_party";
      } else if (route.field === "Operator / Management Company") {
        resolution_status = FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED;
        escalation_status = "webhound_or_first_party";
        proposed_action = "lane_c_candidate_after_ladder";
      } else if (MATERIAL_HARD_FIELDS.includes(route.field)) {
        resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
        escalation_status = classifyMaterialEscalation(route.field, researchMeta);
        proposed_action = "directory_miss_page_blocked_escalate_or_unknown";
      } else if (/Flag$/i.test(route.field)) {
        resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
        escalation_status = "accept_unknown";
        proposed_action = "do_not_infer_no_from_absence";
      } else {
        resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
        escalation_status = "accept_unknown";
        proposed_action = "stop_low_expected_value";
      }
    } else if (route.field === "Owner Name") {
      resolution_status = FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED;
      escalation_status = classifyOwnerEscalation(researchMeta);
      proposed_action = "accept_unknown_or_first_party";
    } else if (route.field === "Operator / Management Company") {
      resolution_status = FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED;
      escalation_status = "webhound_or_first_party";
      proposed_action = "lane_c_candidate_after_ladder";
    } else if (MATERIAL_HARD_FIELDS.includes(route.field)) {
      resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
      escalation_status = classifyMaterialEscalation(route.field, researchMeta);
      proposed_action = "escalate_or_accept_unknown";
    } else if (/Flag$/i.test(route.field)) {
      // Absence ≠ No
      resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
      escalation_status = "accept_unknown";
      proposed_action = "do_not_infer_no_from_absence";
    } else if (route.primary === "source_only" || route.primary === "governance_field") {
      resolution_status = FIELD_RESOLUTION_STATUS.NOT_APPLICABLE;
      proposed_action = "governance_or_derived_later";
    } else {
      resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
      escalation_status = "accept_unknown";
      proposed_action = "stop_low_expected_value";
    }

    const stop =
      RESOLVED_STATUSES.has(resolution_status) &&
      (confidence === "High" || confidence === "Exact" || confidence === "Medium");

    fields.push({
      field: route.field,
      group: route.group,
      researched_value,
      normalized_value: researched_value,
      prior_value: prior,
      resolution_status,
      confidence: confidence || "Unknown",
      source: claim?.source || null,
      source_type: claim?.source_type || null,
      evidence_date: claim?.evidence_date || null,
      last_verified: claim?.last_verified || null,
      evidence_url: claim?.evidence_url || null,
      corroborating_source: claim?.corroborating_source || null,
      contradiction_found,
      proposed_action,
      escalation_status,
      temporal_status: claim?.temporal_status || null,
      open_date_kind: claim?.open_date_kind || null,
      resolution_level,
      source_attempt_count,
      research_effort_score,
      stop_research: stop,
      legacy_used_as_source: false,
      cvent_used_as_source: false,
    });
  }

  return fields;
}

function INDEX_BASELINE(record, field) {
  const map = {
    "Property Name": record.name,
    "Canonical Property Name": record.name,
    "Current Brand": record.brand,
    "Brand Family": record.family,
    Country: record.country,
    City: record.city,
    "Affiliation Status": record.status,
    "Official Property URL": record.website,
    "Family / Source Family": record.family,
    "Source URL": record.discovery_source || record.website,
    "Property Identity Key":
      (record.property_ids && record.property_ids[0]) || record.independent_record_id,
    Phone: record.phone,
    Address: record.address,
    Latitude: record.latitude ?? record.lat,
    Longitude: record.longitude ?? record.lng,
    "Rooms / Keys": record.rooms,
    "Opening Date": record.open_date || record.opening_date,
    "Owner Name": record.owner,
    "Operator / Management Company": record.operator || record.management_company,
  };
  return map[field] ?? null;
}

function isMaterialComparable(field) {
  return ["Current Brand", "Affiliation Status", "Rooms / Keys", "Official Property URL"].includes(
    field
  );
}

function classifyOwnerEscalation(meta) {
  if ((meta.resolution_level_max || 0) >= 3) return "accept_unknown"; // opaque after reasonable attempts
  return "first_party_preferable";
}

function classifyMaterialEscalation(field, meta) {
  if (field === "Rooms / Keys" || field === "Opening Date") {
    return (meta.resolution_level_max || 0) >= 3 ? "webhound_likely_useful" : "retry_later";
  }
  if (["Latitude", "Longitude", "Phone", "Address"].includes(field)) {
    return "retry_later";
  }
  return "accept_unknown";
}

/**
 * Live deep research one hotel.
 * @param {object} record
 * @param {object} [opts]
 */
export async function liveDeepResearchHotel(record, opts = {}) {
  const researchable =
    opts.researchable || buildFieldRoutingRegistry().researchable;
  const delayMs = opts.delayMs ?? 400;
  const family =
    record.family ||
    familyFromIdentity(recordToAdapterFields(record), record.independent_record_id);

  const fieldsObj = recordToAdapterFields(record);
  let source_attempt_count = 0;
  let research_effort_score = 0;
  let resolution_level_max = 0;
  let source_blocked = false;
  /** @type {object[]} */
  const attempts = [];
  /** @type {object[]} */
  const failures = [];

  // LEVEL 1 — directory / structured official
  let dirSignals = null;
  /** @type {object|null} */
  let deep = null;
  /** @type {object|null} */
  let page = null;
  try {
    if (delayMs) await sleep(Math.min(delayMs, 200));
    source_attempt_count += 1;
    research_effort_score += 1;
    dirSignals = await resolveFamilyDirectorySignals({
      fields: fieldsObj,
      identityKey: record.independent_record_id,
      family,
      lanes: ["address", "phone", "amenities", "description", "coordinates"],
    });
    attempts.push({
      level: 1,
      lane: "A",
      kind: "family_directory",
      ok: true,
      family,
    });
    resolution_level_max = Math.max(resolution_level_max, 1);
  } catch (err) {
    failures.push({
      level: 1,
      kind: "family_directory",
      error: err?.message || String(err),
    });
    attempts.push({ level: 1, lane: "A", kind: "family_directory", ok: false });
  }

  // LEVEL 1b — Hilton GraphQL (free, no credits) for status / open date
  if (String(family).toLowerCase() === "hilton") {
    try {
      const cty = extractHiltonCtyhocn(fieldsObj, record.independent_record_id);
      if (cty) {
        if (delayMs) await sleep(Math.min(delayMs, 200));
        source_attempt_count += 1;
        research_effort_score += 1;
        const obs = await fetchHiltonHotelObservation(
          {
            ...record,
            website: record.website,
            currentBrand: record.brand,
            ctyhocn: cty,
            propertyId: cty,
          },
          { ctyhocn: cty }
        );
        attempts.push({
          level: 1,
          lane: "A",
          kind: "hilton_graphql",
          ok: obs.hotelFound === true,
          ctyhocn: cty,
        });
        if (obs.hotelFound && obs.rawSignals?.openDate) {
          deep = deep || {};
          deep.openDateHint = obs.rawSignals.openDate;
          deep.openDateKind = "actual";
        }
        resolution_level_max = Math.max(resolution_level_max, 1);
      }
    } catch (err) {
      failures.push({ level: 1, kind: "hilton_graphql", error: err?.message || String(err) });
    }
  }

  // LEVEL 1c — IHG: prefer direct official URL fetch below (adapter directory match often empty when URL known)
  // Kept as optional corroboration only when no website on record.
  if (String(family).toLowerCase() === "ihg" && !record.website) {
    attempts.push({
      level: 1,
      lane: "A",
      kind: "ihg_hoteldetail_adapter",
      ok: false,
      reason: "no_website_on_record",
    });
  }

  // LEVEL 2 — official property deep page (skip if IHG adapter already got Available HTML path —
  // IHG adapter does not currently return body text, so we still fetch once for deep signals)
  let ihgAmenitiesText = null;
  const primaryUrl = record.website || null;
  if (primaryUrl) {
    if (delayMs) await sleep(delayMs);
    source_attempt_count += 1;
    research_effort_score += 2;
    page = await fetchOfficialPage(primaryUrl, family, { timeoutMs: opts.timeoutMs });
    attempts.push({
      level: 2,
      lane: "A",
      kind: "official_property_page",
      url: primaryUrl,
      ok: page.ok,
      status: page.status,
      reason: page.reason || null,
    });
    if (!page.ok) {
      source_blocked = /blocked|interstitial|403|429/i.test(String(page.reason || ""));
      failures.push({ level: 2, url: primaryUrl, reason: page.reason, status: page.status });
    } else {
      deep = mergeDeep(deep, extractDeepOfficialPageSignals(page.text, page.url));
      // V1.2 rooms boost — additional patterns when primary extract missed
      if (deep?.rooms == null) {
        const boosted = extractRoomsBoostFromHtml(page.text);
        if (boosted != null) deep = mergeDeep(deep, { rooms: boosted });
      }
      resolution_level_max = Math.max(resolution_level_max, 2);
      if (String(family).toLowerCase() === "ihg") {
        const labels = extractIhgAmenitiesFromHtml(page.text);
        if (labels.length) ihgAmenitiesText = formatIhgAmenitiesText(labels);
      }
    }
  } else {
    failures.push({ level: 2, reason: "no_official_url" });
  }

  // LEVEL 3 — Lane B standalone hotel website if material gaps remain
  let laneBPage = null;
  let laneBDeep = null;
  const needsLaneB = materialGapsNeedLaneB(record, dirSignals, deep);
  if (needsLaneB && page?.ok) {
    const standalone = extractStandaloneWebsiteCandidate(page.text, page.url);
    if (standalone) {
      if (delayMs) await sleep(delayMs);
      source_attempt_count += 1;
      research_effort_score += 3;
      laneBPage = await fetchOfficialPage(standalone, family, { timeoutMs: opts.timeoutMs });
      attempts.push({
        level: 3,
        lane: "B",
        kind: "standalone_hotel_website",
        url: standalone,
        ok: laneBPage.ok,
        status: laneBPage.status,
        reason: laneBPage.reason || null,
      });
      if (laneBPage.ok) {
        laneBDeep = extractDeepOfficialPageSignals(laneBPage.text, laneBPage.url);
        if (laneBDeep?.rooms == null) {
          const boosted = extractRoomsBoostFromHtml(laneBPage.text);
          if (boosted != null) laneBDeep = { ...laneBDeep, rooms: boosted };
        }
        resolution_level_max = Math.max(resolution_level_max, 3);
        // Merge: prefer filling missing from Lane B
        deep = mergeDeep(deep, laneBDeep);
      } else {
        failures.push({
          level: 3,
          url: standalone,
          reason: laneBPage.reason,
          status: laneBPage.status,
        });
      }
    } else {
      attempts.push({
        level: 3,
        lane: "B",
        kind: "standalone_hotel_website",
        ok: false,
        reason: "no_standalone_candidate_on_brand_page",
      });
    }
  }

  const claims = collectLiveClaims(record, dirSignals, deep, {
    url: page?.url || primaryUrl,
    lane: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
    resolution_level: 2,
    ihgAmenitiesText,
  });
  // Re-apply Lane B deep with higher level markers for newly found values
  if (laneBDeep) {
    const bClaims = collectLiveClaims(record, null, laneBDeep, {
      url: laneBPage.url,
      lane: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
      resolution_level: 3,
    });
    for (const [k, v] of bClaims) {
      if (!claims.has(k) || (claims.get(k).resolution_level || 0) < (v.resolution_level || 0)) {
        // only fill gaps or upgrade from deeper ladder for hard fields
        if (!claims.has(k) || MATERIAL_HARD_FIELDS.includes(k)) {
          if (!claims.has(k) || claims.get(k).researched_value == null) claims.set(k, v);
        }
      }
    }
  }

  const field_results = buildFieldResultsFromClaims(record, claims, researchable, {
    source_blocked,
    source_attempt_count,
    research_effort_score,
    resolution_level_max,
  });

  const resolved = field_results.filter((f) => RESOLVED_STATUSES.has(f.resolution_status)).length;
  const laneAHits = field_results.filter(
    (f) => f.source && RESOLVED_STATUSES.has(f.resolution_status) && (f.resolution_level || 0) <= 2
  ).length;
  const laneBHits = field_results.filter(
    (f) => f.resolution_level === 3 && RESOLVED_STATUSES.has(f.resolution_status)
  ).length;

  return {
    version: LIVE_DEEP_VERSION,
    independent_record_id: record.independent_record_id,
    name: record.name,
    family,
    brand: record.brand,
    country: record.country,
    city: record.city,
    website: record.website,
    status: record.status,
    material_pct_before: record.material_pct ?? null,
    core_pct_before: record.core_pct ?? null,
    fields: field_results,
    fields_researched: field_results.length,
    fields_resolved: resolved,
    fields_unresolved: field_results.length - resolved,
    lane_a_resolved_fields: laneAHits,
    lane_b_resolved_fields: laneBHits,
    attempts,
    failures,
    source_attempt_count,
    research_effort_score,
    resolution_level_max,
    source_blocked,
    directory_ok: Boolean(dirSignals),
    page_ok: Boolean(page?.ok),
    lane_b_ok: Boolean(laneBPage?.ok),
    page_html_snippet: page?.ok && page?.text ? String(page.text).slice(0, 180_000) : null,
    legacy_used_as_source: false,
    cvent_used_as_source: false,
    external_cost_usd: 0,
  };
}

function materialGapsNeedLaneB(record, dirSignals, deep) {
  const hasRooms = deep?.rooms != null;
  const hasOp = Boolean(deep?.managementHint);
  const hasCoords =
    (deep?.latitude != null && deep?.longitude != null) || dirSignals?.coordinates?.ok;
  const hasPhone = Boolean(deep?.phone) || dirSignals?.phone?.ok;
  const hasAmenities =
    (deep?.amenitiesMentioned || []).length > 0 || dirSignals?.amenities?.ok;
  // If directory+deep already filled most hard fields, skip Lane B
  const missing = [!hasRooms, !hasOp, !hasCoords, !hasPhone, !hasAmenities].filter(Boolean)
    .length;
  return missing >= 2;
}

function mergeDeep(a, b) {
  if (!a) return b;
  if (!b) return a;
  return {
    ...a,
    rooms: a.rooms ?? b.rooms,
    phone: a.phone || b.phone,
    openDateHint: a.openDateHint || b.openDateHint,
    openDateKind: a.openDateKind || b.openDateKind,
    latitude: a.latitude ?? b.latitude,
    longitude: a.longitude ?? b.longitude,
    amenitiesMentioned: unique([...(a.amenitiesMentioned || []), ...(b.amenitiesMentioned || [])]),
    fbMentioned: a.fbMentioned || b.fbMentioned,
    meetingFacilities: a.meetingFacilities === "Yes" ? "Yes" : b.meetingFacilities,
    spaWellness: a.spaWellness || b.spaWellness,
    poolMentioned: a.poolMentioned || b.poolMentioned,
    managementHint: a.managementHint || b.managementHint,
    ownerHint: a.ownerHint || b.ownerHint,
  };
}

function unique(arr) {
  const s = new Set();
  const out = [];
  for (const x of arr) {
    const k = String(x).toLowerCase();
    if (s.has(k)) continue;
    s.add(k);
    out.push(x);
  }
  return out;
}

export { warmFamilyDirectoryCaches };
