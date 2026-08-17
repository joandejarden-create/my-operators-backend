/**
 * Level 2 parent extractors — High official Address / Phone / Rooms only.
 *
 * Official sources: brand directory cards, property pages, JSON-LD, embedded data.
 * Never OTAs / Google Maps / Mapbox-as-address / Webhound-as-SoT / weak inference.
 */

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import {
  classifyCensusReviewReasons,
  evaluateLevel2Eligibility,
} from "./census-brand-governance.js";
import { extractDeepOfficialPageSignals } from "./clean-census/field-research.js";
import {
  familyFromIdentity,
  resolveDirectoryAddressCandidate,
  resolveDirectoryPhoneCandidate,
  warmFamilyDirectoryCaches,
  applyDeepOfficialPageSignals,
} from "./census-autopilot-family-directory-adapters.js";
import {
  extractOfficialPhoneFromHtml,
  normalizePhoneNumber,
  PHONE_FIELD,
  isForbiddenPhoneSourceUrl,
} from "./census-phone-number-enrichment.js";
import {
  extractRoomsKeysFromOfficialHtml,
  isFalsePositiveRoomCount,
} from "./production-census-rooms-keys-extractor.js";

export const LEVEL_2_EXTRACTOR_VERSION = "census-level-2-parent-extractors-v2-wave-2";

export const LEVEL_2_PARENT_ORDER = Object.freeze([
  "Choice",
  "Marriott",
  "Hilton",
  "IHG",
  "Accor",
  "Wyndham",
  "Preferred",
]);

export const LEVEL_2_SOURCE_TYPES = Object.freeze({
  OFFICIAL_PROPERTY_PAGE: "official_property_page",
  OFFICIAL_BRAND_DIRECTORY: "official_brand_directory",
  OFFICIAL_JSON_LD: "official_json_ld",
  OFFICIAL_EMBEDDED: "official_embedded_data",
  SOURCE_BLOCKED: "source_blocked_level_2",
  SOURCE_INSUFFICIENT: "source_insufficient",
});

const ROOMS_FIELD = "Rooms / Keys";

function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string" && !String(v).trim()) return true;
  return false;
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function normKey(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function cityMatches(a, b) {
  const x = normKey(a);
  const y = normKey(b);
  if (!x || !y) return true; // soft — missing city on directory does not fail High alone
  return x === y || x.includes(y) || y.includes(x);
}

/**
 * Extract street address from official HTML (JSON-LD PostalAddress / streetAddress).
 * @param {string} html
 * @param {string} [url]
 */
export function extractOfficialAddressFromHtml(html, url = "") {
  if (isForbiddenPhoneSourceUrl(url)) {
    return { ok: false, reason: "forbidden_third_party_source", address: null };
  }
  const text = String(html || "");
  const street =
    text.match(/"streetAddress"\s*:\s*"([^"]{5,160})"/i) ||
    text.match(/"addressLine1"\s*:\s*"([^"]{5,160})"/i) ||
    text.match(/itemprop=["']streetAddress["'][^>]*content=["']([^"']{5,160})["']/i) ||
    text.match(/itemprop=["']streetAddress["'][^>]*>([^<]{5,160})</i);
  if (!street) {
    return { ok: false, reason: "no_street_address_in_page", address: null };
  }
  let address = street[1]
    .replace(/\\u0026/g, "&")
    .replace(/\\"/g, '"')
    .replace(/\s+/g, " ")
    .trim();
  if (!isStreetLevelAddress(address)) {
    return { ok: false, reason: "address_not_street_level", address: null };
  }
  const locality = text.match(/"addressLocality"\s*:\s*"([^"]+)"/i);
  const region = text.match(/"addressRegion"\s*:\s*"([^"]+)"/i);
  const country = text.match(/"addressCountry"\s*:\s*"([^"]+)"/i);
  const postal = text.match(/"postalCode"\s*:\s*"([^"]+)"/i);
  return {
    ok: true,
    address,
    city: locality ? locality[1].trim() : null,
    state: region ? region[1].trim() : null,
    country: country ? country[1].trim() : null,
    postal_code: postal ? postal[1].trim() : null,
    source_url: url || null,
    confidence: "High",
    method: "official_json_ld_street_address",
    source_type: LEVEL_2_SOURCE_TYPES.OFFICIAL_JSON_LD,
  };
}

/**
 * Extract High rooms count from official HTML (numberOfRooms / explicit copy).
 * @param {string} html
 * @param {string} [url]
 */
export function extractOfficialRoomsFromHtml(html, url = "") {
  if (isForbiddenPhoneSourceUrl(url)) {
    return { ok: false, reason: "forbidden_third_party_source", rooms: null };
  }
  const fromExtractor = extractRoomsKeysFromOfficialHtml(html, { url });
  const hits = Array.isArray(fromExtractor)
    ? fromExtractor
    : fromExtractor?.hits || (fromExtractor?.count != null ? [fromExtractor] : []);
  const best =
    hits.find(
      (h) =>
        h &&
        !h.rejected &&
        h.confidence === "High" &&
        Number.isFinite(h.count) &&
        h.count >= 20 &&
        h.count <= 2000
    ) || null;
  if (best) {
    return {
      ok: true,
      rooms: best.count,
      source_url: url || null,
      confidence: "High",
      method: best.method || "official_rooms_keys_extractor",
      source_type: LEVEL_2_SOURCE_TYPES.OFFICIAL_PROPERTY_PAGE,
      note: best.note || null,
    };
  }
  const deep = extractDeepOfficialPageSignals(html, url);
  if (deep.rooms == null) {
    return { ok: false, reason: "no_official_rooms_in_page", rooms: null };
  }
  const n = Number(deep.rooms);
  if (!Number.isFinite(n) || n < 20 || n > 2000) {
    return { ok: false, reason: "rooms_out_of_range", rooms: null };
  }
  // VIC IHG false positive: JS \x22rooms escape often yields rooms=22
  if (
    isFalsePositiveRoomCount(html, n, "official_page_number_of_rooms") ||
    (n === 22 && /ihg\.com/i.test(String(url || "")))
  ) {
    return {
      ok: false,
      reason: "known_vic_false_positive_22rooms_js_escape",
      rooms: null,
    };
  }
  return {
    ok: true,
    rooms: n,
    source_url: url || null,
    confidence: "High",
    method: "official_page_number_of_rooms",
    source_type: LEVEL_2_SOURCE_TYPES.OFFICIAL_PROPERTY_PAGE,
  };
}

function parentMatchesFilter(family, parentFilter) {
  if (!parentFilter) return true;
  const f = String(family || "").toLowerCase();
  const p = String(parentFilter || "").toLowerCase();
  if (!p) return true;
  return f === p || f.includes(p) || p.includes(f);
}

/**
 * Classify Level 2 High patches for one Census record.
 * @param {object} record
 * @param {{
 *   parentCompany?: string|null,
 *   pageHtml?: string|null,
 *   pageUrl?: string|null,
 *   pageBlocked?: boolean,
 *   directoryWarmed?: boolean,
 * }} [opts]
 */
export async function classifyLevel2Extraction(record, opts = {}) {
  const fields = record?.fields || {};
  const identityKey = fields[MAP_FIRST_PASS.identityKey] || fields["Property Identity Key"] || "";
  const family = familyFromIdentity(fields, identityKey);
  const review = classifyCensusReviewReasons({ fields }, opts);
  const held = fields[MAP_FIRST_PASS.humanReview] === true;
  const clean = evaluateCleanCorePass(record, {
    skipBrandSourceOfTruth: opts.skipBrandSourceOfTruth === true,
    continentFieldExists: opts.continentFieldExists,
    activeIndex: opts.activeIndex,
  });

  /** @type {object} */
  const base = {
    record_id: record.id,
    identity_key: identityKey,
    property_name: fields[MAP_FIRST_PASS.propertyName] || fields["Property Name"],
    family,
    country: fields[MAP_FIRST_PASS.country] || fields.Country,
    city: fields[MAP_FIRST_PASS.city] || fields.City,
    clean_core: clean.pass,
    held,
    review_governance_only: Boolean(review.governance_only),
    data_quality_review: Boolean(review.data_quality_review_required),
  };

  // Data-quality HR / dirty governance blocks Level 2; governance-only does not
  if (review.data_quality_review_required) {
    return {
      ...base,
      action: "blocked",
      reason: "data_quality_review_required",
      proposals: [],
    };
  }
  if (held && !review.governance_only) {
    return { ...base, action: "blocked", reason: "human_review_required", proposals: [] };
  }

  const level2Elig = evaluateLevel2Eligibility(record, {
    ...opts,
    cleanCoreResult: clean,
    allowAutofillableCleanCoreGaps: opts.allowAutofillableCleanCoreGaps === true,
  });
  if (!level2Elig.eligible && !level2Elig.clean_core) {
    const autofillOnly =
      opts.allowAutofillableCleanCoreGaps === true &&
      (clean.missing || []).every((m) =>
        ["Canonical Property Name", "Source Family", "Data Confidence Tier"].includes(m)
      ) &&
      (clean.blockers || []).every(
        (b) =>
          ["canonical_blank_can_autofill", "canonical_dirty_can_clean"].includes(b) ||
          (review.governance_only &&
            (b === "human_review_required" || String(b).startsWith("canonical_steward")))
      );
    if (!autofillOnly) {
      return {
        ...base,
        action: "blocked",
        reason: level2Elig.reasons[0] || "clean_core_not_pass",
        proposals: [],
        clean,
      };
    }
    base.clean_core_soft_pass = true;
  } else if (!clean.pass && level2Elig.clean_core) {
    base.clean_core_soft_pass = true;
  }
  if (!parentMatchesFilter(family, opts.parentCompany)) {
    return { ...base, action: "skipped", reason: "parent_filter", proposals: [] };
  }

  const proposals = [];
  const notes = [];
  // pageBlocked must NOT wipe directory High candidates — only marks page-dependent notes
  if (opts.pageBlocked) {
    notes.push("official_page_bot_blocked");
  }

  // Soft Clean Core autofill fields — attached later only when a Level 2 High field writes
  const softAutofillPatch = {};
  if (base.clean_core_soft_pass) {
    const propName = String(fields[MAP_FIRST_PASS.propertyName] || fields["Property Name"] || "").trim();
    if (!String(fields["Canonical Property Name"] || "").trim() && propName) {
      softAutofillPatch["Canonical Property Name"] = propName;
    }
    const brandFamily = String(fields["Brand Family"] || "").trim();
    if (!String(fields["Family / Source Family"] || "").trim() && brandFamily) {
      softAutofillPatch["Family / Source Family"] = brandFamily;
    }
    if (!String(fields["Data Confidence Tier"] || "").trim()) {
      softAutofillPatch["Data Confidence Tier"] = "High";
    }
  }

  // --- Address ---
  const existingAddress = String(fields[MAP_FIRST_PASS.address] || fields.Address || "").trim();
  if (!isStreetLevelAddress(existingAddress)) {
    let addrHit = await resolveDirectoryAddressCandidate({
      fields,
      identityKey,
      family,
      skipVic: opts.skipVic !== false,
    });
    if (
      (!addrHit.ok || !isStreetLevelAddress(addrHit.address)) &&
      opts.pageHtml
    ) {
      const fromPage = extractOfficialAddressFromHtml(
        opts.pageHtml,
        opts.pageUrl || fields[MAP_FIRST_PASS.officialUrl] || fields["Official Property URL"] || ""
      );
      if (fromPage.ok) {
        addrHit = {
          ok: true,
          address: fromPage.address,
          source_url: fromPage.source_url,
          confidence: "High",
          method: fromPage.method,
          source_type: fromPage.source_type,
          city: fromPage.city,
        };
      }
    }
    if (addrHit.ok && isStreetLevelAddress(addrHit.address)) {
      const censusCity = fields[MAP_FIRST_PASS.city] || fields.City;
      if (addrHit.city && censusCity && !cityMatches(addrHit.city, censusCity)) {
        proposals.push({
          field: "Address",
          action: "steward_conflict",
          reason: "address_city_mismatch",
          existing_city: censusCity,
          directory_city: addrHit.city,
          candidate: addrHit.address,
        });
      } else if (existingAddress && existingAddress !== addrHit.address) {
        proposals.push({
          field: "Address",
          action: "steward_conflict",
          reason: "address_conflict_existing",
          existing: existingAddress,
          candidate: addrHit.address,
        });
      } else if (!existingAddress) {
        const sourceUrl = addrHit.source_url || null;
        if (!sourceUrl) {
          notes.push("address_missing_source_url");
        } else {
          const patch = {
            Address: addrHit.address,
            "Address Confidence": "High",
            "Address Source URL": sourceUrl,
            "Last Reviewed Date": todayIsoDate(),
          };
          if (addrHit.state && isBlank(fields["State / Region"])) {
            patch["State / Region"] = addrHit.state;
          }
          proposals.push({
            field: "Address",
            action: "propose_high_write",
            confidence: "High",
            patch,
            method: addrHit.method,
            source_type: addrHit.source_type,
            source_url: sourceUrl,
          });
        }
      }
    } else {
      notes.push(addrHit.reason || "address_source_insufficient");
    }
  }

  // --- Phone ---
  const existingPhone = normalizePhoneNumber(fields[PHONE_FIELD] || fields.Phone || "");
  if (!existingPhone) {
    let phoneHit = await resolveDirectoryPhoneCandidate({ fields, identityKey, family });
    if ((!phoneHit.ok || !phoneHit.phone) && opts.pageHtml) {
      const fromPage = extractOfficialPhoneFromHtml(
        opts.pageHtml,
        opts.pageUrl || fields[MAP_FIRST_PASS.officialUrl] || fields["Official Property URL"] || ""
      );
      if (fromPage.ok) {
        phoneHit = {
          ok: true,
          phone: fromPage.phone,
          source_url: fromPage.source_url,
          confidence: "High",
          method: fromPage.method || "official_property_page_phone",
          source_type: fromPage.source_type,
        };
      }
    }
    if (phoneHit.ok && phoneHit.phone) {
      const normalized = normalizePhoneNumber(phoneHit.phone);
      if (normalized && phoneHit.source_url && !isForbiddenPhoneSourceUrl(phoneHit.source_url)) {
        proposals.push({
          field: PHONE_FIELD,
          action: "propose_high_write",
          confidence: "High",
          patch: {
            [PHONE_FIELD]: normalized,
            "Last Reviewed Date": todayIsoDate(),
          },
          method: phoneHit.method,
          source_type: phoneHit.source_type,
          source_url: phoneHit.source_url,
        });
      } else {
        notes.push(normalized ? "phone_missing_official_source_url" : "phone_normalize_failed");
      }
    } else {
      notes.push(phoneHit.reason || "phone_source_insufficient");
    }
  }

  // --- Rooms ---
  const existingRooms = fields[ROOMS_FIELD] ?? fields.Rooms ?? fields.Keys;
  if (isBlank(existingRooms) && opts.pageHtml) {
    const roomsHit = extractOfficialRoomsFromHtml(
      opts.pageHtml,
      opts.pageUrl || fields[MAP_FIRST_PASS.officialUrl] || fields["Official Property URL"] || ""
    );
    if (roomsHit.ok) {
      proposals.push({
        field: ROOMS_FIELD,
        action: "propose_high_write",
        confidence: "High",
        patch: {
          [ROOMS_FIELD]: roomsHit.rooms,
          "Rooms Confidence": "High",
          "Rooms Source URL": roomsHit.source_url,
          "Rooms Source Type": roomsHit.source_type,
          "Rooms Reviewed Date": todayIsoDate(),
        },
        method: roomsHit.method,
        source_type: roomsHit.source_type,
        source_url: roomsHit.source_url,
      });
    } else {
      notes.push(roomsHit.reason || "rooms_source_insufficient");
    }
  } else if (!isBlank(existingRooms) && opts.pageHtml) {
    const roomsHit = extractOfficialRoomsFromHtml(
      opts.pageHtml,
      opts.pageUrl || fields[MAP_FIRST_PASS.officialUrl] || ""
    );
    if (roomsHit.ok && Number(roomsHit.rooms) !== Number(existingRooms)) {
      proposals.push({
        field: ROOMS_FIELD,
        action: "steward_conflict",
        reason: "rooms_conflict_existing",
        existing: existingRooms,
        candidate: roomsHit.rooms,
      });
    }
  }

  const high = proposals.filter((p) => p.action === "propose_high_write");
  const steward = proposals.filter((p) => p.action === "steward_conflict");

  // Attach soft Clean Core autofill only when at least one Level 2 field (Address/Phone/Rooms) writes
  const level2FieldWrite = high.some((p) =>
    ["Address", PHONE_FIELD, "Phone", ROOMS_FIELD].includes(p.field)
  );
  if (level2FieldWrite && Object.keys(softAutofillPatch).length) {
    high.push({
      field: "Clean Core Autofill",
      action: "propose_high_write",
      confidence: "High",
      patch: softAutofillPatch,
      method: "clean_core_prelude_autofill",
      source_type: "internal_identity_autofill",
    });
  }

  // Marriott / page-blocked with zero High writes → classify source_blocked_level_2
  if (
    !high.length &&
    !steward.length &&
    (opts.pageBlocked ||
      notes.includes("marriott_sitemap_metadata_lacks_address") ||
      notes.some((n) => /marriott_sitemap_metadata_lacks/.test(n)))
  ) {
    return {
      ...base,
      action: "source_blocked_level_2",
      reason: opts.pageBlocked
        ? "official_page_bot_blocked"
        : "marriott_official_metadata_insufficient",
      proposals: [],
      steward_conflicts: [],
      notes,
      classification: LEVEL_2_SOURCE_TYPES.SOURCE_BLOCKED,
    };
  }

  return {
    ...base,
    action: high.length ? "propose" : steward.length ? "steward" : "source_insufficient",
    proposals: high,
    steward_conflicts: steward,
    notes,
    deep_page: opts.pageHtml
      ? applyDeepOfficialPageSignals(
          opts.pageHtml,
          opts.pageUrl || fields[MAP_FIRST_PASS.officialUrl] || ""
        )
      : null,
  };
}

/**
 * Collapse per-field proposals into one Autopilot patch per record.
 * @param {object} classified
 */
export function mergeLevel2ProposalsToPatch(classified) {
  const patch = {};
  const sources = [];
  for (const p of classified.proposals || []) {
    if (p.action !== "propose_high_write" || !p.patch) continue;
    Object.assign(patch, p.patch);
    sources.push({
      field: p.field,
      method: p.method,
      source_url: p.source_url,
      source_type: p.source_type,
    });
  }
  return { patch, sources };
}

export { warmFamilyDirectoryCaches, LEVEL_2_PARENT_ORDER as PARENT_ORDER };
