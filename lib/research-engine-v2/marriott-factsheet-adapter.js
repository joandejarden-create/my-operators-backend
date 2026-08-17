/**
 * Marriott factsheet + linked hotel microsite + rooms adapters.
 * Official sources only; Webhound may discover URLs but never acts as SoT.
 */

import {
  extractOfficialAddressFromHtml,
  extractOfficialRoomsFromHtml,
} from "./census-level-2-parent-extractors.js";
import {
  extractOfficialPhoneFromHtml,
  isForbiddenPhoneSourceUrl,
} from "./census-phone-number-enrichment.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import {
  isFalsePositiveRoomCount,
} from "./production-census-rooms-keys-extractor.js";
import { fetchMarriottOfficialPage } from "./marriott-official-metadata-adapter.js";

export const MARRIOTT_FACTSHEET_ADAPTER_VERSION = "marriott-factsheet-adapter-v1";
export const MARRIOTT_LINKED_SITE_ADAPTER_VERSION =
  "marriott-linked-hotel-site-adapter-v1";
export const MARRIOTT_ROOMS_ADAPTER_VERSION = "marriott-rooms-source-adapter-v1";

/**
 * Brand-prefix hints for Marriott DAM factsheet paths (Webhound-confirmed template).
 * Incomplete by design — unknown brands return null; never invent paths.
 */
export const MARRIOTT_DAM_BRAND_PREFIX = Object.freeze({
  fairfield: "fi",
  "fairfield by marriott": "fi",
  "jw marriott": "jw",
  sheraton: "si",
  westin: "wi",
  "w hotels": "wh",
  w: "wh",
  "st. regis": "xr",
  "st regis": "xr",
  "the ritz-carlton": "rz",
  ritz: "rz",
  "courtyard": "cy",
  "residence inn": "ri",
  "four points": "fp",
  aloft: "al",
  element: "el",
  moxy: "ox",
  "autograph collection": "ak",
  "tribute portfolio": "tp",
  "design hotels": "de",
  "marriott hotels": "mc",
  marriott: "mc",
});

/**
 * Build candidate DAM factsheet URL when brand prefix + MARSHA + asset id known.
 * Webhound finding — URL must still be fetched and revalidated before Census write.
 *
 * @param {{ marsha: string, brandPrefix?: string, brand?: string, region?: string, assetId?: string }} input
 */
export function buildMarriottDamFactsheetUrlCandidate(input = {}) {
  const marsha = String(input.marsha || "")
    .trim()
    .toLowerCase();
  if (!/^[a-z0-9]{4,6}$/i.test(marsha)) {
    return { ok: false, reason: "invalid_marsha", webhound_as_sot: false };
  }
  let prefix = String(input.brandPrefix || "").trim().toLowerCase();
  if (!prefix && input.brand) {
    const brandKey = String(input.brand).trim().toLowerCase();
    prefix =
      MARRIOTT_DAM_BRAND_PREFIX[brandKey] ||
      Object.entries(MARRIOTT_DAM_BRAND_PREFIX).find(([k]) =>
        brandKey.includes(k)
      )?.[1] ||
      "";
  }
  if (!prefix) {
    return { ok: false, reason: "unknown_brand_prefix", webhound_as_sot: false };
  }
  const region = String(input.region || "cala").trim().toLowerCase() || "cala";
  const assetId = String(input.assetId || "").trim();
  if (!assetId) {
    return {
      ok: false,
      reason: "asset_id_required",
      webhound_as_sot: false,
      url_template: `https://www.marriott.com/content/dam/marriott-digital/${prefix}/${region}/hws/${marsha[0]}/${marsha}/en_us/document/assets/${prefix}-${marsha}-fact-sheet-{id}.pdf`,
    };
  }
  const url = `https://www.marriott.com/content/dam/marriott-digital/${prefix}/${region}/hws/${marsha[0]}/${marsha}/en_us/document/assets/${prefix}-${marsha}-fact-sheet-${assetId}.pdf`;
  return {
    ok: true,
    url,
    webhound_as_sot: false,
    requires_underlying_revalidation: true,
    source_type: "official_factsheet",
  };
}

/** Confirmed working DAM example from Webhound (SJULU) — evidence only, not SoT. */
export const WEBHOUND_CONFIRMED_DAM_EXAMPLE = Object.freeze({
  marsha: "SJULU",
  property: "Fairfield by Marriott Luquillo Beach",
  url: "https://www.marriott.com/content/dam/marriott-digital/fi/cala/hws/s/sjulu/en_us/document/assets/fi-sjulu-fact-sheet-23971.pdf",
  claimed_rooms: 104,
  webhound_as_sot: false,
});

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function propertyNameMatch(a, b) {
  const x = norm(a);
  const y = norm(b);
  if (!x || !y) return false;
  return x === y || x.includes(y) || y.includes(x);
}

/**
 * Reject meeting-room / residence false positives for rooms.
 */
export function validateMarriottRoomsCandidate(input = {}) {
  const count = Number(input.count);
  const evidence = String(input.evidence || input.context || "");
  const sourceUrl = String(input.source_url || "");
  if (!Number.isFinite(count) || count < 10 || count > 5000) {
    return { ok: false, reason: "rooms_count_out_of_range" };
  }
  if (!sourceUrl || isForbiddenPhoneSourceUrl(sourceUrl)) {
    return { ok: false, reason: "missing_or_forbidden_source_url" };
  }
  if (isFalsePositiveRoomCount(evidence, count, "official_rooms")) {
    return { ok: false, reason: "false_positive_room_count" };
  }
  if (
    /\bmeeting\s*rooms?\b|\bbanquet\b|\bballroom\b|\bresidences?\s+only\b|\bkeys\s+tbd\b/i.test(
      evidence
    ) &&
    !/\bguest\s*rooms?\b|\bguestrooms\b|\bkeys\b|\bhotel\s*rooms?\b/i.test(evidence)
  ) {
    return { ok: false, reason: "meeting_or_residence_confusion" };
  }
  // Also reject when the only room mention is meeting rooms
  if (/\bmeeting\s*rooms?\b/i.test(evidence) && !/\bguest\s*rooms?\b|\bguestrooms\b/i.test(evidence)) {
    return { ok: false, reason: "meeting_or_residence_confusion" };
  }
  if (input.property_name && input.source_property_name) {
    if (!propertyNameMatch(input.property_name, input.source_property_name)) {
      return { ok: false, reason: "property_name_mismatch" };
    }
  }
  return {
    ok: true,
    count,
    source_url: sourceUrl,
    source_type: input.source_type || "official_factsheet",
    confidence: "High",
    evidence_tier: input.evidence_tier || "official_factsheet",
  };
}

/**
 * Extract from an official linked hotel microsite HTML.
 */
export async function extractFromLinkedHotelSite(url, opts = {}) {
  if (!url || isForbiddenPhoneSourceUrl(url)) {
    return { ok: false, reason: "forbidden_or_blank", webhound_as_sot: false };
  }
  if (/marriott\.com/i.test(url) && opts.requireNonMarriott === true) {
    return { ok: false, reason: "expected_non_marriott_microsite" };
  }
  const fetched = await fetchMarriottOfficialPage(url, opts);
  if (!fetched.ok) {
    return {
      ok: false,
      reason: fetched.reason,
      blocked: fetched.blocked,
      webhound_as_sot: false,
    };
  }
  const html = fetched.html || "";
  const addr = extractOfficialAddressFromHtml(html, url);
  const phone = extractOfficialPhoneFromHtml(html, url);
  const rooms = extractOfficialRoomsFromHtml(html, url);
  const roomCount = rooms.rooms ?? rooms.count;
  const roomsVal =
    rooms.ok && roomCount != null
      ? validateMarriottRoomsCandidate({
          count: roomCount,
          evidence: rooms.note || html.slice(0, 500),
          source_url: url,
          source_type: "official_linked_hotel_website",
          evidence_tier: "official_hotel_microsite",
          property_name: opts.propertyName,
          source_property_name: opts.propertyName,
        })
      : { ok: false };

  return {
    ok: Boolean(addr.ok || phone.ok || roomsVal.ok),
    webhound_as_sot: false,
    version: MARRIOTT_LINKED_SITE_ADAPTER_VERSION,
    source_url: url,
    address: addr.ok && isStreetLevelAddress(addr.address) ? addr : null,
    phone: phone.ok ? phone : null,
    rooms: roomsVal.ok ? roomsVal : null,
  };
}

/**
 * Extract rooms/address/phone hints from factsheet HTML/text/PDF-extracted text.
 * Caller supplies text (PDF text extraction happens outside).
 */
export function extractFromFactsheetText(text, opts = {}) {
  const sourceUrl = String(opts.sourceUrl || "").trim();
  if (!sourceUrl || isForbiddenPhoneSourceUrl(sourceUrl)) {
    return { ok: false, reason: "missing_or_forbidden_source_url", webhound_as_sot: false };
  }
  const body = String(text || "");
  const roomsMatch =
    body.match(
      /\b(?:boasts|features|offers|with)\s+(\d{2,4})\s+generously\s+sized\s+rooms?\b/i
    ) ||
    body.match(
      /\b(?:boasts|features|offers)\s+(\d{2,4})\s+(?:guest\s*)?rooms?\b/i
    ) ||
    body.match(
      /\b(\d{2,4})\s*(?:guest\s*)?(?:rooms?|keys|guestrooms|keys\/rooms)\b/i
    ) ||
    body.match(
      /\b(?:rooms?|keys|guestrooms)\s*[:\-]?\s*(\d{2,4})\b/i
    );
  let rooms = null;
  if (roomsMatch) {
    const validated = validateMarriottRoomsCandidate({
      count: Number(roomsMatch[1]),
      evidence: roomsMatch[0],
      source_url: sourceUrl,
      source_type: "official_factsheet",
      evidence_tier: "official_factsheet",
      property_name: opts.propertyName,
      source_property_name: opts.propertyName,
    });
    if (validated.ok) rooms = validated;
  }

  // Reuse HTML extractors on factsheet HTML when available
  let addr = extractOfficialAddressFromHtml(body, sourceUrl);
  let phone = extractOfficialPhoneFromHtml(body, sourceUrl);

  // Marriott DAM factsheet layout: "110 Seaside Drive | Luquillo, PR 00773"
  if (!addr.ok || !isStreetLevelAddress(addr.address)) {
    const pipeAddr = body.match(
      /(\d{1,5}\s+[A-Za-z0-9][A-Za-z0-9 .'#\-]{3,60})\s*\|\s*([A-Za-z][A-Za-z .'\-]{2,40},\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?)/
    );
    if (pipeAddr) {
      const candidate = `${pipeAddr[1].trim()}, ${pipeAddr[2].trim()}`;
      if (isStreetLevelAddress(candidate)) {
        addr = {
          ok: true,
          address: candidate,
          source_url: sourceUrl,
          confidence: "High",
        };
      }
    }
  }

  // Dotted NANP phones common on Marriott factsheets: 787.657.0000
  if (!phone.ok) {
    const dotted = body.match(
      /\b(\+?\d{1,3}[\s.-]?)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}\b/
    );
    if (dotted) {
      const normalized = String(dotted[0]).replace(/[^\d+]/g, "");
      if (normalized.replace(/\D/g, "").length >= 10) {
        phone = {
          ok: true,
          phone: dotted[0].trim(),
          source_url: sourceUrl,
          source_type: "official_factsheet",
          confidence: "High",
        };
      }
    }
  }

  return {
    ok: Boolean(rooms || (addr.ok && isStreetLevelAddress(addr.address)) || phone.ok),
    webhound_as_sot: false,
    version: MARRIOTT_FACTSHEET_ADAPTER_VERSION,
    source_url: sourceUrl,
    rooms,
    address: addr.ok && isStreetLevelAddress(addr.address) ? addr : null,
    phone: phone.ok ? phone : null,
  };
}

/**
 * Unified rooms extraction across official Marriott sources.
 */
export async function extractMarriottRooms(record, opts = {}) {
  const f = record?.fields || {};
  const propertyName = f["Property Name"] || f["Canonical Property Name"];
  const tried = [];

  if (opts.pageHtml && opts.pageUrl) {
    const fromPage = extractOfficialRoomsFromHtml(opts.pageHtml, opts.pageUrl);
    const roomCount = fromPage.rooms ?? fromPage.count;
    tried.push({ source: "official_property_page", ok: fromPage.ok });
    if (fromPage.ok && roomCount != null) {
      const v = validateMarriottRoomsCandidate({
        count: roomCount,
        evidence: fromPage.note || "",
        source_url: opts.pageUrl,
        source_type: "official_property_page",
        evidence_tier: "official_marriott_html",
        property_name: propertyName,
        source_property_name: propertyName,
      });
      if (v.ok) {
        return { ok: true, rooms: v, tried, webhound_as_sot: false, version: MARRIOTT_ROOMS_ADAPTER_VERSION };
      }
    }
  }

  for (const url of opts.linkedSiteUrls || []) {
    const hit = await extractFromLinkedHotelSite(url, {
      propertyName,
      requireNonMarriott: false,
    });
    tried.push({ source: "linked_hotel_site", url, ok: hit.ok });
    if (hit.rooms?.ok) {
      return {
        ok: true,
        rooms: hit.rooms,
        tried,
        webhound_as_sot: false,
        version: MARRIOTT_ROOMS_ADAPTER_VERSION,
      };
    }
  }

  for (const fs of opts.factsheets || []) {
    const hit = extractFromFactsheetText(fs.text || "", {
      sourceUrl: fs.url,
      propertyName,
    });
    tried.push({ source: "factsheet", url: fs.url, ok: hit.ok });
    if (hit.rooms?.ok) {
      return {
        ok: true,
        rooms: hit.rooms,
        tried,
        webhound_as_sot: false,
        version: MARRIOTT_ROOMS_ADAPTER_VERSION,
      };
    }
  }

  return {
    ok: false,
    reason: "rooms_source_insufficient",
    tried,
    webhound_as_sot: false,
    version: MARRIOTT_ROOMS_ADAPTER_VERSION,
  };
}
