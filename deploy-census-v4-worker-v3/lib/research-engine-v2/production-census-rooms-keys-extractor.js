/**
 * Rooms / Keys extraction from official HTML + claim text.
 * Never invent counts. Reject booking-widget / escape-sequence false positives.
 * Mixed-use / residences / villas / apartments → hold or hotel-only split.
 */

export const ROOMS_EXTRACTOR_VERSION = "production-census-rooms-keys-extractor-v1";

const MIXED_USE_RE =
  /\b(residences?|villas?|apartments?|vacation ownership|residential units?|condo(?:minium)?s?|fractional|total units?|masterplan|pipeline)\b/i;

const HOTEL_ROOM_CONTEXT_RE =
  /\b(hotel rooms?|guest rooms?|guestrooms?|rooms? and suites?|keys?|room count|number of rooms)\b/i;

/** Patterns that produced VIC false positive "22" from \x22rooms JS escapes. */
const FALSE_POSITIVE_ESCAPE_RE = /\\x22rooms\\x22|\\u0022rooms\\u0022|"rooms"\s*:\s*\{\s*"Rooms"/i;

/**
 * @typedef {{
 *   count: number,
 *   method: string,
 *   confidence: 'High'|'Medium'|'Low'|'Hold',
 *   hotel_only: boolean,
 *   mixed_use_risk: boolean,
 *   note?: string,
 *   rejected?: boolean,
 *   reject_reason?: string
 * }} RoomHit
 */

function cleanText(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function parseCount(raw) {
  const n = Number(String(raw).replace(/,/g, ""));
  if (!Number.isFinite(n) || !Number.isInteger(n)) return null;
  if (n < 5 || n > 5000) return null; // sanity band for hotel rooms
  return n;
}

/**
 * Detect mixed-use / non-hotel unit language near a count.
 * @param {string} text
 * @param {number} [index]
 */
export function assessMixedUseRisk(text, index = 0) {
  const window = String(text || "").slice(Math.max(0, index - 120), index + 160);
  const mixed = MIXED_USE_RE.test(window);
  const hotelCtx = HOTEL_ROOM_CONTEXT_RE.test(window) || /\bhotel\b/i.test(window);
  return {
    mixed_use_risk: mixed,
    hotel_context: hotelCtx,
    window: cleanText(window).slice(0, 200),
  };
}

/**
 * Parse "80 hotel rooms and 40 residences" style splits.
 * @param {string} text
 * @returns {RoomHit|null}
 */
export function extractHotelOnlySplit(text) {
  const t = String(text || "");
  const m =
    t.match(
      /(\d{1,4})\s*(?:hotel\s+)?(?:guest\s+)?rooms?\s+(?:and|&)\s+(\d{1,4})\s+(?:branded\s+)?residences?/i
    ) ||
    t.match(
      /(\d{1,4})\s*(?:hotel\s+)?(?:guest\s+)?rooms?[^\d]{0,40}?(\d{1,4})\s+(?:branded\s+)?residences?/i
    );
  if (!m) return null;
  const hotel = parseCount(m[1]);
  const residences = parseCount(m[2]);
  if (hotel == null || residences == null) return null;
  return {
    count: hotel,
    method: "hotel_rooms_plus_residences_split",
    confidence: "High",
    hotel_only: true,
    mixed_use_risk: true,
    note: `Hotel rooms ${hotel} stated separately from ${residences} residences`,
  };
}

/**
 * Reject known false-positive patterns (VIC "22" from JS \x22rooms).
 * @param {string} htmlOrText
 * @param {number} count
 * @param {string} method
 */
export function isFalsePositiveRoomCount(htmlOrText, count, method) {
  const t = String(htmlOrText || "");
  if (FALSE_POSITIVE_ESCAPE_RE.test(t) && count === 22 && /rooms?\b/i.test(method)) {
    return true;
  }
  // Booking widget max rooms (usually 1–20)
  if (
    /"max"\s*:\s*20[\s\S]{0,80}"rooms"/i.test(t) &&
    count <= 22 &&
    method.includes("loose")
  ) {
    return true;
  }
  // Bare digit glued to rooms without separator in minified JS / media filenames
  if (new RegExp(`${count}rooms`, "i").test(t) && !new RegExp(`${count}\\s+rooms`, "i").test(t)) {
    return true;
  }
  if (new RegExp(`rooms${count}`, "i").test(t) && !new RegExp(`rooms\\s*[:\\-]?\\s*${count}\\b`, "i").test(t)) {
    return true;
  }
  // Choice.com pages often expose a sitewide default numberOfRooms=25 — not property-specific
  if (
    count === 25 &&
    /choicehotels\.com/i.test(t + String(method || "")) &&
    !/\b(25\s+guest rooms|25\s+rooms and suites|25-room hotel)\b/i.test(t)
  ) {
    return true;
  }
  return false;
}

/**
 * Extract room/key counts from official HTML.
 * @param {string} html
 * @param {{ url?: string, propertyName?: string }} [opts]
 */
export function extractRoomsKeysFromOfficialHtml(html, opts = {}) {
  const text = String(html || "");
  /** @type {RoomHit[]} */
  const hits = [];
  const patterns_matched = [];

  // 1) Hotel+residences split (prefer hotel-only)
  const split = extractHotelOnlySplit(text);
  if (split) {
    hits.push(split);
    patterns_matched.push(split.method);
  }

  // 2) JSON-LD / schema numberOfRooms (number or numeric string; skip empty)
  const jsonLdRooms = [
    ...text.matchAll(/"numberOfRooms"\s*:\s*(\d{1,4})\b/gi),
    ...text.matchAll(/"numberOfRooms"\s*:\s*"(\d{1,4})"/gi),
    ...text.matchAll(/numberOfRooms["'\s:=]+(\d{1,4})/gi),
  ];
  for (const m of jsonLdRooms) {
    const count = parseCount(m[1]);
    if (count == null) continue;
    const risk = assessMixedUseRisk(text, m.index || 0);
    hits.push({
      count,
      method: "json_ld_numberOfRooms",
      confidence: risk.mixed_use_risk ? "Hold" : "High",
      hotel_only: !risk.mixed_use_risk,
      mixed_use_risk: risk.mixed_use_risk,
      note: risk.mixed_use_risk ? "numberOfRooms near mixed-use language" : undefined,
      rejected: risk.mixed_use_risk,
      reject_reason: risk.mixed_use_risk ? "mixed_use_ambiguity" : undefined,
    });
    patterns_matched.push("json_ld_numberOfRooms");
  }

  // 3) Explicit official phrases
  const phrasePatterns = [
    [/(\d{1,4})\s*-\s*room\s+hotel\b/i, "phrase_N_room_hotel", "High"],
    [/(\d{1,4})\s*room\s+hotel\b/i, "phrase_N_room_hotel", "High"],
    [/\bhotel\s+with\s+(\d{1,4})\s+(?:guest\s+)?rooms?\b/i, "phrase_hotel_with_N_rooms", "High"],
    [/\boffers\s+(\d{1,4})\s+(?:guest\s+)?rooms?\b/i, "phrase_offers_N_rooms", "High"],
    [/\bfeatures\s+(\d{1,4})\s+(?:guest\s+)?rooms?\b/i, "phrase_features_N_rooms", "High"],
    [/\b(?:number of rooms|room count)\s*[:\s]+\s*(\d{1,4})\b/i, "phrase_number_of_rooms", "High"],
    [/(\d{1,4})\s+guest rooms\b/i, "phrase_N_guest_rooms", "High"],
    [/(\d{1,4})\s+rooms and suites\b/i, "phrase_N_rooms_and_suites", "High"],
    [/(\d{1,4})\s+keys\b/i, "phrase_N_keys", "Medium"],
    [/(\d{1,4})\s*-\s*key(?:s)?\b/i, "phrase_N_key_hyphen", "Medium"],
    [/(\d{1,4})\s+units\b/i, "phrase_N_units", "Hold"],
  ];

  for (const [rx, method, baseConf] of phrasePatterns) {
    const m = text.match(rx);
    if (!m) continue;
    const count = parseCount(m[1]);
    if (count == null) continue;
    if (isFalsePositiveRoomCount(text, count, method)) continue;
    const risk = assessMixedUseRisk(text, m.index || 0);
    let confidence = /** @type {'High'|'Medium'|'Low'|'Hold'} */ (baseConf);
    let rejected = false;
    let reject_reason;
    let note;

    if (method === "phrase_N_units") {
      confidence = "Hold";
      rejected = true;
      reject_reason = "units_ambiguity";
      note = "Source says units — may include non-hotel inventory";
    } else if (/\bincluding residences|including villas|total keys across\b/i.test(m[0])) {
      confidence = "Hold";
      rejected = true;
      reject_reason = "includes_residences_or_multi_asset";
    } else if (risk.mixed_use_risk && !/hotel rooms?/i.test(m[0])) {
      confidence = "Hold";
      rejected = true;
      reject_reason = "mixed_use_ambiguity";
      note = risk.window;
    } else if (/\bplanned|pipeline|proposed|under development\b/i.test(risk.window)) {
      confidence = "Hold";
      rejected = true;
      reject_reason = "pipeline_or_planned_count";
    }

    hits.push({
      count,
      method,
      confidence,
      hotel_only: !risk.mixed_use_risk || /hotel rooms?/i.test(m[0]),
      mixed_use_risk: risk.mixed_use_risk,
      note,
      rejected,
      reject_reason,
    });
    patterns_matched.push(method);
  }

  void opts;
  return { hits, patterns_matched: [...new Set(patterns_matched)] };
}

/**
 * Validate a VIC / external rooms claim before considering it.
 * @param {{ value: unknown, confidence?: string, evidence_url?: string, source?: string }} claim
 * @param {{ html?: string }} [ctx]
 */
export function assessRoomsClaim(claim, ctx = {}) {
  const count = parseCount(claim?.value);
  if (count == null) {
    return { ok: false, confidence: "Low", reason: "invalid_count" };
  }
  const source = String(claim?.source || "");
  const conf = String(claim?.confidence || "");
  // Known bad VIC pattern: all Medium "IHG hoteldetail page text" with 22
  if (
    count === 22 &&
    /IHG hoteldetail page text \(explicit room count\)/i.test(source)
  ) {
    return {
      ok: false,
      confidence: "Low",
      reason: "known_vic_false_positive_22rooms_js_escape",
    };
  }
  if (ctx.html && isFalsePositiveRoomCount(ctx.html, count, "claim")) {
    return { ok: false, confidence: "Low", reason: "false_positive_against_html" };
  }
  if (conf === "Low" || conf === "Insufficient") {
    return { ok: false, confidence: "Low", reason: "claim_confidence_too_low" };
  }
  // Without HTML confirmation, VIC claims max Medium (never auto High)
  return {
    ok: true,
    count,
    confidence: "Medium",
    reason: "vic_claim_needs_official_page_confirmation",
    method: "vic_field_claim",
  };
}

/**
 * Select best writable / reviewable room hit.
 * High = production candidate; Medium = review; Low/Hold = blocked.
 * @param {RoomHit[]} hits
 */
export function selectBestRoomsHit(hits) {
  const usable = (hits || []).filter((h) => h && !h.rejected && h.count != null);
  if (!usable.length) {
    const held = (hits || []).filter((h) => h && (h.confidence === "Hold" || h.rejected));
    return held[0] || null;
  }
  const rank = { High: 4, Medium: 3, Low: 2, Hold: 1 };
  usable.sort((a, b) => (rank[b.confidence] || 0) - (rank[a.confidence] || 0));
  return usable[0];
}

/**
 * Map engine confidence onto existing Airtable Rooms Confidence options.
 * Existing: Exact, High, Medium, Low, Insufficient, Unknown
 * @param {'High'|'Medium'|'Low'|'Hold'} confidence
 */
export function mapToExistingRoomsConfidence(confidence) {
  if (confidence === "High") return "High";
  if (confidence === "Medium") return "Medium";
  if (confidence === "Low") return "Low";
  if (confidence === "Hold") return "Insufficient";
  return "Unknown";
}
