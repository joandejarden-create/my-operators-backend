/**
 * Multilingual room-count phrase extraction.
 * Reuses production-census-rooms-keys-extractor; adds ES/PT/FR explicit phrases.
 * Never infers from room types, availability, photos, or review counts.
 */

import {
  extractRoomsKeysFromOfficialHtml,
  selectBestRoomsHit,
  isFalsePositiveRoomCount,
  assessMixedUseRisk,
  ROOMS_EXTRACTOR_VERSION,
} from "../../research-engine-v2/production-census-rooms-keys-extractor.js";

export const ROOM_COUNT_EXTRACT_VERSION = "room-count-research-extract-v1";

function parseCount(raw) {
  const n = Number(String(raw).replace(/,/g, "").replace(/\./g, ""));
  if (!Number.isFinite(n) || !Number.isInteger(n)) return null;
  if (n < 5 || n > 5000) return null;
  return n;
}

/**
 * Extract a short supporting quote around a match.
 * @param {string} text
 * @param {number} index
 * @param {number} [len]
 */
export function extractQuote(text, index, len = 0) {
  const t = String(text || "");
  const start = Math.max(0, index - 40);
  const end = Math.min(t.length, index + Math.max(len, 20) + 80);
  let quote = t
    .slice(start, end)
    .replace(/\s+/g, " ")
    .trim();
  // Prefer sentence-ish boundaries
  const m = quote.match(
    /[^.!?]{0,40}\b\d{1,4}\b[^.!?]{0,80}(?:habitaciones?|quartos?|chambres?|guestrooms?|guest rooms?|rooms?|keys?|llaves)[^.!?]{0,40}[.!?]?/i
  );
  if (m) quote = m[0].trim();
  if (quote.length > 180) quote = `${quote.slice(0, 177)}...`;
  return quote;
}

/**
 * Explicit multilingual room-count phrases (CALA).
 * @param {string} text
 */
export function extractMultilingualRoomPhrases(text) {
  const t = String(text || "");
  /** @type {Array<{count:number,method:string,confidence:string,hotel_only:boolean,mixed_use_risk:boolean,quote:string,language:string,rejected?:boolean,reject_reason?:string}>} */
  const hits = [];

  const patterns = [
    // Spanish
    [/(\d{1,4})\s+habitaciones?\b/gi, "es_N_habitaciones", "High", "es"],
    [/\bhotel\s+de\s+(\d{1,4})\s+habitaciones?\b/gi, "es_hotel_de_N_habitaciones", "High", "es"],
    [/\bcuenta\s+con\s+(\d{1,4})\s+habitaciones?\b/gi, "es_cuenta_con_N", "High", "es"],
    [/\bofrece\s+(\d{1,4})\s+habitaciones?\b/gi, "es_ofrece_N", "High", "es"],
    [/(\d{1,4})\s+llaves\b/gi, "es_N_llaves", "Medium", "es"],
    [/(\d{1,4})\s*-\s*habitaciones?\b/gi, "es_N_hyphen_habitaciones", "High", "es"],
    // Portuguese
    [/(\d{1,4})\s+quartos?\b/gi, "pt_N_quartos", "High", "pt"],
    [/\bhotel\s+com\s+(\d{1,4})\s+quartos?\b/gi, "pt_hotel_com_N", "High", "pt"],
    [/\boferece\s+(\d{1,4})\s+quartos?\b/gi, "pt_oferece_N", "High", "pt"],
    // French
    [/(\d{1,4})\s+chambres?\b/gi, "fr_N_chambres", "High", "fr"],
    [/\bh[oô]tel\s+de\s+(\d{1,4})\s+chambres?\b/gi, "fr_hotel_de_N", "High", "fr"],
    [/\bpropose\s+(\d{1,4})\s+chambres?\b/gi, "fr_propose_N", "High", "fr"],
    // English extras aligned with analyst phrasing
    [/\bfeaturing\s+(\d{1,4})\s+(?:guest\s*)?rooms?\b/gi, "en_featuring_N", "High", "en"],
    [/\b(?:a|an)\s+(\d{1,4})\s*-\s*room\s+hotel\b/gi, "en_a_N_room_hotel", "High", "en"],
    [/\b(\d{1,4})\s*-\s*key\s+hotel\b/gi, "en_N_key_hotel", "High", "en"],
    [/\btotal\s+(?:of\s+)?(\d{1,4})\s+(?:guest\s*)?rooms?\b/gi, "en_total_of_N", "High", "en"],
    [/(\d{1,4})\s+guestrooms?\b/gi, "en_N_guestrooms", "High", "en"],
    [/\bfeaturing\s+(\d{1,4})\s+guestrooms?\b/gi, "en_featuring_N_guestrooms", "High", "en"],
  ];

  for (const [rx, method, baseConf, language] of patterns) {
    rx.lastIndex = 0;
    let m;
    while ((m = rx.exec(t)) !== null) {
      const count = parseCount(m[1]);
      if (count == null) continue;
      if (isFalsePositiveRoomCount(t, count, method)) continue;
      const risk = assessMixedUseRisk(t, m.index || 0);
      let confidence = baseConf;
      let rejected = false;
      let reject_reason;
      if (risk.mixed_use_risk && !/hotel|habitacion|quarto|chambre/i.test(m[0])) {
        confidence = "Hold";
        rejected = true;
        reject_reason = "mixed_use_ambiguity";
      }
      hits.push({
        count,
        method,
        confidence,
        hotel_only: !risk.mixed_use_risk,
        mixed_use_risk: risk.mixed_use_risk,
        quote: extractQuote(t, m.index || 0, m[0].length),
        language,
        rejected,
        reject_reason,
      });
      if (hits.length >= 12) break;
    }
    if (hits.length >= 12) break;
  }

  return hits;
}

/**
 * Extract room counts from HTML or plain text / snippet.
 * @param {string} htmlOrText
 * @param {{ url?: string }} [opts]
 */
export function extractRoomCountsFromText(htmlOrText, opts = {}) {
  const text = String(htmlOrText || "");
  const fromOfficial = extractRoomsKeysFromOfficialHtml(text, opts);
  const multi = extractMultilingualRoomPhrases(text);

  // Attach quotes to English official hits where missing
  const enrichedOfficial = (fromOfficial.hits || []).map((h) => {
    if (h.quote) return h;
    const re = new RegExp(
      String(h.count) +
        "[^\\d]{0,40}(?:guest\\s+)?(?:rooms?|keys?|guestrooms?)",
      "i"
    );
    const m = text.match(re);
    const idx = m ? text.search(re) : -1;
    return {
      ...h,
      quote: idx >= 0 ? extractQuote(text, idx, m?.[0]?.length || 0) : null,
      language: "en",
    };
  });

  const hits = [...enrichedOfficial, ...multi];
  const best = selectBestRoomsHit(hits);
  return {
    hits,
    best,
    patterns_matched: [
      ...new Set([
        ...(fromOfficial.patterns_matched || []),
        ...multi.map((h) => h.method),
      ]),
    ],
    extractor_versions: {
      production: ROOMS_EXTRACTOR_VERSION,
      multilingual: ROOM_COUNT_EXTRACT_VERSION,
    },
  };
}
