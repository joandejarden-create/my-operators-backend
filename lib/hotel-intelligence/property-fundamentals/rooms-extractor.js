/**
 * Extract CURRENT room/key observations from research text/artifacts.
 * Hotel-neutral — identity-scoped; avoids adjacent-hotel leakage.
 */

import { toPositiveInt } from "./rooms-resolver.js";

const HISTORICAL_CUES =
  /\b(formerly|previously|before (the )?renovation|pre-renovation|historic(ally)?|was\s+\d{2,4}\s+rooms?)\b/i;
const ANNOUNCED_CUES =
  /\b(will (feature|include|have)|planned|proposed|announced|under (construction|development)|pipeline|expected to)\b/i;
const TENT_CUES = /\b(tents?|glamping|yurts?)\b/i;
const ACCOMMODATION_CUES = /\b(accommodations?|units?|cabins?)\b/i;

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Text windows AFTER each hotel-name hit until the next markdown property heading.
 * Looking only forward prevents "197 rooms. **Wayfinder Waikiki**" leakage.
 */
export function windowsAfterHotelName(text, hotelName, { maxLen = 420 } = {}) {
  const raw = String(text || "");
  const name = String(hotelName || "").trim();
  if (!raw || !name) return [];
  const re = new RegExp(escapeRegExp(name), "gi");
  const windows = [];
  let m;
  while ((m = re.exec(raw)) !== null) {
    const start = m.index + m[0].length;
    let end = Math.min(raw.length, start + maxLen);
    const after = raw.slice(start, end);
    // Stop at next bold property-style heading
    const nextHeading = after.search(/\n\s*\*\*[^*\n]{3,80}\*\*/);
    if (nextHeading >= 0) end = start + nextHeading;
    // Also stop at a clear next-hotel table row pattern
    const nextRow = after.search(/\n[A-Z][^\n]{2,60}\n[A-Z][^\n]{2,40}\n\*\*?OWNED|\nMEDIUM |\nHIGH /);
    if (nextRow >= 0) end = Math.min(end, start + nextRow);
    windows.push(raw.slice(start, end));
  }
  return windows;
}

/**
 * Parse a room count from a text window.
 * Rejects tents-as-rooms unless allowNontraditional.
 * Optionally returns accommodation_units when tents/cabins language is present.
 */
export function parseRoomsFromWindow(windowText, { allowNontraditional = false } = {}) {
  const w = String(windowText || "");
  if (!w) return null;

  let temporal = "CURRENT";
  if (HISTORICAL_CUES.test(w)) temporal = "HISTORICAL";
  if (ANNOUNCED_CUES.test(w) && !/\b(opened|open since|currently|assumed management)\b/i.test(w)) {
    temporal = temporal === "HISTORICAL" ? "HISTORICAL" : "ANNOUNCED";
  }

  const tentLike = TENT_CUES.test(w);
  const accommodationOnly = tentLike && !/\b(rooms?|keys?|suites?)\b/i.test(w);

  if (accommodationOnly && !allowNontraditional) {
    const acc = w.match(/(\d{1,4})\s+accommodations?\b/i);
    if (acc) {
      return {
        value: null,
        accommodation_units: toPositiveInt(acc[1]),
        temporal_status: temporal,
        match: acc[0],
        unit_kind: "accommodations_nontraditional",
      };
    }
    return null;
  }

  const patterns = [
    /(\d{2,4})\s*[-–]?\s*room(?:s)?\b/i,
    /(\d{2,4})\s+guest\s+rooms?\b/i,
    /(\d{2,4})\s+keys?\b/i,
    /(\d{2,4})\s+suites?\b/i,
    /\brooms?\s*[:=]\s*(\d{2,4})\b/i,
    /(\d{2,4})\s+accommodations?\b/i,
    /\b(\d{2,4})\s+rooms?\b/i,
  ];

  for (const p of patterns) {
    const m = w.match(p);
    if (!m) continue;
    const value = toPositiveInt(m[1]);
    if (!value) continue;
    // "86 accommodations" with suite language elsewhere is fine; tents stay nontraditional
    if (tentLike && ACCOMMODATION_CUES.test(m[0]) && !allowNontraditional) {
      return {
        value: null,
        accommodation_units: value,
        temporal_status: temporal,
        match: m[0],
        unit_kind: "accommodations_nontraditional",
      };
    }
    return { value, temporal_status: temporal, match: m[0], unit_kind: "rooms" };
  }
  return null;
}

/**
 * Extract a rooms observation for one hotel from corpus text.
 */
export function extractRoomsObservationFromText(text, hotel, opts = {}) {
  const name = hotel?.name || hotel?.hotel_name;
  if (!name) return null;
  const windows = windowsAfterHotelName(text, name, { maxLen: opts.radius || 420 });
  if (!windows.length) return null;

  let best = null;
  let bestAcc = null;
  for (const win of windows) {
    const parsed = parseRoomsFromWindow(win, {
      allowNontraditional: Boolean(opts.allowNontraditional),
    });
    if (!parsed) continue;
    if (parsed.temporal_status !== "CURRENT") {
      // Keep rejected candidates for audit only when caller wants them
      if (opts.includeRejected && !best) {
        best = {
          field: "rooms",
          value: parsed.value,
          hotel_name: name,
          rejected: true,
          rejection_reason: parsed.temporal_status,
          temporal_status: parsed.temporal_status,
          match_snippet: parsed.match,
        };
      }
      continue;
    }
    if (parsed.accommodation_units && !parsed.value) {
      bestAcc = {
        field: "accommodation_units",
        value: parsed.accommodation_units,
        hotel_name: name,
        property_id: hotel.id || hotel.hotel_id || null,
        source_type: opts.source_type || "research_corpus",
        source_url: opts.source_url || null,
        source_title: opts.source_title || null,
        source_provider: opts.source_provider || "webhound",
        observed_at: opts.observed_at || null,
        confidence: opts.defaultConfidence || "HIGH",
        temporal_status: "CURRENT",
        match_snippet: parsed.match,
        unit_kind: parsed.unit_kind,
        provenance: {
          extraction: "context_window_after_hotel_name",
          match: parsed.match,
        },
      };
      continue;
    }
    if (!parsed.value) continue;
    const candidate = {
      field: "rooms",
      value: parsed.value,
      hotel_name: name,
      property_id: hotel.id || hotel.hotel_id || null,
      source_type: opts.source_type || "research_corpus",
      source_url: opts.source_url || null,
      source_title: opts.source_title || null,
      source_provider: opts.source_provider || "webhound",
      observed_at: opts.observed_at || null,
      confidence: opts.defaultConfidence || "HIGH",
      temporal_status: "CURRENT",
      match_snippet: parsed.match,
      unit_kind: parsed.unit_kind || "rooms",
      provenance: {
        extraction: "context_window_after_hotel_name",
        match: parsed.match,
      },
    };
    if (!best || best.rejected) best = candidate;
  }
  return best && !best.rejected ? best : bestAcc || null;
}

/**
 * Extract rooms observations for many hotels from one or more corpus texts.
 */
export function extractRoomsObservationsForHotels(corpusTexts, hotels, opts = {}) {
  const texts = (Array.isArray(corpusTexts) ? corpusTexts : [corpusTexts])
    .map((t) => String(t || ""))
    .filter(Boolean);
  const out = [];
  for (const hotel of hotels || []) {
    for (const text of texts) {
      const obs = extractRoomsObservationFromText(text, hotel, opts);
      if (obs) {
        out.push(obs);
        break;
      }
    }
  }
  return out;
}
