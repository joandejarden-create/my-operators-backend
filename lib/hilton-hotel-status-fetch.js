/**
 * Fetch Hilton hotel open/pipeline status via public GraphQL.
 */

import { STATUS_OPEN, STATUS_PIPELINE } from "./hotel-census/fields.js";
import { HILTON_GRAPHQL_URL, HILTON_GRAPHQL_HEADERS } from "./hilton-hotel-description-fetch.js";

const HOTEL_STATUS_QUERY = `query hotelStatus($ctyhocn: String!, $language: String!) {
  hotel(ctyhocn: $ctyhocn, language: $language) {
    name
    ctyhocn
    display {
      open
      openDate
    }
  }
}`;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} ctyhocn
 * @param {{ language?: string, refererUrl?: string }} [opts]
 */
export async function fetchHiltonHotelStatus(ctyhocn, opts = {}) {
  const code = String(ctyhocn || "")
    .trim()
    .toUpperCase();
  if (!code) throw new Error("ctyhocn required");

  const language = opts.language || "en";
  const refererUrl =
    opts.refererUrl || `https://www.hilton.com/en/hotels/${code.toLowerCase()}-hotel/`;

  const res = await fetch(HILTON_GRAPHQL_URL, {
    method: "POST",
    headers: {
      ...HILTON_GRAPHQL_HEADERS,
      Referer: refererUrl,
    },
    body: JSON.stringify({
      operationName: "hotelStatus",
      query: HOTEL_STATUS_QUERY,
      variables: { ctyhocn: code, language },
    }),
  });

  const json = await res.json();
  if (!res.ok) {
    throw new Error(`GraphQL HTTP ${res.status}: ${JSON.stringify(json).slice(0, 300)}`);
  }
  if (json.errors?.length) {
    throw new Error(json.errors.map((e) => e.message).join("; "));
  }

  const hotel = json?.data?.hotel;
  if (!hotel) throw new Error(`No hotel returned for ${code}`);

  const open = hotel.display?.open === true;
  return {
    source: "hilton_graphql",
    ctyhocn: String(hotel.ctyhocn || code).toUpperCase(),
    name: String(hotel.name || "").trim(),
    hiltonOpen: open,
    hiltonStatus: open ? STATUS_OPEN : STATUS_PIPELINE,
    openDate: String(hotel.display?.openDate || "").trim() || null,
  };
}

/**
 * @param {string[]} codes
 * @param {{ delayMs?: number, onProgress?: (msg: string) => void }} [opts]
 */
export async function fetchHiltonHotelStatusBatch(codes, opts = {}) {
  const delayMs = opts.delayMs ?? 250;
  const results = [];
  const errors = [];

  for (let i = 0; i < codes.length; i++) {
    const code = codes[i];
    if (opts.onProgress) opts.onProgress(`[${i + 1}/${codes.length}] ${code}`);
    try {
      results.push(await fetchHiltonHotelStatus(code));
    } catch (err) {
      errors.push({ ctyhocn: code, error: err?.message || String(err) });
    }
    if (delayMs > 0 && i < codes.length - 1) await sleep(delayMs);
  }

  return { results, errors };
}
