/**
 * Fetch Hilton hotel marketing copy via public GraphQL (no detail HTML required).
 *
 * Primary website "Description" maps to facilityOverview.shortDesc.
 */

export const HILTON_GRAPHQL_URL = "https://www.hilton.com/graphql/customer";

export const HILTON_GRAPHQL_HEADERS = {
  "Content-Type": "application/json",
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  Accept: "application/json",
  Origin: "https://www.hilton.com",
};

const HOTEL_DESCRIPTION_QUERY = `query hotelDescription($ctyhocn: String!, $language: String!) {
  hotel(ctyhocn: $ctyhocn, language: $language) {
    name
    ctyhocn
    facilityOverview {
      shortDesc
      headline
      locationShortDesc
      hotelTeaserText
      directionsTo
      homeUrlTemplate
    }
  }
}`;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * @param {string} ctyhocn
 * @param {{ language?: string, refererUrl?: string }} [opts]
 */
export async function fetchHiltonHotelDescription(ctyhocn, opts = {}) {
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
      operationName: "hotelDescription",
      query: HOTEL_DESCRIPTION_QUERY,
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

  const fo = hotel.facilityOverview || {};
  return {
    source: "hilton_graphql",
    ctyhocn: String(hotel.ctyhocn || code).toUpperCase(),
    name: String(hotel.name || "").trim(),
    website: String(fo.homeUrlTemplate || "").trim(),
    shortDesc: String(fo.shortDesc || "").trim(),
    headline: String(fo.headline || "").trim(),
    locationShortDesc: String(fo.locationShortDesc || "").trim(),
    hotelTeaserText: String(fo.hotelTeaserText || "").trim(),
    directionsTo: String(fo.directionsTo || "").trim(),
  };
}

/**
 * @param {string[]} ctyhocnList
 * @param {{ delayMs?: number, language?: string, onProgress?: (msg: string) => void }} [opts]
 */
export async function fetchHiltonHotelDescriptionsBatch(ctyhocnList, opts = {}) {
  const delayMs = opts.delayMs ?? 300;
  const out = [];
  const errors = [];

  for (let i = 0; i < ctyhocnList.length; i++) {
    const code = ctyhocnList[i];
    if (opts.onProgress) opts.onProgress(`[${i + 1}/${ctyhocnList.length}] ${code}`);
    try {
      const row = await fetchHiltonHotelDescription(code, { language: opts.language });
      out.push(row);
    } catch (err) {
      errors.push({ ctyhocn: code, error: err?.message || String(err) });
    }
    if (delayMs > 0 && i < ctyhocnList.length - 1) await sleep(delayMs);
  }

  return { descriptions: out, errors };
}

/**
 * Website description paragraph shown on Hilton hotel pages.
 * @param {{ shortDesc?: string, hotelTeaserText?: string, locationShortDesc?: string }} row
 */
export function pickPrimaryHiltonDescription(row) {
  return (
    String(row?.shortDesc || "").trim() ||
    String(row?.hotelTeaserText || "").trim() ||
    String(row?.locationShortDesc || "").trim()
  );
}
