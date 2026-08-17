/**
 * Deterministic search-query builder for one hotel's room-count research.
 * Not a crawler — produces a small ordered list of targeted queries.
 */

export const ROOM_COUNT_QUERIES_VERSION = "room-count-queries-v1";

/**
 * @param {{
 *   hotel_name?: string|null,
 *   city?: string|null,
 *   country?: string|null,
 *   brand?: string|null,
 *   website?: string|null,
 * }} hotel
 * @returns {Array<{step:string,q:string,purpose:string}>}
 */
export function buildRoomCountQueries(hotel = {}) {
  const name = String(hotel.hotel_name || hotel.name || "").trim();
  const city = String(hotel.city || "").trim();
  const country = String(hotel.country || "").trim();
  const brand = String(hotel.brand || "").trim();
  const place = [city, country].filter(Boolean).join(" ");
  const id = [name, place].filter(Boolean).join(" ");

  /** @type {Array<{step:string,q:string,purpose:string}>} */
  const out = [];

  if (name) {
    out.push({
      step: "official_hotel",
      q: `"${name}" ${place} (rooms OR guestrooms OR habitaciones OR "guest rooms" OR keys)`.trim(),
      purpose: "Find official hotel pages stating total rooms",
    });
  }
  if (name && brand) {
    out.push({
      step: "official_brand",
      q: `"${name}" ${brand} (rooms OR habitaciones OR guestrooms)`.trim(),
      purpose: "Find official brand page for this property",
    });
  }
  if (name) {
    out.push({
      step: "owner_operator",
      q: `"${name}" ${place} (owner OR operator OR management) (rooms OR habitaciones)`.trim(),
      purpose: "Find owner/operator pages with room counts",
    });
  }
  if (name && (city || country)) {
    out.push({
      step: "convention_bureau",
      q: `"${name}" ${city || country} (CVB OR "convention bureau" OR "meeting planners" OR tourism) rooms`.trim(),
      purpose: "Find CVB / DMO fact sheets",
    });
  }
  if (name) {
    out.push({
      step: "directory_press",
      q: `"${name}" ${place} ("featuring" OR "offers" OR "cuenta con") (rooms OR habitaciones)`.trim(),
      purpose: "Find press / directory statements with explicit wording",
    });
  }

  // Cap — never broad crawl
  return out.slice(0, 5);
}

/**
 * Rank organic URLs for follow-up fetch priority.
 * @param {Array<{link?:string,title?:string,snippet?:string}>} organic
 * @param {{ hotelWebsite?: string|null, hotel_name?: string|null }} ctx
 * @param {(url:string,ctx?:object)=>string} classifyFn
 * @param {(url:string)=>boolean} eligibleFn
 */
export function selectFetchCandidates(organic, ctx, classifyFn, eligibleFn) {
  const nameToken = String(ctx.hotel_name || "")
    .toLowerCase()
    .split(/\s+/)
    .filter((t) => t.length > 3)
    .slice(0, 3);

  const scored = [];
  for (const row of organic || []) {
    const url = String(row.link || row.url || "").trim();
    if (!url || !eligibleFn(url)) continue;
    const category = classifyFn(url, { hotelWebsite: ctx.hotelWebsite });
    let score = 0;
    if (category === "Official Hotel") score += 50;
    else if (category === "Official Brand") score += 45;
    else if (category === "Tourism Authority") score += 35;
    else if (category === "Convention Bureau") score += 32;
    else if (category === "Historic Press Release") score += 28;
    else if (category === "News") score += 15;
    else score += 5;

    const blob = `${row.title || ""} ${row.snippet || ""}`.toLowerCase();
    for (const t of nameToken) {
      if (blob.includes(t)) score += 5;
    }
    if (/\b\d{2,4}\s+(guest\s+)?rooms?\b|\bhabitaciones?\b|\bquartos?\b|\bchambres?\b/i.test(blob)) {
      score += 20;
    }
    scored.push({ url, category, score, title: row.title || null, snippet: row.snippet || null });
  }
  scored.sort((a, b) => b.score - a.score);
  // Max 3 follow-up pages
  return scored.slice(0, 3);
}
