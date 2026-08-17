/**
 * Barbados BTPA Registered Tourist Accommodation Directory adapter.
 * Field: Number of Bedrooms — Hotels category preferred for Rooms / Keys.
 */
export const BARBADOS_BTPA_ADAPTER_VERSION = "barbados-btpa-directory-adapter-v1";

export const MAP_BARBADOS_BTPA = Object.freeze({
  sourceDirectoryUrl: "https://www.barbadostouristaccommodation.com/directory",
  country: "Barbados",
  familySourceFamily: "Government — Barbados BTPA",
  sourceType: "official_licensed_accommodation_directory",
});

export function parseBarbadosBedroomCount(raw) {
  const n = Number(String(raw || "").replace(/[^\d]/g, ""));
  if (!Number.isFinite(n) || n <= 0 || n > 2000) {
    return { ok: false, rooms: null };
  }
  return { ok: true, rooms: n };
}

/**
 * Parse property rows from BTPA directory HTML.
 * @param {string} html
 */
export function parseBarbadosBtpaDirectoryHtml(html) {
  const text = String(html || "");
  /** @type {Array<object>} */
  const rows = [];

  // Table-ish: Property Name ... Number of Bedrooms
  // Also catch "Name</td><td>…</td>…<td>224</td>" patterns and markdown-ish dumps
  const blockRe =
    /(?:Hotel|Hotels|Apartment|Apartments|Guest\s*House|Guest\s*Houses)[\s\S]{0,200}?([A-Z][^<\n|]{3,80})[\s\S]{0,120}?(\d{1,4})\s*(?:bedrooms?|rooms?)?/gi;

  // Prefer explicit "Number of Bedrooms" adjacent patterns
  const lineRe =
    /([A-Za-z0-9][^|\n<>]{2,90}?)\s*(?:\||<\/td>\s*<td[^>]*>)\s*(\d{1,4})\s*(?:\||<\/td>)/g;

  const seen = new Set();
  let m;
  while ((m = lineRe.exec(text)) !== null) {
    const name = String(m[1] || "")
      .replace(/<[^>]+>/g, "")
      .replace(/\s+/g, " ")
      .trim();
    if (!name || /number of bedrooms|property name|category|total/i.test(name)) {
      continue;
    }
    if (name.length < 4 || name.length > 90) continue;
    const roomsParse = parseBarbadosBedroomCount(m[2]);
    if (!roomsParse.ok) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push({
      adapter: "barbados_btpa",
      property_name: name,
      city: null,
      state_region: null,
      country: "Barbados",
      rooms: roomsParse.rooms,
      rooms_field_used: "Number of Bedrooms",
      source_url: MAP_BARBADOS_BTPA.sourceDirectoryUrl,
      category_hint: null,
    });
  }

  // Fallback: Accra Beach style plain text listings
  if (rows.length < 5) {
    const plain =
      /([A-Z][A-Za-z0-9 &'.,\-]{3,70})\s+[-–]\s*(\d{1,4})\s+bedrooms?/gi;
    while ((m = plain.exec(text)) !== null) {
      const name = m[1].trim();
      const roomsParse = parseBarbadosBedroomCount(m[2]);
      if (!roomsParse.ok) continue;
      const key = name.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      rows.push({
        adapter: "barbados_btpa",
        property_name: name,
        city: null,
        state_region: null,
        country: "Barbados",
        rooms: roomsParse.rooms,
        rooms_field_used: "Number of Bedrooms",
        source_url: MAP_BARBADOS_BTPA.sourceDirectoryUrl,
        category_hint: null,
      });
    }
  }

  void blockRe; // reserved for richer HTML variants
  return rows;
}

/**
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function fetchBarbadosBtpaDirectoryRows(opts = {}) {
  const url = MAP_BARBADOS_BTPA.sourceDirectoryUrl;
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), opts.timeoutMs || 45000);
    const res = await fetch(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent":
          "Mozilla/5.0 (compatible; DealalityCensusBot/1.0; +property-fundamentals)",
        Accept: "text/html",
      },
    });
    clearTimeout(t);
    if (!res.ok) {
      return {
        ok: false,
        error_kind: "http_error",
        message: `BTPA HTTP ${res.status}`,
        rows: [],
      };
    }
    const html = await res.text();
    const rows = parseBarbadosBtpaDirectoryHtml(html);
    return {
      ok: rows.length > 0,
      adapter_version: BARBADOS_BTPA_ADAPTER_VERSION,
      source_url: url,
      rows,
      message:
        rows.length > 0
          ? null
          : "BTPA HTML parse returned 0 rows — directory markup may have changed",
    };
  } catch (err) {
    return {
      ok: false,
      error_kind: "fetch_error",
      message: String(err?.message || err).slice(0, 200),
      rows: [],
    };
  }
}
