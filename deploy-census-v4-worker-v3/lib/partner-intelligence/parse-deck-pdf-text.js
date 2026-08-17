/**
 * Structured parsing for regional experience deck PDFs (smashed table text).
 */

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

const HOTEL_ROW =
  /([A-Za-z0-9][A-Za-z0-9\s&',.\-]{4,90})\|(Operations|Sales|Operaciones|Ventas)/gi;

/**
 * @param {string} text
 * @param {{ sourceTitle?: string, localFilePath?: string }} [sourceMeta]
 */
export function parseRegionalExperienceDeck(text, sourceMeta = {}) {
  const raw = nz(text);
  const hotels = [];
  let m;
  HOTEL_ROW.lastIndex = 0;
  while ((m = HOTEL_ROW.exec(raw)) !== null) {
    hotels.push({
      property: m[1].replace(/\s+/g, " ").trim(),
      function: m[2],
    });
  }

  const byFunction = {};
  for (const h of hotels) {
    const key = h.function;
    if (!byFunction[key]) byFunction[key] = [];
    byFunction[key].push(h.property);
  }

  const countries = new Set();
  if (/mexico|méxico/i.test(raw)) countries.add("Mexico");
  if (/latam|latin america|resto de latam/i.test(raw)) countries.add("Latin America");
  if (/caribbean|caribe/i.test(raw)) countries.add("Caribbean");
  if (/united states|u\.s\./i.test(raw)) countries.add("United States");

  const serializedHotels = hotels.length
    ? hotels.map((h) => `${h.property} (${h.function})`).join("; ")
    : "";

  const narrativeText = [
    "Regional Experience Deck",
    sourceMeta.sourceTitle || sourceMeta.localFilePath || "",
    `Countries mentioned: ${[...countries].join(", ") || "unknown"}`,
    `Properties (${hotels.length}): ${serializedHotels}`,
  ].join("\n");

  const portfolioJson = {
    source: "regional_deck",
    hotelCount: hotels.length,
    countries: [...countries],
    hotels,
    byFunction,
  };

  return {
    kind: "regional_deck",
    hotelCount: hotels.length,
    countries: [...countries],
    hotels,
    serializedHotels,
    narrativeText,
    portfolioJson: JSON.stringify(portfolioJson),
    /** Combined text for LLM (structured header + raw excerpt) */
    enrichedText: [narrativeText, raw.slice(0, 12000)].filter(Boolean).join("\n\n---\n\n"),
  };
}

/**
 * @param {string} text
 * @param {object} classification
 * @param {object} [sourceMeta]
 */
export function enrichDocumentTextForExtraction(text, classification, sourceMeta = {}) {
  if (!classification?.parseAsDeck) {
    return { text, structured: null };
  }
  const parsed = parseRegionalExperienceDeck(text, sourceMeta);
  return {
    text: parsed.enrichedText,
    structured: parsed,
  };
}
