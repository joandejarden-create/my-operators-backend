/**
 * Packet 2.8C-2 — Document section targeting + lightweight table extraction.
 */

export const DOCUMENT_INTEL_VERSION = "document-intelligence-v1";

const SECTION_PATTERNS = Object.freeze({
  SUBSIDIARIES: [/subsidiari/i, /subsidiary|subsidiaries/i, /filiales/i],
  PROPERTIES: [/propiedades|properties|hoteles|portfolio hotels/i, /inmuebles/i],
  ACQUISITIONS: [/adquisici/i, /acquisition|acquired|purchase/i],
  DISPOSITIONS: [/disposici/i, /disposition|divest|sold/i],
  RELATED_PARTIES: [/partes relacionadas|related part/i],
  LEASES: [/arrendamiento|lease|leasing/i],
  COMMITMENTS: [/compromiso|commitment|contingen/i],
  PORTFOLIO: [/portafolio|portfolio/i],
  DEVELOPMENT: [/desarrollo|development|pipeline|construcción/i],
  MANAGEMENT: [/administraci[oó]n|management team|equipo directivo/i],
  BRAND_FRANCHISE: [/marca|franchise|brand|afiliaci/i],
  MANAGEMENT_TEAM: [/consejo|board of directors|directivos|officers/i],
});

export function findDocumentSections(text = "", { maxSections = 8 } = {}) {
  const raw = String(text || "");
  const lower = raw.toLowerCase();
  const hits = [];
  for (const [name, patterns] of Object.entries(SECTION_PATTERNS)) {
    for (const re of patterns) {
      const m = re.exec(raw);
      if (m && m.index >= 0) {
        const start = Math.max(0, m.index - 40);
        const end = Math.min(raw.length, m.index + 1800);
        hits.push({
          section: name,
          index: m.index,
          excerpt: raw.slice(start, end),
          heading_hint: raw.slice(m.index, Math.min(raw.length, m.index + 80)).split("\n")[0],
        });
        break;
      }
    }
  }
  hits.sort((a, b) => a.index - b.index);
  return {
    version: DOCUMENT_INTEL_VERSION,
    sections: hits.slice(0, maxSections),
    toc_like: /table of contents|contenido|índice/i.test(lower),
  };
}

export function prioritizeDocumentText(text = "", { domain = "OWNERSHIP", maxChars = 6000 } = {}) {
  const sections = findDocumentSections(text);
  const prefer =
    /OWNER|PROPCO|PORTFOLIO/i.test(domain)
      ? ["SUBSIDIARIES", "PROPERTIES", "PORTFOLIO", "RELATED_PARTIES", "ACQUISITIONS"]
      : /BRAND/i.test(domain)
        ? ["BRAND_FRANCHISE", "DEVELOPMENT", "PROPERTIES"]
        : /TRANSACT/i.test(domain)
          ? ["ACQUISITIONS", "DISPOSITIONS", "RELATED_PARTIES"]
          : /PEOPLE/i.test(domain)
            ? ["MANAGEMENT_TEAM", "MANAGEMENT"]
            : ["PROPERTIES", "PORTFOLIO", "DEVELOPMENT"];

  const preferred = sections.sections.filter((s) => prefer.includes(s.section));
  if (!preferred.length) {
    return { text: String(text || "").slice(0, maxChars), sections, strategy: "head_truncate" };
  }
  const packed = preferred.map((s) => `## ${s.section}\n${s.excerpt}`).join("\n\n");
  return {
    text: packed.slice(0, maxChars),
    sections,
    strategy: "section_targeting",
    preferred_sections: preferred.map((s) => s.section),
  };
}

/**
 * Heuristic portfolio table row extraction from plain text.
 * Does NOT infer ownership merely from table presence.
 */
export function extractPortfolioTableRows(text = {}, opts = {}) {
  const raw = typeof text === "string" ? text : String(text?.text || "");
  const lines = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  const rows = [];
  const hotelHint = opts.hotel_name ? String(opts.hotel_name).toLowerCase() : null;

  for (const line of lines) {
    if (line.length < 12 || line.length > 220) continue;
    const roomsMatch = line.match(/\b(\d{2,4})\b/);
    const looksTabular =
      (line.match(/[|,;\t]/g) || []).length >= 2 ||
      /\b(rooms?|llaves|habitaciones|keys)\b/i.test(line) ||
      /\d{2,4}\s*(rooms?|llaves)?/i.test(line);
    if (!looksTabular && !(hotelHint && line.toLowerCase().includes(hotelHint))) continue;

    const cityMatch = line.match(/\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){0,2})\b/);
    rows.push({
      raw_line: line,
      hotel_name_guess: hotelHint && line.toLowerCase().includes(hotelHint) ? opts.hotel_name : null,
      city_guess: cityMatch ? cityMatch[1] : null,
      rooms_guess: roomsMatch ? Number(roomsMatch[1]) : null,
      ownership_pct: null,
      relationship_classification: null,
      brand: null,
      operator: null,
      status: null,
      provenance: { extractor: "table_line_heuristic_v1", note: "appearance_in_table_not_ownership" },
      ownership_inferred: false,
    });
  }

  return {
    version: DOCUMENT_INTEL_VERSION,
    row_count: rows.length,
    rows: rows.slice(0, 40),
    warning: "Do not infer ownership merely from portfolio table appearance.",
  };
}
