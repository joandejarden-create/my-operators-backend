/**
 * Decode HTML/XML entities in RSS and import text (&#8217;, &apos;, &amp;, etc.).
 * Safe for Node; no DOM dependency.
 */

const NAMED_ENTITIES = {
  amp: "&",
  lt: "<",
  gt: ">",
  quot: '"',
  apos: "'",
  nbsp: " ",
  rsquo: "\u2019",
  lsquo: "\u2018",
  rdquo: "\u201D",
  ldquo: "\u201C",
  hellip: "\u2026",
  mdash: "\u2014",
  ndash: "\u2013",
  copy: "\u00A9",
  reg: "\u00AE",
  trade: "\u2122",
  euro: "\u20AC",
  pound: "\u00A3",
  yen: "\u00A5",
};

const ENTITY_RE = /&(#x[0-9a-f]+|#\d+|[a-z]+);/i;

/** True if string likely contains undecoded HTML entities. */
export function hasHtmlEntities(text) {
  if (!text || typeof text !== "string") return false;
  return ENTITY_RE.test(text);
}

function decodeOnce(text) {
  let s = text;
  s = s.replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
    const cp = parseInt(hex, 16);
    return Number.isFinite(cp) ? String.fromCodePoint(cp) : _;
  });
  s = s.replace(/&#(\d+);/g, (_, dec) => {
    const cp = parseInt(dec, 10);
    return Number.isFinite(cp) ? String.fromCodePoint(cp) : _;
  });
  s = s.replace(/&([a-z]+);/gi, (match, name) => {
    const key = name.toLowerCase();
    return Object.prototype.hasOwnProperty.call(NAMED_ENTITIES, key) ? NAMED_ENTITIES[key] : match;
  });
  return s;
}

/**
 * Decode entities; runs up to 3 passes for double-encoded source text.
 * @param {string|null|undefined} text
 * @returns {string}
 */
export function decodeHtmlEntities(text) {
  if (text == null) return "";
  if (typeof text !== "string") return String(text);
  let out = text;
  for (let i = 0; i < 3; i++) {
    const next = decodeOnce(out);
    if (next === out) break;
    out = next;
  }
  return out.replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
}

/** Decode without collapsing internal whitespace (for long summaries). */
export function decodeHtmlEntitiesPreserveWhitespace(text) {
  if (text == null) return "";
  if (typeof text !== "string") return String(text);
  let out = text;
  for (let i = 0; i < 3; i++) {
    const next = decodeOnce(out);
    if (next === out) break;
    out = next;
  }
  return out.replace(/\u00A0/g, " ");
}
