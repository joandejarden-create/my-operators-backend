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
  // Latin-1 / HTML4 (feeds often encode accented hotel & place names this way)
  Aacute: "\u00C1",
  aacute: "\u00E1",
  Acirc: "\u00C2",
  acirc: "\u00E2",
  AElig: "\u00C6",
  aelig: "\u00E6",
  Agrave: "\u00C0",
  agrave: "\u00E0",
  Aring: "\u00C5",
  aring: "\u00E5",
  Atilde: "\u00C3",
  atilde: "\u00E3",
  Auml: "\u00C4",
  auml: "\u00E4",
  Ccedil: "\u00C7",
  ccedil: "\u00E7",
  Eacute: "\u00C9",
  eacute: "\u00E9",
  Ecirc: "\u00CA",
  ecirc: "\u00EA",
  Egrave: "\u00C8",
  egrave: "\u00E8",
  ETH: "\u00D0",
  eth: "\u00F0",
  Euml: "\u00CB",
  euml: "\u00EB",
  Iacute: "\u00CD",
  iacute: "\u00ED",
  Icirc: "\u00CE",
  icirc: "\u00EE",
  Igrave: "\u00CC",
  igrave: "\u00EC",
  Iuml: "\u00CF",
  iuml: "\u00EF",
  Ntilde: "\u00D1",
  ntilde: "\u00F1",
  Oacute: "\u00D3",
  oacute: "\u00F3",
  Ocirc: "\u00D4",
  ocirc: "\u00F4",
  Ograve: "\u00D2",
  ograve: "\u00F2",
  Oslash: "\u00D8",
  oslash: "\u00F8",
  Otilde: "\u00D5",
  otilde: "\u00F5",
  Ouml: "\u00D6",
  ouml: "\u00F6",
  Uacute: "\u00DA",
  uacute: "\u00FA",
  Ucirc: "\u00DB",
  ucirc: "\u00FB",
  Ugrave: "\u00D9",
  ugrave: "\u00F9",
  Uuml: "\u00DC",
  uuml: "\u00FC",
  Yacute: "\u00DD",
  yacute: "\u00FD",
  yuml: "\u00FF",
  szlig: "\u00DF",
};

const ENTITY_RE = /&(#x[0-9a-f]+|#\d+|[a-zA-Z]+);/;

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
  s = s.replace(/&([a-zA-Z]+);/g, (match, name) => {
    const key = name in NAMED_ENTITIES ? name : name.toLowerCase();
    if (Object.prototype.hasOwnProperty.call(NAMED_ENTITIES, key)) return NAMED_ENTITIES[key];
    const lower = name.toLowerCase();
    return Object.prototype.hasOwnProperty.call(NAMED_ENTITIES, lower) ? NAMED_ENTITIES[lower] : match;
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
