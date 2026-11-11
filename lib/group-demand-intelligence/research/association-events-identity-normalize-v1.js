/**
 * Association / Event identity normalization V1 (expanded run-050).
 *
 * Separates source-page headlines from verified event display titles.
 * Does not invent official names when identity remains uncertain.
 */

export const ASSOCIATION_EVENTS_IDENTITY_NORMALIZE_V1 =
  "gdi-association-events-identity-normalize-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

const SPEAKER_HEADING_RE =
  /^(?:speakers?|ponentes?|agenda|programa|program|faculty|keynotes?)\s*[-–—|:]\s*/i;

const NEWS_OUTLET_SUFFIX_RE =
  /\s*[-–—|]\s*(?:Revista\s+Magazzine|Poder\s+y\s+Finanzas|El\s+Economista|Reforma|Milenio|Expansi[oó]n|Forbes|Business\s+Wire|PR\s+Newswire)\s*$/i;

const DUPLICATE_PHRASE_RE = /^(.{8,}?)\s*[-–—|:]\s*(?:en\s*[-–—|:]\s*)?\1$/i;

const PIPE_TAGLINE_RE = /\s*[|·•]\s*.{8,}$/;

/** Known association / event acronyms preserved under Proper Case. */
export const ASSOCIATION_DISPLAY_ACRONYMS = Object.freeze({
  anade: "ANADE",
  canaco: "CANACO",
  amexcap: "AMEXCAP",
  icuh: "ICUH",
  lasa: "LASA",
  andellac: "ANDELLAC",
  amrob: "AMRob",
  comrob: "COMRob",
  smne: "SMNE",
  cdmx: "CDMX",
  gtf: "GTF",
  mpi: "MPI",
  pco: "PCO",
});

/**
 * Strip speaker/agenda webpage headings, news outlet suffixes, and recover a
 * cleaner event label without inventing a new official name.
 */
export function normalizeAssociationEventDisplayName(raw = "") {
  let name = text(raw);
  if (!name) return { name: "", changed: false, prior: null, reason: null };
  const before = name;
  name = name.replace(SPEAKER_HEADING_RE, "");
  name = name.replace(NEWS_OUTLET_SUFFIX_RE, "");
  // "Cumbre AMEXCAP - en - Cumbre AMEXCAP"
  const dup = name.match(DUPLICATE_PHRASE_RE);
  if (dup) name = text(dup[1]);
  // Drop trailing SEO taglines after pipe when left side looks like an event/org name
  if (PIPE_TAGLINE_RE.test(name) && name.split(/\s*[|·•]\s*/)[0].length >= 4) {
    const left = text(name.split(/\s*[|·•]\s*/)[0]);
    if (left.length >= 4 && left.length < name.length) name = left;
  }
  // Headline-style Spanish news leads → keep core event when pattern is clear
  name = name
    .replace(/^de\s+productores\s+a\s+proveedores:\s*/i, "")
    .replace(/\s+busca\s+abrir\s+nuevos\s+mercados\s+en\s+cdmx\s*$/i, "")
    .replace(/^andellac\s+llevar[aá]\s+su\s+/i, "ANDELLAC ")
    .replace(/\s+a\s+la\s+cdmx\s+en\s+\w+\s*$/i, "")
    .replace(/^conmemoraci[oó]n\s+del\s+d[ií]a\s+de\s+la\s+abogac[ií]a\s*[-–—|:]\s*/i, "")
    .replace(/,\s*el\s+di[aá]logo\s+serio.+$/i, "");
  name = text(name);
  // Collapse ALL-CAPS headlines to readable title case while preserving acronyms
  if (name.length > 12 && name === name.toUpperCase() && /[A-ZÁÉÍÓÚÑ]{4,}/.test(name)) {
    name = applyAssociationDisplayCasing(name);
  } else {
    name = applyAssociationDisplayCasing(name);
  }
  return {
    name: name || before,
    changed: name !== before,
    prior: name !== before ? before : null,
    reason: name !== before ? "normalized_source_headline_or_heading" : null,
    sourcePageHeadline: before,
  };
}

/**
 * Proper-case display with acronym / accent preservation (no CSS capitalize).
 * Do NOT use JS \\b word boundaries — they break on accented Spanish letters
 * (e.g. "Médicos" → "Mé" + "Dicos").
 */
export function applyAssociationDisplayCasing(raw = "") {
  const s = text(raw);
  if (!s) return s;
  // Already mixed-case short titles that are not ALL CAPS / news headlines — keep.
  if (
    s.length <= 90 &&
    /[a-záéíóúñ]/.test(s) &&
    /[A-ZÁÉÍÓÚÑ]/.test(s) &&
    s !== s.toUpperCase() &&
    !NEWS_OUTLET_SUFFIX_RE.test(s) &&
    !/llevar[aá]\s+su|busca abrir|- en -/i.test(s)
  ) {
    // Only force known acronym tokens upward; leave the rest intact.
    return s
      .split(/(\s+|[-–—|:/,()]+)/)
      .map((tok) => {
        const lower = tok.toLowerCase();
        if (ASSOCIATION_DISPLAY_ACRONYMS[lower]) return ASSOCIATION_DISPLAY_ACRONYMS[lower];
        return tok;
      })
      .join("");
  }
  // ALL-CAPS or headline cleanup — tokenise on whitespace/punctuation without \\b
  return s
    .split(/(\s+|[-–—|:/,()]+)/)
    .map((tok, idx, arr) => {
      if (!/[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(tok)) return tok;
      const lower = tok.toLowerCase();
      if (ASSOCIATION_DISPLAY_ACRONYMS[lower]) return ASSOCIATION_DISPLAY_ACRONYMS[lower];
      if (/^[A-Z]{2,8}$/.test(tok)) return tok;
      if (/^[A-Z0-9]{2,8}$/.test(tok) && /\d/.test(tok)) return tok;
      const isFirstWord = arr.slice(0, idx).every((t) => !/[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(t));
      if (/^(de|del|la|las|los|y|e|en|a|al)$/i.test(tok) && !isFirstWord) {
        return tok.toLowerCase();
      }
      return tok.charAt(0).toUpperCase() + tok.slice(1).toLowerCase();
    })
    .join("");
}

/**
 * Normalize opportunity event/org identity fields without inventing a new event.
 * Preserves original source headline separately.
 */
export function normalizeAssociationEventsOpportunityIdentity(opportunity = {}) {
  const next = { ...opportunity };
  const eventRaw =
    text(opportunity.eventName) ||
    text(opportunity.title) ||
    text(opportunity.whatIsHappening)?.slice(0, 160) ||
    "";
  const norm = normalizeAssociationEventDisplayName(eventRaw);
  if (norm.name) {
    if (norm.changed || !opportunity.canonicalEventDisplayTitle) {
      next.sourcePageHeadline =
        opportunity.sourcePageHeadline || norm.sourcePageHeadline || eventRaw;
      next._priorEventNameRaw = opportunity._priorEventNameRaw || eventRaw;
      next.eventName = norm.name;
      next.canonicalEventDisplayTitle = norm.name;
      next.displayTitle = norm.name;
      if (text(opportunity.title) === eventRaw || !text(opportunity.title)) {
        next.title = norm.name;
      }
      next.eventIdentityNormalized = true;
      next.eventIdentityNormalizeReason = norm.reason || "display_casing";
      next.associationEventsIdentityNormalizeVersion =
        ASSOCIATION_EVENTS_IDENTITY_NORMALIZE_V1;
    }
  }

  const org = text(opportunity.organizationName || opportunity.organizer || "");
  if (
    org &&
    (/^(de productores|design:|asociación nacional de distribuidores.+anunció)/i.test(org) ||
      org.length > 120 ||
      /\|/.test(org) ||
      NEWS_OUTLET_SUFFIX_RE.test(org))
  ) {
    next.organizationIdentityNeedsReview = true;
    const cleaned = applyAssociationDisplayCasing(
      text(org.split(/\s*[|·•]\s*/)[0].replace(NEWS_OUTLET_SUFFIX_RE, "")).slice(0, 120),
    );
    if (cleaned && cleaned.length < org.length) {
      next._priorOrganizationNameRaw = org;
      next.organizationName = cleaned;
    }
  }

  return next;
}
