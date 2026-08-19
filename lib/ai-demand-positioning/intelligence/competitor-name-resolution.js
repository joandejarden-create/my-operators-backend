/**
 * AI Demand Positioning — competitor property name resolution.
 * Prefer specific hotel names over brand fragments; block non-hotel entities.
 */

/** Names that must never surface as hotel competitors. */
const NON_HOTEL_EXACT = new Set([
  "the lambs club",
  "lambs club",
  "the lamb's club",
  "m&m world",
  "hershey's times square",
]);

/** Generic tokens that should not drive declared-comp fuzzy matches. */
const GENERIC_NAME_TOKENS = new Set([
  "hotel", "resort", "inn", "suites", "suite", "lodge", "the", "a", "an", "by", "at",
  "new", "york", "manhattan", "nyc", "collection", "marriott", "hilton", "hyatt",
  "kimpton", "luxury", "extended", "stay", "residence", "firmdale",
]);

/**
 * Canonical property registry — longest/specific names win over fragments.
 * Order matters: more specific patterns should appear before generic ones.
 */
export const NYC_CANONICAL_PROPERTIES = Object.freeze([
  {
    canonical: "The Chatwal, a Luxury Collection Hotel",
    match: (n) => /chatwal/i.test(n) || /^luxury collection hotel$/i.test(n) || (/luxury collection/i.test(n) && /hotel/i.test(n)),
  },
  {
    canonical: "Residence Inn by Marriott New York Manhattan/Times Square",
    match: (n) => /residence inn by marriott/i.test(n) || /^residence inn$/i.test(n),
  },
  {
    canonical: "Kimpton Muse Hotel",
    match: (n) => /kimpton muse/i.test(n),
  },
  {
    canonical: "Kimpton Hotel Eventi",
    match: (n) => /kimpton hotel eventi/i.test(n) || /kimpton eventi/i.test(n),
  },
  {
    canonical: "Kimpton Ink48 Hotel",
    match: (n) => /ink48/i.test(n) || /kimpton ink48/i.test(n),
  },
  {
    canonical: "Renaissance New York Midtown Hotel",
    match: (n) => /renaissance new york midtown/i.test(n),
  },
  {
    canonical: "Renaissance New York Times Square Hotel",
    match: (n) => /renaissance new york times square/i.test(n) || (/renaissance/i.test(n) && /times square/i.test(n)),
  },
  {
    canonical: "The Knickerbocker Hotel",
    match: (n) => /knickerbocker/i.test(n),
  },
  {
    canonical: "Crosby Street Hotel",
    match: (n) => /crosby street hotel/i.test(n),
  },
  {
    canonical: "The Bowery Hotel",
    match: (n) => /^the bowery hotel$/i.test(n) || (/bowery/i.test(n) && /hotel/i.test(n) && !/luxury collection/i.test(n)),
  },
  {
    canonical: "The NoMad Hotel",
    match: (n) => /nomad hotel/i.test(n) || /^the nomad$/i.test(n),
  },
  {
    canonical: "Ace Hotel New York",
    match: (n) => /ace hotel new york/i.test(n) || /^ace hotel$/i.test(n),
  },
  {
    canonical: "The Ludlow Hotel",
    match: (n) => /ludlow hotel/i.test(n),
  },
  {
    canonical: "The Greenwich Hotel",
    match: (n) => /greenwich hotel/i.test(n),
  },
  {
    canonical: "The Standard, High Line",
    match: (n) => /standard, high line/i.test(n) || /standard high line/i.test(n),
  },
  {
    canonical: "Soho Grand Hotel",
    match: (n) => /soho grand/i.test(n),
  },
  {
    canonical: "The Roxy Hotel",
    match: (n) => /roxy hotel/i.test(n),
  },
  {
    canonical: "The Dominick Hotel",
    match: (n) => /dominick hotel/i.test(n),
  },
  {
    canonical: "Baccarat Hotel",
    match: (n) => /baccarat hotel/i.test(n),
  },
  {
    canonical: "The Times Square EDITION",
    match: (n) => /times square edition/i.test(n) || /edition.*times square/i.test(n),
  },
  {
    canonical: "The Westin New York at Times Square",
    match: (n) => /westin new york at times square/i.test(n) || (/westin/i.test(n) && /times square/i.test(n)),
  },
  {
    canonical: "W New York - Times Square",
    match: (n) => /w new york.*times square/i.test(n),
  },
  {
    canonical: "Element New York Times Square West",
    match: (n) => /element new york times square/i.test(n),
  },
  {
    canonical: "CitizenM Times Square",
    match: (n) => /citizenm times square/i.test(n),
  },
  {
    canonical: "Hotel Edison",
    match: (n) => /^hotel edison$/i.test(n),
  },
  {
    canonical: "Marriott Marquis New York",
    match: (n) => /marriott marquis/i.test(n),
  },
  {
    canonical: "InterContinental New York Times Square",
    match: (n) => /intercontinental new york times square/i.test(n),
  },
  {
    canonical: "NOW NOW NOHO",
    match: (n) => /now now noho/i.test(n),
  },
]);

function normalizeKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/\b(hotel|resort|inn|suites?|lodge|the|a|an|by|at)\b/g, "")
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

export function isNonHotelEntity(name) {
  const low = String(name || "").trim().toLowerCase();
  if (!low) return true;
  if (NON_HOTEL_EXACT.has(low)) return true;
  // "Club" endings without Hotel/Resort/Inn are usually restaurants/venues
  if (/\bclub$/i.test(low) && !/\bhotel\b/i.test(low) && !/\bresort\b/i.test(low)) return true;
  return false;
}

export function resolveCompetitorName(rawName, { market } = {}) {
  const cleaned = String(rawName || "").trim().replace(/\s+/g, " ");
  if (!cleaned || isNonHotelEntity(cleaned)) return null;

  const registry = market === "nyc" || market === "New York City" ? NYC_CANONICAL_PROPERTIES : NYC_CANONICAL_PROPERTIES;
  for (const entry of registry) {
    if (entry.match(cleaned)) return entry.canonical;
  }

  const tooGeneric = /^(kimpton hotel|marriott hotel|hilton hotel|hyatt hotel|luxury collection hotel|residence inn)$/i;
  if (tooGeneric.test(cleaned)) return null;

  // Title-case cleanup for extracted regex hits
  if (/\b(hotel|resort|inn|suites?|lodge)\b/i.test(cleaned)) {
    return cleaned.replace(/\s+/g, " ").trim();
  }
  return null;
}

function isSubjectProperty(name, propertyProfile) {
  if (!propertyProfile?.name) return false;
  const subj = propertyProfile.name.toLowerCase();
  const low = name.toLowerCase();
  if (low.includes(subj) || subj.includes(low)) return true;
  const first = subj.split(/\s+/)[0];
  return first.length > 3 && low.startsWith(first);
}

/**
 * Extract hotel name candidates from AI response text (longest matches first).
 */
export function extractHotelNameCandidates(response) {
  if (!response) return [];
  const found = new Map();

  function addCandidate(raw, startIdx = 0) {
    const cleaned = String(raw || "")
      .trim()
      .replace(/\*\*/g, "")
      .replace(/\s+/g, " ")
      .replace(/\s*[—–-]\s+.*$/, "")
      .trim();
    if (!cleaned || cleaned.length < 4) return;
    if (!/\b(hotel|resort|inn|suites?|lodge)\b/i.test(cleaned)) return;
    const key = cleaned.toLowerCase();
    const existing = found.get(key);
    if (!existing || cleaned.length > existing.text.length) {
      found.set(key, { text: cleaned, start: startIdx, length: cleaned.length });
    }
  }

  // Markdown bold names (common in AI lists)
  const boldPattern = /\*\*([^*]+)\*\*/g;
  let m;
  while ((m = boldPattern.exec(response)) !== null) {
    if (/\b(hotel|resort|inn|suites?|lodge)\b/i.test(m[1])) addCandidate(m[1], m.index);
  }

  // Numbered / bulleted list lines — extract hotel phrase from line body
  const listLinePattern = /^\s*(?:\d+[\.\)]|\-|\*)\s+(.+)$/gim;
  while ((m = listLinePattern.exec(response)) !== null) {
    const line = m[1];
    const inlineHotel = line.match(
      /((?:The\s+)?[A-Za-z0-9'&,.\-]+(?:\s+(?:[A-Za-z0-9'&,.\-]+|a|by|at|on|in)){0,10}\s+(?:Hotel|Resort|Inn|Suites?|Lodge)(?:\s+(?:by|at|on|in)\s+[A-Za-z0-9'&,.\-\/]+(?:\s+[A-Za-z0-9'&,.\-\/]+){0,6})?)/i,
    );
    if (inlineHotel) addCandidate(inlineHotel[1], m.index);
  }

  // Inline hotel names on plain text (strip markdown first)
  const plain = response.replace(/\*\*/g, "");
  const hotelPattern = /(?:The\s+)?[A-Z][A-Za-z0-9'&,.\-]+(?:\s+(?:[A-Z][A-Za-z0-9'&,.\-]+|a|by|at|on|in)){0,10}\s+(?:Hotel|Resort|Inn|Suites?|Lodge)\b(?:\s+(?:by|at|on|in)\s+[A-Za-z0-9'&,.\-\/]+(?:\s+[A-Za-z0-9'&,.\-\/]+){0,6})?/g;
  while ((m = hotelPattern.exec(plain)) !== null) {
    addCandidate(m[0], m.index);
  }

  // Sort longest first; drop candidates fully contained in a longer one
  const candidates = [...found.values()].sort((a, b) => b.length - a.length);
  const kept = [];
  for (const c of candidates) {
    const contained = kept.some((k) => {
      const kStart = k.start;
      const kEnd = k.start + k.length;
      const cStart = c.start;
      const cEnd = c.start + c.length;
      return cStart >= kStart && cEnd <= kEnd && k.text.toLowerCase() !== c.text.toLowerCase();
    });
    if (!contained) kept.push(c);
  }
  return kept.map((c) => c.text);
}

/**
 * Resolve and dedupe competitor list for one observation.
 */
export function resolveCompetitorList(rawNames, propertyProfile, options = {}) {
  const market = options.market || propertyProfile?.market || "";
  const seen = new Set();
  const resolved = [];

  for (const raw of rawNames || []) {
    const name = resolveCompetitorName(raw, { market });
    if (!name || isSubjectProperty(name, propertyProfile)) continue;
    const key = normalizeKey(name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    resolved.push(name);
    if (resolved.length >= 15) break;
  }
  return resolved;
}

export function extractAndResolveCompetitors(response, propertyProfile, options = {}) {
  const declared = propertyProfile?.declaredCompSet || [];
  const known = [...declared];
  const fromText = extractHotelNameCandidates(response);
  const fromKnown = [];
  for (const comp of known) {
    if (response.toLowerCase().includes(comp.toLowerCase())) fromKnown.push(comp);
  }
  // Known declared names first (specific), then extracted candidates (longest-match order preserved)
  return resolveCompetitorList([...fromKnown, ...fromText], propertyProfile, options);
}

/** Stricter declared-comp match — avoids false positives on generic tokens. */
export function matchesDeclaredComp(observedName, declaredName) {
  const oLow = observedName.toLowerCase().trim();
  const dLow = declaredName.toLowerCase().trim();
  if (oLow === dLow) return true;
  if (oLow.includes(dLow) || dLow.includes(oLow)) return true;

  const tokenize = (s) => s.replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter((w) => w.length > 2 && !GENERIC_NAME_TOKENS.has(w));
  const oWords = tokenize(oLow);
  const dWords = tokenize(dLow);
  if (!oWords.length || !dWords.length) return false;
  const overlap = oWords.filter((w) => dWords.includes(w));
  return overlap.length >= 2 && overlap.length >= Math.min(oWords.length, dWords.length) * 0.6;
}

export function canonicalizeCompetitorName(name, options = {}) {
  return resolveCompetitorName(name, options) || name;
}
