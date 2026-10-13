/**
 * Decode HTML entities and strip directory noise from entity display names.
 */
export function cleanEntityDisplayName(name = "") {
  return String(name || "")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&amp;/g, "&")
    .replace(/&nbsp;/g, " ")
    .replace(/&ndash;|&mdash;/g, "–")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Residual V2 noise that must not deepen or become hotel opportunities.
 */
export function isV3QueueNoise(entity = {}) {
  const n = cleanEntityDisplayName(entity.entityName || "").toLowerCase();
  if (!n || n.length < 4) return true;

  // Directory / CMS chrome — not companies
  if (
    /^(scan exhibitors|interested in exhibiting|supporters?\s*&\s*sponsors?|powered by|view all|see all|search exhibitors|filter by|exhibit with us|become an exhibitor|exhibitor login|exhibitor portal|exhibitor application|exhibitor console|explore your|explore other browsers|google chrome|mozilla firefox|apple safari|floor plan|booth map|why exhibit|how to exhibit|register now|learn more|contact us|about us|home|menu|login|sign up|follow us|share this|sponsor opportunities|agenda|overview)\b/i.test(
      n
    )
  ) {
    return true;
  }
  if (/https?:\/\//i.test(n) || /\b(chrome|firefox|safari|browser)\b/i.test(n)) return true;
  if (
    /\b(colleqt|a2z events|expocad|map your show|exhibitor portal|powered by)\b/i.test(n) &&
    !/\b(inc|llc|corp|group|ltd|company|systems|solutions)\b/i.test(n)
  ) {
    return true;
  }

  // City / region schedule labels
  if (
    /^(abbotsford|montreal|new england|d\.?c\.?|virginia|providence|boston|kansas city|north american show|exhibitor intelligence|confirmed exhibitor|exhibiting company origin|for booth contractors|list of acronyms|preliminary statement|third international conference|indigenous materials|correction in|authority to|anti-?\s*smuggling|see proclamation|signing authority|collection inventory|technical session|description instances|national historical|fritz lab|government publishing)/i.test(
      n
    )
  ) {
    return true;
  }
  if (/show schedule|exhibitor list|booth contractors|fall$|spring$|new!$|fiscal|acronyms|proclamation|signing authority|fr doc/i.test(n)) {
    return true;
  }
  // Federal register / legal PDF debris
  if (/\b(fr doc|cea|proclamation|signing authority|fiscal|inventory lynn|box\s+\d)\b/i.test(n)) {
    return true;
  }
  // Truncated sentences as "orgs"
  if (/\b(although|instances|behavior design|to the agency)\b/i.test(n)) return true;
  // Person-only First Last without corp marker
  if (/^[A-Z][a-z]+\s+[A-Z][a-z.]+$/.test(cleanEntityDisplayName(entity.entityName || ""))) {
    if (!/\b(Inc|LLC|Corp|Group|Ltd|Company|Systems|Solutions)\b/i.test(entity.entityName || "")) {
      return true;
    }
  }
  return false;
}

/**
 * Prefer exhibitor-directory entities for deep research over PDF debris.
 */
export function scoreV3QueuePriority(entity = {}) {
  let s = 0;
  const src = String(entity.sourceType || "");
  if (src === "EXHIBITOR_DIRECTORY") s += 40;
  else if (src === "SPONSOR_DIRECTORY") s += 30;
  else if (src === "PROGRAM_PDF") s += 5;
  else s += 10;
  if (entity.boothNumber) s += 15;
  if (entity.country && !/united\s*states|usa/i.test(entity.country)) s += 20;
  if (
    entity.lodgingSignalStrength === "STRONG" ||
    entity.lodgingSignalStrength === "MEDIUM"
  ) {
    s += 25;
  }
  if (/\b(Inc\.?|LLC|Corp\.?|Group|Systems|Solutions|International)\b/i.test(entity.entityName || "")) {
    s += 15;
  }
  if (isV3QueueNoise(entity)) s = -100;
  return s;
}
