/**
 * Official domain resolution for Native WHO V3.
 * Prefer EVENT_OFFICIAL / ORGANIZATION_OFFICIAL over generic SERP noise.
 */

export const DOMAIN_CLASS = Object.freeze({
  EVENT_OFFICIAL: "EVENT_OFFICIAL",
  ORGANIZATION_OFFICIAL: "ORGANIZATION_OFFICIAL",
  THIRD_PARTY_OPERATOR_OFFICIAL: "THIRD_PARTY_OPERATOR_OFFICIAL",
  VENUE_OFFICIAL: "VENUE_OFFICIAL",
  UNKNOWN: "UNKNOWN",
});

const VENUE_HOST_RE =
  /marriott|hilton|hyatt|ihg|hotel|resort|venue|convention.?center|arena|stadium/i;
const HOUSING_OPERATOR_RE =
  /onpeak|passkey|team.?travel|connections.?housing|conference.?direct|experient|cvent|goeshow/i;
const SOCIAL_RE =
  /facebook\.com|linkedin\.com|twitter\.com|x\.com|instagram\.com|youtube\.com|wikipedia\.org/i;

/**
 * @param {string} url
 */
export function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return null;
  }
}

/**
 * Classify a host relative to opportunity context.
 * @param {string} host
 * @param {{ organization?: string|null, opportunityName?: string|null, eventSourceUrls?: string[] }} ctx
 */
export function classifyDomain(host, ctx = {}) {
  if (!host || SOCIAL_RE.test(host)) return DOMAIN_CLASS.UNKNOWN;
  if (VENUE_HOST_RE.test(host)) return DOMAIN_CLASS.VENUE_OFFICIAL;
  if (HOUSING_OPERATOR_RE.test(host)) {
    return DOMAIN_CLASS.THIRD_PARTY_OPERATOR_OFFICIAL;
  }

  const seedHosts = (ctx.eventSourceUrls || [])
    .map(hostnameOf)
    .filter(Boolean);
  if (seedHosts.includes(host)) {
    // Seed URLs from opportunity discovery — treat as event/org official
    return DOMAIN_CLASS.EVENT_OFFICIAL;
  }

  const orgTokens = tokenizeOrg(ctx.organization || ctx.opportunityName || "");
  if (orgTokens.length && orgTokens.some((t) => hostMatchesOrgToken(host, t))) {
    return DOMAIN_CLASS.ORGANIZATION_OFFICIAL;
  }

  return DOMAIN_CLASS.UNKNOWN;
}

function tokenizeOrg(name) {
  const raw = String(name || "");
  const tokens = raw
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter(
      (t) =>
        t.length >= 4 &&
        !/^(association|conference|national|american|the|inc|llc|corp|organization|society|foundation)$/.test(
          t
        )
    );

  // Parenthetical / leading acronyms: (FIA), AMWA, NAMT — critical for fia.org-class domains
  for (const m of raw.matchAll(/\(([A-Za-z][A-Za-z0-9.&/-]{1,10})\)/g)) {
    const a = m[1].toLowerCase().replace(/[^a-z0-9]/g, "");
    if (a.length >= 2 && a.length <= 8) tokens.push(a);
  }
  const lead = raw.match(/^([A-Z]{2,8})\b/);
  if (lead) tokens.push(lead[1].toLowerCase());

  return [...new Set(tokens)].slice(0, 8);
}

/**
 * Promote SERP hosts that match org/acronym tokens into official domain list.
 * Blind-safe: no person names; uses organization string only.
 */
export function discoverOfficialDomainsFromSerpHits(hits = [], input = {}) {
  const tokens = tokenizeOrg(input.organization || input.opportunityName || "");
  const byHost = new Map();
  for (const h of hits) {
    const host = hostnameOf(h.url || h.link);
    if (!host || SOCIAL_RE.test(host)) continue;
    // Prefer apex domains (fia.org) over api.fia.com / fia.uk.com
    if (host.split(".").length > 2) continue;
    if (
      /investopedia|britannica|wikipedia|linkedin|facebook|\.gov\.(pk|ug|in|uk)|fia-tech/i.test(
        host
      )
    ) {
      continue;
    }
    const match = tokens.some((t) => hostMatchesOrgToken(host, t));
    if (!match) continue;
    const cls = classifyDomain(host, input);
    const promoted =
      cls === DOMAIN_CLASS.UNKNOWN ? DOMAIN_CLASS.ORGANIZATION_OFFICIAL : cls;
    const seedUrl = h.url || h.link;
    const prev = byHost.get(host);
    if (!prev || rankClass(promoted) < rankClass(prev.class)) {
      byHost.set(host, { host, class: promoted, seedUrl });
    }
  }
  // Prefer apex acronym.org / acronym.com first
  return [...byHost.values()].sort((a, b) => {
    const apexBonus = (h) => (/^[a-z0-9-]+\.(org|com)$/i.test(h) ? 0 : 1);
    return apexBonus(a.host) - apexBonus(b.host) || rankClass(a.class) - rankClass(b.class);
  });
}

/** Short acronyms must match a DNS label; long tokens may be substrings. */
function hostMatchesOrgToken(host, token) {
  const t = String(token || "").toLowerCase();
  const h = String(host || "").toLowerCase();
  if (!t || !h) return false;
  if (t.length <= 4) {
    const labels = h.split(".");
    return labels[0] === t || labels.some((l) => l === t);
  }
  return h === t || h.startsWith(`${t}.`) || h.includes(t);
}

/**
 * Resolve ranked official domains from seed URLs + org name heuristics.
 * @param {{ organization?: string|null, opportunityName?: string|null, eventSourceUrls?: string[] }} input
 */
export function resolveOfficialDomains(input = {}) {
  const byHost = new Map();

  for (const url of input.eventSourceUrls || []) {
    const host = hostnameOf(url);
    if (!host || SOCIAL_RE.test(host)) continue;
    const cls = classifyDomain(host, input);
    const prev = byHost.get(host);
    if (!prev || rankClass(cls) < rankClass(prev.class)) {
      byHost.set(host, { host, class: cls, seedUrl: url });
    }
  }

  // Promote org-matching hosts even if only seen via seed path
  for (const [host, entry] of byHost) {
    const orgTokens = tokenizeOrg(input.organization || "");
    if (
      entry.class === DOMAIN_CLASS.UNKNOWN &&
      orgTokens.some((t) => host.includes(t))
    ) {
      entry.class = DOMAIN_CLASS.ORGANIZATION_OFFICIAL;
    }
  }

  return [...byHost.values()].sort(
    (a, b) => rankClass(a.class) - rankClass(b.class)
  );
}

function rankClass(cls) {
  switch (cls) {
    case DOMAIN_CLASS.EVENT_OFFICIAL:
      return 0;
    case DOMAIN_CLASS.ORGANIZATION_OFFICIAL:
      return 1;
    case DOMAIN_CLASS.THIRD_PARTY_OPERATOR_OFFICIAL:
      return 2;
    case DOMAIN_CLASS.VENUE_OFFICIAL:
      return 3;
    default:
      return 9;
  }
}

export function isOfficialClass(cls) {
  return (
    cls === DOMAIN_CLASS.EVENT_OFFICIAL ||
    cls === DOMAIN_CLASS.ORGANIZATION_OFFICIAL ||
    cls === DOMAIN_CLASS.THIRD_PARTY_OPERATOR_OFFICIAL
  );
}

export function isSocialOrAggregator(url) {
  const host = hostnameOf(url) || "";
  return (
    SOCIAL_RE.test(host) ||
    /leadiq|zoominfo|rocketreach|crunchbase|bloomberg|wikipedia|investopedia|britannica|dictionary\.com/i.test(
      host
    ) ||
    // Structural conference-listing aggregators (multi-event directories)
    /(?:^|\.)[a-z0-9-]*conferences?\.(?:com|net|org)$/i.test(host) ||
    /(?:^|\.)(?:10times|eventbrite|allconferences?|conferencealerts?|conferenceindex|clocate)\b/i.test(
      host
    )
  );
}
