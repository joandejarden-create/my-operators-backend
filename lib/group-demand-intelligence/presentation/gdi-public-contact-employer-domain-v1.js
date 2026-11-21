/**
 * Employer-domain resolution for public contact discovery (V2).
 *
 * Prevents event microsites, expo platforms, and charity aggregators from
 * being treated as the employer's canonical email domain.
 */

export const GDI_PUBLIC_CONTACT_EMPLOYER_DOMAIN_V1 =
  "gdi-public-contact-employer-domain-v1";

export const HOST_CLASS = Object.freeze({
  EMPLOYER_OFFICIAL: "EMPLOYER_OFFICIAL",
  GOVERNMENT: "GOVERNMENT",
  EVENT_MICROSITE: "EVENT_MICROSITE",
  EXPO_PLATFORM: "EXPO_PLATFORM",
  DIRECTORY_AGGREGATOR: "DIRECTORY_AGGREGATOR",
  SOCIAL: "SOCIAL",
  UNKNOWN: "UNKNOWN",
});

const AGGREGATOR_HOST_RE =
  /(^|\.)(causeiq|guidestar|charitynavigator|bloomberg|crunchbase|zoominfo|apollo\.io|rocketreach|hunter\.io|clearbit|builtwith|similarweb|chancity)\./i;

const EXPO_PLATFORM_RE =
  /(expoplatform|cvent|eventsair|passkey|experient|imexexhibitions|conferenceharvester)/i;

const EVENT_MICROSITE_HINT_RE =
  /(20\d{2}|annual|conference|congress|summit|forum|week|symposium)/i;

const SOCIAL_RE =
  /(linkedin\.com|facebook\.com|twitter\.com|x\.com|instagram\.com|youtube\.com|google\.)/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function hostnameOf(raw) {
  const s = text(raw);
  if (!s) return null;
  try {
    const withProto = /^https?:\/\//i.test(s) ? s : `https://${s}`;
    const host = new URL(withProto).hostname.toLowerCase().replace(/^www\./, "");
    return host || null;
  } catch {
    const bare = s
      .toLowerCase()
      .replace(/^https?:\/\//, "")
      .replace(/^www\./, "")
      .split("/")[0];
    return bare.includes(".") ? bare : null;
  }
}

export function classifyContactHost(hostOrUrl) {
  const host = hostnameOf(hostOrUrl);
  if (!host) return { host: null, class: HOST_CLASS.UNKNOWN, score: 0 };
  if (SOCIAL_RE.test(host)) {
    return { host, class: HOST_CLASS.SOCIAL, score: -50 };
  }
  if (AGGREGATOR_HOST_RE.test(host)) {
    return { host, class: HOST_CLASS.DIRECTORY_AGGREGATOR, score: -40 };
  }
  if (EXPO_PLATFORM_RE.test(host)) {
    return { host, class: HOST_CLASS.EXPO_PLATFORM, score: -30 };
  }
  if (/\.gob\.mx$|\.gov(\.|$)/i.test(host)) {
    return { host, class: HOST_CLASS.GOVERNMENT, score: 90 };
  }
  // Year-branded / conference-only hosts (iscb2027.info, acvnurenalweek.org)
  const labels = host.split(".");
  const sld = labels.length >= 2 ? labels[labels.length - 2] : host;
  if (EVENT_MICROSITE_HINT_RE.test(sld) && !/\.(org|edu)$/i.test(host)) {
    return { host, class: HOST_CLASS.EVENT_MICROSITE, score: 10 };
  }
  if (/20\d{2}/.test(sld)) {
    return { host, class: HOST_CLASS.EVENT_MICROSITE, score: 15 };
  }
  if (/\.(org|edu|com|net)$/i.test(host)) {
    return { host, class: HOST_CLASS.EMPLOYER_OFFICIAL, score: 80 };
  }
  return { host, class: HOST_CLASS.UNKNOWN, score: 40 };
}

/**
 * Choose the best employer domain for email finding / site: queries.
 * Never prefers aggregators or expo platforms when a better host exists.
 */
export function resolvePreferredEmployerDomain({
  organizationDomain = null,
  organizationWebsite = null,
  sourceUrls = [],
  organizationName = null,
} = {}) {
  const candidates = [];
  const push = (raw, source) => {
    const c = classifyContactHost(raw);
    if (!c.host) return;
    candidates.push({ ...c, source });
  };
  push(organizationDomain, "organizationDomain");
  push(organizationWebsite, "organizationWebsite");
  for (const u of sourceUrls || []) push(u, "sourceUrl");

  // Acronym in org name → guess .org (only as last-resort hint, low score)
  const org = text(organizationName);
  const acronym = org.match(/\(([A-Z]{2,8})\)/);
  if (acronym) {
    candidates.push({
      host: `${acronym[1].toLowerCase()}.org`,
      class: HOST_CLASS.EMPLOYER_OFFICIAL,
      score: 25,
      source: "orgAcronymGuess",
    });
  }

  const ranked = candidates
    .filter(
      (c) =>
        c.class !== HOST_CLASS.SOCIAL &&
        c.class !== HOST_CLASS.DIRECTORY_AGGREGATOR &&
        c.class !== HOST_CLASS.EXPO_PLATFORM,
    )
    .sort((a, b) => b.score - a.score);

  const best = ranked[0] || null;
  return {
    version: GDI_PUBLIC_CONTACT_EMPLOYER_DOMAIN_V1,
    preferredDomain: best?.host || null,
    preferredClass: best?.class || null,
    rejected: candidates.filter(
      (c) =>
        c.class === HOST_CLASS.DIRECTORY_AGGREGATOR ||
        c.class === HOST_CLASS.EXPO_PLATFORM ||
        c.class === HOST_CLASS.SOCIAL,
    ),
    candidates,
  };
}

export function isWeakEmployerDomain(hostOrUrl) {
  const c = classifyContactHost(hostOrUrl);
  return (
    !c.host ||
    c.class === HOST_CLASS.DIRECTORY_AGGREGATOR ||
    c.class === HOST_CLASS.EXPO_PLATFORM ||
    c.class === HOST_CLASS.SOCIAL ||
    c.class === HOST_CLASS.EVENT_MICROSITE
  );
}

export default {
  GDI_PUBLIC_CONTACT_EMPLOYER_DOMAIN_V1,
  HOST_CLASS,
  hostnameOf,
  classifyContactHost,
  resolvePreferredEmployerDomain,
  isWeakEmployerDomain,
};
