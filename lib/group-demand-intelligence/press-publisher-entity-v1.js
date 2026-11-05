/**
 * Press / wire publisher entity separation V1.
 *
 * Distinguishes a press-distribution platform (Mynewsdesk, PR Newswire, etc.)
 * from the target company whose activity was published there.
 * Does not invent company contact routes or buyers.
 */

export const PRESS_PUBLISHER_ENTITY_V1 = "gdi-press-publisher-entity-v1";

const PRESS_PUBLISHER_HOST_RE =
  /(?:^|\.)(?:mynewsdesk|prnewswire|businesswire|globenewswire|einpresswire|newswire\.ca|accesswire|benzinga|cision)\.com$/i;

/** Publisher about/contact/home/team pages that are never the target company's decision route. */
const PRESS_PUBLISHER_NON_COMPANY_PATH_RE =
  /\/(?:about(?:\/contact)?|contact(?:-us)?|team|careers?|privacy|terms|cookies|login|signup|help)(?:\/|$|\?)/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function hostnameOf(url = "") {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

export function isPressPublisherHost(urlOrHost = "") {
  const raw = String(urlOrHost || "").trim();
  if (!raw) return false;
  const host = raw.includes("/") || raw.includes(":") ? hostnameOf(raw) : raw.replace(/^www\./i, "").toLowerCase();
  return PRESS_PUBLISHER_HOST_RE.test(host);
}

/**
 * True when the URL is a press-platform page that must not be treated as the
 * target company's organizational contact / decision-making route.
 */
export function isPressPublisherNonCompanyRoute(url = "") {
  const u = text(url);
  if (!u || !isPressPublisherHost(u)) return false;
  try {
    const parsed = new URL(u);
    const path = parsed.pathname || "/";
    if (path === "/" || path === "") return true;
    if (PRESS_PUBLISHER_NON_COMPANY_PATH_RE.test(path)) return true;
    // Bare publisher section roots (e.g. mynewsdesk.com/contact) without a company slug.
    if (/^\/(?:contact|about)(?:\/|$)/i.test(path)) return true;
    return false;
  } catch {
    return /mynewsdesk\.com\/(?:about\/contact|contact)|prnewswire\.com\/?$/i.test(u);
  }
}

/**
 * Press release article URLs remain valid *evidence* of the target company.
 * They are not organizational outreach routes.
 */
export function isPressReleaseEvidenceUrl(url = "") {
  const u = text(url);
  if (!u || !isPressPublisherHost(u)) return false;
  return /\/(?:pressreleases?|news-releases|news\/home|release)\b/i.test(u);
}

/**
 * Reject publisher non-company routes as organizationRouteUrl / contactUrl.
 * Preserve press-release URLs as evidence sources.
 */
export function filterPressPublisherOrgRoute(url = "") {
  const u = text(url);
  if (!u) return null;
  if (isPressPublisherNonCompanyRoute(u)) return null;
  // Even a press-release URL is not an org outreach route.
  if (isPressPublisherHost(u) && isPressReleaseEvidenceUrl(u)) return null;
  if (isPressPublisherHost(u)) {
    try {
      const path = new URL(u).pathname || "";
      if (!/\/[a-z0-9_-]{2,}\//i.test(path)) return null;
    } catch {
      return null;
    }
  }
  return u;
}

/**
 * Keep evidence URLs that support claims; drop publisher about/contact shells.
 */
export function filterEvidenceUrlsAgainstPublisherShells(sources = []) {
  const list = Array.isArray(sources) ? sources : [];
  return list.filter((s) => {
    const url = typeof s === "string" ? s : s?.url;
    if (!url) return false;
    return !isPressPublisherNonCompanyRoute(url);
  });
}

export function describePublisherEntitySeparation(url = "", targetCompany = "") {
  const org = text(targetCompany) || "the target company";
  if (!isPressPublisherHost(url)) {
    return { isPublisher: false, note: null };
  }
  return {
    isPublisher: true,
    isNonCompanyRoute: isPressPublisherNonCompanyRoute(url),
    isPressReleaseEvidence: isPressReleaseEvidenceUrl(url),
    note: `URL is a press-distribution platform page — not ${org}'s commercial decision-making organization.`,
  };
}
