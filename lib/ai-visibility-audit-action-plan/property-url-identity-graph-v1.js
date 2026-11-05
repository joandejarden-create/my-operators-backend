/**
 * Property URL Identity Graph V1
 *
 * Brand property pages and hotel-operated sites are tracked separately.
 * Corporate brand homepages are never treated as property-specific destinations.
 */
export const PROPERTY_URL_IDENTITY_CONTRACT = "AVA_PROPERTY_URL_IDENTITY_GRAPH_V1";

export const PROPERTY_URL_CLASSES = Object.freeze({
  BRAND_PROPERTY_PAGE: "BRAND_PROPERTY_PAGE",
  HOTEL_OWNED_SITE: "HOTEL_OWNED_SITE",
  CANONICAL_PROPERTY_URL: "CANONICAL_PROPERTY_URL",
  ALTERNATE_VERIFIED_URL: "ALTERNATE_VERIFIED_URL",
  HISTORICAL_DOMAIN: "HISTORICAL_DOMAIN",
  BOOKING_URL: "BOOKING_URL",
  UNVERIFIED_PROPERTY_URL: "UNVERIFIED_PROPERTY_URL",
});

const BRAND_HOME_HOSTS = new Set([
  "marriott.com",
  "hilton.com",
  "ihg.com",
  "hyatt.com",
  "accor.com",
  "radissonhotels.com",
  "wyndhamhotels.com",
  "choicehotels.com",
  "fiestamericana.com",
  "fiestamericanagrand.com",
]);

export function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

export function originOf(url) {
  try {
    return new URL(url).origin.toLowerCase();
  } catch {
    return "";
  }
}

export function normalizeUrlForMatch(url) {
  try {
    const u = new URL(url);
    u.hash = "";
    u.hostname = u.hostname.replace(/^www\./i, "").toLowerCase();
    let path = u.pathname.replace(/\/+$/, "") || "";
    return `${u.protocol}//${u.hostname}${path}${u.search}`.toLowerCase();
  } catch {
    return String(url || "").toLowerCase();
  }
}

/** True if URL is only a brand corporate homepage (not property-specific). */
export function isBrandCorporateHomepage(url) {
  const host = hostOf(url);
  if (!BRAND_HOME_HOSTS.has(host) && ![...BRAND_HOME_HOSTS].some((h) => host.endsWith(`.${h}`))) {
    return false;
  }
  try {
    const path = new URL(url).pathname.replace(/\/+$/, "") || "/";
    // Property pages usually include /hotels/ or a property code segment
    if (/\/hotels?\//i.test(path)) return false;
    if (/\/[a-z]{3}[a-z0-9]{2,}-/i.test(path)) return false; // marriott codes like lcgco-
    if (path === "/" || path === "") return true;
    if (/^\/(en|es|de|fr|pt)(\/|$)/i.test(path) && path.split("/").filter(Boolean).length <= 1) {
      return true;
    }
    return path.split("/").filter(Boolean).length <= 1;
  } catch {
    return false;
  }
}

export function classifyDestinationAgainstGraph(url, graph) {
  if (!url || isBrandCorporateHomepage(url)) {
    return {
      match: false,
      reason: isBrandCorporateHomepage(url)
        ? "BRAND_CORPORATE_HOMEPAGE_NOT_PROPERTY_SPECIFIC"
        : "EMPTY_URL",
    };
  }
  const norm = normalizeUrlForMatch(url);
  const host = hostOf(url);
  for (const node of graph?.urls || []) {
    if (!node?.url) continue;
    const nodeNorm = normalizeUrlForMatch(node.url);
    const nodeHost = hostOf(node.url);
    if (norm === nodeNorm || norm.startsWith(nodeNorm + "/") || nodeNorm.startsWith(norm + "/")) {
      return { match: true, urlClass: node.urlClass, matchedUrl: node.url, matchType: "EXACT_OR_PATH" };
    }
    // Hotel-owned: accept any path on same host
    if (
      node.urlClass === PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE ||
      node.urlClass === PROPERTY_URL_CLASSES.CANONICAL_PROPERTY_URL ||
      node.urlClass === PROPERTY_URL_CLASSES.ALTERNATE_VERIFIED_URL
    ) {
      if (host && host === nodeHost) {
        return { match: true, urlClass: node.urlClass, matchedUrl: node.url, matchType: "SAME_HOST" };
      }
    }
    // Brand property: require path overlap with property slug/code when present
    if (node.urlClass === PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE) {
      try {
        const nodePath = new URL(node.url).pathname.toLowerCase();
        const urlPath = new URL(url).pathname.toLowerCase();
        const slug = nodePath.split("/").filter(Boolean).slice(-1)[0];
        if (slug && slug.length > 8 && urlPath.includes(slug)) {
          return {
            match: true,
            urlClass: node.urlClass,
            matchedUrl: node.url,
            matchType: "BRAND_PROPERTY_SLUG",
          };
        }
        // Marriott hotel code in path (e.g. /hotels/pmilc-...)
        const code = (nodePath.match(/\/hotels\/([a-z0-9]+)-/i) || [])[1];
        if (code && urlPath.includes(`/${code}-`)) {
          return {
            match: true,
            urlClass: node.urlClass,
            matchedUrl: node.url,
            matchType: "BRAND_PROPERTY_CODE",
          };
        }
      } catch {
        /* ignore */
      }
    }
  }
  return { match: false, reason: "NO_PROPERTY_URL_MATCH" };
}

/**
 * Build identity graph from AVA payload + optional known URLs.
 */
export function buildPropertyUrlIdentityGraph(payload, extras = {}) {
  const propertyId = payload?.property?.propertyId || extras.propertyId || null;
  const hotelName = payload?.property?.name || extras.hotelName || null;
  const urls = [];
  const push = (url, urlClass, meta = {}) => {
    if (!url || !/^https?:\/\//i.test(url)) return;
    if (urls.some((u) => normalizeUrlForMatch(u.url) === normalizeUrlForMatch(url))) return;
    urls.push({
      url,
      urlClass,
      host: hostOf(url),
      active: meta.active !== false,
      note: meta.note || null,
      evidenceUrl: meta.evidenceUrl || url,
      lastChecked: meta.lastChecked || new Date().toISOString().slice(0, 10),
    });
  };

  const website = payload?.property?.website || extras.hotelOwnedSite || null;
  if (website) {
    const host = hostOf(website);
    const isBrandHost =
      [...BRAND_HOME_HOSTS].some((h) => host === h || host.endsWith(`.${h}`)) ||
      /fiestamericanatravelty\.com|marriott\.com|hilton\.com/i.test(host);
    if (isBrandHost && /\/hotels?\//i.test(website)) {
      push(website, PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE, {
        note: "Primary website field points at brand property page",
      });
    } else if (isBrandHost) {
      push(website, PROPERTY_URL_CLASSES.UNVERIFIED_PROPERTY_URL, {
        note: "Brand-hosted URL without clear property path",
      });
    } else {
      push(website, PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE);
      push(website, PROPERTY_URL_CLASSES.CANONICAL_PROPERTY_URL);
    }
  }

  for (const u of extras.brandPropertyPages || []) {
    push(u, PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE);
  }
  for (const u of extras.hotelOwnedSites || []) {
    push(u, PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE);
  }
  for (const u of extras.alternateVerifiedUrls || []) {
    push(u, PROPERTY_URL_CLASSES.ALTERNATE_VERIFIED_URL);
  }
  for (const u of extras.historicalDomains || []) {
    push(u, PROPERTY_URL_CLASSES.HISTORICAL_DOMAIN, {
      note: "Historical — ownership not assumed",
      active: false,
    });
  }
  for (const u of extras.bookingUrls || []) {
    push(u, PROPERTY_URL_CLASSES.BOOKING_URL);
  }

  // Pull from DCR source inventory when present
  for (const s of payload?.digitalContentReadiness?.sourceInventory || []) {
    if (!s?.url) continue;
    if (s.sourceCategory === "BRAND_PROPERTY_PAGE") {
      push(s.url, PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE, { evidenceUrl: s.evidenceUrl });
    } else if (s.sourceCategory === "HOTEL_OPERATED_WEBSITE") {
      push(s.url, PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE, { evidenceUrl: s.evidenceUrl });
    } else if (s.sourceCategory === "BOOKING_ENGINE") {
      push(s.url, PROPERTY_URL_CLASSES.BOOKING_URL, { evidenceUrl: s.evidenceUrl });
    }
  }

  return {
    contract: PROPERTY_URL_IDENTITY_CONTRACT,
    propertyId,
    hotelName,
    urls,
    brandPropertyPages: urls.filter((u) => u.urlClass === PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE),
    hotelOwnedSites: urls.filter((u) => u.urlClass === PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE),
    generatedAt: new Date().toISOString(),
  };
}

/** Destination hosts eligible for backlink matching (excludes historical unless opted in). */
export function backlinkEligibleHosts(graph, { includeHistorical = false } = {}) {
  const hosts = new Set();
  for (const u of graph?.urls || []) {
    if (!u.active && u.urlClass === PROPERTY_URL_CLASSES.HISTORICAL_DOMAIN && !includeHistorical) {
      continue;
    }
    if (
      [
        PROPERTY_URL_CLASSES.HOTEL_OWNED_SITE,
        PROPERTY_URL_CLASSES.CANONICAL_PROPERTY_URL,
        PROPERTY_URL_CLASSES.ALTERNATE_VERIFIED_URL,
        PROPERTY_URL_CLASSES.BRAND_PROPERTY_PAGE,
      ].includes(u.urlClass)
    ) {
      if (u.host) hosts.add(u.host);
    }
  }
  return [...hosts];
}
