/**
 * Recover publisher/title/type for sources that only have URLs in provider payloads.
 * No network — derive from URL + optional MD source index.
 */

const PUBLISHER_BY_HOST = Object.freeze({
  "bernews.com": "Bernews",
  "royalgazette.com": "The Royal Gazette",
  "gov.bm": "Government of Bermuda",
  "bermudalaws.bm": "Government of Bermuda",
  "cambridgebeaches.com": "Cambridge Beaches",
  "dovetailandco.com": "Dovetail + Co",
  "pyramidglobal.com": "Pyramid Global Hospitality",
  "benchmarkresortsandhotels.com": "Benchmark Resorts & Hotels",
  "hospitalitynet.org": "Hospitality Net",
  "forbes.com": "Forbes",
  "timeout.com": "Time Out",
  "wwd.com": "WWD",
  "hotel-online.com": "Hotel Online",
  "gotobermuda.com": "Go To Bermuda",
  "ehotelier.com": "eHotelier",
  "insights.ehotelier.com": "eHotelier",
  "linkedin.com": "LinkedIn",
  "offshoreleaks.icij.org": "ICIJ Offshore Leaks",
  "dnb.com": "Dun & Bradstreet",
  "facebook.com": "Facebook",
  "butterfieldgroup.com": "Butterfield",
  "tripadvisor.com": "TripAdvisor",
  "gsf-hotels.com": "Grupo Hotelero Santa Fe",
  "hyatt.com": "Hyatt",
  "hyattinclusivecollection.com": "Hyatt Inclusive Collection",
  "newsroom.hyatt.com": "Hyatt Newsroom",
  "openjaw.com": "Open Jaw",
  "travelweekly.com": "Travel Weekly",
  "hotelnewsresource.com": "Hotel News Resource",
});

const TYPE_BY_HOST = Object.freeze({
  "gov.bm": "Government / Legal",
  "bermudalaws.bm": "Government / Legal",
  "cambridgebeaches.com": "First Party — Hotel",
  "dovetailandco.com": "First Party — Owner/Sponsor",
  "pyramidglobal.com": "Operator / Portfolio",
  "benchmarkresortsandhotels.com": "Operator / Portfolio",
  "royalgazette.com": "Local Press",
  "bernews.com": "Local Press",
  "linkedin.com": "Professional Profile",
  "offshoreleaks.icij.org": "Historical Corporate Data",
  "dnb.com": "Historical Corporate Data",
  "forbes.com": "Trade / Editorial Press",
  "timeout.com": "Trade / Editorial Press",
  "wwd.com": "Trade / Editorial Press",
  "hotel-online.com": "Trade / Editorial Press",
  "hospitalitynet.org": "Trade / Editorial Press",
  "gotobermuda.com": "Destination Listing",
  "facebook.com": "First Party — Lender/Social",
  "tripadvisor.com": "Guest / Reputation",
  "gsf-hotels.com": "First Party — Owner/Operator",
  "hyatt.com": "First Party — Brand",
  "hyattinclusivecollection.com": "First Party — Brand",
  "newsroom.hyatt.com": "First Party — Brand Press",
  "openjaw.com": "Trade / Editorial Press",
  "travelweekly.com": "Trade / Editorial Press",
  "hotelnewsresource.com": "Trade / Editorial Press",
});

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function slugToTitle(slug) {
  return String(slug || "")
    .replace(/\.[a-z0-9]+$/i, "")
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function derivePublisherFromUrl(url) {
  const host = hostnameOf(url);
  if (!host) return null;
  if (PUBLISHER_BY_HOST[host]) return PUBLISHER_BY_HOST[host];
  const parts = host.split(".");
  if (parts.length >= 2) {
    const base = parts.slice(-2).join(".");
    if (PUBLISHER_BY_HOST[base]) return PUBLISHER_BY_HOST[base];
  }
  return host;
}

export function deriveSourceTypeFromUrl(url) {
  const host = hostnameOf(url);
  if (!host) return "Other";
  if (TYPE_BY_HOST[host]) return TYPE_BY_HOST[host];
  const parts = host.split(".");
  if (parts.length >= 2) {
    const base = parts.slice(-2).join(".");
    if (TYPE_BY_HOST[base]) return TYPE_BY_HOST[base];
  }
  if (/linkedin\.com\/in\//i.test(url)) return "Professional Profile";
  return "Other";
}

export function deriveTitleFromUrl(url) {
  try {
    const u = new URL(url);
    const segs = u.pathname.split("/").filter(Boolean);
    if (!segs.length) return derivePublisherFromUrl(url) || u.hostname;
    let last = segs[segs.length - 1];
    if (/^\d+$/.test(last) && segs.length > 1) last = segs[segs.length - 2];
    const title = slugToTitle(decodeURIComponent(last));
    if (title.length < 4) return derivePublisherFromUrl(url) || title;
    return title;
  } catch {
    return "Source";
  }
}

export function humanizeSourceTitle(rawTitle, url, publisher) {
  let title = String(rawTitle || "").trim();
  const pub = String(publisher || "").trim();
  const u = String(url || "");

  // TripAdvisor review slug → guest reviews
  if (/tripadvisor\.com/i.test(u) && /Hotel_Review|Reviews-/i.test(u)) {
    const m = u.match(/Reviews-([^/?#]+)/i);
    const hotel = m
      ? decodeURIComponent(m[1])
          .replace(/\.html?/gi, "")
          .replace(/[-_]+/g, " ")
          .replace(/\b\w/g, (c) => c.toUpperCase())
          .trim()
      : "Hotel";
    return `TripAdvisor — ${hotel} Guest Reviews`;
  }
  if (/gsf-hotels\.com/i.test(u) && /Corporate_Presentation|Company_Corporate/i.test(u)) {
    return "Grupo Hotelero Santa Fe — Corporate Presentation";
  }
  if (/gsf-hotels\.com/i.test(u) && /1Q26|Q1.?2026|HOTEL_1Q/i.test(u)) {
    return "Grupo Hotelero Santa Fe — Q1 2026 Results";
  }
  if (/gsf-hotels\.com/i.test(u) && /Evento_Relevante|del.?Pe/i.test(u)) {
    return "Grupo Hotelero Santa Fe — Relevant Event / Board Disclosure";
  }
  if (/hyattinclusivecollection\.com/i.test(u) && /breathless/i.test(u)) {
    return "Hyatt Inclusive Collection — Breathless Property Page";
  }
  if (/hyattinclusivecollection\.com/i.test(u) && /krystal/i.test(u)) {
    return "Hyatt Inclusive Collection — Krystal Grand Puerto Vallarta";
  }
  if (/newsroom\.hyatt\.com/i.test(u)) {
    return "Hyatt Newsroom — Brand Announcement";
  }
  if (/tradingview\.com/i.test(u)) {
    return `${pub || "TradingView"} — Financial Overview`;
  }
  if (/marketscreener\.com/i.test(u)) {
    return `${pub || "MarketScreener"} — Company Overview`;
  }

  // Strip crawler-ish numeric ID clusters from titles
  title = title
    .replace(/\bG\d{5,}\b/gi, "")
    .replace(/\bD\d{5,}\b/gi, "")
    .replace(/\bHOTEL[_\s-]?\d{4,}\b/gi, "")
    .replace(/\b\d{7,}\b/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  if (!title || title.length < 4 || /^www\./i.test(title)) {
    title = deriveTitleFromUrl(url);
  }
  if (pub && !title.toLowerCase().includes(pub.toLowerCase().slice(0, 8))) {
    // keep title as-is; publisher shown separately
  }
  return title;
}

/**
 * Parse `[n] [url](url)` lines from research MD Sources section.
 */
export function parseMarkdownSourceIndex(md) {
  const map = new Map();
  const text = String(md || "");
  const re =
    /\[(\d{1,3})\]\s*\[(https?:\/\/[^\]]+)\]\((https?:\/\/[^)]+)\)/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const n = Number(m[1]);
    const url = m[3] || m[2];
    map.set(n, {
      provider_local_number: n,
      url,
      title: deriveTitleFromUrl(url),
      publisher: derivePublisherFromUrl(url),
      source_type: deriveSourceTypeFromUrl(url),
    });
  }
  return map;
}

/**
 * Normalize slim/deep sources into customer bibliography rows.
 */
export function normalizeResearchSources(rawSources, opts = {}) {
  const mdIndex = opts.mdSourceIndex instanceof Map ? opts.mdSourceIndex : new Map();
  const seenUrl = new Set();
  const normalized = [];
  const rejected = [];
  const byInternalId = new Map();

  for (let i = 0; i < (rawSources || []).length; i += 1) {
    const s = rawSources[i] || {};
    const url = String(s.url || s.source_url || "").trim();
    const providerNum = Number(s.number) || null;
    const fromMd = providerNum && mdIndex.get(providerNum);
    let title = String(s.title || s.name || fromMd?.title || "").trim();
    if (!title || /^source\s+\d+$/i.test(title)) {
      title = url ? deriveTitleFromUrl(url) : `Untitled source ${i + 1}`;
    }
    const publisher =
      s.publisher ||
      s.domain ||
      fromMd?.publisher ||
      (url ? derivePublisherFromUrl(url) : null);
    title = humanizeSourceTitle(title, url, publisher);
    const source_type =
      s.source_type_label ||
      fromMd?.source_type ||
      (url ? deriveSourceTypeFromUrl(url) : null) ||
      s.source_type ||
      s.type ||
      "Other";

    const urlKey = url.toLowerCase();
    if (url && seenUrl.has(urlKey)) {
      rejected.push({ reason: "duplicate_url", title, url });
      continue;
    }
    if (url) seenUrl.add(urlKey);

    const number = normalized.length + 1;
    const row = {
      id: `src_${number}`,
      number,
      source_id_internal: s.source_id || s.id || null,
      provider_local_number: providerNum,
      title,
      url: url || null,
      publisher: publisher || "—",
      published_date: s.published_date || null,
      access_date: s.accessed_at || s.access_date || opts.accessDate || null,
      observed_date: opts.observedDate || null,
      domain: url ? hostnameOf(url) : null,
      authority_class: source_type,
      source_type,
      first_party: /First Party/i.test(String(source_type)),
      note: s.note || null,
    };
    normalized.push(row);
    if (row.source_id_internal) byInternalId.set(row.source_id_internal, number);
    if (providerNum) byInternalId.set(`provider_local_${providerNum}`, number);
    if (url) byInternalId.set(`url:${urlKey}`, number);
  }

  return {
    raw_count: (rawSources || []).length,
    normalized,
    rejected,
    deduped: rejected.length,
    byInternalId,
  };
}

export function mapClaimSourceIdsToDisplayNumbers(sourceIds, byInternalId) {
  const out = [];
  for (const id of sourceIds || []) {
    const s = String(id || "");
    if (/^\d{1,3}$/.test(s)) {
      const via = byInternalId.get(`provider_local_${s}`);
      if (via) out.push(via);
      continue;
    }
    if (/^https?:\/\//i.test(s)) {
      const viaUrl = byInternalId.get(`url:${s.toLowerCase()}`);
      if (viaUrl) out.push(viaUrl);
      continue;
    }
    const n = byInternalId.get(s);
    if (n) out.push(n);
  }
  return [...new Set(out)].filter((n) => n > 0).slice(0, 6);
}
