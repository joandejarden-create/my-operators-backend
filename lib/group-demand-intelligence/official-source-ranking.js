/**
 * Official source ranking for GDI event-series discovery.
 * Prefer first-party org / institute / operator calendars over aggregators.
 */

export const SOURCE_RANK_VERSION = "gdi_official_source_rank_v3";

const AGGREGATOR_HOST_RE =
  /\b(?:eventbrite|10times|conferenceindex|allconferencealert|clocate|conferencealerts|meetup|facebook|instagram|linkedin|x\.com|twitter|youtube|tiktok|wikipedia|reddit|yelp|tripadvisor|cvent\.com\/events\/directory)\b/i;

const GOV_HOST_RE = /\.(?:gov|mil)(?:\/|:|$)/i;
const EDU_HOST_RE = /\.edu(?:\/|:|$)/i;

const FUTURE_MEETING_PATH_RE =
  /future[-_ ]?meetings?|upcoming[-_ ]?(?:meetings?|events?)|annual[-_ ]?meeting|events?(?:\/|$)|symposium|conference|calendar|save[-_ ]?the[-_ ]?date|registration|housing|accommodations?|hotel[-_ ]?(?:program|selection|block)|stay[-_ ]?to[-_ ]?play|tournaments?/i;

/** V3: elevate official future-meeting / registration / housing / institute detail pages. */
const HIGH_VALUE_OFFICIAL_PATH_RE =
  /(?:\/(?:future[-_]?meetings?|upcoming[-_]?(?:meetings?|events?)|annual[-_]?meeting|events?|calendar|registration|housing|accommodations?|hotel[-_]?(?:program|selection|block)|symposium|program|agenda)(?:\/|$|\?|#)|commonfund\.nih\.gov|ncifrederick\.cancer\.gov|natcher)/i;

/**
 * @param {string} url
 * @param {{ title?: string, snippet?: string, knownOfficialDomains?: string[] }} [ctx]
 * @returns {{ score: number, tier: string, reasons: string[] }}
 */
export function scoreOfficialSource(url, ctx = {}) {
  const reasons = [];
  let score = 0;
  let host = "";
  try {
    host = new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return { score: 0, tier: "INVALID", reasons: ["bad_url"] };
  }

  const blob = `${url} ${ctx.title || ""} ${ctx.snippet || ""}`;
  const known = (ctx.knownOfficialDomains || []).map((d) =>
    String(d).replace(/^www\./, "").toLowerCase()
  );

  if (AGGREGATOR_HOST_RE.test(host) || AGGREGATOR_HOST_RE.test(blob)) {
    score -= 40;
    reasons.push("aggregator_or_social");
  }

  if (known.some((d) => host === d || host.endsWith(`.${d}`))) {
    score += 50;
    reasons.push("known_official_domain");
  }

  if (GOV_HOST_RE.test(host) || host.endsWith(".gov")) {
    score += 35;
    reasons.push("government");
  }
  if (EDU_HOST_RE.test(host)) {
    score += 25;
    reasons.push("university");
  }
  if (/\.(?:org)(?:\/|:|$)/i.test(host) || host.endsWith(".org")) {
    score += 20;
    reasons.push("nonprofit_org");
  }

  if (FUTURE_MEETING_PATH_RE.test(url) || FUTURE_MEETING_PATH_RE.test(blob)) {
    score += 25;
    reasons.push("future_meeting_path_or_copy");
  }

  // V3: extra weight for official future-meeting / registration / housing / institute detail pages
  // Prefer .gov / known official domains — do not let generic .org path pages outrank NIH.
  if (HIGH_VALUE_OFFICIAL_PATH_RE.test(url) || HIGH_VALUE_OFFICIAL_PATH_RE.test(blob)) {
    const isGovOrKnown =
      GOV_HOST_RE.test(host) ||
      known.some((d) => host === d || host.endsWith(`.${d}`));
    score += isGovOrKnown ? 20 : 8;
    reasons.push("high_value_official_path_v3");
  }

  if (/\b(?:natcher|conference center|campus)\b/i.test(blob)) {
    score += 10;
    reasons.push("campus_venue_signal");
  }
  if (/\b(?:hotel selection|stay to play|room block|host hotel|official hotel)\b/i.test(blob)) {
    score += 15;
    reasons.push("housing_program_signal");
  }

  // Slight boost for HTTPS first-party looking paths
  if (/^https:\/\//i.test(url) && score > 0) score += 2;

  let tier = "LOW";
  if (score >= 60) tier = "PRIMARY_OFFICIAL";
  else if (score >= 35) tier = "LIKELY_OFFICIAL";
  else if (score >= 15) tier = "SECONDARY";
  else if (score < 0) tier = "DEPRIORITIZE";

  return { score, tier, reasons, host };
}

/**
 * Sort SERP organic results by official score (desc), stable by original index.
 * @param {Array<{url?: string, link?: string, title?: string, snippet?: string}>} organic
 * @param {{ knownOfficialDomains?: string[] }} [opts]
 */
export function rankOrganicByOfficialSource(organic = [], opts = {}) {
  return (organic || [])
    .map((o, index) => {
      const url = o.url || o.link || "";
      const rank = scoreOfficialSource(url, {
        title: o.title,
        snippet: o.snippet,
        knownOfficialDomains: opts.knownOfficialDomains,
      });
      return { ...o, url, officialRank: rank, _index: index };
    })
    .sort((a, b) => {
      const ds = (b.officialRank?.score || 0) - (a.officialRank?.score || 0);
      if (ds !== 0) return ds;
      return a._index - b._index;
    });
}

export function isDeprioritizedSource(url, ctx = {}) {
  return scoreOfficialSource(url, ctx).tier === "DEPRIORITIZE";
}
