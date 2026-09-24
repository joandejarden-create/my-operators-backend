/**
 * Bounded same-domain staff / leadership / events-team crawler for Native WHO V3.
 * Does not crawl whole sites — path/anchor vocabulary + depth limit.
 */

import {
  fetchResearchPage,
  htmlToSearchableText,
} from "../../../hotel-intelligence/room-count-research/fetch.js";
import {
  hostnameOf,
  isSocialOrAggregator,
} from "./official-domain.js";

export const STAFF_PATH_HINTS = Object.freeze([
  "staff",
  "team",
  "leadership",
  "about",
  "who-we-are",
  "whoweare",
  "contact",
  "meetings",
  "events",
  "conferences",
  "programs",
  "education",
  "housing",
  "registration",
  "convention",
  "operations",
  "directory",
  "people",
  "our-team",
  "our-staff",
]);

const ROLE_ANCHOR_RE =
  /staff|team|leadership|contact|meetings|events|conference|housing|registration|directory|about|people|program|sponsor|exhibit/i;

const MAX_LINKS_PER_DOMAIN = 12;
const MAX_PAGES_FETCH = 6;
const MAX_DEPTH = 1;

/**
 * Extract same-domain http(s) links from HTML.
 * @param {string} html
 * @param {string} baseUrl
 */
export function extractSameDomainLinks(html, baseUrl) {
  const host = hostnameOf(baseUrl);
  if (!host) return [];
  const links = [];
  const re = /href\s*=\s*["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(html)) && links.length < 80) {
    let href = m[1].trim();
    if (!href || href.startsWith("#") || href.startsWith("mailto:") || href.startsWith("tel:")) {
      continue;
    }
    try {
      const abs = new URL(href, baseUrl).href;
      if (!/^https?:/i.test(abs)) continue;
      if (isSocialOrAggregator(abs)) continue;
      if (hostnameOf(abs) !== host) continue;
      links.push(abs.split("#")[0]);
    } catch {
      /* ignore */
    }
  }
  return [...new Set(links)];
}

/**
 * Score a URL for staff/events relevance.
 * @param {string} url
 * @param {string} [anchorText]
 */
export function scoreStaffUrl(url, anchorText = "") {
  const u = String(url || "").toLowerCase();
  const a = String(anchorText || "").toLowerCase();
  let score = 0;
  for (const hint of STAFF_PATH_HINTS) {
    if (u.includes(`/${hint}`) || u.includes(`${hint}/`) || u.endsWith(hint)) {
      score += 3;
    }
    if (a.includes(hint)) score += 2;
  }
  if (ROLE_ANCHOR_RE.test(u) || ROLE_ANCHOR_RE.test(a)) score += 2;
  if (/\.pdf($|\?)/i.test(u)) score += 4;
  if (/speaker|agenda|news|blog|press|job|career|donate|shop/i.test(u)) score -= 4;
  return score;
}

/**
 * Discover candidate staff/contact/PDF URLs from official domain seeds.
 * @param {{ seeds: string[], maxPages?: number }} opts
 */
export async function crawlOfficialStaffPages(opts = {}) {
  const seeds = (opts.seeds || []).filter((u) => /^https?:/i.test(u) && !isSocialOrAggregator(u));
  const maxPages = opts.maxPages ?? MAX_PAGES_FETCH;
  const visited = new Set();
  const candidates = [];
  const pages = [];
  const pdfUrls = [];
  const linkQueue = [];

  for (const seed of seeds.slice(0, 4)) {
    linkQueue.push({ url: seed, depth: 0, score: 10 });
  }

  // Also probe common staff paths on first official host
  if (seeds[0]) {
    try {
      const origin = new URL(seeds[0]).origin;
      for (const hint of [
        "staff",
        "about/staff",
        "about/team",
        "about/leadership",
        "about/our-team",
        "contact",
        "contact-us",
        "contactus",
        "team",
        "leadership",
        "our-team",
        "our-staff",
        "meetings",
        "events",
        "conferences",
        "member-services",
        "membership",
        "partnerships",
        "business-development",
      ]) {
        linkQueue.push({
          url: `${origin}/${hint}`,
          depth: 0,
          score: scoreStaffUrl(`${origin}/${hint}`),
        });
      }
    } catch {
      /* ignore */
    }
  }

  linkQueue.sort((a, b) => b.score - a.score);

  while (linkQueue.length && pages.length < maxPages) {
    const next = linkQueue.shift();
    if (!next?.url || visited.has(next.url)) continue;
    visited.add(next.url);

    if (/\.pdf($|\?)/i.test(next.url)) {
      pdfUrls.push(next.url);
      continue;
    }

    const fetched = await fetchResearchPage(next.url, { timeoutMs: 16000 });
    if (!fetched.ok || !fetched.text) continue;

    const text = htmlToSearchableText(fetched.text)
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 9000);
    pages.push({
      url: fetched.url || next.url,
      text,
      html: fetched.text.slice(0, 200_000),
      kind: "staff_crawl",
      score: next.score,
    });

    if (next.depth >= MAX_DEPTH) continue;

    const links = extractSameDomainLinks(fetched.text, fetched.url || next.url);
    for (const link of links.slice(0, MAX_LINKS_PER_DOMAIN)) {
      if (visited.has(link)) continue;
      const sc = scoreStaffUrl(link);
      if (sc < 3) continue;
      if (/\.pdf($|\?)/i.test(link)) {
        pdfUrls.push(link);
        continue;
      }
      linkQueue.push({ url: link, depth: next.depth + 1, score: sc });
    }
    linkQueue.sort((a, b) => b.score - a.score);
    candidates.push(...links.filter((l) => scoreStaffUrl(l) >= 3).slice(0, 8));
  }

  return {
    pages,
    pdfUrls: [...new Set(pdfUrls)].slice(0, 8),
    candidateUrls: [...new Set(candidates)].slice(0, 20),
    visitedCount: visited.size,
  };
}
