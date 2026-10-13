/**
 * Native WHO V7 — official domain / event domain resolver.
 * Blind-safe: organization + event context only (no gold people/URLs).
 *
 * Requires org/event evidence on host or title/snippet — does not promote
 * every SERP hit to PRIMARY_ORG_DOMAIN (prevents Applebee's / DMV / wrong-state collisions).
 */

import { serpapiSearch } from "../../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  hostnameOf,
  classifyDomain,
  isOfficialClass,
  isSocialOrAggregator,
  DOMAIN_CLASS,
  discoverOfficialDomainsFromSerpHits,
} from "./official-domain.js";
import { expandEntityAliasesV7 } from "./entity-alias-expansion-v7.js";
import { classifySearchResultV7, RESULT_TIER } from "./search-result-classifier-v7.js";

export const DOMAIN_ROLE = Object.freeze({
  PRIMARY_ORG_DOMAIN: "PRIMARY_ORG_DOMAIN",
  EVENT_DOMAIN: "EVENT_DOMAIN",
  EVENT_SUBDOMAIN: "EVENT_SUBDOMAIN",
  THIRD_PARTY_OPERATOR_DOMAIN: "THIRD_PARTY_OPERATOR_DOMAIN",
  HOUSING_DOMAIN: "HOUSING_DOMAIN",
});

const JUNK_HOST_RE =
  /applebees|prnewswire|einpresswire|businesswire|globenewswire|txdmv|dmv\.|tripadvisor|booking\.com|expedia|hudl|tiktok|qwoted|act\.org|pa\.gov|navysports|sevenpeaks|intelliguards|bluesombrero|ballertv|gotsoccer|soccerwire|hotelducentre|hoteldefrance|haveanicelifeband|nice\.com$|traceup\.com|galaxydigital/i;

const GEO_TOKENS = [
  "maryland",
  "michigan",
  "virginia",
  "florida",
  "texas",
  "california",
  "ohio",
  "pennsylvania",
  "new york",
  "illinois",
  "georgia",
  "north carolina",
  "south carolina",
  "massachusetts",
  "washington",
  "colorado",
  "arizona",
  "bethesda",
  "montgomery",
  "potomac",
];

async function serp(q, num = 5) {
  const result = await serpapiSearch(
    { engine: "google", q, num, hl: "en", gl: "us" },
    { timeoutMs: 25000 }
  );
  if (!result.ok) return [];
  return (result.data?.organic_results || [])
    .map((r) => ({
      title: r.title || null,
      url: r.link || r.url || null,
      snippet: r.snippet || null,
    }))
    .filter((r) => r.url && !isSocialOrAggregator(r.url));
}

function compact(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function distinctiveOrgTokens(input, aliases) {
  const org = String(input.organization || "");
  const tokens = new Set();
  for (const a of aliases.orgAliases || []) {
    const c = compact(a);
    if (c.length >= 4) tokens.add(c);
  }
  for (const a of aliases.acronyms || []) {
    const c = compact(a);
    if (c.length >= 2 && c.length <= 8) tokens.add(c);
  }
  // Geographic / distinctive words from org string
  for (const w of org.toLowerCase().split(/[^a-z0-9]+/)) {
    if (
      w.length >= 4 &&
      !/^(association|conference|national|american|initiative|education|cybersecurity|youth|soccer|society|foundation|organization|institute)$/.test(
        w
      )
    ) {
      tokens.add(w);
    }
  }
  return [...tokens];
}

function orgGeoHints(input, aliases) {
  const blob = `${input.organization || ""} ${(aliases.orgAliases || []).join(" ")}`.toLowerCase();
  return GEO_TOKENS.filter((g) => blob.includes(g));
}

/**
 * Evidence that this host belongs to the target org/event — not acronym collision noise.
 */
export function hostHasOrgEvidence(host, hit, input, aliases) {
  if (!host || JUNK_HOST_RE.test(host)) return { ok: false, reason: "junk_host" };

  const blob = `${hit?.title || ""} ${hit?.snippet || ""} ${host}`.toLowerCase();
  const tokens = distinctiveOrgTokens(input, aliases);
  const hostC = compact(host);

  // Host contains org/acronym token (msysa.org, potomacsoccer.org, niceconference.org)
  const hostMatch = tokens.some((t) => t.length >= 3 && hostC.includes(t.slice(0, Math.min(12, t.length))));

  // Short acronym host collisions (nice.com CX vendor vs NICE conference)
  const shortAcr = (aliases.acronyms || []).map((a) => compact(a)).filter((a) => a.length >= 2 && a.length <= 5);
  const hostIsBareAcr =
    shortAcr.some((a) => hostC === `${a}com` || hostC === `${a}net` || hostC === `get${a}com`) ||
    /^(nice|show|nar|hits)\.com$/i.test(host);
  if (hostIsBareAcr) {
    const programmatic =
      /cyber|education|conference|expo|workshop|association|realtor|afcea|nih|nhlbi|nist|soccer|tournament|institute|chapter/i.test(
        blob
      );
    if (!programmatic) return { ok: false, reason: "acronym_commercial_collision" };
  }

  // Random .gov noise (grants.gov, edd.ca.gov) unless federal program host or strong program mention
  if (/\.gov$/i.test(host) && !/\b(nist|nih|nhlbi|cdc|fda|ed\.gov|congress)\b/i.test(host)) {
    if (!/(nice conference|cybersecurity education|national initiative)/i.test(blob)) {
      return { ok: false, reason: "unrelated_gov" };
    }
  }

  // Title/snippet mentions org alias or event core
  const aliasHit = [...(aliases.orgAliases || []), ...(aliases.eventAliases || []), ...(aliases.acronyms || [])]
    .filter((a) => String(a).length >= 3)
    .some((a) => blob.includes(String(a).toLowerCase()));

  // Geographic consistency: if org is Maryland-tied, reject Michigan-only hits
  const geos = orgGeoHints(input, aliases);
  if (geos.length) {
    const conflicting = GEO_TOKENS.filter((g) => !geos.includes(g) && (blob.includes(g) || hostC.includes(compact(g))));
    const affirming = geos.some((g) => blob.includes(g) || hostC.includes(compact(g)));
    if (conflicting.length && !affirming) {
      return { ok: false, reason: "geo_conflict" };
    }
  }

  if (hostMatch) return { ok: true, reason: "host_token", boost: 14 };
  if (aliasHit && /\.(org|edu|gov)$/i.test(host)) return { ok: true, reason: "alias_tld", boost: 10 };
  if (aliasHit) return { ok: true, reason: "alias_title", boost: 6 };
  return { ok: false, reason: "no_org_evidence" };
}

/**
 * Resolve ranked official domains for an opportunity.
 * @returns {Promise<{ domains: Array, evidence: Array, queries: string[] }>}
 */
export async function resolveOfficialDomainsV7(input = {}) {
  const aliases = expandEntityAliasesV7(input);
  const queries = [];
  const evidence = [];
  const byHost = new Map();

  function addHost(host, role, seedUrl, scoreBoost = 0, hit = null) {
    if (!host || isSocialOrAggregator(`https://${host}`)) return;
    if (host.split(".").length > 3) return;
    if (JUNK_HOST_RE.test(host)) return;

    const ev = hostHasOrgEvidence(host, hit || { title: "", snippet: "", url: seedUrl }, input, aliases);
    // Housing / operator domains may not match org tokens
    const allowWeak =
      role === DOMAIN_ROLE.HOUSING_DOMAIN || role === DOMAIN_ROLE.THIRD_PARTY_OPERATOR_DOMAIN;
    if (!ev.ok && !allowWeak) return;

    const prev = byHost.get(host);
    const entry = {
      host,
      class:
        role === DOMAIN_ROLE.HOUSING_DOMAIN || role === DOMAIN_ROLE.THIRD_PARTY_OPERATOR_DOMAIN
          ? DOMAIN_CLASS.THIRD_PARTY_OPERATOR_OFFICIAL
          : DOMAIN_CLASS.ORGANIZATION_OFFICIAL,
      role,
      seedUrl,
      score: scoreBoost + (ev.boost || 0) + (role === DOMAIN_ROLE.PRIMARY_ORG_DOMAIN ? 8 : 4),
      evidenceReason: ev.reason,
    };
    if (!prev || entry.score > prev.score) byHost.set(host, entry);
  }

  for (const url of input.eventSourceUrls || []) {
    const host = hostnameOf(url);
    if (!host) continue;
    const cls = classifyDomain(host, input);
    addHost(
      host,
      isOfficialClass(cls) ? DOMAIN_ROLE.EVENT_DOMAIN : DOMAIN_ROLE.PRIMARY_ORG_DOMAIN,
      url,
      isOfficialClass(cls) ? 8 : 2,
      { url, title: input.organization, snippet: input.opportunityName }
    );
  }

  // Prefer full org name over bare acronym to reduce collisions
  for (const org of aliases.orgAliases.slice(0, 3)) {
    const q = `"${org}" (official website OR homepage OR association OR "about us") -wikipedia -linkedin`;
    queries.push(q);
    const hits = await serp(q, 5);
    for (const h of hits) {
      const classified = classifySearchResultV7(h, input, aliases);
      if (classified.tier === RESULT_TIER.IRRELEVANT) continue;
      const host = hostnameOf(h.url);
      if (!host) continue;
      evidence.push({ query: q, ...h, tier: classified.tier });
      addHost(
        host,
        DOMAIN_ROLE.PRIMARY_ORG_DOMAIN,
        h.url,
        classified.tier === RESULT_TIER.HIGH_VALUE_WHO_SOURCE ? 8 : 4,
        h
      );
    }
  }

  // Acronym queries — require evidence gate (already in addHost)
  for (const acr of aliases.acronyms.slice(0, 2)) {
    const geo = orgGeoHints(input, aliases)[0];
    const geoClause = geo ? `"${geo}"` : "(association OR conference OR society)";
    const q = `"${acr}" ${geoClause} (official OR website OR association) -wikipedia -linkedin`;
    queries.push(q);
    const hits = await serp(q, 4);
    for (const h of hits) {
      const classified = classifySearchResultV7(h, input, aliases);
      if (classified.tier === RESULT_TIER.IRRELEVANT) continue;
      const host = hostnameOf(h.url);
      addHost(host, DOMAIN_ROLE.PRIMARY_ORG_DOMAIN, h.url, 5, h);
      evidence.push({ query: q, ...h, tier: classified.tier });
    }
  }

  for (const ev of aliases.eventAliases.slice(0, 2)) {
    const q = `"${ev}" (official OR organizer OR tournament OR conference) (contact OR staff OR website)`;
    queries.push(q);
    const hits = await serp(q, 5);
    for (const h of hits) {
      const classified = classifySearchResultV7(h, input, aliases);
      if (classified.tier === RESULT_TIER.IRRELEVANT) continue;
      const host = hostnameOf(h.url);
      addHost(
        host,
        /chapter|chapters|tournament|conference|event/i.test(host + h.url)
          ? DOMAIN_ROLE.EVENT_SUBDOMAIN
          : DOMAIN_ROLE.EVENT_DOMAIN,
        h.url,
        classified.tier === RESULT_TIER.HIGH_VALUE_WHO_SOURCE ? 7 : 3,
        h
      );
      evidence.push({ query: q, ...h, tier: classified.tier });
    }
  }

  for (const ev of aliases.eventAliases.slice(0, 1)) {
    const q = `"${ev}" (housing OR "room block" OR "stay to play" OR "event services")`;
    queries.push(q);
    const hits = await serp(q, 3);
    for (const h of hits) {
      if (!/housing|passkey|onpeak|hbc|stay.?to.?play|room.?block/i.test(`${h.title} ${h.url} ${h.snippet}`)) {
        continue;
      }
      const host = hostnameOf(h.url);
      addHost(host, DOMAIN_ROLE.HOUSING_DOMAIN, h.url, 4, h);
    }
  }

  const discovered = discoverOfficialDomainsFromSerpHits(
    evidence.map((e) => ({ url: e.url, title: e.title })),
    input
  );
  for (const d of discovered) {
    const hit = evidence.find((e) => hostnameOf(e.url) === d.host) || {
      url: d.seedUrl,
      title: d.host,
      snippet: "",
    };
    addHost(d.host, DOMAIN_ROLE.PRIMARY_ORG_DOMAIN, d.seedUrl, 6, hit);
  }

  const domains = [...byHost.values()].sort((a, b) => b.score - a.score).slice(0, 6);

  return {
    aliases,
    domains,
    evidence: evidence.slice(0, 25),
    queries: [...new Set(queries)].slice(0, 12),
  };
}
