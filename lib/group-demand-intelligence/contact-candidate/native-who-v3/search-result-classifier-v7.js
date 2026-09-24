/**
 * Native WHO V7 — search result classifier for crawl-budget discipline.
 */

export const RESULT_TIER = Object.freeze({
  HIGH_VALUE_WHO_SOURCE: "HIGH_VALUE_WHO_SOURCE",
  POSSIBLE_WHO_SOURCE: "POSSIBLE_WHO_SOURCE",
  EVENT_INFO_ONLY: "EVENT_INFO_ONLY",
  IRRELEVANT: "IRRELEVANT",
});

const HIGH_RE =
  /\b(staff|leadership|our team|directory|contact|organizer|tournament director|cups director|conference staff|event staff|meetings|member services|housing|registration|registrar|who we are)\b/i;

const POSSIBLE_RE =
  /\b(about|chapter|committee|board|programs?|education|events?|conference|summit|tournament|institute)\b/i;

const BAD_RE =
  /\b(speaker|keynote|sponsor|exhibitor list|newsroom|press release|wikipedia|linkedin|facebook|hotel marriott|hyatt|hilton|booking\.com|tripadvisor|hudl|waze|tourism|vehicle registration|renew (your )?vehicle|motor vehicle|driver'?s license|applebee|pr newswire|ein presswire|tiktok|instagram|youtube)\b/i;

/**
 * @param {{ url?: string, title?: string, snippet?: string }} hit
 * @param {object} input
 * @param {object} aliases
 */
export function classifySearchResultV7(hit = {}, input = {}, aliases = {}) {
  const blob = `${hit.title || ""} ${hit.url || ""} ${hit.snippet || ""}`.toLowerCase();
  if (!hit.url) return { tier: RESULT_TIER.IRRELEVANT, reasons: ["no_url"] };

  if (BAD_RE.test(blob)) {
    // Hotel venue pages for the opportunity hotel are irrelevant for WHO
    return { tier: RESULT_TIER.IRRELEVANT, reasons: ["bad_pattern"] };
  }

  // Ordinal street / city noise when query had "47th Annual"
  if (/\b\d+(st|nd|rd|th)\s+(street|st|avenue|ave|precinct|boulevard)\b/i.test(blob)) {
    return { tier: RESULT_TIER.IRRELEVANT, reasons: ["street_noise"] };
  }

  const tokens = [
    ...(aliases.orgAliases || []),
    ...(aliases.acronyms || []),
    ...(aliases.eventAliases || []),
  ]
    .map((t) => String(t).toLowerCase())
    .filter((t) => t.length >= 3);

  const hostMatch = tokens.some((t) => {
    try {
      const host = new URL(hit.url).hostname.toLowerCase();
      const compact = t.replace(/[^a-z0-9]/g, "");
      return compact.length >= 3 && host.replace(/[^a-z0-9]/g, "").includes(compact.slice(0, 12));
    } catch {
      return false;
    }
  });

  // "registration" alone is too weak (DMV / ACT noise) — require org/host affinity
  if (HIGH_RE.test(blob)) {
    if (!hostMatch && /registration|register/i.test(blob) && !/staff|leadership|contact|director|tournament|conference/i.test(blob)) {
      return { tier: RESULT_TIER.IRRELEVANT, reasons: ["registration_noise"] };
    }
    return {
      tier: hostMatch || tokens.some((t) => blob.includes(t))
        ? RESULT_TIER.HIGH_VALUE_WHO_SOURCE
        : RESULT_TIER.POSSIBLE_WHO_SOURCE,
      reasons: ["high_keywords", hostMatch ? "host_match" : null].filter(Boolean),
    };
  }

  if (POSSIBLE_RE.test(blob) || hostMatch) {
    return {
      tier: RESULT_TIER.POSSIBLE_WHO_SOURCE,
      reasons: [POSSIBLE_RE.test(blob) ? "possible_keywords" : null, hostMatch ? "host_match" : null].filter(
        Boolean
      ),
    };
  }

  if (/\b(event|conference|tournament|summit|meeting)\b/i.test(blob)) {
    return { tier: RESULT_TIER.EVENT_INFO_ONLY, reasons: ["event_info"] };
  }

  return { tier: RESULT_TIER.IRRELEVANT, reasons: ["no_signal"] };
}

export function rankUrlsForFetchV7(hits = [], input = {}, aliases = {}) {
  return [...hits]
    .map((h) => {
      const c = classifySearchResultV7(h, input, aliases);
      const score =
        c.tier === RESULT_TIER.HIGH_VALUE_WHO_SOURCE
          ? 30
          : c.tier === RESULT_TIER.POSSIBLE_WHO_SOURCE
            ? 15
            : c.tier === RESULT_TIER.EVENT_INFO_ONLY
              ? 5
              : 0;
      return { ...h, tier: c.tier, score };
    })
    .filter((h) => h.score > 0)
    .sort((a, b) => b.score - a.score);
}
