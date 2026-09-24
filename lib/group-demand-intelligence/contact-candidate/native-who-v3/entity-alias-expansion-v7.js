/**
 * Native WHO V7 — cleaner alias / acronym expansion for discovery queries.
 * Blind-safe. No person names.
 */

import { expandEntityAliases } from "./entity-alias-expansion.js";

/**
 * Strip hotel-sales / venue / overflow marketing from opportunity titles.
 */
export function cleanEventTitle(title = "") {
  return String(title || "")
    .replace(/\s*[—–]\s*.+$/, "")
    .replace(
      /\b(official overflow block|player\s*\/\s*vip housing|stay-to-play.*?|hotel\/venue not named|location TBD|destination TBD|overflow.*?cycle|booked at .+)$/i,
      ""
    )
    .replace(/\([^)]*TBD[^)]*\)/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Prefer searchable event forms without ordinal-year noise when helpful.
 * Keeps "Potomac Memorial Tournament" from "47th Annual Potomac Memorial Tournament 2027".
 */
export function eventCoreName(title = "") {
  const cleaned = cleanEventTitle(title);
  return cleaned
    .replace(/^\d+(st|nd|rd|th)\s+/i, "")
    .replace(/\b(annual|spring|fall|winter|summer)\b/gi, " ")
    .replace(/\b20\d{2}\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function expandEntityAliasesV7(input = {}) {
  const base = expandEntityAliases({
    ...input,
    opportunityName: cleanEventTitle(input.opportunityName || ""),
  });
  const core = eventCoreName(input.opportunityName || "");
  const org = String(input.organization || "");

  const orgAliases = new Set(base.orgAliases);
  const eventAliases = new Set(base.eventAliases);
  const acronyms = new Set(base.acronyms);

  if (core && core.length >= 6) eventAliases.add(core);

  // Split "NIST / National Initiative…" style orgs
  for (const part of org.split(/\s*\/\s*/)) {
    const p = part.trim();
    if (p.length >= 3) orgAliases.add(p.replace(/\s*\([^)]*\)\s*/g, " ").trim());
  }

  // REALTORS® / trademark cleanup
  for (const a of [...orgAliases]) {
    const scrubbed = a.replace(/[®™]/g, "").replace(/\s+/g, " ").trim();
    if (scrubbed) orgAliases.add(scrubbed);
  }

  // Leading event acronym already in title (NICE, SHOW, NAR, AFCEA, MSYSA)
  const lead = cleanEventTitle(input.opportunityName || "").match(/^([A-Z]{2,8})\b/);
  if (lead) acronyms.add(lead[1]);

  // "18th Annual NICE Conference…" → keep "NICE Conference and Expo" form
  const conf = cleanEventTitle(input.opportunityName || "").match(
    /\b([A-Z]{2,8}\s+Conference(?:\s+and\s+Expo)?)\b/i
  );
  if (conf) eventAliases.add(conf[1]);

  const eventAliasesArr = [...eventAliases]
    .filter(Boolean)
    .sort((a, b) => a.length - b.length)
    .slice(0, 6);

  return {
    orgAliases: [...orgAliases].filter(Boolean).slice(0, 8),
    eventAliases: eventAliasesArr,
    acronyms: [...acronyms].filter(Boolean).slice(0, 6),
    queryTokens: [
      ...[...orgAliases].slice(0, 3),
      ...eventAliasesArr.slice(0, 3),
      ...[...acronyms].slice(0, 3),
    ],
    cleanEventTitle: cleanEventTitle(input.opportunityName || ""),
    eventCore: core,
  };
}

/**
 * V7 discovery query families — short clean labels, not hotel-sales titles.
 */
export function buildDiscoveryQueriesV7(input = {}, aliases = null, officialDomains = []) {
  const a = aliases || expandEntityAliasesV7(input);
  const queries = [];

  // Site-scoped deep links FIRST — these were previously truncated away
  for (const d of officialDomains.slice(0, 3)) {
    const host = d.host || d;
    if (!host || typeof host !== "string") continue;
    // Skip obvious non-org hosts that slipped through
    if (/\.(gov)$/i.test(host) && !/nist|nih|nhlbi|cdc|fda/i.test(host)) {
      // allow major federal program hosts only when acronym matches
    }
    queries.push(`site:${host} (staff OR leadership OR "our team" OR directory OR contact)`);
    queries.push(`site:${host} ("tournament director" OR "cups director" OR "conference director" OR meetings)`);
    queries.push(`site:${host} (events OR conferences OR tournament OR programs OR education)`);
    queries.push(`site:${host} filetype:pdf (prospectus OR program OR housing OR "annual meeting" OR guide)`);
  }

  const labels = [
    ...(a.eventAliases || []).slice(0, 3),
    ...(a.orgAliases || []).slice(0, 2),
    ...(a.acronyms || []).slice(0, 2),
  ].filter((x) => x && String(x).length >= 2);

  for (const label of [...new Set(labels)].slice(0, 4)) {
    queries.push(`"${label}" (contact OR staff OR "our team" OR organizer)`);
    queries.push(
      `"${label}" ("tournament director" OR "conference director" OR "cups director" OR "meetings director")`
    );
    queries.push(`"${label}" (housing OR registration OR "member services" OR "event management")`);
  }

  return [...new Set(queries)].slice(0, 16);
}
