/**
 * Controlled entity/alias expansion for Native WHO V6 discovery.
 * Blind-safe: derives aliases from opportunity context only (no gold people).
 */

/**
 * @param {{ organization?: string, opportunityName?: string, eventType?: string, segment?: string }} input
 * @returns {{ orgAliases: string[], eventAliases: string[], acronyms: string[], queryTokens: string[] }}
 */
export function expandEntityAliases(input = {}) {
  const org = String(input.organization || "").trim();
  const event = String(input.opportunityName || "").trim();
  const orgAliases = new Set();
  const eventAliases = new Set();
  const acronyms = new Set();

  if (org) {
    orgAliases.add(org);
    // Parenthetical acronym: "Futures Industry Association (FIA)"
    const paren = org.match(/\(([A-Z][A-Z0-9.&/-]{1,12})\)/);
    if (paren) {
      acronyms.add(paren[1]);
      orgAliases.add(paren[1]);
      orgAliases.add(org.replace(/\s*\([^)]*\)\s*/g, " ").replace(/\s+/g, " ").trim());
    }
    // Leading acronym: "FIA Boca" already in event; also "NIST / NICE"
    for (const part of org.split(/\s*\/\s*|\s*\|\s*/)) {
      const p = part.trim();
      if (p) orgAliases.add(p);
      const lead = p.match(/^([A-Z]{2,8})\b/);
      if (lead && lead[1].length >= 2) acronyms.add(lead[1]);
    }
  }

  if (event) {
    eventAliases.add(event);
    // Strip hotel-sales suffixes
    const stripped = event
      .replace(/\s*[—–-]\s*.+$/, "")
      .replace(/\b(official overflow block|player\s*\/\s*vip housing|stay-to-play.*?)$/i, "")
      .replace(/\s+/g, " ")
      .trim();
    if (stripped) eventAliases.add(stripped);
    // Yearless form
    const yearless = stripped.replace(/\b20\d{2}\b/g, "").replace(/\s+/g, " ").trim();
    if (yearless.length >= 8) eventAliases.add(yearless);
    // Acronyms embedded in event title
    for (const m of stripped.matchAll(/\b([A-Z]{2,8})\b/g)) {
      if (!/^(US|USA|VIP|PDF|HTML|WWW|THE|AND|FOR)$/.test(m[1])) acronyms.add(m[1]);
    }
  }

  const queryTokens = [
    ...[...orgAliases].slice(0, 4),
    ...[...eventAliases].slice(0, 3),
    ...[...acronyms].slice(0, 3),
  ].filter(Boolean);

  return {
    orgAliases: [...orgAliases].filter(Boolean).slice(0, 6),
    eventAliases: [...eventAliases].filter(Boolean).slice(0, 5),
    acronyms: [...acronyms].filter(Boolean).slice(0, 5),
    queryTokens: [...new Set(queryTokens)].slice(0, 8),
  };
}

/**
 * Role-oriented query families (Stage 4) — no person names.
 */
export function buildRoleSearchQueries(input = {}, aliases = null, officialDomains = []) {
  const a = aliases || expandEntityAliases(input);
  const org = a.orgAliases[0] || input.organization || "";
  const event = a.eventAliases[0] || input.opportunityName || "";
  const acr = a.acronyms[0] || "";
  const queries = [];

  const rolePhrases = [
    "director of meetings",
    "conference director",
    "tournament director",
    "executive director",
    "business development",
    "member services",
    "housing contact",
    "staff directory",
    "event staff",
  ];

  for (const label of [org, event, acr].filter((x) => x && String(x).length >= 2).slice(0, 3)) {
    for (const role of rolePhrases.slice(0, 6)) {
      queries.push(`"${label}" "${role}"`);
    }
    queries.push(`"${label}" (contact OR staff OR "our team") (meetings OR events OR tournament OR conference)`);
  }

  for (const d of officialDomains.slice(0, 2)) {
    const host = d.host || d;
    queries.push(
      `site:${host} (staff OR contact OR "tournament director" OR "business development" OR schedule)`
    );
    queries.push(`site:${host} ("event staff" OR "executive staff" OR "our team" OR directory)`);
  }

  return [...new Set(queries)].slice(0, 10);
}

/**
 * Stage-2 deep-link discovery queries: search engine → official domain pages.
 */
export function buildDeepLinkQueries(input = {}, aliases = null, officialDomains = []) {
  const a = aliases || expandEntityAliases(input);
  const queries = [];
  for (const d of officialDomains.slice(0, 3)) {
    const host = d.host || d;
    queries.push(`site:${host} (staff OR contact OR team OR directory)`);
    queries.push(`site:${host} (schedule OR housing OR registration OR prospectus OR "event staff")`);
    queries.push(`site:${host} (events OR conference OR forum OR tournament OR "partner-us" OR "business development")`);
    queries.push(`site:${host} filetype:pdf (contact OR housing OR prospectus OR program)`);
  }
  const org = a.orgAliases[0] || input.organization;
  if (org) {
    queries.push(`"${org}" (staff directory OR "our team" OR "event staff")`);
    queries.push(`"${org}" (exhibitor prospectus OR housing guide OR program pdf)`);
  }
  return [...new Set(queries)].slice(0, 8);
}
