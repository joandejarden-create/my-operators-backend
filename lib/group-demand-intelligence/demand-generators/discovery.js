/**
 * Hotel-centered Demand Generator discovery queries.
 * Driven by hotel profile config — never hardcodes hotel/city/org names in logic.
 */

import { organizationTypesForHotelProfile } from "./fit-relevance.js";

function clean(s) {
  return String(s || "").trim();
}

/**
 * Build portable discovery query set from hotel profile.
 */
export function buildGeneratorDiscoveryQueries(hotelProfile = {}, opts = {}) {
  const territory =
    clean(hotelProfile.demandTerritory?.label) ||
    clean(hotelProfile.marketLabel) ||
    clean(opts.marketLabel) ||
    "local market";
  const anchors = (
    hotelProfile.commercialPriorities?.demandAnchorFocus || []
  ).map(clean).filter(Boolean);
  const segments = (
    hotelProfile.commercialPriorities?.targetSegments || []
  ).map(clean).filter(Boolean);
  const types = organizationTypesForHotelProfile(hotelProfile);
  const year = opts.year || new Date().getUTCFullYear() + 1;

  const queries = [];
  const push = (q, lane) => {
    if (!q) return;
    queries.push({ query: q, lane, marketLabel: territory });
  };

  for (const seg of segments.slice(0, 8)) {
    push(`"${seg}" conference OR training OR meeting ${territory} ${year}`, "segment");
    push(`"${seg}" annual meeting hotel ${territory}`, "segment_lodging");
  }
  for (const anchor of anchors.slice(0, 8)) {
    push(`"${anchor}" conference OR symposium OR training ${year}`, "anchor");
    push(`site:gov "${anchor}" workshop OR training ${territory}`, "anchor_gov");
  }
  for (const t of types.slice(0, 6)) {
    const label = t.replace(/_/g, " ").toLowerCase();
    push(`${label} ${territory} annual conference ${year}`, "org_type");
  }

  push(`${territory} association annual conference hotel block ${year}`, "generic_assoc");
  push(`${territory} youth soccer tournament stay-to-play ${year}`, "sports");
  push(`${territory} university residential program OR orientation hotel`, "university");
  push(`${territory} corporate training academy hotel`, "training");

  const max = Number(opts.maxQueries) > 0 ? Number(opts.maxQueries) : 28;
  return queries.slice(0, max);
}

/**
 * Targeted research queries once a generator is known (prefer official domain).
 */
export function buildGeneratorGuidedQueries(generator = {}, program = {}, opts = {}) {
  const name = clean(generator.organizationName);
  const domain = clean(generator.officialDomain);
  const programName = clean(program.programName);
  const year = opts.year || new Date().getUTCFullYear() + 1;
  const queries = [];
  if (domain) {
    queries.push({
      query: `site:${domain} ${programName || "conference OR training OR calendar"} ${year}`,
      lane: "official_domain",
    });
    queries.push({
      query: `site:${domain} hotel OR lodging OR housing OR registration ${year}`,
      lane: "official_lodging",
    });
  }
  if (name) {
    queries.push({
      query: `"${name}" ${programName || "annual"} ${year} hotel OR venue`,
      lane: "named_org",
    });
  }
  if (name && programName) {
    queries.push({
      query: `"${programName}" "${name}" ${year}`,
      lane: "named_program",
    });
  }
  return queries.slice(0, opts.maxQueries || 6);
}

/**
 * Compare yield shapes for founder efficiency section.
 */
export function compareSearchEfficiency(generic = {}, guided = {}) {
  const gq = Number(generic.queries) || 0;
  const qq = Number(guided.queries) || 0;
  const gQual = Number(generic.qualified) || 0;
  const qQual = Number(guided.qualified) || 0;
  const gTrue = Number(generic.trueActionable) || 0;
  const qTrue = Number(guided.trueActionable) || 0;
  return {
    generic: {
      queries: gq,
      urls: Number(generic.urls) || 0,
      qualified: gQual,
      trueActionable: gTrue,
      qualifiedPerQuery: gq ? Number((gQual / gq).toFixed(3)) : 0,
      truePerQuery: gq ? Number((gTrue / gq).toFixed(3)) : 0,
    },
    guided: {
      queries: qq,
      urls: Number(guided.urls) || 0,
      qualified: qQual,
      trueActionable: qTrue,
      qualifiedPerQuery: qq ? Number((qQual / qq).toFixed(3)) : 0,
      truePerQuery: qq ? Number((qTrue / qq).toFixed(3)) : 0,
    },
    guidedWinsOnQualifiedPerQuery:
      (qq ? qQual / qq : 0) >= (gq ? gQual / gq : 0),
    guidedWinsOnTruePerQuery: (qq ? qTrue / qq : 0) >= (gq ? gTrue / gq : 0),
  };
}

/**
 * Incremental at-bats: generator-driven NEW minus overlap with generic search ids.
 */
export function measureIncrementalAtBats({
  generatorDrivenOpportunityIds = [],
  genericSearchOpportunityIds = [],
} = {}) {
  const generic = new Set(genericSearchOpportunityIds.map(String));
  const driven = [...new Set(generatorDrivenOpportunityIds.map(String))];
  const overlap = driven.filter((id) => generic.has(id));
  const incremental = driven.filter((id) => !generic.has(id));
  return {
    generatorDrivenNew: driven.length,
    genericSearchOverlap: overlap.length,
    incrementalGeneratorAtBats: incremental.length,
    generatorDrivenIds: driven,
    overlapIds: overlap,
    incrementalIds: incremental,
  };
}
