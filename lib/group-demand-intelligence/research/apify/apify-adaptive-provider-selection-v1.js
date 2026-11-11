/**
 * Adaptive discovery: pick Apify vs Exa for the next research question.
 * Does not execute providers — returns a recommendation ledger entry.
 */

import {
  listEnabledApifyActors,
  getApifyActorById,
  APIFY_ACTOR_REGISTRY,
} from "./apify-actor-registry-v1.js";

const OBJECTIVE_HINTS = [
  {
    id: "wedding_planners_directory",
    match: /wedding.?planner|the.?knot|weddingwire|destination.?wedding|social.?group/i,
    preferred: "apify",
    actors: [
      "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
      "hanamira/wedding-vendor-scraper",
      "compass/crawler-google-places",
    ],
    reason: "Structured wedding directory / local planner discovery is stronger on Apify Actors",
  },
  {
    id: "dated_public_events",
    match: /eventbrite|meetup|luma|public.?event|association.?calendar|aug(ust)?|sep(t|tember)?|oct(ober)?/i,
    preferred: "apify",
    actors: [
      "aitorsm/eventbrite",
      "solidcode/eventbrite-scraper",
      "memo23/eventbrite-scraper",
      "filip_cicvarek/meetup-scraper",
      "apify/facebook-events-scraper",
    ],
    reason:
      "Event platforms expose structured dates better via marketplace Actors; prefer absolute YYYY-MM-DD Actors (aitorsm/solidcode) for far-future need months",
  },
  {
    id: "sports_calendar",
    match: /tournament|sports|espn|competition|visiting.?team/i,
    preferred: "apify",
    actors: ["hgservices/apify-actor-espn", "apify/google-search-scraper"],
    reason: "Sports calendars benefit from structured crawlers; verify geo fit before ORS",
  },
  {
    id: "local_demand_generators",
    match: /google.?maps|local.?employer|venue.?without.?rooms|medical|university|business.?park/i,
    preferred: "apify",
    actors: ["compass/crawler-google-places", "apify/website-content-crawler"],
    reason: "Local business / venue discovery via Maps-style Actors",
  },
  {
    id: "company_background",
    match: /company.?background|buyer|decision.?maker|corporate.?account|deep.?research|qualify/i,
    preferred: "exa",
    actors: [],
    reason: "Exa Deep Research / ORS owns buyer verification and commercial qualification",
  },
  {
    id: "association_programme",
    match: /association|conference.?programme|scientific|academic.?calendar/i,
    preferred: "exa",
    actors: ["apify/website-content-crawler"],
    reason: "Association sites often need Exa discovery; Apify crawl is supplemental extraction only",
  },
];

/**
 * @param {{ question: string, researchObjective?: string, blockers?: string[], priorProviders?: string[] }} ctx
 */
export function selectNextDiscoveryProvider(ctx = {}) {
  const question = String(ctx.question || ctx.researchObjective || "");
  const prior = new Set(ctx.priorProviders || []);
  const blockers = ctx.blockers || [];

  let hit = OBJECTIVE_HINTS.find((h) => h.match.test(question));
  if (!hit) {
    hit = {
      id: "default_exa",
      preferred: "exa",
      actors: [],
      reason: "No Apify-specific pattern matched; default to Exa discovery",
    };
  }

  // Avoid blind re-runs
  if (hit.preferred === "apify" && prior.has("apify") && !/additional|another|more|expand/i.test(question)) {
    return {
      provider: "exa",
      actors: [],
      reason: "Apify already used for this thread — switch to Exa/ORS for qualification depth",
      objectiveId: hit.id,
      skipDuplicateApify: true,
      blockers,
    };
  }

  const enabled = listEnabledApifyActors();
  const actors = (hit.actors || [])
    .map((id) => enabled.find((a) => a.actorId === id) || getApifyActorById(id) || APIFY_ACTOR_REGISTRY.find((a) => a.actorId === id))
    .filter((a) => a && a.enabled);

  if (hit.preferred === "apify" && actors.length === 0) {
    return {
      provider: "exa",
      actors: [],
      reason: `Apify preferred for ${hit.id} but no enabled verified Actors — fall back to Exa`,
      objectiveId: hit.id,
      blockers: [...blockers, "APIFY_ACTORS_DISABLED_OR_MISSING"],
    };
  }

  return {
    provider: hit.preferred,
    actors: actors.map((a) => a.actorId),
    reason: hit.reason,
    objectiveId: hit.id,
    skipDuplicateApify: false,
    blockers,
  };
}

/**
 * Build investigation ledger row for ORS.
 */
export function appendInvestigationLedger(ledger = [], entry) {
  return [
    ...ledger,
    {
      at: new Date().toISOString(),
      ...entry,
    },
  ];
}

export function recommendActorsForObjective(researchObjective) {
  const sel = selectNextDiscoveryProvider({ researchObjective });
  return (sel.actors || []).map((id) => getApifyActorById(id)).filter(Boolean);
}
