/**
 * AI Demand Positioning — Response Parser.
 * Extracts property mentions, competitors, attributes, and sources from AI responses.
 */

import { extractAndResolveCompetitors } from "../intelligence/competitor-name-resolution.js";

/**
 * Check if target property is mentioned in a response.
 * Uses name variants and fuzzy matching.
 */
export function detectPropertyMention(response, propertyProfile) {
  if (!response) return { mentioned: false, position: null, context: null };
  const text = response.toLowerCase();
  const nameVariants = buildNameVariants(propertyProfile);

  for (const variant of nameVariants) {
    const idx = text.indexOf(variant.toLowerCase());
    if (idx !== -1) {
      const contextStart = Math.max(0, idx - 50);
      const contextEnd = Math.min(text.length, idx + variant.length + 100);
      const context = response.slice(contextStart, contextEnd).trim();
      const position = detectPosition(response, idx);
      return { mentioned: true, position, context };
    }
  }
  return { mentioned: false, position: null, context: null };
}

function buildNameVariants(profile) {
  const variants = [profile.name];
  if (profile.name.includes("&")) {
    variants.push(profile.name.replace("&", "and"));
  }
  for (const alias of profile.identityAliases || []) {
    if (alias) variants.push(String(alias));
  }
  const words = profile.name.replace(/,/g, " ").split(/\s+/).filter(Boolean);
  const genericLead = new Set(["hotel", "the", "a", "an"]);
  if (words.length > 2) {
    const two = words.slice(0, 2).join(" ");
    if (!genericLead.has(words[0].toLowerCase()) || words[1]) {
      variants.push(two);
    }
    if (words.length > 3) variants.push(words.slice(0, 3).join(" "));
  }
  if (profile.affiliation) {
    variants.push(`${profile.name}, ${profile.affiliation}`);
  }
  return [...new Set(variants.filter(Boolean))];
}

function detectPosition(response, mentionIndex) {
  const before = response.slice(0, mentionIndex);
  const numberedPattern = /(\d+)\.\s*$/;
  const match = before.match(numberedPattern);
  if (match) return parseInt(match[1], 10);

  const lines = before.split("\n");
  let numbered = 0;
  for (const line of lines) {
    if (/^\s*\d+[\.\)]\s/.test(line)) numbered++;
  }
  return numbered > 0 ? numbered + 1 : null;
}

/**
 * Extract competitor hotel names from response (resolved to specific property names).
 */
export function extractCompetitors(response, propertyProfile) {
  if (!response) return [];
  const market = propertyProfile?.market || "";
  return extractAndResolveCompetitors(response, propertyProfile, { market });
}

/**
 * Detect which property attributes AI recognizes in its response.
 */
export function extractRecognizedAttributes(response, propertyProfile) {
  if (!response) return [];
  const text = response.toLowerCase();
  const attributeKeywords = {
    // Waterstone
    waterfront: ["waterfront", "water view", "intracoastal", "lake boca", "waterway"],
    marina: ["marina", "boat", "dock", "yacht", "boating"],
    watersports: ["paddleboard", "kayak", "jet ski", "water sport", "snorkel"],
    intracoastal_waterway: ["intracoastal", "waterway", "lake boca"],
    walking_distance_beach: ["walking distance", "walk to the beach", "steps from the beach", "near the beach"],
    near_mizner_park: ["mizner park", "mizner"],
    heated_outdoor_pool: ["heated pool", "outdoor pool", "pool"],
    pet_friendly: ["pet-friendly", "pet friendly", "dog-friendly", "dogs welcome"],
    soft_brand: ["soft brand", "curio collection", "independent feel", "individuality"],
    hilton_honors: ["hilton", "honors", "hilton honors", "curio"],
    meeting_space: ["meeting", "conference", "ballroom", "event space", "banquet"],
    event_space_outdoor: ["outdoor event", "terrace event", "outdoor terrace", "outdoor venue"],
    ballroom: ["ballroom", "atlantic ballroom"],
    boutique_feel: ["boutique", "intimate", "charming", "unique character"],
    panoramic_views: ["panoramic", "panoramic view", "sweeping view"],
    private_balconies: ["balcony", "private balcony", "private terrace"],
    ev_charging: ["ev charging", "electric vehicle", "ev station"],

    // Renaissance Times Square
    times_square_location: ["times square", "heart of times square", "two times square"],
    broadway_theater_district: ["broadway", "theater district", "theatre district", "shows"],
    midtown_manhattan: ["midtown", "midtown manhattan"],
    rooftop_bar: ["rooftop", "r lounge", "rooftop bar", "rooftop lounge"],
    times_square_views: ["times square view", "view of times square", "overlooking times square"],
    business_center: ["business center", "work station"],
    marriott_bonvoy: ["marriott", "bonvoy", "marriott bonvoy", "marriott rewards"],
    walking_distance_broadway: ["walk to broadway", "walking distance to broadway", "steps from broadway", "near broadway"],
    near_central_park: ["central park", "near central park"],
    near_rockefeller_center: ["rockefeller", "rock center", "30 rock"],
    urban_lifestyle: ["urban", "lifestyle", "city lifestyle", "urban lifestyle"],
    design_forward: ["design", "modern design", "design-forward", "contemporary design", "stylish"],
    full_service: ["full-service", "full service"],
    concierge: ["concierge", "navigator"],

    // Cambridge Beaches
    private_beaches: ["private beach", "private beaches", "secluded beach", "own beach"],
    five_private_coves: ["five beach", "five cove", "5 beach", "five private", "multiple beach", "multiple cove"],
    cottage_style: ["cottage", "cottage colony", "cottage-style", "cottages"],
    oceanfront: ["oceanfront", "ocean front", "ocean view", "beachfront", "seaside"],
    full_service_spa: ["spa", "ocean spa", "spa treatment", "wellness center"],
    heated_infinity_pool: ["infinity pool", "heated pool", "pool"],
    tennis_courts: ["tennis", "tennis court"],
    kayaking: ["kayak", "kayaking"],
    snorkeling: ["snorkel", "snorkeling", "reef"],
    paddleboard: ["paddleboard", "paddle board", "stand-up paddle", "sup"],
    scuba_diving: ["scuba", "diving", "dive"],
    wedding_venue: ["wedding", "weddings", "wedding venue", "ceremony"],
    honeymoon_destination: ["honeymoon", "romantic getaway", "couples retreat"],
    adults_only: ["adults-only", "adults only", "no children", "adult only"],
    historic_property: ["historic", "heritage", "since 1947", "established", "storied"],
    bermuda_heritage: ["bermuda", "bermudian", "island heritage"],
    all_inclusive_option: ["all-inclusive", "all inclusive", "inclusive package"],
    island_resort: ["island resort", "island getaway", "tropical resort"],
  };

  const recognized = [];
  for (const [attr, keywords] of Object.entries(attributeKeywords)) {
    if (keywords.some((kw) => text.includes(kw))) {
      recognized.push(attr);
    }
  }
  return recognized;
}

/**
 * Extract source citations from response (mainly Perplexity).
 */
export function extractSources(response) {
  if (!response) return [];
  const urlPattern = /https?:\/\/[^\s\]\)]+/g;
  const urls = response.match(urlPattern) || [];
  const bracketPattern = /\[([^\]]+)\]\(([^)]+)\)/g;
  const sources = [];
  let match;
  while ((match = bracketPattern.exec(response)) !== null) {
    sources.push({ label: match[1], url: match[2] });
  }
  for (const url of urls) {
    if (!sources.find((s) => s.url === url)) {
      sources.push({ label: null, url });
    }
  }
  return sources.slice(0, 20);
}

/**
 * Parse a single observation's raw response and populate structured fields.
 */
export function parseObservation(observation, propertyProfile) {
  const response = observation.rawResponse || "";
  const mention = detectPropertyMention(response, propertyProfile);
  const competitors = extractCompetitors(response, propertyProfile);
  const attributes = mention.mentioned ? extractRecognizedAttributes(response, propertyProfile) : [];
  let sources = extractSources(response);
  // Perplexity returns citations as a separate array on the observation
  if (!sources.length && observation.providerCitations && observation.providerCitations.length) {
    sources = observation.providerCitations.map((url) => ({ label: null, url }));
  }

  return {
    ...observation,
    mentioned: mention.mentioned,
    position: mention.position,
    context: mention.context,
    competitorsMentioned: competitors,
    attributesRecognized: attributes,
    sourcesCited: sources,
    parsed: true,
  };
}

/**
 * Parse all observations in a period.
 */
export function parsePeriodObservations(period, propertyProfile) {
  period.observations = period.observations.map((obs) =>
    obs.parsed ? obs : parseObservation(obs, propertyProfile)
  );
  period.status = "PARSED";
  return period;
}
