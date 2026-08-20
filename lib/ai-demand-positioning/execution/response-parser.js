/**
 * AI Demand Positioning — Response Parser.
 * Extracts property mentions, competitors, attributes, and sources from AI responses.
 */

import { extractAndResolveCompetitors } from "../intelligence/competitor-name-resolution.js";

/**
 * Check if target property is mentioned in a response.
 * Uses name variants (exact substring on normalized text) — no unrestricted fuzzy merge.
 */
export function detectPropertyMention(response, propertyProfile) {
  if (!response) return { mentioned: false, position: null, context: null, matchedVariant: null };
  const text = normalizeSubjectHaystack(response);
  const nameVariants = buildNameVariants(propertyProfile);

  for (const variant of nameVariants) {
    const needle = normalizeSubjectHaystack(variant);
    if (needle.length < 4) continue;
    const idx = text.indexOf(needle);
    if (idx !== -1) {
      // Prefer original-case context from raw response
      const rawLower = response.toLowerCase();
      const rawIdx = rawLower.indexOf(String(variant).toLowerCase());
      const useIdx = rawIdx !== -1 ? rawIdx : Math.min(idx, response.length - 1);
      const contextStart = Math.max(0, useIdx - 50);
      const contextEnd = Math.min(response.length, useIdx + String(variant).length + 100);
      const context = response.slice(contextStart, contextEnd).trim();
      const position = detectPosition(response, useIdx >= 0 ? useIdx : 0);
      return { mentioned: true, position, context, matchedVariant: variant };
    }
  }
  return { mentioned: false, position: null, context: null, matchedVariant: null };
}

/** Normalize for subject substring matching: lowercase, &→and, collapse punctuation/space. */
export function normalizeSubjectHaystack(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Governed subject name variants — exact/alias only (no fuzzy competitor merges).
 */
export function buildNameVariants(profile) {
  const variants = [];
  const add = (v) => {
    const t = String(v || "").trim();
    if (t.length >= 4) variants.push(t);
  };

  add(profile.name);
  if (profile.name?.includes("&")) add(profile.name.replace(/&/g, "and"));
  if (profile.name?.includes(" and ")) add(profile.name.replace(/\band\b/gi, "&"));

  for (const alias of profile.identityAliases || profile.aliases || []) add(alias);

  // Leading "The …"
  for (const v of [...variants]) {
    if (!/^the\s+/i.test(v)) add(`The ${v}`);
    else add(v.replace(/^the\s+/i, ""));
  }

  // "Hotel Phillips …" ↔ "The Phillips Hotel" / "Phillips Kansas City"
  const name = String(profile.name || "");
  const hotelLead = name.match(/^hotel\s+(.+)$/i);
  if (hotelLead) {
    const rest = hotelLead[1].replace(/,.*/, "").trim(); // drop affiliation clause
    add(`The ${rest.split(/\s+/).slice(0, 2).join(" ")} Hotel`);
    add(`The ${rest} Hotel`);
    add(rest);
    const firstToken = rest.split(/\s+/)[0];
    if (firstToken && firstToken.length >= 5) {
      add(`The ${firstToken} Hotel`);
      add(`${firstToken} Hotel`);
    }
  }

  const words = name.replace(/,/g, " ").split(/\s+/).filter(Boolean);
  const genericLead = new Set(["hotel", "the", "a", "an"]);
  const requiredLocationTokens = [];
  if (/times\s*square/i.test(name)) requiredLocationTokens.push("times square");
  if (/boca\s*raton/i.test(name)) requiredLocationTokens.push("boca raton");
  if (/kansas\s*city/i.test(name)) requiredLocationTokens.push("kansas city");
  if (/noho/i.test(name)) requiredLocationTokens.push("noho");

  function retainsRequiredLocation(candidate) {
    const c = normalizeSubjectHaystack(candidate);
    return requiredLocationTokens.every((tok) => c.includes(tok));
  }

  if (words.length > 2) {
    const two = words.slice(0, 2).join(" ");
    if ((!genericLead.has(words[0].toLowerCase()) || words[1]) && retainsRequiredLocation(two)) add(two);
    if (words.length > 3) {
      const three = words.slice(0, 3).join(" ");
      if (retainsRequiredLocation(three)) add(three);
    }
    const beforeComma = name.split(",")[0].trim().split(/\s+/).filter(Boolean);
    if (beforeComma.length >= 2) {
      const bc2 = beforeComma.slice(0, 2).join(" ");
      if (retainsRequiredLocation(bc2)) add(bc2);
    }
    if (beforeComma.length >= 3) {
      const bc3 = beforeComma.slice(0, 3).join(" ");
      if (retainsRequiredLocation(bc3)) add(bc3);
    }
    if (beforeComma.length >= 4 && retainsRequiredLocation(beforeComma.join(" "))) {
      add(beforeComma.join(" "));
    }
  }
  if (profile.affiliation) {
    add(`${profile.name}, ${profile.affiliation}`);
    const short = name.split(",")[0].trim();
    if (short) add(`${short}, ${profile.affiliation}`);
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
