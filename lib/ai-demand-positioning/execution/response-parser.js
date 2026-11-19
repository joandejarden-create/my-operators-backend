/**
 * AI Demand Positioning — Response Parser.
 * Extracts property mentions, competitors, attributes, and sources from AI responses.
 */

import { extractAndResolveCompetitors } from "../intelligence/competitor-name-resolution.js";
import { ATTRIBUTE_RECOGNITION_KEYWORDS } from "./attribute-recognition-keywords-v1.js";

/**
 * Check if target property is mentioned in a response.
 * Uses name variants (exact substring on normalized text) — no unrestricted fuzzy merge.
 * Rejects matches that are substrings of profile.identityConfusableExclusions (distinct entities).
 */
export function detectPropertyMention(response, propertyProfile) {
  if (!response) return { mentioned: false, position: null, context: null, matchedVariant: null };
  const text = normalizeSubjectHaystack(response);
  const nameVariants = buildNameVariants(propertyProfile);
  const confusable = (propertyProfile.identityConfusableExclusions || []).map((c) =>
    normalizeSubjectHaystack(c)
  );

  // Prefer longer variants first so "Bethesda Marriott Hotel" wins over shorter stems.
  const ordered = [...nameVariants].sort(
    (a, b) => normalizeSubjectHaystack(b).length - normalizeSubjectHaystack(a).length
  );

  for (const variant of ordered) {
    const needle = normalizeSubjectHaystack(variant);
    if (needle.length < 4) continue;
    let from = 0;
    while (from < text.length) {
      const idx = text.indexOf(needle, from);
      if (idx === -1) break;
      const spannedByConfusable = confusable.some((c) => {
        if (!c || c.length <= needle.length) return false;
        let cFrom = 0;
        while (cFrom < text.length) {
          const cIdx = text.indexOf(c, cFrom);
          if (cIdx === -1) break;
          if (idx >= cIdx && idx + needle.length <= cIdx + c.length) return true;
          cFrom = cIdx + 1;
        }
        return false;
      });
      if (spannedByConfusable) {
        from = idx + needle.length;
        continue;
      }
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
 * Find needle in a normalized haystack only on token boundaries
 * (start/end of string or whitespace). Returns index or -1.
 */
export function findTokenBoundaryIndex(haystack, needle, fromIndex = 0) {
  const text = String(haystack || "");
  const n = String(needle || "");
  if (!n) return -1;
  let from = Math.max(0, Number(fromIndex) || 0);
  while (from <= text.length - n.length) {
    const idx = text.indexOf(n, from);
    if (idx === -1) return -1;
    const beforeOk = idx === 0 || /\s/.test(text[idx - 1]);
    const afterOk = idx + n.length === text.length || /\s/.test(text[idx + n.length]);
    if (beforeOk && afterOk) return idx;
    from = idx + 1;
  }
  return -1;
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
  for (const s of profile.commonShorthand || []) add(s);
  for (const s of profile.brandShorthand || []) add(s);
  // Brand + neighborhood composites (identity canary BRAND_LOCATION_SHORTHAND)
  const brandShorthand = [...(profile.brandShorthand || [])].map((s) => String(s || "").trim()).filter(Boolean);
  const neighborhoodAliases = [...(profile.neighborhoodAliases || [])]
    .map((s) => String(s || "").trim())
    .filter(Boolean);
  for (const b of brandShorthand) {
    for (const n of neighborhoodAliases) add(`${b} ${n}`);
  }

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
  const attributeKeywords = ATTRIBUTE_RECOGNITION_KEYWORDS;

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

/** An attribute is Core-testable when the current detector can recognize it. */
export function isAttributeEligibleForCoreAiTesting(attributeId) {
  const id = String(attributeId || "").trim();
  if (!id) return false;
  const phrase = id.replace(/_/g, " ");
  return extractRecognizedAttributes(phrase, null).includes(id);
}
