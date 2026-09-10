/**
 * Process AI responses into AdpLeakAuditObservations fields.
 * Reuses ADP mention detection for subject hotel only — does not write production periods.
 */

import { detectPropertyMention } from "../execution/response-parser.js";

function extractCompetitorCandidates(text, subjectName) {
  const subject = String(subjectName || "").toLowerCase();
  const hotelLike =
    text.match(
      /\b(?:[A-Z][A-Za-z0-9'&.-]+(?:\s+[A-Z][A-Za-z0-9'&.-]+){0,4})\s+(?:Hotel|Resort|Inn|Suites|Spa)\b/g
    ) || [];
  const out = [];
  for (const raw of hotelLike) {
    const name = raw.trim();
    if (!name) continue;
    if (subject && name.toLowerCase().includes(subject.split(/\s+/)[0] || "___")) continue;
    if (!out.includes(name)) out.push(name);
    if (out.length >= 5) break;
  }
  return out;
}

function extractUrls(text) {
  const matches = String(text || "").match(/https?:\/\/[^\s)\]>"']+/g) || [];
  return [...new Set(matches)].slice(0, 8);
}

/**
 * @param {object} input
 * @param {string} input.responseText
 * @param {{ name: string, aliases?: string[] }} input.subjectProfile
 * @param {object} input.planItem — from prompt catalog (without storing full prompt)
 */
export function processObservationFromResponse(input) {
  const responseText = String(input.responseText || "");
  const profile = input.subjectProfile || { name: "" };
  const planItem = input.planItem || {};

  const mention = detectPropertyMention(responseText, profile);
  const competitors = extractCompetitorCandidates(responseText, profile.name);
  const displaced =
    !mention.mentioned && competitors.length > 0
      ? { displaced: true, name: competitors[0] }
      : { displaced: false, name: null };

  const excerpt = responseText.slice(0, 500).trim();
  const urls = extractUrls(responseText);

  return {
    provider: planItem.provider,
    demandTerritory: planItem.demandTerritoryLabel || planItem.demandTerritory,
    promptId: planItem.promptId,
    promptLabel: planItem.promptLabel,
    promptIntentSummary: planItem.promptIntentSummary,
    subjectHotelMentioned: Boolean(mention.mentioned),
    subjectMentionRank: mention.mentioned ? mention.position || 1 : null,
    competitorsMentioned: competitors,
    displacedByCompetitor: displaced.displaced,
    displacedCompetitorName: displaced.name,
    aiResponseExcerpt: excerpt,
    citedSources: [],
    sourceUrls: urls,
    notes: input.notes || "",
  };
}

/**
 * Phase 1 manual/fixture observation builder when no live provider calls are made.
 */
export function buildPhase1ManualObservations({
  hotelName,
  territoryKeys,
  providers,
  seedObservations,
  plan,
}) {
  if (Array.isArray(seedObservations) && seedObservations.length > 0) {
    return seedObservations.map((seed, idx) => {
      const planItem = plan[idx % plan.length] || plan[0] || {};
      if (seed.responseText) {
        return processObservationFromResponse({
          responseText: seed.responseText,
          subjectProfile: { name: hotelName },
          planItem: { ...planItem, ...seed },
          notes: seed.notes || "manual_seed",
        });
      }
      return {
        provider: seed.provider || planItem.provider,
        demandTerritory: seed.demandTerritory || planItem.demandTerritoryLabel,
        promptId: seed.promptId || planItem.promptId,
        promptLabel: seed.promptLabel || planItem.promptLabel,
        promptIntentSummary: seed.promptIntentSummary || planItem.promptIntentSummary,
        subjectHotelMentioned: Boolean(seed.subjectHotelMentioned),
        subjectMentionRank: seed.subjectMentionRank ?? null,
        competitorsMentioned: seed.competitorsMentioned || [],
        displacedByCompetitor: Boolean(seed.displacedByCompetitor),
        displacedCompetitorName: seed.displacedCompetitorName || null,
        aiResponseExcerpt: String(seed.aiResponseExcerpt || "").slice(0, 800),
        citedSources: seed.citedSources || [],
        sourceUrls: seed.sourceUrls || [],
        notes: seed.notes || "manual_seed",
      };
    });
  }

  // Deterministic demo observations for Phase 1 (no live provider spend)
  const out = [];
  for (const item of plan.slice(0, 12)) {
    const isPrimaryTerritory = territoryKeys[0] === item.demandTerritory;
    const mentioned = !isPrimaryTerritory && item.provider === providers[0];
    const competitor = `${hotelName.split(/\s+/)[0] || "Rival"} Grand Hotel`;
    const responseText = mentioned
      ? `${hotelName} is a solid option for travelers in this market, alongside other properties.`
      : `Travelers often consider ${competitor} and nearby alternatives for this trip type.`;
    out.push(
      processObservationFromResponse({
        responseText,
        subjectProfile: { name: hotelName },
        planItem: item,
        notes: "phase1_manual_synthetic",
      })
    );
  }
  return out;
}
