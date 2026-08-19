/**
 * AI Demand Positioning — Demand White Space.
 * Identifies scenarios where no competitor dominates and property could own.
 */

export function computeWhiteSpace(observations, scenarios, propertyProfile) {
  const scenarioCompetitors = new Map();
  for (const obs of observations) {
    if (!scenarioCompetitors.has(obs.scenarioId)) {
      scenarioCompetitors.set(obs.scenarioId, { competitors: {}, propertyMentioned: false, totalObs: 0 });
    }
    const entry = scenarioCompetitors.get(obs.scenarioId);
    entry.totalObs += 1;
    if (obs.mentioned) entry.propertyMentioned = true;
    for (const comp of obs.competitorsMentioned || []) {
      entry.competitors[comp] = (entry.competitors[comp] || 0) + 1;
    }
  }

  const opportunities = [];
  for (const scenario of scenarios) {
    const data = scenarioCompetitors.get(scenario.scenarioId);
    if (!data) continue;
    if (data.propertyMentioned) continue;

    const compEntries = Object.entries(data.competitors);
    const maxMentions = compEntries.length ? Math.max(...compEntries.map(([, c]) => c)) : 0;
    const concentration = data.totalObs > 0 ? maxMentions / data.totalObs : 0;

    const attributeAlignment = computeAttributeAlignment(scenario, propertyProfile);

    let ownership;
    if (concentration < 0.3) ownership = "unowned";
    else if (concentration < 0.6) ownership = "contested";
    else ownership = "owned";

    if (ownership === "owned") continue;

    let opportunityScore;
    if (ownership === "unowned" && attributeAlignment >= 2) opportunityScore = "HIGH";
    else if (ownership === "contested" && attributeAlignment >= 2) opportunityScore = "HIGH";
    else if (attributeAlignment >= 1) opportunityScore = "MEDIUM";
    else opportunityScore = "LOW";

    if (opportunityScore === "LOW") continue;

    const topCompetitors = compEntries
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([name, count]) => ({ name, count }));

    opportunities.push({
      scenarioId: scenario.scenarioId,
      intent: scenario.intent,
      query: null,
      ownerIntentSummary: summarizeIntent(scenario),
      opportunityScore,
      currentOwnership: ownership,
      concentration: Math.round(concentration * 100),
      attributeAlignment,
      topCompetitors,
      rationale: buildRationale(scenario, propertyProfile, ownership, attributeAlignment),
    });
  }

  opportunities.sort((a, b) => {
    const scoreOrder = { HIGH: 0, MEDIUM: 1, LOW: 2 };
    return (scoreOrder[a.opportunityScore] || 2) - (scoreOrder[b.opportunityScore] || 2);
  });

  return {
    totalOpportunities: opportunities.length,
    highOpportunities: opportunities.filter((o) => o.opportunityScore === "HIGH").length,
    mediumOpportunities: opportunities.filter((o) => o.opportunityScore === "MEDIUM").length,
    opportunities: opportunities.slice(0, 10),
  };
}

function computeAttributeAlignment(scenario, profile) {
  const query = (scenario.query || "").toLowerCase();
  const attrs = profile.attributes || [];
  let alignment = 0;
  if ((query.includes("waterfront") || query.includes("water")) && attrs.includes("waterfront")) alignment++;
  if ((query.includes("marina") || query.includes("boat")) && attrs.includes("marina")) alignment++;
  if ((query.includes("meeting") || query.includes("event") || query.includes("retreat")) && attrs.includes("meeting_space")) alignment++;
  if ((query.includes("dining") || query.includes("restaurant")) && attrs.includes("fine_dining")) alignment++;
  if (query.includes("boca raton")) alignment++;
  if (query.includes("upscale") || query.includes("resort")) alignment++;
  return alignment;
}

function summarizeIntent(scenario) {
  const words = (scenario.query || "").split(/\s+/).slice(0, 10).join(" ");
  return words.length > 60 ? words.slice(0, 60) + "..." : words;
}

function buildRationale(scenario, profile, ownership, alignment) {
  const parts = [];
  if (ownership === "unowned") parts.push("No clear leader in AI responses for this demand.");
  else parts.push("Competitor presence is inconsistent. Opportunity to establish authority.");
  if (alignment >= 2) parts.push("Your property attributes align well with this demand.");
  if ((scenario.query || "").toLowerCase().includes("marina")) parts.push("Your marina is a unique differentiator here.");
  if ((scenario.query || "").toLowerCase().includes("meeting")) parts.push("Your 18,899 sq ft event space is relevant.");
  return parts.join(" ");
}
