/**
 * AI Demand Positioning — Observed AI Competitive Set.
 * Compares declared comp set vs what AI actually surfaces.
 */

export function computeCompetitiveSet(observations, propertyProfile) {
  const competitorCounts = {};
  const competitorScenarios = {};

  const canonicalNames = {};
  function canonicalize(name) {
    const key = name.toLowerCase()
      .replace(/\b(hotel|resort|inn|suites?|lodge|club|the|a|an|by|at)\b/g, "")
      .replace(/[^a-z0-9]/g, "");
    if (!canonicalNames[key]) canonicalNames[key] = name;
    return canonicalNames[key];
  }

  for (const obs of observations) {
    for (const comp of obs.competitorsMentioned || []) {
      const canonical = canonicalize(comp);
      competitorCounts[canonical] = (competitorCounts[canonical] || 0) + 1;
      if (!competitorScenarios[canonical]) competitorScenarios[canonical] = new Set();
      competitorScenarios[canonical].add(obs.scenarioId);
    }
  }

  const observed = Object.entries(competitorCounts)
    .map(([name, mentions]) => ({
      name,
      mentions,
      scenarioCount: competitorScenarios[name]?.size || 0,
    }))
    .sort((a, b) => b.mentions - a.mentions);

  const declared = propertyProfile.declaredCompSet || [];

  function matchesDeclared(observedName) {
    const oLow = observedName.toLowerCase();
    for (const d of declared) {
      const dLow = d.toLowerCase();
      if (dLow === oLow) return true;
      if (dLow.includes(oLow) || oLow.includes(dLow)) return true;
      // Check if key words overlap (e.g. "Boca Beach Club" matches "Beach Club")
      const oWords = oLow.replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(w => w.length > 2);
      const dWords = dLow.replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter(w => w.length > 2);
      const overlap = oWords.filter(w => dWords.includes(w));
      if (overlap.length >= 2 && overlap.length >= oWords.length * 0.6) return true;
    }
    return false;
  }

  const inDeclared = observed.filter((c) => matchesDeclared(c.name));
  const notInDeclared = observed.filter((c) => !matchesDeclared(c.name));
  const declaredNotObserved = declared.filter(
    (d) => !observed.find((o) => matchesDeclared(o.name) && (o.name.toLowerCase().includes(d.toLowerCase()) || d.toLowerCase().includes(o.name.toLowerCase())))
  );

  observed.forEach((c) => {
    c.isCore = matchesDeclared(c.name);
  });

  return {
    declaredCount: declared.length,
    observedCount: observed.length,
    observed: observed.slice(0, 15),
    surprises: notInDeclared.filter((c) => c.scenarioCount >= 2).slice(0, 5),
    inDeclaredAndObserved: inDeclared,
    declaredButNotObserved: declaredNotObserved,
    overlapRate: declared.length > 0
      ? Math.round((inDeclared.length / declared.length) * 100)
      : 0,
  };
}
