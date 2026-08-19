/**
 * AI Demand Positioning — Observed AI Competitive Set.
 * Compares declared comp set vs what AI actually surfaces.
 */

import { roundAdpPercent } from "../format-percent.js";
import {
  canonicalizeCompetitorName,
  matchesDeclaredComp,
} from "./competitor-name-resolution.js";

export function computeCompetitiveSet(observations, propertyProfile) {
  const competitorCounts = {};
  const competitorScenarios = {};
  const market = propertyProfile?.market || "";

  const canonicalNames = {};
  function canonicalize(name) {
    const resolved = canonicalizeCompetitorName(name, { market }) || name;
    const key = resolved.toLowerCase()
      .replace(/\b(hotel|resort|inn|suites?|lodge|club|the|a|an|by|at)\b/g, "")
      .replace(/[^a-z0-9]/g, "");
    if (!canonicalNames[key]) canonicalNames[key] = resolved;
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
    return declared.some((d) => matchesDeclaredComp(observedName, d));
  }

  const inDeclared = observed.filter((c) => matchesDeclared(c.name));
  const notInDeclared = observed.filter((c) => !matchesDeclared(c.name));
  const declaredNotObserved = declared.filter(
    (d) => !observed.some((o) => matchesDeclaredComp(o.name, d)),
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
      ? roundAdpPercent((inDeclared.length / declared.length) * 100)
      : 0,
  };
}
