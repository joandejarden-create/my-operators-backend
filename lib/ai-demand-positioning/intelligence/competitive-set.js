/**
 * AI Demand Positioning — Observed AI Competitive Set.
 * Compares declared comp set vs what AI actually surfaces.
 */

import { roundAdpPercent } from "../format-percent.js";
import { matchesDeclaredComp } from "./competitor-name-resolution.js";
import {
  resolveCustomerFacingEntity,
  mergeCustomerEntityCounts,
} from "../customer/customer-entity-resolution-v1.js";

export function computeCompetitiveSet(observations, propertyProfile) {
  const competitorScenarios = {};
  const rawEntries = [];

  for (const obs of observations) {
    for (const comp of obs.competitorsMentioned || []) {
      const resolved = resolveCustomerFacingEntity(comp, propertyProfile);
      if (!resolved.ok) continue;
      rawEntries.push({ name: comp, count: 1, profile: propertyProfile, resolved });
      const key = resolved.mergeKey;
      if (!competitorScenarios[key]) competitorScenarios[key] = new Set();
      competitorScenarios[key].add(obs.scenarioId);
    }
  }

  const merged = mergeCustomerEntityCounts(rawEntries);
  const observed = merged
    .map((row) => {
      const resolved = resolveCustomerFacingEntity(row.name, propertyProfile);
      const key = resolved.mergeKey;
      return {
        name: row.name,
        mentions: row.count,
        scenarioCount: competitorScenarios[key]?.size || 0,
        entityId: row.entityId || null,
      };
    })
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
