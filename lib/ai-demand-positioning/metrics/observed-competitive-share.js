/**
 * Observed competitive share — research / candidate calculations.
 * Customer publish blocked until denominator governance is certified.
 */

import { roundAdpPercent } from "../format-percent.js";
import { filterComparableObservations } from "./grain-governance.js";
import { matchesDeclaredComp } from "../intelligence/competitor-name-resolution.js";
import { canonicalizeCompetitorName } from "../intelligence/competitor-name-resolution.js";

const MIN_RECURRENCE_SCENARIOS = 2;
const MIN_GOVERNED_COMPETITORS = 3;

function canonicalKey(name, market) {
  const resolved = canonicalizeCompetitorName(name, { market }) || name;
  return resolved.toLowerCase().replace(/[^a-z0-9]/g, "");
}

export function buildCompetitorAppearanceLedger(observations, propertyProfile) {
  const market = propertyProfile?.market || "";
  const comparable = filterComparableObservations(observations);
  const ledger = [];

  for (const obs of comparable) {
    const uniqueComps = new Set();
    for (const comp of obs.competitorsMentioned || []) {
      const key = canonicalKey(comp, market);
      if (key) uniqueComps.add(canonicalizeCompetitorName(comp, { market }) || comp);
    }
    ledger.push({
      observationId: obs.observationId,
      scenarioId: obs.scenarioId,
      provider: obs.provider,
      subjectPresent: !!obs.mentioned,
      competitorsPresent: [...uniqueComps],
      uniqueHotelCount: uniqueComps.size + (obs.mentioned ? 1 : 0),
    });
  }
  return ledger;
}

export function classifyObservedCompetitors(observations, propertyProfile) {
  const market = propertyProfile?.market || "";
  const declared = propertyProfile.declaredCompSet || [];
  const scenarioCounts = {};
  const mentionCounts = {};

  for (const obs of filterComparableObservations(observations)) {
    for (const comp of obs.competitorsMentioned || []) {
      const name = canonicalizeCompetitorName(comp, { market }) || comp;
      const key = name.toLowerCase();
      mentionCounts[key] = mentionCounts[key] || { name, mentions: 0, scenarios: new Set() };
      mentionCounts[key].mentions += 1;
      mentionCounts[key].scenarios.add(obs.scenarioId);
    }
  }

  const observed = Object.values(mentionCounts)
    .map((row) => ({
      name: row.name,
      mentions: row.mentions,
      scenarioCount: row.scenarios.size,
      isDeclared: declared.some((d) => matchesDeclaredComp(row.name, d)),
    }))
    .sort((a, b) => b.scenarioCount - a.scenarioCount);

  const aiDiscovered = observed.filter((o) => !o.isDeclared && o.scenarioCount >= MIN_RECURRENCE_SCENARIOS);
  const declaredObserved = observed.filter((o) => o.isDeclared);
  const overlapCount = declared.filter((d) =>
    observed.some((o) => matchesDeclaredComp(o.name, d))
  ).length;
  const governedRelevant = [
    ...declaredObserved,
    ...aiDiscovered.filter((o) => o.scenarioCount >= MIN_RECURRENCE_SCENARIOS),
  ];

  return {
    declaredCompetitors: declared.length,
    observedAiAlternatives: aiDiscovered.length,
    overlapCount,
    aiDiscoveredCount: aiDiscovered.length,
    declaredSetCoverage:
      declared.length > 0 ? roundAdpPercent((overlapCount / declared.length) * 100) : null,
    frequentlyObservedAlternatives: aiDiscovered.slice(0, 10),
    governedRelevantCompetitors: governedRelevant.map((g) => g.name),
    observedCompetitiveShareStatus: "RESEARCH_ONLY",
  };
}

/**
 * Candidate share variants — not customer-published.
 */
export function computeObservedShareCandidates(observations, propertyProfile, propertyName) {
  const market = propertyProfile?.market || "";
  const comparable = filterComparableObservations(observations);
  const appearanceCounts = {};
  const SUBJECT_KEY = "__subject__";

  function credit(name, isSubject = false) {
    const key = isSubject ? SUBJECT_KEY : (canonicalizeCompetitorName(name, { market }) || name).toLowerCase();
    const display = isSubject ? propertyName : (canonicalizeCompetitorName(name, { market }) || name);
    appearanceCounts[key] = appearanceCounts[key] || { name: display, appearances: 0 };
    appearanceCounts[key].appearances += 1;
  }

  for (const obs of comparable) {
    const seen = new Set();
    if (obs.mentioned) {
      credit(propertyName, true);
      seen.add(SUBJECT_KEY);
    }
    for (const comp of obs.competitorsMentioned || []) {
      const cname = canonicalizeCompetitorName(comp, { market }) || comp;
      const ck = cname.toLowerCase();
      if (seen.has(ck)) continue;
      seen.add(ck);
      credit(cname);
    }
  }

  const subjectAppearances = appearanceCounts[SUBJECT_KEY]?.appearances || 0;

  function shareFor(keys) {
    const denom = keys.reduce((s, k) => s + (appearanceCounts[k]?.appearances || 0), 0);
    return denom > 0 ? roundAdpPercent((subjectAppearances / denom) * 100) : null;
  }

  const allKeys = Object.keys(appearanceCounts);
  const total = Object.values(appearanceCounts).reduce((s, r) => s + r.appearances, 0);
  const declared = propertyProfile.declaredCompSet || [];
  const declaredKeys = allKeys.filter((k) =>
    declared.some((d) => matchesDeclaredComp(appearanceCounts[k].name, d))
  );
  const governed = allKeys.filter((k) => {
    const row = appearanceCounts[k];
    const isDeclared = declared.some((d) => matchesDeclaredComp(row.name, d));
    return isDeclared || row.appearances >= MIN_RECURRENCE_SCENARIOS;
  });

  return {
    variantA_allObserved: shareFor(allKeys),
    variantB_declaredOnly: declaredKeys.length >= MIN_GOVERNED_COMPETITORS ? shareFor([...declaredKeys, SUBJECT_KEY]) : null,
    variantC_governedHybrid: governed.length >= MIN_GOVERNED_COMPETITORS ? shareFor([...governed, SUBJECT_KEY]) : null,
    totalAppearances: total,
    uniqueHotels: allKeys.length,
    recommendedVariant: "C_governed_hybrid_with_recurrence_floor",
    customerPublishAllowed: false,
  };
}
