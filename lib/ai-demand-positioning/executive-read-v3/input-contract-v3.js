/**
 * ADP Executive Read V3 — canonical analytical input contract.
 * Certified/publishable payload only — never presentation copy as analytical source.
 *
 * Doctrine: EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.
 */

export const ADP_EXECUTIVE_READ_INPUT_V3 = "ADP_EXECUTIVE_READ_INPUT_V3";

function asPayload(publishedOrPayload) {
  if (!publishedOrPayload) return null;
  return publishedOrPayload.payload || publishedOrPayload;
}

function extractTerritories(p) {
  const ipi = p?.intentPresenceIndex || {};
  // Governed IPI may be keyed by intent at root (business/leisure/…) or nested under byIntent.
  const byIntent =
    ipi.byIntent ||
    ipi.intents ||
    (ipi && typeof ipi === "object" && (ipi.business || ipi.leisure || ipi.family) ? ipi : {});
  const out = [];
  if (Array.isArray(byIntent)) {
    for (const t of byIntent) {
      out.push({
        intent: t.intent || t.label,
        label: t.label || t.name || t.territory || t.intent,
        rate: t.rate ?? t.myRate ?? t.subjectRatePct ?? t.presence ?? t.aiPresencePct ?? null,
        index: t.index ?? t.presenceIndex ?? null,
      });
    }
  } else if (byIntent && typeof byIntent === "object") {
    for (const [k, v] of Object.entries(byIntent)) {
      if (!v || typeof v !== "object") continue;
      // Skip nested version metadata bags
      if (k === "byIntent" || k === "intents" || k === "versions" || k === "methodology") continue;
      if (typeof v.myRate !== "number" && typeof v.rate !== "number" && typeof v.subjectRatePct !== "number" && typeof v.index !== "number") {
        continue;
      }
      out.push({
        intent: k,
        label: v.territory || v.label || k,
        rate: v.rate ?? v.myRate ?? v.subjectRatePct ?? v.presence ?? v.aiPresencePct ?? null,
        index: v.index ?? v.presenceIndex ?? null,
      });
    }
  }
  return out.filter((t) => typeof t.rate === "number" || typeof t.index === "number");
}

/**
 * Build ADP_EXECUTIVE_READ_INPUT_V3 from a certified owner/published payload.
 * @param {object} publishedOrPayload
 * @param {{ propertyId?: string, market?: string }=} meta
 */
export function buildExecutiveReadInputV3(publishedOrPayload, meta = {}) {
  const p = asPayload(publishedOrPayload);
  if (!p?.ok && !p?.property && !p?.executiveMetrics) {
    return {
      contract: ADP_EXECUTIVE_READ_INPUT_V3,
      ok: false,
      reason: "MISSING_CERTIFIED_ANALYTICAL_STATE",
    };
  }

  const er = p.executiveRead || {};
  const territories = extractTerritories(p);
  const weakT = [...territories].sort((a, b) => (a.rate ?? 0) - (b.rate ?? 0));
  const strongT = [...territories].sort((a, b) => (b.rate ?? 0) - (a.rate ?? 0));
  const gaps = (p.realityGap?.gaps || []).map((g) => ({
    label: g.label || g.attribute,
    attribute: g.attribute || g.label,
    severity: g.severity,
    recognitionRate: g.recognitionRate ?? g.rate ?? null,
  }));
  const displacement = (p.lostDemand?.displacement || []).map((d) => ({
    name: d.name,
    entityId: d.entityId,
    displacementCount: d.displacementCount ?? d.count ?? 0,
  }));

  const consideration = p.executiveMetrics?.considerationRate?.rate ?? null;
  const scenarioPresence =
    p.executiveMetrics?.scenarioPresence?.rate ?? p.demandCapture?.overallRate ?? null;
  const top3 =
    p.executiveMetrics?.rankMetrics?.topThreeAppearanceRate ??
    p.executiveMetrics?.positionMetrics?.top3Rate ??
    null;
  const numberOne =
    p.executiveMetrics?.rankMetrics?.numberOneAppearanceRate ??
    p.executiveMetrics?.positionMetrics?.numberOneRate ??
    null;
  const rankEligibleN =
    p.executiveMetrics?.rankMetrics?.rankEligibleN ??
    p.executiveMetrics?.rankMetrics?.rankEligible ??
    p.executiveMetrics?.positionMetrics?.rankEligibleObservations ??
    null;
  // Customer-narrative eligibility for Top-3 / #1 (thin samples stay diagnostic-only).
  const RANK_CUSTOMER_NARRATIVE_MIN_N = 40;
  const rankMetricsCustomerEligible =
    typeof rankEligibleN === "number" && rankEligibleN >= RANK_CUSTOMER_NARRATIVE_MIN_N;

  const demandByIntent = p.demandCapture?.demandCaptureByIntent || p.demandCapture?.byIntent || null;
  const providerPresenceRows = Array.isArray(p.providerPresence)
    ? p.providerPresence
    : Array.isArray(p.evidence?.providers)
      ? p.evidence.providers
      : null;

  return {
    contract: ADP_EXECUTIVE_READ_INPUT_V3,
    ok: true,
    property: {
      propertyId: p.property?.propertyId || meta.propertyId || null,
      name: p.property?.name || null,
      market: meta.market || p.property?.city || null,
      brand: p.property?.affiliation || p.property?.brand || null,
    },
    period: {
      periodId: p.period?.periodId || null,
      executionDate: p.period?.executionDate || null,
      status: p.period?.status || null,
    },
    aiConsideration: consideration,
    scenarioPresence,
    presenceIndex: {
      territories,
      strongest: strongT[0] || null,
      weakest: weakT[0] || null,
    },
    demandTerritories: territories,
    competitiveDisplacement: displacement,
    topObservedAlternative: p.competitiveSet?.topObservedAlternative || null,
    rankMetrics: {
      top3,
      numberOne,
      rankEligibleN,
      customerNarrativeEligible: rankMetricsCustomerEligible,
      customerNarrativeMinN: RANK_CUSTOMER_NARRATIVE_MIN_N,
    },
    demandCaptureByIntent: demandByIntent,
    realityGaps: gaps,
    realityGapOverall:
      p.realityGap?.overallRecognitionRate ?? p.realityGap?.coverageRate ?? null,
    providerPresence: providerPresenceRows || p.evidence?.providerCitations || null,
    trends: p.trends || p.executiveMetrics?.currentVsPrior || null,
    bpp: p.brandPortfolioPosition || null,
    evidenceRefs: {
      periodId: p.period?.periodId || null,
      reportVersionLabel: p.reportVersionLabel || null,
      measurementContractVersion: p.measurementContractVersion || null,
    },
    actionIntelligenceRefs: (p.actions || []).slice(0, 8).map((a) => ({
      id: a.id || a.type,
      title: a.title,
      priority: a.priority,
    })),
    classifierHints: {
      // Diagnostic only — not analytical source of truth for V3 composition
      positionPattern: er.current?.positionPattern || null,
      primaryStrength: er.current?.primaryStrength || er.primaryStrength || null,
      primaryConstraint: er.current?.primaryConstraint || er.primaryConstraint || null,
      benchmarkFinding: er.current?.benchmarkFinding || null,
    },
    competitorPresentShare:
      p.lostDemand?.competitorPresentShare ??
      (typeof p.executiveMetrics?.competitorPresentScenarios?.scenarioCount === "number" &&
      typeof p.executiveMetrics?.scenarioPresence?.eligibleScenarios === "number" &&
      p.executiveMetrics.scenarioPresence.eligibleScenarios > 0
        ? p.executiveMetrics.competitorPresentScenarios.scenarioCount /
          p.executiveMetrics.scenarioPresence.eligibleScenarios
        : null),
    sourceSnapshotSeed: {
      consideration,
      scenarioPresence,
      top3: rankMetricsCustomerEligible ? top3 : null,
      numberOne: rankMetricsCustomerEligible ? numberOne : null,
      rankEligibleN,
      gapCount: gaps.length,
      displacementTop: displacement[0]?.name || null,
    },
  };
}
