/**
 * AI Demand Positioning — API Routes.
 * Owner-facing endpoints for property AI demand intelligence.
 */

import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { estimateCost } from "../lib/ai-demand-positioning/execution/multi-provider-runner.js";

export function getAiDemandPositioningProperties(req, res) {
  const properties = [
    {
      propertyId: "adp_waterstone_boca_raton",
      name: "Waterstone Resort & Marina",
      city: "Boca Raton",
      state: "Florida",
      chainScale: "Upper Upscale",
      affiliation: "Curio Collection by Hilton",
    },
    {
      propertyId: "adp_renaissance_times_square",
      name: "Renaissance New York Times Square Hotel",
      city: "New York",
      state: "New York",
      chainScale: "Upper Upscale",
      affiliation: "Renaissance Hotels (Marriott)",
    },
    {
      propertyId: "adp_cambridge_beaches_bermuda",
      name: "Cambridge Beaches Resort & Spa",
      city: "Sandys Parish",
      state: "Bermuda",
      chainScale: "Luxury",
      affiliation: "Independent",
    },
  ];
  res.json({ ok: true, properties });
}

export function getAiDemandPositioningReport(req, res) {
  try {
    const { propertyId } = req.params;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) {
      return res.status(404).json({ ok: false, error: "property_not_found" });
    }

    const period = loadLatestPeriod(propertyId);
    if (!period) {
      return res.status(404).json({
        ok: false,
        error: "no_monitoring_data",
        message: "No monitoring period has been executed for this property yet.",
        property: { name: profile.name, city: profile.city },
      });
    }

    const scenarios = buildScenarioUniverse(profile);
    const payload = buildOwnerPayload(period, scenarios, profile);

    // Build trend data from all historical periods
    const allPeriods = loadAllPeriods(propertyId);
    if (allPeriods.length > 1) {
      payload.trends = allPeriods.map((p) => {
        const pPayload = buildOwnerPayload(p, scenarios, profile);
        return {
          periodId: p.periodId,
          date: p.executionDate,
          demandCaptureRate: pPayload.demandCapture ? pPayload.demandCapture.overallRate : null,
          providerCount: p.providers ? p.providers.length : null,
          observationCount: p.observations ? p.observations.length : 0,
        };
      });
    }

    return res.json(payload);
  } catch (err) {
    console.error("[AI Demand Positioning] report error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function getAiDemandPositioningEvidence(req, res) {
  try {
    const { propertyId } = req.params;
    const { intent, type, competitor } = req.query;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) return res.status(404).json({ ok: false, error: "property_not_found" });

    const period = loadLatestPeriod(propertyId);
    if (!period) return res.status(404).json({ ok: false, error: "no_data" });

    const scenarios = buildScenarioUniverse(profile);
    const scenarioMap = Object.fromEntries(scenarios.map((s) => [s.scenarioId, s]));

    let observations = period.observations;

    if (type === "displacement" && competitor) {
      // Find scenarios where property is NOT mentioned but competitor IS
      const compLow = competitor.toLowerCase();
      observations = observations.filter((o) =>
        !o.mentioned &&
        (o.competitorsMentioned || []).some((c) => c.toLowerCase().includes(compLow) || compLow.includes(c.toLowerCase()))
      );
      // Deduplicate by scenario (show one per scenario, prefer provider with most detail)
      const byScenario = {};
      for (const obs of observations) {
        if (!byScenario[obs.scenarioId] || (obs.rawResponse || "").length > (byScenario[obs.scenarioId].rawResponse || "").length) {
          byScenario[obs.scenarioId] = obs;
        }
      }
      observations = Object.values(byScenario);
    } else {
      if (intent) {
        const intentScenarioIds = scenarios.filter((s) => s.intent === intent).map((s) => s.scenarioId);
        observations = observations.filter((o) => intentScenarioIds.includes(o.scenarioId));
      }
      if (type === "missing") {
        observations = observations.filter((o) => !o.mentioned);
      } else if (type === "present") {
        observations = observations.filter((o) => o.mentioned);
      }
    }

    const evidence = observations.slice(0, 5).map((obs) => ({
      scenarioId: obs.scenarioId,
      scenarioLabel: scenarioMap[obs.scenarioId]?.label || obs.scenarioId,
      intent: scenarioMap[obs.scenarioId]?.intent || "",
      provider: obs.provider,
      mentioned: obs.mentioned,
      competitorsMentioned: obs.competitorsMentioned || [],
      responseExcerpt: obs.rawResponse ? obs.rawResponse.slice(0, 2000) : "",
      sourcesCited: obs.sourcesCited || [],
      providerCitations: obs.providerCitations || [],
      timestamp: period.executionDate,
    }));

    return res.json({ ok: true, propertyId, intent, type, competitor, evidence, total: observations.length });
  } catch (err) {
    console.error("[AI Demand Positioning] evidence error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function getAiDemandPositioningCostEstimate(req, res) {
  try {
    const { propertyId } = req.params;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) return res.status(404).json({ ok: false, error: "property_not_found" });

    const scenarios = buildScenarioUniverse(profile);
    const estimate = estimateCost(scenarios.length);
    return res.json({ ok: true, propertyId, ...estimate });
  } catch (err) {
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
}
