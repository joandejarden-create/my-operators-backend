/**
 * AI Demand Positioning — API Routes.
 * Owner-facing endpoints for property AI demand intelligence.
 * Production reads pre-computed published snapshots (file or Airtable).
 */

import { loadPropertyProfile, listPropertyProfiles } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { estimateCost } from "../lib/ai-demand-positioning/execution/multi-provider-runner.js";
import {
  getPublishedOwnerReport,
  getPublishedEvidenceResponse,
} from "../lib/ai-demand-positioning/published-read-service.js";

export function getAiDemandPositioningProperties(req, res) {
  const properties = listPropertyProfiles();
  res.json({ ok: true, properties });
}

export async function getAiDemandPositioningReport(req, res) {
  try {
    const { propertyId } = req.params;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) {
      return res.status(404).json({ ok: false, error: "property_not_found" });
    }

    const result = await getPublishedOwnerReport(propertyId);
    if (!result.ok) {
      const status = result.error === "property_not_found" ? 404 : 404;
      return res.status(status).json(result);
    }

    const payload = {
      ...result.payload,
      propertyId: propertyId,
      property: {
        ...(result.payload.property || {}),
        propertyId,
      },
    };
    return res.json(payload);
  } catch (err) {
    console.error("[AI Demand Positioning] report error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export async function getAiDemandPositioningEvidence(req, res) {
  try {
    const { propertyId } = req.params;
    const { intent, type, competitor, competitorId, scope } = req.query;
    const profile = loadPropertyProfile(propertyId);
    if (!profile) return res.status(404).json({ ok: false, error: "property_not_found" });

    const result = await getPublishedEvidenceResponse(propertyId, {
      intent,
      type,
      competitor,
      competitorId,
      scope,
    });
    if (!result.ok) {
      return res.status(404).json(result);
    }

    return res.json(result);
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
