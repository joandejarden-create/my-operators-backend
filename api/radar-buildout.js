/**
 * CALA Radar Buildout API handlers.
 */

import { listCountryConfigs, getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import { fetchLiveCountsForCountry } from "../lib/radar-buildout/live-country-counts.js";
import { buildCountryPlanPayload } from "../lib/radar-buildout/build-plan-generator.js";
import { getGrowthProfilesForCountry } from "../lib/radar-buildout/growth-signals/index.js";
import {
  fetchRadarBuildPlans,
} from "../lib/radar-buildout/airtable-radar-build-plans-io.js";
import {
  normalizeRadarBuildPlan,
  toApiCountryBuildPlan,
} from "../lib/radar-buildout/normalize-radar-build-plan.js";

function planFromConfigAndLive(country, config, live) {
  const payload = buildCountryPlanPayload(country, config, live);
  return toApiCountryBuildPlan(payload);
}

export async function getRadarBuildoutCountries(req, res) {
  try {
    const tier = req.query.priorityTier || req.query.tier || "";
    const strategy = req.query.buildStrategy || req.query.strategy || "";
    const region = req.query.region || "";
    const status = req.query.buildStatus || req.query.status || "";

    const configs = listCountryConfigs({
      tier: tier || undefined,
      strategy: strategy || undefined,
    });

    const liveByCountry = await import("../lib/radar-buildout/live-country-counts.js").then((m) =>
      m.fetchLiveCountsByCountry()
    );

    let storedPlans = [];
    const storedResult = await fetchRadarBuildPlans();
    if (!storedResult.error) storedPlans = storedResult.plans || [];

    const storedByCountry = Object.fromEntries(storedPlans.map((p) => [p.country, p]));

    let countries = configs.map((entry) => {
      const { country, ...config } = entry;
      const live = liveByCountry[country] || { summary: { demandAnchors: 0, travelInfrastructure: 0, totalRadarPoints: 0 } };
      const computed = planFromConfigAndLive(country, config, live);
      const stored = storedByCountry[country];
      if (stored) {
        computed.buildStatus = stored.buildStatus || computed.buildStatus;
        computed.nextRecommendedAction =
          stored.nextRecommendedAction || computed.nextRecommendedAction;
        computed.lastBuildDate = stored.lastBuildDate || computed.lastBuildDate;
        computed.lastQaDate = stored.lastQaDate || computed.lastQaDate;
        computed.notes = stored.notes || computed.notes;
        computed.recommendedBuildSequence =
          stored.recommendedBuildSequence ?? computed.recommendedBuildSequence;
        computed.nextBuildMarket = stored.nextBuildMarket || computed.nextBuildMarket;
        computed.buildApproachNotes = stored.buildApproachNotes || computed.buildApproachNotes;
        computed.firstPassTargetDescription =
          stored.firstPassTargetDescription || computed.firstPassTargetDescription;
      }
      return computed;
    });

    if (region) countries = countries.filter((c) => c.region === region);
    if (status) countries = countries.filter((c) => c.buildStatus === status);

    const tierOrder = { "Tier 1": 1, "Tier 2": 2, "Tier 3": 3, Future: 4 };
    countries.sort((a, b) => {
      const t = (tierOrder[a.priorityTier] || 9) - (tierOrder[b.priorityTier] || 9);
      if (t !== 0) return t;
      const s = (a.recommendedBuildSequence ?? 999) - (b.recommendedBuildSequence ?? 999);
      if (s !== 0) return s;
      return String(a.buildStatus || "").localeCompare(String(b.buildStatus || ""));
    });

    return res.json({ ok: true, totalCount: countries.length, countries });
  } catch (err) {
    console.error("[radar-buildout] countries list error", err?.message || err);
    return res.status(500).json({ ok: false, error: "radar_buildout_failed", message: err?.message });
  }
}

export async function getRadarBuildoutCountry(req, res) {
  try {
    const country = decodeURIComponent(req.params.country || "").trim();
    if (!country) {
      return res.status(400).json({ ok: false, error: "validation_failed", message: "country required" });
    }

    const config = getCountryConfig(country);
    if (!config) {
      return res.status(404).json({ ok: false, error: "not_found", message: `No config for ${country}` });
    }

    const live = await fetchLiveCountsForCountry(country);
    const body = planFromConfigAndLive(country, config, live);

    const storedResult = await fetchRadarBuildPlans({ country });
    if (!storedResult.error && storedResult.plans?.length) {
      const stored = normalizeRadarBuildPlan(storedResult.records[0]);
      body.buildStatus = stored.buildStatus || body.buildStatus;
      body.nextRecommendedAction = stored.nextRecommendedAction || body.nextRecommendedAction;
      body.lastBuildDate = stored.lastBuildDate || body.lastBuildDate;
      body.lastQaDate = stored.lastQaDate || body.lastQaDate;
      body.notes = stored.notes || body.notes;
      body.storedPlanId = stored.id;
    }

    body.pointTypeBreakdown = live.summary?.pointTypes || {};
    body.infraTypeBreakdown = live.summary?.infraTypes || {};
    body.submarketCoverage = live.summary?.submarkets || {};

    const growthProfiles = getGrowthProfilesForCountry(country);
    body.growthSignals = {
      profileCount: growthProfiles.length,
      signalCount: growthProfiles.reduce((n, p) => n + p.signals.length, 0),
      highEarlyEntrySubmarkets: growthProfiles
        .filter((p) => p.earlyEntryOpportunity === "high" || p.earlyEntryOpportunity === "medium-high")
        .map((p) => p.submarket),
    };

    return res.json(body);
  } catch (err) {
    console.error("[radar-buildout] country detail error", err?.message || err);
    return res.status(500).json({ ok: false, error: "radar_buildout_failed", message: err?.message });
  }
}
