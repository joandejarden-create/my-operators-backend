/**
 * Live Demand Anchors + Travel Infrastructure counts grouped by country.
 */

import { fetchDemandAnchorRecords } from "../demand-anchors/airtable-demand-anchors-io.js";
import { fetchTravelInfrastructureRecords } from "../travel-infrastructure/airtable-travel-infrastructure-io.js";
import { summarizeCountryRadarPoints } from "./coverage-metrics.js";

/**
 * @returns {Promise<Record<string, { demandAnchors: object[], travelInfrastructure: object[], summary: object }>>}
 */
export async function fetchLiveCountsByCountry() {
  const byCountry = {};

  const [daResult, tiResult] = await Promise.all([
    fetchDemandAnchorRecords({ includeHidden: true }),
    fetchTravelInfrastructureRecords({ includeHidden: true }),
  ]);

  const daPoints =
    daResult.error && daResult.error !== "demand_anchors_table_missing"
      ? []
      : daResult.allPoints || daResult.points || [];
  const tiPoints =
    tiResult.error && tiResult.error !== "travel_infrastructure_table_missing"
      ? []
      : tiResult.allPoints || tiResult.points || [];

  for (const p of daPoints) {
    const country = String(p.country || "").trim() || "(unknown)";
    if (!byCountry[country]) {
      byCountry[country] = { demandAnchors: [], travelInfrastructure: [] };
    }
    byCountry[country].demandAnchors.push(p);
  }

  for (const p of tiPoints) {
    const country = String(p.country || "").trim() || "(unknown)";
    if (!byCountry[country]) {
      byCountry[country] = { demandAnchors: [], travelInfrastructure: [] };
    }
    byCountry[country].travelInfrastructure.push(p);
  }

  for (const country of Object.keys(byCountry)) {
    const bucket = byCountry[country];
    bucket.summary = summarizeCountryRadarPoints(bucket.demandAnchors, bucket.travelInfrastructure);
    bucket.summary.demandAnchorPoints = bucket.demandAnchors;
    bucket.summary.travelInfraPoints = bucket.travelInfrastructure;
  }

  return byCountry;
}

/**
 * @param {string} country
 */
export async function fetchLiveCountsForCountry(country) {
  const all = await fetchLiveCountsByCountry();
  const key = String(country || "").trim();
  const bucket = all[key] || { demandAnchors: [], travelInfrastructure: [], summary: summarizeCountryRadarPoints([], []) };
  return {
    country: key,
    ...bucket,
    summary: {
      ...bucket.summary,
      demandAnchorPoints: bucket.demandAnchors || [],
      travelInfraPoints: bucket.travelInfrastructure || [],
    },
  };
}
