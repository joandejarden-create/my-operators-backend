/**
 * Durable event-series graph persistence (filesystem hotel bag companion).
 * Organization → Event Series → Event Cycle → Source
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";

export const EVENT_SERIES_GRAPH_VERSION = "gdi_event_series_graph_v1";

function graphPath(hotelId) {
  return join(
    process.cwd(),
    "data/group-demand-intelligence/hotels",
    hotelId,
    "event-series-graph.json"
  );
}

export function loadEventSeriesGraph(hotelId) {
  const p = graphPath(hotelId);
  if (!existsSync(p)) {
    return {
      version: EVENT_SERIES_GRAPH_VERSION,
      hotelId,
      organizations: {},
      series: {},
      cycles: {},
      updatedAt: null,
    };
  }
  return JSON.parse(readFileSync(p, "utf8"));
}

export function upsertEventSeriesGraph(hotelId, { series = [], cycles = [] } = {}) {
  const graph = loadEventSeriesGraph(hotelId);
  let newSeries = 0;
  let newCycles = 0;

  for (const s of series) {
    if (!s?.eventSeriesId) continue;
    const orgKey = String(s.organization || "unknown")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 48);
    if (!graph.organizations[orgKey]) {
      graph.organizations[orgKey] = {
        name: s.organization || null,
        sourceDomains: [],
        seriesIds: [],
      };
    }
    try {
      const host = new URL(s.sourceUrl).hostname;
      if (host && !graph.organizations[orgKey].sourceDomains.includes(host)) {
        graph.organizations[orgKey].sourceDomains.push(host);
      }
    } catch {
      /* ignore */
    }
    if (!graph.series[s.eventSeriesId]) {
      newSeries += 1;
      graph.series[s.eventSeriesId] = {
        ...s,
        firstSeenAt: new Date().toISOString(),
        nextResearchAt: null,
        cadence: "MONTHLY",
      };
      if (!graph.organizations[orgKey].seriesIds.includes(s.eventSeriesId)) {
        graph.organizations[orgKey].seriesIds.push(s.eventSeriesId);
      }
    } else {
      graph.series[s.eventSeriesId] = {
        ...graph.series[s.eventSeriesId],
        ...s,
        updatedAt: new Date().toISOString(),
      };
    }
  }

  for (const c of cycles) {
    if (!c?.eventCycleId) continue;
    if (!graph.cycles[c.eventCycleId]) {
      newCycles += 1;
      graph.cycles[c.eventCycleId] = {
        ...c,
        firstSeenAt: new Date().toISOString(),
      };
    } else {
      graph.cycles[c.eventCycleId] = {
        ...graph.cycles[c.eventCycleId],
        ...c,
        updatedAt: new Date().toISOString(),
      };
    }
    const s = graph.series[c.eventSeriesId];
    if (s && c.year >= 2026) {
      s.futureCycles = s.futureCycles || [];
      if (!s.futureCycles.some((f) => f.eventCycleId === c.eventCycleId)) {
        s.futureCycles.push({
          year: c.year,
          eventCycleId: c.eventCycleId,
          title: c.title,
        });
      }
    }
  }

  graph.updatedAt = new Date().toISOString();
  const dir = join(process.cwd(), "data/group-demand-intelligence/hotels", hotelId);
  mkdirSync(dir, { recursive: true });
  writeFileSync(graphPath(hotelId), JSON.stringify(graph, null, 2));
  return { graph, newSeries, newCycles, path: graphPath(hotelId) };
}
