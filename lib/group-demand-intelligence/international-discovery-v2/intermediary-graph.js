/**
 * Market-level intermediary ecosystem graph (PCO/DMC/CVB/secretariat…).
 * Reuse entities across campaigns — do not rediscover from zero.
 */

import fs from "node:fs";
import path from "node:path";
import { CONTROLLER_TYPE, IDV2_VERSION } from "./constants.js";
import { buildDemandController } from "./demand-controller-model.js";

export const INTERMEDIARY_RELATIONS = Object.freeze([
  "works_with_event",
  "works_with_association",
  "works_with_venue",
  "works_with_hotel",
  "controls_housing",
  "manages_registration",
  "manages_travel",
  "manages_delegates",
  "manages_exhibitors",
]);

function defaultStorePath(root = process.cwd()) {
  return path.join(root, "data", "group-demand-intelligence", "international-discovery-v2", "intermediary-graph.json");
}

export function loadIntermediaryGraph(storePath = defaultStorePath()) {
  try {
    if (!fs.existsSync(storePath)) {
      return { version: IDV2_VERSION, entities: {}, relations: [], updatedAt: null };
    }
    return JSON.parse(fs.readFileSync(storePath, "utf8"));
  } catch (err) {
    return {
      version: IDV2_VERSION,
      entities: {},
      relations: [],
      updatedAt: null,
      loadError: String(err?.message || err),
    };
  }
}

export function saveIntermediaryGraph(graph, storePath = defaultStorePath()) {
  const dir = path.dirname(storePath);
  fs.mkdirSync(dir, { recursive: true });
  const out = {
    ...graph,
    version: IDV2_VERSION,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(storePath, JSON.stringify(out, null, 2), "utf8");
  return out;
}

/**
 * Upsert a controller into the intermediary graph for reuse.
 */
export function upsertIntermediaryEntity(graph, controllerInput = {}) {
  const built = buildDemandController(controllerInput);
  if (!built.ok) return { graph, entity: null, reason: built.reason };

  const c = built.controller;
  const prev = graph.entities[c.demandControllerId] || {};
  const merged = {
    ...prev,
    ...c,
    historicalCampaigns: [
      ...new Set([...(prev.historicalCampaigns || []), ...(c.historicalCampaigns || []), c.campaignId].filter(Boolean)),
    ],
    markets: [...new Set([...(prev.markets || []), ...(c.markets || [])].filter(Boolean))],
    knownContacts: [...new Set([...(prev.knownContacts || []), ...(c.knownContacts || [])].filter(Boolean))],
    lastVerified: c.lastVerified || prev.lastVerified || new Date().toISOString(),
    reuseCount: Number(prev.reuseCount || 0) + 1,
  };
  graph.entities[c.demandControllerId] = merged;
  return { graph, entity: merged, reason: "upserted" };
}

export function addIntermediaryRelation(graph, { fromId, toId, relation, campaignId, evidenceSource } = {}) {
  if (!INTERMEDIARY_RELATIONS.includes(relation)) {
    return { graph, ok: false, reason: "unknown_relation" };
  }
  const row = {
    fromId,
    toId,
    relation,
    campaignId: campaignId || null,
    evidenceSource: evidenceSource || null,
    at: new Date().toISOString(),
  };
  graph.relations = Array.isArray(graph.relations) ? graph.relations : [];
  const dup = graph.relations.find(
    (r) => r.fromId === fromId && r.toId === toId && r.relation === relation && r.campaignId === campaignId
  );
  if (!dup) graph.relations.push(row);
  return { graph, ok: true, relation: row };
}

/** Find reusable intermediaries for a market / event category. */
export function findReusableIntermediaries(graph, { market, country, controllerType, eventCategory } = {}) {
  const entities = Object.values(graph.entities || {});
  return entities
    .filter((e) => {
      if (controllerType && e.controllerType !== controllerType && controllerType !== CONTROLLER_TYPE.UNKNOWN) {
        return false;
      }
      if (market && !(e.markets || []).some((m) => String(m).toLowerCase().includes(String(market).toLowerCase()))) {
        if (e.market && !String(e.market).toLowerCase().includes(String(market).toLowerCase())) {
          if (country && String(e.country || "").toUpperCase() !== String(country).toUpperCase()) return false;
        }
      }
      if (eventCategory) {
        const blob = JSON.stringify(e.historicalCampaigns || []).toLowerCase();
        if (!blob.includes(String(eventCategory).toLowerCase())) {
          /* soft filter — still allow market match */
        }
      }
      return true;
    })
    .sort((a, b) => Number(b.reuseCount || 0) - Number(a.reuseCount || 0));
}
