/**
 * Persist Demand Controllers for reuse across campaigns/markets.
 */

import fs from "node:fs";
import path from "node:path";
import { IDV2_VERSION } from "./constants.js";
import { buildDemandController } from "./demand-controller-model.js";

function defaultPath(root = process.cwd()) {
  return path.join(
    root,
    "data",
    "group-demand-intelligence",
    "international-discovery-v2",
    "demand-controllers.json"
  );
}

export function loadDemandControllers(storePath = defaultPath()) {
  try {
    if (!fs.existsSync(storePath)) {
      return { version: IDV2_VERSION, controllers: {}, updatedAt: null };
    }
    return JSON.parse(fs.readFileSync(storePath, "utf8"));
  } catch (err) {
    return { version: IDV2_VERSION, controllers: {}, updatedAt: null, loadError: String(err?.message || err) };
  }
}

export function saveDemandControllers(store, storePath = defaultPath()) {
  const dir = path.dirname(storePath);
  fs.mkdirSync(dir, { recursive: true });
  const out = { ...store, version: IDV2_VERSION, updatedAt: new Date().toISOString() };
  fs.writeFileSync(storePath, JSON.stringify(out, null, 2), "utf8");
  return out;
}

export function upsertDemandController(store, input = {}) {
  const built = buildDemandController(input);
  if (!built.ok) return { store, controller: null, reason: built.reason };
  const c = built.controller;
  const prev = store.controllers[c.demandControllerId] || {};
  store.controllers[c.demandControllerId] = {
    ...prev,
    ...c,
    historicalCampaigns: [
      ...new Set(
        [...(prev.historicalCampaigns || []), ...(c.historicalCampaigns || []), c.campaignId].filter(Boolean)
      ),
    ],
    markets: [...new Set([...(prev.markets || []), ...(c.markets || [])].filter(Boolean))],
    lastVerified: c.lastVerified || new Date().toISOString(),
  };
  return { store, controller: store.controllers[c.demandControllerId], reason: "upserted" };
}

export function getDemandController(store, id) {
  return store?.controllers?.[id] || null;
}

export function listDemandControllers(store, { market, country } = {}) {
  return Object.values(store.controllers || {}).filter((c) => {
    if (market && !String(c.market || "").toLowerCase().includes(String(market).toLowerCase())) {
      if (!(c.markets || []).some((m) => String(m).toLowerCase().includes(String(market).toLowerCase()))) {
        return false;
      }
    }
    if (country && String(c.country || "").toUpperCase() !== String(country).toUpperCase()) return false;
    return true;
  });
}
