/**
 * Hotel-local Demand Campaign store (filesystem).
 * Campaigns ≠ opportunities. Never auto-promotes children to ready.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { filterVisibleDemandGenerators, isGdiDemandGeneratorVisible } from "./visibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..", "..", "..", "data", "group-demand-intelligence", "hotels");

function campaignsPath(hotelId) {
  return path.join(ROOT, hotelId, "demand-campaigns.json");
}

export function loadDemandCampaigns(hotelId) {
  const p = campaignsPath(hotelId);
  if (!fs.existsSync(p)) {
    return {
      hotelId,
      updatedAt: null,
      campaigns: [],
      schemaVersion: "gdi_demand_campaign_v1",
    };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return {
    hotelId,
    updatedAt: doc.updatedAt || null,
    campaigns: Array.isArray(doc.campaigns) ? doc.campaigns : [],
    schemaVersion: doc.schemaVersion || "gdi_demand_campaign_v1",
  };
}

export function saveDemandCampaigns(hotelId, campaigns, meta = {}) {
  const dir = path.join(ROOT, hotelId);
  fs.mkdirSync(dir, { recursive: true });
  const doc = {
    hotelId,
    schemaVersion: "gdi_demand_campaign_v1",
    updatedAt: new Date().toISOString(),
    note:
      meta.note ||
      "Demand campaigns / generators — research universe. Not customer-ready opportunities.",
    campaigns,
  };
  fs.writeFileSync(campaignsPath(hotelId), JSON.stringify(doc, null, 2), "utf8");
  return doc;
}

/**
 * Upsert campaigns by campaignId without deleting history.
 * Existing records with same campaignId are merged; others preserved.
 */
export function upsertDemandCampaigns(hotelId, incoming = [], opts = {}) {
  const existing = loadDemandCampaigns(hotelId);
  const byId = new Map(existing.campaigns.map((c) => [c.campaignId, c]));
  const now = new Date().toISOString();
  for (const row of incoming) {
    const id = row.campaignId;
    if (!id) continue;
    const prev = byId.get(id) || {};
    byId.set(id, {
      ...prev,
      ...row,
      campaignId: id,
      createdAt: prev.createdAt || row.createdAt || now,
      updatedAt: now,
      // Preserve child history counts unless explicitly provided
      childEntitiesDiscovered:
        row.childEntitiesDiscovered ?? prev.childEntitiesDiscovered ?? 0,
      researchLeads: row.researchLeads ?? prev.researchLeads ?? 0,
      candidateOpportunities: row.candidateOpportunities ?? prev.candidateOpportunities ?? 0,
      customerReady: row.customerReady ?? prev.customerReady ?? 0,
      validFutureWatch: row.validFutureWatch ?? prev.validFutureWatch ?? 0,
      rejected: row.rejected ?? prev.rejected ?? 0,
      unresolved: row.unresolved ?? prev.unresolved ?? 0,
    });
  }
  const campaigns = [...byId.values()];
  return saveDemandCampaigns(hotelId, campaigns, opts);
}

export function listVisibleDemandCampaigns(hotelId, opts = {}) {
  const doc = loadDemandCampaigns(hotelId);
  const visible = filterVisibleDemandGenerators(doc.campaigns, opts);
  return {
    ...doc,
    campaigns: visible,
    count: visible.length,
    totalStored: doc.campaigns.length,
    visibility: visible.map((c) => ({
      campaignId: c.campaignId,
      ...isGdiDemandGeneratorVisible(c, opts),
    })),
  };
}
