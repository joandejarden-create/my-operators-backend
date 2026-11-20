/**
 * Durable hotel discovery inventory (filesystem research store).
 * Survives missing HPC coverage. Idempotent upsert by discoveryId.
 */

import fs from "node:fs";
import path from "node:path";
import { DISCOVERY_INVENTORY_DESTINATION, DISCOVERY_STATUS } from "./contract.js";

export const DISCOVERY_INVENTORY_VERSION = "sec-edgar-discovery-inventory-v1";

export function defaultInventoryRoot(cwd = process.cwd()) {
  return path.join(cwd, DISCOVERY_INVENTORY_DESTINATION.rootRelative);
}

function inventoryPath(root) {
  return path.join(root, "hotels.json");
}

export function loadDiscoveryInventory(root = defaultInventoryRoot()) {
  const p = inventoryPath(root);
  if (!fs.existsSync(p)) {
    return {
      version: DISCOVERY_INVENTORY_VERSION,
      destination: DISCOVERY_INVENTORY_DESTINATION,
      updatedAt: null,
      hotels: [],
    };
  }
  try {
    const raw = JSON.parse(fs.readFileSync(p, "utf8"));
    return {
      version: DISCOVERY_INVENTORY_VERSION,
      destination: DISCOVERY_INVENTORY_DESTINATION,
      updatedAt: raw.updatedAt || null,
      hotels: Array.isArray(raw.hotels) ? raw.hotels : [],
    };
  } catch {
    return {
      version: DISCOVERY_INVENTORY_VERSION,
      destination: DISCOVERY_INVENTORY_DESTINATION,
      updatedAt: null,
      hotels: [],
      error: "INVENTORY_PARSE_FAILED",
    };
  }
}

export function saveDiscoveryInventory(inventory, root = defaultInventoryRoot()) {
  fs.mkdirSync(root, { recursive: true });
  const payload = {
    version: DISCOVERY_INVENTORY_VERSION,
    destination: DISCOVERY_INVENTORY_DESTINATION,
    updatedAt: new Date().toISOString(),
    hotels: inventory.hotels || [],
  };
  fs.writeFileSync(inventoryPath(root), JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

/**
 * Merge discovered hotels + optional contract findings into inventory.
 * Never drops an unmatched hotel because HPC is missing.
 */
export function upsertDiscoveredHotels(inventory, hotels = [], opts = {}) {
  const byId = new Map((inventory.hotels || []).map((h) => [h.discoveryId, { ...h }]));
  const now = new Date().toISOString();

  for (const h of hotels) {
    if (!h?.discoveryId || !h?.hotelName) continue;
    const prev = byId.get(h.discoveryId);
    const next = {
      ...(prev || {}),
      ...h,
      discoveryId: h.discoveryId,
      hotelName: h.hotelName,
      firstDiscoveredAt: prev?.firstDiscoveredAt || h.discoveredAt || now,
      lastResearchedAt: now,
      mentionCount: (prev?.mentionCount || 0) + (h.mentionCount || 1),
      contractFindings: mergeFindings(prev?.contractFindings, h.contractFindings),
      hpcMatchStatus:
        h.hpcMatchStatus ||
        prev?.hpcMatchStatus ||
        DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
      hpcRecordId: h.hpcRecordId ?? prev?.hpcRecordId ?? null,
      persistenceStatus: h.persistenceStatus || prev?.persistenceStatus || "INVENTORY_ONLY",
      evidenceConfidence: h.evidenceConfidence || prev?.evidenceConfidence || "MEDIUM",
    };
    byId.set(h.discoveryId, next);
  }

  // Attach contract findings by hotel name hint when provided separately
  if (opts.contractLinks?.length) {
    for (const link of opts.contractLinks) {
      const target = findBestInventoryHotel(byId, link);
      if (!target) continue;
      target.contractFindings = mergeFindings(target.contractFindings, [link.finding]);
      target.lastResearchedAt = now;
      if (link.verified && target.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC) {
        target.hpcMatchStatus = DISCOVERY_STATUS.CONTRACT_VERIFIED_PENDING_HPC;
      }
      if (link.hpcId) {
        target.hpcRecordId = link.hpcId;
        target.hpcMatchStatus = DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC;
      }
      byId.set(target.discoveryId, target);
    }
  }

  return {
    version: DISCOVERY_INVENTORY_VERSION,
    destination: DISCOVERY_INVENTORY_DESTINATION,
    updatedAt: now,
    hotels: [...byId.values()].sort((a, b) =>
      String(a.hotelName).localeCompare(String(b.hotelName)),
    ),
  };
}

function mergeFindings(prev = [], next = []) {
  const out = [...(prev || [])];
  const keys = new Set(
    out.map(
      (f) =>
        `${f?.evidenceFieldName || ""}|${f?.primaryDate || ""}|${f?.source?.accessionNumber || ""}`,
    ),
  );
  for (const f of next || []) {
    if (!f) continue;
    const key = `${f.evidenceFieldName || ""}|${f.primaryDate || ""}|${f.source?.accessionNumber || ""}`;
    if (keys.has(key)) continue;
    keys.add(key);
    out.push(f);
  }
  return out;
}

function findBestInventoryHotel(byId, link) {
  const hint = normalize(link.hotelName || link.hint || "");
  if (!hint) return null;
  let best = null;
  let bestScore = 0;
  for (const h of byId.values()) {
    const n = normalize(h.hotelName);
    if (!n) continue;
    let score = 0;
    if (n === hint) score = 1;
    else if (n.includes(hint) || hint.includes(n)) score = 0.8;
    if (score > bestScore) {
      bestScore = score;
      best = h;
    }
  }
  return bestScore >= 0.8 ? best : null;
}

function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function inventorySummary(inventory) {
  const hotels = inventory.hotels || [];
  const byStatus = {};
  for (const s of Object.values(DISCOVERY_STATUS)) byStatus[s] = 0;
  for (const h of hotels) {
    const st = h.hpcMatchStatus || DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC;
    byStatus[st] = (byStatus[st] || 0) + 1;
  }
  return {
    totalHotels: hotels.length,
    withContractFindings: hotels.filter((h) => (h.contractFindings || []).length > 0).length,
    matchedHpc: byStatus[DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC] || 0,
    notInHpc: byStatus[DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC] || 0,
    contractVerifiedPendingHpc:
      byStatus[DISCOVERY_STATUS.CONTRACT_VERIFIED_PENDING_HPC] || 0,
    ambiguous: byStatus[DISCOVERY_STATUS.MATCH_AMBIGUOUS] || 0,
    byStatus,
  };
}
