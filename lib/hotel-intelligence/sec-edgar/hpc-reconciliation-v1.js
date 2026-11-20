/**
 * Future HPC reconciliation — attach previously discovered intelligence
 * without re-downloading SEC filings or repeating paid AI.
 */

import { DISCOVERY_STATUS, PERSISTENCE_CLASS } from "./contract.js";
import { matchHotelHintToHpc } from "./hotel-attribution-v1.js";
import { planContractPersistence } from "./persistence-planner-v1.js";

export const HPC_RECONCILIATION_VERSION = "sec-edgar-hpc-reconciliation-v1";

function normalize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * @param {object} inventory — discovery inventory
 * @param {Array<{id:string,name:string,city?:string,brand?:string}>} hpcCandidates
 * @param {object} [opts]
 */
export function reconcileDiscoveryInventoryToHpc(inventory, hpcCandidates = [], opts = {}) {
  const now = new Date().toISOString();
  const updates = [];
  const newlyMatchable = [];
  const stillUnmatched = [];
  const ambiguous = [];
  const unchangedMatched = [];

  for (const hotel of inventory.hotels || []) {
    const priorStatus = hotel.hpcMatchStatus;
    const priorHpcId = hotel.hpcRecordId || null;

    // Already matched — keep unless opts.revalidate
    if (priorHpcId && priorStatus === DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC && !opts.revalidate) {
      unchangedMatched.push({ discoveryId: hotel.discoveryId, hpcRecordId: priorHpcId });
      continue;
    }

    const hint = [hotel.hotelName, hotel.city].filter(Boolean).join(" ");
    const match = matchHotelHintToHpc(hint, hpcCandidates, {
      minScore: opts.minScore ?? 0.58,
    });

    // Location guard: if both have cities and they differ sharply, reject
    const hasVerifiedContracts = (hotel.contractFindings || []).some((f) =>
      String(f.verificationClass || "").startsWith("VERIFIED_"),
    );
    const pendingStatus = hasVerifiedContracts
      ? DISCOVERY_STATUS.CONTRACT_VERIFIED_PENDING_HPC
      : DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC;

    if (
      match.status === "MATCHED" &&
      hotel.city &&
      match.hotel?.city &&
      normalize(hotel.city) !== normalize(match.hotel.city) &&
      !normalize(match.hotel.name).includes(normalize(hotel.city))
    ) {
      stillUnmatched.push({
        discoveryId: hotel.discoveryId,
        hotelName: hotel.hotelName,
        reason: "city_mismatch_guard",
      });
      updates.push({
        discoveryId: hotel.discoveryId,
        hpcMatchStatus: pendingStatus,
        hpcRecordId: null,
        lastReconciledAt: now,
      });
      continue;
    }

    if (match.status === "AMBIGUOUS") {
      ambiguous.push({
        discoveryId: hotel.discoveryId,
        hotelName: hotel.hotelName,
        candidates: match.candidates,
      });
      updates.push({
        discoveryId: hotel.discoveryId,
        hpcMatchStatus: DISCOVERY_STATUS.MATCH_AMBIGUOUS,
        hpcRecordId: null,
        lastReconciledAt: now,
      });
      continue;
    }

    if (match.status === "MATCHED") {
      newlyMatchable.push({
        discoveryId: hotel.discoveryId,
        hotelName: hotel.hotelName,
        hpcRecordId: match.hpcId,
        score: match.score,
        contractFindingCount: (hotel.contractFindings || []).length,
        hasVerifiedContracts,
      });
      updates.push({
        discoveryId: hotel.discoveryId,
        hpcMatchStatus: DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC,
        hpcRecordId: match.hpcId,
        lastReconciledAt: now,
      });
      continue;
    }

    stillUnmatched.push({
      discoveryId: hotel.discoveryId,
      hotelName: hotel.hotelName,
      reason: match.reason || "no_match",
    });
    updates.push({
      discoveryId: hotel.discoveryId,
      hpcMatchStatus: pendingStatus,
      hpcRecordId: null,
      lastReconciledAt: now,
    });
  }

  // Apply updates to a cloned inventory
  const byId = new Map((inventory.hotels || []).map((h) => [h.discoveryId, { ...h }]));
  for (const u of updates) {
    const row = byId.get(u.discoveryId);
    if (!row) continue;
    Object.assign(row, u);
    byId.set(u.discoveryId, row);
  }
  const nextInventory = {
    ...inventory,
    updatedAt: now,
    hotels: [...byId.values()],
  };

  // Plan persistence only for newly matchable hotels with verified contracts
  const attributedForPlan = [];
  for (const m of newlyMatchable) {
    if (!m.hasVerifiedContracts) continue;
    const hotel = byId.get(m.discoveryId);
    for (const f of hotel.contractFindings || []) {
      if (!String(f.verificationClass || "").startsWith("VERIFIED_")) continue;
      attributedForPlan.push({
        persistable: true,
        persistenceClass: PERSISTENCE_CLASS.STAGED_LOCAL,
        verificationClass: f.verificationClass,
        attribution: { status: "MATCHED", hpcId: m.hpcRecordId, quarantine: false },
        finding: {
          ...f,
          hpcHotelId: m.hpcRecordId,
          attributedHotelName: hotel.hotelName,
          hotelAttributed: true,
        },
      });
    }
  }

  const persistencePlan = planContractPersistence(attributedForPlan, {
    planRelationships: true,
    planHpcNullFill: false,
  });

  return {
    version: HPC_RECONCILIATION_VERSION,
    dryRun: opts.apply !== true,
    at: now,
    counts: {
      inventoryHotels: (inventory.hotels || []).length,
      newlyMatchable: newlyMatchable.length,
      stillUnmatched: stillUnmatched.length,
      ambiguous: ambiguous.length,
      unchangedMatched: unchangedMatched.length,
      plannedEvidenceWrites: persistencePlan.counts.evidence,
    },
    newlyMatchable,
    stillUnmatched,
    ambiguous,
    unchangedMatched,
    nextInventory,
    persistencePlan,
    note: "Reconciliation uses stored discovery inventory — no SEC re-download required.",
  };
}
