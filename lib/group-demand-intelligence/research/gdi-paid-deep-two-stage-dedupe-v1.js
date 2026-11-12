/**
 * Two-stage paid-deep deduplication for GDI Full E2E.
 *
 * Stage A (before paid deep): skip orgs already in hotel inventory (88 Bethesda),
 * prior Exa/ORS discoveries in this run, and cross-segment paid-deep org keys.
 *
 * Stage B (after deep): reconcile richer organizer / venue / date / event-family
 * identity — do not collapse distinct annual editions or genuinely distinct
 * commercial opportunities.
 *
 * Does not invent demand. Does not spend. Does not publish.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { normalizeOrgIdentityKey } from "../workflow/reuse-prior-intelligence-v1.js";

export const GDI_PAID_DEEP_TWO_STAGE_DEDUPE_V1 =
  "gdi-paid-deep-two-stage-dedupe-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const HOTELS_DIR = path.join(ROOT, "data/group-demand-intelligence/hotels");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function orgDeepKey(opp = {}) {
  return normalizeOrgIdentityKey(
    opp.organizationName || opp.organizingEntity || opp.venueName || ""
  );
}

export function editionIdentityKey(opp = {}) {
  const org = orgDeepKey(opp);
  const start = String(opp.eventStartDate || opp.programmeStartDate || "").slice(
    0,
    10
  );
  const title = text(opp.title || opp.programmeName || opp.activityTitle)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .slice(0, 80);
  const family = text(opp.eventFamilyId || opp.commercialEventFamilyId || "")
    .toLowerCase()
    .slice(0, 40);
  return `${org}|${start || "undated"}|${title || "untitled"}|${family || "nofamily"}`;
}

function ingestOpportunityLike(row, keys, ids) {
  if (!row || typeof row !== "object") return;
  const id = String(row.id || row.opportunityId || "");
  if (id) ids.add(id);
  const k = orgDeepKey(row);
  if (k) keys.add(k);
}

export function loadHotelInventoryOrgKeys(hotelId) {
  const keys = new Set();
  const ids = new Set();
  if (!hotelId) return { orgKeys: keys, opportunityIds: ids, count: 0 };
  const hotelRoot = path.join(HOTELS_DIR, String(hotelId));
  const p = path.join(hotelRoot, "opportunities.json");
  let rowCount = 0;
  if (fs.existsSync(p)) {
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      const rows = Array.isArray(raw) ? raw : raw.opportunities || [];
      rowCount = rows.length;
      for (const row of rows) ingestOpportunityLike(row, keys, ids);
    } catch {
      /* keep partial */
    }
  }
  // Also ingest durable ORS / research-state files (covers union inventory beyond
  // the customer-facing opportunities.json surface).
  const orsDir = path.join(hotelRoot, "opportunity-research-state");
  if (fs.existsSync(orsDir)) {
    try {
      for (const name of fs.readdirSync(orsDir)) {
        if (!name.endsWith(".json")) continue;
        try {
          const doc = JSON.parse(
            fs.readFileSync(path.join(orsDir, name), "utf8")
          );
          const opp = doc.opportunity || doc;
          ingestOpportunityLike(opp, keys, ids);
          if (doc.id || doc.opportunityId) {
            ids.add(String(doc.id || doc.opportunityId));
          }
          // Filename often equals opportunity id
          const stem = name.replace(/\.json$/i, "");
          if (stem) ids.add(stem);
        } catch {
          /* skip corrupt */
        }
      }
    } catch {
      /* ignore */
    }
  }
  return { orgKeys: keys, opportunityIds: ids, count: Math.max(rowCount, ids.size) };
}

/**
 * Stage A — should we skip paid Exa deep seed for this opportunity?
 */
export function evaluatePrePaidDeepDedupe(opportunity = {}, context = {}) {
  const orgKey = orgDeepKey(opportunity);
  const oppId = String(opportunity.id || opportunity.opportunityId || "");
  const reasons = [];
  let skipPaidDeep = false;
  let reasonCode = null;

  if (
    context.completedDeepResearchIds instanceof Set &&
    oppId &&
    context.completedDeepResearchIds.has(oppId)
  ) {
    skipPaidDeep = true;
    reasonCode = "ALREADY_SETTLED_IN_LEDGER";
    reasons.push("opportunity_id_already_settled_in_paid_ledger");
  }

  if (
    !skipPaidDeep &&
    context.paidDeepOrgKeys instanceof Set &&
    orgKey &&
    context.paidDeepOrgKeys.has(orgKey)
  ) {
    skipPaidDeep = true;
    reasonCode = "CROSS_SEGMENT_ORG_ALREADY_PAID_DEEP";
    reasons.push("same_org_already_paid_deep_in_this_run");
  }

  if (
    !skipPaidDeep &&
    context.inventoryOrgKeys instanceof Set &&
    orgKey &&
    context.inventoryOrgKeys.has(orgKey)
  ) {
    // Existing inventory org — do not burn paid deep unless a distinct future
    // edition / overflow thesis is explicitly supported.
    const distinctEdition =
      opportunity.futureCycleSupported === true ||
      opportunity.futureCycleOpportunity === true ||
      /OVERFLOW|FUTURE_CYCLE|COMPETITIVE_FUTURE/i.test(
        `${opportunity.opportunityType || ""} ${opportunity.internalDisposition || ""} ${opportunity.hotelOpportunityThesis || ""}`
      );
    if (!distinctEdition) {
      skipPaidDeep = true;
      reasonCode = "EXISTING_HOTEL_INVENTORY_ORG";
      reasons.push("organization_already_in_hotel_opportunity_inventory");
    } else {
      reasons.push(
        "inventory_org_but_distinct_future_cycle_or_overflow_thesis_allows_research"
      );
    }
  }

  if (
    !skipPaidDeep &&
    context.inventoryOpportunityIds instanceof Set &&
    oppId &&
    context.inventoryOpportunityIds.has(oppId)
  ) {
    skipPaidDeep = true;
    reasonCode = "EXISTING_HOTEL_INVENTORY_ID";
    reasons.push("opportunity_id_already_in_hotel_inventory");
  }

  return {
    version: GDI_PAID_DEEP_TWO_STAGE_DEDUPE_V1,
    stage: "PRE_PAID_DEEP",
    skipPaidDeep,
    reasonCode,
    orgKey: orgKey || null,
    opportunityId: oppId || null,
    reasons,
    allowLiveExaDeepSeed: !skipPaidDeep,
  };
}

/**
 * Stage B — after deep research, reconcile identity without collapsing editions.
 */
export function reconcilePostDeepIdentity(opportunity = {}, peers = []) {
  const selfKey = editionIdentityKey(opportunity);
  const orgKey = orgDeepKey(opportunity);
  const collisions = [];
  for (const peer of peers) {
    if (!peer) continue;
    const peerId = String(peer.id || peer.opportunityId || "");
    const selfId = String(opportunity.id || opportunity.opportunityId || "");
    if (peerId && selfId && peerId === selfId) continue;
    const peerEdition = editionIdentityKey(peer);
    const peerOrg = orgDeepKey(peer);
    if (peerEdition && peerEdition === selfKey) {
      collisions.push({
        peerId,
        kind: "SAME_EDITION_IDENTITY",
        note: "Same org + date + title — reuse evidence; do not treat as new opportunity",
      });
    } else if (peerOrg && peerOrg === orgKey) {
      const peerStart = String(peer.eventStartDate || "").slice(0, 10);
      const selfStart = String(opportunity.eventStartDate || "").slice(0, 10);
      if (peerStart && selfStart && peerStart !== selfStart) {
        collisions.push({
          peerId,
          kind: "SAME_ORG_DISTINCT_EDITION",
          note: "Preserve both annual editions — do not collapse",
        });
      } else {
        collisions.push({
          peerId,
          kind: "SAME_ORG_RELATED",
          note: "Related org identity — link as event-family / account, keep distinct if commercial thesis differs",
        });
      }
    }
  }
  return {
    version: GDI_PAID_DEEP_TWO_STAGE_DEDUPE_V1,
    stage: "POST_DEEP",
    orgKey: orgKey || null,
    editionKey: selfKey,
    collisions,
    collapseProhibited: collisions.some(
      (c) => c.kind === "SAME_ORG_DISTINCT_EDITION"
    ),
  };
}

export function buildInventoryDedupeContextForHotels(hotelIds = []) {
  const orgKeys = new Set();
  const opportunityIds = new Set();
  const perHotel = {};
  for (const hotelId of hotelIds) {
    const loaded = loadHotelInventoryOrgKeys(hotelId);
    perHotel[hotelId] = { count: loaded.count };
    for (const k of loaded.orgKeys) orgKeys.add(k);
    for (const id of loaded.opportunityIds) opportunityIds.add(id);
  }
  return {
    version: GDI_PAID_DEEP_TWO_STAGE_DEDUPE_V1,
    inventoryOrgKeys: orgKeys,
    inventoryOpportunityIds: opportunityIds,
    perHotel,
    totalOrgKeys: orgKeys.size,
    totalOpportunityIds: opportunityIds.size,
  };
}

export default {
  GDI_PAID_DEEP_TWO_STAGE_DEDUPE_V1,
  orgDeepKey,
  editionIdentityKey,
  loadHotelInventoryOrgKeys,
  evaluatePrePaidDeepDedupe,
  reconcilePostDeepIdentity,
  buildInventoryDedupeContextForHotels,
};
