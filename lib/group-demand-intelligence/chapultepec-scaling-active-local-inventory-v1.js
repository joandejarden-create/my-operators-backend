/**
 * Chapultepec Scaling — active local inventory cohort (preview only).
 *
 * Pins the local Scaling Companies list for Grand Fiesta Americana Chapultepec
 * to protected gold IDs + verified FullE2E campaign IDs. Legacy Scaling rows
 * remain in shadow/history stores but are marked archived / inactive for the
 * active local list (not deleted; not Airtable; not published).
 *
 * Does not affect Association / Group Demand or other hotels.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isScalingOpportunity } from "./scaling-display-identity-v1.js";
import { PROTECTED_GOLD_IDS } from "./research/mexico-scaling-live-campaign-v1.js";

export const CHAPULTEPEC_SCALING_ACTIVE_LOCAL_INVENTORY_V1 =
  "chapultepec-scaling-active-local-inventory-v1";

export const CHAPULTEPEC_HOTEL_ID = "recterKmNPdntCm0v";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export const ACTIVE_INVENTORY_REGISTRY_REL =
  "data/group-demand-intelligence/local-shadow/chapultepec-scaling-active-local-inventory-v1.json";

/** Frozen FullE2E live IDs from campaign ledger (fallback if registry missing). */
export const LIVE_CAMPAIGN_V1_IDS = Object.freeze([
  "gdi_sc_mx_live_maxhub_a91fd65324",
  "gdi_sc_mx_live_netflix_a08f478716",
  "gdi_sc_mx_live_toshiba_global_commerce_solutions_beb1e9df68",
  "gdi_sc_mx_live_salesforce_2e8373bd68",
  "gdi_sc_mx_live_immunotec_d0ff2ad190",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function activeInventoryRegistryPath(root = ROOT) {
  return path.join(root, ACTIVE_INVENTORY_REGISTRY_REL);
}

export function loadChapultepecScalingActiveInventory(root = ROOT) {
  const p = activeInventoryRegistryPath(root);
  if (!fs.existsSync(p)) {
    return {
      version: CHAPULTEPEC_SCALING_ACTIVE_LOCAL_INVENTORY_V1,
      hotelId: CHAPULTEPEC_HOTEL_ID,
      protectedGoldIds: [...PROTECTED_GOLD_IDS],
      activeCampaignIds: [...LIVE_CAMPAIGN_V1_IDS],
      source: "code_fallback",
    };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return {
    ...doc,
    protectedGoldIds: Array.isArray(doc.protectedGoldIds)
      ? doc.protectedGoldIds.map(String)
      : [...PROTECTED_GOLD_IDS],
    activeCampaignIds: Array.isArray(doc.activeCampaignIds)
      ? doc.activeCampaignIds.map(String)
      : [...LIVE_CAMPAIGN_V1_IDS],
    source: p,
  };
}

export function chapultepecScalingActiveIdSet(root = ROOT) {
  const doc = loadChapultepecScalingActiveInventory(root);
  return new Set([
    ...(doc.protectedGoldIds || []),
    ...(doc.activeCampaignIds || []),
  ]);
}

export function isChapultepecHotel(hotelId = "") {
  return text(hotelId) === CHAPULTEPEC_HOTEL_ID;
}

/**
 * Scaling Companies family for this hotel's local inventory scoping.
 * Association / conference group demand must not match.
 */
export function isChapultepecScalingFamilyOpportunity(opportunity = {}) {
  if (isScalingOpportunity(opportunity)) return true;
  const type = text(opportunity.opportunityType).toUpperCase();
  if (type === "CORPORATE_ACCOUNT_RELATIONSHIP") return true;
  if (
    /Corporate Account Relationship/i.test(
      text(opportunity.opportunityTypeLabel || opportunity.segmentDisplayLabel),
    )
  ) {
    return true;
  }
  // Explicit scaling demand family / filter keys
  if (text(opportunity.demandFamily).toUpperCase() === "SCALING_COMPANY") return true;
  if (text(opportunity.opportunityTypeFilterKey).toUpperCase() === "SCALING_COMPANY") {
    return true;
  }
  return false;
}

export function isActiveChapultepecScalingLocalId(id, root = ROOT) {
  return chapultepecScalingActiveIdSet(root).has(String(id || ""));
}

/**
 * In-memory overlay: keep legacy Scaling rows in the merge map for history /
 * detail-by-id, but mark them inactive for the active local list.
 * Does not write shadow files or Airtable.
 */
export function applyChapultepecScalingActiveLocalInventory(
  opportunities = [],
  hotelId = "",
  opts = {},
) {
  const env = opts.env || process.env;
  if (
    String(env.GDI_CHAPULTEPEC_SCALING_ACTIVE_INVENTORY || "1").trim() === "0"
  ) {
    return opportunities;
  }
  if (!isChapultepecHotel(hotelId)) return opportunities;

  const root = opts.root || ROOT;
  const active = chapultepecScalingActiveIdSet(root);
  const cohortVersion = CHAPULTEPEC_SCALING_ACTIVE_LOCAL_INVENTORY_V1;
  const now = opts.nowIso || new Date().toISOString();

  return (opportunities || []).map((o) => {
    if (!o?.id) return o;
    if (!isChapultepecScalingFamilyOpportunity(o)) {
      return o;
    }
    const id = String(o.id);
    if (active.has(id)) {
      return {
        ...o,
        localActiveInventory: true,
        localActiveInventoryCohort: cohortVersion,
        localInventorySuperseded: false,
        // Never flip gold/live to archived via this overlay
        archived: o.archived === true ? o.archived : false,
      };
    }
    // Legacy Scaling — retain object, hide from active local customer list
    return {
      ...o,
      localActiveInventory: false,
      localActiveInventoryCohort: cohortVersion,
      localInventorySuperseded: true,
      localInventorySupersededAt: o.localInventorySupersededAt || now,
      localInventorySupersedeReason:
        o.localInventorySupersedeReason ||
        "chapultepec_scaling_active_cohort_v1_excludes_legacy",
      archived: true,
      // Inventory visibility + customer list treat archived as non-active
      doesNotAffectProduction: true,
      historicalResearchRetained: true,
    };
  });
}

/**
 * Admit a newly verified FullE2E opportunity ID into the active registry
 * (caller writes file). Never removes protected gold IDs.
 */
export function buildRegistryWithAdmission(extraIds = [], root = ROOT) {
  const doc = loadChapultepecScalingActiveInventory(root);
  const gold = new Set(doc.protectedGoldIds || PROTECTED_GOLD_IDS);
  const campaign = new Set(doc.activeCampaignIds || []);
  for (const id of extraIds) {
    const s = String(id || "").trim();
    if (!s || gold.has(s)) continue;
    campaign.add(s);
  }
  return {
    ...doc,
    updatedAt: new Date().toISOString(),
    protectedGoldIds: [...gold],
    activeCampaignIds: [...campaign],
  };
}
