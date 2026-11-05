/**
 * Waterstone clean sales inventory V1 — local-only, reversible.
 *
 * Primary customer-facing list = Sales Ready IDs only.
 * Research Continuation → internal research queue (not deleted).
 * Watch → monitoring queue (not deleted).
 * Not Customer Ready → reversible local archive.
 *
 * Never deletes. Never writes Airtable. Never spends.
 * Protected Sales Ready IDs cannot be archived or hidden by this layer.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const WATERSTONE_CLEAN_SALES_INVENTORY_V1 =
  "gdi-waterstone-clean-sales-inventory-v1";

export const WATERSTONE_HOTEL_ID = "recgMYovrrZDJMqzX";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export const WATERSTONE_CLEAN_INVENTORY_REGISTRY_REL =
  "data/group-demand-intelligence/local-shadow/waterstone-clean-sales-inventory-v1.json";

/** Shippable client pack (lib/) — Railway always receives CODE overlays; local-shadow may not. */
export const WATERSTONE_CLEAN_28_CLIENT_FACING_PACK_REL =
  "lib/group-demand-intelligence/waterstone-clean-28-client-facing-pack-v1.json";

export const WATERSTONE_CLEAN_INVENTORY_REGISTRY_LIB_REL =
  "lib/group-demand-intelligence/waterstone-clean-sales-inventory-registry-v1.json";

/** Exact Sales Ready keep-active set from independent certification + research completion. */
export const WATERSTONE_SALES_READY_PROTECTED_IDS = Object.freeze([
  "gdi_opp_uca_convergence_2027_overflow_vip_housing_9",
  "gdi_sc_797d667984852b1c",
  "gdi_sc_6ffc0b24ef79d3f6",
  "gdi_ws_v72_df2965c8bb3f",
  "gdi_ws_v72_c7b53ff6f327",
  "gdi_ws_v72_3227578098ad",
  "gdi_ws_v72_f8787e429950",
  "gdi_ws_v72_2e5338207de9",
  "gdi_ws_v72_13a370031aae",
  "gdi_ws_v72_aa9f7baf6c9e",
  "gdi_ws_v72_775a48524791",
  "gdi_ws_v72_85a4fed44afd",
  "gdi_ws_v72_179eefcd3616",
  "gdi_ws_v72_6f115b961753",
  "gdi_ws_v72_40735f47a19f",
  "gdi_ws_v72_c70723cb20ff",
]);

/** Exact NCR archive IDs from certification. */
export const WATERSTONE_NCR_ARCHIVE_IDS = Object.freeze([
  "gdi_sc_rc_66ad7b1f0777", // Alliance Virtual Offices
  "gdi_e2d_0f3d43cf3a3c093e", // ANADE
  "gdi_e2d_91f668e040e94e8a", // AACY
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function cleanInventoryRegistryPath(root = ROOT) {
  return path.join(root, WATERSTONE_CLEAN_INVENTORY_REGISTRY_REL);
}

export function waterstoneClean28ClientFacingPackPath(root = ROOT) {
  return path.join(root, WATERSTONE_CLEAN_28_CLIENT_FACING_PACK_REL);
}

const jsonCache = new Map();

function readJsonIfExists(absPath) {
  if (!absPath) return null;
  if (jsonCache.has(absPath)) return jsonCache.get(absPath);
  if (!fs.existsSync(absPath)) {
    jsonCache.set(absPath, null);
    return null;
  }
  try {
    const doc = JSON.parse(fs.readFileSync(absPath, "utf8"));
    jsonCache.set(absPath, doc);
    return doc;
  } catch {
    jsonCache.set(absPath, null);
    return null;
  }
}

/**
 * Load founder-approved clean inventory registry.
 * Prefer local-shadow file, then shippable lib registry, then pack primaryIds
 * (applied:true) so production share parity does not depend on lean-excluded
 * local-shadow uploads.
 */
export function loadWaterstoneCleanSalesInventoryRegistry(root = ROOT) {
  const candidates = [
    cleanInventoryRegistryPath(root),
    path.join(root, WATERSTONE_CLEAN_INVENTORY_REGISTRY_LIB_REL),
  ];
  for (const p of candidates) {
    const doc = readJsonIfExists(p);
    if (!doc || typeof doc !== "object") continue;
    return {
      ...doc,
      version: doc.version || WATERSTONE_CLEAN_SALES_INVENTORY_V1,
      hotelId: WATERSTONE_HOTEL_ID,
      applied: doc.applied === true,
      primarySalesReadyIds: Array.isArray(doc.primarySalesReadyIds)
        ? doc.primarySalesReadyIds.map(String)
        : [...WATERSTONE_SALES_READY_PROTECTED_IDS],
      archivedIds: Array.isArray(doc.archivedIds)
        ? doc.archivedIds.map(String)
        : [],
      researchContinuationIds: Array.isArray(doc.researchContinuationIds)
        ? doc.researchContinuationIds.map(String)
        : [],
      watchIds: Array.isArray(doc.watchIds) ? doc.watchIds.map(String) : [],
      source: p,
    };
  }

  const pack = readJsonIfExists(waterstoneClean28ClientFacingPackPath(root));
  const packIds = Array.isArray(pack?.primaryIds)
    ? pack.primaryIds.map(String)
    : Array.isArray(pack?.opportunities)
      ? pack.opportunities.map((o) => String(o?.id || "")).filter(Boolean)
      : [];
  if (packIds.length === 28) {
    return {
      version: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
      hotelId: WATERSTONE_HOTEL_ID,
      applied: true,
      primarySalesReadyIds: packIds,
      archivedIds: [],
      researchContinuationIds: [],
      watchIds: [],
      source: "embedded_clean_28_client_facing_pack",
    };
  }

  return {
    version: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
    hotelId: WATERSTONE_HOTEL_ID,
    applied: false,
    primarySalesReadyIds: [...WATERSTONE_SALES_READY_PROTECTED_IDS],
    archivedIds: [],
    researchContinuationIds: [],
    watchIds: [],
    source: "missing_registry",
  };
}

/**
 * Union Waterstone clean-28 client-facing pack into the live opportunity set.
 * Ensures shadow-only primary IDs exist on production without Airtable writes.
 */
export function mergeWaterstoneClean28ClientFacingPack(
  opportunities = [],
  hotelId = "",
  opts = {},
) {
  if (String(hotelId) !== WATERSTONE_HOTEL_ID) return opportunities;
  const env = opts.env || process.env;
  if (String(env.GDI_WATERSTONE_CLEAN_SALES_INVENTORY || "1").trim() === "0") {
    return opportunities;
  }
  const root = opts.root || ROOT;
  const pack = readJsonIfExists(waterstoneClean28ClientFacingPackPath(root));
  const packOpps = Array.isArray(pack?.opportunities) ? pack.opportunities : [];
  if (!packOpps.length) return opportunities;

  const byId = new Map();
  for (const o of opportunities || []) {
    if (o?.id) byId.set(String(o.id), o);
  }
  for (const o of packOpps) {
    if (!o?.id) continue;
    const id = String(o.id);
    const cur = byId.get(id) || null;
    // Pack is the certified customer-facing authority. Do not retain stale
    // historical narrative/contact fields merely because the ID matched.
    const packContact = o.primaryContact || null;
    const curContact = cur?.primaryContact || null;
    const packHasNamed = Boolean(
      text(packContact?.name || packContact?.fullName || o.contactName),
    );
    const stamped = {
      ...(cur || {}),
      ...o,
      hotelId: WATERSTONE_HOTEL_ID,
      title: text(o.title) || text(cur?.title) || null,
      organizationName:
        text(o.organizationName) || text(cur?.organizationName) || null,
      whatIsHappening:
        text(o.whatIsHappening) || text(o.summaryWhat) || null,
      summaryWhat: text(o.summaryWhat) || text(o.whatIsHappening) || null,
      hotelOpportunityThesis:
        text(o.hotelOpportunityThesis) || text(o.summaryWhyHotel) || null,
      whyThisHotel:
        text(o.whyThisHotel) || text(o.hotelFitRationale) || null,
      hotelFitRationale:
        text(o.hotelFitRationale) || text(o.whyThisHotel) || null,
      recommendedAction:
        text(o.recommendedAction) ||
        text(o.whyEstablishContactNow) ||
        text(o.salesActionRecommended) ||
        null,
      primaryContact: packHasNamed
        ? packContact
        : packContact || curContact || null,
      contactName: packHasNamed
        ? text(packContact?.name || o.contactName)
        : text(o.contactName || curContact?.name || cur?.contactName) || null,
      sources: Array.isArray(o.sources) && o.sources.length
        ? o.sources
        : cur?.sources || [],
      evidenceUrls: Array.isArray(o.evidenceUrls) && o.evidenceUrls.length
        ? o.evidenceUrls
        : cur?.evidenceUrls || [],
      waterstonePrimarySalesInventory: true,
      waterstoneInventoryLane: "PRIMARY_SALES_READY",
      waterstoneClean28ClientPack: true,
      localActiveInventory: true,
      customerVisible: true,
      archived: false,
      isArchived: false,
      prospectSuppressed: false,
      doesNotAffectProduction: false,
      localShadowOnly: false,
      campaignCustomerFacingDisabled: false,
      founderCustomerVisibilityDisable: false,
    };
    byId.set(id, stamped);
  }
  return [...byId.values()];
}

export function writeWaterstoneCleanSalesInventoryRegistry(doc, root = ROOT) {
  const p = cleanInventoryRegistryPath(root);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const out = {
    ...doc,
    version: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
    hotelId: WATERSTONE_HOTEL_ID,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(p, JSON.stringify(out, null, 2));
  return out;
}

/**
 * Build applied registry from live disposition snapshot.
 * Protected Sales Ready IDs always win over disposition stamps.
 */
export function buildWaterstoneCleanInventoryRegistryFromOpportunities(
  opportunities = [],
  opts = {},
) {
  const protectedSet = new Set(WATERSTONE_SALES_READY_PROTECTED_IDS);
  const archiveSet = new Set(
    (opts.archivedIds || WATERSTONE_NCR_ARCHIVE_IDS).filter(
      (id) => id && !protectedSet.has(id),
    ),
  );
  const researchContinuationIds = [];
  const watchIds = [];
  const primarySalesReadyIds = [];

  for (const o of opportunities || []) {
    const id = String(o?.id || "");
    if (!id) continue;
    if (protectedSet.has(id)) {
      primarySalesReadyIds.push(id);
      continue;
    }
    if (archiveSet.has(id)) continue;
    const d = String(
      o.customerReadinessDisposition || o.contactCommercialDisposition || "",
    ).toUpperCase();
    if (d === "SALES_READY") {
      // Unexpected SR not in protected list — keep internal until requalify
      researchContinuationIds.push(id);
      continue;
    }
    if (d === "WATCH") watchIds.push(id);
    else if (d === "NOT_CUSTOMER_READY") archiveSet.add(id);
    else researchContinuationIds.push(id);
  }

  return {
    version: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
    hotelId: WATERSTONE_HOTEL_ID,
    applied: true,
    appliedAt: opts.nowIso || new Date().toISOString(),
    founderApproval:
      opts.founderApproval ||
      "LOCAL CLEAN INVENTORY APPLY — Waterstone research-completion / clean-inventory directive 2026-11-02",
    localOnly: true,
    doesNotWriteAirtable: true,
    reversible: true,
    primarySalesReadyIds: [...new Set(primarySalesReadyIds)],
    researchContinuationIds: [...new Set(researchContinuationIds)],
    watchIds: [...new Set(watchIds)],
    archivedIds: [...archiveSet],
    archiveReasons: {
      gdi_sc_rc_66ad7b1f0777:
        "COMPETING_MEETING_SPACE — Alliance Delray meeting-room product",
      gdi_e2d_0f3d43cf3a3c093e:
        "HISTORICAL — ANADE; no evidenced future Boca Waterstone edition",
      gdi_e2d_91f668e040e94e8a:
        "HISTORICAL — AACY caregiving institute past/undated",
    },
    reactivationPolicy:
      "Require commercial requalification before primary sales visibility; do not auto-resurrect on weekly org rediscovery",
  };
}

/**
 * Apply clean inventory visibility stamps.
 * Detail-by-id remains available; list filter uses archived / localActiveInventory.
 */
export function applyWaterstoneCleanSalesInventory(
  opportunities = [],
  hotelId = "",
  opts = {},
) {
  const env = opts.env || process.env;
  if (String(hotelId) !== WATERSTONE_HOTEL_ID) return opportunities;
  if (String(env.GDI_WATERSTONE_CLEAN_SALES_INVENTORY || "1").trim() === "0") {
    return opportunities;
  }

  const root = opts.root || ROOT;
  const doc = loadWaterstoneCleanSalesInventoryRegistry(root);
  if (doc.applied !== true) return opportunities;

  const protectedSet = new Set([
    ...WATERSTONE_SALES_READY_PROTECTED_IDS,
    ...(doc.primarySalesReadyIds || []),
  ]);
  const archived = new Set(
    (doc.archivedIds || []).filter((id) => id && !protectedSet.has(id)),
  );
  const research = new Set(doc.researchContinuationIds || []);
  const watch = new Set(doc.watchIds || []);
  const now = opts.nowIso || new Date().toISOString();

  return (opportunities || []).map((o) => {
    if (!o?.id) return o;
    const id = String(o.id);

    if (protectedSet.has(id)) {
      return {
        ...o,
        waterstoneCleanInventoryV1: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
        waterstonePrimarySalesInventory: true,
        waterstoneInventoryLane: "PRIMARY_SALES_READY",
        localActiveInventory: true,
        // Primary inventory always surfaces locally — prior wave rows may have
        // stamped customerVisible=false while held in INTERNAL_RESEARCH.
        customerVisible: true,
        archived: false,
        isArchived: false,
        prospectSuppressed: false,
        // Client share must not treat primary pack rows as local-only fixtures.
        doesNotAffectProduction: false,
        localShadowOnly: false,
        campaignCustomerFacingDisabled: false,
        founderCustomerVisibilityDisable: false,
        internalResearchEligible: true,
        monitoringEligible: false,
        historicalResearchRetained: true,
      };
    }

    if (archived.has(id)) {
      return {
        ...o,
        waterstoneCleanInventoryV1: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
        waterstonePrimarySalesInventory: false,
        waterstoneInventoryLane: "LOCAL_ARCHIVE",
        archived: true,
        isArchived: true,
        localActiveInventory: false,
        localInventorySuperseded: true,
        localInventorySupersededAt: o.localInventorySupersededAt || now,
        localInventorySupersedeReason:
          o.localInventorySupersedeReason ||
          "waterstone_clean_sales_inventory_v1_archive",
        customerVisible: false,
        prospectSuppressed: true,
        historicalResearchRetained: true,
        doesNotAffectProduction: true,
        internalResearchEligible: false,
        monitoringEligible: false,
        archiveReversible: true,
      };
    }

    if (watch.has(id)) {
      return {
        ...o,
        waterstoneCleanInventoryV1: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
        waterstonePrimarySalesInventory: false,
        waterstoneInventoryLane: "MONITORING",
        localActiveInventory: false,
        localInventorySuperseded: true,
        localInventorySupersedeReason:
          "waterstone_clean_sales_inventory_v1_watch_monitor",
        customerVisible: false,
        prospectSuppressed: true,
        historicalResearchRetained: true,
        doesNotAffectProduction: true,
        internalResearchEligible: true,
        monitoringEligible: true,
        archived: false,
      };
    }

    // Default: research continuation / other → internal research queue
    if (research.has(id) || true) {
      return {
        ...o,
        waterstoneCleanInventoryV1: WATERSTONE_CLEAN_SALES_INVENTORY_V1,
        waterstonePrimarySalesInventory: false,
        waterstoneInventoryLane: research.has(id)
          ? "INTERNAL_RESEARCH"
          : "INTERNAL_RESEARCH",
        localActiveInventory: false,
        localInventorySuperseded: true,
        localInventorySupersedeReason:
          "waterstone_clean_sales_inventory_v1_research_queue",
        customerVisible: false,
        prospectSuppressed: true,
        historicalResearchRetained: true,
        doesNotAffectProduction: true,
        internalResearchEligible: true,
        monitoringEligible: false,
        archived: false,
      };
    }
  });
}

/** Temporarily remove one ID from archive for restore certification, then caller rewrites. */
export function withTemporaryArchiveExclusion(doc, opportunityId) {
  const id = String(opportunityId || "");
  return {
    ...doc,
    archivedIds: (doc.archivedIds || []).filter((x) => String(x) !== id),
    temporaryRestoreTestId: id,
  };
}

export function assertProtectedIdsNeverArchived(doc = {}) {
  const protectedSet = new Set(WATERSTONE_SALES_READY_PROTECTED_IDS);
  const bad = (doc.archivedIds || []).filter((id) => protectedSet.has(String(id)));
  if (bad.length) {
    const err = new Error(
      `Protected Sales Ready IDs must never be archived: ${bad.join(",")}`,
    );
    err.code = "waterstone_protected_archive_violation";
    throw err;
  }
  return true;
}
