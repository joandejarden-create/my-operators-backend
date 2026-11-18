/**
 * Complete research persistence V1 — three lifecycle layers.
 * Research Inventory ≠ Qualified GDO ≠ Customer Publication.
 * Production writes and schema creates remain fail-closed.
 */

import fs from "node:fs";
import path from "node:path";
import { evaluateFullE2eAirtableWriteEligibility } from "./gdi-full-e2e-airtable-persistence-eligibility-v1.js";
import {
  GDI_AIRTABLE_BASE,
  GDI_AIRTABLE_TABLES,
  AIRTABLE_PRODUCTION_WRITE_GUARD,
} from "./gdi-full-e2e-airtable-persistence-contract-v1.js";
import { ROCK_CREEK_VENUE_IDENTITY } from "./gdi-multi-hotel-airtable-reconciliation-v2.js";
import { YOTEL_LEGACY_DELETE_AUTH } from "./gdi-yotel-legacy-gdo-deletion-v1.js";

export const GDI_COMPLETE_RESEARCH_PERSISTENCE_V1 =
  "gdi-complete-research-persistence-v1";

/** Distinct lifecycle layers (never collapse into one ambiguous status). */
export const RESEARCH_LIFECYCLE_LAYER = Object.freeze({
  RESEARCH_INVENTORY: "RESEARCH_INVENTORY",
  QUALIFIED_GDO: "QUALIFIED_GDO",
  CUSTOMER_PUBLICATION: "CUSTOMER_PUBLICATION",
});

/**
 * Proposed minimal Airtable table for research-only / watchlist / exploratory
 * hotel opportunities. NOT created until founder schema approval.
 */
export const PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES = Object.freeze({
  name: "GDI Hotel Research Opportunities",
  proposedTableId: "tblbkDj0vhQLfrssV",
  status: "CREATED_IN_AIRTABLE",
  identityField: "opportunityId",
  purpose:
    "Durable Airtable mirror of complete hotel research inventory without implying commercial GDO qualification or customer publication.",
  requiredFields: Object.freeze([
    "opportunityId",
    "hotelId",
    "organizationName",
    "researchLifecycleStatus",
    "commercialEligibilityStatus",
    "demandThesis",
    "priority",
    "title",
    "segment",
    "sourceSummary",
    "firstDiscoveredAt",
    "firstDiscoveredRunId",
    "lastResearchedAt",
    "lastMaterialChangeAt",
    "dgGeneratorId",
    "researchPayloadJson",
    "schemaVersion",
  ]),
  forbiddenUses: Object.freeze([
    "Do not treat rows as customer-published",
    "Do not auto-promote into CURRENT packages",
    "Do not use as substitute for Demand Generators",
    "Do not recreate YOTEL legacy 108 GDO IDs",
  ]),
  reuseExistingInstead: Object.freeze({
    qualifiedCommercial: GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name,
    orgIdentity: GDI_AIRTABLE_TABLES.DEMAND_GENERATORS.name,
    researchTargets: GDI_AIRTABLE_TABLES.GDI_RESEARCH_TARGETS.name,
    filesystemFallback:
      "data/group-demand-intelligence/hotels/{hotelId}/opportunities.json remains operational SoT until table is approved and backfilled",
  }),
});

export const COMPLETE_PERSISTENCE_CONTRACT = Object.freeze({
  version: GDI_COMPLETE_RESEARCH_PERSISTENCE_V1,
  base: GDI_AIRTABLE_BASE,
  layers: RESEARCH_LIFECYCLE_LAYER,
  researchInventoryDestination: PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES,
  qualifiedGdoDestination: GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES,
  customerPublicationDestination:
    "lib/group-demand-intelligence/bundled-publication-packages/{hotelId}/CURRENT.json",
  yotelLegacyGdoIdsMustNotRecreate: true,
  yotelHotelId: YOTEL_LEGACY_DELETE_AUTH.hotelId,
  rockCreekNextAllowlist: {
    airtableRecordId: ROCK_CREEK_VENUE_IDENTITY.airtableRecordId,
    field: "Venue ID",
    after: ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId,
    authorizedByThisPrompt: false,
  },
  productionWriteGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
  schemaChangeGuard: Object.freeze({
    blocked: true,
    code: "SCHEMA_CHANGES_REQUIRE_FOUNDER_APPROVAL",
  }),
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Normalize demand thesis enums; ignore free-text hotelOpportunityThesis prose. */
export function extractDemandThesisEnum(opp = {}) {
  const candidates = [
    opp.demandThesis,
    opp.demandThesisStandard,
    opp.thesisStandard,
    opp.demandStatus,
    opp.qualification?.demandThesis,
    opp.commercial?.demandThesis,
  ];
  for (const raw of candidates) {
    const u = text(raw).toUpperCase().replace(/\s+/g, "_");
    if (!u) continue;
    if (
      u === "CONFIRMED_DEMAND" ||
      u === "CONFIRMED" ||
      u === "RELIABLE_INFERRED_DEMAND" ||
      u === "RELIABLE_INFERRED" ||
      u === "SPECULATIVE_DEMAND" ||
      u === "SPECULATIVE" ||
      u === "CONTRADICTORY_OR_NEGATIVE" ||
      u === "WATCHLIST"
    ) {
      return u === "CONFIRMED"
        ? "CONFIRMED_DEMAND"
        : u === "RELIABLE_INFERRED"
          ? "RELIABLE_INFERRED_DEMAND"
          : u === "SPECULATIVE"
            ? "SPECULATIVE_DEMAND"
            : u;
    }
  }
  return null;
}

export function classifyOpportunityLifecycleLayer(opp = {}, hotel = {}) {
  const demandThesis = extractDemandThesisEnum(opp) || opp.demandThesis || null;
  const eligibility = evaluateFullE2eAirtableWriteEligibility({
    kind: "HOTEL_OPPORTUNITY",
    candidate: {
      ...opp,
      opportunityId: opp.opportunityId || opp.id,
      hotelId: hotel.hotelId || opp.hotelId,
      demandThesis,
      demandThesisStandard: demandThesis,
    },
  });
  const allowedGdo = (eligibility.allowedTables || []).includes(
    "Group Demand Opportunities",
  );
  // Research inventory always applies; GDO is an additional promotion layer.
  const research = {
    layer: RESEARCH_LIFECYCLE_LAYER.RESEARCH_INVENTORY,
    destinationTable: PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.name,
    destinationTableId: PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.proposedTableId,
    schemaReady: PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.status === "CREATED_IN_AIRTABLE",
    decision:
      PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.status === "CREATED_IN_AIRTABLE"
        ? "UPSERT_RESEARCH_INVENTORY"
        : "UPSERT_RESEARCH_INVENTORY_PENDING_SCHEMA",
    eligibility,
  };
  if (allowedGdo) {
    return {
      ...research,
      commerciallyQualified: true,
      gdoLayer: RESEARCH_LIFECYCLE_LAYER.QUALIFIED_GDO,
      gdoDestinationTable: GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name,
      gdoDestinationTableId: GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.tableId,
      gdoDecision: "UPSERT_QUALIFIED_GDO",
    };
  }
  return {
    ...research,
    commerciallyQualified: false,
    gdoLayer: null,
    gdoDecision: "NO_GDO_BELOW_ELIGIBILITY",
  };
}

export function discoverLocalHotelUniverse(repoRoot) {
  const dataRoot = path.join(repoRoot, "data/group-demand-intelligence/hotels");
  const configRoot = path.join(repoRoot, "config/group-demand-intelligence/hotels");
  const hotels = new Map();

  const add = (hotelId, source) => {
    const id = text(hotelId);
    if (!id || id.startsWith("gdi_hotel_")) {
      // provisional alias dirs — record but mark
      if (!id) return;
    }
    const prev = hotels.get(id) || {
      hotelId: id,
      sources: [],
      provisionalAlias: id.startsWith("gdi_hotel_"),
    };
    if (!prev.sources.includes(source)) prev.sources.push(source);
    hotels.set(id, prev);
  };

  if (fs.existsSync(dataRoot)) {
    for (const ent of fs.readdirSync(dataRoot, { withFileTypes: true })) {
      if (!ent.isDirectory()) continue;
      const oppPath = path.join(dataRoot, ent.name, "opportunities.json");
      if (fs.existsSync(oppPath)) add(ent.name, "data/opportunities.json");
      else add(ent.name, "data/dir");
    }
  }
  if (fs.existsSync(configRoot)) {
    for (const f of fs.readdirSync(configRoot)) {
      if (!f.endsWith(".json")) continue;
      add(f.replace(/\.json$/, ""), "config");
    }
  }
  return [...hotels.values()].sort((a, b) => a.hotelId.localeCompare(b.hotelId));
}

export function loadLocalOpportunities(repoRoot, hotelId) {
  const p = path.join(
    repoRoot,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) {
    return { present: false, path: p, count: 0, rows: [], byId: new Map() };
  }
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const arr = Array.isArray(j) ? j : j.opportunities || [];
  const byId = new Map();
  const rows = [];
  for (const o of arr) {
    const id = text(o.id || o.opportunityId);
    if (!id) continue;
    const primaryContact =
      o.primaryContact || o.verifiedContact || o.contact || null;
    const demandThesis = extractDemandThesisEnum(o);
    const row = {
      id,
      opportunityId: id,
      hotelId: text(o.hotelId) || hotelId,
      organizationName: o.organizationName || o.company || null,
      title: o.title || o.opportunityName || o.displayTitle || null,
      priority: o.priority || null,
      demandThesis,
      demandStatus: o.demandStatus || null,
      hotelOpportunityThesis: o.hotelOpportunityThesis || null,
      opportunityQualification: o.opportunityQualification || null,
      inventoryPopulationPrimary: o.inventoryPopulationPrimary || null,
      commercialActionability: o.commercialActionability || null,
      status: o.status || o.lifecycleStatus || null,
      disposition: o.disposition || o.evaluationLabel || null,
      dgGeneratorId: o.dgGeneratorId || null,
      firstDiscoveredAt: o.firstDiscoveredAt || o.firstSeenAt || null,
      firstDiscoveredRunId: o.firstDiscoveredRunId || o.firstSeenRunId || null,
      lastMaterialChangeAt: o.lastMaterialChangeAt || null,
      verifiedNamedContact: o.verifiedNamedContact === true,
      hasCredibleCommercialEntryPoint:
        o.hasCredibleCommercialEntryPoint === true ||
        ["DIRECT_BUYER", "RELEVANT_COMMERCIAL_ENTRY_POINT"].includes(
          text(primaryContact?.contactClass).toUpperCase(),
        ),
      primaryContact: primaryContact
        ? {
            name: primaryContact.name || primaryContact.fullName || null,
            role: primaryContact.role || primaryContact.title || null,
            email: primaryContact.email || null,
            contactClass: primaryContact.contactClass || null,
          }
        : null,
      hotelName: (!Array.isArray(j) && j.hotelName) || o.hotelName || null,
    };
    byId.set(id, row);
    rows.push(row);
  }
  return {
    present: true,
    path: p,
    count: byId.size,
    rows,
    byId,
    hotelName: (!Array.isArray(j) && j.hotelName) || null,
  };
}

export function publicationState(repoRoot, hotelId) {
  const dir = path.join(
    repoRoot,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const out = { staged: null, current: null };
  for (const name of ["STAGED.json", "CURRENT.json"]) {
    const p = path.join(dir, name);
    const key = name.replace(".json", "").toLowerCase();
    if (!fs.existsSync(p)) {
      out[key] = null;
      continue;
    }
    try {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      out[key] = {
        version: j.version || j.packageVersion || null,
        opportunityCount: Array.isArray(j.opportunities)
          ? j.opportunities.length
          : null,
      };
    } catch {
      out[key] = { parseError: true };
    }
  }
  return out;
}

/**
 * Plan one hotel's complete persistence (dry-run only).
 * @param {object} opts
 * @param {Set<string>} [opts.forbiddenRecreateIds] YOTEL legacy GDO opportunity IDs
 */
export function planHotelCompletePersistence({
  hotel,
  filesystem,
  airtableGdos = [],
  forbiddenRecreateIds = new Set(),
} = {}) {
  const atById = new Map();
  for (const o of airtableGdos) {
    const id = text(o.id || o.opportunityId);
    if (!id) continue;
    atById.set(id, {
      id,
      airtableRecordId: o._airtableRecordId || o.airtableRecordId || null,
      organizationName: o.organizationName || null,
      title: o.title || o.opportunityName || null,
      hotelId: o.hotelId || hotel.hotelId,
    });
  }

  const researchCreates = [];
  const gdoCreates = [];
  const gdoUpdates = [];
  const gdoUnchanged = [];
  const gdoPreserveExisting = [];
  const conflicts = [];
  const forbiddenBlocked = [];
  const rows = [];

  for (const fsRow of filesystem.rows || []) {
    if (forbiddenRecreateIds.has(fsRow.opportunityId)) {
      forbiddenBlocked.push({
        opportunityId: fsRow.opportunityId,
        reason: "YOTEL_LEGACY_GDO_ID_MUST_NOT_RECREATE",
      });
      rows.push({
        opportunityId: fsRow.opportunityId,
        layer: "FORBIDDEN",
        decision: "BLOCK_LEGACY_RECREATE",
      });
      continue;
    }

    const cls = classifyOpportunityLifecycleLayer(fsRow, hotel);
    const at = atById.get(fsRow.opportunityId) || null;

    // Layer A — always account for research inventory (schema-gated)
    researchCreates.push({
      opportunityId: fsRow.opportunityId,
      organizationName: fsRow.organizationName,
      title: fsRow.title,
      decision: "CREATE_RESEARCH_INVENTORY_PENDING_SCHEMA",
      destination: PROPOSED_GDI_HOTEL_RESEARCH_OPPORTUNITIES.name,
      commerciallyQualified: cls.commerciallyQualified === true,
      commercialEligibilityStatus:
        cls.eligibility?.commercialQualificationStatus || "RESEARCH_ONLY",
      eligibilityReasons: cls.eligibility?.reasons || [],
      alsoPresentInGdo: Boolean(at),
      filesystemDurable: true,
    });

    // Layer B — qualified GDO registry
    let gdoDecision = "NO_GDO";
    if (at) {
      if (
        at.organizationName &&
        fsRow.organizationName &&
        text(at.organizationName).toLowerCase() !==
          text(fsRow.organizationName).toLowerCase()
      ) {
        conflicts.push({
          opportunityId: fsRow.opportunityId,
          reason: "ORGANIZATION_NAME_MISMATCH",
          filesystem: fsRow.organizationName,
          airtable: at.organizationName,
          airtableRecordId: at.airtableRecordId,
        });
        gdoDecision = "CONFLICT";
      } else {
        // Preserve existing GDO regardless of re-qualification — do not overwrite
        gdoPreserveExisting.push(fsRow.opportunityId);
        gdoUnchanged.push(fsRow.opportunityId);
        gdoDecision = "PRESERVE_EXISTING_GDO";
      }
    } else if (cls.commerciallyQualified) {
      gdoCreates.push({
        opportunityId: fsRow.opportunityId,
        organizationName: fsRow.organizationName,
        title: fsRow.title,
        decision: "CREATE_QUALIFIED_GDO",
        eligibilityReasons: cls.eligibility?.reasons || [],
      });
      gdoDecision = "CREATE_QUALIFIED_GDO";
    } else {
      gdoDecision = "RESEARCH_ONLY_NO_GDO_CREATE";
    }

    rows.push({
      opportunityId: fsRow.opportunityId,
      researchDecision: "NEEDS_SCHEMA_THEN_PERSIST",
      gdoDecision,
      commerciallyQualified: cls.commerciallyQualified === true,
      airtableRecordId: at?.airtableRecordId || null,
      layer: cls.commerciallyQualified
        ? RESEARCH_LIFECYCLE_LAYER.QUALIFIED_GDO
        : RESEARCH_LIFECYCLE_LAYER.RESEARCH_INVENTORY,
    });
  }

  const airtableOnly = [];
  for (const [id, at] of atById) {
    if (filesystem.byId?.has(id)) continue;
    airtableOnly.push({
      opportunityId: id,
      airtableRecordId: at.airtableRecordId,
      organizationName: at.organizationName,
      treatment:
        hotel.hotelId === YOTEL_LEGACY_DELETE_AUTH.hotelId
          ? "UNEXPECTED_AFTER_YOTEL_CLEANUP"
          : "PRESERVE_AIRTABLE_ONLY_PENDING_CLASSIFICATION",
    });
  }

  const overlap = (filesystem.rows || []).filter((r) =>
    atById.has(r.opportunityId),
  ).length;

  return {
    hotel,
    filesystemCount: filesystem.count || 0,
    airtableGdoCount: atById.size,
    idOverlapCount: overlap,
    counts: {
      researchInventoryPendingSchema: researchCreates.length,
      qualifiedGdoCreate: gdoCreates.length,
      qualifiedGdoUnchanged: gdoUnchanged.length,
      qualifiedGdoPreserveExisting: gdoPreserveExisting.length,
      qualifiedGdoUpdate: gdoUpdates.length,
      conflicts: conflicts.length,
      airtableOnly: airtableOnly.length,
      forbiddenBlocked: forbiddenBlocked.length,
      commerciallyQualifiedFs: rows.filter((r) => r.commerciallyQualified)
        .length,
    },
    researchCreates,
    gdoCreates,
    gdoUpdates,
    gdoUnchanged,
    gdoPreserveExisting,
    conflicts,
    airtableOnly,
    forbiddenBlocked,
    rows,
  };
}

export function assertNoBulkProductionApply(opts = {}) {
  if (opts.apply === true || opts.authorizeProductionWrites === true) {
    const err = new Error(
      "complete_research_persistence_bulk_apply_not_authorized",
    );
    err.code = "BULK_PRODUCTION_WRITES_NOT_AUTHORIZED";
    throw err;
  }
  if (opts.createSchema === true) {
    const err = new Error("schema_create_not_authorized");
    err.code = "SCHEMA_CHANGES_REQUIRE_FOUNDER_APPROVAL";
    throw err;
  }
  return { ok: true };
}

export function loadYotelForbiddenRecreateIds(repoRoot) {
  const p = path.join(
    repoRoot,
    "reports/gdi-yotel-replacement-other-hotel-reconciliation-v1/YOTEL_108_RECORD_DELETE_MANIFEST.json",
  );
  if (!fs.existsSync(p)) return new Set();
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return new Set((j.opportunityIdsExact || []).map(text).filter(Boolean));
}
