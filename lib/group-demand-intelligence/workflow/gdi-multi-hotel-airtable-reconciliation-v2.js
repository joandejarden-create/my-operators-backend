/**
 * Multi-hotel Airtable ↔ filesystem reconciliation + dry-run sync planner V2.
 * Read-only / non-mutating. No production writes.
 */

import fs from "node:fs";
import path from "node:path";
import { computeVenueId } from "../private-events/venue-entity.js";
import { evaluateFullE2eAirtableWriteEligibility } from "./gdi-full-e2e-airtable-persistence-eligibility-v1.js";
import {
  extractSharedDemandIdentity,
  assertCrossHotelSharedIdentityIsolation,
} from "./gdi-shared-demand-identity-v1.js";
import {
  buildDurableWeeklyChangeAnnotation,
  WEEKLY_CHANGE_DURABILITY_CONTRACT,
} from "./gdi-weekly-change-durability-v1.js";
import { AIRTABLE_PRODUCTION_WRITE_GUARD } from "./gdi-full-e2e-airtable-persistence-contract-v1.js";

export const GDI_MULTI_HOTEL_AIRTABLE_RECONCILIATION_V2 =
  "gdi-multi-hotel-airtable-reconciliation-v2";

export const ROCK_CREEK_VENUE_IDENTITY = Object.freeze({
  airtableRecordId: "recf35Q9B6jpX3Ivm",
  structuredIdAfterPilot: "pev_42542bf58aac936b",
  historicalDomainId: "pev_53b689545914754d",
  authoritativeCanonicalId: "pev_53b689545914754d",
  aliasErroneousPilotId: "pev_42542bf58aac936b",
  venueName: "Rock Creek Mansion",
  officialDomain: "rockcreekmansion.com",
  samePhysicalVenue: true,
  divergenceCause:
    "Dry-run/pilot computed venueId from name+city without official domain; structured Venue ID was updated while Venue Payload JSON retained the domain-based historical ID.",
});

export const STORAGE_AUTHORITY_V2 = Object.freeze({
  researchExecution: "filesystem.execution_ledger_and_hotel_research_stores",
  globalIntelligence:
    "airtable.DemandGenerators|DemandPrograms|Signals|PrivateEventVenues",
  hotelSpecificOpportunities:
    "airtable.GroupDemandOpportunities after reconciliation; filesystem remains research SoT until authorized sync",
  customerPublication: "founder_approved.publication_packages.CURRENT",
  hotelFeedback: "airtable.Decisions|DecisionEvents + authorized feedback stores",
  neverAutoPromoteToCurrent: true,
  neverCopyHotelPrivateFeedbackToGlobal: true,
});

export const SYNC_DECISION = Object.freeze({
  CREATE: "CREATE",
  UPDATE: "UPDATE",
  UNCHANGED: "UNCHANGED",
  DUPLICATE_LINKED: "DUPLICATE_LINKED",
  DEFERRED: "DEFERRED",
  CONFLICT: "CONFLICT",
  REJECTED: "REJECTED",
});

export const INVENTORY_CLASS = Object.freeze({
  SHARED: "SHARED",
  SHARED_IDENTITY_CONFLICT: "SHARED_IDENTITY_CONFLICT",
  AIRTABLE_ONLY_ACTIVE: "AIRTABLE_ONLY_ACTIVE",
  AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED: "AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED",
  FILESYSTEM_ONLY: "FILESYSTEM_ONLY",
  INCORRECT_HOTEL_RELATIONSHIP: "INCORRECT_HOTEL_RELATIONSHIP",
  IDENTITY_COLLISION: "IDENTITY_COLLISION",
  UNKNOWN: "UNKNOWN",
});

export const REFERENCE_HOTELS_V2 = Object.freeze([
  {
    key: "yotel",
    name: "YOTEL Geneva Lake",
    hotelId: "recrPQcZg7SFARRb2",
    cohort: "PRIMARY",
  },
  {
    key: "radisson",
    name: "Radisson Blu Toronto Downtown",
    hotelId: "recH9h3McgMNFQEla",
    cohort: "PRIMARY",
  },
  {
    key: "guadalajara",
    name: "Sheraton Guadalajara Expo",
    hotelId: "recCAGqkERSXgiku4",
    cohort: "PRIMARY",
  },
  {
    key: "bethesda",
    name: "Bethesda Marriott",
    hotelId: "recLuxvwwxID7U2B8",
    cohort: "REFERENCE",
  },
  {
    key: "wyndham",
    name: "Wyndham San Diego Bayside",
    hotelId: "recnPB3Q02NO8Ma1o",
    cohort: "REFERENCE",
  },
  {
    key: "castillo",
    name: "Castillo Hotel Son Vida",
    hotelId: "rec82D9zpB8fede1I",
    cohort: "REFERENCE",
  },
  {
    key: "nobu",
    name: "Nobu Hotel London Portman Square",
    hotelId: "recC2kbCaS48jFjFF",
    cohort: "REFERENCE",
  },
  {
    key: "waterstone",
    name: "Waterstone Resort & Marina",
    hotelId: "recgMYovrrZDJMqzX",
    cohort: "REFERENCE",
  },
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

/**
 * Prefer domain-stable venue IDs. Geo-only IDs are provisional until domain evidence exists.
 */
export function resolveAuthoritativeVenueId(input = {}) {
  const venueName = text(input.venueName || input.organizationName);
  const officialDomain = text(input.officialDomain);
  const website = text(input.website || input.sourceUrl);
  const city = text(input.city) || null;
  const region = text(input.region) || null;
  const country = text(input.country) || "US";

  const domainId =
    officialDomain || website
      ? computeVenueId({
          venueName,
          officialDomain: officialDomain || null,
          website: website || null,
        })
      : null;
  const geoId = computeVenueId({
    venueName,
    city,
    region,
    country,
  });

  const existingStructured = text(input.existingStructuredVenueId) || null;
  const existingPayload = text(input.existingPayloadVenueId) || null;

  if (domainId) {
    return {
      authoritativeId: domainId,
      provisionalGeoId: geoId,
      reason: "DOMAIN_PREFERRED",
      conflictWithStructured:
        existingStructured && existingStructured !== domainId
          ? existingStructured
          : null,
      conflictWithPayload:
        existingPayload && existingPayload !== domainId ? existingPayload : null,
      shouldRewriteStructuredId: false,
    };
  }

  return {
    authoritativeId: existingPayload || existingStructured || geoId,
    provisionalGeoId: geoId,
    reason: existingPayload || existingStructured ? "PRESERVE_EXISTING" : "GEO_PROVISIONAL",
    conflictWithStructured: null,
    conflictWithPayload: null,
    shouldRewriteStructuredId: false,
  };
}

export function detectEmbeddedStructuredVenueConflict({
  structuredVenueId,
  payloadVenueId,
} = {}) {
  const a = text(structuredVenueId);
  const b = text(payloadVenueId);
  if (!a || !b) {
    return {
      conflict: false,
      code: a || b ? "SINGLE_SIDE_PRESENT" : "BOTH_ABSENT",
    };
  }
  if (a === b) return { conflict: false, code: "ALIGNED" };
  return {
    conflict: true,
    code: "EMBEDDED_JSON_STRUCTURED_ID_MISMATCH",
    structuredVenueId: a,
    payloadVenueId: b,
  };
}

export function analyzeRockCreekVenueIdentity(liveRecord = {}) {
  const structured =
    text(liveRecord.structuredVenueId) ||
    ROCK_CREEK_VENUE_IDENTITY.structuredIdAfterPilot;
  const payload =
    text(liveRecord.payloadVenueId) ||
    ROCK_CREEK_VENUE_IDENTITY.historicalDomainId;
  const domainRecompute = computeVenueId({
    venueName: ROCK_CREEK_VENUE_IDENTITY.venueName,
    officialDomain: ROCK_CREEK_VENUE_IDENTITY.officialDomain,
  });
  const geoRecompute = computeVenueId({
    venueName: ROCK_CREEK_VENUE_IDENTITY.venueName,
    city: "Bethesda",
    country: "US",
  });
  const conflict = detectEmbeddedStructuredVenueConflict({
    structuredVenueId: structured,
    payloadVenueId: payload,
  });

  return {
    ...ROCK_CREEK_VENUE_IDENTITY,
    live: {
      structuredVenueId: structured,
      payloadVenueId: payload,
      website: liveRecord.website || null,
      officialDomain: liveRecord.officialDomain || null,
      city: liveRecord.city || null,
      onSiteLodgingStatus: liveRecord.onSiteLodgingStatus || null,
    },
    recomputation: {
      domainBased: domainRecompute,
      cityOnly: geoRecompute,
      domainMatchesHistorical:
        domainRecompute === ROCK_CREEK_VENUE_IDENTITY.historicalDomainId,
      cityOnlyMatchesPilot:
        geoRecompute === ROCK_CREEK_VENUE_IDENTITY.structuredIdAfterPilot,
    },
    conflict,
    remediation: {
      mutationInThisPhase: false,
      restoreStructuredVenueIdTo: ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId,
      keepPayloadAlignedTo: ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId,
      treatPilotIdAs: "HISTORICAL_ALIAS_ERRONEOUS_REWRITE",
      requireFounderAuthorizationForWrite: true,
      guards: [
        "NEVER_REWRITE_VENUE_ID_FROM_GEO_ONLY_WHEN_DOMAIN_KNOWN",
        "MATCH_EXISTING_BY_DOMAIN_OR_NAME_BEFORE_CREATE",
        "PATCH_STRUCTURED_AND_PAYLOAD_TOGETHER_WHEN_AUTHORIZED",
      ],
    },
  };
}

export function loadFilesystemOpportunities(repoRoot, hotelId) {
  const p = path.join(
    repoRoot,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) {
    return { present: false, path: p, byId: new Map(), count: 0, rows: [] };
  }
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const arr = Array.isArray(j) ? j : j.opportunities || [];
  const byId = new Map();
  const rows = [];
  for (const o of arr) {
    const id = text(o.id || o.opportunityId);
    if (!id) continue;
    const row = {
      id,
      opportunityId: id,
      hotelId: text(o.hotelId) || hotelId,
      organizationName: o.organizationName || o.company || null,
      title: o.title || o.opportunityName || null,
      status: o.status || o.lifecycleStatus || null,
      demandThesis:
        o.demandThesis || o.demandThesisStandard || o.thesisStandard || null,
      priority: o.priority || null,
      disposition: o.disposition || o.evaluationLabel || null,
      officialDomain: o.officialDomain || null,
      website: o.website || o.sourceUrl || null,
      sourceUrl: o.sourceUrl || null,
      firstDiscoveredAt: o.firstDiscoveredAt || o.firstSeenAt || null,
      firstDiscoveredRunId: o.firstDiscoveredRunId || o.firstSeenRunId || null,
      lastMaterialChangeAt: o.lastMaterialChangeAt || null,
      lastMaterialChangeRunId: o.lastMaterialChangeRunId || null,
      dgGeneratorId: o.dgGeneratorId || null,
      dgProgramId: o.dgProgramId || null,
      dgSignalId: o.dgSignalId || null,
      peVenueId: o.peVenueId || null,
      verifiedNamedContact: o.verifiedNamedContact === true,
      hasCredibleCommercialEntryPoint:
        o.hasCredibleCommercialEntryPoint === true,
      salesReadyContacts: o.salesReadyContacts || 0,
      primaryContact: o.primaryContact
        ? {
            name: o.primaryContact.name || o.primaryContact.fullName || null,
            role: o.primaryContact.role || o.primaryContact.title || null,
            // omit email/phone from planner outputs
          }
        : null,
      raw: o,
    };
    byId.set(id, row);
    rows.push(row);
  }
  return { present: true, path: p, byId, count: byId.size, rows };
}

export function publicationPointers(repoRoot, hotelId) {
  const dir = path.join(
    repoRoot,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const out = {};
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
          : Array.isArray(j.items)
            ? j.items.length
            : null,
        path: p,
      };
    } catch {
      out[key] = { parseError: true, path: p };
    }
  }
  return out;
}

export function classifyInventoryPair(fsRow, atRow, hotelId) {
  if (fsRow && atRow) {
    const conflicts = [];
    if (
      fsRow.organizationName &&
      atRow.organizationName &&
      text(fsRow.organizationName).toLowerCase() !==
        text(atRow.organizationName).toLowerCase()
    ) {
      conflicts.push("ORGANIZATION_NAME_MISMATCH");
    }
    const fsHotel = text(fsRow.hotelId || hotelId);
    const atHotel = text(atRow.hotelId || hotelId);
    if (fsHotel && atHotel && fsHotel !== atHotel) {
      conflicts.push("HOTEL_ID_MISMATCH");
    }
    if (conflicts.includes("HOTEL_ID_MISMATCH")) {
      return {
        class: INVENTORY_CLASS.INCORRECT_HOTEL_RELATIONSHIP,
        conflicts,
      };
    }
    return {
      class: conflicts.length
        ? INVENTORY_CLASS.SHARED_IDENTITY_CONFLICT
        : INVENTORY_CLASS.SHARED,
      conflicts,
    };
  }
  if (atRow && !fsRow) {
    const active = /active|live|open|pursue|watch/i.test(
      String(atRow.status || ""),
    );
    return {
      class: active
        ? INVENTORY_CLASS.AIRTABLE_ONLY_ACTIVE
        : INVENTORY_CLASS.AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED,
      conflicts: [],
    };
  }
  if (fsRow && !atRow) {
    return { class: INVENTORY_CLASS.FILESYSTEM_ONLY, conflicts: [] };
  }
  return { class: INVENTORY_CLASS.UNKNOWN, conflicts: [] };
}

export function classifySyncDecisionForFilesystemOnly(fsRow = {}, hotel = {}) {
  const eligibility = evaluateFullE2eAirtableWriteEligibility({
    kind: "HOTEL_OPPORTUNITY",
    candidate: {
      ...fsRow,
      opportunityId: fsRow.opportunityId || fsRow.id,
      hotelId: hotel.hotelId || fsRow.hotelId,
    },
  });
  const allowedGdo = (eligibility.allowedTables || []).some(
    (t) =>
      text(t?.name || t) === "Group Demand Opportunities" ||
      text(t) === "Group Demand Opportunities",
  );
  const thesis = upper(eligibility.demandThesis || fsRow.demandThesis);
  const speculative =
    thesis === "SPECULATIVE_DEMAND" ||
    thesis === "SPECULATIVE" ||
    eligibility.commercialQualificationStatus === "RESEARCH_ONLY";

  if (eligibility.reasons?.includes("EXCLUDED_OR_REJECTED")) {
    return {
      decision: SYNC_DECISION.REJECTED,
      reason: "EXCLUDED_OR_REJECTED",
      eligibility,
    };
  }
  if (!allowedGdo || speculative) {
    return {
      decision: SYNC_DECISION.DEFERRED,
      reason: speculative
        ? "SPECULATIVE_OR_RESEARCH_ONLY"
        : "COMMERCIAL_ELIGIBILITY_NOT_MET",
      eligibility,
    };
  }
  return {
    decision: SYNC_DECISION.CREATE,
    reason: "FILESYSTEM_ONLY_ELIGIBLE_FOR_GDO",
    eligibility,
  };
}

export function classifySyncDecisionForShared(fsRow = {}, atRow = {}) {
  const conflicts = [];
  if (
    fsRow.organizationName &&
    atRow.organizationName &&
    text(fsRow.organizationName).toLowerCase() !==
      text(atRow.organizationName).toLowerCase()
  ) {
    conflicts.push("ORGANIZATION_NAME_MISMATCH");
  }
  if (conflicts.length) {
    return {
      decision: SYNC_DECISION.CONFLICT,
      reason: "SHARED_IDENTITY_CONFLICT",
      conflicts,
      fieldChanges: [],
    };
  }

  const fieldChanges = [];
  for (const key of ["title", "demandThesis", "status", "priority"]) {
    const before = atRow[key] ?? null;
    const after = fsRow[key] ?? null;
    if (before != null && after != null && String(before) !== String(after)) {
      fieldChanges.push({ field: key, before, after, materialCandidate: key !== "title" });
    }
  }

  if (!fieldChanges.length) {
    return {
      decision: SYNC_DECISION.UNCHANGED,
      reason: "IDENTICAL_CORE_FIELDS",
      conflicts: [],
      fieldChanges: [],
    };
  }

  const material = fieldChanges.filter((c) => c.materialCandidate);
  if (!material.length) {
    return {
      decision: SYNC_DECISION.UNCHANGED,
      reason: "NON_MATERIAL_NARRATIVE_DIFF_ONLY",
      conflicts: [],
      fieldChanges,
    };
  }

  return {
    decision: SYNC_DECISION.UPDATE,
    reason: "MATERIAL_FIELD_DIFF",
    conflicts: [],
    fieldChanges,
  };
}

export function planHotelOpportunitySync({
  hotel,
  filesystem,
  airtableRows = [],
} = {}) {
  const atById = new Map();
  for (const o of airtableRows) {
    const id = text(o.id || o.opportunityId);
    if (!id) continue;
    atById.set(id, {
      id,
      opportunityId: id,
      airtableRecordId: o.airtableRecordId || o._airtableRecordId || null,
      hotelId: o.hotelId || hotel.hotelId,
      organizationName: o.organizationName || o.company || null,
      title: o.title || o.opportunityName || null,
      status: o.status || o.lifecycleStatus || null,
      demandThesis:
        o.demandThesis || o.demandThesisStandard || o.thesisStandard || null,
      priority: o.priority || null,
      firstDiscoveredAt: o.firstDiscoveredAt || null,
      firstDiscoveredRunId: o.firstDiscoveredRunId || null,
      lastMaterialChangeAt: o.lastMaterialChangeAt || null,
      dgGeneratorId: o.dgGeneratorId || null,
    });
  }

  const allIds = new Set([
    ...(filesystem?.byId?.keys?.() || []),
    ...atById.keys(),
  ]);
  const inventoryRows = [];
  const syncOperations = [];
  const counts = {
    SHARED: 0,
    SHARED_IDENTITY_CONFLICT: 0,
    AIRTABLE_ONLY_ACTIVE: 0,
    AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED: 0,
    FILESYSTEM_ONLY: 0,
    INCORRECT_HOTEL_RELATIONSHIP: 0,
    IDENTITY_COLLISION: 0,
    UNKNOWN: 0,
  };
  const decisionCounts = Object.fromEntries(
    Object.values(SYNC_DECISION).map((d) => [d, 0]),
  );

  for (const id of [...allIds].sort()) {
    const fsRow = filesystem?.byId?.get(id) || null;
    const atRow = atById.get(id) || null;
    const { class: cls, conflicts } = classifyInventoryPair(
      fsRow,
      atRow,
      hotel.hotelId,
    );
    counts[cls] = (counts[cls] || 0) + 1;

    let sync = null;
    if (cls === INVENTORY_CLASS.FILESYSTEM_ONLY) {
      sync = classifySyncDecisionForFilesystemOnly(fsRow, hotel);
    } else if (
      cls === INVENTORY_CLASS.SHARED ||
      cls === INVENTORY_CLASS.SHARED_IDENTITY_CONFLICT
    ) {
      sync = classifySyncDecisionForShared(fsRow, atRow);
    } else if (String(cls).startsWith("AIRTABLE_ONLY")) {
      sync = {
        decision: SYNC_DECISION.DEFERRED,
        reason: "AIRTABLE_ONLY_PRESERVE_HISTORY_NO_SILENT_IMPORT",
        eligibility: null,
      };
    } else if (cls === INVENTORY_CLASS.INCORRECT_HOTEL_RELATIONSHIP) {
      sync = {
        decision: SYNC_DECISION.CONFLICT,
        reason: "HOTEL_ID_CONFLICT",
        conflicts,
      };
    } else {
      sync = {
        decision: SYNC_DECISION.DEFERRED,
        reason: "UNKNOWN_CLASS",
      };
    }

    decisionCounts[sync.decision] = (decisionCounts[sync.decision] || 0) + 1;

    const weekly =
      fsRow && atRow
        ? buildDurableWeeklyChangeAnnotation({
            baselineOpportunity: {
              ...atRow,
              firstSeenAt: atRow.firstDiscoveredAt,
              firstSeenRunId: atRow.firstDiscoveredRunId,
            },
            currentOpportunity: fsRow,
            runId: "dry-run-multi-hotel-recon-v2",
          })
        : null;

    inventoryRows.push({
      opportunityId: id,
      class: cls,
      conflicts,
      filesystem: fsRow
        ? {
            id: fsRow.id,
            organizationName: fsRow.organizationName,
            title: fsRow.title,
            status: fsRow.status,
            demandThesis: fsRow.demandThesis,
            priority: fsRow.priority,
            hotelId: fsRow.hotelId,
            dgGeneratorId: fsRow.dgGeneratorId || null,
          }
        : null,
      airtable: atRow
        ? {
            id: atRow.id,
            airtableRecordId: atRow.airtableRecordId,
            organizationName: atRow.organizationName,
            title: atRow.title,
            status: atRow.status,
            demandThesis: atRow.demandThesis,
            hotelId: atRow.hotelId,
          }
        : null,
      syncDecision: sync.decision,
      syncReason: sync.reason,
      eligibilityReasons: sync.eligibility?.reasons || null,
      commercialQualificationStatus:
        sync.eligibility?.commercialQualificationStatus || null,
      fieldChanges: sync.fieldChanges || [],
      weeklyChange: weekly
        ? {
            weeklyDeltaState: weekly.weeklyDeltaState,
            firstSeenRunId: weekly.firstSeenRunId,
            firstSeenAt: weekly.firstSeenAt,
            lastMaterialChangeAt: weekly.lastMaterialChangeAt || null,
          }
        : null,
      sharedIdentity: fsRow ? extractSharedDemandIdentity(fsRow) : null,
      recommendedTreatment:
        sync.decision === SYNC_DECISION.CREATE
          ? "QUALIFIED_CREATE_CANDIDATE_REQUIRES_FOUNDER_AUTH"
          : sync.decision === SYNC_DECISION.UPDATE
            ? "QUALIFIED_UPDATE_CANDIDATE_REQUIRES_FOUNDER_AUTH"
            : sync.decision === SYNC_DECISION.UNCHANGED
              ? "NO_WRITE"
              : sync.decision === SYNC_DECISION.CONFLICT
                ? "MANUAL_REVIEW_NO_AUTO_OVERWRITE"
                : sync.decision === SYNC_DECISION.REJECTED
                  ? "DO_NOT_PERSIST"
                  : "DEFER_PRESERVE_HISTORY_OR_RESEARCH_ONLY",
    });

    if (
      sync.decision === SYNC_DECISION.CREATE ||
      sync.decision === SYNC_DECISION.UPDATE
    ) {
      syncOperations.push({
        hotelId: hotel.hotelId,
        opportunityId: id,
        decision: sync.decision,
        reason: sync.reason,
        airtableRecordId: atRow?.airtableRecordId || null,
        fieldChanges: sync.fieldChanges || [],
        eligibilityReasons: sync.eligibility?.reasons || null,
        preserveFirstDiscovery: true,
        publicationImpact: "NONE",
        authorizeRequired: true,
      });
    }
  }

  return {
    hotel,
    filesystemCount: filesystem?.count || 0,
    airtableCount: atById.size,
    idOverlapCount: counts.SHARED + counts.SHARED_IDENTITY_CONFLICT,
    filesystemOnlyCount: counts.FILESYSTEM_ONLY,
    airtableOnlyCount:
      counts.AIRTABLE_ONLY_ACTIVE +
      counts.AIRTABLE_ONLY_HISTORICAL_OR_UNSPECIFIED,
    counts,
    decisionCounts,
    estimatedAirtableOperations: syncOperations.length,
    syncOperations,
    inventoryRows,
    idempotentRerunExpectation: {
      afterAuthorizedApply:
        "CREATE/UPDATE rows become UNCHANGED; no duplicate opportunityIds",
      withoutApply: "Identical dry-run decisions",
    },
  };
}

export function buildMultiHotelDryRunManifest({
  hotelPlans = [],
  rockCreekAnalysis = null,
  generatedAt = new Date().toISOString(),
} = {}) {
  const totals = {
    hotels: hotelPlans.length,
    filesystemOpportunities: 0,
    airtableGdos: 0,
    createCandidates: 0,
    updateCandidates: 0,
    unchanged: 0,
    deferred: 0,
    conflicts: 0,
    rejected: 0,
  };
  for (const plan of hotelPlans) {
    totals.filesystemOpportunities += plan.filesystemCount || 0;
    totals.airtableGdos += plan.airtableCount || 0;
    totals.createCandidates += plan.decisionCounts?.[SYNC_DECISION.CREATE] || 0;
    totals.updateCandidates += plan.decisionCounts?.[SYNC_DECISION.UPDATE] || 0;
    totals.unchanged += plan.decisionCounts?.[SYNC_DECISION.UNCHANGED] || 0;
    totals.deferred += plan.decisionCounts?.[SYNC_DECISION.DEFERRED] || 0;
    totals.conflicts += plan.decisionCounts?.[SYNC_DECISION.CONFLICT] || 0;
    totals.rejected += plan.decisionCounts?.[SYNC_DECISION.REJECTED] || 0;
  }

  return {
    version: GDI_MULTI_HOTEL_AIRTABLE_RECONCILIATION_V2,
    generatedAt,
    mutation: false,
    productionWritesAuthorized: false,
    writeGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
    storageAuthority: STORAGE_AUTHORITY_V2,
    weeklyChangeContract: WEEKLY_CHANGE_DURABILITY_CONTRACT.version,
    rockCreekVenueIdentity: rockCreekAnalysis,
    totals,
    hotels: hotelPlans.map((p) => ({
      key: p.hotel.key,
      name: p.hotel.name,
      hotelId: p.hotel.hotelId,
      cohort: p.hotel.cohort,
      filesystemCount: p.filesystemCount,
      airtableCount: p.airtableCount,
      idOverlapCount: p.idOverlapCount,
      filesystemOnlyCount: p.filesystemOnlyCount,
      airtableOnlyCount: p.airtableOnlyCount,
      counts: p.counts,
      decisionCounts: p.decisionCounts,
      estimatedAirtableOperations: p.estimatedAirtableOperations,
      createCandidateIds: p.syncOperations
        .filter((o) => o.decision === SYNC_DECISION.CREATE)
        .map((o) => o.opportunityId),
      updateCandidateIds: p.syncOperations
        .filter((o) => o.decision === SYNC_DECISION.UPDATE)
        .map((o) => o.opportunityId),
      conflictIds: p.inventoryRows
        .filter((r) => r.syncDecision === SYNC_DECISION.CONFLICT)
        .map((r) => r.opportunityId),
    })),
    nextProductionPilotRequires: [
      "FOUNDER_EXPLICIT_ALLOWLIST",
      "ROCK_CREEK_STRUCTURED_ID_REMEDIATION_DECISION",
      "PER_HOTEL_QUALIFIED_CREATE_UPDATE_SET",
      "authorizeProductionWrites=true",
      "exactWriteSetAuthorization=<validated manifest>",
      "apply=true",
    ],
  };
}

export function assertNoUnauthorizedProductionWrite(opts = {}) {
  if (opts.apply === true || opts.authorizeProductionWrites === true) {
    const err = new Error(
      "gdi_multi_hotel_reconciliation_v2_production_writes_not_authorized",
    );
    err.code = "PRODUCTION_WRITES_NOT_AUTHORIZED";
    throw err;
  }
  return { ok: true, mutation: false };
}

export function certifyWeeklyChangeStabilityAcrossIdenticalReruns(planA, planB) {
  const a = JSON.stringify(
    (planA.inventoryRows || []).map((r) => ({
      id: r.opportunityId,
      decision: r.syncDecision,
      weekly: r.weeklyChange?.weeklyDeltaState || null,
      firstSeen: r.weeklyChange?.firstSeenRunId || null,
    })),
  );
  const b = JSON.stringify(
    (planB.inventoryRows || []).map((r) => ({
      id: r.opportunityId,
      decision: r.syncDecision,
      weekly: r.weeklyChange?.weeklyDeltaState || null,
      firstSeen: r.weeklyChange?.firstSeenRunId || null,
    })),
  );
  return { ok: a === b, identical: a === b };
}

export function sampleCrossHotelSharedIdentitySafety(oppsByHotel = {}) {
  const hotels = Object.keys(oppsByHotel);
  if (hotels.length < 2) {
    return { ok: true, checked: 0, note: "NEED_TWO_HOTELS" };
  }
  const [h1, h2] = hotels;
  const a = (oppsByHotel[h1] || []).find((o) => o.dgGeneratorId) || oppsByHotel[h1]?.[0];
  const b = (oppsByHotel[h2] || []).find(
    (o) => a?.dgGeneratorId && o.dgGeneratorId === a.dgGeneratorId,
  );
  if (!a || !b) {
    return {
      ok: true,
      checked: 0,
      note: "NO_SHARED_DG_IN_SAMPLE",
      isolationContractPresent: true,
    };
  }
  return assertCrossHotelSharedIdentityIsolation({
    hotelAOpportunity: { ...a, hotelId: h1 },
    hotelBOpportunity: { ...b, hotelId: h2 },
  });
}
