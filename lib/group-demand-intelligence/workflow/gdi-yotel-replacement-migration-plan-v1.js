/**
 * YOTEL GDO replacement migration planner V1 (founder-amended PR #368).
 * Preparation only — no Airtable deletions or writes.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import {
  evaluateFullE2eAirtableWriteEligibility,
} from "./gdi-full-e2e-airtable-persistence-eligibility-v1.js";
import {
  SYNC_DECISION,
  ROCK_CREEK_VENUE_IDENTITY,
} from "./gdi-multi-hotel-airtable-reconciliation-v2.js";
import { AIRTABLE_PRODUCTION_WRITE_GUARD } from "./gdi-full-e2e-airtable-persistence-contract-v1.js";

export const GDI_YOTEL_REPLACEMENT_MIGRATION_PLAN_V1 =
  "gdi-yotel-replacement-migration-plan-v1";

export const YOTEL_HOTEL = Object.freeze({
  key: "yotel",
  name: "YOTEL Geneva Lake",
  hotelId: "recrPQcZg7SFARRb2",
  strategy: "REPLACE_AIRTABLE_GDO_WITH_LOCAL_BASELINE",
});

export const OTHER_HOTEL_STRATEGIES = Object.freeze({
  radisson: {
    hotelId: "recH9h3McgMNFQEla",
    name: "Radisson Blu Toronto Downtown",
    strategy: "RECONCILE_NON_DESTRUCTIVE",
  },
  guadalajara: {
    hotelId: "recCAGqkERSXgiku4",
    name: "Sheraton Guadalajara Expo",
    strategy: "RECONCILE_NON_DESTRUCTIVE",
  },
  wyndham: {
    hotelId: "recnPB3Q02NO8Ma1o",
    name: "Wyndham San Diego Bayside",
    strategy: "PRESERVE_UNCHANGED",
  },
  castillo: {
    hotelId: "rec82D9zpB8fede1I",
    name: "Castillo Hotel Son Vida",
    strategy: "PRESERVE_UNCHANGED",
  },
});

const PII_FIELD_RE =
  /email|phone|mobile|contact.*mail|contact.*phone/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function assertYotelHotelScope(hotelId) {
  if (text(hotelId) !== YOTEL_HOTEL.hotelId) {
    const err = new Error("yotel_replacement_hotel_scope_violation");
    err.code = "YOTEL_HOTEL_SCOPE_VIOLATION";
    err.expected = YOTEL_HOTEL.hotelId;
    err.actual = hotelId;
    throw err;
  }
  return true;
}

export function redactPiiFields(fields = {}) {
  const out = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (PII_FIELD_RE.test(k)) {
      out[k] = v == null || v === "" ? v : "[REDACTED_PII]";
      continue;
    }
    if (typeof v === "string" && /@/.test(v) && /\./.test(v)) {
      out[k] = "[REDACTED_PII_EMAIL_LIKE]";
      continue;
    }
    out[k] = v;
  }
  return out;
}

export function fingerprintRecord(rec = {}) {
  const fields = redactPiiFields(rec.fields || {});
  const body = JSON.stringify({
    id: rec.id || null,
    opportunityId: fields.opportunityId || fields.OpportunityId || null,
    hotelId: fields.hotelId || null,
    keys: Object.keys(fields).sort(),
    fields,
  });
  return createHash("sha256").update(body).digest("hex");
}

/**
 * Verify every candidate delete record is YOTEL-scoped.
 */
export function buildYotelDeleteManifest({
  airtableRawRecords = [],
  generatedAt = new Date().toISOString(),
  priorReconciliationCount = 108,
} = {}) {
  const records = [];
  const rejected = [];
  for (const rec of airtableRawRecords) {
    const fields = rec.fields || {};
    const hotelId = text(fields.hotelId);
    const opportunityId = text(fields.opportunityId);
    if (hotelId !== YOTEL_HOTEL.hotelId) {
      rejected.push({
        airtableRecordId: rec.id,
        opportunityId: opportunityId || null,
        hotelId: hotelId || null,
        reason: "HOTEL_ID_MISMATCH_NOT_YOTEL",
      });
      continue;
    }
    records.push({
      airtableRecordId: rec.id,
      opportunityId: opportunityId || null,
      organizationName: fields.organizationName || null,
      opportunityName: fields.opportunityName || null,
      decisionId: fields.decisionId || null,
      actionStatus: fields.actionStatus || null,
      dgGeneratorId: fields.dgGeneratorId || null,
      dgProgramId: fields.dgProgramId || null,
      dgSignalId: fields.dgSignalId || null,
      peVenueId: fields.peVenueId || null,
      linkedDemandGenerator: fields["Demand Generator"] || null,
      linkedDemandProgram: fields["Demand Program"] || null,
      linkedSignals: fields["Demand Generator Signals"] || fields["Demand Generator Signal"] || null,
      createdAt: fields.createdAt || null,
      updatedAt: fields.updatedAt || null,
      fingerprint: fingerprintRecord(rec),
      proposedAction: "DELETE_GDO_ONLY",
    });
  }

  const airtableIds = records.map((r) => r.airtableRecordId).sort();
  const opportunityIds = records
    .map((r) => r.opportunityId)
    .filter(Boolean)
    .sort();

  return {
    version: GDI_YOTEL_REPLACEMENT_MIGRATION_PLAN_V1,
    generatedAt,
    mutation: false,
    productionDeletionAuthorized: false,
    hotel: YOTEL_HOTEL,
    scoping: {
      filter: `hotelId='${YOTEL_HOTEL.hotelId}'`,
      allowlistHotelIds: [YOTEL_HOTEL.hotelId],
      forbidCrossHotelDelete: true,
      forbidDeleteSharedIntelligence: true,
    },
    counts: {
      proposedDeletes: records.length,
      rejectedOutOfScope: rejected.length,
      priorReconciliationCount,
      countMatchesPrior: records.length === priorReconciliationCount,
    },
    airtableRecordIdsExact: airtableIds,
    opportunityIdsExact: opportunityIds,
    records,
    rejected,
    neverDelete: [
      "Demand Generators",
      "Demand Programs",
      "Demand Generator Signals",
      "Private Event Venues",
      "Private Event Signals",
      "Hotel Venue Fit",
      "Hotel Demand Generator Fit",
      "other hotels' Group Demand Opportunities",
      "STAGED/CURRENT publication packages",
      "filesystem research stores",
    ],
  };
}

export function classifyLocalOpportunityForReplacement(opp = {}) {
  const eligibility = evaluateFullE2eAirtableWriteEligibility({
    kind: "HOTEL_OPPORTUNITY",
    candidate: {
      ...opp,
      opportunityId: opp.opportunityId || opp.id,
      hotelId: YOTEL_HOTEL.hotelId,
    },
  });
  const allowedGdo = (eligibility.allowedTables || []).includes(
    "Group Demand Opportunities",
  );
  if (allowedGdo) {
    return {
      destination: "QUALIFIED_GDO_CREATE_CANDIDATE",
      decision: SYNC_DECISION.CREATE,
      reason: "GDO_COMMERCIAL_ELIGIBILITY_MET",
      eligibility,
    };
  }
  return {
    destination: "RESEARCH_STORE_ONLY",
    decision: SYNC_DECISION.DEFERRED,
    reason:
      eligibility.commercialQualificationStatus === "RESEARCH_ONLY" ||
      eligibility.reasons?.includes("GDO_DEFERRED_WATCHLIST_BELOW_ELIGIBILITY")
        ? "RESEARCH_ONLY_NOT_QUALIFIED_GDO"
        : "COMMERCIAL_ELIGIBILITY_NOT_MET",
    eligibility,
  };
}

export function buildYotel42TargetState({
  localOpportunities = [],
  generatedAt = new Date().toISOString(),
} = {}) {
  const rows = [];
  for (const raw of localOpportunities) {
    const id = text(raw.id || raw.opportunityId);
    if (!id) continue;
    const rawHotelId = text(raw.hotelId) || YOTEL_HOTEL.hotelId;
    const hotelMismatch =
      rawHotelId && rawHotelId !== YOTEL_HOTEL.hotelId ? rawHotelId : null;
    const classification = classifyLocalOpportunityForReplacement({
      ...raw,
      id,
      opportunityId: id,
      hotelId: YOTEL_HOTEL.hotelId,
    });
    rows.push({
      opportunityId: id,
      organizationName: raw.organizationName || raw.company || null,
      title: raw.title || raw.opportunityName || null,
      demandThesis:
        raw.demandThesis || raw.demandThesisStandard || raw.thesisStandard || null,
      priority: raw.priority || null,
      status: raw.status || raw.lifecycleStatus || null,
      hotelId: YOTEL_HOTEL.hotelId,
      hotelIdMismatchOnLocalRow: hotelMismatch,
      dgGeneratorId: raw.dgGeneratorId || null,
      peVenueId: raw.peVenueId || null,
      firstDiscoveredAt: raw.firstDiscoveredAt || raw.firstSeenAt || null,
      firstDiscoveredRunId: raw.firstDiscoveredRunId || raw.firstSeenRunId || null,
      destination: classification.destination,
      syncDecision: classification.decision,
      reason: classification.reason,
      eligibilityReasons: classification.eligibility?.reasons || [],
      commercialQualificationStatus:
        classification.eligibility?.commercialQualificationStatus || null,
      // no contact PII in target-state manifest
    });
  }

  const qualified = rows.filter(
    (r) => r.destination === "QUALIFIED_GDO_CREATE_CANDIDATE",
  );
  const researchOnly = rows.filter(
    (r) => r.destination === "RESEARCH_STORE_ONLY",
  );

  return {
    version: GDI_YOTEL_REPLACEMENT_MIGRATION_PLAN_V1,
    generatedAt,
    mutation: false,
    hotel: YOTEL_HOTEL,
    authoritativeBaseline: "filesystem.data.group-demand-intelligence.hotels.recrPQcZg7SFARRb2.opportunities.json",
    expectedLocalCount: 42,
    actualLocalCount: rows.length,
    countMatchesExpected: rows.length === 42,
    opportunityIdsExact: rows.map((r) => r.opportunityId).sort(),
    qualifiedGdoCount: qualified.length,
    researchOnlyCount: researchOnly.length,
    qualifiedOpportunityIds: qualified.map((r) => r.opportunityId).sort(),
    researchOnlyOpportunityIds: researchOnly.map((r) => r.opportunityId).sort(),
    rows,
    notes: [
      "Local 42-record inventory is the research baseline authority.",
      "Only commercially qualified rows are eligible for GDO create after separate founder authorization.",
      "Research-only rows remain on the filesystem research store and must not be forced into GDO.",
    ],
  };
}

export function assessYotelDependencies({
  deleteManifest,
  decisionsBySubjectId = [],
  decisionEventsBySubjectId = [],
  filesystemFeedback = null,
  publication = null,
} = {}) {
  const deleteOppIds = new Set(deleteManifest?.opportunityIdsExact || []);
  const deleteAirtableIds = new Set(deleteManifest?.airtableRecordIdsExact || []);

  const linkedDecisions = [];
  for (const d of decisionsBySubjectId) {
    const subjectId = text(d.subjectId || d.opportunityId);
    if (deleteOppIds.has(subjectId) || deleteAirtableIds.has(text(d.airtableRecordId))) {
      linkedDecisions.push({
        decisionId: d.decisionId || d.id || null,
        subjectId,
        decisionType: d.decisionType || null,
        actionStatus: d.actionStatus || d.status || null,
        preserveSeparately: true,
      });
    }
  }

  const gdosWithDecisionId = (deleteManifest?.records || []).filter((r) =>
    text(r.decisionId),
  );

  const sharedLinks = {
    demandGeneratorLinks: (deleteManifest?.records || []).filter(
      (r) =>
        (Array.isArray(r.linkedDemandGenerator) && r.linkedDemandGenerator.length) ||
        r.dgGeneratorId,
    ).length,
    programLinks: (deleteManifest?.records || []).filter(
      (r) =>
        (Array.isArray(r.linkedDemandProgram) && r.linkedDemandProgram.length) ||
        r.dgProgramId,
    ).length,
    signalLinks: (deleteManifest?.records || []).filter(
      (r) =>
        (Array.isArray(r.linkedSignals) && r.linkedSignals.length) ||
        r.dgSignalId,
    ).length,
    peVenueLinks: (deleteManifest?.records || []).filter((r) => r.peVenueId)
      .length,
  };

  return {
    version: GDI_YOTEL_REPLACEMENT_MIGRATION_PLAN_V1,
    mutation: false,
    hotel: YOTEL_HOTEL,
    gdoRowsWithDecisionIdField: gdosWithDecisionId.map((r) => ({
      airtableRecordId: r.airtableRecordId,
      opportunityId: r.opportunityId,
      decisionId: r.decisionId,
      actionStatus: r.actionStatus,
    })),
    linkedDecisionsFound: linkedDecisions,
    decisionEventsSampleCount: decisionEventsBySubjectId.length,
    filesystemFeedbackPresent: Boolean(filesystemFeedback),
    publication: {
      stagedPresent: Boolean(publication?.staged),
      currentPresent: Boolean(publication?.current),
      mustRemainUnchanged: true,
    },
    sharedIntelligence: {
      ...sharedLinks,
      deletionPolicy:
        "UNLINK_ONLY_VIA_GDO_ROW_DELETE — never delete shared DG/Program/Signal/Venue records",
    },
    blockers: [],
    recoveryRequirements: [
      "Restore GDO rows from backup JSON if replacement fails",
      "Preserve Decision/Decision Event rows independently of GDO deletion",
      "Filesystem opportunities.json remains authoritative research baseline",
      "STAGED/CURRENT packages untouched",
    ],
  };
}

export function designYotelReplacementSequence({
  deleteManifest,
  targetState,
} = {}) {
  return {
    version: GDI_YOTEL_REPLACEMENT_MIGRATION_PLAN_V1,
    mutation: false,
    productionAuthorized: false,
    atomicMultiRecordTransactions: false,
    note: "Airtable does not offer atomic multi-record transactions; use staged recoverable sequence.",
    hotelAllowlist: [YOTEL_HOTEL.hotelId],
    phases: [
      {
        phase: 1,
        name: "FREEZE_AND_BACKUP",
        actions: [
          "Re-query YOTEL GDOs by exact hotelId",
          "Write full recoverable backup + fingerprints",
          "Snapshot local opportunities.json",
          "Audit Decisions/feedback/publication pointers",
        ],
        writes: false,
      },
      {
        phase: 2,
        name: "FOUNDER_AUTHORIZATION",
        actions: [
          "Review exact delete ID list",
          "Review qualified GDO replacement set",
          "Authorize with exactWriteSetAuthorization token bound to manifests",
        ],
        writes: false,
      },
      {
        phase: 3,
        name: "CREATE_QUALIFIED_REPLACEMENTS_FIRST",
        actions: [
          `Create only ${targetState?.qualifiedGdoCount ?? 0} qualified GDO rows from local baseline`,
          "Read-back verify each create",
          "Do not create research-only rows into GDO",
        ],
        writes: "AUTHORIZED_CREATES_ONLY",
        risk: "Temporary coexistence of old+new inventories until phase 4",
      },
      {
        phase: 4,
        name: "DELETE_OBSOLETE_108",
        actions: [
          "Delete only airtableRecordIdsExact from delete manifest",
          "Re-verify hotelId on each record immediately before delete",
          "Never delete shared DG/Program/Signal/Venue",
          "Skip any ID not in allowlist",
        ],
        writes: "AUTHORIZED_DELETES_ONLY",
      },
      {
        phase: 5,
        name: "POST_MIGRATION_RECONCILE",
        actions: [
          "Re-list YOTEL GDOs",
          "Expect GDO count == qualified creates (not necessarily 42)",
          "Confirm local 42 still present on filesystem",
          "Confirm publication packages unchanged",
          "Idempotent replan: no further deletes/creates",
        ],
        writes: false,
      },
    ],
    counts: {
      proposedDeletes: deleteManifest?.counts?.proposedDeletes ?? null,
      qualifiedCreates: targetState?.qualifiedGdoCount ?? null,
      researchOnlyRemainOnFilesystem: targetState?.researchOnlyCount ?? null,
    },
    writeGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
    rockCreekExcluded: {
      note: "Rock Creek structured Venue ID correction is separate authorization",
      canonicalId: ROCK_CREEK_VENUE_IDENTITY.authoritativeCanonicalId,
      applyWithYotelMigration: false,
    },
  };
}

export function assertNoMigrationMutation(opts = {}) {
  if (
    opts.apply === true ||
    opts.delete === true ||
    opts.authorizeProductionWrites === true
  ) {
    const err = new Error(
      "yotel_replacement_production_mutations_not_authorized_in_preparation_phase",
    );
    err.code = "PRODUCTION_MUTATIONS_NOT_AUTHORIZED";
    throw err;
  }
  return { ok: true, mutation: false };
}

export function loadLocalYotelOpportunities(repoRoot) {
  const p = path.join(
    repoRoot,
    "data/group-demand-intelligence/hotels",
    YOTEL_HOTEL.hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) {
    return { path: p, present: false, opportunities: [] };
  }
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const opportunities = Array.isArray(j) ? j : j.opportunities || [];
  return { path: p, present: true, opportunities, hotelId: j.hotelId || null };
}

export function verifyNoCrossHotelIds(deleteManifest, foreignHotelIds = []) {
  const bad = [];
  for (const rec of deleteManifest.records || []) {
    // hotel already validated; also ensure opportunityId not claiming foreign hotel prefix wrongly is soft
    for (const foreign of foreignHotelIds) {
      if (text(rec.hotelId) === text(foreign)) {
        bad.push(rec.airtableRecordId);
      }
    }
  }
  return { ok: bad.length === 0, badAirtableIds: bad };
}
