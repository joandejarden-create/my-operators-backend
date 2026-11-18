#!/usr/bin/env node
/**
 * D2 Option B — Exact Bethesda proposed write manifest (read-only).
 * No Airtable mutations. Production writes remain unauthorized.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadBethesdaLiveExaProcessedCandidates,
  BETHESDA_LIVE_EXA_RUN,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-exa-airtable-reconciliation-v1.js";
import { dryRunFullE2eAirtablePersistence } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-dry-run-v1.js";
import {
  evaluateFullE2eAirtableWriteEligibility,
  resolveCanonicalPersistenceIdentities,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-eligibility-v1.js";
import { findOpportunityRecordById } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import {
  findDemandGeneratorById,
  findDemandGeneratorByOrganizationName,
} from "../lib/group-demand-intelligence/demand-generators/airtable-stores.js";
import { upsertVenue } from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";
import { WRITE_OUTCOME } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-full-e2e-airtable-persistence-adapter-v1",
);

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

function pickEvidence(candidate = {}) {
  return {
    opportunityId: candidate.opportunityId || null,
    organizationName: candidate.organizationName || null,
    segmentId: candidate.segmentId || null,
    disposition: candidate.disposition || null,
    evaluationLabel: candidate.evaluationLabel || null,
    salesReadyContacts: candidate.salesReadyContacts ?? null,
    contactsInvestigated: candidate.contactsInvestigated ?? null,
    demandThesis:
      candidate.demandThesis ||
      candidate.demandThesisStandard ||
      candidate.thesisStandard ||
      null,
    sourceUrl: candidate.sourceUrl || candidate.primarySourceUrl || null,
    sourceUrls: candidate.sourceUrls || null,
  };
}

async function collideGdo(opportunityId) {
  const rec = await findOpportunityRecordById(opportunityId);
  if (!rec) {
    return {
      collision: "NONE",
      airtableRecordId: null,
      proposedOutcome: WRITE_OUTCOME.CREATED,
    };
  }
  return {
    collision: "EXISTING_OPPORTUNITY_ID",
    airtableRecordId: rec.id,
    existingHotelId: rec.fields?.hotelId || null,
    existingOrg: rec.fields?.organizationName || null,
    proposedOutcome: WRITE_OUTCOME.UNCHANGED,
  };
}

async function collideDg(organizationName, demandGeneratorId) {
  const byId = await findDemandGeneratorById(demandGeneratorId);
  if (byId) {
    return {
      collision: "EXISTING_DEMAND_GENERATOR_ID",
      airtableRecordId: byId.id,
      existingId: byId.fields?.["Demand Generator ID"] || null,
      existingOrg: byId.fields?.["Organization Name"] || null,
      proposedOutcome: WRITE_OUTCOME.UPDATED,
    };
  }
  const byName = await findDemandGeneratorByOrganizationName(organizationName);
  if (byName) {
    return {
      collision: "NAME_MATCH_DIFFERENT_OR_SAME_ID",
      airtableRecordId: byName.id,
      existingId: byName.fields?.["Demand Generator ID"] || null,
      existingOrg: byName.fields?.["Organization Name"] || null,
      proposedOutcome:
        byName.fields?.["Demand Generator ID"] === demandGeneratorId
          ? WRITE_OUTCOME.UPDATED
          : WRITE_OUTCOME.DUPLICATE_LINKED,
    };
  }
  return {
    collision: "NONE",
    airtableRecordId: null,
    proposedOutcome: WRITE_OUTCOME.CREATED,
  };
}

async function collideVenue(candidate, identities) {
  const preview = await upsertVenue(
    {
      venueName: candidate.organizationName,
      city: "Bethesda",
      venueId: identities.venueId,
      website: candidate.sourceUrl || candidate.website || null,
    },
    { dryRun: true },
  );
  if (preview?.reused || preview?.action === "update") {
    return {
      collision: "EXISTING_VENUE_MATCH",
      airtableRecordId: preview.recordId || null,
      matchedBy: preview.matchedBy || null,
      priorVenueId: preview.priorVenueId || null,
      venueId: preview.venueId || identities.venueId,
      proposedOutcome: WRITE_OUTCOME.UPDATED,
      fieldDiffPreview: {
        beforeVenueId: preview.priorVenueId || null,
        afterVenueId: preview.venueId || identities.venueId,
        note: "Dry-run venue match; no mutation performed",
      },
    };
  }
  return {
    collision: "NONE",
    airtableRecordId: null,
    venueId: identities.venueId,
    proposedOutcome: WRITE_OUTCOME.CREATED,
    fieldDiffPreview: null,
  };
}

export async function buildBethesdaExactWriteManifest(opts = {}) {
  const loaded = loadBethesdaLiveExaProcessedCandidates(opts);
  const dry = dryRunFullE2eAirtablePersistence({
    hotelId: loaded.hotelId,
    displayName: BETHESDA_LIVE_EXA_RUN.hotelName,
    city: "Bethesda",
    runId: loaded.runId,
    candidates: loaded.processed,
  });

  const structuralOps = [];
  for (const plan of dry.plans) {
    if (!plan.plannedWrites?.length) continue;
    const candidate =
      loaded.processed.find(
        (p) =>
          p.organizationName === plan.organizationName ||
          p.opportunityId === plan.plannedWrites.find((w) => w.tableName === "Group Demand Opportunities")?.identityKey,
      ) || {};
    const identities = resolveCanonicalPersistenceIdentities(candidate, {
      city: "Bethesda",
    });
    const eligibility = evaluateFullE2eAirtableWriteEligibility({
      candidate,
      kind: plan.kind,
    });

    for (const write of plan.plannedWrites) {
      let collision = null;
      let canonicalId = write.identityKey;
      if (write.tableName === "Demand Generators") {
        canonicalId = identities.demandGeneratorId;
        collision = await collideDg(plan.organizationName, canonicalId);
      } else if (write.tableName === "Group Demand Opportunities") {
        canonicalId = identities.opportunityId;
        collision = await collideGdo(canonicalId);
      } else if (write.tableName === "Private Event Venues") {
        canonicalId = identities.venueId;
        collision = await collideVenue(candidate, identities);
      }

      const eligibleForTable = eligibility.allowedTables.includes(write.tableName);
      const proposedOutcome = !eligibleForTable
        ? WRITE_OUTCOME.DEFERRED
        : collision?.proposedOutcome || WRITE_OUTCOME.DEFERRED;

      structuralOps.push({
        organizationName: plan.organizationName,
        kind: plan.kind,
        destinationTable: write.tableName,
        softPlannerIdentityKey: write.identityKey,
        canonicalId,
        airtableRecordId: collision?.airtableRecordId || null,
        proposedOutcome,
        eligibility: {
          eligible: eligibleForTable,
          reasons: eligibility.reasons,
          commercialQualificationStatus: eligibility.commercialQualificationStatus,
          demandThesis: eligibility.demandThesis,
        },
        hotelRelationship: {
          hotelId: BETHESDA_LIVE_EXA_RUN.hotelId,
          hotelName: BETHESDA_LIVE_EXA_RUN.hotelName,
          hotelQualified: candidate.hotelQualified === true,
        },
        sourceEvidence: pickEvidence(candidate),
        collision,
        beforeAfterFieldDifferences:
          collision?.fieldDiffPreview ||
          (proposedOutcome === WRITE_OUTCOME.CREATED
            ? { before: null, after: write.fields, note: "New record preview from dry-run planner fields" }
            : proposedOutcome === WRITE_OUTCOME.DEFERRED
              ? {
                  before: null,
                  after: null,
                  note: "Deferred by D1 commercial eligibility — no write planned",
                }
              : {
                  before: "existing Airtable record",
                  after: write.fields,
                  note: "Material field diff requires apply-time compare; dry-run only",
                }),
        plannedFieldsPreview: write.fields,
      });
    }
  }

  const eligibleWriteCount = structuralOps.filter(
    (o) =>
      o.eligibility.eligible &&
      (o.proposedOutcome === WRITE_OUTCOME.CREATED ||
        o.proposedOutcome === WRITE_OUTCOME.UPDATED),
  ).length;
  const deferredCount = structuralOps.filter(
    (o) => o.proposedOutcome === WRITE_OUTCOME.DEFERRED,
  ).length;

  return {
    version: "gdi-bethesda-exact-write-manifest-v1",
    decision: "D2_OPTION_B",
    mutation: false,
    productionWritesAuthorized: false,
    run: BETHESDA_LIVE_EXA_RUN,
    structuralProposedOperationCount: structuralOps.length,
    eligibleWriteCount,
    deferredCount,
    unchangedOrDuplicateCount: structuralOps.filter((o) =>
      [WRITE_OUTCOME.UNCHANGED, WRITE_OUTCOME.DUPLICATE_LINKED].includes(
        o.proposedOutcome,
      ),
    ).length,
    operations: structuralOps,
    notes: [
      "Structural 7 ops come from processed-20 dry-run routing (3 DG + 3 GDO + 1 PE venue).",
      "D1 eligibility defers GDO writes that lack CONFIRMED_DEMAND / RELIABLE_INFERRED_DEMAND + credible commercial entry point.",
      "Canonical DG/Venue IDs use hashed compute* helpers (not soft dg_name keys).",
      "No Airtable writes executed.",
    ],
  };
}

async function main() {
  ensureOut();
  const manifest = await buildBethesdaExactWriteManifest();
  const outPath = path.join(OUT, "BETHESDA_EXACT_WRITE_MANIFEST.json");
  fs.writeFileSync(outPath, JSON.stringify(manifest, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        structural: manifest.structuralProposedOperationCount,
        eligibleWriteCount: manifest.eligibleWriteCount,
        deferredCount: manifest.deferredCount,
        outcomes: Object.fromEntries(
          manifest.operations.map((o) => [
            `${o.destinationTable}:${o.canonicalId}`,
            o.proposedOutcome,
          ]),
        ),
      },
      null,
      2,
    ),
  );
}

const isDirectRun = process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isDirectRun) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
