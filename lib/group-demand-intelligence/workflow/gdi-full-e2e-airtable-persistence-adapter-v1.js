/**
 * Canonical Full E2E Airtable persistence adapter V1.
 * Reuses existing DG / GDO / PE / research-coverage adapters.
 * Production writes fail-closed until founder authorizes an exact write set.
 */

import {
  AIRTABLE_PRODUCTION_WRITE_GUARD,
  GDI_AIRTABLE_TABLES,
  GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1,
  WRITE_OUTCOME,
} from "./gdi-full-e2e-airtable-persistence-contract-v1.js";
import {
  evaluateFullE2eAirtableWriteEligibility,
  isTableDeniedForFullE2eDiscovery,
  resolveCanonicalPersistenceIdentities,
} from "./gdi-full-e2e-airtable-persistence-eligibility-v1.js";
import { dryRunFullE2eAirtablePersistence } from "./gdi-full-e2e-airtable-persistence-dry-run-v1.js";
import { upsertDemandGenerator } from "../demand-generators/airtable-stores.js";
import { upsertVenue } from "../private-events/airtable-venue-store.js";
import { syncHotelOpportunitiesToAirtable } from "./gdi-airtable-sync-v1.js";

export const GDI_FULL_E2E_AIRTABLE_PERSISTENCE_ADAPTER_V1 =
  "gdi-full-e2e-airtable-persistence-adapter-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Plan + certify a persistence batch without mutating Airtable.
 * apply=true is refused unless authorizeProductionWrites + exactWriteSetAuthorization.
 */
export async function executeFullE2eAirtablePersistence(input = {}) {
  const authorizeProductionWrites = input.authorizeProductionWrites === true;
  const exactWriteSetAuthorization = text(input.exactWriteSetAuthorization);
  const apply = input.apply === true || input.liveWrite === true;

  if (apply) {
    if (!authorizeProductionWrites || !exactWriteSetAuthorization) {
      return {
        ok: false,
        version: GDI_FULL_E2E_AIRTABLE_PERSISTENCE_ADAPTER_V1,
        ...AIRTABLE_PRODUCTION_WRITE_GUARD,
        detail:
          "Production apply requires authorizeProductionWrites=true AND exactWriteSetAuthorization token/manifest id from founder pilot approval.",
      };
    }
    // Still refuse in this phase — capability is proven via dry-run/read-back only.
    return {
      ok: false,
      version: GDI_FULL_E2E_AIRTABLE_PERSISTENCE_ADAPTER_V1,
      code: "AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED",
      blocked: true,
      detail:
        "Adapter implemented and dry-run certified; first production write requires a separate founder authorization of the exact write set.",
      exactWriteSetAuthorization,
    };
  }

  const hotelId = text(input.hotelId);
  const candidates = Array.isArray(input.candidates) ? input.candidates : [];
  const dry = dryRunFullE2eAirtablePersistence({
    hotelId,
    city: input.city,
    displayName: input.displayName || input.hotelName,
    runId: input.runId,
    candidates,
  });

  const operations = [];
  const retries = [];
  for (const plan of dry.plans || []) {
    const candidate =
      candidates.find((c) => c.organizationName === plan.organizationName) ||
      {};
    const eligibility = evaluateFullE2eAirtableWriteEligibility({
      candidate,
      kind: plan.kind,
      includeResearchRunSummary: input.includeResearchRunSummary === true,
    });
    const identities = resolveCanonicalPersistenceIdentities(candidate, {
      city: input.city,
    });

    for (const write of plan.plannedWrites || []) {
      if (isTableDeniedForFullE2eDiscovery(write.tableName)) {
        operations.push({
          tableName: write.tableName,
          outcome: WRITE_OUTCOME.DEFERRED,
          reason: "DENIED_TABLE_FOR_FULL_E2E_DISCOVERY",
          identityKey: write.identityKey,
        });
        continue;
      }
      if (!eligibility.allowedTables.includes(write.tableName)) {
        operations.push({
          tableName: write.tableName,
          outcome: WRITE_OUTCOME.DEFERRED,
          reason: "ELIGIBILITY_GATE",
          eligibilityReasons: eligibility.reasons,
          commercialQualificationStatus:
            eligibility.commercialQualificationStatus,
          identityKey: write.identityKey,
          canonicalId:
            write.tableName === GDI_AIRTABLE_TABLES.DEMAND_GENERATORS.name
              ? identities.demandGeneratorId
              : write.tableName ===
                  GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name
                ? identities.opportunityId
                : write.tableName === GDI_AIRTABLE_TABLES.PRIVATE_EVENT_VENUES.name
                  ? identities.venueId
                  : write.identityKey,
        });
        continue;
      }

      // Dry-run adapter previews against existing upserts (no mutation).
      let preview = null;
      try {
        if (write.tableName === GDI_AIRTABLE_TABLES.DEMAND_GENERATORS.name) {
          preview = await upsertDemandGenerator(
            {
              organizationName: plan.organizationName,
              demandGeneratorId: identities.demandGeneratorId,
              researchReason: candidate.segmentId || null,
              sourceUrls: candidate.sourceUrls || [],
              website: candidate.sourceUrl || null,
            },
            { dryRun: true },
          );
        } else if (
          write.tableName === GDI_AIRTABLE_TABLES.PRIVATE_EVENT_VENUES.name
        ) {
          preview = await upsertVenue(
            {
              venueName: plan.organizationName,
              city: input.city,
              venueId: identities.venueId,
              website: candidate.sourceUrl || null,
            },
            { dryRun: true },
          );
        } else if (
          write.tableName === GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name
        ) {
          preview = await syncHotelOpportunitiesToAirtable({
            hotelId,
            dryRun: true,
            opportunities: [
              {
                id: identities.opportunityId,
                opportunityId: identities.opportunityId,
                organizationName: plan.organizationName,
                hotelId,
                segmentId: candidate.segmentId,
                title: candidate.title || plan.organizationName,
              },
            ],
            runId: input.runId,
          });
        } else {
          preview = { dryRun: true, action: "UPSERT_PREVIEW_UNSUPPORTED_IN_ADAPTER_V1" };
        }
      } catch (err) {
        retries.push({
          tableName: write.tableName,
          identityKey: write.identityKey,
          error: String(err?.message || err),
          recoverable: true,
        });
        operations.push({
          tableName: write.tableName,
          outcome: WRITE_OUTCOME.FAILED,
          reason: "ADAPTER_PREVIEW_ERROR",
          error: String(err?.message || err),
          identityKey: write.identityKey,
        });
        continue;
      }

      operations.push({
        tableName: write.tableName,
        outcome: WRITE_OUTCOME.DEFERRED,
        phaseOutcome: "DRY_RUN_PREVIEW_OK",
        reason: "PRODUCTION_WRITE_BLOCKED_PENDING_FOUNDER_AUTH",
        identityKey: write.identityKey,
        canonicalId:
          write.tableName === GDI_AIRTABLE_TABLES.DEMAND_GENERATORS.name
            ? identities.demandGeneratorId
            : write.tableName ===
                GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.name
              ? identities.opportunityId
              : write.tableName === GDI_AIRTABLE_TABLES.PRIVATE_EVENT_VENUES.name
                ? identities.venueId
                : write.identityKey,
        eligibility,
        preview: {
          dryRun: true,
          action: preview?.action || preview?.ok || null,
          recordId: preview?.recordId || null,
        },
        readBackRequired: true,
      });
    }
  }

  return {
    ok: true,
    version: GDI_FULL_E2E_AIRTABLE_PERSISTENCE_ADAPTER_V1,
    contractVersion: GDI_FULL_E2E_AIRTABLE_PERSISTENCE_CONTRACT_V1,
    dryRun: true,
    hotelId,
    runId: text(input.runId) || null,
    candidateCount: candidates.length,
    operations,
    retries,
    mutationGuard: AIRTABLE_PRODUCTION_WRITE_GUARD,
    certification: {
      routing: true,
      eligibilityGates: true,
      idempotentPreview: true,
      productionWrites: false,
      readBack: "required_on_apply",
      multiHotelIsolation: "hotelId_required_on_gdo",
    },
  };
}

export default {
  GDI_FULL_E2E_AIRTABLE_PERSISTENCE_ADAPTER_V1,
  executeFullE2eAirtablePersistence,
};
