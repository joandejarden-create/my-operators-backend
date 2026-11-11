/**
 * Persistent HPC enrichment cycle — plan → authorize apply → readback → ledger.
 * Invoked from ADP Full E2E `hpc_onboarding_enrichment` and standalone scripts.
 */

import {
  planHpcEnrichmentForAdpHotel,
  executeHpcEnrichmentForAdpHotel,
  fetchHpcRecord,
  fetchLiveHpcSchema,
  applyHpcGovernedSemanticCorrection,
} from "./hpc-shared-upsert-v1.js";
import {
  validateHotelHpcSemantics,
  HPC_FIELD_SEMANTICS_CERT,
} from "./hpc-field-semantic-validation-v1.js";
import {
  HPC_CERT_STATES,
  isFieldEmpty,
} from "./hpc-full-onboarding-contract-v1.js";
import { OFFICIAL_RESEARCH_PACK_V1 } from "./hpc-exhaustive-field-completion-v1.js";
import {
  emptyHotelLedger,
  loadHotelHpcResearchLedger,
  saveHotelHpcResearchLedger,
  reconcileLedgerWithLiveRecord,
  recordResearchAttempt,
  markFieldCompletedAndVerified,
  isEligibleForResearchAttempt,
  summarizeHotelLedger,
  HPC_RESEARCH_STATUS,
  HPC_ENRICHMENT_READINESS,
  createFieldResearchTask,
} from "./hpc-persistent-field-research-ledger-v1.js";
import {
  classifyHpcFieldImportance,
  HPC_FIELD_IMPORTANCE,
} from "./hpc-field-importance-v1.js";

export const HPC_PERSISTENT_ENRICHMENT_EXECUTOR_V1 =
  "HPC_PERSISTENT_ENRICHMENT_EXECUTOR_V1";

/**
 * Run one persistent enrichment cycle for an ADP hotel.
 *
 * @param {object} opts
 * @param {string} opts.adpPropertyId
 * @param {object} [opts.profile]
 * @param {object} [opts.schema]
 * @param {boolean} [opts.dryRun=true]
 * @param {boolean} [opts.allowAirtableWrites=false] — must be true with dryRun=false to PATCH
 * @param {string} [opts.reason]
 * @param {string} [opts.cwd]
 */
export async function runPersistentHpcEnrichmentCycle({
  adpPropertyId,
  profile = null,
  schema = null,
  dryRun = true,
  allowAirtableWrites = false,
  reason = "hpc_persistent_enrichment_cycle_v1",
  cwd = process.cwd(),
} = {}) {
  const started = new Date().toISOString();
  const liveSchema = schema || (await fetchLiveHpcSchema());
  const plan = await planHpcEnrichmentForAdpHotel({
    adpPropertyId,
    profile,
    schema: liveSchema,
  });

  if (!plan.ok || !plan.recordId) {
    const ledger = saveHotelHpcResearchLedger(
      summarizeIdentityFailure(adpPropertyId, plan),
      cwd
    );
    return {
      ok: false,
      adpPropertyId,
      started,
      finished: new Date().toISOString(),
      plan,
      applied: false,
      ledger,
      enrichmentReadiness: ledger.enrichmentReadiness,
      qualityGate: {
        passed: false,
        reasons: [plan.error || "HPC_UNRESOLVED"],
      },
    };
  }

  let ledger =
    loadHotelHpcResearchLedger(adpPropertyId, cwd) ||
    emptyHotelLedger({
      propertyId: adpPropertyId,
      hpcRecordId: plan.recordId,
      hotelName: plan.propertyName,
    });

  const beforeRecord = await fetchHpcRecord(plan.recordId);
  ledger = reconcileLedgerWithLiveRecord({
    ledger: {
      ...ledger,
      hpcRecordId: plan.recordId,
      hotelName: plan.propertyName || ledger.hotelName,
      propertyId: adpPropertyId,
    },
    liveFields: beforeRecord?.fields || {},
    schemaFields: liveSchema?.fields || [],
  });

  // Attach pack evidence memory for proposed fields (genuine sources)
  const pack = OFFICIAL_RESEARCH_PACK_V1[adpPropertyId] || {};
  const proposals = plan.proposals || [];
  const researchActions = [];

  for (const p of proposals) {
    const field = p.field;
    let task =
      ledger.fields[field] ||
      createFieldResearchTask({
        fieldName: field,
        currentValue: null,
        importance: classifyHpcFieldImportance(field),
      });

    const packSpec = pack[field];
    if (packSpec?.sourceUrl || p.source) {
      if (isEligibleForResearchAttempt(task)) {
        task = recordResearchAttempt(task, {
          status: HPC_RESEARCH_STATUS.EVIDENCE_FOUND,
          sourceUrl: packSpec?.sourceUrl || null,
          sourcesExamined: packSpec?.sourceUrl ? [packSpec.sourceUrl] : [],
          finding: `proposal:${p.source || packSpec?.source || "plan"}`,
          evidenceFound: [
            `value=${JSON.stringify(p.value)}`,
            `source=${p.source || packSpec?.source || ""}`,
          ],
          evidenceConfidence: p.confidence || "OFFICIAL_RESEARCH",
          nextResearchAction: "APPLY_NULL_FILL_WITH_READBACK",
        });
        researchActions.push({ field, action: "EVIDENCE_RECORDED_FROM_PACK" });
      }
    } else if (isEligibleForResearchAttempt(task)) {
      // Proposal from profile recovery — not external web research
      task = {
        ...task,
        researchStatus: HPC_RESEARCH_STATUS.RECOVER_EXISTING_DATA,
        resolutionStatus: HPC_RESEARCH_STATUS.RECOVER_EXISTING_DATA,
        nextResearchAction: "APPLY_NULL_FILL_WITH_READBACK",
      };
      researchActions.push({ field, action: "RECOVER_EXISTING_DATA" });
    }
    ledger.fields[field] = task;
  }

  const applyAuthorized = allowAirtableWrites === true && dryRun === false;
  let exec = null;
  if (applyAuthorized && proposals.length > 0) {
    // Mark write pending
    for (const p of proposals) {
      if (ledger.fields[p.field]) {
        ledger.fields[p.field] = {
          ...ledger.fields[p.field],
          researchStatus: HPC_RESEARCH_STATUS.AIRTABLE_WRITE_PENDING,
          resolutionStatus: HPC_RESEARCH_STATUS.AIRTABLE_WRITE_PENDING,
        };
      }
    }
    saveHotelHpcResearchLedger(ledger, cwd);

    exec = await executeHpcEnrichmentForAdpHotel({
      adpPropertyId,
      profile,
      schema: liveSchema,
      dryRun: false,
      reason,
    });

    const after = exec.writeResult?.after || (await fetchHpcRecord(plan.recordId));
    const readback = exec.writeResult?.readback || {};
    for (const [field, rb] of Object.entries(readback)) {
      let task = ledger.fields[field];
      if (!task) continue;
      if (rb.match) {
        task = markFieldCompletedAndVerified(task, {
          value: rb.actual,
          sourceUrl: pack[field]?.sourceUrl || null,
          confidence: "READBACK_MATCH",
        });
        researchActions.push({ field, action: "COMPLETED_AND_VERIFIED" });
      } else {
        task = recordResearchAttempt(task, {
          status: HPC_RESEARCH_STATUS.AIRTABLE_READBACK_PENDING,
          blockingReason: "READBACK_MISMATCH",
          finding: `expected=${JSON.stringify(rb.expected)} actual=${JSON.stringify(rb.actual)}`,
          nextResearchAction: "RETRY_WRITE_OR_STEWARD_REVIEW",
        });
        researchActions.push({ field, action: "READBACK_MISMATCH" });
      }
      ledger.fields[field] = task;
    }

    ledger = reconcileLedgerWithLiveRecord({
      ledger,
      liveFields: after?.fields || {},
      schemaFields: liveSchema?.fields || [],
    });
  } else if (proposals.length > 0 && dryRun) {
    for (const p of proposals) {
      if (ledger.fields[p.field]) {
        ledger.fields[p.field] = {
          ...ledger.fields[p.field],
          researchStatus: HPC_RESEARCH_STATUS.WAITING_FOR_AUTHORIZATION,
          resolutionStatus: HPC_RESEARCH_STATUS.WAITING_FOR_AUTHORIZATION,
          nextResearchAction: "AUTHORIZE_AIRTABLE_NULL_FILL",
          blockingReason: "DRY_RUN_OR_WRITES_DISABLED",
        };
      }
    }
  }

  // Mark empty eligible fields without proposals as still requiring research
  // (do not fabricate attempts)
  for (const [fieldName, task] of Object.entries(ledger.fields)) {
    if (isTerminalCompleteStatusSafe(task.researchStatus)) continue;
    if (proposals.some((p) => p.field === fieldName)) continue;
    if (
      task.researchStatus === HPC_RESEARCH_STATUS.HEURISTIC_UNRESOLVED ||
      task.researchStatus === HPC_RESEARCH_STATUS.VERIFIED_UNAVAILABLE
    ) {
      continue;
    }
    if (isFieldEmpty(beforeRecord?.fields?.[fieldName])) {
      ledger.fields[fieldName] = {
        ...task,
        researchStatus:
          task.researchStatus || HPC_RESEARCH_STATUS.RESEARCH_REQUIRED,
        nextResearchAction: task.nextResearchAction || "AWAIT_EVIDENCE_OR_PACK",
      };
    }
  }

  // --- Field semantic validation (incorrect populated values, not only empties) ---
  const liveForSemantics =
    (exec && exec.applied && (await fetchHpcRecord(plan.recordId))) || beforeRecord;
  const semantics = validateHotelHpcSemantics({
    adpPropertyId,
    hpcRecordId: plan.recordId,
    hotelName: plan.propertyName,
    fields: liveForSemantics?.fields || {},
    schema: liveSchema,
    profile,
  });

  let semanticApply = null;
  const semanticPatch = {};
  for (const c of semantics.corrections || []) {
    semanticPatch[c.field] = c.to;
  }
  if (Object.keys(semanticPatch).length) {
    if (allowAirtableWrites && !dryRun) {
      semanticApply = await applyHpcGovernedSemanticCorrection({
        recordId: plan.recordId,
        patch: semanticPatch,
        schema: liveSchema,
        dryRun: false,
        reason: `${reason}:semantic_correction`,
      });
      researchActions.push({
        type: "SEMANTIC_CORRECTION_APPLY",
        fields: Object.keys(semanticPatch),
        ok: !!semanticApply?.ok,
      });
      if (semanticApply?.ok && semanticApply.readback) {
        for (const [field, rb] of Object.entries(semanticApply.readback)) {
          if (!rb.match) continue;
          ledger.fields[field] = {
            ...markFieldCompletedAndVerified(
              ledger.fields[field] ||
                createFieldResearchTask({
                  fieldName: field,
                  currentValue: rb.before,
                  importance: classifyHpcFieldImportance(field),
                }),
              {
                value: rb.actual,
                sourceUrl: "HPC_FIELD_SEMANTIC_VALIDATION_V1",
                confidence: "HIGH",
              }
            ),
            semanticIssueCode: null,
            discrepancyReason: `Corrected ${rb.before} → ${rb.actual}`,
            priorValuePreserved: rb.before,
          };
        }
        // Re-validate after apply
        const afterSem = await fetchHpcRecord(plan.recordId);
        Object.assign(
          semantics,
          validateHotelHpcSemantics({
            adpPropertyId,
            hpcRecordId: plan.recordId,
            hotelName: plan.propertyName,
            fields: afterSem?.fields || {},
            schema: liveSchema,
            profile,
          })
        );
      }
    } else {
      researchActions.push({
        type: "SEMANTIC_CORRECTION_PENDING_AUTH",
        fields: Object.keys(semanticPatch),
        dryRun: true,
      });
    }
  }

  // Persist non-auto semantic discrepancies as research obligations (dedupe by field+code)
  for (const d of semantics.discrepancies || []) {
    if (d.autoApplicable && allowAirtableWrites && !dryRun) continue;
    const key = d.field;
    const existing = ledger.fields[key];
    if (
      existing &&
      (existing.researchStatus === HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED ||
        existing.researchStatus === HPC_RESEARCH_STATUS.VERIFIED_COMPLETE) &&
      String(existing.currentValue) === String(d.proposedValue)
    ) {
      continue;
    }
    if (
      existing?.semanticIssueCode === d.code &&
      existing?.researchStatus === HPC_RESEARCH_STATUS.RESEARCH_REQUIRED
    ) {
      continue; // idempotent — same open obligation
    }
    ledger.fields[key] = {
      ...(existing ||
        createFieldResearchTask({
          fieldName: key,
          currentValue: d.existingValue,
          importance: d.importance || classifyHpcFieldImportance(key),
        })),
      currentValue: d.existingValue,
      proposedValue: d.proposedValue,
      researchStatus: d.autoApplicable
        ? HPC_RESEARCH_STATUS.WAITING_FOR_AUTHORIZATION
        : HPC_RESEARCH_STATUS.RESEARCH_REQUIRED,
      semanticIssueCode: d.code,
      discrepancyReason: d.reason,
      nextResearchAction: d.autoApplicable
        ? "AUTHORIZE_SEMANTIC_CORRECTION"
        : "RESEARCH_OR_MANUAL_REVIEW",
      blockingReason: d.code,
    };
  }

  ledger = summarizeHotelLedger({
    ...ledger,
    lastCycleAt: new Date().toISOString(),
    lastResearchAttemptAt: researchActions.length
      ? new Date().toISOString()
      : ledger.lastResearchAttemptAt,
    lastSemanticsCert: semantics.cert,
    cycleHistory: [
      ...(ledger.cycleHistory || []).slice(-19),
      {
        at: started,
        dryRun,
        allowAirtableWrites,
        proposalCount: proposals.length,
        applied: !!(exec && exec.applied),
        researchActionCount: researchActions.length,
        semanticDiscrepancies: semantics.discrepancyCount,
        semanticAutoCorrections: Object.keys(semanticPatch).length,
        semanticApplied: !!(semanticApply && semanticApply.ok),
        enrichmentReadiness: null,
      },
    ],
  });
  // fix readiness on last history entry
  if (ledger.cycleHistory?.length) {
    ledger.cycleHistory[ledger.cycleHistory.length - 1].enrichmentReadiness =
      ledger.enrichmentReadiness;
  }

  ledger = saveHotelHpcResearchLedger(ledger, cwd);

  const cert = plan.certification;
  const criticalBlocked =
    ledger.enrichmentReadiness ===
      HPC_ENRICHMENT_READINESS.HPC_BLOCKED_CRITICAL ||
    semantics.cert?.passed === false;
  const minOk = cert?.minimumOperationalReady === true;
  const backlogReady =
    ledger.enrichmentReadiness ===
    HPC_ENRICHMENT_READINESS.HPC_ADP_READY_WITH_ENRICHMENT_BACKLOG;
  const fullyEnriched =
    ledger.enrichmentReadiness ===
    HPC_ENRICHMENT_READINESS.HPC_FULLY_ENRICHED;

  // Fail-closed on critical gaps / critical semantic defects; allow Core when min identity ready + backlog
  const qualityPassed =
    !criticalBlocked &&
    (fullyEnriched ||
      cert?.certState === HPC_CERT_STATES.HPC_CERTIFIED_FOR_ADP ||
      cert?.certState === HPC_CERT_STATES.HPC_ENRICHMENT_COMPLETE ||
      (minOk && backlogReady) ||
      (minOk && proposals.length === 0 && !criticalBlocked));

  return {
    ok: true,
    adpPropertyId,
    started,
    finished: new Date().toISOString(),
    plan,
    exec,
    applied: !!(exec && exec.applied),
    semanticApply,
    semantics,
    [HPC_FIELD_SEMANTICS_CERT]: semantics.cert,
    dryRun,
    allowAirtableWrites,
    proposalCount: proposals.length,
    researchActions,
    ledger,
    enrichmentReadiness: ledger.enrichmentReadiness,
    certification: cert,
    downstreamInvalidation: assessDownstreamInvalidation(
      Object.keys(semanticPatch)
    ),
    qualityGate: {
      passed: qualityPassed,
      reasons: qualityPassed
        ? [
            ledger.enrichmentReadiness,
            cert?.certState,
            backlogReady ? "ENRICHMENT_BACKLOG_RETAINED" : null,
            HPC_FIELD_SEMANTICS_CERT + "_PASS",
          ].filter(Boolean)
        : [
            criticalBlocked && semantics.cert?.passed === false
              ? "HPC_FIELD_SEMANTICS_CERT_FAILED"
              : null,
            criticalBlocked &&
            ledger.enrichmentReadiness ===
              HPC_ENRICHMENT_READINESS.HPC_BLOCKED_CRITICAL
              ? "CRITICAL_HPC_FIELDS_OPEN"
              : null,
            cert?.certState || "HPC_INCOMPLETE",
          ].filter(Boolean),
    },
    stageStateHint: criticalBlocked
      ? "BLOCKED_EVIDENCE"
      : minOk
        ? "COMPLETE"
        : "BLOCKED_EVIDENCE",
  };
}

function isTerminalCompleteStatusSafe(status) {
  return (
    status === HPC_RESEARCH_STATUS.VERIFIED_COMPLETE ||
    status === HPC_RESEARCH_STATUS.COMPLETED_AND_VERIFIED ||
    status === HPC_RESEARCH_STATUS.NOT_APPLICABLE
  );
}

function summarizeIdentityFailure(adpPropertyId, plan) {
  return {
    ledgerVersion: "HPC_PERSISTENT_FIELD_RESEARCH_LEDGER_V1",
    propertyId: adpPropertyId,
    hpcRecordId: null,
    fields: {},
    enrichmentReadiness: HPC_ENRICHMENT_READINESS.HPC_IDENTITY_UNRESOLVED,
    summary: {
      fieldCount: 0,
      completed: 0,
      criticalOpen: 1,
      importantOpen: 0,
      enrichmentOpen: 0,
      heuristicUnresolved: 0,
      verifiedUnavailable: 0,
      criticalBlockers: ["HPC_RECORD_LINK"],
      importantMissing: [],
      enrichmentBacklog: [],
    },
    lastCycleAt: new Date().toISOString(),
    identityError: plan?.error || "HPC_UNRESOLVED",
  };
}

/**
 * Downstream invalidation hints when verified field values change.
 */
export function assessDownstreamInvalidation(changedFields = []) {
  const rules = [];
  for (const field of changedFields) {
    const imp = classifyHpcFieldImportance(field);
    if (["Latitude", "Longitude", "City", "Country", "Market", "Submarket"].includes(field)) {
      rules.push({
        field,
        invalidate: ["market_location_validation", "core_competitive_set", "staged_geo_dependent"],
        remonitorRequired: false,
      });
    }
    if (["Current Brand", "Brand Family", "Family / Source Family"].includes(field)) {
      rules.push({
        field,
        invalidate: ["bpp_portfolio_measurement", "brand_membership"],
        remonitorRequired: false,
      });
    }
    if (["Rooms / Keys", "Property Type", "Operator / Management Company"].includes(field)) {
      rules.push({
        field,
        invalidate: ["property_attribute_intelligence", "property_reality_coverage"],
        remonitorRequired: false,
      });
    }
    if (imp === HPC_FIELD_IMPORTANCE.ENRICHMENT) {
      rules.push({
        field,
        invalidate: ["property_attribute_intelligence"],
        remonitorRequired: false,
      });
    }
  }
  return {
    remonitorRequired: false,
    publishedMutationForbidden: true,
    rules,
  };
}
