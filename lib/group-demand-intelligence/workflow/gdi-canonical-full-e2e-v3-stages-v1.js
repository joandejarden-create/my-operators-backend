/**
 * Canonical GDI Full E2E V3 — five permanent research stages (product-wide).
 *
 * Does not invent a parallel research engine. Maps onto existing discovery,
 * Agent deep research, WHO/HOW restore, public contact HOW, and six-rule FQO.
 */

export const GDI_CANONICAL_FULL_E2E_V3_STAGES_V1 =
  "gdi-canonical-full-e2e-v3-stages-v1";

export const GDI_E2E_V3_STAGE = Object.freeze({
  DEMAND_DISCOVERY: "DEMAND_DISCOVERY",
  OPPORTUNITY_DEEP_RESEARCH: "OPPORTUNITY_DEEP_RESEARCH",
  WHO_HOW_RESOLUTION: "WHO_HOW_RESOLUTION",
  PROFESSIONAL_CONTACT_RESEARCH: "PROFESSIONAL_CONTACT_RESEARCH",
  COMMERCIAL_QUALIFICATION: "COMMERCIAL_QUALIFICATION",
});

export const GDI_E2E_V3_STAGE_OUTCOME = Object.freeze({
  PASS: "PASS",
  HOLD: "HOLD",
  FAILED: "FAILED",
  SKIPPED: "SKIPPED",
  PARTIAL: "PARTIAL",
  NOT_STARTED: "NOT_STARTED",
});

/** Ordered permanent sequence. */
export const GDI_E2E_V3_STAGE_ORDER = Object.freeze([
  GDI_E2E_V3_STAGE.DEMAND_DISCOVERY,
  GDI_E2E_V3_STAGE.OPPORTUNITY_DEEP_RESEARCH,
  GDI_E2E_V3_STAGE.WHO_HOW_RESOLUTION,
  GDI_E2E_V3_STAGE.PROFESSIONAL_CONTACT_RESEARCH,
  GDI_E2E_V3_STAGE.COMMERCIAL_QUALIFICATION,
]);

/**
 * Existing architecture mapping for founder closeout / operators.
 */
export const GDI_E2E_V3_EXISTING_MAPPINGS = Object.freeze({
  [GDI_E2E_V3_STAGE.DEMAND_DISCOVERY]: Object.freeze({
    existing: [
      "fourteen-segment discovery matrix",
      "leisure module applicability + LDI runners",
      "CAD competitive account discovery",
      "preferred_lodging_partnerships segment",
      "screening dispositions / lossless register",
      "commercial priorities soft guidance",
    ],
    newInV3: [
      "Dedicated Invoke-GdiDiscovery.ps1 stage runner",
      "Stage checkpoint + completeness report",
      "Unified Full E2E V3 orchestrator sequencing",
    ],
    powershell: "scripts/gdi/Invoke-GdiDiscovery.ps1",
    nodeRunner: "scripts/gdi/run-gdi-discovery-stage-v1.mjs",
  }),
  [GDI_E2E_V3_STAGE.OPPORTUNITY_DEEP_RESEARCH]: Object.freeze({
    existing: [
      "COMMERCIAL_FULL_E2E operating mode (PR #612)",
      "Exa Agent deep job store + paid budget ledger",
      "completion queue / fair select",
      "segment-aware Agent prompt families",
    ],
    newInV3: [
      "Invoke-GdiDeepResearch.ps1",
      "Explicit route of missing buyer → Stage 3 (no FQO termination)",
      "Stage checkpoint reuse of settled Agent jobs",
      "Commercial viability precheck at Stage 2 end (not a sixth stage; reuses FQO demand/fit/future)",
    ],
    powershell: "scripts/gdi/Invoke-GdiDeepResearch.ps1",
    nodeRunner: "scripts/gdi/run-gdi-deep-research-stage-v1.mjs",
    viabilityPrecheck:
      "lib/group-demand-intelligence/workflow/gdi-commercial-viability-precheck-v1.js",
  }),
  [GDI_E2E_V3_STAGE.WHO_HOW_RESOLUTION]: Object.freeze({
    existing: [
      "ensureBuyerWhoResearchExecuted",
      "gdi-who-how-contact-intelligence-restore-v1",
      "named contact coverage / scaling two-contact",
      "RELEVANT_COMMERCIAL_ENTRY_POINT classification",
    ],
    newInV3: [
      "Dedicated WHO/HOW Agent contract (dossier-input, not rediscovery)",
      "Mandatory two alternative entry-point fallback",
      "WHO_HOW_STATUS taxonomy (buyer vs influencer vs entry points)",
      "Invoke-GdiWhoHow.ps1 + ContinuationOnly",
    ],
    powershell: "scripts/gdi/Invoke-GdiWhoHow.ps1",
    nodeRunner: "scripts/gdi/run-gdi-who-how-stage-v1.mjs",
  }),
  [GDI_E2E_V3_STAGE.PROFESSIONAL_CONTACT_RESEARCH]: Object.freeze({
    existing: [
      "runPublicContactDiscoveryWorkflow / HOW restore",
      "assessPublicContactVerification",
      "Findymail/Surfe optional OFF by default",
    ],
    newInV3: [
      "Stage-4-only after identity/role verification",
      "Channel provenance schema (person vs org)",
      "No invented emails; Surfe never auto-persisted",
      "Invoke-GdiContactResearch.ps1",
    ],
    powershell: "scripts/gdi/Invoke-GdiContactResearch.ps1",
    nodeRunner: "scripts/gdi/run-gdi-contact-research-stage-v1.mjs",
  }),
  [GDI_E2E_V3_STAGE.COMMERCIAL_QUALIFICATION]: Object.freeze({
    existing: [
      "gdi-finalized-qualified-opportunity-contract-v1 (six-rule FQO)",
      "GDI drawer Who to Contact / two-contact slots",
      "contact actionability tiers",
    ],
    newInV3: [
      "Audited entry-point reachability rule for FQO",
      "Drawer PRIMARY DECISION-MAKER + ALTERNATIVE CONTACT 1/2",
      "Invoke-GdiQualification.ps1",
    ],
    powershell: "scripts/gdi/Invoke-GdiQualification.ps1",
    nodeRunner: "scripts/gdi/run-gdi-qualification-stage-v1.mjs",
  }),
});

export function getE2eV3Stage(stageId) {
  const id = String(stageId || "").trim().toUpperCase();
  return GDI_E2E_V3_STAGE_ORDER.includes(id) ? id : null;
}

export function buildDefaultStageCheckpoint(hotelId, extras = {}) {
  const stages = {};
  for (const id of GDI_E2E_V3_STAGE_ORDER) {
    stages[id] = {
      stageId: id,
      outcome: GDI_E2E_V3_STAGE_OUTCOME.NOT_STARTED,
      completedAt: null,
      reportPath: null,
      notes: [],
    };
  }
  return {
    version: GDI_CANONICAL_FULL_E2E_V3_STAGES_V1,
    hotelId: hotelId || null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    stages,
    ...extras,
  };
}

/**
 * Advance checkpoint after a stage run (immutable-style merge).
 */
export function applyStageCheckpointResult(checkpoint, stageId, result = {}) {
  const id = getE2eV3Stage(stageId);
  if (!id) throw new Error(`unknown_e2e_v3_stage:${stageId}`);
  const next = {
    ...checkpoint,
    updatedAt: new Date().toISOString(),
    stages: {
      ...(checkpoint.stages || {}),
      [id]: {
        ...(checkpoint.stages?.[id] || {}),
        stageId: id,
        outcome: result.outcome || GDI_E2E_V3_STAGE_OUTCOME.PASS,
        completedAt: result.completedAt || new Date().toISOString(),
        reportPath: result.reportPath || null,
        notes: Array.isArray(result.notes) ? result.notes : [],
        metrics: result.metrics || null,
      },
    },
  };
  return next;
}

export function summarizeE2eV3Completeness(checkpoint = {}) {
  const stages = checkpoint.stages || {};
  const rows = GDI_E2E_V3_STAGE_ORDER.map((id) => ({
    stageId: id,
    outcome: stages[id]?.outcome || GDI_E2E_V3_STAGE_OUTCOME.NOT_STARTED,
  }));
  const passCount = rows.filter(
    (r) =>
      r.outcome === GDI_E2E_V3_STAGE_OUTCOME.PASS ||
      r.outcome === GDI_E2E_V3_STAGE_OUTCOME.SKIPPED,
  ).length;
  const partial = rows.some(
    (r) =>
      r.outcome === GDI_E2E_V3_STAGE_OUTCOME.PARTIAL ||
      r.outcome === GDI_E2E_V3_STAGE_OUTCOME.HOLD,
  );
  const failed = rows.some((r) => r.outcome === GDI_E2E_V3_STAGE_OUTCOME.FAILED);
  return {
    version: GDI_CANONICAL_FULL_E2E_V3_STAGES_V1,
    hotelId: checkpoint.hotelId || null,
    rows,
    passCount,
    totalStages: GDI_E2E_V3_STAGE_ORDER.length,
    complete: passCount === GDI_E2E_V3_STAGE_ORDER.length && !failed,
    partial: partial === true,
    failed: failed === true,
  };
}
