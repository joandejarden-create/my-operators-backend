/**
 * Canonical Full E2E V3 orchestrator — sequences five permanent stages.
 *
 * DryRun default. Live paid stages require explicit authorization flags.
 * Reuses existing modules; does not create a parallel research engine.
 */

import fs from "fs";
import path from "path";
import {
  GDI_CANONICAL_FULL_E2E_V3_STAGES_V1,
  GDI_E2E_V3_STAGE,
  GDI_E2E_V3_STAGE_OUTCOME,
  GDI_E2E_V3_STAGE_ORDER,
  GDI_E2E_V3_EXISTING_MAPPINGS,
  buildDefaultStageCheckpoint,
  applyStageCheckpointResult,
  summarizeE2eV3Completeness,
} from "./gdi-canonical-full-e2e-v3-stages-v1.js";
import {
  buildWhoHowAgentInputFromDossier,
  executeWhoHowResolutionFixture,
  WHO_HOW_STATUS,
} from "./gdi-who-how-agent-contract-v1.js";
import { runProfessionalContactResearchStage } from "./gdi-contact-research-stage-v1.js";
import {
  evaluateEntryPointFqoReachability,
  mergeEntryPointReachabilityIntoFqoGates,
} from "./gdi-fqo-entry-point-reachability-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "./gdi-finalized-qualified-opportunity-contract-v1.js";
import { DEEP_RESEARCH_OPERATING_MODE } from "../research/gdi-commercial-deep-research-operating-modes-v1.js";
import {
  batchCommercialViabilityPrecheck,
  GDI_COMMERCIAL_VIABILITY_PRECHECK_V1,
} from "./gdi-commercial-viability-precheck-v1.js";

export const GDI_FULL_E2E_V3_ORCHESTRATOR_V1 = "gdi-full-e2e-v3-orchestrator-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function resolveV3CheckpointPath(hotelId, outDir) {
  return path.join(
    outDir,
    "checkpoints",
    `${text(hotelId) || "unknown"}-e2e-v3-checkpoint.json`,
  );
}

export function loadOrCreateCheckpoint(hotelId, outDir) {
  const p = resolveV3CheckpointPath(hotelId, outDir);
  if (fs.existsSync(p)) {
    return { path: p, checkpoint: JSON.parse(fs.readFileSync(p, "utf8")) };
  }
  const checkpoint = buildDefaultStageCheckpoint(hotelId);
  return { path: p, checkpoint };
}

export function saveCheckpoint(filePath, checkpoint) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(checkpoint, null, 2));
  return filePath;
}

/**
 * Stage 1 DryRun — inventory existing discovery artifacts / plan.
 */
export function runDiscoveryStageDry(hotelId, opts = {}) {
  const matrixPath =
    opts.discoveryMatrixPath ||
    "reports/gdi-wyndham-sd-bayside-paid-live-cert-25usd-v1/fourteen-segment-discovery-matrix.json";
  const hasMatrix = fs.existsSync(matrixPath);
  let segmentCount = 0;
  if (hasMatrix) {
    try {
      const m = JSON.parse(fs.readFileSync(matrixPath, "utf8"));
      segmentCount = Array.isArray(m.cells) ? m.cells.length : 0;
    } catch {
      segmentCount = 0;
    }
  }
  return {
    stageId: GDI_E2E_V3_STAGE.DEMAND_DISCOVERY,
    outcome: hasMatrix
      ? GDI_E2E_V3_STAGE_OUTCOME.PASS
      : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    mode: "DryRun",
    paid: false,
    metrics: {
      hotelId,
      discoveryMatrixPath: hasMatrix ? matrixPath : null,
      segmentCells: segmentCount,
      leisureModulesAssessedSeparately: true,
      note: "DryRun reuses existing discovery artifacts; live discovery requires authorization",
    },
  };
}

/**
 * Stage 2 DryRun — confirm commercial queue / dossiers exist; no paid calls.
 * Ends with commercial viability precheck (not a sixth stage) before Stage 3.
 */
export function runDeepResearchStageDry(hotelId, opts = {}) {
  const queueDir =
    opts.queueDossierDir ||
    "reports/gdi-wyndham-commercial-full-e2e-queue-20usd-v1/dossiers";
  let dossierCount = 0;
  const dossiers = [];
  if (fs.existsSync(queueDir)) {
    for (const f of fs.readdirSync(queueDir).filter((x) => x.endsWith(".json"))) {
      dossierCount += 1;
      try {
        const raw = JSON.parse(fs.readFileSync(path.join(queueDir, f), "utf8"));
        dossiers.push(raw.opportunity || raw);
      } catch {
        /* skip unreadable fixture */
      }
    }
  }
  const viabilityInputs =
    Array.isArray(opts.viabilityOpportunities) && opts.viabilityOpportunities.length
      ? opts.viabilityOpportunities
      : dossiers;
  const viability = batchCommercialViabilityPrecheck(viabilityInputs, opts);
  return {
    stageId: GDI_E2E_V3_STAGE.OPPORTUNITY_DEEP_RESEARCH,
    outcome:
      dossierCount > 0
        ? GDI_E2E_V3_STAGE_OUTCOME.PASS
        : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    mode: "DryRun",
    paid: false,
    operatingMode: DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E,
    commercialViabilityPrecheck: viability,
    metrics: {
      hotelId,
      dossierCount,
      queueDossierDir: queueDir,
      missingBuyerRoutesToStage3: true,
      commercialViabilitySchema: GDI_COMMERCIAL_VIABILITY_PRECHECK_V1,
      proceedToWhoHow: viability.proceedToWhoHow,
      viabilityCounts: viability.counts,
      note: "DryRun does not seed Exa Agent; settled jobs reused when Live. Viability precheck gates paid WHO/HOW prioritization.",
    },
  };
}

/**
 * Stage 3 — WHO/HOW on fixtures or provided dossiers (no paid by default).
 */
export function runWhoHowStageDry(hotelId, opts = {}) {
  const fixtures = opts.whoHowFixtures || [];
  const results = [];
  for (const fx of fixtures) {
    const executed = executeWhoHowResolutionFixture(
      fx.dossier || { opportunity: fx.opportunity },
      fx.output,
      { hotelId, reused: fx.reused === true },
    );
    results.push(executed);
  }
  const continuationOnly = opts.continuationOnly === true;
  return {
    stageId: GDI_E2E_V3_STAGE.WHO_HOW_RESOLUTION,
    outcome:
      results.length || continuationOnly
        ? GDI_E2E_V3_STAGE_OUTCOME.PASS
        : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    mode: "DryRun",
    paid: false,
    continuationOnly,
    metrics: {
      hotelId,
      processed: results.length,
      statuses: results.map((r) => r.whoHow?.whoHowStatus),
    },
    results,
  };
}

/**
 * Stage 4 DryRun contact research.
 */
export async function runContactResearchStageDry(opportunities = [], opts = {}) {
  const results = [];
  for (const opp of opportunities) {
    results.push(
      await runProfessionalContactResearchStage(opp, {
        mode: "DryRun",
        fixture: true,
        ...opts,
      }),
    );
  }
  const hold = results.some((r) => r.outcome === "HOLD");
  return {
    stageId: GDI_E2E_V3_STAGE.PROFESSIONAL_CONTACT_RESEARCH,
    outcome: results.length
      ? hold
        ? GDI_E2E_V3_STAGE_OUTCOME.PARTIAL
        : GDI_E2E_V3_STAGE_OUTCOME.PASS
      : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    mode: "DryRun",
    paid: false,
    findymail: false,
    surfe: false,
    metrics: { processed: results.length },
    results,
  };
}

/**
 * Stage 5 — independent FQO + entry-point reachability.
 */
export function runQualificationStageDry(opportunities = [], opts = {}) {
  const results = [];
  for (const opp of opportunities) {
    const reach = evaluateEntryPointFqoReachability(opp, opts);
    let fqo = evaluateFinalizedQualifiedOpportunity(opp, opts);
    fqo = mergeEntryPointReachabilityIntoFqoGates(fqo, reach);
    results.push({ opportunityId: opp.opportunityId || opp.candidateId, fqo, reach });
  }
  return {
    stageId: GDI_E2E_V3_STAGE.COMMERCIAL_QUALIFICATION,
    outcome: results.length
      ? GDI_E2E_V3_STAGE_OUTCOME.PASS
      : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    mode: "DryRun",
    metrics: {
      processed: results.length,
      finalized: results.filter((r) => r.fqo?.finalizedQualified).length,
    },
    results,
  };
}

/**
 * Full DryRun orchestrator for a hotel.
 */
export async function runFullE2eV3DryRun(opts = {}) {
  const hotelId = text(opts.hotelId);
  if (!hotelId) throw new Error("HotelId required");
  const outDir =
    opts.outDir ||
    path.join("reports", "gdi-canonical-full-e2e-v3", hotelId);
  fs.mkdirSync(outDir, { recursive: true });

  if (opts.mode === "Live" && opts.authorizePaidResearch !== true) {
    throw new Error("Live mode requires authorizePaidResearch");
  }
  if (opts.authorizePaidResearch === true && opts.mode !== "Live") {
    throw new Error("authorizePaidResearch requires Mode=Live");
  }

  const { path: ckPath, checkpoint } = loadOrCreateCheckpoint(hotelId, outDir);
  let ck = checkpoint;

  const stageReports = {};

  // 1 Discovery
  if (!opts.skipDiscovery) {
    const d = runDiscoveryStageDry(hotelId, opts);
    stageReports.discovery = d;
    ck = applyStageCheckpointResult(ck, d.stageId, {
      outcome: d.outcome,
      reportPath: path.join(outDir, "01-discovery.json"),
      metrics: d.metrics,
    });
    fs.writeFileSync(
      path.join(outDir, "01-discovery.json"),
      JSON.stringify(d, null, 2),
    );
  }

  // 2 Deep (+ commercial viability precheck at stage end)
  if (!opts.skipDeep) {
    const deep = runDeepResearchStageDry(hotelId, opts);
    stageReports.deep = deep;
    stageReports.commercialViabilityPrecheck =
      deep.commercialViabilityPrecheck || null;
    ck = applyStageCheckpointResult(ck, deep.stageId, {
      outcome: deep.outcome,
      reportPath: path.join(outDir, "02-deep-research.json"),
      metrics: deep.metrics,
    });
    fs.writeFileSync(
      path.join(outDir, "02-deep-research.json"),
      JSON.stringify(deep, null, 2),
    );
    if (deep.commercialViabilityPrecheck) {
      fs.writeFileSync(
        path.join(outDir, "02b-commercial-viability-precheck.json"),
        JSON.stringify(deep.commercialViabilityPrecheck, null, 2),
      );
    }
  }

  // 3 WHO/HOW — prioritize viability PROCEED rows when provided
  if (!opts.skipWhoHow) {
    const viabilityRows =
      stageReports.commercialViabilityPrecheck?.rows || [];
    const proceedIds = new Set(
      viabilityRows
        .filter((r) => r.ok === true)
        .map((r) => text(r.opportunityId))
        .filter(Boolean),
    );
    let whoHowFixtures = opts.whoHowFixtures || [];
    if (proceedIds.size && whoHowFixtures.length) {
      whoHowFixtures = whoHowFixtures.filter((fx) => {
        const id = text(
          fx.opportunityId ||
            fx.opportunity?.id ||
            fx.opportunity?.opportunityId ||
            fx.dossier?.opportunity?.id,
        );
        return !id || proceedIds.has(id);
      });
    }
    const who = runWhoHowStageDry(hotelId, {
      whoHowFixtures,
      continuationOnly: opts.continuationOnly === true,
    });
    stageReports.whoHow = who;
    ck = applyStageCheckpointResult(ck, who.stageId, {
      outcome: who.outcome,
      reportPath: path.join(outDir, "03-who-how.json"),
      metrics: {
        ...who.metrics,
        viabilityFiltered: proceedIds.size > 0,
        viabilityProceedCount: proceedIds.size,
      },
    });
    fs.writeFileSync(
      path.join(outDir, "03-who-how.json"),
      JSON.stringify(who, null, 2),
    );
  }

  // Opportunities for stages 4–5 from WHO results or opts
  const opps =
    opts.opportunities ||
    (stageReports.whoHow?.results || []).map((r) => r.opportunity).filter(Boolean);

  // 4 Contact
  if (!opts.skipContact) {
    const contact = await runContactResearchStageDry(opps, opts);
    stageReports.contact = contact;
    ck = applyStageCheckpointResult(ck, contact.stageId, {
      outcome: contact.outcome,
      reportPath: path.join(outDir, "04-contact-research.json"),
      metrics: contact.metrics,
    });
    fs.writeFileSync(
      path.join(outDir, "04-contact-research.json"),
      JSON.stringify(contact, null, 2),
    );
  }

  const oppsAfterContact =
    (stageReports.contact?.results || []).map((r) => r.opportunity).filter(Boolean)
      .length
      ? (stageReports.contact.results || []).map((r) => r.opportunity)
      : opps;

  // 5 Qualification
  if (!opts.skipQualification) {
    const qual = runQualificationStageDry(oppsAfterContact, opts);
    stageReports.qualification = qual;
    ck = applyStageCheckpointResult(ck, qual.stageId, {
      outcome: qual.outcome,
      reportPath: path.join(outDir, "05-qualification.json"),
      metrics: qual.metrics,
    });
    fs.writeFileSync(
      path.join(outDir, "05-qualification.json"),
      JSON.stringify(qual, null, 2),
    );
  }

  saveCheckpoint(ckPath, ck);
  const completeness = summarizeE2eV3Completeness(ck);
  const summary = {
    version: GDI_FULL_E2E_V3_ORCHESTRATOR_V1,
    stagesVersion: GDI_CANONICAL_FULL_E2E_V3_STAGES_V1,
    hotelId,
    mode: opts.mode || "DryRun",
    paid: false,
    outDir,
    checkpointPath: ckPath,
    completeness,
    existingMappings: GDI_E2E_V3_EXISTING_MAPPINGS,
    stageOrder: GDI_E2E_V3_STAGE_ORDER,
    whoHowStatuses: WHO_HOW_STATUS,
    at: new Date().toISOString(),
  };
  fs.writeFileSync(
    path.join(outDir, "00-ORCHESTRATOR_SUMMARY.json"),
    JSON.stringify(summary, null, 2),
  );
  return { summary, checkpoint: ck, stageReports };
}

export {
  GDI_E2E_V3_STAGE,
  GDI_E2E_V3_STAGE_ORDER,
  buildWhoHowAgentInputFromDossier,
};
