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
  GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
  GDI_CANONICAL_PUBLICATION_LIFECYCLE,
  stageQualifiedInventoryFromE2e,
} from "../publication-package/gdi-canonical-e2e-to-staged-lifecycle-v1.js";
import { createGdiPublicationPackageStore } from "../publication-package/index.js";
import { describeFindymailKeyPresence } from "../providers/gdi-findymail-client-v1.js";
import { buildResearchReceiptFromInventory } from "../executive-summary/gdi-executive-summary-v1.js";
import {
  assertFindymailPersistAuthorized,
  buildFindymailPersistPatchesFromContactResults,
} from "../persistence/gdi-findymail-persist-authorization-v1.js";

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
 */
export function runDeepResearchStageDry(hotelId, opts = {}) {
  const queueDir =
    opts.queueDossierDir ||
    "reports/gdi-wyndham-commercial-full-e2e-queue-20usd-v1/dossiers";
  let dossierCount = 0;
  if (fs.existsSync(queueDir)) {
    dossierCount = fs
      .readdirSync(queueDir)
      .filter((f) => f.endsWith(".json")).length;
  }
  return {
    stageId: GDI_E2E_V3_STAGE.OPPORTUNITY_DEEP_RESEARCH,
    outcome:
      dossierCount > 0
        ? GDI_E2E_V3_STAGE_OUTCOME.PASS
        : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    mode: "DryRun",
    paid: false,
    operatingMode: DEEP_RESEARCH_OPERATING_MODE.COMMERCIAL_FULL_E2E,
    metrics: {
      hotelId,
      dossierCount,
      queueDossierDir: queueDir,
      missingBuyerRoutesToStage3: true,
      note: "DryRun does not seed Exa Agent; settled jobs reused when Live",
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

  // 2 Deep
  if (!opts.skipDeep) {
    const deep = runDeepResearchStageDry(hotelId, opts);
    stageReports.deep = deep;
    ck = applyStageCheckpointResult(ck, deep.stageId, {
      outcome: deep.outcome,
      reportPath: path.join(outDir, "02-deep-research.json"),
      metrics: deep.metrics,
    });
    fs.writeFileSync(
      path.join(outDir, "02-deep-research.json"),
      JSON.stringify(deep, null, 2),
    );
  }

  // 3 WHO/HOW
  if (!opts.skipWhoHow) {
    const who = runWhoHowStageDry(hotelId, {
      whoHowFixtures: opts.whoHowFixtures || [],
      continuationOnly: opts.continuationOnly === true,
    });
    stageReports.whoHow = who;
    ck = applyStageCheckpointResult(ck, who.stageId, {
      outcome: who.outcome,
      reportPath: path.join(outDir, "03-who-how.json"),
      metrics: who.metrics,
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

  // 6 Mandatory STAGED progression (FULL E2E → STAGED). Never CURRENT.
  // DryRun validates package construction; Live activates STAGED pointer.
  const mode = opts.mode || "DryRun";
  const { stagedProgression, stagedInventoryValidation } =
    runStagedProgressionStage({
      hotelId,
      opts,
      mode,
      outDir,
      oppsAfterContact,
      qualification: stageReports.qualification,
    });
  stageReports.stagedProgression = stagedProgression;
  stageReports.stagedInventoryValidation = stagedInventoryValidation;

  saveCheckpoint(ckPath, ck);
  const completeness = summarizeE2eV3Completeness(ck);
  const summary = {
    version: GDI_FULL_E2E_V3_ORCHESTRATOR_V1,
    stagesVersion: GDI_CANONICAL_FULL_E2E_V3_STAGES_V1,
    hotelId,
    mode,
    paid: false,
    outDir,
    checkpointPath: ckPath,
    completeness,
    lifecycle: GDI_CANONICAL_PUBLICATION_LIFECYCLE,
    stagedProgression: {
      ok: stagedProgression.ok !== false,
      activated: stagedProgression.activated === true,
      publicationVersion: stagedProgression.publicationVersion || null,
      opportunityCount: stagedProgression.opportunityCount || null,
      currentUnchanged: stagedProgression.currentUnchanged !== false,
      reason: stagedProgression.reason || null,
    },
    stagedInventoryValidation,
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

/**
 * Stage 6 — mandatory STAGED progression + local STAGED inventory validation.
 * Shared by DryRun and Live. Never touches CURRENT.
 */
function runStagedProgressionStage({
  hotelId,
  opts = {},
  mode,
  outDir,
  oppsAfterContact = [],
  qualification = null,
}) {
  const qualResults = qualification?.results || [];
  const passOpps = [];
  for (const row of qualResults) {
    if (row?.fqo?.finalizedQualified !== true) continue;
    const id = text(row.opportunityId);
    const src =
      oppsAfterContact.find(
        (o) => text(o.opportunityId || o.id || o.candidateId) === id,
      ) || { id, opportunityId: id };
    passOpps.push({ ...src, id: id || src.id, opportunityId: id || src.id });
  }
  let stagedProgression = {
    contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
    lifecycle: GDI_CANONICAL_PUBLICATION_LIFECYCLE,
    skipped: opts.skipStagedProgression === true,
    reason: null,
    ok: true,
    activated: false,
    currentUnchanged: true,
  };
  if (opts.skipStagedProgression === true) {
    stagedProgression.reason = "SKIP_STAGED_PROGRESSION";
  } else if (!passOpps.length) {
    stagedProgression.reason = "NO_FINALIZED_QUALIFIED_TO_STAGE";
    stagedProgression.ok = true;
  } else {
    const activate =
      mode === "Live" &&
      opts.activateStagedPointer !== false &&
      opts.dryRun !== true;
    stagedProgression = {
      ...stageQualifiedInventoryFromE2e({
        hotelId,
        hotelName: opts.hotelName || hotelId,
        opportunities: passOpps,
        dryRun: !activate,
        activate,
        runId: `e2e_v3_${hotelId}_${Date.now().toString(36)}`,
        notes: {
          contract: GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1,
          orchestrator: GDI_FULL_E2E_V3_ORCHESTRATOR_V1,
          mode,
          at: new Date().toISOString(),
        },
      }),
      skipped: false,
    };
  }
  // Local inventory validation (read STAGED pointer + package when present)
  const store = createGdiPublicationPackageStore({});
  const stagedPtr = store.readPointer(hotelId, "STAGED");
  const currentPtr = store.readPointer(hotelId, "CURRENT");
  const stagedInventoryValidation = {
    ok: true,
    stagedVersion: stagedPtr?.publicationVersion || null,
    currentVersion: currentPtr?.publicationVersion || null,
    opportunityCount: null,
  };
  if (stagedPtr?.publicationVersion) {
    const loaded = store.loadPackage(hotelId, stagedPtr.publicationVersion);
    stagedInventoryValidation.opportunityCount = loaded?.ok
      ? loaded.package?.opportunityCount ||
        (loaded.package?.opportunities || []).length
      : null;
    stagedInventoryValidation.ok = loaded?.ok === true;
  } else if (
    passOpps.length &&
    mode === "Live" &&
    opts.activateStagedPointer !== false &&
    opts.skipStagedProgression !== true
  ) {
    stagedInventoryValidation.ok = false;
    stagedInventoryValidation.reason = "STAGED_POINTER_MISSING_AFTER_LIVE_PROGRESSION";
  }
  fs.writeFileSync(
    path.join(outDir, "06-staged-progression.json"),
    JSON.stringify(
      { stagedProgression, stagedInventoryValidation },
      null,
      2,
    ),
  );
  return { passOpps, stagedProgression, stagedInventoryValidation };
}

// ---------------------------------------------------------------------------
// Live orchestrator (hotel-agnostic). Reuses existing adapters; no new engine.
// ---------------------------------------------------------------------------

const LIVE_WHO_HOW_ADAPTER_PATH =
  "../research/exa-agent/gdi-exa-who-how-live-adapter-v1.js";
const LIVE_PERSISTENCE_BRIDGE_PATH =
  "./gdi-full-e2e-research-persistence-bridge-v1.js";
const LEISURE_STAGE_PATH = "./gdi-canonical-leisure-opt-in-wire-v1.js";
const CAD_STAGE_PATH = "./gdi-canonical-cad-opt-in-stage-v1.js";
const FINDYMAIL_PERSIST_PATH =
  "../persistence/gdi-findymail-airtable-contact-persist-v1.js";

/** PLP is the preferred_lodging_partnerships segment of the fourteen-segment chain. */
export const PLP_SEGMENT_ID = "preferred_lodging_partnerships";

function liveError(code, message, extra = {}) {
  const err = new Error(message);
  err.code = code;
  Object.assign(err, extra);
  return err;
}

function readJsonIfExists(filePath) {
  try {
    if (!filePath || !fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function firstExisting(paths = []) {
  for (const p of paths) {
    if (p && fs.existsSync(p)) return p;
  }
  return null;
}

/**
 * Load a hotel-specific WHO/HOW tranche manifest into adapter items.
 * No hotel-specific default manifest or rationale text.
 */
export function loadWhoHowManifestItems(manifestPath, hotelId, opportunityId = null) {
  const raw = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  let opps = Array.isArray(raw.opportunities) ? raw.opportunities : [];
  if (text(opportunityId)) {
    opps = opps.filter(
      (o) => text(o.opportunityId || o.id || o.candidateId) === text(opportunityId),
    );
  }
  return opps
    .map((o) => {
      const id = text(o.opportunityId || o.id || o.candidateId);
      if (!id) return null;
      if (o.dossier && typeof o.dossier === "object") {
        return { opportunityId: id, dossier: o.dossier };
      }
      return {
        opportunityId: id,
        dossier: {
          opportunity: {
            candidateId: id,
            opportunityId: id,
            hotelId,
            organizationName: o.organizationName,
            title: o.title,
            segmentId: o.segmentId,
            competitiveClass: o.competitiveClass,
            hotelFitRationale:
              o.hotelFitRationale ||
              "Stage 3 WHO/HOW resolution for an existing research opportunity.",
          },
        },
      };
    })
    .filter(Boolean);
}

/**
 * Stage 1 Live — reuse existing discovery artifacts. Never invents paid discovery.
 * Missing artifacts → HOLD (never a silent PASS).
 */
export function runDiscoveryStageLive(hotelId, opts = {}) {
  const outDir =
    opts.outDir || path.join("reports", "gdi-canonical-full-e2e-v3", hotelId);
  const matrixPath = firstExisting([
    opts.discoveryMatrixPath,
    path.join(outDir, "fourteen-segment-discovery-matrix.json"),
    path.join(
      "reports",
      "gdi-canonical-full-e2e-v3",
      hotelId,
      "fourteen-segment-discovery-matrix.json",
    ),
  ]);
  // Sentinel path guarantees the Dry helper HOLDs instead of defaulting to
  // another hotel's artifacts.
  const dry = runDiscoveryStageDry(hotelId, {
    ...opts,
    discoveryMatrixPath:
      matrixPath || path.join(outDir, "__missing-discovery-matrix__.json"),
  });
  const plp = assessPlpFromDiscoveryMatrix(matrixPath);
  return {
    ...dry,
    mode: "Live",
    paid: false,
    reuseExistingIntelligence: true,
    reason: matrixPath ? null : "DISCOVERY_ARTIFACTS_MISSING",
    plpAssessed: plp.plpAssessed,
    metrics: {
      ...dry.metrics,
      plpAssessed: plp.plpAssessed,
      plpCellCount: plp.plpCellCount,
      plpNote: plp.note,
      note: "Live reuses existing discovery artifacts; no new paid discovery is invented",
    },
  };
}

/**
 * PLP (Preferred Lodging Partnerships) is not a separate engine: it is the
 * `preferred_lodging_partnerships` segment of the canonical discovery chain.
 * Replaying a historical matrix counts as PLP-assessed only when the matrix
 * actually contains cells for that segment.
 */
export function assessPlpFromDiscoveryMatrix(matrixPath) {
  const matrix = readJsonIfExists(matrixPath);
  const cells = Array.isArray(matrix?.cells) ? matrix.cells : [];
  const plpCells = cells.filter(
    (c) => text(c?.segmentId || c?.segment) === PLP_SEGMENT_ID,
  );
  return {
    plpAssessed: plpCells.length > 0,
    plpCellCount: plpCells.length,
    segmentId: PLP_SEGMENT_ID,
    note: plpCells.length
      ? "PLP assessed via preferred_lodging_partnerships segment cells in the reused discovery matrix"
      : "No preferred_lodging_partnerships cells in the discovery matrix (PLP not assessed by reuse)",
  };
}

/**
 * Stage 2 Live — reuse existing commercial queue dossiers. No paid deep research.
 */
export function runDeepResearchStageLive(hotelId, opts = {}) {
  const outDir =
    opts.outDir || path.join("reports", "gdi-canonical-full-e2e-v3", hotelId);
  const queueDir = firstExisting([
    opts.queueDossierDir,
    path.join(outDir, "dossiers"),
    path.join("reports", "gdi-canonical-full-e2e-v3", hotelId, "dossiers"),
  ]);
  const dry = runDeepResearchStageDry(hotelId, {
    ...opts,
    queueDossierDir: queueDir || path.join(outDir, "__missing-dossiers__"),
  });
  return {
    ...dry,
    mode: "Live",
    paid: false,
    reuseExistingIntelligence: true,
    reason: queueDir ? null : "DEEP_RESEARCH_ARTIFACTS_MISSING",
    metrics: {
      ...dry.metrics,
      note: "Live reuses settled commercial queue dossiers; no new paid deep research is seeded",
    },
  };
}

/**
 * Stage 3 Live — WHO/HOW via the existing Exa adapter (Checkpoint A pattern).
 * Fail closed on missing authorization / ceiling / manifest. Hotel-agnostic.
 */
export async function runWhoHowStageLive(hotelId, opts = {}) {
  const authorizationId = text(opts.authorizationId);
  const hardCeilingUsd = Number(opts.hardCeilingUsd);
  const manifestPath = text(opts.whoHowManifestPath || opts.manifestPath);
  if (opts.authorizePaidResearch !== true) {
    throw liveError(
      "LIVE_WHO_HOW_REQUIRES_PAID_AUTHORIZATION",
      "Live WHO/HOW requires authorizePaidResearch",
    );
  }
  if (!authorizationId) {
    throw liveError(
      "LIVE_WHO_HOW_REQUIRES_AUTHORIZATION_ID",
      "Live WHO/HOW requires authorizationId (no hotel-specific default)",
    );
  }
  if (!Number.isFinite(hardCeilingUsd) || hardCeilingUsd <= 0) {
    throw liveError(
      "LIVE_WHO_HOW_REQUIRES_HARD_CEILING",
      "Live WHO/HOW requires hardCeilingUsd > 0",
    );
  }
  if (!manifestPath) {
    throw liveError(
      "LIVE_WHO_HOW_REQUIRES_MANIFEST",
      "Live WHO/HOW requires whoHowManifestPath/manifestPath (no hotel-specific default)",
    );
  }
  const resolvedManifest = path.resolve(manifestPath);
  if (!fs.existsSync(resolvedManifest)) {
    throw liveError(
      "LIVE_WHO_HOW_MANIFEST_NOT_FOUND",
      `WHO/HOW manifest not found: ${resolvedManifest}`,
    );
  }

  const adapter = opts.whoHowAdapter || (await import(LIVE_WHO_HOW_ADAPTER_PATH));
  const authProbe = adapter.assertWhoHowLiveAuthorization({
    mode: "Live",
    authorizePaidResearch: true,
    authorizeExaAgentLive: true,
    authorizationId,
    hardCeilingUsd,
    spentUsd: 0,
  });
  if (authProbe.ok !== true) {
    throw liveError(
      "LIVE_WHO_HOW_AUTHORIZATION_REFUSED",
      `Live WHO/HOW authorization refused: ${(authProbe.errors || []).join(", ")}`,
      { authErrors: authProbe.errors || [] },
    );
  }

  const items = loadWhoHowManifestItems(
    resolvedManifest,
    hotelId,
    opts.opportunityId || null,
  );
  if (!items.length) {
    throw liveError(
      "LIVE_WHO_HOW_MANIFEST_EMPTY",
      "WHO/HOW manifest has no opportunities in scope",
    );
  }

  const reconBefore = adapter.reconcileWhoHowJobSpend({ authorizationId });
  const spentBefore = Number(reconBefore?.totalUsd || 0);
  const tranche = await adapter.executeWhoHowTranche(items, {
    mode: "Live",
    authorizePaidResearch: true,
    authorizeExaAgentLive: true,
    authorizationId,
    hardCeilingUsd,
    spentUsd: spentBefore,
    hotelId,
  });
  const results = tranche.results || [];
  const newSpendUsd = Math.max(
    0,
    Number(tranche.spentUsd || 0) - spentBefore,
  );
  const paid = newSpendUsd > 0 || results.some((r) => r.paid === true);
  const resolved = results.filter((r) => r.whoHow);
  let outcome = GDI_E2E_V3_STAGE_OUTCOME.PARTIAL;
  if (!results.length) {
    outcome = GDI_E2E_V3_STAGE_OUTCOME.HOLD;
  } else if (!tranche.blocked && resolved.length === items.length) {
    outcome = GDI_E2E_V3_STAGE_OUTCOME.PASS;
  } else if (!resolved.length) {
    outcome = GDI_E2E_V3_STAGE_OUTCOME.HOLD;
  }
  return {
    stageId: GDI_E2E_V3_STAGE.WHO_HOW_RESOLUTION,
    outcome,
    mode: "Live",
    paid,
    continuationOnly: opts.continuationOnly === true,
    authorizationId,
    hardCeilingUsd,
    spentUsd: Number(tranche.spentUsd || 0),
    newSpendUsd: Number(newSpendUsd.toFixed(6)),
    remainingUsd: tranche.remainingUsd ?? null,
    manifestPath: resolvedManifest,
    blocked: tranche.blocked || null,
    metrics: {
      hotelId,
      manifestOpportunities: items.length,
      processed: results.length,
      resolved: resolved.length,
      statuses: results.map((r) => r.whoHow?.whoHowStatus || null),
      blocked: tranche.blocked?.reason || null,
    },
    results,
  };
}

function resolveFindymailLiveMode(opts = {}) {
  const findymailLive =
    opts.authorizeFindymail === true && opts.findymailDryRun !== true;
  const allowFindymail =
    opts.allowFindymail === true || opts.authorizeFindymail === true;
  return { findymailLive, allowFindymail };
}

/**
 * Stage 4 Live — professional contact research per opportunity.
 * Public-first. Findymail only when explicitly authorized; Surfe never.
 */
export async function runContactResearchStageLive(opportunities = [], opts = {}) {
  if (opts.authorizeSurfe === true) {
    throw liveError(
      "SURFE_FORBIDDEN_IN_FULL_E2E",
      "Full E2E Stage 4 refuses automatic Surfe",
    );
  }
  const { findymailLive, allowFindymail } = resolveFindymailLiveMode(opts);
  if (opts.authorizeFindymail === true && opts.authorizePaidResearch !== true) {
    throw liveError(
      "FINDYMAIL_REQUIRES_PAID_AUTHORIZATION",
      "Live Findymail requires authorizePaidResearch",
    );
  }
  if (findymailLive && !describeFindymailKeyPresence().present && !opts.apiKey) {
    throw liveError(
      "MISSING_FINDYMAIL_API_KEY",
      "FINDYMAIL_API_KEY missing for Live Findymail Stage 4",
    );
  }
  // Persist flag is passed through only when separately authorized (default false).
  // AuthorizePaidResearch / AuthorizeFindymail alone never unlock it.
  const persistFindymailResults = opts.persistFindymailResults === true;
  if (persistFindymailResults) {
    assertFindymailPersistAuthorized(opts);
    if (opts.authorizeFindymail !== true) {
      throw liveError(
        "FINDYMAIL_PERSIST_REQUIRES_FINDYMAIL",
        "persistFindymailResults requires authorizeFindymail (nothing to persist otherwise)",
      );
    }
  }

  const creditHardCapRaw = Number(opts.findymailCreditHardCap ?? opts.creditHardCap);
  const creditHardCap =
    Number.isFinite(creditHardCapRaw) && creditHardCapRaw > 0
      ? creditHardCapRaw
      : 40;
  const findymailCreditBudget = {
    hardCap: creditHardCap,
    used: 0,
  };

  const results = [];
  for (const opp of opportunities) {
    results.push(
      await runProfessionalContactResearchStage(opp, {
        ...(opts.contactStageOpts || {}),
        mode: "Live",
        hotelId: opts.hotelId || opp?.hotelId || null,
        allowFindymail,
        ...(opts.authorizeFindymail === true ? { authorizeFindymail: true } : {}),
        findymailDryRun: !findymailLive,
        findymailPermanentComponent: true,
        persistFindymailResults,
        creditHardCap,
        findymailCreditBudget,
        authorizationId: text(opts.findymailAuthorizationId) || undefined,
        authorizeSurfe: false,
        authorizePaidPublicSearch: false,
      }),
    );
  }
  const hold = results.some(
    (r) => r.outcome === "HOLD" || r.outcome === "SKIPPED",
  );
  return {
    stageId: GDI_E2E_V3_STAGE.PROFESSIONAL_CONTACT_RESEARCH,
    outcome: results.length
      ? hold
        ? GDI_E2E_V3_STAGE_OUTCOME.PARTIAL
        : GDI_E2E_V3_STAGE_OUTCOME.PASS
      : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    mode: "Live",
    paid: findymailLive && results.some((r) => r.paid === true),
    findymail: findymailLive && results.some((r) => r.findymail === true),
    findymailAuthorized: findymailLive,
    findymailPlanningOnly: allowFindymail && !findymailLive,
    persistFindymailResults,
    findymailCreditHardCap: creditHardCap,
    findymailCreditsUsed: Number(findymailCreditBudget.used || 0),
    surfe: false,
    metrics: {
      processed: results.length,
      findymailCreditHardCap: creditHardCap,
      findymailCreditsUsed: Number(findymailCreditBudget.used || 0),
    },
    results,
  };
}

/**
 * Resolve the canonical research-spine opt-ins (Leisure + CAD).
 * Default ON for Live FULL when authorizePaidDiscovery or allowHistoricalReplay is
 * true; explicit enableLeisure / enableCad true|false always wins. Neither stage
 * can spend: leisure runs mock_live/assess-only here and CAD is pure classification.
 */
export function resolveLiveResearchSpine(opts = {}) {
  const defaultOn =
    opts.authorizePaidDiscovery === true || opts.allowHistoricalReplay === true;
  const pick = (explicit) =>
    explicit === true ? true : explicit === false ? false : defaultOn;
  return {
    defaultOn,
    leisure: pick(opts.enableLeisure),
    cad: pick(opts.enableCad),
  };
}

function slimLeisureReport(leisure) {
  if (!leisure || typeof leisure !== "object") return leisure;
  const { integration, moduleResults, ...rest } = leisure;
  return {
    ...rest,
    moduleResultCount: Array.isArray(moduleResults) ? moduleResults.length : 0,
    integrationOk: integration ? integration.ok !== false : null,
  };
}

/**
 * Canonical research spine for Live FULL: invokes the permanent Leisure and CAD
 * standard stages (no parallel engine) over the run's opportunities. $0 only.
 * Writes 01b-leisure-opt-in.json / 01c-cad-opt-in.json.
 */
export async function runCanonicalResearchSpineLive(hotelId, opts = {}, ctx = {}) {
  const outDir = opts.outDir;
  const opps = Array.isArray(ctx.opps) ? ctx.opps : [];
  const resolved = resolveLiveResearchSpine(opts);
  const out = {
    resolved,
    leisure: null,
    cad: null,
    opps,
    failed: false,
  };

  if (resolved.leisure) {
    try {
      const run =
        opts.leisureStageRunner ||
        (await import(LEISURE_STAGE_PATH)).runCanonicalLeisureOptInStage;
      const stage = await run({
        hotelId,
        hotelProfile: {
          hotelId,
          displayName: opts.hotelName || opts.hotelDisplayName || null,
          city: opts.hotelCity || null,
          ...(opts.hotelProfile && typeof opts.hotelProfile === "object"
            ? opts.hotelProfile
            : {}),
        },
        // Paid leisure research is never authorized from this orchestrator.
        authorizePaidDiscovery: opts.authorizePaidDiscovery === true,
        authorizePaidResearch: false,
        authorizePaidLeisureResearch: false,
        leisureResearchMode: "mock_live",
        leisureModules: opts.leisureModules,
        disableLeisureModules: opts.disableLeisureModules,
        existingInventory: opps,
        outDir: path.join(outDir, "leisure-standard-stage"),
        writeLeisureArtifacts: opts.writeLeisureArtifacts !== false,
      });
      out.leisure = slimLeisureReport(stage);
    } catch (err) {
      out.failed = true;
      out.leisure = {
        status: "APPLICABLE_EXECUTION_FAILED",
        enabled: true,
        ok: false,
        spentUsd: 0,
        paidCalls: 0,
        error: String(err?.message || err),
      };
    }
    out.leisure = {
      ...out.leisure,
      stageId: "LEISURE_OPT_IN",
      mode: "Live",
      paid: Number(out.leisure.spentUsd || 0) > 0,
      outcome:
        out.leisure.ok === false
          ? GDI_E2E_V3_STAGE_OUTCOME.PARTIAL
          : GDI_E2E_V3_STAGE_OUTCOME.PASS,
    };
    fs.writeFileSync(
      path.join(outDir, "01b-leisure-opt-in.json"),
      JSON.stringify(out.leisure, null, 2),
    );
  }

  if (resolved.cad) {
    try {
      const mod = opts.cadStageRunner
        ? { runCanonicalCadOptInStage: opts.cadStageRunner }
        : await import(CAD_STAGE_PATH);
      const stage = mod.runCanonicalCadOptInStage({
        hotelId,
        opportunities: opps,
        seedOpportunities: [],
      });
      out.cad = stage;
      if (
        opts.applyCadEnrichment !== false &&
        stage?.enabled === true &&
        typeof mod.applyCadEnrichmentToResearchRows === "function"
      ) {
        out.opps = mod.applyCadEnrichmentToResearchRows(opps, stage);
      }
    } catch (err) {
      out.failed = true;
      out.cad = {
        status: "FAILED",
        enabled: true,
        ok: false,
        classifiedCount: 0,
        fqoAutoPromoted: false,
        persistenceAuthorized: false,
        error: String(err?.message || err),
      };
    }
    out.cad = {
      ...out.cad,
      stageId: "CAD_OPT_IN",
      mode: "Live",
      paid: false,
      outcome:
        out.cad.ok === false
          ? GDI_E2E_V3_STAGE_OUTCOME.PARTIAL
          : GDI_E2E_V3_STAGE_OUTCOME.PASS,
    };
    fs.writeFileSync(
      path.join(outDir, "01c-cad-opt-in.json"),
      JSON.stringify(out.cad, null, 2),
    );
  }

  return out;
}

/**
 * Post-Stage-4 Findymail persistence step (hotel-agnostic). Dry-run plan by
 * default; apply only with applyFindymailPersist + authorization + env gate.
 */
export async function runFindymailPersistStageLive(hotelId, contactResults = [], opts = {}) {
  const apply = opts.applyFindymailPersist === true;
  const auth = assertFindymailPersistAuthorized(opts, { apply });
  const { patches, skipped } = buildFindymailPersistPatchesFromContactResults(contactResults);
  const persist =
    opts.findymailPersistFn ||
    (await import(FINDYMAIL_PERSIST_PATH)).persistFindymailContactsToAirtable;
  const result = await persist(patches, {
    hotelId,
    apply,
    dryRun: !apply,
    runId: `e2e_v3_${hotelId}_findymail_${Date.now().toString(36)}`,
  });
  return {
    stageId: "FINDYMAIL_PERSIST",
    mode: "Live",
    paid: false,
    outcome: GDI_E2E_V3_STAGE_OUTCOME.PASS,
    dryRun: result?.dryRun !== false,
    applied: result?.dryRun === false,
    authorizedVia: auth.via,
    candidateCount: patches.length,
    skipped,
    wouldWrite: result?.wouldWrite ?? null,
    written: result?.written ?? 0,
  };
}

/**
 * Stage inventory for the Live summary: every canonical stage, whether it was
 * executed/assessed, and whether it spent. Honest NOT_ENABLED / NOT_REQUESTED
 * states — never a silent PASS.
 */
function buildLiveStageInventory({
  stageReports = {},
  spine = {},
  plpAssessed = false,
  findymailPersist = null,
  persistence = {},
  outDir,
}) {
  const row = (report, extra = {}) =>
    report
      ? {
          executed: true,
          outcome: report.outcome || null,
          paid: report.paid === true,
          ...extra,
        }
      : { executed: false, outcome: null, paid: false, ...extra };
  const f = (name) => path.join(outDir, name);
  return {
    discovery: row(stageReports.discovery, {
      mode: "REUSE_EXISTING_ARTIFACTS",
      reportPath: f("01-discovery.json"),
    }),
    leisure: spine.resolved?.leisure
      ? row(stageReports.leisure, {
          status: stageReports.leisure?.status || null,
          reportPath: f("01b-leisure-opt-in.json"),
        })
      : { executed: false, outcome: null, paid: false, status: "NOT_ENABLED" },
    cad: spine.resolved?.cad
      ? row(stageReports.cad, {
          status: stageReports.cad?.status || null,
          reportPath: f("01c-cad-opt-in.json"),
        })
      : { executed: false, outcome: null, paid: false, status: "NOT_ENABLED" },
    plpAssessed: {
      assessed: plpAssessed,
      via: "discovery_matrix_preferred_lodging_partnerships_segment",
      paid: false,
    },
    deep: row(stageReports.deep, {
      mode: "REUSE_EXISTING_ARTIFACTS",
      reportPath: f("02-deep-research.json"),
    }),
    whoHow: row(stageReports.whoHow, { reportPath: f("03-who-how.json") }),
    contact: row(stageReports.contact, { reportPath: f("04-contact-research.json") }),
    findymail: {
      executed: stageReports.contact?.findymail === true,
      paid: stageReports.contact?.paid === true,
      authorized: stageReports.contact?.findymailAuthorized === true,
      persist: findymailPersist
        ? {
            dryRun: findymailPersist.dryRun,
            applied: findymailPersist.applied,
            reportPath: f("04b-findymail-persist.json"),
          }
        : { status: "NOT_REQUESTED" },
    },
    qualification: row(stageReports.qualification, {
      reportPath: f("05-qualification.json"),
    }),
    staged: {
      executed: Boolean(stageReports.stagedProgression),
      activated: stageReports.stagedProgression?.activated === true,
      paid: false,
      reportPath: f("06-staged-progression.json"),
    },
    persistence: {
      requested: persistence.requested === true,
      stageStatus: persistence.stageStatus || "NOT_REQUESTED",
      applied: persistence.applied === true,
      paid: false,
    },
    researchReceipt: row(stageReports.researchReceipt, {
      reportPath: f("07-research-receipt.json"),
    }),
  };
}

/**
 * Build + write 07-research-receipt.json using the canonical
 * buildResearchReceiptFromInventory. Inventory = finalized-qualified opps of this
 * run. With none, the canonical empty shell is emitted (no synthetic counts).
 */
function buildLiveResearchReceiptReport({
  hotelId,
  outDir,
  passOpps = [],
  stageReports = {},
  stagedProgression = {},
  stagedInventoryValidation = {},
  buildReceipt,
}) {
  const hasInventory = passOpps.length > 0;
  const receipt = buildReceipt({
    hotelId,
    opportunities: passOpps,
    customerAuthorizedInventory: hasInventory,
    lastVerifiedAt: new Date().toISOString(),
    publicationPointer: stagedProgression.publicationVersion
      ? {
          kind: "STAGED_CANDIDATE",
          publicationVersion: stagedProgression.publicationVersion,
        }
      : null,
  });
  const report = {
    stageId: "RESEARCH_RECEIPT",
    mode: "Live",
    paid: false,
    outcome: hasInventory
      ? GDI_E2E_V3_STAGE_OUTCOME.PASS
      : GDI_E2E_V3_STAGE_OUTCOME.HOLD,
    reason: hasInventory ? null : "NO_FINALIZED_QUALIFIED_INVENTORY_FOR_RECEIPT",
    source: "FINALIZED_QUALIFIED_STAGED_CANDIDATE_INVENTORY_INTERNAL",
    currentUnchanged: stagedProgression.currentUnchanged !== false,
    inventoryOpportunityCount: passOpps.length,
    runStageMetrics: {
      discovery: stageReports.discovery?.metrics || null,
      plpAssessed: stageReports.discovery?.plpAssessed === true,
      deep: stageReports.deep?.metrics || null,
      whoHow: stageReports.whoHow?.metrics || null,
      contact: stageReports.contact?.metrics || null,
      qualification: stageReports.qualification?.metrics || null,
      leisureStatus: stageReports.leisure?.status || null,
      cadStatus: stageReports.cad?.status || null,
      stagedVersion: stagedInventoryValidation.stagedVersion || null,
    },
    receipt,
  };
  fs.writeFileSync(
    path.join(outDir, "07-research-receipt.json"),
    JSON.stringify(report, null, 2),
  );
  return report;
}

function loadPriorLiveStageReport(outDir, fileName) {
  const report = readJsonIfExists(path.join(outDir, fileName));
  return report?.mode === "Live" ? report : null;
}

function assertLiveFullE2eRequest(opts = {}) {
  const hotelId = text(opts.hotelId);
  if (!hotelId) throw liveError("HOTEL_ID_REQUIRED", "HotelId required");
  if (opts.mode !== "Live") {
    throw liveError("LIVE_REQUIRES_MODE_LIVE", "runFullE2eV3Live requires mode=Live");
  }
  if (opts.authorizePaidResearch !== true) {
    throw liveError(
      "LIVE_REQUIRES_PAID_AUTHORIZATION",
      "Live mode requires authorizePaidResearch",
    );
  }
  if (opts.authorizeSurfe === true) {
    throw liveError(
      "SURFE_FORBIDDEN_IN_FULL_E2E",
      "Full E2E refuses automatic Surfe",
    );
  }
  if (opts.authorizeFindymail === true && opts.skipContact === true) {
    throw liveError(
      "FINDYMAIL_REQUIRES_CONTACT_STAGE",
      "authorizeFindymail requires Stage 4 (skipContact must be false)",
    );
  }
  if (
    opts.applyResearchPersistence === true &&
    (opts.authorizeResearchPersistence !== true ||
      !text(opts.exactWriteSetAuthorization))
  ) {
    throw liveError(
      "RESEARCH_PERSISTENCE_APPLY_REQUIRES_AUTH",
      "applyResearchPersistence requires authorizeResearchPersistence + exactWriteSetAuthorization (paid research authorization never unlocks Airtable writes)",
    );
  }
  if (opts.persistFindymailResults === true) {
    if (opts.authorizeFindymail !== true) {
      throw liveError(
        "FINDYMAIL_PERSIST_REQUIRES_FINDYMAIL",
        "persistFindymailResults requires authorizeFindymail",
      );
    }
    if (opts.skipContact === true) {
      throw liveError(
        "FINDYMAIL_PERSIST_REQUIRES_CONTACT_STAGE",
        "persistFindymailResults requires Stage 4 (skipContact must be false)",
      );
    }
    // Separate authorization only — paid research authorization never unlocks it.
    assertFindymailPersistAuthorized(opts, {
      apply: opts.applyFindymailPersist === true,
    });
  }
  if (opts.applyFindymailPersist === true && opts.persistFindymailResults !== true) {
    throw liveError(
      "FINDYMAIL_PERSIST_APPLY_REQUIRES_PERSIST",
      "applyFindymailPersist requires persistFindymailResults",
    );
  }
  if (opts.skipWhoHow !== true) {
    if (!text(opts.authorizationId)) {
      throw liveError(
        "LIVE_WHO_HOW_REQUIRES_AUTHORIZATION_ID",
        "Live Full E2E requires authorizationId unless skipWhoHow is set (no hotel-specific default)",
      );
    }
    if (!(Number(opts.hardCeilingUsd) > 0)) {
      throw liveError(
        "LIVE_WHO_HOW_REQUIRES_HARD_CEILING",
        "Live Full E2E requires hardCeilingUsd > 0 unless skipWhoHow is set",
      );
    }
    if (!text(opts.whoHowManifestPath || opts.manifestPath)) {
      throw liveError(
        "LIVE_WHO_HOW_REQUIRES_MANIFEST",
        "Live Full E2E requires whoHowManifestPath/manifestPath unless skipWhoHow is set (no hotel-specific default)",
      );
    }
  }
  if (
    opts.authorizeFindymail === true &&
    opts.findymailDryRun !== true &&
    !describeFindymailKeyPresence().present &&
    !opts.apiKey
  ) {
    throw liveError(
      "MISSING_FINDYMAIL_API_KEY",
      "FINDYMAIL_API_KEY missing for Live Findymail Stage 4",
    );
  }
  return hotelId;
}

/**
 * Live Full E2E orchestrator. Requires mode=Live + authorizePaidResearch.
 * Spend occurs only for WHO/HOW (Exa adapter) and Findymail when explicitly
 * authorized. Discovery / Deep reuse existing artifacts. Airtable research
 * persistence needs its own flags. CURRENT is never touched.
 */
export async function runFullE2eV3Live(opts = {}) {
  // Fail closed before any filesystem or provider side effect.
  const hotelId = assertLiveFullE2eRequest(opts);

  const outDir =
    opts.outDir ||
    path.join("reports", "gdi-canonical-full-e2e-v3", hotelId);
  fs.mkdirSync(outDir, { recursive: true });
  const liveOpts = { ...opts, hotelId, outDir, mode: "Live" };

  const { path: ckPath, checkpoint } = loadOrCreateCheckpoint(hotelId, outDir);
  let ck = checkpoint;
  const stageReports = {};
  const resume = opts.resume === true;

  const record = (key, fileName, report) => {
    stageReports[key] = report;
    ck = applyStageCheckpointResult(ck, report.stageId, {
      outcome: report.outcome,
      reportPath: path.join(outDir, fileName),
      metrics: report.metrics,
    });
    fs.writeFileSync(
      path.join(outDir, fileName),
      JSON.stringify(report, null, 2),
    );
  };

  // 1 Discovery (reuse existing artifacts)
  if (!opts.skipDiscovery) {
    const prior = resume ? loadPriorLiveStageReport(outDir, "01-discovery.json") : null;
    if (prior?.outcome === GDI_E2E_V3_STAGE_OUTCOME.PASS) {
      stageReports.discovery = { ...prior, resumed: true };
    } else {
      record("discovery", "01-discovery.json", runDiscoveryStageLive(hotelId, liveOpts));
    }
  }

  // 2 Deep research (reuse existing artifacts)
  if (!opts.skipDeep) {
    const prior = resume ? loadPriorLiveStageReport(outDir, "02-deep-research.json") : null;
    if (prior?.outcome === GDI_E2E_V3_STAGE_OUTCOME.PASS) {
      stageReports.deep = { ...prior, resumed: true };
    } else {
      record("deep", "02-deep-research.json", runDeepResearchStageLive(hotelId, liveOpts));
    }
  }

  // 3 WHO/HOW (existing Exa adapter, Checkpoint A pattern)
  if (!opts.skipWhoHow) {
    const prior = resume ? loadPriorLiveStageReport(outDir, "03-who-how.json") : null;
    if (prior?.outcome === GDI_E2E_V3_STAGE_OUTCOME.PASS) {
      stageReports.whoHow = { ...prior, resumed: true };
    } else {
      record("whoHow", "03-who-how.json", await runWhoHowStageLive(hotelId, liveOpts));
    }
  }

  // Opportunities for stages 4–5
  let opps = [];
  if (Array.isArray(opts.opportunities) && opts.opportunities.length) {
    opps = opts.opportunities;
  } else if (stageReports.whoHow?.results?.length) {
    opps = stageReports.whoHow.results.map((r) => r.opportunity).filter(Boolean);
  } else if (opts.skipWhoHow && (resume || opts.continuationOnly === true)) {
    const prior = loadPriorLiveStageReport(outDir, "03-who-how.json");
    opps = (prior?.results || []).map((r) => r.opportunity).filter(Boolean);
  }
  opps = opps.map((o) => ({ ...o, hotelId: text(o.hotelId) || hotelId }));

  // 3b Canonical research spine: Leisure + CAD standard stages ($0; no paid spend).
  // PLP is the preferred_lodging_partnerships segment of the discovery matrix
  // (see stageReports.discovery.plpAssessed), not a separate engine.
  const spine = await runCanonicalResearchSpineLive(
    hotelId,
    liveOpts,
    { opps },
  );
  stageReports.leisure = spine.leisure;
  stageReports.cad = spine.cad;
  opps = spine.opps;

  // 4 Contact (public-first; Findymail only when authorized; Surfe never)
  let contactPaid = false;
  if (!opts.skipContact) {
    const contact = await runContactResearchStageLive(opps, liveOpts);
    contactPaid = contact.paid === true;
    record("contact", "04-contact-research.json", contact);
  }

  // 4b Findymail persistence — only when persistFindymailResults is separately
  // authorized. Dry-run plan unless applyFindymailPersist (+ env gate).
  let findymailPersist = null;
  if (opts.persistFindymailResults === true && stageReports.contact) {
    findymailPersist = await runFindymailPersistStageLive(
      hotelId,
      stageReports.contact.results || [],
      liveOpts,
    );
    stageReports.findymailPersist = findymailPersist;
    fs.writeFileSync(
      path.join(outDir, "04b-findymail-persist.json"),
      JSON.stringify(findymailPersist, null, 2),
    );
  }

  const contactOpps = (stageReports.contact?.results || [])
    .map((r) => r.opportunity)
    .filter(Boolean);
  const oppsAfterContact = contactOpps.length ? contactOpps : opps;

  // 5 Qualification
  if (!opts.skipQualification) {
    const qual = runQualificationStageDry(oppsAfterContact, opts);
    record("qualification", "05-qualification.json", {
      ...qual,
      mode: "Live",
      paid: false,
    });
  }

  // 6 STAGED progression (Live activates STAGED unless skipped). Never CURRENT.
  const { passOpps, stagedProgression, stagedInventoryValidation } =
    runStagedProgressionStage({
      hotelId,
      opts: liveOpts,
      mode: "Live",
      outDir,
      oppsAfterContact,
      qualification: stageReports.qualification,
    });
  stageReports.stagedProgression = stagedProgression;
  stageReports.stagedInventoryValidation = stagedInventoryValidation;

  // 7 Research Receipt — derived from the finalized-qualified STAGED candidate
  // inventory of THIS run (internal artifact; never CURRENT / customer-published).
  const researchReceiptReport = buildLiveResearchReceiptReport({
    hotelId,
    outDir,
    passOpps,
    stageReports,
    stagedProgression,
    stagedInventoryValidation,
    buildReceipt: opts.buildResearchReceipt || buildResearchReceiptFromInventory,
  });
  stageReports.researchReceipt = researchReceiptReport;

  // Optional Airtable research persistence — separate authorization only.
  let persistence = {
    requested: false,
    stageStatus: "NOT_REQUESTED",
    applied: false,
  };
  if (
    opts.planResearchPersistence === true ||
    opts.authorizeResearchPersistence === true
  ) {
    const bridge = opts.persistenceBridge || (await import(LIVE_PERSISTENCE_BRIDGE_PATH));
    const outcome = await bridge.persistFullE2eResearchInventory({
      hotelId,
      runId: `e2e_v3_${hotelId}_${Date.now().toString(36)}`,
      repoRoot: opts.repoRoot || process.cwd(),
      opportunities: oppsAfterContact.map((o) => ({
        ...o,
        opportunityId: text(o.opportunityId || o.id || o.candidateId),
      })),
      authorizeResearchPersistence: opts.authorizeResearchPersistence === true,
      exactWriteSetAuthorization: opts.exactWriteSetAuthorization,
      apply: opts.applyResearchPersistence === true,
      // Paid research authorization must never unlock Airtable writes.
      authorizePaidResearch: false,
      researchCompleted: false,
    });
    const ledgerPath = bridge.writeResearchPersistenceLedger?.(outDir, outcome) || null;
    persistence = {
      requested: true,
      stageStatus: outcome.dryRun === false ? "APPLIED" : "DRY_RUN",
      applied: outcome.dryRun === false,
      blockedApply: outcome.blockedApply === true,
      code: outcome.code || null,
      productionUnlocked: outcome.authorization?.productionUnlocked === true,
      summary: bridge.buildResearchPersistenceExecutionSummary?.(outcome) || null,
      ledgerPath,
    };
  }

  saveCheckpoint(ckPath, ck);
  const completeness = summarizeE2eV3Completeness(ck);
  const whoHowPaid = stageReports.whoHow?.paid === true;
  const leisurePaid = stageReports.leisure?.paid === true;
  const plpAssessed = stageReports.discovery?.plpAssessed === true;
  const stageInventory = buildLiveStageInventory({
    stageReports,
    spine,
    plpAssessed,
    findymailPersist,
    persistence,
    outDir,
  });
  const summary = {
    version: GDI_FULL_E2E_V3_ORCHESTRATOR_V1,
    stagesVersion: GDI_CANONICAL_FULL_E2E_V3_STAGES_V1,
    hotelId,
    mode: "Live",
    paid: whoHowPaid || contactPaid || leisurePaid,
    paidBreakdown: {
      discovery: false,
      leisure: leisurePaid,
      cad: false,
      plp: false,
      deep: false,
      deepResearch: false,
      whoHow: whoHowPaid,
      contact: contactPaid,
      findymail: contactPaid,
      surfe: false,
      qualification: false,
      staged: false,
      persistence: false,
      researchReceipt: false,
    },
    plpAssessed,
    paidDiscoveryAuthorized: opts.authorizePaidDiscovery === true,
    // Orchestrator reuses discovery artifacts; paid discovery runs only via the
    // canonical entry (gdi:canonical-full-e2e) — never invented here.
    paidDiscoveryInvoked: false,
    researchSpine: {
      defaultOn: spine.resolved.defaultOn,
      leisureEnabled: spine.resolved.leisure,
      cadEnabled: spine.resolved.cad,
      failed: spine.failed === true,
    },
    stageInventory,
    findymailPersist: findymailPersist
      ? {
          dryRun: findymailPersist.dryRun,
          applied: findymailPersist.applied,
          candidateCount: findymailPersist.candidateCount,
          authorizedVia: findymailPersist.authorizedVia,
        }
      : { requested: false, applied: false },
    researchReceipt: {
      outcome: researchReceiptReport.outcome,
      reason: researchReceiptReport.reason,
      inventoryOpportunityCount: researchReceiptReport.inventoryOpportunityCount,
      path: path.join(outDir, "07-research-receipt.json"),
      currentUnchanged: researchReceiptReport.currentUnchanged,
    },
    reuseExistingIntelligence: true,
    whoHowSpentUsd: stageReports.whoHow?.newSpendUsd ?? 0,
    outDir,
    checkpointPath: ckPath,
    completeness,
    lifecycle: GDI_CANONICAL_PUBLICATION_LIFECYCLE,
    stagedProgression: {
      ok: stagedProgression.ok !== false,
      activated: stagedProgression.activated === true,
      publicationVersion: stagedProgression.publicationVersion || null,
      opportunityCount: stagedProgression.opportunityCount || null,
      currentUnchanged: stagedProgression.currentUnchanged !== false,
      reason: stagedProgression.reason || null,
    },
    stagedInventoryValidation,
    researchPersistence: persistence,
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

/**
 * Read-only validation of existing V3 artifacts + local STAGED inventory.
 * No stage execution, no pointer writes, no paid calls, no checkpoint writes.
 */
export function validateFullE2eV3LiveArtifacts(opts = {}) {
  const hotelId = text(opts.hotelId);
  if (!hotelId) throw liveError("HOTEL_ID_REQUIRED", "HotelId required");
  const outDir =
    opts.outDir || path.join("reports", "gdi-canonical-full-e2e-v3", hotelId);
  const ckPath = resolveV3CheckpointPath(hotelId, outDir);
  const checkpoint = readJsonIfExists(ckPath);
  const completeness = checkpoint
    ? summarizeE2eV3Completeness(checkpoint)
    : null;
  const store = createGdiPublicationPackageStore({});
  const stagedPtr = store.readPointer(hotelId, "STAGED");
  const currentPtr = store.readPointer(hotelId, "CURRENT");
  let stagedOk = null;
  let opportunityCount = null;
  if (stagedPtr?.publicationVersion) {
    const loaded = store.loadPackage(hotelId, stagedPtr.publicationVersion);
    stagedOk = loaded?.ok === true;
    opportunityCount = loaded?.ok
      ? loaded.package?.opportunityCount ||
        (loaded.package?.opportunities || []).length
      : null;
  }
  const summary = readJsonIfExists(path.join(outDir, "00-ORCHESTRATOR_SUMMARY.json"));
  const reasons = [];
  if (!checkpoint) reasons.push("CHECKPOINT_MISSING");
  if (completeness?.failed) reasons.push("STAGE_FAILED");
  if (stagedPtr?.publicationVersion && stagedOk !== true) {
    reasons.push("STAGED_PACKAGE_INVALID");
  }
  return {
    ok: reasons.length === 0,
    validationOnly: true,
    paid: false,
    activated: false,
    hotelId,
    outDir,
    reasons,
    completeness,
    lastRunMode: summary?.mode || null,
    stagedVersion: stagedPtr?.publicationVersion || null,
    stagedOpportunityCount: opportunityCount,
    currentVersion: currentPtr?.publicationVersion || null,
  };
}

export {
  GDI_E2E_V3_STAGE,
  GDI_E2E_V3_STAGE_ORDER,
  buildWhoHowAgentInputFromDossier,
};
