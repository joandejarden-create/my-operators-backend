/**
 * Authenticated research route contracts for HOTEL_INTELLIGENCE_RESEARCH_ENGINE_V1.
 * P1: dry-run / status only — no Airtable apply, no paid providers.
 *
 * Mount (optional until Integration wires auth middleware):
 *   import { getHeIntelEngineMeta, postHeIntelEngineDryRun } from "./api/hotel-intelligence-research-engine-v1.js";
 */

import {
  ENGINE_CONTRACT_ID,
  ENGINE_CONTRACT_VERSION,
  ENGINE_STAGE_GRAPH,
  P1_POLICY,
  writeGuaranteesP1,
  loadPilotManifest,
  listCanonicalPilotHpcIds,
  runHotelIntelligenceResearchEngine,
  APPLY_MODE,
  KGPV_HPC_ID,
} from "../lib/hotel-intelligence/research-engine-v1/index.js";

function requireAuth(req, res) {
  // Mirror other HI routes: Memberstack / Dealality user must be present when mounted.
  if (req.dealalityUser || req.user || req.member) return true;
  if (String(process.env.HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL || "").trim() === "1") {
    return true;
  }
  res.status(401).json({
    ok: false,
    error: "UNAUTHORIZED",
    contract: ENGINE_CONTRACT_ID,
  });
  return false;
}

export function getHeIntelEngineMeta(req, res) {
  if (!requireAuth(req, res)) return;
  const manifest = loadPilotManifest();
  res.json({
    ok: true,
    contract: ENGINE_CONTRACT_ID,
    contract_version: ENGINE_CONTRACT_VERSION,
    stage_graph: ENGINE_STAGE_GRAPH,
    policy: P1_POLICY,
    write_guarantees: writeGuaranteesP1(),
    pilot: {
      manifestId: manifest.manifestId,
      founderPropertyCount: manifest.founderPropertyCount,
      hpcDistinctRecordCount: manifest.hpcDistinctRecordCount,
      canonicalHpcIds: listCanonicalPilotHpcIds(manifest),
      goldenDemoExcluded: [KGPV_HPC_ID],
    },
    apply_modes: Object.values(APPLY_MODE),
    default_apply_mode: APPLY_MODE.DRY_RUN,
  });
}

export async function postHeIntelEngineDryRun(req, res) {
  if (!requireAuth(req, res)) return;
  try {
    const body = req.body || {};
    if (body.apply_mode === APPLY_MODE.APPLY || body.apply === true) {
      res.status(403).json({
        ok: false,
        error: "APPLY_DISABLED",
        message: "Airtable apply disabled in P1",
        write_guarantees: writeGuaranteesP1(),
      });
      return;
    }
    const result = await runHotelIntelligenceResearchEngine({
      hpcId: body.hpcId || body.hpc_id || body.hotel_id,
      apply_mode: APPLY_MODE.DRY_RUN,
      use_live_workflow: false,
      resume_case_id: body.resume_case_id || null,
      probe_paid_provider: body.probe_paid_provider || null,
    });
    res.status(result.ok ? 200 : 400).json(result);
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: "ENGINE_ERROR",
      message: String(err?.message || err),
      contract: ENGINE_CONTRACT_ID,
      write_guarantees: writeGuaranteesP1(),
    });
  }
}
