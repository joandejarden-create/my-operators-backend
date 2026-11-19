/**
 * Authenticated research route contracts for HOTEL_INTELLIGENCE_RESEARCH_ENGINE_V1.
 * P1: dry-run / status only — no Airtable apply, no paid providers.
 *
 * Auth: Memberstack + Dealality user + admin/workspace-admin required for initiation.
 * HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL never applies in production.
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
import { WORKSPACE_ADMIN } from "../lib/company-workspace-access.js";
import { hasLocalDealalityAdminAccess } from "../lib/dealality/local-dealality-admin-access.js";

/**
 * Production-safe environment check (never trust unauth flag in prod).
 */
export function isHeIntelProductionEnv(env = process.env) {
  const nodeEnv = String(env.NODE_ENV || "").trim().toLowerCase();
  const vercelEnv = String(env.VERCEL_ENV || "").trim().toLowerCase();
  if (nodeEnv === "production") return true;
  if (vercelEnv === "production") return true;
  return false;
}

/**
 * Whether local unauth bypass flag is honored.
 * Production: always false. Non-production: only when explicitly set.
 */
export function allowHeIntelUnauthLocal(env = process.env) {
  if (isHeIntelProductionEnv(env)) return false;
  return String(env.HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL || "").trim() === "1";
}

/**
 * Research initiation authorization.
 * Requires verified Dealality identity + admin / workspace-admin (or local admin).
 * Customer/share contexts without admin are refused.
 *
 * @returns {{ ok: true } | { ok: false, status: number, body: object }}
 */
export function assertHeIntelEngineAuth(req, env = process.env) {
  const contract = ENGINE_CONTRACT_ID;

  // Production must never honor unauth local flag
  if (isHeIntelProductionEnv(env) && String(env.HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL || "").trim() === "1") {
    // Flag present in prod is ignored for access; still require real identity below.
  }

  const user = req?.dealalityUser || null;
  if (!user) {
    if (allowHeIntelUnauthLocal(env)) {
      return { ok: true, auth_mode: "UNAUTH_LOCAL_DEV_ONLY" };
    }
    return {
      ok: false,
      status: 401,
      body: {
        ok: false,
        error: "UNAUTHORIZED",
        message: "Dealality identity required.",
        contract,
      },
    };
  }

  // Customer / share isolation: share tokens and customer-only users cannot initiate research
  if (user.isShareSession === true || user.shareToken || user.authMode === "share") {
    return {
      ok: false,
      status: 403,
      body: {
        ok: false,
        error: "FORBIDDEN_SHARE_ISOLATION",
        message: "Share sessions cannot initiate hotel intelligence research.",
        contract,
      },
    };
  }

  if (user.isCustomerOnly === true || user.customerShare === true) {
    return {
      ok: false,
      status: 403,
      body: {
        ok: false,
        error: "FORBIDDEN_CUSTOMER_ISOLATION",
        message: "Customer contexts cannot initiate hotel intelligence research.",
        contract,
      },
    };
  }

  const isAdmin = user.isAdmin === true || user.flags?.isAdmin === true;
  const workspaces = Array.isArray(user.workspaceAccess) ? user.workspaceAccess : [];
  const isWorkspaceAdmin = workspaces.includes(WORKSPACE_ADMIN);
  const localAdmin = hasLocalDealalityAdminAccess(user, req, env);

  if (isAdmin || isWorkspaceAdmin || localAdmin) {
    return {
      ok: true,
      auth_mode: isAdmin
        ? "ADMIN"
        : isWorkspaceAdmin
          ? "WORKSPACE_ADMIN"
          : "LOCAL_ADMIN",
    };
  }

  return {
    ok: false,
    status: 403,
    body: {
      ok: false,
      error: "FORBIDDEN_ADMIN",
      message: "Internal admin access required to initiate research.",
      contract,
    },
  };
}

function requireEngineAuth(req, res) {
  const gate = assertHeIntelEngineAuth(req, process.env);
  if (gate.ok) return true;
  res.status(gate.status).json(gate.body);
  return false;
}

export function getHeIntelEngineMeta(req, res) {
  if (!requireEngineAuth(req, res)) return;
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
    auth: {
      unauth_local_honored: allowHeIntelUnauthLocal(process.env),
      production_env: isHeIntelProductionEnv(process.env),
    },
  });
}

export async function postHeIntelEngineDryRun(req, res) {
  if (!requireEngineAuth(req, res)) return;
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
