/**
 * Operator Fit — internal pilot access (server-controlled).
 * Global OWNER kill switch remains OPERATOR_FIT_ENGINE_V2 (default off).
 * Internal pilot requires OPERATOR_FIT_INTERNAL_PILOT=1 + admin + deal allowlist.
 */

import { isInternalRunbookAdmin } from "../dealality/internal-runbook-admin.js";
import {
  isOperatorFitEngineV2Enabled,
  OPERATOR_FIT_ENGINE_VERSION,
} from "./feature-flag.js";

export const OPERATOR_FIT_INTERNAL_PILOT_ENV = "OPERATOR_FIT_INTERNAL_PILOT";
export const OPERATOR_FIT_PILOT_DEAL_ALLOWLIST_ENV = "OPERATOR_FIT_PILOT_DEAL_ALLOWLIST";

function truthy(raw) {
  const v = String(raw || "0").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

export function isOperatorFitInternalPilotEnabled(env = process.env) {
  return truthy(env[OPERATOR_FIT_INTERNAL_PILOT_ENV]);
}

/**
 * Comma-separated deal IDs (Airtable rec… or redacted labels like deal_deal_a).
 */
export function getOperatorFitPilotDealAllowlist(env = process.env) {
  const raw = String(env[OPERATOR_FIT_PILOT_DEAL_ALLOWLIST_ENV] || "").trim();
  if (!raw) return [];
  return raw
    .split(/[,;\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export function isPilotDealAllowed(dealId, env = process.env) {
  const id = String(dealId || "").trim();
  if (!id) return false;
  const list = getOperatorFitPilotDealAllowlist(env);
  if (!list.length) return false;
  return list.some((x) => x === id || x.toLowerCase() === id.toLowerCase());
}

/**
 * Deterministic access evaluation for internal pilot APIs/UI.
 * Never trust client-only flags.
 */
export function evaluateOperatorFitInternalPilotAccess({
  env = process.env,
  user = null,
  dealId = null,
  requireDeal = true,
} = {}) {
  const reasons = [];
  const globalOwnerEnabled = isOperatorFitEngineV2Enabled(env);
  const internalPilot = isOperatorFitInternalPilotEnabled(env);
  const nodeEnv = String(env.NODE_ENV || "").toLowerCase();
  const allowProd = truthy(env.OPERATOR_FIT_INTERNAL_PILOT_ALLOW_PRODUCTION);

  if (!internalPilot) {
    reasons.push("OPERATOR_FIT_INTERNAL_PILOT is off");
    return {
      allowed: false,
      reasons,
      engineVersion: OPERATOR_FIT_ENGINE_VERSION,
      globalOwnerEnabled,
      internalPilot,
    };
  }

  if (nodeEnv === "production" && !allowProd) {
    reasons.push("Internal pilot blocked in production without OPERATOR_FIT_INTERNAL_PILOT_ALLOW_PRODUCTION=1");
    return {
      allowed: false,
      reasons,
      engineVersion: OPERATOR_FIT_ENGINE_VERSION,
      globalOwnerEnabled,
      internalPilot,
    };
  }

  const admin = isInternalRunbookAdmin({
    email: user?.email,
    dealality: {
      isAdmin: user?.isAdmin,
      flags: user?.flags,
    },
    companyName: user?.companyName,
  });
  if (!admin) {
    reasons.push("Internal/admin runbook role required");
    return {
      allowed: false,
      reasons,
      engineVersion: OPERATOR_FIT_ENGINE_VERSION,
      globalOwnerEnabled,
      internalPilot,
    };
  }

  if (requireDeal) {
    if (!isPilotDealAllowed(dealId, env)) {
      reasons.push("Deal not on OPERATOR_FIT_PILOT_DEAL_ALLOWLIST");
      return {
        allowed: false,
        reasons,
        engineVersion: OPERATOR_FIT_ENGINE_VERSION,
        globalOwnerEnabled,
        internalPilot,
      };
    }
  }

  return {
    allowed: true,
    reasons: ["internal_pilot_ok"],
    engineVersion: OPERATOR_FIT_ENGINE_VERSION,
    globalOwnerEnabled,
    internalPilot,
    /** Owner My Deals must remain gated by global flag separately */
    ownerMyDealsWouldExpose: globalOwnerEnabled,
  };
}

export function getOperatorFitInternalPilotFlagState(env = process.env) {
  return {
    internalPilotEnabled: isOperatorFitInternalPilotEnabled(env),
    dealAllowlist: getOperatorFitPilotDealAllowlist(env),
    globalOwnerEnabled: isOperatorFitEngineV2Enabled(env),
    engineVersion: OPERATOR_FIT_ENGINE_VERSION,
    defaultOwnerOff: true,
    clientSideInsufficient: true,
  };
}
