/**
 * Local-only exact Memberstack member-ID → Dealality Demo email binding.
 *
 * Used when JWT verifies (JWKS) but carries no email claim and Memberstack Admin
 * API email enrichment is unavailable. Does NOT trust browser headers or client email.
 *
 * Enable only via gitignored local config:
 *   DEALALITY_LOCAL_DEMO_MEMBERSTACK_ID_BINDING=1
 *   DEALALITY_LOCAL_DEMO_MEMBERSTACK_IDS=mem_sb_…[,mem_sb_…]
 *
 * Production: always disabled. Never elevates from mem_sb_ prefix alone.
 */

import { isLocalDevRequest } from "./local-dealality-admin-access.js";
import { APPROVED_LOCAL_DEMO_EMAIL } from "./local-demo-dealality-user.js";

export const DEALALITY_LOCAL_DEMO_MEMBERSTACK_ID_BINDING =
  "DEALALITY_LOCAL_DEMO_MEMBERSTACK_ID_BINDING";

function parseIdList(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim())
    .filter((id) => id.startsWith("mem_sb_") || id.startsWith("mem_"));
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {boolean}
 */
export function isLocalDemoMemberstackIdBindingEnabled(env = process.env) {
  if (String(env.NODE_ENV || "").toLowerCase() === "production") return false;
  return String(env.DEALALITY_LOCAL_DEMO_MEMBERSTACK_ID_BINDING || "").trim() === "1";
}

/**
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {Set<string>}
 */
export function loadLocalDemoMemberstackIdBindings(env = process.env) {
  if (!isLocalDemoMemberstackIdBindingEnabled(env)) return new Set();
  return new Set(parseIdList(env.DEALALITY_LOCAL_DEMO_MEMBERSTACK_IDS));
}

/**
 * After JWT signature/issuer/audience/exp verification only: if the verified
 * member id is an explicitly configured local Demo sandbox/live id, return the
 * canonical Demo email for finalize / Users resolution.
 *
 * @param {string|null|undefined} verifiedMemberstackId
 * @param {object} [req]
 * @param {NodeJS.ProcessEnv} [env]
 * @returns {string|null}
 */
export function resolveLocalDemoEmailForVerifiedMemberstackId(
  verifiedMemberstackId,
  req = {},
  env = process.env
) {
  if (!isLocalDemoMemberstackIdBindingEnabled(env)) return null;
  if (!isLocalDevRequest(req, env)) return null;
  const id = String(verifiedMemberstackId || "").trim();
  if (!id || id === "dev_bypass") return null;
  // Exact id only — never prefix match.
  if (!loadLocalDemoMemberstackIdBindings(env).has(id)) return null;
  return APPROVED_LOCAL_DEMO_EMAIL;
}
