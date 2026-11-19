/**
 * Hotel Explorer local API target guard.
 *
 * HE research/audit scripts must hit the HE worktree server (typically :8091
 * with DEALALITY_ALLOW_WORKTREE_SERVER=1), not Integration/ADP on :8080.
 * Contact Intelligence writes follow the serving checkout's data root — wrong
 * BASE URL silently pollutes another registered workspace.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadWorktreeRegistry,
  findWorkspaceByPath,
} from "../../lib/dealality-dev-os/worktree-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const HE_SCRIPTS_REPO_ROOT = path.resolve(__dirname, "../..");

/** Default HE isolated port (8080 reserved for Integration canonical checkout). */
export const HE_DEFAULT_LOCAL_BASE_URL = "http://127.0.0.1:8091";

const HE_BASENAME_ALLOW = new Set(["ws-hotel-explorer"]);

function argValue(argv, flag, fallback = "") {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] || fallback : fallback;
}

export function resolveHeAuditBaseUrl(argv = process.argv, env = process.env) {
  const fromArg = argValue(argv, "--base-url", "");
  if (fromArg) return String(fromArg).replace(/\/$/, "");
  if (env.HE_LOCAL_BASE_URL) return String(env.HE_LOCAL_BASE_URL).replace(/\/$/, "");
  if (env.HOTEL_EXPLORER_BASE_URL) {
    return String(env.HOTEL_EXPLORER_BASE_URL).replace(/\/$/, "");
  }
  return HE_DEFAULT_LOCAL_BASE_URL;
}

/**
 * Fail-closed: require runtime-identity servingDirectoryBasename to be HE
 * (or expectedStream=he when contactIntelligence.servingStream is present).
 */
export async function assertHeLocalRuntimeTarget({
  baseUrl,
  fetchImpl = globalThis.fetch,
  expectedBasenames = HE_BASENAME_ALLOW,
  expectedStream = "he",
  allowSkip = false,
} = {}) {
  const base = String(baseUrl || "").replace(/\/$/, "");
  if (!base) {
    return { ok: false, error: "he_base_url_required" };
  }

  let identity;
  try {
    const res = await fetchImpl(`${base}/api/dev/runtime-identity`);
    if (!res.ok) {
      return {
        ok: false,
        error: "runtime_identity_http_failed",
        status: res.status,
        baseUrl: base,
        hint:
          "Start the Hotel Explorer server from ws-hotel-explorer on an isolated port " +
          `(e.g. PORT=8091 DEALALITY_ALLOW_WORKTREE_SERVER=1). Default audit BASE is ${HE_DEFAULT_LOCAL_BASE_URL}.`,
      };
    }
    identity = await res.json();
  } catch (err) {
    return {
      ok: false,
      error: "runtime_identity_unreachable",
      baseUrl: base,
      detail: String(err?.message || err).slice(0, 200),
      hint:
        `Nothing listening at ${base}. Start HE with PORT=8091 DEALALITY_ALLOW_WORKTREE_SERVER=1 ` +
        "from C:/Dev/dealality/ws-hotel-explorer (do not use Integration/ADP :8080 for HE audits).",
    };
  }

  const basename = String(identity?.servingDirectoryBasename || "");
  const servingStream = identity?.contactIntelligence?.servingStream || null;
  const basenameOk = expectedBasenames.has(basename);
  const streamOk = servingStream ? servingStream === expectedStream : basenameOk;

  if (!basenameOk && !streamOk) {
    return {
      ok: false,
      error: "foreign_runtime_target",
      baseUrl: base,
      servingDirectoryBasename: basename || null,
      servingStream,
      expectedBasenames: [...expectedBasenames],
      expectedStream,
      hint:
        "HE audit targeted a non-HE server. Refusing to continue so Contact Intelligence " +
        "runtime data is not written into another registered workspace (e.g. ws-adp).",
      identity,
    };
  }

  const affinityOk = identity?.contactIntelligence?.affinityOk;
  if (affinityOk === false && !allowSkip) {
    return {
      ok: false,
      error: "contact_intelligence_affinity_failed",
      baseUrl: base,
      affinityError: identity?.contactIntelligence?.affinityError || null,
      identity,
    };
  }

  let scriptStream = null;
  try {
    const registry = loadWorktreeRegistry(HE_SCRIPTS_REPO_ROOT);
    const ws = findWorkspaceByPath(registry, HE_SCRIPTS_REPO_ROOT);
    scriptStream = ws?.stream || null;
  } catch (_err) {
    /* optional */
  }

  return {
    ok: true,
    baseUrl: base,
    servingDirectoryBasename: basename,
    servingStream: servingStream || (basenameOk ? expectedStream : null),
    scriptStream,
    contactIntelligence: identity?.contactIntelligence || null,
    identity,
  };
}
