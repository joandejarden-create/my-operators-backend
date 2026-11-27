/**
 * Canonical localhost:8080 ownership — fail-closed workspace identity.
 *
 * Port 8080 is exclusively reserved for the cumulative integrated founder
 * review environment. Env flags alone (DEALALITY_CANONICAL_CHECKOUT,
 * DEALALITY_ALLOW_WORKTREE_SERVER) MUST NOT authorize :8080.
 *
 * Authorized workspace paths (exact match after normalizePath):
 *   - Registered GDI home (ws-gdi) — primary cumulative founder review
 *   - Registered Integration home (my-operators-backend) — only when
 *     DEALALITY_8080_OWNER=integration (post-merge IC serve)
 *
 * Feature / temporary / foreign worktrees must use assigned non-8080 ports.
 *
 * Contract: DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  findWorkspaceByPath,
  findWorkspaceByStream,
  loadWorktreeRegistry,
  normalizePath,
} from "./worktree-registry.js";

export const DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1 =
  "DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1";

export const CANONICAL_FOUNDER_REVIEW_PORT = 8080;

/** Assigned development ports for product streams (non-canonical). */
export const STREAM_DEV_PORTS = Object.freeze({
  gdi: 8095,
  he: 8091,
  adp: 8092,
  be: 8093,
  integration_feature: 8094,
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT_FROM_MODULE = path.resolve(__dirname, "../..");

function text(v) {
  return String(v == null ? "" : v).trim();
}

function norm(p) {
  return normalizePath(path.resolve(String(p || "")));
}

/**
 * Absolute durable GDI publication-package root (bundled store under registered GDI home).
 * STAGED / CURRENT pointers for founder review resolve here so inventory does not
 * disappear when a foreign worktree briefly binds another port.
 */
export function resolveDurableGdiPublicationPackageRoot(options = {}) {
  const env = options.env || process.env;
  const explicit = text(env.GDI_PUBLICATION_PACKAGE_ROOT);
  if (explicit) {
    const abs = path.resolve(explicit);
    if (!fs.existsSync(abs)) {
      return {
        ok: false,
        error: "GDI_PUBLICATION_PACKAGE_ROOT_MISSING",
        root: abs,
      };
    }
    return { ok: true, root: abs, source: "env:GDI_PUBLICATION_PACKAGE_ROOT" };
  }

  let registry;
  try {
    registry = options.registry || loadWorktreeRegistry(options.registryRoot || REPO_ROOT_FROM_MODULE);
  } catch (err) {
    return {
      ok: false,
      error: "REGISTRY_LOAD_FAILED",
      detail: String(err?.message || err).slice(0, 200),
    };
  }
  const registryRoot = path.resolve(
    options.registryRoot || REPO_ROOT_FROM_MODULE,
  );
  const gdi = findWorkspaceByStream(registry, "gdi");
  if (gdi?.path) {
    const root = path.join(
      gdi.path,
      "lib/group-demand-intelligence/bundled-publication-packages",
    );
    if (fs.existsSync(root)) {
      return {
        ok: true,
        root: path.resolve(root),
        source: "registry:gdi/bundled-publication-packages",
        gdiPath: gdi.path,
      };
    }
  }

  // CI / non-Windows checkouts: registry GDI path may be a Windows permanent
  // home that does not exist on the runner. Fall back to this repo checkout.
  const checkoutRoot = path.join(
    registryRoot,
    "lib/group-demand-intelligence/bundled-publication-packages",
  );
  if (fs.existsSync(checkoutRoot)) {
    return {
      ok: true,
      root: path.resolve(checkoutRoot),
      source: "checkout:bundled-publication-packages",
      gdiPath: gdi?.path || null,
    };
  }

  if (!gdi?.path) {
    return { ok: false, error: "GDI_WORKSPACE_NOT_REGISTERED" };
  }
  return {
    ok: false,
    error: "DURABLE_PACKAGE_ROOT_MISSING",
    root: path.join(
      gdi.path,
      "lib/group-demand-intelligence/bundled-publication-packages",
    ),
  };
}

/**
 * Resolve the directory that owns server.js (import.meta / script path).
 * Ownership for :8080 requires this to match the authorized workspace —
 * cwd alone is insufficient (Integration cwd + foreign server.js was the
 * repeated founder regression: AUTHORIZED_INTEGRATION_HOME while serving
 * `_verify_pr627_main` HTML → missing ES / 0 inventory UX).
 */
export function resolveServerScriptDir(input = {}) {
  if (input.serverScriptDir) return norm(input.serverScriptDir);
  if (input.serverScriptPath) return norm(path.dirname(input.serverScriptPath));
  return null;
}

/**
 * Decide whether cwd (+ server script dir) may bind the requested listen port.
 */
export function evaluateCanonicalPort8080Ownership(input = {}) {
  const env = input.env || process.env;
  const cwd = norm(input.cwd || process.cwd());
  const serverScriptDir = resolveServerScriptDir(input);
  const listenPort = Number(input.listenPort ?? env.PORT ?? CANONICAL_FOUNDER_REVIEW_PORT);
  const nodeEnv = text(env.NODE_ENV) || "development";

  const result = {
    contract: DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
    ok: false,
    listenPort,
    cwd,
    serverScriptDir,
    nodeEnv,
    authorizedFor8080: false,
    ownerStream: null,
    ownerId: null,
    reason: null,
    code: null,
    suggestedDevPort: STREAM_DEV_PORTS.gdi,
    durablePackageRoot: null,
    envBypassIgnored: {
      DEALALITY_CANONICAL_CHECKOUT: text(env.DEALALITY_CANONICAL_CHECKOUT) === "1",
      DEALALITY_ALLOW_WORKTREE_SERVER: text(env.DEALALITY_ALLOW_WORKTREE_SERVER) === "1",
    },
  };

  if (nodeEnv === "production") {
    result.ok = true;
    result.authorizedFor8080 = listenPort === CANONICAL_FOUNDER_REVIEW_PORT;
    result.reason = "production_skip_local_ownership";
    result.code = "PRODUCTION";
    return result;
  }

  let registry;
  try {
    registry =
      input.registry ||
      loadWorktreeRegistry(input.registryRoot || REPO_ROOT_FROM_MODULE);
  } catch (err) {
    result.code = "REGISTRY_LOAD_FAILED";
    result.reason = String(err?.message || err).slice(0, 240);
    return result;
  }

  const matched = findWorkspaceByPath(registry, cwd);
  const gdi = findWorkspaceByStream(registry, "gdi");
  const integration = findWorkspaceByStream(registry, "integration");
  const gdiPath = gdi ? norm(gdi.path) : null;
  const integrationPath = integration ? norm(integration.path) : null;
  const ownerFlag = text(env.DEALALITY_8080_OWNER).toLowerCase();

  const isGdiHome = Boolean(gdiPath && cwd === gdiPath);
  const isIntegrationHome = Boolean(integrationPath && cwd === integrationPath);
  const integrationOwnerRequested = ownerFlag === "integration";
  const repoRoot = norm(input.registryRoot || REPO_ROOT_FROM_MODULE);
  const isRepoCheckoutRoot = cwd === repoRoot;
  // Require GITHUB_ACTIONS specifically — many local tools set CI=true.
  const isGithubActionsCi = text(env.GITHUB_ACTIONS).toLowerCase() === "true";

  if (listenPort === CANONICAL_FOUNDER_REVIEW_PORT) {
    if (isGdiHome) {
      result.ok = true;
      result.authorizedFor8080 = true;
      result.ownerStream = "gdi";
      result.ownerId = gdi.id;
      result.code = "AUTHORIZED_GDI_HOME";
      result.reason =
        "Port 8080 authorized for registered GDI home (cumulative founder review).";
    } else if (isIntegrationHome && integrationOwnerRequested) {
      result.ok = true;
      result.authorizedFor8080 = true;
      result.ownerStream = "integration";
      result.ownerId = integration.id;
      result.code = "AUTHORIZED_INTEGRATION_HOME";
      result.reason =
        "Port 8080 authorized for Integration Controller with DEALALITY_8080_OWNER=integration.";
    } else if (isGithubActionsCi && isRepoCheckoutRoot) {
      // GitHub Actions checks out the Integration repo to a Linux path that is
      // not the registered Windows Integration home. Authorize only the exact
      // repo checkout root under CI — never foreign/local worktrees.
      result.ok = true;
      result.authorizedFor8080 = true;
      result.ownerStream = "integration";
      result.ownerId = integration?.id || "ci-repo-checkout";
      result.code = "AUTHORIZED_CI_REPO_CHECKOUT";
      result.reason =
        "Port 8080 authorized for GitHub Actions repository checkout (CI only).";
    } else {
      result.ok = false;
      result.authorizedFor8080 = false;
      result.code = "PORT_8080_UNAUTHORIZED_WORKSPACE";
      result.ownerStream = matched?.stream || null;
      result.ownerId = matched?.id || null;
      result.suggestedDevPort =
        STREAM_DEV_PORTS[matched?.stream] || STREAM_DEV_PORTS.gdi;
      const bypassNote = result.envBypassIgnored.DEALALITY_CANONICAL_CHECKOUT
        ? " DEALALITY_CANONICAL_CHECKOUT=1 was ignored for :8080."
        : "";
      result.reason =
        `Port ${CANONICAL_FOUNDER_REVIEW_PORT} is reserved for the registered GDI home` +
        ` (${gdiPath || "ws-gdi"}) or Integration with DEALALITY_8080_OWNER=integration.` +
        ` Current cwd=${cwd}. Use PORT=${result.suggestedDevPort} for feature work.` +
        bypassNote;
      return result;
    }

    // Fail-closed: server.js directory must equal the authorized workspace.
    // Prevents `cd my-operators-backend && node ../_verify_*/server.js` from
    // claiming AUTHORIZED_INTEGRATION_HOME while serving a foreign public/.
    if (serverScriptDir) {
      const authorizedScriptRoots = [];
      if (result.code === "AUTHORIZED_GDI_HOME" && gdiPath) {
        authorizedScriptRoots.push(gdiPath);
      } else if (result.code === "AUTHORIZED_INTEGRATION_HOME" && integrationPath) {
        authorizedScriptRoots.push(integrationPath);
      } else if (result.code === "AUTHORIZED_CI_REPO_CHECKOUT") {
        authorizedScriptRoots.push(repoRoot);
      }
      if (
        authorizedScriptRoots.length > 0 &&
        !authorizedScriptRoots.includes(serverScriptDir)
      ) {
        result.ok = false;
        result.authorizedFor8080 = false;
        result.code = "PORT_8080_SERVER_SCRIPT_MISMATCH";
        result.reason =
          `Port ${CANONICAL_FOUNDER_REVIEW_PORT} denied: server.js lives in ${serverScriptDir}` +
          ` but ownership was claimed for cwd=${cwd} (${result.code} candidate).` +
          ` Serve only from the authorized workspace tree (ws-gdi), never a foreign server.js path.` +
          ` Feature streams use PORT=${STREAM_DEV_PORTS.he}/${STREAM_DEV_PORTS.gdi}/…`;
        return result;
      }
    } else if (input.requireServerScriptDir === true) {
      result.ok = false;
      result.authorizedFor8080 = false;
      result.code = "PORT_8080_SERVER_SCRIPT_DIR_REQUIRED";
      result.reason =
        "Port 8080 ownership requires serverScriptDir (directory containing server.js).";
      return result;
    }

    const durable = resolveDurableGdiPublicationPackageRoot({
      env,
      registry,
      registryRoot: input.registryRoot || REPO_ROOT_FROM_MODULE,
    });
    result.durablePackageRoot = durable;
    if (!durable.ok) {
      result.ok = false;
      result.code = "DURABLE_STAGED_ROOT_UNAVAILABLE";
      result.reason = `Authorized for :8080 but durable GDI publication root failed: ${durable.error}`;
      return result;
    }
    return result;
  }

  // Non-8080: registered permanent homes + allow-listed worktree flag for temps.
  const allowWorktree = text(env.DEALALITY_ALLOW_WORKTREE_SERVER) === "1";
  const isRegisteredPermanent =
    matched &&
    matched.lifecycle === "permanent" &&
    ["gdi", "he", "adp", "be", "integration"].includes(matched.stream);

  if (isRegisteredPermanent || allowWorktree) {
    result.ok = true;
    result.authorizedFor8080 = false;
    result.ownerStream = matched?.stream || null;
    result.ownerId = matched?.id || null;
    result.code = isRegisteredPermanent
      ? "AUTHORIZED_DEV_PORT_REGISTERED"
      : "AUTHORIZED_DEV_PORT_ALLOW_WORKTREE";
    result.reason = `Non-canonical port ${listenPort} allowed.`;
    result.suggestedDevPort =
      STREAM_DEV_PORTS[matched?.stream] || STREAM_DEV_PORTS.gdi;
    return result;
  }

  result.ok = false;
  result.code = "UNAUTHORIZED_DEV_WORKSPACE";
  result.reason =
    `Server started from unexpected directory: ${cwd}. ` +
    `Use a registered permanent workspace or set DEALALITY_ALLOW_WORKTREE_SERVER=1 with a non-8080 PORT.`;
  return result;
}

/**
 * Throw / exit helper for server boot.
 */
export function assertCanonicalPort8080Ownership(input = {}) {
  const evaluation = evaluateCanonicalPort8080Ownership(input);
  if (evaluation.ok) return evaluation;
  const err = new Error(evaluation.reason || evaluation.code || "PORT_OWNERSHIP_DENIED");
  err.code = evaluation.code;
  err.evaluation = evaluation;
  throw err;
}

/**
 * Env patch for authorized :8080 launches — pins durable STAGED root.
 * Does not authorize :8080 by itself.
 */
export function buildCanonical8080LaunchEnv(baseEnv = process.env, options = {}) {
  const env = { ...baseEnv };
  const cwd = options.cwd || process.cwd();
  const evaluation = evaluateCanonicalPort8080Ownership({
    ...options,
    env,
    listenPort: CANONICAL_FOUNDER_REVIEW_PORT,
    cwd,
    serverScriptDir: options.serverScriptDir || cwd,
    requireServerScriptDir: true,
  });
  if (!evaluation.ok || !evaluation.authorizedFor8080) {
    return { ok: false, evaluation, env: null };
  }
  const durable = evaluation.durablePackageRoot;
  env.PORT = String(CANONICAL_FOUNDER_REVIEW_PORT);
  env.GDI_PUBLICATION_PACKAGE_ROOT = durable.root;
  env.DEALALITY_CANONICAL_8080_OWNERSHIP = DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1;
  // Retain legacy flags for older helpers, but ownership module ignores them for :8080.
  env.DEALALITY_ALLOW_WORKTREE_SERVER = "1";
  if (evaluation.ownerStream === "integration") {
    env.DEALALITY_8080_OWNER = "integration";
  } else {
    // Do not inherit a sticky DEALALITY_8080_OWNER=integration from parent shells
    // when launching the GDI home cumulative server.
    delete env.DEALALITY_8080_OWNER;
  }
  return { ok: true, evaluation, env };
}

function listListeningPidsWindows(port) {
  try {
    const out = execFileSync(
      "powershell.exe",
      [
        "-NoProfile",
        "-Command",
        `(Get-NetTCPConnection -LocalPort ${Number(port)} -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique) -join ','`,
      ],
      { encoding: "utf8", windowsHide: true },
    );
    return String(out || "")
      .split(/[,\s]+/)
      .map((x) => Number(String(x).trim()))
      .filter((n) => Number.isFinite(n) && n > 0);
  } catch {
    return [];
  }
}

function commandLineForPidWindows(pid) {
  try {
    const out = execFileSync(
      "powershell.exe",
      [
        "-NoProfile",
        "-Command",
        `(Get-CimInstance Win32_Process -Filter "ProcessId=${Number(pid)}").CommandLine`,
      ],
      { encoding: "utf8", windowsHide: true },
    );
    return String(out || "").trim();
  } catch {
    return "";
  }
}

/**
 * Live OS-level audit of whoever currently listens on :8080.
 * Meta PID files alone are not proof — foreign trees repeatedly stole the port.
 */
export function auditLiveCanonical8080Listener(options = {}) {
  const port = Number(options.port || CANONICAL_FOUNDER_REVIEW_PORT);
  const registry =
    options.registry ||
    loadWorktreeRegistry(options.registryRoot || REPO_ROOT_FROM_MODULE);
  const gdi = findWorkspaceByStream(registry, "gdi");
  const integration = findWorkspaceByStream(registry, "integration");
  const gdiPath = gdi?.path ? norm(gdi.path) : null;
  const integrationPath = integration?.path ? norm(integration.path) : null;
  const gdiServerNeedle = gdiPath
    ? `${gdiPath.replace(/\//g, "\\")}\\server.js`
    : "\\ws-gdi\\server.js";
  const gdiServerNeedleAlt = gdiPath
    ? `${gdiPath}/server.js`
    : "/ws-gdi/server.js";
  const integrationServerNeedle = integrationPath
    ? `${integrationPath.replace(/\//g, "\\")}\\server.js`
    : "\\my-operators-backend\\server.js";

  const pids = listListeningPidsWindows(port);
  if (!pids.length) {
    return {
      ok: false,
      code: "PORT_8080_NOT_LISTENING",
      port,
      pids: [],
      reason: `Nothing is listening on port ${port}.`,
      authorized: false,
    };
  }

  const listeners = pids.map((pid) => {
    const cmd = commandLineForPidWindows(pid);
    const cmdNorm = cmd.replace(/\//g, "\\").toLowerCase();
    const gdiNeedle = gdiServerNeedle.replace(/\//g, "\\").toLowerCase();
    const isGdiScript =
      cmdNorm.includes(gdiNeedle) ||
      cmd.toLowerCase().includes(gdiServerNeedleAlt.toLowerCase());
    const isIntegrationScript = cmdNorm.includes(
      integrationServerNeedle.replace(/\//g, "\\").toLowerCase(),
    );
    const foreignVerify =
      /_verify_pr\d+_main/i.test(cmd) ||
      /\\wt-[a-z0-9_-]+\\/i.test(cmd);
    const relativeServerOnly = /(^|[\\/"])server\.js("|$|\s)/i.test(cmd) && !isGdiScript && !isIntegrationScript;
    return {
      pid,
      commandLine: cmd.slice(0, 500),
      isGdiScript,
      isIntegrationScript,
      relativeServerOnly,
      foreignHeuristic: foreignVerify && !isGdiScript,
    };
  });

  const gdiListener = listeners.find((l) => l.isGdiScript);
  if (gdiListener) {
    return {
      ok: true,
      code: "LIVE_AUTHORIZED_GDI_SERVER_SCRIPT",
      port,
      pids,
      listeners,
      authorized: true,
      ownerStream: "gdi",
      gdiPath,
    };
  }

  const foreign = listeners.find((l) => l.foreignHeuristic || l.isIntegrationScript || l.relativeServerOnly) || listeners[0];
  return {
    ok: false,
    code: foreign?.foreignHeuristic
      ? "LIVE_FOREIGN_8080_LISTENER"
      : foreign?.isIntegrationScript
        ? "LIVE_INTEGRATION_OR_MISMATCHED_SCRIPT"
        : foreign?.relativeServerOnly
          ? "LIVE_RELATIVE_SERVER_JS_AMBIGUOUS"
          : "LIVE_FOREIGN_8080_LISTENER",
    port,
    pids,
    listeners,
    authorized: false,
    reason:
      `Port ${port} listener is not the registered GDI server.js` +
      ` (expected path containing ${gdiServerNeedle}).` +
      ` Observed: ${(foreign?.commandLine || listeners[0]?.commandLine || "").slice(0, 240)}`,
    gdiPath,
  };
}
