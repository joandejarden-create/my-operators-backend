#!/usr/bin/env node
/**
 * Mandatory pre-prompt / pre-implementation assignment preflight.
 *
 *   npm run dealality:assignment-preflight -- --stream gdi
 *
 * Fail-closed: do not start feature work from a regressed :8080 baseline.
 * Contract: DEALALITY_INTER_PROMPT_NON_REGRESSION_V1
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  auditLiveCanonical8080Listener,
  DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
} from "../lib/dealality-dev-os/canonical-port-8080-ownership-v1.js";
import { validateGdiCumulativeFunctionalityManifest } from "../lib/dealality-dev-os/gdi-cumulative-functionality-manifest-v1.js";
import { findWorkspaceByStream, loadWorktreeRegistry } from "../lib/dealality-dev-os/worktree-registry.js";
import { preflightFromArgv } from "../lib/dealality-dev-os/git-preflight.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CONTRACT = "DEALALITY_INTER_PROMPT_NON_REGRESSION_V1";

function getArg(argv, name) {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : "";
}

function fetchJson(url, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, { timeout: timeoutMs }, (res) => {
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, json: JSON.parse(body) });
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error("timeout"));
    });
  });
}

function git(args) {
  try {
    return execFileSync("git", args, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

const argv = process.argv.slice(2);
const stream = (getArg(argv, "--stream") || "gdi").toLowerCase();
const json = argv.includes("--json");
const requireLive8080 = !argv.includes("--allow-missing-8080");
const errors = [];
const checks = [];

const registry = loadWorktreeRegistry(ROOT);
const home = findWorkspaceByStream(registry, stream);
const cwdNorm = ROOT.replace(/\\/g, "/").toLowerCase();
const homeNorm = String(home?.path || "")
  .replace(/\\/g, "/")
  .toLowerCase();
if (!home) errors.push(`stream ${stream} not registered`);
else if (cwdNorm !== homeNorm) {
  errors.push(`cwd ${ROOT} is not registered home ${home.path}`);
}
checks.push({
  id: "workspace_home",
  ok: errors.length === 0,
  cwd: ROOT,
  home: home?.path || null,
});

try {
  execFileSync("git", ["fetch", "origin", "main"], {
    cwd: ROOT,
    stdio: "ignore",
    windowsHide: true,
  });
} catch {
  /* offline — continue with existing origin/main */
}
const head = git(["rev-parse", "HEAD"]);
const originMain = git(["rev-parse", "origin/main"]);
const branch = git(["rev-parse", "--abbrev-ref", "HEAD"]);
checks.push({
  id: "origin_main",
  ok: Boolean(originMain),
  head,
  originMain,
  branch,
  headEqualsOriginMain: head === originMain,
});

const gitPf = preflightFromArgv([
  "--op",
  "status-check",
  "--stream",
  stream,
]);
checks.push({
  id: "git_preflight_status",
  ok: gitPf.ok === true,
  status: gitPf.status,
  errors: gitPf.errors || [],
});
if (!gitPf.ok) errors.push(`git-preflight status-check failed: ${gitPf.status}`);

const manifest = validateGdiCumulativeFunctionalityManifest({ root: ROOT });
checks.push({
  id: "cumulative_functionality_manifest",
  ok: manifest.ok,
  errors: manifest.errors,
  capabilityCount: manifest.manifest?.capabilities?.length || 0,
});
if (!manifest.ok) {
  for (const e of manifest.errors) errors.push(`manifest: ${e}`);
}

const live = auditLiveCanonical8080Listener({ registry });
checks.push({
  id: "live_8080_listener",
  ok: live.ok === true,
  code: live.code,
  reason: live.reason || null,
  pids: live.pids,
  contract: DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1,
});
if (requireLive8080 && !live.ok) {
  errors.push(`live :8080 audit failed (${live.code}): ${live.reason || ""}`);
}

let identity = null;
if (live.ok || live.pids?.length) {
  try {
    const { status, json: body } = await fetchJson(
      "http://127.0.0.1:8080/api/dev/runtime-identity",
    );
    identity = { status, ...body };
    const scriptOk = body?.servingScriptDirectoryBasename === "ws-gdi";
    const basenameOk = body?.servingDirectoryBasename === "ws-gdi";
    const stagedOk = Boolean(body?.publication?.wyndhamStagedVersion);
    const ownershipOk = body?.portOwnership?.authorizedFor8080 === true;
    const matchOk = body?.cwdMatchesServerScript !== false;
    const idOk =
      status === 200 && scriptOk && basenameOk && stagedOk && ownershipOk && matchOk;
    checks.push({
      id: "runtime_identity",
      ok: idOk,
      servingDirectoryBasename: body?.servingDirectoryBasename,
      servingScriptDirectoryBasename: body?.servingScriptDirectoryBasename,
      cwdMatchesServerScript: body?.cwdMatchesServerScript,
      ownership: body?.portOwnership,
      wyndhamStagedVersion: body?.publication?.wyndhamStagedVersion,
    });
    if (!idOk) {
      errors.push(
        "runtime-identity does not prove authorized ws-gdi cumulative server (cwd/script/STAGED)",
      );
    }
  } catch (err) {
    checks.push({
      id: "runtime_identity",
      ok: false,
      error: String(err?.message || err),
    });
    if (requireLive8080) {
      errors.push(`runtime-identity unreachable: ${err?.message || err}`);
    }
  }
}

const result = {
  ok: errors.length === 0,
  contract: CONTRACT,
  stream,
  errors,
  checks,
  identity,
  remediation:
    errors.length === 0
      ? null
      : [
          "STOP new feature implementation.",
          "cd C:\\Dev\\dealality\\ws-gdi",
          "npm run dealality:gdi-local-8080",
          "npm run dealality:canonical-8080-guard -- --once",
          "npm run dealality:validate-gdi-cumulative-8080",
          "npm run dealality:assignment-preflight -- --stream gdi",
          "Resolve via Integration Controller if home/main/STAGED drift remains.",
        ],
};

if (json) {
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} else {
  console.log(
    `[dealality:assignment-preflight] contract=${CONTRACT} ok=${result.ok} stream=${stream}`,
  );
  for (const c of checks) {
    console.log(`  [${c.ok ? "PASS" : "FAIL"}] ${c.id}${c.code ? ` code=${c.code}` : ""}`);
  }
  if (errors.length) {
    console.log("  ERRORS:");
    for (const e of errors) console.log(`    - ${e}`);
    if (result.remediation) {
      console.log("  REMEDIATION:");
      for (const line of result.remediation) console.log(`    ${line}`);
    }
  } else {
    console.log("  RESULT READY — cumulative baseline verified; implementation may proceed");
  }
}

process.exit(result.ok ? 0 : 2);
