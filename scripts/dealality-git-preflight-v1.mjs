#!/usr/bin/env node
/**
 * Dealality mandatory Git preflight (fail-closed).
 *
 * Usage:
 *   npm run dealality:git-preflight -- --op commit --stream integration
 *   npm run dealality:git-preflight -- --op push --expect-branch chore/dealality-dev-os-governance-v1
 *   npm run dealality:git-preflight -- --op force-push --founder-auth FOUNDER_AUTH_FORCE_PUSH
 *   npm run dealality:git-preflight -- --op status-check --json
 *
 * Does not intercept raw `git` automatically. Use with Cursor rules + optional .githooks.
 */
import {
  formatPreflightResult,
  preflightFromArgv,
} from "../lib/dealality-dev-os/git-preflight.js";

const argv = process.argv.slice(2);
const json = argv.includes("--json");
const result = preflightFromArgv(argv);

if (json) {
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
} else {
  process.stdout.write(`${formatPreflightResult(result)}\n`);
}

process.exit(result.ok ? 0 : 2);
