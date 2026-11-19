#!/usr/bin/env node
/**
 * Integration Controller — remote PR merge without checking out the feature branch
 * or mutating the dirty primary worktree.
 *
 * Usage:
 *   npm run dealality:remote-pr-merge -- --pr 427
 *   npm run dealality:remote-pr-merge -- --pr 427 --method merge
 *
 * Does NOT: git switch, stash, reset, or clean the Integration Controller tree.
 */
import { execFileSync } from "node:child_process";

function get(argv, flag) {
  const i = argv.indexOf(flag);
  return i >= 0 ? argv[i + 1] : "";
}

const argv = process.argv.slice(2);
const pr = get(argv, "--pr");
const method = get(argv, "--method") || "merge";
if (!pr) {
  console.error("Usage: dealality:remote-pr-merge -- --pr <n> [--method merge|squash|rebase]");
  process.exit(2);
}

function ghJson(args) {
  const out = execFileSync("gh", args, { encoding: "utf8" });
  return JSON.parse(out);
}

const view = ghJson([
  "pr",
  "view",
  String(pr),
  "--json",
  "number,state,mergeable,mergeStateStatus,url,title,headRefName,baseRefName,statusCheckRollup",
]);

const checks = Array.isArray(view.statusCheckRollup) ? view.statusCheckRollup : [];
const failed = checks.filter(
  (c) => c.status === "COMPLETED" && c.conclusion && c.conclusion !== "SUCCESS" && c.conclusion !== "SKIPPED" && c.conclusion !== "NEUTRAL",
);
const pending = checks.filter((c) => c.status !== "COMPLETED");

if (view.state === "MERGED") {
  console.log(
    JSON.stringify(
      {
        ok: true,
        alreadyMerged: true,
        pr: view.number,
        url: view.url,
        note: "PR already merged — no local checkout performed",
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

if (failed.length) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        status: "BLOCKED — CI_FAILED",
        pr: view.number,
        failed: failed.map((c) => ({ name: c.name, conclusion: c.conclusion })),
      },
      null,
      2,
    ),
  );
  process.exit(3);
}

if (pending.length) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        status: "BLOCKED — CI_PENDING",
        pr: view.number,
        pending: pending.map((c) => c.name),
      },
      null,
      2,
    ),
  );
  process.exit(4);
}

if (view.mergeable !== "MERGEABLE" || view.mergeStateStatus === "BEHIND") {
  // Attempt update-branch then re-check once
  try {
    execFileSync("gh", ["pr", "update-branch", String(pr)], { encoding: "utf8", stdio: "pipe" });
  } catch (err) {
    console.error(
      JSON.stringify(
        {
          ok: false,
          status: "BLOCKED — UPDATE_BRANCH_FAILED",
          pr: view.number,
          mergeable: view.mergeable,
          mergeStateStatus: view.mergeStateStatus,
          error: String(err?.message || err),
        },
        null,
        2,
      ),
    );
    process.exit(5);
  }
}

const mergeArgs = ["pr", "merge", String(pr), `--${method}`, "--delete-branch=false"];
execFileSync("gh", mergeArgs, { encoding: "utf8", stdio: "inherit" });
const after = ghJson([
  "pr",
  "view",
  String(pr),
  "--json",
  "state,mergedAt,mergeCommit,url,number",
]);

console.log(
  JSON.stringify(
    {
      ok: after.state === "MERGED",
      status: after.state === "MERGED" ? "MERGED_REMOTE" : "MERGE_INCOMPLETE",
      pr: after.number,
      url: after.url,
      mergeCommit: after.mergeCommit?.oid || null,
      mergedAt: after.mergedAt,
      localCheckout: false,
      dirtyWorktreeTouched: false,
      note: "Remote merge only — run dealality:post-merge-sync for product homes; leave Integration dirty tree untouched",
    },
    null,
    2,
  ),
);
process.exit(after.state === "MERGED" ? 0 : 6);
