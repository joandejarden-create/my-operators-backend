#!/usr/bin/env node
/**
 * Suggest PR validation commands from changed files.
 *
 *   node scripts/dealality-pr-check-suggest.mjs
 *   node scripts/dealality-pr-check-suggest.mjs --base main
 *   node scripts/dealality-pr-check-suggest.mjs --json
 *
 * Matrix source: lib/dealality-pr-check-matrix.js
 * Docs: docs/dealality-pr-validation-matrix.md
 */
import { execSync } from "child_process";
import { fileURLToPath } from "url";
import path from "path";
import { suggestPrChecks } from "../lib/dealality-pr-check-matrix.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function hasArg(name) {
  return process.argv.includes(name);
}

function getArg(name, fallback) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

function runGit(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  } catch {
    return "";
  }
}

function getChangedFiles(base) {
  if (base) {
    const mergeBase = runGit(`git merge-base HEAD ${base}`);
    const ref = mergeBase || base;
    const diff = runGit(`git diff --name-only ${ref}...HEAD`);
    const unstaged = runGit("git diff --name-only");
    const staged = runGit("git diff --name-only --cached");
    const untracked = runGit("git ls-files --others --exclude-standard");
    return [
      ...new Set(
        [...diff.split("\n"), ...unstaged.split("\n"), ...staged.split("\n"), ...untracked.split("\n")].filter(Boolean)
      ),
    ];
  }

  const unstaged = runGit("git diff --name-only");
  const staged = runGit("git diff --name-only --cached");
  const untracked = runGit("git ls-files --others --exclude-standard");
  return [...new Set([...unstaged.split("\n"), ...staged.split("\n"), ...untracked.split("\n")].filter(Boolean))];
}

const base = getArg("--base", "");
const asJson = hasArg("--json");
const files = getChangedFiles(base || null);

if (!files.length) {
  if (asJson) {
    console.log(JSON.stringify({ changedFiles: [], rules: [], allCommands: [], maxRisk: "Low" }, null, 2));
  } else {
    console.log("[dealality] No changed files detected (working tree clean vs HEAD).");
    console.log("Tip: use --base main to include commits since branch diverged.");
  }
  process.exit(0);
}

const result = suggestPrChecks(files);

if (asJson) {
  console.log(
    JSON.stringify(
      {
        changedFiles: result.changedFiles,
        maxRisk: result.maxRisk,
        allCommands: result.allCommands,
        rules: result.rules.map((r) => ({
          id: r.id,
          label: r.label,
          risk: r.risk,
          commands: [...r.commands],
          files: [...r.files],
        })),
      },
      null,
      2
    )
  );
  process.exit(0);
}

console.log("[dealality] PR check suggestions\n");
console.log(`Changed files (${result.changedFiles.length}):`);
for (const f of result.changedFiles.slice(0, 30)) console.log(`  - ${f}`);
if (result.changedFiles.length > 30) console.log(`  … and ${result.changedFiles.length - 30} more`);

console.log(`\nSuggested max risk tier: ${result.maxRisk}\n`);

if (!result.rules.length) {
  console.log("No matrix rules matched. Review docs/dealality-pr-validation-matrix.md manually.");
  process.exit(0);
}

for (const rule of result.rules) {
  console.log(`## ${rule.label} (${rule.risk})`);
  for (const f of [...rule.files].sort()) console.log(`  file: ${f}`);
  if (rule.commands.size) {
    console.log("  commands:");
    for (const cmd of [...rule.commands].sort()) console.log(`    ${cmd}`);
  } else {
    console.log("  commands: (manual QA — see validation matrix doc)");
  }
  console.log("");
}

if (result.allCommands.length) {
  console.log("---\nRun all suggested commands:\n");
  for (const cmd of result.allCommands) console.log(cmd);
  console.log("\nDocs: docs/dealality-pr-validation-matrix.md");
}
