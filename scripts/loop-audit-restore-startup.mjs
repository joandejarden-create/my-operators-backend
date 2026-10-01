#!/usr/bin/env node
/**
 * Iteratively audit server import graph and restore missing local modules
 * from Cursor history + agent transcripts until no missing remain (or stall).
 */
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";

const ROOT = process.cwd();

function runAudit() {
  const r = spawnSync(process.execPath, ["scripts/audit-startup-import-graph.mjs", "server.js"], {
    cwd: ROOT,
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
  });
  const out = r.stdout || "";
  try {
    return JSON.parse(out);
  } catch {
    console.error(out.slice(0, 500));
    throw new Error("audit_parse_failed");
  }
}

function restoreTried(triedRel) {
  const rel = String(triedRel || "").replace(/\\/g, "/");
  if (!rel || rel.startsWith("..")) return false;
  // history path fragment
  let r = spawnSync(
    process.execPath,
    ["scripts/_restore-path-fragment-from-history.mjs", rel],
    { cwd: ROOT, encoding: "utf8" }
  );
  process.stdout.write(r.stdout || "");
  if (fs.existsSync(path.join(ROOT, rel))) return true;

  // parent dir
  const parent = path.posix.dirname(rel) + "/";
  r = spawnSync(
    process.execPath,
    ["scripts/_restore-path-fragment-from-history.mjs", parent],
    { cwd: ROOT, encoding: "utf8" }
  );
  process.stdout.write(r.stdout || "");
  if (fs.existsSync(path.join(ROOT, rel))) return true;

  r = spawnSync(
    process.execPath,
    ["scripts/_restore-named-from-transcripts.mjs", rel],
    { cwd: ROOT, encoding: "utf8" }
  );
  process.stdout.write(r.stdout || r.stderr || "");
  return fs.existsSync(path.join(ROOT, rel));
}

const report = { rounds: [], unrecovered: [] };
for (let round = 1; round <= 25; round++) {
  const audit = runAudit();
  console.log(`ROUND ${round} missing=${audit.missingCount}`);
  report.rounds.push({ round, missingCount: audit.missingCount, missing: audit.missing });
  if (!audit.missingCount) break;
  let progress = false;
  for (const m of audit.missing) {
    const tried = m.tried;
    if (!tried) continue;
    const abs = path.join(ROOT, tried);
    if (fs.existsSync(abs)) continue;
    console.log("RESTORE", tried);
    if (restoreTried(tried)) {
      progress = true;
      console.log("OK", tried);
    } else {
      console.log("FAIL", tried);
      if (!report.unrecovered.includes(tried)) report.unrecovered.push(tried);
    }
  }
  if (!progress) {
    console.log("STALLED");
    break;
  }
}

fs.mkdirSync(path.join(ROOT, "reports"), { recursive: true });
fs.writeFileSync(
  path.join(ROOT, "reports/startup-recovery-import-loop.json"),
  JSON.stringify(report, null, 2)
);
console.log("FINAL_UNRECOVERED", report.unrecovered);
