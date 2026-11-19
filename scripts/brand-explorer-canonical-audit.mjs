#!/usr/bin/env node
/**
 * Read-only canonical audit. Does not write Airtable.
 * Pass a JSON profile path to audit one in-memory profile.
 */
import { readFileSync } from "fs";
import { evaluateMasterRelease } from "../lib/brand-explorer/canonical/master-release.js";

const fileIdx = process.argv.indexOf("--profile");
if (fileIdx < 0) {
  console.log(JSON.stringify({
    mode: "contract-only",
    note: "Pass --profile <file.json> to audit one profile. Active-universe audit is brand-explorer:backscan.",
    writes: 0,
  }, null, 2));
  process.exit(0);
}
const profile = JSON.parse(readFileSync(process.argv[fileIdx + 1], "utf8"));
const result = evaluateMasterRelease(profile);
console.log(JSON.stringify({ MASTER_RELEASE_CONTRACT_PASS: result.MASTER_RELEASE_CONTRACT_PASS, status: result.status, failedGates: result.gates.filter((g) => !g.pass).map((g) => g.name), findings: result.findings.map((f) => f.defectClass), writes: 0 }, null, 2));
process.exit(result.MASTER_RELEASE_CONTRACT_PASS ? 0 : 1);
