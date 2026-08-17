#!/usr/bin/env node
/**
 * READ-ONLY evidence validation for Active operators.
 *   node scripts/operator-fit-evidence-validation.mjs
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows } from "../api/lib/operator-setup-new-base-read.js";
import { adaptOperatorFromPrefill } from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { validateOperatorEvidence } from "../lib/operator-fit/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const { candidates } = await loadActiveOperatorCandidatesForAlignment();
const rows = [];
for (const c of candidates || []) {
  const prefill = buildPrefillObjectFromNewBaseRows(
    c.master,
    c.profile,
    c.platform,
    c.commercial,
    c.governance
  );
  const op = adaptOperatorFromPrefill(
    { ...(prefill || {}), submission_status: "Active", companyName: c.companyName },
    { operatorId: c.operatorId, companyName: c.companyName }
  );
  const issues = validateOperatorEvidence(op);
  if (issues.length) {
    rows.push({ operatorId: c.operatorId, companyName: c.companyName, issues });
  }
}

const report = {
  mode: "read-only",
  generatedAt: new Date().toISOString(),
  operatorsWithIssues: rows.length,
  rows,
};
mkdirSync(join(root, "reports"), { recursive: true });
const out = join(root, "reports", "operator-fit-evidence-validation.json");
writeFileSync(out, JSON.stringify(report, null, 2), "utf8");
console.log(JSON.stringify({ wrote: out, operatorsWithIssues: rows.length }, null, 2));
