/**
 * Audit Dealality batch learning system health.
 *
 *   npm run dealality:batch-learning-audit
 *
 * Reports last Census/BE batches, rules/fixtures/tests, unresolved Webhound/steward,
 * and whether the process actually learned.
 */

import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  PATHS,
  STATUS,
  buildLedgerDocument,
  runBatchLearningAudit,
  renderAuditMarkdown,
  renderSystemDocMarkdown,
} from "../lib/data-intelligence/dealality-batch-learning-system.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function write(rel, text) {
  const p = join(ROOT, rel);
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, text, "utf8");
}

function loadLedger() {
  const p = join(ROOT, PATHS.ledgerJson);
  if (existsSync(p)) {
    try {
      return JSON.parse(readFileSync(p, "utf8"));
    } catch {
      /* fall through */
    }
  }
  return buildLedgerDocument();
}

function main() {
  const ledger = loadLedger();
  const audit = runBatchLearningAudit(ledger);

  write(PATHS.systemReportJson, JSON.stringify(audit, null, 2) + "\n");
  write(PATHS.systemReportMd, renderAuditMarkdown(audit));
  // Keep system doc fresh
  write(PATHS.systemDoc, renderSystemDocMarkdown());

  console.log(`[batch-learning-audit] status=${audit.status}`);
  console.log(`[batch-learning-audit] process_actually_learned=${audit.process_actually_learned}`);
  console.log(
    `[batch-learning-audit] last_census=${audit.last_census_batch?.batch_name || "—"} last_be=${audit.last_brand_explorer_batch?.batch_name || "—"}`
  );
  console.log(
    `[batch-learning-audit] code_rules=${audit.code_rules_added} fixtures=${audit.fixtures_added} tests=${audit.tests_added} webhound=${audit.unresolved_webhound_candidates} steward=${audit.unresolved_steward_cases}`
  );
  console.log(`[batch-learning-audit] wrote ${PATHS.systemReportJson}`);

  process.exit(audit.status === STATUS.BLOCKED ? 2 : 0);
}

main();
