/**
 * Regenerate Dealality batch learning ledger (JSON + human markdown).
 *
 *   npm run dealality:batch-learning-ledger
 *
 * No Airtable writes. No Brand Explorer patches.
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  PATHS,
  STATUS,
  buildLedgerDocument,
  buildSeedLearningEntries,
  renderLedgerMarkdown,
  renderSystemDocMarkdown,
  validateLedger,
} from "../lib/data-intelligence/dealality-batch-learning-system.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function write(rel, text) {
  const p = join(ROOT, rel);
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, text, "utf8");
}

function main() {
  const entries = buildSeedLearningEntries();
  const ledger = buildLedgerDocument(entries);
  const v = validateLedger(ledger);
  if (!v.ok) {
    console.error("[batch-learning-ledger] validation failed:");
    for (const e of v.errors) console.error(" -", e);
    process.exit(1);
  }

  write(PATHS.ledgerJson, JSON.stringify(ledger, null, 2) + "\n");
  write(PATHS.ledgerMd, renderLedgerMarkdown(ledger));
  write(PATHS.systemDoc, renderSystemDocMarkdown());
  // Mirror seed for fixture consumers
  write(
    PATHS.seedEntries,
    JSON.stringify(
      {
        version: ledger.version,
        note: "Canonical seed is buildSeedLearningEntries() in lib; this file is a durable mirror.",
        entry_count: entries.length,
        entries,
      },
      null,
      2
    ) + "\n"
  );

  console.log(`[batch-learning-ledger] entries=${entries.length}`);
  console.log(`[batch-learning-ledger] wrote ${PATHS.ledgerJson}`);
  console.log(`[batch-learning-ledger] wrote ${PATHS.ledgerMd}`);
  console.log(`[batch-learning-ledger] wrote ${PATHS.systemDoc}`);
  console.log(`[batch-learning-ledger] status=${STATUS.READY}`);
}

main();
