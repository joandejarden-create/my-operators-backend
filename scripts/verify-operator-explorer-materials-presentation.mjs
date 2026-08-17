#!/usr/bin/env node
/**
 * Verify Operator Setup - Explorer Materials rows link to Master and normalize for API.
 *
 *   node scripts/verify-operator-explorer-materials-presentation.mjs
 *   node scripts/verify-operator-explorer-materials-presentation.mjs --master recTUjuDxL96yWcQA
 */
import "../load-env.js";
import { normalizeOperatorExplorerPresentationRecords } from "../api/lib/operator-materials-explorer-presentation-map.js";
import {
  NEW_BASE_EXPLORER_MATERIALS_TABLE,
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
  fetchRecordsLinkedToMaster,
} from "../api/lib/operator-setup-new-base-read.js";

const masterArg = process.argv.find((a, i) => process.argv[i - 1] === "--master");

async function main() {
  const masters = masterArg
    ? [{ id: masterArg }]
    : await fetchAllRecordsRest(NEW_BASE_MASTER_TABLE);

  let fail = 0;
  for (const m of masters) {
    const rows = await fetchRecordsLinkedToMaster(NEW_BASE_EXPLORER_MATERIALS_TABLE, m.id);
    const normalized = normalizeOperatorExplorerPresentationRecords(rows);
    const fileCount = normalized.blocks.filter((b) => b.slotKey === "materials.file").length;
    const galleryCount = normalized.blocks.filter((b) =>
      /^materials\.gallery\.\d+$/.test(b.slotKey)
    ).length;
    const ok = fileCount >= 1 && galleryCount >= 6;
    console.log(
      ok ? "OK" : "FAIL",
      m.id,
      `rows=${rows.length}`,
      `blocks=${normalized.blocks.length}`,
      `files=${fileCount}`,
      `gallery=${galleryCount}`
    );
    if (!ok) fail += 1;
  }
  if (fail) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
