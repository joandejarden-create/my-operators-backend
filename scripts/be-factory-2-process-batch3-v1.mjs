#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 3 depth processing (voco, Motto, avid, Tribute, Indigo).
 * No production Presentation writeback.
 */
import "../load-env.js";
import { listBatch3 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import { runDepthBatch } from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();

async function main() {
  const packs = listBatch3();
  if (packs.length !== 5) {
    console.error(`Expected 5 Batch 3 packs, got ${packs.length}`);
    process.exit(2);
  }

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-3",
    reportPrefix: "21-batch3",
    candidateVersion: "factory-2-0-batch-3-v1",
    contract: "BE_FACTORY_2_0_BATCH_3_REVIEW_V1",
    title: "Batch 3",
  });

  // Name resolvers are maintained in brand-explorer-staged-candidate-registry.js
  // (do not auto-insert before the first `return null` — that corrupts byId).

  const feeReq = [
    `# Batch 3 — Fee Exhibit Request (Paid Budget Still $0)`,
    ``,
    `Depth candidates staged for voco, Motto, avid, Tribute Portfolio, and Hotel Indigo.`,
    `Blocker to READY_FOR_APPROVAL: verified fee exhibits.`,
    ``,
    `| Brand | ID | Preferred free source |`,
    `|---|---|---|`,
    `| voco Hotels | \`recwONQTqGU1jHCsM\` | IHG development disclosure for voco |`,
    `| Motto by Hilton | \`reclt44apoi8co0e6\` | Hilton franchise disclosure for Motto |`,
    `| avid hotels | \`recoEarnE8T6sDjZq\` | IHG development disclosure for avid |`,
    `| Tribute Portfolio | \`recCvV0PuZOi8c3hC\` | Marriott FDD / development exhibit for Tribute |`,
    `| Hotel Indigo | \`recegXrqaPiSLGCIe\` | IHG development disclosure for Hotel Indigo |`,
    ``,
    `Paid research not initiated. Do not authorize unrestricted spend from this note alone.`,
  ].join("\n");
  mkdirSync(join(ROOT, "reports/brand-explorer/factory-2-0"), { recursive: true });
  writeFileSync(
    join(ROOT, "reports/brand-explorer/factory-2-0/22-batch3-fee-exhibit-request.md"),
    feeReq + "\n"
  );

  console.log(JSON.stringify(consolidated, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
