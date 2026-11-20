#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 7 depth processing (economy / midscale).
 * All ten reuse existing Brand Basics. No Basics creates. No Presentation writeback.
 */
import "../load-env.js";
import { listBatch7 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import {
  runDepthBatch,
  appendRegistryNameResolvers,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { writeFileSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();

async function main() {
  const packs = listBatch7();
  if (packs.length !== 10) {
    console.error(`Expected 10 Batch 7 packs, got ${packs.length}`);
    process.exit(2);
  }

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-7",
    reportPrefix: "61-batch7",
    candidateVersion: "factory-2-0-batch-7-v1",
    contract: "BE_FACTORY_2_0_BATCH_7_REVIEW_V1",
    title: "Batch 7 — Economy / Midscale / Affordable Extended-Stay",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet:
        "if (/^garner\\b/i.test(name)) return bySlug('garner-hotels');\n    if (/sleep inn/i.test(name)) return bySlug('sleep-inn');\n    if (/econo\\s*lodge/i.test(name)) return bySlug('econo-lodge');\n    if (/rodeway/i.test(name)) return bySlug('rodeway-inn');\n    if (/mainstay/i.test(name)) return bySlug('mainstay-suites');\n    if (/days inn/i.test(name)) return bySlug('days-inn-by-wyndham');\n    if (/super\\s*8/i.test(name)) return bySlug('super-8-by-wyndham');\n    if (/microtel/i.test(name)) return bySlug('microtel-by-wyndham');\n    if (/baymont/i.test(name)) return bySlug('baymont-by-wyndham');\n    if (/travelodge/i.test(name)) return bySlug('travelodge-by-wyndham');",
      insertBefore:
        "if (/six senses/i.test(name)) return bySlug('six-senses');\n    if (/hyatt place/i.test(name)) return bySlug('hyatt-place');\n    return null;\n  }",
    },
  ]);

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(
    readFileSync(join(dir, "60-batch7-identity-confirm.json"), "utf8")
  );

  writeFileSync(
    join(dir, "62-batch7-canonical-creation-plan.md"),
    [
      `# Batch 7 — Canonical Creation Plan (No Duplicate Basics)`,
      ``,
      `All ten brands EXISTING_CANONICAL (Under Review). **New Brand Basics creates: 0.**`,
      ``,
      `| Brand | ID | Status | Live Presentation blocks |`,
      `|---|---|---|---:|`,
      ...identity.rows.map(
        (r) =>
          `| ${r.expectedName} | \`${r.brandId}\` | ${r.status} | ${r.presentationBlockCount} |`
      ),
      ``,
      `Franchise fee amounts not invented — FDD / disclosure confirmation language only.`,
      `Production Presentation writeback blocked until exact-candidate founder approval.`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "62-batch7-fee-exhibit-request.md"),
    [
      `# Batch 7 — FDD / Fee Exhibit & Image CDN Request ($0 paid)`,
      ``,
      `READY blockers: verified current FDD/franchise exhibits + verified gallery CDN URLs.`,
      `Distinguish historical FDD figures from live contractual terms.`,
      ``,
      `| Brand | Program | Fee note |`,
      `|---|---|---|`,
      ...packs.map(
        (p) => `| ${p.brandName} | ${p.loyaltyProgram} | ${p.feeGeography} |`
      ),
      ``,
    ].join("\n") + "\n"
  );

  consolidated.identityClassification = {
    allExistingCanonical: true,
    newBrandBasicsCreates: 0,
    rows: identity.rows.map((r) => ({
      brandId: r.brandId,
      brandName: r.expectedName,
      liveName: r.liveName,
      status: r.status,
      classification: r.classification,
      presentationBlockCount: r.presentationBlockCount,
    })),
  };
  writeFileSync(
    join(dir, "61-batch7-consolidated-review.json"),
    JSON.stringify(consolidated, null, 2) + "\n"
  );
  console.log(JSON.stringify(consolidated, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
