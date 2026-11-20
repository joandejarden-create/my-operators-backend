#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 6 depth processing (10 brands).
 * All ten reuse existing Brand Basics (Draft/Under Review). No Basics creates.
 * No production Presentation writeback.
 */
import "../load-env.js";
import { listBatch6 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import {
  runDepthBatch,
  appendRegistryNameResolvers,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { writeFileSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();

async function main() {
  const packs = listBatch6();
  if (packs.length !== 10) {
    console.error(`Expected 10 Batch 6 packs, got ${packs.length}`);
    process.exit(2);
  }

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-6",
    reportPrefix: "51-batch6",
    candidateVersion: "factory-2-0-batch-6-v1",
    contract: "BE_FACTORY_2_0_BATCH_6_REVIEW_V1",
    title: "Batch 6 — Ten Brands (Existing Draft / Under Review)",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet:
        "if (/ritz[- ]?carlton/i.test(name)) return bySlug('ritz-carlton');\n    if (/st\\.?\\s*regis/i.test(name)) return bySlug('st-regis');\n    if (/\\bedition\\b/i.test(name)) return bySlug('edition-hotels');\n    if (/luxury collection/i.test(name)) return bySlug('luxury-collection');\n    if (/renaissance/i.test(name)) return bySlug('renaissance-hotels');\n    if (/signia/i.test(name)) return bySlug('signia-by-hilton');\n    if (/^alila\\b/i.test(name)) return bySlug('alila');\n    if (/hyatt house/i.test(name)) return bySlug('hyatt-house');\n    if (/intercontinental/i.test(name)) return bySlug('intercontinental');\n    if (/six senses/i.test(name)) return bySlug('six-senses');",
      insertBefore:
        "if (/hyatt place/i.test(name)) return bySlug('hyatt-place');\n    return null;\n  }",
    },
  ]);

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(
    readFileSync(join(dir, "50-batch6-identity-confirm.json"), "utf8")
  );

  writeFileSync(
    join(dir, "52-batch6-canonical-creation-plan.md"),
    [
      `# Batch 6 — Canonical Creation Plan (No Duplicate Basics)`,
      ``,
      `## Finding`,
      `Live Brand Basics inventory already contains all ten Batch 6 brands as Draft or Under Review.`,
      `**New Brand Basics creates: 0.** Duplicate creation is forbidden.`,
      ``,
      `| Brand | Canonical ID | Status | Presentation blocks | Basics create |`,
      `|---|---|---|---:|---|`,
      ...identity.rows.map(
        (r) =>
          `| ${r.expectedName} | \`${r.brandId}\` | ${r.status} | ${r.presentationBlockCount} | NO |`
      ),
      ``,
      `## Authorization boundary`,
      `- Research + durable staging: authorized`,
      `- Production Brand Basics create: not required`,
      `- Production Presentation writeback: blocked until exact-candidate founder approval`,
      `- Customer publication: blocked`,
      `- Brand Status promotion: separate founder gate`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "52-batch6-fee-exhibit-request.md"),
    [
      `# Batch 6 — Fee Exhibit & Image CDN Request (Paid Budget Still $0)`,
      ``,
      `Shared READY blockers:`,
      `1. Verified fee / management exhibits (do not invent franchise fees for management-led luxury brands)`,
      `2. Verified property image CDN URLs`,
      ``,
      `| Brand | ID | Fee geography note |`,
      `|---|---|---|`,
      ...packs.map((p) => `| ${p.brandName} | \`${p.brandId}\` | ${p.feeGeography} |`),
      ``,
      `Paid research not initiated.`,
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
    join(dir, "51-batch6-consolidated-review.json"),
    JSON.stringify(consolidated, null, 2) + "\n"
  );

  console.log(JSON.stringify(consolidated, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
