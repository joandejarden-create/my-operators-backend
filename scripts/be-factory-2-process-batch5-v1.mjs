#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 5 depth processing (10 brands).
 * All ten reuse existing Brand Basics (Draft/Under Review). No Basics creates.
 * No production Presentation writeback.
 */
import "../load-env.js";
import { listBatch5 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import {
  runDepthBatch,
  appendRegistryNameResolvers,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { writeFileSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();

async function main() {
  const packs = listBatch5();
  if (packs.length !== 10) {
    console.error(`Expected 10 Batch 5 packs, got ${packs.length}`);
    process.exit(2);
  }

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-5",
    reportPrefix: "41-batch5",
    candidateVersion: "factory-2-0-batch-5-v1",
    contract: "BE_FACTORY_2_0_BATCH_5_REVIEW_V1",
    title: "Batch 5 — Ten Brands (Existing Draft / Under Review)",
  });

  // Name resolvers must land only inside resolveForBrand (before its final return null).
  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet:
        "if (/^jw marriott\\b/i.test(name)) return bySlug('jw-marriott');\n    if (/^w hotels\\b/i.test(name)) return bySlug('w-hotels');\n    if (/le m[eé]ridien/i.test(name)) return bySlug('le-meridien');\n    if (/waldorf/i.test(name)) return bySlug('waldorf-astoria');\n    if (/^conrad\\b/i.test(name)) return bySlug('conrad-hotels-resorts');\n    if (/\\blxr\\b/i.test(name)) return bySlug('lxr-hotels-resorts');\n    if (/park hyatt/i.test(name)) return bySlug('park-hyatt');\n    if (/grand hyatt/i.test(name)) return bySlug('grand-hyatt');\n    if (/^andaz\\b/i.test(name)) return bySlug('andaz');\n    if (/hyatt place/i.test(name)) return bySlug('hyatt-place');",
      insertBefore: "if (/holiday inn express/i.test(name)) return bySlug('holiday-inn-express');\n    return null;\n  }",
    },
  ]);

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });

  const identity = JSON.parse(
    readFileSync(join(dir, "40-batch5-identity-confirm.json"), "utf8")
  );

  writeFileSync(
    join(dir, "42-batch5-canonical-creation-plan.md"),
    [
      `# Batch 5 — Canonical Creation Plan (No Duplicate Basics)`,
      ``,
      `## Finding`,
      `Live Brand Basics inventory (265 records) already contains all ten Batch 5 brands as Draft or Under Review.`,
      `**New Brand Basics creates: 0.** Duplicate creation is forbidden.`,
      ``,
      `## Per brand`,
      `| Brand | Canonical ID | Status | Presentation blocks (live) | Basics create | Presentation plan |`,
      `|---|---|---|---:|---|---|`,
      ...identity.rows.map(
        (r) =>
          `| ${r.expectedName} | \`${r.brandId}\` | ${r.status} | ${r.presentationBlockCount} | NO | Dry-run create/update from staged candidate only |`
      ),
      ``,
      `## Authorization boundary`,
      `- Research + durable staging: authorized`,
      `- Production Brand Basics create: not required (EXISTING_CANONICAL)`,
      `- Production Presentation writeback: blocked until exact-candidate founder approval`,
      `- Customer publication: blocked`,
      `- Brand Status promotion (Draft / Under Review → Active/Live): separate founder gate`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "42-batch5-fee-exhibit-request.md"),
    [
      `# Batch 5 — Fee Exhibit & Image CDN Request (Paid Budget Still $0)`,
      ``,
      `Depth candidates staged for 10 brands. Shared READY blockers:`,
      `1. Verified fee exhibits (segment-correct — do not copy luxury fees onto Hyatt Place)`,
      `2. Verified property image CDN URLs (gallery captions staged; imageUrl pending)`,
      ``,
      `| Brand | ID | Segment | Preferred free source |`,
      `|---|---|---|---|`,
      ...packs.map(
        (p) =>
          `| ${p.brandName} | \`${p.brandId}\` | ${p.feeGeography} | ${p.sources?.development || p.sources?.brandPage} |`
      ),
      ``,
      `Paid research not initiated.`,
    ].join("\n") + "\n"
  );

  // Enrich consolidated with identity classification
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
    join(dir, "41-batch5-consolidated-review.json"),
    JSON.stringify(consolidated, null, 2) + "\n"
  );

  console.log(JSON.stringify(consolidated, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
