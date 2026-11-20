#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 4 depth processing (10 brands).
 * No production Presentation writeback. Continues on per-brand failures.
 */
import "../load-env.js";
import { listBatch4 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import { runDepthBatch } from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();

function registerBatch4InRegistry(results) {
  const registryPath = join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js");
  let src = readFileSync(registryPath, "utf8");
  const packs = listBatch4();
  const byId = new Map(packs.map((p) => [p.brandId, p]));
  const marker = "\n  ];\n\n  function nz(s)";
  if (!src.includes(marker)) {
    console.error("Registry marker missing — manual update required");
    return { ok: false };
  }
  const added = [];
  for (const r of results) {
    if (!r.ok) continue;
    if (src.includes(`id: '${r.brandId}'`)) continue;
    const pack = byId.get(r.brandId);
    if (!pack) continue;
    const entry = `,
    {
      id: '${pack.brandId}',
      slug: '${pack.slug}',
      name: '${pack.brandName}',
      overlayUrl: '/brand-explorer-phase2e-preview/${pack.slug}-factory2-frozen-approval-payload-v1.json',
      scorecardUrl: null,
      baselineUrl: null,
      manifestUrl: null,
      candidateVersion: 'factory-2-0-batch-4-v1',
      source: 'factory-2-0-depth-batch',
      founderApprovalStatus: 'NOT_APPROVED',
      approvalContentHash: '${r.assembledHash}'
    }`;
    src = src.replace(marker, entry + marker);
    added.push(pack.slug);
  }

  // Name resolvers — insert only inside resolveForBrand, before its final return null
  const resolveMarker = "    if (/hotel indigo/i.test(name)) return bySlug('hotel-indigo');\n    return null;\n  }";
  if (src.includes(resolveMarker) && !src.includes("tapestry-collection-by-hilton")) {
    const resolvers = `    if (/hotel indigo/i.test(name)) return bySlug('hotel-indigo');
    if (/tapestry/i.test(name)) return bySlug('tapestry-collection-by-hilton');
    if (/^tempo\\b/i.test(name)) return bySlug('tempo-by-hilton');
    if (/design hotels/i.test(name)) return bySlug('design-hotels');
    if (/^aloft\\b/i.test(name)) return bySlug('aloft-hotels');
    if (/kimpton/i.test(name)) return bySlug('kimpton-hotels');
    if (/bunkhouse/i.test(name)) return bySlug('bunkhouse-hotels');
    if (/hyatt centric/i.test(name)) return bySlug('hyatt-centric');
    if (/courtyard/i.test(name)) return bySlug('courtyard-by-marriott');
    if (/^westin\\b/i.test(name)) return bySlug('westin');
    if (/holiday inn express/i.test(name)) return bySlug('holiday-inn-express');
    return null;
  }`;
    src = src.replace(resolveMarker, resolvers);
  }

  writeFileSync(registryPath, src);
  return { ok: true, added };
}

async function main() {
  const packs = listBatch4();
  if (packs.length !== 10) {
    console.error(`Expected 10 Batch 4 packs, got ${packs.length}`);
    process.exit(2);
  }

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-4",
    reportPrefix: "31-batch4",
    candidateVersion: "factory-2-0-batch-4-v1",
    contract: "BE_FACTORY_2_0_BATCH_4_REVIEW_V1",
    title: "Batch 4 — Ten Brands",
  });

  const registry = registerBatch4InRegistry(consolidated.batchResults);
  consolidated.registryManual = registry;

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  writeFileSync(
    join(dir, "32-batch4-fee-exhibit-request.md"),
    [
      `# Batch 4 — Fee Exhibit Request (Paid Budget Still $0)`,
      ``,
      `Depth candidates staged for 10 brands. Blocker to READY_FOR_APPROVAL: verified fee exhibits.`,
      ``,
      `| Brand | ID | Preferred free source |`,
      `|---|---|---|`,
      ...packs.map(
        (p) =>
          `| ${p.brandName} | \`${p.brandId}\` | ${p.feeGeography} |`
      ),
      ``,
      `Paid research not initiated. Do not authorize unrestricted spend from this note alone.`,
    ].join("\n") + "\n"
  );

  // Refresh consolidated JSON with registry result
  writeFileSync(
    join(dir, "31-batch4-consolidated-review.json"),
    JSON.stringify(consolidated, null, 2) + "\n"
  );

  console.log(JSON.stringify(consolidated, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
