#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 10 depth processing (20 EXISTING Active brands).
 * Accor / Choice / Wyndham / BWH / independent collections.
 * No Basics creates. No Presentation writeback. $0 paid.
 */
import "../load-env.js";
import { listBatch10 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
import {
  runDepthBatch,
  appendRegistryNameResolvers,
} from "../lib/brand-explorer/factory-2/be-factory-2-depth-batch-runner-v1.js";
import { reconcileMasterQueue } from "../lib/brand-explorer/factory-2/be-factory-2-master-queue-reconcile-v1.js";
import { writeFileSync, mkdirSync, readFileSync, copyFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";

async function main() {
  const packs = listBatch10();
  if (packs.length !== 20) {
    console.error(`Expected 20 Batch 10 packs, got ${packs.length}`);
    process.exit(2);
  }

  const demandSignatures = new Set();
  for (const p of packs) {
    demandSignatures.add((p.demandScenarios || []).map((d) => `${d.title}|${d.fit}`).join("||"));
  }
  if (demandSignatures.size < 10) {
    console.error(`Demand Scenario sets too repetitive (${demandSignatures.size} / ${packs.length})`);
    process.exit(2);
  }

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-10",
    reportPrefix: "101-batch10",
    candidateVersion: "factory-2-0-batch-10-v1",
    contract: "BE_FACTORY_2_0_BATCH_10_REVIEW_V1",
    title: "Batch 10 — EXISTING Active Accor / Choice / Wyndham / BWH / Independent Collections",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet: [
        "if (/handwritten/i.test(name)) return bySlug('handwritten-collection');",
        "if (/mgallery|m gallery/i.test(name)) return bySlug('mgallery-collection');",
        "if (/^pullman\\b/i.test(name)) return bySlug('pullman');",
        "if (/^fairmont\\b/i.test(name)) return bySlug('fairmont');",
        "if (/^mercure\\b/i.test(name) && !/grand\\s*mercure/i.test(name)) return bySlug('mercure');",
        "if (/^ibis\\b/i.test(name) && !/styles|budget/i.test(name)) return bySlug('ibis');",
        "if (/mama\\s*shelter/i.test(name)) return bySlug('mama-shelter');",
        "if (/^novotel\\b/i.test(name)) return bySlug('novotel');",
        "if (/^so\\/?$/i.test(name) || /^so\\/\\b/i.test(name) || /\\bso\\/\\b/i.test(name)) return bySlug('so');",
        "if (/bw\\s*premier|premier\\s*collection/i.test(name) && /best\\s*western|bw\\b/i.test(name)) return bySlug('bw-premier-collection');",
        "if (/bw\\s*signature|signature\\s*collection/i.test(name) && /best\\s*western|bw\\b/i.test(name)) return bySlug('bw-signature-collection');",
        "if (/trademark/i.test(name) && /wyndham/i.test(name)) return bySlug('trademark-collection-by-wyndham');",
        "if (/dazzler/i.test(name)) return bySlug('dazzler-by-wyndham');",
        "if (/suburban\\s*studios|^suburban\\b/i.test(name)) return bySlug('suburban-studios');",
        "if (/everhome/i.test(name)) return bySlug('everhome-suites');",
        "if (/^quality\\s*inn\\b/i.test(name)) return bySlug('quality-inn');",
        "if (/radisson\\s*red/i.test(name)) return bySlug('radisson-red-by-choice');",
        "if (/^radisson\\b/i.test(name) && !/red|blu|individuals|collection/i.test(name)) return bySlug('radisson-by-choice');",
        "if (/preferred\\s*hotels/i.test(name)) return bySlug('preferred-hotels-and-resorts');",
        "if (/small\\s*luxury\\s*hotels|\\bslh\\b/i.test(name)) return bySlug('small-luxury-hotels-of-the-world');",
      ].join("\n    "),
      insertBefore:
        "if (/six senses/i.test(name)) return bySlug('six-senses');\n    if (/hyatt place/i.test(name)) return bySlug('hyatt-place');\n    return null;\n  }",
    },
  ]);

  try {
    reconcileMasterQueue({ repoRoot: ROOT, writeReports: true });
  } catch (err) {
    console.error("master queue reconcile warning:", err.message || err);
  }

  if (existsSync(HE)) {
    for (const rel of [
      "public/js/brand-explorer-staged-candidate-registry.js",
      "public/js/brand-explorer-compare-studio.js",
    ]) {
      const src = join(ROOT, rel);
      const dest = join(HE, rel);
      if (existsSync(src) && existsSync(join(HE, "public/js"))) copyFileSync(src, dest);
    }
    const previewSrc = join(ROOT, "public/brand-explorer-phase2e-preview");
    const previewDest = join(HE, "public/brand-explorer-phase2e-preview");
    if (existsSync(previewDest)) {
      for (const f of readdirSync(previewSrc).filter(
        (x) =>
          x.includes("-factory2-frozen-approval-payload-v1.json") ||
          x.includes("-factory2-depth-candidate.json")
      )) {
        copyFileSync(join(previewSrc, f), join(previewDest, f));
      }
    }
    console.error("Synced Batch 10 artifacts to HE 8080 workspace");
  }

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(readFileSync(join(dir, "100-batch10-identity-confirm.json"), "utf8"));

  writeFileSync(
    join(dir, "102-batch10-canonical-creation-plan.md"),
    [
      `# Batch 10 — Canonical Creation Plan (No Duplicate Basics)`,
      ``,
      `All 20 brands EXISTING_ACTIVE. **New Brand Basics creates: 0.**`,
      `Substitutions: ${identity.substitutions?.length || 0}.`,
      ``,
      `| Brand | ID | Status | Live Presentation blocks |`,
      `|---|---|---|---:|`,
      ...identity.rows.map(
        (r) => `| ${r.expectedName} | \`${r.brandId}\` | ${r.status} | ${r.presentationBlockCount} |`
      ),
      ``,
      `Preserve CURRENT Presentation; stage Proposed enrichment only.`,
      `Preferred / SLH: membership exhibits — not franchise royalty tables.`,
      `Production Presentation writeback blocked until exact-candidate founder approval.`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "102-batch10-fee-exhibit-request.md"),
    [
      `# Batch 10 — FDD / Membership Exhibit & Image CDN Request ($0 paid)`,
      ``,
      `| Brand | Program | Fee geography |`,
      `|---|---|---|`,
      ...packs.map((p) => `| ${p.brandName} | ${p.loyaltyProgram} | ${p.feeGeography} |`),
      ``,
    ].join("\n") + "\n"
  );

  consolidated.identityClassification = {
    allExistingActive: true,
    newBrandBasicsCreates: 0,
    substitutions: identity.substitutions || [],
    uniqueDemandScenarioSets: demandSignatures.size,
    rows: identity.rows.map((r) => ({
      brandId: r.brandId,
      brandName: r.expectedName,
      liveName: r.liveName,
      status: r.status,
      presentationBlockCount: r.presentationBlockCount,
    })),
  };
  writeFileSync(join(dir, "101-batch10-consolidated-review.json"), JSON.stringify(consolidated, null, 2) + "\n");

  const rows = (consolidated.founderReviewRows || []).map((r) => {
    const pack = packs.find((p) => p.brandId === r.brandId);
    const idRow = identity.rows.find((x) => x.brandId === r.brandId);
    return `| ${r.brand} | \`${r.brandId}\` | EXISTING_ACTIVE | ${idRow?.presentationBlockCount ?? "—"} | ${r.researchCompletion} | ${r.qualityStatus} | ${pack?.loyaltyProgram || "—"} | ${r.candidateHash?.slice(0, 12) || "—"} | ${r.compareStudioUrl || "—"} | ${r.proposedProductUrl || "—"} | ${JSON.stringify(r.expectedAirtableWrites || {})} | $${r.researchCostsUsd || 0} | ${(r.remainingGaps || []).slice(0, 3).join("; ")} |`;
  });
  writeFileSync(
    join(dir, "101-batch10-founder-dashboard.md"),
    [
      `# Batch 10 — Founder Dashboard (Accor / Choice / Wyndham / BWH / Independent Collections)`,
      ``,
      `Generated: ${consolidated.generatedAt}`,
      `Paid research: $0 · Production Presentation writes: OFF · Customer publication: OFF`,
      `Unique Demand Scenario Fit sets: ${demandSignatures.size}/20`,
      ``,
      `## Summary`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Processed | ${consolidated.summary?.processed ?? "—"} |`,
      `| NEEDS_REMEDIATION | ${consolidated.summary?.needsRemediation ?? "—"} |`,
      `| READY_FOR_REVIEW | ${consolidated.summary?.readyForReview ?? "—"} |`,
      `| BLOCKED | ${consolidated.summary?.blocked ?? "—"} |`,
      `| Research spend USD | ${consolidated.summary?.researchSpendUsdTotal ?? 0} |`,
      `| New Brand Basics creates | 0 |`,
      `| Substitutions | ${identity.substitutions?.length || 0} |`,
      ``,
      `## Brands`,
      ``,
      `| Brand | Canonical ID | Identity | Live blocks | Research | Status | Loyalty / Network | Candidate hash | Compare | Proposed | Expected writes | Spend | Blockers |`,
      `|---|---|---|---:|---|---|---|---|---|---|---|---:|---|`,
      ...rows,
      ``,
      `## Notes`,
      ``,
      `- All 20 already Active — Factory stages Proposed enrichment; CURRENT preserved.`,
      `- Preferred / SLH use membership-network proof cards — not fabricated franchise loyalty economics.`,
      `- Fee amounts remain disclosure-gated (FDD / membership exhibits).`,
      `- No individual founder interrupts — this dashboard is the consolidated review.`,
      ``,
    ].join("\n") + "\n"
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        processed: consolidated.summary?.processed,
        needsRemediation: consolidated.summary?.needsRemediation,
        blocked: consolidated.summary?.blocked,
        uniqueDemandSets: demandSignatures.size,
        spend: consolidated.summary?.researchSpendUsdTotal,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
