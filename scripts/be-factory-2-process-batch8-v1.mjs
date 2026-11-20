#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 8 depth processing (20 brands: Wyndham / BWH / Sonesta).
 * All reuse existing Brand Basics. No Basics creates. No Presentation writeback.
 * $0 paid. Brand-specific Demand Scenario Fit six. Empty-brand slot parity via builder.
 */
import "../load-env.js";
import { listBatch8 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
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
  const packs = listBatch8();
  if (packs.length !== 20) {
    console.error(`Expected 20 Batch 8 packs, got ${packs.length}`);
    process.exit(2);
  }

  // Demand scenarios must be brand-specific (not identical across the batch)
  const demandSignatures = new Set();
  for (const p of packs) {
    const sig = (p.demandScenarios || []).map((d) => `${d.title}|${d.fit}`).join("||");
    demandSignatures.add(sig);
  }
  if (demandSignatures.size < 10) {
    console.error(
      `Demand Scenario sets look too repetitive (${demandSignatures.size} unique / ${packs.length})`
    );
    process.exit(2);
  }

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-8",
    reportPrefix: "81-batch8",
    candidateVersion: "factory-2-0-batch-8-v1",
    contract: "BE_FACTORY_2_0_BATCH_8_REVIEW_V1",
    title: "Batch 8 — Economy / Midscale / Extended-Stay (Wyndham / BWH / Sonesta)",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet: [
        "if (/la\\s*quinta/i.test(name)) return bySlug('la-quinta-by-wyndham');",
        "if (/americinn/i.test(name)) return bySlug('americinn-by-wyndham');",
        "if (/howard\\s*johnson|\\bhojo\\b/i.test(name)) return bySlug('howard-johnson-by-wyndham');",
        "if (/^wingate\\b/i.test(name)) return bySlug('wingate-by-wyndham');",
        "if (/^ramada\\b/i.test(name)) return bySlug('ramada-by-wyndham');",
        "if (/hawthorn/i.test(name)) return bySlug('hawthorn-extended-stay-by-wyndham');",
        "if (/wyndham\\s*garden/i.test(name)) return bySlug('wyndham-garden');",
        "if (/best\\s*western\\s*plus/i.test(name)) return bySlug('best-western-plus');",
        "if (/surestay\\s*studio/i.test(name)) return bySlug('surestay-studio-by-best-western');",
        "if (/surestay\\s*plus/i.test(name)) return bySlug('surestay-plus-by-best-western');",
        "if (/surestay/i.test(name)) return bySlug('surestay-by-best-western');",
        "if (/\\bgl[oō]\\b|glo by best western/i.test(name)) return bySlug('glo-by-best-western');",
        "if (/executive\\s*residency/i.test(name)) return bySlug('executive-residency-by-best-western');",
        "if (/^aiden\\b/i.test(name)) return bySlug('aiden-by-best-western');",
        "if (/^best\\s*western\\b/i.test(name) && !/plus|premier|surestay|aiden|gl/i.test(name)) return bySlug('best-western');",
        "if (/sonesta\\s*select/i.test(name)) return bySlug('sonesta-select');",
        "if (/sonesta\\s*es|\\bes\\s*suites\\b/i.test(name)) return bySlug('sonesta-es-suites');",
        "if (/red\\s*lion/i.test(name)) return bySlug('red-lion-hotels');",
        "if (/signature\\s*inn/i.test(name)) return bySlug('signature-inn');",
        "if (/americas?\\s*best\\s*value|\\babvi\\b/i.test(name)) return bySlug('americas-best-value-inn');",
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
        // Only sync batch-8 new files + always sync all factory2 frozen (safe)
        copyFileSync(join(previewSrc, f), join(previewDest, f));
      }
    }
    console.error("Synced Batch 8 artifacts to HE 8080 workspace");
  }

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(
    readFileSync(join(dir, "80-batch8-identity-confirm.json"), "utf8")
  );

  writeFileSync(
    join(dir, "82-batch8-canonical-creation-plan.md"),
    [
      `# Batch 8 — Canonical Creation Plan (No Duplicate Basics)`,
      ``,
      `All 20 brands EXISTING_CANONICAL (Under Review / Draft). **New Brand Basics creates: 0.**`,
      ``,
      `| Brand | ID | Status | Live Presentation blocks |`,
      `|---|---|---|---:|`,
      ...identity.rows.map(
        (r) =>
          `| ${r.expectedName} | \`${r.brandId}\` | ${r.status} | ${r.presentationBlockCount} |`
      ),
      ``,
      `Franchise fee amounts not invented — FDD / disclosure confirmation language only.`,
      `Demand Scenario Fit: brand-specific exact six (not mechanical repeats).`,
      `Production Presentation writeback blocked until exact-candidate founder approval.`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "82-batch8-fee-exhibit-request.md"),
    [
      `# Batch 8 — FDD / Fee Exhibit & Image CDN Request ($0 paid)`,
      ``,
      `READY blockers: verified current FDD/franchise exhibits + verified property gallery CDN URLs.`,
      `Distinguish historical FDD figures from live contractual terms.`,
      ``,
      `| Brand | Program | Fee geography |`,
      `|---|---|---|`,
      ...packs.map((p) => `| ${p.brandName} | ${p.loyaltyProgram} | ${p.feeGeography} |`),
      ``,
    ].join("\n") + "\n"
  );

  consolidated.identityClassification = {
    allExistingCanonical: true,
    newBrandBasicsCreates: 0,
    uniqueDemandScenarioSets: demandSignatures.size,
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
    join(dir, "81-batch8-consolidated-review.json"),
    JSON.stringify(consolidated, null, 2) + "\n"
  );

  // Founder dashboard markdown
  const rows = (consolidated.founderReviewRows || []).map((r) => {
    const pack = packs.find((p) => p.brandId === r.brandId);
    return `| ${r.brand} | \`${r.brandId}\` | EXISTING_CANONICAL | ${r.researchCompletion} | ${r.qualityStatus} | ${pack?.loyaltyProgram || "—"} | ${r.candidateHash?.slice(0, 12) || "—"} | ${r.compareStudioUrl || "—"} | ${r.proposedProductUrl || "—"} | ${JSON.stringify(r.expectedAirtableWrites || {})} | $${r.researchCostsUsd || 0} | ${(r.remainingGaps || []).slice(0, 3).join("; ")} |`;
  });
  writeFileSync(
    join(dir, "81-batch8-founder-dashboard.md"),
    [
      `# Batch 8 — Founder Dashboard (Wyndham / BWH / Sonesta)`,
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
      ``,
      `## Brands`,
      ``,
      `| Brand | Canonical ID | Identity | Research | Status | Loyalty | Candidate hash | Compare | Proposed | Expected writes | Spend | Blockers |`,
      `|---|---|---|---|---|---|---|---|---|---|---:|---|`,
      ...rows,
      ``,
      `## Notes`,
      ``,
      `- Empty live Presentation for all 20 → Factory depth + Autograph slot-parity completion.`,
      `- Fee amounts remain disclosure-gated (FDD / franchise exhibits).`,
      `- Gallery CDN may use International Reference fixture pools until property-specific CDN is verified.`,
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
