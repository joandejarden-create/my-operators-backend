#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 9 depth processing (20 EXISTING Active brands).
 * Marriott / Hilton / Hyatt enrichment. No Basics creates. No Presentation writeback.
 * $0 paid. Brand-specific Demand Scenario Fit six. Preserves CURRENT; stages Proposed.
 */
import "../load-env.js";
import { listBatch9 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
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
  const packs = listBatch9();
  if (packs.length !== 20) {
    console.error(`Expected 20 Batch 9 packs, got ${packs.length}`);
    process.exit(2);
  }

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
    batchTag: "batch-9",
    reportPrefix: "91-batch9",
    candidateVersion: "factory-2-0-batch-9-v1",
    contract: "BE_FACTORY_2_0_BATCH_9_REVIEW_V1",
    title: "Batch 9 — EXISTING Active Marriott / Hilton / Hyatt Enrichment",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet: [
        "if (/city\\s*express/i.test(name)) return bySlug('city-express-by-marriott');",
        "if (/spring\\s*hill|springhill/i.test(name)) return bySlug('springhill-suites-by-marriott');",
        "if (/studio\\s*res|studiores/i.test(name)) return bySlug('studiores');",
        "if (/towne\\s*place|towneplace/i.test(name)) return bySlug('towneplace-suites-by-marriott');",
        "if (/fairfield/i.test(name)) return bySlug('fairfield-by-marriott');",
        "if (/four\\s*points/i.test(name)) return bySlug('four-points-by-sheraton');",
        "if (/residence\\s*inn/i.test(name)) return bySlug('residence-inn-by-marriott');",
        "if (/^marriott\\s*hotels?\\b/i.test(name) && !/bonvoy|city express|fairfield|spring|towne|residence|delta|autograph|tribute|aloft|moxy|ac hotels|courtyard|westin|sheraton|renaissance|design|bunkhouse|edition|ritz|st\\.?\\s*regis|jw|w hotels|le m/i.test(name)) return bySlug('marriott-hotels');",
        "if (/^sheraton\\b/i.test(name) && !/four\\s*points/i.test(name)) return bySlug('sheraton');",
        "if (/hilton\\s*hotels|hilton\\s*&\\s*resorts/i.test(name)) return bySlug('hilton-hotels-resorts');",
        "if (/double\\s*tree|doubletree/i.test(name)) return bySlug('doubletree-by-hilton');",
        "if (/^hampton\\b/i.test(name)) return bySlug('hampton-by-hilton');",
        "if (/hilton\\s*garden/i.test(name)) return bySlug('hilton-garden-inn');",
        "if (/home\\s*2|home2/i.test(name)) return bySlug('home2-suites-by-hilton');",
        "if (/homewood/i.test(name)) return bySlug('homewood-suites-by-hilton');",
        "if (/\\bspark\\b/i.test(name) && /hilton/i.test(name)) return bySlug('spark-by-hilton');",
        "if (/^tru\\b|tru by hilton/i.test(name)) return bySlug('tru-by-hilton');",
        "if (/hyatt\\s*regency/i.test(name)) return bySlug('hyatt-regency');",
        "if (/thompson/i.test(name)) return bySlug('thompson-hotels');",
        "if (/delta\\s*hotels/i.test(name)) return bySlug('delta-hotels-by-marriott');",
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
    console.error("Synced Batch 9 artifacts to HE 8080 workspace");
  }

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(
    readFileSync(join(dir, "90-batch9-identity-confirm.json"), "utf8")
  );

  writeFileSync(
    join(dir, "92-batch9-canonical-creation-plan.md"),
    [
      `# Batch 9 — Canonical Creation Plan (No Duplicate Basics)`,
      ``,
      `All 20 brands EXISTING_ACTIVE (Active Brand Basics with live Presentation). **New Brand Basics creates: 0.**`,
      `Delta Hotels by Marriott resolved: \`${identity.deltaHotelsResolvedId}\`.`,
      ``,
      `| Brand | ID | Status | Live Presentation blocks |`,
      `|---|---|---|---:|`,
      ...identity.rows.map(
        (r) =>
          `| ${r.expectedName} | \`${r.brandId}\` | ${r.status} | ${r.presentationBlockCount} |`
      ),
      ``,
      `Preserve CURRENT Presentation; stage Proposed enrichment only.`,
      `Franchise fee amounts not invented — FDD / disclosure confirmation language only.`,
      `Demand Scenario Fit: brand-specific exact six (not mechanical repeats).`,
      `Production Presentation writeback blocked until exact-candidate founder approval.`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "92-batch9-fee-exhibit-request.md"),
    [
      `# Batch 9 — FDD / Fee Exhibit & Image CDN Request ($0 paid)`,
      ``,
      `READY blockers: verified current FDD/franchise exhibits + verified property gallery CDN URLs.`,
      `Distinguish historical FDD figures from live contractual terms.`,
      `These brands already have live Presentation — enrichment must not invent fees.`,
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
    deltaHotelsResolvedId: identity.deltaHotelsResolvedId,
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
    join(dir, "91-batch9-consolidated-review.json"),
    JSON.stringify(consolidated, null, 2) + "\n"
  );

  const rows = (consolidated.founderReviewRows || []).map((r) => {
    const pack = packs.find((p) => p.brandId === r.brandId);
    const idRow = identity.rows.find((x) => x.brandId === r.brandId);
    return `| ${r.brand} | \`${r.brandId}\` | EXISTING_ACTIVE | ${idRow?.presentationBlockCount ?? "—"} | ${r.researchCompletion} | ${r.qualityStatus} | ${pack?.loyaltyProgram || "—"} | ${r.candidateHash?.slice(0, 12) || "—"} | ${r.compareStudioUrl || "—"} | ${r.proposedProductUrl || "—"} | ${JSON.stringify(r.expectedAirtableWrites || {})} | $${r.researchCostsUsd || 0} | ${(r.remainingGaps || []).slice(0, 3).join("; ")} |`;
  });
  writeFileSync(
    join(dir, "91-batch9-founder-dashboard.md"),
    [
      `# Batch 9 — Founder Dashboard (EXISTING Active Marriott / Hilton / Hyatt)`,
      ``,
      `Generated: ${consolidated.generatedAt}`,
      `Paid research: $0 · Production Presentation writes: OFF · Customer publication: OFF`,
      `Unique Demand Scenario Fit sets: ${demandSignatures.size}/20`,
      `Delta Hotels by Marriott: \`${identity.deltaHotelsResolvedId}\``,
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
      `| Brand | Canonical ID | Identity | Live blocks | Research | Status | Loyalty | Candidate hash | Compare | Proposed | Expected writes | Spend | Blockers |`,
      `|---|---|---|---:|---|---|---|---|---|---|---|---:|---|`,
      ...rows,
      ``,
      `## Notes`,
      ``,
      `- All 20 already Active with live Presentation (97–166 blocks) — Factory stages Proposed enrichment; CURRENT preserved.`,
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
        deltaHotelsId: identity.deltaHotelsResolvedId,
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
