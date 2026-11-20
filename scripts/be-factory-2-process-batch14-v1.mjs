#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 14 depth processing (20 brands).
 * Accor / Partner + Marriott + Wyndham.
 * Four NEW_BRAND_CANDIDATE brands (MEA, ECHO Suites, WaterWalk, Alltra).
 * No invented Airtable rec IDs. No Presentation writeback. $0 paid.
 */
import "../load-env.js";
import { listBatch14 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
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
  const packs = listBatch14();
  if (packs.length !== 20) {
    console.error(`Expected 20 Batch 14 packs, got ${packs.length}`);
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

  const newCandidates = packs.filter((p) => String(p.brandId || "").startsWith("NEW_"));

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-14",
    reportPrefix: "141-batch14",
    candidateVersion: "factory-2-0-batch-14-v1",
    contract: "BE_FACTORY_2_0_BATCH_14_REVIEW_V1",
    title: "Batch 14 — Accor / Partner + Marriott + Wyndham",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet: [
        "if (/sofitel\\s*legend/i.test(name)) return bySlug('sofitel-legend');",
        "if (/^sofitel\\b/i.test(name) && !/legend/i.test(name)) return bySlug('sofitel');",
        "if (/\\braffles\\b/i.test(name)) return bySlug('raffles-hotels-resorts');",
        "if (/m[oö]venpick|movenpick/i.test(name)) return bySlug('movenpick-hotels-resorts');",
        "if (/grand\\s*mercure/i.test(name)) return bySlug('grand-mercure');",
        "if (/\\bpeppers\\b/i.test(name)) return bySlug('peppers');",
        "if (/\\bmantra\\b/i.test(name)) return bySlug('mantra');",
        "if (/break\\s*free|breakfree/i.test(name)) return bySlug('breakfree');",
        "if (/\\bsebel\\b/i.test(name)) return bySlug('the-sebel');",
        "if (/banyan\\s*tree/i.test(name)) return bySlug('banyan-tree');",
        "if (/\\bangsana\\b/i.test(name)) return bySlug('angsana');",
        "if (/art\\s*series/i.test(name)) return bySlug('art-series-hotels');",
        "if (/citizen\\s*m|citizenm/i.test(name)) return bySlug('citizenm');",
        "if (/\\belement\\b/i.test(name) && /westin|hotel/i.test(name)) return bySlug('element-hotels');",
        "if (/\\bprotea\\b/i.test(name)) return bySlug('protea-hotels-by-marriott');",
        "if (/four\\s*points\\s*flex/i.test(name)) return bySlug('four-points-flex-by-sheraton');",
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
    console.error("Synced Batch 14 artifacts to HE 8080 workspace");
  }

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(readFileSync(join(dir, "140-batch14-identity-confirm.json"), "utf8"));

  writeFileSync(
    join(dir, "142-batch14-canonical-creation-plan.md"),
    [
      `# Batch 14 — Canonical Creation Plan`,
      ``,
      `## Existing Brand Basics (reuse — no duplicate creates)`,
      ``,
      `| Brand | ID | Classification | Live Presentation blocks |`,
      `|---|---|---|---:|`,
      ...identity.rows
        .filter((r) => r.brandId)
        .map(
          (r) =>
            `| ${r.expectedName} | \`${r.brandId}\` | ${r.classification} | ${r.presentationBlockCount} |`
        ),
      ``,
      `## NEW_BRAND_CANDIDATE (Basics create AFTER founder auth)`,
      ``,
      `| Brand | Provisional staging key | Proposed action |`,
      `|---|---|---|`,
      ...(identity.creationPlan || []).map(
        (c) => `| ${c.brandName} | \`NEW_${c.key}\` | ${c.proposedAction} |`
      ),
      ``,
      `**New Brand Basics creates proposed: ${identity.newBrandBasicsCreatesProposed || 0}.**`,
      `Do not invent Airtable record IDs.`,
      `Production Presentation writeback blocked until exact-candidate founder approval.`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "142-batch14-fee-exhibit-request.md"),
    [
      `# Batch 14 — FDD / Disclosure Exhibit & Image CDN Request ($0 paid)`,
      ``,
      `| Brand | Program | Fee geography | Staging ID |`,
      `|---|---|---|---|`,
      ...packs.map(
        (p) =>
          `| ${p.brandName} | ${p.loyaltyProgram} | ${p.feeGeography} | \`${p.brandId}\` |`
      ),
      ``,
    ].join("\n") + "\n"
  );

  consolidated.identityClassification = {
    existingFound: identity.existingFound,
    newBrandBasicsCreatesProposed: newCandidates.length,
    newBrandProvisionalKeys: newCandidates.map((p) => p.brandId),
    substitutions: identity.substitutions || [],
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
  writeFileSync(join(dir, "141-batch14-consolidated-review.json"), JSON.stringify(consolidated, null, 2) + "\n");

  const idByBrandId = new Map(identity.rows.filter((r) => r.brandId).map((r) => [r.brandId, r]));
  const idByExpected = new Map(identity.rows.map((r) => [r.expectedName, r]));

  const rows = (consolidated.founderReviewRows || []).map((r) => {
    const pack = packs.find((p) => p.brandId === r.brandId);
    const idRow = idByBrandId.get(r.brandId) || idByExpected.get(r.brand);
    const classification =
      idRow?.classification ||
      (String(r.brandId || "").startsWith("NEW_") ? "NEW_BRAND_CANDIDATE" : "EXISTING");
    const compare = /^rec[a-zA-Z0-9]+$/.test(String(r.brandId || ""))
      ? r.compareStudioUrl || "—"
      : "N/A — awaiting Basics create";
    const proposed = /^rec[a-zA-Z0-9]+$/.test(String(r.brandId || ""))
      ? r.proposedProductUrl || "—"
      : `staged:/brand-explorer-phase2e-preview/${pack?.slug}-factory2-frozen-approval-payload-v1.json`;
    return `| ${r.brand} | \`${r.brandId}\` | ${classification} | ${idRow?.presentationBlockCount ?? 0} | ${r.researchCompletion} | ${r.qualityStatus} | ${pack?.loyaltyProgram || "—"} | ${r.candidateHash?.slice(0, 12) || "—"} | ${compare} | ${proposed} | ${JSON.stringify(r.expectedAirtableWrites || {})} | $${r.researchCostsUsd || 0} | ${(r.remainingGaps || []).slice(0, 3).join("; ")} |`;
  });

  writeFileSync(
    join(dir, "141-batch14-founder-dashboard.md"),
    [
      `# Batch 14 — Founder Dashboard (Accor / Partner + Marriott + Wyndham)`,
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
      `| Existing Airtable | ${identity.existingFound ?? 16} |`,
      `| NEW_BRAND_CANDIDATE | ${newCandidates.length} |`,
      `| Substitutions | ${identity.substitutions?.length || 0} |`,
      ``,
      `## Brands`,
      ``,
      `| Brand | Canonical / Staging ID | Identity | Live blocks | Research | Status | Loyalty | Candidate hash | Compare | Proposed | Expected writes | Spend | Blockers |`,
      `|---|---|---|---:|---|---|---|---|---|---|---|---:|---|`,
      ...rows,
      ``,
      `## Notes`,
      ``,
      `- 16 brands reuse live Under Review / Draft Brand Basics IDs — no duplicate creates.`,
      `- Four NEW brands: Marriott Executive Apartments, ECHO Suites, WaterWalk, Wyndham Alltra — provisional NEW_* keys; Basics create requires founder authorization.`,
      `- Preserve live names (Raffles, Mövenpick, BreakFree Resort, Art Series, Element by Westin, citizenM).`,
      `- Banyan Tree / Angsana are partner-affiliated (Banyan Tree Holdings + Accor ALL distribution)—do not import Accor-owned franchise tables.`,
      `- Alltra uses all-inclusive underwriting; ECHO / WaterWalk / Four Points Flex are newer brands—no invented mature metrics.`,
      `- Fee amounts remain disclosure-gated (FDD / partnership exhibits).`,
      `- No individual founder interrupts — this dashboard is the consolidated 20-brand review.`,
      ``,
      `## Approval readiness`,
      ``,
      `- Production Presentation overwrite: **BLOCKED** until exact-candidate-hash founder approval.`,
      `- Customer publication: **OFF**.`,
      `- NEW brand Basics creates: **BLOCKED** until founder authorizes \`142-batch14-canonical-creation-plan.md\`.`,
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
        newCandidates: newCandidates.map((p) => p.brandId),
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
