#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 11 depth processing (20 brands).
 * Part A: five EXISTING Active (original inventory remainder).
 * Part B: twelve existing Under Review/Draft + three NEW_BRAND_CANDIDATE (provisional NEW_* keys).
 * No invented Airtable rec IDs. No Presentation writeback. $0 paid.
 */
import "../load-env.js";
import { listBatch11 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
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
  const packs = listBatch11();
  if (packs.length !== 20) {
    console.error(`Expected 20 Batch 11 packs, got ${packs.length}`);
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
  const existingPacks = packs.filter((p) => /^rec[a-zA-Z0-9]+$/.test(String(p.brandId || "")));

  const consolidated = await runDepthBatch(ROOT, packs, {
    batchTag: "batch-11",
    reportPrefix: "111-batch11",
    candidateVersion: "factory-2-0-batch-11-v1",
    contract: "BE_FACTORY_2_0_BATCH_11_REVIEW_V1",
    title:
      "Batch 11 — Part A original inventory remainder + Part B Choice/Wyndham/Hyatt/Sonesta expansion",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet: [
        "if (/radisson\\s*blu/i.test(name)) return bySlug('radisson-blu-by-choice');",
        "if (/comfort\\s*inn\\s*&\\s*suites|comfort\\s*inn\\s*and\\s*suites/i.test(name)) return bySlug('comfort-inn-suites');",
        "if (/radisson\\s*individuals/i.test(name)) return bySlug('radisson-individuals-by-choice');",
        "if (/country\\s*inn\\s*&\\s*suites|country\\s*inn\\s*and\\s*suites/i.test(name)) return bySlug('country-inn-suites-by-choice');",
        "if (/woodspring/i.test(name)) return bySlug('woodspring-suites');",
        "if (/^cambria\\b/i.test(name)) return bySlug('cambria-hotels');",
        "if (/clarion\\s*pointe/i.test(name)) return bySlug('clarion-pointe');",
        "if (/^clarion\\b/i.test(name) && !/pointe/i.test(name)) return bySlug('clarion');",
        "if (/wyndham\\s*grand/i.test(name)) return bySlug('wyndham-grand');",
        "if (/\\bdolce\\b/i.test(name)) return bySlug('dolce-hotels-resorts-by-wyndham');",
        "if (/\\btryp\\b/i.test(name)) return bySlug('tryp-by-wyndham');",
        "if (/esplendor/i.test(name)) return bySlug('esplendor-by-wyndham');",
        "if (/registry\\s*collection/i.test(name)) return bySlug('registry-collection-hotels');",
        "if (/caption\\s*by\\s*hyatt|^caption\\b/i.test(name)) return bySlug('caption-by-hyatt');",
        "if (/joie\\s*de\\s*vivre|\\bjdv\\b/i.test(name)) return bySlug('jdv-by-hyatt');",
        "if (/unbound\\s*collection/i.test(name)) return bySlug('unbound-collection-by-hyatt');",
        "if (/sonesta\\s*simply\\s*suites/i.test(name)) return bySlug('sonesta-simply-suites');",
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
    console.error("Synced Batch 11 artifacts to HE 8080 workspace");
  }

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(readFileSync(join(dir, "110-batch11-identity-confirm.json"), "utf8"));

  writeFileSync(
    join(dir, "112-batch11-canonical-creation-plan.md"),
    [
      `# Batch 11 — Canonical Creation Plan`,
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
      `Do not invent Airtable record IDs. Stage research against provisional keys until authorized create.`,
      `Production Presentation writeback blocked until exact-candidate founder approval.`,
    ].join("\n") + "\n"
  );

  writeFileSync(
    join(dir, "112-batch11-fee-exhibit-request.md"),
    [
      `# Batch 11 — FDD / Disclosure Exhibit & Image CDN Request ($0 paid)`,
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
    partAExistingActive: 5,
    partBExisting: existingPacks.length - 5,
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
  writeFileSync(join(dir, "111-batch11-consolidated-review.json"), JSON.stringify(consolidated, null, 2) + "\n");

  const idByExpected = new Map(identity.rows.map((r) => [r.expectedName, r]));
  const idByBrandId = new Map(identity.rows.filter((r) => r.brandId).map((r) => [r.brandId, r]));

  const rows = (consolidated.founderReviewRows || []).map((r) => {
    const pack = packs.find((p) => p.brandId === r.brandId);
    const idRow =
      idByBrandId.get(r.brandId) ||
      idByExpected.get(r.brand) ||
      identity.rows.find((x) => x.key && pack?.slug?.includes(x.key));
    const classification =
      idRow?.classification ||
      (String(r.brandId || "").startsWith("NEW_") ? "NEW_BRAND_CANDIDATE" : "EXISTING");
    const compare =
      /^rec[a-zA-Z0-9]+$/.test(String(r.brandId || ""))
        ? r.compareStudioUrl || "—"
        : "N/A — awaiting Basics create";
    const proposed =
      /^rec[a-zA-Z0-9]+$/.test(String(r.brandId || ""))
        ? r.proposedProductUrl || "—"
        : `staged:/brand-explorer-phase2e-preview/${pack?.slug}-factory2-frozen-approval-payload-v1.json`;
    return `| ${r.brand} | \`${r.brandId}\` | ${classification} | ${idRow?.presentationBlockCount ?? 0} | ${r.researchCompletion} | ${r.qualityStatus} | ${pack?.loyaltyProgram || "—"} | ${r.candidateHash?.slice(0, 12) || "—"} | ${compare} | ${proposed} | ${JSON.stringify(r.expectedAirtableWrites || {})} | $${r.researchCostsUsd || 0} | ${(r.remainingGaps || []).slice(0, 3).join("; ")} |`;
  });

  writeFileSync(
    join(dir, "111-batch11-founder-dashboard.md"),
    [
      `# Batch 11 — Founder Dashboard (Original Inventory Remainder + 15 Expansion Brands)`,
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
      `| Part A EXISTING Active | 5 |`,
      `| Part B existing Airtable | ${identity.partBExisting ?? 12} |`,
      `| NEW_BRAND_CANDIDATE (Basics create pending) | ${newCandidates.length} |`,
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
      `- Part A five brands: preserve existing Brand Basics / Setup / Presentation / evidence; stage Proposed enrichment only.`,
      `- Part B twelve brands: reuse live Under Review / Draft Brand Basics IDs — no duplicate creates.`,
      `- Three NEW_BRAND_CANDIDATE brands (Hyatt Studios, Hyatt Select, Sonesta Essential Hotels): provisional \`NEW_*\` staging keys only; Basics create requires separate founder authorization bound to creation plan.`,
      `- JdV live name is **Joie de Vivre Hotels**; Unbound live name is **Unbound Collection by Hyatt**; Dolce live name uses "and" not "&".`,
      `- Fee amounts remain disclosure-gated (FDD exhibits).`,
      `- No individual founder interrupts — this dashboard is the consolidated 20-brand review.`,
      ``,
      `## Approval readiness`,
      ``,
      `- Production Presentation overwrite: **BLOCKED** until exact-candidate-hash founder approval.`,
      `- Customer publication: **OFF**.`,
      `- NEW brand Basics creates: **BLOCKED** until founder authorizes \`112-batch11-canonical-creation-plan.md\`.`,
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
