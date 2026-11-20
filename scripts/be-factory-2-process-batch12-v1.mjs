#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 12 depth processing (20 brands).
 * Hilton / Accor-Ennismore / Choice. One NEW_BRAND_CANDIDATE (Outset).
 * No invented Airtable rec IDs. No Presentation writeback. $0 paid.
 */
import "../load-env.js";
import { listBatch12 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
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
  const packs = listBatch12();
  if (packs.length !== 20) {
    console.error(`Expected 20 Batch 12 packs, got ${packs.length}`);
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
    batchTag: "batch-12",
    reportPrefix: "121-batch12",
    candidateVersion: "factory-2-0-batch-12-v1",
    contract: "BE_FACTORY_2_0_BATCH_12_REVIEW_V1",
    title: "Batch 12 — Hilton / Accor-Ennismore / Choice",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet: [
        "if (/graduate/i.test(name) && /hilton/i.test(name)) return bySlug('graduate-by-hilton');",
        "if (/livsmart/i.test(name)) return bySlug('livsmart-studios-by-hilton');",
        "if (/embassy\\s*suites/i.test(name)) return bySlug('embassy-suites-by-hilton');",
        "if (/\\bnomad\\b/i.test(name)) return bySlug('nomad-hotels');",
        "if (/ibis\\s*styles/i.test(name)) return bySlug('ibis-styles');",
        "if (/ibis\\s*budget/i.test(name)) return bySlug('ibis-budget');",
        "if (/^greet\\b/i.test(name)) return bySlug('greet');",
        "if (/hotelf1|hotel\\s*f1|formule\\s*1/i.test(name)) return bySlug('hotelf1');",
        "if (/^tribe\\b/i.test(name)) return bySlug('tribe');",
        "if (/adagio\\s*access/i.test(name)) return bySlug('adagio-access');",
        "if (/\\badagio\\b/i.test(name) && !/access/i.test(name)) return bySlug('adagio');",
        "if (/25\\s*hours|25hours/i.test(name)) return bySlug('25hours-hotels');",
        "if (/hoxton/i.test(name)) return bySlug('the-hoxton');",
        "if (/mondrian/i.test(name)) return bySlug('mondrian');",
        "if (/^hyde\\b/i.test(name)) return bySlug('hyde-hotels');",
        "if (/\\bsls\\b/i.test(name)) return bySlug('sls-hotels');",
        "if (/swissotel|swissôtel/i.test(name)) return bySlug('swissotel');",
        "if (/park\\s*plaza/i.test(name)) return bySlug('park-plaza');",
        "if (/park\\s*inn/i.test(name)) return bySlug('park-inn-by-radisson');",
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
    console.error("Synced Batch 12 artifacts to HE 8080 workspace");
  }

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(readFileSync(join(dir, "120-batch12-identity-confirm.json"), "utf8"));

  writeFileSync(
    join(dir, "122-batch12-canonical-creation-plan.md"),
    [
      `# Batch 12 — Canonical Creation Plan`,
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
    join(dir, "122-batch12-fee-exhibit-request.md"),
    [
      `# Batch 12 — FDD / Disclosure Exhibit & Image CDN Request ($0 paid)`,
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
  writeFileSync(join(dir, "121-batch12-consolidated-review.json"), JSON.stringify(consolidated, null, 2) + "\n");

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
    join(dir, "121-batch12-founder-dashboard.md"),
    [
      `# Batch 12 — Founder Dashboard (Hilton / Accor-Ennismore / Choice)`,
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
      `| Existing Airtable | ${identity.existingFound ?? 19} |`,
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
      `- 19 brands reuse live Under Review / Draft Brand Basics IDs — no duplicate creates.`,
      `- Outset Collection by Hilton: provisional \`NEW_outset-collection\` only; Basics create requires founder authorization.`,
      `- Preserve live names (Graduate Hotels, NoMad, Greet, 25Hours Hotel, Hyde, SLS, Swissotel, Adagio Aparthotel, Aparthotel Adagio Access).`,
      `- Park Plaza / Park Inn: Airtable parent may still read Radisson Hotel Group; commercial path is Choice Privileges.`,
      `- Fee amounts remain disclosure-gated (FDD exhibits).`,
      `- No individual founder interrupts — this dashboard is the consolidated 20-brand review.`,
      ``,
      `## Approval readiness`,
      ``,
      `- Production Presentation overwrite: **BLOCKED** until exact-candidate-hash founder approval.`,
      `- Customer publication: **OFF**.`,
      `- NEW brand Basics creates: **BLOCKED** until founder authorizes \`122-batch12-canonical-creation-plan.md\`.`,
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
