#!/usr/bin/env node
/**
 * Factory 2.0 — Batch 13 depth processing (20 brands).
 * IHG Premium/Suites + Hyatt All-Inclusive & Wellness.
 * Five NEW_BRAND_CANDIDATE brands (Ruby, Noted, HICV, Zoëtry, Alua).
 * No invented Airtable rec IDs. No Presentation writeback. $0 paid.
 */
import "../load-env.js";
import { listBatch13 } from "../lib/brand-explorer/factory-2/be-factory-2-depth-research-packs-v1.js";
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
  const packs = listBatch13();
  if (packs.length !== 20) {
    console.error(`Expected 20 Batch 13 packs, got ${packs.length}`);
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
    batchTag: "batch-13",
    reportPrefix: "131-batch13",
    candidateVersion: "factory-2-0-batch-13-v1",
    contract: "BE_FACTORY_2_0_BATCH_13_REVIEW_V1",
    title: "Batch 13 — IHG Premium/Suites + Hyatt All-Inclusive & Wellness",
  });

  appendRegistryNameResolvers(ROOT, [
    {
      testSnippet: [
        "if (/^regent\\b/i.test(name)) return bySlug('regent-hotels-resorts');",
        "if (/crowne\\s*plaza/i.test(name)) return bySlug('crowne-plaza');",
        "if (/hualuxe/i.test(name)) return bySlug('hualuxe-hotels-resorts');",
        "if (/staybridge/i.test(name)) return bySlug('staybridge-suites');",
        "if (/candlewood/i.test(name)) return bySlug('candlewood-suites');",
        "if (/atwell/i.test(name)) return bySlug('atwell-suites');",
        "if (/^holiday\\s*inn\\b/i.test(name) && !/express|club/i.test(name)) return bySlug('holiday-inn');",
        "if (/hyatt\\s*ziva|^ziva\\b/i.test(name)) return bySlug('hyatt-ziva');",
        "if (/hyatt\\s*zilara|^zilara\\b/i.test(name)) return bySlug('hyatt-zilara');",
        "if (/secrets\\s*resorts|\\bsecrets\\b/i.test(name)) return bySlug('secrets-resorts-spas');",
        "if (/dreams\\s*resorts/i.test(name) || (/\\bdreams\\b/i.test(name) && /resort|spa|hyatt/i.test(name))) return bySlug('dreams-resorts-spas');",
        "if (/breathless/i.test(name)) return bySlug('breathless-resorts-spas');",
        "if (/sunscape/i.test(name)) return bySlug('sunscape-resorts-spas');",
        "if (/hyatt\\s*vivid|^vivid\\b/i.test(name)) return bySlug('hyatt-vivid');",
        "if (/miraval/i.test(name)) return bySlug('miraval-resorts-spas');",
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
    console.error("Synced Batch 13 artifacts to HE 8080 workspace");
  }

  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const identity = JSON.parse(readFileSync(join(dir, "130-batch13-identity-confirm.json"), "utf8"));

  writeFileSync(
    join(dir, "132-batch13-canonical-creation-plan.md"),
    [
      `# Batch 13 — Canonical Creation Plan`,
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
    join(dir, "132-batch13-fee-exhibit-request.md"),
    [
      `# Batch 13 — FDD / Disclosure Exhibit & Image CDN Request ($0 paid)`,
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
  writeFileSync(join(dir, "131-batch13-consolidated-review.json"), JSON.stringify(consolidated, null, 2) + "\n");

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
    join(dir, "131-batch13-founder-dashboard.md"),
    [
      `# Batch 13 — Founder Dashboard (IHG Premium/Suites + Hyatt All-Inclusive & Wellness)`,
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
      `| Existing Airtable | ${identity.existingFound ?? 15} |`,
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
      `- 15 brands reuse live Under Review / Draft Brand Basics IDs — no duplicate creates.`,
      `- Five NEW brands: Ruby Hotels, Noted Collection, Holiday Inn Club Vacations, Zoëtry, Alua — provisional NEW_* keys; Basics create requires founder authorization.`,
      `- Preserve live names (Crowne Plaza, HUALUXE Hotels and Resorts, Hyatt Vivid, Miraval). AI resort economics ≠ transient hotel franchise tables.`,
      `- Inclusive Collection / Ziva-Zilara use all-inclusive underwriting; Club Vacations uses ownership diligence—not hotel royalty tables.`,
      `- Fee amounts remain disclosure-gated (FDD exhibits).`,
      `- No individual founder interrupts — this dashboard is the consolidated 20-brand review.`,
      ``,
      `## Approval readiness`,
      ``,
      `- Production Presentation overwrite: **BLOCKED** until exact-candidate-hash founder approval.`,
      `- Customer publication: **OFF**.`,
      `- NEW brand Basics creates: **BLOCKED** until founder authorizes \`132-batch13-canonical-creation-plan.md\`.`,
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
