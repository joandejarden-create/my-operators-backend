#!/usr/bin/env node
/**
 * Brand Explorer — Portfolio Quality Closure Phase 2 ($0 paid).
 *
 * 1) Prioritized image gap queue + easiest evidence-backed fills
 * 2) Fee disclosure reclassification A–E
 * 3) Permanent rules compliance snapshot
 * 4) Near-certification brand list
 * 5) Cumulative Phase 2 founder report
 *
 * No production Airtable writeback. No merge to main (IC-owned).
 *
 * Usage:
 *   node scripts/be-factory-portfolio-quality-closure-phase2-v1.mjs --dry-run
 *   node scripts/be-factory-portfolio-quality-closure-phase2-v1.mjs --apply
 *   node scripts/be-factory-portfolio-quality-closure-phase2-v1.mjs --apply --images-only
 */
import "../load-env.js";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
  copyFileSync,
  renameSync,
  unlinkSync,
} from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";
import { hashPresentationBlocks } from "../lib/brand-explorer/be-founder-approval-publication-v1.js";
import {
  discoverBrandHotelImages,
  harvestImagesFromBlocks,
  loadDiscoveredImagePool,
  poolToGalleryCards,
} from "../lib/brand-explorer/factory-2/be-factory-2-hotel-image-discovery-v1.js";
import {
  classifyFeeDisclosure,
  summarizeFeeClassifications,
  FEE_DISCLOSURE_CLASS,
} from "../lib/brand-explorer/factory-2/be-factory-2-fee-disclosure-classification-v1.js";
import { validateLoyaltyProofExactSix } from "../lib/brand-explorer/be-factory-permanent-loyalty-region-standards-v1.js";
import { validateLoyaltyEliteTier } from "../lib/brand-explorer/be-factory-permanent-loyalty-tier-completeness-v1.js";
import { validateLoyaltyGlanceKpis } from "../lib/brand-explorer/be-factory-permanent-loyalty-glance-kpi-v1.js";
import { evaluateDemandScenariosPermanentGate } from "../lib/brand-explorer/be-factory-permanent-demand-scenarios-six-v1.js";
import { resolveLoyaltyProgramId } from "../lib/brand-explorer/be-factory-permanent-loyalty-tier-completeness-v1.js";

const ROOT = process.cwd();
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const OUT = join(ROOT, "reports/brand-explorer/portfolio-content-quality-audit-v1");
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const apply = process.argv.includes("--apply");
const dryRun = !apply;
const imagesOnly = process.argv.includes("--images-only");

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function writeJsonAtomic(abs, obj) {
  const tmp = `${abs}.tmp-${process.pid}`;
  const body = JSON.stringify(obj, null, 2) + "\n";
  writeFileSync(tmp, body);
  try {
    renameSync(tmp, abs);
  } catch {
    writeFileSync(abs, body);
    try {
      unlinkSync(tmp);
    } catch {
      /* ignore */
    }
  }
}

function countImages(blocks) {
  const gallery = (blocks || []).filter(
    (b) =>
      /^materials\.gallery\.\d+$/.test(b.slotKey || "") &&
      /^https?:\/\//i.test(nz(b.imageUrl))
  ).length;
  const scenario = (blocks || []).filter(
    (b) =>
      /^overview\.scenario\.[123]$/.test(b.slotKey || "") &&
      /^https?:\/\//i.test(nz(b.imageUrl))
  ).length;
  const openings = (blocks || []).filter(
    (b) =>
      /^footprint\.openings/.test(b.slotKey || "") &&
      /^https?:\/\//i.test(nz(b.imageUrl))
  ).length;
  return { gallery, scenario, openings };
}

function patchImages(blocks, cards, brandName) {
  const list = Array.isArray(blocks) ? [...blocks] : [];
  let changed = false;
  const pool = (cards || []).filter((c) => /^https?:\/\//i.test(nz(c.imageUrl)));
  let pi = 0;

  function nextCard() {
    if (pi >= pool.length) return null;
    return pool[pi++];
  }

  // Fill gallery slots 1–6
  for (let i = 1; i <= 6; i++) {
    const sk = `materials.gallery.${i}`;
    let row = list.find((b) => b.slotKey === sk);
    if (row && /^https?:\/\//i.test(nz(row.imageUrl))) continue;
    const card = nextCard();
    if (!card) break;
    if (!row) {
      row = {
        slotKey: sk,
        title: card.caption || `${brandName} — ${card.role || "Hotel"}`,
        body: card.caption || "",
        imageUrl: card.imageUrl,
        sort: 5000 + i,
        active: true,
      };
      list.push(row);
    } else {
      row.imageUrl = card.imageUrl;
      if (!nz(row.title)) row.title = card.caption || row.title;
    }
    changed = true;
  }

  // Fill scenario 1–3 images (keep titles/bodies)
  pi = 0;
  for (let i = 1; i <= 3; i++) {
    const sk = `overview.scenario.${i}`;
    const row = list.find((b) => b.slotKey === sk);
    if (!row) continue;
    if (/^https?:\/\//i.test(nz(row.imageUrl))) continue;
    // Prefer unused pool images
    while (pi < pool.length && list.some((b) => b.imageUrl === pool[pi].imageUrl && b !== row)) {
      pi++;
    }
    const card = pool[pi++];
    if (!card) break;
    row.imageUrl = card.imageUrl;
    changed = true;
  }

  return { blocks: list, changed };
}

function permanentRulesSnapshot(blocks, meta = {}) {
  const programId = resolveLoyaltyProgramId({
    loyaltyProgram: (blocks || []).find((b) => b.slotKey === "loyalty.program")?.title,
    brandName: meta.brandName,
  });
  const proof = validateLoyaltyProofExactSix(blocks, { requireParticipating: true });
  const demand = evaluateDemandScenariosPermanentGate({ blocks, allowEmpty: false });
  const elite = validateLoyaltyEliteTier(blocks, { programId, participates: true });
  const glance = validateLoyaltyGlanceKpis(blocks, {
    participates: programId !== "non_tiered_network",
    notApplicable: programId === "non_tiered_network",
  });
  const imgs = countImages(blocks);
  const website =
    nz(meta.brandWebsite) ||
    nz(meta.sources?.brandPage) ||
    "";
  const fee = classifyFeeDisclosure({
    blocks,
    brandName: meta.brandName,
    parentCompany: meta.parentCompany,
  });

  const checks = {
    proofSix: proof.ok && proof.count === 6,
    demandSix: demand.ok,
    loyaltyElite: elite.ok || programId === "non_tiered_network",
    loyaltyGlance: glance.ok || glance.classification === "KPI_NOT_APPLICABLE",
    gallerySix: imgs.gallery >= 6,
    scenarioThree: imgs.scenario >= 3,
    brandWebsite: Boolean(website) && !/marriott\.com\/?$/i.test(website) && !/hilton\.com\/?$/i.test(website),
    feeClassified: fee.classification !== "UNKNOWN_NEEDS_REVIEW",
  };
  const passCount = Object.values(checks).filter(Boolean).length;
  const blockers = Object.entries(checks)
    .filter(([, v]) => !v)
    .map(([k]) => k);

  // Near-cert = images complete AND at most two other permanent-rule blockers.
  // QUALITY_CERTIFIED still requires authenticated :8080 (never auto).
  const nearCert =
    checks.gallerySix &&
    checks.scenarioThree &&
    blockers.filter((b) => b !== "gallerySix" && b !== "scenarioThree").length <= 2 &&
    passCount >= 6;

  return {
    checks,
    passCount,
    totalChecks: Object.keys(checks).length,
    blockers,
    nearCert,
    fee,
    imgs,
    programId,
  };
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const files = readdirSync(PREVIEW).filter((f) =>
    f.endsWith("-factory2-frozen-approval-payload-v1.json")
  );

  const before = { galleryOk: 0, galleryGap: 0, scenarioOk: 0, scenarioGap: 0 };
  const rows = [];
  const feeRows = [];
  let patchedImages = 0;

  // Baseline pass
  for (const f of files) {
    const j = JSON.parse(readFileSync(join(PREVIEW, f), "utf8"));
    const c = countImages(j.brandExplorer?.blocks);
    if (c.gallery >= 6) before.galleryOk++;
    else before.galleryGap++;
    if (c.scenario >= 3) before.scenarioOk++;
    else before.scenarioGap++;
  }

  // Prioritized queue
  const queue = [];
  for (const f of files) {
    const abs = join(PREVIEW, f);
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const blocks = j.brandExplorer?.blocks || [];
    const c = countImages(blocks);
    let priority = 99;
    let bucket = "ok";
    if (c.gallery >= 4 && c.gallery < 6 && c.scenario >= 3) {
      priority = 1;
      bucket = "gallery_near_miss";
    } else if (c.gallery >= 6 && c.scenario > 0 && c.scenario < 3) {
      priority = 2;
      bucket = "scenario_only";
    } else if (c.gallery >= 6 && c.scenario === 0) {
      priority = 3;
      bucket = "scenario_empty";
    } else if (c.gallery >= 3 && c.gallery < 6) {
      priority = 4;
      bucket = "gallery_partial";
    } else if (c.gallery < 3 || c.scenario < 3) {
      priority = 5;
      bucket = "both_gaps";
    }
    queue.push({
      f,
      abs,
      j,
      c,
      priority,
      bucket,
      slug: j.slug,
      brandName: j.brandName,
      brandId: j.brandId,
    });
  }
  queue.sort((a, b) => a.priority - b.priority || a.brandName.localeCompare(b.brandName));

  writeFileSync(
    join(OUT, "19-image-remediation-priority-queue.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        before,
        buckets: queue.reduce((acc, q) => {
          acc[q.bucket] = (acc[q.bucket] || 0) + 1;
          return acc;
        }, {}),
        queue: queue
          .filter((q) => q.bucket !== "ok")
          .map((q) => ({
            brand: q.brandName,
            slug: q.slug,
            brandId: q.brandId,
            gallery: q.c.gallery,
            scenario: q.c.scenario,
            priority: q.priority,
            bucket: q.bucket,
          })),
      },
      null,
      2
    ) + "\n"
  );

  // Image remediation — easiest first (priorities 1–4)
  for (const q of queue.filter((x) => x.priority <= 4)) {
    const blocks = q.j.brandExplorer?.blocks || [];
    let cards = harvestImagesFromBlocks(blocks, q.slug, q.brandName);
    const pool = loadDiscoveredImagePool(ROOT, q.slug);
    if (pool?.images?.length) {
      const fromPool = poolToGalleryCards(pool, q.brandName);
      const seen = new Set(cards.map((c) => c.imageUrl));
      for (const c of fromPool) {
        if (seen.has(c.imageUrl)) continue;
        cards.push(c);
        seen.add(c.imageUrl);
      }
    }
    // Fresh discovery only when still short
    const needGal = Math.max(0, 6 - q.c.gallery);
    const needSc = Math.max(0, 3 - q.c.scenario);
    if (cards.length < 6 && (needGal > 0 || needSc > 0)) {
      try {
        const disc = await discoverBrandHotelImages(
          ROOT,
          {
            slug: q.slug,
            brandName: q.brandName,
            blocks,
            sources: q.j.sources || {},
            airtableWebsite: q.j.brandWebsite || q.j.sources?.brandPage,
          },
          { need: 6 }
        );
        if (disc?.images?.length) {
          const more = poolToGalleryCards(disc, q.brandName);
          const seen = new Set(cards.map((c) => c.imageUrl));
          for (const c of more) {
            if (seen.has(c.imageUrl)) continue;
            cards.push(c);
            seen.add(c.imageUrl);
          }
        }
      } catch (err) {
        console.error(`discover skip ${q.slug}:`, err.message || err);
      }
      await sleep(120);
    }

    const patch = patchImages(blocks, cards, q.brandName);
    const afterCounts = countImages(patch.blocks);
    const row = {
      brand: q.brandName,
      slug: q.slug,
      bucket: q.bucket,
      before: q.c,
      after: afterCounts,
      changed: patch.changed,
      patched: false,
    };

    if (!dryRun && patch.changed) {
      q.j.brandExplorer.blocks = patch.blocks;
      q.j.approvalContentHash = hashPresentationBlocks(patch.blocks);
      q.j.phase2ImageClosure = {
        remediatedAt: new Date().toISOString(),
        before: q.c,
        after: afterCounts,
        researchSpendUsd: 0,
      };
      writeJsonAtomic(q.abs, q.j);
      if (existsSync(join(HE, "public/brand-explorer-phase2e-preview"))) {
        try {
          copyFileSync(q.abs, join(HE, "public/brand-explorer-phase2e-preview", q.f));
        } catch {
          /* ignore */
        }
      }
      row.patched = true;
      patchedImages += 1;
    }
    rows.push(row);
    console.error(
      `${afterCounts.gallery >= 6 && afterCounts.scenario >= 3 ? "PASS_IMG" : "GAP"} ${q.brandName} gal ${q.c.gallery}→${afterCounts.gallery} sc ${q.c.scenario}→${afterCounts.scenario}`
    );
  }

  // Full portfolio permanent-rules + fee classification (always)
  const compliance = [];
  const after = { galleryOk: 0, galleryGap: 0, scenarioOk: 0, scenarioGap: 0 };
  for (const f of files) {
    const j = JSON.parse(readFileSync(join(PREVIEW, f), "utf8"));
    const blocks = j.brandExplorer?.blocks || [];
    const c = countImages(blocks);
    if (c.gallery >= 6) after.galleryOk++;
    else after.galleryGap++;
    if (c.scenario >= 3) after.scenarioOk++;
    else after.scenarioGap++;

    if (imagesOnly) continue;

    const snap = permanentRulesSnapshot(blocks, {
      brandName: j.brandName,
      parentCompany: j.parentCompany,
      brandWebsite: j.brandWebsite,
      sources: j.sources,
    });
    feeRows.push({
      brandId: j.brandId,
      brand: j.brandName,
      slug: j.slug,
      ...snap.fee,
    });
    compliance.push({
      brandId: j.brandId,
      brand: j.brandName,
      slug: j.slug,
      passCount: snap.passCount,
      totalChecks: snap.totalChecks,
      blockers: snap.blockers,
      nearCert: snap.nearCert,
      checks: snap.checks,
      feeClass: snap.fee.classification,
      gallery: snap.imgs.gallery,
      scenario: snap.imgs.scenario,
      qualityCertified: false, // requires authenticated 8080 — never auto
      browser8080: "NOT_EVALUATED",
    });
  }

  const feeSummary = summarizeFeeClassifications(feeRows);
  const nearCert = compliance.filter((r) => r.nearCert).sort((a, b) => b.passCount - a.passCount);

  const summary = {
    generatedAt: new Date().toISOString(),
    contract: "BE_PORTFOLIO_QUALITY_CLOSURE_PHASE2_V1",
    mode: dryRun ? "dry-run" : "apply",
    researchSpendUsd: 0,
    pr: 491,
    headNote:
      "Feature branch HEAD advanced past founder-reviewed cc408e9d3; current tip includes loyalty tier + glance KPI permanent rules.",
    integration: {
      prOpen: true,
      mergeableVsMain: "BEHIND — IC must update-from-main then merge; BE product chat does not merge",
      behindOriginMain: 13,
      note: "Do not blind-reset worktrees. Preserve dirty ADP/GDI/HE/BE.",
    },
    images: {
      before,
      after,
      prioritizedRemediationPatched: patchedImages,
      stillGalleryGap: after.galleryGap,
      stillScenarioGap: after.scenarioGap,
      durablePools: existsSync(join(ROOT, "data/brand-explorer/factory-2-0/image-pools"))
        ? readdirSync(join(ROOT, "data/brand-explorer/factory-2-0/image-pools")).filter((x) =>
            x.endsWith(".json")
          ).length
        : 0,
    },
    fees: feeSummary,
    feeClassLegend: FEE_DISCLOSURE_CLASS,
    permanentRules: {
      brandsScored: compliance.length,
      nearCertCount: nearCert.length,
      avgPassCount:
        compliance.length === 0
          ? 0
          : Number(
              (
                compliance.reduce((s, r) => s + r.passCount, 0) / compliance.length
              ).toFixed(2)
            ),
      qualityCertifiedCount: 0,
      browser8080: "NOT_EVALUATED_PORTFOLIO",
    },
    PORTFOLIO_AUDIT_COMPLETE: false,
    LOYALTY_TIER_AUDIT_COMPLETE: false,
    LOYALTY_GLANCE_KPI_AUDIT_COMPLETE: false,
  };

  writeFileSync(join(OUT, "20-phase2-closure-summary.json"), JSON.stringify(summary, null, 2) + "\n");
  writeFileSync(
    join(OUT, "21-fee-disclosure-classification.json"),
    JSON.stringify({ generatedAt: summary.generatedAt, summary: feeSummary, rows: feeRows }, null, 2) +
      "\n"
  );
  writeFileSync(
    join(OUT, "22-permanent-rules-compliance.json"),
    JSON.stringify({ generatedAt: summary.generatedAt, rows: compliance }, null, 2) + "\n"
  );
  writeFileSync(
    join(OUT, "23-near-certification-queue.json"),
    JSON.stringify(
      {
        generatedAt: summary.generatedAt,
        count: nearCert.length,
        note: "Near-cert = ≤2 permanent-rule blockers remaining. QUALITY_CERTIFIED still requires authenticated :8080.",
        rows: nearCert.slice(0, 80),
      },
      null,
      2
    ) + "\n"
  );

  const md = [
    `# Brand Explorer — Portfolio Quality Closure Phase 2`,
    ``,
    `Generated: ${summary.generatedAt}`,
    `Mode: \`${summary.mode}\``,
    `Research spend: **$0**`,
    `PR: [#491](https://github.com/joandejarden-create/my-operators-backend/pull/491)`,
    ``,
    `## Integration`,
    ``,
    `- Branch behind \`origin/main\` by **13** commits — **MERGEABLE** but **BEHIND**.`,
    `- Integration Controller owns update-from-main + merge. BE product chat does **not** merge.`,
    `- Preserve dirty ADP / GDI / HE / BE worktrees.`,
    `- Founder-reviewed image closure SHA \`cc408e9d3\` superseded by loyalty tier + glance KPI commits on same PR.`,
    ``,
    `## Images (before → after)`,
    ``,
    `| Metric | Before | After |`,
    `|---|---:|---:|`,
    `| Gallery 6/6 | ${before.galleryOk} | ${after.galleryOk} |`,
    `| Gallery gaps | ${before.galleryGap} | ${after.galleryGap} |`,
    `| Scenario 3/3 | ${before.scenarioOk} | ${after.scenarioOk} |`,
    `| Scenario gaps | ${before.scenarioGap} | ${after.scenarioGap} |`,
    `| Priority image patches | — | ${patchedImages} |`,
    `| Durable image pools | — | ${summary.images.durablePools} |`,
    ``,
    `Priority queue: \`19-image-remediation-priority-queue.json\``,
    ``,
    `## Fee disclosure reclassification`,
    ``,
    `| Class | Count |`,
    `|---|---:|`,
    ...Object.entries(feeSummary.byClass || {}).map(([k, v]) => `| ${k} | ${v} |`),
    ``,
    `- Research gaps (FDD-likely parents still unverified): **${feeSummary.researchGaps}**`,
    `- Certifiable without invented amounts (C/D posture accurate): **${feeSummary.certifiableWithoutAmounts}**`,
    ``,
    `Not Publicly Disclosed ≠ unfinished research when no public schedule exists.`,
    ``,
    `## Permanent rules (independent)`,
    ``,
    `- Proof Points exact 6`,
    `- Demand Scenario Fit exact 6`,
    `- Loyalty Benefits exact 6`,
    `- Loyalty Tiers = actual official count`,
    `- Loyalty at a Glance = 4 meaningful KPIs (Estimated Mix authorized)`,
    `- Brand Website brand-specific`,
    `- Gallery 6 + Scenario 3 where applicable`,
    ``,
    `| Metric | Count |`,
    `|---|---:|`,
    `| Brands scored | ${compliance.length} |`,
    `| Near-cert (≤2 blockers) | ${nearCert.length} |`,
    `| Avg checks passed | ${summary.permanentRules.avgPassCount} / 8 |`,
    `| QUALITY_CERTIFIED | 0 (requires :8080) |`,
    ``,
    `## Near-certification queue (top 25)`,
    ``,
    `| Brand | Pass | Remaining blockers | Fee class |`,
    `|---|---:|---|---|`,
    ...nearCert
      .slice(0, 25)
      .map(
        (r) =>
          `| ${r.brand} | ${r.passCount}/${r.totalChecks} | ${r.blockers.join(", ") || "—"} | ${r.feeClass} |`
      ),
    ``,
    `## Authenticated :8080`,
    ``,
    `**NOT_EVALUATED** at portfolio scale this cycle — brand-by-brand certification required; do not infer from 8082.`,
    ``,
    `## PORTFOLIO_AUDIT_COMPLETE`,
    ``,
    `**NO** — image gaps remain (${after.galleryGap} gallery / ${after.scenarioGap} scenario), fee exhibits pending where Class A/E, and :8080 certification outstanding.`,
    ``,
    `## Paid research request`,
    ``,
    `**None opened.** Initial paid budget remains **$0**. Next consolidated ask only if free discovery saturates on a named blocker set.`,
    ``,
  ].join("\n");

  writeFileSync(join(OUT, "24-phase2-founder-dashboard.md"), md);

  // Update 00-summary pointer
  const sumPath = join(OUT, "00-summary.json");
  if (existsSync(sumPath)) {
    try {
      const s = JSON.parse(readFileSync(sumPath, "utf8"));
      s.phase2Closure = {
        report: "24-phase2-founder-dashboard.md",
        galleryOk: after.galleryOk,
        scenarioOk: after.scenarioOk,
        nearCert: nearCert.length,
        feeResearchGaps: feeSummary.researchGaps,
        PORTFOLIO_AUDIT_COMPLETE: false,
      };
      s.galleryGaps = after.galleryGap;
      writeFileSync(sumPath, JSON.stringify(s, null, 2) + "\n");
    } catch {
      /* ignore */
    }
  }

  console.log(JSON.stringify(summary, null, 2));
  if (dryRun) console.error("Dry run only. Re-run with --apply to write image patches.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
