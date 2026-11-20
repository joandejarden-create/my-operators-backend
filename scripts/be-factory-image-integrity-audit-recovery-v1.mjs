#!/usr/bin/env node
/**
 * Brand Explorer Factory — Image Integrity Audit & Recovery V1
 *
 * Audits required gallery + scenario slots across frozen Factory candidates
 * (and optional live Presentation). Runs fetch+decode+visual pipeline,
 * optional bounded recovery from authorized image pools, and emits
 * before/after scorecards. Does NOT merge, deploy, pay for research, or
 * publish to customers.
 *
 *   node scripts/be-factory-image-integrity-audit-recovery-v1.mjs --dry-run
 *   node scripts/be-factory-image-integrity-audit-recovery-v1.mjs --dry-run --limit 20
 *   node scripts/be-factory-image-integrity-audit-recovery-v1.mjs --dry-run --slugs ac-hotels-by-marriott
 *   node scripts/be-factory-image-integrity-audit-recovery-v1.mjs --apply-recovery --limit 5
 *
 * Browser CERTIFIED requires a separate pass:
 *   node scripts/be-factory-image-integrity-browser-cert-v1.mjs --slugs ...
 */
import "../load-env.js";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "fs";
import { join } from "path";
import {
  BE_FACTORY_IMAGE_INTEGRITY_V1,
  REQUIRED_IMAGE_SLOTS,
  evaluateBrandBlocksImageIntegrity,
  buildImageIntegrityPortfolioSection,
  extractImageUrl,
  IMAGE_INTEGRITY_STATUS,
} from "../lib/brand-explorer/factory-2/be-factory-image-integrity-v1.js";
import {
  recoverBrandImageSlots,
  applyRecoveredUrlsToBlocks,
} from "../lib/brand-explorer/factory-2/be-factory-image-integrity-recovery-v1.js";

const ROOT = process.cwd();
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const OUT = join(ROOT, "reports/brand-explorer/factory-image-integrity-v1");

function parseArgs(argv) {
  const out = {
    dryRun: true,
    applyRecovery: false,
    allowDiscovery: false,
    allowTechnicalCertify: false,
    limit: 0,
    slugs: new Set(),
    concurrency: 3,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply-recovery") {
      out.applyRecovery = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--allow-discovery") out.allowDiscovery = true;
    else if (a === "--allow-technical-certify") out.allowTechnicalCertify = true;
    else if (a === "--limit" && argv[i + 1]) out.limit = Number(argv[++i]) || 0;
    else if (a.startsWith("--limit=")) out.limit = Number(a.split("=")[1]) || 0;
    else if (a === "--slugs" && argv[i + 1]) {
      String(argv[++i])
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
        .forEach((s) => out.slugs.add(s));
    } else if (a === "--concurrency" && argv[i + 1]) {
      out.concurrency = Math.max(1, Number(argv[++i]) || 3);
    }
  }
  return out;
}

function listFrozen() {
  if (!existsSync(PREVIEW)) return [];
  return readdirSync(PREVIEW)
    .filter((f) => f.endsWith("-factory2-frozen-approval-payload-v1.json"))
    .map((f) => {
      const slug = f.replace(/-factory2-frozen-approval-payload-v1\.json$/i, "");
      return { slug, file: f, abs: join(PREVIEW, f) };
    })
    .sort((a, b) => a.slug.localeCompare(b.slug));
}

function legacyUrlScore(blocks) {
  let populated = 0;
  for (const slot of REQUIRED_IMAGE_SLOTS) {
    const b = (blocks || []).find((x) => x.slotKey === slot);
    if (b && extractImageUrl(b)) populated += 1;
  }
  return {
    populated,
    total: REQUIRED_IMAGE_SLOTS.length,
    completion: populated / REQUIRED_IMAGE_SLOTS.length,
  };
}

async function mapPool(items, concurrency, fn) {
  const out = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return out;
}

async function main() {
  const args = parseArgs(process.argv);
  mkdirSync(OUT, { recursive: true });
  mkdirSync(join(OUT, "brands"), { recursive: true });

  let frozen = listFrozen();
  if (args.slugs.size) frozen = frozen.filter((f) => args.slugs.has(f.slug));
  // Prioritize AC Hotels then alphabetical
  frozen.sort((a, b) => {
    if (a.slug === "ac-hotels-by-marriott") return -1;
    if (b.slug === "ac-hotels-by-marriott") return 1;
    return a.slug.localeCompare(b.slug);
  });
  if (args.limit > 0) frozen = frozen.slice(0, args.limit);

  console.error(
    `[image-integrity] auditing ${frozen.length} frozen candidates (recovery=${args.applyRecovery})`
  );

  const brandRows = await mapPool(frozen, args.concurrency, async (entry) => {
    const json = JSON.parse(readFileSync(entry.abs, "utf8"));
    const blocks = json.brandExplorer?.blocks || json.blocks || [];
    const brandId = json.brandId || null;
    const brandName = json.brandName || entry.slug;
    const beforeLegacy = legacyUrlScore(blocks);

    const before = await evaluateBrandBlocksImageIntegrity(
      blocks,
      { brandId, brandName, slug: entry.slug, repoRoot: ROOT },
      {
        allowTechnicalCertify: args.allowTechnicalCertify,
        recordRejections: true,
      }
    );

    let after = before;
    let recovery = null;
    let patchedBlocks = blocks;

    if (args.applyRecovery) {
      recovery = await recoverBrandImageSlots({
        repoRoot: ROOT,
        brandId,
        brandName,
        slug: entry.slug,
        slotResults: before.slotResults,
        blocks,
        opts: {
          allowDiscovery: args.allowDiscovery,
          allowTechnicalCertify: args.allowTechnicalCertify,
          brandWebsite: json.brandWebsite || null,
        },
      });
      const applied = applyRecoveredUrlsToBlocks(blocks, recovery, {
        allowOverwriteApproved: false,
      });
      patchedBlocks = applied.blocks;
      after = await evaluateBrandBlocksImageIntegrity(
        patchedBlocks,
        { brandId, brandName, slug: entry.slug, repoRoot: ROOT },
        {
          allowTechnicalCertify: args.allowTechnicalCertify,
          recordRejections: true,
        }
      );
    }

    const row = {
      slug: entry.slug,
      brandId,
      brandName,
      frozenFile: entry.file,
      before: {
        legacyUrlPopulationCompletion: Number(beforeLegacy.completion.toFixed(4)),
        legacyPopulated: beforeLegacy.populated,
        score: before.score,
        gate: before.gate,
      },
      after: {
        score: after.score,
        gate: after.gate,
      },
      repair: recovery
        ? {
            attempts: recovery.attempts,
            successes: recovery.successes,
            discoveries: recovery.discoveries,
            unresolved: recovery.results.filter((r) => r.unresolvedGap).length,
          }
        : { attempts: 0, successes: 0, discoveries: 0, unresolved: null },
      rootCauses: (before.slotResults || [])
        .filter((s) => s.status !== IMAGE_INTEGRITY_STATUS.CERTIFIED)
        .map((s) => ({
          slotKey: s.slotKey,
          status: s.status,
          reason: s.reason || null,
          failureClass: s.failureClass || s.visual?.failureClass || null,
          url: s.url || null,
        })),
      score: after.score,
    };

    writeFileSync(
      join(OUT, "brands", `${entry.slug}.json`),
      JSON.stringify(
        {
          contract: BE_FACTORY_IMAGE_INTEGRITY_V1,
          ...row,
          beforeSlots: before.slotResults,
          afterSlots: after.slotResults,
          recovery,
        },
        null,
        2
      ) + "\n"
    );
    process.stderr.write(
      `  ${entry.slug}: legacy=${beforeLegacy.populated}/9 certified=${after.score.certifiedImages}/9 broken=${after.score.invalidBrokenImages} missing=${after.score.missingImages}\n`
    );
    return row;
  });

  const portfolio = buildImageIntegrityPortfolioSection(brandRows);
  const beforeLegacyAvg =
    brandRows.reduce((n, r) => n + (r.before?.legacyUrlPopulationCompletion || 0), 0) /
    (brandRows.length || 1);
  const beforeCertifiedAvg =
    brandRows.reduce(
      (n, r) => n + (r.before?.score?.certifiedImageCompletion || 0),
      0
    ) / (brandRows.length || 1);
  const afterCertifiedAvg =
    brandRows.reduce((n, r) => n + (r.after?.score?.certifiedImageCompletion || 0), 0) /
    (brandRows.length || 1);

  const summary = {
    contract: "BE_FACTORY_IMAGE_INTEGRITY_AUDIT_RECOVERY_V1",
    parentContract: BE_FACTORY_IMAGE_INTEGRITY_V1,
    generatedAt: new Date().toISOString(),
    mode: args.applyRecovery ? "AUDIT_WITH_RECOVERY" : "AUDIT_DRY_RUN",
    allowDiscovery: args.allowDiscovery,
    allowTechnicalCertify: args.allowTechnicalCertify,
    brandsAudited: brandRows.length,
    portfolio,
    beforeAfter: {
      previouslyClaimedImageCompletionAvg: Number(beforeLegacyAvg.toFixed(4)),
      browserOrTechnicalCertifiedBeforeAvg: Number(beforeCertifiedAvg.toFixed(4)),
      certifiedAfterAvg: Number(afterCertifiedAvg.toFixed(4)),
      correctionDisclosed:
        "Previous factory scoring treated URL population as image completion. Certified Image Completion only counts CERTIFIED slots after fetch+decode(+browser) + visual validation.",
    },
    brandsFullyCertified: brandRows
      .filter((r) => r.score?.meetsFactoryApprovalThreshold)
      .map((r) => r.slug),
    brandsWithBroken: brandRows
      .filter((r) => (r.score?.invalidBrokenImages || 0) > 0)
      .map((r) => ({
        slug: r.slug,
        broken: r.score.invalidBrokenImages,
      })),
    acHotels: brandRows.find((r) => r.slug === "ac-hotels-by-marriott") || null,
    paidResearchUsd: 0,
    customerPublication: "NOT_AUTHORIZED",
    mergeDeployment: "STOP_FOR_FOUNDER_REVIEW",
  };

  writeFileSync(join(OUT, "00-audit-summary.json"), JSON.stringify(summary, null, 2) + "\n");
  writeFileSync(
    join(OUT, "01-brand-matrix.json"),
    JSON.stringify({ generatedAt: summary.generatedAt, rows: brandRows }, null, 2) + "\n"
  );

  const md = [
    `# Brand Explorer Factory — Image Integrity Audit V1`,
    ``,
    `**Generated:** ${summary.generatedAt}`,
    `**Mode:** ${summary.mode}`,
    `**Brands audited:** ${summary.brandsAudited}`,
    ``,
    `## Correction disclosure`,
    ``,
    summary.beforeAfter.correctionDisclosed,
    ``,
    `| Metric | Value |`,
    `|---|---|`,
    `| Previously claimed (URL population) avg | ${(100 * beforeLegacyAvg).toFixed(1)}% |`,
    `| Certified completion before recovery avg | ${(100 * beforeCertifiedAvg).toFixed(1)}% |`,
    `| Certified completion after avg | ${(100 * afterCertifiedAvg).toFixed(1)}% |`,
    `| Brands fully certified | ${portfolio.brandsWithFullyCertifiedRequiredImagery} |`,
    `| Broken images | ${portfolio.brokenImageCount} |`,
    `| Missing images | ${portfolio.missingImageCount} |`,
    `| Visual review required | ${portfolio.imagesRequiringVisualReview} |`,
    `| Technical recovery success rate | ${portfolio.technicalRecoverySuccessRate ?? "n/a"} |`,
    ``,
    `## AC Hotels by Marriott`,
    ``,
    summary.acHotels
      ? JSON.stringify(
          {
            legacy: summary.acHotels.before?.legacyUrlPopulationCompletion,
            certifiedBefore: summary.acHotels.before?.score?.certifiedImageCompletion,
            certifiedAfter: summary.acHotels.after?.score?.certifiedImageCompletion,
            broken: summary.acHotels.score?.invalidBrokenImages,
            missing: summary.acHotels.score?.missingImages,
            rootCauses: summary.acHotels.rootCauses?.slice(0, 9),
          },
          null,
          2
        )
      : "_Not in this audit slice._",
    ``,
    `## STOP`,
    ``,
    `No merge, deployment, paid research, destructive persistence, or customer publication in this run.`,
    ``,
  ].join("\n");
  writeFileSync(join(OUT, "02-founder-audit-report.md"), md);

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
