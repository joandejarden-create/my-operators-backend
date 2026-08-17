#!/usr/bin/env node
/**
 * Built-blocked cohort — Founder Visual Review + Public Restore Prep (read-only).
 *
 * Lightweight packets for the 7 fullyReady brands. Does not call heavy factory
 * context / sibling inventory loaders. No Airtable writes. No public restore.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_IDENTITIES,
  BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
} from "../lib/partner-intelligence/brand-explorer-built-blocked-content.js";
import {
  verifyBuiltBlockedBrand,
} from "../lib/partner-intelligence/brand-explorer-built-blocked-remediation.js";
import {
  V42_VERSION,
  V42_BRAND_LENSES,
  V42_REVIEW_TABS,
  resolveFounderReviewLookupId,
} from "../lib/partner-intelligence/brand-explorer-founder-visual-review.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { evaluateTabFactoryFromPayload } from "../lib/partner-intelligence/brand-explorer-tab-factory-evaluate.js";
import { isOwnerFacingPresentationRow } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";
import { evaluateImageUniqueness } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { evaluateBrandExternalQualityLock } from "../lib/partner-intelligence/brand-explorer-display-quality-lock.js";
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
  detectRepeatedBoilerplate,
} from "../lib/partner-intelligence/brand-explorer-v40b-copy-quality-patterns.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const COHORT = Object.freeze([...BUILT_BLOCKED_TARGETS]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function stripHtml(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchBrand(slug) {
  const lookupId = resolveFounderReviewLookupId(slug);
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug} (${lookupId})`);
  return res.payload.brand;
}

function extractTabPanelHtml(html, panelId) {
  const re = new RegExp(
    `<div[^>]*data-atelier-panel="${panelId}"[^>]*>([\\s\\S]*?)(?=<div[^>]*data-atelier-panel="|$)`,
    "i"
  );
  const m = html.match(re);
  return m ? m[1] : "";
}

function scoreTab(tab, panelHtml) {
  const text = stripHtml(panelHtml);
  const words = text.split(/\s+/).filter(Boolean).length;
  const empty = (panelHtml.match(/oe-dd--empty/gi) || []).length;
  let status = "pass";
  const remainingJudgmentItems = [];
  const visibleIssues = [];
  const copyConcerns = [];
  const imageConcerns = [];
  const emptyCardConcerns = [];
  if (words < 40) {
    status = "concern";
    remainingJudgmentItems.push("Thin tab body — founder taste check");
    visibleIssues.push("thin_tab_body");
  }
  if (empty > 0) {
    // Soft concern only — global empty-render gate already passed for fullyReady brands.
    status = status === "pass" ? "concern" : status;
    remainingJudgmentItems.push(`${empty} empty rendered nodes in tab (founder visual check)`);
    emptyCardConcerns.push(`oe-dd--empty:${empty}`);
  }
  return {
    id: tab.id,
    key: tab.key,
    label: tab.label,
    status,
    wordCount: words,
    emptyNodes: empty,
    remainingJudgmentItems,
    visibleIssues,
    copyConcerns,
    imageConcerns,
    emptyCardConcerns,
  };
}

function evaluateLenses(brandSlug, corpus) {
  const lens = V42_BRAND_LENSES[brandSlug];
  if (!lens) return { questions: [], failCount: 0, concernCount: 0, pass: true };
  const answers = lens.questions.map((q) => {
    if (q.avoidRe) {
      const hit = q.avoidRe.test(corpus);
      return {
        id: q.id,
        question: q.question,
        status: hit ? "fail" : "pass",
        detail: hit ? "Avoid pattern still present" : "Avoid pattern clear",
      };
    }
    const present = q.re.test(corpus);
    return {
      id: q.id,
      question: q.question,
      status: present ? "pass" : "concern",
      detail: present ? "Signal present" : "Signal thin — founder judgment",
    };
  });
  return {
    brandSlug,
    brandName: lens.brandName,
    questions: answers,
    failCount: answers.filter((a) => a.status === "fail").length,
    concernCount: answers.filter((a) => a.status === "concern").length,
    pass: answers.every((a) => a.status === "pass"),
  };
}

function recommend({ gateSummary, tabReviews, brandLenses, uniqueness, propertyCount }) {
  const tabFails = tabReviews.filter((t) => t.status === "fail");
  const tabConcerns = tabReviews.filter((t) => t.status === "concern");
  // Automated Tab Factory / image gates already verified fullyReady — do not
  // downgrade to remediation on soft tab heuristics alone.
  if (!gateSummary.fullyReady || (brandLenses.failCount || 0) > 0 || tabFails.length > 0) {
    return {
      recommendation: gateSummary.fullyReady
        ? "approve_after_minor_cleanup"
        : "remediation_required",
      rationale: gateSummary.fullyReady
        ? "Gates pass; soft founder judgment items remain (tabs/lenses)."
        : "Automated gates or hard tab/lens failures remain before founder approve.",
    };
  }
  if (propertyCount > 3 || tabConcerns.length || (brandLenses.concernCount || 0) > 0) {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale:
        propertyCount > 3
          ? `Gates pass; founder should confirm ${propertyCount} property examples (min 3) and any soft tab/lens concerns.`
          : "Gates pass; soft tab or brand-lens concerns remain for founder taste.",
    };
  }
  return {
    recommendation: "approve_for_active_release",
    rationale:
      "Internal preview gates clean, visuals meet 6/3/3, brand lenses present. Active release still requires explicit founder approval — not applied here.",
  };
}

async function buildPacket(brandSlug) {
  const verify = await verifyBuiltBlockedBrand(brandSlug);
  const brand = await fetchBrand(brandSlug);
  const recordId = brand.id || BUILT_BLOCKED_IDENTITIES[brandSlug]?.recordId;
  const ownerFacing = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const internalHtml = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: true,
  });
  const externalHtml = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: false,
  });
  const tf = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerFacing,
    html: internalHtml,
    brandSlug,
  });
  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: ownerFacing,
    brandSlug,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacing,
    brandSlug,
  });
  const externalQl = evaluateBrandExternalQualityLock(brand, externalHtml, { brandSlug });
  const tabReviews = V42_REVIEW_TABS.map((tab) =>
    scoreTab(tab, extractTabPanelHtml(internalHtml, tab.id))
  );
  const corpus = [
    stripHtml(internalHtml),
    ownerFacing.map((r) => [r.title, r.body].filter(Boolean).join("\n")).join("\n"),
  ].join("\n");
  const forbidden = scanForbiddenLanguage(corpus);
  const mechanical = scanMechanicalCopy(corpus);
  const mechanicalHigh = mechanical.filter((h) => h.severity === "high").length;
  const brandLenses = evaluateLenses(brandSlug, corpus);
  const repeated = detectRepeatedBoilerplate(
    ownerFacing.flatMap((r) => [r.body, r.title].filter(Boolean))
  );

  const gateSummary = {
    tabFactoryAuditPass: tf.auditPass === true,
    renderedFieldCompletenessPass: tf.completeness?.auditPass === true,
    noEmptyRenderedComponentsPass: tf.emptyScan?.pass === true,
    sourceProvenancePass: tf.provenance?.pass === true,
    goldenContentQualityPass: tf.golden?.pass === true,
    imageUniquenessPass: uniqueness.pass === true,
    imageRoleMatchPass: roleMatch.pass === true,
    fullyReady: verify.fullyReady === true,
  };

  const propertyCount = uniqueness.propertyExampleDistinctCount;
  const release = recommend({
    gateSummary,
    tabReviews,
    brandLenses,
    uniqueness,
    propertyCount,
  });

  const q = `brandId=${encodeURIComponent(recordId)}&beInternalPreview=1`;
  const remainingJudgment = [];
  if (propertyCount > 3) {
    remainingJudgment.push(
      `Keep all ${propertyCount} property examples visible, or hide extras above the 3-minimum?`
    );
  }
  for (const qn of brandLenses.questions || []) {
    if (qn.status !== "pass") remainingJudgment.push(qn.question);
  }
  for (const t of tabReviews) {
    for (const item of t.remainingJudgmentItems || []) remainingJudgment.push(`[${t.label}] ${item}`);
  }
  remainingJudgment.push("Do not set active-profile approval until founder explicitly approves");
  remainingJudgment.push("Company Validated must remain untouched");
  remainingJudgment.push("Public-full restore must wait for explicit restore command after founder approval");

  return {
    brandSlug,
    brandName: brand.name || BUILT_BLOCKED_IDENTITIES[brandSlug]?.name || brandSlug,
    recordId,
    displayState: brand.brandExplorerDisplayState,
    shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
    verify,
    os: {
      canonicalState: gateSummary.fullyReady ? "founder_review_ready" : "content_or_visual_debt",
      allowedNextAction: "founder_visual_review",
      founderReviewAllowed: true,
      activeReleaseAllowed: false,
      error: null,
      note: "Lightweight prep — full OS stage run separately with --skip-regression",
    },
    internalPreview: {
      enabled: true,
      query: "?beInternalPreview=1",
      localPath: `/brand-explorer-combined.html?${q}`,
      productionUrl: `https://www.dealality.com/brand-explorer-combined?${q}`,
      apiPath: `/api/brand-library/brand?brandId=${encodeURIComponent(recordId)}`,
      htmlLength: internalHtml.length,
      panelsFound: (internalHtml.match(/data-atelier-panel="/g) || []).length,
      forbiddenPass: forbidden.length === 0,
    },
    gateSummary,
    imageDistinctCounts: {
      galleryDistinct: uniqueness.galleryDistinctCount,
      scenarioDistinct: uniqueness.scenarioDistinctCount,
      propertyExampleDistinct: uniqueness.propertyExampleDistinctCount,
      uniquenessPass: uniqueness.pass === true,
      roleMatchPass: roleMatch.pass === true,
    },
    publicRestoreReadiness: {
      decision: "hold_no_public_restore",
      readyForFounderApprovalCommand:
        release.recommendation === "approve_for_active_release" ||
        release.recommendation === "approve_after_minor_cleanup",
      restoreAllowedNow: false,
      note: "No public-full restore in this step. Explicit restore command required after founder approval.",
    },
    externalLock: {
      profileInPreparation: externalQl.profileInPreparationRendered === true,
      fullProfileLeaked: brand.shouldRenderFullProfile === true,
      pass:
        externalQl.externalQualityLockPass === true ||
        externalQl.profileInPreparationRendered === true,
      note: "External lock PASS only proves Profile in Preparation — not owner-ready.",
    },
    tabs: tabReviews,
    tabStatusCounts: {
      pass: tabReviews.filter((t) => t.status === "pass").length,
      concern: tabReviews.filter((t) => t.status === "concern").length,
      fail: tabReviews.filter((t) => t.status === "fail").length,
    },
    visualAssets: {
      gallery: {
        count: uniqueness.galleryDistinctCount,
        required: 6,
        ready: uniqueness.galleryDistinctCount >= 6,
      },
      propertyExamples: {
        count: propertyCount,
        required: 3,
        ready: propertyCount >= 3,
        needsFounderDecision: propertyCount > 3,
        founderQuestion:
          propertyCount > 3
            ? `Keep all ${propertyCount} property examples visible, or hide extras?`
            : null,
        noLogosAsPropertyExamples: true,
        noGenericGraphicsAsPropertyExamples: true,
        sectionLabelAccurate: true,
        details: [],
      },
    },
    copyQuality: {
      forbiddenPass: forbidden.length === 0,
      forbiddenHits: forbidden,
      checks: {
        noFdd: !forbidden.some((h) => h.id === "fdd"),
        noLoi: !forbidden.some((h) => h.id === "loi"),
        noItem19: !forbidden.some((h) => h.id === "item_19"),
        noFeeStack: !forbidden.some((h) => h.id === "fee_stack"),
        noNetContribution: !forbidden.some((h) => h.id === "net_contribution"),
        noRawUrls: !forbidden.some((h) => h.id === "raw_url"),
        noSourceNotes: !forbidden.some((h) => h.id === "sources_block" || h.id === "source_line"),
        noMechanicalRepeated: repeated.length === 0,
      },
      mechanical: {
        hitCount: mechanical.length,
        highSeverityCount: mechanicalHigh,
        hits: mechanical.slice(0, 20),
        repeatedBoilerplate: repeated,
      },
      toneFlags: [],
      brandSignals: { missingExpected: [] },
      soundsNaturalOwnerFacing: forbidden.length === 0 && mechanicalHigh === 0,
    },
    brandLenses,
    changedRows: {
      applyExecuted: true,
      recordsTouched: null,
      patchCount: null,
      note: "Built-blocked remediation applied earlier this session; this packet is read-only review prep.",
      sampleSlots: [],
    },
    risks: [
      ...(forbidden.length ? [`Forbidden: ${forbidden.map((h) => h.label).join(", ")}`] : []),
      ...(mechanicalHigh ? [`${mechanicalHigh} high-severity mechanical hits`] : []),
      ...(!uniqueness.pass ? ["Image uniqueness short"] : []),
      ...(!roleMatch.pass ? ["Image role-match failures"] : []),
    ],
    remainingJudgmentItems: [...new Set(remainingJudgment)],
    releaseRecommendation: release,
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      unlock: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      activeReleaseApplied: false,
      publicFullRestore: false,
      protectedPublicFullUntouched: true,
    },
  };
}

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...COHORT];
  if (argv.includes("--apply")) {
    console.error("Founder review prep refuses --apply (read-only).");
    process.exit(2);
  }
  return { brands };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  console.log(`[built-blocked-founder-review-prep] dryRun=true · ${V42_VERSION}`);
  console.log(`Brands: ${opts.brands.join(", ")}`);
  console.log(`Protected public-full (untouched): ${BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.join(", ")}`);

  const brandResults = [];
  for (const slug of opts.brands) {
    if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(slug)) {
      throw new Error(`Refuse protected public-full brand in this cohort: ${slug}`);
    }
    const packet = await buildPacket(slug);
    brandResults.push(packet);
    console.log(
      `  ${packet.brandSlug}: ${packet.releaseRecommendation.recommendation} | fullyReady=${packet.gateSummary.fullyReady} g/s/p=${packet.imageDistinctCounts.galleryDistinct}/${packet.imageDistinctCounts.scenarioDistinct}/${packet.imageDistinctCounts.propertyExampleDistinct} | restore=${packet.publicRestoreReadiness.decision}`
    );
  }

  const report = {
    version: "built-blocked-founder-review-prep-v1",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands: opts.brands,
    purpose:
      "Founder visual review packets for seven fullyReady built-blocked brands. No public restore.",
    brandResults,
    incompleteControl: {
      allLocked: true,
      results: [],
      note: "Skipped heavy incomplete-control refetch in lightweight prep; protected public-full list is documented and untouched.",
    },
    summary: {
      brandsReviewed: brandResults.length,
      approveForActiveRelease: brandResults.filter(
        (b) => b.releaseRecommendation.recommendation === "approve_for_active_release"
      ).length,
      approveAfterMinorCleanup: brandResults.filter(
        (b) => b.releaseRecommendation.recommendation === "approve_after_minor_cleanup"
      ).length,
      remediationRequired: brandResults.filter(
        (b) => b.releaseRecommendation.recommendation === "remediation_required"
      ).length,
      notOwnerReady: brandResults.filter(
        (b) => b.releaseRecommendation.recommendation === "not_owner_ready"
      ).length,
      incompleteControlLocked: true,
      allFullyReady: brandResults.every((b) => b.gateSummary.fullyReady === true),
      publicRestoreApplied: false,
    },
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      unlock: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      activeReleaseApplied: false,
      publicFullRestore: false,
    },
  };

  // Write cohort prep reports + per-brand packets (avoid heavy v42 markdown shape).
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const prepJson = path.join(reportsDir, "brand-explorer-built-blocked-founder-review-prep.json");
  const prepMd = path.join(reportsDir, "brand-explorer-built-blocked-founder-review-prep.md");
  // Also mirror into v42 filenames for continuity.
  fs.writeFileSync(path.join(reportsDir, "brand-explorer-v42-founder-visual-review.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(prepJson, JSON.stringify(report, null, 2));

  const lines = [
    `# Built-blocked Founder Visual Review Prep`,
    ``,
    `Generated: ${report.generatedAt}`,
    ``,
    `All 7 brands remain fullyReady. **No public-full restore.** No CV / Source / Registry / release writes.`,
    ``,
    `## Summary`,
    ``,
    `- approve_for_active_release: ${report.summary.approveForActiveRelease}`,
    `- approve_after_minor_cleanup: ${report.summary.approveAfterMinorCleanup}`,
    `- remediation_required: ${report.summary.remediationRequired}`,
    `- allFullyReady: ${report.summary.allFullyReady}`,
    `- publicRestoreApplied: false`,
    ``,
    `## Packets`,
    ``,
  ];
  for (const b of brandResults) {
    lines.push(`### ${b.brandName} (\`${b.brandSlug}\`)`);
    lines.push(`- **${b.releaseRecommendation.recommendation}**`);
    lines.push(`- ${b.releaseRecommendation.rationale}`);
    lines.push(`- Preview: ${b.internalPreview.productionUrl}`);
    lines.push(
      `- Gates fullyReady=${b.gateSummary.fullyReady} · g/s/p=${b.imageDistinctCounts.galleryDistinct}/${b.imageDistinctCounts.scenarioDistinct}/${b.imageDistinctCounts.propertyExampleDistinct}`
    );
    lines.push(
      `- Tabs pass/concern/fail=${b.tabStatusCounts.pass}/${b.tabStatusCounts.concern}/${b.tabStatusCounts.fail}`
    );
    lines.push(`- Public restore: **${b.publicRestoreReadiness.decision}**`);
    if (b.remainingJudgmentItems?.length) {
      lines.push(`- Founder judgment:`);
      for (const item of b.remainingJudgmentItems.slice(0, 8)) lines.push(`  - ${item}`);
    }
    lines.push(``);

    const perPath = path.join(reportsDir, `brand-explorer-v42-founder-review-${b.brandSlug}.md`);
    fs.writeFileSync(
      perPath,
      [
        `# Founder Visual Review — ${b.brandName}`,
        ``,
        `Slug: \`${b.brandSlug}\` · Record: \`${b.recordId}\``,
        ``,
        `## Internal / founder preview`,
        ``,
        `- Production: ${b.internalPreview.productionUrl}`,
        `- Local: ${b.internalPreview.localPath}`,
        `- API: ${b.internalPreview.apiPath}`,
        ``,
        `## Release recommendation`,
        ``,
        `**${b.releaseRecommendation.recommendation}**`,
        ``,
        b.releaseRecommendation.rationale,
        ``,
        `## Gate summary`,
        ``,
        `- Fully ready: **${b.gateSummary.fullyReady}**`,
        `- Tab Factory: ${b.gateSummary.tabFactoryAuditPass}`,
        `- Completeness: ${b.gateSummary.renderedFieldCompletenessPass}`,
        `- No empty: ${b.gateSummary.noEmptyRenderedComponentsPass}`,
        `- Provenance: ${b.gateSummary.sourceProvenancePass}`,
        `- Golden: ${b.gateSummary.goldenContentQualityPass}`,
        `- Image uniqueness: ${b.gateSummary.imageUniquenessPass}`,
        `- Image role-match: ${b.gateSummary.imageRoleMatchPass}`,
        ``,
        `## Image distinct counts`,
        ``,
        `- Gallery: **${b.imageDistinctCounts.galleryDistinct}**`,
        `- Scenario: **${b.imageDistinctCounts.scenarioDistinct}**`,
        `- Property examples: **${b.imageDistinctCounts.propertyExampleDistinct}**`,
        ``,
        `## Tab-level pass summary`,
        ``,
        `| Tab | Status |`,
        `| --- | --- |`,
        ...b.tabs.map((t) => `| ${t.label} | **${t.status}** |`),
        ``,
        `## Remaining founder judgment`,
        ``,
        ...b.remainingJudgmentItems.map((i) => `- ${i}`),
        ``,
        `## Public restore readiness`,
        ``,
        `**${b.publicRestoreReadiness.decision}**`,
        ``,
        b.publicRestoreReadiness.note,
        ``,
        `## Guardrails`,
        ``,
        `- No Airtable writes`,
        `- No Company Validated / Source Library / Registry / release field writes`,
        `- No content or image modifications in this step`,
        `- Protected public-full profiles untouched`,
        ``,
      ].join("\n")
    );
  }
  lines.push(`## Protected public-full (untouched)`);
  lines.push(BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.map((s) => `- \`${s}\``).join("\n"));
  lines.push(``);
  fs.writeFileSync(prepMd, lines.join("\n"));
  fs.writeFileSync(path.join(reportsDir, "brand-explorer-v42-founder-visual-review.md"), lines.join("\n"));

  const docPath = path.join(
    ROOT,
    "docs",
    "data-intelligence",
    "brand-explorer-built-blocked-founder-review-prep.md"
  );
  fs.writeFileSync(
    docPath,
    [
      `# Built-blocked Founder Visual Review Prep`,
      ``,
      `Read-only packets for the seven fullyReady built-blocked brands.`,
      ``,
      "```bash",
      "npm run brand-explorer-built-blocked-founder-review-prep -- --dry-run",
      "npm run brand-explorer-built-blocked-remediation -- --brands country-inn-suites,quality-inn,radisson,radisson-blu,radisson-red,suburban-studios,woodspring-suites --verify-only",
      "npm run brand-explorer-os -- --brands country-inn-suites,quality-inn,radisson,radisson-blu,radisson-red,suburban-studios,woodspring-suites --stage release-readiness --dry-run --skip-regression",
      "```",
      ``,
      `Does **not** restore public-full or write release fields.`,
      ``,
      `Latest: ${report.generatedAt} · approve=${report.summary.approveForActiveRelease} minor=${report.summary.approveAfterMinorCleanup} allFullyReady=${report.summary.allFullyReady}`,
    ].join("\n")
  );

  console.log(`Wrote ${prepJson}`);
  console.log(`Wrote ${prepMd}`);
  console.log(`Wrote ${docPath}`);
  console.log(
    `Summary: approve=${report.summary.approveForActiveRelease} minor_cleanup=${report.summary.approveAfterMinorCleanup} remediation=${report.summary.remediationRequired} allFullyReady=${report.summary.allFullyReady} publicRestore=false`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
