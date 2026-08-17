#!/usr/bin/env node

/**

 * Brand Explorer Active Profile Factory v34D.

 *

 *   npm run brand-explorer-active-profile-preflight -- --brand suburban-studios --dry-run

 *   npm run brand-explorer-active-profile-founder-review -- --brand woodspring-suites --dry-run

 */

import "../load-env.js";

import { mkdirSync, writeFileSync } from "fs";

import { dirname, join } from "path";

import { fileURLToPath } from "url";

import {

  FACTORY_GUARD_FLAGS,

  FACTORY_VERSION,

  buildBrandExplorerActiveProfileFactoryReport,

  perBrandReportBasename,

  perBrandStageReportBasename,

} from "../lib/partner-intelligence/brand-explorer-active-profile-factory.js";



const __dirname = dirname(fileURLToPath(import.meta.url));

const ROOT = join(__dirname, "..");



const STAGE_FROM_SCRIPT = {

  "brand-explorer-active-profile-preflight": "preflight",

  "brand-explorer-active-profile-asset-pack": "asset-pack",

  "brand-explorer-active-profile-build-draft": "build-draft",

  "brand-explorer-active-profile-copy-governance": "copy-governance",

  "brand-explorer-active-profile-apply-draft": "apply-draft",

  "brand-explorer-active-profile-founder-review": "founder-review",

  "brand-explorer-active-profile-apply-approved": "apply-approved",

  "brand-explorer-active-profile-final-qa": "final-qa",

};



function hasFlag(name) {

  return process.argv.includes(name);

}



function argValue(name, fallback = "") {

  const idx = process.argv.indexOf(name);

  if (idx < 0) return fallback;

  return process.argv[idx + 1] || fallback;

}



function detectStage() {

  const explicit = argValue("--stage", "");

  if (explicit) return explicit;

  const scriptPath = process.argv[1] || "";

  const base = scriptPath.split(/[/\\]/).pop() || "";

  for (const [script, stage] of Object.entries(STAGE_FROM_SCRIPT)) {

    if (base === `${script}.mjs` || base.startsWith(script)) {

      return stage;

    }

  }

  return "preflight";

}



function collectGuardFlags() {

  return {

    founderVisualReview: hasFlag(FACTORY_GUARD_FLAGS.founderVisualReview),

    approveBrandExplorerActiveProfile: hasFlag(FACTORY_GUARD_FLAGS.approveActiveProfile),

    approveBrandExplorerActiveProfileDraft: hasFlag(FACTORY_GUARD_FLAGS.approveDraft),

    confirmFounderVisualReviewPassed: hasFlag(FACTORY_GUARD_FLAGS.confirmFounderVisualReviewPassed),

    confirmNoCompanyValidationClaim: hasFlag(FACTORY_GUARD_FLAGS.noCompanyValidation),

    confirmNoSummaryUrlField: hasFlag(FACTORY_GUARD_FLAGS.noSummaryUrl),

    confirmBrandOnly: hasFlag(FACTORY_GUARD_FLAGS.brandOnly),

    confirmOfficialSourceImagesOnly: hasFlag(FACTORY_GUARD_FLAGS.officialImagesOnly),

    confirmMinimumSixVisibleGalleryImages: hasFlag(FACTORY_GUARD_FLAGS.minimumSixGallery),

    confirmPropertyExamplesHaveHotelImages: hasFlag(FACTORY_GUARD_FLAGS.propertyExamplesHaveHotelImages),

    confirmNoLogoLifestylePropertyImages: hasFlag(FACTORY_GUARD_FLAGS.noLogoLifestylePropertyImages),

    confirmStandardDetailGovernanceReviewed: hasFlag(FACTORY_GUARD_FLAGS.standardDetailGovernance),

    approveCopyGovernance: hasFlag(FACTORY_GUARD_FLAGS.approveCopyGovernance),

  };

}



async function main() {

  const stage = detectStage();

  const brand = argValue("--brand", "suburban-studios");

  const apply =
    hasFlag("--apply") && (stage === "apply-approved" || stage === "apply-draft");

  const guardFlags = collectGuardFlags();



  const report = await buildBrandExplorerActiveProfileFactoryReport({

    brandArg: brand,

    stage,

    apply,

    guardFlags,

  });



  const basename = perBrandReportBasename(report.brand.slug);

  const stageBasename = perBrandStageReportBasename(report.brand.slug, stage);

  const reportJson = join(ROOT, "reports", `${basename}.json`);

  const reportMd = join(ROOT, "reports", `${basename}.md`);

  const stageJson = join(ROOT, "reports", `${stageBasename}.json`);

  const stageMd = join(ROOT, "reports", `${stageBasename}.md`);

  const consolidationMd = join(

    ROOT,

    "docs/data-intelligence/brand-explorer-active-profile-staged-apply-v34D.md"

  );



  mkdirSync(dirname(reportJson), { recursive: true });

  writeFileSync(reportJson, `${JSON.stringify(report, null, 2)}\n`);

  writeFileSync(reportMd, `${report.markdown}\n`);

  writeFileSync(stageJson, `${JSON.stringify(report, null, 2)}\n`);



  if (report.copyGovernancePlan?.founderQueueResolution?.summary) {

    const q = report.copyGovernancePlan.founderQueueResolution.summary;

    console.log(`Queue resolved by rewrite: ${q.resolvedByRewrite}`);

    console.log(`Queue resolved by hide: ${q.resolvedByHide}`);

    console.log(`Queue remaining manual: ${q.remainingManual}`);

  }

  if (report.copyGovernancePlan?.summary) {

    console.log(`Copy governance repairs: ${report.copyGovernancePlan.summary.repairsProposed}`);

    console.log(`Copy founder review queue: ${report.copyGovernancePlan.summary.founderReviewRequired}`);

  }

  if (stage === "asset-pack" && report.assetPackMarkdown) {

    writeFileSync(stageMd, `${report.assetPackMarkdown}\n`);

  } else   if (stage === "copy-governance" && report.copyGovernanceMarkdown) {

    writeFileSync(stageMd, `${report.copyGovernanceMarkdown}\n`);

    if (report.founderQueueAuditMarkdown) {

      const queueMd = join(

        ROOT,

        "reports",

        `${perBrandStageReportBasename(report.brand.slug, "founder-queue-audit")}.md`

      );

      writeFileSync(queueMd, `${report.founderQueueAuditMarkdown}\n`);

      console.log(`Wrote ${queueMd}`);

    }

  } else if (stage === "founder-review" && report.founderVisualReviewMarkdown) {

    writeFileSync(stageMd, `${report.founderVisualReviewMarkdown}\n`);

    const templateMd = join(

      ROOT,

      "reports",

      `${perBrandStageReportBasename(report.brand.slug, "founder-visual-review")}.md`

    );

    writeFileSync(templateMd, `${report.founderVisualReviewMarkdown}\n`);

    console.log(`Wrote ${templateMd}`);

  } else if (stage === "apply-draft") {

    writeFileSync(stageMd, `${report.founderVisualReviewMarkdown || report.markdown}\n`);

    if (report.postDraftApply) {

      const postMd = join(

        ROOT,

        "reports",

        `${perBrandStageReportBasename(report.brand.slug, "post-draft-apply")}.md`

      );

      writeFileSync(

        postMd,

        `${report.founderVisualReviewMarkdown || report.markdown}\n`

      );

      console.log(`Wrote ${postMd}`);

    }

  } else if (stage === "build-draft" && report.draftMarkdown) {

    writeFileSync(stageMd, `${report.draftMarkdown}\n`);

  } else {

    writeFileSync(stageMd, `${report.markdown}\n`);

  }



  if (stage === "preflight" && brand === "suburban-studios") {

    writeFileSync(consolidationMd, buildConsolidationDoc(report));

  }

  if (stage === "founder-review" || stage === "apply-draft") {

    writeFileSync(consolidationMd, buildConsolidationDoc(report));

  }



  console.log(`Wrote ${reportJson}`);

  console.log(`Wrote ${stageJson}`);

  console.log(`Wrote ${stageMd}`);

  console.log(`Factory ${FACTORY_VERSION} stage: ${stage}`);

  console.log(`Brand: ${report.brand.name} (${report.brand.slug})`);

  console.log(`Factory rules pass: ${report.factoryRules.pass ? "yes" : "no"}`);

  console.log(`Blockers: ${report.factoryRules.blockers.length}`);

  if (report.assetPack?.summary) {

    console.log(`Asset pack readiness: ${report.assetPack.summary.readinessBand}`);

    console.log(`Gallery ready: ${report.assetPack.summary.galleryReady}`);

    console.log(`Property examples ready: ${report.assetPack.summary.propertyExamplesReady}`);

  }

  if (report.draftPlan?.summary) {

    console.log(`Draft patches: ${report.draftPlan.summary.patchCount}`);

  }

  if (report.suburbanReadiness) {

    console.log(`Suburban recommendation: ${report.suburbanReadiness.recommendation}`);

    console.log(`Path required: ${report.suburbanReadiness.pathRequired || "n/a"}`);

  }

  console.log(`Founder visual review: ${report.founderVisualReview?.pass ? "PASS" : "FAIL"}`);

  if (report.applyDraft?.exactCommand) {

    console.log(`Draft apply command: ${report.applyDraft.exactCommand}`);

  }

  if (report.applyApproved?.exactCommand) {

    console.log(`Active approval command: ${report.applyApproved.exactCommand}`);

  }

}



function buildConsolidationDoc(report) {

  const sr = report.suburbanReadiness || {};

  return `# Brand Explorer Active Profile Factory ${FACTORY_VERSION}



Generic factory layer — brand config + shared stages (no suburban-only writer chain).



## Architecture



| Module | Purpose |

|--------|---------|

| \`brand-explorer-active-profile-brand-config.js\` | Per-brand config model |

| \`brand-explorer-active-profile-asset-pack-builder.js\` | Gallery / property / scenario asset discovery |

| \`brand-explorer-active-profile-draft-builder.js\` | Dry-run presentation + registry patch proposals |

| \`brand-explorer-active-profile-factory.js\` | Stage orchestration |



## Factory commands



| Stage | Command |

|-------|---------|

| Preflight | \`brand-explorer-active-profile-preflight\` |

| Asset pack | \`brand-explorer-active-profile-asset-pack\` |

| Build draft | \`brand-explorer-active-profile-build-draft\` |

| Copy governance | \`brand-explorer-active-profile-copy-governance\` |

| Apply draft | \`brand-explorer-active-profile-apply-draft\` |

| Founder review | \`brand-explorer-active-profile-founder-review\` |

| Apply approved | \`brand-explorer-active-profile-apply-approved\` |

| Final QA | \`brand-explorer-active-profile-final-qa\` |



## Suburban assessment (${report.generatedAt})



- Factory pass: **${report.factoryRules.pass ? "yes" : "no"}**

- Asset pack readiness: **${report.assetPack?.summary?.readinessBand || "not run"}**

- Recommendation: **${sr.recommendation || "run asset-pack stage"}**

- Path required: **${sr.pathRequired || "config + asset pack"}**

- Custom code required: **${sr.customCodeRequired ? "yes" : "no"}**

- Draft patches (dry-run): **${sr.draftPatchCount ?? "n/a"}**



## Global rules



1. Gallery — minimum 6 visible \`materials.gallery\` with API \`imageUrl\`

2. Property examples — hotel/property images only; U.S. fallback labeled

3. Scenario images — hide when no source; no IMAGE placeholders

4. Registry traceability — durable source URLs + approved registry rows

5. Copy safety — ADR/FDD/performance claims blocked in sanitizer

6. Apply gated — staged: draft apply → founder visual review → active approval



## Validation brands



- **suburban-studios** — first generic factory test

- **woodspring-suites** — validation reference

- **everhome-suites** — validation reference (property catalog TBD)

`;

}



main().catch((err) => {

  console.error(err);

  process.exit(1);

});


