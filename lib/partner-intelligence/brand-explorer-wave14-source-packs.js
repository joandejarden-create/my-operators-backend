/**
 * Wave 14 Stage 3 — Official Source Packs (read-only).
 * Writes report markdown/json + docs only. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE14_VERSION,
  WAVE14_SLUGS,
  WAVE14_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave14-factory-plan.js";
import { EXPECTED_ACTIVE_COUNT_46 } from "./brand-explorer-46-active-public-full-baseline.js";
import {
  WAVE14_SOURCE_PACKS_VERSION,
  WAVE14_SOURCE_PACKS_BY_SLUG,
  TGS_AVOID_NOTE,
  getWave14SourcePack,
} from "./brand-explorer-wave14-source-packs-content.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const PREFLIGHT_READY = "protected_46_live_clean_wave14_may_resume";
const MANIFEST_READY_PREFIX = "wave14_manifest_ready";
const COHORT_APPLIED = "wave14_factory_preview_cohort_applied";
const COHORT_DRY_RUN_READY = "wave14_factory_preview_cohort_dry_run_ready";
const DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function readJson(name) {
  const p = path.join(REPORTS_DIR, name);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    console.error(`[wave14-source-packs] failed reading ${name}:`, err?.message || err);
    return null;
  }
}

function ageMs(iso) {
  if (!iso) return Number.POSITIVE_INFINITY;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return Number.POSITIVE_INFINITY;
  return Date.now() - t;
}

function pvqlPublicFullClean(pvql) {
  const rows = (pvql?.brands || []).filter((b) => b.publicFullProfile === true);
  if (rows.length !== EXPECTED_ACTIVE_COUNT_46) return false;
  return rows.every(
    (b) =>
      b.lockPass === true &&
      (b.gateResults?.forbidden_owner_facing_language?.hits?.length || 0) === 0
  );
}

function qualityFreezeClean(quality) {
  const approve = (quality?.brandResults || []).filter(
    (b) => b.overallRecommendation === "approve_for_baseline_freeze"
  ).length;
  return (
    quality?.baselineFreezeDecision === "ready_to_freeze_46_active_public_full_baseline" ||
    quality?.baselineFreezeDecision === "frozen_46_active_public_full_baseline" ||
    (approve === EXPECTED_ACTIVE_COUNT_46 &&
      (quality?.recommendationCounts?.remediation_required || 0) === 0)
  );
}

export function assessWave14SourcePacksGate({
  reuseFreshReports = false,
  maxAgeMs = DEFAULT_MAX_AGE_MS,
} = {}) {
  const preflight = readJson("brand-explorer-wave14-preflight.json");
  const manifest = readJson("brand-explorer-wave14-manifest.json");
  const cohort = readJson("brand-explorer-wave14-factory-preview-cohort.json");
  const pvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const quality = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const warnings = [];
  const issues = [];

  const preflightOk =
    preflight?.pass === true &&
    preflight?.readyStatement === PREFLIGHT_READY &&
    ageMs(preflight?.generatedAt) <= maxAgeMs;

  const reportsOk =
    reuseFreshReports &&
    pvqlPublicFullClean(pvql) &&
    qualityFreezeClean(quality) &&
    ageMs(pvql?.generatedAt) <= maxAgeMs &&
    ageMs(quality?.generatedAt) <= maxAgeMs;

  const gateOk = preflightOk || reportsOk;
  if (!gateOk) {
    issues.push(
      preflight
        ? `preflight_not_clean:pass=${preflight.pass}:ready=${preflight.readyStatement}`
        : "preflight_missing"
    );
  }

  const manifestOk =
    manifest &&
    !manifest.deferred &&
    nz(manifest.readyStatement).startsWith(MANIFEST_READY_PREFIX) &&
    manifest.mayProceedToFactoryPreviewCohort === true;

  if (!manifestOk) {
    issues.push(
      manifest
        ? `manifest_not_ready:ready=${manifest.readyStatement}:deferred=${manifest.deferred}`
        : "manifest_missing"
    );
  }

  const cohortReady =
    cohort &&
    !cohort.deferred &&
    (cohort.readyStatement === COHORT_APPLIED ||
      cohort.readyStatement === COHORT_DRY_RUN_READY);

  if (!cohortReady) {
    warnings.push(
      cohort
        ? `factory_preview_cohort_not_applied_yet:ready=${cohort.readyStatement}`
        : "factory_preview_cohort_report_missing"
    );
  }

  return {
    ok: gateOk && manifestOk,
    warnings,
    issues,
    preflightOk,
    reportsOk,
    manifestOk,
    cohortReady,
    nextCommand: !gateOk
      ? "npm run brand-explorer-wave14-factory -- --stage preflight --dry-run --reuse-fresh-reports"
      : !manifestOk
        ? "npm run brand-explorer-wave14-factory -- --stage manifest --dry-run --reuse-fresh-reports"
        : null,
  };
}

function enrichPackFromManifest(pack, manifestBrand) {
  if (!manifestBrand) return pack;
  return {
    ...pack,
    brandBasicsName: manifestBrand.brandName || pack.brandBasicsName,
    recordId: manifestBrand.recordId || pack.recordId,
    brandStatus: manifestBrand.brandStatus || pack.brandStatus,
    parentPlatform: manifestBrand.parentCompany || pack.parentPlatform,
    classification: manifestBrand.classification || null,
    presentationRowCount: manifestBrand.presentationRowCount ?? null,
  };
}

function renderPackMarkdown(pack) {
  const lines = [
    `# Wave 14 Source Pack — ${pack.officialBrandName}`,
    ``,
    `| Field | Value |`,
    `| --- | --- |`,
    `| Official brand name | ${pack.officialBrandName} |`,
    `| Slug | \`${pack.slug}\` |`,
    `| Brand Basics record ID | \`${pack.recordId || "—"}\` |`,
    `| Current Brand Basics name | ${pack.brandBasicsName || "—"} |`,
    `| Current Brand Status | ${pack.brandStatus || "—"} |`,
    `| Parent / platform | ${pack.parentPlatform} |`,
    `| Classification | \`${pack.classification || "—"}\` |`,
    `| CALA availability | ${pack.calaAvailability} |`,
    `| International Reference required | ${pack.internationalReferenceRequired} |`,
    `| Stage 4 readiness | \`${pack.stage4Readiness}\` |`,
    ``,
    `## Lens`,
    ``,
    pack.lens,
    ``,
    `## CALA-first posture`,
    ``,
    pack.calaFirstPosture,
    ``,
    `## Official sources`,
    ``,
    `- Brand page: [${pack.officialBrandPage?.label}](${pack.officialBrandPage?.url})`,
    `- Development page: [${pack.developmentPage?.label}](${pack.developmentPage?.url})`,
    ``,
    `### Parent / platform / sibling context`,
    ``,
    ...(pack.parentPlatformContext || []).map(
      (s) =>
        `- [${s.label}](${s.url}) — role=\`${s.role}\`${s.note ? ` — ${s.note}` : ""}`
    ),
    ``,
    `## Property examples`,
    ``,
    ...(pack.propertyExamples || []).map(
      (p) =>
        `- **${p.propertyName}** (${p.geographyLabel}${p.market ? ` · ${p.market}` : ""}) — ${p.url}${p.note ? ` — ${p.note}` : ""}`
    ),
    ``,
    `## Recent Momentum candidates`,
    ``,
    ...(pack.recentMomentumCandidates || []).map(
      (m) =>
        `- **${m.dateLine}** — ${m.title}\n  - ${m.summary}\n  - Source: [${m.linkLabel}](${m.announcementUrl})\n  - Geography: ${m.geographyLabel}\n  - Why: ${m.whyRelevant}`
    ),
    ``,
    `## Openings / Examples / Properties candidates`,
    ``,
    ...(pack.openingsExamplesPropertiesCandidates || []).map((x) => `- ${x}`),
    ``,
    `## Image source candidates`,
    ``,
    ...(pack.imageSourceHints || []).map(
      (i) => `- [${i.label}](${i.url}) — trust=${i.trust}${i.note ? ` — ${i.note}` : ""}`
    ),
    ``,
    `## Target Guest Segment recommendation`,
    ``,
    `- Recommended: ${(pack.targetGuestSegmentsRecommendation?.recommended || []).join(", ") || "—"}`,
    `- Avoid: ${(pack.targetGuestSegmentsRecommendation?.avoid || []).join("; ") || "—"}`,
    `- Rationale: ${pack.targetGuestSegmentsRecommendation?.rationale || "—"}`,
    `- Note: ${TGS_AVOID_NOTE}`,
    ``,
    `## Owner-facing positioning notes`,
    ``,
    ...(pack.ownerFacingPositioningNotes || []).map((n) => `- ${n}`),
    ``,
    `## Sibling-brand distinction notes`,
    ``,
    ...(pack.siblingBrandDistinctionNotes || []).map((n) => `- ${n}`),
    ``,
    `## Source gaps`,
    ``,
    ...(pack.sourceGaps || []).map((g) => `- ${g}`),
    ``,
    `## Manual review risks`,
    ``,
    ...(pack.manualReviewRisks || []).map((r) => `- ${r}`),
    ``,
    `## Recommendation for Stage 4 readiness`,
    ``,
    `\`${pack.stage4Readiness}\``,
    ``,
    `## Guardrails`,
    ``,
    `- No Airtable / Presentation / Brand Status / release / CV / Source Library / Registry writes`,
    `- No raw URLs in future public body copy`,
    `- Parent/platform context clearly labeled`,
    `- Property URLs must match the specific property name`,
    ``,
  ];
  return `${lines.join("\n")}\n`;
}

function renderSummaryMarkdown(summary) {
  return [
    `# Brand Explorer Wave 14 — Source Pack Summary`,
    ``,
    `Generated: ${summary.generatedAt}`,
    `Version: ${summary.version}`,
    `Pass: **${summary.pass}**`,
    `May proceed to Stage 4 tab-factory-build: **${summary.mayProceedToTabFactoryBuild}**`,
    ``,
    `| Metric | Value |`,
    `| --- | --- |`,
    `| Packs produced | ${summary.packsProduced}/9 |`,
    `| Blocked / held | ${summary.blockedHeld.length} |`,
    `| CALA-first brands | ${summary.calaFirstCount} |`,
    `| International Reference required | ${summary.intlRefRequiredCount} |`,
    `| Airtable writes | false |`,
    `| Presentation writes | false |`,
    `| Protected 46 touched | false |`,
    ``,
    `## Per-brand readiness`,
    ``,
    `| Slug | Official name | CALA | Intl Ref | Stage 4 |`,
    `| --- | --- | --- | --- | --- |`,
    ...summary.brandRows.map(
      (b) =>
        `| \`${b.slug}\` | ${b.officialBrandName} | ${b.calaAvailability} | ${b.internationalReferenceRequired} | \`${b.stage4Readiness}\` |`
    ),
    ``,
    `## Critical alias reminders`,
    ``,
    `- Four Points Flex by Sheraton ≠ Four Points by Sheraton`,
    `- StudioRes ≠ Residence Inn / TownePlace Suites / Element / Apartments by Marriott Bonvoy`,
    `- Marriott Hotels ≠ Marriott International corporate`,
    ``,
    `## Docs`,
    ``,
    `- \`docs/data-intelligence/brand-explorer-wave14-source-packs.md\``,
    ``,
    summary.mayProceedToTabFactoryBuild
      ? "Wave 14 **may proceed to Stage 4 tab-factory-build** after Brand Basics gaps (if any) are steward-resolved — still no Brand Status promotion in Stage 4."
      : "**Hold Stage 4** until source-pack gate issues are cleared.",
    ``,
  ].join("\n");
}

/**
 * @param {{ dryRun?: boolean, reuseFreshReports?: boolean }} opts
 */
export async function runWave14SourcePacks({
  dryRun = true,
  reuseFreshReports = false,
} = {}) {
  const gate = assessWave14SourcePacksGate({ reuseFreshReports });
  if (!gate.ok) {
    const deferred = {
      version: WAVE14_SOURCE_PACKS_VERSION,
      factoryVersion: WAVE14_VERSION,
      stage: "source-packs",
      generatedAt: new Date().toISOString(),
      dryRun: true,
      deferred: true,
      pass: false,
      stopRecommended: true,
      airtableWrites: false,
      mayProceedToTabFactoryBuild: false,
      gate,
      message: "Wave 14 source packs deferred — preflight/manifest not clean.",
      nextRequired: gate.nextCommand,
    };
    ensureDir(REPORTS_DIR);
    const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-source-pack-summary.json");
    const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-source-pack-summary.md");
    fs.writeFileSync(jsonPath, `${JSON.stringify(deferred, null, 2)}\n`);
    fs.writeFileSync(
      mdPath,
      `# Wave 14 Source Packs — Deferred\n\n${deferred.message}\n\nNext: \`${deferred.nextRequired}\`\n`
    );
    return { ...deferred, paths: { jsonPath, mdPath, packPaths: [] } };
  }

  const manifest = readJson("brand-explorer-wave14-manifest.json");
  const manifestBySlug = Object.fromEntries(
    (manifest?.brands || []).map((b) => [b.slug, b])
  );

  ensureDir(REPORTS_DIR);
  ensureDir(DOCS_DIR);

  const packPaths = [];
  const brandRows = [];
  const blockedHeld = [];

  for (const slug of WAVE14_SLUGS) {
    const base = getWave14SourcePack(slug);
    const pack = enrichPackFromManifest(base, manifestBySlug[slug]);
    if (pack.classification === "blocked_requires_manual_review") {
      blockedHeld.push(slug);
    }
    const md = renderPackMarkdown(pack);
    const mdPath = path.join(REPORTS_DIR, `brand-explorer-wave14-source-pack-${slug}.md`);
    fs.writeFileSync(mdPath, md);
    packPaths.push(mdPath);
    brandRows.push({
      slug,
      officialBrandName: pack.officialBrandName,
      recordId: pack.recordId,
      brandStatus: pack.brandStatus,
      classification: pack.classification,
      calaAvailability: pack.calaAvailability,
      internationalReferenceRequired: pack.internationalReferenceRequired,
      stage4Readiness: pack.stage4Readiness,
      sourceGaps: pack.sourceGaps,
      manualReviewRisks: pack.manualReviewRisks,
    });
  }

  const calaFirstCount = brandRows.filter(
    (b) => b.calaAvailability === "supported" || b.calaAvailability === "pipeline"
  ).length;
  const intlRefRequiredCount = brandRows.filter((b) => b.internationalReferenceRequired).length;

  const mayProceedToTabFactoryBuild =
    brandRows.length === 9 &&
    blockedHeld.length === 0 &&
    brandRows.every((b) =>
      String(b.stage4Readiness || "").startsWith("ready_for_tab_factory_build")
    );

  const summary = {
    version: WAVE14_SOURCE_PACKS_VERSION,
    factoryVersion: WAVE14_VERSION,
    stage: "source-packs",
    generatedAt: new Date().toISOString(),
    dryRun: dryRun !== false,
    deferred: false,
    pass: true,
    stopRecommended: false,
    airtableWrites: false,
    presentationWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    protected46Touched: false,
    protectedBaselineCount: WAVE14_PROTECTED_BASELINE_COUNT,
    packsProduced: brandRows.length,
    blockedHeld,
    calaFirstCount,
    intlRefRequiredCount,
    brandRows,
    gate,
    mayProceedToTabFactoryBuild,
    readyStatement: mayProceedToTabFactoryBuild
      ? "wave14_source_packs_ready_for_stage4_tab_factory_build"
      : "wave14_source_packs_produced_with_holds",
    nextRequired: mayProceedToTabFactoryBuild
      ? "npm run brand-explorer-wave14-factory -- --stage tab-factory-build --dry-run (when kicked off)"
      : "Resolve blocked/held brands or steward gaps before Stage 4",
    packsBySlug: Object.fromEntries(
      WAVE14_SLUGS.map((s) => [s, WAVE14_SOURCE_PACKS_BY_SLUG[s]])
    ),
    tgsAvoidNote: TGS_AVOID_NOTE,
  };

  const summaryJson = path.join(REPORTS_DIR, "brand-explorer-wave14-source-pack-summary.json");
  const summaryMd = path.join(REPORTS_DIR, "brand-explorer-wave14-source-pack-summary.md");
  fs.writeFileSync(summaryJson, `${JSON.stringify(summary, null, 2)}\n`);
  fs.writeFileSync(summaryMd, renderSummaryMarkdown(summary));

  const docsMd = [
    `# Brand Explorer Wave 14 — Official Source Packs`,
    ``,
    `> Generated: ${summary.generatedAt}  `,
    `> Version: ${WAVE14_SOURCE_PACKS_VERSION}  `,
    `> Protected baseline: **46** (untouched)  `,
    `> Airtable / Presentation / Brand Status / release writes: **none**`,
    ``,
    `## Cohort (9)`,
    ``,
    WAVE14_SLUGS.map((s) => `- \`${s}\` — ${WAVE14_SOURCE_PACKS_BY_SLUG[s].officialBrandName}`).join(
      "\n"
    ),
    ``,
    `## Stage 4 readiness`,
    ``,
    `May proceed to Stage 4 tab-factory-build: **${summary.mayProceedToTabFactoryBuild}**`,
    ``,
    `See per-brand packs under \`reports/brand-explorer-wave14-source-pack-*.md\` and summary \`reports/brand-explorer-wave14-source-pack-summary.md\`.`,
    ``,
    `## Critical distinctions`,
    ``,
    `- Four Points Flex by Sheraton ≠ Four Points by Sheraton`,
    `- StudioRes ≠ Residence Inn / TownePlace / Element / Apartments by Marriott Bonvoy`,
    `- Marriott Hotels ≠ Marriott International corporate`,
    ``,
    `## CALA-first vs International Reference`,
    ``,
    ...brandRows.map(
      (b) =>
        `- \`${b.slug}\`: CALA=${b.calaAvailability}; IntlRefRequired=${b.internationalReferenceRequired}`
    ),
    ``,
  ].join("\n");

  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave14-source-packs.md");
  fs.writeFileSync(docsPath, `${docsMd}\n`);

  return {
    ...summary,
    paths: {
      jsonPath: summaryJson,
      mdPath: summaryMd,
      docsPath,
      packPaths,
    },
    summary: {
      packsProduced: summary.packsProduced,
      mayProceedToTabFactoryBuild: summary.mayProceedToTabFactoryBuild,
    },
  };
}
