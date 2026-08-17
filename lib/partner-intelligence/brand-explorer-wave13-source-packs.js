/**
 * Wave 13 Stage 3 — Official Source Packs (read-only).
 * Writes report markdown/json + docs only. No Airtable writes.
 *
 * Gate: preflight PASS + manifest ready; factory-preview cohort is soft warning
 * unless missing when required. Supports --reuse-fresh-reports.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE13_VERSION,
  WAVE13_SLUGS,
  WAVE13_BRAND_PLAN,
  WAVE13_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave13-factory-plan.js";
import {
  EXPECTED_ACTIVE_COUNT_39,
} from "./brand-explorer-39-active-public-full-baseline.js";
import {
  WAVE13_SOURCE_PACKS_VERSION,
  WAVE13_SOURCE_PACKS_BY_SLUG,
  TGS_AVOID_NOTE,
  getWave13SourcePack,
} from "./brand-explorer-wave13-source-packs-content.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const PREFLIGHT_READY = "protected_39_live_clean_wave13_may_resume";
const MANIFEST_READY_PREFIX = "wave13_manifest_ready";
const COHORT_APPLIED = "wave13_factory_preview_cohort_applied";
const COHORT_DRY_RUN_READY = "wave13_factory_preview_cohort_dry_run_ready";
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
    console.error(`[wave13-source-packs] failed reading ${name}:`, err?.message || err);
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
  if (rows.length !== EXPECTED_ACTIVE_COUNT_39) return false;
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
    quality?.baselineFreezeDecision === "ready_to_freeze_45_active_public_full_baseline" ||
    quality?.baselineFreezeDecision === "ready_to_freeze_39_active_public_full_baseline" ||
    (approve === EXPECTED_ACTIVE_COUNT_39 &&
      (quality?.recommendationCounts?.remediation_required || 0) === 0)
  );
}

/**
 * Source-packs may run when preflight + manifest are clean/ready.
 * Factory-preview-cohort is a soft warning unless the cohort module is required.
 */
export function assessWave13SourcePacksGate({
  reuseFreshReports = false,
  maxAgeMs = DEFAULT_MAX_AGE_MS,
} = {}) {
  const preflight = readJson("brand-explorer-wave13-preflight.json");
  const manifest = readJson("brand-explorer-wave13-manifest.json");
  const cohort = readJson("brand-explorer-wave13-factory-preview-cohort.json");
  const pvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const quality = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const warnings = [];
  const issues = [];

  const preflightContentOk =
    preflight?.pass === true && preflight?.readyStatement === PREFLIGHT_READY;
  const preflightFresh = ageMs(preflight?.generatedAt) <= maxAgeMs;
  const preflightOk = preflightContentOk && preflightFresh;

  const manifestContentOk =
    manifest?.deferred !== true &&
    manifest?.mayProceedToFactoryPreviewCohort === true &&
    nz(manifest?.readyStatement).startsWith(MANIFEST_READY_PREFIX);
  const manifestFresh = ageMs(manifest?.generatedAt) <= maxAgeMs;
  const manifestOk = manifestContentOk && manifestFresh;

  const reportsOk =
    pvqlPublicFullClean(pvql) &&
    qualityFreezeClean(quality) &&
    ageMs(pvql?.generatedAt) <= maxAgeMs &&
    ageMs(quality?.generatedAt) <= maxAgeMs;

  let ok = false;
  let source = "blocked";
  let reusedFreshReports = false;

  if (preflightOk && manifestOk) {
    ok = true;
    source = "same_session_reports";
  } else if (reuseFreshReports && preflightContentOk && manifestContentOk && reportsOk) {
    // Accept aged but still-passing Wave 13 reports only when live PVQL+quality are clean AND current.
    // Never proceed on stale cached PVQL alone.
    ok = true;
    source = "reuse_fresh_reports";
    reusedFreshReports = true;
  }

  if (!ok) {
    if (!preflightContentOk || (!preflightFresh && !(reuseFreshReports && reportsOk))) {
      return {
        ok: false,
        reason: "protected_39_preflight_not_clean",
        source: "blocked",
        reusedFreshReports: false,
        issues: [
          preflight
            ? `preflight_not_clean:pass=${preflight.pass}:ready=${preflight.readyStatement}:ageMin=${Math.round(ageMs(preflight.generatedAt) / 60000)}`
            : "preflight_report_missing",
          ...(reuseFreshReports && !reportsOk
            ? [
                `fresh_reports_not_clean_or_stale:pvqlClean=${pvqlPublicFullClean(pvql)}:qualityClean=${qualityFreezeClean(quality)}:pvqlAgeMin=${Math.round(ageMs(pvql?.generatedAt) / 60000)}:qualityAgeMin=${Math.round(ageMs(quality?.generatedAt) / 60000)}`,
              ]
            : []),
          ...(!reuseFreshReports && !preflightOk
            ? ["pass_--reuse-fresh-reports_only_if_pvql_quality_and_wave13_reports_are_clean_and_current"]
            : []),
        ],
        nextCommand: "npm run brand-explorer-wave13-factory -- --stage preflight --dry-run",
        warnings,
      };
    }
    if (!manifestContentOk || (!manifestFresh && !(reuseFreshReports && reportsOk))) {
      return {
        ok: false,
        reason: "wave13_manifest_not_ready",
        source: "blocked",
        reusedFreshReports: false,
        issues: [
          manifest
            ? `manifest_not_ready:mayProceed=${manifest.mayProceedToFactoryPreviewCohort}:ready=${manifest.readyStatement}:ageMin=${Math.round(ageMs(manifest.generatedAt) / 60000)}`
            : "manifest_report_missing",
        ],
        nextCommand: "npm run brand-explorer-wave13-factory -- --stage manifest --dry-run",
        warnings,
      };
    }
    return {
      ok: false,
      reason: "protected_39_preflight_not_clean",
      source: "blocked",
      reusedFreshReports: false,
      issues,
      nextCommand: "npm run brand-explorer-wave13-factory -- --stage preflight --dry-run",
      warnings,
    };
  }

  const cohortReady =
    cohort?.pass === true &&
    (cohort?.readyStatement === COHORT_APPLIED ||
      cohort?.readyStatement === COHORT_DRY_RUN_READY ||
      cohort?.wroteModule === true);
  if (!cohortReady) {
    warnings.push({
      code: "factory_preview_cohort_not_applied_or_ready",
      message:
        "Factory-preview-cohort report is missing or not applied/ready. Soft warning — source packs do not require the cohort file for read-only pack generation.",
      nextCommand:
        "npm run brand-explorer-wave13-factory -- --stage factory-preview-cohort --dry-run",
    });
  }

  return {
    ok: true,
    reason: null,
    source,
    reusedFreshReports,
    preflightReadyStatement: preflight?.readyStatement || null,
    manifestReadyStatement: manifest?.readyStatement || null,
    cohortReadyStatement: cohort?.readyStatement || null,
    cohortWarning: !cohortReady,
    issues: [],
    warnings,
    nextCommand: null,
  };
}

function validatePack(pack) {
  const failures = [];
  if (!pack) {
    failures.push("missing_pack");
    return failures;
  }
  if (!pack.officialBrandPage?.url) failures.push("missing_official_brand_page");
  if (!/^https?:\/\//i.test(nz(pack.officialBrandPage?.url))) {
    failures.push("official_brand_page_not_http");
  }
  if (!(pack.propertyExamples || []).length) failures.push("missing_property_examples");
  for (const p of pack.propertyExamples || []) {
    if (!nz(p.propertyName) || !nz(p.matchKey)) failures.push(`property_missing_name:${p.url || "?"}`);
    if (p.propertyName !== p.matchKey) {
      failures.push(`property_match_key_mismatch:${p.propertyName}`);
    }
    if (!["CALA", "International Reference"].includes(p.geographyLabel)) {
      failures.push(`property_geography_label:${p.propertyName}`);
    }
  }
  if (!(pack.recentMomentumCandidates || []).length) {
    failures.push("missing_recent_momentum_candidates");
  }
  for (const m of pack.recentMomentumCandidates || []) {
    if (!nz(m.dateLine)) failures.push(`momentum_missing_date:${m.title || "?"}`);
    if (!/^https?:\/\//i.test(nz(m.announcementUrl))) {
      failures.push(`momentum_missing_url:${m.title || "?"}`);
    }
    if (!nz(m.linkLabel)) failures.push(`momentum_missing_link_label:${m.title || "?"}`);
  }
  const tgs = pack.targetGuestSegmentsRecommendation?.recommended || [];
  if (!tgs.length) failures.push("missing_tgs_recommendation");
  const joined = tgs.join(", ");
  if (/Luxury\s*\/\s*Discerning/i.test(joined) && /\bLeisure\b/i.test(joined)) {
    failures.push("tgs_luxury_discerning_leisure_adjacency");
  }
  if (pack.writeAirtable === true) failures.push("write_airtable_must_be_false");
  if (pack.writeTargetGuestSegments === true) failures.push("tgs_write_must_be_false");
  if (pack.writePresentation === true) failures.push("presentation_write_must_be_false");
  return failures;
}

function renderPackMarkdown(pack, validationFailures) {
  const lines = [
    `# Wave 13 Source Pack — ${pack.name}`,
    "",
    `Slug: \`${pack.slug}\` · Brand Basics ID: \`${pack.recordId || "null"}\` · Brand Basics name: **${pack.brandBasicsName || "(none)"}**`,
    `Brand Status: **${pack.brandStatus || "(none)"}** · Parent/platform: **${pack.parentPlatform}**`,
    `Version: \`${WAVE13_SOURCE_PACKS_VERSION}\` · Airtable writes: **false**`,
    `CALA availability: **${pack.calaAvailability}** · International Reference required: **${pack.internationalReferenceRequired === true}**`,
    `CALA-first posture: ${pack.calaFirstPosture || "—"}`,
    "",
    "## Brand lens",
    "",
    pack.lens,
    "",
    "## Official brand page",
    "",
    `- [${pack.officialBrandPage.label}](${pack.officialBrandPage.url}) — trust: ${pack.officialBrandPage.trust}`,
    pack.officialBrandPage.note ? `  - Note: ${pack.officialBrandPage.note}` : "",
    "",
  ];

  if (pack.developmentPage) {
    lines.push(
      "## Official development page",
      "",
      `- [${pack.developmentPage.label}](${pack.developmentPage.url}) — trust: ${pack.developmentPage.trust}`,
      pack.developmentPage.note ? `  - Note: ${pack.developmentPage.note}` : "",
      ""
    );
  } else {
    lines.push(
      "## Official development page",
      "",
      "_Not located as a dedicated brand development URL; use brand page + labeled parent/platform hubs._",
      ""
    );
  }

  lines.push("## Parent / platform context (labeled)", "");
  for (const p of pack.parentPlatformContext || []) {
    lines.push(`- [${p.label}](${p.url}) — ${p.note || "Parent/platform context only."}`);
  }
  lines.push("");

  if (pack.officialNamingAssessment) {
    const n = pack.officialNamingAssessment;
    lines.push(
      "## Official naming assessment (SO/)",
      "",
      `- Variants seen: ${(n.variantsSeen || []).map((v) => `\`${v}\``).join(", ")}`,
      `- Recommended display: **${n.recommendedDisplayName}**`,
      `- Recommended Brand Basics Name: **${n.recommendedBrandBasicsName}**`,
      `- Rationale: ${n.rationale}`,
      `- Create in this stage: **false**`,
      ""
    );
  }

  if (pack.brandBasicsCreationRecommendation) {
    const c = pack.brandBasicsCreationRecommendation;
    lines.push(
      "## Brand Basics creation recommendation (do not create now)",
      "",
      `- Brand Name: \`${c.brandName}\``,
      `- slug: \`${c.slug}\``,
      `- parent/platform: \`${c.parentPlatform}\``,
      `- initial Brand Status: \`${c.initialBrandStatus}\``,
      ...(c.notes || []).map((n) => `- Note: ${n}`),
      ""
    );
  }

  if (pack.namingDisplayIssue) {
    const n = pack.namingDisplayIssue;
    lines.push(
      "## Naming / display issue (document only — no rename)",
      "",
      `- Brand Basics name: **${n.brandBasicsName}**`,
      `- Slug: \`${n.slug}\``,
      `- Consumer/legal often uses: **${n.consumerOfficialOftenUses}**`,
      `- ${n.recommendation}`,
      `- Rename in this stage: **false**`,
      ""
    );
  }

  if (pack.officialStatusAssessment) {
    const s = pack.officialStatusAssessment;
    lines.push(
      "## Official status / platform assessment",
      "",
      `- Status: \`${s.status}\``,
      `- Current official successor (if any): **${s.currentOfficialSuccessor || "—"}**`,
      s.successorBrandPage ? `- Successor page: ${s.successorBrandPage}` : "",
      s.historicalLaunchSource ? `- Historical launch: ${s.historicalLaunchSource}` : "",
      s.ennismorePortfolioNote ? `- ${s.ennismorePortfolioNote}` : "",
      `- Recommendation: ${s.recommendation}`,
      ""
    );
  }

  lines.push(
    "## Property / opening examples",
    "",
    "URLs are matched by **property name** (`matchKey`), never by array index.",
    "",
    "| Property name (match key) | Geography | Market | Official URL |",
    "| --- | --- | --- | --- |"
  );
  for (const p of pack.propertyExamples || []) {
    lines.push(`| ${p.propertyName} | ${p.geographyLabel} | ${p.market} | ${p.url} |`);
  }
  lines.push("");
  for (const p of pack.propertyExamples || []) {
    if (p.note) lines.push(`- **${p.propertyName}:** ${p.note}`);
  }
  lines.push("");

  lines.push(
    "## Recent Momentum candidates",
    "",
    "Each candidate includes **date + structured link label + announcement URL** (URL stays in source pack / trailing link field — not raw in public body prose).",
    "",
    "| Date | Title | Geography | Link label | Announcement URL |",
    "| --- | --- | --- | --- | --- |"
  );
  for (const m of pack.recentMomentumCandidates || []) {
    lines.push(
      `| ${m.dateLine} | ${m.title} | ${m.geographyLabel} | ${m.linkLabel} | ${m.announcementUrl} |`
    );
  }
  lines.push("");
  for (const m of pack.recentMomentumCandidates || []) {
    lines.push(`### ${m.title}`, "", m.summary, "");
    if (m.whyRelevant) lines.push(`_Why relevant:_ ${m.whyRelevant}`, "");
  }

  lines.push("## Image source candidates", "");
  for (const img of pack.imageSourceHints || []) {
    lines.push(`- [${img.label}](${img.url})${img.note ? ` — ${img.note}` : ""}`);
  }
  lines.push("");

  const tgs = pack.targetGuestSegmentsRecommendation || {};
  lines.push(
    "## Target Guest Segments recommendation (do not write yet)",
    "",
    `- Recommended: **${(tgs.recommended || []).join(", ")}**`,
    `- Avoid: ${(tgs.avoid || []).join("; ") || TGS_AVOID_NOTE}`,
    `- Rationale: ${tgs.rationale || ""}`,
    "",
    `> ${TGS_AVOID_NOTE}`,
    "",
    "## Owner-facing positioning notes",
    "",
    ...(pack.ownerFacingPositioningNotes || []).map((n) => `- ${n}`),
    "",
    "## Sibling-brand distinction notes",
    "",
    ...(pack.siblingBrandDistinctionNotes || []).map((n) => `- ${n}`),
    "",
    "## Distinguish from (slugs)",
    "",
    ...(pack.distinguishFrom || []).map((s) => `- \`${s}\``),
    "",
    "## Source gaps",
    "",
    ...(pack.sourceGaps || []).map((n) => `- ${n}`),
    "",
    "## Manual review risks",
    "",
    ...(pack.manualReviewRisks || []).map((n) => `- ${n}`),
    "",
    "## Stage 4 readiness recommendation",
    "",
    pack.stage4ReadinessRecommendation || "—",
    "",
    "## Steward notes",
    "",
    ...(pack.notes || []).map((n) => `- ${n}`),
    "",
    "## Validation",
    "",
    validationFailures.length
      ? validationFailures.map((f) => `- FAIL: \`${f}\``).join("\n")
      : "- PASS — pack structure meets Stage 3 acceptance checks",
    "",
    "## Protections",
    "",
    "- No Airtable writes",
    "- No Presentation / Brand Status / release / CV / Source Library / Registry writes",
    "- No Target Guest Segments writes in this stage",
    "- No SO/ Brand Basics creation in this stage",
    "- No Fairmont rename in this stage",
    "- No protected 39 Presentation/Basics writes",
    ""
  );
  return `${lines.filter((l) => l !== undefined).join("\n").replace(/\n{3,}/g, "\n\n")}\n`;
}

function packFileSlug(slug) {
  return `brand-explorer-wave13-source-pack-${slug}.md`;
}

function writeDeferred({ reason, message, nextCommand, dryRun, gate }) {
  ensureDir(REPORTS_DIR);
  const deferred = {
    version: WAVE13_SOURCE_PACKS_VERSION,
    factoryVersion: WAVE13_VERSION,
    stage: "source-packs",
    generatedAt: new Date().toISOString(),
    dryRun: dryRun !== false,
    deferred: true,
    pass: false,
    stopRecommended: true,
    airtableWrites: false,
    reason,
    message,
    nextRequired: nextCommand,
    gate,
    summary: { deferred: true, stage: "source-packs", reason },
  };
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-source-packs-deferred.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-source-packs-deferred.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(deferred, null, 2)}\n`, "utf8");
  fs.writeFileSync(
    mdPath,
    [
      "# Wave 13 source-packs — Deferred",
      "",
      `Reason: \`${reason}\``,
      "",
      message,
      "",
      `Next: \`${nextCommand}\``,
      "",
    ].join("\n"),
    "utf8"
  );
  return {
    version: WAVE13_VERSION,
    stage: "source-packs",
    generatedAt: deferred.generatedAt,
    dryRun: deferred.dryRun,
    deferred: true,
    pass: false,
    stopRecommended: true,
    airtableWrites: false,
    reason,
    message,
    nextRequired: nextCommand,
    gate,
    summary: deferred.summary,
    paths: { jsonPath, mdPath },
  };
}

export async function runWave13SourcePacks({
  dryRun = true,
  reuseFreshReports = false,
  maxAgeMs = DEFAULT_MAX_AGE_MS,
} = {}) {
  ensureDir(REPORTS_DIR);
  ensureDir(DOCS_DIR);

  const gate = assessWave13SourcePacksGate({ reuseFreshReports, maxAgeMs });
  if (!gate.ok) {
    return writeDeferred({
      reason: gate.reason,
      message: `DEFERRED: Wave 13 stage 'source-packs' blocked — ${gate.reason}.`,
      nextCommand: gate.nextCommand,
      dryRun,
      gate,
    });
  }

  const brands = [];
  const packPaths = [];
  let passCount = 0;

  for (const slug of WAVE13_SLUGS) {
    const plan = WAVE13_BRAND_PLAN[slug];
    const pack = getWave13SourcePack(slug);
    const failures = validatePack(pack);
    const ok = failures.length === 0;
    if (ok) passCount += 1;

    const mdName = packFileSlug(slug);
    const mdPath = path.join(REPORTS_DIR, mdName);
    if (pack) {
      fs.writeFileSync(mdPath, renderPackMarkdown(pack, failures), "utf8");
      packPaths.push(mdPath);
    }

    const calaCount = (pack?.propertyExamples || []).filter((p) => p.geographyLabel === "CALA").length;
    const intlCount = (pack?.propertyExamples || []).filter(
      (p) => p.geographyLabel === "International Reference"
    ).length;

    brands.push({
      slug,
      name: pack?.name || plan?.name || slug,
      brandBasicsName: pack?.brandBasicsName ?? null,
      recordId: pack?.recordId || null,
      brandStatus: pack?.brandStatus ?? null,
      missingBrandBasics: pack?.missingBrandBasics === true || !pack?.recordId,
      parentPlatform: pack?.parentPlatform || plan?.parentPlatform || null,
      calaAvailability: pack?.calaAvailability || "unknown",
      calaFirstPosture: pack?.calaFirstPosture || null,
      internationalReferenceRequired: pack?.internationalReferenceRequired === true,
      hasOfficialBrandPage: Boolean(pack?.officialBrandPage?.url),
      hasDevelopmentPage: Boolean(pack?.developmentPage?.url),
      parentContextCount: (pack?.parentPlatformContext || []).length,
      propertyExampleCount: (pack?.propertyExamples || []).length,
      calaPropertyCount: calaCount,
      internationalReferencePropertyCount: intlCount,
      momentumCandidateCount: (pack?.recentMomentumCandidates || []).length,
      imageSourceCandidateCount: (pack?.imageSourceHints || []).length,
      targetGuestSegmentsRecommended: pack?.targetGuestSegmentsRecommendation?.recommended || [],
      stage4ReadinessRecommendation: pack?.stage4ReadinessRecommendation || null,
      brandBasicsCreationRecommendation: pack?.brandBasicsCreationRecommendation || null,
      namingDisplayIssue: pack?.namingDisplayIssue || null,
      officialStatusAssessment: pack?.officialStatusAssessment
        ? {
            status: pack.officialStatusAssessment.status,
            currentOfficialSuccessor: pack.officialStatusAssessment.currentOfficialSuccessor,
          }
        : null,
      sourceGaps: pack?.sourceGaps || [],
      manualReviewRisks: pack?.manualReviewRisks || [],
      packPath: `reports/${mdName}`,
      validationPass: ok,
      validationFailures: failures,
      writeAirtable: false,
      writeTargetGuestSegments: false,
      writePresentation: false,
    });
  }

  const so = brands.find((b) => b.slug === "so-hotels-and-resorts");
  const house = brands.find((b) => b.slug === "the-house-of-originals");
  const fairmont = brands.find((b) => b.slug === "fairmont-hotels-and-resorts");

  const mayProceedToSoBrandBasicsCreation = Boolean(so?.validationPass);
  const soBasicsStillUncreated = so?.missingBrandBasics === true;
  const houseRequiresManualReview =
    house?.officialStatusAssessment?.status === "likely_superseded_manual_review_required";
  const mayProceedToTabFactoryBuild =
    passCount === WAVE13_SLUGS.length &&
    soBasicsStillUncreated === false &&
    houseRequiresManualReview === false;

  const summary = {
    version: WAVE13_SOURCE_PACKS_VERSION,
    factoryVersion: WAVE13_VERSION,
    stage: "source-packs",
    generatedAt: new Date().toISOString(),
    dryRun: dryRun !== false,
    deferred: false,
    airtableWrites: false,
    presentationWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    targetGuestSegmentsWrites: false,
    soBrandBasicsCreated: false,
    fairmontRenamed: false,
    protected39Writes: false,
    protectedBaselineCount: WAVE13_PROTECTED_BASELINE_COUNT,
    expectedPacks: WAVE13_SLUGS.length,
    packsCreated: packPaths.length,
    packsPassingValidation: passCount,
    allPacksCreated: packPaths.length === WAVE13_SLUGS.length,
    allPacksValid: passCount === WAVE13_SLUGS.length,
    brandsWithOfficialBrandPage: brands.filter((b) => b.hasOfficialBrandPage).length,
    brandsWithCalaExamples: brands.filter((b) => b.calaPropertyCount > 0).length,
    brandsIntlReferenceOnly: brands.filter(
      (b) => b.calaPropertyCount === 0 && b.internationalReferencePropertyCount > 0
    ).length,
    gate,
    openItems: {
      missingBrandBasics: brands.filter((b) => b.missingBrandBasics).map((b) => b.slug),
      fairmontNamingDisplayIssue: Boolean(fairmont?.namingDisplayIssue),
      houseOfOriginalsManualReview: houseRequiresManualReview,
    },
    mayProceedToSoBrandBasicsCreation,
    mayProceedToTabFactoryBuild,
    stage4Blockers: [
      ...(soBasicsStillUncreated
        ? ["so-hotels-and-resorts missing Brand Basics — create in separate stage first"]
        : []),
      ...(houseRequiresManualReview
        ? [
            "the-house-of-originals official status likely superseded by Morgans Originals — founder/manual review required",
          ]
        : []),
    ],
    readyStatement:
      passCount === WAVE13_SLUGS.length
        ? "wave13_source_packs_ready_with_open_items"
        : "wave13_source_packs_incomplete",
    nextStage: "tab-factory-build",
    wave13ResumeNote:
      "Stage 3 source packs complete (read-only). Create SO/ Brand Basics separately; resolve House of Originals manual review; then proceed to Stage 4 tab-factory-build for ready brands only.",
  };

  const report = {
    ...summary,
    tgsAvoidRule: TGS_AVOID_NOTE,
    brands,
    packFiles: packPaths.map((p) => path.relative(ROOT, p).replace(/\\/g, "/")),
    knownPackSlugs: Object.keys(WAVE13_SOURCE_PACKS_BY_SLUG),
  };

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-source-pack-summary.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-source-pack-summary.md");
  const docPath = path.join(DOCS_DIR, "brand-explorer-wave13-source-packs.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const md = [
    "# Wave 13 — Official Source Pack Summary",
    "",
    `Version: \`${WAVE13_SOURCE_PACKS_VERSION}\` · Generated: ${summary.generatedAt}`,
    `Dry-run: **${summary.dryRun}** · Airtable writes: **false**`,
    `Ready: \`${summary.readyStatement}\``,
    "",
    "## Gate",
    "",
    `- Source: \`${gate.source}\``,
    `- Reused fresh reports: **${gate.reusedFreshReports === true}**`,
    `- Preflight: \`${gate.preflightReadyStatement}\``,
    `- Manifest: \`${gate.manifestReadyStatement}\``,
    `- Cohort: \`${gate.cohortReadyStatement || "—"}\`${gate.cohortWarning ? " _(warning)_" : ""}`,
    ...(gate.warnings || []).map((w) => `- Warning: ${w.message}`),
    "",
    "## Acceptance",
    "",
    `| Check | Result |`,
    `| --- | --- |`,
    `| Packs created | ${summary.packsCreated}/${summary.expectedPacks} |`,
    `| Packs valid | ${summary.packsPassingValidation}/${summary.expectedPacks} |`,
    `| Official brand pages | ${summary.brandsWithOfficialBrandPage}/8 |`,
    `| Brands with CALA examples | ${summary.brandsWithCalaExamples} |`,
    `| Brands International Reference only | ${summary.brandsIntlReferenceOnly} |`,
    `| SO/ Brand Basics created | **false** (recommendation only) |`,
    `| Fairmont renamed | **false** (documented only) |`,
    `| Protected 39 changes | **false** |`,
    `| May proceed to SO/ Brand Basics creation | **${mayProceedToSoBrandBasicsCreation}** |`,
    `| May proceed to Stage 4 tab-factory-build | **${mayProceedToTabFactoryBuild}** |`,
    "",
    summary.allPacksCreated && summary.allPacksValid
      ? "**Stage 3 PASS (with open items)** — see Stage 4 blockers before content generation."
      : "**Stage 3 incomplete** — fix validation failures before content generation.",
    "",
    "## Stage 4 blockers",
    "",
    ...(summary.stage4Blockers.length
      ? summary.stage4Blockers.map((b) => `- ${b}`)
      : ["- None"]),
    "",
    "## Per-brand packs",
    "",
    "| Brand | Basics ID | Status | CALA avail | Props (CALA/Intl) | Momentum | TGS | Pack | Valid |",
    "| --- | --- | --- | --- | ---: | ---: | --- | --- | --- |",
  ];
  for (const b of brands) {
    md.push(
      `| ${b.name} | \`${b.recordId || "null"}\` | ${b.brandStatus || "—"} | ${b.calaAvailability} | ${b.calaPropertyCount}/${b.internationalReferencePropertyCount} | ${b.momentumCandidateCount} | ${(b.targetGuestSegmentsRecommended || []).join(", ")} | [\`${path.basename(b.packPath)}\`](${b.packPath}) | ${b.validationPass ? "PASS" : "FAIL"} |`
    );
  }
  md.push(
    "",
    "## Target Guest Segments rule",
    "",
    TGS_AVOID_NOTE,
    "",
    "Recommendations are recorded for a later approved Brand Basics patch stage — **not written now**.",
    "",
    "## Protections",
    "",
    "- No Airtable writes",
    "- No Presentation / Brand Status / release field writes",
    "- No Company Validated / Source Library / Registry writes",
    "- No protected 39 Presentation or Basics changes",
    "- No SO/ Brand Basics creation",
    "- No Fairmont rename",
    "- No Wave 13 content generation in this stage",
    "",
    "## Next",
    "",
    summary.wave13ResumeNote,
    "",
    "```bash",
    "npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run",
    "npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run --reuse-fresh-reports",
    "```",
    ""
  );
  fs.writeFileSync(mdPath, `${md.join("\n")}\n`, "utf8");

  const doc = [
    "# Brand Explorer — Wave 13 Official Source Packs",
    "",
    `Version: \`${WAVE13_SOURCE_PACKS_VERSION}\``,
    "",
    "## Purpose",
    "",
    "Stage 3 of the Wave 13 factory builds **official source packs** for 8 Accor / Accor-adjacent brands before any tab-factory content generation or SO/ Brand Basics creation.",
    "",
    "## Source hierarchy",
    "",
    "1. Brand-specific official brand page",
    "2. Brand-specific development page where available",
    "3. Official property pages (match by **property name**, not array index)",
    "4. Official parent-company pages — **parent/platform context only**",
    "5. Credible announcement / opening / development sources",
    "6. Image sources tied to official brand or property pages",
    "",
    "## Geography labels",
    "",
    "- **CALA** — Caribbean & Latin America examples preferred when available",
    "- **International Reference** — non-CALA examples explicitly labeled",
    "",
    "## Gate",
    "",
    "Source-packs runs when:",
    "",
    "- Wave 13 preflight = PASS / `protected_39_live_clean_wave13_may_resume`",
    "- Wave 13 manifest = ready / `mayProceedToFactoryPreviewCohort = true`",
    "- Factory-preview-cohort = applied or dry-run ready (soft warning if not)",
    "",
    "Accept same-session reports, or `--reuse-fresh-reports` when Wave 13 reports still pass **and** live PVQL + quality audits are clean and current. Never proceed on stale cached PVQL alone.",
    "",
    "## Open items retained",
    "",
    "- **SO/ Hotels & Resorts** — missing Brand Basics; creation recommendation documented; not created in Stage 3",
    "- **Fairmont** — Brand Basics name `Fairmont` vs consumer `Fairmont Hotels & Resorts`; documented; not renamed",
    "- **The House of Originals** — likely superseded by **Morgans Originals**; founder/manual review before Stage 4",
    "",
    "## Target Guest Segments",
    "",
    TGS_AVOID_NOTE,
    "",
    "## Command",
    "",
    "```bash",
    "npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run",
    "npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run --reuse-fresh-reports",
    "```",
    "",
    "## Outputs",
    "",
    "- `reports/brand-explorer-wave13-source-pack-<slug>.md` (8 files)",
    "- `reports/brand-explorer-wave13-source-pack-summary.{json,md}`",
    "- This doc",
    "",
    "## Next stage",
    "",
    "1. Separate SO/ Brand Basics creation (recommended values in SO pack)",
    "2. Founder review for House of Originals vs Morgans Originals",
    "3. Stage 4 — `tab-factory-build` for brands that clear open items",
    "",
  ];
  fs.writeFileSync(docPath, `${doc.join("\n")}\n`, "utf8");

  // Replace deferred stubs with a pointer to the real summary (no longer deferred).
  const deferredJson = path.join(REPORTS_DIR, "brand-explorer-wave13-source-packs-deferred.json");
  const deferredMd = path.join(REPORTS_DIR, "brand-explorer-wave13-source-packs-deferred.md");
  const deferredReplacement = {
    version: WAVE13_SOURCE_PACKS_VERSION,
    stage: "source-packs",
    generatedAt: summary.generatedAt,
    deferred: false,
    superseded: true,
    message:
      "Wave 13 source-packs is no longer deferred. See brand-explorer-wave13-source-pack-summary.{json,md}.",
    summaryPath: "reports/brand-explorer-wave13-source-pack-summary.json",
    readyStatement: summary.readyStatement,
  };
  fs.writeFileSync(deferredJson, `${JSON.stringify(deferredReplacement, null, 2)}\n`, "utf8");
  fs.writeFileSync(
    deferredMd,
    [
      "# Wave 13 source-packs — No longer deferred",
      "",
      "Stage 3 source packs have been generated.",
      "",
      "See:",
      "",
      "- `reports/brand-explorer-wave13-source-pack-summary.md`",
      "- `reports/brand-explorer-wave13-source-pack-summary.json`",
      "- `docs/data-intelligence/brand-explorer-wave13-source-packs.md`",
      "",
      `Ready: \`${summary.readyStatement}\``,
      "",
    ].join("\n"),
    "utf8"
  );

  return {
    version: WAVE13_VERSION,
    stage: "source-packs",
    generatedAt: summary.generatedAt,
    dryRun: summary.dryRun,
    deferred: false,
    pass: summary.allPacksCreated && summary.allPacksValid,
    stopRecommended: !(summary.allPacksCreated && summary.allPacksValid),
    airtableWrites: false,
    readyStatement: summary.readyStatement,
    mayProceedToSoBrandBasicsCreation,
    mayProceedToTabFactoryBuild,
    gate,
    summary: {
      packsCreated: summary.packsCreated,
      packsPassingValidation: summary.packsPassingValidation,
      allPacksValid: summary.allPacksValid,
      brandsWithCalaExamples: summary.brandsWithCalaExamples,
      mayProceedToSoBrandBasicsCreation,
      mayProceedToTabFactoryBuild,
      stage4Blockers: summary.stage4Blockers,
      nextStage: summary.nextStage,
      readyStatement: summary.readyStatement,
    },
    paths: {
      jsonPath,
      mdPath,
      docPath,
      packPaths,
      deferredJson,
      deferredMd,
    },
    report,
  };
}
