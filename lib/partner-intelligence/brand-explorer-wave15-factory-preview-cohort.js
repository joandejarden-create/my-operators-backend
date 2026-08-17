/**
 * Wave 15 Stage 2 — Factory preview cohort (code/config only; no Airtable).
 *
 * Replaces factory-preview-candidates.js current cohort with Wave 15
 * Hilton Brand Family targets. Never writes Brand Status, release fields, CV,
 * Source Library, Registry, or protected 54 Presentation/Basics.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FACTORY_PREVIEW_CANDIDATE_SLUGS,
  FACTORY_PREVIEW_DISPLAY_STATE,
  FACTORY_PREVIEW_BANNER_TEXT,
  assertFactoryPreviewDoesNotAffectActiveUniverse,
} from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE15_SLUGS,
  WAVE15_FACTORY_PREVIEW_APPLY_FLAGS,
  WAVE15_PROTECTED_BASELINE_COUNT,
  getWave15Plan,
} from "./brand-explorer-wave15-factory-plan.js";
import { EXPECTED_ACTIVE_COUNT_54 } from "./brand-explorer-54-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";

export const WAVE15_FACTORY_PREVIEW_COHORT_VERSION =
  "brand-explorer-wave15-factory-preview-cohort-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const MODULE_PATH = path.join(
  ROOT,
  "lib/partner-intelligence/brand-explorer-factory-preview-candidates.js"
);

function writeJsonMd(base, report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, `${base}.json`);
  const mdPath = path.join(REPORTS_DIR, `${base}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);
  return { jsonPath, mdPath };
}

function parseApplyFlags(argv, required) {
  const missing = required.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function readManifest() {
  const p = path.join(REPORTS_DIR, "brand-explorer-wave15-manifest.json");
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function planWave15FactoryPreviewCohort(manifestBrands = []) {
  const identities = {};
  for (const slug of WAVE15_SLUGS) {
    const plan = getWave15Plan(slug);
    const live = (manifestBrands || []).find((b) => b.slug === slug) || {};
    identities[slug] = {
      slug,
      name: live.brandName || plan.name,
      recordId: live.recordId || null,
      recommendedStatusWhileInFactory: plan.recommendedStatusWhileInFactory,
      parentPlatform: plan.parentPlatform || live.parentCompany || "Hilton Worldwide",
      wave: "wave15",
      classification: live.classification || null,
      brandStatus: live.brandStatus || null,
      missingBasics: live.classification === "missing_brand_basics_record",
    };
  }
  return {
    version: WAVE15_FACTORY_PREVIEW_COHORT_VERSION,
    stage: "factory-preview-cohort",
    displayState: FACTORY_PREVIEW_DISPLAY_STATE,
    bannerText: FACTORY_PREVIEW_BANNER_TEXT,
    candidateSlugs: [...WAVE15_SLUGS],
    identities,
    priorCandidatesPreservedNote:
      "Wave 14 Marriott factory preview brands that are now Active/Live sit in the protected 54 baseline — Wave 15 replaces the factory preview allowlist with Hilton Brand Family targets.",
    affectsActiveUniverse: false,
    airtableWrites: false,
    protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
    aiAssistedFootnoteInPreview: true,
  };
}

export function renderWave15FactoryPreviewCandidatesModule({ candidateSlugs, identities }) {
  const existing = fs.readFileSync(MODULE_PATH, "utf8");
  const keepFrom = existing.indexOf("export const FACTORY_PREVIEW_BANNER_TEXT");
  if (keepFrom < 0) {
    throw new Error("factory-preview-candidates.js missing FACTORY_PREVIEW_BANNER_TEXT anchor");
  }
  const tail = existing.slice(keepFrom);

  const slugLit = candidateSlugs.map((s) => `  "${s}",`).join("\n");
  const idBlocks = candidateSlugs
    .map((slug) => {
      const id = identities[slug] || {};
      const recordLine = id.recordId
        ? `    recordId: "${id.recordId}",`
        : `    recordId: null, // fill after Brand Basics create`;
      return [
        `  "${slug}": Object.freeze({`,
        `    slug: "${slug}",`,
        `    name: ${JSON.stringify(id.name || slug)},`,
        recordLine,
        `    recommendedStatusWhileInFactory: ${JSON.stringify(
          id.recommendedStatusWhileInFactory || "Under Review"
        )},`,
        `  }),`,
      ].join("\n");
    })
    .join("\n");

  return `/**
 * Brand Explorer — Factory Preview Mode candidates.
 *
 * Wave 15 factory cohort (${candidateSlugs.length} Hilton Brand Family brands). Separate from
 * production Active/Live universe and the protected 54 public-full baseline.
 *
 * Production public-full remains: Brand Status Active/Live + release gates + PVQL.
 * Factory preview never writes Airtable.
 * AI-Assisted Profile footnote must render in preview mode (always-on enricher).
 */
import { isBrandStatusActive } from "../brand-status-active.js";
import { ACTIVE_UNIVERSE_SOURCE } from "./brand-explorer-active-universe.js";

export const FACTORY_PREVIEW_VERSION = "factory-preview-mode-v5-wave15";

/** Effective UI display state while factory preview is active (internal only). */
export const FACTORY_PREVIEW_DISPLAY_STATE = "factory_preview_internal";

/**
 * Wave 15 factory candidate cohort for new brand setup work.
 * Not a public release registry. Not the Active/Live universe.
 */
export const FACTORY_PREVIEW_CANDIDATE_SLUGS = Object.freeze([
${slugLit}
]);

/** Known identity anchors for deep-link preview (record ID preferred over slug). */
export const FACTORY_PREVIEW_CANDIDATE_IDENTITIES = Object.freeze({
${idBlocks}
});

${tail}`;
}

/**
 * @param {{ apply?: boolean, argv?: string[] }} opts
 */
export async function runWave15FactoryPreviewCohort({ apply = false, argv = [] } = {}) {
  const manifest = readManifest();
  if (!manifest || manifest.deferred || manifest.mayProceedToFactoryPreviewCohort !== true) {
    const deferred = {
      version: WAVE15_FACTORY_PREVIEW_COHORT_VERSION,
      stage: "factory-preview-cohort",
      generatedAt: new Date().toISOString(),
      dryRun: true,
      deferred: true,
      status: "deferred",
      reason: "wave15_manifest_not_ready",
      message:
        "Factory preview cohort deferred — run a clean Wave 15 manifest dry-run first.",
      nextRequired:
        "npm run brand-explorer-wave15-factory -- --stage manifest --dry-run --reuse-fresh-reports",
      airtableWrites: false,
      pass: false,
      stopRecommended: true,
    };
    const paths = writeJsonMd(
      "brand-explorer-wave15-factory-preview-cohort",
      deferred,
      `# Wave 15 Factory Preview Cohort — Deferred\n\n${deferred.message}\n`
    );
    return { ...deferred, paths };
  }

  const manifestBrands = manifest.brands || [];
  if (manifestBrands.some((b) => b.isActiveLive)) {
    throw new Error(
      "Wave15 Active/Live drift detected — refuse factory-preview-cohort until resolved"
    );
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  const activeSlugs = new Set((universe.brands || []).map((b) => b.slug));
  const drift = WAVE15_SLUGS.filter((s) => activeSlugs.has(s));
  if (drift.length) {
    throw new Error(`Wave15 Active/Live drift: ${drift.join(", ")}`);
  }
  if (universe.totalCount !== EXPECTED_ACTIVE_COUNT_54) {
    throw new Error(
      `Protected Active/Live count is ${universe.totalCount}, expected ${EXPECTED_ACTIVE_COUNT_54}`
    );
  }

  const plan = planWave15FactoryPreviewCohort(manifestBrands);
  const nextSource = renderWave15FactoryPreviewCandidatesModule(plan);
  const flags = parseApplyFlags(argv, WAVE15_FACTORY_PREVIEW_APPLY_FLAGS);
  const invariant = assertFactoryPreviewDoesNotAffectActiveUniverse();

  const openItems = {
    missingBrandBasics: manifestBrands
      .filter((b) => b.classification === "missing_brand_basics_record")
      .map((b) => b.slug),
    blockedManualReview: manifestBrands
      .filter((b) => b.classification === "blocked_requires_manual_review")
      .map((b) => b.slug),
  };

  const report = {
    ...plan,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: apply,
    applyFlagsOk: !apply || flags.ok,
    missingFlags: flags.missing,
    modulePath: path.relative(ROOT, MODULE_PATH).replace(/\\/g, "/"),
    priorCandidateSlugs: [...FACTORY_PREVIEW_CANDIDATE_SLUGS],
    protectedActiveCount: universe.totalCount,
    expectedProtectedCount: EXPECTED_ACTIVE_COUNT_54,
    activeUniverseDrift: drift,
    openItems,
    invariant,
    wroteModule: false,
    airtableWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    protectedBaselineWrites: false,
    presentationWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    pass: true,
    stopRecommended: false,
    readyStatement: apply
      ? "wave15_factory_preview_cohort_applied"
      : "wave15_factory_preview_cohort_dry_run_ready",
  };

  if (apply) {
    if (!flags.ok) {
      throw new Error(`Missing apply flags: ${flags.missing.join(", ")}`);
    }
    if (!invariant.ok) {
      throw new Error(`Factory preview invariant failed: ${invariant.errors.join(", ")}`);
    }
    fs.writeFileSync(MODULE_PATH, nextSource, "utf8");
    report.wroteModule = true;
  }

  const md = [
    `# Brand Explorer Wave 15 — Factory Preview Cohort`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Dry-run: **${!apply}** · Module written: **${report.wroteModule}**`,
    `Display state: \`${FACTORY_PREVIEW_DISPLAY_STATE}\``,
    `Banner: ${FACTORY_PREVIEW_BANNER_TEXT}`,
    `AI-Assisted footnote in preview: **true** (always-on enricher)`,
    `Airtable writes: **false**`,
    `Protected Active/Live: **${report.protectedActiveCount}** (expected ${report.expectedProtectedCount})`,
    `Ready: **${report.readyStatement}**`,
    ``,
    `## Candidates (${report.candidateSlugs.length})`,
    ``,
    ...report.candidateSlugs.map((s) => {
      const id = report.identities[s];
      const miss = id?.missingBasics ? " · **missing Brand Basics**" : "";
      return `- \`${s}\` — ${id?.name || s} (\`${id?.recordId || "no recordId yet"}\`)${miss}`;
    }),
    ``,
    `## Open items`,
    ``,
    openItems.missingBrandBasics.length
      ? openItems.missingBrandBasics.map((s) => `- Missing Basics: \`${s}\``).join("\n")
      : "- Missing Basics: none",
    openItems.blockedManualReview.length
      ? openItems.blockedManualReview.map((s) => `- Manual review: \`${s}\``).join("\n")
      : "- Manual review holds: none",
    ``,
    `## Notes`,
    ``,
    `- Replaces prior factory preview cohort (${report.priorCandidateSlugs.join(", ")}).`,
    `- Does not change Brand Status, release fields, or protected 54 baseline brands.`,
    `- No slug in the Wave 15 eight is held (unlike Wave 14 Four Points Flex by Sheraton).`,
    `- Next: Stage 3 source packs (read-only).`,
    ``,
  ].join("\n");

  const paths = writeJsonMd("brand-explorer-wave15-factory-preview-cohort", report, md);
  return {
    ...report,
    paths,
    nextSource,
    summary: { wroteModule: report.wroteModule, candidates: report.candidateSlugs.length },
  };
}
