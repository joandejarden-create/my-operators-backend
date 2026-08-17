/**
 * Brand Explorer — Factory Preview Mode audit + CLI (read-only).
 *
 * No Airtable writes. No Brand Status / CV / Source / Registry changes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FACTORY_PREVIEW_BANNER_TEXT,
  FACTORY_PREVIEW_CANDIDATE_SLUGS,
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_DISPLAY_STATE,
  FACTORY_PREVIEW_VERSION,
  assertFactoryPreviewDoesNotAffectActiveUniverse,
  buildFactoryPreviewUrls,
  canRenderFactoryPreview,
  factoryCandidateIsInActiveUniverseByStatus,
  getFactoryPreviewDisplayState,
} from "./brand-explorer-factory-preview-candidates.js";
import {
  loadActiveUniverse,
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
} from "./brand-explorer-active-universe.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  EXPECTED_ACTIVE_COUNT,
  REPORT_JSON,
  ROOT as BASELINE_ROOT,
} from "./brand-explorer-24-active-public-full-baseline.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

export const AUDIT_JSON = "brand-explorer-factory-preview-mode-audit.json";
export const AUDIT_MD = "brand-explorer-factory-preview-mode-audit.md";
export const STATUS_CORRECTION_MD = "brand-explorer-factory-preview-status-correction-plan.md";

/** Historical + wave12 expected factory-queue statuses (not Active/Live). */
export const PRIOR_EXPECTED_STATUS_BY_SLUG = Object.freeze({
  "even-hotels": "Under Review",
  "voco-hotels": "Under Review",
  "avid-hotels": "Under Review",
  "holiday-inn-express": "Under Review",
  "courtyard-by-marriott": "Under Review",
  "ac-hotels-by-marriott": "Under Review",
  "city-express-by-marriott": "Under Review",
  "moxy-hotels": "Under Review",
  "canopy-by-hilton": "Under Review",
  "motto-by-hilton": "Under Review",
  "tempo-by-hilton": "Under Review",
  "bunkhouse-hotels": "Under Review",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

async function fetchBrand(recordIdOrSlug) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordIdOrSlug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.success || !res.payload?.brand) {
    return {
      ok: false,
      statusCode: res.statusCode,
      error: res.payload?.error || res.payload || "fetch_failed",
    };
  }
  return { ok: true, brand: res.payload.brand };
}

function presentationRowCount(brand) {
  const blocks = brand?.brandExplorer?.blocks;
  return Array.isArray(blocks) ? blocks.length : 0;
}

/**
 * @param {{ dryRun?: boolean, writeReports?: boolean }} [options]
 */
export async function runFactoryPreviewModeAudit(options = {}) {
  const dryRun = options.dryRun !== false;
  const writeReports = options.writeReports !== false;
  const invariant = assertFactoryPreviewDoesNotAffectActiveUniverse();

  const universe = await loadActiveUniverse({ includeDetails: false });
  const activeSlugs = new Set(universe.brands.map((b) => nz(b.slug).toLowerCase()));

  let frozenSlugs = new Set();
  let frozenExcludedSlugs = new Set(
    NON_ACTIVE_STATUS_CONFLICT_PROBES.map((p) => nz(p.slug).toLowerCase())
  );
  const baselineFailures = [];
  try {
    const frozenPath = path.join(BASELINE_ROOT, "reports", REPORT_JSON);
    if (fs.existsSync(frozenPath)) {
      const frozen = JSON.parse(fs.readFileSync(frozenPath, "utf8"));
      frozenSlugs = new Set((frozen.brands || []).map((b) => nz(b.slug).toLowerCase()));
      for (const ex of frozen.excludedNonActive || []) {
        frozenExcludedSlugs.add(nz(ex.slug).toLowerCase());
      }
      if (universe.totalCount !== EXPECTED_ACTIVE_COUNT) {
        baselineFailures.push(
          `active_universe_count_changed:${universe.totalCount}_expected_${EXPECTED_ACTIVE_COUNT}`
        );
      }
      for (const slug of activeSlugs) {
        if (!frozenSlugs.has(slug)) {
          baselineFailures.push(`unexpected_active_brand:${slug}`);
        }
      }
      for (const slug of frozenExcludedSlugs) {
        if (activeSlugs.has(slug)) {
          baselineFailures.push(
            `excluded_brand_became_active_without_baseline_revision:${slug}`
          );
          baselineFailures.push(`excluded_brand_present_in_active_universe:${slug}`);
        }
      }
    } else {
      baselineFailures.push("frozen_baseline_missing");
    }
  } catch (err) {
    baselineFailures.push(`baseline_eval_error:${err.message || String(err)}`);
  }

  // Public-full probe uses brand API truth (avoid PVQL slug/record-id case normalization issues).
  const pvqlBySlug = new Map();

  const candidates = [];
  for (const slug of FACTORY_PREVIEW_CANDIDATE_SLUGS) {
    const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    const universeRow = universe.bySlug.get(slug) || null;
    const fetchId = identity?.recordId || slug;
    const fetched = await fetchBrand(fetchId);
    const brand = fetched.ok ? fetched.brand : null;
    const brandStatus =
      nz(brand?.brandStatus) ||
      nz(brand?.status) ||
      nz(universeRow?.status) ||
      "";
    const inActiveUniverse = activeSlugs.has(slug);
    const activeByStatus = factoryCandidateIsInActiveUniverseByStatus(brandStatus);
    const publicDisplayState = brand?.brandExplorerDisplayState || null;
    const publicShouldRender = brand?.shouldRenderFullProfile === true;
    const factoryDisplayState = getFactoryPreviewDisplayState(slug);
    const canPreview = canRenderFactoryPreview(brand || { slug, id: identity?.recordId }, {
      factoryPreview: true,
      slug,
      recordId: identity?.recordId,
      hasPresentationRows: presentationRowCount(brand) > 0,
      requirePresentationRows: true,
    });
    const baselineFailForSlug = baselineFailures.some((f) => String(f).includes(slug));
    const pvqlRow = pvqlBySlug.get(slug) || null;
    const urls = buildFactoryPreviewUrls({
      recordId: brand?.id || identity?.recordId,
      slug,
    });

    let factoryPreviewHtmlHasBanner = false;
    let publicHtmlLocked = null;
    if (brand) {
      try {
        const previewHtml = renderBrandExplorerHtmlForTest(brand, {
          factoryPreview: true,
          allPanels: true,
        });
        factoryPreviewHtmlHasBanner =
          previewHtml.includes("Factory Preview — Not Public") ||
          previewHtml.includes('data-be-display-gate="factory_preview_internal"');
        const publicHtml = renderBrandExplorerHtmlForTest(brand, { allPanels: true });
        publicHtmlLocked = publicHtml.includes('data-be-display-gate="profile-in-preparation"');
      } catch (err) {
        factoryPreviewHtmlHasBanner = false;
        publicHtmlLocked = `render_error:${err.message || String(err)}`;
      }
    }

    candidates.push({
      slug,
      name: brand?.name || identity?.name || slug,
      recordId: brand?.id || identity?.recordId || null,
      currentBrandStatus: brandStatus || null,
      priorExpectedStatus: PRIOR_EXPECTED_STATUS_BY_SLUG[slug] || null,
      recommendedStatusWhileInFactory:
        identity?.recommendedStatusWhileInFactory || "Under Review",
      presentationRowCount: presentationRowCount(brand),
      fetchOk: fetched.ok,
      fetchError: fetched.ok ? null : fetched.error,
      displayStatePublic: publicDisplayState,
      displayStateFactoryPreview: factoryDisplayState,
      shouldRenderFullProfilePublic: publicShouldRender,
      canRenderFactoryPreview: canPreview,
      accidentallyInActiveUniverse: inActiveUniverse || activeByStatus,
      inActiveUniverseByList: inActiveUniverse,
      activeByBrandStatus: activeByStatus,
      protectedBaselineFailsBecauseOfIt: baselineFailForSlug,
      baselineFailureCodes: baselineFailures.filter((f) => String(f).includes(slug)),
      pvqlPublicFullProfile: pvqlRow?.publicFullProfile === true || publicShouldRender,
      pvqlShouldRenderFullProfile:
        pvqlRow?.shouldRenderFullProfile === true || publicShouldRender,
      factoryPreview: brand?.factoryPreview || null,
      previewUrls: urls,
      factoryPreviewHtmlHasBanner,
      publicHtmlShowsPreparationGate: publicHtmlLocked,
      productionFullProfileStatesForbidden: [
        "active_profile_ready",
        "public_full",
        "legacyVisibilityUnlock",
        "public_restore_approved",
      ].includes(factoryDisplayState)
        ? "FAIL"
        : "ok",
      note:
        inActiveUniverse && !frozenSlugs.has(slug)
          ? "In Active/Live because Brand Status is Active/Live — not because of Factory Preview Mode. Revert status or revise baseline intentionally."
          : "Not in frozen 24 baseline as Active; Factory Preview does not add it to Active universe.",
    });
  }

  const activeDriftSlugs = candidates
    .filter((c) => c.accidentallyInActiveUniverse)
    .map((c) => c.slug);
  const recommendation =
    activeDriftSlugs.length > 0
      ? "revert_factory_candidates_to_under_review_and_use_factory_preview"
      : "factory_preview_ready_no_active_drift";

  const report = {
    version: FACTORY_PREVIEW_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    airtableWrites: false,
    brandStatusWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    factoryPreviewDisplayState: FACTORY_PREVIEW_DISPLAY_STATE,
    factoryPreviewBannerText: FACTORY_PREVIEW_BANNER_TEXT,
    activeUniverseCount: universe.totalCount,
    expectedProtectedBaselineCount: EXPECTED_ACTIVE_COUNT,
    activeUniverseDrift: universe.totalCount !== EXPECTED_ACTIVE_COUNT,
    activeDriftSlugs,
    invariant,
    baselineFailureSample: baselineFailures.slice(0, 20),
    candidates,
    recommendation,
    howToPreview: {
      query:
        "?brandId=<recordId>&beInternalPreview=1&factoryPreview=1",
      envAllowlist:
        "BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS=tapestry-collection-by-hilton,dazzler-by-wyndham,trademark-collection-by-wyndham",
      envEnable: "BRAND_EXPLORER_FACTORY_PREVIEW=1",
    },
  };

  if (writeReports) {
    fs.mkdirSync(REPORTS_DIR, { recursive: true });
    fs.writeFileSync(
      path.join(REPORTS_DIR, AUDIT_JSON),
      `${JSON.stringify(report, null, 2)}\n`,
      "utf8"
    );
    fs.writeFileSync(path.join(REPORTS_DIR, AUDIT_MD), formatAuditMarkdown(report), "utf8");
    fs.writeFileSync(
      path.join(REPORTS_DIR, STATUS_CORRECTION_MD),
      formatStatusCorrectionPlan(report),
      "utf8"
    );
  }

  return report;
}

function formatAuditMarkdown(report) {
  const lines = [
    `# Brand Explorer — Factory Preview Mode Audit`,
    ``,
    `> Read-only. Generated \`${report.generatedAt}\`. No Airtable writes.`,
    ``,
    `## Summary`,
    ``,
    `| Check | Result |`,
    `|---|---|`,
    `| Active universe count | **${report.activeUniverseCount}** (expected ${report.expectedProtectedBaselineCount}) |`,
    `| Active universe drift | ${report.activeUniverseDrift ? "YES" : "no"} |`,
    `| Drift slugs | ${report.activeDriftSlugs.join(", ") || "—"} |`,
    `| Factory display state | \`${report.factoryPreviewDisplayState}\` |`,
    `| Invariant (preview ≠ universe) | ${report.invariant?.ok ? "PASS" : "FAIL"} |`,
    `| Recommendation | \`${report.recommendation}\` |`,
    ``,
    `## Candidates`,
    ``,
  ];

  for (const c of report.candidates) {
    lines.push(`### ${c.name} (\`${c.slug}\`)`);
    lines.push(``);
    lines.push(`| Field | Value |`);
    lines.push(`|---|---|`);
    lines.push(`| Record ID | \`${c.recordId || "—"}\` |`);
    lines.push(`| Current Brand Status | **${c.currentBrandStatus || "—"}** |`);
    lines.push(`| Prior expected status | ${c.priorExpectedStatus || "—"} |`);
    lines.push(`| Presentation row count | ${c.presentationRowCount} |`);
    lines.push(`| displayState (public) | \`${c.displayStatePublic || "—"}\` |`);
    lines.push(`| displayState (factory preview) | \`${c.displayStateFactoryPreview}\` |`);
    lines.push(`| shouldRenderFullProfile (public) | ${c.shouldRenderFullProfilePublic} |`);
    lines.push(`| canRenderFactoryPreview | ${c.canRenderFactoryPreview} |`);
    lines.push(`| In active universe | ${c.accidentallyInActiveUniverse} |`);
    lines.push(`| Baseline fails because of it | ${c.protectedBaselineFailsBecauseOfIt} |`);
    lines.push(`| PVQL publicFullProfile | ${c.pvqlPublicFullProfile} |`);
    lines.push(`| Factory banner in preview HTML | ${c.factoryPreviewHtmlHasBanner} |`);
    lines.push(`| Public HTML preparation gate | ${c.publicHtmlShowsPreparationGate} |`);
    if (c.previewUrls?.combined) {
      lines.push(`| Preview URL | \`${c.previewUrls.combined}\` |`);
    }
    if (c.presentationRowCount === 0) {
      lines.push(
        `| Note | **No Presentation rows yet** — Factory Preview eligibility is true, but full atelier render waits on Presentation materialization. |`
      );
    }
    lines.push(``);
  }

  lines.push(`## Baseline failure sample`);
  lines.push(``);
  if (!report.baselineFailureSample?.length) {
    lines.push(`_(none)_`);
  } else {
    for (const f of report.baselineFailureSample) {
      lines.push(`- \`${f}\``);
    }
  }
  lines.push(``);
  lines.push(`## How to preview locally`);
  lines.push(``);
  lines.push(`1. Keep Brand Status as Draft / Under Review (do **not** set Active for visual QA).`);
  lines.push(
    `2. Open: \`/brand-explorer-combined.html?brandId=<recordId>&beInternalPreview=1&factoryPreview=1\``
  );
  lines.push(
    `3. Optional env: \`BRAND_EXPLORER_FACTORY_PREVIEW=1\` and \`BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS=...\``
  );
  lines.push(``);
  lines.push(`Banner must read: **${report.factoryPreviewBannerText}**`);
  lines.push(``);
  return `${lines.join("\n")}\n`;
}

function formatStatusCorrectionPlan(report) {
  const lines = [
    `# Brand Explorer — Factory Preview Status Correction Plan`,
    ``,
    `> **Plan only — do not apply automatically.** No Brand Status writes in Factory Preview Mode task.`,
    `> Generated \`${report.generatedAt}\`.`,
    ``,
    `## Context`,
    ``,
    `Founder temporarily set factory candidates to **Active** for local visual review. That correctly broke the protected 24-brand baseline (\`excluded_brand_became_active_without_baseline_revision\`).`,
    ``,
    `**Factory Preview Mode** now allows full internal profile render without Brand Status Active/Live.`,
    ``,
    `## Default recommendation`,
    ``,
    `Revert all three candidates to **Under Review** and use Factory Preview Mode for visual QA until factory pass + founder approval.`,
    ``,
    `| Slug | Current | Recommended | Rationale |`,
    `|---|---|---|---|`,
  ];

  for (const c of report.candidates) {
    const keepActive =
      c.currentBrandStatus === "Active" || c.currentBrandStatus === "Live"
        ? "revert to Under Review (unless founder intentionally wants production Active now)"
        : "keep current non-Active status; use Factory Preview";
    const recommended = c.recommendedStatusWhileInFactory || "Under Review";
    lines.push(
      `| \`${c.slug}\` | ${c.currentBrandStatus || "—"} | **${recommended}** | ${keepActive} |`
    );
  }

  lines.push(``);
  lines.push(`## Per-candidate decision checklist`);
  lines.push(``);

  for (const c of report.candidates) {
    lines.push(`### ${c.name}`);
    lines.push(``);
    lines.push(`- [ ] **Option A (recommended):** Revert Brand Status → **Under Review**; preview via Factory Preview Mode.`);
    lines.push(
      `- [ ] **Option B:** Keep **Active/Live** only if founder intentionally wants this brand in the production active universe **now** — then run intentional baseline revision (24→N) before merge.`
    );
    lines.push(`- Current status: \`${c.currentBrandStatus || "—"}\``);
    lines.push(`- Prior expected at freeze: \`${c.priorExpectedStatus || "—"}\``);
    lines.push(`- In active universe today: ${c.accidentallyInActiveUniverse}`);
    lines.push(`- Baseline failure attributed: ${c.protectedBaselineFailsBecauseOfIt}`);
    if (c.previewUrls?.combined) {
      lines.push(`- Factory preview URL: \`${c.previewUrls.combined}\``);
    }
    lines.push(``);
  }

  lines.push(`## After correction`);
  lines.push(``);
  lines.push("```bash");
  lines.push("npm run test:brand-explorer-24-active-public-full-baseline");
  lines.push("npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only");
  lines.push("npm run test:brand-explorer-factory-preview-mode");
  lines.push("```");
  lines.push(``);
  lines.push(`Expected after revert: Active/Live count returns to **24**; baseline PASS; factory preview still works for Under Review candidates.`);
  lines.push(``);
  lines.push(`## Explicit non-goals`);
  lines.push(``);
  lines.push(`- Do not write Company Validated / Source Library / Registry.`);
  lines.push(`- Do not mark \`active_profile_ready\` or public-full via preview.`);
  lines.push(`- Do not evolve the 24-brand baseline unless Option B is chosen intentionally.`);
  lines.push(``);
  return `${lines.join("\n")}\n`;
}
