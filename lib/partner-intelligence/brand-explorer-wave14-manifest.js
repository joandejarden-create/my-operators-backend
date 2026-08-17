/**
 * Wave 14 Stage 1 — Manifest / record discovery (read-only).
 *
 * Never writes Airtable, Presentation, Brand Status, release fields,
 * Company Validated, Source Library, Registry, or protected 46 brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  isIntentionalPublicRestoreSlug,
  readIntentionalPublicRestoreSlugs,
} from "./brand-explorer-public-restore-registry.js";
import {
  FACTORY_PREVIEW_CANDIDATE_SLUGS,
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
} from "./brand-explorer-factory-preview-candidates.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import {
  EXPECTED_ACTIVE_COUNT_46,
  BASELINE_VERSION_46,
} from "./brand-explorer-46-active-public-full-baseline.js";
import {
  WAVE14_PLAN_VERSION,
  WAVE14_SLUGS,
  WAVE14_BRAND_PLAN,
  WAVE14_PROTECTED_BASELINE_COUNT,
  WAVE14_EXPECTED_FINAL_ACTIVE_COUNT,
  getWave14Plan,
} from "./brand-explorer-wave14-factory-plan.js";

export const WAVE14_MANIFEST_VERSION = "brand-explorer-wave14-manifest-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

const READY_STATEMENT = "protected_46_live_clean_wave14_may_resume";
const DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return String(v || "").replace(/'/g, "\\'");
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
    },
  };
}

async function fetchBrand(idOrSlug) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: idOrSlug }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function readJson(name) {
  const p = path.join(REPORTS_DIR, name);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[wave14-manifest] json read failed", name, err?.message || err);
    }
    return null;
  }
}

function writeJsonMd(base, report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, `${base}.json`);
  const mdPath = path.join(REPORTS_DIR, `${base}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);
  return { jsonPath, mdPath };
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

export function assessWave14ManifestPreflightGate({
  reuseFreshReports = false,
  maxAgeMs = DEFAULT_MAX_AGE_MS,
} = {}) {
  const preflight = readJson("brand-explorer-wave14-preflight.json");
  const pvql = readJson("brand-explorer-public-visibility-quality-lock.json");
  const quality = readJson("brand-explorer-24-tab-section-quality-audit.json");
  const issues = [];

  const preflightOk =
    preflight?.pass === true &&
    preflight?.readyStatement === READY_STATEMENT &&
    ageMs(preflight?.generatedAt) <= maxAgeMs;

  const reportsOk =
    pvqlPublicFullClean(pvql) &&
    qualityFreezeClean(quality) &&
    ageMs(pvql?.generatedAt) <= maxAgeMs &&
    ageMs(quality?.generatedAt) <= maxAgeMs;

  if (preflightOk) {
    return {
      ok: true,
      source: "preflight_report",
      reusedFreshReports: false,
      preflightGeneratedAt: preflight.generatedAt,
      readyStatement: preflight.readyStatement,
      issues: [],
    };
  }

  if (reuseFreshReports && reportsOk) {
    return {
      ok: true,
      source: "reuse_fresh_reports",
      reusedFreshReports: true,
      pvqlGeneratedAt: pvql.generatedAt,
      qualityGeneratedAt: quality.generatedAt,
      readyStatement: READY_STATEMENT,
      issues: [],
    };
  }

  if (!preflightOk) {
    issues.push(
      preflight
        ? `preflight_stale_or_failed:pass=${preflight.pass}:ready=${preflight.readyStatement}:ageMin=${Math.round(ageMs(preflight.generatedAt) / 60000)}`
        : "preflight_report_missing"
    );
  }
  if (reuseFreshReports && !reportsOk) {
    issues.push(
      `fresh_reports_not_clean_or_stale:pvqlClean=${pvqlPublicFullClean(pvql)}:qualityClean=${qualityFreezeClean(quality)}:pvqlAgeMin=${Math.round(ageMs(pvql?.generatedAt) / 60000)}:qualityAgeMin=${Math.round(ageMs(quality?.generatedAt) / 60000)}`
    );
  }
  if (!reuseFreshReports && !preflightOk) {
    issues.push("pass_--reuse-fresh-reports_only_if_pvql_quality_reports_are_clean_and_current");
  }

  return {
    ok: false,
    source: "blocked",
    reason: "protected_46_preflight_not_clean",
    reusedFreshReports: false,
    issues,
    nextCommand: "npm run brand-explorer-wave14-factory -- --stage preflight --dry-run --reuse-fresh-reports",
  };
}

async function airtableListBasicsByName(name) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID / AIRTABLE_API_KEY required");
  const formula = `{Brand Name}='${escapeFormulaValue(name)}'`;
  const url =
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}?` +
    new URLSearchParams({ filterByFormula: formula, pageSize: "20" });
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics list failed ${res.status}`);
  return json.records || [];
}

async function airtableSearchBasicsContaining(token) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID / AIRTABLE_API_KEY required");
  const t = nz(token).toLowerCase();
  if (!t || t.length < 2) return [];
  const formula = `FIND('${escapeFormulaValue(t)}', LOWER({Brand Name}))`;
  const url =
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}?` +
    new URLSearchParams({ filterByFormula: formula, pageSize: "50" });
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics search failed ${res.status}`);
  return json.records || [];
}

function uniqRecords(records) {
  const map = new Map();
  for (const r of records || []) {
    if (r?.id) map.set(r.id, r);
  }
  return [...map.values()];
}

function classifyManifestRow(row) {
  if (row.manualReviewRequired) return "blocked_requires_manual_review";
  if (row.duplicateOrSlugConflict) return "duplicate_or_slug_conflict";
  if (!row.basicsExists) return "missing_brand_basics_record";
  if (row.isActiveLive) return "existing_needs_status_correction";
  if (
    row.presentationRowCount >= 80 &&
    row.shouldRenderFullProfile === true &&
    !row.release?.activeProfileApproved
  ) {
    return "existing_ready_for_audit";
  }
  if (row.basicsExists) return "existing_needs_factory_build";
  return "blocked_requires_manual_review";
}

function sourcePackAvailability(slug) {
  const candidates = [
    path.join(ROOT, "reports", `brand-explorer-wave14-source-pack-${slug}.md`),
    path.join(ROOT, "reports", `brand-explorer-wave14-source-pack-${slug}.json`),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      return { available: true, path: path.relative(ROOT, p).replace(/\\/g, "/") };
    }
  }
  return { available: false, path: null, note: "pending_stage_source_packs" };
}

function summarizeImages(rows) {
  const list = rows || [];
  const withImage = list.filter((r) => nz(r.imageUrl));
  const gallery = withImage.filter((r) => /^gallery\./i.test(nz(r.slotKey)));
  const scenario = withImage.filter((r) => /valueOwners\.scenario|scenario\./i.test(nz(r.slotKey)));
  const openings = withImage.filter((r) => /opening|example|propert/i.test(nz(r.slotKey)));
  return {
    presentationRowsWithImage: withImage.length,
    galleryImageCount: gallery.length,
    scenarioImageCount: scenario.length,
    openingsOrPropertyImageCount: openings.length,
  };
}

/** Known sibling / corporate aliases that must never be auto-selected as the Wave 14 target. */
const FORBIDDEN_PRIMARY_NAMES = Object.freeze({
  "marriott-hotels": ["marriott international"],
  "four-points-flex-by-sheraton": ["four points by sheraton"],
  studiores: [
    "residence inn",
    "towneplace",
    "element",
    "apartments by marriott bonvoy",
  ],
});

function isForbiddenPrimaryName(slug, brandName) {
  const n = nz(brandName).toLowerCase();
  const forbidden = FORBIDDEN_PRIMARY_NAMES[slug] || [];
  return forbidden.some((f) => n === f || n.startsWith(`${f} `));
}

async function discoverBrand(slug, { activeSlugSet, intentionalRestoreSlugs }) {
  const plan = getWave14Plan(slug);
  const exactNames = [plan.name, ...(plan.nameAliases || [])].filter(Boolean);
  let exactMatches = [];
  for (const name of exactNames) {
    const found = await airtableListBasicsByName(name);
    exactMatches = uniqRecords([...exactMatches, ...found]);
  }

  let fuzzyMatches = [];
  for (const token of plan.aliasSearchTokens || []) {
    const found = await airtableSearchBasicsContaining(token);
    fuzzyMatches = uniqRecords([...fuzzyMatches, ...found]);
  }

  const preferred =
    exactMatches.find((r) => nz(r.fields?.["Brand Name"]) === plan.name) ||
    exactMatches.find((r) => !isForbiddenPrimaryName(slug, r.fields?.["Brand Name"])) ||
    null;

  let brandApi = null;
  if (preferred?.id) {
    brandApi = await fetchBrand(preferred.id);
  } else {
    brandApi = await fetchBrand(slug);
  }

  const fields = preferred?.fields || {};
  const brandName = nz(fields["Brand Name"] || brandApi?.name || plan.name);
  const brandStatus = nz(fields["Brand Status"] || brandApi?.brandStatus || brandApi?.status);
  const recordId = preferred?.id || brandApi?.id || null;
  const parentCompany =
    nz(
      fields["Parent Company"] ||
        fields["Parent / Platform"] ||
        fields["Parent Platform"] ||
        brandApi?.parentCompany ||
        brandApi?.parentPlatform
    ) || plan.parentPlatform;

  let presentationRows = [];
  let presentationRowCount = 0;
  if (recordId && brandName) {
    try {
      const listed = await listPresentationRowsLight(recordId, brandName);
      presentationRows = listed.rows || [];
      presentationRowCount = presentationRows.length;
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn(`[wave14-manifest] presentation list failed for ${slug}`, err?.message || err);
      }
      presentationRowCount = 0;
    }
  }

  const aliasCandidates = fuzzyMatches.map((r) => ({
    recordId: r.id,
    brandName: nz(r.fields?.["Brand Name"]),
    brandStatus: nz(r.fields?.["Brand Status"]),
    exactNameMatch: exactNames.some((n) => n === nz(r.fields?.["Brand Name"])),
    forbiddenPrimary: isForbiddenPrimaryName(slug, r.fields?.["Brand Name"]),
  }));

  const exactSamePlannedName = exactMatches.filter(
    (r) => nz(r.fields?.["Brand Name"]).toLowerCase() === plan.name.toLowerCase()
  );
  const duplicateOrSlugConflict = exactSamePlannedName.length > 1;

  const conflictingFuzzy = aliasCandidates.filter((c) => {
    if (!c.recordId || c.recordId === recordId) return false;
    return nz(c.brandName).toLowerCase() === plan.name.toLowerCase();
  });

  // Critical alias risks for Flex / StudioRes / Marriott Hotels corporate confusion.
  const criticalAliasHits = aliasCandidates.filter((c) => {
    if (!c.recordId || c.recordId === recordId) return false;
    if (slug === "four-points-flex-by-sheraton") {
      return /^four points by sheraton$/i.test(c.brandName) && !/flex/i.test(c.brandName);
    }
    if (slug === "studiores") {
      return /residence inn|towneplace|element|apartments by marriott/i.test(c.brandName);
    }
    if (slug === "marriott-hotels") {
      return /^marriott international$/i.test(c.brandName);
    }
    return false;
  });

  const inFactoryPreview = FACTORY_PREVIEW_CANDIDATE_SLUGS.includes(slug);
  const factoryIdentity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug] || null;
  const intentionalRestore = isIntentionalPublicRestoreSlug(slug, {
    intentionalSlugs: intentionalRestoreSlugs,
  });
  const pack = sourcePackAvailability(slug);
  const images = summarizeImages(presentationRows);

  const manualReviewRequired =
    (slug === "four-points-flex-by-sheraton" || slug === "studiores") &&
    !recordId &&
    criticalAliasHits.length > 0;

  const row = {
    slug,
    plannedName: plan.name,
    parentPlatform: plan.parentPlatform,
    segmentHint: plan.segmentHint,
    siblingDistinctions: plan.siblingDistinctions || [],
    criticalAliasWarning: plan.criticalAliasWarning || null,
    basicsExists: Boolean(recordId),
    brandName,
    recordId,
    brandStatus: brandStatus || null,
    isActiveLive: isBrandStatusActive(brandStatus) || activeSlugSet.has(slug),
    parentCompany: parentCompany || null,
    presentationRowCount,
    shouldRenderFullProfile: brandApi?.shouldRenderFullProfile === true,
    displayState:
      brandApi?.brandExplorerDisplayState ||
      brandApi?.displayState ||
      brandApi?.publicDisplayState ||
      null,
    release: {
      activeProfileApproved:
        fields["Active Profile Approved"] === true || brandApi?.activeProfileApproved === true,
      readyForActiveProfile:
        fields["Ready for Active Profile"] === true || brandApi?.readyForActiveProfile === true,
      activeProfileApprovedDate:
        nz(fields["Active Profile Approved Date"]) || brandApi?.activeProfileApprovedDate || null,
      founderVisualReviewPass:
        fields["Founder Visual Review Pass"] === true ||
        brandApi?.founderVisualReviewPass === true,
    },
    publicRestoreStatus: intentionalRestore
      ? "in_intentional_public_restore_registry"
      : "not_in_intentional_public_restore_registry",
    intentionalPublicRestore: intentionalRestore,
    sourcePackAvailability: pack,
    images,
    factoryPreviewStatus: inFactoryPreview
      ? { inFactoryPreviewCohort: true, identity: factoryIdentity }
      : { inFactoryPreviewCohort: false, identity: null },
    factoryState: inFactoryPreview ? "factory_preview_candidate" : "wave14_candidate_not_in_preview",
    duplicateOrSlugConflict: Boolean(duplicateOrSlugConflict),
    basicsExactMatchCount: exactMatches.length,
    aliasCandidates,
    conflictingAliasRecords: conflictingFuzzy,
    criticalAliasHits,
    manualReviewRequired,
    classification: null,
  };
  row.classification = classifyManifestRow(row);
  return row;
}

function renderManifestMarkdown(report) {
  const lines = [
    `# Brand Explorer Wave 14 — Manifest`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Dry-run: **${report.dryRun}**`,
    `Airtable writes: **${report.airtableWrites}**`,
    ``,
    `## 1. Executive summary`,
    ``,
    report.deferred
      ? `Manifest **deferred**: ${report.reason || report.message}`
      : `Read-only discovery for **${report.wave14Count}** Wave 14 Marriott brands against protected **${report.protectedActiveCount}** Active/Live baseline.`,
    ``,
    `| Metric | Value |`,
    `| --- | --- |`,
    `| Protected Active/Live | ${report.protectedActiveCount} (expected ${report.expectedProtectedCount}) |`,
    `| Wave 14 targets | ${report.wave14Count} |`,
    `| Active/Live drift | ${report.activeUniverseDrift?.length ? report.activeUniverseDrift.join(", ") : "none"} |`,
    `| Stop recommended | ${report.stopRecommended} |`,
    `| May proceed to factory-preview-cohort | ${report.mayProceedToFactoryPreviewCohort} |`,
    `| Ready statement | ${report.readyStatement || "—"} |`,
    ``,
    `## 2. Preflight dependency status`,
    ``,
    `- Gate ok: **${report.preflightGate?.ok}**`,
    `- Source: \`${report.preflightGate?.source || "—"}\``,
    `- Reused fresh reports: **${report.preflightGate?.reusedFreshReports === true}**`,
    ...(report.preflightGate?.issues || []).map((i) => `- Issue: ${i}`),
    ``,
  ];

  if (report.deferred) {
    lines.push(`## Next`, ``, `\`${report.nextRequired || report.preflightGate?.nextCommand}\``, ``);
    return `${lines.join("\n")}\n`;
  }

  lines.push(
    `## 3. Target brand table`,
    ``,
    `| Slug | Name | Record ID | Status | Active? | Rows | Full | Class |`,
    `| --- | --- | --- | --- | --- | ---: | --- | --- |`,
    ...(report.brands || []).map(
      (b) =>
        `| \`${b.slug}\` | ${b.brandName || b.plannedName} | \`${b.recordId || "—"}\` | ${b.brandStatus || "—"} | ${b.isActiveLive} | ${b.presentationRowCount} | ${b.shouldRenderFullProfile} | \`${b.classification}\` |`
    ),
    ``,
    `## 4. Per-brand discovery details`,
    ``
  );

  for (const b of report.brands || []) {
    lines.push(
      `### ${b.brandName || b.plannedName} (\`${b.slug}\`)`,
      ``,
      `- Basics exists: **${b.basicsExists}**`,
      `- Record ID: \`${b.recordId || "—"}\``,
      `- Brand Status: ${b.brandStatus || "—"}`,
      `- Parent / platform: ${b.parentCompany || "—"}`,
      `- Presentation rows: ${b.presentationRowCount}`,
      `- shouldRenderFullProfile: ${b.shouldRenderFullProfile}`,
      `- displayState: ${b.displayState || "—"}`,
      `- Release: approved=${b.release?.activeProfileApproved} ready=${b.release?.readyForActiveProfile} date=${b.release?.activeProfileApprovedDate || "—"} founderVisual=${b.release?.founderVisualReviewPass}`,
      `- Public restore: ${b.publicRestoreStatus}`,
      `- Source pack: ${b.sourcePackAvailability?.available ? b.sourcePackAvailability.path : b.sourcePackAvailability?.note || "unavailable"}`,
      `- Images: gallery=${b.images?.galleryImageCount ?? 0} scenario=${b.images?.scenarioImageCount ?? 0} openings/property=${b.images?.openingsOrPropertyImageCount ?? 0}`,
      `- Factory preview cohort: ${b.factoryPreviewStatus?.inFactoryPreviewCohort === true}`,
      `- Factory state: \`${b.factoryState}\``,
      `- Classification: \`${b.classification}\``,
      b.criticalAliasWarning ? `- Critical alias warning: ${b.criticalAliasWarning}` : null,
      `- Alias candidates: ${(b.aliasCandidates || []).map((a) => `${a.brandName} (${a.recordId})`).join("; ") || "none"}`,
      `- Critical alias hits: ${(b.criticalAliasHits || []).map((a) => `${a.brandName} (${a.recordId})`).join("; ") || "none"}`,
      ``
    );
  }

  lines.push(
    `## 5. Active/Live drift check`,
    ``,
    report.activeUniverseDrift?.length
      ? `**STOP** — Wave 14 brands already Active/Live: ${report.activeUniverseDrift.join(", ")}.`
      : "No Wave 14 Active/Live drift. Targets are outside the protected 46 universe.",
    ``,
    `## 6. Duplicate / alias check`,
    ``,
    report.duplicateAliasFindings?.length
      ? report.duplicateAliasFindings.map((d) => `- ${d}`).join("\n")
      : "- No exact-name duplicate conflicts detected among primary matches.",
    ``,
    `Critical checks: Marriott Hotels ≠ Marriott International; Four Points Flex ≠ Four Points by Sheraton; StudioRes ≠ Residence Inn / TownePlace / Element / Apartments by Marriott Bonvoy.`,
    ``,
    `## 7. Classification summary`,
    ``,
    ...Object.entries(report.classificationCounts || {}).map(([k, v]) => `- \`${k}\`: ${v}`),
    ``,
    `## 8. Recommended next stage`,
    ``,
    report.mayProceedToFactoryPreviewCohort
      ? "Wave 14 may proceed to **Stage 2 factory-preview-cohort** (code/config only; no Airtable)."
      : "**STOP** — resolve Active/Live drift or status-correction items before factory-preview-cohort.",
    ``,
    report.mayProceedToFactoryPreviewCohort
      ? "Suggested: `npm run brand-explorer-wave14-factory -- --stage factory-preview-cohort --dry-run`"
      : `Suggested: \`${report.nextRequired || "npm run brand-explorer-wave14-factory -- --stage preflight --dry-run"}\``,
    ``,
    `## Guardrails`,
    ``,
    `- Read-only / dry-run`,
    `- No Airtable / Presentation / Brand Status / release / CV / Source Library / Registry writes`,
    `- No protected 46 brand changes`,
    `- No Presentation content build or image materialization in this stage`,
    ``
  );

  return `${lines.filter((l) => l != null).join("\n")}\n`;
}

export async function runWave14Manifest({
  reuseFreshReports = false,
  maxAgeMs = DEFAULT_MAX_AGE_MS,
  dryRun = true,
} = {}) {
  const preflightGate = assessWave14ManifestPreflightGate({ reuseFreshReports, maxAgeMs });

  if (!preflightGate.ok) {
    const deferred = {
      version: WAVE14_MANIFEST_VERSION,
      planVersion: WAVE14_PLAN_VERSION,
      stage: "manifest",
      generatedAt: new Date().toISOString(),
      dryRun: true,
      deferred: true,
      status: "deferred",
      reason: "protected_46_preflight_not_clean",
      message: "Wave 14 manifest deferred — protected 46 preflight is not clean/current.",
      nextRequired: preflightGate.nextCommand,
      airtableWrites: false,
      presentationWrites: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      protected46Touched: false,
      preflightGate,
      pass: false,
      stopRecommended: true,
      mayProceedToFactoryPreviewCohort: false,
      readyStatement: null,
      brands: [],
      wave14Count: WAVE14_SLUGS.length,
      protectedActiveCount: null,
      expectedProtectedCount: WAVE14_PROTECTED_BASELINE_COUNT,
    };
    const md = renderManifestMarkdown(deferred);
    const paths = writeJsonMd("brand-explorer-wave14-manifest", deferred, md);
    return { ...deferred, paths };
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  const activeSlugSet = new Set((universe.brands || []).map((b) => b.slug));
  const intentionalRestoreSlugs = readIntentionalPublicRestoreSlugs();
  const protectedCollision = (universe.brands || []).filter((b) => WAVE14_SLUGS.includes(b.slug));

  const brands = [];
  for (const slug of WAVE14_SLUGS) {
    console.log(`[wave14-manifest] discovering ${slug}...`);
    const row = await discoverBrand(slug, { activeSlugSet, intentionalRestoreSlugs });
    brands.push(row);
  }

  const activeUniverseDrift = brands.filter((b) => b.isActiveLive).map((b) => b.slug);
  const duplicateAliasFindings = [];
  for (const b of brands) {
    if (b.duplicateOrSlugConflict) {
      duplicateAliasFindings.push(
        `${b.slug}: exact-name duplicate/conflict (exactMatchCount=${b.basicsExactMatchCount})`
      );
    }
    for (const c of b.conflictingAliasRecords || []) {
      duplicateAliasFindings.push(
        `${b.slug}: conflicting alias candidate ${c.brandName} (${c.recordId})`
      );
    }
    for (const c of b.criticalAliasHits || []) {
      duplicateAliasFindings.push(
        `${b.slug}: CRITICAL sibling/corporate alias present ${c.brandName} (${c.recordId}) — do not conflate`
      );
    }
    if (b.criticalAliasWarning) {
      duplicateAliasFindings.push(`${b.slug}: ${b.criticalAliasWarning}`);
    }
  }

  const classificationCounts = brands.reduce((acc, b) => {
    acc[b.classification] = (acc[b.classification] || 0) + 1;
    return acc;
  }, {});

  const stopRecommended =
    activeUniverseDrift.length > 0 || protectedCollision.length > 0;

  const mayProceedToFactoryPreviewCohort =
    !stopRecommended &&
    activeUniverseDrift.length === 0 &&
    brands.every((b) => b.classification !== "existing_needs_status_correction");

  const hasMissing = brands.some((b) => b.classification === "missing_brand_basics_record");
  const hasDupes = brands.some((b) => b.classification === "duplicate_or_slug_conflict");
  const hasBlocked = brands.some((b) => b.classification === "blocked_requires_manual_review");

  const report = {
    version: WAVE14_MANIFEST_VERSION,
    planVersion: WAVE14_PLAN_VERSION,
    stage: "manifest",
    generatedAt: new Date().toISOString(),
    dryRun: dryRun !== false,
    deferred: false,
    status: stopRecommended ? "blocked" : "ok",
    airtableWrites: false,
    presentationWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    protected46Touched: false,
    protectedBaselineVersion: BASELINE_VERSION_46,
    protectedActiveCount: universe.totalCount,
    expectedProtectedCount: WAVE14_PROTECTED_BASELINE_COUNT,
    wave14Count: WAVE14_SLUGS.length,
    targetFinalActiveCount: WAVE14_EXPECTED_FINAL_ACTIVE_COUNT,
    preflightGate,
    stopRecommended,
    mayProceedToFactoryPreviewCohort,
    activeUniverseDrift,
    protectedUniverseCollisions: protectedCollision.map((b) => ({
      slug: b.slug,
      recordId: b.recordId,
      brandName: b.brandName || b.name,
    })),
    duplicateAliasFindings,
    classificationCounts,
    brands,
    planSlugs: [...WAVE14_SLUGS],
    brandPlan: WAVE14_BRAND_PLAN,
    readyStatement: mayProceedToFactoryPreviewCohort
      ? hasMissing || hasDupes || hasBlocked
        ? "wave14_manifest_ready_for_factory_preview_cohort_with_open_items"
        : "wave14_manifest_ready_for_factory_preview_cohort"
      : "wave14_manifest_blocked",
    openItems: {
      missingBrandBasics: brands
        .filter((b) => b.classification === "missing_brand_basics_record")
        .map((b) => b.slug),
      duplicateOrSlugConflicts: brands
        .filter((b) => b.classification === "duplicate_or_slug_conflict")
        .map((b) => b.slug),
      needsStatusCorrection: brands
        .filter((b) => b.classification === "existing_needs_status_correction")
        .map((b) => b.slug),
      blockedManualReview: brands
        .filter((b) => b.classification === "blocked_requires_manual_review")
        .map((b) => b.slug),
    },
    pass: !stopRecommended,
    nextRequired: mayProceedToFactoryPreviewCohort
      ? "npm run brand-explorer-wave14-factory -- --stage factory-preview-cohort --dry-run"
      : activeUniverseDrift.length
        ? "Resolve Active/Live drift on Wave 14 targets before continuing"
        : "npm run brand-explorer-wave14-factory -- --stage preflight --dry-run",
  };

  const md = renderManifestMarkdown(report);
  const paths = writeJsonMd("brand-explorer-wave14-manifest", report, md);
  return { ...report, paths, summary: { brands: brands.length, mayProceed: mayProceedToFactoryPreviewCohort } };
}
