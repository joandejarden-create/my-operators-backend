/**
 * Brand Explorer Wave 12 factory orchestrator.
 *
 * Stages: preflight → manifest → factory-preview-cohort → source-packs →
 * tab-factory-build → image-materialization → evidence → gates → founder →
 * status-promotion → public-release → baseline-39.
 *
 * Guardrails:
 * - Never writes protected 27 baseline brands or Radisson Collection.
 * - Never writes CV / Source Library / Registry.
 * - Brand Status / release fields only in dedicated later stages with full flags.
 * - Factory preview cohort is code-only (no Airtable).
 * - Source packs are report-only (no Airtable / no TGS writes).
 * - Tab factory build may write target Presentation + limited Basics positioning
 *   fields after dry-run + explicit apply flags.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  EXPECTED_ACTIVE_COUNT_27,
  BASELINE_VERSION_27,
} from "./brand-explorer-27-active-public-full-baseline.js";
import {
  WAVE12_VERSION,
  WAVE12_STAGES,
  WAVE12_SLUGS,
  WAVE12_BRAND_PLAN,
  WAVE12_PROTECTED_BASELINE_COUNT,
  WAVE12_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE12_FACTORY_PREVIEW_APPLY_FLAGS,
  getWave12Plan,
} from "./brand-explorer-wave12-factory-plan.js";
import {
  FACTORY_PREVIEW_CANDIDATE_SLUGS,
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_DISPLAY_STATE,
  FACTORY_PREVIEW_BANNER_TEXT,
  assertFactoryPreviewDoesNotAffectActiveUniverse,
} from "./brand-explorer-factory-preview-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function ensureReportsDir() {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
}

function writeJsonMd(basename, json, md) {
  ensureReportsDir();
  const jsonPath = path.join(REPORTS_DIR, `${basename}.json`);
  const mdPath = path.join(REPORTS_DIR, `${basename}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(json, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath };
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

function runNpm(scriptArgs, { timeoutMs = 45 * 60 * 1000 } = {}) {
  const result = spawnSync(
    process.platform === "win32" ? "npm.cmd" : "npm",
    ["run", ...scriptArgs],
    {
      cwd: ROOT,
      encoding: "utf8",
      timeout: timeoutMs,
      maxBuffer: 20 * 1024 * 1024,
      shell: process.platform === "win32",
    }
  );
  return {
    ok: result.status === 0,
    status: result.status,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
    error: result.error ? String(result.error.message || result.error) : null,
  };
}

function parseApplyFlags(argv, required) {
  const missing = required.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function airtableListBasicsByName(name) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID / AIRTABLE_API_KEY required");
  const formula = `{Brand Name}='${String(name).replace(/'/g, "\\'")}'`;
  const url =
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}?` +
    new URLSearchParams({ filterByFormula: formula, pageSize: "10" });
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics list failed ${res.status}`);
  return json.records || [];
}

async function countPresentationRows(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const formula = `{Brand Name}='${String(brandName).replace(/'/g, "\\'")}'`;
  let count = 0;
  let offset = "";
  do {
    const params = new URLSearchParams({
      pageSize: "100",
      filterByFormula: formula,
      fields: [],
    });
    // Airtable ignores empty fields; just page for count
    params.delete("fields");
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
      PRESENTATION_TABLE
    )}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed ${res.status}`);
    count += (json.records || []).length;
    offset = json.offset || "";
  } while (offset);
  return count;
}

function classifyManifestRow(row) {
  if (row.duplicateOrSlugConflict) return "duplicate_or_slug_conflict";
  if (!row.basicsExists) return "missing_brand_basics_record";
  if (row.isActiveLive) return "blocked_requires_manual_review";
  if (row.presentationRowCount >= 80 && row.shouldRenderFullProfile !== true) {
    return "existing_ready_for_audit";
  }
  if (row.presentationRowCount > 0) return "existing_needs_factory_build";
  if (row.basicsExists) return "existing_needs_factory_build";
  return "blocked_requires_manual_review";
}

/**
 * STAGE 0 — Protected 27 baseline preflight (read-only).
 */
export async function runWave12Preflight({ skipLongGates = false } = {}) {
  const gates = [];
  const commands = [
    ["test:brand-explorer-27-active-public-full-baseline"],
    ["test:brand-explorer-recent-momentum-evidence-quality"],
    ["test:brand-explorer-mandatory-release-gates"],
  ];
  if (!skipLongGates) {
    commands.push(
      ["test:brand-explorer-public-visibility-quality-lock", "--", "--public-full-only"],
      ["brand-explorer-os", "--", "--stage", "release-readiness", "--dry-run", "--skip-regression"],
      ["brand-explorer-24-tab-section-quality-audit", "--", "--dry-run"]
    );
  }

  for (const args of commands) {
    console.log(`[wave12-preflight] running npm run ${args.join(" ")}`);
    const r = runNpm(args);
    gates.push({
      command: `npm run ${args.join(" ")}`,
      ok: r.ok,
      status: r.status,
      error: r.error,
      tail: `${r.stdout}\n${r.stderr}`.split(/\r?\n/).filter(Boolean).slice(-8),
    });
    if (!r.ok) break;
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  const waveInActive = (universe.brands || []).filter((b) => WAVE12_SLUGS.includes(b.slug));
  const pass =
    gates.every((g) => g.ok) &&
    universe.totalCount === WAVE12_PROTECTED_BASELINE_COUNT &&
    waveInActive.length === 0;

  const report = {
    version: WAVE12_VERSION,
    stage: "preflight",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    protectedBaselineVersion: BASELINE_VERSION_27,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_27,
    liveActiveCount: universe.totalCount,
    wave12SlugsInActiveUniverse: waveInActive.map((b) => b.slug),
    pass,
    stopRecommended: !pass,
    gates,
    summary: {
      protectedBaselineClean: pass,
      activeUniverseIs27: universe.totalCount === 27,
      noWave12ActiveDrift: waveInActive.length === 0,
    },
  };

  const md = [
    `# Brand Explorer Wave 12 — Preflight`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Pass: **${report.pass}**`,
    ``,
    `| Metric | Value |`,
    `| --- | --- |`,
    `| Protected baseline | ${report.protectedBaselineVersion} |`,
    `| Live Active/Live count | ${report.liveActiveCount} (expected ${report.expectedActiveCount}) |`,
    `| Wave12 in Active universe | ${report.wave12SlugsInActiveUniverse.join(", ") || "none"} |`,
    `| Airtable writes | false |`,
    ``,
    `## Gates`,
    ``,
    ...gates.map(
      (g) =>
        `- ${g.ok ? "PASS" : "FAIL"} \`${g.command}\`${g.tail?.length ? `\n  - ${g.tail.slice(-3).join(" / ")}` : ""}`
    ),
    ``,
    report.pass
      ? "Preflight clean — proceed to manifest."
      : "**STOP** — do not continue wave12 until protected 27 baseline is clean and no wave12 Active/Live drift.",
    ``,
  ].join("\n");

  const paths = writeJsonMd("brand-explorer-wave12-preflight", report, md);
  return { ...report, paths };
}

/**
 * STAGE 1 — Manifest / record discovery (read-only).
 */
export async function runWave12Manifest() {
  const universe = await loadActiveUniverse({ includeDetails: false });
  const activeSlugs = new Set((universe.brands || []).map((b) => b.slug));
  const brands = [];
  let activeDrift = [];

  for (const slug of WAVE12_SLUGS) {
    const plan = getWave12Plan(slug);
    const nameCandidates = [plan.name, ...(plan.nameAliases || [])].filter(Boolean);
    let basicsRecords = [];
    for (const name of nameCandidates) {
      const found = await airtableListBasicsByName(name);
      if (found.length) {
        basicsRecords = found;
        break;
      }
    }

    // Fallback: try slug via brand library API
    let brandApi = null;
    if (basicsRecords[0]?.id) {
      brandApi = await fetchBrand(basicsRecords[0].id);
    } else {
      brandApi = await fetchBrand(slug);
    }

    const primary = basicsRecords[0] || null;
    const fields = primary?.fields || {};
    const brandName = nz(fields["Brand Name"] || brandApi?.name || plan.name);
    const brandStatus = nz(fields["Brand Status"] || brandApi?.brandStatus || brandApi?.status);
    const recordId = primary?.id || brandApi?.id || null;
    const presentationRowCount = brandName ? await countPresentationRows(brandName).catch(() => null) : 0;

    const row = {
      slug,
      plannedName: plan.name,
      parentPlatform: plan.parentPlatform,
      family: plan.family,
      lens: plan.lens,
      basicsExists: Boolean(recordId),
      brandName,
      recordId,
      brandStatus: brandStatus || null,
      isActiveLive: isBrandStatusActive(brandStatus) || activeSlugs.has(slug),
      parentCompany:
        nz(fields["Parent Company"] || fields["Parent / Platform"] || brandApi?.parentCompany) ||
        null,
      presentationRowCount: presentationRowCount ?? 0,
      shouldRenderFullProfile: brandApi?.shouldRenderFullProfile === true,
      displayState: brandApi?.brandExplorerDisplayState || null,
      release: {
        readyForActiveProfile: brandApi?.readyForActiveProfile === true,
        activeProfileApproved: brandApi?.activeProfileApproved === true,
        founderVisualReviewPass: brandApi?.founderVisualReviewPass === true,
      },
      publicRestoreStatus: "not_in_protected_27_restore_set",
      sourcePackAvailability: "pending_stage_source_packs",
      galleryOrImageRows: "pending_image_stage",
      factoryState: "wave12_candidate",
      duplicateOrSlugConflict: basicsRecords.length > 1,
      basicsMatchCount: basicsRecords.length,
      classification: null,
    };
    row.classification = classifyManifestRow(row);
    if (row.isActiveLive) activeDrift.push(slug);
    brands.push(row);
  }

  const stopRecommended = activeDrift.length > 0;
  const report = {
    version: WAVE12_VERSION,
    stage: "manifest",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    protectedActiveCount: universe.totalCount,
    expectedProtectedCount: WAVE12_PROTECTED_BASELINE_COUNT,
    wave12Count: WAVE12_SLUGS.length,
    targetFinalActiveCount: WAVE12_EXPECTED_FINAL_ACTIVE_COUNT,
    stopRecommended,
    activeUniverseDrift: activeDrift,
    classificationCounts: brands.reduce((acc, b) => {
      acc[b.classification] = (acc[b.classification] || 0) + 1;
      return acc;
    }, {}),
    brands,
  };

  const mdLines = [
    `# Brand Explorer Wave 12 — Manifest`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Protected Active/Live: **${report.protectedActiveCount}** (expected ${report.expectedProtectedCount})`,
    `Wave12 brands: **${report.wave12Count}**`,
    `Active drift: **${activeDrift.join(", ") || "none"}**`,
    `Stop recommended: **${stopRecommended}**`,
    ``,
    `## Classification counts`,
    ``,
    ...Object.entries(report.classificationCounts).map(([k, v]) => `- \`${k}\`: ${v}`),
    ``,
    `## Brands`,
    ``,
    `| Slug | Name | Record ID | Status | Active? | Rows | Full | Class |`,
    `| --- | --- | --- | --- | --- | ---: | --- | --- |`,
    ...brands.map(
      (b) =>
        `| \`${b.slug}\` | ${b.brandName || b.plannedName} | \`${b.recordId || "—"}\` | ${b.brandStatus || "—"} | ${b.isActiveLive} | ${b.presentationRowCount} | ${b.shouldRenderFullProfile} | \`${b.classification}\` |`
    ),
    ``,
    stopRecommended
      ? "**STOP** — one or more wave12 brands are already Active/Live. Resolve active-universe drift before factory work."
      : "No wave12 Active/Live drift. Safe to configure factory preview cohort.",
    ``,
  ];

  const paths = writeJsonMd("brand-explorer-wave12-manifest", report, mdLines.join("\n"));
  return { ...report, paths };
}

/**
 * STAGE 2 — Factory preview cohort (code/config only; no Airtable).
 * Returns the desired candidate list to write into factory-preview-candidates.js.
 */
export function planWave12FactoryPreviewCohort(manifestBrands = []) {
  const identities = {};
  for (const slug of WAVE12_SLUGS) {
    const plan = getWave12Plan(slug);
    const live = (manifestBrands || []).find((b) => b.slug === slug) || {};
    identities[slug] = {
      slug,
      name: live.brandName || plan.name,
      recordId: live.recordId || null,
      recommendedStatusWhileInFactory: plan.recommendedStatusWhileInFactory,
      parentPlatform: plan.parentPlatform,
      wave: "wave12",
    };
  }
  return {
    version: WAVE12_VERSION,
    stage: "factory-preview-cohort",
    displayState: FACTORY_PREVIEW_DISPLAY_STATE,
    bannerText: FACTORY_PREVIEW_BANNER_TEXT,
    candidateSlugs: [...WAVE12_SLUGS],
    identities,
    priorCandidatesPreservedNote:
      "Prior tapestry/dazzler/trademark remain Active/Live — remove from factory preview allowlist to avoid confusion; wave12 replaces factory preview cohort.",
    affectsActiveUniverse: false,
    airtableWrites: false,
  };
}

export function renderFactoryPreviewCandidatesModule({ candidateSlugs, identities }) {
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
 * Wave 12 factory cohort (12 brands). Separate from production Active/Live
 * universe and the protected 27 public-full baseline.
 *
 * Production public-full remains: Brand Status Active/Live + release gates + PVQL.
 * Factory preview never writes Airtable.
 */
import { isBrandStatusActive } from "../brand-status-active.js";
import { ACTIVE_UNIVERSE_SOURCE } from "./brand-explorer-active-universe.js";

export const FACTORY_PREVIEW_VERSION = "factory-preview-mode-v2-wave12";

/** Effective UI display state while factory preview is active (internal only). */
export const FACTORY_PREVIEW_DISPLAY_STATE = "factory_preview_internal";

/**
 * Wave 12 factory candidate cohort for new brand setup work.
 * Not a public release registry. Not the Active/Live universe.
 */
export const FACTORY_PREVIEW_CANDIDATE_SLUGS = Object.freeze([
${slugLit}
]);

/** Known identity anchors for deep-link preview (record ID preferred over slug). */
export const FACTORY_PREVIEW_CANDIDATE_IDENTITIES = Object.freeze({
${idBlocks}
});

export const FACTORY_PREVIEW_BANNER_TEXT =
  "Factory Preview — Not Public / Not Active Baseline";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeSlug(slug) {
  return nz(slug).toLowerCase();
}

/**
 * Parse allowlist from env (comma-separated) or fall back to built-in candidates.
 * Env: BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS=slug1,slug2
 */
export function resolveFactoryPreviewAllowlist({
  env = process.env,
  fallback = FACTORY_PREVIEW_CANDIDATE_SLUGS,
} = {}) {
  const raw = nz(env.BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS);
  if (!raw) return [...fallback];
  return [
    ...new Set(
      raw
        .split(",")
        .map((s) => normalizeSlug(s))
        .filter(Boolean)
    ),
  ];
}

/**
 * Master enable for server-side factory-preview eligibility metadata.
 * - Explicit off: BRAND_EXPLORER_FACTORY_PREVIEW=0
 * - Explicit on: BRAND_EXPLORER_FACTORY_PREVIEW=1
 * - Allowlist env alone enables metadata for those slugs
 * - Default: on outside production; off in production unless explicitly enabled
 */
export function isFactoryPreviewModeEnabled({ env = process.env } = {}) {
  const v = nz(env.BRAND_EXPLORER_FACTORY_PREVIEW).toLowerCase();
  if (v === "0" || v === "false" || v === "off") return false;
  if (v === "1" || v === "true" || v === "on") return true;
  if (nz(env.BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS)) return true;
  return nz(env.NODE_ENV).toLowerCase() !== "production";
}

export function isFactoryPreviewCandidate(slug, { env = process.env } = {}) {
  const s = normalizeSlug(slug);
  if (!s) return false;
  return resolveFactoryPreviewAllowlist({ env }).includes(s);
}

export function getFactoryPreviewIdentity(slugOrRecordId) {
  const key = normalizeSlug(slugOrRecordId);
  if (!key) return null;
  if (FACTORY_PREVIEW_CANDIDATE_IDENTITIES[key]) {
    return FACTORY_PREVIEW_CANDIDATE_IDENTITIES[key];
  }
  for (const identity of Object.values(FACTORY_PREVIEW_CANDIDATE_IDENTITIES)) {
    if (nz(identity.recordId) === nz(slugOrRecordId)) return identity;
  }
  return null;
}

export function resolveFactoryPreviewSlug(brand = {}, options = {}) {
  const direct = normalizeSlug(brand.slug || options.slug);
  if (direct && isFactoryPreviewCandidate(direct, { env: options.env || process.env })) {
    return direct;
  }
  const byId = getFactoryPreviewIdentity(brand.id || brand.recordId || options.recordId);
  if (byId?.slug) return byId.slug;
  if (direct) return direct;
  return "";
}

export function getFactoryPreviewDisplayState(slug, { env = process.env } = {}) {
  if (!isFactoryPreviewCandidate(slug, { env })) return null;
  return FACTORY_PREVIEW_DISPLAY_STATE;
}

export function isFactoryPreviewQuery(search = "") {
  const q = String(search || "");
  return (
    /(?:\\?|&)beInternalPreview=1(?:&|$)/.test(q) &&
    /(?:\\?|&)factoryPreview=1(?:&|$)/.test(q)
  );
}

export function buildFactoryPreviewUrls({ recordId, slug } = {}) {
  const id = nz(recordId) || nz(slug);
  if (!id) return null;
  const q = \`brandId=\${encodeURIComponent(id)}&beInternalPreview=1&factoryPreview=1\`;
  return {
    combined: \`/brand-explorer-combined.html?\${q}\`,
    explorer: \`/brand-explorer?brand=\${encodeURIComponent(nz(slug) || id)}&beInternalPreview=1&factoryPreview=1\`,
    api: \`/api/brand-library/brand?brandId=\${encodeURIComponent(id)}\`,
  };
}

export function canRenderFactoryPreview(brand = {}, options = {}) {
  const env = options.env || process.env;
  const slug = resolveFactoryPreviewSlug(brand, options);
  if (!isFactoryPreviewCandidate(slug, { env })) return false;

  const previewRequested =
    options.factoryPreview === true || isFactoryPreviewQuery(options.search || "");
  if (!previewRequested) return false;

  if (options.requireModeEnabled === true && !isFactoryPreviewModeEnabled({ env })) {
    return false;
  }

  const blocks = brand?.brandExplorer?.blocks;
  const hasRows =
    options.hasPresentationRows === true ||
    (Array.isArray(blocks) && blocks.length > 0);
  if (options.requirePresentationRows !== false && !hasRows) return false;
  return true;
}

export function buildFactoryPreviewApiMeta(brand = {}, { env = process.env } = {}) {
  const slug = resolveFactoryPreviewSlug(brand, { env });
  const identity = getFactoryPreviewIdentity(slug) || getFactoryPreviewIdentity(brand.id);
  const candidate = Boolean(slug && isFactoryPreviewCandidate(slug, { env }));
  const eligible = isFactoryPreviewModeEnabled({ env }) && candidate;
  const urls = buildFactoryPreviewUrls({
    recordId: brand.id || identity?.recordId,
    slug: slug || identity?.slug,
  });
  const blocks = brand?.brandExplorer?.blocks;
  const hasRows = Array.isArray(blocks) && blocks.length > 0;
  return {
    version: FACTORY_PREVIEW_VERSION,
    eligible,
    candidate,
    slug: slug || identity?.slug || null,
    factoryPreviewDisplayState: eligible ? FACTORY_PREVIEW_DISPLAY_STATE : null,
    canRenderFactoryPreview: eligible && hasRows,
    affectsActiveUniverse: false,
    affectsPvqlPublicFull: false,
    affectsProtectedBaseline: false,
    productionShouldRenderFullProfile: brand.shouldRenderFullProfile === true,
    productionDisplayState: brand.brandExplorerDisplayState || null,
    previewUrls: urls,
    bannerText: FACTORY_PREVIEW_BANNER_TEXT,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE.name,
  };
}

export function assertFactoryPreviewDoesNotAffectActiveUniverse() {
  const errors = [];
  if (!ACTIVE_UNIVERSE_SOURCE?.formula?.includes("Active")) {
    errors.push("active_universe_source_unexpected");
  }
  for (const slug of FACTORY_PREVIEW_CANDIDATE_SLUGS) {
    if (!slug || typeof slug !== "string") errors.push(\`invalid_candidate:\${slug}\`);
  }
  if (FACTORY_PREVIEW_DISPLAY_STATE === "active_profile_ready") {
    errors.push("factory_preview_must_not_reuse_active_profile_ready");
  }
  if (FACTORY_PREVIEW_DISPLAY_STATE === "external_owner_ready") {
    errors.push("factory_preview_must_not_reuse_external_owner_ready");
  }
  return {
    ok: errors.length === 0,
    errors,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    candidateCount: FACTORY_PREVIEW_CANDIDATE_SLUGS.length,
  };
}

export function factoryCandidateIsInActiveUniverseByStatus(brandStatus) {
  return isBrandStatusActive(brandStatus);
}

export default {
  FACTORY_PREVIEW_VERSION,
  FACTORY_PREVIEW_DISPLAY_STATE,
  FACTORY_PREVIEW_CANDIDATE_SLUGS,
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_BANNER_TEXT,
  resolveFactoryPreviewAllowlist,
  isFactoryPreviewModeEnabled,
  isFactoryPreviewCandidate,
  getFactoryPreviewIdentity,
  resolveFactoryPreviewSlug,
  getFactoryPreviewDisplayState,
  isFactoryPreviewQuery,
  buildFactoryPreviewUrls,
  canRenderFactoryPreview,
  buildFactoryPreviewApiMeta,
  assertFactoryPreviewDoesNotAffectActiveUniverse,
  factoryCandidateIsInActiveUniverseByStatus,
};
`;
}

export async function runWave12FactoryPreviewCohort({ apply = false, argv = [] } = {}) {
  const manifestPath = path.join(REPORTS_DIR, "brand-explorer-wave12-manifest.json");
  let manifestBrands = [];
  if (fs.existsSync(manifestPath)) {
    const m = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
    manifestBrands = m.brands || [];
  } else {
    const m = await runWave12Manifest();
    manifestBrands = m.brands || [];
  }

  if ((manifestBrands || []).some((b) => b.isActiveLive)) {
    throw new Error(
      "Wave12 Active/Live drift detected — refuse factory-preview-cohort until resolved"
    );
  }

  const plan = planWave12FactoryPreviewCohort(manifestBrands);
  const modulePath = path.join(
    ROOT,
    "lib/partner-intelligence/brand-explorer-factory-preview-candidates.js"
  );
  const nextSource = renderFactoryPreviewCandidatesModule(plan);
  const flags = parseApplyFlags(argv, WAVE12_FACTORY_PREVIEW_APPLY_FLAGS);

  const report = {
    ...plan,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: apply,
    applyFlagsOk: !apply || flags.ok,
    missingFlags: flags.missing,
    modulePath,
    priorCandidateSlugs: [...FACTORY_PREVIEW_CANDIDATE_SLUGS],
    invariant: assertFactoryPreviewDoesNotAffectActiveUniverse(),
    wroteModule: false,
  };

  if (apply) {
    if (!flags.ok) {
      throw new Error(`Missing apply flags: ${flags.missing.join(", ")}`);
    }
    fs.writeFileSync(modulePath, nextSource, "utf8");
    report.wroteModule = true;
    report.airtableWrites = false;
  }

  const md = [
    `# Brand Explorer Wave 12 — Factory Preview Cohort`,
    ``,
    `Generated: ${report.generatedAt}`,
    `Dry-run: **${!apply}** · Module written: **${report.wroteModule}**`,
    `Display state: \`${FACTORY_PREVIEW_DISPLAY_STATE}\``,
    `Banner: ${FACTORY_PREVIEW_BANNER_TEXT}`,
    `Airtable writes: **false**`,
    ``,
    `## Candidates (${report.candidateSlugs.length})`,
    ``,
    ...report.candidateSlugs.map((s) => {
      const id = report.identities[s];
      return `- \`${s}\` — ${id?.name || s} (\`${id?.recordId || "no recordId yet"}\`)`;
    }),
    ``,
    `## Notes`,
    ``,
    `- Replaces prior factory preview cohort (${report.priorCandidateSlugs.join(", ")}) which are now in the protected 27 Active/Live baseline.`,
    `- Does not change Brand Status, release fields, or protected baseline brands.`,
    ``,
  ].join("\n");

  const paths = writeJsonMd("brand-explorer-wave12-factory-preview-cohort", report, md);
  return { ...report, paths, nextSource };
}

export async function runWave12Factory({ stage, dryRun = true, argv = [] } = {}) {
  const s = String(stage || "").trim().toLowerCase();
  if (!WAVE12_STAGES.includes(s) && s !== "preflight") {
    // preflight is aliased
  }
  const apply = argv.includes("--apply") && !dryRun;

  switch (s) {
    case "preflight":
      return runWave12Preflight({
        skipLongGates: argv.includes("--skip-long-gates"),
      });
    case "manifest":
      return runWave12Manifest();
    case "factory-preview-cohort":
      return runWave12FactoryPreviewCohort({
        apply: argv.includes("--apply"),
        argv,
      });
    case "source-packs": {
      const { runWave12SourcePacks } = await import("./brand-explorer-wave12-source-packs.js");
      return runWave12SourcePacks({ dryRun: dryRun !== false });
    }
    case "tab-factory-build": {
      const { runWave12TabFactoryBuild } = await import("./brand-explorer-wave12-tab-factory-build.js");
      return runWave12TabFactoryBuild({
        dryRun: dryRun !== false && !argv.includes("--apply"),
        argv,
      });
    }
    case "image-materialization": {
      const { runWave12ImageMaterialization } = await import(
        "./brand-explorer-wave12-image-materialization.js"
      );
      const brandsIdx = argv.indexOf("--brands");
      const brands =
        brandsIdx >= 0 && argv[brandsIdx + 1]
          ? argv[brandsIdx + 1]
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
          : null;
      return runWave12ImageMaterialization({
        dryRun: dryRun !== false && !argv.includes("--apply"),
        argv,
        brands,
      });
    }
    case "post-image-content-cleanup": {
      const { runWave12PostImageContentCleanup } = await import(
        "./brand-explorer-wave12-post-image-content-cleanup.js"
      );
      return runWave12PostImageContentCleanup({
        dryRun: dryRun !== false && !argv.includes("--apply"),
        argv,
      });
    }
    case "founder-review": {
      const { runWave12FounderReview } = await import("./brand-explorer-wave12-founder-review.js");
      return runWave12FounderReview({ argv });
    }
    case "status-promotion": {
      const { runWave12StatusPromotion } = await import(
        "./brand-explorer-wave12-status-promotion.js"
      );
      return runWave12StatusPromotion({
        apply: argv.includes("--apply"),
        argv,
      });
    }
    case "public-release": {
      const { runWave12PublicRelease } = await import("./brand-explorer-wave12-public-release.js");
      return runWave12PublicRelease({
        apply: argv.includes("--apply"),
        argv,
      });
    }
    case "post-release-freeze-cleanup": {
      const { runWave12PostReleaseFreezeCleanup } = await import(
        "./brand-explorer-wave12-post-release-freeze-cleanup.js"
      );
      return runWave12PostReleaseFreezeCleanup({
        apply: argv.includes("--apply"),
        argv,
      });
    }
    case "evidence-quality-fixes":
    case "gate-suite":
    case "baseline-39":
      return {
        version: WAVE12_VERSION,
        stage: s,
        generatedAt: new Date().toISOString(),
        dryRun: true,
        deferred: true,
        message: `Stage '${s}' is scaffolded but not implemented in this pass. Complete prior stages first; then implement per Tab Factory / Lane2 patterns.`,
        nextRequired: "See task brief for stage requirements.",
      };
    default:
      throw new Error(`Unknown stage '${stage}'. Allowed: ${WAVE12_STAGES.join(", ")}`);
  }
}

export {
  WAVE12_VERSION,
  WAVE12_STAGES,
  WAVE12_SLUGS,
  WAVE12_BRAND_PLAN,
};
