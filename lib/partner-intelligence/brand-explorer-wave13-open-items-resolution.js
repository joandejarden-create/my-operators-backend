/**
 * Wave 13 Stage 3.5 — Open items resolution.
 *
 * Allowed Airtable write (apply only + flags): create one Brand Basics row for SO/.
 * Fairmont: document-only (no rename).
 * House of Originals: founder/manual review note only (no Morgans Originals writes).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  WAVE13_VERSION,
  WAVE13_SLUGS,
  WAVE13_PROTECTED_BASELINE_COUNT,
  WAVE13_OPEN_ITEMS_APPLY_FLAGS,
  WAVE13_BASICS_FIELD_MAP,
  WAVE13_SO_BASICS_CREATE_PLAN,
} from "./brand-explorer-wave13-factory-plan.js";
import { EXPECTED_ACTIVE_COUNT_39 } from "./brand-explorer-39-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { getWave13SourcePack } from "./brand-explorer-wave13-source-packs-content.js";
import {
  planWave13FactoryPreviewCohort,
  renderWave13FactoryPreviewCandidatesModule,
} from "./brand-explorer-wave13-factory-preview-cohort.js";

export const WAVE13_OPEN_ITEMS_VERSION = "brand-explorer-wave13-open-items-resolution-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");
const FACTORY_PREVIEW_MODULE = path.join(
  ROOT,
  "lib/partner-intelligence/brand-explorer-factory-preview-candidates.js"
);

const FORBIDDEN_CREATE_FIELDS = Object.freeze([
  WAVE13_BASICS_FIELD_MAP.companyValidated,
  WAVE13_BASICS_FIELD_MAP.companyValidationDate,
  WAVE13_BASICS_FIELD_MAP.validationStatus,
  WAVE13_BASICS_FIELD_MAP.activeProfileApproved,
  WAVE13_BASICS_FIELD_MAP.readyForActiveProfile,
  WAVE13_BASICS_FIELD_MAP.activeProfileApprovedDate,
  WAVE13_BASICS_FIELD_MAP.founderVisualReviewPass,
  WAVE13_BASICS_FIELD_MAP.sourceLibrary,
  WAVE13_BASICS_FIELD_MAP.assetRegistry,
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeText(filePath, contents) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, contents.endsWith("\n") ? contents : `${contents}\n`, "utf8");
}

function readJson(name) {
  const p = path.join(REPORTS_DIR, name);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    console.error(`[wave13-open-items] failed reading ${name}:`, err?.message || err);
    return null;
  }
}

function parseApplyFlags(argv) {
  const missing = WAVE13_OPEN_ITEMS_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function airtableHeaders() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!apiKey) throw new Error("AIRTABLE_API_KEY required");
  return {
    Authorization: `Bearer ${apiKey}`,
    "Content-Type": "application/json",
  };
}

function basicsBaseUrl() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!baseId) throw new Error("AIRTABLE_BASE_ID required");
  return `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}`;
}

async function listBasicsByNames(names) {
  const results = [];
  for (const name of names) {
    const formula = `{Brand Name}='${String(name).replace(/'/g, "\\'")}'`;
    const url =
      `${basicsBaseUrl()}?` +
      new URLSearchParams({ filterByFormula: formula, pageSize: "20" });
    const res = await fetch(url, { headers: airtableHeaders() });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(json.error?.message || `Basics list failed ${res.status} for ${name}`);
    }
    for (const r of json.records || []) results.push(r);
  }
  const map = new Map();
  for (const r of results) map.set(r.id, r);
  return [...map.values()];
}

async function getBasicsById(recordId) {
  const res = await fetch(`${basicsBaseUrl()}/${recordId}`, { headers: airtableHeaders() });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics get failed ${res.status}`);
  return json;
}

function buildSoCreatePayload() {
  const plan = WAVE13_SO_BASICS_CREATE_PLAN;
  const fields = {
    [WAVE13_BASICS_FIELD_MAP.brandName]: plan.brandName,
    [WAVE13_BASICS_FIELD_MAP.brandStatus]: plan.brandStatus,
    [WAVE13_BASICS_FIELD_MAP.parentCompany]: plan.parentCompany,
    [WAVE13_BASICS_FIELD_MAP.internalNotes]: plan.internalNotes,
  };
  const validationFailures = [];
  for (const key of Object.keys(fields)) {
    if (!plan.allowedFields.includes(key)) {
      validationFailures.push(`field_not_allowed:${key}`);
    }
    if (FORBIDDEN_CREATE_FIELDS.includes(key)) {
      validationFailures.push(`forbidden_field:${key}`);
    }
  }
  if (fields[WAVE13_BASICS_FIELD_MAP.brandStatus] !== "Under Review") {
    validationFailures.push("brand_status_must_be_under_review");
  }
  if (isBrandStatusActive(fields[WAVE13_BASICS_FIELD_MAP.brandStatus])) {
    validationFailures.push("brand_status_must_not_be_active_live");
  }
  if (nz(fields[WAVE13_BASICS_FIELD_MAP.brandName]) !== "SO/") {
    validationFailures.push("brand_name_must_be_SO/");
  }
  if (nz(fields[WAVE13_BASICS_FIELD_MAP.parentCompany]) !== "AccorHotels") {
    validationFailures.push("parent_company_must_match_source_pack_siblings");
  }
  return {
    validation: {
      pass: validationFailures.length === 0,
      failures: validationFailures,
    },
    fieldMapping: {
      brandName: WAVE13_BASICS_FIELD_MAP.brandName,
      brandStatus: WAVE13_BASICS_FIELD_MAP.brandStatus,
      parentCompany: WAVE13_BASICS_FIELD_MAP.parentCompany,
      internalNotes: WAVE13_BASICS_FIELD_MAP.internalNotes,
      slugNote:
        "No Brand Basics Slug field in schema — Wave 13 slug remains code-side so-hotels-and-resorts via factory-preview identity + WAVE13_SLUGS.",
      displayAliasNote:
        "No dedicated display-alias field — alias SO/ Hotels & Resorts recorded in Internal Notes + factory-preview identity name.",
    },
    sanitizedPayloadPreview: {
      table: BASICS_TABLE,
      fields: { ...fields },
    },
    fields,
  };
}

async function createSoBasics(fields) {
  const res = await fetch(basicsBaseUrl(), {
    method: "POST",
    headers: airtableHeaders(),
    body: JSON.stringify({ fields, typecast: false }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(json.error?.message || `Basics create failed ${res.status}`);
    err.airtable = json;
    throw err;
  }
  return json;
}

function updateFactoryPreviewSoIdentity({ recordId, brandName, displayAlias }) {
  const manifest = readJson("brand-explorer-wave13-manifest.json");
  const brands = [...(manifest?.brands || [])];
  const idx = brands.findIndex((b) => b.slug === "so-hotels-and-resorts");
  const soRow = {
    slug: "so-hotels-and-resorts",
    brandName: brandName || "SO/",
    recordId,
    classification: "existing_needs_factory_build",
    brandStatus: "Under Review",
    parentCompany: "AccorHotels",
  };
  if (idx >= 0) brands[idx] = { ...brands[idx], ...soRow };
  else brands.push(soRow);

  const plan = planWave13FactoryPreviewCohort(brands);
  // Keep consumer display alias on factory identity name while Basics name is SO/.
  if (plan.identities["so-hotels-and-resorts"]) {
    plan.identities["so-hotels-and-resorts"].name = displayAlias || "SO/ Hotels & Resorts";
    plan.identities["so-hotels-and-resorts"].recordId = recordId;
    plan.identities["so-hotels-and-resorts"].missingBasics = false;
    plan.identities["so-hotels-and-resorts"].brandStatus = "Under Review";
  }
  const nextSource = renderWave13FactoryPreviewCandidatesModule(plan);
  fs.writeFileSync(FACTORY_PREVIEW_MODULE, nextSource, "utf8");
  return {
    wroteModule: true,
    modulePath: path.relative(ROOT, FACTORY_PREVIEW_MODULE).replace(/\\/g, "/"),
    identity: plan.identities["so-hotels-and-resorts"],
  };
}

function runNpm(scriptArgs, { timeoutMs = 90 * 60 * 1000 } = {}) {
  const result = spawnSync("npm", ["run", ...scriptArgs], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: timeoutMs,
    maxBuffer: 20 * 1024 * 1024,
    shell: true,
  });
  return {
    ok: result.status === 0,
    status: result.status,
    error: result.error ? String(result.error.message || result.error) : null,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
  };
}

function buildHouseFounderReview() {
  const pack = getWave13SourcePack("the-house-of-originals");
  const status = pack?.officialStatusAssessment || {};
  return {
    slug: "the-house-of-originals",
    brandBasicsName: "The House of Originals",
    recordId: "rec7ZPOVYsldGmNfx",
    brandStatus: "Under Review",
    currentOfficialStatusFromSourcePack: status.status || "unknown",
    currentOfficialSuccessor: status.currentOfficialSuccessor || "Morgans Originals",
    successorBrandPage: status.successorBrandPage || null,
    historicalLaunchSource: status.historicalLaunchSource || null,
    evidenceSupersededByMorgansOriginals: [
      "2019 Accor/sbe PR launched The House of Originals (historical).",
      "Accor Brandbook (March 2026) lifestyle set lists Morgans Originals — not The House of Originals.",
      "Accor Group brand page exists for Morgans Originals; no current consumer brand hub found for House of Originals.",
      "Accor–Ennismore JV materials list Morgans Originals among lifestyle brands.",
      "April 2021 trade/press coverage frames Morgans Originals launch as successor lifestyle collection.",
    ],
    officialSourcesStillSupportHouseAsActive: false,
    officialSupportNote:
      "No current official Accor/Ennismore brand hub supports The House of Originals as an active development brand/collection. Support is historical (2019 launch PDF) only.",
    options: {
      A: "Keep The House of Originals in Wave 13 (active factory build under current name)",
      B: "Replace with Morgans Originals (separate Basics/slug — not created in this task)",
      C: "Remove from Wave 13 Stage 4 scope and proceed with seven brands",
      D: "Hold for later manual review (block House content; do not decide A–C yet)",
    },
    recommendation: "C",
    recommendationRationale:
      "Official sources no longer support House of Originals as an active Accor/Ennismore brand; Morgans Originals appears to be the current collection. Replacing (B) requires a separate approved Basics/slug task. Keeping (A) risks publishing under an obsolete name. Recommend excluding House from Stage 4 and proceeding with the other seven Wave 13 brands after SO/ Basics exists.",
    doNotCreateMorgansOriginalsInThisTask: true,
    morgansOriginalsAirtableWrites: false,
  };
}

function buildFairmontHandling() {
  return {
    slug: "fairmont-hotels-and-resorts",
    recordId: "recJhPaDVU3YUDQUt",
    brandBasicsNameRemains: "Fairmont",
    consumerDisplayContextAllowed: "Fairmont Hotels & Resorts",
    renamePerformed: false,
    slugChanged: false,
    airtableWrites: false,
    note:
      "Brand Basics name remains Fairmont. “Fairmont Hotels & Resorts” may be used only as consumer/display context in copy where appropriate — no Airtable rename and no slug change in this stage.",
  };
}

function renderSoCreationMd(so) {
  return [
    "# Wave 13 — SO/ Brand Basics creation",
    "",
    `Generated: ${so.generatedAt}`,
    `Dry-run: **${so.dryRun}** · Created: **${so.created}**`,
    "",
    "## Planned / applied values",
    "",
    `| Field | Value |`,
    `| --- | --- |`,
    `| Brand Name | \`${so.plan.brandName}\` |`,
    `| Brand Status | \`${so.plan.brandStatus}\` |`,
    `| Parent Company | \`${so.plan.parentCompany}\` |`,
    `| Display alias (no Airtable field) | \`${so.plan.displayAlias}\` |`,
    `| Code-side slug | \`${so.plan.slug}\` |`,
    `| Record ID | \`${so.recordId || "— (dry-run)"}\` |`,
    "",
    "## Validation",
    "",
    so.payload.validation.pass
      ? "- PASS — create payload limited to allowed fields; Status Under Review"
      : so.payload.validation.failures.map((f) => `- FAIL: \`${f}\``).join("\n"),
    "",
    "## Field mapping",
    "",
    ...Object.entries(so.payload.fieldMapping).map(([k, v]) => `- **${k}:** ${v}`),
    "",
    "## Sanitized payload preview",
    "",
    "```json",
    JSON.stringify(so.payload.sanitizedPayloadPreview, null, 2),
    "```",
    "",
    "## Error handling",
    "",
    "- Validation error → refuse create; report failures (no Airtable call).",
    "- API error → surface Airtable message; no partial Presentation/release writes.",
    "- Network error → fail stage; retry after connectivity restore.",
    "",
    "## Protections",
    "",
    "- No Active/Live status",
    "- No release fields",
    "- No Company Validated / Source Library / Registry",
    "- No Presentation / image writes",
    "- No protected 39 brand changes",
    "- No Morgans Originals record changes",
    "",
  ]
    .join("\n");
}

function renderHouseReviewMd(house) {
  return [
    "# Wave 13 — The House of Originals founder / manual review",
    "",
    `Brand Basics: **${house.brandBasicsName}** (\`${house.recordId}\`) · Status: **${house.brandStatus}**`,
    "",
    "## Current official status (from Stage 3 source pack)",
    "",
    `- Assessment: \`${house.currentOfficialStatusFromSourcePack}\``,
    `- Likely successor: **${house.currentOfficialSuccessor}**`,
    house.successorBrandPage ? `- Successor page: ${house.successorBrandPage}` : "",
    house.historicalLaunchSource ? `- Historical launch: ${house.historicalLaunchSource}` : "",
    "",
    "## Evidence it may be superseded by Morgans Originals",
    "",
    ...house.evidenceSupersededByMorgansOriginals.map((e) => `- ${e}`),
    "",
    "## Do official sources still support House of Originals as active?",
    "",
    `**${house.officialSourcesStillSupportHouseAsActive}** — ${house.officialSupportNote}`,
    "",
    "## Options",
    "",
    `- **A.** ${house.options.A}`,
    `- **B.** ${house.options.B}`,
    `- **C.** ${house.options.C}`,
    `- **D.** ${house.options.D}`,
    "",
    "## Recommendation",
    "",
    `**${house.recommendation}** — ${house.recommendationRationale}`,
    "",
    "## Guardrails",
    "",
    "- No Morgans Originals Brand Basics create/update in this task",
    "- No House Brand Status / rename / Presentation writes in this task",
    "",
  ]
    .filter((l) => l !== "")
    .join("\n");
}

/**
 * @param {{ dryRun?: boolean, apply?: boolean, argv?: string[], runPostValidation?: boolean }} opts
 */
export async function runWave13OpenItemsResolution({
  dryRun = true,
  apply = false,
  argv = [],
  runPostValidation = true,
} = {}) {
  ensureDir(REPORTS_DIR);
  ensureDir(DOCS_DIR);

  const flags = parseApplyFlags(argv);
  const applyRequested = apply || flags.apply;
  const isDryRun = !applyRequested;

  const sourcePacks = readJson("brand-explorer-wave13-source-pack-summary.json");
  if (!sourcePacks || sourcePacks.deferred || !sourcePacks.allPacksValid) {
    const deferred = {
      version: WAVE13_OPEN_ITEMS_VERSION,
      stage: "open-items-resolution",
      generatedAt: new Date().toISOString(),
      deferred: true,
      pass: false,
      stopRecommended: true,
      reason: "wave13_source_packs_not_ready",
      message:
        "Open-items resolution deferred — complete Stage 3 source-packs dry-run first.",
      nextRequired:
        "npm run brand-explorer-wave13-factory -- --stage source-packs --dry-run --reuse-fresh-reports",
      airtableWrites: false,
    };
    const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-open-items-resolution.json");
    const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-open-items-resolution.md");
    writeText(jsonPath, JSON.stringify(deferred, null, 2));
    writeText(
      mdPath,
      `# Wave 13 open-items-resolution — Deferred\n\n${deferred.message}\n\nNext: \`${deferred.nextRequired}\`\n`
    );
    return { ...deferred, paths: { jsonPath, mdPath } };
  }

  const existingSo = await listBasicsByNames([
    "SO/",
    "SO/ Hotels & Resorts",
    "SO Hotels & Resorts",
    "SO/ Hotels",
  ]);
  const fairmontLive = await getBasicsById("recJhPaDVU3YUDQUt");
  const houseLive = await getBasicsById("rec7ZPOVYsldGmNfx");
  const universe = await loadActiveUniverse({ includeDetails: false });

  const payload = buildSoCreatePayload();
  const house = buildHouseFounderReview();
  const fairmont = buildFairmontHandling();
  fairmont.liveBrandName = nz(fairmontLive.fields?.["Brand Name"]);
  fairmont.liveBrandStatus = nz(fairmontLive.fields?.["Brand Status"]);
  house.liveBrandName = nz(houseLive.fields?.["Brand Name"]);
  house.liveBrandStatus = nz(houseLive.fields?.["Brand Status"]);
  house.liveParentCompany = nz(houseLive.fields?.["Parent Company"]);

  const soAlreadyExists = existingSo.length > 0;
  let soRecord = soAlreadyExists ? existingSo[0] : null;
  let created = false;
  let factoryPreviewUpdate = null;
  const writeErrors = [];

  if (applyRequested) {
    if (!flags.ok) {
      throw new Error(`Missing apply flags: ${flags.missing.join(", ")}`);
    }
    if (!payload.validation.pass) {
      throw new Error(`SO create validation failed: ${payload.validation.failures.join(", ")}`);
    }
    if (soAlreadyExists) {
      // Idempotent: do not create a duplicate; verify Under Review.
      const status = nz(soRecord.fields?.["Brand Status"]);
      if (status !== "Under Review") {
        throw new Error(
          `Existing SO/ Basics ${soRecord.id} has Brand Status "${status}" — refuse (must be Under Review)`
        );
      }
      if (isBrandStatusActive(status)) {
        throw new Error(`Existing SO/ Basics ${soRecord.id} is Active/Live — refuse`);
      }
    } else {
      try {
        soRecord = await createSoBasics(payload.fields);
        created = true;
      } catch (err) {
        writeErrors.push({
          type: "api_error",
          message: err?.message || String(err),
          airtable: err?.airtable || null,
        });
        throw err;
      }
    }

    factoryPreviewUpdate = updateFactoryPreviewSoIdentity({
      recordId: soRecord.id,
      brandName: nz(soRecord.fields?.["Brand Name"]) || "SO/",
      displayAlias: WAVE13_SO_BASICS_CREATE_PLAN.displayAlias,
    });
  }

  const soStatus = nz(soRecord?.fields?.["Brand Status"]) || null;
  const soName = nz(soRecord?.fields?.["Brand Name"]) || null;
  const soParent = nz(soRecord?.fields?.["Parent Company"]) || null;

  const soReport = {
    generatedAt: new Date().toISOString(),
    dryRun: isDryRun,
    created,
    alreadyExisted: soAlreadyExists && !created,
    recordId: soRecord?.id || null,
    brandName: soName || WAVE13_SO_BASICS_CREATE_PLAN.brandName,
    brandStatus: soStatus || WAVE13_SO_BASICS_CREATE_PLAN.brandStatus,
    parentCompany: soParent || WAVE13_SO_BASICS_CREATE_PLAN.parentCompany,
    displayAlias: WAVE13_SO_BASICS_CREATE_PLAN.displayAlias,
    slugCodeSide: WAVE13_SO_BASICS_CREATE_PLAN.slug,
    isActiveLive: isBrandStatusActive(soStatus),
    releaseFieldsSet: false,
    companyValidatedSet: false,
    presentationRowsCreated: false,
    plan: WAVE13_SO_BASICS_CREATE_PLAN,
    payload,
    factoryPreviewUpdate,
    writeErrors,
  };

  // Post-apply validation (or dry-run prechecks that don't rewrite).
  let postValidation = {
    skipped: !runPostValidation || isDryRun,
    manifest: null,
    activeUniverse: null,
    baseline39: null,
  };

  if (applyRequested && runPostValidation) {
    console.log("[wave13-open-items] re-running manifest dry-run...");
    const manifestRun = runNpm([
      "brand-explorer-wave13-factory",
      "--",
      "--stage",
      "manifest",
      "--dry-run",
      "--reuse-fresh-reports",
    ]);
    const manifestJson = readJson("brand-explorer-wave13-manifest.json");
    const soManifest = (manifestJson?.brands || []).find((b) => b.slug === "so-hotels-and-resorts");

    console.log("[wave13-open-items] active universe SoT dry-run...");
    const universeRun = runNpm([
      "brand-explorer-active-universe-source-of-truth",
      "--",
      "--dry-run",
    ]);

    console.log("[wave13-open-items] protected 39 baseline regression...");
    const baselineRun = runNpm([
      "test:brand-explorer-39-active-public-full-baseline",
      "--",
      "--allow-cached-pvql-if-pass",
    ]);

    const universeAfter = await loadActiveUniverse({ includeDetails: false });
    postValidation = {
      skipped: false,
      manifest: {
        ok: manifestRun.ok,
        status: manifestRun.status,
        soRecordId: soManifest?.recordId || null,
        soBrandName: soManifest?.brandName || null,
        soBrandStatus: soManifest?.brandStatus || null,
        soClassification: soManifest?.classification || null,
        soBasicsExists: soManifest?.basicsExists === true,
        mayProceedToFactoryPreviewCohort: manifestJson?.mayProceedToFactoryPreviewCohort === true,
        protectedActiveCount: manifestJson?.protectedActiveCount ?? null,
      },
      activeUniverse: {
        ok: universeRun.ok,
        status: universeRun.status,
        totalCount: universeAfter.totalCount,
        expected: EXPECTED_ACTIVE_COUNT_39,
        remains39: universeAfter.totalCount === EXPECTED_ACTIVE_COUNT_39,
        soInActiveUniverse: (universeAfter.brands || []).some(
          (b) => b.slug === "so-hotels-and-resorts" || b.recordId === soRecord?.id
        ),
      },
      baseline39: {
        ok: baselineRun.ok,
        status: baselineRun.status,
        tail: (baselineRun.stdout || "")
          .split(/\r?\n/)
          .filter(Boolean)
          .slice(-8),
      },
    };
  }

  const soReady =
    Boolean(soRecord?.id) &&
    (soStatus === "Under Review" || (!soStatus && applyRequested === false && payload.validation.pass));
  const soReadyApplied =
    applyRequested &&
    Boolean(soRecord?.id) &&
    soStatus === "Under Review" &&
    !isBrandStatusActive(soStatus);

  const stage4Posture = {
    allEightBrands: false,
    sevenExcludingHouseOfOriginals: soReadyApplied || (soAlreadyExists && soStatus === "Under Review"),
    replacementBrandPendingApproval: false,
    houseRecommendation: house.recommendation,
    narrative:
      house.recommendation === "C"
        ? "Stage 4 may proceed with seven brands excluding The House of Originals (after SO/ Basics exists). Replacement with Morgans Originals remains a separate approved task if founder chooses B later."
        : "Stage 4 posture depends on founder decision for House of Originals.",
  };

  // If dry-run and SO missing, seven-brand posture is "pending SO apply".
  if (isDryRun && !soAlreadyExists) {
    stage4Posture.sevenExcludingHouseOfOriginals = false;
    stage4Posture.narrative =
      "After apply creates SO/ Under Review Basics, Stage 4 may proceed with seven brands excluding The House of Originals (recommendation C). All-eight is blocked until House founder decision changes.";
  }

  const report = {
    version: WAVE13_OPEN_ITEMS_VERSION,
    factoryVersion: WAVE13_VERSION,
    stage: "open-items-resolution",
    generatedAt: new Date().toISOString(),
    dryRun: isDryRun,
    deferred: false,
    applyRequested,
    applyFlagsOk: !applyRequested || flags.ok,
    missingFlags: flags.missing,
    airtableWrites: created,
    presentationWrites: false,
    brandStatusPromotionWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    imageWrites: false,
    protected39Writes: false,
    morgansOriginalsWrites: false,
    fairmontRenamed: false,
    protectedBaselineCount: WAVE13_PROTECTED_BASELINE_COUNT,
    activeUniverseBefore: {
      totalCount: universe.totalCount,
      expected: EXPECTED_ACTIVE_COUNT_39,
      remains39: universe.totalCount === EXPECTED_ACTIVE_COUNT_39,
    },
    so: soReport,
    fairmont,
    houseOfOriginals: house,
    postValidation,
    stage4Posture,
    acceptance: {
      soBasicsExists: Boolean(soRecord?.id) || (isDryRun && payload.validation.pass),
      soUnderReview: isDryRun ? true : soStatus === "Under Review",
      soNotActiveLive: isDryRun ? true : !isBrandStatusActive(soStatus),
      soNoReleaseFields: true,
      soNoCompanyValidated: true,
      soNoPresentationRowsCreatedByThisTask: true,
      activeUniverseRemains39:
        postValidation.activeUniverse?.remains39 ??
        universe.totalCount === EXPECTED_ACTIVE_COUNT_39,
      protected39BaselinePass: postValidation.baseline39?.ok ?? null,
      fairmontRemainsFairmont:
        fairmont.liveBrandName === "Fairmont" && fairmont.renamePerformed === false,
      houseFounderReviewDocumented: true,
    },
    pass:
      payload.validation.pass &&
      fairmont.liveBrandName === "Fairmont" &&
      (!applyRequested ||
        (soReadyApplied &&
          postValidation.manifest?.soBasicsExists === true &&
          postValidation.manifest?.soBrandStatus === "Under Review" &&
          postValidation.activeUniverse?.remains39 === true &&
          postValidation.baseline39?.ok === true)),
    stopRecommended: false,
    readyStatement: isDryRun
      ? "wave13_open_items_resolution_dry_run_ready"
      : soReadyApplied
        ? "wave13_open_items_resolution_applied_stage4_may_proceed_seven_excluding_house"
        : "wave13_open_items_resolution_incomplete",
    wave13Slugs: [...WAVE13_SLUGS],
  };

  report.stopRecommended = report.pass !== true;

  const soMdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-brand-basics-creation.md");
  const houseMdPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave13-house-of-originals-founder-review.md"
  );
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-open-items-resolution.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-open-items-resolution.md");
  const docPath = path.join(DOCS_DIR, "brand-explorer-wave13-open-items-resolution.md");

  writeText(soMdPath, renderSoCreationMd(soReport));
  writeText(houseMdPath, renderHouseReviewMd(house));

  const md = [
    "# Wave 13 — Open items resolution",
    "",
    `Version: \`${WAVE13_OPEN_ITEMS_VERSION}\` · Generated: ${report.generatedAt}`,
    `Dry-run: **${report.dryRun}** · Airtable writes: **${report.airtableWrites}**`,
    `Ready: \`${report.readyStatement}\``,
    "",
    "## 1. SO/ Brand Basics",
    "",
    `| Check | Result |`,
    `| --- | --- |`,
    `| Record ID | \`${soReport.recordId || "— (dry-run preview)"}\` |`,
    `| Brand Name | \`${soReport.brandName}\` |`,
    `| Brand Status | \`${soReport.brandStatus}\` |`,
    `| Parent Company | \`${soReport.parentCompany}\` |`,
    `| Created this run | **${soReport.created}** |`,
    `| Active/Live | **${soReport.isActiveLive}** |`,
    `| Display alias (notes / factory identity) | \`${soReport.displayAlias}\` |`,
    `| Code-side slug | \`${soReport.slugCodeSide}\` |`,
    "",
    `Details: [\`brand-explorer-wave13-so-brand-basics-creation.md\`](brand-explorer-wave13-so-brand-basics-creation.md)`,
    "",
    "## 2. Fairmont",
    "",
    `- Brand Basics name remains **${fairmont.liveBrandName}** (expected Fairmont)`,
    `- Consumer/display context allowed: **${fairmont.consumerDisplayContextAllowed}**`,
    `- Rename performed: **false** · Slug changed: **false** · Airtable writes: **false**`,
    "",
    "## 3. The House of Originals",
    "",
    `- Recommendation: **${house.recommendation}**`,
    `- ${house.recommendationRationale}`,
    `- Details: [\`brand-explorer-wave13-house-of-originals-founder-review.md\`](brand-explorer-wave13-house-of-originals-founder-review.md)`,
    "",
    "## Stage 4 posture",
    "",
    `| Path | Allowed |`,
    `| --- | --- |`,
    `| All eight brands | **${stage4Posture.allEightBrands}** |`,
    `| Seven brands excluding The House of Originals | **${stage4Posture.sevenExcludingHouseOfOriginals}** |`,
    `| Replacement brand pending approval | **${stage4Posture.replacementBrandPendingApproval}** |`,
    "",
    stage4Posture.narrative,
    "",
    "## Post-validation",
    "",
    postValidation.skipped
      ? "_Skipped on dry-run (run after --apply)._"
      : [
          `- Manifest SO Basics exists: **${postValidation.manifest?.soBasicsExists}** (\`${postValidation.manifest?.soRecordId}\`, status ${postValidation.manifest?.soBrandStatus})`,
          `- Active universe remains 39: **${postValidation.activeUniverse?.remains39}** (count=${postValidation.activeUniverse?.totalCount})`,
          `- SO in Active universe: **${postValidation.activeUniverse?.soInActiveUniverse}** (must be false)`,
          `- Protected 39 baseline: **${postValidation.baseline39?.ok}**`,
        ].join("\n"),
    "",
    "## Commands",
    "",
    "```bash",
    "npm run brand-explorer-wave13-factory -- --stage open-items-resolution --dry-run",
    "npm run brand-explorer-wave13-factory -- --stage open-items-resolution --apply \\",
    "  --approve-so-brand-basics-creation \\",
    "  --confirm-so-under-review-only \\",
    "  --confirm-no-active-live-status \\",
    "  --confirm-no-release-field-writes \\",
    "  --confirm-no-company-validation-changes \\",
    "  --confirm-no-source-library-status-changes \\",
    "  --confirm-no-registry-approval-changes \\",
    "  --confirm-no-presentation-writes \\",
    "  --confirm-no-image-writes \\",
    "  --confirm-no-protected-39-brand-changes \\",
    "  --confirm-no-morgans-originals-record-changes",
    "```",
    "",
  ].join("\n");
  writeText(mdPath, md);
  writeText(jsonPath, JSON.stringify(report, null, 2));

  const doc = [
    "# Brand Explorer — Wave 13 Open Items Resolution",
    "",
    `Version: \`${WAVE13_OPEN_ITEMS_VERSION}\``,
    "",
    "## Purpose",
    "",
    "Stage 3.5 resolves prerequisites before Stage 4 tab-factory-build:",
    "",
    "1. Create SO/ Brand Basics (Under Review only)",
    "2. Document Fairmont naming (no rename)",
    "3. Founder/manual review for The House of Originals",
    "",
    "## Schema notes",
    "",
    `- Table: \`${BASICS_TABLE}\``,
    "- Allowed create fields: Brand Name, Brand Status, Parent Company, Internal Notes",
    "- No Slug field and no display-alias field on Brand Basics — slug/display alias are code-side / notes",
    "- Brand Status option used: `Under Review` (Active/Live forbidden here)",
    "",
    "## Stage 4 recommendation",
    "",
    "Proceed with **seven brands excluding The House of Originals** after SO/ Basics exists (recommendation **C**).",
    "",
    "## Command",
    "",
    "```bash",
    "npm run brand-explorer-wave13-factory -- --stage open-items-resolution --dry-run",
    "```",
    "",
  ].join("\n");
  writeText(docPath, doc);

  return {
    version: WAVE13_VERSION,
    stage: "open-items-resolution",
    generatedAt: report.generatedAt,
    dryRun: report.dryRun,
    deferred: false,
    pass: report.pass,
    stopRecommended: report.stopRecommended,
    airtableWrites: report.airtableWrites,
    readyStatement: report.readyStatement,
    stage4Posture,
    summary: {
      soRecordId: soReport.recordId,
      soCreated: soReport.created,
      soStatus: soReport.brandStatus,
      fairmontRenamed: false,
      houseRecommendation: house.recommendation,
      mayProceedSevenExcludingHouse: stage4Posture.sevenExcludingHouseOfOriginals,
      mayProceedAllEight: stage4Posture.allEightBrands,
      readyStatement: report.readyStatement,
    },
    paths: {
      jsonPath,
      mdPath,
      docPath,
      soMdPath,
      houseMdPath,
    },
    report,
  };
}
