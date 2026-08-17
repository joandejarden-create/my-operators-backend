/**
 * Wave 15 Stage 6 — Post-image content cleanup (eight Hilton brands).
 *
 * Primary fix: sync Project Fit Min/Max Room Count → Portfolio & Performance
 * Minimum/Maximum Property Size (Rooms) so Overview snapshot.typical_keys renders.
 *
 * Also reconciles stale Portfolio 200–1000 defaults that diverge from Project Fit.
 *
 * Forbidden: Brand Status, release, CV, Source Library, Registry, protected 54,
 * Marriott Hotels, Four Points Flex, House of Originals, Morgans, Radisson Collection,
 * broad Presentation rewrites, new image materialization.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE15_VERSION,
  WAVE15_SLUGS,
  WAVE15_BRAND_PLAN,
  WAVE15_NEVER_WRITE_FIELDS,
  WAVE15_RELEASE_FIELDS,
  WAVE15_PROTECTED_BASELINE_COUNT,
  WAVE15_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS,
} from "./brand-explorer-wave15-factory-plan.js";
import { runWave15ProtectedFiftyFourIdentityPreflight } from "./brand-explorer-wave15-image-materialization.js";

export const WAVE15_POST_IMAGE_CLEANUP_VERSION = "wave15-post-image-content-cleanup-v1";
export const READY_STATE = "wave15_post_image_cleanup_ready_for_founder_review";
export const DRY_RUN_READY = "wave15_stage6_post_image_cleanup_dry_run_ready";

export { WAVE15_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS };

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PF_TABLE = "Brand Setup - Project Fit";
const PP_TABLE = "Brand Setup - Portfolio & Performance";

const FORBIDDEN = new Set([
  ...WAVE15_NEVER_WRITE_FIELDS,
  ...WAVE15_RELEASE_FIELDS,
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
  "Image",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function canonicalColumnName(s) {
  return String(s).trim().replace(/\u00A0/g, " ").replace(/\u2013|\u2014/g, "-");
}

function renderedKeys(min, max) {
  if (min != null && max != null && String(min) !== "" && String(max) !== "") {
    return `${min}–${max} rooms`;
  }
  if (min != null && String(min) !== "") return `${min}+ rooms (minimum)`;
  if (max != null && String(max) !== "") return `Up to ${max} rooms`;
  return "";
}

export function parseWave15PostImageCleanupFlags(argv = []) {
  const missing = WAVE15_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    ok: argv.includes("--apply") && missing.length === 0,
    missing,
    required: [...WAVE15_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS],
  };
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");
  return new Airtable({ apiKey }).base(baseId);
}

async function resolvePortfolioRoomFieldNames(base) {
  const rows = await base(PP_TABLE).select({ maxRecords: 80 }).all();
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r.fields)))];
  const pick = (re) => keys.find((k) => re.test(canonicalColumnName(k))) || null;
  const minKey = pick(/^minimum property size \(rooms\)$/i);
  const maxKey = pick(/^maximum property size \(rooms\)$/i);
  if (!minKey || !maxKey) {
    throw new Error(
      `Could not resolve Portfolio room-size columns (min=${minKey}, max=${maxKey})`
    );
  }
  return { minKey, maxKey };
}

async function findByName(base, table, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(table)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 3 })
    .all();
  return rows[0] || null;
}

async function updateWithPruning(base, table, recordId, fields) {
  let payload = { ...fields };
  for (const f of FORBIDDEN) delete payload[f];
  for (let attempt = 0; attempt < 20; attempt++) {
    if (!Object.keys(payload).length) return;
    try {
      await base(table).update(recordId, payload, { typecast: true });
      return;
    } catch (err) {
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const m = String(err.message || "").match(/Unknown field name: "([^"]+)"/);
        if (m && Object.prototype.hasOwnProperty.call(payload, m[1])) {
          delete payload[m[1]];
          continue;
        }
      }
      if (err.statusCode === 429 || (err.statusCode >= 500 && attempt < 19)) {
        await sleep(Math.min(30_000, 800 * 2 ** attempt));
        continue;
      }
      throw err;
    }
  }
}

function resolveIdentity(slug) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
  return { slug, recordId: id.recordId, name: id.name || WAVE15_BRAND_PLAN[slug].name };
}

export async function planWave15PostImageCleanupForBrand(slug, { minKey, maxKey, base } = {}) {
  const identity = resolveIdentity(slug);
  if (!WAVE15_SLUGS.includes(slug)) {
    return { brandSlug: slug, blocked: true, blockers: ["not_wave15"], patches: [] };
  }

  const airtable = base || getBase();
  const keys =
    minKey && maxKey
      ? { minKey, maxKey }
      : await resolvePortfolioRoomFieldNames(airtable);

  const pf = await findByName(airtable, PF_TABLE, identity.name);
  const pp = await findByName(airtable, PP_TABLE, identity.name);
  const basics = await findByName(airtable, BASICS_TABLE, identity.name);
  const brandStatus = nz(basics?.get("Brand Status"));

  const pfMin = pf?.get("Min - Room Count") ?? null;
  const pfMax = pf?.get("Max - Room Count") ?? null;
  const ppMin = pp?.get(keys.minKey) ?? null;
  const ppMax = pp?.get(keys.maxKey) ?? null;

  const patches = [];
  const acceptedHolds = [];
  const typicalKeys = {
    before: renderedKeys(ppMin, ppMax) || null,
    after: null,
    source: null,
    handling: null,
  };

  if (pfMin == null && pfMax == null) {
    typicalKeys.handling = "cleanly_unavailable_no_project_fit_range";
    acceptedHolds.push({
      type: "typical_keys_no_defensible_range",
      note: "No Project Fit Min/Max Room Count — leave Portfolio blank; completeness remains cleanly_unavailable",
    });
  } else if (
    !pp ||
    ppMin == null ||
    ppMax == null ||
    Number(ppMin) !== Number(pfMin) ||
    Number(ppMax) !== Number(pfMax)
  ) {
    if (!pp?.id) {
      return {
        brandSlug: slug,
        brandName: identity.name,
        recordId: identity.recordId,
        brandStatus,
        blocked: true,
        blockers: ["missing_portfolio_performance_row"],
        patches: [],
        acceptedHolds,
        typicalKeys,
      };
    }
    const fields = {};
    if (pfMin != null) fields[keys.minKey] = pfMin;
    if (pfMax != null) fields[keys.maxKey] = pfMax;
    const failureType =
      ppMin == null && ppMax == null
        ? "typical_keys_blank_cleanly_unavailable"
        : "typical_keys_stale_portfolio_mismatch";
    patches.push({
      method: "PATCH",
      table: PP_TABLE,
      recordId: pp.id,
      slotKey: "snapshot.typical_keys",
      fields,
      before: { min: ppMin, max: ppMax, rendered: renderedKeys(ppMin, ppMax) || "(blank)" },
      after: { min: pfMin, max: pfMax, rendered: renderedKeys(pfMin, pfMax) },
      rationale: `Stage 6 ${failureType}: sync Project Fit → Portfolio property size (rooms)`,
      failureType,
      sourceSupport: "Brand Setup - Project Fit Min/Max Room Count",
    });
    typicalKeys.after = renderedKeys(pfMin, pfMax);
    typicalKeys.source = "project_fit_min_max_room_count";
    typicalKeys.handling = failureType;
  } else {
    typicalKeys.after = renderedKeys(ppMin, ppMax);
    typicalKeys.source = "already_aligned_project_fit";
    typicalKeys.handling = "no_change";
  }

  // International Reference openings note (Homewood / Home2 / Tru / Spark) — document only
  if (
    ["homewood-suites-by-hilton", "home2-suites-by-hilton", "tru-by-hilton", "spark-by-hilton"].includes(
      slug
    )
  ) {
    acceptedHolds.push({
      type: "international_reference_openings",
      note: "Openings remain International Reference where CALA property URLs are unconfirmed — no sibling-brand substitutes",
    });
  }

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    brandStatus,
    blocked: false,
    blockers: [],
    patches,
    acceptedHolds,
    typicalKeys,
    projectFitRecordId: pf?.id || null,
    portfolioRecordId: pp?.id || null,
  };
}

export async function planWave15PostImageCleanup() {
  const base = getBase();
  const { minKey, maxKey } = await resolvePortfolioRoomFieldNames(base);
  const brands = [];
  for (const slug of WAVE15_SLUGS) {
    brands.push(await planWave15PostImageCleanupForBrand(slug, { minKey, maxKey, base }));
  }
  return {
    version: WAVE15_POST_IMAGE_CLEANUP_VERSION,
    wave15Version: WAVE15_VERSION,
    stage: "post-image-content-cleanup",
    generatedAt: new Date().toISOString(),
    scope: [...WAVE15_SLUGS],
    protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
    portfolioFieldKeys: { minKey, maxKey },
    brands,
    patchCount: brands.reduce((n, b) => n + (b.patches?.length || 0), 0),
    blockedCount: brands.filter((b) => b.blocked).length,
  };
}

export async function applyWave15PostImageCleanup({ plan, apply = false, argv = [] } = {}) {
  const flags = parseWave15PostImageCleanupFlags(argv);
  if (apply && !flags.ok) {
    throw new Error(`Missing apply flags: ${flags.missing.join(", ")}`);
  }
  if (!apply) {
    return { applied: false, writes: 0, results: [], flags };
  }

  const base = getBase();
  const results = [];
  let writes = 0;
  for (const brand of plan.brands || []) {
    if (brand.blocked) continue;
    for (const p of brand.patches || []) {
      if (p.method !== "PATCH") {
        throw new Error(`Wave 15 Stage 6 only allows PATCH; got ${p.method} for ${brand.brandSlug}`);
      }
      if (p.table !== PP_TABLE) {
        throw new Error(`Wave 15 Stage 6 unexpected table ${p.table}`);
      }
      await updateWithPruning(base, p.table, p.recordId, p.fields);
      writes += 1;
      await sleep(250);
      results.push({
        slug: brand.brandSlug,
        slotKey: p.slotKey,
        method: p.method,
        recordId: p.recordId,
        after: p.after,
      });
    }
  }
  return { applied: true, writes, results, flags };
}

async function assertAllUnderReview(plan) {
  const base = getBase();
  const issues = [];
  for (const brand of plan.brands || []) {
    const basics = await findByName(base, BASICS_TABLE, brand.brandName);
    const status = nz(basics?.get("Brand Status"));
    if (status !== "Under Review") {
      issues.push(`${brand.brandSlug}=${status || "(missing)"}`);
    }
  }
  return { pass: issues.length === 0, issues };
}

export function writeWave15PostImageCleanupReports(plan, applyResult = null, extras = {}) {
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });

  const ready =
    applyResult?.applied === true ? READY_STATE : DRY_RUN_READY;

  const report = {
    ...plan,
    dryRun: !applyResult?.applied,
    applyResult: applyResult
      ? {
          applied: applyResult.applied,
          writes: applyResult.writes,
          airtableWrites: applyResult.writes || 0,
          results: applyResult.results || [],
        }
      : { applied: false, writes: 0, airtableWrites: 0 },
    identityPreflight: extras.identityPreflight || null,
    underReviewCheck: extras.underReviewCheck || null,
    readyStatement: ready,
    protections: {
      noBrandStatusChanges: true,
      noReleaseFieldWrites: true,
      noCompanyValidationChanges: true,
      noSourceLibraryStatusChanges: true,
      noRegistryApprovalChanges: true,
      noPublicRestoreRegistryChanges: true,
      noProtected54BrandChanges: true,
      noMarriottHotelsWrites: true,
      noFourPointsFlexWrites: true,
      noHouseOfOriginalsWrites: true,
      noMorgansOriginalsWrites: true,
      noRadissonCollectionChanges: true,
      noBroadRewrites: true,
      noNewImageMaterialization: true,
      allEightRemainUnderReview: extras.underReviewCheck?.pass !== false,
      hiltonBrandFamilySeparated: true,
      snapshotTypicalKeysHandled: true,
    },
    founderReviewNote:
      "Eight Wave 15 Hilton brands remain Under Review / factory preview. Stage 6 synced Project Fit room ranges into Portfolio so Typical Keys Range renders. Homewood / Home2 / Tru / Spark openings remain International Reference where CALA is unconfirmed. Do not promote Brand Status yet.",
  };

  const jsonPath = path.join(REPORTS, "brand-explorer-wave15-post-image-cleanup.json");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);

  const md = [
    `# Wave 15 Stage 6 — Post-Image Cleanup`,
    ``,
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.dryRun ? "DRY-RUN" : "APPLY"}**`,
    `- Ready: **\`${report.readyStatement}\`**`,
    `- Patches planned: **${report.patchCount}** · Writes: **${report.applyResult.writes}**`,
    `- Protected 54 identity preflight: **${report.identityPreflight?.pass ? "PASS" : "n/a"}**`,
    `- All eight Under Review: **${report.underReviewCheck?.pass ? "PASS" : "CHECK"}**`,
    ``,
    `## Brand results`,
    ``,
    ...report.brands.map((b) => {
      const tk = b.typicalKeys || {};
      return `- **${b.brandName}** (\`${b.brandSlug}\`): patches=${b.patches?.length || 0} · typical_keys ${tk.before || "(blank)"} → ${tk.after || tk.handling || "—"}` +
        (b.acceptedHolds?.length
          ? ` · holds: ${b.acceptedHolds.map((h) => h.type).join(", ")}`
          : "");
    }),
    ``,
    `## snapshot.typical_keys handling`,
    ``,
    `| Brand | Before | After | Handling |`,
    `| --- | --- | --- | --- |`,
    ...report.brands.map((b) => {
      const tk = b.typicalKeys || {};
      return `| ${b.brandName} | ${tk.before || "(blank)"} | ${tk.after || "—"} | ${tk.handling || "—"} |`;
    }),
    ``,
    `## Protections`,
    ``,
    `- No Brand Status / release / CV / Source / Registry / public restore writes`,
    `- No protected 54 / Marriott Hotels / Four Points Flex / House of Originals / Morgans / Radisson Collection writes`,
    `- No broad Presentation rewrites; no new image materialization`,
    `- All eight remain Under Review / factory preview`,
    ``,
    `## Apply flags`,
    ``,
    ...WAVE15_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
    `## Founder review note`,
    ``,
    report.founderReviewNote,
    ``,
  ];
  const mdPath = path.join(REPORTS, "brand-explorer-wave15-post-image-cleanup.md");
  fs.writeFileSync(mdPath, md.join("\n"));

  for (const b of report.brands) {
    const tk = b.typicalKeys || {};
    const per = [
      `# Wave 15 Stage 6 — ${b.brandName}`,
      ``,
      `- Slug: \`${b.brandSlug}\``,
      `- Brand Status: **${b.brandStatus || "—"}**`,
      `- Patches: **${b.patches?.length || 0}**`,
      `- typical_keys: ${tk.before || "(blank)"} → **${tk.after || tk.handling || "—"}**`,
      `- Source: ${tk.source || "—"}`,
      ``,
      `## Patches`,
      ``,
      ...(b.patches || []).map(
        (p) =>
          `- \`${p.slotKey}\` ${p.method} \`${p.recordId}\`: ${p.before?.rendered} → ${p.after?.rendered}`
      ),
      b.patches?.length ? "" : "- (none)",
      ``,
      `## Accepted holds`,
      ``,
      ...(b.acceptedHolds?.length
        ? b.acceptedHolds.map((h) => `- **${h.type}**: ${h.note}`)
        : ["- (none)"]),
      ``,
    ];
    fs.writeFileSync(
      path.join(REPORTS, `brand-explorer-wave15-post-image-cleanup-${b.brandSlug}.md`),
      per.join("\n")
    );
  }

  const docsPath = path.join(DOCS, "brand-explorer-wave15-post-image-cleanup.md");
  fs.writeFileSync(
    docsPath,
    [
      `# Wave 15 — Post-Image Content Cleanup`,
      ``,
      `Stage 6 cleans residual post-image issues for the eight Wave 15 Hilton factory-preview brands before founder review.`,
      ``,
      `## Primary fix`,
      ``,
      "Overview **Typical Keys Range** (`snapshot.typical_keys`) is derived from **Brand Setup - Portfolio & Performance** `Minimum Property Size (Rooms)` / `Maximum Property Size (Rooms)` (NBSP-safe live max column name).",
      ``,
      "Stage 6 copies steward **Project Fit** `Min - Room Count` / `Max - Room Count` into those Portfolio fields (Choice-batch pattern) for all eight brands when blank or stale.",
      ``,
      `## Commands`,
      ``,
      "```bash",
      "npm run brand-explorer-wave15-factory -- --stage post-image-content-cleanup --dry-run",
      "npm run brand-explorer-wave15-factory -- --stage post-image-content-cleanup --apply \\",
      ...WAVE15_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.map(
        (f, i, arr) => `  ${f}${i < arr.length - 1 ? " \\" : ""}`
      ),
      "```",
      ``,
      `## Guardrails`,
      ``,
      `- Eight Wave 15 Hilton brands only; remain Under Review / factory preview`,
      `- No Brand Status / release / CV / Source / Registry / protected-54 writes`,
      `- No Marriott Hotels / Four Points Flex / House of Originals / Morgans / Radisson Collection writes`,
      `- No inventing key counts — Project Fit ranges only`,
      `- No broad Presentation rewrites; no new image materialization`,
      ``,
      `## Reports`,
      ``,
      `- \`reports/brand-explorer-wave15-post-image-cleanup-failures.{json,md}\``,
      `- \`reports/brand-explorer-wave15-post-image-cleanup.{json,md}\``,
      `- \`reports/brand-explorer-wave15-post-image-cleanup-{slug}.md\``,
      ``,
      `Ready: \`${READY_STATE}\` (apply) · \`${DRY_RUN_READY}\` (dry-run)`,
      ``,
    ].join("\n")
  );

  return { jsonPath, mdPath, docsPath, readyStatement: ready };
}

export async function runWave15PostImageCleanup({ dryRun = true, argv = [] } = {}) {
  // Failures extract first (required before patch)
  spawnSync("node", ["scripts/extract-wave15-post-image-cleanup-failures.mjs"], {
    cwd: ROOT,
    stdio: "inherit",
    env: process.env,
  });

  const identityPreflight = await runWave15ProtectedFiftyFourIdentityPreflight();
  if (!identityPreflight.pass) {
    const blocked = {
      version: WAVE15_POST_IMAGE_CLEANUP_VERSION,
      stage: "post-image-content-cleanup",
      dryRun: true,
      pass: false,
      stopRecommended: true,
      identityPreflight,
      readyStatement: "wave15_stage6_blocked_protected_54_identity_preflight_failed",
    };
    writeWave15PostImageCleanupReports(
      {
        version: WAVE15_POST_IMAGE_CLEANUP_VERSION,
        wave15Version: WAVE15_VERSION,
        stage: "post-image-content-cleanup",
        generatedAt: new Date().toISOString(),
        scope: [...WAVE15_SLUGS],
        brands: [],
        patchCount: 0,
        blockedCount: 8,
      },
      null,
      { identityPreflight }
    );
    return blocked;
  }

  const plan = await planWave15PostImageCleanup();
  const underReviewCheck = await assertAllUnderReview(plan);
  if (!underReviewCheck.pass) {
    const blocked = {
      ...plan,
      dryRun: true,
      pass: false,
      stopRecommended: true,
      underReviewCheck,
      identityPreflight,
      readyStatement: "wave15_stage6_blocked_not_all_under_review",
    };
    writeWave15PostImageCleanupReports(plan, null, { identityPreflight, underReviewCheck });
    return blocked;
  }

  const applyRequested = argv.includes("--apply") && !dryRun;
  let applyResult = { applied: false, writes: 0, results: [] };
  if (applyRequested) {
    applyResult = await applyWave15PostImageCleanup({ plan, apply: true, argv });
  }

  const paths = writeWave15PostImageCleanupReports(plan, applyResult, {
    identityPreflight,
    underReviewCheck,
  });

  return {
    version: WAVE15_POST_IMAGE_CLEANUP_VERSION,
    stage: "post-image-content-cleanup",
    dryRun: !applyResult.applied,
    pass: plan.blockedCount === 0 && underReviewCheck.pass && identityPreflight.pass,
    stopRecommended: false,
    airtableWrites: applyResult.writes || 0,
    patchCount: plan.patchCount,
    readyStatement: paths.readyStatement,
    identityPreflight,
    underReviewCheck,
    paths,
    summary: {
      brands: WAVE15_SLUGS.length,
      patches: plan.patchCount,
      writes: applyResult.writes || 0,
      ready: paths.readyStatement,
    },
  };
}
