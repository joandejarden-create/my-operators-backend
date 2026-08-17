/**
 * Active-universe PVQL scrub — 16 public_full_failing_pvql brands only.
 *
 * Patches exact owner-facing Presentation fields (Title/Body/Case Summary/tags).
 * Preserves Recent Momentum / openings trailing announcement URLs.
 * Never writes CV / Source / Registry / Brand Status / release / images.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  planPvqlFailureScrubForBrand,
  extractPvqlFieldOffenders,
} from "./brand-explorer-pvql-failure-scrub.js";
import {
  isOwnerFacingPresentationRow,
  evaluateBrandPublicVisibility,
} from "./brand-explorer-public-visibility-quality-lock.js";
import {
  MAP_PRESENTATION_FIELDS,
  PRESENTATION_TABLE,
} from "./brand-explorer-residual-owner-copy-remediation.js";
import { resolveSectionPatternBrandIdentity } from "./brand-explorer-section-pattern-parity.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const ACTIVE_UNIVERSE_PVQL_SCRUB_VERSION = "active-universe-pvql-scrub-v1";

export const ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS = Object.freeze([
  "ascend",
  "autograph-collection",
  "comfort-inn-suites",
  "country-inn-suites",
  "curio-collection",
  "design-hotels",
  "handwritten-collection",
  "hotel-indigo",
  "kimpton",
  "mgallery-collection",
  "radisson-individuals-by-choice",
  "small-luxury-hotels-of-the-world",
  "suburban-studios",
  "tribute-portfolio",
  "vignette-collection",
  "woodspring-suites",
]);

/** Explicitly out of scope for this scrub. */
export const ACTIVE_UNIVERSE_PVQL_SCRUB_EXCLUDED = Object.freeze([
  "everhome-suites",
  "quality-inn",
  "radisson",
  "radisson-blu",
  "radisson-red",
  "bw-premier-collection",
  "bw-signature-collection",
  "preferred-hotels-and-resorts",
  "radisson-collection",
  "tapestry-collection-by-hilton",
]);

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-active-universe-pvql-scrub",
  "--confirm-visible-owner-facing-fields-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-public-restore-changes",
  "--confirm-no-image-writes",
  "--confirm-targeted-pvql-fields-only",
  "--confirm-no-raw-urls",
  "--confirm-no-forbidden-owner-facing-language",
]);

const ALLOWED_AIRTABLE_FIELDS = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Owner Objective",
  "Case Summary Interpretation",
  "Case Summary Tags",
]);

const FORBIDDEN_AIRTABLE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Source Library Status",
  "Registry Status",
  "Brand Status",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Image URL",
  "Image",
  "Primary Image",
]);

const AIRTABLE_TO_API = Object.fromEntries(
  Object.entries(MAP_PRESENTATION_FIELDS).map(([api, at]) => [at, api])
);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

export const REPORT_JSON = "brand-explorer-active-universe-pvql-scrub.json";
export const REPORT_MD = "brand-explorer-active-universe-pvql-scrub.md";
export const DOC_MD = "brand-explorer-active-universe-pvql-scrub.md";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parseActiveUniversePvqlScrubFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function normalizeBrandArg(raw) {
  const s = nz(raw).toLowerCase();
  if (s === "curio") return "curio-collection";
  return s;
}

async function fetchBrandApi(slug) {
  const identity = resolveSectionPatternBrandIdentity(slug);
  const lookupId = identity.recordId || slug;
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
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug}`);
  return res.payload.brand;
}

function projectPatches(blocks, patches) {
  return (blocks || []).map((b) => {
    const next = { ...b };
    for (const p of patches || []) {
      if (p.recordId !== b.recordId) continue;
      for (const [airtableKey, value] of Object.entries(p.fields || {})) {
        const apiKey = AIRTABLE_TO_API[airtableKey];
        if (apiKey) next[apiKey] = value;
      }
    }
    return next;
  });
}

function assertPatchAllowed(patch) {
  if (patch.table !== PRESENTATION_TABLE) {
    throw new Error(`Refuse non-Presentation table: ${patch.table}`);
  }
  if (!patch.recordId) throw new Error(`Missing recordId for ${patch.slotKey}`);
  for (const key of Object.keys(patch.fields || {})) {
    if (FORBIDDEN_AIRTABLE_FIELDS.has(key)) {
      throw new Error(`Refuse forbidden field write: ${key}`);
    }
    if (!ALLOWED_AIRTABLE_FIELDS.has(key)) {
      throw new Error(`Refuse non-allowed Presentation field: ${key}`);
    }
  }
}

async function airtablePatch({ baseId, apiKey, table, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json.error?.message || `PATCH failed ${recordId}: ${res.status}`);
  }
  return json;
}

/**
 * @param {{ brands?: string[] }} [opts]
 */
export async function planActiveUniversePvqlScrub(opts = {}) {
  const list = (opts.brands?.length ? opts.brands : [...ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS])
    .map(normalizeBrandArg)
    .filter(Boolean);

  for (const slug of list) {
    if (ACTIVE_UNIVERSE_PVQL_SCRUB_EXCLUDED.includes(slug)) {
      throw new Error(`Refuse scrub of out-of-scope brand: ${slug}`);
    }
    if (!ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS.includes(slug)) {
      throw new Error(`Brand not in active-universe PVQL scrub target set: ${slug}`);
    }
  }

  const brandPlans = [];
  for (const slug of list) {
    console.log(`[${ACTIVE_UNIVERSE_PVQL_SCRUB_VERSION}] planning ${slug}`);
    const brand = await fetchBrandApi(slug);
    if (brand.shouldRenderFullProfile !== true) {
      console.warn(`  warn: ${slug} shouldRenderFullProfile!=true — still scrubbing flagged owner-facing fields only`);
    }
    const plan = planPvqlFailureScrubForBrand(brand, slug, { force: true });
    // Drop any accidental non-allowed fields
    const patches = (plan.patches || []).filter((p) => {
      try {
        assertPatchAllowed(p);
        return true;
      } catch (err) {
        console.warn(`  skip patch: ${err.message}`);
        return false;
      }
    });
    const projectedBlocks = projectPatches(brand.brandExplorer?.blocks || [], patches);
    const projectedBrand = {
      ...brand,
      brandExplorer: { ...(brand.brandExplorer || {}), blocks: projectedBlocks },
    };
    const remaining = extractPvqlFieldOffenders(projectedBrand, slug);
    brandPlans.push({
      ...plan,
      patches,
      patchCount: patches.length,
      remainingAfterProjection: remaining.length,
      remainingSample: remaining.slice(0, 5).map((r) => ({
        section: r.section,
        field: r.field,
        failureType: r.failureType,
      })),
      publicFull: brand.shouldRenderFullProfile === true,
      recordId: brand.id,
      brandName: brand.name,
    });
    console.log(
      `  offenders=${plan.offenderCount} patches=${patches.length} remaining=${remaining.length}`
    );
  }

  const unclean = brandPlans.filter((b) => b.remainingAfterProjection > 0);
  return {
    version: ACTIVE_UNIVERSE_PVQL_SCRUB_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    targets: list,
    excluded: [...ACTIVE_UNIVERSE_PVQL_SCRUB_EXCLUDED],
    brands: brandPlans,
    summary: {
      brands: brandPlans.length,
      offenders: brandPlans.reduce((n, b) => n + (b.offenderCount || 0), 0),
      patches: brandPlans.reduce((n, b) => n + (b.patchCount || 0), 0),
      fieldRows: brandPlans.reduce((n, b) => n + (b.fieldRows?.length || 0), 0),
      uncleanAfterProjection: unclean.map((b) => b.brandSlug),
    },
    validation: {
      pass: unclean.length === 0,
      failedChecks: unclean.map(
        (b) => `remaining_offenders:${b.brandSlug}:${b.remainingAfterProjection}`
      ),
    },
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      releaseFieldsUntouched: true,
      publicRestoreUntouched: true,
      imageFieldsUntouched: true,
      visibleOwnerFacingFieldsOnly: true,
      targetedPvqlFieldsOnly: true,
      recentMomentumAnnouncementUrlsPreserved: true,
      everhomeUntouched: true,
      restoredPendingUntouched: true,
      unconfiguredUntouched: true,
    },
    allowedWrites: [...ALLOWED_AIRTABLE_FIELDS],
    forbiddenWrites: [...FORBIDDEN_AIRTABLE_FIELDS],
  };
}

export async function applyActiveUniversePvqlScrub({
  report,
  apply = false,
  argv = [],
} = {}) {
  const flags = parseActiveUniversePvqlScrubFlags(argv);
  if (!apply) {
    return { applied: false, reason: "dry_run_only", flags, results: [] };
  }
  if (!flags.ok) {
    return {
      applied: false,
      reason: "missing_apply_flags",
      missing: flags.missing,
      flags,
      results: [],
    };
  }
  if (!report?.validation?.pass) {
    return {
      applied: false,
      reason: "validation_failed",
      failedChecks: report?.validation?.failedChecks || [],
      results: [],
    };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const results = [];
  for (const brand of report.brands || []) {
    if (ACTIVE_UNIVERSE_PVQL_SCRUB_EXCLUDED.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to excluded brand ${brand.brandSlug}`);
    }
    if (!ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to non-target brand ${brand.brandSlug}`);
    }
    for (const patch of brand.patches || []) {
      assertPatchAllowed(patch);
      const json = await airtablePatch({
        baseId,
        apiKey,
        table: patch.table,
        recordId: patch.recordId,
        fields: patch.fields,
      });
      results.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        fields: Object.keys(patch.fields),
        action: "PATCH",
        id: json.id,
      });
    }
  }

  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsUntouched: true,
    publicRestoreUntouched: true,
    imageFieldsUntouched: true,
  };
}

/**
 * Spot-check forbidden/raw_url gates for the 16 targets after scrub.
 * Full public-full-only suite remains the acceptance command.
 */
export async function validateActiveUniversePvqlScrubTargets(slugs = ACTIVE_UNIVERSE_PVQL_SCRUB_TARGETS) {
  const rows = [];
  for (const slug of slugs) {
    const row = await evaluateBrandPublicVisibility(slug);
    const failures = row.failures || [];
    const copyFails = failures.filter((f) =>
      /raw_url|forbidden_owner_facing|generic_copy/i.test(f)
    );
    rows.push({
      slug,
      publicFullProfile: row.publicFullProfile === true,
      lockPass: row.lockPass === true,
      failures,
      copyGateFails: copyFails,
      copyGatesClean: copyFails.length === 0,
    });
  }
  return {
    brands: rows,
    publicFullCount: rows.filter((r) => r.publicFullProfile).length,
    allCopyGatesClean: rows.every((r) => r.copyGatesClean),
    allLockPass: rows.every((r) => r.lockPass),
  };
}

function mdEsc(s) {
  return nz(s).replace(/\|/g, "\\|");
}

export function writeActiveUniversePvqlScrubReports(report, applyResult = null) {
  const reportsDir = path.join(ROOT, "reports");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const out = {
    ...report,
    applyResult: applyResult || { applied: false, reason: "dry_run_only" },
  };

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const docPath = path.join(docsDir, DOC_MD);

  fs.writeFileSync(jsonPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");

  const lines = [
    "# Active Universe PVQL Scrub",
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Applied: **${applyResult?.applied === true}**`,
    "",
    "## Targets (16 public-full)",
    "",
    ...(report.targets || []).map((s) => `- \`${s}\``),
    "",
    "## Summary",
    "",
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Brands | ${report.summary.brands} |`,
    `| Offenders | ${report.summary.offenders} |`,
    `| Patches | ${report.summary.patches} |`,
    `| Unclean after projection | ${(report.summary.uncleanAfterProjection || []).length} |`,
    "",
    "## Field patches",
    "",
    "| Brand | Section | Record ID | Field | Failure | Before (trim) | After (trim) |",
    "| --- | --- | --- | --- | --- | --- | --- |",
  ];
  for (const b of report.brands || []) {
    for (const row of b.fieldRows || []) {
      if (!(b.patches || []).some((p) => p.recordId === row.recordId && p.fields?.[row.field])) {
        continue;
      }
      lines.push(
        `| \`${b.brandSlug}\` | \`${mdEsc(row.section)}\` | \`${row.recordId || "—"}\` | ${mdEsc(row.field)} | ${mdEsc(row.failureType)} | ${mdEsc((row.currentValue || "").slice(0, 70))} | ${mdEsc((row.proposedFix || "").slice(0, 70))} |`
      );
    }
  }
  lines.push(
    "",
    "## Guardrails",
    "",
    "```json",
    JSON.stringify(report.guardrails, null, 2),
    "```",
    "",
    "## Apply result",
    "",
    "```json",
    JSON.stringify(applyResult || { applied: false }, null, 2),
    "```",
    ""
  );
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  const doc = [
    "# Active Universe PVQL Scrub",
    "",
    "Targeted owner-facing Presentation scrub for the **16** `public_full_failing_pvql` brands in the Active/Live universe.",
    "",
    "## Scope",
    "",
    "- Only the 16 public-full targets",
    "- Presentation Title / Body / Case Summary / tags only",
    "- Preserves Recent Momentum & openings trailing announcement URLs",
    "",
    "## Out of scope",
    "",
    "- Everhome",
    "- restored_pending_validation (Quality / Radisson / Blu / RED)",
    "- active_but_unconfigured (BW Premier / Signature / Preferred)",
    "- Draft / Under Review (Radisson Collection / Tapestry)",
    "",
    "## Run",
    "",
    "```bash",
    "npm run brand-explorer-active-universe-pvql-scrub -- --dry-run",
    "npm run brand-explorer-active-universe-pvql-scrub -- --apply \\",
    "  --approve-active-universe-pvql-scrub \\",
    "  --confirm-visible-owner-facing-fields-only \\",
    "  --confirm-no-company-validation-changes \\",
    "  --confirm-no-source-library-status-changes \\",
    "  --confirm-no-registry-approval-changes \\",
    "  --confirm-no-brand-status-changes \\",
    "  --confirm-no-release-field-changes \\",
    "  --confirm-no-public-restore-changes \\",
    "  --confirm-no-image-writes \\",
    "  --confirm-targeted-pvql-fields-only \\",
    "  --confirm-no-raw-urls \\",
    "  --confirm-no-forbidden-owner-facing-language",
    "```",
    "",
    "## Validation",
    "",
    "```bash",
    "npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only",
    "```",
    "",
    `Latest: see \`reports/${REPORT_JSON}\``,
    "",
  ].join("\n");
  fs.writeFileSync(docPath, `${doc}\n`, "utf8");

  return { jsonPath, mdPath, docPath };
}

export { isOwnerFacingPresentationRow };
