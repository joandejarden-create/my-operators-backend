/**
 * Editorial scenario owner-value remediation — Title/Body only on overview.scenario.1–3.
 *
 * Uses EDITORIAL_SCENARIO_PACKAGES. Does not write images, Brand Status, CV,
 * Source Library, Registry, or release fields.
 *
 * Apply flags (all required with --apply):
 *   --approve-scenario-owner-value-editorial-remediation
 *   --confirm-scenario-slots-only
 *   --confirm-title-body-only
 *   --confirm-no-image-writes
 *   --confirm-no-company-validation-changes
 *   --confirm-no-source-library-status-changes
 *   --confirm-no-registry-approval-changes
 *   --confirm-no-brand-status-changes
 *   --confirm-no-release-field-writes
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import {
  evaluateScenarioOwnerValueBar,
  toProperCaseScenarioTitle,
  hasOwnerValueCues,
  words,
  SCENARIO_SLOTS,
} from "./brand-explorer-scenario-owner-value-bar.js";
import {
  EDITORIAL_SCENARIO_PACKAGES_VERSION,
  EDITORIAL_SCENARIO_PACKAGES,
  getEditorialScenarioPackage,
  listEditorialScenarioPackageSlugs,
} from "./brand-explorer-scenario-owner-value-editorial-packages.js";

export const EDITORIAL_SCENARIO_REMEDIATION_VERSION =
  "scenario-owner-value-editorial-remediation-v1";

export const EDITORIAL_SCENARIO_APPLY_FLAGS = Object.freeze([
  "--approve-scenario-owner-value-editorial-remediation",
  "--confirm-scenario-slots-only",
  "--confirm-title-body-only",
  "--confirm-no-image-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 280;
const ALLOWED_FIELD_KEYS = Object.freeze(["Title", "Body"]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function checkFlags(argv, apply) {
  const missing = EDITORIAL_SCENARIO_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...EDITORIAL_SCENARIO_APPLY_FLAGS],
  };
}

function liveScenarioRows(rows) {
  return SCENARIO_SLOTS.map((slotKey) => {
    const matches = (rows || [])
      .filter(
        (r) =>
          nz(r.slotKey) === slotKey &&
          r.active !== false &&
          !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
      )
      .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
    return matches[0] || null;
  });
}

function sanitizeTitleBodyFields(fields) {
  const out = {};
  for (const key of ALLOWED_FIELD_KEYS) {
    if (fields[key] != null) out[key] = fields[key];
  }
  return out;
}

/**
 * Resolve package slugs to Active/Live brand identities via loadActiveUniverse.
 */
export async function resolveEditorialTargetBrands({ brands = null } = {}) {
  const packageSlugs = listEditorialScenarioPackageSlugs();
  const filter =
    brands?.length > 0
      ? brands.map((s) => String(s).trim().toLowerCase()).filter(Boolean)
      : packageSlugs;
  const universe = await loadActiveUniverse({ includeDetails: false });
  const bySlug = new Map(
    (universe.brands || []).map((b) => [nz(b.slug).toLowerCase(), b])
  );

  const targets = [];
  const missing = [];
  for (const slug of filter) {
    if (!EDITORIAL_SCENARIO_PACKAGES[slug]) {
      missing.push({ slug, reason: "no_editorial_package" });
      continue;
    }
    const live = bySlug.get(slug);
    if (!live?.recordId) {
      missing.push({ slug, reason: "not_in_active_universe" });
      continue;
    }
    const pkg = getEditorialScenarioPackage(slug);
    targets.push({
      slug,
      name: live.name || live.brandName || pkg.brandName,
      recordId: live.recordId,
      brandName: pkg.brandName,
      cohort: "active",
    });
  }
  return { targets, missing, packageSlugs, filter };
}

/**
 * Plan Title/Body patches for one brand from its editorial package.
 */
export async function planEditorialScenarioRemediationForBrand(brand) {
  const pkg = getEditorialScenarioPackage(brand.slug);
  if (!pkg) {
    return {
      brandSlug: brand.slug,
      brandName: brand.name,
      recordId: brand.recordId,
      blocked: true,
      blockers: ["no_editorial_package"],
      patches: [],
    };
  }

  const fetch = await listPresentationRowsLight(brand.recordId, brand.name);
  const liveList = liveScenarioRows(fetch.rows || []);
  const before = evaluateScenarioOwnerValueBar(fetch.rows || [], {
    brandSlug: brand.slug,
  });

  const patches = [];
  const blockers = [];

  for (let i = 0; i < SCENARIO_SLOTS.length; i++) {
    const slotKey = SCENARIO_SLOTS[i];
    const live = liveList[i];
    const next = pkg.scenarios[i];
    if (!next?.title || !next?.body) {
      blockers.push(`missing_package_scenario:${slotKey}`);
      continue;
    }

    const nextTitle = toProperCaseScenarioTitle(nz(next.title));
    const nextBody = nz(next.body);
    const liveTitle = nz(live?.title);
    const liveBody = nz(live?.body);

    const fields = sanitizeTitleBodyFields({});
    const reasons = [];
    if (!liveTitle || liveTitle !== nextTitle) {
      fields.Title = nextTitle;
      reasons.push(liveTitle ? "editorial_title_refresh" : "editorial_title_set");
    }
    if (!liveBody || liveBody !== nextBody) {
      fields.Body = nextBody;
      reasons.push(
        liveBody && !hasOwnerValueCues(liveBody)
          ? "editorial_owner_value_body"
          : "editorial_body_refresh"
      );
    }

    if (!Object.keys(fields).length) continue;

    if (!live?.recordId) {
      blockers.push(`missing_live_row:${slotKey}`);
      continue;
    }

    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      brandSlug: brand.slug,
      brandName: brand.name,
      recordId: live.recordId,
      slotKey,
      fields,
      reasons,
      packageWordCount: words(nextBody),
      packageHasOwnerValueCues: hasOwnerValueCues(nextBody),
      before: {
        title: liveTitle,
        body: liveBody.slice(0, 180),
      },
      after: {
        title: fields.Title || liveTitle,
        body: (fields.Body || liveBody).slice(0, 180),
      },
    });
  }

  return {
    brandSlug: brand.slug,
    brandName: brand.name,
    recordId: brand.recordId,
    blocked: blockers.length > 0 && patches.length === 0,
    blockers,
    beforePass: before.pass,
    beforeFailures: before.failures,
    patches,
    plannedWrites: patches.length,
    brandStatusUntouched: true,
    companyValidatedUntouched: true,
    imageWrites: false,
    titleBodyOnly: true,
    scenarioSlotsOnly: true,
  };
}

async function applyPatches(patches, { apply, argv }) {
  const flagCheck = checkFlags(argv, apply);
  if (!apply) {
    return { applied: 0, skipped: patches.length, flagCheck, results: [] };
  }
  if (!flagCheck.ok) {
    return {
      applied: 0,
      skipped: patches.length,
      flagCheck,
      error: "missing_apply_flags",
      results: [],
    };
  }

  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    return {
      applied: 0,
      skipped: patches.length,
      flagCheck,
      error: "missing_airtable_env",
      results: [],
    };
  }

  const results = [];
  for (const patch of patches) {
    const safeFields = sanitizeTitleBodyFields(patch.fields || {});
    if (!Object.keys(safeFields).length) {
      results.push({
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        ok: false,
        error: "no_title_body_fields",
      });
      continue;
    }
    if (safeFields.Image != null || Object.keys(patch.fields || {}).some((k) => !ALLOWED_FIELD_KEYS.includes(k))) {
      results.push({
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        ok: false,
        error: "forbidden_non_title_body_field",
      });
      continue;
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${patch.recordId}`;
    try {
      const res = await fetch(url, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: safeFields }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error?.message || `PATCH ${res.status}`);
      results.push({ recordId: patch.recordId, slotKey: patch.slotKey, ok: true });
    } catch (err) {
      results.push({
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        ok: false,
        error: err.message,
      });
    }
    await sleep(WRITE_THROTTLE_MS);
  }

  return {
    applied: results.filter((r) => r.ok).length,
    skipped: 0,
    flagCheck,
    results,
  };
}

function writeReports(summary) {
  if (!fs.existsSync(REPORTS_DIR)) fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-scenario-owner-value-editorial-remediation.json"
  );
  const mdPath = path.join(
    REPORTS_DIR,
    "brand-explorer-scenario-owner-value-editorial-remediation.md"
  );
  fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");

  const lines = [
    `# Scenario Owner-Value Editorial Remediation`,
    ``,
    `- Packages version: ${EDITORIAL_SCENARIO_PACKAGES_VERSION}`,
    `- Remediation version: ${EDITORIAL_SCENARIO_REMEDIATION_VERSION}`,
    `- Mode: **${summary.dryRun ? "DRY-RUN" : "APPLY"}**`,
    `- Brands planned: ${summary.counts.brands}`,
    `- Planned patches: **${summary.counts.patches}**`,
    `- Missing identities: ${summary.counts.missing}`,
    ``,
    `Title/Body only on \`overview.scenario.1–3\`. No image, Brand Status, CV, Source Library, Registry, or release writes.`,
    ``,
    `## Brands`,
    ``,
  ];
  for (const b of summary.brands || []) {
    lines.push(
      `- **${b.name || b.brandSlug}** (\`${b.brandSlug}\`): ${b.patchCount || 0} patch(es)` +
        (b.beforePass === false
          ? ` · before failures: ${(b.beforeFailures || []).join(", ") || "n/a"}`
          : "")
    );
    for (const p of b.patchPreview || []) {
      lines.push(`  - \`${p.slotKey}\` → ${p.reasons} · “${p.title}”`);
    }
  }
  if ((summary.missing || []).length) {
    lines.push(``, `## Missing / skipped`, ``);
    for (const m of summary.missing) {
      lines.push(`- \`${m.slug}\`: ${m.reason}`);
    }
  }
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}

/**
 * Plan (and optionally apply) editorial scenario Title/Body remediation.
 */
export async function runEditorialScenarioOwnerValueRemediation({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const apply = dryRun === false;
  const flagCheck = checkFlags(argv, apply);
  const { targets, missing } = await resolveEditorialTargetBrands({ brands });

  const brandResults = [];
  for (const brand of targets) {
    try {
      brandResults.push(await planEditorialScenarioRemediationForBrand(brand));
    } catch (err) {
      brandResults.push({
        brandSlug: brand.slug,
        brandName: brand.name,
        recordId: brand.recordId,
        blocked: true,
        blockers: [`fetch_error:${err.message}`],
        patches: [],
        beforePass: false,
        beforeFailures: [`fetch_error:${err.message}`],
      });
    }
  }

  const allPatches = brandResults.flatMap((b) => b.patches || []);
  const applyResult = await applyPatches(allPatches, { apply, argv });

  const summary = {
    version: EDITORIAL_SCENARIO_REMEDIATION_VERSION,
    packagesVersion: EDITORIAL_SCENARIO_PACKAGES_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: apply,
    goldReferences: ["Kimpton Hotels", "Curio Collection by Hilton", "Design Hotels"],
    flagCheck,
    counts: {
      brands: brandResults.length,
      patches: allPatches.length,
      brandsWithPatches: brandResults.filter((b) => b.patches?.length).length,
      missing: missing.length,
      applied: applyResult.applied || 0,
    },
    missing,
    applyResult,
    brands: brandResults.map((b) => ({
      brandSlug: b.brandSlug,
      name: b.brandName,
      recordId: b.recordId,
      beforePass: b.beforePass,
      beforeFailures: b.beforeFailures || [],
      patchCount: b.patches?.length || 0,
      blockers: b.blockers || [],
      patchPreview: (b.patches || []).map((p) => ({
        slotKey: p.slotKey,
        reasons: (p.reasons || []).join("+"),
        title: p.after?.title || "",
      })),
    })),
    patches: allPatches,
  };

  const paths = writeReports(summary);
  return { ...summary, paths };
}
