/**
 * Active + future scenario owner-value mechanical remediation (Title/Body).
 *
 * Gold bar: Kimpton / Curio / Design Hotels.
 * - Proper Case titles
 * - Strip identical Wave 12 diligence closer
 * - Does NOT invent full owner-value essays (editorial packages required)
 * - Does NOT rewrite images (use image rematerialization / Wave 12 scenario remediation)
 *
 * Allowed writes: Presentation Title / Body on overview.scenario.1–3 only.
 *
 *   node scripts/brand-explorer-scenario-owner-value-mechanical-remediation.mjs --dry-run
 *   node scripts/brand-explorer-scenario-owner-value-mechanical-remediation.mjs --apply [flags…]
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import {
  evaluateScenarioOwnerValueBar,
  toProperCaseScenarioTitle,
  stripRepeatedScenarioDiligencePad,
  hasRepeatedDiligenceCloser,
  SCENARIO_SLOTS,
} from "../lib/partner-intelligence/brand-explorer-scenario-owner-value-bar.js";

export const MECHANICAL_SCENARIO_REMEDIATION_VERSION = "scenario-owner-value-mechanical-v1";

export const MECHANICAL_APPLY_FLAGS = Object.freeze([
  "--approve-scenario-owner-value-mechanical-remediation",
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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function checkFlags(argv, apply) {
  const missing = MECHANICAL_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return { apply: apply === true, ok: apply === true && missing.length === 0, missing };
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

async function planBrand(brand) {
  const fetch = await listPresentationRowsLight(brand.recordId, brand.name);
  const live = liveScenarioRows(fetch.rows || []);
  const before = evaluateScenarioOwnerValueBar(fetch.rows || [], { brandSlug: brand.slug });
  const patches = [];

  for (const row of live) {
    if (!row?.recordId) continue;
    const fields = {};
    const reasons = [];
    const title = nz(row.title);
    const body = nz(row.body);
    const nextTitle = toProperCaseScenarioTitle(title);
    if (title && nextTitle && title !== nextTitle) {
      fields.Title = nextTitle;
      reasons.push("proper_case_title");
    }
    if (hasRepeatedDiligenceCloser(body)) {
      const stripped = stripRepeatedScenarioDiligencePad(body);
      if (stripped && stripped !== body) {
        fields.Body = stripped;
        reasons.push("strip_repeated_diligence_closer");
      }
    }
    if (Object.keys(fields).length) {
      patches.push({
        brandSlug: brand.slug,
        brandName: brand.name,
        recordId: row.recordId,
        slotKey: row.slotKey,
        fields,
        reasons,
        before: { title, body: body.slice(0, 160) },
        after: {
          title: fields.Title || title,
          body: (fields.Body || body).slice(0, 160),
        },
      });
    }
  }

  return {
    ...brand,
    beforePass: before.pass,
    beforeFailures: before.failures,
    patches,
    remainingAfterMechanical: before.failures.filter(
      (f) =>
        !String(f).startsWith("sentence_case_title_") &&
        !String(f).startsWith("repeated_diligence_closer_")
    ),
  };
}

async function applyPatches(patches, { apply, argv }) {
  const flagCheck = checkFlags(argv, apply);
  if (!apply) {
    return { applied: 0, skipped: patches.length, flagCheck, results: [] };
  }
  if (!flagCheck.ok) {
    return { applied: 0, skipped: patches.length, flagCheck, error: "missing_apply_flags", results: [] };
  }

  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    return { applied: 0, skipped: patches.length, flagCheck, error: "missing_airtable_env", results: [] };
  }

  const results = [];
  for (const patch of patches) {
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${patch.recordId}`;
    try {
      const res = await fetch(url, {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: patch.fields }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error?.message || `PATCH ${res.status}`);
      results.push({ recordId: patch.recordId, slotKey: patch.slotKey, ok: true });
    } catch (err) {
      results.push({ recordId: patch.recordId, slotKey: patch.slotKey, ok: false, error: err.message });
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

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply") && !argv.includes("--dry-run");
  const brandIdx = argv.indexOf("--brands");
  const brandFilter =
    brandIdx >= 0 && argv[brandIdx + 1]
      ? argv[brandIdx + 1].split(",").map((s) => s.trim().toLowerCase()).filter(Boolean)
      : null;

  const universe = await loadActiveUniverse({ includeDetails: false });
  const brands = [];
  const seen = new Set();
  for (const b of universe.brands) {
    if (brandFilter && !brandFilter.includes(b.slug)) continue;
    seen.add(b.slug);
    brands.push({ slug: b.slug, name: b.name || b.brandName, recordId: b.recordId, cohort: "active" });
  }
  for (const slug of WAVE12_SLUGS) {
    if (brandFilter && !brandFilter.includes(slug)) continue;
    if (seen.has(slug)) continue;
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) continue;
    brands.push({ slug, name: id.name, recordId: id.recordId, cohort: "future_wave12" });
  }

  const plans = [];
  for (const brand of brands) {
    try {
      plans.push(await planBrand(brand));
      process.stdout.write(".");
    } catch (err) {
      plans.push({ ...brand, beforePass: false, beforeFailures: [`fetch_error:${err.message}`], patches: [] });
      process.stdout.write("E");
    }
  }
  process.stdout.write("\n");

  const allPatches = plans.flatMap((p) => p.patches || []);
  const applyResult = await applyPatches(allPatches, { apply, argv });

  const summary = {
    version: MECHANICAL_SCENARIO_REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: argv.includes("--apply"),
    goldReferences: ["Kimpton Hotels", "Curio Collection by Hilton", "Design Hotels"],
    counts: {
      brands: plans.length,
      brandsNeedingMechanical: plans.filter((p) => p.patches?.length).length,
      patches: allPatches.length,
      stillNeedEditorial: plans.filter((p) => (p.remainingAfterMechanical || []).length).length,
    },
    applyResult,
    brands: plans.map((p) => ({
      slug: p.slug,
      name: p.name,
      cohort: p.cohort,
      beforePass: p.beforePass,
      patchCount: p.patches?.length || 0,
      remainingAfterMechanical: p.remainingAfterMechanical || [],
      reasons: [...new Set((p.patches || []).flatMap((x) => x.reasons || []))],
    })),
    patches: allPatches,
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, "brand-explorer-scenario-owner-value-mechanical-remediation.json");
  const mdPath = path.join(REPORTS, "brand-explorer-scenario-owner-value-mechanical-remediation.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  fs.writeFileSync(
    mdPath,
    [
      `# Scenario Owner-Value Mechanical Remediation`,
      ``,
      `- Mode: **${apply ? "APPLY" : "DRY-RUN"}**`,
      `- Brands: ${summary.counts.brands} · Mechanical patches: **${summary.counts.patches}**`,
      `- Still need editorial / images after mechanical: **${summary.counts.stillNeedEditorial}**`,
      ``,
      `Mechanical only: Proper Case titles + strip repeated diligence closer.`,
      `Full Kimpton-quality body/image rewrites use Wave 12 scenario remediation or brand content packages.`,
      ``,
      `## Brands with mechanical patches`,
      ``,
      ...plans
        .filter((p) => p.patches?.length)
        .map(
          (p) =>
            `- **${p.name}** (\`${p.slug}\`): ${p.patches.length} patch(es) — ${(p.patches || []).flatMap((x) => x.reasons).join(", ")}`
        ),
      ``,
    ].join("\n"),
    "utf8"
  );

  console.log(`Wrote ${mdPath}`);
  console.log(
    `patches=${allPatches.length} stillEditorial=${summary.counts.stillNeedEditorial} apply=${apply}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
