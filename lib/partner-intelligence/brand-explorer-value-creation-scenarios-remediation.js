/**
 * Value Creation Scenarios remediation — Title/Body on valueOwners.scenario.1–4.
 *
 * Gold bar: Ascend — 4 Proper Case titles + short paragraphs (~26–58 words).
 * Creates missing rows; patches thin/blank/long/wrong titles.
 *
 * Forbidden: Brand Status, CV, Source Library, Registry, release, Image.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import {
  evaluateValueCreationScenariosBar,
  toProperCaseValueCreationTitle,
  words,
  VALUE_CREATION_SCENARIO_SLOTS,
  VALUE_CREATION_MIN_BODY_WORDS,
  VALUE_CREATION_MAX_BODY_WORDS,
} from "./brand-explorer-value-creation-scenarios-bar.js";
import {
  VALUE_CREATION_SCENARIOS_PACKAGES_VERSION,
  getValueCreationScenarioPackage,
  VALUE_CREATION_SCENARIO_PACKAGES,
} from "./brand-explorer-value-creation-scenarios-packages.js";

export const VALUE_CREATION_SCENARIOS_REMEDIATION_VERSION =
  "value-creation-scenarios-remediation-v1";

export const VALUE_CREATION_SCENARIOS_APPLY_FLAGS = Object.freeze([
  "--approve-value-creation-scenarios-remediation",
  "--confirm-value-owners-scenario-slots-only",
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
  const missing = VALUE_CREATION_SCENARIOS_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...VALUE_CREATION_SCENARIOS_APPLY_FLAGS],
  };
}

function liveScenarioRows(rows) {
  return VALUE_CREATION_SCENARIO_SLOTS.map((slotKey) => {
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

function bodyNeedsRewrite(liveBody, nextBody) {
  const live = nz(liveBody);
  const next = nz(nextBody);
  if (!live) return true;
  if (live !== next) {
    const wc = words(live);
    if (wc < VALUE_CREATION_MIN_BODY_WORDS || wc > VALUE_CREATION_MAX_BODY_WORDS) return true;
    // Always align to package when live differs (standardize Active universe)
    return true;
  }
  return false;
}

export async function resolveValueCreationTargetBrands({ brands = null } = {}) {
  const packageSlugs = Object.keys(VALUE_CREATION_SCENARIO_PACKAGES);
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
    if (!VALUE_CREATION_SCENARIO_PACKAGES[slug]) {
      missing.push({ slug, reason: "no_package" });
      continue;
    }
    const live = bySlug.get(slug);
    if (!live?.recordId) {
      missing.push({ slug, reason: "not_in_active_universe" });
      continue;
    }
    const pkg = getValueCreationScenarioPackage(slug);
    targets.push({
      slug,
      name: live.name || live.brandName || pkg.brandName,
      recordId: live.recordId,
      brandName: pkg.brandName,
    });
  }
  return { targets, missing, packageSlugs, filter };
}

export async function planValueCreationScenariosRemediationForBrand(brand) {
  const pkg = getValueCreationScenarioPackage(brand.slug);
  if (!pkg) {
    return {
      brandSlug: brand.slug,
      blocked: true,
      blockers: ["no_package"],
      patches: [],
    };
  }

  const fetch = await listPresentationRowsLight(brand.recordId, brand.name);
  const liveList = liveScenarioRows(fetch.rows || []);
  const before = evaluateValueCreationScenariosBar(fetch.rows || [], {
    brandSlug: brand.slug,
    brandName: brand.name,
  });

  const patches = [];
  const blockers = [];

  for (let i = 0; i < VALUE_CREATION_SCENARIO_SLOTS.length; i++) {
    const slotKey = VALUE_CREATION_SCENARIO_SLOTS[i];
    const live = liveList[i];
    const next = pkg.scenarios[i];
    if (!next?.title || !next?.body) {
      blockers.push(`missing_package_scenario:${slotKey}`);
      continue;
    }

    const nextTitle = toProperCaseValueCreationTitle(nz(next.title));
    const nextBody = nz(next.body);
    const liveTitle = nz(live?.title);
    const liveBody = nz(live?.body);

    const fields = {};
    const reasons = [];
    if (!liveTitle || liveTitle !== nextTitle) {
      fields.Title = nextTitle;
      reasons.push(liveTitle ? "title_refresh" : "title_set");
    }
    if (bodyNeedsRewrite(liveBody, nextBody)) {
      fields.Body = nextBody;
      reasons.push(
        !liveBody
          ? "body_set"
          : words(liveBody) > VALUE_CREATION_MAX_BODY_WORDS
            ? "body_shorten"
            : words(liveBody) < VALUE_CREATION_MIN_BODY_WORDS
              ? "body_thicken"
              : "body_standardize"
      );
    }

    const safe = sanitizeTitleBodyFields(fields);
    if (!Object.keys(safe).length) continue;

    if (live?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey,
        fields: safe,
        reasons,
        before: { title: liveTitle, body: liveBody.slice(0, 160) },
        after: {
          title: safe.Title || liveTitle,
          body: (safe.Body || liveBody).slice(0, 160),
        },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        fields: {
          "Slot Key": slotKey,
          "Brand Name": brand.name,
          Brand: [brand.recordId],
          Active: true,
          "Sort Order": 210 + i,
          Title: nextTitle,
          Body: nextBody,
        },
        reasons: ["create_missing_scenario"],
        before: null,
        after: { title: nextTitle, body: nextBody.slice(0, 160) },
      });
    }
  }

  return {
    brandSlug: brand.slug,
    brandName: brand.name,
    recordId: brand.recordId,
    blocked: false,
    blockers,
    beforePass: before.pass,
    beforeFailures: before.failures,
    patches,
    plannedWrites: patches.length,
    brandStatusUntouched: true,
    companyValidatedUntouched: true,
    imageWrites: false,
    titleBodyOnly: true,
  };
}

async function airtableWrite({ baseId, apiKey, method, recordId, fields }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`${method} ${recordId || "new"}: ${res.status} ${JSON.stringify(json)}`);
  }
  return json;
}

export async function runValueCreationScenariosRemediation({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const flagCheck = checkFlags(argv, !dryRun);
  const { targets, missing } = await resolveValueCreationTargetBrands({ brands });

  const brandResults = [];
  for (const brand of targets) {
    brandResults.push(await planValueCreationScenariosRemediationForBrand(brand));
  }

  const applyResults = {};
  if (!dryRun && flagCheck.ok) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

    for (const plan of brandResults) {
      const wrote = [];
      const errors = [];
      for (const patch of plan.patches || []) {
        try {
          if (patch.action === "PATCH" && patch.recordId) {
            await airtableWrite({
              baseId,
              apiKey,
              method: "PATCH",
              recordId: patch.recordId,
              fields: sanitizeTitleBodyFields(patch.fields),
            });
            wrote.push({ slotKey: patch.slotKey, action: "PATCH", recordId: patch.recordId });
          } else if (patch.action === "POST") {
            const created = await airtableWrite({
              baseId,
              apiKey,
              method: "POST",
              fields: patch.fields,
            });
            wrote.push({
              slotKey: patch.slotKey,
              action: "POST",
              recordId: created.id || null,
            });
          }
          await sleep(WRITE_THROTTLE_MS);
        } catch (err) {
          errors.push({ slotKey: patch.slotKey, message: err.message });
        }
      }
      applyResults[plan.brandSlug] = {
        applied: errors.length === 0,
        wrote,
        errors,
      };
    }
  }

  const report = {
    version: VALUE_CREATION_SCENARIOS_REMEDIATION_VERSION,
    packagesVersion: VALUE_CREATION_SCENARIOS_PACKAGES_VERSION,
    dryRun: dryRun !== false,
    flagCheck,
    missing,
    brandResults,
    applyResults,
    summary: {
      brands: brandResults.length,
      plannedPatches: brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0),
      creates: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "POST").length,
        0
      ),
      patches: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "PATCH").length,
        0
      ),
      previouslyPassing: brandResults.filter((b) => b.beforePass).length,
    },
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-value-creation-scenarios-remediation.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-value-creation-scenarios-remediation.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Value Creation Scenarios Remediation`,
    ``,
    `- Version: ${report.version}`,
    `- Packages: ${report.packagesVersion}`,
    `- Dry-run: ${report.dryRun}`,
    `- Brands: ${report.summary.brands}`,
    `- Planned writes: ${report.summary.plannedPatches} (create ${report.summary.creates}, patch ${report.summary.patches})`,
    ``,
    ...brandResults.map((b) => {
      const lines = [
        `## ${b.brandName || b.brandSlug}`,
        `- Before pass: ${b.beforePass}`,
        `- Patches: ${b.patches?.length || 0}`,
      ];
      for (const p of b.patches || []) {
        lines.push(
          `- \`${p.slotKey}\` → ${p.action} · ${(p.reasons || []).join("+")} · “${p.after?.title || ""}”`
        );
      }
      return lines.join("\n");
    }),
  ].join("\n\n");
  fs.writeFileSync(mdPath, md);

  return {
    ...report,
    paths: { jsonPath, mdPath },
    summary: report.summary,
    flagCheck,
  };
}
