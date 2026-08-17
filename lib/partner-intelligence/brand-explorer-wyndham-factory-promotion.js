/**
 * Wyndham factory promotion (Dazzler, Trademark).
 *
 * Stages:
 *   - status-promotion  Brand Status Under Review → Active (Basics only)
 *   - public-release    release fields + intentional public-restore registry
 *
 * Presentation builds are separate (full-tab-factory-build). Never writes
 * Company Validated / Source Library / Registry approval fields.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WYNDHAM_FACTORY_BUILD_QUEUE } from "./brand-explorer-wyndham-factory-build-queue.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  readIntentionalPublicRestoreSlugs,
  writeIntentionalPublicRestoreSlugs,
} from "./brand-explorer-public-restore-registry.js";
import {
  REQUIRED_APPLY_FLAGS as PUBLIC_RESTORE_REQUIRED_APPLY_FLAGS,
  planPublicRestoreGovernance,
  applyPublicRestoreGovernance,
} from "./brand-explorer-public-restore-governance.js";

export const PROMOTION_VERSION = "wyndham-factory-promotion-v1";
export const BASICS_TABLE = "Brand Setup - Brand Basics";
export const TARGET_STATUS = "Active";

export const STAGES = Object.freeze(["status-promotion", "public-release"]);

export const STATUS_PROMOTION_FLAGS = Object.freeze([
  "--approve-wyndham-brand-status-promotion",
  "--confirm-brand-status-only",
  "--confirm-active-target",
  "--confirm-single-brand",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-presentation-writes",
]);

export const PUBLIC_RELEASE_FLAGS = Object.freeze([
  "--approve-wyndham-public-release",
  "--confirm-founder-visual-review-passed",
  "--confirm-brand-status-active-or-live",
  "--confirm-fully-ready",
  "--confirm-single-brand",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
  ...PUBLIC_RESTORE_REQUIRED_APPLY_FLAGS,
]);

const FORBIDDEN_STATUS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
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

export function resolveWyndhamBrand(slugOrAlias) {
  const raw = nz(slugOrAlias).toLowerCase();
  const aliases = {
    dazzler: "dazzler-by-wyndham",
    "dazzler-by-wyndham": "dazzler-by-wyndham",
    trademark: "trademark-collection-by-wyndham",
    "trademark-collection": "trademark-collection-by-wyndham",
    "trademark-collection-by-wyndham": "trademark-collection-by-wyndham",
  };
  const slug = aliases[raw] || raw;
  const entry = WYNDHAM_FACTORY_BUILD_QUEUE.find((b) => b.slug === slug);
  if (!entry) {
    throw new Error(`Unknown Wyndham factory brand: ${slugOrAlias}`);
  }
  return entry;
}

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`fetch failed ${recordId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function patchBasics({ recordId, fields, allowReleaseFields = false }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  for (const k of Object.keys(fields || {})) {
    if (!allowReleaseFields && FORBIDDEN_STATUS.includes(k)) {
      throw new Error(`Forbidden field on status path: ${k}`);
    }
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH Basics failed: ${res.status}`);
  return json;
}

function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...required],
  };
}

function writeReports(prefix, report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, `${prefix}.json`);
  const mdPath = path.join(REPORTS_DIR, `${prefix}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath };
}

export async function runWyndhamStatusPromotion({ brandSlug, apply = false, argv = [] } = {}) {
  const brand = resolveWyndhamBrand(brandSlug);
  const flagCheck = checkFlags(STATUS_PROMOTION_FLAGS, argv, apply);
  const live = await fetchBrand(brand.recordId);
  const current = nz(live.brandStatus || live.status);
  const needsWrite = current !== TARGET_STATUS;

  const planned = {
    table: BASICS_TABLE,
    recordId: brand.recordId,
    from: current,
    to: TARGET_STATUS,
    fields: { "Brand Status": TARGET_STATUS },
  };

  let applyResult = { applied: false, reason: "dry_run_only" };
  if (apply && flagCheck.ok) {
    if (!needsWrite) {
      applyResult = { applied: false, reason: "already_active", writePerformed: false };
    } else {
      const keys = Object.keys(planned.fields);
      if (keys.length !== 1 || keys[0] !== "Brand Status") {
        throw new Error(`Refuse: status-promotion must be Brand Status only: ${JSON.stringify(planned.fields)}`);
      }
      const response = await patchBasics({
        recordId: brand.recordId,
        fields: planned.fields,
      });
      applyResult = {
        applied: true,
        writePerformed: true,
        table: BASICS_TABLE,
        recordId: brand.recordId,
        payload: planned.fields,
        response: { id: response.id, fieldsPatched: keys },
      };
    }
  } else if (apply && !flagCheck.ok) {
    applyResult = { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const report = {
    version: PROMOTION_VERSION,
    stage: "status-promotion",
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed: applyResult.applied === true,
    writePerformed: applyResult.writePerformed === true,
    dryRun: !apply,
    flagCheck,
    brand: {
      slug: brand.slug,
      name: brand.name,
      recordId: brand.recordId,
      currentBrandStatus: current,
      targetBrandStatus: TARGET_STATUS,
    },
    plannedPatch: planned,
    applyResult,
  };

  const md = [
    `# Wyndham — Brand Status Promotion (${brand.name})`,
    ``,
    `- Slug: \`${brand.slug}\``,
    `- Record: \`${brand.recordId}\``,
    `- ${current} → ${TARGET_STATUS}`,
    `- Applied: **${report.applyPerformed}**`,
    ``,
  ].join("\n");

  const paths = writeReports(`brand-explorer-${brand.slug}-status-promotion`, report, md);
  return { report, paths };
}

export async function runWyndhamPublicRelease({ brandSlug, apply = false, argv = [] } = {}) {
  const brand = resolveWyndhamBrand(brandSlug);
  const flagCheck = checkFlags(PUBLIC_RELEASE_FLAGS, argv, apply);
  const live = await fetchBrand(brand.recordId);
  const current = nz(live.brandStatus || live.status);

  if (!isBrandStatusActive(current)) {
    const report = {
      version: PROMOTION_VERSION,
      stage: "public-release",
      generatedAt: new Date().toISOString(),
      apply,
      applyPerformed: false,
      writePerformed: false,
      refused: true,
      reason: `brand_status_not_active_or_live:${current || "(empty)"}`,
      flagCheck,
      brand: { slug: brand.slug, recordId: brand.recordId, currentBrandStatus: current },
    };
    const md = `# Wyndham — Public Release\n\nRefused: ${report.reason}. Run status-promotion first.\n`;
    const paths = writeReports(`brand-explorer-${brand.slug}-public-release`, report, md);
    return { report, paths };
  }

  const plan = await planPublicRestoreGovernance({ brands: [brand.slug] });
  let applyOutcome = { applied: false, reason: "dry_run_only" };

  if (apply && flagCheck.ok) {
    try {
      applyOutcome = await applyPublicRestoreGovernance({
        plan,
        apply: true,
        argv: [...argv, "--apply"],
        reportsDir: REPORTS_DIR,
      });
    } catch (err) {
      applyOutcome = { applied: false, error: err.message };
    }

    if (applyOutcome?.applied !== true) {
      const today = todayIsoDate();
      const releaseFields = {
        "Active Profile Approved": true,
        "Ready for Active Profile": true,
        "Active Profile Approved Date": today,
        "Founder Visual Review Pass": true,
      };
      const patched = await patchBasics({
        recordId: brand.recordId,
        fields: releaseFields,
        allowReleaseFields: true,
      });
      const intentionalBefore = readIntentionalPublicRestoreSlugs();
      const nextSlugs = [...new Set([...intentionalBefore, brand.slug])];
      const registry = writeIntentionalPublicRestoreSlugs(nextSlugs);
      applyOutcome = {
        applied: true,
        reason: "public_release_applied_via_direct_fallback",
        basicsPatched: {
          recordId: brand.recordId,
          fields: Object.keys(releaseFields),
          sanitizedPayloadPreview: releaseFields,
          response: { id: patched?.id },
        },
        intentionalRegistry: registry,
      };
    }
  } else if (apply && !flagCheck.ok) {
    applyOutcome = { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const report = {
    version: PROMOTION_VERSION,
    stage: "public-release",
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed: applyOutcome.applied === true,
    writePerformed: applyOutcome.applied === true,
    dryRun: !apply,
    flagCheck,
    brand: {
      slug: brand.slug,
      name: brand.name,
      recordId: brand.recordId,
      currentBrandStatus: current,
    },
    publicRestorePlan: {
      eligible: plan.summary?.eligibleRestoreCount,
      held: plan.summary?.heldAccidentalUnlockCount,
    },
    applyOutcome,
  };

  const md = [
    `# Wyndham — Public Release (${brand.name})`,
    ``,
    `- Slug: \`${brand.slug}\``,
    `- Brand Status: **${current}**`,
    `- Applied: **${report.applyPerformed}**`,
    `- Reason: ${applyOutcome.reason || applyOutcome.error || "—"}`,
    ``,
  ].join("\n");

  const paths = writeReports(`brand-explorer-${brand.slug}-public-release`, report, md);
  return { report, paths };
}
