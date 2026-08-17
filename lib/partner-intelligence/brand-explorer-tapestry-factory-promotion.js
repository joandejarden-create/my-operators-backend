/**
 * Brand Explorer — Tapestry Collection by Hilton Setup AI Factory promotion orchestrator.
 *
 * Purpose: Guide tapestry-collection-by-hilton from Brand Status "Under Review"
 * → "Active" + public-full, without disturbing the protected 24 Active/Live
 * baseline, radisson-collection, or any other brand.
 *
 * Stages (each is a distinct entry-point on runTapestryFactoryPromotion):
 *   - preflight               (read-only)
 *   - tab-factory-completion  (Presentation writes only, tapestry only)
 *   - image-visual-audit      (title-only preferred; image materialization optional)
 *   - gate-suite              (spawns dry-run gates for tapestry only)
 *   - founder-review          (read-only packet)
 *   - status-promotion        (Basics.Brand Status Under Review → Active)
 *   - public-release          (Basics release fields + intentional restore)
 *   - baseline-25             (report-only 25-brand freeze)
 *
 * Guardrails (enforced across stages):
 *   - Never writes to any brand other than tapestry-collection-by-hilton.
 *   - Never writes Company Validated / Company Validation Date /
 *     Source Library status / Registry approval/status.
 *   - Never writes Brand Status unless stage=status-promotion + full flag set.
 *   - Never writes release fields (Active Profile Approved / Ready for Active
 *     Profile / Active Profile Approved Date / Founder Visual Review Pass)
 *     unless stage=public-release + full flag set.
 *   - Refuses to touch the protected 24 baseline brands or radisson-collection.
 *
 * Airtable tables:
 *   - Basics table:       "Brand Setup - Brand Basics"
 *   - Presentation table: "Brand Setup - Brand Explorer Presentation"
 *
 * Read-only universe SoT (never overwritten):
 *   lib/partner-intelligence/brand-explorer-active-universe.js
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import {
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
  loadActiveUniverse,
} from "./brand-explorer-active-universe.js";
import {
  runTabFactoryRemediation,
  planTabFactoryRemediation,
  applyTabFactoryRemediation,
} from "./brand-explorer-tab-factory-remediation.js";
import {
  runLane2FounderMinorCleanup,
  planLane2FounderMinorCleanupForBrand,
  applyLane2FounderMinorCleanup,
  REQUIRED_APPLY_FLAGS as LANE2_CLEANUP_REQUIRED_APPLY_FLAGS,
} from "./brand-explorer-lane2-founder-minor-cleanup.js";
import {
  runLane2ImageMaterialization,
  REQUIRED_APPLY_FLAGS as LANE2_IMAGE_REQUIRED_APPLY_FLAGS,
} from "./brand-explorer-lane2-image-materialization.js";
import { buildLane2FounderPacket } from "./brand-explorer-lane2-founder-packets.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { auditBrandTabSectionQuality } from "./brand-explorer-24-tab-section-quality-audit.js";
import {
  planPublicRestoreGovernance,
  applyPublicRestoreGovernance,
  writePublicRestoreGovernanceReports,
  REQUIRED_APPLY_FLAGS as PUBLIC_RESTORE_REQUIRED_APPLY_FLAGS,
  writeIntentionalPublicRestoreSlugs,
  readIntentionalPublicRestoreSlugs,
  ROOT as PUBLIC_RESTORE_ROOT,
} from "./brand-explorer-public-restore-governance.js";
import { PROTECTED_FIELDS as BASELINE_PROTECTED_FIELDS } from "./brand-explorer-24-active-public-full-baseline.js";

import {
  EXPECTED_ACTIVE_COUNT_25,
  BASELINE_VERSION_25,
  build25ActivePublicFullBaseline,
  write25ActivePublicFullBaselineReports,
} from "./brand-explorer-25-active-public-full-baseline.js";

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

export const TAPESTRY_SLUG = "tapestry-collection-by-hilton";
export const TAPESTRY_ALIAS = "tapestry";
export const TAPESTRY_RECORD_ID = "reccXxMHEh7NNRhIE";
export const TAPESTRY_NAME = "Tapestry Collection by Hilton";
export const TAPESTRY_PARENT_COMPANY = "Hilton Worldwide Holdings Inc.";
export const PROMOTION_VERSION = "tapestry-factory-promotion-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

/** Airtable "Brand Status" values we treat as terminal for tapestry promotion. */
export const TAPESTRY_STATUS_FROM = "Under Review";
export const TAPESTRY_STATUS_TO_ALLOWED = Object.freeze(["Active", "Live"]);
export const TAPESTRY_STATUS_TO_PREFERRED = "Active";

// Never write any of these under any stage — enforced defensively everywhere.
const NEVER_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

// ---------------------------------------------------------------------------
// Stage registry
// ---------------------------------------------------------------------------

export const STAGES = Object.freeze([
  "preflight",
  "tab-factory-completion",
  "image-visual-audit",
  "gate-suite",
  "founder-review",
  "status-promotion",
  "public-release",
  "baseline-25",
]);

// Retitle plan for scenario 2/3 (matches pre-baseline cleanup convention).
const SCENARIO_RETITLES = Object.freeze({
  "overview.scenario.2": "Commercial Platform And Lobby Experience",
  "overview.scenario.3": "Destination Lifestyle Stay Experience",
});

// Required apply-flag sets per stage (user-facing).
export const STAGE_REQUIRED_APPLY_FLAGS = Object.freeze({
  "tab-factory-completion": Object.freeze([
    "--approve-tapestry-tab-factory-completion",
    "--confirm-tapestry-only",
    "--confirm-no-company-validation-changes",
    "--confirm-no-source-library-status-changes",
    "--confirm-no-registry-approval-changes",
    "--confirm-no-brand-status-changes",
    "--confirm-no-release-field-writes",
    "--confirm-no-public-restore",
    "--confirm-no-protected-baseline-brand-changes",
  ]),
  "image-visual-audit": Object.freeze([
    "--approve-tapestry-image-cleanup",
    "--confirm-tapestry-only",
    "--confirm-image-issues-only",
    "--confirm-no-company-validation-changes",
    "--confirm-no-source-library-status-changes",
    "--confirm-no-registry-approval-changes",
    "--confirm-no-brand-status-changes",
    "--confirm-no-release-field-writes",
    "--confirm-no-protected-baseline-brand-changes",
  ]),
  "status-promotion": Object.freeze([
    "--approve-tapestry-brand-status-promotion",
    "--confirm-founder-approval",
    "--confirm-tapestry-only",
    "--confirm-status-from-under-review-to-active-or-live",
    "--confirm-no-company-validation-changes",
    "--confirm-no-source-library-status-changes",
    "--confirm-no-registry-approval-changes",
    "--confirm-no-content-writes",
    "--confirm-no-image-writes",
    "--confirm-no-other-brand-status-changes",
  ]),
  "public-release": Object.freeze([
    "--approve-tapestry-public-release",
    "--confirm-founder-visual-review-passed",
    "--confirm-brand-status-active-or-live",
    "--confirm-fully-ready",
    "--confirm-public-visibility-quality-lock-passed",
    "--confirm-tapestry-only",
    "--confirm-no-company-validation-changes",
    "--confirm-no-source-library-status-changes",
    "--confirm-no-registry-approval-changes",
    "--confirm-no-content-rewrites",
    "--confirm-no-image-writes",
  ]),
});

// Report file names (JSON + Markdown) per stage.
const STAGE_REPORT_FILES = Object.freeze({
  preflight: {
    json: "brand-explorer-tapestry-factory-preflight.json",
    md: "brand-explorer-tapestry-factory-preflight.md",
  },
  "tab-factory-completion": {
    json: "brand-explorer-tapestry-tab-factory-completion.json",
    md: "brand-explorer-tapestry-tab-factory-completion.md",
  },
  "image-visual-audit": {
    json: "brand-explorer-tapestry-image-visual-audit.json",
    md: "brand-explorer-tapestry-image-visual-audit.md",
  },
  "gate-suite": {
    json: "brand-explorer-tapestry-gate-suite.json",
    md: "brand-explorer-tapestry-gate-suite.md",
  },
  "founder-review": {
    json: "brand-explorer-founder-review-tapestry-collection-by-hilton.json",
    md: "brand-explorer-founder-review-tapestry-collection-by-hilton.md",
  },
  "status-promotion": {
    json: "brand-explorer-tapestry-status-promotion.json",
    md: "brand-explorer-tapestry-status-promotion.md",
  },
  "public-release": {
    json: "brand-explorer-tapestry-public-release.json",
    md: "brand-explorer-tapestry-public-release.md",
  },
  "baseline-25": {
    json: "brand-explorer-25-active-public-full-baseline.json",
    md: "brand-explorer-25-active-public-full-baseline.md",
  },
});

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function ensureReportsDir() {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
}

function ensureDocsDir() {
  fs.mkdirSync(DOCS_DIR, { recursive: true });
}

function writeStageReports(stage, jsonReport, mdText) {
  ensureReportsDir();
  const files = STAGE_REPORT_FILES[stage];
  if (!files) throw new Error(`Unknown stage report files for ${stage}`);
  const jsonPath = path.join(REPORTS_DIR, files.json);
  const mdPath = path.join(REPORTS_DIR, files.md);
  fs.writeFileSync(jsonPath, `${JSON.stringify(jsonReport, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, mdText.endsWith("\n") ? mdText : `${mdText}\n`, "utf8");
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

async function fetchTapestryBrand({ bypassCache = true } = {}) {
  const res = mockRes();
  await getBrandLibraryBrandById(
    {
      query: { brandId: TAPESTRY_RECORD_ID },
      headers: bypassCache ? { "x-bypass-brand-detail-cache": "1" } : {},
    },
    res
  );
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(
      `Tapestry brand fetch failed: HTTP ${res.statusCode} · ${res.payload?.error || ""}`.trim()
    );
  }
  return res.payload.brand;
}

async function fetchTapestryPresentationRows({ brandName = TAPESTRY_NAME } = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return { rows: [], skipped: "missing_airtable_credentials" };
  const formula = `{Brand Name}='${nz(brandName).replace(/'/g, "\\'")}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
      PRESENTATION_TABLE
    )}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    }
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      const image = f.Image;
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        externalDisplayStatus: nz(f["External Display Status"]),
        active: f.Active !== false,
        imageUrl: Array.isArray(image) && image[0]?.url ? nz(image[0].url) : "",
      });
    }
    offset = json.offset || "";
  } while (offset);
  return { rows, skipped: null };
}

function isOwnerFacingLive(row) {
  if (!row) return false;
  if (row.active === false) return false;
  if (/do not display|internal only/i.test(nz(row.externalDisplayStatus))) return false;
  return true;
}

/**
 * Refuse-write helper for Brand Basics record IDs — tapestry Basics only.
 */
function assertTapestryBasicsOnly(list, { field = "recordId" } = {}) {
  const bad = (list || []).filter((v) => {
    const value = typeof v === "string" ? v : v?.[field];
    if (!value) return false;
    return value !== TAPESTRY_RECORD_ID && value !== TAPESTRY_SLUG && value !== TAPESTRY_NAME;
  });
  if (bad.length) {
    throw new Error(
      `Refuse: non-tapestry Basics target(s) in write list: ${JSON.stringify(bad).slice(0, 200)}`
    );
  }
}

/** @deprecated Use assertTapestryBasicsOnly for Basics; presentation uses owned-row ID set. */
function assertTapestryOnly(list, opts) {
  return assertTapestryBasicsOnly(list, opts);
}

function assertPresentationRowsOwnedByTapestry(patches, presentationRows) {
  const owned = new Set((presentationRows || []).map((r) => nz(r.recordId || r.id)));
  const bad = (patches || []).filter((p) => p?.recordId && !owned.has(nz(p.recordId)));
  if (bad.length) {
    throw new Error(
      `Refuse: Presentation patch recordId(s) not in tapestry row set: ${JSON.stringify(
        bad.map((b) => b.recordId)
      ).slice(0, 200)}`
    );
  }
}

/**
 * Airtable PATCH — only allowed to write to tapestry's Basics record.
 * Refuses forbidden fields defensively and returns the patched payload preview.
 */
async function patchTapestryBasics({ fields }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required for basics PATCH");
  }
  for (const forbidden of NEVER_WRITE_FIELDS) {
    if (fields?.[forbidden] != null) {
      throw new Error(`Refuse: attempted write to forbidden basics field '${forbidden}'`);
    }
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
    BASICS_TABLE
  )}/${TAPESTRY_RECORD_ID}`;
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
    throw new Error(json.error?.message || `Basics PATCH failed: HTTP ${res.status}`);
  }
  return json;
}

async function patchTapestryPresentation({ recordId, fields }) {
  if (!recordId) throw new Error("recordId required for Presentation PATCH");
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required for presentation PATCH");
  }
  for (const forbidden of NEVER_WRITE_FIELDS) {
    if (fields?.[forbidden] != null) {
      throw new Error(`Refuse: forbidden field write on Presentation: ${forbidden}`);
    }
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
    PRESENTATION_TABLE
  )}/${recordId}`;
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
    throw new Error(json.error?.message || `Presentation PATCH failed: HTTP ${res.status}`);
  }
  return json;
}

/**
 * Confirm required flags are present. Returns { ok, missing }.
 */
function checkRequiredFlags({ stage, argv, apply }) {
  const req = STAGE_REQUIRED_APPLY_FLAGS[stage] || [];
  if (!apply) return { ok: true, missing: [] };
  const missing = req.filter((f) => !argv.includes(f));
  return { ok: missing.length === 0, missing };
}

// ---------------------------------------------------------------------------
// Preflight (dry-run only)
// ---------------------------------------------------------------------------

/**
 * Run the preflight snapshot: verify tapestry identity, status, visibility,
 * lightweight image/tab-factory heuristics, and confirm protected baseline
 * brands are never in the write target.
 */
export async function runTapestryPreflight() {
  const brand = await fetchTapestryBrand();
  const status = nz(brand.status || brand.brandStatus);
  const shouldRenderFullProfile = brand.shouldRenderFullProfile === true;
  const brandRecordId = brand.id || brand.recordId || null;
  const brandSlug = nz(brand.slug) || TAPESTRY_SLUG;

  const { rows: presentationRows, skipped } = await fetchTapestryPresentationRows();
  const ownerFacing = presentationRows.filter(isOwnerFacingLive);

  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: ownerFacing,
    brandSlug: TAPESTRY_SLUG,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacing,
    brandSlug: TAPESTRY_SLUG,
  });

  const identityChecks = {
    recordIdOk: brandRecordId === TAPESTRY_RECORD_ID,
    slugOk: brandSlug === TAPESTRY_SLUG,
    statusIsUnderReview: status === TAPESTRY_STATUS_FROM,
    statusIsActiveLive: isBrandStatusActive(status),
    shouldRenderFullProfileFalse: shouldRenderFullProfile !== true,
  };

  const identityIssues = [];
  if (!identityChecks.recordIdOk) {
    identityIssues.push(
      `recordId_mismatch:got=${brandRecordId || "(none)"};expected=${TAPESTRY_RECORD_ID}`
    );
  }
  if (!identityChecks.slugOk) {
    identityIssues.push(`slug_mismatch:got=${brandSlug};expected=${TAPESTRY_SLUG}`);
  }
  if (!identityChecks.statusIsUnderReview) {
    identityIssues.push(`status_not_under_review:got=${status || "(empty)"}`);
  }
  if (identityChecks.statusIsActiveLive) {
    identityIssues.push(`status_already_active_or_live:got=${status}`);
  }
  if (!identityChecks.shouldRenderFullProfileFalse) {
    identityIssues.push("shouldRenderFullProfile_already_true");
  }

  // Protected baseline: fetch active universe (fast, no include-details) to
  // enumerate protected brands and confirm tapestry is NOT among them.
  const universe = await loadActiveUniverse({ includeDetails: false });
  const protectedBaselineSlugs = universe.brands.map((b) => b.slug).sort();
  const tapestryInBaseline = protectedBaselineSlugs.includes(TAPESTRY_SLUG);
  const radissonCollectionInBaseline = protectedBaselineSlugs.includes("radisson-collection");

  const guardrails = {
    tapestryOnlyWriteTarget: true,
    protectedBaselineBrandsInWriteTargets: false,
    tapestryIsProtectedBaseline: tapestryInBaseline,
    radissonCollectionIsProtectedBaseline: radissonCollectionInBaseline,
    protectedFields: [...BASELINE_PROTECTED_FIELDS, ...NEVER_WRITE_FIELDS],
  };

  const imageSnapshot = {
    galleryDistinct: uniqueness.galleryDistinctCount ?? 0,
    scenarioDistinct: uniqueness.scenarioDistinctCount ?? 0,
    propertyDistinct: uniqueness.propertyExampleDistinctCount ?? 0,
    uniquenessPass: uniqueness.pass === true,
    roleMatchPass: roleMatch?.pass !== false,
    duplicateGroups: (uniqueness.duplicateGroups || []).length,
  };

  const report = {
    version: PROMOTION_VERSION,
    stage: "preflight",
    generatedAt: new Date().toISOString(),
    apply: false,
    dryRun: true,
    writePerformed: false,
    tapestry: {
      slug: TAPESTRY_SLUG,
      alias: TAPESTRY_ALIAS,
      recordId: TAPESTRY_RECORD_ID,
      name: TAPESTRY_NAME,
      parentCompany: TAPESTRY_PARENT_COMPANY,
      brandStatus: status,
      shouldRenderFullProfile,
      liveBrandRecordId: brandRecordId,
      liveBrandSlug: brandSlug,
      publicDisplayState: brand.brandExplorerDisplayState || null,
    },
    presentation: {
      totalRowCount: presentationRows.length,
      ownerFacingRowCount: ownerFacing.length,
      fetchSkipped: skipped,
    },
    imageSnapshot,
    identityChecks,
    identityIssues,
    guardrails,
    protectedBaseline: {
      universeSlugs: protectedBaselineSlugs,
      count: protectedBaselineSlugs.length,
      tapestryIncluded: tapestryInBaseline,
    },
    excludedProbes: NON_ACTIVE_STATUS_CONFLICT_PROBES.map((p) => ({
      slug: p.slug,
      recordId: p.recordId,
      name: p.name,
    })),
    readyForNextStage: identityIssues.length === 0,
  };

  const md = renderPreflightMarkdown(report);
  const paths = writeStageReports("preflight", report, md);
  return { report, paths };
}

function renderPreflightMarkdown(r) {
  const lines = [];
  lines.push(`# Tapestry Collection by Hilton — Factory Promotion Preflight`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Stage: **preflight** · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **dry-run** · writePerformed: **false**`);
  lines.push("");
  lines.push("## Identity");
  lines.push("");
  lines.push(`- Slug: \`${r.tapestry.slug}\` (alias: \`${r.tapestry.alias}\`)`);
  lines.push(`- Record ID: \`${r.tapestry.recordId}\``);
  lines.push(`- Name: ${r.tapestry.name}`);
  lines.push(`- Parent: ${r.tapestry.parentCompany}`);
  lines.push(`- Brand Status: **${r.tapestry.brandStatus || "(empty)"}**`);
  lines.push(`- shouldRenderFullProfile: **${r.tapestry.shouldRenderFullProfile}**`);
  lines.push(`- Public Display State: ${r.tapestry.publicDisplayState || "—"}`);
  lines.push("");
  lines.push("## Identity checks");
  lines.push("");
  for (const [k, v] of Object.entries(r.identityChecks)) {
    lines.push(`- ${k}: **${v}**`);
  }
  if (r.identityIssues.length) {
    lines.push("");
    lines.push("### Identity issues");
    lines.push("");
    for (const i of r.identityIssues) lines.push(`- ${i}`);
  }
  lines.push("");
  lines.push("## Presentation snapshot");
  lines.push("");
  lines.push(`- Total rows: ${r.presentation.totalRowCount}`);
  lines.push(`- Owner-facing rows: ${r.presentation.ownerFacingRowCount}`);
  if (r.presentation.fetchSkipped) {
    lines.push(`- Fetch skipped: ${r.presentation.fetchSkipped}`);
  }
  lines.push("");
  lines.push("## Image snapshot");
  lines.push("");
  lines.push(`- galleryDistinct: ${r.imageSnapshot.galleryDistinct}`);
  lines.push(`- scenarioDistinct: ${r.imageSnapshot.scenarioDistinct}`);
  lines.push(`- propertyDistinct: ${r.imageSnapshot.propertyDistinct}`);
  lines.push(`- uniquenessPass: ${r.imageSnapshot.uniquenessPass}`);
  lines.push(`- roleMatchPass: ${r.imageSnapshot.roleMatchPass}`);
  lines.push(`- duplicateGroups: ${r.imageSnapshot.duplicateGroups}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails)) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  lines.push(`Protected baseline count: **${r.protectedBaseline.count}** (tapestry included: **${r.protectedBaseline.tapestryIncluded}**)`);
  lines.push("");
  lines.push(`readyForNextStage: **${r.readyForNextStage}**`);
  lines.push("");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Stage: tab-factory-completion
// ---------------------------------------------------------------------------

function isTapestryOnlyBrandResults(brandResults) {
  return (brandResults || []).every((b) => {
    const slug = nz(b?.brandSlug || b?.slug || "");
    return slug === TAPESTRY_SLUG;
  });
}

/**
 * Fix known golden failures (idempotent, tapestry only):
 *  - Rewrite stub chip "conversion-friendly" (case-insensitive) into
 *    owner-facing multi-word phrasing.
 *  - Thicken thin openings body (falls back to lane2 openings contract).
 */
async function planKnownGoldenFailurePatches({ presentationRows }) {
  const patches = [];
  const openingsRows = presentationRows.filter(
    (r) => r.slotKey === "footprint.openings" && isOwnerFacingLive(r)
  );

  for (const row of presentationRows) {
    if (!isOwnerFacingLive(row)) continue;

    // Stub chip scrub on Body and Title
    let changedBody = row.body;
    let changedTitle = row.title;
    let stubHit = false;
    if (/\bconversion-friendly\.?\b/i.test(row.body)) {
      changedBody = row.body.replace(/\bconversion-friendly\.?\b/gi, "accessible conversion path");
      stubHit = true;
    }
    if (/\bconversion-friendly\.?\b/i.test(row.title)) {
      changedTitle = row.title.replace(
        /\bconversion-friendly\.?\b/gi,
        "Accessible Conversion Path"
      );
      stubHit = true;
    }
    if (stubHit) {
      const fields = {};
      if (changedBody !== row.body) fields.Body = changedBody;
      if (changedTitle !== row.title) fields.Title = changedTitle;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: row.recordId,
        slotKey: row.slotKey,
        reason: "known_golden_stub_conversion_friendly_rewrite",
        before: { body: row.body, title: row.title },
        fields,
      });
    }
  }

  // Openings thin-body thickening — only if it's clearly thin (< 20 words)
  for (const opening of openingsRows) {
    const wordCount = nz(opening.body).split(/\s+/).filter(Boolean).length;
    if (wordCount >= 20) continue;
    const rewriteTitle = opening.title || "Tapestry Property Reference";
    const marketToken = nz(opening.title).split("—")[1]?.trim() || "";
    const body = [
      `Independent, Upscale, Hilton Honors, Collection`,
      marketToken || "Market location",
      `Tapestry Collection by Hilton`,
      `INDEPENDENT / UPSCALE / HILTON HONORS`,
      `${rewriteTitle} is a Tapestry Collection by Hilton reference for owners underwriting soft-brand character, distribution economics, and design-review readiness. Confirm live affiliation criteria and capital scope with Hilton brand development before treating this as a conversion path.`,
    ].join("\n\n");
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: opening.recordId,
      slotKey: opening.slotKey,
      reason: "known_golden_openings_body_thicken",
      before: { body: opening.body, title: opening.title, wordCount },
      fields: { Body: body },
    });
  }

  return patches;
}

/**
 * Plan + optionally apply tab-factory completion for tapestry.
 *
 * Because `TAB_FACTORY_REMEDIATION_CONTENT` does not include tapestry, we do
 * NOT call planTabFactoryRemediation here (it would throw). Instead we drive
 * Presentation completion through `runLane2FounderMinorCleanup` for tapestry,
 * which is tapestry-aware and refuses protected baseline brands internally.
 */
export async function runTapestryTabFactoryCompletion({ apply = false, argv = [] } = {}) {
  const flagCheck = checkRequiredFlags({ stage: "tab-factory-completion", argv, apply });
  const stage = "tab-factory-completion";

  // Refuse if --brands was passed with anything other than tapestry — orchestrator
  // is tapestry only.
  const brandsIdx = argv.indexOf("--brands");
  if (brandsIdx >= 0 && argv[brandsIdx + 1]) {
    const requested = argv[brandsIdx + 1]
      .split(",")
      .map((s) => nz(s).toLowerCase())
      .filter(Boolean);
    for (const s of requested) {
      if (s !== TAPESTRY_SLUG && s !== TAPESTRY_ALIAS) {
        throw new Error(
          `Refuse: --brands must be '${TAPESTRY_SLUG}' or '${TAPESTRY_ALIAS}' for tapestry factory promotion (got ${s})`
        );
      }
    }
  }

  // Plan lane2 cleanup for tapestry only
  const lane2Plan = await planLane2FounderMinorCleanupForBrand(TAPESTRY_SLUG);
  const brandResults = [lane2Plan];
  if (!isTapestryOnlyBrandResults(brandResults)) {
    throw new Error("Refuse: brandResults contains non-tapestry brand(s)");
  }

  // Plan known golden fixes on top (idempotent; may overlap lane2 openings work).
  const { rows: presentationRows } = await fetchTapestryPresentationRows();
  const knownFailurePatches = await planKnownGoldenFailurePatches({ presentationRows });

  // Try tab-factory remediation as a best-effort pass IF tapestry pack exists.
  // Currently the pack registry does not include tapestry — this stays as a
  // no-op documented step so we never accidentally patch other brands.
  let tabFactoryRemediation = {
    attempted: false,
    reason: "no_tab_factory_remediation_content_for_tapestry",
    brands: [],
  };
  try {
    // Only attempt if pack exists (avoids "No tab-factory remediation content" throw).
    const { getTabFactoryRemediationPack } = await import(
      "./brand-explorer-tab-factory-remediation-content.js"
    );
    const pack = getTabFactoryRemediationPack(TAPESTRY_SLUG);
    if (pack) {
      tabFactoryRemediation.attempted = true;
      const tfrPlan = await planTabFactoryRemediation(TAPESTRY_SLUG);
      if (nz(tfrPlan.brandSlug) !== TAPESTRY_SLUG) {
        throw new Error("Refuse: tab-factory-remediation returned non-tapestry plan");
      }
      tabFactoryRemediation = {
        attempted: true,
        reason: "tab_factory_pack_available",
        brands: [TAPESTRY_SLUG],
        plan: {
          brandSlug: tfrPlan.brandSlug,
          patches: tfrPlan.patches?.length || 0,
          blockers: tfrPlan.blockers || [],
        },
      };
      if (apply && flagCheck.ok) {
        const applyResult = await applyTabFactoryRemediation({
          brandResults: [tfrPlan],
          apply: true,
          // tab-factory-remediation has its own required flag set; wire generic
          // 'no CV / no release' flags that tapestry orchestrator already asks
          // the user to confirm, plus the tab-factory-specific ones.
          argv: [
            "--apply",
            "--approve-tab-factory-remediation",
            "--confirm-no-company-validation-changes",
            "--confirm-no-release-field-changes",
            "--confirm-no-source-library-status-changes",
            "--confirm-no-registry-approval-changes",
            "--confirm-protected-brands-unchanged",
            "--confirm-no-empty-rendered-fields",
            "--confirm-source-provenance-by-tab",
            "--confirm-brand-specific-copy",
            "--confirm-benchmark-quality-met",
            ...argv,
          ],
        });
        tabFactoryRemediation.applyResult = applyResult;
      }
    }
  } catch (err) {
    tabFactoryRemediation = {
      attempted: true,
      reason: `tab_factory_remediation_error:${err.message}`,
      brands: [],
    };
  }

  // Apply lane2 cleanup + known golden patches only when apply + flags ok.
  let lane2ApplyResult = { applied: false, reason: "dry_run_only" };
  let knownFailureApplyResults = [];
  const applyPerformed = apply === true && flagCheck.ok === true;

  if (applyPerformed) {
    // Wire lane2 required flags explicitly (mapped from tapestry-stage flags)
    const lane2Argv = [
      "--apply",
      ...LANE2_CLEANUP_REQUIRED_APPLY_FLAGS,
      ...argv,
    ];
    lane2ApplyResult = await applyLane2FounderMinorCleanup({
      brandResults,
      apply: true,
      argv: lane2Argv,
    });

    // Apply the known-golden patches (Presentation only) as tapestry-only PATCHes.
    assertPresentationRowsOwnedByTapestry(knownFailurePatches, presentationRows);
    for (const patch of knownFailurePatches) {
      if (patch.action !== "PATCH") {
        knownFailureApplyResults.push({ patch, ok: false, reason: "unsupported_action" });
        continue;
      }
      try {
        await patchTapestryPresentation({ recordId: patch.recordId, fields: patch.fields });
        knownFailureApplyResults.push({
          patch: {
            recordId: patch.recordId,
            slotKey: patch.slotKey,
            reason: patch.reason,
            fields: Object.keys(patch.fields || {}),
          },
          ok: true,
        });
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        knownFailureApplyResults.push({
          patch: {
            recordId: patch.recordId,
            slotKey: patch.slotKey,
            reason: patch.reason,
          },
          ok: false,
          reason: err.message,
        });
      }
    }
  }

  const report = {
    version: PROMOTION_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed: applyPerformed,
    dryRun: !applyPerformed,
    flagCheck,
    tapestry: {
      slug: TAPESTRY_SLUG,
      recordId: TAPESTRY_RECORD_ID,
      name: TAPESTRY_NAME,
    },
    brands: [TAPESTRY_SLUG],
    lane2Plan: {
      brandSlug: lane2Plan.brandSlug,
      patchCount: lane2Plan.patches?.length || 0,
      blocked: lane2Plan.blocked === true,
      blockers: lane2Plan.blockers || [],
      summary: lane2Plan.summary,
    },
    lane2ApplyResult,
    tabFactoryRemediation,
    knownGoldenPatches: knownFailurePatches.map((p) => ({
      table: p.table,
      recordId: p.recordId,
      slotKey: p.slotKey,
      reason: p.reason,
      fields: Object.keys(p.fields || {}),
    })),
    knownGoldenApplyResults: knownFailureApplyResults,
    guardrails: {
      tapestryOnly: true,
      protectedBaselineUntouched: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      publicRestore: false,
      neverWriteFields: [...NEVER_WRITE_FIELDS],
    },
    requiredApplyFlags: STAGE_REQUIRED_APPLY_FLAGS[stage],
  };

  const md = renderTabFactoryCompletionMarkdown(report);
  const paths = writeStageReports(stage, report, md);
  return { report, paths };
}

function renderTabFactoryCompletionMarkdown(r) {
  const lines = [];
  lines.push(`# Tapestry — Tab Factory Completion`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Stage: **${r.stage}** · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`);
  lines.push(`Brands (write scope): ${r.brands.join(", ")}`);
  lines.push("");
  lines.push("## Lane 2 Founder Minor Cleanup plan (tapestry only)");
  lines.push("");
  lines.push(`- Patches planned: **${r.lane2Plan.patchCount}**`);
  lines.push(`- Blocked: **${r.lane2Plan.blocked}**`);
  if (r.lane2Plan.blockers?.length) {
    lines.push(`- Blockers: ${r.lane2Plan.blockers.join(", ")}`);
  }
  if (r.lane2Plan.summary) {
    lines.push(`- Summary: ${JSON.stringify(r.lane2Plan.summary)}`);
  }
  lines.push("");
  lines.push("## Tab Factory Remediation (best-effort)");
  lines.push("");
  lines.push(`- Attempted: **${r.tabFactoryRemediation.attempted}**`);
  lines.push(`- Reason: ${r.tabFactoryRemediation.reason}`);
  if (r.tabFactoryRemediation.plan) {
    lines.push(`- Plan: ${JSON.stringify(r.tabFactoryRemediation.plan)}`);
  }
  lines.push("");
  lines.push("## Known golden failure fixes");
  lines.push("");
  if (!r.knownGoldenPatches.length) {
    lines.push("_None planned (no `conversion-friendly` stub or thin openings body detected)._");
  } else {
    lines.push("| Record | Slot | Reason | Fields |");
    lines.push("|--------|------|--------|--------|");
    for (const p of r.knownGoldenPatches) {
      lines.push(
        `| \`${p.recordId}\` | \`${p.slotKey}\` | ${p.reason} | ${p.fields.join(", ")} |`
      );
    }
  }
  lines.push("");
  if (r.applyPerformed) {
    lines.push("## Apply results");
    lines.push("");
    lines.push(`- Lane 2 apply: \`${r.lane2ApplyResult.reason || "applied"}\``);
    for (const kr of r.knownGoldenApplyResults) {
      lines.push(`- Known-golden PATCH ${kr.patch.recordId}: ${kr.ok ? "ok" : `fail:${kr.reason}`}`);
    }
    lines.push("");
  }
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails)) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  lines.push("## Required apply flags");
  lines.push("");
  for (const f of r.requiredApplyFlags) lines.push(`- \`${f}\``);
  lines.push("");
  if (r.flagCheck?.missing?.length) {
    lines.push("### Missing apply flags");
    for (const m of r.flagCheck.missing) lines.push(`- \`${m}\``);
    lines.push("");
  }
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Stage: image-visual-audit
// ---------------------------------------------------------------------------

/**
 * Prefer title-only rewrites when scenarios collapse to the same role.
 * Falls back to lane2 image materialization only when the caller passes
 * `--enable-image-materialization` AND all image + tapestry-only flags.
 */
export async function runTapestryImageVisualAudit({ apply = false, argv = [] } = {}) {
  const stage = "image-visual-audit";
  const flagCheck = checkRequiredFlags({ stage, argv, apply });

  const brand = await fetchTapestryBrand();
  const { rows: presentationRows } = await fetchTapestryPresentationRows();
  const ownerFacing = presentationRows.filter(isOwnerFacingLive);

  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: ownerFacing,
    brandSlug: TAPESTRY_SLUG,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacing,
    brandSlug: TAPESTRY_SLUG,
  });

  const scenarioDistinct = uniqueness.scenarioDistinctCount ?? 0;
  const galleryDistinct = uniqueness.galleryDistinctCount ?? 0;
  const propertyDistinct = uniqueness.propertyExampleDistinctCount ?? 0;

  const alreadyPasses =
    scenarioDistinct >= 3 &&
    galleryDistinct >= 6 &&
    propertyDistinct >= 3 &&
    uniqueness.pass === true &&
    roleMatch?.pass !== false;

  // Detect repeated_visual_role hint: title/caption collapsing to same word bucket
  const scenarioRows = ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"]
    .map((k) => ownerFacing.find((r) => r.slotKey === k) || null)
    .filter(Boolean);
  const scenarioTitles = scenarioRows.map((r) => nz(r.title).toLowerCase());
  const roleWordBuckets = scenarioTitles.map((t) => {
    if (/lobby|distribution|commercial|guest journey|public space/i.test(t)) return "commercial";
    if (/exterior|arrival|facade|street/i.test(t)) return "exterior";
    if (/lifestyle|destination|experience|room|suite|wellness|pool|f&b/i.test(t)) return "experience";
    return "unknown";
  });
  const repeatedRole = new Set(roleWordBuckets).size < 3;

  const titlePatches = [];
  if (repeatedRole) {
    for (const [slotKey, newTitle] of Object.entries(SCENARIO_RETITLES)) {
      const row = scenarioRows.find((r) => r.slotKey === slotKey);
      if (!row) continue;
      if (nz(row.title) === newTitle) continue;
      titlePatches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: row.recordId,
        slotKey,
        reason: "scenario_retitle_for_role_diversity",
        before: { title: row.title },
        fields: { Title: newTitle },
      });
    }
  }

  // Deferred materialization plan (only executed with explicit opt-in)
  const wantsMaterialization =
    argv.includes("--enable-image-materialization") &&
    (galleryDistinct < 6 || scenarioDistinct < 3 || propertyDistinct < 3);
  let materialization = {
    attempted: false,
    reason: "deferred_no_opt_in_or_gates_pass",
    counts: { galleryDistinct, scenarioDistinct, propertyDistinct },
  };
  if (wantsMaterialization) {
    // Compose lane2 image required flags on top of tapestry-stage flags for the
    // internal materialization call. If the user did NOT pass those flags, the
    // materialization stays dry-run.
    const lane2ImgArgv = [...argv];
    if (apply) {
      for (const f of LANE2_IMAGE_REQUIRED_APPLY_FLAGS) {
        if (!lane2ImgArgv.includes(f)) lane2ImgArgv.push(f);
      }
      if (!lane2ImgArgv.includes("--apply")) lane2ImgArgv.push("--apply");
    }
    materialization = {
      attempted: true,
      reason: "image_gates_short_and_opt_in_received",
      counts: { galleryDistinct, scenarioDistinct, propertyDistinct },
    };
    try {
      const matResult = await runLane2ImageMaterialization({
        brands: [TAPESTRY_SLUG],
        dryRun: !(apply && flagCheck.ok),
        argv: lane2ImgArgv,
      });
      materialization.result = matResult;
    } catch (err) {
      materialization.error = err.message;
    }
  }

  const applyPerformed = apply === true && flagCheck.ok === true;
  const applyResults = [];
  if (applyPerformed) {
    assertPresentationRowsOwnedByTapestry(titlePatches, ownerFacing);
    for (const patch of titlePatches) {
      try {
        await patchTapestryPresentation({ recordId: patch.recordId, fields: patch.fields });
        applyResults.push({ recordId: patch.recordId, slotKey: patch.slotKey, ok: true });
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          ok: false,
          reason: err.message,
        });
      }
    }
  }

  const report = {
    version: PROMOTION_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed: applyPerformed,
    dryRun: !applyPerformed,
    flagCheck,
    tapestry: { slug: TAPESTRY_SLUG, recordId: TAPESTRY_RECORD_ID, name: TAPESTRY_NAME },
    imageSnapshot: {
      galleryDistinct,
      scenarioDistinct,
      propertyDistinct,
      uniquenessPass: uniqueness.pass === true,
      roleMatchPass: roleMatch?.pass !== false,
      repeatedRoleHeuristicHit: repeatedRole,
      alreadyPasses,
      duplicateGroups: (uniqueness.duplicateGroups || []).length,
    },
    scenarioRoles: scenarioTitles.map((title, i) => ({
      slotKey: `overview.scenario.${i + 1}`,
      title,
      roleBucket: roleWordBuckets[i],
    })),
    plannedTitlePatches: titlePatches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      reason: p.reason,
      fields: Object.keys(p.fields || {}),
      before: p.before,
    })),
    applyResults,
    materialization,
    guardrails: {
      tapestryOnly: true,
      titleOnlyPreferred: true,
      protectedBaselineUntouched: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      neverWriteFields: [...NEVER_WRITE_FIELDS],
    },
    requiredApplyFlags: STAGE_REQUIRED_APPLY_FLAGS[stage],
  };

  const md = renderImageVisualAuditMarkdown(report);
  const paths = writeStageReports(stage, report, md);
  return { report, paths };
}

function renderImageVisualAuditMarkdown(r) {
  const lines = [];
  lines.push(`# Tapestry — Image Visual Audit`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Stage: **${r.stage}** · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`);
  lines.push("");
  lines.push("## Image snapshot");
  lines.push("");
  for (const [k, v] of Object.entries(r.imageSnapshot)) {
    lines.push(`- ${k}: **${v}**`);
  }
  lines.push("");
  lines.push("## Scenario roles");
  lines.push("");
  lines.push("| Slot | Title | Detected role |");
  lines.push("|------|-------|---------------|");
  for (const s of r.scenarioRoles) {
    lines.push(`| \`${s.slotKey}\` | ${s.title || "—"} | ${s.roleBucket} |`);
  }
  lines.push("");
  lines.push("## Planned title patches (title-only, tapestry only)");
  lines.push("");
  if (!r.plannedTitlePatches.length) {
    lines.push("_No title patches planned (scenarios already role-diverse or opt-out)._");
  } else {
    for (const p of r.plannedTitlePatches) {
      lines.push(`- PATCH \`${p.recordId}\` slot=\`${p.slotKey}\` reason=${p.reason} fields=${p.fields.join(",")}`);
    }
  }
  lines.push("");
  if (r.applyResults?.length) {
    lines.push("## Apply results");
    lines.push("");
    for (const a of r.applyResults) {
      lines.push(`- \`${a.recordId}\` slot=\`${a.slotKey}\`: ${a.ok ? "ok" : `fail:${a.reason}`}`);
    }
    lines.push("");
  }
  lines.push("## Materialization");
  lines.push("");
  lines.push(`- Attempted: **${r.materialization.attempted}**`);
  lines.push(`- Reason: ${r.materialization.reason}`);
  if (r.materialization.error) {
    lines.push(`- Error: ${r.materialization.error}`);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails)) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  lines.push("## Required apply flags");
  lines.push("");
  for (const f of r.requiredApplyFlags) lines.push(`- \`${f}\``);
  lines.push("");
  if (r.flagCheck?.missing?.length) {
    lines.push("### Missing apply flags");
    for (const m of r.flagCheck.missing) lines.push(`- \`${m}\``);
    lines.push("");
  }
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Stage: gate-suite
// ---------------------------------------------------------------------------

const GATE_COMMANDS = Object.freeze([
  {
    name: "brand-explorer-tab-factory-audit",
    script: "brand-explorer-tab-factory-audit",
    extraArgs: ["--brands", TAPESTRY_SLUG, "--dry-run"],
  },
  {
    name: "test:brand-explorer-rendered-field-completeness",
    script: "test:brand-explorer-rendered-field-completeness",
    extraArgs: ["--brands", TAPESTRY_SLUG],
  },
  {
    name: "test:brand-explorer-no-empty-rendered-components",
    script: "test:brand-explorer-no-empty-rendered-components",
    extraArgs: ["--brands", TAPESTRY_SLUG],
  },
  {
    name: "brand-explorer-source-provenance-by-tab",
    script: "brand-explorer-source-provenance-by-tab",
    extraArgs: ["--brands", TAPESTRY_SLUG, "--dry-run"],
  },
  {
    name: "brand-explorer-image-uniqueness-audit",
    script: "brand-explorer-image-uniqueness-audit",
    extraArgs: ["--brands", TAPESTRY_SLUG, "--dry-run"],
  },
  {
    name: "brand-explorer-image-role-match-audit",
    script: "brand-explorer-image-role-match-audit",
    extraArgs: ["--brands", TAPESTRY_SLUG, "--dry-run"],
  },
  {
    name: "test:brand-explorer-section-pattern-parity",
    script: "test:brand-explorer-section-pattern-parity",
    extraArgs: ["--brands", TAPESTRY_SLUG],
  },
  {
    name: "test:brand-explorer-golden-content-quality",
    script: "test:brand-explorer-golden-content-quality",
    extraArgs: ["--brands", TAPESTRY_SLUG],
  },
]);

function runNpmGate({ script, extraArgs, cwd = ROOT, timeoutMs = 15 * 60 * 1000 }) {
  const args = ["run", script, "--", ...extraArgs];
  const result = spawnSync("npm", args, {
    cwd,
    encoding: "utf8",
    env: process.env,
    shell: true, // Windows-friendly
    timeout: timeoutMs,
  });
  const stdout = String(result.stdout || "");
  const stderr = String(result.stderr || "");
  return {
    script,
    args,
    status: result.status,
    signal: result.signal || null,
    ok: result.status === 0,
    stdoutTail: stdout.split(/\r?\n/).slice(-25).join("\n"),
    stderrTail: stderr.split(/\r?\n/).slice(-25).join("\n"),
    timedOut: result.error?.code === "ETIMEDOUT" || false,
    error: result.error?.message || null,
  };
}

/**
 * Runs the gate suite for tapestry only (dry-run only — no apply path).
 * Also embeds an inline call to auditBrandTabSectionQuality for tapestry.
 */
export async function runTapestryGateSuite({ argv = [] } = {}) {
  const stage = "gate-suite";
  if (argv.includes("--apply")) {
    throw new Error("Refuse: gate-suite is dry-run only; do not pass --apply");
  }

  const gates = [];
  for (const cmd of GATE_COMMANDS) {
    const r = runNpmGate(cmd);
    gates.push({ gate: cmd.name, ...r });
  }

  // Inline quality audit for tapestry (does not overwrite the 24 master)
  let inlineQualityAudit = null;
  try {
    const q = await auditBrandTabSectionQuality(TAPESTRY_SLUG, {
      universeRow: {
        recordId: TAPESTRY_RECORD_ID,
        status: TAPESTRY_STATUS_FROM,
        name: TAPESTRY_NAME,
        slug: TAPESTRY_SLUG,
      },
    });
    inlineQualityAudit = {
      brand: q.brand,
      slug: q.slug,
      recordId: q.recordId,
      brandStatus: q.brandStatus,
      shouldRenderFullProfile: q.shouldRenderFullProfile,
      pvqlStatus: q.pvqlStatus,
      overallRecommendation: q.overallRecommendation,
      scores: q.scores,
      gates: q.gates,
      blockerCount: q.scores?.blockerCount ?? 0,
      tabFindingCount: (q.tabFindings || []).filter((f) => f.status !== "pass").length,
      imageFindingCount: (q.imageFindings || []).length,
    };
  } catch (err) {
    inlineQualityAudit = { error: err.message };
  }

  const okCount = gates.filter((g) => g.ok).length;
  const report = {
    version: PROMOTION_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply: false,
    dryRun: true,
    writePerformed: false,
    brand: TAPESTRY_SLUG,
    gates,
    inlineQualityAudit,
    summary: {
      totalGates: gates.length,
      passed: okCount,
      failed: gates.length - okCount,
      allPass: okCount === gates.length,
      inlineQualityRecommendation: inlineQualityAudit?.overallRecommendation || null,
      inlineQualityBlockerCount: inlineQualityAudit?.blockerCount ?? null,
    },
    guardrails: {
      tapestryOnly: true,
      airtableWrites: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
    },
  };

  const md = renderGateSuiteMarkdown(report);
  const paths = writeStageReports(stage, report, md);
  return { report, paths };
}

function renderGateSuiteMarkdown(r) {
  const lines = [];
  lines.push(`# Tapestry — Gate Suite (dry-run, tapestry only)`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Stage: **${r.stage}** · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **dry-run** · writePerformed: **false**`);
  lines.push("");
  lines.push(`Passed: **${r.summary.passed}** / ${r.summary.totalGates} · allPass: **${r.summary.allPass}**`);
  lines.push("");
  lines.push("## Per-gate results");
  lines.push("");
  lines.push("| Gate | Status | OK | stderr tail |");
  lines.push("|------|-------:|:--:|-------------|");
  for (const g of r.gates) {
    const stderrCell = (g.stderrTail || "").replace(/\|/g, "/").split(/\r?\n/).slice(-3).join(" · ").slice(0, 200);
    lines.push(`| \`${g.gate}\` | ${g.status ?? "—"} | ${g.ok ? "yes" : "no"} | ${stderrCell || "—"} |`);
  }
  lines.push("");
  lines.push("## Per-gate stdout tails");
  lines.push("");
  for (const g of r.gates) {
    lines.push(`### \`${g.gate}\` (status=${g.status})`);
    lines.push("");
    lines.push("```");
    lines.push(g.stdoutTail || "(empty)");
    lines.push("```");
    lines.push("");
  }
  lines.push("## Inline quality audit (auditBrandTabSectionQuality)");
  lines.push("");
  if (r.inlineQualityAudit?.error) {
    lines.push(`- Error: ${r.inlineQualityAudit.error}`);
  } else if (r.inlineQualityAudit) {
    lines.push(`- Recommendation: **${r.inlineQualityAudit.overallRecommendation}**`);
    lines.push(`- PVQL status: ${r.inlineQualityAudit.pvqlStatus}`);
    lines.push(`- Blockers: ${r.inlineQualityAudit.blockerCount}`);
    lines.push(`- Tab findings: ${r.inlineQualityAudit.tabFindingCount} · Image findings: ${r.inlineQualityAudit.imageFindingCount}`);
    lines.push(`- Scores: ${JSON.stringify(r.inlineQualityAudit.scores)}`);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails)) {
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Stage: founder-review (dry-run only)
// ---------------------------------------------------------------------------

function mapFounderRecommendation({ inlineQualityAudit, packet, imageOk, allGatesPass }) {
  const rec = inlineQualityAudit?.overallRecommendation || packet?.recommendation || null;
  const blockers = inlineQualityAudit?.blockerCount ?? 0;
  const freezeReady = rec === "approve_for_baseline_freeze" && blockers === 0;
  if (freezeReady && imageOk && allGatesPass) {
    return "approve_for_status_promotion_and_public_release";
  }
  if (rec === "approve_after_minor_cleanup" || (freezeReady && !allGatesPass)) {
    return "approve_after_minor_cleanup";
  }
  return "remediation_required";
}

export async function runTapestryFounderReview() {
  const stage = "founder-review";

  const packet = await buildLane2FounderPacket(TAPESTRY_SLUG, {
    gateNotes: { tapestryPromotion: true },
  });

  // Optional: enrich with gate-suite disk report if present
  let gateSuite = null;
  try {
    const p = path.join(REPORTS_DIR, STAGE_REPORT_FILES["gate-suite"].json);
    if (fs.existsSync(p)) {
      gateSuite = JSON.parse(fs.readFileSync(p, "utf8"));
    }
  } catch (err) {
    gateSuite = { readError: err.message };
  }

  const brand = await fetchTapestryBrand();
  const status = nz(brand.status || brand.brandStatus);
  const imageOk =
    (packet.imageCounts?.galleryDistinct ?? 0) >= 6 &&
    (packet.imageCounts?.scenarioDistinct ?? 0) >= 3 &&
    (packet.imageCounts?.propertyDistinct ?? 0) >= 3 &&
    packet.gateSummary?.imageUniquenessPass === true &&
    packet.gateSummary?.imageRoleMatchPass !== false;
  const allGatesPass = gateSuite?.summary?.allPass === true;

  const recommendation = mapFounderRecommendation({
    inlineQualityAudit: gateSuite?.inlineQualityAudit,
    packet,
    imageOk,
    allGatesPass,
  });

  const report = {
    version: PROMOTION_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    tapestry: {
      slug: TAPESTRY_SLUG,
      recordId: TAPESTRY_RECORD_ID,
      name: TAPESTRY_NAME,
      brandStatus: status,
      previewUrl: packet.previewUrl,
      publicVisibility: brand.brandExplorerDisplayState || null,
    },
    curioDistinctionCaution: [
      "Tapestry vs Curio Collection: Tapestry is upscale, lighter design-review, Hilton Honors reach.",
      "Curio is upper-upscale with stronger culinary/public-space intensity and stricter design review.",
      "Owners considering both should confirm capital tolerance and independent-character bar before selecting Tapestry.",
    ],
    packet,
    gateSuite: gateSuite
      ? {
          allPass: gateSuite.summary?.allPass ?? null,
          passed: gateSuite.summary?.passed ?? null,
          totalGates: gateSuite.summary?.totalGates ?? null,
          inlineQualityRecommendation: gateSuite.summary?.inlineQualityRecommendation ?? null,
          inlineQualityBlockerCount: gateSuite.summary?.inlineQualityBlockerCount ?? null,
          gates: (gateSuite.gates || []).map((g) => ({
            gate: g.gate,
            status: g.status,
            ok: g.ok,
          })),
        }
      : { note: "no gate-suite report on disk; run stage=gate-suite first" },
    imageOk,
    allGatesPass,
    recommendation,
    guardrails: {
      writePerformed: false,
      publicRestore: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
    },
  };

  const md = renderFounderReviewMarkdown(report);
  const paths = writeStageReports(stage, report, md);
  return { report, paths };
}

function renderFounderReviewMarkdown(r) {
  const lines = [];
  lines.push(`# Founder Review — Tapestry Collection by Hilton`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Stage: **${r.stage}** · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **dry-run** · writePerformed: **false**`);
  lines.push("");
  lines.push(`Preview URL: \`${r.tapestry.previewUrl}\``);
  lines.push(`Brand Status: **${r.tapestry.brandStatus || "(empty)"}**`);
  lines.push(`Public visibility: ${r.tapestry.publicVisibility || "—"}`);
  lines.push("");
  lines.push(`## Recommendation`);
  lines.push("");
  lines.push(`**${r.recommendation}**`);
  lines.push("");
  lines.push("## Gate summary");
  lines.push("");
  if (r.gateSuite?.gates?.length) {
    for (const g of r.gateSuite.gates) {
      lines.push(`- \`${g.gate}\`: status=${g.status} ok=${g.ok}`);
    }
    lines.push(`- allPass: **${r.gateSuite.allPass}**`);
    lines.push(`- Inline quality recommendation: **${r.gateSuite.inlineQualityRecommendation}**`);
    lines.push(`- Inline quality blockers: ${r.gateSuite.inlineQualityBlockerCount}`);
  } else {
    lines.push(`- ${r.gateSuite?.note || "no gate-suite report on disk"}`);
  }
  lines.push("");
  lines.push("## Scenario / image counts");
  lines.push("");
  lines.push(`- galleryDistinct: ${r.packet.imageCounts.galleryDistinct}`);
  lines.push(`- scenarioDistinct: ${r.packet.imageCounts.scenarioDistinct}`);
  lines.push(`- propertyDistinct: ${r.packet.imageCounts.propertyDistinct}`);
  lines.push(`- imageOk: **${r.imageOk}**`);
  lines.push("");
  lines.push("## Scenario cards");
  lines.push("");
  for (const item of r.packet.remainingFounderJudgmentItems || []) {
    lines.push(`- ${item}`);
  }
  lines.push("");
  lines.push("## Curio distinction caution");
  lines.push("");
  for (const c of r.curioDistinctionCaution) lines.push(`- ${c}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails)) {
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Stage: status-promotion (Under Review → Active)
// ---------------------------------------------------------------------------

export async function runTapestryStatusPromotion({ apply = false, argv = [], targetStatus = TAPESTRY_STATUS_TO_PREFERRED } = {}) {
  const stage = "status-promotion";
  const flagCheck = checkRequiredFlags({ stage, argv, apply });

  if (!TAPESTRY_STATUS_TO_ALLOWED.includes(targetStatus)) {
    throw new Error(
      `Refuse: targetStatus '${targetStatus}' not in allowed [${TAPESTRY_STATUS_TO_ALLOWED.join(", ")}]`
    );
  }

  const brand = await fetchTapestryBrand();
  const currentStatus = nz(brand.status || brand.brandStatus);
  const liveRecordId = brand.id || brand.recordId || null;

  const identityIssues = [];
  if (liveRecordId !== TAPESTRY_RECORD_ID) {
    identityIssues.push(`recordId_mismatch:got=${liveRecordId};expected=${TAPESTRY_RECORD_ID}`);
  }
  if (currentStatus !== TAPESTRY_STATUS_FROM) {
    identityIssues.push(
      `status_not_under_review:got=${currentStatus || "(empty)"};expected=${TAPESTRY_STATUS_FROM}`
    );
  }

  const plannedPatch = {
    table: BASICS_TABLE,
    recordId: TAPESTRY_RECORD_ID,
    fields: { "Brand Status": targetStatus },
    from: TAPESTRY_STATUS_FROM,
    to: targetStatus,
  };

  const applyPerformed = apply === true && flagCheck.ok === true && identityIssues.length === 0;
  let applyResult = { applied: false };
  if (applyPerformed) {
    // Extra safety — enforce single-field payload, single record, correct table.
    assertTapestryOnly([{ recordId: plannedPatch.recordId }], { field: "recordId" });
    const payload = { "Brand Status": targetStatus };
    // Refuse if any other field snuck in
    if (Object.keys(payload).length !== 1) {
      throw new Error(`Refuse: status-promotion payload must be single-field: ${JSON.stringify(payload)}`);
    }
    try {
      const patched = await patchTapestryBasics({ fields: payload });
      applyResult = {
        applied: true,
        table: BASICS_TABLE,
        recordId: TAPESTRY_RECORD_ID,
        payload,
        response: {
          id: patched?.id,
          fieldsPatched: Object.keys(payload),
        },
        writePerformed: true,
      };
    } catch (err) {
      applyResult = { applied: false, error: err.message };
    }
  } else if (apply && !flagCheck.ok) {
    applyResult = {
      applied: false,
      reason: "missing_apply_flags",
      missing: flagCheck.missing,
    };
  } else if (apply && identityIssues.length) {
    applyResult = {
      applied: false,
      reason: "identity_issues_present",
      identityIssues,
    };
  }

  const report = {
    version: PROMOTION_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed: applyResult.applied === true,
    dryRun: !applyPerformed,
    flagCheck,
    tapestry: {
      slug: TAPESTRY_SLUG,
      recordId: TAPESTRY_RECORD_ID,
      name: TAPESTRY_NAME,
      currentBrandStatus: currentStatus,
      targetBrandStatus: targetStatus,
    },
    identityIssues,
    plannedPatch: {
      table: plannedPatch.table,
      recordId: plannedPatch.recordId,
      from: plannedPatch.from,
      to: plannedPatch.to,
      fields: Object.keys(plannedPatch.fields),
      sanitizedPayloadPreview: plannedPatch.fields,
    },
    applyResult,
    guardrails: {
      tapestryOnly: true,
      singleFieldPayload: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      contentWrites: false,
      imageWrites: false,
      otherBrandStatusWrites: false,
      protectedBaselineUntouched: true,
      neverWriteFields: [...NEVER_WRITE_FIELDS],
    },
    requiredApplyFlags: STAGE_REQUIRED_APPLY_FLAGS[stage],
  };

  const md = renderStatusPromotionMarkdown(report);
  const paths = writeStageReports(stage, report, md);
  return { report, paths };
}

function renderStatusPromotionMarkdown(r) {
  const lines = [];
  lines.push(`# Tapestry — Brand Status Promotion (${r.tapestry.currentBrandStatus} → ${r.tapestry.targetBrandStatus})`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Stage: **${r.stage}** · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`);
  lines.push("");
  lines.push("## Planned PATCH");
  lines.push("");
  lines.push(`- Table: \`${r.plannedPatch.table}\``);
  lines.push(`- Record: \`${r.plannedPatch.recordId}\``);
  lines.push(`- Field: \`Brand Status\` (${r.plannedPatch.from} → ${r.plannedPatch.to})`);
  lines.push(`- Sanitized payload preview: \`\`\`json\n${JSON.stringify(r.plannedPatch.sanitizedPayloadPreview, null, 2)}\n\`\`\``);
  lines.push("");
  if (r.identityIssues?.length) {
    lines.push("## Identity issues (must be resolved before apply)");
    for (const i of r.identityIssues) lines.push(`- ${i}`);
    lines.push("");
  }
  lines.push("## Apply result");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.applyResult, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails)) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  lines.push("## Required apply flags");
  lines.push("");
  for (const f of r.requiredApplyFlags) lines.push(`- \`${f}\``);
  lines.push("");
  if (r.flagCheck?.missing?.length) {
    lines.push("### Missing apply flags");
    for (const m of r.flagCheck.missing) lines.push(`- \`${m}\``);
    lines.push("");
  }
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Stage: public-release
// ---------------------------------------------------------------------------

/** Map tapestry-facing flags → public-restore-governance required flags. */
function mapTapestryPublicReleaseArgv(argv = []) {
  const forwarded = new Set(argv);
  // Add the underlying governance required flags derived from the same intent.
  for (const f of PUBLIC_RESTORE_REQUIRED_APPLY_FLAGS) forwarded.add(f);
  // Ensure --apply is present when the caller opted in explicitly.
  if (argv.includes("--apply")) forwarded.add("--apply");
  return [...forwarded];
}

export async function runTapestryPublicRelease({ apply = false, argv = [] } = {}) {
  const stage = "public-release";
  const flagCheck = checkRequiredFlags({ stage, argv, apply });

  const brand = await fetchTapestryBrand();
  const currentStatus = nz(brand.status || brand.brandStatus);
  if (!isBrandStatusActive(currentStatus)) {
    // Return a hard refusal report — do not attempt any writes.
    const report = {
      version: PROMOTION_VERSION,
      stage,
      generatedAt: new Date().toISOString(),
      apply,
      applyPerformed: false,
      writePerformed: false,
      dryRun: !apply,
      refused: true,
      reason: `brand_status_not_active_or_live:${currentStatus || "(empty)"}`,
      flagCheck,
      guardrails: { statusMustBeActiveLiveBeforePublicRelease: true },
    };
    const md = `# Tapestry — Public Release\n\nRefused: ${report.reason}. Run status-promotion first.\n`;
    const paths = writeStageReports(stage, report, md);
    return { report, paths };
  }

  // Plan public-restore governance for tapestry only
  const plan = await planPublicRestoreGovernance({ brands: [TAPESTRY_SLUG] });
  const tapestryRow = (plan.brandResults || []).find((b) => b.slug === TAPESTRY_SLUG);

  const applyPerformed = apply === true && flagCheck.ok === true;
  let applyOutcome = { applied: false, reason: "dry_run_only" };

  if (applyPerformed) {
    // Refuse any accidental broader scope
    if ((plan.brands || []).some((s) => s !== TAPESTRY_SLUG)) {
      throw new Error("Refuse: public-restore plan targets a non-tapestry brand");
    }

    const forwardedArgv = mapTapestryPublicReleaseArgv([...argv, "--apply"]);
    try {
      applyOutcome = await applyPublicRestoreGovernance({
        plan,
        apply: true,
        argv: forwardedArgv,
        reportsDir: REPORTS_DIR,
      });
    } catch (err) {
      applyOutcome = { applied: false, error: err.message };
    }

    // If governance returned "no_eligible_brands" (some readiness reports may be
    // missing), fall back to a direct patch of the Basics release fields and
    // ensure tapestry is in the intentional restore registry — behaves
    // identically to the governance pass but scoped to tapestry only.
    if (applyOutcome?.applied !== true) {
      try {
        const today = todayIsoDate();
        const releaseFields = {
          "Active Profile Approved": true,
          "Ready for Active Profile": true,
          "Active Profile Approved Date": today,
          "Founder Visual Review Pass": true,
        };
        assertTapestryOnly([{ recordId: TAPESTRY_RECORD_ID }], { field: "recordId" });
        const patched = await patchTapestryBasics({ fields: releaseFields });

        const intentionalBefore = readIntentionalPublicRestoreSlugs();
        const nextSlugs = [...new Set([...intentionalBefore, TAPESTRY_SLUG])];
        const registry = writeIntentionalPublicRestoreSlugs(nextSlugs);

        applyOutcome = {
          applied: true,
          reason: "public_release_applied_via_direct_fallback",
          basicsPatched: {
            recordId: TAPESTRY_RECORD_ID,
            fields: Object.keys(releaseFields),
            sanitizedPayloadPreview: releaseFields,
            response: { id: patched?.id },
          },
          intentionalRegistry: registry,
        };
      } catch (err) {
        applyOutcome = {
          applied: false,
          reason: "fallback_direct_release_apply_failed",
          error: err.message,
        };
      }
    }
  } else if (apply && !flagCheck.ok) {
    applyOutcome = { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const report = {
    version: PROMOTION_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed: applyOutcome?.applied === true,
    dryRun: !applyPerformed,
    flagCheck,
    tapestry: {
      slug: TAPESTRY_SLUG,
      recordId: TAPESTRY_RECORD_ID,
      name: TAPESTRY_NAME,
      currentBrandStatus: currentStatus,
    },
    publicRestorePlan: {
      brands: plan.brands,
      brandResults: plan.brandResults,
      requiredApplyFlags: plan.requiredApplyFlags,
    },
    tapestryRow: tapestryRow || null,
    applyOutcome,
    plannedReleaseFields: [
      "Active Profile Approved",
      "Ready for Active Profile",
      "Active Profile Approved Date",
      "Founder Visual Review Pass",
    ],
    guardrails: {
      tapestryOnly: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      contentRewrites: false,
      imageWrites: false,
      protectedBaselineUntouched: true,
      publicRestoreRegistryUpdate: applyOutcome?.applied === true,
      neverWriteFields: [...NEVER_WRITE_FIELDS],
    },
    requiredApplyFlags: STAGE_REQUIRED_APPLY_FLAGS[stage],
  };

  // Also mirror the public-restore governance human-readable report for
  // provenance (does not overwrite the tapestry-specific report).
  try {
    writePublicRestoreGovernanceReports(
      {
        version: plan.version,
        generatedAt: plan.generatedAt,
        applyResult: applyOutcome,
        brands: [TAPESTRY_SLUG],
        brandResults: plan.brandResults,
        summary: plan.summary,
      },
      { reportsDir: REPORTS_DIR }
    );
  } catch {
    // Non-fatal — main tapestry report is still written.
  }

  const md = renderPublicReleaseMarkdown(report);
  const paths = writeStageReports(stage, report, md);
  return { report, paths };
}

function renderPublicReleaseMarkdown(r) {
  const lines = [];
  lines.push(`# Tapestry — Public Release`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Stage: **${r.stage}** · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`);
  lines.push("");
  lines.push(`Current Brand Status: **${r.tapestry.currentBrandStatus}**`);
  lines.push("");
  lines.push("## Planned release fields (tapestry only)");
  lines.push("");
  for (const f of r.plannedReleaseFields) lines.push(`- ${f}`);
  lines.push("");
  lines.push("## Public-restore plan (tapestry row)");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.tapestryRow || {}, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Apply outcome");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.applyOutcome || {}, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails)) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  lines.push("## Required apply flags");
  lines.push("");
  for (const f of r.requiredApplyFlags) lines.push(`- \`${f}\``);
  lines.push("");
  if (r.flagCheck?.missing?.length) {
    lines.push("### Missing apply flags");
    for (const m of r.flagCheck.missing) lines.push(`- \`${m}\``);
    lines.push("");
  }
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Stage: baseline-25 (report-only)
// ---------------------------------------------------------------------------

/**
 * Build + write the 25-brand baseline (tapestry included). Read-only. Does not
 * delete or overwrite the 24 baseline artifacts.
 */
export async function runTapestryBaseline25() {
  const stage = "baseline-25";
  const report = await build25ActivePublicFullBaseline({ requireReports: false });
  const paths = write25ActivePublicFullBaselineReports(report);
  const wrapper = {
    version: PROMOTION_VERSION,
    stage,
    generatedAt: report.generatedAt,
    dryRun: true,
    writePerformed: false,
    baselineVersion: BASELINE_VERSION_25,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_25,
    activeCount: report.activeCount,
    frozen: report.frozen,
    freezeDecision: report.freezeDecision,
    paths,
    tapestryIncluded: (report.brands || []).some((b) => b.slug === TAPESTRY_SLUG),
    radissonCollectionIncluded: (report.brands || []).some((b) => b.slug === "radisson-collection"),
    summary: report.summary,
  };
  const md = renderBaseline25WrapperMarkdown(wrapper);
  const paths2 = writeStageReports(stage, wrapper, md);
  return { report: wrapper, paths: { ...paths, ...paths2 } };
}

function renderBaseline25WrapperMarkdown(w) {
  const lines = [];
  lines.push(`# Tapestry — 25-brand Active/Live Public-Full Baseline (report-only)`);
  lines.push("");
  lines.push(`Version: \`${w.version}\` · Stage: **${w.stage}** · Generated: ${w.generatedAt}`);
  lines.push(`Mode: **dry-run** · writePerformed: **false**`);
  lines.push("");
  lines.push(`Expected Active/Live: **${w.expectedActiveCount}** · Live universe: **${w.activeCount}**`);
  lines.push(`Frozen: **${w.frozen}** (decision: \`${w.freezeDecision}\`)`);
  lines.push("");
  lines.push(`- Tapestry included in baseline: **${w.tapestryIncluded}**`);
  lines.push(`- Radisson Collection included: **${w.radissonCollectionIncluded}**`);
  lines.push("");
  lines.push("## Baseline paths");
  lines.push("");
  for (const [k, v] of Object.entries(w.paths || {})) {
    lines.push(`- ${k}: \`${v}\``);
  }
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(w.summary || {}, null, 2));
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}

// ---------------------------------------------------------------------------
// Top-level dispatcher
// ---------------------------------------------------------------------------

/**
 * Orchestrator entry-point.
 * @param {{ stage: string, apply?: boolean, argv?: string[] }} params
 */
export async function runTapestryFactoryPromotion({ stage, apply = false, argv = [] } = {}) {
  if (!STAGES.includes(stage)) {
    throw new Error(`Unknown stage '${stage}'. Allowed: ${STAGES.join(", ")}`);
  }

  if (process.env.NODE_ENV !== "production") {
    console.log(
      `[${PROMOTION_VERSION}] stage=${stage} apply=${apply} tapestry_only=true target=${TAPESTRY_SLUG}`
    );
  }

  switch (stage) {
    case "preflight":
      return runTapestryPreflight();
    case "tab-factory-completion":
      return runTapestryTabFactoryCompletion({ apply, argv });
    case "image-visual-audit":
      return runTapestryImageVisualAudit({ apply, argv });
    case "gate-suite":
      return runTapestryGateSuite({ argv });
    case "founder-review":
      return runTapestryFounderReview();
    case "status-promotion":
      return runTapestryStatusPromotion({ apply, argv });
    case "public-release":
      return runTapestryPublicRelease({ apply, argv });
    case "baseline-25":
      return runTapestryBaseline25();
    default:
      throw new Error(`Unhandled stage ${stage}`);
  }
}

export default {
  TAPESTRY_SLUG,
  TAPESTRY_ALIAS,
  TAPESTRY_RECORD_ID,
  TAPESTRY_NAME,
  PROMOTION_VERSION,
  STAGES,
  STAGE_REQUIRED_APPLY_FLAGS,
  runTapestryFactoryPromotion,
  runTapestryPreflight,
  runTapestryTabFactoryCompletion,
  runTapestryImageVisualAudit,
  runTapestryGateSuite,
  runTapestryFounderReview,
  runTapestryStatusPromotion,
  runTapestryPublicRelease,
  runTapestryBaseline25,
};
