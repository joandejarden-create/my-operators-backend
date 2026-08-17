/**
 * Wave 15 Stage 9 — Full Brand Status promotion (Under Review → Active).
 *
 * Scope: all eight Hilton Worldwide founder-approved brands. Unlike Wave 14
 * (Four Points Flex by Sheraton held INSIDE the Marriott cohort), no slug in
 * the Wave 15 eight is held — Four Points Flex is held OUTSIDE this cohort
 * entirely (it is not a Hilton brand and was never part of Wave 15). Preflight
 * still verifies Flex remains Under Review and outside the active universe,
 * but no write ever targets it. House of Originals / Morgans Originals /
 * Radisson Collection remain excluded. Allowed write: Brand Status only on
 * the eight Basics records.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  WAVE15_VERSION,
  WAVE15_SLUGS,
  WAVE15_PROMOTION_SLUGS,
  WAVE15_PROTECTED_BASELINE_COUNT,
  WAVE15_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE15_STATUS_FROM,
  WAVE15_STATUS_TO_PREFERRED,
  WAVE15_STATUS_TO_ALLOWED,
  WAVE15_STATUS_PROMOTION_APPLY_FLAGS,
  WAVE15_NEVER_WRITE_FIELDS,
  WAVE15_FOUNDER_APPROVE_RECOMMENDATION,
  WAVE15_FLEX_HELD_SLUG,
  WAVE15_FLEX_HELD_RECORD_ID,
} from "./brand-explorer-wave15-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { run54ActivePublicFullBaselineRegression } from "./brand-explorer-54-active-public-full-baseline.js";

export const WAVE15_STATUS_PROMOTION_VERSION = "wave15-status-promotion-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 280;
const HOUSE_SLUG = "the-house-of-originals";
const MORGANS_SLUG = "morgans-originals";
const RADISSON_COLLECTION_SLUG = "radisson-collection";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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

function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...required],
  };
}

function readJsonSafe(relOrAbs) {
  const p = path.isAbsolute(relOrAbs) ? relOrAbs : path.join(ROOT, relOrAbs);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[wave15-status-promotion] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function loadProtected54RecordIds() {
  const freeze =
    readJsonSafe("reports/brand-explorer-54-active-public-full-baseline.json") ||
    readJsonSafe("docs/data-intelligence/brand-explorer-54-active-public-full-baseline.json");
  const ids = new Set();
  for (const b of freeze?.brands || []) {
    const id = nz(b.recordId || b.id);
    if (id) ids.add(id);
  }
  return ids;
}

function promotionIdentities() {
  return WAVE15_PROMOTION_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
    return { slug, name: id.name, recordId: id.recordId };
  });
}

/** Four Points Flex — verified held OUTSIDE the Wave 15 cohort. Never written. */
function flexHeldIdentity() {
  return {
    slug: WAVE15_FLEX_HELD_SLUG,
    name: "Four Points Flex by Sheraton",
    recordId: WAVE15_FLEX_HELD_RECORD_ID,
  };
}

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`fetch failed ${recordId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function patchBasicsBrandStatus({ recordId, targetStatus }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const fields = { "Brand Status": targetStatus };
  for (const k of Object.keys(fields)) {
    if (WAVE15_NEVER_WRITE_FIELDS.filter((f) => f !== "Brand Status").includes(k)) {
      throw new Error(`Refuse: never-write field in payload: ${k}`);
    }
  }
  if (Object.keys(fields).length !== 1 || !("Brand Status" in fields)) {
    throw new Error(`Refuse: status-promotion payload must be Brand Status only: ${JSON.stringify(fields)}`);
  }
  if (recordId === WAVE15_FLEX_HELD_RECORD_ID) {
    throw new Error("Refuse: Four Points Flex is held outside the Wave 15 cohort — no Brand Status write");
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
  return { id: json.id, fieldsPatched: Object.keys(fields), sanitizedPayloadPreview: fields };
}

async function runProtected54BaselineCheck({ skip = false } = {}) {
  if (skip) return { ok: true, skipped: true, reason: "skipBaselineNpm" };
  try {
    console.log("[wave15-status-promotion] confirming protected 54 baseline…");
    const result = await run54ActivePublicFullBaselineRegression({
      allowCachedPvqlIfPass: true,
      maxPvqlAgeMs: 24 * 60 * 60 * 1000,
      forceLivePvql: false,
    });
    const ok = result?.regression?.pass === true;
    return {
      ok,
      status: ok ? 0 : 1,
      error: ok ? null : "protected_54_baseline_regression_failed",
      pvqlSource: result?.pvqlSource || null,
      liveUniverseCount: result?.liveUniverseCount ?? null,
      failures: (result?.regression?.failures || []).slice(0, 20),
      tail: [
        `pass=${ok}`,
        `liveCount=${result?.liveUniverseCount ?? "?"}`,
        `pvqlSource=${result?.pvqlSource || "?"}`,
      ],
    };
  } catch (err) {
    return { ok: false, status: 1, error: err.message, tail: [err.message] };
  }
}

function assertFounderApprovals() {
  const summary = readJsonSafe("reports/brand-explorer-wave15-founder-review-summary.json");
  const issues = [];
  if (!summary) {
    return { ok: false, issues: ["missing_founder_review_summary"], summary: null };
  }
  const brands = summary.brands || [];
  for (const slug of WAVE15_PROMOTION_SLUGS) {
    const row = brands.find((b) => nz(b.brandSlug || b.slug).toLowerCase() === slug);
    const rec = nz(row?.recommendation);
    if (rec !== WAVE15_FOUNDER_APPROVE_RECOMMENDATION) {
      issues.push(`founder_not_approve:${slug}:${rec || "(missing)"}`);
    }
    if (row?.holdForPromotion === true) {
      issues.push(`founder_hold_unexpected_on_approved:${slug}`);
    }
  }
  const approveCount = (summary.counts || {})[WAVE15_FOUNDER_APPROVE_RECOMMENDATION] || 0;
  if (approveCount !== WAVE15_PROMOTION_SLUGS.length) {
    issues.push(
      `founder_approve_count_mismatch:got=${approveCount};expected=${WAVE15_PROMOTION_SLUGS.length}`
    );
  }
  const flexNote = nz(summary.heldExcluded?.fourPointsFlex?.status);
  if (flexNote && !/held|under review/i.test(flexNote)) {
    issues.push(`founder_summary_flex_note_unexpected:${flexNote}`);
  }
  return { ok: issues.length === 0, issues, summary };
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave15-status-promotion.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave15-status-promotion.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath };
}

function renderMarkdown(r) {
  const lines = [
    `# Brand Explorer Wave 15 — Status Promotion`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    ``,
    `Target Brand Status: **${r.targetBrandStatus}**`,
    `Active universe before: **${r.universeBefore?.totalCount ?? "n/a"}** (expected ${WAVE15_PROTECTED_BASELINE_COUNT})`,
  ];
  if (r.universeAfter) {
    lines.push(
      `Active universe after: **${r.universeAfter.totalCount}** (expected ${WAVE15_EXPECTED_FINAL_ACTIVE_COUNT})`
    );
  }
  lines.push(
    ``,
    `## Scope`,
    ``,
    `- Promote (8, all — no held slug in cohort): ${WAVE15_PROMOTION_SLUGS.map((s) => `\`${s}\``).join(", ")}`,
    `- Four Points Flex by Sheraton: held **outside** the Wave 15 cohort (verified Under Review, not written)`,
    `- Excluded: House of Originals · Morgans Originals · Radisson Collection`,
    ``,
    `## Preflight`,
    ``,
    `- Protected 54 baseline: **${r.preflight?.baseline54?.ok ? "PASS" : "FAIL"}**`,
    `- Founder eight approvals: **${r.preflight?.founderApprovals?.ok ? "PASS" : "FAIL"}**`,
    `- Four Points Flex verified held (Under Review, not in active universe): **${r.preflight?.flexHeldOk ? "PASS" : "FAIL"}** (${r.held?.from || "—"})`,
    `- Status gate (Under Review → Active): **${r.preflight?.statusGateOk ? "PASS" : "FAIL"}**`
  );
  if (r.preflight?.issues?.length) {
    for (const i of r.preflight.issues) lines.push(`  - ${i}`);
  }
  lines.push(
    ``,
    `## Planned patches (Brand Status only)`,
    ``,
    `| Slug | Record | From | To | Needs write |`,
    `| --- | --- | --- | --- | --- |`
  );
  for (const b of r.brands || []) {
    lines.push(
      `| ${b.slug} | \`${b.recordId}\` | ${b.from || "(empty)"} | ${b.to} | ${b.needsWrite} |`
    );
  }
  lines.push(
    ``,
    `## Apply results`,
    ``,
    "```json",
    JSON.stringify(r.applyResults, null, 2),
    "```",
    ``,
    `## Guardrails`,
    ``
  );
  for (const [k, v] of Object.entries(r.guardrails || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[], targetStatus?: string, skipBaselineNpm?: boolean }} opts
 */
export async function runWave15StatusPromotion({
  apply = false,
  argv = [],
  targetStatus = WAVE15_STATUS_TO_PREFERRED,
  skipBaselineNpm = false,
} = {}) {
  const stage = "status-promotion";
  if (!WAVE15_STATUS_TO_ALLOWED.includes(targetStatus)) {
    throw new Error(
      `Refuse: targetStatus '${targetStatus}' not in allowed [${WAVE15_STATUS_TO_ALLOWED.join(", ")}]`
    );
  }
  if (targetStatus !== "Active" && argv.includes("--confirm-status-to-active")) {
    throw new Error("Refuse: --confirm-status-to-active requires target Active");
  }

  const flagCheck = checkFlags(WAVE15_STATUS_PROMOTION_APPLY_FLAGS, argv, apply);
  const identities = promotionIdentities();
  const held = flexHeldIdentity();
  const protectedIds = loadProtected54RecordIds();
  const preflightIssues = [];

  for (const id of identities) {
    if (protectedIds.has(id.recordId)) {
      preflightIssues.push(`target_collides_with_protected_54:${id.slug}:${id.recordId}`);
    }
    if ([HOUSE_SLUG, MORGANS_SLUG, RADISSON_COLLECTION_SLUG, WAVE15_FLEX_HELD_SLUG].includes(id.slug)) {
      preflightIssues.push(`forbidden_slug_in_promotion_targets:${id.slug}`);
    }
  }
  if (identities.length !== WAVE15_SLUGS.length) {
    preflightIssues.push(
      `promotion_scope_mismatch:got=${identities.length};expected=${WAVE15_SLUGS.length}`
    );
  }

  const founderApprovals = assertFounderApprovals();
  if (!founderApprovals.ok) preflightIssues.push(...founderApprovals.issues);

  const baseline54 = await runProtected54BaselineCheck({ skip: skipBaselineNpm });
  if (!baseline54.ok) preflightIssues.push("protected_54_baseline_failed");

  const universeBefore = await loadActiveUniverse({ includeDetails: false });
  if (
    universeBefore.totalCount !== WAVE15_PROTECTED_BASELINE_COUNT &&
    universeBefore.totalCount !== WAVE15_EXPECTED_FINAL_ACTIVE_COUNT
  ) {
    preflightIssues.push(
      `active_universe_count_unexpected:got=${universeBefore.totalCount};expected=${WAVE15_PROTECTED_BASELINE_COUNT}_or_${WAVE15_EXPECTED_FINAL_ACTIVE_COUNT}`
    );
  }

  const waveAlreadyActive = (universeBefore.brands || []).filter((b) =>
    WAVE15_PROMOTION_SLUGS.includes(nz(b.slug).toLowerCase())
  );
  if (
    universeBefore.totalCount === WAVE15_PROTECTED_BASELINE_COUNT &&
    waveAlreadyActive.length > 0
  ) {
    preflightIssues.push(
      `wave15_already_in_active_universe:${waveAlreadyActive.map((b) => b.slug).join(",")}`
    );
  }

  const flexInUniverseBefore = (universeBefore.brands || []).some(
    (b) => nz(b.slug).toLowerCase() === WAVE15_FLEX_HELD_SLUG
  );
  if (flexInUniverseBefore) {
    preflightIssues.push("flex_unexpectedly_in_active_universe_before_promotion");
  }

  const brands = [];
  for (const id of identities) {
    const live = await fetchBrand(id.recordId);
    await sleep(90);
    const current = nz(live.brandStatus || live.status);
    if (current !== WAVE15_STATUS_FROM && current !== targetStatus) {
      preflightIssues.push(`status_unexpected:${id.slug}:got=${current || "(empty)"}`);
    }
    if (isBrandStatusActive(current) && universeBefore.totalCount === WAVE15_PROTECTED_BASELINE_COUNT) {
      preflightIssues.push(`already_active_before_promotion:${id.slug}`);
    }
    brands.push({
      slug: id.slug,
      name: id.name,
      recordId: id.recordId,
      from: current,
      to: targetStatus,
      needsWrite: current !== targetStatus,
      alreadyActiveOrLive: isBrandStatusActive(current),
      plannedFields: ["Brand Status"],
      sanitizedPayloadPreview: { "Brand Status": targetStatus },
    });
  }

  const flexLive = await fetchBrand(held.recordId);
  await sleep(90);
  const flexStatus = nz(flexLive.brandStatus || flexLive.status);
  const flexHeldOk =
    flexStatus === WAVE15_STATUS_FROM && !isBrandStatusActive(flexStatus) && !flexInUniverseBefore;
  if (flexStatus !== WAVE15_STATUS_FROM || isBrandStatusActive(flexStatus)) {
    preflightIssues.push(`flex_not_under_review:got=${flexStatus || "(empty)"}`);
  }

  const statusGateOk = brands.every(
    (b) => b.from === WAVE15_STATUS_FROM || b.from === targetStatus
  );

  const preflightOk =
    preflightIssues.length === 0 &&
    founderApprovals.ok &&
    baseline54.ok &&
    statusGateOk &&
    flexHeldOk &&
    brands.every((b) => !protectedIds.has(b.recordId));

  const applyPerformed = apply === true && flagCheck.ok === true && preflightOk;
  const applyResults = [];
  let writePerformed = false;

  if (applyPerformed) {
    for (const b of brands) {
      if (!b.needsWrite) {
        applyResults.push({
          slug: b.slug,
          recordId: b.recordId,
          applied: false,
          reason: "already_at_target",
          writePerformed: false,
        });
        continue;
      }
      try {
        const response = await patchBasicsBrandStatus({
          recordId: b.recordId,
          targetStatus,
        });
        writePerformed = true;
        applyResults.push({
          slug: b.slug,
          recordId: b.recordId,
          applied: true,
          writePerformed: true,
          table: BASICS_TABLE,
          fieldMapping: { brandStatus: "Brand Status" },
          sanitizedPayloadPreview: response.sanitizedPayloadPreview,
          response: { id: response.id, fieldsPatched: response.fieldsPatched },
        });
      } catch (err) {
        applyResults.push({
          slug: b.slug,
          recordId: b.recordId,
          applied: false,
          writePerformed: false,
          error: err.message,
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
  } else if (apply && !flagCheck.ok) {
    applyResults.push({ applied: false, reason: "missing_apply_flags", missing: flagCheck.missing });
  } else if (apply && !preflightOk) {
    applyResults.push({ applied: false, reason: "preflight_failed", issues: preflightIssues });
  }

  let universeAfter = null;
  if (applyPerformed) {
    await sleep(500);
    universeAfter = await loadActiveUniverse({ includeDetails: false });
  }

  const promotedCount = applyResults.filter((r) => r.applied === true).length;

  const report = {
    version: WAVE15_STATUS_PROMOTION_VERSION,
    waveVersion: WAVE15_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    targetBrandStatus: targetStatus,
    allowedStatuses: [...WAVE15_STATUS_TO_ALLOWED],
    statusFromExpected: WAVE15_STATUS_FROM,
    flagCheck,
    requiredApplyFlags: [...WAVE15_STATUS_PROMOTION_APPLY_FLAGS],
    preflight: {
      ok: preflightOk,
      issues: preflightIssues,
      baseline54,
      founderApprovals: { ok: founderApprovals.ok, issues: founderApprovals.issues },
      statusGateOk,
      flexHeldOk,
      protected54RecordIdCount: protectedIds.size,
    },
    held: {
      slug: held.slug,
      recordId: held.recordId,
      from: flexStatus,
      to: "(unchanged)",
      needsWrite: false,
      insideCohort: false,
      note: "Four Points Flex is not a Hilton brand and was never part of Wave 15 — held outside this cohort entirely.",
    },
    universeBefore: {
      totalCount: universeBefore.totalCount,
      expected: WAVE15_PROTECTED_BASELINE_COUNT,
      wave15Present: waveAlreadyActive.map((b) => b.slug),
      flexPresent: flexInUniverseBefore,
    },
    universeAfter: universeAfter
      ? {
          totalCount: universeAfter.totalCount,
          expected: WAVE15_EXPECTED_FINAL_ACTIVE_COUNT,
          wave15Present: (universeAfter.brands || [])
            .filter((b) => WAVE15_PROMOTION_SLUGS.includes(nz(b.slug).toLowerCase()))
            .map((b) => b.slug),
          flexPresent: (universeAfter.brands || []).some(
            (b) => nz(b.slug).toLowerCase() === WAVE15_FLEX_HELD_SLUG
          ),
        }
      : null,
    summary: {
      brandCount: brands.length,
      needsWrite: brands.filter((b) => b.needsWrite).length,
      promotedCount,
      heldCount: 0,
      acceptanceUniverse62:
        (universeAfter?.totalCount ?? null) === WAVE15_EXPECTED_FINAL_ACTIVE_COUNT ||
        (universeBefore.totalCount === WAVE15_EXPECTED_FINAL_ACTIVE_COUNT &&
          brands.every((b) => !b.needsWrite)),
    },
    brands,
    applyResults,
    fieldMapping: { brandStatus: "Brand Status" },
    guardrails: {
      targetBrandsOnly: true,
      allEightApprovedNoHeldSlugInCohort: true,
      flexHeldOutsideCohort: true,
      flexUntouched: true,
      singleFieldPayload: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      contentWrites: false,
      imageWrites: false,
      releaseFieldWrites: false,
      protected54Untouched: true,
      protected54ReadOnlyValidation: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      neverWriteFields: [...WAVE15_NEVER_WRITE_FIELDS],
      baselineConvention: `frozen_${WAVE15_PROTECTED_BASELINE_COUNT}_were_Active`,
      writeThrottleMs: WRITE_THROTTLE_MS,
    },
    readyStatement: applyPerformed
      ? "wave15_status_promotion_applied_ready_for_public_release"
      : "wave15_status_promotion_dry_run",
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, report, paths, pass: preflightOk || applyPerformed, ok: preflightOk || !apply };
}
