/**
 * Wave 13 Stage 9 — Partial Brand Status promotion (Under Review → Active).
 *
 * Scope: six founder-approved brands only. SO/ held. House / Morgans / Radisson excluded.
 * Allowed write: Brand Status only on those six Basics records.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  WAVE13_VERSION,
  WAVE13_PARTIAL_PROMOTION_SLUGS,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_PROTECTED_BASELINE_COUNT,
  WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
  WAVE13_STATUS_PROMOTION_APPLY_FLAGS,
  WAVE13_STATUS_FROM,
  WAVE13_STATUS_TO_PREFERRED,
  WAVE13_STATUS_TO_ALLOWED,
  WAVE13_FOUNDER_APPROVE_RECOMMENDATION,
  WAVE13_FOUNDER_HOLD_RECOMMENDATION,
  WAVE13_NEVER_WRITE_FIELDS,
} from "./brand-explorer-wave13-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  EXPECTED_ACTIVE_COUNT_39,
  run39ActivePublicFullBaselineRegression,
} from "./brand-explorer-39-active-public-full-baseline.js";

export const WAVE13_STATUS_PROMOTION_VERSION = "wave13-partial-status-promotion-v1";

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
      console.warn("[wave13-status-promotion] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function loadProtected39RecordIds() {
  const freeze =
    readJsonSafe("reports/brand-explorer-39-active-public-full-baseline.json") ||
    readJsonSafe("docs/data-intelligence/brand-explorer-39-active-public-full-baseline.json");
  const ids = new Set();
  for (const b of freeze?.brands || []) {
    const id = nz(b.recordId || b.id);
    if (id) ids.add(id);
  }
  return ids;
}

function partialIdentities() {
  return WAVE13_PARTIAL_PROMOTION_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
    return { slug, name: id.name, recordId: id.recordId };
  });
}

function heldIdentity() {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[WAVE13_HELD_PROMOTION_SLUG];
  if (!id?.recordId) throw new Error(`Missing identity for held slug ${WAVE13_HELD_PROMOTION_SLUG}`);
  return { slug: WAVE13_HELD_PROMOTION_SLUG, name: id.name, recordId: id.recordId };
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
    if (WAVE13_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in payload: ${k}`);
    }
  }
  if (Object.keys(fields).length !== 1 || !("Brand Status" in fields)) {
    throw new Error(`Refuse: status-promotion payload must be Brand Status only: ${JSON.stringify(fields)}`);
  }
  if (recordId === heldIdentity().recordId) {
    throw new Error("Refuse: SO/ is held — no Brand Status write");
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

async function runProtected39BaselineCheck({ skip = false } = {}) {
  if (skip) return { ok: true, skipped: true, reason: "skipBaselineNpm" };
  try {
    console.log("[wave13-status-promotion] confirming protected 39 baseline…");
    const result = await run39ActivePublicFullBaselineRegression({
      allowCachedPvqlIfPass: true,
      maxPvqlAgeMs: 24 * 60 * 60 * 1000,
      forceLivePvql: false,
    });
    const ok = result?.regression?.pass === true;
    return {
      ok,
      status: ok ? 0 : 1,
      error: ok ? null : "protected_39_baseline_regression_failed",
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

function assertFounderPartialApprovals() {
  const summary = readJsonSafe("reports/brand-explorer-wave13-founder-review-summary.json");
  const issues = [];
  if (!summary) {
    return { ok: false, issues: ["missing_founder_review_summary"], summary: null };
  }
  const brands = summary.brands || [];
  for (const slug of WAVE13_PARTIAL_PROMOTION_SLUGS) {
    const row = brands.find((b) => nz(b.brandSlug || b.slug).toLowerCase() === slug);
    const rec = nz(row?.recommendation);
    if (rec !== WAVE13_FOUNDER_APPROVE_RECOMMENDATION) {
      issues.push(`founder_not_approve:${slug}:${rec || "(missing)"}`);
    }
    if (row?.holdForPromotion === true) {
      issues.push(`founder_hold_unexpected_on_approved:${slug}`);
    }
  }
  const so = brands.find(
    (b) => nz(b.brandSlug || b.slug).toLowerCase() === WAVE13_HELD_PROMOTION_SLUG
  );
  const soRec = nz(so?.recommendation);
  if (soRec !== WAVE13_FOUNDER_HOLD_RECOMMENDATION) {
    issues.push(`so_not_held_recommendation:got=${soRec || "(missing)"}`);
  }
  if (so && so.holdForPromotion !== true) {
    issues.push("so_holdForPromotion_not_true");
  }
  const approveCount = (summary.counts || {})[WAVE13_FOUNDER_APPROVE_RECOMMENDATION] || 0;
  if (approveCount !== WAVE13_PARTIAL_PROMOTION_SLUGS.length) {
    issues.push(
      `founder_approve_count_mismatch:got=${approveCount};expected=${WAVE13_PARTIAL_PROMOTION_SLUGS.length}`
    );
  }
  return { ok: issues.length === 0, issues, summary };
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-partial-status-promotion.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-partial-status-promotion.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath };
}

function renderMarkdown(r) {
  const lines = [
    `# Brand Explorer Wave 13 — Partial Status Promotion`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    ``,
    `Target Brand Status: **${r.targetBrandStatus}**`,
    `Active universe before: **${r.universeBefore?.totalCount ?? "n/a"}** (expected ${WAVE13_PROTECTED_BASELINE_COUNT})`,
  ];
  if (r.universeAfter) {
    lines.push(
      `Active universe after: **${r.universeAfter.totalCount}** (expected ${WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT})`
    );
  }
  lines.push(
    ``,
    `## Scope`,
    ``,
    `- Promote (6): ${WAVE13_PARTIAL_PROMOTION_SLUGS.map((s) => `\`${s}\``).join(", ")}`,
    `- Held: \`${WAVE13_HELD_PROMOTION_SLUG}\` (no Brand Status write)`,
    `- Excluded: House of Originals · Morgans Originals · Radisson Collection`,
    ``,
    `## Preflight`,
    ``,
    `- Protected 39 baseline: **${r.preflight?.baseline39?.ok ? "PASS" : "FAIL"}**`,
    `- Founder six-only approvals: **${r.preflight?.founderApprovals?.ok ? "PASS" : "FAIL"}**`,
    `- SO/ held Under Review: **${r.preflight?.soHeldOk ? "PASS" : "FAIL"}** (${r.held?.from || "—"})`,
    `- Status gate (Under Review → Active): **${r.preflight?.statusGateOk ? "PASS" : "FAIL"}**`,
    `- \`--approved-only\`: **${r.approvedOnly ? "yes" : "MISSING"}**`
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
export async function runWave13StatusPromotion({
  apply = false,
  argv = [],
  targetStatus = WAVE13_STATUS_TO_PREFERRED,
  skipBaselineNpm = false,
} = {}) {
  const stage = "status-promotion";
  if (!WAVE13_STATUS_TO_ALLOWED.includes(targetStatus)) {
    throw new Error(
      `Refuse: targetStatus '${targetStatus}' not in allowed [${WAVE13_STATUS_TO_ALLOWED.join(", ")}]`
    );
  }
  if (targetStatus !== "Active" && argv.includes("--confirm-status-to-active")) {
    throw new Error("Refuse: --confirm-status-to-active requires target Active");
  }

  const approvedOnly = argv.includes("--approved-only") || apply === true;
  if (!argv.includes("--approved-only")) {
    // Require explicit partial scope on dry-run and apply
    if (!apply) {
      // dry-run without flag still proceeds but records issue unless we require it
    }
  }
  const requireApprovedOnly = true;
  const flagCheck = checkFlags(WAVE13_STATUS_PROMOTION_APPLY_FLAGS, argv, apply);
  const identities = partialIdentities();
  const held = heldIdentity();
  const protectedIds = loadProtected39RecordIds();
  const preflightIssues = [];

  if (requireApprovedOnly && !argv.includes("--approved-only")) {
    preflightIssues.push("missing_--approved-only_partial_scope_flag");
  }

  for (const id of identities) {
    if (protectedIds.has(id.recordId)) {
      preflightIssues.push(`target_collides_with_protected_39:${id.slug}:${id.recordId}`);
    }
    if ([HOUSE_SLUG, MORGANS_SLUG, RADISSON_COLLECTION_SLUG, WAVE13_HELD_PROMOTION_SLUG].includes(id.slug)) {
      preflightIssues.push(`forbidden_slug_in_partial_targets:${id.slug}`);
    }
  }

  const founderApprovals = assertFounderPartialApprovals();
  if (!founderApprovals.ok) preflightIssues.push(...founderApprovals.issues);

  const baseline39 = await runProtected39BaselineCheck({ skip: skipBaselineNpm });
  if (!baseline39.ok) preflightIssues.push("protected_39_baseline_failed");

  const universeBefore = await loadActiveUniverse({ includeDetails: false });
  if (
    universeBefore.totalCount !== WAVE13_PROTECTED_BASELINE_COUNT &&
    universeBefore.totalCount !== WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT
  ) {
    preflightIssues.push(
      `active_universe_count_unexpected:got=${universeBefore.totalCount};expected=${WAVE13_PROTECTED_BASELINE_COUNT}_or_${WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT}`
    );
  }

  const waveAlreadyActive = (universeBefore.brands || []).filter((b) =>
    WAVE13_PARTIAL_PROMOTION_SLUGS.includes(nz(b.slug).toLowerCase())
  );
  if (
    universeBefore.totalCount === WAVE13_PROTECTED_BASELINE_COUNT &&
    waveAlreadyActive.length > 0
  ) {
    preflightIssues.push(
      `wave13_partial_already_in_active_universe:${waveAlreadyActive.map((b) => b.slug).join(",")}`
    );
  }

  const brands = [];
  for (const id of identities) {
    const live = await fetchBrand(id.recordId);
    await sleep(90);
    const current = nz(live.brandStatus || live.status);
    if (current !== WAVE13_STATUS_FROM && current !== targetStatus) {
      preflightIssues.push(`status_unexpected:${id.slug}:got=${current || "(empty)"}`);
    }
    if (isBrandStatusActive(current) && universeBefore.totalCount === WAVE13_PROTECTED_BASELINE_COUNT) {
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

  const soLive = await fetchBrand(held.recordId);
  await sleep(90);
  const soStatus = nz(soLive.brandStatus || soLive.status);
  const soHeldOk = soStatus === WAVE13_STATUS_FROM && !isBrandStatusActive(soStatus);
  if (!soHeldOk) {
    preflightIssues.push(`so_not_under_review:got=${soStatus || "(empty)"}`);
  }

  const statusGateOk = brands.every(
    (b) => b.from === WAVE13_STATUS_FROM || b.from === targetStatus
  );

  const preflightOk =
    preflightIssues.length === 0 &&
    founderApprovals.ok &&
    baseline39.ok &&
    statusGateOk &&
    soHeldOk &&
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
    version: WAVE13_STATUS_PROMOTION_VERSION,
    waveVersion: WAVE13_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    approvedOnly: argv.includes("--approved-only"),
    targetBrandStatus: targetStatus,
    allowedStatuses: [...WAVE13_STATUS_TO_ALLOWED],
    statusFromExpected: WAVE13_STATUS_FROM,
    flagCheck,
    requiredApplyFlags: [...WAVE13_STATUS_PROMOTION_APPLY_FLAGS],
    preflight: {
      ok: preflightOk,
      issues: preflightIssues,
      baseline39,
      founderApprovals: { ok: founderApprovals.ok, issues: founderApprovals.issues },
      statusGateOk,
      soHeldOk,
      protected39RecordIdCount: protectedIds.size,
    },
    held: {
      slug: held.slug,
      recordId: held.recordId,
      from: soStatus,
      to: "(unchanged)",
      needsWrite: false,
      recommendation: WAVE13_FOUNDER_HOLD_RECOMMENDATION,
    },
    universeBefore: {
      totalCount: universeBefore.totalCount,
      expected: WAVE13_PROTECTED_BASELINE_COUNT,
      wave13PartialPresent: waveAlreadyActive.map((b) => b.slug),
    },
    universeAfter: universeAfter
      ? {
          totalCount: universeAfter.totalCount,
          expected: WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
          wave13PartialPresent: (universeAfter.brands || [])
            .filter((b) =>
              WAVE13_PARTIAL_PROMOTION_SLUGS.includes(nz(b.slug).toLowerCase())
            )
            .map((b) => b.slug),
          soPresent: (universeAfter.brands || []).some(
            (b) => nz(b.slug).toLowerCase() === WAVE13_HELD_PROMOTION_SLUG
          ),
        }
      : null,
    summary: {
      brandCount: brands.length,
      needsWrite: brands.filter((b) => b.needsWrite).length,
      promotedCount,
      heldCount: 1,
      acceptanceUniverse45:
        (universeAfter?.totalCount ?? null) === WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT ||
        (universeBefore.totalCount === WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT &&
          brands.every((b) => !b.needsWrite)),
    },
    brands,
    applyResults,
    fieldMapping: { brandStatus: "Brand Status" },
    guardrails: {
      targetBrandsOnly: true,
      sixApprovedOnly: true,
      soHeld: true,
      singleFieldPayload: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      contentWrites: false,
      imageWrites: false,
      releaseFieldWrites: false,
      protected39Untouched: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      neverWriteFields: [...WAVE13_NEVER_WRITE_FIELDS],
      baselineConvention: `frozen_${EXPECTED_ACTIVE_COUNT_39}_were_Active`,
    },
    readyStatement: applyPerformed
      ? "wave13_partial_status_promotion_applied_ready_for_public_release"
      : "wave13_partial_status_promotion_dry_run",
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, report, paths, pass: preflightOk || applyPerformed, ok: preflightOk || !apply };
}
