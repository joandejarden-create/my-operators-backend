/**
 * Wave 12 Stage 9 — Brand Status promotion (Under Review → Active).
 *
 * Allowed write: Brand Status on the 12 Wave 12 Basics records only.
 * Target status matches protected-27 freeze convention (Active).
 *
 * Forbidden: CV / validation date / Source Library / Registry / release /
 * content / images / protected 27 / Radisson Collection.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  WAVE12_VERSION,
  WAVE12_SLUGS,
  WAVE12_PROTECTED_BASELINE_COUNT,
  WAVE12_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE12_STATUS_PROMOTION_APPLY_FLAGS,
  WAVE12_STATUS_FROM,
  WAVE12_STATUS_TO_PREFERRED,
  WAVE12_STATUS_TO_ALLOWED,
  WAVE12_FOUNDER_APPROVE_RECOMMENDATION,
  WAVE12_NEVER_WRITE_FIELDS,
} from "./brand-explorer-wave12-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  EXPECTED_ACTIVE_COUNT_27,
  run27ActivePublicFullBaselineRegression,
} from "./brand-explorer-27-active-public-full-baseline.js";

export const WAVE12_STATUS_PROMOTION_VERSION = "wave12-status-promotion-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 260;
const RADISSON_COLLECTION_SLUG = "radisson-collection";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

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
      console.warn("[wave12-status-promotion] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function loadProtected27RecordIds() {
  const freeze =
    readJsonSafe("reports/brand-explorer-27-active-public-full-baseline.json") ||
    readJsonSafe("docs/data-intelligence/brand-explorer-27-active-public-full-baseline.json");
  const ids = new Set();
  for (const b of freeze?.brands || []) {
    const id = nz(b.recordId || b.id);
    if (id) ids.add(id);
  }
  return ids;
}

function wave12Identities() {
  return WAVE12_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) {
      throw new Error(`Missing factory-preview identity for ${slug}`);
    }
    return { slug, name: id.name, recordId: id.recordId };
  });
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
    if (WAVE12_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in payload: ${k}`);
    }
  }
  if (Object.keys(fields).length !== 1 || !("Brand Status" in fields)) {
    throw new Error(`Refuse: status-promotion payload must be Brand Status only: ${JSON.stringify(fields)}`);
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

async function runProtected27BaselineCheck() {
  try {
    const result = await run27ActivePublicFullBaselineRegression({
      allowCachedPvqlIfPass: true,
      maxPvqlAgeMs: 24 * 60 * 60 * 1000,
      forceLivePvql: false,
    });
    const ok = result?.regression?.pass === true;
    return {
      ok,
      status: ok ? 0 : 1,
      error: ok ? null : "protected_27_baseline_regression_failed",
      pvqlSource: result?.pvqlSource || null,
      liveUniverseCount: result?.liveUniverseCount ?? null,
      failures: (result?.regression?.failures || []).slice(0, 20),
      tail: [
        `pass=${ok}`,
        `liveCount=${result?.liveUniverseCount ?? "?"}`,
        `pvqlSource=${result?.pvqlSource || "?"}`,
        ...((result?.regression?.failures || []).slice(0, 8) || []),
      ],
    };
  } catch (err) {
    return {
      ok: false,
      status: 1,
      error: err.message,
      tail: [err.message],
    };
  }
}

function assertFounderApprovals() {
  const summary = readJsonSafe("reports/brand-explorer-wave12-founder-review-summary.json");
  const issues = [];
  if (!summary) {
    issues.push("missing_founder_review_summary");
    return { ok: false, issues, summary: null };
  }
  const counts = summary.counts || {};
  const approveCount = counts[WAVE12_FOUNDER_APPROVE_RECOMMENDATION] || 0;
  if (approveCount !== WAVE12_SLUGS.length) {
    issues.push(
      `founder_approve_count_mismatch:got=${approveCount};expected=${WAVE12_SLUGS.length}`
    );
  }
  const brands = summary.brands || summary.brandResults || [];
  for (const slug of WAVE12_SLUGS) {
    const row =
      brands.find((b) => nz(b.slug || b.brandSlug).toLowerCase() === slug) ||
      (summary.bySlug && summary.bySlug[slug]);
    const rec = nz(row?.recommendation || row?.founderRecommendation);
    if (rec !== WAVE12_FOUNDER_APPROVE_RECOMMENDATION) {
      issues.push(`founder_not_approve:${slug}:${rec || "(missing)"}`);
    }
  }
  return { ok: issues.length === 0, issues, summary };
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave12-status-promotion.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave12-status-promotion.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath };
}

function renderMarkdown(r) {
  const lines = [];
  lines.push(`# Brand Explorer Wave 12 — Status Promotion`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`);
  lines.push("");
  lines.push(`Target Brand Status: **${r.targetBrandStatus}** (allowed: ${r.allowedStatuses.join(", ")})`);
  lines.push(`Active universe before: **${r.universeBefore?.totalCount ?? "n/a"}** (expected ${WAVE12_PROTECTED_BASELINE_COUNT})`);
  if (r.universeAfter) {
    lines.push(`Active universe after: **${r.universeAfter.totalCount}** (expected ${WAVE12_EXPECTED_FINAL_ACTIVE_COUNT})`);
  }
  lines.push("");
  lines.push("## Preflight");
  lines.push("");
  lines.push(`- Protected 27 baseline check: **${r.preflight?.baseline27?.ok ? "PASS" : "FAIL"}**`);
  lines.push(`- Founder approvals: **${r.preflight?.founderApprovals?.ok ? "PASS" : "FAIL"}**`);
  lines.push(`- All Under Review (or already target): **${r.preflight?.statusGateOk ? "PASS" : "FAIL"}**`);
  if (r.preflight?.issues?.length) {
    for (const i of r.preflight.issues) lines.push(`  - ${i}`);
  }
  lines.push("");
  lines.push("## Planned patches (Brand Status only)");
  lines.push("");
  lines.push("| Slug | Record | From | To | Needs write |");
  lines.push("| --- | --- | --- | --- | --- |");
  for (const b of r.brands || []) {
    lines.push(
      `| ${b.slug} | \`${b.recordId}\` | ${b.from || "(empty)"} | ${b.to} | ${b.needsWrite} |`
    );
  }
  lines.push("");
  lines.push("## Apply results");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.applyResults, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  lines.push("## Required apply flags");
  lines.push("");
  for (const f of r.requiredApplyFlags || []) lines.push(`- \`${f}\``);
  if (r.flagCheck?.missing?.length) {
    lines.push("");
    lines.push("### Missing");
    for (const m of r.flagCheck.missing) lines.push(`- \`${m}\``);
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[], targetStatus?: string, skipBaselineNpm?: boolean }} opts
 */
export async function runWave12StatusPromotion({
  apply = false,
  argv = [],
  targetStatus = WAVE12_STATUS_TO_PREFERRED,
  skipBaselineNpm = false,
} = {}) {
  const stage = "status-promotion";
  if (!WAVE12_STATUS_TO_ALLOWED.includes(targetStatus)) {
    throw new Error(
      `Refuse: targetStatus '${targetStatus}' not in allowed [${WAVE12_STATUS_TO_ALLOWED.join(", ")}]`
    );
  }

  const flagCheck = checkFlags(WAVE12_STATUS_PROMOTION_APPLY_FLAGS, argv, apply);
  const identities = wave12Identities();
  const protectedIds = loadProtected27RecordIds();
  const preflightIssues = [];

  // Identity / protected-record safety
  for (const id of identities) {
    if (protectedIds.has(id.recordId)) {
      preflightIssues.push(`target_collides_with_protected_27:${id.slug}:${id.recordId}`);
    }
    if (id.slug === RADISSON_COLLECTION_SLUG) {
      preflightIssues.push(`radisson_collection_in_targets`);
    }
  }

  const founderApprovals = assertFounderApprovals();
  if (!founderApprovals.ok) preflightIssues.push(...founderApprovals.issues);

  let baseline27 = { ok: true, skipped: true, reason: "skipBaselineNpm" };
  if (!skipBaselineNpm) {
    console.log("[wave12-status-promotion] confirming protected 27 baseline…");
    baseline27 = await runProtected27BaselineCheck();
    if (!baseline27.ok) preflightIssues.push("protected_27_baseline_failed");
  }

  const universeBefore = await loadActiveUniverse({ includeDetails: false });
  if (universeBefore.totalCount !== WAVE12_PROTECTED_BASELINE_COUNT) {
    // Allow re-run after partial promotion only when applying is blocked by other gates;
    // still record the mismatch for the report.
    if (universeBefore.totalCount !== WAVE12_EXPECTED_FINAL_ACTIVE_COUNT) {
      preflightIssues.push(
        `active_universe_count_unexpected:got=${universeBefore.totalCount};expected=${WAVE12_PROTECTED_BASELINE_COUNT}_or_${WAVE12_EXPECTED_FINAL_ACTIVE_COUNT}`
      );
    }
  }
  const waveAlreadyActive = (universeBefore.brands || []).filter((b) =>
    WAVE12_SLUGS.includes(nz(b.slug).toLowerCase())
  );
  if (
    universeBefore.totalCount === WAVE12_PROTECTED_BASELINE_COUNT &&
    waveAlreadyActive.length > 0
  ) {
    preflightIssues.push(
      `wave12_already_in_active_universe:${waveAlreadyActive.map((b) => b.slug).join(",")}`
    );
  }

  const brands = [];
  for (const id of identities) {
    const live = await fetchBrand(id.recordId);
    await sleep(80);
    const current = nz(live.brandStatus || live.status);
    const liveSlug = nz(live.slug).toLowerCase();
    if (liveSlug && liveSlug !== id.slug && !WAVE12_SLUGS.includes(liveSlug)) {
      preflightIssues.push(`slug_mismatch:${id.slug}:live=${liveSlug}`);
    }
    if (current !== WAVE12_STATUS_FROM && current !== targetStatus) {
      preflightIssues.push(`status_unexpected:${id.slug}:got=${current || "(empty)"}`);
    }
    const needsWrite = current !== targetStatus;
    brands.push({
      slug: id.slug,
      name: id.name,
      recordId: id.recordId,
      from: current,
      to: targetStatus,
      needsWrite,
      alreadyActiveOrLive: isBrandStatusActive(current),
      plannedFields: ["Brand Status"],
      sanitizedPayloadPreview: { "Brand Status": targetStatus },
    });
  }

  const statusGateOk = brands.every(
    (b) => b.from === WAVE12_STATUS_FROM || b.from === targetStatus
  );
  if (!statusGateOk) {
    // already pushed per-brand issues
  }

  const preflightOk =
    preflightIssues.length === 0 &&
    founderApprovals.ok &&
    baseline27.ok &&
    statusGateOk &&
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
  if (applyPerformed && writePerformed) {
    await sleep(400);
    universeAfter = await loadActiveUniverse({ includeDetails: false });
  }

  const promotedCount = applyResults.filter((r) => r.applied === true).length;
  const alreadyCount = brands.filter((b) => !b.needsWrite).length;

  const report = {
    version: WAVE12_STATUS_PROMOTION_VERSION,
    waveVersion: WAVE12_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    targetBrandStatus: targetStatus,
    allowedStatuses: [...WAVE12_STATUS_TO_ALLOWED],
    statusFromExpected: WAVE12_STATUS_FROM,
    flagCheck,
    requiredApplyFlags: [...WAVE12_STATUS_PROMOTION_APPLY_FLAGS],
    preflight: {
      ok: preflightOk,
      issues: preflightIssues,
      baseline27,
      founderApprovals: {
        ok: founderApprovals.ok,
        issues: founderApprovals.issues,
      },
      statusGateOk,
      protected27RecordIdCount: protectedIds.size,
    },
    universeBefore: {
      totalCount: universeBefore.totalCount,
      expected: WAVE12_PROTECTED_BASELINE_COUNT,
      wave12Present: waveAlreadyActive.map((b) => b.slug),
    },
    universeAfter: universeAfter
      ? {
          totalCount: universeAfter.totalCount,
          expected: WAVE12_EXPECTED_FINAL_ACTIVE_COUNT,
          wave12Present: (universeAfter.brands || [])
            .filter((b) => WAVE12_SLUGS.includes(nz(b.slug).toLowerCase()))
            .map((b) => b.slug),
        }
      : null,
    summary: {
      brandCount: brands.length,
      needsWrite: brands.filter((b) => b.needsWrite).length,
      alreadyAtTarget: alreadyCount,
      promotedCount,
      acceptanceUniverse39:
        (universeAfter?.totalCount ?? null) === WAVE12_EXPECTED_FINAL_ACTIVE_COUNT ||
        (universeBefore.totalCount === WAVE12_EXPECTED_FINAL_ACTIVE_COUNT && alreadyCount === 12),
    },
    brands,
    applyResults,
    fieldMapping: { brandStatus: "Brand Status" },
    guardrails: {
      targetBrandsOnly: true,
      singleFieldPayload: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      contentWrites: false,
      imageWrites: false,
      releaseFieldWrites: false,
      protected27Untouched: true,
      radissonCollectionUntouched: true,
      neverWriteFields: [...WAVE12_NEVER_WRITE_FIELDS],
      baselineConvention: `frozen_${EXPECTED_ACTIVE_COUNT_27}_were_Active`,
    },
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { report, paths };
}
