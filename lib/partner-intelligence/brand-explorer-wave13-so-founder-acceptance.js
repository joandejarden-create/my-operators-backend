/**
 * Wave 13 — SO/ founder acceptance of cleanly-unavailable steward posture.
 * Report-only. No Airtable writes.
 *
 * This task instruction (with --confirm-founder-accepts-cleanly-unavailable-steward-posture)
 * is the explicit founder acceptance signal for promotion/release.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_FOUNDER_APPROVE_RECOMMENDATION,
} from "./brand-explorer-wave13-factory-plan.js";
import {
  EXPECTED_ACTIVE_COUNT_45,
  run45ActivePublicFullBaselineRegression,
} from "./brand-explorer-45-active-public-full-baseline.js";

export const WAVE13_SO_FOUNDER_ACCEPTANCE_VERSION = "wave13-so-founder-acceptance-v1";

const SO_SLUG = WAVE13_HELD_PROMOTION_SLUG;
const SO_RECORD_ID = "recTJdPlr4mDs9app";
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
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

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`fetch failed ${recordId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

/**
 * @param {{ argv?: string[], requireExplicitFlag?: boolean }} opts
 */
export async function runWave13SoFounderAcceptance({
  argv = [],
  requireExplicitFlag = true,
} = {}) {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[SO_SLUG];
  if (!identity?.recordId || identity.recordId !== SO_RECORD_ID) {
    throw new Error(`SO/ identity mismatch: ${identity?.recordId}`);
  }

  const explicit =
    argv.includes("--confirm-founder-accepts-cleanly-unavailable-steward-posture") ||
    argv.includes("--approve-so-brand-status-promotion") ||
    argv.includes("--approve-so-public-release");

  if (requireExplicitFlag && !explicit) {
    const report = {
      version: WAVE13_SO_FOUNDER_ACCEPTANCE_VERSION,
      generatedAt: new Date().toISOString(),
      dryRun: true,
      writePerformed: false,
      founder_accepts_cleanly_unavailable_steward_posture: false,
      promotion_recommendation: null,
      stop: true,
      reason:
        "Founder acceptance not explicit — keep SO/ Under Review; do not promote; do not release.",
      readyStatement: "so_founder_acceptance_not_recorded_stop",
    };
    return writeAcceptance(report);
  }

  const brand = await fetchBrand(SO_RECORD_ID);
  const status = brand.status || brand.brandStatus || null;
  const universe = await loadActiveUniverse({ includeDetails: false });
  const issues = [];

  // After Stage 9, SO/ is Active — reuse a prior accepted report instead of blocking reuse.
  const priorPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-founder-acceptance.json");
  let prior = null;
  if (fs.existsSync(priorPath)) {
    try {
      prior = JSON.parse(fs.readFileSync(priorPath, "utf8"));
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[wave13-so-founder-acceptance] prior read failed", err?.message || err);
      }
    }
  }
  if (
    isBrandStatusActive(status) &&
    prior?.founder_accepts_cleanly_unavailable_steward_posture === true &&
    prior?.promotion_recommendation === WAVE13_FOUNDER_APPROVE_RECOMMENDATION
  ) {
    const reused = {
      ...prior,
      generatedAt: new Date().toISOString(),
      brandStatusBefore: status,
      reusedAfterStatusPromotion: true,
      note: "Prior founder acceptance reused after SO/ Brand Status Active (Stage 9).",
      readyStatement: "so_founder_acceptance_recorded_ready_for_status_promotion",
      stop: false,
    };
    return writeAcceptance(reused);
  }

  if (isBrandStatusActive(status)) {
    issues.push(`so_already_active_live:${status}`);
  } else if (!/under review/i.test(nz(status))) {
    issues.push(`so_status_unexpected:${status || "missing"}`);
  }
  if (universe.totalCount !== EXPECTED_ACTIVE_COUNT_45) {
    issues.push(`active_universe_not_45:got=${universe.totalCount}`);
  }
  if ((universe.brands || []).some((b) => b.slug === SO_SLUG)) {
    issues.push("so_already_in_active_universe");
  }

  let baseline45 = { ok: false, skipped: false };
  try {
    const result = await run45ActivePublicFullBaselineRegression({
      allowCachedPvqlIfPass: true,
      maxPvqlAgeMs: 72 * 60 * 60 * 1000,
      forceLivePvql: false,
      evaluateEvidence: true,
    });
    baseline45 = {
      ok: result?.regression?.pass === true,
      liveUniverseCount: result?.liveUniverseCount ?? null,
      pvqlSource: result?.pvqlSource || null,
      failures: (result?.regression?.failures || []).slice(0, 12),
    };
    if (!baseline45.ok) issues.push("protected_45_baseline_regression_failed");
  } catch (err) {
    baseline45 = { ok: false, error: err.message };
    issues.push(`protected_45_baseline_error:${err.message}`);
  }

  const accepted = issues.length === 0;
  const report = {
    version: WAVE13_SO_FOUNDER_ACCEPTANCE_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    airtableWrites: false,
    brandSlug: SO_SLUG,
    brandName: "SO/",
    recordId: SO_RECORD_ID,
    brandStatusBefore: status,
    founder_accepts_cleanly_unavailable_steward_posture: accepted,
    promotion_recommendation: accepted ? WAVE13_FOUNDER_APPROVE_RECOMMENDATION : null,
    stewardPostureAccepted: {
      "snapshot.*_scale_fields": "cleanly_unavailable",
      "footprint.primary_regions": "cleanly_unavailable",
      note: "Not invented; founder accepts promotion/release with these gaps disclosed.",
    },
    preflight: {
      soUnderReview: /under review/i.test(nz(status)),
      activeUniverseCount: universe.totalCount,
      expectedActiveUniverseCount: EXPECTED_ACTIVE_COUNT_45,
      soNotInActiveUniverse: !(universe.brands || []).some((b) => b.slug === SO_SLUG),
      protected45Baseline: baseline45,
      issues,
    },
    stop: !accepted,
    readyStatement: accepted
      ? "so_founder_acceptance_recorded_ready_for_status_promotion"
      : "so_founder_acceptance_blocked",
    previewUrl: `/brand-explorer-combined.html?brandId=${SO_RECORD_ID}&beInternalPreview=1&factoryPreview=1`,
  };

  return writeAcceptance(report);
}

function writeAcceptance(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-founder-acceptance.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-founder-acceptance.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  const md = [
    `# Wave 13 — SO/ Founder Acceptance`,
    "",
    `Generated: ${report.generatedAt}`,
    `Ready: \`${report.readyStatement}\``,
    "",
    `founder_accepts_cleanly_unavailable_steward_posture: **${report.founder_accepts_cleanly_unavailable_steward_posture}**`,
    `promotion_recommendation: **${report.promotion_recommendation || "—"}**`,
    "",
    `## Brand`,
    "",
    `- Slug: \`${report.brandSlug}\``,
    `- Name: **${report.brandName}**`,
    `- Record: \`${report.recordId}\``,
    `- Brand Status (before): **${report.brandStatusBefore || "—"}**`,
    "",
    `## Steward posture accepted`,
    "",
    `- snapshot.* scale fields: cleanly unavailable`,
    `- footprint.primary_regions: cleanly unavailable`,
    `- No invent-fills`,
    "",
    `## Preflight`,
    "",
    `- Active universe: ${report.preflight?.activeUniverseCount} (expected ${report.preflight?.expectedActiveUniverseCount})`,
    `- Protected 45 baseline: **${report.preflight?.protected45Baseline?.ok ? "PASS" : "FAIL"}**`,
    `- Stop: **${report.stop}**`,
    "",
  ];
  if (report.preflight?.issues?.length) {
    md.push(`### Issues`, "");
    for (const i of report.preflight.issues) md.push(`- ${i}`);
    md.push("");
  }
  if (report.reason) {
    md.push(`## Reason`, "", report.reason, "");
  }
  fs.writeFileSync(mdPath, `${md.join("\n")}\n`);
  return { ...report, paths: { jsonPath, mdPath } };
}
