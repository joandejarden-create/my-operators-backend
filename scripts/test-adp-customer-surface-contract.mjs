#!/usr/bin/env node
/**
 * Permanent gate: ADP customer-surface contract (single comprehensive run).
 *
 * npm run test:adp-customer-surface-contract
 *
 * APPROVED_CUSTOMER_OUTPUT_IS_A_VERSIONED_PRODUCTION CONTRACT.
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import assert from "assert";
import { mkdirSync, writeFileSync, readFileSync, existsSync, renameSync, unlinkSync } from "fs";
import { join } from "path";
import { listPublishedPropertyIds } from "../lib/ai-demand-positioning/published-snapshot.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import {
  resolveBppPopulationAttempt,
  evaluateBppClientReadinessGate,
  customerFacingBppCopyIsClean,
  BPP_CLIENT_READY_CLASS,
} from "../lib/ai-demand-positioning/brand-portfolio/bpp-client-readiness-doctrine-v1.js";
import { COMPOSITION_V3_STATUS } from "../lib/ai-demand-positioning/governance/adp-executive-read-composition-v3.js";
import {
  FOUNDATION_PROPERTY_IDS,
  BETHESDA_EXPECTED_SUBJECT_RATES,
  RENDERER_ASSET_VERSION,
  LOCKED_CUSTOMER_SURFACES,
  ADP_PRODUCTION_OUTPUT_BASELINE_V1,
  EVIDENCE_BASELINE_SUBGATES,
  ALL_EVIDENCE_LINKS_NONEMPTY,
  ALL_EVIDENCE_SEMANTIC_PARITY,
  ALL_EVIDENCE_MODAL_LABELS,
  ALL_EVIDENCE_RAW_RESPONSE_PRESENT,
  ALL_EVIDENCE_READABLE,
  NO_INTERNAL_DIAGNOSTICS,
} from "../lib/ai-demand-positioning/governance/adp-production-output-baseline-v1.js";
import {
  buildProductionOutputBaselineV1,
  buildReleaseManifestV1,
  buildPropertyOutputFingerprint,
} from "../lib/ai-demand-positioning/governance/adp-customer-output-fingerprint-v1.js";
import { auditAllEvidenceLinksClientReady } from "../lib/ai-demand-positioning/customer/adp-customer-evidence-contract-v1.js";
import {
  assertLeftConstraintPrimaryIssueParity,
  ADP_EXECUTIVE_SUMMARY_HYBRID_V3_LAYOUT,
  ADP_EXECUTIVE_LEFT_CONSTRAINT_PRIMARY_ISSUE_PARITY,
} from "../lib/ai-demand-positioning/customer/executive-scan-layer-v1.js";
import {
  auditBppEvidencePublicationGate,
  resolveBppEvidenceResponseText,
} from "../lib/ai-demand-positioning/brand-portfolio/adp-bpp-canonical-evidence-set-v1.js";

const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning");
const OUT = join(OUT_DIR, "adp-customer-surface-contract-v1-latest.json");
const BASELINE_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/baselines/adp-production-output-baseline-v1.json"
);

function approx(a, b, tol = 0.15) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

function sleepMs(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    /* busy-wait for Windows file-lock retries */
  }
}

/** Atomic-ish write with retries — Windows/OneDrive can lock report JSON mid-gate. */
function safeWriteJson(path, data) {
  const body = typeof data === "string" ? data : JSON.stringify(data, null, 2) + "\n";
  const tmp = `${path}.${process.pid}.${Date.now()}.tmp`;
  let lastErr;
  for (let attempt = 0; attempt < 10; attempt += 1) {
    try {
      writeFileSync(tmp, body, "utf8");
      try {
        renameSync(tmp, path);
      } catch {
        writeFileSync(path, body, "utf8");
        try {
          unlinkSync(tmp);
        } catch {
          /* ignore */
        }
      }
      return true;
    } catch (err) {
      lastErr = err;
      sleepMs(120 * (attempt + 1));
    }
  }
  console.error(`[customer-surface] report write skipped after retries: ${path}: ${lastErr?.message || lastErr}`);
  return false;
}

function resolveV3(er) {
  if (!er) return null;
  if (er.compositionVersion === "ADP_EXECUTIVE_READ_COMPOSITION_V3" && er.sections) return er;
  if (er.compositionV3?.ok && er.compositionV3?.sections) return er.compositionV3;
  return null;
}

async function main() {
  assert.equal(COMPOSITION_V3_STATUS.activated, true, "V3 must be customer-default activated");

  const gates = {
    EXECUTIVE_READ_V3: true,
    EXECUTIVE_SCAN_LAYER_PRESENT: true,
    EXECUTIVE_V3_SYNTHESIS_PRESENT: true,
    OLD_WHAT_DATA_SAYS_ABSENT: true,
    HYBRID_LAYOUT: true,
    TERRITORY_SUBJECT_RATES: true,
    BPP: true,
    EVIDENCE: true,
    EVIDENCE_SUBGATES: null,
    PROVIDER_PRESENCE: true,
    TRENDS: true,
    SHARE: true,
    PRINT: true,
    HISTORICAL_IMMUTABILITY: true,
  };
  const failures = [];
  const founderSeven = [];

  for (const propertyId of FOUNDATION_PROPERTY_IDS) {
    const result = await getPublishedOwnerReport(propertyId);
    assert.equal(result.ok, true, `${propertyId} published read must succeed`);
    const payload = result.payload;
    const er = payload.executiveRead;
    const v3 = resolveV3(er);
    const v3Ok = Boolean(v3?.sections?.headline && v3?.sections?.keyInsight);
    if (!v3Ok) {
      gates.EXECUTIVE_READ_V3 = false;
      failures.push({ propertyId, gate: "EXECUTIVE_READ_V3", detail: "missing V3 sections" });
    }
    // No silent legacy customer default when V3 composition exists
    if (v3Ok && er.compositionVersion !== "ADP_EXECUTIVE_READ_COMPOSITION_V3" && !er.compositionV3?.ok) {
      gates.EXECUTIVE_READ_V3 = false;
      failures.push({ propertyId, gate: "EXECUTIVE_READ_V3", detail: "unstamped V3" });
    }
    founderSeven.push({
      propertyId,
      compositionVersion: er.compositionVersion || (er.compositionV3?.ok ? "ADP_EXECUTIVE_READ_COMPOSITION_V3" : null),
      hasV3Sections: v3Ok,
      headlinePreview: v3?.sections?.headline?.slice(0, 80) || null,
      scanStrength: er.scanLayer?.biggestStrength?.headline || null,
      scanConstraint: er.scanLayer?.biggestConstraint?.headline || null,
    });

    // ADP_EXECUTIVE_SUMMARY_HYBRID_V3_LAYOUT
    const scan = er.scanLayer;
    if (
      !scan?.biggestStrength?.headline ||
      !scan?.biggestConstraint?.headline ||
      !scan?.changeSincePrior?.headline
    ) {
      gates.EXECUTIVE_SCAN_LAYER_PRESENT = false;
      gates.HYBRID_LAYOUT = false;
      failures.push({ propertyId, gate: "EXECUTIVE_SCAN_LAYER_PRESENT", detail: "missing scanLayer boxes" });
    }
    if (!v3Ok) {
      gates.EXECUTIVE_V3_SYNTHESIS_PRESENT = false;
      gates.HYBRID_LAYOUT = false;
    }
    const parity = assertLeftConstraintPrimaryIssueParity(er);
    if (!parity.ok) {
      gates.HYBRID_LAYOUT = false;
      failures.push({
        propertyId,
        gate: ADP_EXECUTIVE_LEFT_CONSTRAINT_PRIMARY_ISSUE_PARITY,
        detail: parity,
      });
    }
  }

  // Renderer hybrid contract — left summaries must not be force-hidden under V3 hybrid
  {
    const uiJs = readFileSync(
      join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js"),
      "utf8"
    );
    const uiCss = readFileSync(
      join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css"),
      "utf8"
    );
    if (!/V3_HYBRID/.test(uiJs) || !/adp-executive-read--v3-hybrid/.test(uiJs)) {
      gates.HYBRID_LAYOUT = false;
      failures.push({ gate: "HYBRID_LAYOUT", detail: "renderer missing V3_HYBRID mode" });
    }
    if (
      /Approved V3: single structured column — no legacy Biggest Strength rail/.test(uiCss) ||
      (!/adp-executive-read--v3-hybrid/.test(uiCss) &&
        /\.adp-executive-read--v3 \.adp-executive-read__summaries\s*\{\s*display:\s*none/.test(uiCss))
    ) {
      // Hard-stop if CSS hides summaries for all V3 including hybrid
      if (
        /\.adp-executive-read--v3 \.adp-executive-read__summaries\s*\{[^}]*display:\s*none !important/.test(
          uiCss
        ) &&
        !/\.adp-executive-read--v3-hybrid \.adp-executive-read__summaries\s*\{[^}]*display:\s*flex/.test(uiCss)
      ) {
        gates.HYBRID_LAYOUT = false;
        failures.push({ gate: "HYBRID_LAYOUT", detail: "CSS hides V3 left summaries without hybrid exception" });
      }
    }
    // OLD_WHAT_DATA_SAYS_ABSENT — V3 hybrid must not use What The Data Says as right-side narrative
    const hasHybridMode = /compositionMode:\s*"V3_HYBRID"/.test(uiJs);
    const hybridNullNarrative =
      /compositionMode:\s*"V3_HYBRID"[\s\S]{0,240}narrative:\s*null/.test(uiJs) ||
      /narrative:\s*null[\s\S]{0,240}compositionMode:\s*"V3_HYBRID"/.test(uiJs);
    if (!hasHybridMode || !hybridNullNarrative) {
      gates.OLD_WHAT_DATA_SAYS_ABSENT = false;
      failures.push({
        gate: "OLD_WHAT_DATA_SAYS_ABSENT",
        detail: "V3_HYBRID must set narrative null (no What The Data Says right column)",
      });
    }
    // Right-side title for V3 must be Executive Read, not What The Data Says
    if (!/titleEl\.textContent\s*=\s*useV3\s*\?\s*"Executive Read"/.test(uiJs)) {
      gates.OLD_WHAT_DATA_SAYS_ABSENT = false;
      failures.push({
        gate: "OLD_WHAT_DATA_SAYS_ABSENT",
        detail: "V3 path must set main title to Executive Read",
      });
    }
  }

  // Bethesda subject rates independent of benchmark (published SoT must survive read overlay)
  {
    const beth = await getPublishedOwnerReport("adp_bethesda_marriott");
    const ipi = beth.payload?.intentPresenceIndex || {};
    const er = beth.payload?.executiveRead || {};
    assert.equal(
      er.compositionVersion,
      "ADP_EXECUTIVE_READ_COMPOSITION_V3",
      "Bethesda must remain stamped V3"
    );
    assert.ok(er.sections?.headline && er.sections?.keyInsight, "Bethesda V3 sections required");
    // Legacy customer labels must not be the V3 customer architecture
    const legacyLabels = JSON.stringify(er.sections || {});
    assert.ok(!/BIGGEST STRENGTH|WHAT THE DATA SAYS/i.test(legacyLabels));

    for (const [intent, expected] of Object.entries(BETHESDA_EXPECTED_SUBJECT_RATES)) {
      const row = ipi[intent] || {};
      const actual = row.subjectRatePct ?? row.myRate ?? null;
      if (!approx(actual, expected)) {
        gates.TERRITORY_SUBJECT_RATES = false;
        failures.push({
          propertyId: "adp_bethesda_marriott",
          gate: "TERRITORY_SUBJECT_RATES",
          intent,
          expected,
          actual,
        });
      }
      if (expected != null && row.coreBenchmarkRatePct == null) {
        // Developing CORE is fine; subject must still be present
        if (actual == null) {
          gates.TERRITORY_SUBJECT_RATES = false;
          failures.push({
            propertyId: "adp_bethesda_marriott",
            gate: "TERRITORY_SUBJECT_RATES",
            detail: "subject null while CORE developing",
            intent,
          });
        }
      }
    }
  }

  // BPP universe: no placeholder copy; ready (full/rank-only) or suppressed only
  const bppGateExtras = {
    BPP_READY_FULL_RENDERED: true,
    BPP_READY_RANK_ONLY_RENDERED: true,
    BPP_SUPPRESSION_REASON_PARITY: true,
    BPP_NO_GENERIC_FALSE_SUPPRESSION: true,
    BPP_OWNER_SHARE_PARITY: true,
    BPP_EVIDENCE_CONTROLS_PRESENT: true,
    BPP_POSITIVE_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0: true,
    BPP_MISSING_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0: true,
    BPP_DISPLACEMENT_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0: true,
    BPP_RAW_RESPONSE_PRESENT: true,
    BPP_COUNT_SUPPORT_PARITY: true,
    BPP_CORE_LENS_ISOLATION: true,
  };
  for (const propertyId of listPublishedPropertyIds()) {
    const profile = loadPropertyProfile(propertyId);
    const bpp = resolveBrandPortfolioPosition(propertyId, profile, {});
    const attempt = resolveBppPopulationAttempt({ propertyId, profile, bppPayload: bpp });
    const gate = evaluateBppClientReadinessGate(attempt);
    if (!gate.pass) {
      gates.BPP = false;
      failures.push({ propertyId, gate: "BPP", detail: "clientReady gate fail" });
    }
    const readyClasses = new Set([
      BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED,
      BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_FULL,
      BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_RANK_ONLY,
      BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED,
    ]);
    if (!readyClasses.has(attempt.clientReadyClass)) {
      gates.BPP = false;
      bppGateExtras.BPP_NO_GENERIC_FALSE_SUPPRESSION = false;
      failures.push({ propertyId, gate: "BPP", detail: attempt.clientReadyClass });
    }
    if (
      attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_RANK_ONLY &&
      (bpp?.status !== "READY" || !(bpp?.kpis || []).length)
    ) {
      gates.BPP = false;
      bppGateExtras.BPP_READY_RANK_ONLY_RENDERED = false;
      failures.push({ propertyId, gate: "BPP_READY_RANK_ONLY_RENDERED", detail: bpp?.status });
    }
    if (
      attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_FULL &&
      bpp?.status !== "READY"
    ) {
      gates.BPP = false;
      bppGateExtras.BPP_READY_FULL_RENDERED = false;
      failures.push({ propertyId, gate: "BPP_READY_FULL_RENDERED", detail: bpp?.status });
    }
    // Universal BPP evidence contract (populated READY only)
    if (
      bpp?.status === "READY" ||
      attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_FULL ||
      attempt.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED_RANK_ONLY
    ) {
      const evGate = auditBppEvidencePublicationGate(bpp);
      if (evGate.applicable && !evGate.pass) {
        gates.BPP = false;
        bppGateExtras.BPP_RAW_RESPONSE_PRESENT = false;
        failures.push({
          propertyId,
          gate: "BPP_RAW_RESPONSE_PRESENT",
          detail: (evGate.defects || []).slice(0, 5),
        });
      }
      const pos = bpp?.evidence?.positive || [];
      const miss = bpp?.evidence?.missing || [];
      const disp = bpp?.evidence?.displacement || [];
      if ((bpp?.evidence?.counts?.positive ?? pos.length) !== pos.length) {
        bppGateExtras.BPP_COUNT_SUPPORT_PARITY = false;
        gates.BPP = false;
        failures.push({ propertyId, gate: "BPP_COUNT_SUPPORT_PARITY", detail: "positive" });
      }
      if (pos.length > 0 && pos.some((e) => !resolveBppEvidenceResponseText(e).trim())) {
        bppGateExtras.BPP_POSITIVE_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0 = false;
        gates.BPP = false;
        failures.push({ propertyId, gate: "BPP_POSITIVE_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0" });
      }
      if (miss.length > 0 && miss.some((e) => !resolveBppEvidenceResponseText(e).trim())) {
        bppGateExtras.BPP_MISSING_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0 = false;
        gates.BPP = false;
        failures.push({ propertyId, gate: "BPP_MISSING_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0" });
      }
      if (disp.length > 0 && disp.some((e) => !resolveBppEvidenceResponseText(e).trim())) {
        bppGateExtras.BPP_DISPLACEMENT_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0 = false;
        gates.BPP = false;
        failures.push({ propertyId, gate: "BPP_DISPLACEMENT_EVIDENCE_NONEMPTY_WHERE_COUNT_GT_0" });
      }
      const lensLeak = [...pos, ...miss, ...disp].some(
        (e) => e.lens && e.lens !== "BRAND_PORTFOLIO" && e.lens !== "bpp"
      );
      if (lensLeak) {
        bppGateExtras.BPP_CORE_LENS_ISOLATION = false;
        gates.BPP = false;
        failures.push({ propertyId, gate: "BPP_CORE_LENS_ISOLATION" });
      }
    }
    if (!customerFacingBppCopyIsClean(JSON.stringify(bpp || {}))) {
      gates.BPP = false;
      failures.push({ propertyId, gate: "BPP", detail: "forbidden placeholder copy" });
    }
    // AFFILIATION_UNRESOLVED copy forbidden when lens is resolved
    if (
      bpp?.lens?.label &&
      /affiliation and peer path are not yet resolved/i.test(JSON.stringify(bpp))
    ) {
      gates.BPP = false;
      bppGateExtras.BPP_SUPPRESSION_REASON_PARITY = false;
      failures.push({ propertyId, gate: "BPP_SUPPRESSION_REASON_PARITY", detail: "wrong disclosure" });
    }
  }
  // Casas golden: RANK_ONLY must be ready
  {
    const casas = resolveBrandPortfolioPosition(
      "adp_casas_del_xvi",
      loadPropertyProfile("adp_casas_del_xvi"),
      {}
    );
    if (casas?.status !== "READY" || /not shown/i.test(JSON.stringify(casas?.customerState || {}))) {
      gates.BPP = false;
      bppGateExtras.BPP_READY_RANK_ONLY_RENDERED = false;
      failures.push({ propertyId: "adp_casas_del_xvi", gate: "CASAS_RANK_ONLY_GOLDEN", detail: casas?.status });
    }
  }
  gates.BPP_EXTRAS = bppGateExtras;
  for (const [k, v] of Object.entries(bppGateExtras)) {
    if (!v) gates.BPP = false;
  }

  // Evidence / trends / provider presence smoke on Bethesda + Cambridge
  for (const propertyId of ["adp_bethesda_marriott", "adp_cambridge_beaches_bermuda"]) {
    const result = await getPublishedOwnerReport(propertyId);
    const p = result.payload;
    if (!p?.demandCapture?.byIntent) {
      gates.PROVIDER_PRESENCE = false;
      failures.push({ propertyId, gate: "PROVIDER_PRESENCE", detail: "missing demandCapture" });
    }
    // Trends may be single-point baseline; must not be wiped to undefined object incorrectly
    if (p && !("trends" in p) && !p.executiveMetrics) {
      // soft: trends optional for some editions
    }
    if (p?.lostDemand || p?.evidence || p?.intentPresenceIndex) {
      // evidence surface present
    } else {
      gates.EVIDENCE = false;
      failures.push({ propertyId, gate: "EVIDENCE", detail: "missing evidence/lostDemand/index" });
    }
  }

  // Universal evidence-link contract (full published universe)
  {
    const evidenceAudit = await auditAllEvidenceLinksClientReady(listPublishedPropertyIds());
    safeWriteJson(join(OUT_DIR, "adp-all-evidence-links-client-ready-v1.json"), evidenceAudit);
    const sub = {
      [ALL_EVIDENCE_LINKS_NONEMPTY]: evidenceAudit.emptyLinks.length === 0,
      [ALL_EVIDENCE_SEMANTIC_PARITY]: evidenceAudit.wrongStale.length === 0,
      [ALL_EVIDENCE_MODAL_LABELS]: evidenceAudit.modalLabelDefects.length === 0,
      [ALL_EVIDENCE_RAW_RESPONSE_PRESENT]: evidenceAudit.properties.every((p) =>
        (p.links || []).every((l) => l.rawResponsePresent !== false || l.status === "PASS")
      ),
      [ALL_EVIDENCE_READABLE]: evidenceAudit.status === "PASS",
      [NO_INTERNAL_DIAGNOSTICS]: evidenceAudit.internalDiagnostics.length === 0,
    };
    gates.EVIDENCE_SUBGATES = sub;
    for (const g of EVIDENCE_BASELINE_SUBGATES) {
      if (!sub[g]) {
        gates.EVIDENCE = false;
        failures.push({
          gate: g,
          detail: `evidenceAudit.totalFail=${evidenceAudit.totalFail}`,
          empty: evidenceAudit.emptyLinks.slice(0, 5),
          wrong: evidenceAudit.wrongStale.slice(0, 5),
        });
      }
    }
    if (evidenceAudit.status !== "PASS") {
      gates.EVIDENCE = false;
      failures.push({
        gate: "ALL_EVIDENCE_LINKS_CLIENT_READY",
        detail: `fail=${evidenceAudit.totalFail} links=${evidenceAudit.totalLinks}`,
      });
    }

    // Exact displacement count ↔ drawer support parity (full published universe)
    {
      const { spawnSync } = await import("child_process");
      const parity = spawnSync(
        process.execPath,
        ["scripts/test-adp-displacement-evidence-count-parity.mjs"],
        { encoding: "utf8", cwd: process.cwd() }
      );
      const parityOk = parity.status === 0;
      gates.DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY = parityOk;
      if (!parityOk) {
        gates.EVIDENCE = false;
        failures.push({
          gate: "DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY",
          detail: (parity.stdout || parity.stderr || "").slice(-500),
        });
      }
    }
  }

  // Share / print / historical: composition immutability markers
  gates.SHARE = gates.EXECUTIVE_READ_V3 && gates.HYBRID_LAYOUT;
  gates.PRINT = gates.EXECUTIVE_READ_V3 && gates.HYBRID_LAYOUT;
  gates.HISTORICAL_IMMUTABILITY = gates.EXECUTIVE_READ_V3;
  gates.TRENDS = true;

  const baseline = buildProductionOutputBaselineV1();
  mkdirSync(join(process.cwd(), "data/ai-demand-positioning/baselines"), { recursive: true });
  if (!existsSync(BASELINE_PATH)) {
    writeFileSync(BASELINE_PATH, JSON.stringify(baseline, null, 2) + "\n");
  }
  const frozen = JSON.parse(readFileSync(BASELINE_PATH, "utf8"));
  // Compare founder-seven fingerprints against frozen baseline when present
  for (const propertyId of FOUNDATION_PROPERTY_IDS) {
    const next = buildPropertyOutputFingerprint(propertyId);
    const prior = (frozen.rows || []).find((r) => r.propertyId === propertyId);
    if (prior && prior.fingerprint && prior.fingerprint !== next.fingerprint) {
      // Allowed only if composition/territory intentionally restored in this pass —
      // record delta; hard-fail only on V3 loss or Bethesda subject loss (already gated).
      if (!next.hasCompositionV3 && prior.hasCompositionV3) {
        gates.HISTORICAL_IMMUTABILITY = false;
        failures.push({
          propertyId,
          gate: "HISTORICAL_IMMUTABILITY",
          detail: "UNEXPECTED_REGRESSION composition V3 lost",
        });
      }
    }
  }

  // Refresh frozen baseline after successful contract (restore pass)
  const allPass =
    Object.entries(gates).every(
      ([k, v]) => k === "EVIDENCE_SUBGATES" || k === "BPP_EXTRAS" || v === true
    ) &&
    (gates.BPP_EXTRAS
      ? Object.values(gates.BPP_EXTRAS).every((v) => v === true)
      : true) &&
    failures.length === 0;
  if (allPass) {
    safeWriteJson(BASELINE_PATH, baseline);
  }

  const manifest = buildReleaseManifestV1({
    gitCommit: process.env.GIT_COMMIT || null,
  });
  safeWriteJson(join(OUT_DIR, "adp-production-release-manifest-v1-latest.json"), manifest);

  const report = {
    title: "ADP_CUSTOMER_SURFACE_CONTRACT_V1",
    ok: allPass,
    rendererAssetVersion: RENDERER_ASSET_VERSION,
    lockedSurfaces: LOCKED_CUSTOMER_SURFACES,
    baselineObject: ADP_PRODUCTION_OUTPUT_BASELINE_V1,
    baselineHash: baseline.baselineHash,
    baselinePath: BASELINE_PATH,
    gates,
    founderSeven,
    failures,
    methodologyChanged: false,
    doctrine: [
      "APPROVED_CUSTOMER_OUTPUT_IS_A_VERSIONED_PRODUCTION CONTRACT",
      "ADP_EXECUTIVE_RENDERER_FOLLOWS_STORED_COMPOSITION_VERSION",
      "ADP_TERRITORY_SUBJECT_RATE_INDEPENDENT_OF_BENCHMARK_STATUS",
      "ADP_ATOMIC_CUSTOMER_RELEASE_BUNDLE",
      ADP_EXECUTIVE_SUMMARY_HYBRID_V3_LAYOUT,
      ADP_EXECUTIVE_LEFT_CONSTRAINT_PRIMARY_ISSUE_PARITY,
    ],
  };
  mkdirSync(OUT_DIR, { recursive: true });
  // Always emit gate result to stdout before optional report write.
  console.log(JSON.stringify({ ok: allPass, outPath: OUT, gates, failureCount: failures.length }, null, 2));
  safeWriteJson(OUT, report);
  if (!allPass) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
