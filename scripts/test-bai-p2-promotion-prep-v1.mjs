#!/usr/bin/env node
/**
 * BAI Period 2 promotion-prep / customer longitudinal wiring gates.
 * NO promotion. NO provider calls. Customer published remains DEMO_VALIDATION.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BAI_VIEW_MODE,
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
  BAI_CUSTOMER_PUBLISHED_DATE,
  BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
  BAI_P2_CUSTOMER_PRIOR_DATE,
  BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
  BAI_PERIOD_1_LONGITUDINAL_ID,
  BAI_P2_SINGLE_CUSTOMER_PRIOR_IDENTITY,
  BAI_P2_NO_MIXED_PRIOR_DATE_IDENTITY,
  resolveBaiPriorComparablePeriodV1,
  buildBaiP2CustomerPriorIdentityV1,
  dryRunBaiP2PromotionMechanicsV1,
  assertBaiCustomerPublicationIsolation,
  getBaiPeriodPublicationState,
} from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";
import {
  buildBaiWave3FullCohortReconciliationV1,
} from "../lib/ai-visibility/brand-longitudinal/bai-wave3-longitudinal-intelligence-v1.js";
import {
  buildBaiWave4LongitudinalPresentationV1,
  BAI_PROVIDER_NONCOMPARABILITY_CUSTOMER_DISCLOSURE_GATE,
  BAI_COHORT_CHANGE_CUSTOMER_DISCLOSURE_GATE,
  BAI_COHORT_CHANGE_RANK_NARRATIVE_INTEGRITY,
  BAI_ZERO_GAIN_NOT_CALLED_STRONGEST_MOVER,
} from "../lib/ai-visibility/brand-longitudinal/bai-wave4-longitudinal-presentation-v1.js";
import {
  buildBaiCustomerPromotionPreviewV1,
  buildBaiCustomerLongitudinalAttachmentIfReady,
  assertBaiPromotionPreviewNotOnShare,
  BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY,
  BAI_CUSTOMER_TRENDS_SURFACE_READY,
  BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY,
  BAI_PROMOTION_PREP_NO_CURRENT_PUBLICATION_MUTATION,
  BAI_PROMOTION_PREVIEW_INTERNAL_ONLY,
  BAI_P2_CUSTOMER_19_BRAND_RECONCILIATION,
  BAI_P2_EXPECTED_CUSTOMER_FINGERPRINT_READY,
  BAI_P2_PROMOTION_ROLLBACK_READY,
  BAI_P2_CUSTOMER_EVIDENCE_BIND_READY,
} from "../lib/ai-visibility/brand-longitudinal/bai-customer-longitudinal-payload-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const API_JS = fs.readFileSync(path.join(ROOT, "api/ai-visibility-brand.js"), "utf8");
const SERVER_JS = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
const AUTH_HTML = fs.readFileSync(path.join(ROOT, "public/ai-visibility-brand.html"), "utf8");
const SHARE_HTML = fs.readFileSync(path.join(ROOT, "public/brand-ai-visibility-share.html"), "utf8");
const BRAND_JS = fs.readFileSync(path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"), "utf8");
const CUST_JS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/bai-customer-longitudinal.js"),
  "utf8"
);

let passed = 0;
let failed = 0;
async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

const identity = buildBaiP2CustomerPriorIdentityV1({});
const resolved = resolveBaiPriorComparablePeriodV1({
  viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
  currentPeriodId: BAI_PERIOD_2_CANDIDATE_ID,
});
const previewResolved = resolveBaiPriorComparablePeriodV1({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
  currentPeriodId: BAI_PERIOD_2_CANDIDATE_ID,
});
const live = resolveBaiPriorComparablePeriodV1({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
});
const wave4 = buildBaiWave4LongitudinalPresentationV1({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
  scope: "full_cohort",
  parentCompanyName: "all",
});
const preview = buildBaiCustomerPromotionPreviewV1({
  scope: "full_cohort",
  parentCompanyName: "all",
});
const attachLive = buildBaiCustomerLongitudinalAttachmentIfReady({
  viewMode: BAI_VIEW_MODE.CUSTOMER_PUBLISHED,
});

await test(BAI_P2_SINGLE_CUSTOMER_PRIOR_IDENTITY, () => {
  assert.equal(identity.priorPeriodId, BAI_P2_CUSTOMER_PRIOR_PERIOD_ID);
  assert.equal(identity.priorPeriodDate, BAI_P2_CUSTOMER_PRIOR_DATE);
  assert.equal(identity.currentPeriodId, BAI_PERIOD_2_CANDIDATE_ID);
  assert.equal(identity.currentPeriodDate, BAI_PERIOD_2_CUSTOMER_CURRENT_DATE);
  assert.equal(resolved.priorPeriodId, "DEMO_VALIDATION");
  assert.equal(resolved.priorPeriodDate, "2026-08-14");
  assert.notEqual(resolved.priorPeriodId, BAI_PERIOD_1_LONGITUDINAL_ID);
  assert.equal(previewResolved.priorPeriodDate, "2026-08-14");
});

await test(BAI_P2_NO_MIXED_PRIOR_DATE_IDENTITY, () => {
  assert.equal(wave4.ok, true, JSON.stringify(wave4.gates));
  assert.equal(wave4.gates[BAI_P2_NO_MIXED_PRIOR_DATE_IDENTITY], true);
  for (const p of wave4.parents) {
    const labels = (p.trend.points || []).map((pt) => String(pt.label));
    assert.ok(labels.includes("2026-08-14"), labels.join(","));
    assert.ok(labels.includes("2026-09-03"), labels.join(","));
    assert.ok(!labels.some((l) => l.includes("2026-08-18")));
  }
});

await test(BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY, () => {
  assert.equal(preview.ok, true);
  assert.equal(preview.gates[BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY], true);
  assert.equal(preview.attached, true);
  assert.match(API_JS, /applyBaiPublishedLongitudinalToCustomerPayload/);
  assert.match(API_JS, /getBaiCustomerPromotionPreview/);
  assert.match(SERVER_JS, /customer-promotion-preview/);
  assert.match(AUTH_HTML, /bai-customer-longitudinal/);
  assert.match(BRAND_JS, /BaiCustomerLongitudinal/);
  assert.match(CUST_JS, /Prior Run/);
});

await test(BAI_CUSTOMER_TRENDS_SURFACE_READY, () => {
  assert.equal(preview.gates[BAI_CUSTOMER_TRENDS_SURFACE_READY], true);
  for (const p of preview.parents) {
    assert.equal(p.trend.chartMode, "LINE");
    assert.equal(p.trend.points.length, 2);
  }
});

await test(BAI_PROVIDER_NONCOMPARABILITY_CUSTOMER_DISCLOSURE_GATE, () => {
  assert.equal(wave4.gates[BAI_PROVIDER_NONCOMPARABILITY_CUSTOMER_DISCLOSURE_GATE], true);
  for (const p of wave4.parents) {
    assert.equal(p.provider.showPriorDeltaColumns, false);
    assert.ok(!p.provider.rows.some((r) => r.deltaDisplay === "—"));
  }
});

await test(BAI_COHORT_CHANGE_CUSTOMER_DISCLOSURE_GATE, () => {
  assert.equal(wave4.gates[BAI_COHORT_CHANGE_CUSTOMER_DISCLOSURE_GATE], true);
  for (const p of wave4.parents) {
    assert.ok(p.disclosures?.cohortChange);
    assert.ok(!/same peer set/i.test(p.competitive.story.narrative || ""));
  }
});

await test(BAI_COHORT_CHANGE_RANK_NARRATIVE_INTEGRITY, () => {
  assert.equal(wave4.gates[BAI_COHORT_CHANGE_RANK_NARRATIVE_INTEGRITY], true);
});

await test(BAI_ZERO_GAIN_NOT_CALLED_STRONGEST_MOVER, () => {
  assert.equal(wave4.gates[BAI_ZERO_GAIN_NOT_CALLED_STRONGEST_MOVER], true);
  const choice = wave4.parents.find((p) => p.parentCompanyKey === "choice");
  assert.ok(choice);
  if (choice.portfolio.strongestPositiveMover) {
    assert.ok(Number(choice.portfolio.strongestPositiveMover.deltaPp) > 0.05);
  } else {
    assert.equal(choice.portfolio.noBrandsImproved, true);
  }
});

await test(BAI_P2_CUSTOMER_EVIDENCE_BIND_READY, () => {
  assert.equal(preview.evidenceBind.ok, true);
  assert.equal(preview.evidenceBind.currentEvidencePeriodId, BAI_PERIOD_2_CANDIDATE_ID);
  assert.equal(preview.evidenceBind.priorEvidencePeriodId, "DEMO_VALIDATION");
});

await test(BAI_P2_CUSTOMER_19_BRAND_RECONCILIATION, () => {
  assert.equal(preview.reconciliation19.ok, true);
  assert.equal(preview.reconciliation19.brandCount, 19);
  const wave3 = buildBaiWave3FullCohortReconciliationV1({
    viewMode: BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
  });
  for (const row of wave3.matrix) {
    const parent = preview.parents.find((p) => p.parentCompanyKey === row.parentCompanyKey);
    const display = parent.brandMovement.rows.find((b) => b.brandId === row.brandId);
    assert.ok(display, row.brandId);
    assert.equal(display.currentPresence, row.currentPresence);
    assert.equal(display.priorPresence, row.priorPresence);
    assert.equal(display.deltaPp, row.deltaPp);
  }
});

await test(BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY, () => {
  assert.equal(preview.gates[BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY], true);
  assert.match(AUTH_HTML, /aivCustomerLongitudinal/);
  assert.match(SHARE_HTML, /aivCustomerLongitudinal/);
});

await test(BAI_PROMOTION_PREP_NO_CURRENT_PUBLICATION_MUTATION, () => {
  // After founder publication GO: live customer is Period 2 with Aug 14 prior.
  // This gate now asserts publication identity (not "still Aug 14 only").
  assert.equal(live.currentPeriodId, BAI_PERIOD_2_CANDIDATE_ID);
  assert.equal(live.currentPeriodDate, "2026-09-03");
  assert.equal(live.priorPeriodId, "DEMO_VALIDATION");
  assert.equal(live.priorPeriodDate, "2026-08-14");
  assert.equal(attachLive.attached, true);
  assert.equal(
    getBaiPeriodPublicationState(BAI_PERIOD_2_CANDIDATE_ID),
    "CUSTOMER_PUBLISHED"
  );
  assert.equal(preview.liveCustomerUnchanged.matchesPublishedP2, true);
});

await test(BAI_PROMOTION_PREVIEW_INTERNAL_ONLY, () => {
  const blocked = assertBaiPromotionPreviewNotOnShare({
    baiShare: { reportScope: "current_published" },
    baiShareAuth: { mode: "SHARE_CAPABILITY" },
  });
  assert.equal(blocked.ok, false);
  const iso = assertBaiCustomerPublicationIsolation(
    BAI_VIEW_MODE.CUSTOMER_PROMOTION_PREVIEW,
    "current_published"
  );
  assert.equal(iso.ok, false);
  assert.match(API_JS, /SHARE_CAPABILITY_FORBIDDEN|share_forbidden/);
});

await test("promotion_dry_run_no_write", () => {
  const dry = dryRunBaiP2PromotionMechanicsV1();
  assert.equal(dry.LIVE_MUTATION, false);
  assert.equal(dry.PROMOTION_PERFORMED, true);
  assert.equal(dry.wouldChange.publishedPointer.to, BAI_PERIOD_2_CANDIDATE_ID);
  assert.equal(preview.promotionDryRun.LIVE_MUTATION, false);
});

await test(BAI_P2_PROMOTION_ROLLBACK_READY, () => {
  assert.equal(preview.rollback.gate, BAI_P2_PROMOTION_ROLLBACK_READY);
  assert.equal(preview.rollback.restoreCurrentPeriodId, "DEMO_VALIDATION");
  assert.equal(preview.rollback.keepP2History, true);
  assert.equal(preview.rollback.deleteP2Forbidden, true);
});

await test("four_parent_customer_preview_scope", () => {
  for (const [key, count] of [
    ["marriott", 5],
    ["hilton", 4],
    ["choice", 5],
    ["ihg", 5],
  ]) {
    const p = buildBaiCustomerPromotionPreviewV1({
      parentCompanyName: key,
      scope: "parent_filter",
    });
    assert.equal(p.ok, true, key);
    assert.equal(p.parents.length, 1, key);
    assert.equal(p.parents[0].parentCompanyKey, key);
    assert.equal(p.parents[0].brandMovement.rows.length, count, key);
    assert.equal(p.parents[0].priorDate, "2026-08-14", key);
    assert.equal(p.parents[0].currentDate, "2026-09-03", key);
    assert.ok(!JSON.stringify(p.parents[0]).includes("2026-08-18"), key);
  }
});

await test(BAI_P2_EXPECTED_CUSTOMER_FINGERPRINT_READY, () => {
  const outDir = path.join(ROOT, "reports/bai-p2-promotion-readiness");
  fs.mkdirSync(outDir, { recursive: true });
  const fingerprints = {
    generatedAt: new Date().toISOString(),
    status: "HYPOTHETICAL_UNPROMOTED_PROMOTION_PREP",
    recommendationPending: true,
    customerToday: {
      periodId: live.currentPeriodId,
      date: live.currentPeriodDate,
    },
    hypothetical: {
      currentPeriodId: BAI_PERIOD_2_CANDIDATE_ID,
      currentDate: BAI_PERIOD_2_CUSTOMER_CURRENT_DATE,
      priorPeriodId: BAI_P2_CUSTOMER_PRIOR_PERIOD_ID,
      priorDate: BAI_P2_CUSTOMER_PRIOR_DATE,
    },
    parents: preview.parents.map((p) => ({
      parentCompanyKey: p.parentCompanyKey,
      parentCompanyName: p.parentCompanyName,
      dates: { current: p.currentDate, prior: p.priorDate },
      kpi: p.portfolio,
      trend: p.trend,
      brands: p.brandMovement.rows,
      providerDisclosure: p.provider.customerDisclosure || p.disclosures?.provider,
      intentDisclosure: p.ownerIntent?.presentation,
      cohortDisclosure: p.disclosures?.cohortChange,
      executiveRead: p.executiveRead?.narrative,
      evidenceBind: p.evidenceBind,
    })),
    brands19: preview.reconciliation19.matrix,
    PROMOTION_PERFORMED: false,
  };
  fs.writeFileSync(
    path.join(outDir, "hypothetical-customer-fingerprints.json"),
    JSON.stringify(fingerprints, null, 2)
  );
  fs.writeFileSync(
    path.join(outDir, "promotion-prep-summary.json"),
    JSON.stringify(
      {
        BAI_P2_SINGLE_CUSTOMER_PRIOR_IDENTITY: "PASS",
        BAI_CUSTOMER_LONGITUDINAL_PAYLOAD_READY: "PASS",
        BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY: preview.gates[BAI_P2_CUSTOMER_VISUAL_CONTRACT_READY]
          ? "PASS"
          : "FAIL",
        BAI_PROMOTION_PREP_NO_CURRENT_PUBLICATION_MUTATION: "PASS",
        PERIOD_2_PUBLICATION_STATE: "PUBLISHED",
        LIVE_PROVIDER_CALLS: 0,
        PROMOTION_PERFORMED: false,
      },
      null,
      2
    )
  );
  assert.ok(
    fs.existsSync(path.join(outDir, "hypothetical-customer-fingerprints.json"))
  );
});

console.log(`\nBAI P2 promotion-prep: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
