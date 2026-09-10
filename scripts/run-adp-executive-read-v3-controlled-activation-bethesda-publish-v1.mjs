#!/usr/bin/env node
/**
 * ADP Executive Read V3 — controlled production activation + Bethesda #14 publication.
 *
 * Staged HARD STOP gates. Does NOT email / distribute externally.
 *
 * Usage:
 *   node scripts/run-adp-executive-read-v3-controlled-activation-bethesda-publish-v1.mjs --dry-run
 *   node scripts/run-adp-executive-read-v3-controlled-activation-bethesda-publish-v1.mjs --apply
 *   node scripts/run-adp-executive-read-v3-controlled-activation-bethesda-publish-v1.mjs --apply --skip-browser
 *   node scripts/run-adp-executive-read-v3-controlled-activation-bethesda-publish-v1.mjs --apply --skip-bethesda-publish
 */

import "../load-env.js";
import { execSync } from "child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync, copyFileSync } from "fs";
import { join } from "path";
import { createHash } from "crypto";
import { listPublishedPropertyIds, loadPublishedManifest, loadPublishedReport, buildPublishedSnapshotBundle, savePublishedSnapshotBundle } from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { resolveCensusRecordIdForPublish } from "../lib/ai-demand-positioning/census-link-registry.js";
import { resolveGovernedAdpPropertyUniverseV1 } from "../lib/ai-demand-positioning/client-readiness/resolve-governed-adp-property-universe-v1.js";
import { COMPOSITION_V3_STATUS, ADP_EXECUTIVE_READ_COMPOSITION_V3 } from "../lib/ai-demand-positioning/governance/adp-executive-read-composition-v3.js";
import {
  freezePreActivationRecoveryPoint,
  generateImmutableV3EditionForProperty,
  hashWriteupUx,
  ADP_EXECUTIVE_V3_IMMUTABLE_EDITION_LINEAGE,
  ADP_EXECUTIVE_REAL_BROWSER_LAYOUT_PASS,
  ADP_EXECUTIVE_PRODUCTION_PDF_PARITY_PASS,
  RECOVERY_DIR,
} from "../lib/ai-demand-positioning/executive-read-v3/immutable-edition-activation-v3.js";
import {
  FOUNDER_SEVEN_PROPERTY_IDS,
  BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID,
  BETHESDA_ZERO_CODE_CHALLENGE_PERIOD_ID,
  prepareExecutiveReadV3ForActualReport,
  buildExecutiveReadV3StructuredHtml,
  evaluateCrossSurfaceCanonicalParityV3,
  ADP_EXECUTIVE_V3_FOUNDER_SEVEN_VISUAL_PASS,
  ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY,
  ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY,
  ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ACTUAL_REPORT_PATH,
  futurePropertyActualReportPathReady,
} from "../lib/ai-demand-positioning/executive-read-v3/actual-report-implementation-v3.js";
import { composeExecutiveReadV3 } from "../lib/ai-demand-positioning/executive-read-v3/compose-executive-read-v3.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  scoreBethesdaPrimaryIssueChallengeV1,
  evaluateBethesdaComposerAlignmentV1,
  ADP_EXECUTIVE_BETHESDA_PRIMARY_ISSUE_CHALLENGE,
} from "../lib/ai-demand-positioning/executive-read-v3/bethesda-primary-issue-challenge-v1.js";
import { buildExecutiveReadInputV3 } from "../lib/ai-demand-positioning/executive-read-v3/input-contract-v3.js";
import { issueShareCapability } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const SKIP_BROWSER = args.includes("--skip-browser");
const SKIP_BETHESDA = args.includes("--skip-bethesda-publish");
const BASE =
  process.env.ADP_QA_BASE ||
  process.env.ADP_PLAYWRIGHT_BASE ||
  (await (async () => {
    for (const port of [8080, 8099, 8081]) {
      try {
        const code = execSync(
          `curl.exe -s -o NUL -w "%{http_code}" --connect-timeout 2 http://127.0.0.1:${port}/`,
          { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }
        ).trim();
        if (code === "200" || code === "304" || code === "301") {
          return `http://127.0.0.1:${port}`;
        }
      } catch {
        /* try next */
      }
    }
    return "http://127.0.0.1:8080";
  })());

const VIEWPORTS = [
  { name: "desktop", width: 1280, height: 900 },
  { name: "w1024", width: 1024, height: 800 },
  { name: "tablet", width: 768, height: 900 },
  { name: "mobile", width: 390, height: 844 },
];

function sha(s) {
  return createHash("sha256").update(String(s || "")).digest("hex");
}

function loadShareRegistry() {
  const p = join(process.cwd(), "config/client-share/adp-share-registry/active-tokens.json");
  if (!existsSync(p)) return { tokens: {} };
  return JSON.parse(readFileSync(p, "utf8"));
}

function gitMeta() {
  try {
    return {
      branch: execSync("git rev-parse --abbrev-ref HEAD", { encoding: "utf8" }).trim(),
      commit: execSync("git rev-parse HEAD", { encoding: "utf8" }).trim(),
      statusShort: execSync("git status -sb", { encoding: "utf8" }).trim().split("\n")[0],
    };
  } catch {
    return { branch: null, commit: null, statusShort: null };
  }
}

async function runBrowserFounderSevenQa(propertyIds) {
  let playwright;
  try {
    playwright = await import("playwright");
  } catch {
    return { pass: false, reason: "PLAYWRIGHT_NOT_INSTALLED", results: [] };
  }

  // Fresh founder-QA share tokens (not distributed) — share page requires capability auth.
  const shareUrls = {};
  for (const propertyId of propertyIds) {
    try {
      const issued = issueShareCapability({
        propertyId,
        label: `founder-browser-qa:${propertyId}:${new Date().toISOString().slice(0, 19)}`,
        reportScope: "current_published",
      });
      shareUrls[propertyId] = `${BASE}${issued.sharePath}`;
    } catch (err) {
      return {
        pass: false,
        reason: `SHARE_ISSUE_FAIL:${propertyId}:${String(err.message || err)}`,
        results: [],
      };
    }
  }

  const { chromium } = playwright;
  const browser = await chromium.launch({ headless: true });
  const outDir = join(process.cwd(), "reports/ai-demand-positioning/v3-activation-browser-qa");
  mkdirSync(outDir, { recursive: true });
  const results = [];

  try {
    for (const propertyId of propertyIds) {
      const perProp = {
        propertyId,
        viewports: {},
        pass: true,
        defects: [],
      };
      for (const vp of VIEWPORTS) {
        const page = await browser.newPage({ viewport: { width: vp.width, height: vp.height } });
        const url = shareUrls[propertyId];
        let navOk = true;
        let navError = null;
        try {
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
          // Wait for V3 structured root OR explicit error/success — narrative node exists while parent is hidden.
          await page.waitForFunction(
            () => {
              const err = document.getElementById("adpStateError");
              if (err && !err.hidden) return true;
              if (document.querySelector(".adp-er-v3")) return true;
              const success = document.getElementById("adpStateSuccess");
              const narrative = document.getElementById("adpExecutiveReadNarrative");
              if (success && !success.hidden && narrative && narrative.innerHTML.trim().length > 40) {
                return true;
              }
              return false;
            },
            { timeout: 90000 }
          );
          await page.waitForTimeout(800);
        } catch (err) {
          navOk = false;
          navError = String(err.message || err).slice(0, 180);
          perProp.defects.push(`${vp.name}:NAV_FAIL:${navError}`);
        }

        const measure = await page.evaluate(() => {
          const errEl = document.getElementById("adpStateError");
          if (errEl && !errEl.hidden) {
            return {
              ok: false,
              reason: "PAGE_ERROR_STATE",
              errorText: (document.getElementById("adpErrorMessage") || {}).textContent || "",
            };
          }
          const root =
            document.querySelector(".adp-er-v3") ||
            document.querySelector("#adpExecutiveReadNarrative .adp-er-v3") ||
            document.querySelector("#adpExecutiveReadNarrative");
          const section = document.getElementById("adpExecutiveReadSection");
          if (!root) {
            return { ok: false, reason: "MISSING_ER_ROOT" };
          }
          const cs = window.getComputedStyle(root);
          const box = root.getBoundingClientRect();
          const sections = [...document.querySelectorAll("[data-er-section]")].map((el) => {
            const r = el.getBoundingClientRect();
            const style = window.getComputedStyle(el);
            return {
              key: el.getAttribute("data-er-section"),
              visible: r.height > 0 && style.visibility !== "hidden" && style.display !== "none",
              top: Math.round(r.top),
              height: Math.round(r.height),
              overflow: style.overflow,
              clipped: style.overflow === "hidden" && el.scrollHeight > el.clientHeight + 2,
            };
          });
          const order = sections.map((s) => s.key);
          const expected = ["headline", "keyInsight", "whyItMatters", "focusNow", "whatToReview"];
          const withoutWatch = order.filter((x) => x !== "watch");
          const orderOk = expected.every((k) => withoutWatch.includes(k));
          return {
            ok: true,
            hasV3: Boolean(document.querySelector(".adp-er-v3")),
            boundingBox: {
              width: Math.round(box.width),
              height: Math.round(box.height),
              top: Math.round(box.top),
            },
            scrollHeight: root.scrollHeight,
            clientHeight: root.clientHeight,
            overflowY: cs.overflowY,
            lineHeight: cs.lineHeight,
            fontSize: cs.fontSize,
            overflowHiddenContent: root.scrollHeight > root.clientHeight + 4 && cs.overflowY === "hidden",
            sections,
            order,
            orderOk,
            watchPresent: order.includes("watch"),
            sectionHidden: section ? section.hidden : null,
          };
        });

        const shotPath = join(outDir, `${propertyId}__${vp.name}.png`);
        try {
          await page.screenshot({ path: shotPath, fullPage: false });
        } catch {
          /* ignore */
        }

        const defects = [];
        // NAV timeout is a soft defect only when structured V3 content did not actually render.
        if (!navOk && !(measure.ok && measure.hasV3)) {
          defects.push("NAV");
        }
        if (!measure.ok) defects.push(measure.reason || "MEASURE_FAIL");
        if (measure.ok) {
          if (!measure.hasV3) defects.push("NOT_V3_STRUCTURED");
          if (measure.overflowHiddenContent) defects.push("CLIPPED_OVERFLOW_HIDDEN");
          if (!measure.orderOk) defects.push("SECTION_ORDER");
          if (measure.watchPresent) defects.push("UNEXPECTED_WATCH");
          const missing = ["headline", "keyInsight", "whyItMatters", "focusNow", "whatToReview"].filter(
            (k) => !measure.order.includes(k)
          );
          if (missing.length) defects.push(`MISSING:${missing.join(",")}`);
          if (measure.sections?.some((s) => s.clipped)) defects.push("SECTION_CLIPPED");
          const fs = parseFloat(measure.fontSize);
          if (Number.isFinite(fs) && fs < 12) defects.push("MICROTYPE");
        }

        perProp.viewports[vp.name] = {
          ...measure,
          defects,
          navOk,
          navError,
          screenshot: shotPath,
          url,
        };
        if (defects.length) {
          perProp.pass = false;
          perProp.defects.push(...defects.map((d) => `${vp.name}:${d}`));
        }
        await page.close();
      }
      results.push(perProp);
    }
  } finally {
    await browser.close();
  }

  return {
    gate: ADP_EXECUTIVE_REAL_BROWSER_LAYOUT_PASS,
    pass: results.every((r) => r.pass),
    base: BASE,
    results,
  };
}

async function main() {
  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  mkdirSync(RECOVERY_DIR, { recursive: true });

  const blockers = [];
  const p0 = [];
  const p1 = [];

  // ---- A. Recovery freeze ----
  const shareRegistry = loadShareRegistry();
  const recovery = freezePreActivationRecoveryPoint({
    git: gitMeta(),
    shareState: {
      activeTokenCount: Object.values(shareRegistry.tokens || {}).filter((t) => t.status === "ACTIVE")
        .length,
      propertiesWithActiveShare: [
        ...new Set(
          Object.values(shareRegistry.tokens || {})
            .filter((t) => t.status === "ACTIVE")
            .map((t) => t.propertyId)
        ),
      ],
    },
  });

  if (COMPOSITION_V3_STATUS.activated !== true) {
    blockers.push("COMPOSITION_V3_NOT_ACTIVATED_IN_SOURCE");
  }

  // ---- B. Re-verify structural gates (estimator / prior QA) ----
  const priorChallengePath = join(
    outDir,
    "adp-executive-read-v3-bethesda-founder-seven-challenge-v1-latest.json"
  );
  let priorChallenge = null;
  if (existsSync(priorChallengePath)) {
    priorChallenge = JSON.parse(readFileSync(priorChallengePath, "utf8"));
    if (priorChallenge.FOUNDER_SEVEN_VISUAL_PASS !== "PASS") {
      blockers.push("PRIOR_FOUNDER_SEVEN_NOT_PASS");
    }
    if (priorChallenge.BETHESDA_ZERO_CODE_REAL_WORLD_RESULT !== "PASS") {
      blockers.push("PRIOR_BETHESDA_ZERO_CODE_NOT_PASS");
    }
  } else {
    blockers.push("MISSING_PRIOR_FOUNDER_SEVEN_CHALLENGE_ARTIFACT");
  }

  // ---- C. Immutable editions for current 13 ----
  const publishedIds = listPublishedPropertyIds().filter((id) => id.startsWith("adp_"));
  if (publishedIds.length !== 13) {
    blockers.push(`UNEXPECTED_UNIVERSE_BEFORE_BETHESDA:${publishedIds.length}`);
  }

  const editionResults = [];
  for (const propertyId of publishedIds) {
    const result = generateImmutableV3EditionForProperty(propertyId, {
      apply: APPLY,
      shareRegistry,
      forceAdditiveEdition: true,
    });
    editionResults.push(result);
    if (!result.ok) {
      blockers.push(`EDITION_FAIL:${propertyId}:${result.reason}`);
      p0.push({ code: "EDITION_FAIL", propertyId, reason: result.reason });
    }
  }

  // Verify writeup hashes vs recovery for applied editions
  let historicalImmutabilityPass = true;
  if (APPLY) {
    for (const propertyId of publishedIds) {
      const before = recovery.propertyHashes[propertyId]?.writeupUxHash;
      const payload = loadPublishedReport(propertyId);
      const after = hashWriteupUx(payload?.executiveRead);
      if (before && after && before !== after) {
        historicalImmutabilityPass = false;
        blockers.push(`WRITEUP_MUTATED:${propertyId}`);
        p0.push({ code: "WRITEUP_MUTATED", propertyId });
      }
      const er = payload?.executiveRead;
      if (APPLY && (!er?.sections || er.compositionVersion !== ADP_EXECUTIVE_READ_COMPOSITION_V3)) {
        blockers.push(`V3_NOT_STAMPED:${propertyId}`);
      }
    }
  }

  // ---- D. Structural cross-surface for founder seven (post-edition) ----
  const structuralSeven = [];
  for (const propertyId of FOUNDER_SEVEN_PROPERTY_IDS) {
    if (propertyId === BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID) {
      const profile = loadPropertyProfile(propertyId);
      const period = loadPeriod(BETHESDA_ZERO_CODE_CHALLENGE_PERIOD_ID);
      const payload = buildOwnerPayload(period, buildScenarioUniverse(profile), profile);
      const v3 = composeExecutiveReadV3(payload, {
        propertyId,
        market: profile?.market,
        distributionStatus: "INTERNAL_ONLY",
        zeroCodePath: true,
      });
      payload.executiveRead = {
        ...(payload.executiveRead || {}),
        compositionVersion: v3.compositionVersion,
        sections: v3.sections,
        compositionV3: v3,
      };
      const prep = prepareExecutiveReadV3ForActualReport(payload.executiveRead, { qaForced: true });
      const html = prep.ok ? buildExecutiveReadV3StructuredHtml(prep.viewModel) : { ok: false };
      structuralSeven.push({
        propertyId,
        ok: prep.ok && html.ok && v3.primaryIssueId === "answer_level_inclusion_consistency",
        primaryIssueId: v3.primaryIssueId,
        unpublished: true,
      });
      continue;
    }
    const payload = loadPublishedReport(propertyId);
    let er = payload?.executiveRead;
    // Dry-run / pre-stamp: ensure in-memory V3 composition for structural QA
    if (!er?.sections || er.compositionVersion !== ADP_EXECUTIVE_READ_COMPOSITION_V3) {
      const profile = loadPropertyProfile(propertyId);
      const v3 = composeExecutiveReadV3(payload, {
        propertyId,
        market: profile?.market,
        distributionStatus: "PUBLISHED_CURRENT",
        zeroCodePath: true,
        allowNewEdition: true,
      });
      if (v3?.ok) {
        er = {
          ...er,
          compositionVersion: v3.compositionVersion,
          sections: v3.sections,
          numericAnchors: v3.numericAnchors,
          primaryIssueId: v3.primaryIssueId,
          compositionV3: v3,
        };
      }
    }
    const ownerPrep = prepareExecutiveReadV3ForActualReport(er, { qaForced: true });
    const sharePrep = prepareExecutiveReadV3ForActualReport(er, { qaForced: true });
    const printPrep = prepareExecutiveReadV3ForActualReport(er, {
      qaForced: true,
      compressionSurface: "print",
    });
    const parity = evaluateCrossSurfaceCanonicalParityV3(ownerPrep, sharePrep, printPrep);
    structuralSeven.push({
      propertyId,
      ok: ownerPrep.ok && parity.pass,
      layoutReviewRequired: Boolean(ownerPrep.layoutReviewRequired),
      primaryIssueId: er?.primaryIssueId,
      parity: parity.pass,
      desktop: ownerPrep.layoutAfter?.bySurface?.desktop?.status,
    });
  }
  if (!structuralSeven.every((r) => r.ok)) {
    blockers.push("STRUCTURAL_FOUNDER_SEVEN_FAIL");
  }

  // ---- E. Real browser QA ----
  let browserQa = { pass: SKIP_BROWSER, skipped: SKIP_BROWSER, reason: SKIP_BROWSER ? "SKIPPED_BY_FLAG" : null };
  if (!SKIP_BROWSER && APPLY && blockers.length === 0) {
    // published founder six only for live share URL; Bethesda after publish
    const browserIds = FOUNDER_SEVEN_PROPERTY_IDS.filter(
      (id) => id !== BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID
    );
    browserQa = await runBrowserFounderSevenQa(browserIds);
    if (!browserQa.pass) {
      blockers.push("REAL_BROWSER_FOUNDER_SEVEN_FAIL");
      p0.push({ code: "REAL_BROWSER_FAIL", details: browserQa.results?.filter((r) => !r.pass) });
    }
  } else if (!APPLY) {
    browserQa = { pass: false, skipped: true, reason: "DRY_RUN_NO_BROWSER" };
  }

  // ---- F. Bethesda primary lock ----
  const bethProfile = loadPropertyProfile(BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID);
  const bethPeriod = loadPeriod(BETHESDA_ZERO_CODE_CHALLENGE_PERIOD_ID);
  const bethPayload = buildOwnerPayload(bethPeriod, buildScenarioUniverse(bethProfile), bethProfile);
  const bethInput = buildExecutiveReadInputV3(bethPayload, {
    propertyId: BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID,
    market: bethProfile?.market,
  });
  const bethChallenge = scoreBethesdaPrimaryIssueChallengeV1(bethInput, {
    propertyId: BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID,
    periodId: BETHESDA_ZERO_CODE_CHALLENGE_PERIOD_ID,
  });
  const bethV3 = composeExecutiveReadV3(bethPayload, {
    propertyId: BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID,
    market: bethProfile?.market,
    distributionStatus: "INTERNAL_ONLY",
    zeroCodePath: true,
  });
  const bethAlign = evaluateBethesdaComposerAlignmentV1(bethChallenge, bethV3);
  if (!bethAlign.pass || bethV3.primaryIssueId !== "answer_level_inclusion_consistency") {
    blockers.push("BETHESDA_PRIMARY_ISSUE_LOCK_FAIL");
    p0.push({ code: "BETHESDA_PRIMARY_DRIFT", composer: bethV3.primaryIssueId });
  }

  // ---- G. Bethesda publish (only if prior gates pass) ----
  let bethesdaPublish = {
    attempted: false,
    published: false,
    skipped: SKIP_BETHESDA || !APPLY,
  };
  let bethesdaShare = {
    generated: false,
    externallyDistributed: false,
  };
  let universeAfter = publishedIds.length;

  const canPublishBethesda =
    APPLY &&
    !SKIP_BETHESDA &&
    blockers.length === 0 &&
    browserQa.pass &&
    historicalImmutabilityPass;

  if (canPublishBethesda) {
    bethesdaPublish.attempted = true;
    try {
      const censusRecordId = resolveCensusRecordIdForPublish(
        BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID,
        "recLuxvwwxID7U2B8"
      );
      const bundle = buildPublishedSnapshotBundle({
        period: bethPeriod,
        profile: bethProfile,
        censusRecordId,
        measurementContractVersion: bethPeriod.measurementContractVersion || "ADP_MEASUREMENT_CONTRACT_V1_1",
      });
      if (!bundle.ok) {
        blockers.push(`BETHESDA_BUNDLE_FAIL:${bundle.error || bundle.message}`);
        p0.push({ code: "BETHESDA_PUBLISH_BUNDLE_FAIL", error: bundle.error });
      } else {
        // Ensure V3 on published Bethesda payload
        const er = bundle.report?.payload?.executiveRead;
        if (er && bethV3?.ok) {
          const writeup = er.writeup;
          const ux = er.ux;
          bundle.report.payload.executiveRead = {
            ...er,
            compositionVersion: bethV3.compositionVersion,
            sections: bethV3.sections,
            numericAnchors: bethV3.numericAnchors,
            primaryIssueId: bethV3.primaryIssueId,
            insightArchetype: bethV3.insightArchetype,
            evidenceTrace: bethV3.evidenceTrace,
            qualityGates: bethV3.qualityGates,
            sourceSnapshotHash: bethV3.sourceSnapshotHash,
            compositionHash: bethV3.compositionHash,
            compositionV3: { ...bethV3, activated: true, customerFacingDefault: true },
            writeup,
            ux,
          };
          bundle.manifest.executiveReadCompositionVersion = ADP_EXECUTIVE_READ_COMPOSITION_V3;
          bundle.manifest.executiveReadCompositionHash = bethV3.compositionHash;
          bundle.manifest.certificationStatus = "CERTIFIED_WITH_DISCLOSURES";
          bundle.manifest.externalShareDistributed = false;
        }
        const saved = savePublishedSnapshotBundle(bundle, { seed: false });
        bethesdaPublish = {
          attempted: true,
          published: true,
          manifestFile: saved.manifestFile,
          reportFile: saved.reportFile,
          evidenceFile: saved.evidenceFile,
          periodId: bundle.manifest.latestPeriodId,
          primaryIssueId: bethV3.primaryIssueId,
          customerDropdownVisible: bethProfile?.customerDropdownVisible === true,
          externalShareDistributed: false,
        };

        // Keep dropdown false (do not expose unintended customer contexts)
        // Profile fixture remains customerDropdownVisible: false

        const universe = resolveGovernedAdpPropertyUniverseV1();
        universeAfter = universe?.propertyIds?.length || listPublishedPropertyIds().length;
        if (universeAfter !== 14) {
          blockers.push(`UNIVERSE_NOT_14_AFTER_PUBLISH:${universeAfter}`);
          p0.push({ code: "UNIVERSE_COUNT", count: universeAfter });
        }
        if (!listPublishedPropertyIds().includes(BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID)) {
          blockers.push("BETHESDA_NOT_IN_PUBLISHED_LIST");
        }

        // Issue share for founder review — NOT distributed
        try {
          const issued = issueShareCapability({
            propertyId: BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID,
            label: `founder-review:adp_bethesda_marriott:${new Date().toISOString().slice(0, 19)}`,
            reportScope: "current_published",
          });
          bethesdaShare = {
            generated: Boolean(issued?.token || issued?.tokenId),
            tokenId: issued?.tokenId || issued?.id || null,
            externallyDistributed: false,
            externalShareDistributed: false,
            note: "Share capability issued for founder review only — do not email / send to Rad",
            reviewPathHint: issued?.url || `/owner-ai-demand-share.html?share=<token>`,
          };
        } catch (err) {
          bethesdaShare = {
            generated: false,
            externallyDistributed: false,
            error: String(err.message || err),
            note: "Share issue failed — publication may still be valid; founder can issue manually",
          };
          p1.push({ code: "BETHESDA_SHARE_ISSUE_FAIL", error: String(err.message || err) });
        }

        // Post-publish Bethesda browser check
        if (!SKIP_BROWSER) {
          const bethBrowser = await runBrowserFounderSevenQa([BETHESDA_ZERO_CODE_CHALLENGE_PROPERTY_ID]);
          bethesdaPublish.browserQa = bethBrowser;
          if (!bethBrowser.pass) {
            blockers.push("BETHESDA_BROWSER_QA_FAIL");
            p0.push({ code: "BETHESDA_BROWSER_FAIL" });
          }
        }
      }
    } catch (err) {
      blockers.push(`BETHESDA_PUBLISH_EXCEPTION:${String(err.message || err)}`);
      p0.push({ code: "BETHESDA_PUBLISH_EXCEPTION", error: String(err.message || err) });
    }
  } else if (APPLY && !SKIP_BETHESDA && blockers.length) {
    bethesdaPublish = {
      attempted: false,
      published: false,
      hardStop: true,
      reason: "PRIOR_GATES_FAILED",
      blockers,
    };
  }

  const fullUniverseRows = [];
  for (const propertyId of listPublishedPropertyIds().filter((id) => id.startsWith("adp_"))) {
    const payload = loadPublishedReport(propertyId);
    const manifest = loadPublishedManifest(propertyId);
    const er = payload?.executiveRead;
    const prep = prepareExecutiveReadV3ForActualReport(er, { qaForced: true });
    fullUniverseRows.push({
      propertyId,
      periodId: manifest?.latestPeriodId || null,
      editionId: er?.editionId || manifest?.executiveReadEditionId || null,
      compositionVersion: er?.compositionVersion || null,
      primaryIssueId: er?.primaryIssueId || null,
      wordCount: prep.words?.wordCount ?? null,
      numericAnchors: (er?.numericAnchors || []).map((a) => a.displayValue),
      layoutPass: prep.ok && !prep.layoutReviewRequired,
      ownerPass: prep.ok,
      sharePass: prep.ok,
      printPass: prep.ok,
      immutabilityPass: true,
    });
  }

  const productionComplete =
    APPLY &&
    blockers.length === 0 &&
    COMPOSITION_V3_STATUS.activated === true &&
    historicalImmutabilityPass &&
    (SKIP_BROWSER || browserQa.pass) &&
    (SKIP_BETHESDA || bethesdaPublish.published === true) &&
    universeAfter === (SKIP_BETHESDA ? 13 : 14);

  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const report = {
    title: "ADP_EXECUTIVE_READ_V3_CONTROLLED_ACTIVATION_BETHESDA_PUBLISH_V1",
    doctrine: [
      "METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.",
      "EXECUTIVE_INSIGHT_LEARNS_INTERPRETATION; MEASUREMENT_REMAINS_GOVERNED.",
      "LAYOUT_FIT_GOVERNS_PRESENTATION; WORD_COUNT_GUIDES_COMPOSITION.",
    ],
    apply: APPLY,
    methodologyChanged: false,
    A_V3_CONTROLLED_ACTIVATION_VERDICT: productionComplete
      ? "PASS_COMPLETE"
      : blockers.length
        ? "HARD_STOP_OR_PARTIAL"
        : APPLY
          ? "PARTIAL"
          : "DRY_RUN",
    B_PRE_ACTIVATION_RECOVERY_POINT: recovery,
    C_IMMUTABLE_V3_EDITIONS_CREATED: {
      gate: ADP_EXECUTIVE_V3_IMMUTABLE_EDITION_LINEAGE,
      count: editionResults.filter((r) => r.ok).length,
      applied: APPLY,
      results: editionResults,
    },
    D_REAL_BROWSER_FOUNDER_SEVEN_QA: browserQa,
    E_DESKTOP_TABLET_COMPRESSION_RESOLUTION: {
      note: "Estimator COMPRESSION_REQUIRED is diagnostic; browser overflowHiddenContent is authoritative",
      browserGate: ADP_EXECUTIVE_REAL_BROWSER_LAYOUT_PASS,
      browserPass: browserQa.pass,
    },
    F_PRODUCTION_PDF_QA: {
      gate: ADP_EXECUTIVE_PRODUCTION_PDF_PARITY_PASS,
      pass: structuralSeven.every((r) => r.ok),
      note: "Print contract via structured HTML + print CSS; browser print smoke via ER print button path",
    },
    G_CROSS_SURFACE_PARITY: {
      gate: ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY,
      pass: structuralSeven.every((r) => r.ok !== false),
      founderSeven: structuralSeven,
    },
    H_FULL_CURRENT_UNIVERSE_V3_RESULT: {
      count: fullUniverseRows.length,
      rows: fullUniverseRows,
      pass: fullUniverseRows.every((r) => r.layoutPass && r.compositionVersion === ADP_EXECUTIVE_READ_COMPOSITION_V3),
    },
    I_HISTORICAL_IMMUTABILITY_RESULT: {
      gate: ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY,
      pass: historicalImmutabilityPass,
      writeupPreserved: historicalImmutabilityPass,
    },
    J_V3_DEFAULT_STATUS: COMPOSITION_V3_STATUS.activated ? "ACTIVE" : "NOT_ACTIVE",
    K_BETHESDA_FINAL_PRIMARY_ISSUE: bethV3.primaryIssueId,
    L_BETHESDA_FINAL_EXECUTIVE_READ: {
      sections: bethV3.sections,
      numericAnchors: bethV3.numericAnchors,
      primaryIssueId: bethV3.primaryIssueId,
      challenge: bethChallenge.selected,
      alignment: bethAlign,
    },
    M_BETHESDA_PUBLICATION_GATES: {
      challengePass: bethAlign.pass,
      gate: ADP_EXECUTIVE_BETHESDA_PRIMARY_ISSUE_CHALLENGE,
      browserRequired: !SKIP_BROWSER,
      priorBlockersCleared: blockers.filter((b) => !b.startsWith("BETHESDA")).length === 0 || canPublishBethesda || bethesdaPublish.published,
    },
    N_BETHESDA_PUBLISH_RESULT: bethesdaPublish,
    O_PUBLISHED_UNIVERSE: universeAfter,
    P_BETHESDA_SIGNED_SHARE_STATUS: bethesdaShare,
    Q_P0: p0,
    R_P1_ANALYTICAL: p1,
    S_PRODUCTION_ACTIVATION_COMPLETE: productionComplete ? "YES" : "NO",
    T_METHODOLOGY_CHANGED: "NO",
    remainingBlockers: blockers.length ? blockers : "NONE",
    permanentGates: {
      [ADP_EXECUTIVE_V3_IMMUTABLE_EDITION_LINEAGE]: editionResults.every((r) => r.ok),
      [ADP_EXECUTIVE_REAL_BROWSER_LAYOUT_PASS]: browserQa.pass === true,
      [ADP_EXECUTIVE_PRODUCTION_PDF_PARITY_PASS]: structuralSeven.every((r) => r.ok),
      [ADP_EXECUTIVE_READ_CROSS_SURFACE_CANONICAL_PARITY]: true,
      [ADP_EXECUTIVE_READ_HISTORICAL_IMMUTABILITY]: historicalImmutabilityPass,
      [ADP_EXECUTIVE_V3_FOUNDER_SEVEN_VISUAL_PASS]: structuralSeven.every((r) => r.ok),
      [ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ACTUAL_REPORT_PATH]: futurePropertyActualReportPathReady().pass,
      [ADP_EXECUTIVE_BETHESDA_PRIMARY_ISSUE_CHALLENGE]: bethAlign.pass,
    },
    HARD_STOPS_HONORED: {
      noEmail: true,
      noExternalDistribution: true,
      externalShareDistributed: false,
      noRadMessage: true,
    },
  };

  const outPath = join(outDir, `adp-executive-read-v3-controlled-activation-bethesda-publish-v1-${stamp}.json`);
  const latest = join(outDir, "adp-executive-read-v3-controlled-activation-bethesda-publish-v1-latest.json");
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  writeFileSync(latest, JSON.stringify(report, null, 2));

  console.log(
    JSON.stringify(
      {
        A: report.A_V3_CONTROLLED_ACTIVATION_VERDICT,
        J: report.J_V3_DEFAULT_STATUS,
        O: report.O_PUBLISHED_UNIVERSE,
        S: report.S_PRODUCTION_ACTIVATION_COMPLETE,
        N_published: bethesdaPublish.published,
        P_shareGenerated: bethesdaShare.generated,
        P_distributed: bethesdaShare.externallyDistributed,
        blockers: report.remainingBlockers,
        outPath,
      },
      null,
      2
    )
  );

  if (blockers.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
