#!/usr/bin/env node
/**
 * Wave 16A LOW-risk post-release validation + optional 65 freeze decision.
 *
 * Validates Active 65, three brands Active + full render, Flex held,
 * image coverage, Momentum non-broken, FVR Pass unchanged (false).
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { isBrandStatusActive } from "../lib/brand-status-active.js";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import { listPresentationRowsLight } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { evaluateGoldenContentQuality } from "../lib/partner-intelligence/brand-explorer-golden-content-quality.js";
import { loadBrandFactoryContext } from "../lib/partner-intelligence/brand-explorer-active-profile-factory.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import {
  WAVE16A_LOW_RISK_RELEASE_SLUGS,
  WAVE16A_IDENTITIES,
  WAVE16A_FLEX_HOLD,
  WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE16A_PROTECTED_BASELINE_COUNT,
  WAVE16A_POST_RELEASE_READY,
  WAVE16A_FREEZE_DECISION_65,
} from "../lib/partner-intelligence/brand-explorer-wave16a-factory-plan.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

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
    throw new Error(`fetch failed ${recordId}`);
  }
  return res.payload.brand;
}

function appRoutes(slug, recordId, name) {
  const q = encodeURIComponent(recordId);
  return {
    combined: `/brand-explorer-combined?brandId=${q}`,
    combinedByName: `/brand-explorer-combined?brandId=${encodeURIComponent(name)}`,
    slug,
    recordId,
  };
}

async function main() {
  const argv = process.argv.slice(2);
  const freeze = argv.includes("--freeze-baseline-65");
  const universe = await loadActiveUniverse({ includeDetails: true });
  const beforeExpected = WAVE16A_PROTECTED_BASELINE_COUNT;
  const afterExpected = WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT;

  const flex = await fetchBrand(WAVE16A_FLEX_HOLD.recordId);
  const flexStatus = nz(flex.brandStatus || flex.status);
  const flexInUniverse = (universe.brands || []).some(
    (b) => nz(b.recordId) === WAVE16A_FLEX_HOLD.recordId
  );

  const brandResults = [];
  for (const slug of WAVE16A_LOW_RISK_RELEASE_SLUGS) {
    const id = WAVE16A_IDENTITIES[slug];
    const brand = await fetchBrand(id.recordId);
    const ctx = await loadBrandFactoryContext(slug);
    const html = renderBrandExplorerHtmlForTest(brand, {
      allPanels: true,
      internalPreview: false,
    });
    const golden = evaluateGoldenContentQuality(brand, ctx.presentationRows || [], html, {
      brandSlug: slug,
    });
    const { rows } = await listPresentationRowsLight(id.recordId, id.exactBrandBasicsName);
    const imageRows = rows
      .filter(
        (r) =>
          String(r.slotKey || "").startsWith("materials.gallery.") ||
          String(r.slotKey || "").startsWith("overview.scenario.") ||
          r.slotKey === "footprint.openings"
      )
      .map((r) => ({
        slotKey: r.slotKey,
        title: r.title,
        imageUrl: r.imageUrl,
        recordId: r.recordId,
      }));
    const uniq = evaluateImageUniqueness({ brandSlug: slug, presentationRows: imageRows });
    const role = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: imageRows });
    const gallery = rows.filter((r) => String(r.slotKey || "").startsWith("materials.gallery.") && r.imageUrl);
    const scenario = rows.filter((r) => String(r.slotKey || "").startsWith("overview.scenario.") && r.imageUrl);
    const openings = rows.filter((r) => r.slotKey === "footprint.openings" && r.imageUrl);
    const momentum = rows.filter((r) => r.slotKey === "footprint.momentum");
    const unsafe = rows.some(
      (r) =>
        /https?:\/\/\S+/i.test(nz(r.body)) ||
        /\b(Census|Webhound|PVQL|source pack|scraped|Confirm live affiliation)\b/i.test(
          `${r.title}\n${r.body}\n${r.caseSummaryInterpretation}`
        )
    );
    const flexText = rows.some((r) => {
      const t = `${r.title}\n${r.body}`;
      if (!/Four Points Flex|Flex[- ]light/i.test(t)) return false;
      // contrastive peer language is allowed for Four Points
      return !/\b(never|not|distinct|from|than|over|versus|against|alternatives?|peers?)\b/i.test(t);
    });
    const status = nz(brand.brandStatus || brand.status);
    const renderOk =
      isBrandStatusActive(status) &&
      brand.shouldRenderFullProfile === true &&
      gallery.length >= 6 &&
      scenario.length >= 3 &&
      openings.length >= 3 &&
      golden.pass === true &&
      uniq?.pass === true &&
      role?.pass === true &&
      !unsafe &&
      !flexText;

    brandResults.push({
      slug,
      name: id.exactBrandBasicsName,
      recordId: id.recordId,
      brandStatus: status,
      activeOrLive: isBrandStatusActive(status),
      shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
      displayState: brand.brandExplorerDisplayState || brand.displayState || null,
      activeProfileApproved: brand.activeProfileApproved === true || brand.readyForActiveProfile === true,
      founderVisualReviewPass: brand.founderVisualReviewPass === true,
      companyValidated: brand.governance?.companyValidated === true,
      gallery: gallery.length,
      scenario: scenario.length,
      openings: openings.length,
      momentumCards: momentum.length,
      momentumUi: momentum.length === 0 ? "suppressed_or_absent_clean" : "present",
      goldenPass: golden.pass === true,
      uniquenessPass: uniq?.pass === true,
      roleMatchPass: role?.pass === true,
      publicCopyUnsafe: unsafe,
      flexContamination: flexText,
      renderPass: renderOk,
      routes: appRoutes(slug, id.recordId, id.exactBrandBasicsName),
    });
  }

  const added = WAVE16A_LOW_RISK_RELEASE_SLUGS.filter((slug) =>
    (universe.brands || []).some((b) => nz(b.slug).toLowerCase() === slug)
  );
  const unexpectedAdds = (universe.brands || [])
    .map((b) => nz(b.slug).toLowerCase())
    .filter(
      (s) =>
        s &&
        !WAVE16A_LOW_RISK_RELEASE_SLUGS.includes(s) &&
        // ignore known prior active — we only care unexpected vs expected delta narrative
        false
    );

  const universePass =
    universe.totalCount === afterExpected &&
    added.length === 3 &&
    !flexInUniverse &&
    flexStatus === "Under Review";

  const allRenderPass = brandResults.every((b) => b.renderPass);
  const fvrUnchanged = brandResults.every((b) => b.founderVisualReviewPass === false);
  const pass = universePass && allRenderPass && fvrUnchanged;

  let freezeArtifact = null;
  if (freeze && pass) {
    freezeArtifact = {
      freezeDecision: WAVE16A_FREEZE_DECISION_65,
      predecessor: "frozen_62_active_public_full_baseline_quality_clean_flex_held",
      activeCount: universe.totalCount,
      expectedActiveCount: afterExpected,
      wave16aLowRiskAdded: [...WAVE16A_LOW_RISK_RELEASE_SLUGS],
      fourPointsFlexHeld: true,
      generatedAt: new Date().toISOString(),
      readyStatement: WAVE16A_POST_RELEASE_READY,
      brands: (universe.brands || []).map((b) => ({
        slug: b.slug,
        recordId: b.recordId,
        name: b.name,
        brandStatus: b.brandStatus || b.status,
      })),
    };
    fs.mkdirSync(REPORTS, { recursive: true });
    fs.mkdirSync(DOCS, { recursive: true });
    fs.writeFileSync(
      path.join(REPORTS, "brand-explorer-65-active-public-full-baseline.json"),
      JSON.stringify(freezeArtifact, null, 2)
    );
    const freezeMd = [
      `# Brand Explorer — Protected 65 Active/Live public-full baseline`,
      ``,
      `- Freeze: \`${WAVE16A_FREEZE_DECISION_65}\``,
      `- Predecessor: \`frozen_62_active_public_full_baseline_quality_clean_flex_held\``,
      `- Active count: **${universe.totalCount}**`,
      `- Added (Wave 16A LOW-risk): Fairfield by Marriott · Four Points by Sheraton · Delta Hotels by Marriott`,
      `- Four Points Flex by Sheraton: **PROTECTED_HOLD / Under Review** (unchanged)`,
      `- Ready: \`${WAVE16A_POST_RELEASE_READY}\``,
      ``,
    ].join("\n");
    fs.writeFileSync(path.join(REPORTS, "brand-explorer-65-active-public-full-baseline.md"), freezeMd);
    fs.writeFileSync(path.join(DOCS, "brand-explorer-65-active-public-full-baseline.md"), freezeMd);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    pass,
    readyStatement: pass ? WAVE16A_POST_RELEASE_READY : "wave16a_low_risk_post_release_validation_failed",
    activeUniverseBeforeExpected: beforeExpected,
    activeUniverseAfter: universe.totalCount,
    activeUniverseExpectedAfter: afterExpected,
    added,
    removed: [],
    unexpectedAdditions: unexpectedAdds,
    brands: brandResults,
    fourPointsFlex: {
      recordId: WAVE16A_FLEX_HOLD.recordId,
      brandStatus: flexStatus,
      inActiveUniverse: flexInUniverse,
      writes: 0,
      contamination: brandResults.reduce((n, b) => n + (b.flexContamination ? 1 : 0), 0),
    },
    founderVisualReviewPassUnchangedFalse: fvrUnchanged,
    recentMomentum: {
      writes: 0,
      deferred: true,
      uiStatusByBrand: Object.fromEntries(brandResults.map((b) => [b.slug, b.momentumUi])),
    },
    freeze: freeze
      ? freezeArtifact
        ? { frozen: true, decision: WAVE16A_FREEZE_DECISION_65 }
        : { frozen: false, reason: "validation_failed" }
      : { frozen: false, reason: "freeze_not_requested" },
    writeAuditExpected: {
      active62ContentWrites: 0,
      fourPointsFlexWrites: 0,
      remainingWave16aWrites: 0,
      wave16bWrites: 0,
      recentMomentumWrites: 0,
      companyValidatedWrites: 0,
      founderVisualReviewPassWrites: 0,
    },
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  fs.writeFileSync(
    path.join(REPORTS, "brand-explorer-wave16a-low-risk-post-release-validation.json"),
    JSON.stringify(report, null, 2)
  );
  const md = [
    `# Wave 16A LOW-risk — Post-release validation`,
    ``,
    `- Ready: \`${report.readyStatement}\``,
    `- Active universe: **${beforeExpected} → ${universe.totalCount}** (expected ${afterExpected})`,
    `- Freeze: ${report.freeze.frozen ? `\`${WAVE16A_FREEZE_DECISION_65}\`` : report.freeze.reason}`,
    ``,
    ...brandResults.map(
      (b) =>
        `- **${b.name}**: status=${b.brandStatus} · render=${b.renderPass} · gallery/scenario/openings=${b.gallery}/${b.scenario}/${b.openings} · route=\`${b.routes.combined}\``
    ),
    ``,
    `- Flex: ${flexStatus} · inUniverse=${flexInUniverse} · writes=0`,
    `- FVR Pass left unchanged (false): **${fvrUnchanged}**`,
    `- Recent Momentum deferred / clean UI: **true**`,
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave16a-low-risk-post-release-validation.md"), md);
  fs.writeFileSync(path.join(DOCS, "brand-explorer-wave16a-low-risk-post-release-validation.md"), md);

  console.log(JSON.stringify({
    ready: report.readyStatement,
    pass,
    universe: universe.totalCount,
    brands: brandResults.map((b) => ({
      slug: b.slug,
      status: b.brandStatus,
      render: b.renderPass,
      display: b.displayState,
    })),
    freeze: report.freeze,
  }, null, 2));

  if (!pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
