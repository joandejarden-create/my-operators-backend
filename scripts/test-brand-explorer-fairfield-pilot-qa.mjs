#!/usr/bin/env node
/**
 * Pilot QA — Fairfield by Marriott fixture against production benchmark gates.
 * Validates merged fixture payload without requiring live Airtable presentation rows.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import "../load-env.js";
import { evaluateTabFactoryFromPayload } from "../lib/partner-intelligence/brand-explorer-tab-factory-evaluate.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { evaluateBrandExternalQualityLock } from "../lib/partner-intelligence/brand-explorer-display-quality-lock.js";
import { evaluateBrandExplorerOsBrand } from "../lib/partner-intelligence/brand-explorer-os-run.js";
import { evaluatePilotContentQuality } from "../lib/partner-intelligence/brand-explorer-pilot-content-quality.js";
import {
  FACTORY_PREVIEW_DISPLAY_STATE,
  buildFactoryPreviewApiMeta,
  getFactoryPreviewIdentity,
} from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const SLUG = "fairfield-by-marriott";
const RECORD_ID = "recpUTDtwt1wPMDPj";
const FIXTURE = path.join(ROOT, "fixtures", `brand-explorer-presentation-${SLUG}-full.json`);
const MOMENTUM_SLOT = "footprint.momentum";

/** Production display states that mean defects / incomplete — block READY FOR CHATGPT QA. */
const DEFECT_DISPLAY_STATES = new Set(["draft_applied_with_defects", "hidden_incomplete"]);

/**
 * Fixture-only brand shape for factory-preview pilot QA when Airtable is unavailable.
 * Uses factory_preview_internal (not active_profile_ready) so production display is not
 * inventively upgraded, and so resolveBrandExplorerDisplayState is not re-run without
 * Brand Basics (which falsely yields draft_applied_with_defects).
 */
function buildFixtureOnlyBrandStub() {
  const identity = getFactoryPreviewIdentity(SLUG) || {
    slug: SLUG,
    name: "Fairfield by Marriott",
    recordId: RECORD_ID,
  };
  const base = {
    id: identity.recordId || RECORD_ID,
    recordId: identity.recordId || RECORD_ID,
    slug: identity.slug || SLUG,
    name: identity.name || "Fairfield by Marriott",
    brandStatus: identity.recommendedStatusWhileInFactory || "Under Review",
    parentCompany: "Marriott International",
    brandExplorer: { version: 1, blocks: [] },
    guestPsychographics: "",
    brandPositioning: "",
    brandExplorerDisplayState: FACTORY_PREVIEW_DISPLAY_STATE,
    shouldRenderFullProfile: false,
    shouldSuppressIncompleteExternalSections: true,
    shouldHideExternalProfile: true,
    _fixtureOnlyBrandStub: true,
  };
  return {
    ...base,
    factoryPreview: buildFactoryPreviewApiMeta(base),
  };
}

async function fetchBrand(brandId) {
  const hasCreds =
    Boolean(process.env.AIRTABLE_API_KEY) && Boolean(process.env.AIRTABLE_BASE_ID);
  if (!hasCreds) {
    console.warn(
      "[pilot-qa] Airtable credentials missing — using fixture-only factory-preview brand stub"
    );
    return { brand: buildFixtureOnlyBrandStub(), brandSource: "fixture_stub" };
  }
  try {
    const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
    const res = {
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
    await getBrandLibraryBrandById({ query: { brandId, refresh: "1" }, headers: {} }, res);
    if (res.statusCode !== 200 || !res.payload?.brand) {
      throw new Error(`Brand fetch failed for ${brandId}`);
    }
    return { brand: res.payload.brand, brandSource: "airtable_live" };
  } catch (err) {
    console.warn(
      `[pilot-qa] Airtable brand fetch failed (${err?.message || err}) — using fixture-only factory-preview brand stub`
    );
    return { brand: buildFixtureOnlyBrandStub(), brandSource: "fixture_stub" };
  }
}

function fixtureRowsToPresentation(rows) {
  return rows.map((r) => ({
    slotKey: r.slotKey,
    title: r.title || "",
    body: r.body || "",
    sortOrder: r.sort ?? 0,
    imageUrl: r.imageUrl,
    caseSummaryOverview: r.caseSummaryOverview,
    caseSummaryBrandRelevance: r.caseSummaryBrandRelevance,
    caseSummaryOwnerObjective: r.caseSummaryOwnerObjective,
    caseSummaryInterpretation: r.caseSummaryInterpretation,
    caseSummaryTags: r.caseSummaryTags,
  }));
}

function slotBody(rows, slotKey) {
  return rows.find((r) => r.slotKey === slotKey)?.body || "";
}

function injectPresentationIntoBrand(brand, rows) {
  const blocks = rows.map((r, i) => ({
    recordId: `fixture-${i}`,
    slotKey: r.slotKey,
    title: r.title || "",
    body: r.body || "",
    sort: r.sortOrder ?? r.sort ?? 0,
    imageUrl: r.imageUrl || "",
    caseSummaryOverview: r.caseSummaryOverview || "",
    caseSummaryBrandRelevance: r.caseSummaryBrandRelevance || "",
    caseSummaryOwnerObjective: r.caseSummaryOwnerObjective || "",
    caseSummaryInterpretation: r.caseSummaryInterpretation || "",
    caseSummaryTags: r.caseSummaryTags || "",
  }));
  const guestPsychographics = slotBody(rows, "Guest Psychographics Description");
  const brandPositioning = slotBody(rows, "Brand Positioning");
  return {
    ...brand,
    slug: SLUG,
    brandExplorer: { version: 1, blocks },
    guestPsychographics: guestPsychographics || brand.guestPsychographics,
    brandPositioning: brandPositioning || brand.brandPositioning,
    _fixturePilotMerge: true,
  };
}

function listTabFactoryFailFindings(tabFactory) {
  const findings = tabFactory?.findings || tabFactory?.completeness?.findings || [];
  return findings
    .filter(
      (f) =>
        f.status !== "pass" &&
        f.status !== "should_suppress" &&
        f.status !== "cleanly_unavailable" &&
        f.recommendedAction !== "suppress_component"
    )
    .map((f) => ({
      fieldName: f.fieldName || f.fieldId || null,
      status: f.status,
      recommendedAction: f.recommendedAction || null,
      reason: f.reason || null,
    }));
}

function computeReadyForChatGptQa({
  tabFactory,
  contentQuality,
  galleryCount,
  momentumCount,
  externalDisplayState,
}) {
  const failFindings =
    typeof tabFactory.failFindings === "number" ? tabFactory.failFindings : 0;
  const blockers = [];
  if (tabFactory.auditPass !== true) blockers.push("tabFactory.auditPass !== true");
  if (failFindings > 0) blockers.push(`tabFactoryFailFindings=${failFindings}`);
  if (tabFactory.golden?.pass !== true) blockers.push("goldenPass !== true");
  if (tabFactory.provenance?.pass !== true) blockers.push("provenancePass !== true");
  if (tabFactory.sectionPatternParity?.pass !== true) {
    blockers.push("sectionPatternParityPass !== true");
  }
  if (tabFactory.gates?.image_distinctiveness !== true) {
    blockers.push("image_distinctiveness !== true");
  }
  if (tabFactory.gates?.image_role_match !== true) blockers.push("image_role_match !== true");
  if (contentQuality.pass !== true) blockers.push("contentQualityPass !== true");
  if (galleryCount < 6) blockers.push("galleryCount < 6");
  if (momentumCount < 2) blockers.push("momentumCount < 2");
  if (DEFECT_DISPLAY_STATES.has(externalDisplayState)) {
    blockers.push(`externalDisplayState=${externalDisplayState}`);
  }
  return { ready: blockers.length === 0, blockers };
}

async function main() {
  if (!fs.existsSync(FIXTURE)) {
    throw new Error(`Missing fixture: ${FIXTURE}. Run export-brand-explorer-wave16a-full-fixture first.`);
  }
  const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const rows = fixtureRowsToPresentation(fixture.rows || []);
  console.log(`[pilot-qa] ${SLUG} fixture rows: ${rows.length}`);

  const { brand, brandSource } = await fetchBrand(RECORD_ID);
  const mergedBrand = injectPresentationIntoBrand(brand, rows);
  const html = renderBrandExplorerHtmlForTest(mergedBrand, {
    allPanels: true,
    internalPreview: true,
    factoryPreview: true,
  });

  const tabFactory = evaluateTabFactoryFromPayload({
    brand: mergedBrand,
    rows,
    html,
    brandSlug: SLUG,
  });

  // Correct signature: (brand, renderedHtml, options)
  const externalLock = evaluateBrandExternalQualityLock(mergedBrand, html, {
    brandSlug: SLUG,
    presentationRows: rows,
  });

  const contentQuality = evaluatePilotContentQuality(rows);

  let os = null;
  try {
    os = await evaluateBrandExplorerOsBrand(SLUG);
  } catch (err) {
    console.warn(`OS evaluate skipped: ${err.message}`);
  }

  const galleryCount = rows.filter((r) => /^materials\.gallery\./.test(r.slotKey)).length;
  const momentumCount = rows.filter((r) => r.slotKey === MOMENTUM_SLOT).length;
  const openingsCount = rows.filter((r) => r.slotKey === "footprint.openings").length;
  const scenarioCount = rows.filter((r) => /^overview\.scenario\./.test(r.slotKey)).length;
  const valueScenarioCount = rows.filter((r) => /^valueOwners\.scenario\./.test(r.slotKey)).length;
  const externalDisplayState = externalLock.displayState || null;
  const tabFactoryFailFindingsList = listTabFactoryFailFindings(tabFactory);

  const ready = computeReadyForChatGptQa({
    tabFactory,
    contentQuality,
    galleryCount,
    momentumCount,
    externalDisplayState,
  });

  // Internal consistency invariant: auditPass and failFindings must agree.
  if (tabFactory.auditPass === true && (tabFactory.failFindings || 0) > 0) {
    throw new Error(
      `Inconsistent tab-factory result: auditPass=true with failFindings=${tabFactory.failFindings}`
    );
  }
  if (tabFactory.auditPass !== true && (tabFactory.failFindings || 0) === 0) {
    // Other gates can fail auditPass with zero field fails (e.g. golden/provenance).
    // That is allowed; readyForChatGptQa still requires auditPass === true.
  }

  const report = {
    slug: SLUG,
    fixturePath: FIXTURE,
    brandSource,
    rowCount: rows.length,
    tabFactoryAuditPass: tabFactory.auditPass === true,
    tabFactoryFailFindings: tabFactory.failFindings || 0,
    tabFactoryFailFindingsList,
    goldenPass: tabFactory.golden?.pass === true,
    provenancePass: tabFactory.provenance?.pass === true,
    sectionPatternParityPass: tabFactory.sectionPatternParity?.pass === true,
    imageUniquenessPass: tabFactory.imageUniqueness?.pass === true,
    imageRoleMatchPass: tabFactory.imageRoleMatch?.pass === true,
    externalQualityLockPass: externalLock.externalQualityLockPass === true,
    externalQualityIssues: externalLock.issues || [],
    contentQualityPass: contentQuality.pass === true,
    contentQualityIssueCount: contentQuality.issueCount,
    contentQualityIssues: contentQuality.issues,
    osState: os?.canonicalState || null,
    galleryCount,
    momentumCount,
    openingsCount,
    scenarioCount,
    valueScenarioCount,
    externalDisplayState,
    readyForChatGptQaBlockers: ready.blockers,
    readyForChatGptQa: ready.ready,
  };

  const outJson = path.join(ROOT, "reports", "brand-explorer-fairfield-pilot-qa.json");
  const outMd = path.join(ROOT, "docs", "data-intelligence", "brand-explorer-fairfield-pilot-qa-report.md");
  fs.mkdirSync(path.dirname(outJson), { recursive: true });
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2), "utf8");

  const failListMd =
    report.tabFactoryFailFindingsList.length > 0
      ? `### Tab factory fail findings\n\n${report.tabFactoryFailFindingsList
          .map(
            (i) =>
              `- **${i.fieldName || "unknown"}** (${i.status}): ${i.reason || i.recommendedAction || ""}`
          )
          .join("\n")}\n`
      : "";

  const contentIssuesMd =
    report.contentQualityIssues?.length > 0
      ? `### Content quality issues\n\n${report.contentQualityIssues
          .map((i) => `- **${i.code}** (${i.slotKey || "global"}): ${i.message}`)
          .join("\n")}\n`
      : "";

  const blockersMd =
    report.readyForChatGptQaBlockers.length > 0
      ? `### Ready blockers\n\n${report.readyForChatGptQaBlockers.map((b) => `- ${b}`).join("\n")}\n`
      : "";

  const md = `# Brand Explorer Pilot QA — Fairfield by Marriott

> **Status:** ${report.readyForChatGptQa ? "READY FOR CHATGPT QA" : "NEEDS REMEDIATION"}
> **Generated:** ${new Date().toISOString()}
> **Fixture:** \`fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json\` (${report.rowCount} rows)
> **Brand source:** \`${report.brandSource}\`

## Selected brand

**Fairfield by Marriott** (\`fairfield-by-marriott\`, \`recpUTDtwt1wPMDPj\`)

## Benchmark brands

- **Kimpton Hotels** — IHG lifestyle gold bar (full tabs, momentum, gallery, value scenarios)
- **Everhome Suites** — Choice extended-stay Tier 1 full fixture reference
- **Radisson Individuals by Choice** — recent CALA-forward completion reference

## Gate results

| Gate | Result |
| --- | --- |
| Tab factory audit | ${report.tabFactoryAuditPass ? "PASS" : "FAIL"} (${report.tabFactoryFailFindings} fail findings) |
| Golden content | ${report.goldenPass ? "PASS" : "FAIL"} |
| Source provenance | ${report.provenancePass ? "PASS" : "FAIL"} |
| Section pattern parity | ${report.sectionPatternParityPass ? "PASS" : "FAIL"} |
| Image uniqueness | ${report.imageUniquenessPass ? "PASS" : "FAIL"} |
| Image role match | ${report.imageRoleMatchPass ? "PASS" : "FAIL"} |
| Content quality (semantic) | ${report.contentQualityPass ? "PASS" : "FAIL"} (${report.contentQualityIssueCount} issues) |
| External quality lock | ${report.externalQualityLockPass ? "PASS" : "DEFERRED (factory preview — expected until founder approval)"} |
| External display state | \`${report.externalDisplayState || "null"}\` |

${failListMd}${contentIssuesMd}${blockersMd}
## ChatGPT QA remediation (2026-10-01)

### Round 1
- Fixed grammar (\`a efficient\` → \`an efficient\`), double punctuation, and duplicated opening titles
- Removed repeated boilerplate (\`Keep Fairfield by Marriott product and service responsibilities…\`)
- Rewrote psychographics, \`insight.similar\` peer comparisons, and softened over-strong operating claims
- Added semantic content-quality gate: \`npm run test:brand-explorer-pilot-content-quality\`
- Remediation module: \`lib/partner-intelligence/brand-explorer-fairfield-content-remediation.js\`

### Round 2 (second independent ChatGPT QA)
- Fixed \`footprint.portfolio_mix\` run-on (\`weak fit Curated sample mix\` → proper paragraph break)
- Canonicalized Cancún property name to \`Fairfield Inn & Suites Cancun Airport\` in \`footprint.region.cala\`
- Removed unsourced \`rather than lifestyle-hotel personality\` contrast from psychographics
- Replaced \`king-room prototype\` with \`rooms-focused select-service prototype\` in \`insight.similar\`
- Expanded semantic gate (\`pilot-content-quality-v2\`) so malformed run-on joins fail automatically
- Report: \`reports/brand-explorer-fairfield-pilot-remediation-round2.json\`

### Round 3 (QA report consistency)
- Root cause: \`evaluateTabFactoryFromPayload\` counted \`cleanly_unavailable\` Brand Basics snapshot fields as \`failFindings\` while \`completeness.auditPass\` correctly treated them as resolved → \`auditPass=true\` with \`failFindings=10\`
- Root cause: pilot QA called \`evaluateBrandExternalQualityLock(brand, options)\` instead of \`(brand, html, options)\`, and the incomplete stub re-resolved production display without Brand Basics → false \`draft_applied_with_defects\`
- Fix: align \`failFindings\` with completeness governance (\`auditPass\` requires \`failFindings === 0\`); correct external-lock call; factory-preview stub uses \`factory_preview_internal\`; \`readyForChatGptQa\` blocks on fail findings, audit fail, or defect display states

## Coverage

- Gallery images: **${report.galleryCount}**
- Scenario cards: **${report.scenarioCount}**
- Value creation scenarios: **${report.valueScenarioCount}**
- Openings cards: **${report.openingsCount}**
- Recent Momentum cards: **${report.momentumCount}**

## Production database writes

**None in this PR.** Fixture-only deliverable. To materialize in Airtable (Presentation table only):

\`\`\`bash
node scripts/apply-brand-explorer-presentation-fixture.mjs --dry-run \\
  --brand-record-id recpUTDtwt1wPMDPj \\
  --fixture fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json \\
  --only-missing
\`\`\`

## Preview for founder review

1. Start local server: \`npm start\`
2. Factory preview URL: \`/brand-explorer.html?id=recpUTDtwt1wPMDPj&beInternalPreview=1&factoryPreview=1\`
3. After staged Airtable apply (founder-approved): \`/brand-explorer.html?id=fairfield-by-marriott\`
`;
  fs.writeFileSync(outMd, md, "utf8");

  console.log(JSON.stringify(report, null, 2));
  console.log(`Wrote ${outJson}`);
  console.log(`Wrote ${outMd}`);

  if (!report.readyForChatGptQa) process.exit(1);
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
