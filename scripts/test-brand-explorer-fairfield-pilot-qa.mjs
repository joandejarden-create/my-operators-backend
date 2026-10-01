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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const SLUG = "fairfield-by-marriott";
const RECORD_ID = "recpUTDtwt1wPMDPj";
const FIXTURE = path.join(ROOT, "fixtures", `brand-explorer-presentation-${SLUG}-full.json`);

async function fetchBrand(brandId) {
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
  return res.payload.brand;
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

async function main() {
  if (!fs.existsSync(FIXTURE)) {
    throw new Error(`Missing fixture: ${FIXTURE}. Run export-brand-explorer-wave16a-full-fixture first.`);
  }
  const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  const rows = fixtureRowsToPresentation(fixture.rows || []);
  console.log(`[pilot-qa] ${SLUG} fixture rows: ${rows.length}`);

  const brand = await fetchBrand(RECORD_ID);
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

  const externalLock = evaluateBrandExternalQualityLock(mergedBrand, {
    brandSlug: SLUG,
    presentationRows: rows,
    html,
  });

  const contentQuality = evaluatePilotContentQuality(rows);

  let os = null;
  try {
    os = await evaluateBrandExplorerOsBrand(SLUG);
  } catch (err) {
    console.warn(`OS evaluate skipped: ${err.message}`);
  }

  const report = {
    slug: SLUG,
    fixturePath: FIXTURE,
    rowCount: rows.length,
    tabFactoryAuditPass: tabFactory.auditPass,
    tabFactoryFailFindings: tabFactory.failFindings,
    goldenPass: tabFactory.golden?.pass,
    provenancePass: tabFactory.provenance?.pass,
    sectionPatternParityPass: tabFactory.sectionPatternParity?.pass,
    imageUniquenessPass: tabFactory.imageUniqueness?.pass,
    imageRoleMatchPass: tabFactory.imageRoleMatch?.pass,
    externalQualityLockPass: externalLock.externalQualityLockPass === true,
    externalQualityIssues: externalLock.issues || [],
    contentQualityPass: contentQuality.pass,
    contentQualityIssueCount: contentQuality.issueCount,
    contentQualityIssues: contentQuality.issues,
    osState: os?.canonicalState || null,
    galleryCount: rows.filter((r) => /^materials\.gallery\./.test(r.slotKey)).length,
    momentumCount: rows.filter((r) => r.slotKey === "footprint.momentum").length,
    openingsCount: rows.filter((r) => r.slotKey === "footprint.openings").length,
    scenarioCount: rows.filter((r) => /^overview\.scenario\./.test(r.slotKey)).length,
    valueScenarioCount: rows.filter((r) => /^valueOwners\.scenario\./.test(r.slotKey)).length,
    externalDisplayState: externalLock.displayState || null,
    readyForChatGptQa:
      tabFactory.auditPass === true &&
      tabFactory.golden?.pass === true &&
      tabFactory.provenance?.pass === true &&
      tabFactory.sectionPatternParity?.pass === true &&
      tabFactory.gates?.image_distinctiveness === true &&
      tabFactory.gates?.image_role_match === true &&
      contentQuality.pass === true &&
      rows.filter((r) => /^materials\.gallery\./.test(r.slotKey)).length >= 6 &&
      rows.filter((r) => r.slotKey === "footprint.momentum").length >= 2,
  };

  const outJson = path.join(ROOT, "reports", "brand-explorer-fairfield-pilot-qa.json");
  const outMd = path.join(ROOT, "docs", "data-intelligence", "brand-explorer-fairfield-pilot-qa-report.md");
  fs.mkdirSync(path.dirname(outJson), { recursive: true });
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2), "utf8");

  const md = `# Brand Explorer Pilot QA — Fairfield by Marriott

> **Status:** ${report.readyForChatGptQa ? "READY FOR CHATGPT QA" : "NEEDS REMEDIATION"}
> **Generated:** ${new Date().toISOString()}
> **Fixture:** \`fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json\` (${report.rowCount} rows)

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

${
  report.contentQualityIssues?.length
    ? `### Content quality issues\n\n${report.contentQualityIssues.map((i) => `- **${i.code}** (${i.slotKey || "global"}): ${i.message}`).join("\n")}\n`
    : ""
}

## ChatGPT QA remediation (2026-10-01)

- Fixed grammar (\`a efficient\` → \`an efficient\`), double punctuation, and duplicated opening titles
- Removed repeated boilerplate (\`Keep Fairfield by Marriott product and service responsibilities…\`)
- Rewrote psychographics, \`insight.similar\` peer comparisons, and softened over-strong operating claims
- Added semantic content-quality gate: \`npm run test:brand-explorer-pilot-content-quality\`
- Remediation module: \`lib/partner-intelligence/brand-explorer-fairfield-content-remediation.js\`

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
