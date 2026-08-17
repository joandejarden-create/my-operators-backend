/**
 * Brand Explorer QA gate — gap audit + fixture readiness checks.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { auditChoiceBrandPresentationGaps, airtableNameAliases } from "./brand-explorer-gap-audit.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..");

/**
 * @param {import('../../scripts/lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest extends Function ? ReturnType<import('../../scripts/lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest>[number] : never} brand
 * @param {{ skipAirtable?: boolean }} [opts]
 */
export function checkFixtureReadiness(brand) {
  const issues = [];
  const slug = brand.slug;

  if (brand.premium?.applyScript) {
    return { ready: true, issues, warnings: [], splitFixtures: ["premium-split"] };
  }

  const fullPath = path.join(ROOT, "fixtures", `brand-explorer-presentation-${slug}-full.json`);
  const hasFull =
    fs.existsSync(fullPath) || (brand.fullFixture && fs.existsSync(path.join(ROOT, brand.fullFixture)));
  if (!hasFull) {
    issues.push(`Missing full fixture: brand-explorer-presentation-${slug}-full.json`);
  }

  const splitSuffixes = ["case-studies", "footprint-momentum", "materials", "gallery", "footprint-openings"];
  const foundSplits = splitSuffixes.filter((s) =>
    fs.existsSync(path.join(ROOT, "fixtures", `brand-explorer-presentation-${slug}-${s}.json`))
  );

  if (foundSplits.length === 0 && brand.parity === "needs-enrichment") {
    // Warning only — L1 can pass without split overlays
    return {
      ready: issues.length === 0,
      issues,
      warnings: ["No L2 split overlays yet (needed for Blu parity, not L1 gate)"],
      splitFixtures: foundSplits,
    };
  }

  return {
    ready: issues.length === 0,
    issues,
    warnings: [],
    splitFixtures: foundSplits,
  };
}

/**
 * @param {import('../../scripts/lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest extends Function ? ReturnType<import('../../scripts/lib/choice-brand-explorer-manifest.mjs').listChoiceBrandManifest>[number] : never} brand
 * @param {{ skipAirtable?: boolean }} [opts]
 */
export async function runBrandExplorerQaGate(brand, opts = {}) {
  const fixtureCheck = opts.skipFixtureCheck
    ? { ready: true, issues: [], warnings: [], splitFixtures: [] }
    : checkFixtureReadiness(brand);
  /** @type {string[]} */
  const blockers = [...fixtureCheck.issues];

  let gapAudit = null;
  if (!opts.skipAirtable) {
    try {
      gapAudit = await auditChoiceBrandPresentationGaps(brand.airtableName, {
        aliases: airtableNameAliases(brand.airtableName, brand.profileName),
      });
      if (!gapAudit.l1Complete) {
        blockers.push(
          `Airtable gaps: ${gapAudit.missingSlotKeys.length} missing, ${gapAudit.shortCounts.length} short counts`
        );
      }
    } catch (err) {
      blockers.push(`Gap audit failed: ${err.message || err}`);
    }
  }

  const pass = blockers.length === 0;

  return {
    pass,
    l1Complete: gapAudit?.l1Complete ?? null,
    brand: brand.profileName,
    airtableName: brand.airtableName,
    recordId: brand.recordId,
    parity: brand.parity,
    blockers,
    warnings: fixtureCheck.warnings || [],
    gapAudit,
    fixtureCheck,
    uiUrl: `/brand-explorer-combined.html?id=${encodeURIComponent(brand.airtableName)}`,
  };
}
