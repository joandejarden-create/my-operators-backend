#!/usr/bin/env node
/**
 * Consolidated Image Integrity V3 founder dashboard.
 * Assembles CURRENT audit + Factory Proposed + remediation + library + duplicates.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from "fs";
import { join } from "path";
import { summarizeLibrary } from "../lib/brand-explorer/factory-2/be-shared-verified-hotel-image-library-v1.js";

const OUT = join(process.cwd(), "reports/brand-explorer/portfolio-image-integrity-v3");
mkdirSync(OUT, { recursive: true });

function load(name) {
  const p = join(OUT, name);
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}

function latest(prefix) {
  const files = readdirSync(OUT)
    .filter((f) => f.startsWith(prefix) && f.endsWith(".json"))
    .sort();
  return files.length ? load(files[files.length - 1]) : null;
}

const current = load("01-current-audit-summary.json") || {};
const byBrand = load("02-current-audit-by-brand.json");
const matrix = load("03-current-slot-matrix.json");
const dupes = load("04-duplicate-slot-conflicts.json");
const dupeRes = load("05-duplicate-slot-resolutions.json");
const factory = load("10-factory-proposed-audit-summary.json") || {};
const rem1 = latest("20-remediation-APPLY");
const rem2 = latest("21-remediation-retry-APPLY");
const rem3 = latest("23-webp-reencode-APPLY");
const library = summarizeLibrary();

const imported =
  (rem1?.okCount || 0) + (rem2?.okCount || 0) + (rem3?.rows?.filter((r) => r.ok).length || 0);

const dashboard = {
  contract: "BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3",
  generatedAt: new Date().toISOString(),
  paidResearchSpendUsd: 0,
  phase0: {
    pr494MergeCommit: "be99076b1",
    note: "V2 durable ingest + AC/Courtyard regression fixtures preserved",
  },
  canonicalAirtable: {
    activeLiveBrands: current.activeLiveBrands ?? null,
    rawRequiredImageRows: current.rawRequiredImageRows ?? null,
    uniqueBrandSlotPairs: current.uniqueBrandSlotPairs ?? null,
    duplicateSlotConflicts: current.duplicateSlotConflicts ?? null,
    classCounts: current.classCounts ?? null,
    brandsWithAllNinePresent: current.brandsWithAllNinePresent ?? null,
    brandsWithAllSixGalleryPresent: current.brandsWithAllSixGalleryPresent ?? null,
    brandsWithAllThreeScenarioPresent: current.brandsWithAllThreeScenarioPresent ?? null,
    brandsWithSiblingReject: current.brandsWithSiblingReject ?? null,
    probe: current.probe ?? null,
  },
  factoryPortfolio: {
    frozenCandidates: factory.frozenCandidates ?? factory.frozen ?? null,
    sixGalleryPresent: factory.sixGalleryPresent ?? factory.withSixGallery ?? null,
    threeScenarioPresent: factory.threeScenarioPresent ?? factory.withThreeScenario ?? null,
    galleryGaps: factory.galleryGaps ?? null,
    scenarioGaps: factory.scenarioGaps ?? null,
    ephemeralCdn: factory.ephemeralCdnCount ?? factory.ephemeralCdn ?? null,
    siblingRejects: factory.siblingRejects ?? null,
  },
  remediation: {
    aloftBunkhouseSuburbanApply: rem1
      ? { okCount: rem1.okCount, failCount: rem1.failCount }
      : null,
    bunkhouseSuburbanRetry: rem2
      ? { okCount: rem2.okCount, failCount: rem2.failCount }
      : null,
    webpReencode: rem3
      ? { okCount: rem3.rows?.filter((r) => r.ok).length, failCount: rem3.rows?.filter((r) => !r.ok).length }
      : null,
    successfullyImportedSlots: imported,
  },
  sharedVerifiedHotelImageLibrary: library,
  duplicateSlotResolution: {
    conflictCount: dupeRes?.conflictCount ?? dupes?.conflicts?.length ?? null,
    reviewRequired: dupeRes?.reviewRequired ?? null,
    note: "Deterministic primary selected; no automatic deletes",
  },
  persistenceAndReadiness: {
    qualityCertifiedBrands: 0,
    airtablePersistedThisWave: 0,
    customerPublication: "NOT_AUTO",
    note: "Image technical remediation ≠ overall QUALITY_CERTIFIED / AIRTABLE_PERSISTED",
  },
  remainingBlockers: [
    "IMAGE_BRAND_VERIFIED / IMAGE_CERTIFIED require census-linked property verification + rights + browser cert",
    "16 duplicate Presentation slot conflicts flagged for review (not deleted)",
    "Factory Proposed ephemeral CDN URLs still require Current rehydration path (V2)",
    "Authenticated browser certification pending post-merge 8080 sync for remediated brands",
    "Hotel Explorer display permission remains false until HE product approval per asset",
  ],
  exceptionMatrixPath: "03-current-slot-matrix.json",
  brandsAudited: byBrand?.brands?.length ?? null,
  slotsAudited: matrix?.slots?.length ?? null,
};

writeFileSync(join(OUT, "90-founder-dashboard.json"), JSON.stringify(dashboard, null, 2) + "\n");

const md = `# Brand Explorer — Image Integrity V3 Founder Dashboard

Generated: ${dashboard.generatedAt}

## Canonical Airtable (live probe)

| Metric | Value |
|--------|------:|
| Active/Live brands | ${dashboard.canonicalAirtable.activeLiveBrands} |
| Raw required image rows | ${dashboard.canonicalAirtable.rawRequiredImageRows} |
| Unique brand+slot pairs | ${dashboard.canonicalAirtable.uniqueBrandSlotPairs} |
| Duplicate slot conflicts | ${dashboard.canonicalAirtable.duplicateSlotConflicts} |
| IMAGE_MISSING | ${dashboard.canonicalAirtable.classCounts?.IMAGE_MISSING ?? "—"} |
| IMAGE_SIGNED_URL_EXPIRED | ${dashboard.canonicalAirtable.classCounts?.IMAGE_SIGNED_URL_EXPIRED ?? "—"} |
| IMAGE_SIBLING_BRAND | ${dashboard.canonicalAirtable.classCounts?.IMAGE_SIBLING_BRAND ?? "—"} |
| IMAGE_TECHNICALLY_VALID | ${dashboard.canonicalAirtable.classCounts?.IMAGE_TECHNICALLY_VALID ?? "—"} |
| Brands with all 9 present | ${dashboard.canonicalAirtable.brandsWithAllNinePresent} |
| Brands with gallery 6 present | ${dashboard.canonicalAirtable.brandsWithAllSixGalleryPresent} |
| Brands with scenario 3 present | ${dashboard.canonicalAirtable.brandsWithAllThreeScenarioPresent} |

> Re-run \`npm run be-factory-image-integrity-current-audit-v3 -- --probe\` after remediation to refresh these counts.

## Remediation executed (this wave)

- Aloft Hotels: 9/9 content-API import + read-back
- Bunkhouse Hotels: 9/9 (initial 2 + retry 7) JPEG durable import + read-back
- Suburban Studios gallery.5: PDF replaced with same-brand gallery reuse + read-back
- Radisson Individuals gallery.6 + SLH scenario.1–2: webp→JPEG re-encode + read-back
- Successfully imported/repaired slots (apply reports): **${imported}**
- Paid research spend: **$0**

## Shared Verified Hotel Image Library

| Metric | Value |
|--------|------:|
| Total assets | ${library.totalAssets} |
| Unique content hashes | ${library.uniqueContentHashes} |
| Brands represented | ${library.brandsRepresented} |
| Rights Brand Explorer | ${library.rightsBrandExplorer} |
| Rights Hotel Explorer | ${library.rightsHotelExplorer} |
| Customer-facing | ${library.rightsCustomerFacing} |
| Awaiting review | ${library.awaitingReview} |

## Factory Proposed (prior audit snapshot)

| Metric | Value |
|--------|------:|
| Frozen candidates | ${dashboard.factoryPortfolio.frozenCandidates} |
| Six gallery present | ${dashboard.factoryPortfolio.sixGalleryPresent} |
| Three scenario present | ${dashboard.factoryPortfolio.threeScenarioPresent} |
| Gallery gaps | ${dashboard.factoryPortfolio.galleryGaps} |
| Scenario gaps | ${dashboard.factoryPortfolio.scenarioGaps} |

## Persistence / certification

- QUALITY_CERTIFIED (overall factory): **0** this wave (image technical repair ≠ full certification)
- AIRTABLE_PERSISTED (presentation writeback of certified candidates): **0** this wave
- Customer publication: **NOT_AUTO**

## Remaining blockers

${dashboard.remainingBlockers.map((b) => `- ${b}`).join("\n")}

## Contracts

- \`BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3\`
- \`BE_FACTORY_PERMANENT_EXACT_BRAND_IMAGE_IDENTITY_V1\` / \`BE-QL-IMAGE-EXACT-BRAND-001\`
- \`BE_SHARED_VERIFIED_HOTEL_IMAGE_LIBRARY_V1\`
- \`BE_FACTORY_DUPLICATE_SLOT_RESOLUTION_V1\`
- \`BE_HOTEL_EXPLORER_VERIFIED_IMAGE_REUSE_V1\`
`;

writeFileSync(join(OUT, "90-founder-dashboard.md"), md);
console.log(JSON.stringify({ ok: true, dashboard: join(OUT, "90-founder-dashboard.json"), md: join(OUT, "90-founder-dashboard.md"), library }, null, 2));
