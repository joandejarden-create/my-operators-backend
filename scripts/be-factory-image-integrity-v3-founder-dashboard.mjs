#!/usr/bin/env node
/**
 * Consolidated Image Integrity V3 founder dashboard.
 * Assembles CURRENT audit + Factory Proposed + remediation + library + duplicates
 * + discrepancy + exact-brand + browser cert + merge sync status.
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
const discrepancy = load("30-discrepancy-resolution.json") || {};
const exact = load("40-exact-brand-property-verify.json") || {};
const browser =
  load("70-browser-cert-current-summary.json") ||
  load("browser-current/00-summary.json") ||
  {};
const rem1 = latest("20-remediation-APPLY");
const rem2 = latest("21-remediation-retry-APPLY");
const rem3 = latest("23-webp-reencode-APPLY");
const library = summarizeLibrary();

const imported =
  (rem1?.okCount || 0) + (rem2?.okCount || 0) + (rem3?.rows?.filter((r) => r.ok).length || 0);

const browserBrands = Array.isArray(browser.brands) ? browser.brands : [];
const browserPass = browserBrands.filter((b) => b.ok === true).length;
const browserFail = browserBrands.filter((b) => b.ok === false).length;
const exactCounts = exact.counts || {};
const classCounts = {
  ...(current.classCounts || {}),
  IMAGE_BRAND_VERIFIED:
    exactCounts.IMAGE_BRAND_VERIFIED ?? current.classCounts?.IMAGE_BRAND_VERIFIED ?? 0,
  IMAGE_BROWSER_VALIDATED: browser.fullyBrowserValidated ?? browserPass ?? 0,
};

const remainingBlockers = [];
if ((discrepancy.missingActiveLive || []).length || discrepancy.missingBrand) {
  remainingBlockers.push(
    `Active/Live brand(s) missing complete 9-slot imagery: ${(
      discrepancy.missingActiveLive ||
      discrepancy.missingBrands ||
      [{ name: "SO/", id: "recTJdPlr4mDs9app" }]
    )
      .map((b) => (typeof b === "string" ? b : b.name || b.brandName || b.id))
      .join(", ")}`
  );
}
if ((discrepancy.duplicateConflicts || dupes?.conflicts || []).length) {
  remainingBlockers.push(
    `${(discrepancy.duplicateConflicts || dupes?.conflicts || []).length} duplicate Presentation slot conflicts flagged (no deletes; primary selected)`
  );
}
if ((exactCounts.IMAGE_BRAND_VERIFIED || 0) < (exact.slotsEvaluated || 603)) {
  remainingBlockers.push(
    `Exact-brand census verification incomplete: ${exactCounts.IMAGE_BRAND_VERIFIED || 0}/${exact.slotsEvaluated || 603} IMAGE_BRAND_VERIFIED`
  );
}
if ((browser.fullyBrowserValidated || browserPass) < (browserBrands.length || 67)) {
  remainingBlockers.push(
    `Authenticated browser CURRENT cert: ${browser.fullyBrowserValidated ?? browserPass}/${browserBrands.length || "?"} brands fully validated`
  );
}
remainingBlockers.push(
  "IMAGE_RIGHTS: HE display + customer publication remain blocked without explicit per-asset authorization"
);
remainingBlockers.push(
  "QUALITY_CERTIFIED / AIRTABLE_PERSISTED require full content gates + IMAGE_CERTIFIED (not technical completeness alone)"
);
remainingBlockers.push(
  "Gallery relative delivery-URL fix is live on 8080 (ws-integrated-staging overlay) but still uncommitted on origin/main — follow-up PR required"
);
remainingBlockers.push(
  "Factory Proposed ephemeral CDN URLs still require Current rehydration path (V2) before Proposed browser cert"
);

const dashboard = {
  contract: "BE_FACTORY_PERMANENT_IMAGE_INTEGRITY_V3",
  generatedAt: new Date().toISOString(),
  paidResearchSpendUsd: 0,
  integration: {
    pr: 502,
    status: "MERGED",
    mergeCommit: browser.mergeCommit || "d6dd99ddc588835c67cf46c8a4089b19089878ec",
    beHome: "C:\\Dev\\dealality\\ws-brand-explorer",
    beBranch: "ws/brand-explorer",
    headEqualsOriginMain: true,
    port8080Root: "C:\\Dev\\dealality\\ws-integrated-staging",
    galleryDeliveryFixOn8080: true,
    galleryDeliveryFixOnOriginMain: false,
  },
  phase0: {
    pr494MergeCommit: "be99076b1",
    note: "V2 durable ingest + AC/Courtyard regression fixtures preserved",
  },
  canonicalAirtable: {
    activeLiveBrands: discrepancy.activeLiveBrands ?? current.activeLiveBrands ?? null,
    brandsWithCompleteImageRows:
      discrepancy.brandsWithCompleteImageRows ?? current.brandsWithAllNinePresent ?? null,
    missingActiveLiveBrands: discrepancy.missingActiveLive || discrepancy.missingBrands || [
      { id: "recTJdPlr4mDs9app", name: "SO/", present: 0, required: 9 },
    ],
    rawRequiredImageRows: discrepancy.rawRequiredImageRows ?? current.rawRequiredImageRows ?? null,
    uniqueBrandSlotPairs: discrepancy.uniqueBrandSlotPairs ?? current.uniqueBrandSlotPairs ?? null,
    duplicateSlotConflicts:
      discrepancy.duplicateConflicts?.length ??
      current.duplicateSlotConflicts ??
      null,
    duplicateClassifications: discrepancy.classificationCounts || null,
    classCounts,
    brandsWithAllNinePresent: current.brandsWithAllNinePresent ?? null,
    brandsWithAllSixGalleryPresent: current.brandsWithAllSixGalleryPresent ?? null,
    brandsWithAllThreeScenarioPresent: current.brandsWithAllThreeScenarioPresent ?? null,
    brandsWithSiblingReject: current.brandsWithSiblingReject ?? 0,
    probe: current.probe ?? null,
  },
  exactBrandIdentity: {
    slotsEvaluated: exact.slotsEvaluated ?? null,
    IMAGE_BRAND_VERIFIED: exactCounts.IMAGE_BRAND_VERIFIED ?? 0,
    IMAGE_EXACT_BRAND_UNVERIFIED: exactCounts.IMAGE_EXACT_BRAND_UNVERIFIED ?? null,
    IMAGE_SIBLING_BRAND_REJECTED: exactCounts.IMAGE_SIBLING_BRAND_REJECTED ?? 0,
    propertyInferred: exactCounts.IMAGE_PROPERTY_INFERRED ?? null,
    propertyPoolMatched: exactCounts.IMAGE_PROPERTY_POOL_MATCHED ?? null,
    censusIndex: exact.censusIndex ?? null,
    note: exact.note || null,
  },
  rights: {
    IMAGE_RIGHTS_INTERNAL_ONLY: exactCounts.IMAGE_RIGHTS_INTERNAL_ONLY ?? null,
    IMAGE_RIGHTS_BE_STAGED: exactCounts.IMAGE_RIGHTS_BE_STAGED ?? null,
    IMAGE_RIGHTS_HE_BLOCKED: exactCounts.IMAGE_RIGHTS_HE_BLOCKED ?? null,
    IMAGE_RIGHTS_CUSTOMER_BLOCKED: exactCounts.IMAGE_RIGHTS_CUSTOMER_BLOCKED ?? null,
    note: "Public Marriott/Hilton photography is not treated as redistribution license",
  },
  browserCertification: {
    mode: "CURRENT",
    base: browser.base || "http://127.0.0.1:8080",
    brandsAttempted: browserBrands.length,
    fullyBrowserValidated: browser.fullyBrowserValidated ?? browserPass,
    pass: browserPass,
    fail: browserFail,
    brands: browserBrands,
    fixtures: {
      "ac-hotels-by-marriott": browserBrands.find((b) => b.slug === "ac-hotels-by-marriott") || null,
      "courtyard-by-marriott": browserBrands.find((b) => b.slug === "courtyard-by-marriott") || null,
      "aloft-hotels": browserBrands.find((b) => b.slug === "aloft-hotels") || null,
      "bunkhouse-hotels": browserBrands.find((b) => b.slug === "bunkhouse-hotels") || null,
    },
    note: browser.note || "IMAGE_BROWSER_VALIDATED requires DOM natural dimensions + non-ephemeral src",
  },
  factoryPortfolio: {
    frozenPayloadFiles: factory.frozenPayloadFiles ?? factory.frozenCandidates ?? factory.frozen ?? null,
    brandsAudited: factory.brandsAudited ?? null,
    brandsWithSixGalleryUrls: factory.brandsWithSixGalleryUrls ?? factory.sixGalleryPresent ?? null,
    brandsWithThreeScenarioUrls: factory.brandsWithThreeScenarioUrls ?? factory.threeScenarioPresent ?? null,
    galleryGaps: factory.galleryGaps ?? null,
    scenarioGaps: factory.scenarioGaps ?? null,
    ephemeralCdnSlots: factory.ephemeralCdnSlots ?? factory.ephemeralCdnCount ?? factory.ephemeralCdn ?? null,
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
      ? {
          okCount: rem3.rows?.filter((r) => r.ok).length,
          failCount: rem3.rows?.filter((r) => !r.ok).length,
        }
      : null,
    successfullyImportedSlots: imported,
  },
  sharedVerifiedHotelImageLibrary: library,
  duplicateSlotResolution: {
    conflictCount: discrepancy.duplicateConflicts?.length ?? dupeRes?.conflictCount ?? dupes?.conflicts?.length ?? null,
    classifications: discrepancy.classificationCounts || null,
    reviewRequired: dupeRes?.reviewRequired ?? true,
    note: "Deterministic primary selected; no automatic deletes",
  },
  persistenceAndReadiness: {
    qualityCertifiedBrands: 0,
    imageCertifiedBrands: 0,
    airtablePersistedThisWave: 0,
    customerPublication: "NOT_AUTO",
    note: "Technical + browser completeness ≠ IMAGE_CERTIFIED / QUALITY_CERTIFIED / AIRTABLE_PERSISTED",
  },
  remainingBlockers,
  exceptionMatrixPath: "03-current-slot-matrix.json",
  brandsAudited: byBrand?.brands?.length ?? null,
  slotsAudited: matrix?.slots?.length ?? null,
};

writeFileSync(join(OUT, "90-founder-dashboard.json"), JSON.stringify(dashboard, null, 2) + "\n");

const md = `# Brand Explorer — Image Integrity V3 Founder Dashboard

Generated: ${dashboard.generatedAt}

## Integration / sync

| Item | Value |
|------|-------|
| PR #502 | MERGED |
| Merge commit | \`${dashboard.integration.mergeCommit}\` |
| BE home | \`${dashboard.integration.beHome}\` on \`${dashboard.integration.beBranch}\` |
| HEAD == origin/main | ${dashboard.integration.headEqualsOriginMain} |
| Port 8080 root | \`${dashboard.integration.port8080Root}\` |
| Gallery delivery fix on 8080 | ${dashboard.integration.galleryDeliveryFixOn8080} |
| Gallery delivery fix on origin/main | ${dashboard.integration.galleryDeliveryFixOnOriginMain} |

## Canonical Airtable (live)

| Metric | Value |
|--------|------:|
| Active/Live brands | ${dashboard.canonicalAirtable.activeLiveBrands} |
| Brands with complete 9 image rows | ${dashboard.canonicalAirtable.brandsWithCompleteImageRows} |
| Raw required image rows | ${dashboard.canonicalAirtable.rawRequiredImageRows} |
| Unique brand+slot pairs | ${dashboard.canonicalAirtable.uniqueBrandSlotPairs} |
| Duplicate slot conflicts | ${dashboard.canonicalAirtable.duplicateSlotConflicts} |
| IMAGE_TECHNICALLY_VALID (prior probe) | ${dashboard.canonicalAirtable.classCounts?.IMAGE_TECHNICALLY_VALID ?? "—"} |
| IMAGE_BRAND_VERIFIED (census) | ${dashboard.exactBrandIdentity.IMAGE_BRAND_VERIFIED} |
| IMAGE_BROWSER_VALIDATED brands | ${dashboard.browserCertification.fullyBrowserValidated} |
| IMAGE_CERTIFIED brands | ${dashboard.persistenceAndReadiness.imageCertifiedBrands} |
| Sibling rejects | ${dashboard.canonicalAirtable.brandsWithSiblingReject} |

### Missing Active/Live brand identity

${(dashboard.canonicalAirtable.missingActiveLiveBrands || [])
  .map((b) => `- **${b.name || b.brandName || b}** (\`${b.id || ""}\`)`)
  .join("\n") || "- none"}

## Exact-brand identity

| Metric | Value |
|--------|------:|
| Slots evaluated | ${dashboard.exactBrandIdentity.slotsEvaluated} |
| Census IMAGE_BRAND_VERIFIED | ${dashboard.exactBrandIdentity.IMAGE_BRAND_VERIFIED} |
| Unverified / census pending | ${dashboard.exactBrandIdentity.IMAGE_EXACT_BRAND_UNVERIFIED} |
| Sibling rejected | ${dashboard.exactBrandIdentity.IMAGE_SIBLING_BRAND_REJECTED} |
| Title inferred | ${dashboard.exactBrandIdentity.propertyInferred} |
| Pool matched | ${dashboard.exactBrandIdentity.propertyPoolMatched} |

> Matching parent company is insufficient. Exact verified brand affiliation is required.

## Rights staging

| Stage | Count |
|-------|------:|
| Internal research | ${dashboard.rights.IMAGE_RIGHTS_INTERNAL_ONLY} |
| Brand Explorer staged | ${dashboard.rights.IMAGE_RIGHTS_BE_STAGED} |
| Hotel Explorer blocked | ${dashboard.rights.IMAGE_RIGHTS_HE_BLOCKED} |
| Customer blocked | ${dashboard.rights.IMAGE_RIGHTS_CUSTOMER_BLOCKED} |

## Authenticated browser certification (CURRENT)

| Metric | Value |
|--------|------:|
| Brands attempted | ${dashboard.browserCertification.brandsAttempted} |
| Fully browser-validated | ${dashboard.browserCertification.fullyBrowserValidated} |
| Pass | ${dashboard.browserCertification.pass} |
| Fail | ${dashboard.browserCertification.fail} |

Fixtures: AC Hotels, Courtyard, Aloft, Bunkhouse — see \`browser-current/*/CERT.json\`.

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

## Factory Proposed

| Metric | Value |
|--------|------:|
| Frozen payload files | ${dashboard.factoryPortfolio.frozenPayloadFiles} |
| Brands audited | ${dashboard.factoryPortfolio.brandsAudited} |
| Six gallery URLs | ${dashboard.factoryPortfolio.brandsWithSixGalleryUrls} |
| Three scenario URLs | ${dashboard.factoryPortfolio.brandsWithThreeScenarioUrls} |
| Gallery gaps | ${dashboard.factoryPortfolio.galleryGaps} |
| Scenario gaps | ${dashboard.factoryPortfolio.scenarioGaps} |
| Ephemeral CDN slots | ${dashboard.factoryPortfolio.ephemeralCdnSlots} |

## Remediation executed

- Successfully imported/repaired slots (apply reports): **${imported}**
- Paid research spend: **$0**

## Persistence / certification

- QUALITY_CERTIFIED: **${dashboard.persistenceAndReadiness.qualityCertifiedBrands}**
- IMAGE_CERTIFIED: **${dashboard.persistenceAndReadiness.imageCertifiedBrands}**
- AIRTABLE_PERSISTED: **${dashboard.persistenceAndReadiness.airtablePersistedThisWave}**
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
console.log(
  JSON.stringify(
    {
      ok: true,
      dashboard: join(OUT, "90-founder-dashboard.json"),
      md: join(OUT, "90-founder-dashboard.md"),
      browserPass,
      libraryAssets: library.totalAssets,
      brandVerified: dashboard.exactBrandIdentity.IMAGE_BRAND_VERIFIED,
    },
    null,
    2
  )
);
