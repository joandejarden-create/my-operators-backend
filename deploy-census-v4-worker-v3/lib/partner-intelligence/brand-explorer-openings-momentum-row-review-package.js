/**
 * Brand Explorer Openings + Momentum Row Review Package v25C-3B.
 *
 * Founder-review package with polished UI copy and exact proposed presentation
 * row payloads for Tribute Portfolio. Read-only — no Airtable writes.
 *
 * @see docs/data-intelligence/brand-explorer-openings-momentum-row-review-package-v25C-3B.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { listPartnerSources } from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  REPORT_JSON_NAME as COMPLETION_JSON,
} from "./brand-explorer-openings-momentum-source-capture-completion.js";

export const PACKAGE_VERSION = "25C-3B";
export const REPORT_JSON_NAME = "brand-explorer-openings-momentum-row-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-openings-momentum-row-review-package.md";
export const DOC_MD_NAME = "brand-explorer-openings-momentum-row-review-package-v25C-3B.md";

export const NEXT_WRITER = "brand-explorer-openings-momentum-row-creation-writer";
export const NEXT_WRITER_VERSION = "25C-3C";

export const OPENINGS_SLOT = "footprint.openings";
export const MOMENTUM_SLOT = "footprint.momentum";
export const OPENINGS_MINIMUM = 3;
export const MOMENTUM_MINIMUM = 3;

export const CONSUMER_SITE_URL = "https://tribute-portfolio.marriott.com/";
export const CONSUMER_SOURCE_ID = "recF0qS9JIZjM3qza";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const GOVERNANCE_LABELS = [
  "AI-drafted from official-source metadata",
  "Pending founder review",
  "Not company-validated",
  "Not Marriott-validated",
];

const FORBIDDEN_UI_COPY = /take a photo tour of|view photos of our boutique rooms|approved source excerpt/i;

const REFERENCE_BRANDS = [
  { name: "Curio Collection by Hilton", id: "receQkxgjlezsc1xg" },
  { name: "Kimpton Hotels", id: "recCKuXCmGvxHPfb3" },
  { name: "Radisson Blu by Choice", id: "recWPEvxBQxVVzSq3" },
  { name: "Ascend Hotel Collection", id: "reclkgOzvAcBheUSo" },
];

/** Polished owner-facing openings cards — not generic Marriott template copy. */
export const POLISHED_OPENINGS_CARDS = [
  {
    marsha: "CUNAN",
    propertyName: "Casa Nizuc, a Tribute Portfolio Resort",
    classification: "Future Opening Example",
    sort: 0,
    tags: "Resort, Mexico, CALA, Riviera Maya",
    location: "Cancún, Quintana Roo, Mexico",
    meta: "Future Tribute resort · MARSHA CUNAN · Consumer-site listing Nov 2026",
    scenario: "UPCOMING RESORT EXAMPLE",
    teaser:
      "Cancún-area resort on the official Tribute consumer map with a future listing date—useful when owners evaluate Marriott affiliation for an independent-character leisure asset before any opening claim is confirmed.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/cunan-casa-nizuc-a-tribute-portfolio-resort/overview/",
    sourceBasis:
      "Marriott property URL + Tribute consumer-site embedded listing metadata (recF0qS9JIZjM3qza); not a press-release opening announcement.",
  },
  {
    marsha: "BGITY",
    propertyName: "Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort",
    classification: "Opening Example",
    sort: 1,
    tags: "Resort, All-Inclusive, Barbados, CALA",
    location: "St. James, Barbados",
    meta: "All-inclusive resort · MARSHA BGITY · Consumer-site listing Feb 2025",
    scenario: "CARIBBEAN RESORT EXAMPLE",
    teaser:
      "Barbados all-inclusive resort operating under Tribute Portfolio—illustrates how the collection can anchor a Caribbean leisure asset with resort-scale positioning within Marriott's network.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/bgity-crystal-cove-barbados-a-tribute-portfolio-all-inclusive-resort/overview/",
    sourceBasis:
      "Marriott property URL + consumer-site dated listing metadata; framed as property/opening example—not newsroom PR.",
  },
  {
    marsha: "SJUTX",
    propertyName: "Hotel Rumbao, a Tribute Portfolio Hotel",
    classification: "Opening Example",
    sort: 2,
    tags: "Urban, Puerto Rico, CALA, Old San Juan",
    location: "San Juan, Puerto Rico",
    meta: "Urban lifestyle hotel · MARSHA SJUTX · Consumer-site listing Jan 2024",
    scenario: "OLD SAN JUAN URBAN EXAMPLE",
    teaser:
      "Old San Juan lifestyle hotel under Tribute—relevant for urban conversion or repositioning deals where owners want local character with Marriott systems and Bonvoy participation.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/sjutx-hotel-rumbao-a-tribute-portfolio-hotel/overview/",
    sourceBasis:
      "Marriott property URL + consumer-site dated listing metadata; not a press-release opening announcement.",
  },
  {
    marsha: "LIMTX",
    propertyName: "Humano, Lima, a Tribute Portfolio Hotel",
    classification: "Urban Example",
    sort: 3,
    tags: "Urban, Peru, South America, Waterfront",
    location: "Lima, Peru",
    meta: "Urban lifestyle hotel · MARSHA LIMTX · Consumer-site listing Apr 2026",
    scenario: "SOUTH AMERICA URBAN EXAMPLE",
    teaser:
      "Malecón waterfront hotel in Lima showing Tribute's South America urban footprint—useful when owners compare lifestyle urban affiliation options within Marriott.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/limtx-humano-lima-a-tribute-portfolio-hotel/overview/",
    sourceBasis:
      "Marriott property URL + consumer-site listing metadata; property example—not dated PR.",
  },
  {
    marsha: "MDETX",
    propertyName: "Loma, Medellin, a Tribute Portfolio Hotel",
    classification: "Urban Example",
    sort: 4,
    tags: "Urban, Colombia, South America",
    location: "Medellín, Colombia",
    meta: "Urban lifestyle hotel · MARSHA MDETX · Consumer-site listing Dec 2025",
    scenario: "ANDEAN URBAN EXAMPLE",
    teaser:
      "Medellín urban hotel under Tribute—reference for Andean secondary-city lifestyle positioning where owners want independent design sensibility inside Marriott's commercial stack.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/mdetx-loma-medellin-a-tribute-portfolio-hotel/overview/",
    sourceBasis:
      "Marriott property URL + consumer-site listing metadata; property example—not dated PR.",
  },
];

/** Polished momentum rows — dated consumer-site activity only; no newsroom claims. */
export const POLISHED_MOMENTUM_ROWS = [
  {
    marsha: "LIMTX",
    propertyName: "Humano, Lima, a Tribute Portfolio Hotel",
    location: "Lima, Peru",
    dateLine: "Apr 2026",
    openingDate: "2026-04-10",
    title: "Lima waterfront hotel dated on Tribute consumer site",
    summary:
      "Official Tribute Portfolio consumer brand site shows Humano, Lima with a dated listing in Lima, Peru—consumer-site portfolio activity; not a Marriott newsroom announcement.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza)",
    sort: 0,
  },
  {
    marsha: "MDETX",
    propertyName: "Loma, Medellin, a Tribute Portfolio Hotel",
    location: "Medellín, Colombia",
    dateLine: "Dec 2025",
    openingDate: "2025-12-18",
    title: "Medellín urban listing on Tribute brand site",
    summary:
      "Consumer brand site lists Loma, Medellín with a December 2025 dated entry—dated property-listing activity on Marriott's Tribute consumer map.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza)",
    sort: 1,
  },
  {
    marsha: "BGITY",
    propertyName: "Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort",
    location: "St. James, Barbados",
    dateLine: "Feb 2025",
    openingDate: "2025-02-12",
    title: "Barbados all-inclusive resort dated on Tribute site",
    summary:
      "Crystal Cove, Barbados appears on the official Tribute consumer site with a February 2025 dated listing—consumer-site activity supporting Caribbean resort momentum.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza)",
    sort: 2,
  },
  {
    marsha: "SJUTX",
    propertyName: "Hotel Rumbao, a Tribute Portfolio Hotel",
    location: "San Juan, Puerto Rico",
    dateLine: "Jan 2024",
    openingDate: "2024-01-31",
    title: "San Juan hotel listing on Tribute consumer map",
    summary:
      "Hotel Rumbao, San Juan is shown on the Tribute consumer brand site with a January 2024 dated listing—earlier CALA urban activity on the official brand map.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza)",
    sort: 3,
  },
  {
    marsha: "BDOGP",
    propertyName: "Grand Hotel Preanger, Bandung, a Tribute Portfolio Hotel",
    location: "Bandung, Indonesia",
    dateLine: "Jun 2026",
    openingDate: "2026-06-30",
    title: "Bandung heritage hotel dated on Tribute site",
    summary:
      "Grand Hotel Preanger, Bandung appears on the Tribute consumer site with a June 2026 dated listing—APAC portfolio activity from official brand-site metadata.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza)",
    sort: 4,
  },
  {
    marsha: "MILNT",
    propertyName: "NEMI, Milan, a Tribute Portfolio Hotel",
    location: "Milan, Italy",
    dateLine: "Jun 2026",
    openingDate: "2026-06-30",
    title: "Milan urban hotel listing on Tribute consumer site",
    summary:
      "NEMI, Milan is listed on the official Tribute consumer brand site with a June 2026 dated entry—European urban activity on the brand's public portfolio map.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza)",
    sort: 5,
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-openings-momentum-source-capture-completion.md",
  "reports/brand-explorer-openings-momentum-source-capture-completion.json",
  "reports/brand-explorer-openings-momentum-source-capture-package.md",
  "reports/brand-explorer-openings-momentum-source-capture-package.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/brand-explorer-required-section-source-capture-package.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "fixtures/brand-explorer-presentation-radisson-footprint-openings.json",
  "fixtures/brand-explorer-presentation-radisson-footprint-momentum.json",
  "live Tribute presentation rows (API)",
  "live Tribute Source Library records (API)",
  "live Curio/Kimpton/Radisson/Ascend openings/momentum rows (API)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-openings-momentum-row-review-package.js",
  "scripts/brand-explorer-openings-momentum-row-review-package.mjs",
  "docs/data-intelligence/brand-explorer-openings-momentum-row-review-package-v25C-3B.md",
  "reports/brand-explorer-openings-momentum-row-review-package.md",
  "reports/brand-explorer-openings-momentum-row-review-package.json",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function readJsonIfExists(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function containsForbiddenUiCopy(text) {
  return FORBIDDEN_UI_COPY.test(nz(text));
}

function buildOpeningsBody(card) {
  return [
    card.tags,
    card.location,
    card.meta,
    card.scenario,
    card.teaser,
    card.sourceUrl,
  ]
    .filter(Boolean)
    .join("\n\n");
}

function buildMomentumBody(row) {
  return [row.dateLine, row.summary, row.sourceUrl].filter(Boolean).join("\n\n");
}

function completionCandidateByMarsha(completion, marsha) {
  return (completion?.propertyExampleCandidates || []).find((c) => nz(c.marsha) === marsha) || null;
}

export function buildFlattenedOpeningsRowTargets(
  brandRecordId = TRIBUTE_RECORD_ID,
  brandName = BRAND_NAME,
  imageByMarsha = {}
) {
  return POLISHED_OPENINGS_CARDS.map((card) => ({
    slotKey: OPENINGS_SLOT,
    title: card.propertyName,
    body: buildOpeningsBody(card),
    sort: card.sort,
    classification: card.classification,
    marsha: card.marsha,
    imageUrl: nz(imageByMarsha[card.marsha]),
    sourceUrl: card.sourceUrl,
    fields: {
      "Slot Key": OPENINGS_SLOT,
      Title: card.propertyName,
      Body: buildOpeningsBody(card),
      Brand: [brandRecordId],
      "Brand Name": brandName,
      Active: true,
      "Sort Order": card.sort,
    },
  }));
}

export function buildFlattenedMomentumRowTargets(
  brandRecordId = TRIBUTE_RECORD_ID,
  brandName = BRAND_NAME
) {
  return POLISHED_MOMENTUM_ROWS.map((row) => ({
    slotKey: MOMENTUM_SLOT,
    title: row.title,
    body: buildMomentumBody(row),
    sort: row.sort,
    marsha: row.marsha,
    dateLine: row.dateLine,
    sourceUrl: row.sourceUrl,
    fields: {
      "Slot Key": MOMENTUM_SLOT,
      Title: row.title,
      Body: buildMomentumBody(row),
      Brand: [brandRecordId],
      "Brand Name": brandName,
      Active: true,
      "Sort Order": row.sort,
    },
  }));
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function buildBrandExplorerOpeningsMomentumRowReviewPackageMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Openings + Momentum Row Review Package v${PACKAGE_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v25C-3B exists: **${report.v25C3BReviewPackageExists ? "yes" : "no"}**`);
  lines.push(`- Openings ready: **${report.openingsRowsReadyForFounderReview}/${report.openingsProposedCards.length}**`);
  lines.push(`- Momentum ready: **${report.momentumRowsReadyForFounderReview}/${report.momentumProposedRows.length}**`);
  lines.push(`- Openings meets minimum after v25C-3C: **${report.openingsMeetsMinimumAfterV25C3C ? "yes" : "no"}**`);
  lines.push(`- Momentum meets minimum after v25C-3C: **${report.momentumMeetsMinimumAfterV25C3C ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Openings / Examples proposed cards");
  for (const c of report.openingsProposedCards) {
    lines.push(`- **${c.propertyName}** · ${c.classification} · ready: ${c.readyForFounderReview ? "yes" : "no"}`);
  }
  lines.push("");
  lines.push("## Recent Momentum proposed rows");
  for (const m of report.momentumProposedRows) {
    lines.push(`- **${m.title}** · ${m.dateLine} · ready: ${m.readyForFounderReview ? "yes" : "no"}`);
  }
  lines.push("");
  lines.push("## Exact next writer");
  lines.push("```bash");
  lines.push(report.exactNextWriterCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerOpeningsMomentumRowReviewPackageReport(options = {}) {
  const brandRecordId = TRIBUTE_RECORD_ID;
  const completion = readJsonIfExists(`reports/${COMPLETION_JSON}`);
  if (!completion?.v25C3ASourceCaptureCompletionExists) {
    throw new Error("v25C-3A completion report missing — run brand-explorer-openings-momentum-source-capture-completion first");
  }

  const brandBasics = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);
  const tribute = await fetchBrandApiShape(brandRecordId);

  let existingSources = [];
  try {
    let offset = "";
    do {
      const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
      existingSources.push(...(page.sources || []));
      offset = page.offset || "";
    } while (offset);
  } catch (err) {
    console.warn("[v25C-3B] source list warning:", err?.message || err);
  }

  const imageByMarsha = {};
  for (const c of completion.propertyExampleCandidates || []) {
    if (c.marsha && c.imageUrl) imageByMarsha[c.marsha] = c.imageUrl;
  }

  const referenceRowPatterns = [];
  for (const ref of REFERENCE_BRANDS) {
    const b = await fetchBrandApiShape(ref.id);
    if (!b) continue;
    const openings = blocksForSlot(b, OPENINGS_SLOT);
    const momentum = blocksForSlot(b, MOMENTUM_SLOT);
    referenceRowPatterns.push({
      brand: ref.name,
      openingsCount: openings.length,
      momentumCount: momentum.length,
      sampleOpeningTitle: nz(openings[0]?.title),
      sampleMomentumTitle: nz(momentum[0]?.title),
    });
  }

  const existingOpenings = blocksForSlot(tribute, OPENINGS_SLOT);
  const existingMomentum = blocksForSlot(tribute, MOMENTUM_SLOT);

  const futureMomentumHeld = (completion.recentMomentumFutureHeld || []).map((r) => ({
    propertyName: r.propertyName,
    marsha: r.marsha,
    openingDate: r.openingDate,
    excludedFromMomentum: true,
    reason: "Future-dated consumer-site listing — not Recent Momentum",
  }));

  const openingsProposedCards = POLISHED_OPENINGS_CARDS.map((card) => {
    const captured = completionCandidateByMarsha(completion, card.marsha);
    const imageUrl = nz(imageByMarsha[card.marsha]);
    const registryRecordIds = captured?.registryRecordIds || [];
    const body = buildOpeningsBody(card);
    const missing = [];
    if (!card.propertyName) missing.push("propertyName");
    if (!imageUrl) missing.push("imageUrl");
    if (!card.location) missing.push("location");
    if (!card.teaser) missing.push("bodySummary");
    if (!card.sourceUrl) missing.push("sourceUrl");
    const readyForFounderReview =
      missing.length === 0 && !containsForbiddenUiCopy(body) && Boolean(captured?.imageUsageConfirmed);
    const readyForV25C3C =
      readyForFounderReview && hasVal(imageUrl) && hasVal(card.sourceUrl) && hasVal(card.teaser);

    return {
      slotKey: OPENINGS_SLOT,
      propertyName: card.propertyName,
      title: card.propertyName,
      classification: card.classification,
      location: card.location,
      imageUrl,
      imageAssetReference: {
        registryRecordIds,
        coverImageUrl: imageUrl,
        gallerySlotKey: captured?.gallerySlotKey || null,
      },
      sourceUrl: card.sourceUrl,
      sourceBasis: card.sourceBasis,
      sourceExcerptSeparate:
        captured?.bodySummary && captured?.genericTemplateSummary ? captured.bodySummary : null,
      genericTemplateRewritten: Boolean(captured?.genericTemplateSummary),
      uiCopy: {
        tags: card.tags,
        location: card.location,
        meta: card.meta,
        scenario: card.scenario,
        teaser: card.teaser,
        body,
      },
      founderReviewStatus: GOVERNANCE_LABELS.join("; "),
      governanceLabels: GOVERNANCE_LABELS,
      readyForFounderReview,
      readyForV25C3C,
      missingFields: missing,
      blockedReasons: readyForV25C3C
        ? []
        : missing.map((m) => `missing_${m}`).concat(
            containsForbiddenUiCopy(body) ? ["forbidden_template_copy_in_ui"] : []
          ),
      sort: card.sort,
      marsha: card.marsha,
      openingDate: captured?.openingDate || null,
      casaNizucFutureOpeningExample:
        card.marsha === "CUNAN" ? card.classification === "Future Opening Example" : undefined,
    };
  });

  const today = new Date().toISOString().slice(0, 10);
  const momentumProposedRows = POLISHED_MOMENTUM_ROWS.map((row) => {
    const isFuture = row.openingDate > today;
    const body = buildMomentumBody(row);
    const missing = [];
    if (!row.dateLine) missing.push("date");
    if (!row.title) missing.push("title");
    if (!row.summary) missing.push("summary");
    if (!row.sourceUrl) missing.push("sourceUrl");
    const readyForFounderReview = !isFuture && missing.length === 0 && !containsForbiddenUiCopy(body);
    const readyForV25C3C = readyForFounderReview;

    return {
      slotKey: MOMENTUM_SLOT,
      dateLine: row.dateLine,
      openingDate: row.openingDate,
      title: row.title,
      propertyName: row.propertyName,
      location: row.location,
      summary: row.summary,
      body,
      sourceUrl: row.sourceUrl,
      sourceBasis: row.sourceBasis,
      founderReviewStatus: GOVERNANCE_LABELS.join("; "),
      governanceLabels: GOVERNANCE_LABELS,
      readyForFounderReview,
      readyForV25C3C,
      excludedAsFutureDated: isFuture,
      missingFields: missing,
      blockedReasons: isFuture ? ["future_dated_excluded"] : missing.map((m) => `missing_${m}`),
      sort: row.sort,
      marsha: row.marsha,
    };
  });

  const openingsReady = openingsProposedCards.filter((c) => c.readyForFounderReview);
  const openingsBlocked = openingsProposedCards.filter((c) => !c.readyForFounderReview);
  const momentumReady = momentumProposedRows.filter((m) => m.readyForFounderReview);
  const momentumBlocked = momentumProposedRows.filter((m) => !m.readyForFounderReview);

  const proposedOpeningsPayloads = buildFlattenedOpeningsRowTargets(
    brandRecordId,
    BRAND_NAME,
    imageByMarsha
  );
  const proposedMomentumPayloads = buildFlattenedMomentumRowTargets(brandRecordId, BRAND_NAME);

  const photosUrlsNeeded = POLISHED_OPENINGS_CARDS.map((c) =>
    nz(c.sourceUrl).replace(/\/overview\/?$/i, "/photos/")
  );
  const knownSourceUrls = new Set(existingSources.map((s) => nz(s.sourceUrl).toLowerCase()).filter(Boolean));
  const photosSourcesMissing = photosUrlsNeeded.filter((u) => u && !knownSourceUrls.has(u.toLowerCase()));

  const sourceLibraryRequiredBeforeV25C3C = photosSourcesMissing.length > 0;
  const partnerFactsRequiredBeforeV25C3C = false;

  const openingsMeetsMinimumAfterV25C3C = openingsReady.length >= OPENINGS_MINIMUM;
  const momentumMeetsMinimumAfterV25C3C = momentumReady.length >= MOMENTUM_MINIMUM;

  const v25C3CReady =
    openingsReady.every((c) => c.readyForV25C3C) &&
    momentumReady.every((m) => m.readyForV25C3C) &&
    openingsMeetsMinimumAfterV25C3C &&
    momentumMeetsMinimumAfterV25C3C;

  return {
    packageVersion: PACKAGE_VERSION,
    v25C3BReviewPackageExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    sortOrderUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: {
      recordId: brandRecordId,
      name: BRAND_NAME,
      slug: "tribute-portfolio",
    },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    sourceOfTruth: `reports/${COMPLETION_JSON}`,
    referenceRowPatterns,
    existingPresentationRows: {
      footprintOpenings: existingOpenings.length,
      footprintMomentum: existingMomentum.length,
    },
    openingsProposedCards,
    momentumProposedRows,
    openingsRowsReadyForFounderReview: openingsReady.length,
    momentumRowsReadyForFounderReview: momentumReady.length,
    openingsRowsBlocked: openingsBlocked.map((c) => ({
      propertyName: c.propertyName,
      reasons: c.blockedReasons,
    })),
    momentumRowsBlocked: momentumBlocked.map((m) => ({
      title: m.title,
      reasons: m.blockedReasons,
    })),
    exactProposedCopyByRow: {
      openings: openingsProposedCards.map((c) => ({
        propertyName: c.propertyName,
        uiCopy: c.uiCopy,
        sourceExcerptSeparate: c.sourceExcerptSeparate,
      })),
      momentum: momentumProposedRows.map((m) => ({
        title: m.title,
        dateLine: m.dateLine,
        summary: m.summary,
        body: m.body,
      })),
    },
    exactProposedRowPayloads: {
      openings: proposedOpeningsPayloads,
      momentum: proposedMomentumPayloads,
    },
    sourceUrlsUsed: [
      CONSUMER_SITE_URL,
      ...POLISHED_OPENINGS_CARDS.map((c) => c.sourceUrl),
    ].filter((v, i, a) => a.indexOf(v) === i),
    imageAssetReferencesUsed: openingsProposedCards.map((c) => c.imageAssetReference),
    casaNizucHandledCorrectly: {
      inOpeningsAsFutureOpeningExample: true,
      excludedFromRecentMomentum: !momentumProposedRows.some((m) => m.marsha === "CUNAN"),
      inFutureMomentumHeld: futureMomentumHeld.some((f) => f.marsha === "CUNAN"),
      openingDate: "2026-11-23",
    },
    futureDatedRowsExcludedFromMomentum: {
      excluded: futureMomentumHeld,
      polishedMomentumContainsFuture: momentumProposedRows.some((m) => m.excludedAsFutureDated),
    },
    genericTemplateExcerptsRewritten: openingsProposedCards.every((c) => c.genericTemplateRewritten),
    sourceLibraryRequiredBeforeV25C3C,
    sourceLibraryGaps: photosSourcesMissing,
    partnerFactsRequiredBeforeV25C3C,
    partnerFactsNote:
      "v25C-3A proposed Pending footprint.propertyExample facts are optional; presentation rows can proceed from founder-reviewed copy without fact approval if v25C-3C gates pass.",
    v25C3CRowCreationReady: v25C3CReady,
    openingsMeetsMinimumAfterV25C3C,
    momentumMeetsMinimumAfterV25C3C,
    rowsNeedingCreationV25C3C: {
      openings: proposedOpeningsPayloads.length,
      momentum: proposedMomentumPayloads.length,
    },
    rowsNeedingUpdateV25C3C: [],
    presentationTable: PRESENTATION_TABLE,
    companyValidatedBefore,
    companyValidatedAfter: companyValidatedBefore,
    exactNextWriter: NEXT_WRITER,
    exactNextWriterVersion: NEXT_WRITER_VERSION,
    exactNextWriterCommand: `npm run ${NEXT_WRITER} -- --brand tribute-portfolio --dry-run`,
    doesNotDo: [
      "Create footprint.openings or footprint.momentum presentation rows",
      "Create Source Library or Partner Fact records",
      "Change images, Sort Order, or Company Validated",
      "Write generic Marriott photos-page template into UI copy",
      "Imply Marriott validated anything",
      "Use future-dated listings in Recent Momentum",
    ],
  };
}
