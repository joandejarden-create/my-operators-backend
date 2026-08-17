/**
 * Brand Explorer Openings + Momentum Source Capture Completion v25C-3A.
 *
 * Captures/verifies official source-backed content for Tribute Portfolio
 * footprint.openings and footprint.momentum backfill. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-openings-momentum-source-capture-completion-v25C-3A.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  fetchMarriottHotelContent,
  fetchMarriottOverviewHtmlPlain,
  marriottOverviewUrlFromWebsite,
} from "../../lib/marriott-hotel-content-fetch.js";
import { listPartnerSources } from "./airtable-source.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import { MAP_PARTNER_SOURCE } from "../../api/lib/partner-intelligence-field-map.js";

export const COMPLETION_VERSION = "25C-3A";
export const REPORT_JSON_NAME = "brand-explorer-openings-momentum-source-capture-completion.json";
export const REPORT_MD_NAME = "brand-explorer-openings-momentum-source-capture-completion.md";
export const DOC_MD_NAME = "brand-explorer-openings-momentum-source-capture-completion-v25C-3A.md";

const CONSUMER_SITE_URL = "https://tribute-portfolio.marriott.com/";
const CONSUMER_SOURCE_ID = "recF0qS9JIZjM3qza";
const OPENINGS_MINIMUM = 3;
const MOMENTUM_MINIMUM = 3;

const CLASSIFICATION = Object.freeze({
  READY: "ready_for_founder_review",
  REQUIRED: "source_capture_required",
  BLOCKED: "source_capture_blocked",
  NOT_SAFE: "not_safe_to_use",
  DUPLICATE: "duplicate_or_low_value",
});

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-openings-momentum-source-capture-package.md",
  "reports/brand-explorer-openings-momentum-source-capture-package.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/brand-explorer-required-section-source-capture-package.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "reports/cala-tribute-property-visual-discovery.json",
  "reports/tribute-visual-asset-slot-review.json",
  "reports/tribute-portfolio-targeted-extract.json",
  "lib/marriott-hotel-content-fetch.js",
  "live Tribute Source Library records (API)",
  "live Tribute Partner Facts (API)",
  "live Tribute Brand Asset Registry records (API)",
  "live Tribute Presentation rows (via v25C-3A package)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-openings-momentum-source-capture-completion.js",
  "scripts/brand-explorer-openings-momentum-source-capture-completion.mjs",
  "docs/data-intelligence/brand-explorer-openings-momentum-source-capture-completion-v25C-3A.md",
  "reports/brand-explorer-openings-momentum-source-capture-completion.md",
  "reports/brand-explorer-openings-momentum-source-capture-completion.json",
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

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

function decodeJsonString(s) {
  try {
    return JSON.parse(`"${String(s).replace(/"/g, '\\"')}"`);
  } catch {
    return String(s || "")
      .replace(/\\u2013/g, "–")
      .replace(/\\u2014/g, "—")
      .replace(/\\n/g, " ")
      .replace(/\\"/g, '"');
  }
}

function photosUrlFromOverview(overviewUrl) {
  return nz(overviewUrl).replace(/\/overview\/?$/i, "/photos/");
}

function formatLocationLabel(record) {
  const city = nz(record?.city);
  const state = nz(record?.state);
  const country = nz(record?.country);
  const parts = [city, state].filter(Boolean);
  if (country && country !== state) parts.push(country);
  return parts.join(", ");
}

function formatMomentumDate(isoDate) {
  const raw = nz(isoDate);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return "";
  const [y, m, d] = raw.split("-").map(Number);
  const months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  return `${months[m - 1]} ${d}, ${y}`;
}

function exampleTypeFromRecord(record, hasDatedOpening) {
  const name = nz(record?.name).toLowerCase();
  if (/resort|all-inclusive/i.test(name)) return "Resort Example";
  if (/urban|lima|medellin|san juan/i.test(name)) return "Urban Example";
  if (hasDatedOpening && record?.openingDateIsPast) return "Opening Example";
  return "Property Example";
}

function isGenericPhotosSummary(text) {
  return /take a photo tour of/i.test(text) && /view photos of our boutique rooms/i.test(text);
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function parseConsumerSitePropertyRecords(html) {
  const text = String(html || "");
  const records = [];
  const re = /"([A-Z]{5})"\s*:\s*\{([\s\S]*?)\}\s*,\s*"/g;
  let match;
  while ((match = re.exec(text))) {
    const marsha = match[1];
    const block = match[2];
    if (!/Tribute Portfolio/i.test(block)) continue;
    const name = decodeJsonString(block.match(/"name"\s*:\s*"([^"\\]+)"/)?.[1] || "");
    if (!name) continue;
    records.push({
      marsha,
      name,
      city: decodeJsonString(block.match(/"city"\s*:\s*"([^"\\]*)"/)?.[1] || ""),
      state: decodeJsonString(block.match(/"state"\s*:\s*(null|"([^"\\]*)")/)?.[2] || ""),
      country: decodeJsonString(block.match(/"country"\s*:\s*"([^"\\]*)"/)?.[1] || ""),
      region: decodeJsonString(block.match(/"region"\s*:\s*"([^"\\]*)"/)?.[1] || ""),
      address: decodeJsonString(block.match(/"address"\s*:\s*"([^"\\]*)"/)?.[1] || ""),
      openingDate: block.match(/"openingDate"\s*:\s*"([^"]+)"/)?.[1] || "",
      status: decodeJsonString(block.match(/"status"\s*:\s*"([^"\\]*)"/)?.[1] || ""),
    });
  }
  const byMarsha = new Map();
  for (const rec of records) {
    if (!byMarsha.has(rec.marsha)) byMarsha.set(rec.marsha, rec);
  }
  return [...byMarsha.values()];
}

export async function fetchMarriottPhotosPageMeta(overviewUrl) {
  const photosUrl = photosUrlFromOverview(overviewUrl);
  if (!photosUrl) {
    return {
      photosUrl: "",
      status: 0,
      accessDenied: true,
      ogTitle: "",
      ogDescription: "",
      errors: ["missing_photos_url"],
    };
  }
  const fetched = await fetchMarriottOverviewHtmlPlain(photosUrl);
  const ogDescription =
    fetched.html.match(/property=["']og:description["'][^>]+content=["']([^"']+)["']/i)?.[1] ||
    fetched.html.match(/content=["']([^"']+)["'][^>]+property=["']og:description["']/i)?.[1] ||
    "";
  const ogTitle =
    fetched.html.match(/property=["']og:title["'][^>]+content=["']([^"']+)["']/i)?.[1] ||
    fetched.html.match(/content=["']([^"']+)["'][^>]+property=["']og:title["']/i)?.[1] ||
    "";
  return {
    photosUrl,
    status: fetched.status,
    accessDenied: fetched.accessDenied,
    ogTitle: decodeJsonString(ogTitle),
    ogDescription: decodeJsonString(ogDescription),
    errors: fetched.accessDenied ? ["access_denied"] : [],
  };
}

function buildProposedOpeningsCardInput(candidate) {
  const tags = [candidate.exampleType.replace(/ Example$/, ""), candidate.locationLabel]
    .filter(Boolean)
    .join(", ");
  const metaLine = [
    candidate.exampleType,
    candidate.marsha ? `MARSHA ${candidate.marsha}` : "",
    candidate.openingDate ? `Listed opening ${candidate.openingDate}` : "",
  ]
    .filter(Boolean)
    .join(" · ");
  const teaser = nz(candidate.bodySummary);
  const url = nz(candidate.propertyUrl) || nz(candidate.photosUrl) || nz(candidate.overviewUrl);
  const body = [tags, candidate.locationLabel, metaLine, candidate.exampleType, teaser, url]
    .filter(Boolean)
    .join("\n\n");
  return {
    slotKey: "footprint.openings",
    title: candidate.propertyName,
    body,
    imageUrl: candidate.imageUrl,
    sourceUrl: url,
    exampleType: candidate.exampleType,
    marsha: candidate.marsha,
  };
}

function buildProposedMomentumRow(candidate) {
  const dateLine = formatMomentumDate(candidate.openingDate) || nz(candidate.openingDate);
  const title = `${candidate.propertyName} — Tribute Portfolio portfolio activity`;
  const body = `${candidate.summary}\n\n${candidate.sourceUrl}`;
  return {
    slotKey: "footprint.momentum",
    title,
    body: `${dateLine}\n\n${candidate.summary}\n\n${candidate.sourceUrl}`,
    dateOrYear: dateLine,
    sourceUrl: candidate.sourceUrl,
    marsha: candidate.marsha,
    openingDate: candidate.openingDate,
  };
}

function buildProposedSourceRecord({ title, sourceUrl, captureMethod, evidenceNotes, brandRecordId }) {
  return {
    table: "Partner Intelligence - Source Library",
    fields: {
      [MAP_PARTNER_SOURCE.sourceTitle]: title,
      [MAP_PARTNER_SOURCE.sourceUrl]: sourceUrl,
      [MAP_PARTNER_SOURCE.sourceType]: "Website Capture",
      [MAP_PARTNER_SOURCE.region]: "CALA / Americas",
      [MAP_PARTNER_SOURCE.profileType]: "Brand",
      [MAP_PARTNER_SOURCE.brand]: [brandRecordId],
      [MAP_PARTNER_SOURCE.sourceOrigin]: "Public Web",
      [MAP_PARTNER_SOURCE.visibility]: "Public",
      [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
      [MAP_PARTNER_SOURCE.sourceQuality]: "Medium",
      [MAP_PARTNER_SOURCE.status]: "Pending Review",
      [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
      [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
      [MAP_PARTNER_SOURCE.notes]: [
        `Capture Method: ${captureMethod}`,
        `Evidence Notes: ${evidenceNotes}`,
        "External Display Status: Pending founder review",
        "Usage Permission: Marriott-controlled public web — not company-validated",
        "Extraction readiness: metadata capture only in v25C-3A completion",
      ].join(" | "),
      [MAP_PARTNER_SOURCE.captureDate]: new Date().toISOString().slice(0, 10),
    },
    humanReviewStatus: "Pending Review",
    applyInDryRun: false,
  };
}

function buildProposedFact({ fieldKey, value, evidenceText, sourceRecordId, brandRecordId }) {
  return {
    table: "Partner Intelligence - Extracted Facts",
    fieldKey,
    extractedValue: value,
    evidenceText,
    sourceRecordId,
    brandRecordId,
    humanReviewStatus: "Pending",
    publicVisibility: "Internal Only",
    applyInDryRun: false,
  };
}

export function buildBrandExplorerOpeningsMomentumSourceCaptureCompletionMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Openings + Momentum Source Capture Completion v${COMPLETION_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v25C-3A completion exists: **${report.v25C3ASourceCaptureCompletionExists ? "yes" : "no"}**`);
  lines.push(`- Row creation safe now: **${report.rowCreationSafeNow ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Property / example candidates");
  lines.push(
    `- Reviewed: ${report.propertyExampleCandidatesReviewed} · ready: ${report.propertyExampleCandidatesReadyForFounderReview} · blocked: ${report.propertyExampleCandidatesStillBlocked}`
  );
  for (const c of report.propertyExampleCandidates) {
    lines.push(`- **${c.propertyName}** · \`${c.classification}\` · ${c.exampleType}`);
  }
  lines.push("");
  lines.push("## Recent Momentum");
  lines.push(
    `- Ready: ${report.recentMomentumRowsReadyForFounderReview} · blocked: ${report.recentMomentumRowsStillBlocked}`
  );
  for (const m of report.recentMomentumCandidateRows) {
    lines.push(`- **${m.title}** · \`${m.classification}\` · ${m.dateOrYear}`);
  }
  lines.push("");
  lines.push("## Proposed Source Library records");
  for (const s of report.newSourceLibraryRecordsProposed) lines.push(`- ${s.fields[MAP_PARTNER_SOURCE.sourceTitle]}`);
  if (!report.newSourceLibraryRecordsProposed.length) lines.push("- none");
  lines.push("");
  lines.push("## Exact next batch");
  lines.push(`- **${report.exactNextBatch}**`);
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerOpeningsMomentumSourceCaptureCompletionReport(options = {}) {
  const brandRecordId = TRIBUTE_RECORD_ID;
  const usePuppeteer = Boolean(options.usePuppeteer);
  const priorPackage = readJsonIfExists("reports/brand-explorer-openings-momentum-source-capture-package.json");
  const calaDiscovery = readJsonIfExists("reports/cala-tribute-property-visual-discovery.json");

  const brandBasics = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);

  let existingSources = [];
  try {
    let offset = "";
    do {
      const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
      existingSources.push(...(page.sources || []));
      offset = page.offset || "";
    } while (offset);
  } catch (err) {
    console.warn("[v25C-3A-completion] source list warning:", err?.message || err);
  }

  let registryAssets = [];
  try {
    registryAssets = await listRegistryAssetsForBrand(brandRecordId);
  } catch (err) {
    console.warn("[v25C-3A-completion] registry list warning:", err?.message || err);
  }

  const consumerHtml = await fetch(CONSUMER_SITE_URL).then((r) => r.text());
  const consumerRecords = parseConsumerSitePropertyRecords(consumerHtml);
  const consumerByName = new Map(consumerRecords.map((r) => [r.name.toLowerCase(), r]));

  const seedCandidates = (priorPackage?.openingsCandidateCards || []).filter((c) =>
    hasVal(c.propertyName)
  );
  const today = new Date().toISOString().slice(0, 10);

  const propertyExampleCandidates = [];
  for (const seed of seedCandidates) {
    const propertyName = nz(seed.propertyName);
    const consumer = consumerByName.get(propertyName.toLowerCase()) || null;
    const discoveryProp =
      (calaDiscovery?.properties || []).find(
        (p) => nz(p.propertyName).toLowerCase() === propertyName.toLowerCase()
      ) || null;

    const overviewUrl =
      nz(seed.sourceUrl) ||
      nz(discoveryProp?.overviewUrl) ||
      marriottOverviewUrlFromWebsite(nz(discoveryProp?.propertyPageUrl));
    const marsha = nz(consumer?.marsha) || nz(discoveryProp?.marsha) || "";
    const openingDate = nz(consumer?.openingDate);
    const openingDateIsPast = openingDate && openingDate <= today;

    const overviewAttempt = overviewUrl
      ? await fetchMarriottHotelContent(overviewUrl, {
          usePuppeteer,
          fallbackPuppeteer: usePuppeteer,
        })
      : {
          accessDenied: true,
          description: "",
          errors: ["missing_overview_url"],
          overviewUrl: "",
        };

    const photosMeta = overviewUrl ? await fetchMarriottPhotosPageMeta(overviewUrl) : null;
    const locationLabel =
      formatLocationLabel(consumer) ||
      nz(seed.location) ||
      nz(discoveryProp?.countryRegion) ||
      "";

    let bodySummary = nz(overviewAttempt.description);
    let summarySource = bodySummary ? "marriott_overview" : "";
    if (!bodySummary && photosMeta?.ogDescription) {
      bodySummary = photosMeta.ogDescription;
      summarySource = "marriott_photos_og_description";
    }

    const genericTemplate = isGenericPhotosSummary(bodySummary);
    const hasDatedOpening = hasVal(openingDate);
    const exampleType = exampleTypeFromRecord(
      { ...consumer, openingDateIsPast, name: propertyName },
      hasDatedOpening
    );

    const imageUrl = nz(seed.imageUrl) || nz(discoveryProp?.coverImage?.url) || "";
    const propertyUrl = overviewUrl || nz(photosMeta?.photosUrl);
    const brandAffiliation = "Tribute Portfolio (Marriott International)";

    const missingFields = [];
    if (!propertyName) missingFields.push("propertyName");
    if (!imageUrl) missingFields.push("imageUrl");
    if (!locationLabel) missingFields.push("location");
    if (!bodySummary) missingFields.push("bodySummary");
    if (!propertyUrl) missingFields.push("sourceUrl");

    let classification = CLASSIFICATION.REQUIRED;
    if (!bodySummary && overviewAttempt.accessDenied && photosMeta?.accessDenied) {
      classification = CLASSIFICATION.BLOCKED;
    } else if (bodySummary && propertyName && imageUrl && locationLabel && propertyUrl) {
      classification = genericTemplate ? CLASSIFICATION.READY : CLASSIFICATION.READY;
    } else if (!bodySummary) {
      classification = CLASSIFICATION.BLOCKED;
    }

    const readyForFounderReview = classification === CLASSIFICATION.READY;

    const candidate = {
      propertyName,
      marsha,
      brandAffiliation,
      locationLabel,
      exampleType,
      openingDate: openingDate || null,
      openingDateIsPast,
      overviewUrl,
      photosUrl: photosMeta?.photosUrl || "",
      propertyUrl,
      imageUrl,
      imageUsageConfirmed: Boolean(seed.imageUsageConfirmed),
      bodySummary: bodySummary || null,
      summarySource,
      genericTemplateSummary: genericTemplate,
      overviewCapture: {
        attempted: Boolean(overviewUrl),
        accessDenied: Boolean(overviewAttempt.accessDenied),
        source: nz(overviewAttempt.source),
        errors: overviewAttempt.errors || [],
      },
      photosCapture: {
        attempted: Boolean(photosMeta),
        status: photosMeta?.status || 0,
        accessDenied: Boolean(photosMeta?.accessDenied),
        ogTitle: photosMeta?.ogTitle || "",
      },
      consumerSiteEvidence: {
        sourceId: CONSUMER_SOURCE_ID,
        sourceUrl: CONSUMER_SITE_URL,
        openingDate: openingDate || null,
        city: consumer?.city || null,
        country: consumer?.country || null,
        status: consumer?.status || null,
      },
      missingFields,
      classification,
      readyForFounderReview,
      founderReviewNotes: [
        genericTemplate
          ? "Photos-page og:description is official Marriott copy but template-generic — founder should refine to owner-facing opening teaser before row package."
          : null,
        overviewAttempt.accessDenied
          ? "Overview page blocked (403); summary sourced from photos page or consumer-site metadata only."
          : null,
        exampleType === "Property Example"
          ? "Classified as Property Example — do not label as Opening without stronger dated opening narrative."
          : null,
      ].filter(Boolean),
    };

    candidate.proposedCardInput = buildProposedOpeningsCardInput(candidate);
    propertyExampleCandidates.push(candidate);
  }

  const pilotNames = new Set(seedCandidates.map((c) => nz(c.propertyName).toLowerCase()));

  const momentumEligible = consumerRecords
    .filter((r) => hasVal(r.openingDate) && hasVal(r.name) && r.openingDate <= today)
    .map((r) => {
      const openingDateIsPast = true;
      const location = formatLocationLabel(r);
      const summary = `Official Tribute Portfolio consumer brand site lists ${r.name}${
        location ? ` in ${location}` : ""
      } with opening date ${r.openingDate}. Confirm opening vs upcoming listing before external display.`;
      return {
        propertyName: r.name,
        marsha: r.marsha,
        openingDate: r.openingDate,
        openingDateIsPast,
        dateOrYear: formatMomentumDate(r.openingDate) || r.openingDate,
        title: `${r.name} — Tribute Portfolio listing`,
        summary,
        sourceUrl: CONSUMER_SITE_URL,
        sourceId: CONSUMER_SOURCE_ID,
        relationship: "Tribute Portfolio property on official Marriott consumer brand site",
        pilotProperty: pilotNames.has(r.name.toLowerCase()),
        classification: CLASSIFICATION.READY,
        readyForFounderReview: true,
        founderReviewNotes: [
          "Consumer-site openingDate is structured metadata — founder should confirm opening vs listing date before external momentum copy.",
        ],
      };
    })
    .sort((a, b) => {
      if (a.pilotProperty !== b.pilotProperty) return a.pilotProperty ? -1 : 1;
      return nz(b.openingDate).localeCompare(nz(a.openingDate));
    });

  const momentumFutureHeld = consumerRecords
    .filter((r) => hasVal(r.openingDate) && r.openingDate > today)
    .map((r) => ({
      propertyName: r.name,
      marsha: r.marsha,
      openingDate: r.openingDate,
      classification: CLASSIFICATION.REQUIRED,
      readyForFounderReview: false,
      founderReviewNotes: [
        "Future opening date on consumer site — hold for Recent Momentum until date passes or PR capture exists.",
      ],
    }));

  const recentMomentumCandidateRows = momentumEligible.slice(0, 6).map((row) => ({
    ...row,
    proposedRowInput: buildProposedMomentumRow(row),
  }));

  const newSourceLibraryRecordsProposed = [];
  const knownUrls = new Set(existingSources.map((s) => nz(s.sourceUrl).toLowerCase()).filter(Boolean));

  if (!knownUrls.has(CONSUMER_SITE_URL.toLowerCase())) {
    newSourceLibraryRecordsProposed.push(
      buildProposedSourceRecord({
        title: `${BRAND_NAME} — Official consumer brand site (refreshed capture)`,
        sourceUrl: CONSUMER_SITE_URL,
        captureMethod: "live_fetch_v25C-3A_completion",
        evidenceNotes: "Embedded property JSON includes openingDate and location metadata",
        brandRecordId,
      })
    );
  }

  for (const candidate of propertyExampleCandidates) {
    const photosUrl = nz(candidate.photosUrl);
    if (!photosUrl || knownUrls.has(photosUrl.toLowerCase())) continue;
    newSourceLibraryRecordsProposed.push(
      buildProposedSourceRecord({
        title: `${BRAND_NAME} — ${candidate.propertyName} — Marriott photos page`,
        sourceUrl: photosUrl,
        captureMethod: "marriott_photos_page_og_meta",
        evidenceNotes: `og:description capture for property example card (${candidate.marsha || "unknown MARSHA"})`,
        brandRecordId,
      })
    );
    knownUrls.add(photosUrl.toLowerCase());
  }

  const newFactsProposed = [];
  for (const candidate of propertyExampleCandidates.filter((c) => c.readyForFounderReview)) {
    if (candidate.bodySummary) {
      newFactsProposed.push(
        buildProposedFact({
          fieldKey: `be.footprint.propertyExample.${candidate.marsha || "unknown"}.summary`,
          value: candidate.bodySummary,
          evidenceText: `Captured from ${candidate.summarySource} (${candidate.photosUrl || candidate.overviewUrl})`,
          sourceRecordId: CONSUMER_SOURCE_ID,
          brandRecordId,
        })
      );
    }
    if (candidate.openingDate) {
      newFactsProposed.push(
        buildProposedFact({
          fieldKey: `be.footprint.propertyExample.${candidate.marsha || "unknown"}.openingDate`,
          value: candidate.openingDate,
          evidenceText: `Consumer site embedded JSON openingDate for ${candidate.propertyName}`,
          sourceRecordId: CONSUMER_SOURCE_ID,
          brandRecordId,
        })
      );
    }
  }

  const propertyReady = propertyExampleCandidates.filter((c) => c.readyForFounderReview);
  const propertyBlocked = propertyExampleCandidates.filter(
    (c) => c.classification === CLASSIFICATION.BLOCKED
  );
  const momentumReady = recentMomentumCandidateRows.filter((m) => m.readyForFounderReview);
  const momentumBlocked = recentMomentumCandidateRows.filter(
    (m) => m.classification === CLASSIFICATION.BLOCKED
  );

  const openingsRowReviewPackageSafe = propertyReady.length >= OPENINGS_MINIMUM;
  const momentumRowReviewPackageSafe = momentumReady.length >= MOMENTUM_MINIMUM;
  const rowCreationSafeNow = false;

  const exactNextBatch = openingsRowReviewPackageSafe && momentumRowReviewPackageSafe
    ? "v25C-3B-brand-explorer-openings-momentum-row-review-package"
    : "v25C-3A-source-capture-completion-follow-up";
  const exactNextCommand =
    openingsRowReviewPackageSafe && momentumRowReviewPackageSafe
      ? "npm run brand-explorer-openings-momentum-row-review-package -- --brand tribute-portfolio --dry-run"
      : "npm run brand-explorer-openings-momentum-source-capture-completion -- --brand tribute-portfolio --dry-run";

  return {
    completionVersion: COMPLETION_VERSION,
    v25C3ASourceCaptureCompletionExists: true,
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
    captureOptions: { usePuppeteer },
    propertyExampleCandidatesReviewed: propertyExampleCandidates.length,
    propertyExampleCandidatesReadyForFounderReview: propertyReady.length,
    propertyExampleCandidatesStillBlocked: propertyBlocked.length,
    propertyExampleCandidates: propertyExampleCandidates,
    proposedOpeningsCardInputs: propertyReady.map((c) => c.proposedCardInput),
    recentMomentumSourcesFound: [
      {
        sourceId: CONSUMER_SOURCE_ID,
        sourceUrl: CONSUMER_SITE_URL,
        type: "consumer_brand_site_embedded_json",
        propertyCount: consumerRecords.length,
      },
      {
        sourceUrl: "https://news.marriott.com/",
        type: "newsroom",
        status: "source_capture_blocked",
        reason: "JS-shell — requires Rendered Source Capture v1",
      },
    ],
    recentMomentumCandidateRows,
    recentMomentumFutureHeld: momentumFutureHeld,
    recentMomentumRowsReadyForFounderReview: momentumReady.length,
    recentMomentumRowsStillBlocked: momentumBlocked.length,
    proposedMomentumRowInputs: momentumReady.map((m) => m.proposedRowInput),
    newSourceLibraryRecordsProposed,
    newFactsProposed,
    rowCreationSafeNow,
    openingsRowReviewPackageSafe,
    momentumRowReviewPackageSafe,
    remainingSourceGaps: [
      ...propertyBlocked.flatMap((c) =>
        c.missingFields.map((f) => `Openings: ${c.propertyName} missing ${f}`)
      ),
      ...(momentumReady.length < MOMENTUM_MINIMUM
        ? [`Momentum: need ${MOMENTUM_MINIMUM - momentumReady.length} more past-dated source-backed rows`]
        : []),
      "Rendered Marriott newsroom PR capture still blocked (JS-shell).",
      "Overview pages remain 403 — rely on photos-page og:description until rendered overview capture exists.",
    ],
    exactNextBatch,
    exactNextCommand,
    companyValidatedBefore,
    companyValidatedAfter: companyValidatedBefore,
    doesNotDo: [
      "Create footprint.openings or footprint.momentum presentation rows",
      "Write Airtable by default",
      "Change images, Sort Order, or Company Validated",
      "Invent property summaries or undated momentum announcements",
      "Imply Marriott validated anything",
    ],
  };
}
