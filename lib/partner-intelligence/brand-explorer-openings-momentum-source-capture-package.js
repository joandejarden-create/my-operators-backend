/**
 * Brand Explorer Openings + Recent Momentum Source Capture Package v25C-3A.
 *
 * Read-only package for Tribute Portfolio: inventories reusable assets/sources,
 * proposes source-backed openings and momentum candidates, and plans the next
 * founder-review row package. Does not create presentation rows or write Airtable.
 *
 * @see docs/data-intelligence/brand-explorer-openings-momentum-source-capture-package-v25C-3A.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { listPartnerSources } from "./airtable-source.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";

const OPENINGS_MINIMUM = 3;
const MOMENTUM_MINIMUM = 3;
const LOYALTY_MINIMUM = 5;

export const PACKAGE_VERSION = "25C-3A";
export const REPORT_JSON_NAME = "brand-explorer-openings-momentum-source-capture-package.json";
export const REPORT_MD_NAME = "brand-explorer-openings-momentum-source-capture-package.md";
export const DOC_MD_NAME = "brand-explorer-openings-momentum-source-capture-package-v25C-3A.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const REF_BRANDS = [
  { name: "Curio Collection by Hilton", id: "receQkxgjlezsc1xg" },
  { name: "Kimpton Hotels", id: "recCKuXCmGvxHPfb3" },
  { name: "Radisson Blu by Choice", id: "recWPEvxBQxVVzSq3" },
  { name: "Ascend Hotel Collection", id: "reclkgOzvAcBheUSo" },
];

const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";
const MIN_OPENINGS = OPENINGS_MINIMUM;
const MIN_MOMENTUM = MOMENTUM_MINIMUM;

const GOVERNANCE_LABELS = [
  "AI-assembled from approved source facts",
  "Pending founder review",
  "Not company-validated",
  "Not Marriott-validated",
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/brand-explorer-required-section-source-capture-package.json",
  "reports/brand-explorer-loyalty-row-creation-writer.md",
  "reports/brand-explorer-loyalty-row-creation-writer.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "reports/cala-tribute-property-visual-discovery.json",
  "reports/tribute-visual-asset-slot-review.json",
  "reports/tribute-portfolio-targeted-extract.json",
  "live Tribute Brand Explorer Presentation rows (API)",
  "live Tribute Source Library records (API)",
  "live Tribute Partner Facts (API)",
  "live Tribute Brand Asset Registry records (API)",
  "live Curio/Kimpton/Radisson/Ascend openings and momentum rows (API)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-openings-momentum-source-capture-package.js",
  "scripts/brand-explorer-openings-momentum-source-capture-package.mjs",
  "docs/data-intelligence/brand-explorer-openings-momentum-source-capture-package-v25C-3A.md",
  "reports/brand-explorer-openings-momentum-source-capture-package.md",
  "reports/brand-explorer-openings-momentum-source-capture-package.json",
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

function parseParagraphs(body) {
  return String(body || "")
    .split(/\n\n+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function firstHttp(paragraphs) {
  return paragraphs.find((p) => /^https?:\/\//i.test(p)) || "";
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function openingIsComplete(row) {
  const title = nz(row?.title);
  const image = nz(row?.imageUrl);
  const paras = parseParagraphs(row?.body);
  const textParas = paras.filter((p) => !/^https?:\/\//i.test(p));
  const location = textParas[1] || "";
  const summary = textParas[3] || textParas[4] || textParas[0] || "";
  const url = nz(row?.summaryUrl) || firstHttp(paras);
  return [title, image, location, summary, url].every(hasVal);
}

function momentumIsComplete(row) {
  const title = nz(row?.title);
  const paras = parseParagraphs(row?.body);
  const date = paras[0] || "";
  const summary = paras.filter((p) => !/^https?:\/\//i.test(p)).slice(1).join(" ");
  const url = firstHttp(paras);
  return [title, date, summary, url].every(hasVal);
}

function loyaltyCoverageCount(brand) {
  return [
    blocksForSlot(brand, "loyalty.hero_title").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.earn").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.redeem").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.elite").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.proof").length ? 1 : 0,
  ].reduce((a, b) => a + b, 0);
}

function extractLocationFromPropertyName(propertyName) {
  const name = nz(propertyName);
  const commaParts = name.split(",").map((p) => p.trim()).filter(Boolean);
  if (commaParts.length >= 2) {
    const loc = commaParts.slice(1).find((p) => !/tribute portfolio/i.test(p));
    if (loc) return loc.replace(/\ba Tribute Portfolio.*$/i, "").trim();
  }
  const embedded = name.match(
    /\b(Lima|Medellin|Medellín|Cartagena|Barbados|Puerto Rico|Mexico|Cancún|Cancun|Holbox|Puebla|Panama|Bariloche)\b/i
  );
  return embedded ? embedded[1] : "";
}

function inferPropertySetting(propertyName, discoveryProp) {
  if (discoveryProp?.propertySetting) return discoveryProp.propertySetting;
  const lower = nz(propertyName).toLowerCase();
  if (/resort|all-inclusive/i.test(lower)) return "Resort";
  if (/lodge|hotel/i.test(lower)) return "Urban / Lifestyle";
  return "Boutique / Lifestyle";
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return TRIBUTE_RECORD_ID;
  }
  return nz(raw);
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

function findDiscoveryProperty(discovery, propertyName) {
  const props = Array.isArray(discovery?.properties) ? discovery.properties : [];
  const needle = nz(propertyName).toLowerCase();
  return (
    props.find((p) => nz(p.propertyName).toLowerCase() === needle) ||
    props.find((p) => {
      const n = nz(p.propertyName).toLowerCase();
      return n.includes(needle.slice(0, 20)) || needle.includes(n.slice(0, 20));
    }) ||
    null
  );
}

function findRegistryForProperty(registryAssets, propertyName) {
  const needle = nz(propertyName).toLowerCase();
  return (registryAssets || []).filter((a) =>
    nz(a.assetName).toLowerCase().includes(needle.slice(0, 24))
  );
}

function buildOpeningCandidate({
  propertyName,
  galleryBlock,
  discoveryProp,
  registryMatches,
  index,
}) {
  const location = extractLocationFromPropertyName(propertyName) || nz(discoveryProp?.countryRegion);
  const overviewUrl = nz(discoveryProp?.overviewUrl) || nz(discoveryProp?.propertyPageUrl);
  const coverFromDiscovery = nz(discoveryProp?.coverImage?.url);
  const galleryImage = nz(galleryBlock?.imageUrl);
  const registryApproved = (registryMatches || []).find(
    (a) =>
      nz(a.explorerUsePermission) === "Approved For Explorer" &&
      nz(a.usageReviewStatus) === "Usage Review Complete"
  );
  const relatedPropertyFromRegistry = registryApproved
    ? nz(registryApproved.assetName).split("—")[0].trim()
    : "";
  const imageConfirmed =
    Boolean(registryApproved) &&
    Boolean(coverFromDiscovery || galleryImage) &&
    hasVal(relatedPropertyFromRegistry) &&
    /tribute portfolio/i.test(propertyName);
  const imageUrl = imageConfirmed ? coverFromDiscovery || galleryImage : "";
  const propertySetting = inferPropertySetting(propertyName, discoveryProp);

  const fields = {
    propertyName: hasVal(propertyName),
    imageUrl: hasVal(imageUrl),
    locationOrDescriptor: hasVal(location),
    bodySummary: false,
    sourceUrl: hasVal(overviewUrl),
  };
  const missingFields = Object.entries(fields)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);

  const readyForFounderReview =
    fields.propertyName &&
    fields.imageUrl &&
    fields.locationOrDescriptor &&
    fields.bodySummary &&
    fields.sourceUrl;

  return {
    candidateId: `openings-candidate-${index + 1}`,
    propertyName,
    location: location || null,
    propertySetting,
    imageUrl: imageUrl || null,
    imageSource: imageConfirmed
      ? registryApproved
        ? `registry:${registryApproved.id}`
        : "discovery+calendar"
      : galleryImage
        ? "gallery_unconfirmed_for_openings"
        : null,
    imageUsageConfirmed: imageConfirmed,
    bodySummary: null,
    sourceUrl: overviewUrl || null,
    sourceBasis: overviewUrl
      ? "Marriott-controlled property overview/photos URL (v2 CALA discovery)"
      : "missing property page URL",
    gallerySlotKey: nz(galleryBlock?.slotKey) || null,
    registryRecordIds: (registryMatches || []).map((a) => a.id),
    fieldsPresent: fields,
    missingFields,
    readyForFounderReview,
    rowCreationSafeNow: false,
    governanceLabels: GOVERNANCE_LABELS,
    notes: [
      !fields.bodySummary
        ? "Opening teaser/summary not extracted — Marriott overview pages returned 403 in discovery; capture rendered property page or steward approved fact before row package."
        : null,
      !imageConfirmed && galleryImage
        ? "Gallery image exists but is approved for materials.gallery.* — confirm footprint.openings usage before reuse."
        : null,
      !overviewUrl ? "Register property-specific Marriott overview URL in Source Library." : null,
    ].filter(Boolean),
  };
}

function buildMomentumCandidateFromFact(fact, index) {
  return {
    candidateId: `momentum-candidate-fact-${index + 1}`,
    dateOrYear: nz(fact.sourceDate) || null,
    title: nz(fact.fieldName) || nz(fact.fieldKey),
    bodySummary: nz(fact.approvedValue) || nz(fact.extractedValue),
    sourceUrl: null,
    sourceRecordId: fact.sourceRecordId || fact.primarySourceId || null,
    fieldsPresent: {
      dateOrYear: hasVal(fact.sourceDate),
      title: hasVal(fact.fieldName) || hasVal(fact.fieldKey),
      bodySummary: hasVal(fact.approvedValue) || hasVal(fact.extractedValue),
      sourceUrl: false,
    },
    missingFields: ["dateOrYear", "sourceUrl"],
    readyForFounderReview: false,
    rowCreationSafeNow: false,
    governanceLabels: GOVERNANCE_LABELS,
    notes: ["Partner fact exists but is not an approved openings/momentum editorial fact."],
  };
}

export function buildNextBatchPlan(report) {
  const openingsReady = report.openingsCandidateCards.filter((c) => c.readyForFounderReview).length;
  const momentumReady = report.recentMomentumCandidateRows.filter((c) => c.readyForFounderReview).length;

  if (openingsReady >= MIN_OPENINGS && momentumReady >= MIN_MOMENTUM) {
    return {
      nextBatch: "v25C-3B-brand-explorer-openings-momentum-row-review-package",
      exactNextCommand:
        "npm run brand-explorer-openings-momentum-row-review-package -- --brand tribute-portfolio --dry-run",
      rationale: "Source-backed openings and momentum candidates meet minimum for founder-review row package.",
    };
  }

  return {
    nextBatch: "v25C-3A-source-capture-completion + v25C-3B-row-review-package",
    exactNextCommand:
      "npm run brand-explorer-openings-momentum-source-capture-package -- --brand tribute-portfolio --dry-run",
    rationale:
      "Complete property-page summaries and rendered PR/newsroom captures before v25C-3B row review package.",
    prerequisiteTasks: [
      ...report.openingsSourceCaptureTasks,
      ...report.recentMomentumSourceCaptureTasks,
    ],
  };
}

export function buildBrandExplorerOpeningsMomentumSourceCapturePackageMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Openings + Momentum Source Capture Package v${PACKAGE_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v25C-3A exists: **${report.v25C3AExists ? "yes" : "no"}**`);
  lines.push(`- Loyalty meets minimum: **${report.loyaltyMeetsMinimum ? "yes" : "no"}** (${report.loyaltyCoverageCount}/5)`);
  lines.push(`- Row creation safe now: **${report.rowCreationSafeNow ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Loyalty status");
  lines.push(report.loyaltyNoLongerCurrentTarget ? "- Loyalty is complete; current targets are Openings + Recent Momentum." : "- Loyalty still below minimum.");
  lines.push("");
  lines.push("## Reusable property assets");
  for (const a of report.existingReusablePropertyAssets) {
    lines.push(`- ${a.label}`);
  }
  lines.push("");
  lines.push("## Reusable official sources");
  for (const s of report.existingReusableOfficialSources) {
    lines.push(`- ${s}`);
  }
  lines.push("");
  lines.push("## Openings candidate cards");
  for (const c of report.openingsCandidateCards) {
    lines.push(
      `- **${c.propertyName}** · ready: ${c.readyForFounderReview ? "yes" : "no"} · missing: ${c.missingFields.join(", ") || "none"}`
    );
  }
  lines.push("");
  lines.push("## Recent Momentum candidate rows");
  if (!report.recentMomentumCandidateRows.length) {
    lines.push("- none source-backed yet");
  }
  for (const r of report.recentMomentumCandidateRows) {
    lines.push(`- **${r.title || "(untitled)"}** · ready: ${r.readyForFounderReview ? "yes" : "no"}`);
  }
  lines.push("");
  lines.push("## Sections ready for founder-review row package");
  for (const s of report.sectionsReadyForFounderReviewRowPackage) lines.push(`- ${s}`);
  if (!report.sectionsReadyForFounderReviewRowPackage.length) lines.push("- none");
  lines.push("");
  lines.push("## Sections not ready");
  for (const s of report.sectionsNotReady) lines.push(`- ${s}`);
  lines.push("");
  lines.push("## Openings source-capture tasks");
  for (const t of report.openingsSourceCaptureTasks) lines.push(`- ${t}`);
  lines.push("");
  lines.push("## Recent Momentum source-capture tasks");
  for (const t of report.recentMomentumSourceCaptureTasks) lines.push(`- ${t}`);
  lines.push("");
  lines.push("## Reference row patterns");
  for (const r of report.referenceRowPatterns) {
    lines.push(`- **${r.brand}**: openings ${r.openingsCount} (${r.openingsComplete} complete), momentum ${r.momentumCount} (${r.momentumComplete} complete)`);
  }
  lines.push("");
  lines.push("## Exact next batch");
  lines.push(`- **${report.exactNextBatch}**`);
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerOpeningsMomentumSourceCapturePackageReport(options = {}) {
  const brandRecordId = normalizeBrandInput(options.brandIdOrName || "tribute-portfolio");
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-3A pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const tribute = await fetchBrandApiShape(brandRecordId);
  if (!tribute) throw new Error("Could not load Tribute Portfolio via brand-library API");

  const brandBasics = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);

  const contract = readJsonIfExists("reports/brand-explorer-required-section-population-contract.json");
  const loyaltyWriter = readJsonIfExists("reports/brand-explorer-loyalty-row-creation-writer.json");
  const sourceCapture = readJsonIfExists("reports/brand-explorer-required-section-source-capture-package.json");
  const calaDiscovery = readJsonIfExists("reports/cala-tribute-property-visual-discovery.json");
  const assetReview = readJsonIfExists("reports/tribute-visual-asset-slot-review.json");
  const targetedExtract = readJsonIfExists("reports/tribute-portfolio-targeted-extract.json");

  let sources = [];
  let facts = [];
  let registryAssets = [];
  try {
    let offset = "";
    do {
      const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
      sources.push(...(page.sources || []));
      offset = page.offset || "";
    } while (offset);
  } catch (err) {
    console.warn("[v25C-3A] Source list warning:", err?.message || err);
  }
  try {
    let offset = "";
    do {
      const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
      facts.push(...(page.facts || []));
      offset = page.offset || "";
    } while (offset);
  } catch (err) {
    console.warn("[v25C-3A] Facts list warning:", err?.message || err);
  }
  try {
    registryAssets = await listRegistryAssetsForBrand(brandRecordId);
  } catch (err) {
    console.warn("[v25C-3A] Registry list warning:", err?.message || err);
  }

  const referenceRowPatterns = [];
  for (const ref of REF_BRANDS) {
    const b = await fetchBrandApiShape(ref.id);
    if (!b) continue;
    const openings = blocksForSlot(b, OPENINGS_SLOT);
    const momentum = blocksForSlot(b, MOMENTUM_SLOT);
    referenceRowPatterns.push({
      brand: ref.name,
      openingsCount: openings.length,
      openingsComplete: openings.filter(openingIsComplete).length,
      momentumCount: momentum.length,
      momentumComplete: momentum.filter(momentumIsComplete).length,
      sampleOpeningShape: openings[0]
        ? {
            title: nz(openings[0].title),
            hasImage: hasVal(openings[0].imageUrl),
            bodyBlockCount: parseParagraphs(openings[0].body).length,
            hasUrl: Boolean(firstHttp(parseParagraphs(openings[0].body))),
          }
        : null,
      sampleMomentumShape: momentum[0]
        ? {
            title: nz(momentum[0].title),
            dateLine: parseParagraphs(momentum[0].body)[0] || "",
            hasUrl: Boolean(firstHttp(parseParagraphs(momentum[0].body))),
          }
        : null,
    });
  }

  const loyaltyCoverage = loyaltyCoverageCount(tribute);
  const loyaltyMeetsMinimum = loyaltyCoverage >= LOYALTY_MINIMUM;
  const loyaltyNoLongerCurrentTarget = loyaltyMeetsMinimum;

  const existingOpeningsRows = blocksForSlot(tribute, OPENINGS_SLOT);
  const existingMomentumRows = blocksForSlot(tribute, MOMENTUM_SLOT);
  const openingsCompleteCount = existingOpeningsRows.filter(openingIsComplete).length;
  const momentumCompleteCount = existingMomentumRows.filter(momentumIsComplete).length;

  const galleryBlocks = [1, 2, 3, 4, 5, 6]
    .map((i) => blocksForSlot(tribute, `materials.gallery.${i}`)[0])
    .filter(Boolean);

  const propertySeedNames = galleryBlocks
    .map((b) => nz(b.title))
    .filter((t) => /tribute portfolio/i.test(t));

  const openingsCandidateCards = propertySeedNames.map((propertyName, index) => {
    const galleryBlock = galleryBlocks.find((b) => nz(b.title) === propertyName);
    const discoveryProp = findDiscoveryProperty(calaDiscovery, propertyName);
    const registryMatches = findRegistryForProperty(registryAssets, propertyName);
    return buildOpeningCandidate({
      propertyName,
      galleryBlock,
      discoveryProp,
      registryMatches,
      index,
    });
  });

  const existingReusablePropertyAssets = [];
  for (const block of galleryBlocks) {
    if (!hasVal(block.imageUrl)) continue;
    const discoveryProp = findDiscoveryProperty(calaDiscovery, block.title);
    const registryMatches = findRegistryForProperty(registryAssets, block.title);
    existingReusablePropertyAssets.push({
      label: `${nz(block.title) || block.slotKey} · gallery image · registry matches ${registryMatches.length}`,
      slotKey: block.slotKey,
      propertyName: nz(block.title) || null,
      imageUrl: nz(block.imageUrl),
      overviewUrl: nz(discoveryProp?.overviewUrl) || null,
      coverImageUrl: nz(discoveryProp?.coverImage?.url) || null,
      registryRecordIds: registryMatches.map((a) => a.id),
      explorerApproved: registryMatches.some(
        (a) => nz(a.explorerUsePermission) === "Approved For Explorer"
      ),
    });
  }

  const approvedSources = sources.filter((s) => {
    const flag = nz(s.approvedForExplorerUse).toLowerCase();
    return flag === "yes" || flag === "true" || s.approvedForExplorerUse === true;
  });
  const existingReusableOfficialSources = approvedSources.map(
    (s) => `${s.id}: ${nz(s.sourceTitle)}${s.sourceUrl ? ` (${s.sourceUrl})` : ""}`
  );

  const openingsMissingFields = openingsCandidateCards.flatMap((c) =>
    c.missingFields.map((f) => `${c.propertyName}: ${f}`)
  );
  const openingsSourceCaptureTasks = [
    "Capture rendered Marriott property overview pages (overview returned 403 in v2 discovery) for opening teaser + location confirmation.",
    "For each of 3+ priority properties, steward one property-specific overview URL in Source Library with Approved for Explorer Use.",
    "Draft one-sentence opening teaser per property from captured page text — no fake openings or undated claims.",
    "Confirm footprint.openings image usage in Brand Asset Registry (property + brand + slot context) before reusing gallery heroes.",
    "Do not promote materials.gallery images to footprint.openings without usage review.",
  ];

  const momentumFacts = facts.filter((f) => {
    const key = nz(f.fieldName).toLowerCase();
    return key.includes("momentum") || key.includes("opening") || key.includes("footprint.recent");
  });

  const recentMomentumCandidateRows = momentumFacts
    .filter((f) => nz(f.humanReviewStatus) === "Approved" || nz(f.humanReviewStatus) === "Edited")
    .map((f, i) => buildMomentumCandidateFromFact(f, i));

  const recentMomentumSourceCaptureTasks = [
    "Rendered Source Capture v1 for Marriott newsroom PR (news.marriott.com is JS-shell — not extraction-eligible live).",
    "Capture 3 dated Tribute Portfolio property openings/signings from official Marriott media or property pages with headline, date, summary, and URL.",
    "Register each capture in Source Library with Approved for Explorer Use before fact extraction.",
    "Do not create undated momentum rows or infer opening dates from gallery placement alone.",
    "Optional: steward PR / Opening Link registry rows after rendered capture + usage review.",
  ];

  const recentMomentumMissingFields = [
    "dateOrYear: no approved dated momentum facts",
    "title: no stewarded PR headlines",
    "bodySummary: no source-backed opening narrative",
    "sourceUrl: newsroom provenance-only until rendered capture",
  ];

  const sectionsReadyForFounderReviewRowPackage = [];
  const sectionsNotReady = [];
  if (loyaltyMeetsMinimum) {
    sectionsReadyForFounderReviewRowPackage.push("Loyalty Program (v25C-2D applied)");
  } else {
    sectionsNotReady.push("Loyalty Program");
  }
  if (openingsCandidateCards.filter((c) => c.readyForFounderReview).length >= MIN_OPENINGS) {
    sectionsReadyForFounderReviewRowPackage.push("Openings / Examples / Properties");
  } else {
    sectionsNotReady.push(
      `Openings / Examples / Properties (${openingsCandidateCards.filter((c) => c.readyForFounderReview).length}/${MIN_OPENINGS} founder-ready cards)`
    );
  }
  if (recentMomentumCandidateRows.filter((c) => c.readyForFounderReview).length >= MIN_MOMENTUM) {
    sectionsReadyForFounderReviewRowPackage.push("Recent Momentum");
  } else {
    sectionsNotReady.push(
      `Recent Momentum (0/${MIN_MOMENTUM} source-backed dated rows)`
    );
  }

  const rowCreationSafeNow = false;

  const report = {
    packageVersion: PACKAGE_VERSION,
    v25C3AExists: true,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    sortOrderUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: {
      recordId: TRIBUTE_RECORD_ID,
      name: BRAND_NAME,
      slug: "tribute-portfolio",
    },
    contractSourceOfTruth: {
      version: contract?.contractVersion || "25C-1",
      brandExplorerRequiredSectionsReady: contract?.brandExplorerRequiredSectionsReady ?? false,
      openingsMinimum: MIN_OPENINGS,
      momentumMinimum: MIN_MOMENTUM,
    },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    loyaltyMeetsMinimum,
    loyaltyCoverageCount: loyaltyCoverage,
    loyaltyNoLongerCurrentTarget,
    loyaltyWriterApplied: Boolean(loyaltyWriter?.airtableModified),
    existingPresentationRows: {
      footprintOpenings: existingOpeningsRows.length,
      footprintOpeningsComplete: openingsCompleteCount,
      footprintMomentum: existingMomentumRows.length,
      footprintMomentumComplete: momentumCompleteCount,
    },
    existingReusablePropertyAssets,
    existingReusableOfficialSources,
    openingsCandidateCards,
    openingsMissingFields,
    openingsSourceCaptureTasks,
    recentMomentumCandidateRows,
    recentMomentumMissingFields,
    recentMomentumSourceCaptureTasks,
    sectionsReadyForFounderReviewRowPackage,
    sectionsNotReady,
    referenceRowPatterns,
    sourceGaps: {
      openings: openingsMissingFields,
      momentum: recentMomentumMissingFields,
      newsroomNote:
        targetedExtract?.sourceInventory?.find((s) => /newsroom/i.test(s.title || ""))?.note ||
        "news.marriott.com is JS-shell — use rendered capture, not live extraction.",
    },
    assetsReusableSummary: {
      gallerySlotsWithImages: galleryBlocks.filter((b) => hasVal(b.imageUrl)).length,
      registryRecordsScanned: registryAssets.length,
      prOpeningLinkRegistryEmpty: !(assetReview?.recordsBySlot?.["PR / Opening Link"] || []).length,
      calaPropertiesWithCoverImage: (calaDiscovery?.properties || []).filter((p) =>
        hasVal(p?.coverImage?.url)
      ).length,
    },
    rowCreationSafeNow,
    governanceLabels: GOVERNANCE_LABELS,
    companyValidatedBefore,
    companyValidatedAfter: companyValidatedBefore,
    doesNotDo: [
      "Create footprint.openings or footprint.momentum presentation rows",
      "Write Airtable by default",
      "Change images, Sort Order, Brand Basics, or Company Validated",
      "Use unconfirmed gallery images for openings without registry approval",
      "Create fake openings or undated momentum rows",
      "Imply Marriott validated anything",
    ],
  };

  const nextPlan = buildNextBatchPlan(report);
  report.exactNextBatch = nextPlan.nextBatch;
  report.exactNextCommand = nextPlan.exactNextCommand;
  report.nextRowReviewPackagePlan = {
    batch: "v25C-3B-brand-explorer-openings-momentum-row-review-package",
    blockedUntil: nextPlan.prerequisiteTasks || [
      "Complete openings property-page capture (3 summaries)",
      "Complete momentum rendered PR capture (3 dated items)",
    ],
    openingsCandidatesForReview: openingsCandidateCards.slice(0, MIN_OPENINGS),
    momentumCandidatesForReview: recentMomentumCandidateRows,
  };

  return report;
}
