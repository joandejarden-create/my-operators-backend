/**
 * Brand Asset & PR Package Governance v1 — read-only pilot module.
 *
 * Inspects Brand Setup / Explorer media fields, local reference material, and
 * official Marriott-controlled URL candidates. Produces a report-only recommended
 * asset package. Does NOT download images, overwrite fields, or write Airtable.
 *
 * @see docs/data-intelligence/brand-asset-pr-package-governance-v1.md
 */
import fs from "fs";
import path from "path";
import {
  assessAssetSignals,
  assessExplorerActiveStatus,
} from "./active-brand-governance-upgrade.js";
import { SOURCE_ROLE } from "./brand-source-auto-resolver.js";
import {
  downloadUrlWithFallback,
  estimateReadableTextLength,
} from "./choice-legacy-batch-url-capture.js";
import { parseHtmlDocument } from "./extract-source-text.js";
import { listPartnerSources } from "./airtable-source.js";
import { listPartnerFacts } from "./airtable-facts.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
  PARENT_COMPANY,
  COMPANY_FOLDER,
  URL_CANDIDATES,
  scanLocalTributeFiles,
  getTributeBrandRecord,
} from "./tribute-portfolio-brand-package.js";

export const GOVERNANCE_VERSION = "1";
export const REPORT_JSON_NAME = "brand-asset-pr-package-governance.json";
export const REPORT_MD_NAME = "brand-asset-pr-package-governance.md";

export const ASSET_STATUS = {
  CANDIDATE: "Candidate",
  SOURCE_CONFIRMED: "Source-Confirmed",
  APPROVED_EXPLORER: "Approved For Explorer Use",
  NEEDS_USAGE_REVIEW: "Needs Usage Review",
  DO_NOT_USE: "Do Not Use",
  MOCK_DEMO: "Mock/Demo",
  MISSING: "Missing",
};

export const ASSET_TYPE = {
  LOGO: "Logo",
  HERO: "Hero Image",
  EXTERIOR: "Exterior / Property",
  GUESTROOM: "Guestroom",
  LOBBY: "Lobby / Public Space",
  LIFESTYLE: "Restaurant / Bar / Lifestyle",
  PR_IMAGE: "PR / Opening Image",
  PDF: "PDF / Brochure",
  PRESS_LINK: "Press Link",
  OPENING_LINK: "Recent Opening Link",
};

export const SOURCE_BASIS = {
  COMPANY_MATERIALS: "Company Materials",
  MARRIOTT_CONTROLLED: "Marriott-Controlled Source",
  RENDERED_OFFICIAL: "Rendered Official Source",
  LOCAL_REFERENCE: "Local Reference Material",
  THIRD_PARTY: "Third-Party Context",
  UNKNOWN: "Unknown / Do Not Use",
};

const LOGO_FIELD_CANDIDATES = ["Logo", "Brand Logo", "Logo Image", "Brand Logo Image"];
const HERO_FIELD_CANDIDATES = ["Explorer Hero Data Source", "Explorer Hero Verification"];
const MEDIA_FIELD_PATTERNS = [
  { re: /^logo$/i, category: "logo" },
  { re: /hero/i, category: "hero" },
  { re: /image|photo|gallery/i, category: "image" },
  { re: /pdf|brochure|attachment|deck/i, category: "pdf" },
  { re: /press|pr\b|news|opening|recent/i, category: "pr" },
  { re: /media|asset|presentation/i, category: "media" },
];

const IMAGE_EXT = new Set([".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".avif"]);
const COMPANY_DOMAINS = [
  "marriott.com",
  "tribute-portfolio.marriott.com",
  "hotel-development.marriott.com",
  "news.marriott.com",
];

/** Pilot brand registry — extend for future brands. */
export const BRAND_ASSET_PILOT_CONFIG = {
  "tribute-portfolio": {
    recordId: TRIBUTE_RECORD_ID,
    brandName: BRAND_NAME,
    parentCompany: PARENT_COMPANY,
    companyFolder: COMPANY_FOLDER,
    consumerUrl: "https://tribute-portfolio.marriott.com/",
    pressHubUrl: "https://news.marriott.com/brands/tribute-portfolio",
    pressSearchUrl: "https://news.marriott.com/search?q=tribute+portfolio",
    urlCandidates: URL_CANDIDATES,
    localScan: scanLocalTributeFiles,
    fetchBrandRecord: getTributeBrandRecord,
  },
};

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function fieldPopulated(fields, key) {
  const v = fields?.[key];
  if (v == null || v === "") return false;
  if (Array.isArray(v)) return v.length > 0;
  return Boolean(nz(v));
}

function isCompanyUrl(url) {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return COMPANY_DOMAINS.some((d) => host === d || host.endsWith(`.${d}`));
  } catch {
    return false;
  }
}

function classifyLocalAssetType(filename, relPath) {
  const lower = `${filename} ${relPath}`.toLowerCase();
  if (/logo/i.test(lower)) return ASSET_TYPE.LOGO;
  if (/hero|banner|header/i.test(lower)) return ASSET_TYPE.HERO;
  if (/lobby|public|lounge|bar|restaurant/i.test(lower)) return ASSET_TYPE.LOBBY;
  if (/room|guest|suite|bed/i.test(lower)) return ASSET_TYPE.GUESTROOM;
  if (/pool|exterior|facade|property|resort/i.test(lower)) return ASSET_TYPE.EXTERIOR;
  if (/press|opening|news|pr/i.test(lower)) return ASSET_TYPE.PR_IMAGE;
  if (/\.pdf$/i.test(filename)) return ASSET_TYPE.PDF;
  if (IMAGE_EXT.has(path.extname(filename).toLowerCase())) return ASSET_TYPE.EXTERIOR;
  return ASSET_TYPE.EXTERIOR;
}

/** Discover Brand Setup fields related to logo, hero, images, PDFs, PR. */
export function discoverBrandSetupMediaSchema(fields) {
  if (!fields) {
    return { schemaExists: false, fields: [], note: "Brand Setup row not loaded." };
  }
  const discovered = [];
  for (const [name, value] of Object.entries(fields)) {
    const category = MEDIA_FIELD_PATTERNS.find((p) => p.re.test(name))?.category;
    if (!category) continue;
    const populated = fieldPopulated(fields, name);
    let valueKind = "text";
    if (Array.isArray(value) && value.length > 0) {
      valueKind = value[0]?.url ? "attachment" : "array";
    }
    discovered.push({ field: name, category, populated, valueKind });
  }
  return {
    schemaExists: discovered.length > 0,
    fields: discovered,
    logoFields: discovered.filter((f) => f.category === "logo"),
    heroFields: discovered.filter((f) => f.category === "hero"),
    imageFields: discovered.filter((f) => f.category === "image"),
    pdfFields: discovered.filter((f) => f.category === "pdf"),
    prFields: discovered.filter((f) => f.category === "pr"),
    presentationNote:
      "Brand Explorer presentation slot images (overview cards, etc.) live in separate presentation tables — not scanned in v1.",
  };
}

/** Extract image URL references from HTML without downloading image binaries. */
export function extractImageUrlReferencesFromHtml(html, { label, sourceBasis } = {}) {
  const refs = new Set();
  const raw = String(html || "");

  for (const m of raw.matchAll(/"contentUrl"\s*:\s*"([^"]+)"/gi)) {
    if (m[1] && !m[1].startsWith("data:")) refs.add(m[1]);
  }
  for (const m of raw.matchAll(/data-mimg="([^"]+)"/gi)) {
    if (m[1] && !m[1].startsWith("data:")) refs.add(m[1]);
  }
  for (const m of raw.matchAll(/https?:\/\/[^"'\s>]+\.(?:jpg|jpeg|png|webp|gif|svg)(?:\?[^"'\s>]*)?/gi)) {
    refs.add(m[0]);
  }

  try {
    const parsed = parseHtmlDocument(raw);
    const body = parsed.text || "";
    if (body.length > 0) {
      for (const m of body.matchAll(/https?:\/\/\S+\.(?:jpg|jpeg|png|webp)/gi)) {
        refs.add(m[0].replace(/[),.]+$/, ""));
      }
    }
  } catch {
    // ignore parse errors
  }

  return [...refs]
    .filter((u) => !/1x1|pixel|spacer|blank/i.test(u))
    .map((url) => ({
      url,
      label: label || "HTML reference",
      sourceBasis: sourceBasis || SOURCE_BASIS.MARRIOTT_CONTROLLED,
      companyControlled: isCompanyUrl(url),
      assetTypeGuess: /logo|svg/i.test(url)
        ? ASSET_TYPE.LOGO
        : /hero|gallery|slider/i.test(url)
          ? ASSET_TYPE.HERO
          : /pool|exterior|facade|gallery/i.test(url)
            ? ASSET_TYPE.EXTERIOR
            : ASSET_TYPE.EXTERIOR,
      status: ASSET_STATUS.CANDIDATE,
      usageReview: true,
    }));
}

export function resolveBrandAssetConfig(brandKey) {
  const key = nz(brandKey).toLowerCase();
  const config = BRAND_ASSET_PILOT_CONFIG[key];
  if (!config) {
    return { resolved: false, brandKey: key, error: `Unknown brand pilot: ${brandKey}` };
  }
  return { resolved: true, brandKey: key, config };
}

async function fetchAllSources(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ brandId: recordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchApprovedFactCount(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: recordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all.filter(
    (f) =>
      nz(f.fieldName).startsWith("be.") &&
      (nz(f.humanReviewStatus) === "Approved" || nz(f.humanReviewStatus) === "Edited")
  ).length;
}

function assessCurrentLogo(fields, assetSignals) {
  const hasLogo = assetSignals.logo === "present";
  const logoField = LOGO_FIELD_CANDIDATES.find((k) => fieldPopulated(fields, k));
  return {
    status: hasLogo ? ASSET_STATUS.NEEDS_USAGE_REVIEW : ASSET_STATUS.MISSING,
    populated: hasLogo,
    field: logoField || null,
    note: hasLogo
      ? "Logo attachment present in Brand Setup — source/rights not confirmed; do not treat as Source-Confirmed without review."
      : "No logo attachment detected in Brand Setup.",
  };
}

function assessCurrentHero(fields) {
  const dataSource = nz(fields["Explorer Hero Data Source"]);
  const verification = nz(fields["Explorer Hero Verification"]);
  const isMock = /mock|demo/i.test(dataSource) || /mock|demo/i.test(verification);
  return {
    status: isMock ? ASSET_STATUS.MOCK_DEMO : dataSource ? ASSET_STATUS.NEEDS_USAGE_REVIEW : ASSET_STATUS.MISSING,
    dataSource: dataSource || null,
    verification: verification || null,
    note: isMock
      ? "Hero is Mock/Demo — must not be used as governed Explorer hero."
      : dataSource
        ? "Hero signal present — verify source and usage rights before Explorer display."
        : "No Explorer hero data source recorded.",
  };
}

function buildLocalAssetInventory(localScan) {
  const files = localScan?.found || [];
  return files.map((f) => ({
    relativePath: f.relativePath,
    filename: f.filename,
    ext: f.ext,
    sizeBytes: f.sizeBytes,
    tributeSpecific: f.tributeSpecific,
    assetType: f.isPdf ? ASSET_TYPE.PDF : f.isImage ? classifyLocalAssetType(f.filename, f.relativePath) : null,
    status: f.isImage || f.isPdf ? ASSET_STATUS.CANDIDATE : null,
    sourceBasis: SOURCE_BASIS.LOCAL_REFERENCE,
    usageReview: true,
    note: f.tributeSpecific ? "Tribute-specific local file" : "Marriott company-context file",
  }));
}

async function probeOfficialUrlCandidates(config, { probeUrls }) {
  const results = [];
  for (const cand of config.urlCandidates || []) {
    if (!probeUrls) {
      results.push({ ...cand, probed: false, status: "not_probed" });
      continue;
    }
    try {
      const dl = await downloadUrlWithFallback(cand.url);
      const readableTextLength = estimateReadableTextLength(dl.buf, dl.contentType, ".html");
      const jsShellRisk =
        readableTextLength < 400 ? "high" : readableTextLength < 1200 ? "medium" : "low";
      const html = dl.buf?.toString?.("utf8") || "";
      const imageRefs =
        cand.role === SOURCE_ROLE.PR_OPENING && jsShellRisk === "high"
          ? []
          : extractImageUrlReferencesFromHtml(html, {
              label: cand.label,
              sourceBasis: SOURCE_BASIS.MARRIOTT_CONTROLLED,
            }).slice(0, 12);

      results.push({
        slot: cand.slot,
        role: cand.role,
        url: cand.url,
        label: cand.label,
        httpStatus: dl.httpStatus,
        readableTextLength,
        jsShellRisk,
        companyControlled: true,
        imageUrlCandidates: imageRefs,
        pressLinkCandidate: cand.role === SOURCE_ROLE.PR_OPENING,
        assetUsability:
          jsShellRisk === "high"
            ? ASSET_STATUS.DO_NOT_USE
            : imageRefs.length
              ? ASSET_STATUS.CANDIDATE
              : ASSET_STATUS.NEEDS_USAGE_REVIEW,
        note:
          jsShellRisk === "high"
            ? "JS-shell / near-zero extractable text — provenance only; needs Rendered Source Capture v1."
            : "Reachable official URL — image refs parsed from HTML only (no image download).",
      });
    } catch (err) {
      results.push({
        slot: cand.slot,
        role: cand.role,
        url: cand.url,
        label: cand.label,
        probed: true,
        status: "failed",
        error: err.message || String(err),
        assetUsability: ASSET_STATUS.MISSING,
      });
    }
  }
  return results;
}

function extractLocalHtmlImageCandidates(localScan) {
  const candidates = [];
  for (const f of localScan?.found || []) {
    if (!/\.html?$/i.test(f.filename)) continue;
    try {
      const abs = path.join(localScan.referenceRoot, f.relativePath);
      const html = fs.readFileSync(abs, "utf8");
      const refs = extractImageUrlReferencesFromHtml(html, {
        label: `Local capture: ${f.relativePath}`,
        sourceBasis: SOURCE_BASIS.LOCAL_REFERENCE,
      });
      for (const ref of refs) {
        candidates.push({
          ...ref,
          localCapturePath: f.relativePath,
          status: ASSET_STATUS.CANDIDATE,
        });
      }
    } catch {
      // skip unreadable
    }
  }
  return candidates;
}

function buildRecommendedAssetPackage(ctx) {
  const pkg = [];
  const { logo, hero, localImages, officialImageCandidates, pressCandidates, localPdfs } = ctx;

  if (logo.populated) {
    pkg.push({
      assetType: ASSET_TYPE.LOGO,
      status: ASSET_STATUS.NEEDS_USAGE_REVIEW,
      sourceBasis: SOURCE_BASIS.COMPANY_MATERIALS,
      ref: logo.field,
      action: "Confirm existing Brand Setup logo against official Marriott tribute-portfolio.svg",
      priority: 1,
    });
  } else {
    const logoUrl = officialImageCandidates.find((c) => c.assetTypeGuess === ASSET_TYPE.LOGO);
    pkg.push({
      assetType: ASSET_TYPE.LOGO,
      status: ASSET_STATUS.CANDIDATE,
      sourceBasis: logoUrl?.sourceBasis || SOURCE_BASIS.MARRIOTT_CONTROLLED,
      ref: logoUrl?.url || "hotel-development.marriott.com brand logos (from local HTML capture)",
      action: "Select official logo candidate after usage review — do not download in v1",
      priority: 1,
    });
  }

  pkg.push({
    assetType: ASSET_TYPE.HERO,
    status: ASSET_STATUS.MOCK_DEMO,
    sourceBasis: SOURCE_BASIS.UNKNOWN,
    ref: hero.dataSource,
    action: "Replace Mock/Demo hero with Marriott-controlled candidate after Rendered Source Capture",
    priority: 1,
  });

  const heroCandidates = officialImageCandidates.filter((c) =>
    /hero|gallery|premium-brands|tribute-portfolio-bg/i.test(c.url)
  );
  if (heroCandidates.length) {
    pkg.push({
      assetType: ASSET_TYPE.HERO,
      status: ASSET_STATUS.CANDIDATE,
      sourceBasis: SOURCE_BASIS.MARRIOTT_CONTROLLED,
      ref: heroCandidates[0].url,
      action: "Hero candidate from official page HTML — usage review required",
      priority: 2,
    });
  }

  const propertyCandidates = officialImageCandidates.filter(
    (c) => c.assetTypeGuess === ASSET_TYPE.EXTERIOR && !/logo|svg/i.test(c.url)
  );
  for (const c of propertyCandidates.slice(0, 4)) {
    pkg.push({
      assetType: ASSET_TYPE.EXTERIOR,
      status: ASSET_STATUS.CANDIDATE,
      sourceBasis: c.sourceBasis,
      ref: c.url,
      action: "Property/design candidate — usage review before Explorer",
      priority: 3,
    });
  }

  for (const pdf of localPdfs.slice(0, 2)) {
    pkg.push({
      assetType: ASSET_TYPE.PDF,
      status: ASSET_STATUS.SOURCE_CONFIRMED,
      sourceBasis: SOURCE_BASIS.LOCAL_REFERENCE,
      ref: pdf.relativePath,
      action: "FDD/brochure already in PI Source Library — text only; not Explorer hero/logo",
      priority: 4,
    });
  }

  for (const press of pressCandidates) {
    pkg.push({
      assetType: ASSET_TYPE.PRESS_LINK,
      status: press.jsShellRisk === "high" ? ASSET_STATUS.DO_NOT_USE : ASSET_STATUS.CANDIDATE,
      sourceBasis: SOURCE_BASIS.MARRIOTT_CONTROLLED,
      ref: press.url,
      action:
        press.jsShellRisk === "high"
          ? "Provenance only until Rendered Source Capture v1"
          : "Press hub link — capture recent openings manually",
      priority: 5,
    });
  }

  if (localImages.length) {
    pkg.push({
      assetType: ASSET_TYPE.EXTERIOR,
      status: ASSET_STATUS.CANDIDATE,
      sourceBasis: SOURCE_BASIS.LOCAL_REFERENCE,
      ref: localImages.map((i) => i.relativePath).join("; "),
      action: `${localImages.length} local image file(s) found — verify rights before use`,
      priority: 6,
    });
  }

  return pkg.sort((a, b) => a.priority - b.priority);
}

export async function buildBrandAssetPrPackageGovernanceReport({
  brandKey = "tribute-portfolio",
  probeUrls = true,
} = {}) {
  const resolved = resolveBrandAssetConfig(brandKey);
  if (!resolved.resolved) {
    return {
      governanceVersion: GOVERNANCE_VERSION,
      generatedAt: new Date().toISOString(),
      mode: "dry-run",
      airtableModified: false,
      error: resolved.error,
    };
  }

  const { config } = resolved;
  const record = await config.fetchBrandRecord();
  const fields = record.fields || {};
  const explorer = assessExplorerActiveStatus(fields);
  const assetSignals = assessAssetSignals(fields);
  const mediaSchema = discoverBrandSetupMediaSchema(fields);
  const logo = assessCurrentLogo(fields, assetSignals);
  const hero = assessCurrentHero(fields);

  const [sources, approvedFactCount] = await Promise.all([
    fetchAllSources(config.recordId),
    fetchApprovedFactCount(config.recordId),
  ]);

  const approvedSources = sources.filter((s) => nz(s.approvedForExplorerUse) === "Yes");
  const localScan = config.localScan();
  const localInventory = buildLocalAssetInventory(localScan);
  const localImages = localInventory.filter((f) => f.assetType && f.assetType !== ASSET_TYPE.PDF);
  const localPdfs = localInventory.filter((f) => f.assetType === ASSET_TYPE.PDF);

  const urlProbes = await probeOfficialUrlCandidates(config, { probeUrls });
  const officialImageCandidates = [
    ...urlProbes.flatMap((p) => p.imageUrlCandidates || []),
    ...extractLocalHtmlImageCandidates(localScan),
  ];
  const dedupedImages = [];
  const seen = new Set();
  for (const c of officialImageCandidates) {
    if (seen.has(c.url)) continue;
    seen.add(c.url);
    dedupedImages.push(c);
  }

  const pressCandidates = urlProbes.filter((p) => p.pressLinkCandidate || p.role === SOURCE_ROLE.PR_OPENING);

  const governedProfile = {
    textGovernancePlatformReady: approvedFactCount >= 3 && approvedSources.length >= 1,
    validationStatus: nz(fields["Validation Status"]) || null,
    usagePermission: nz(fields["Usage Permission"]) || null,
    externalDisplayStatus: nz(fields["External Display Status"]) || null,
    displayLabel: nz(fields["Display Label"]) || null,
    sourceBasis: "Company Materials",
    companyValidated: fields["Company Validated"] ?? null,
    companyValidationDate: fields["Company Validation Date"] ?? null,
    approvedSources: approvedSources.length,
    approvedFacts: approvedFactCount,
    explorerActive: explorer.active,
  };

  const recommendedPackage = buildRecommendedAssetPackage({
    logo,
    hero,
    localImages,
    officialImageCandidates: dedupedImages,
    pressCandidates,
    localPdfs,
  });

  const safeToUseNow = [
    "Approved PI text facts (7) and governance trust chip (AI-Assisted Profile / Company Materials)",
    "Approved Source Library rows (6 Marriott-controlled sources) for provenance — text extraction only",
    "Local FDD PDF as factual/legal reference in Source Library — not for Explorer hero/logo display",
  ];

  const needsHumanReview = [
    "Existing Brand Setup logo — confirm authoritative Marriott source and usage rights",
    "All image URL candidates parsed from HTML — usage/rights review before any Explorer display",
    "Any local image files — verify not outdated or third-party before use",
  ];

  const requiresFutureTooling = [
    "Rendered Source Capture v1 — required for news.marriott.com PR/openings (JS-shell)",
    "Asset download + rights registry — no approved image download workflow in repo",
    "Airtable asset governance fields — v1 is report-only; no asset status columns written",
    "Explorer hero/logo field writer — must not overwrite Mock/Demo hero without staged approval",
    "Presentation slot image governance — overview cards / property galleries not scanned in v1",
  ];

  return {
    governanceVersion: GOVERNANCE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    brand: {
      key: brandKey,
      recordId: config.recordId,
      name: config.brandName,
      parentCompany: config.parentCompany,
      resolved: record.resolved !== false,
    },
    governedProfileStatus: governedProfile,
    mediaSchema,
    currentStatus: {
      logo,
      hero,
      propertyDesignImages: {
        status:
          assetSignals.propertyDesignImages === "likely_present"
            ? ASSET_STATUS.NEEDS_USAGE_REVIEW
            : ASSET_STATUS.MISSING,
        signal: assetSignals.propertyDesignImages,
        note: "No governed property/design/lifestyle image package in Brand Setup or PI.",
      },
      pdfAttachments: {
        status: localPdfs.length ? ASSET_STATUS.SOURCE_CONFIRMED : ASSET_STATUS.MISSING,
        localPdfCount: localPdfs.length,
        piSourcePdfCount: sources.filter((s) => nz(s.sourceType) === "FDD" || /\.pdf/i.test(nz(s.localFilePath))).length,
        note: assetSignals.pdfBrochures,
      },
      prRecentOpenings: {
        status: ASSET_STATUS.MISSING,
        newsroomJsShell: true,
        note: assetSignals.recentOpeningsPrLinks,
        pressHubProbe: pressCandidates[0] || null,
      },
    },
    localAssets: {
      referenceRoot: localScan.referenceRoot,
      totalFiles: localScan.found?.length || 0,
      images: localImages,
      pdfs: localPdfs,
      inventory: localInventory,
    },
    officialCandidates: {
      imageLogo: dedupedImages.filter((c) => c.assetTypeGuess === ASSET_TYPE.LOGO),
      heroProperty: dedupedImages.filter((c) => c.assetTypeGuess !== ASSET_TYPE.LOGO).slice(0, 15),
      urlProbes,
    },
    prRecentOpeningCandidates: pressCandidates,
    recommendedAssetPackage: recommendedPackage,
    safeToUseNow,
    needsHumanReview,
    requiresFutureTooling,
    renderedSourceCaptureNeeded: true,
    renderedSourceCaptureReason:
      "news.marriott.com/brands/tribute-portfolio returns near-zero readable text (JS-shell). PR links, recent openings, and press imagery require Rendered Source Capture v1 before asset governance can approve press assets.",
    visualParityGap: {
      kimptonRadissonTarget: "Verified logo, hero, 3–6 property/design images, PR/recent-opening links, governed asset statuses",
      tributeCurrent: "Text/governance Platform Ready; hero Mock/Demo; logo unconfirmed; no governed image package; PR not captured",
      remainingWork: [
        "Confirm logo source and usage rights",
        "Replace Mock/Demo hero with Marriott-controlled candidate",
        "Capture 3–6 property/design/lifestyle images with rights metadata",
        "Rendered capture of Marriott newsroom for PR/recent openings",
        "Future v2: asset status fields + Explorer field writer with staging",
      ],
    },
    piSourceLibraryNote:
      "Partner Source Library has Source URL / Local File Path but no dedicated asset-governance fields (image role, rights, Explorer approval) in v1.",
    doesNotDo: [
      "Download images or scrape OTA/booking-site images",
      "Overwrite Brand Setup logo, hero, image, or attachment fields",
      "Write Airtable or create new schema fields",
      "Set Company Validated or Company Validation Date",
      "Imply Marriott validated assets or profile",
      "Publish/display images without source and usage review",
    ],
    nextCommand: `npm run brand-asset-pr-package-governance -- --brand ${brandKey} --dry-run`,
  };
}

export function buildBrandAssetPrPackageGovernanceMarkdown(report) {
  if (report.error) {
    return `# Brand Asset & PR Package Governance v1\n\nError: ${report.error}\n`;
  }

  const lines = [
    "# Brand Asset & PR Package Governance v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Brand: ${report.brand.name} \`${report.brand.recordId}\``,
    "",
    "## 1. Governed profile status",
    "",
    `- Text/governance Platform Ready: **${report.governedProfileStatus.textGovernancePlatformReady ? "yes" : "no"}**`,
    `- Validation Status: ${report.governedProfileStatus.validationStatus || "—"}`,
    `- Display Label: ${report.governedProfileStatus.displayLabel || "—"}`,
    `- Approved PI sources: ${report.governedProfileStatus.approvedSources} · approved facts: ${report.governedProfileStatus.approvedFacts}`,
    `- Company Validated: ${report.governedProfileStatus.companyValidated} (unchanged)`,
    "",
    "## 2. Asset/media schema",
    "",
    `- Schema fields found in Brand Setup: **${report.mediaSchema.schemaExists ? "yes" : "no"}** (${report.mediaSchema.fields?.length || 0} media-related fields)`,
    `- ${report.mediaSchema.presentationNote}`,
    "",
    ...(report.mediaSchema.fields || []).map(
      (f) => `- \`${f.field}\` (${f.category}) — populated: ${f.populated}`
    ),
    "",
    "## 3. Current asset status",
    "",
    `| Asset | Status | Notes |`,
    `|-------|--------|-------|`,
    `| Logo | ${report.currentStatus.logo.status} | ${report.currentStatus.logo.note} |`,
    `| Hero | ${report.currentStatus.hero.status} | ${report.currentStatus.hero.note} |`,
    `| Property/design images | ${report.currentStatus.propertyDesignImages.status} | ${report.currentStatus.propertyDesignImages.note} |`,
    `| PDF / attachments | ${report.currentStatus.pdfAttachments.status} | ${report.currentStatus.pdfAttachments.note} |`,
    `| PR / recent openings | ${report.currentStatus.prRecentOpenings.status} | ${report.currentStatus.prRecentOpenings.note} |`,
    "",
    "## 4. Local asset files",
    "",
    `- Reference root: \`${report.localAssets.referenceRoot}\``,
    `- Images: **${report.localAssets.images.length}** · PDFs: **${report.localAssets.pdfs.length}**`,
    ...(report.localAssets.images.length
      ? report.localAssets.images.map((f) => `- ${f.relativePath} (${f.assetType})`)
      : ["- No local image files found"]),
    ...(report.localAssets.pdfs.map((f) => `- ${f.relativePath} (PDF)`)),
    "",
    "## 5. Official image/logo candidates (HTML refs only — not downloaded)",
    "",
    `- Logo candidates: **${report.officialCandidates.imageLogo.length}**`,
    ...(report.officialCandidates.imageLogo.slice(0, 5).map((c) => `- ${c.url}`)),
    `- Hero/property candidates: **${report.officialCandidates.heroProperty.length}**`,
    ...(report.officialCandidates.heroProperty.slice(0, 6).map((c) => `- ${c.url}`)),
    "",
    "## 6. PR / recent-opening candidates",
    "",
    ...(report.prRecentOpeningCandidates.map(
      (p) =>
        `- ${p.url} — JS-shell: **${p.jsShellRisk || "unknown"}** · usability: ${p.assetUsability || "—"}`
    )),
    `- **Rendered Source Capture v1 needed:** ${report.renderedSourceCaptureNeeded ? "yes" : "no"}`,
    `- ${report.renderedSourceCaptureReason}`,
    "",
    "## 7. Recommended Tribute asset package",
    "",
    "| Priority | Type | Status | Action |",
    "|----------|------|--------|--------|",
    ...report.recommendedAssetPackage.map(
      (p) => `| ${p.priority} | ${p.assetType} | ${p.status} | ${p.action} |`
    ),
    "",
    "## 8. Safe to use now",
    "",
    ...report.safeToUseNow.map((s) => `- ${s}`),
    "",
    "## 9. Needs human / usage review",
    "",
    ...report.needsHumanReview.map((s) => `- ${s}`),
    "",
    "## 10. Requires future tooling",
    "",
    ...report.requiresFutureTooling.map((s) => `- ${s}`),
    "",
    "## 11. Visual parity gap (Kimpton / Radisson Blu)",
    "",
    `- Target: ${report.visualParityGap.kimptonRadissonTarget}`,
    `- Tribute now: ${report.visualParityGap.tributeCurrent}`,
    "",
    "**Remaining:**",
    ...report.visualParityGap.remainingWork.map((w) => `- ${w}`),
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
    "",
  ];
  return lines.join("\n");
}
