/**
 * Tribute Portfolio by Marriott — full Brand Intelligence Package pilot (dry-run default).
 *
 * First non-Choice full-package pilot. Produces a complete package PLAN:
 *   sources → PDFs → images/logo → PR/openings → extraction fields →
 *   Brand Setup completion plan → owner considerations → governance/trust chip.
 *
 * Reuses the existing factory (auto-resolver, active-brand inspection, URL probe,
 * local extraction) rather than one-off logic. Writes NOTHING to Airtable, does
 * not overwrite Brand Setup, does not approve facts, does not publish governance,
 * and never sets Company Validated / Company Validation Date.
 *
 * @see docs/data-intelligence/tribute-portfolio-brand-package-pilot.md
 */
import fs from "fs";
import path from "path";
import {
  assessExistingProfileCompleteness,
  assessExplorerActiveStatus,
  assessAssetSignals,
  PROFILE_COMPLETENESS,
} from "./active-brand-governance-upgrade.js";
import { SOURCE_ROLE } from "./brand-source-auto-resolver.js";
import {
  downloadUrlWithFallback,
  estimateReadableTextLength,
} from "./choice-legacy-batch-url-capture.js";
import { readLocalSourceText } from "./extract-source-text.js";
import { resolveReferenceRoot } from "./reference-material-paths.js";
import { listPartnerSources } from "./airtable-source.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { PARTNER_INTELLIGENCE_LINKS } from "../../api/lib/partner-intelligence-field-map.js";

export const PACKAGE_VERSION = "1";
export const REPORT_JSON_NAME = "tribute-portfolio-brand-package.json";
export const REPORT_MD_NAME = "tribute-portfolio-brand-package.md";

export const TRIBUTE_RECORD_ID = "recCvV0PuZOi8c3hC";
export const BRAND_NAME = "Tribute Portfolio";
export const PARENT_COMPANY = "Marriott International, Inc.";
export const COMPANY_FOLDER = "Marriott International";

/** Official Marriott web properties — used to mark sources company-controlled. */
export const COMPANY_DOMAINS = [
  "marriott.com",
  "tribute-portfolio.marriott.com",
  "development.marriott.com",
  "news.marriott.com",
  "marriottdevelopment.com",
];

/**
 * Official URL candidates by role. Confidence reflects live-probe reachability;
 * JS-shell risk is scored at runtime from readable-text length.
 */
export const URL_CANDIDATES = [
  {
    slot: "consumer_page",
    role: SOURCE_ROLE.CONSUMER_PAGE,
    url: "https://tribute-portfolio.marriott.com/",
    label: "Official Tribute Portfolio consumer brand site",
    priority: 1,
  },
  {
    slot: "bonvoy_page",
    role: SOURCE_ROLE.CONSUMER_PAGE,
    url: "https://www.marriott.com/loyalty.mi",
    label: "Marriott Bonvoy loyalty page (guest promise / Bonvoy relationship)",
    priority: 6,
  },
  {
    slot: "development_page",
    role: SOURCE_ROLE.DEVELOPMENT_PAGE,
    url: "https://development.marriott.com/our-brands/",
    label: "Marriott development brands hub (owner/developer)",
    priority: 2,
  },
  {
    slot: "press_hub",
    role: SOURCE_ROLE.PR_OPENING,
    url: "https://news.marriott.com/brands/tribute-portfolio",
    label: "Marriott newsroom — Tribute Portfolio (PR / openings)",
    priority: 4,
  },
];

/**
 * Local reference material candidates (relative to Brand Reference Material root).
 * Tribute-specific docs are primary; company-context captures support development.
 */
export const LOCAL_CANDIDATES = [
  {
    relativePath: "Marriott International/fdd/Tribute Portfolio/2026-tribute-portfolio-fdd-3-31-2026.pdf",
    role: SOURCE_ROLE.LOCAL_PDF,
    tributeSpecific: true,
    priority: 7,
    note: "2026 Tribute FDD — parent, development model, franchise fees, Item 19 (secondary factual).",
  },
  {
    relativePath: "Marriott International/brands/Tribute Portfolio/tribute portfolio brand page.html",
    role: SOURCE_ROLE.CONSUMER_PAGE,
    tributeSpecific: true,
    priority: 3,
    note: "Captured Marriott premium-brands page (mentions Tribute) — optional consumer context.",
  },
  {
    relativePath: "Marriott International/development/Brand portfolio.html",
    role: SOURCE_ROLE.DEVELOPMENT_PAGE,
    tributeSpecific: false,
    priority: 2,
    note: "Marriott brand-portfolio development capture — company-controlled development context.",
  },
  {
    relativePath: "Marriott International/development/Marriott development home.html",
    role: SOURCE_ROLE.DEVELOPMENT_PAGE,
    tributeSpecific: false,
    priority: 2,
    note: "Marriott development home capture — owner/developer provenance.",
  },
];

/**
 * Target extraction fields (validated registry keys). aiDraftable flags fields
 * that can be AI-drafted from company materials but must stay Pending until
 * human review; sourceBacked flags fields expected to be directly source-stated.
 */
export const TARGET_EXTRACTION_FIELDS = [
  { key: "be.identity.brandName", role: "any", sourceBacked: true, aiDraftable: false, note: "Directly stated." },
  { key: "be.identity.parentCompany", role: "local_pdf", sourceBacked: true, aiDraftable: false, note: "FDD / brand page." },
  { key: "be.positioning.summary", role: "consumer_page", sourceBacked: true, aiDraftable: true, note: "Consumer brand page + FDD framing." },
  { key: "be.positioning.tagline", role: "consumer_page", sourceBacked: true, aiDraftable: false, note: "Marriott brand tagline." },
  { key: "be.positioning.guestPromise", role: "consumer_page", sourceBacked: true, aiDraftable: true, note: "Consumer + Bonvoy page." },
  { key: "be.positioning.history", role: "local_pdf", sourceBacked: true, aiDraftable: true, note: "Launch year / history from FDD." },
  { key: "be.overview.developmentModel", role: "local_pdf", sourceBacked: true, aiDraftable: true, note: "FDD franchise/conversion model." },
  { key: "be.overview.whyValue", role: "development_page", sourceBacked: false, aiDraftable: true, note: "Owner value proposition — AI-draft, human review." },
  { key: "be.overview.typicalUseCase", role: "consumer_page", sourceBacked: false, aiDraftable: true, note: "Conversion / adaptive-reuse fit — AI-draft." },
  { key: "be.loyalty.programName", role: "consumer_page", sourceBacked: true, aiDraftable: false, note: "Marriott Bonvoy." },
  { key: "be.footprint.geoIntro", role: "consumer_page", sourceBacked: false, aiDraftable: true, note: "Regional relevance — AI-draft, verify." },
];

const READABLE_EXT = new Set([".pdf", ".html", ".htm", ".txt", ".md"]);

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

/* ------------------------------------------------------------------ */
/* Brand record                                                        */
/* ------------------------------------------------------------------ */

export async function getTributeBrandRecord() {
  const Airtable = (await import("airtable")).default;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const base = new Airtable({ apiKey }).base(baseId);
  try {
    const rec = await base(PARTNER_INTELLIGENCE_LINKS.brandBasics).find(TRIBUTE_RECORD_ID);
    const fields = rec.fields || {};
    const name = nz(fields["Brand Name"] || fields.brand_name);
    return {
      resolved: true,
      recordId: rec.id,
      name,
      nameMatch: /tribute/i.test(name),
      fields,
    };
  } catch (err) {
    return { resolved: false, recordId: TRIBUTE_RECORD_ID, error: err.message || String(err) };
  }
}

/* ------------------------------------------------------------------ */
/* Local file scan                                                     */
/* ------------------------------------------------------------------ */

export function scanLocalTributeFiles(referenceRoot = resolveReferenceRoot()) {
  const companyDir = path.join(referenceRoot, COMPANY_FOLDER);
  const found = [];
  const rootExists = fs.existsSync(companyDir);

  if (rootExists) {
    walk(companyDir, []);
  }

  function walk(abs, relParts) {
    for (const name of fs.readdirSync(abs)) {
      if (name.startsWith(".")) continue;
      const childAbs = path.join(abs, name);
      const rel = [...relParts, name].join("/");
      const st = fs.statSync(childAbs);
      if (st.isDirectory()) {
        // Restrict depth; always descend Tribute + development + press + brands + fdd.
        if (relParts.length < 4) walk(childAbs, [...relParts, name]);
        continue;
      }
      const ext = path.extname(name).toLowerCase();
      const relFull = `${COMPANY_FOLDER}/${rel}`;
      // \btribute\b avoids false positives like "dis-tribut-ion".
      const tributeSpecific = /\btribute\b/i.test(rel);
      const isDevContext = /^development\//i.test(rel);
      const isPress = /^press\//i.test(rel);
      if (!tributeSpecific && !isDevContext && !isPress) continue;

      let textLength = 0;
      let readable = false;
      if (READABLE_EXT.has(ext)) {
        try {
          const doc = readLocalSourceText(relFull);
          textLength = String(doc.text || "").length;
          readable = textLength > 0;
        } catch {
          readable = false;
        }
      }
      found.push({
        relativePath: relFull,
        filename: name,
        ext,
        sizeBytes: st.size,
        textLength,
        readable,
        tributeSpecific,
        classification: tributeSpecific
          ? "tribute_specific"
          : isDevContext
            ? "marriott_development_context"
            : "marriott_press_context",
        role: classifyLocalRole(name, ext, rel),
        isImage: /\.(png|jpe?g|gif|webp|svg|avif)$/i.test(name),
        isPdf: ext === ".pdf",
      });
    }
  }

  return {
    referenceRoot,
    companyDir,
    exists: rootExists,
    found,
    tributeSpecificFiles: found.filter((f) => f.tributeSpecific),
    pdfs: found.filter((f) => f.isPdf),
    images: found.filter((f) => f.isImage),
  };
}

function classifyLocalRole(name, ext, rel) {
  if (/\.(png|jpe?g|gif|webp|svg|avif)$/i.test(name)) return SOURCE_ROLE.IMAGE_ASSET;
  if (/fdd/i.test(rel) && ext === ".pdf") return SOURCE_ROLE.LOCAL_PDF;
  if (/^development\//i.test(rel)) return SOURCE_ROLE.DEVELOPMENT_PAGE;
  if (/^press\//i.test(rel)) return SOURCE_ROLE.PR_OPENING;
  if (/brand page|premium|portfolio/i.test(name)) return SOURCE_ROLE.CONSUMER_PAGE;
  if (ext === ".pdf") return SOURCE_ROLE.LOCAL_PDF;
  return SOURCE_ROLE.OTHER;
}

/* ------------------------------------------------------------------ */
/* URL probes                                                          */
/* ------------------------------------------------------------------ */

export async function probeTributeUrl(candidate) {
  const { url, slot, role } = candidate;
  try {
    const dl = await downloadUrlWithFallback(url);
    const ext = dl.ext || (/\.pdf/i.test(url) ? ".pdf" : ".html");
    const readableTextLength = estimateReadableTextLength(dl.buf, dl.contentType, ext);
    const reachable = dl.httpStatus >= 200 && dl.httpStatus < 400;
    // JS-shell risk: reachable but almost no extractable text.
    const jsShellRisk = reachable && readableTextLength < 400 ? "high" : reachable && readableTextLength < 1200 ? "medium" : "low";
    return {
      slot,
      role,
      url,
      label: candidate.label,
      priority: candidate.priority,
      status: reachable ? "reachable" : "failed",
      httpStatus: dl.httpStatus,
      contentType: dl.contentType || null,
      bytes: dl.buf?.length ?? 0,
      readableTextLength,
      finalUrl: dl.finalUrl || url,
      companyControlled: true,
      jsShellRisk,
      extractionEligible: reachable && readableTextLength >= 400,
      registerRecommended: reachable && readableTextLength >= 400,
      error: null,
    };
  } catch (err) {
    return {
      slot,
      role,
      url,
      label: candidate.label,
      priority: candidate.priority,
      status: "failed",
      httpStatus: null,
      bytes: 0,
      readableTextLength: 0,
      companyControlled: true,
      jsShellRisk: "unreachable",
      extractionEligible: false,
      registerRecommended: false,
      error: err.message || String(err),
    };
  }
}

/* ------------------------------------------------------------------ */
/* Proposed source package                                             */
/* ------------------------------------------------------------------ */

export function buildProposedSourcePackage({ localScan, urlProbes }) {
  const proposed = [];

  for (const probe of urlProbes) {
    proposed.push({
      origin: "web",
      role: probe.role,
      title: `${BRAND_NAME} — ${probe.label}`,
      sourceUrl: probe.url,
      localFilePath: null,
      companyControlled: probe.companyControlled,
      reachable: probe.status === "reachable",
      readableTextLength: probe.readableTextLength,
      jsShellRisk: probe.jsShellRisk,
      extractionEligible: probe.extractionEligible,
      registrationRecommendation: probe.registerRecommended
        ? "register_website_capture"
        : probe.jsShellRisk === "high"
          ? "provenance_only_js_shell"
          : "hold_unreachable",
      priority: probe.priority,
    });
  }

  for (const cand of LOCAL_CANDIDATES) {
    const hit = (localScan.found || []).find((f) => f.relativePath === cand.relativePath);
    proposed.push({
      origin: "local",
      role: cand.role,
      title: `${BRAND_NAME} — ${cand.note}`,
      sourceUrl: null,
      localFilePath: cand.relativePath,
      companyControlled: true,
      onDisk: Boolean(hit),
      readable: hit?.readable ?? false,
      textLength: hit?.textLength ?? 0,
      tributeSpecific: cand.tributeSpecific,
      extractionEligible: Boolean(hit?.readable),
      registrationRecommendation: hit?.readable
        ? "register_local_document"
        : hit
          ? "hold_unreadable"
          : "missing_on_disk",
      priority: cand.priority,
      note: cand.note,
    });
  }

  const registerable = proposed.filter(
    (p) =>
      p.registrationRecommendation === "register_website_capture" ||
      p.registrationRecommendation === "register_local_document"
  );

  const byRole = {};
  for (const p of registerable) (byRole[p.role] ||= []).push(p.title);

  return {
    proposed,
    registerableCount: registerable.length,
    hasConsumer: registerable.some((p) => p.role === SOURCE_ROLE.CONSUMER_PAGE),
    hasLocalPdf: registerable.some((p) => p.role === SOURCE_ROLE.LOCAL_PDF),
    hasDevelopment: registerable.some((p) => p.role === SOURCE_ROLE.DEVELOPMENT_PAGE),
    hasPress: registerable.some((p) => p.role === SOURCE_ROLE.PR_OPENING),
    allCompanyControlled: registerable.every((p) => p.companyControlled),
    byRole,
  };
}

/* ------------------------------------------------------------------ */
/* Brand Setup completion plan                                         */
/* ------------------------------------------------------------------ */

const BRAND_SETUP_FIELD_MAP = [
  { field: "Brand Name", factKey: "be.identity.brandName", category: "identity" },
  { field: "Parent Company", factKey: "be.identity.parentCompany", category: "identity" },
  { field: "Brand Positioning", factKey: "be.positioning.summary", category: "positioning" },
  { field: "Brand Tagline", factKey: "be.positioning.tagline", category: "positioning" },
  { field: "Brand Customer Promise", factKey: "be.positioning.guestPromise", category: "positioning" },
  { field: "Brand History", factKey: "be.positioning.history", category: "positioning" },
  { field: "Brand Architecture", factKey: null, category: "identity" },
  { field: "Brand Model", factKey: "be.overview.developmentModel", category: "development" },
  { field: "Brand Value Proposition", factKey: "be.overview.whyValue", category: "owner" },
  { field: "Key Brand Differentiators", factKey: "be.overview.whyValue", category: "owner" },
  { field: "Hotel Chain Scale", factKey: null, category: "segment" },
  { field: "Hotel Service Model", factKey: null, category: "segment" },
  { field: "Target Guest Segments", factKey: "be.overview.typicalUseCase", category: "guest" },
  { field: "Guest Psychographics Description", factKey: "be.overview.typicalUseCase", category: "guest" },
  { field: "Region Offered", factKey: "be.footprint.geoIntro", category: "footprint" },
  { field: "Brand Pillars", factKey: "be.positioning.summary", category: "positioning" },
  { field: "Sustainability Positioning", factKey: null, category: "esg" },
];

/** Fields to AI-draft (Pending human review) that rarely exist in Brand Setup. */
const AI_DRAFTABLE_ADDITIONS = [
  { label: "Owner / developer value proposition", factKey: "be.overview.whyValue" },
  { label: "Conversion / adaptive-reuse fit", factKey: "be.overview.typicalUseCase" },
  { label: "Owner considerations narrative", factKey: "be.overview.whyValue" },
  { label: "Questions owners should ask", factKey: "be.overview.scenarios" },
  { label: "Marriott Bonvoy relationship summary", factKey: "be.loyalty.programName" },
];

const KEEP_BLANK_FIELDS = [
  "Company Validated (do not set)",
  "Company Validation Date (do not set)",
  "Any field implying Marriott reviewed/endorsed the profile",
];

export function buildFieldCompletionPlan(fields) {
  const alreadyPopulated = [];
  const missingSourceSupported = [];

  for (const row of BRAND_SETUP_FIELD_MAP) {
    if (fieldPopulated(fields, row.field)) {
      alreadyPopulated.push({
        field: row.field,
        factKey: row.factKey,
        status: "populated_needs_source_backing",
        note: "Existing content preserved; back with source-derived fact (do not overwrite).",
      });
    } else {
      missingSourceSupported.push({
        field: row.field,
        factKey: row.factKey,
        status: "missing_source_supported",
        note: "Absent in Brand Setup; source-supported extraction can populate (staged review).",
      });
    }
  }

  const aiDraftable = AI_DRAFTABLE_ADDITIONS.map((a) => ({
    ...a,
    status: "ai_draftable_pending_review",
    note: "AI-draft from company materials; remains Pending until human review.",
  }));

  const humanReview = [
    { field: "Economics / franchise fees (royalty, initial fee)", factKey: "be.economics.royaltyPct", note: "From FDD Item 5-7 — human review before display." },
    { field: "Footprint counts (hotels / rooms / pipeline)", factKey: "be.footprint.globalHotels", note: "Verify against latest Marriott disclosure; numbers age quickly." },
    { field: "Item 19 financial performance", factKey: null, note: "FDD Item 19 — sensitive; human review, likely internal-only." },
  ];

  const sourceGaps = [
    { area: "Recent openings / PR links", reason: "news.marriott.com is a JS shell (near-zero readable text); needs manual capture or rendered snapshot." },
    { area: "Official development brand page", reason: "development.marriott.com candidates unreachable; rely on local development captures + FDD." },
    { area: "Property / design imagery + verified hero", reason: "Explorer hero is Mock/Demo; no approved brand image-capture workflow in repo." },
  ];

  return {
    alreadyPopulated,
    missingSourceSupported,
    aiDraftable,
    humanReview,
    keepBlank: KEEP_BLANK_FIELDS,
    sourceGaps,
    summary: {
      populated: alreadyPopulated.length,
      missing: missingSourceSupported.length,
      aiDraftable: aiDraftable.length,
      humanReview: humanReview.length,
      sourceGaps: sourceGaps.length,
    },
  };
}

/* ------------------------------------------------------------------ */
/* Asset package plan                                                  */
/* ------------------------------------------------------------------ */

export function buildAssetPackagePlan(fields, localScan) {
  const assetSignals = assessAssetSignals(fields);
  const localImages = localScan.images || [];

  return {
    logoCandidate: assetSignals.logo === "present"
      ? { status: "present_in_brand_setup", source: "Brand Setup - Brand Basics: Logo", action: "reuse_existing" }
      : { status: "missing", action: "capture_official_logo_future" },
    heroImageCandidate: {
      status: nz(fields["Explorer Hero Data Source"]).toLowerCase().includes("mock")
        ? "mock_demo_not_verified"
        : assetSignals.heroImage,
      action: "capture_verified_hero_future_asset_module",
    },
    propertyImages: localImages.length
      ? { status: "local_images_found", count: localImages.length, files: localImages.map((i) => i.relativePath) }
      : { status: "none_local", action: "recommend_future_capture" },
    imageSourceUrls: [
      "https://tribute-portfolio.marriott.com/ (hero/property imagery — rendered capture required)",
    ],
    usageStatus: "unverified_do_not_download_without_rights_review",
    assetGovernanceSchema: {
      exists: false,
      note: "No approved brand image/asset-capture + governance workflow in repo (asset-governance is a future module per active-brand-governance-upgrade v1).",
      recommendedNextStep:
        "Build asset-governance module (logo/hero/property image capture + rights + trust labeling) before downloading Marriott imagery.",
    },
    recommendedAssetsToCapture: [
      "Verified brand logo (confirm vs existing Brand Setup logo)",
      "Verified hero image (replace Mock/Demo hero)",
      "3-6 property / design / public-space images with rights status",
    ],
  };
}

/* ------------------------------------------------------------------ */
/* Governance recommendation                                           */
/* ------------------------------------------------------------------ */

export function buildGovernanceRecommendation(sourcePackage) {
  const strongCompanyMaterials =
    sourcePackage.allCompanyControlled &&
    (sourcePackage.hasLocalPdf || sourcePackage.hasConsumer) &&
    sourcePackage.registerableCount >= 2;

  const posture = strongCompanyMaterials ? "company_materials" : "source_informed_fallback";

  return {
    recommendedPosture: posture,
    expectedGovernance:
      posture === "company_materials"
        ? {
            validationStatus: "Company Published",
            usagePermission: "Platform Display Allowed",
            externalDisplayStatus: "Show Trust Label",
            externalChip: "AI-Assisted Profile",
            sourceBasis: "Company Materials",
            companyValidated: "false / unchanged",
            companyValidationDate: "unchanged",
          }
        : {
            validationStatus: "Source-Informed (fallback — do not publish yet)",
            usagePermission: "Internal review",
            externalDisplayStatus: "Do not show trust label until sources strengthened",
            externalChip: "Source-Informed",
            sourceBasis: "Mixed / Public Web",
            companyValidated: "false / unchanged",
            companyValidationDate: "unchanged",
          },
    rationale: strongCompanyMaterials
      ? "All registerable sources are Marriott-controlled (consumer site + Tribute FDD + development captures); qualifies for Company Materials basis and AI-Assisted Profile chip."
      : "Registerable company-controlled sources are thin (consumer page JS-heavy, dev page unreachable); hold at Source-Informed until at least one strong company PDF/page + one official web page are registered and approved.",
    doNotPublishInDryRun: true,
  };
}

/* ------------------------------------------------------------------ */
/* Report                                                              */
/* ------------------------------------------------------------------ */

export async function buildTributePortfolioBrandPackageReport({ probeUrls = true } = {}) {
  const record = await getTributeBrandRecord();
  const fields = record.fields || {};

  const explorer = assessExplorerActiveStatus(fields);
  const completeness = assessExistingProfileCompleteness(fields, explorer.active);

  const localScan = scanLocalTributeFiles();

  const [sourcesPage, factsPage] = await Promise.all([
    listPartnerSources({ brandId: TRIBUTE_RECORD_ID, limit: 100 }).catch(() => ({ sources: [] })),
    listPartnerFacts({ brandId: TRIBUTE_RECORD_ID, limit: 100 }).catch(() => ({ facts: [] })),
  ]);
  const existingSources = sourcesPage.sources || [];
  const existingFacts = factsPage.facts || [];

  let urlProbes = [];
  if (probeUrls) {
    for (const cand of URL_CANDIDATES) {
      urlProbes.push(await probeTributeUrl(cand));
    }
  } else {
    urlProbes = URL_CANDIDATES.map((c) => ({
      slot: c.slot,
      role: c.role,
      url: c.url,
      label: c.label,
      priority: c.priority,
      status: "not_probed",
      readableTextLength: null,
      jsShellRisk: "unknown",
      companyControlled: true,
      extractionEligible: false,
      registerRecommended: false,
    }));
  }

  const sourcePackage = buildProposedSourcePackage({ localScan, urlProbes });
  const completionPlan = buildFieldCompletionPlan(fields);
  const assetPlan = buildAssetPackagePlan(fields, localScan);
  const governance = buildGovernanceRecommendation(sourcePackage);

  const readyForPipeline =
    sourcePackage.registerableCount >= 2 &&
    (sourcePackage.hasLocalPdf || sourcePackage.hasConsumer) &&
    sourcePackage.allCompanyControlled;

  const duplicateChecks = {
    existingSourceCount: existingSources.length,
    existingFactCount: existingFacts.length,
    duplicateRisk:
      existingSources.length === 0
        ? "none — no PI sources registered yet"
        : "check titles/URLs against proposed package before register",
  };

  return {
    packageVersion: PACKAGE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    brand: {
      recordId: TRIBUTE_RECORD_ID,
      resolved: record.resolved,
      name: record.name || BRAND_NAME,
      nameMatch: record.nameMatch ?? null,
      parentCompany: PARENT_COMPANY,
      resolutionError: record.error || null,
    },
    explorer: {
      active: explorer.active,
      brandStatus: explorer.brandStatus,
      heroDataSource: nz(fields["Explorer Hero Data Source"]) || null,
      heroVerification: nz(fields["Explorer Hero Verification"]) || null,
    },
    profileCompleteness: {
      category: completeness.category,
      score: completeness.score,
      signals: completeness.signals,
      note:
        completeness.category === PROFILE_COMPLETENESS.STRONG
          ? "Strong existing Brand Setup content, but hero is Mock/Demo and content is not source-backed/governed."
          : "See signals.",
    },
    partnerIntelligence: {
      existingSourceCount: existingSources.length,
      existingFactCount: existingFacts.length,
    },
    localFiles: {
      exists: localScan.exists,
      companyDir: localScan.companyDir,
      totalFound: localScan.found.length,
      tributeSpecific: localScan.tributeSpecificFiles,
      pdfs: localScan.pdfs.map((f) => ({ path: f.relativePath, sizeBytes: f.sizeBytes, textLength: f.textLength, readable: f.readable, tributeSpecific: f.tributeSpecific })),
      images: localScan.images.map((f) => f.relativePath),
      recommendedPrimary:
        localScan.tributeSpecificFiles.find((f) => f.isPdf && f.readable)?.relativePath || null,
    },
    urlProbes,
    proposedSourcePackage: sourcePackage,
    proposedExtractionFields: TARGET_EXTRACTION_FIELDS,
    brandSetupCompletionPlan: completionPlan,
    assetPackagePlan: assetPlan,
    governanceRecommendation: governance,
    duplicateChecks,
    readyForPipeline,
    readinessReason: readyForPipeline
      ? "≥2 approved-able Marriott-controlled sources (incl. consumer page and/or Tribute FDD)."
      : "Register + approve at least one strong company PDF/page and one official web page first.",
    nextCommands: buildNextCommands({ readyForPipeline }),
    risksAndCaveats: [
      "Existing Brand Setup content is demo/mock (hero = Mock Data); it must be source-backed via staged extraction, not overwritten.",
      "Marriott consumer + newsroom pages are JS-heavy; readable text is thin — prefer the local Tribute FDD for factual extraction.",
      "development.marriott.com candidates were unreachable at probe time — use local Marriott development captures for provenance.",
      "FDD Item 19 / fee data is sensitive — human review; likely internal-only, not public trust-label display.",
      "No approved image/asset-governance workflow exists — do not download Marriott imagery yet.",
      "Do not imply Marriott validated the profile; Company Validated / Company Validation Date must remain unchanged.",
    ],
    doesNotDo: [
      "Overwrite existing Brand Setup content fields (staged review only)",
      "Register or approve sources in dry-run",
      "Extract, approve facts, or publish governance",
      "Set Company Validated or Company Validation Date",
      "Download images or scrape third-party / OTA pages",
      "Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema",
    ],
  };
}

function buildNextCommands({ readyForPipeline }) {
  const download = (url, type, title) =>
    `npm run partner-reference:download -- --url "${url}" --company "${COMPANY_FOLDER}" --brand "${BRAND_NAME}" --type ${type} --title "${title}" --brand-id ${TRIBUTE_RECORD_ID} --dry-run`;
  return [
    "npm run tribute-portfolio-brand-package -- --dry-run",
    download("https://tribute-portfolio.marriott.com/", "website-capture", "Tribute Portfolio consumer brand page"),
    "Register 2026 Tribute FDD (local) via Source Library with Profile Type Brand, Source Origin FDD Library (dry-run first)",
    readyForPipeline
      ? "After sources registered + approved: run stewardship → extraction → fact stewardship → governance publish (all dry-run first)"
      : "Strengthen company-controlled sources before extraction/governance",
  ];
}

/* ------------------------------------------------------------------ */
/* Markdown                                                            */
/* ------------------------------------------------------------------ */

export function buildTributePortfolioBrandPackageMarkdown(report) {
  const g = report.governanceRecommendation.expectedGovernance;
  const lines = [
    "# Tribute Portfolio by Marriott — Full Brand Intelligence Package (Pilot)",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## 1. Brand record resolution",
    "",
    `| Field | Value |`,
    `|-------|-------|`,
    `| Record | \`${report.brand.recordId}\` |`,
    `| Name | ${report.brand.name} |`,
    `| Resolved | **${report.brand.resolved ? "yes" : "no"}** (name match: ${report.brand.nameMatch}) |`,
    `| Parent | ${report.brand.parentCompany} |`,
    "",
    "## 2. Existing Brand Explorer status",
    "",
    `- Explorer active: **${report.explorer.active ? "yes" : "no"}** (Brand Status: ${report.explorer.brandStatus})`,
    `- Hero data source: **${report.explorer.heroDataSource}** · verification: **${report.explorer.heroVerification}**`,
    `- Existing PI sources: **${report.partnerIntelligence.existingSourceCount}** · facts: **${report.partnerIntelligence.existingFactCount}**`,
    "",
    "## 3. Current profile completeness",
    "",
    `- **${report.profileCompleteness.category}** (score ${report.profileCompleteness.score})`,
    `- ${report.profileCompleteness.note}`,
    "",
    "## 4. Local files found",
    "",
    report.localFiles.exists ? `Company dir: \`${report.localFiles.companyDir}\`` : "_Reference root not found._",
    "",
    ...report.localFiles.pdfs.map(
      (p) => `- ${p.tributeSpecific ? "**[Tribute]** " : ""}\`${p.path}\` — ${p.sizeBytes} bytes · ${p.textLength} chars · readable ${p.readable}`
    ),
    `- Images found: ${report.localFiles.images.length}`,
    `- Recommended primary local doc: \`${report.localFiles.recommendedPrimary || "none"}\``,
    "",
    "## 5. Official web / source candidates",
    "",
    "| Slot | Role | HTTP | Readable | JS-shell | Register? |",
    "|------|------|------|----------|----------|-----------|",
    ...report.urlProbes.map(
      (u) => `| ${u.slot} | ${u.role} | ${u.httpStatus ?? u.status} | ${u.readableTextLength ?? "—"} | ${u.jsShellRisk} | ${u.registerRecommended ? "yes" : "no"} |`
    ),
    "",
    "## 6. Proposed source package",
    "",
    `- Registerable sources: **${report.proposedSourcePackage.registerableCount}** · all company-controlled: **${report.proposedSourcePackage.allCompanyControlled}**`,
    `- Consumer: ${report.proposedSourcePackage.hasConsumer} · Local PDF: ${report.proposedSourcePackage.hasLocalPdf} · Development: ${report.proposedSourcePackage.hasDevelopment} · Press: ${report.proposedSourcePackage.hasPress}`,
    "",
    ...report.proposedSourcePackage.proposed.map(
      (p) => `- [${p.origin}] ${p.role} → **${p.registrationRecommendation}** — ${p.sourceUrl || p.localFilePath}`
    ),
    "",
    "## 7. Proposed extraction fields",
    "",
    ...report.proposedExtractionFields.map(
      (f) => `- \`${f.key}\` (${f.role}) — ${f.sourceBacked ? "source-backed" : "AI-draft"}${f.aiDraftable ? " · AI-draftable" : ""}: ${f.note}`
    ),
    "",
    "## 8. Brand Setup completion plan",
    "",
    `**Already populated (preserve, source-back):** ${report.brandSetupCompletionPlan.summary.populated}`,
    ...report.brandSetupCompletionPlan.alreadyPopulated.map((r) => `- ${r.field} → \`${r.factKey || "n/a"}\``),
    "",
    `**Missing but source-supported:** ${report.brandSetupCompletionPlan.summary.missing}`,
    ...report.brandSetupCompletionPlan.missingSourceSupported.map((r) => `- ${r.field} → \`${r.factKey || "n/a"}\``),
    "",
    `**AI-draftable (Pending review):** ${report.brandSetupCompletionPlan.summary.aiDraftable}`,
    ...report.brandSetupCompletionPlan.aiDraftable.map((r) => `- ${r.label} → \`${r.factKey}\``),
    "",
    `**Human review required:**`,
    ...report.brandSetupCompletionPlan.humanReview.map((r) => `- ${r.field} — ${r.note}`),
    "",
    `**Keep blank:**`,
    ...report.brandSetupCompletionPlan.keepBlank.map((r) => `- ${r}`),
    "",
    `**Source gaps:**`,
    ...report.brandSetupCompletionPlan.sourceGaps.map((r) => `- ${r.area}: ${r.reason}`),
    "",
    "## 9. Asset package plan",
    "",
    `- Logo: ${report.assetPackagePlan.logoCandidate.status} (${report.assetPackagePlan.logoCandidate.action})`,
    `- Hero: ${report.assetPackagePlan.heroImageCandidate.status} (${report.assetPackagePlan.heroImageCandidate.action})`,
    `- Property images: ${report.assetPackagePlan.propertyImages.status}`,
    `- Asset governance schema exists: **${report.assetPackagePlan.assetGovernanceSchema.exists}** — ${report.assetPackagePlan.assetGovernanceSchema.recommendedNextStep}`,
    "",
    "## 10. Governance recommendation",
    "",
    `- Recommended posture: **${report.governanceRecommendation.recommendedPosture}**`,
    `- Validation Status: ${g.validationStatus}`,
    `- Usage Permission: ${g.usagePermission}`,
    `- External Display Status: ${g.externalDisplayStatus}`,
    `- External chip: ${g.externalChip} · Source Basis: ${g.sourceBasis}`,
    `- Company Validated: ${g.companyValidated} · Company Validation Date: ${g.companyValidationDate}`,
    `- Rationale: ${report.governanceRecommendation.rationale}`,
    "",
    "## 11. Ready for pipeline?",
    "",
    `- **${report.readyForPipeline ? "Yes" : "Not yet"}** — ${report.readinessReason}`,
    "",
    "## 12. Risks & caveats",
    "",
    ...report.risksAndCaveats.map((r) => `- ${r}`),
    "",
    "## 13. Next commands",
    "",
    ...report.nextCommands.map((c) => `- \`${c}\``),
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
    "",
  ];
  return lines.join("\n");
}
