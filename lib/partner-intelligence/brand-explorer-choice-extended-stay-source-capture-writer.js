/**
 * Brand Explorer Choice Extended-Stay Source Capture Writer v32B.
 *
 * Creates/repairs Partner Intelligence Source Library records only for Everhome,
 * WoodSpring, and Suburban Studios. No presentation, image, or Company Validated changes.
 *
 * @see docs/data-intelligence/brand-explorer-choice-extended-stay-source-capture-writer-v32B.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  createPartnerSource,
  listPartnerSources,
  patchPartnerSource,
} from "./airtable-source.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  classifyMomentumSourceType,
  followsTributeMomentumRules,
  isMomentumInappropriatePropertyListing,
  momentumEvidenceSourceRank,
} from "./brand-explorer-momentum-link-label.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";

export const WRITER_VERSION = "v32B";
export const REPORT_JSON_NAME = "brand-explorer-choice-extended-stay-source-capture-writer.json";
export const REPORT_MD_NAME = "brand-explorer-choice-extended-stay-source-capture-writer.md";
export const DOC_MD_NAME = "brand-explorer-choice-extended-stay-source-capture-writer-v32B.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v32B-choice-extended-stay-source-capture";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_SOURCE_ONLY = "--confirm-source-library-only";
export const APPLY_FLAG_NO_PRESENTATION = "--confirm-no-presentation-or-image-changes";

export const BATCH_BRANDS = Object.freeze([
  {
    slug: "everhome-suites",
    recordId: "recqkkrsevi4r9ibj",
    name: "Everhome Suites",
  },
  {
    slug: "woodspring-suites",
    recordId: "recsOd51NzRPYsMko",
    name: "WoodSpring Suites",
    discoveryConfigRecordIdPatchRecommended: true,
  },
  {
    slug: "suburban-studios",
    recordId: "reclcjg5Foa9Vs5TC",
    name: "Suburban Studios",
  },
]);

const ALLOWED_SOURCE_TYPES = [
  "FDD",
  "Development Brochure",
  "Development Page",
  "Brand Page",
  "Press Release",
  "Website Capture",
  "Other",
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-choice-extended-stay-batch-readiness-audit.json",
  "reports/brand-explorer-radisson-individuals-final-presentation-quality-sync-writer.json",
  "reports/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.json",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.json",
  "docs/brand-explorer-presentation-slots.md",
  "lib/partner-intelligence/airtable-source.js",
  "api/lib/partner-intelligence-field-map.js",
  "fixtures/choice-dev-site-routes.json",
  "fixtures/choice-media-center-text/manifest.json",
  "live Source Library / Facts / Presentation for batch brands",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-choice-extended-stay-source-capture-writer.js",
  "scripts/brand-explorer-choice-extended-stay-source-capture-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const BLOCKED_URL_RES = [
  /v5\.airtableusercontent\.com/i,
  /airtableusercontent\.com/i,
  /booking\.com/i,
  /expedia\.com/i,
  /localhost/i,
];

const FDD_INTERNAL_RES = [
  /\bfdd\b/i,
  /\bitem\s*19\b/i,
  /\bfranchise disclosure\b/i,
  /choice-fdd-text/i,
];

/** Curated durable source catalog per brand (verified fixtures / v32A momentum URLs). */
export const SOURCE_CAPTURE_CATALOG = Object.freeze({
  "everhome-suites": [
    {
      role: "p0_consumer_brand_page",
      sourceTitle: "Everhome Suites — Choice consumer brand page",
      sourceUrl: "https://www.choicehotels.com/everhome-suites",
      sourceType: "Brand Page",
      evidenceUseCases: ["Portfolio Context", "Standards", "Loyalty"],
      region: "Americas",
    },
    {
      role: "p0_development_page",
      sourceTitle: "Everhome Suites — Choice development page",
      sourceUrl:
        "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/everhome-suites",
      sourceType: "Development Page",
      evidenceUseCases: ["Portfolio Context", "Standards", "Demand Scenario"],
      region: "Americas",
    },
    {
      role: "p0_press_kit",
      sourceTitle: "Everhome Suites — Choice media center press kit",
      sourceUrl: "https://media.choicehotels.com/everhome-suites",
      sourceType: "Press Release",
      evidenceUseCases: ["Momentum", "Portfolio Context"],
      region: "Americas",
    },
    {
      role: "p1_momentum_redesigned_prototype",
      sourceTitle: "Choice introduces redesigned Everhome Suites prototype (Feb 2026)",
      sourceUrl:
        "https://media.choicehotels.com/2026-02-04-Choice-Hotels-International-Introduces-Redesigned-Everhome-Suites-Prototype,-Advancing-Smarter-Extended-Stay-Development",
      sourceType: "Press Release",
      evidenceUseCases: ["Momentum"],
      region: "Americas",
    },
    {
      role: "p1_momentum_30th_opening",
      sourceTitle: "Choice — 30th Everhome Suites opening (Jun 2026)",
      sourceUrl:
        "https://media.choicehotels.com/2026-06-01-Choice-Hotels-International-Strengthens-Extended-Stay-Leadership-with-30th-Everhome-Suites-Opening",
      sourceType: "Press Release",
      evidenceUseCases: ["Momentum"],
      region: "Americas",
    },
    {
      role: "p1_momentum_25_milestone",
      sourceTitle: "Everhome crosses 25-property milestone (Feb 2026)",
      sourceUrl:
        "https://media.choicehotels.com/2026-02-02-Everhome-Suites-Expands-Footprint-with-Openings-in-Texas-and-Kentucky-and-New-Jersey,-Crossing-the-25th-Property-Milestone",
      sourceType: "Press Release",
      evidenceUseCases: ["Momentum"],
      region: "Americas",
    },
    {
      role: "p1_momentum_brand_intro",
      sourceTitle: "Choice introduces Everhome Suites (Jan 2020)",
      sourceUrl:
        "https://media.choicehotels.com/2020-01-27-Choice-Hotels-Introduces-Everhome-Suites-To-Help-Developers-Build-A-Strong-Portfolio-And-Empower-Guests-Success-On-The-Road",
      sourceType: "Press Release",
      evidenceUseCases: ["Momentum", "Portfolio Context"],
      region: "Americas",
    },
  ],
  "woodspring-suites": [
    {
      role: "p0_consumer_brand_site",
      sourceTitle: "WoodSpring Suites — official brand website",
      sourceUrl: "https://www.woodspring.com/",
      sourceType: "Brand Page",
      evidenceUseCases: ["Portfolio Context", "Standards", "Loyalty"],
      region: "Americas",
    },
    {
      role: "p0_choice_brand_directory",
      sourceTitle: "WoodSpring Suites — Choice Hotels brand directory",
      sourceUrl: "https://www.choicehotels.com/woodspring-hotels",
      sourceType: "Brand Page",
      evidenceUseCases: ["Openings", "Portfolio Context"],
      region: "Americas",
    },
    {
      role: "p0_development_page",
      sourceTitle: "WoodSpring Suites — Choice development page",
      sourceUrl:
        "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/woodspring-suites",
      sourceType: "Development Page",
      evidenceUseCases: ["Portfolio Context", "Standards", "Demand Scenario"],
      region: "Americas",
    },
    {
      role: "p0_press_kit",
      sourceTitle: "WoodSpring Suites — Choice media center press kit",
      sourceUrl: "https://media.choicehotels.com/woodspring-suites-press-kit",
      sourceType: "Press Release",
      evidenceUseCases: ["Momentum", "Portfolio Context"],
      region: "Americas",
    },
    {
      role: "p1_extended_stay_positioning",
      sourceTitle: "Choice extended-stay portfolio context (development hub)",
      sourceUrl: "https://www.choicehotelsdevelopment.com/our-brands/extended-stay",
      sourceType: "Development Page",
      evidenceUseCases: ["Portfolio Context", "Momentum"],
      region: "Americas",
    },
  ],
  "suburban-studios": [
    {
      role: "p0_consumer_brand_page",
      sourceTitle: "Suburban Studios — Choice consumer brand page",
      sourceUrl: "https://www.choicehotels.com/suburban-studios",
      sourceType: "Brand Page",
      evidenceUseCases: ["Portfolio Context", "Standards", "Loyalty"],
      region: "Americas",
    },
    {
      role: "p0_development_page",
      sourceTitle: "Suburban Studios — Choice development page",
      sourceUrl:
        "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/suburban-studios",
      sourceType: "Development Page",
      evidenceUseCases: ["Portfolio Context", "Standards", "Demand Scenario"],
      region: "Americas",
    },
    {
      role: "p0_press_kit",
      sourceTitle: "Suburban Studios — Choice media center press kit",
      sourceUrl: "https://media.choicehotels.com/suburban-studios-press-kit",
      sourceType: "Press Release",
      evidenceUseCases: ["Momentum", "Portfolio Context"],
      region: "Americas",
    },
    {
      role: "p1_extended_stay_positioning",
      sourceTitle: "Choice extended-stay portfolio context (development hub)",
      sourceUrl: "https://www.choicehotelsdevelopment.com/our-brands/extended-stay",
      sourceType: "Development Page",
      evidenceUseCases: ["Portfolio Context", "Momentum"],
      region: "Americas",
    },
  ],
});

const INTERNAL_FDD_REFERENCE_NOTES = Object.freeze({
  "everhome-suites":
    "fixtures/choice-fdd-text/ — FDD Item 19 internal research only; do not surface in Explorer copy.",
  "woodspring-suites":
    "fixtures/choice-fdd-text/35798-202604-03.txt — FDD internal research only; not owner-facing.",
  "suburban-studios":
    "fixtures/choice-fdd-text/35786-202604-09.txt — FDD internal research only; not owner-facing.",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\/+$/, "").split("?")[0];
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v32bWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-choice-extended-stay-source-capture-writer.js")
  );
}

export function isBlockedSourceUrl(url) {
  const u = nz(url);
  if (!u) return { blocked: true, reason: "empty_url" };
  if (isTemporaryAirtableUrl(u)) return { blocked: true, reason: "temporary_airtable_url" };
  for (const re of BLOCKED_URL_RES) {
    if (re.test(u)) return { blocked: true, reason: re.source };
  }
  return { blocked: false, reason: null };
}

export function classifySourceUrl(url, title = "") {
  const u = nz(url).toLowerCase();
  const combined = `${u} ${nz(title).toLowerCase()}`;
  if (FDD_INTERNAL_RES.some((re) => re.test(combined))) {
    return {
      category: "fdd",
      internalOnly: true,
      momentumAppropriate: false,
      openingsAppropriate: false,
      classification: "internal_research_only",
    };
  }
  if (/choicehotelsdevelopment\.com/.test(u)) {
    return {
      category: "official_development",
      internalOnly: false,
      momentumAppropriate: true,
      openingsAppropriate: false,
      classification: "official",
    };
  }
  if (/media\.choicehotels\.com/.test(u)) {
    const propertyListing = isMomentumInappropriatePropertyListing(url);
    return {
      category: propertyListing ? "property_listing" : "press_newsroom",
      internalOnly: false,
      momentumAppropriate: !propertyListing && momentumEvidenceSourceRank(url) >= 60,
      openingsAppropriate: propertyListing,
      classification: propertyListing ? "property" : "official",
    };
  }
  if (/woodspring\.com|choicehotels\.com/.test(u)) {
    const isProperty =
      /\/[a-z]{2}\/[^/]+\/[^/]+-(hotels|suites)/i.test(u) &&
      !/\/(everhome-suites|suburban-studios|woodspring-hotels)$/i.test(u);
    return {
      category: isProperty ? "property_listing" : "official_brand",
      internalOnly: false,
      momentumAppropriate: false,
      openingsAppropriate: true,
      classification: isProperty ? "property" : "official",
    };
  }
  if (/hotelbusiness|hotelmanagement|ehotelier|travelweekly|lodgingmagazine/.test(u)) {
    return {
      category: "trade",
      internalOnly: false,
      momentumAppropriate: momentumEvidenceSourceRank(url) >= 65,
      openingsAppropriate: false,
      classification: "trade",
    };
  }
  return {
    category: "other",
    internalOnly: false,
    momentumAppropriate: followsTributeMomentumRules(url).ok,
    openingsAppropriate: false,
    classification: "other",
  };
}

function buildEvidenceNote(candidate) {
  const cases = (candidate.evidenceUseCases || []).join(", ");
  return [
    `v32B evidenceUseCase: ${cases || "General"}`,
    `momentumAppropriate: ${candidate.momentumAppropriate !== false ? "yes" : "no"}`,
    `openingsAppropriate: ${candidate.openingsAppropriate ? "yes" : "no"}`,
    candidate.note || "",
  ]
    .filter(Boolean)
    .join(" | ");
}

export function buildSourceLibraryFields(candidate, brandRecordId) {
  const errors = [];
  if (!nz(candidate.sourceTitle)) errors.push("sourceTitle required");
  if (!ALLOWED_SOURCE_TYPES.includes(candidate.sourceType)) {
    errors.push(`invalid sourceType: ${candidate.sourceType}`);
  }
  const urlCheck = isBlockedSourceUrl(candidate.sourceUrl);
  if (urlCheck.blocked) errors.push(`blocked_url:${urlCheck.reason}`);

  const captureDate = new Date().toISOString().slice(0, 10);
  const reviewDate = captureDate;
  const refreshDue = new Date(Date.now() + 180 * 86400000).toISOString().slice(0, 10);

  const fields = {
    [MAP_PARTNER_SOURCE.sourceTitle]: candidate.sourceTitle,
    [MAP_PARTNER_SOURCE.profileType]: "Brand",
    [MAP_PARTNER_SOURCE.brand]: [brandRecordId],
    [MAP_PARTNER_SOURCE.parentCompany]: undefined,
    [MAP_PARTNER_SOURCE.sourceType]: candidate.sourceType,
    [MAP_PARTNER_SOURCE.sourceUrl]: candidate.sourceUrl,
    [MAP_PARTNER_SOURCE.sourceOrigin]: "Public Web",
    [MAP_PARTNER_SOURCE.sourceQuality]: candidate.sourceQuality || "High",
    [MAP_PARTNER_SOURCE.status]: "Captured",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
    [MAP_PARTNER_SOURCE.region]: candidate.region || "Americas",
    [MAP_PARTNER_SOURCE.captureDate]: captureDate,
    [MAP_PARTNER_SOURCE.lastReviewed]: reviewDate,
    [MAP_PARTNER_SOURCE.notes]: buildEvidenceNote(candidate),
    [MAP_PARTNER_SOURCE.permissionVisibilityNotes]:
      "AI-assisted / company-materials governance — not company validation. Steward before Explorer use.",
    [MAP_PARTNER_SOURCE.confidentialityNotes]: candidate.internalOnly
      ? "internal_research_only — not owner-facing"
      : "",
  };
  delete fields[MAP_PARTNER_SOURCE.parentCompany];
  if (candidate.refreshDueDate) {
    fields[MAP_PARTNER_SOURCE.notes] += ` | refreshDue: ${candidate.refreshDueDate || refreshDue}`;
  }

  return { ok: errors.length === 0, errors, fields };
}

async function fetchAllBrandSources(brandRecordId) {
  const all = [];
  let offset = "";
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset || "";
  } while (offset);
  return all;
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

function extractUrlsFromPresentation(brand) {
  const urls = [];
  const urlRe = /https?:\/\/[^\s<>"')]+/gi;
  for (const block of brand?.brandExplorer?.blocks || []) {
    const text = `${nz(block.title)}\n${nz(block.body)}`;
    const matches = text.match(urlRe) || [];
    for (const raw of matches) {
      const clean = raw.replace(/[.,;]+$/, "");
      if (!isTemporaryAirtableUrl(clean)) urls.push({ url: clean, slotKey: block.slotKey, recordId: block.recordId });
    }
  }
  return urls;
}

function auditExistingSource(source) {
  const url = nz(source.sourceUrl);
  const durable = !isBlockedSourceUrl(url).blocked;
  const classification = classifySourceUrl(url, source.sourceTitle);
  const momentumType = classifyMomentumSourceType(url);
  return {
    recordId: source.id,
    sourceTitle: source.sourceTitle,
    sourceUrl: url,
    sourceType: source.sourceType,
    status: source.status,
    region: source.region,
    approvedForExplorerUse: source.approvedForExplorerUse,
    durable,
    classification: classification.classification,
    category: classification.category,
    momentumAppropriate: classification.momentumAppropriate,
    openingsAppropriate: classification.openingsAppropriate,
    internalOnly: classification.internalOnly,
    momentumSourceType: momentumType,
    approvedExplorer: isApprovedExplorerSource(source),
  };
}

function findExistingByUrl(sources, url) {
  const key = normalizeUrlKey(url);
  return sources.find((s) => normalizeUrlKey(s.sourceUrl) === key) || null;
}

function detectDuplicates(sources) {
  const byUrl = new Map();
  const duplicates = [];
  for (const s of sources) {
    const key = normalizeUrlKey(s.sourceUrl);
    if (!key) continue;
    if (byUrl.has(key)) {
      duplicates.push({
        recordId: s.id,
        duplicateOf: byUrl.get(key),
        url: key,
        title: s.sourceTitle,
      });
    } else {
      byUrl.set(key, s.id);
    }
  }
  return duplicates;
}

function buildSourceGapPlan(brandSlug, existingAudits, catalog) {
  const gaps = [];
  const hasConsumer = existingAudits.some((s) => /consumer|brand page|brand site|woodspring\.com/i.test(s.sourceTitle + s.sourceUrl));
  const hasDevelopment = existingAudits.some((s) => /development/i.test(s.sourceUrl + s.sourceTitle));
  const hasPress = existingAudits.some((s) => /press|media\.choicehotels/i.test(s.sourceUrl));
  const momentumSources = existingAudits.filter((s) => s.momentumAppropriate).length;
  const openingSources = existingAudits.filter((s) => s.openingsAppropriate).length;

  if (!hasConsumer) gaps.push("official_choice_brand_page");
  if (!hasDevelopment) gaps.push("official_development_page");
  if (!hasPress) gaps.push("official_newsroom_press_kit");
  if (momentumSources < 3) gaps.push("credible_momentum_sources");
  if (openingSources < 2) gaps.push("property_listing_examples_for_openings");
  if (existingAudits.filter((s) => s.approvedExplorer).length < 2) {
    gaps.push("approved_explorer_sources_stewardship");
  }
  return {
    requiredCategories: [
      "official Choice brand page",
      "official Choice development/franchise page",
      "official newsroom / press release / press kit",
      "credible hospitality trade coverage",
      "official property listing examples",
      "property-specific sources for Openings",
      "owner-relevant standards / conversion source",
    ],
    detectedGaps: gaps,
    catalogRoles: catalog.map((c) => c.role),
  };
}

function assignSourceReadiness(gapPlan, proposedCreates, proposedUpdates, existingCount) {
  if (proposedCreates.length === 0 && proposedUpdates.length === 0 && existingCount >= 5) {
    return { band: "source_ready_for_backfill", reason: "catalog_largely_registered" };
  }
  if (gapPlan.detectedGaps.includes("credible_momentum_sources") && proposedCreates.length > 0) {
    return { band: "needs_more_source_capture", reason: "momentum_sources_pending_registration" };
  }
  if (proposedCreates.length > 0) {
    return { band: "needs_more_source_capture", reason: "new_sources_proposed" };
  }
  if (gapPlan.detectedGaps.length > 2) {
    return { band: "blocked_by_source_gaps", reason: gapPlan.detectedGaps.join(",") };
  }
  return { band: "source_ready_for_backfill", reason: "minimal_gaps_after_capture" };
}

function enrichCandidate(entry, presentationUrls) {
  const classification = classifySourceUrl(entry.sourceUrl, entry.sourceTitle);
  const tribute = followsTributeMomentumRules(entry.sourceUrl);
  return {
    ...entry,
    ...classification,
    momentumAppropriate:
      entry.evidenceUseCases?.includes("Momentum") !== false
        ? classification.momentumAppropriate && tribute.ok
        : false,
    openingsAppropriate:
      entry.evidenceUseCases?.includes("Openings") || classification.openingsAppropriate,
    tributeMomentumCheck: tribute,
    sourceQuality: classification.category === "trade" ? "Medium" : "High",
    note: entry.note || `v32B curated catalog role: ${entry.role}`,
    fromPresentation: presentationUrls.some(
      (p) => normalizeUrlKey(p.url) === normalizeUrlKey(entry.sourceUrl)
    ),
  };
}

async function processBrand(brand, options = {}) {
  const catalog = SOURCE_CAPTURE_CATALOG[brand.slug] || [];
  const brandBasics = await fetchBrandBasics(brand.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);
  const liveState = await fetchLiveState(brand.recordId).catch(() => ({
    sources: [],
    facts: [],
  }));
  const existingSources = await fetchAllBrandSources(brand.recordId);
  const brandApi = await fetchBrandApiShape(brand.recordId);
  const presentationUrls = extractUrlsFromPresentation(brandApi);

  const existingAudit = existingSources.map(auditExistingSource);
  const gapPlan = buildSourceGapPlan(brand.slug, existingAudit, catalog);

  const internalFindings = [];
  for (const block of brandApi?.brandExplorer?.blocks || []) {
    const text = `${block.title}\n${block.body}`;
    for (const re of FDD_INTERNAL_RES) {
      if (re.test(text)) {
        internalFindings.push({
          pattern: re.source,
          slotKey: block.slotKey,
          recordId: block.recordId,
          classification: "internal_research_only",
          notOwnerFacing: true,
          doNotUseAsVisibleSourceLabel: true,
        });
      }
    }
  }
  internalFindings.push({
    pattern: "fdd_fixture_reference",
    classification: "internal_research_only",
    note: INTERNAL_FDD_REFERENCE_NOTES[brand.slug],
    notOwnerFacing: true,
  });

  const proposedCreates = [];
  const proposedUpdates = [];
  const durableValidation = [];

  for (const entry of catalog) {
    const candidate = enrichCandidate(entry, presentationUrls);
    const urlCheck = isBlockedSourceUrl(candidate.sourceUrl);
    durableValidation.push({
      role: entry.role,
      sourceUrl: candidate.sourceUrl,
      durable: !urlCheck.blocked,
      blockReason: urlCheck.reason,
    });
    if (urlCheck.blocked || candidate.internalOnly) continue;

    const existing = findExistingByUrl(existingSources, candidate.sourceUrl);
    const validation = buildSourceLibraryFields(candidate, brand.recordId);
    if (!validation.ok) {
      proposedCreates.push({
        action: "blocked_validation",
        role: entry.role,
        errors: validation.errors,
        sourceUrl: candidate.sourceUrl,
      });
      continue;
    }
    if (existing) {
      const notePrefix = buildEvidenceNote(candidate);
      if (!nz(existing.notes).includes("v32B evidenceUseCase")) {
        proposedUpdates.push({
          action: "patch_notes",
          recordId: existing.id,
          role: entry.role,
          fields: {
            [MAP_PARTNER_SOURCE.notes]: `${notePrefix}${existing.notes ? ` | prior: ${existing.notes.slice(0, 200)}` : ""}`,
            [MAP_PARTNER_SOURCE.lastReviewed]: new Date().toISOString().slice(0, 10),
          },
          before: { notes: existing.notes, approvedForExplorerUse: existing.approvedForExplorerUse },
        });
      }
    } else {
      proposedCreates.push({
        action: "create",
        role: entry.role,
        fields: validation.fields,
        sourceUrl: candidate.sourceUrl,
        evidenceUseCases: candidate.evidenceUseCases,
      });
    }
  }

  for (const pUrl of presentationUrls) {
    if (catalog.some((c) => normalizeUrlKey(c.sourceUrl) === normalizeUrlKey(pUrl.url))) continue;
    const cls = classifySourceUrl(pUrl.url);
    if (cls.internalOnly || isBlockedSourceUrl(pUrl.url).blocked) continue;
    if (!cls.openingsAppropriate && !cls.momentumAppropriate) continue;
    const existing = findExistingByUrl(existingSources, pUrl.url);
    if (existing) continue;
    const title = `${brand.name} — presentation-linked ${cls.category} source`;
    const candidate = {
      role: "presentation_extracted",
      sourceTitle: title,
      sourceUrl: pUrl.url,
      sourceType: cls.category === "property_listing" ? "Website Capture" : "Press Release",
      evidenceUseCases: cls.openingsAppropriate ? ["Openings"] : ["Momentum"],
      region: "Americas",
      ...cls,
      note: `Extracted from ${pUrl.slotKey} (${pUrl.recordId})`,
    };
    const validation = buildSourceLibraryFields(candidate, brand.recordId);
    if (validation.ok) {
      proposedCreates.push({
        action: "create",
        role: "presentation_extracted",
        fields: validation.fields,
        sourceUrl: pUrl.url,
        fromSlot: pUrl.slotKey,
      });
    }
  }

  const duplicates = detectDuplicates(existingSources);
  const sourceReadiness = assignSourceReadiness(
    gapPlan,
    proposedCreates.filter((p) => p.action === "create"),
    proposedUpdates,
    existingSources.length
  );

  const discovery = getDiscoveryBrandConfig(brand.slug);
  return {
    slug: brand.slug,
    displayName: brand.name,
    recordId: brand.recordId,
    discoveryConfig: {
      present: Boolean(discovery),
      configRecordId: discovery?.recordId || null,
      liveRecordId: brand.recordId,
      recordIdPatchRecommended:
        Boolean(brand.discoveryConfigRecordIdPatchRecommended) && !discovery?.recordId,
      recommendedPatch: discovery && !discovery.recordId ? brand.recordId : null,
    },
    companyValidatedBefore,
    existingSourceAudit: existingAudit,
    sourceGapPlan: gapPlan,
    proposedCreates: proposedCreates.filter((p) => p.action === "create"),
    proposedUpdates,
    blockedProposals: proposedCreates.filter((p) => p.action === "blocked_validation"),
    internalOnlyFindings: internalFindings,
    durableUrlValidation: durableValidation,
    duplicateFindings: duplicates,
    sourceReadiness,
    presentationUrlsExtracted: presentationUrls.length,
    factCount: (liveState.facts || []).length,
  };
}

export function buildApplyCommand({ brands = BATCH_BRANDS.map((b) => b.slug) } = {}) {
  return [
    "npm run brand-explorer-choice-extended-stay-source-capture-writer --",
    `--brands ${brands.join(",")}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_SOURCE_ONLY,
    APPLY_FLAG_NO_PRESENTATION,
  ].join(" ");
}

function rankActivationOrder(brandResults) {
  const order = {
    source_ready_for_backfill: 1,
    needs_more_source_capture: 2,
    blocked_by_source_gaps: 3,
  };
  return [...brandResults].sort((a, b) => {
    const diff =
      (order[a.sourceReadiness.band] || 9) - (order[b.sourceReadiness.band] || 9);
    if (diff !== 0) return diff;
    return (b.existingSourceAudit?.length || 0) - (a.existingSourceAudit?.length || 0);
  });
}

export async function buildBrandExplorerChoiceExtendedStaySourceCaptureWriterReport(options = {}) {
  const brandList = nz(options.brands || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const slugs = brandList.length ? brandList : BATCH_BRANDS.map((b) => b.slug);
  const brands = BATCH_BRANDS.filter((b) => slugs.includes(b.slug));
  const apply = Boolean(options.apply);
  const approveBatch = Boolean(options.approveBatch);
  const noValidationClaim = Boolean(options.noValidationClaim);
  const sourceOnly = Boolean(options.sourceOnly);
  const noPresentation = Boolean(options.noPresentation);

  const brandResults = [];
  for (const brand of brands) {
    brandResults.push(await processBrand(brand));
    await new Promise((r) => setTimeout(r, 300));
  }

  const applyBlockers = [];
  if (apply && !approveBatch) applyBlockers.push(`missing_${APPLY_FLAG_APPROVE}`);
  if (apply && !noValidationClaim) applyBlockers.push(`missing_${APPLY_FLAG_NO_VALIDATION}`);
  if (apply && !sourceOnly) applyBlockers.push(`missing_${APPLY_FLAG_SOURCE_ONLY}`);
  if (apply && !noPresentation) applyBlockers.push(`missing_${APPLY_FLAG_NO_PRESENTATION}`);

  const allCreates = brandResults.flatMap((b) => b.proposedCreates);
  const allUpdates = brandResults.flatMap((b) => b.proposedUpdates);
  for (const c of allCreates) {
    const check = isBlockedSourceUrl(c.sourceUrl || c.fields?.[MAP_PARTNER_SOURCE.sourceUrl]);
    if (check.blocked) applyBlockers.push(`blocked_url_in_create:${c.role}`);
    const notes = nz(c.fields?.[MAP_PARTNER_SOURCE.notes]);
    if (/company validated|company-approved/i.test(notes)) {
      applyBlockers.push(`company_validation_language:${c.role}`);
    }
    if (FDD_INTERNAL_RES.some((re) => re.test(notes + c.sourceUrl))) {
      applyBlockers.push(`fdd_internal_would_be_owner_facing:${c.role}`);
    }
  }

  const hasWork = allCreates.length > 0 || allUpdates.length > 0;
  const dryRunClean = applyBlockers.length === 0 && hasWork;
  const canApply = apply && dryRunClean;

  let airtableModified = false;
  let applyResults = { created: [], updated: [], errors: [] };
  let companyValidatedSnapshots = brandResults.map((b) => ({
    slug: b.slug,
    before: b.companyValidatedBefore,
    after: b.companyValidatedBefore,
  }));

  if (canApply) {
    for (const brand of brandResults) {
      for (const create of brand.proposedCreates) {
        try {
          const created = await createPartnerSource(create.fields);
          applyResults.created.push({
            slug: brand.slug,
            recordId: created.id,
            role: create.role,
            sourceUrl: create.sourceUrl,
          });
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({
            slug: brand.slug,
            role: create.role,
            message: err.message,
          });
        }
      }
      for (const update of brand.proposedUpdates) {
        try {
          const patched = await patchPartnerSource(update.recordId, update.fields);
          applyResults.updated.push({
            slug: brand.slug,
            recordId: patched.id,
            role: update.role,
          });
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({
            slug: brand.slug,
            role: update.role,
            message: err.message,
          });
        }
      }
      const afterBasics = await fetchBrandBasics(brand.recordId);
      const snap = companyValidatedSnapshots.find((s) => s.slug === brand.slug);
      if (snap) snap.after = companyValidatedSnapshot(afterBasics);
    }
    airtableModified =
      (applyResults.created.length > 0 || applyResults.updated.length > 0) &&
      applyResults.errors.length === 0;
  } else if (apply) {
    applyResults.blocked = true;
    applyResults.blockers = applyBlockers;
  }

  const activationRanking = rankActivationOrder(brandResults).map((b, i) => ({
    rank: i + 1,
    slug: b.slug,
    displayName: b.displayName,
    sourceReadiness: b.sourceReadiness.band,
    existingSources: b.existingSourceAudit.length,
    proposedCreates: b.proposedCreates.length,
  }));

  const report = {
    writerVersion: WRITER_VERSION,
    v32bWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (airtableModified ? "apply" : "apply_blocked") : "dry-run",
    batchBrands: slugs,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    brandResults,
    activationRankingAfterSourceCapture: activationRanking,
    recommendedNextWriter: "v32C — Brand Asset Registry normalization writer (per brand)",
    applyBlockers,
    dryRunClean,
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brands: slugs }) : null,
    exactDryRunCommand: `npm run brand-explorer-choice-extended-stay-source-capture-writer -- --brands ${slugs.join(",")} --dry-run`,
    airtableModified,
    companyValidatedUntouched: companyValidatedSnapshots.every(
      (s) => JSON.stringify(s.before) === JSON.stringify(s.after)
    ),
    companyValidatedSnapshots,
    applyResults,
    applyGuardrails: {
      sourceLibraryOnly: true,
      noPresentationChanges: true,
      noImageApprovals: true,
      noCompanyValidationClaims: true,
      noTemporaryAirtableUrls: true,
      fddInternalNotOwnerFacing: true,
    },
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Choice Extended-Stay Source Capture v32B");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v32B exists: **${report.v32bWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Activation ranking after source capture");
  for (const r of report.activationRankingAfterSourceCapture) {
    lines.push(
      `${r.rank}. **${r.displayName}** — ${r.sourceReadiness} (${r.existingSources} existing, ${r.proposedCreates} creates proposed)`
    );
  }
  lines.push("");
  lines.push(`**Next writer:** ${report.recommendedNextWriter}`);
  lines.push("");
  for (const b of report.brandResults) {
    lines.push(`## ${b.displayName}`);
    lines.push(`- Existing sources: ${b.existingSourceAudit.length}`);
    lines.push(`- Proposed creates: ${b.proposedCreates.length}`);
    lines.push(`- Proposed updates: ${b.proposedUpdates.length}`);
    lines.push(`- Source readiness: **${b.sourceReadiness.band}**`);
    if (b.discoveryConfig.recordIdPatchRecommended) {
      lines.push(`- Discovery config recordId patch recommended: \`${b.discoveryConfig.recommendedPatch}\``);
    }
    lines.push("");
  }
  if (report.exactApplyCommand) {
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}
