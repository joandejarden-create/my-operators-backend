/**
 * Brand Explorer — AI-Assisted Profile trust footnote (global rendering requirement).
 *
 * Every Brand Explorer profile (public-full + factory-preview) must render:
 *   AI-Assisted Profile
 *   Last Reviewed: [MMM D, YYYY] · Source Basis: […] · Region: […]
 *
 * Prefer computed fallbacks over Airtable writes. Never implies Company Validated
 * unless governance.companyValidated is truly true. Does not write CV / Source Library /
 * Registry / Brand Status / release / presentation content.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { normalizeProfileGovernance } from "../profile-governance/normalize-profile-governance.js";
import { GOVERNANCE_EXTERNAL_DISPLAY_LABEL } from "../profile-governance/profile-governance-fields.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const AI_ASSISTED_FOOTNOTE_VERSION = "brand-explorer-ai-assisted-footnote-v1";

/** Effective date when the global footnote rendering requirement shipped (fallback only). */
export const AI_ASSISTED_FOOTNOTE_STANDARD_EFFECTIVE_DATE = "2026-07-28";

export const AI_ASSISTED_PROFILE_LABEL = "AI-Assisted Profile";

/** Owner-facing Source Basis labels (Brand Explorer). */
export const ALLOWED_SOURCE_BASIS = Object.freeze([
  "Company Materials",
  "Official Public Sources",
  "Company Materials + Public Sources",
  "Public Sources",
  "Advisor Research",
  "Company Validated",
]);

/** Owner-facing Region Basis labels (Brand Explorer). */
export const ALLOWED_REGION_BASIS = Object.freeze([
  "CALA-specific",
  "CALA-informed",
  "International Reference",
  "Global / International",
  "Market-specific",
  "Not region-specific",
]);

/** Wave 13 Accor / SO region posture from geo-momentum + SO cleanup (source-backed). */
export const WAVE13_REGION_BASIS_BY_SLUG = Object.freeze({
  "mama-shelter": "CALA-informed",
  mercure: "CALA-informed",
  ibis: "CALA-informed",
  novotel: "CALA-informed",
  pullman: "CALA-informed",
  fairmont: "CALA-informed",
  "fairmont-hotels-and-resorts": "CALA-informed",
  so: "International Reference",
  "so-hotels-and-resorts": "International Reference",
  "the-house-of-originals": "International Reference",
});

const DEFAULT_FACTORY_SOURCE_BASIS = "Company Materials + Public Sources";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function lower(v) {
  return nz(v).toLowerCase();
}

function readSelect(fields, columnName) {
  const raw = fields?.[columnName];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return nz(raw) || null;
  if (typeof raw === "object" && raw.name) return nz(raw.name) || null;
  return nz(raw) || null;
}

function readDateIso(raw) {
  const s = nz(raw);
  if (!s) return null;
  const day = s.split("T")[0];
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
    const d = new Date(s);
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString().slice(0, 10);
  }
  return day;
}

/**
 * Canonical Brand Explorer date format: MMM D, YYYY (e.g. Jul 7, 2026).
 */
export function formatBrandExplorerFootnoteDate(isoOrDate) {
  const iso = readDateIso(isoOrDate);
  if (!iso) return null;
  const d = new Date(`${iso}T12:00:00`);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

export function buildFootnoteSubtitle({ lastReviewedFormatted, sourceBasis, regionBasis }) {
  const parts = [];
  if (lastReviewedFormatted) parts.push(`Last Reviewed: ${lastReviewedFormatted}`);
  if (sourceBasis) parts.push(`Source Basis: ${sourceBasis}`);
  if (regionBasis) parts.push(`Region: ${regionBasis}`);
  return parts.length ? parts.join(" · ") : null;
}

function mapSourceBasisForBrandExplorer({ governance, companyValidated }) {
  if (companyValidated) return "Company Validated";

  const existing = nz(governance?.sourceBasis);
  if (ALLOWED_SOURCE_BASIS.includes(existing) && existing !== "Company Validated") {
    return existing;
  }

  // Legacy governance subtitle mappings → owner-facing BE labels
  if (/^company materials$/i.test(existing)) return "Company Materials";
  if (/^reviewed sources$/i.test(existing)) return "Company Materials + Public Sources";
  if (/ai-?assisted research/i.test(existing)) return DEFAULT_FACTORY_SOURCE_BASIS;

  const vs = nz(governance?.validationStatus);
  if (/company published/i.test(vs)) return "Company Materials";
  if (/source-?informed/i.test(vs)) return "Company Materials + Public Sources";
  if (/ai-?assisted/i.test(vs)) return DEFAULT_FACTORY_SOURCE_BASIS;

  const sourceType = nz(governance?.sourceType);
  if (/company materials|company website|official website|company pdf|brochure/i.test(sourceType)) {
    return "Company Materials";
  }
  if (/public|third-?party|news|press|mixed/i.test(sourceType)) {
    return "Company Materials + Public Sources";
  }

  return DEFAULT_FACTORY_SOURCE_BASIS;
}

function mapRegionFromSourceRegion(sourceRegion) {
  const sr = nz(sourceRegion);
  if (!sr) return null;
  if (/^cala-specific$/i.test(sr) || /^cala specific$/i.test(sr)) return "CALA-specific";
  if (/^cala-informed$/i.test(sr)) return "CALA-informed";
  if (/international reference/i.test(sr)) return "International Reference";
  if (/^global reference$/i.test(sr) || /^global$/i.test(sr)) return "International Reference";
  if (/global\s*\/\s*international/i.test(sr)) return "Global / International";
  if (/market-?specific|regional/i.test(sr)) return "Market-specific";
  if (/not region/i.test(sr)) return "Not region-specific";
  return null;
}

function presentationCorpus(brand) {
  const blocks = brand?.brandExplorer?.blocks || brand?.presentationRows || [];
  return (blocks || [])
    .map((r) =>
      [r.title, r.body, r.caseSummaryOverview, r.caseSummaryTags, r.caseSummaryBrandRelevance]
        .map(nz)
        .filter(Boolean)
        .join("\n")
    )
    .filter(Boolean)
    .join("\n\n");
}

function findCalaRegionBody(brand) {
  const blocks = brand?.brandExplorer?.blocks || [];
  const row = (blocks || []).find((r) => nz(r.slotKey) === "footprint.region.cala");
  return nz(row?.body);
}

/**
 * Infer region basis without inventing CALA presence.
 */
export function resolveRegionBasis({ governance, brand, slug }) {
  const slugKey = lower(slug || brand?.slug);
  if (WAVE13_REGION_BASIS_BY_SLUG[slugKey]) {
    return WAVE13_REGION_BASIS_BY_SLUG[slugKey];
  }

  const fromGov = mapRegionFromSourceRegion(governance?.sourceRegion);
  if (fromGov === "CALA-specific" || fromGov === "CALA-informed") {
    // Preserve only when presentation does not contradict.
    const calaBody = findCalaRegionBody(brand);
    if (/no verified cala|cleanly unavailable|no cala operating/i.test(calaBody)) {
      return "International Reference";
    }
    return fromGov;
  }
  if (fromGov) return fromGov;

  const calaBody = findCalaRegionBody(brand);
  const corpus = presentationCorpus(brand);
  const calaUnavailable = /no verified cala|cleanly unavailable|no cala operating/i.test(calaBody);
  const calaEvidence =
    Boolean(calaBody) &&
    !calaUnavailable &&
    calaBody.split(/\s+/).filter(Boolean).length >= 18 &&
    !/diligence-only/i.test(calaBody);
  const hasIr = /international reference/i.test(corpus) || /international reference/i.test(calaBody);

  if (calaEvidence && hasIr) return "CALA-informed";
  if (calaEvidence) return "CALA-specific";
  if (hasIr || calaUnavailable) return "International Reference";

  return "Not region-specific";
}

/**
 * Preferred Last Reviewed date order (ISO YYYY-MM-DD).
 */
export function resolveLastReviewedIso({ governance, brandBasicsFields, brand, options = {} }) {
  const basics = brandBasicsFields || {};

  const candidates = [
    governance?.lastReviewedDate,
    readSelect(basics, "Brand Explorer Last Reviewed"),
    readSelect(basics, "Last Reviewed Date"),
    readSelect(basics, "Profile Last Reviewed"),
    readSelect(basics, "Last Reviewed"),
    brand?.governance?.lastReviewedDate,
    brand?.activeProfileApprovedDate,
    readSelect(basics, "Active Profile Approved Date"),
    brand?.factoryPreview?.lastReviewedDate,
    brand?.factoryPreview?.reviewedAt,
    options.factoryReviewDate,
    options.standardizationReviewDate,
  ];

  for (const c of candidates) {
    const iso = readDateIso(c);
    if (iso) return { iso, source: "metadata" };
  }

  // Presentation standards.last_reviewed (display string — try parse)
  const blocks = brand?.brandExplorer?.blocks || [];
  const std = (blocks || []).find((r) => nz(r.slotKey) === "standards.last_reviewed");
  const stdIso = readDateIso(std?.body || std?.title);
  if (stdIso) return { iso: stdIso, source: "standards.last_reviewed" };

  if (options.allowStandardizationToday === true && options.standardizationReviewDate) {
    const iso = readDateIso(options.standardizationReviewDate);
    if (iso) return { iso, source: "standardization_review" };
  }

  // Final safe fallback: date the global BE footnote standard took effect
  // (only when no brand-specific review metadata exists).
  return {
    iso: AI_ASSISTED_FOOTNOTE_STANDARD_EFFECTIVE_DATE,
    source: "footnote_standard_effective_date",
  };
}

function hasSourceSupportedCala(brand, regionBasis) {
  if (regionBasis !== "CALA-specific" && regionBasis !== "CALA-informed") return true;
  const calaBody = findCalaRegionBody(brand);
  if (/no verified cala|cleanly unavailable/i.test(calaBody)) return false;
  if (WAVE13_REGION_BASIS_BY_SLUG[lower(brand?.slug)]) return true;
  if (calaBody && calaBody.split(/\s+/).filter(Boolean).length >= 12) return true;
  const corpus = presentationCorpus(brand);
  return /\bCALA\b/i.test(corpus) && !/no verified cala/i.test(corpus);
}

/**
 * Resolve the always-on Brand Explorer footnote (does not mutate inputs).
 */
export function resolveBrandExplorerAiAssistedFootnote({
  governance = null,
  brandBasicsFields = null,
  brand = null,
  slug = null,
  options = {},
} = {}) {
  const gov = governance || brand?.governance || {};
  const companyValidated = gov.companyValidated === true || brand?.companyValidated === true;

  const displayLabel = companyValidated
    ? GOVERNANCE_EXTERNAL_DISPLAY_LABEL.companyValidated || "Company-Validated Profile"
    : AI_ASSISTED_PROFILE_LABEL;

  const dateRes = resolveLastReviewedIso({
    governance: gov,
    brandBasicsFields,
    brand,
    options,
  });
  const lastReviewedIso = dateRes.iso;
  const lastReviewedFormatted = formatBrandExplorerFootnoteDate(lastReviewedIso);

  const sourceBasis = mapSourceBasisForBrandExplorer({ governance: gov, companyValidated });
  const regionBasis = resolveRegionBasis({ governance: gov, brand, slug: slug || brand?.slug });

  const displaySubtitle = buildFootnoteSubtitle({
    lastReviewedFormatted,
    sourceBasis,
    regionBasis,
  });

  const calaOk = hasSourceSupportedCala(brand, regionBasis);

  return {
    version: AI_ASSISTED_FOOTNOTE_VERSION,
    displayLabel,
    displaySubtitle,
    lastReviewedIso,
    lastReviewedFormatted,
    lastReviewedSource: dateRes.source,
    sourceBasis,
    regionBasis,
    companyValidated,
    alwaysOn: true,
    enriched: true,
    calaSourceSupported: calaOk,
    incomplete: !lastReviewedFormatted || !sourceBasis || !regionBasis,
  };
}

/**
 * Apply footnote onto brand.governance for Brand Explorer API responses.
 * Preserves internal governance fields; overwrites displayLabel/displaySubtitle for BE.
 */
export function applyBrandExplorerAiAssistedFootnote(brand, options = {}) {
  if (!brand || typeof brand !== "object") return brand;
  const prior = brand.governance && typeof brand.governance === "object" ? brand.governance : {};
  const footnote = resolveBrandExplorerAiAssistedFootnote({
    governance: prior,
    brandBasicsFields: options.brandBasicsFields || null,
    brand,
    slug: brand.slug,
    options,
  });

  brand.governance = {
    ...prior,
    displayLabel: footnote.displayLabel,
    displaySubtitle: footnote.displaySubtitle,
    sourceBasis: footnote.sourceBasis,
    // Keep lastReviewedDate when present; fill from fallback for consumers.
    lastReviewedDate: prior.lastReviewedDate || footnote.lastReviewedIso || null,
    brandExplorerFootnote: {
      version: footnote.version,
      lastReviewedIso: footnote.lastReviewedIso,
      lastReviewedFormatted: footnote.lastReviewedFormatted,
      lastReviewedSource: footnote.lastReviewedSource,
      sourceBasis: footnote.sourceBasis,
      regionBasis: footnote.regionBasis,
      alwaysOn: true,
      calaSourceSupported: footnote.calaSourceSupported,
    },
  };
  return brand;
}

/**
 * Gate: ai_assisted_profile_footnote_visible
 */
export function evaluateAiAssistedProfileFootnoteGate(brand, html = "") {
  const failures = [];
  const gov = brand?.governance || {};
  const footnoteMeta = gov.brandExplorerFootnote || null;
  const label = nz(gov.displayLabel);
  const subtitle = nz(gov.displaySubtitle);
  const companyValidated = gov.companyValidated === true || brand?.companyValidated === true;

  if (!label) {
    failures.push("footnote_component_not_rendered");
  }

  const lastReviewedPresent =
    /Last Reviewed:\s*\S+/i.test(subtitle) || Boolean(footnoteMeta?.lastReviewedFormatted);
  const sourceBasisPresent =
    /Source Basis:\s*\S+/i.test(subtitle) || Boolean(footnoteMeta?.sourceBasis);
  const regionPresent = /Region:\s*\S+/i.test(subtitle) || Boolean(footnoteMeta?.regionBasis);

  if (!lastReviewedPresent) failures.push("last_reviewed_date_missing");
  if (!sourceBasisPresent) failures.push("source_basis_missing");
  if (!regionPresent) failures.push("region_basis_missing");

  if (!companyValidated) {
    if (/company-?validated|brand verified/i.test(label)) {
      failures.push("company_validated_wording_without_company_validated");
    }
    if (/source basis:\s*company validated/i.test(subtitle)) {
      failures.push("company_validated_source_basis_without_company_validated");
    }
  }

  const regionBasis = footnoteMeta?.regionBasis || (subtitle.match(/Region:\s*([^·]+)/i) || [])[1]?.trim();
  if (/^cala-specific$/i.test(nz(regionBasis)) && footnoteMeta?.calaSourceSupported === false) {
    failures.push("cala_specific_without_source_support");
  } else if (/^cala-specific$/i.test(nz(regionBasis)) && !hasSourceSupportedCala(brand, "CALA-specific")) {
    failures.push("cala_specific_without_source_support");
  }

  const htmlText = nz(html);
  if (htmlText) {
    if (!/AI-Assisted Profile|Company-Validated Profile/i.test(htmlText)) {
      failures.push("footnote_not_visible_in_rendered_html");
    }
    if (!/Last Reviewed:/i.test(htmlText)) failures.push("last_reviewed_not_visible_in_html");
    if (!/Source Basis:/i.test(htmlText)) failures.push("source_basis_not_visible_in_html");
    if (!/Region:/i.test(htmlText)) failures.push("region_not_visible_in_html");
  }

  const pass = failures.length === 0;
  return {
    gateId: "ai_assisted_profile_footnote_visible",
    pass,
    failures: [...new Set(failures)],
    displayLabel: label || null,
    displaySubtitle: subtitle || null,
    lastReviewedPresent,
    sourceBasisPresent,
    regionPresent,
    companyValidated,
  };
}

/**
 * Audit one brand using raw normalize (pre-enrichment) vs enriched footnote.
 */
export function auditBrandExplorerFootnoteRow({
  brand,
  brandBasicsFields = null,
  slug = null,
  brandStatus = null,
  mode = "raw",
  options = {},
}) {
  const name = brand?.name || slug;
  const recordId = brand?.id || brand?.recordId || null;
  const status = brandStatus || brand?.brandStatus || null;
  const shouldRenderFullProfile = brand?.shouldRenderFullProfile === true;

  let governance = brand?.governance || null;

  if (mode === "raw" && brandBasicsFields) {
    governance = normalizeProfileGovernance(brandBasicsFields, {
      entityType: "brand",
      fallbackFields: brandBasicsFields,
    });
  }

  if (mode === "enriched") {
    const clone = {
      ...brand,
      governance: brand?.governance ? { ...brand.governance } : {},
    };
    applyBrandExplorerAiAssistedFootnote(clone, {
      brandBasicsFields,
      ...options,
    });
    governance = clone.governance;
  }

  const label = nz(governance?.displayLabel);
  const subtitle = nz(governance?.displaySubtitle);
  const footnoteVisible = Boolean(label);
  const lastReviewedPresent = /Last Reviewed:\s*\S+/i.test(subtitle);
  const sourceBasisPresent = /Source Basis:\s*\S+/i.test(subtitle);
  const regionPresent = /Region:\s*\S+/i.test(subtitle);

  let failureReason = null;
  if (!footnoteVisible) failureReason = "footnote component not rendered";
  else if (!lastReviewedPresent) failureReason = "last reviewed date missing";
  else if (!sourceBasisPresent) failureReason = "source basis missing";
  else if (!regionPresent) failureReason = "region basis missing";
  else if (
    governance?.companyValidated !== true &&
    /company-?validated|brand verified/i.test(label)
  ) {
    failureReason = "company validated wording without company validated";
  }

  if (!failureReason && mode === "raw" && !footnoteVisible) {
    const warnings = governance?.internalWarnings || [];
    if (warnings.some((w) => /governance not set/i.test(w))) {
      failureReason = "footnote component not rendered";
    } else if (warnings.some((w) => /trust label hidden|external display/i.test(w))) {
      failureReason = "renderer condition excludes this brand";
    }
  }

  return {
    brand: name,
    slug: slug || brand?.slug || null,
    recordId,
    brandStatus: status,
    shouldRenderFullProfile,
    footnoteVisible,
    lastReviewedPresent,
    sourceBasisPresent,
    regionBasisPresent: regionPresent,
    failureReason,
    displayLabel: label || null,
    displaySubtitle: subtitle || null,
    validationStatus: governance?.validationStatus || null,
    externalDisplayStatus: governance?.externalDisplayStatus || null,
    companyValidated: governance?.companyValidated === true,
    mode,
  };
}

export function writeFootnoteAuditReports(report, { jsonName, mdName } = {}) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName || "brand-explorer-ai-assisted-footnote-audit.json");
  const mdPath = path.join(reportsDir, mdName || "brand-explorer-ai-assisted-footnote-audit.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    `# Brand Explorer — AI-Assisted Profile Footnote Audit`,
    ``,
    `- Version: \`${report.version}\``,
    `- Generated: ${report.generatedAt}`,
    `- Mode: ${report.mode}`,
    `- Active universe: ${report.summary?.activeCount ?? "—"}`,
    `- Factory preview: ${report.summary?.previewCount ?? "—"}`,
    `- Pass: **${report.summary?.pass ?? 0}** · Fail: **${report.summary?.fail ?? 0}**`,
    ``,
    `| Brand | Slug | Record ID | Brand Status | shouldRenderFullProfile | Footnote Visible? | Last Reviewed? | Source Basis? | Region? | Failure Reason |`,
    `| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |`,
  ];
  for (const r of report.rows || []) {
    lines.push(
      `| ${r.brand || ""} | ${r.slug || ""} | ${r.recordId || ""} | ${r.brandStatus || ""} | ${r.shouldRenderFullProfile} | ${r.footnoteVisible} | ${r.lastReviewedPresent} | ${r.sourceBasisPresent} | ${r.regionBasisPresent} | ${r.failureReason || ""} |`
    );
  }
  lines.push("");
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}

export function factoryPreviewIdentitiesForFootnoteAudit() {
  return { ...FACTORY_PREVIEW_CANDIDATE_IDENTITIES };
}
