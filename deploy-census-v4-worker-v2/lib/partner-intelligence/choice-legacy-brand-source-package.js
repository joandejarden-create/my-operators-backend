/**
 * Choice legacy Brand Explorer profiles — source package planning (dry-run default).
 * Does not rebuild Explorer content, extract facts, or auto-approve sources.
 * @see docs/data-intelligence/choice-legacy-brand-source-package-v1.md
 */
import fs from "fs";
import path from "path";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerSources, createPartnerSource } from "./airtable-source.js";
import {
  resolveLocalSourceAbsolutePath,
  resolveReferenceRoot,
} from "./reference-material-paths.js";
import { readLocalSourceText } from "./extract-source-text.js";
import { countSources } from "./intelligence-profile-workflow.js";
import { buildPackageFromRecords } from "./stewardship-package.js";
import { assessExplorerActiveStatus } from "./active-brand-governance-upgrade.js";

export const PACKAGE_VERSION = "1";
export const REPORT_JSON_NAME = "choice-legacy-brand-source-package.json";
export const REPORT_MD_NAME = "choice-legacy-brand-source-package.md";
export const COMPANY_FOLDER = "Choice Hotels International";

/** Control — platform-ready; excluded from batch processing. */
export const RADISSON_BLU_CONTROL_ID = "recWPEvxBQxVVzSq3";

const READABLE_EXT = new Set([".pdf", ".html", ".htm", ".txt", ".md"]);

/**
 * v1 batch — Explorer-active Choice legacy brands (Evidence Package Needed).
 * URLs marked verified come from fixtures/choice-dev-site-text or choice-media-center-text.
 */
export const CHOICE_LEGACY_BRANDS = [
  {
    key: "ascend-hotel-collection",
    brandName: "Ascend Hotel Collection",
    recordId: "reclkgOzvAcBheUSo",
    referenceFolderCandidates: ["Ascend Collection", "Ascend Hotel Collection", "Ascend"],
    consumerPage: {
      url: "https://www.choicehotels.com/ascend",
      confidence: "verified",
      note: "Brand directory seed + Ascend press kit cross-link",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/upscale/ascend",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: "https://media.choicehotels.com/ascend-hotel-collection-press-kit",
      confidence: "verified",
      note: "fixtures/choice-media-center-text",
    },
    regionalCaveats: [],
    localPdfHints: [
      "Choice Hotels International/Ascend Collection/brochure--ascend.pdf",
      "Choice Hotels International/Ascend Collection/ASC_OnePager_2024_PRINT.pdf",
    ],
  },
  {
    key: "comfort-inn-suites",
    brandName: "Comfort Inn & Suites",
    recordId: "recOzH5iAE1xEjyD0",
    referenceFolderCandidates: ["Comfort Inn & Suites", "Comfort Inn", "Comfort"],
    consumerPage: {
      url: "https://www.choicehotels.com/comfort-hotels",
      confidence: "verified",
      note: "Brand directory seed (comfort-hotels slug)",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/comfort",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: "https://media.choicehotels.com/comfort-press-kit",
      confidence: "verified",
      note: "fixtures/choice-media-center-text",
    },
    regionalCaveats: [],
    localPdfHints: [],
  },
  {
    key: "country-inn-suites-choice",
    brandName: "Country Inn & Suites by Choice",
    recordId: "recaayt9u7YYg8h7Y",
    referenceFolderCandidates: [
      "Country Inn & Suites",
      "Country Inn & Suites by Choice",
      "Country Inn",
    ],
    consumerPage: {
      url: "https://www.choicehotels.com/country-inn-suites",
      confidence: "verified",
      note: "Brand directory seed",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/country-inn-and-suites",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: null,
      confidence: "uncertain",
      note:
        "No dedicated Country Inn press-kit page in choice-media-center manifest; use Comfort/Country PR links (P1) or harvest CIS one-pager PDF",
    },
    regionalCaveats: [
      "Americas brand owned by Choice; global RHG Country Inn materials are separate reference only",
    ],
    localPdfHints: [
      "Choice Hotels International/Country Inn & Suites/CIS_OnePager_2024.pdf",
      "Choice Hotels International/Country Inn & Suites/brochure--country-inn-and-suites.pdf",
    ],
  },
  {
    key: "everhome-suites",
    brandName: "Everhome Suites",
    recordId: "recqkkrsevi4r9ibj",
    referenceFolderCandidates: ["Everhome Suites", "Everhome"],
    consumerPage: {
      url: "https://www.choicehotels.com/everhome-suites",
      confidence: "verified",
      note: "Brand directory seed + Everhome export",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/everhome-suites",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: "https://media.choicehotels.com/everhome-suites",
      confidence: "verified",
      note: "fixtures/choice-media-center-text (everhome-suites slug)",
    },
    regionalCaveats: [],
    localPdfHints: [],
  },
  {
    key: "quality-inn",
    brandName: "Quality Inn",
    recordId: "recd8o4k1JddhkRWW",
    referenceFolderCandidates: ["Quality Inn", "Quality"],
    consumerPage: {
      url: "https://www.choicehotels.com/quality-inn",
      confidence: "verified",
      note: "Brand directory seed + quality press kit",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/midscale/quality-inn",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: "https://media.choicehotels.com/quality-press-kit",
      confidence: "verified",
      note: "fixtures/choice-media-center-text",
    },
    regionalCaveats: [],
    localPdfHints: [],
  },
  {
    key: "radisson-choice",
    brandName: "Radisson by Choice",
    recordId: "recywbx1YQSTCPqW1",
    referenceFolderCandidates: ["Radisson by Choice", "Radisson", "Radisson (Choice)"],
    consumerPage: {
      url: "https://www.choicehotels.com/radisson",
      confidence: "verified",
      note: "Brand directory seed",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: "https://media.choicehotels.com/Radisson-press-kit",
      confidence: "verified",
      note: "Choice media center — Americas disclaimer applies",
    },
    regionalCaveats: [
      "Americas Radisson owned by Choice; do not register RHG global radissonhotels.com facts on this Brand Basics row",
      "Press kit includes explicit Americas vs RHG Belgium ownership split",
    ],
    localPdfHints: [],
  },
  {
    key: "radisson-individuals-choice",
    brandName: "Radisson Individuals by Choice",
    recordId: "recRyvM8OmLlDj9G7",
    referenceFolderCandidates: [
      "Radisson Individuals by Choice",
      "Radisson Individuals",
      "Radisson Individuals (Choice)",
    ],
    consumerPage: {
      url: "https://www.choicehotels.com/radisson-individuals",
      confidence: "verified",
      note: "Brand directory seed",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-individuals",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: "https://media.choicehotels.com/Radisson-Individuals-press-kit",
      confidence: "verified",
      note: "Choice media center — Americas disclaimer applies",
    },
    regionalCaveats: [
      "Americas Radisson Individuals owned by Choice; exclude RHG global portfolio facts",
    ],
    localPdfHints: [],
  },
  {
    key: "radisson-red-choice",
    brandName: "Radisson RED by Choice",
    recordId: "recmKqo7M7mLZgRqQ",
    referenceFolderCandidates: ["Radisson RED by Choice", "Radisson RED", "Radisson Red"],
    consumerPage: {
      url: "https://www.choicehotels.com/radisson-red",
      confidence: "verified",
      note: "Brand directory seed",
    },
    developmentPage: {
      url: "https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson-red",
      confidence: "verified",
      note: "Captured in fixtures/choice-dev-site-text",
    },
    pressKit: {
      url: "https://media.choicehotels.com/Radisson-Red-press-kit",
      confidence: "verified",
      note: "Choice media center — Americas disclaimer applies",
    },
    regionalCaveats: [
      "Americas Radisson RED owned by Choice; RHG Enjoy It brochure is separate global reference (see save-radisson-red-choice-development-pdfs.mjs)",
    ],
    localPdfHints: [
      "Choice Hotels International/Radisson RED by Choice/PIP Template-Radisson RED_2022.pdf",
    ],
  },
];

export const P1_ENRICHMENT_DEFAULTS = [
  {
    role: "recent_openings_pr",
    label: "recent openings / PR links",
    sourceType: "Press Release",
    confidence: "candidate",
    note: "Curate media.choicehotels.com announcement URLs per brand (footprint.momentum pattern)",
  },
  {
    role: "logo_media",
    label: "image/logo/media references",
    sourceType: "Website Capture",
    confidence: "candidate",
    note: "media.choicehotels.com/download/* assets; register as supporting only",
  },
  {
    role: "fdd",
    label: "FDD (optional)",
    sourceType: "FDD",
    confidence: "optional",
    note: "Choice Hotels International/FDDs/ — register only when verified current filing",
  },
  {
    role: "pitch_deck",
    label: "brand brochure / pitch deck",
    sourceType: "Development Brochure",
    confidence: "optional",
    note: "Local development/ or brands/{Brand}/ PDF when on disk",
  },
];

const ALLOWED_SOURCE_TYPES = [
  "FDD",
  "Development Brochure",
  "Development Page",
  "Brand Page",
  "Press Release",
  "Website Capture",
  "Other",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeUrl(u) {
  return nz(u).toLowerCase().replace(/\/+$/, "");
}

export function assessDevelopmentPageJsShellRisk(developmentPageUrl) {
  if (!developmentPageUrl) return { risk: "unknown", reason: "no_development_url" };
  if (!/choicehotelsdevelopment\.com/i.test(developmentPageUrl)) {
    return { risk: "low", reason: "not_choice_dev_domain" };
  }
  return {
    risk: "medium",
    reason:
      "Choice development site may render Salesforce/LWC shell (see Radisson Blu recC9utJdNaKWR56k); prefer DAM PDF / one-pager when extract preview is thin",
  };
}

export function scanLocalBrandFiles(referenceFolderCandidates, opts = {}) {
  const root = opts.referenceRoot || resolveReferenceRoot();
  const companyPrefix = `${COMPANY_FOLDER}/`;
  const found = [];
  const scannedFolders = [];

  for (const folderName of referenceFolderCandidates) {
    const relDir = `${companyPrefix}${folderName}`.replace(/\\/g, "/");
    const absDir = path.join(root, ...relDir.split("/"));
    scannedFolders.push(relDir);
    if (!fs.existsSync(absDir)) continue;

    function walk(rel) {
      const abs = path.join(root, ...rel.split("/"));
      if (!fs.existsSync(abs)) return;
      for (const name of fs.readdirSync(abs)) {
        if (name.startsWith(".") || name === "README.md" || name === "_capture-log.json") continue;
        const childRel = `${rel}/${name}`.replace(/\\/g, "/");
        const childAbs = path.join(root, ...childRel.split("/"));
        if (fs.statSync(childAbs).isDirectory()) {
          walk(childRel);
          continue;
        }
        const ext = path.extname(name).toLowerCase();
        if (!READABLE_EXT.has(ext)) continue;
        let sizeBytes = null;
        let textLength = null;
        let readable = false;
        try {
          const st = fs.statSync(childAbs);
          sizeBytes = st.size;
          if (ext === ".pdf" || ext === ".txt" || ext === ".html" || ext === ".htm") {
            const doc = readLocalSourceText(childRel);
            textLength = nz(doc.text).length;
            readable = textLength > 0;
          } else {
            readable = true;
          }
        } catch {
          readable = false;
        }
        found.push({
          relativePath: childRel,
          filename: name,
          ext,
          sizeBytes,
          textLength,
          readable,
          folder: folderName,
        });
      }
    }
    walk(relDir);
  }

  return { found, scannedFolders, referenceRoot: root };
}

function sourceMatchesUrl(existing, url) {
  if (!url) return false;
  return normalizeUrl(existing.sourceUrl) === normalizeUrl(url);
}

function sourceMatchesLocalPath(existing, localPath) {
  if (!localPath) return false;
  return nz(existing.localFilePath).toLowerCase() === nz(localPath).toLowerCase();
}

function findExistingSource(existingSources, { url, localFilePath, sourceType }) {
  for (const s of existingSources) {
    if (url && sourceMatchesUrl(s, url)) {
      return { sourceId: s.id, matchType: "source_url", sourceTitle: s.sourceTitle };
    }
    if (localFilePath && sourceMatchesLocalPath(s, localFilePath)) {
      return { sourceId: s.id, matchType: "local_file_path", sourceTitle: s.sourceTitle };
    }
  }
  if (localFilePath) {
    const fileName = path.basename(localFilePath).toLowerCase();
    for (const s of existingSources) {
      if (nz(s.localFilePath).toLowerCase().endsWith(fileName)) {
        return { sourceId: s.id, matchType: "filename", sourceTitle: s.sourceTitle };
      }
    }
  }
  return null;
}

export function buildProposedSourceEntry({
  role,
  label,
  sourceType,
  sourceUrl,
  localFilePath,
  confidence,
  note,
  brandId,
  brandName,
  jsShellRisk,
}) {
  return {
    role,
    label,
    sourceType,
    sourceTitle: `${brandName} — ${label}`,
    sourceUrl: sourceUrl || null,
    localFilePath: localFilePath || null,
    confidence,
    note: note || null,
    jsShellRisk: jsShellRisk || null,
    sourceOrigin: localFilePath ? "Brand Provided" : "Public Web",
    sourceQuality: sourceType === "Development Brochure" || sourceType === "FDD" ? "High" : "Medium",
    region: "CALA",
    priority: role.startsWith("p1") ? "P1" : "P0",
  };
}

export function buildP0Package(brandConfig, localScan) {
  const jsShell = assessDevelopmentPageJsShellRisk(brandConfig.developmentPage?.url);
  const localPdfs = (localScan.found || []).filter((f) => f.ext === ".pdf");
  const bestPdf =
    localPdfs.find((f) => /one.?pager|brochure|development/i.test(f.filename)) || localPdfs[0];

  const p0 = [
    buildProposedSourceEntry({
      role: "p0_consumer_page",
      label: "Choice consumer brand page",
      sourceType: "Brand Page",
      sourceUrl: brandConfig.consumerPage?.url,
      confidence: brandConfig.consumerPage?.confidence || "candidate",
      note: brandConfig.consumerPage?.note,
      brandId: brandConfig.recordId,
      brandName: brandConfig.brandName,
    }),
    buildProposedSourceEntry({
      role: "p0_development_page",
      label: "Choice development brand page",
      sourceType: "Development Page",
      sourceUrl: brandConfig.developmentPage?.url,
      confidence: brandConfig.developmentPage?.confidence || "candidate",
      note: brandConfig.developmentPage?.note,
      brandId: brandConfig.recordId,
      brandName: brandConfig.brandName,
      jsShellRisk: jsShell.risk,
    }),
  ];

  if (bestPdf) {
    p0.push(
      buildProposedSourceEntry({
        role: "p0_development_pdf",
        label: "development PDF / one-pager (local)",
        sourceType: "Development Brochure",
        localFilePath: bestPdf.relativePath,
        confidence: "verified_local",
        note: `On disk (${bestPdf.sizeBytes} bytes; text length ${bestPdf.textLength ?? "n/a"})`,
        brandId: brandConfig.recordId,
        brandName: brandConfig.brandName,
      })
    );
  } else {
    p0.push(
      buildProposedSourceEntry({
        role: "p0_development_pdf",
        label: "development PDF / one-pager",
        sourceType: "Development Brochure",
        sourceUrl: null,
        confidence: "missing",
        note:
          "No local PDF found — download from choicehotels.com/content/dam/… or harvest script before registration",
        brandId: brandConfig.recordId,
        brandName: brandConfig.brandName,
      })
    );
  }

  const press = brandConfig.pressKit;
  p0.push(
    buildProposedSourceEntry({
      role: "p0_press_kit",
      label: "Choice press kit / media center",
      sourceType: "Press Release",
      sourceUrl: press?.url || null,
      confidence: press?.confidence || "uncertain",
      note: press?.note,
      brandId: brandConfig.recordId,
      brandName: brandConfig.brandName,
    })
  );

  return { p0, jsShell };
}

export function buildP1Package(brandConfig, localScan) {
  const p1 = P1_ENRICHMENT_DEFAULTS.map((item) =>
    buildProposedSourceEntry({
      role: `p1_${item.role}`,
      label: item.label,
      sourceType: item.sourceType,
      confidence: item.confidence,
      note: item.note,
      brandId: brandConfig.recordId,
      brandName: brandConfig.brandName,
    })
  );

  const extraPdfs = (localScan.found || [])
    .filter((f) => f.ext === ".pdf")
    .slice(1);
  for (const pdf of extraPdfs) {
    p1.push(
      buildProposedSourceEntry({
        role: "p1_extra_local_pdf",
        label: `additional local PDF: ${pdf.filename}`,
        sourceType: "Development Brochure",
        localFilePath: pdf.relativePath,
        confidence: "verified_local",
        note: "Optional enrichment — steward separately",
        brandId: brandConfig.recordId,
        brandName: brandConfig.brandName,
      })
    );
  }

  return p1;
}

export function buildLocalSourceFields(spec, brandId) {
  const captureDate = new Date().toISOString().slice(0, 10);
  const errors = [];
  if (!nz(spec.sourceTitle)) errors.push("sourceTitle is required.");
  if (!ALLOWED_SOURCE_TYPES.includes(spec.sourceType)) {
    errors.push(`sourceType invalid: ${spec.sourceType}`);
  }
  const origin = spec.sourceOrigin || "Brand Provided";
  if (!VAL_PARTNER_SOURCE_SELECTS.sourceOrigin.includes(origin)) {
    errors.push(`sourceOrigin invalid: ${origin}`);
  }
  const quality = spec.sourceQuality || "High";
  if (!VAL_PARTNER_SOURCE_SELECTS.sourceQuality.includes(quality)) {
    errors.push(`sourceQuality invalid: ${quality}`);
  }

  const fields = {
    [MAP_PARTNER_SOURCE.sourceTitle]: spec.sourceTitle,
    [MAP_PARTNER_SOURCE.profileType]: "Brand",
    [MAP_PARTNER_SOURCE.brand]: [brandId],
    [MAP_PARTNER_SOURCE.sourceType]: spec.sourceType,
    [MAP_PARTNER_SOURCE.sourceOrigin]: origin,
    [MAP_PARTNER_SOURCE.sourceQuality]: quality,
    [MAP_PARTNER_SOURCE.status]: "Captured",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "No",
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
    [MAP_PARTNER_SOURCE.region]: spec.region || "CALA",
    [MAP_PARTNER_SOURCE.captureDate]: captureDate,
    [MAP_PARTNER_SOURCE.notes]: spec.note || "Choice legacy brand source package v1 — not auto-approved",
  };
  if (spec.localFilePath) fields[MAP_PARTNER_SOURCE.localFilePath] = spec.localFilePath;
  if (spec.sourceUrl) fields[MAP_PARTNER_SOURCE.sourceUrl] = spec.sourceUrl;

  return { ok: errors.length === 0, errors, fields };
}

function assessProposedSourceRegistration(proposed, existingSources, brandId) {
  const duplicate = findExistingSource(existingSources, {
    url: proposed.sourceUrl,
    localFilePath: proposed.localFilePath,
    sourceType: proposed.sourceType,
  });

  if (duplicate) {
    return {
      ...proposed,
      duplicate,
      alreadyRegistered: true,
      registrationStatus: "skip_already_registered",
      registrationReady: false,
    };
  }

  if (proposed.localFilePath) {
    let fileOk = false;
    let fileIssues = [];
    let textLength = 0;
    try {
      resolveLocalSourceAbsolutePath(proposed.localFilePath);
      const doc = readLocalSourceText(proposed.localFilePath);
      textLength = nz(doc.text).length;
      fileOk = textLength > 0;
    } catch (err) {
      fileIssues.push(err.message || String(err));
    }
    const validation = buildLocalSourceFields(proposed, brandId);
    const status = !fileOk
      ? "blocked_file_missing"
      : !validation.ok
        ? "blocked_validation"
        : "ready_to_register_local";
    return {
      ...proposed,
      duplicate: null,
      alreadyRegistered: false,
      registrationStatus: status,
      registrationReady: status === "ready_to_register_local",
      fileIssues,
      textLength,
      validation,
    };
  }

  if (proposed.sourceUrl && proposed.confidence === "verified") {
    return {
      ...proposed,
      duplicate: null,
      alreadyRegistered: false,
      registrationStatus: "capture_needed_url",
      registrationReady: false,
      captureCommand: buildCaptureCommand(proposed, brandId),
    };
  }

  return {
    ...proposed,
    duplicate: null,
    alreadyRegistered: false,
    registrationStatus: proposed.confidence === "missing" ? "blocked_missing" : "candidate_url_only",
    registrationReady: false,
  };
}

export function buildCaptureCommand(proposed, brandId) {
  if (!proposed.sourceUrl) return null;
  const typeFlag =
    proposed.sourceType === "Brand Page"
      ? "website-capture"
      : proposed.sourceType === "Press Release"
        ? "media-kit"
        : proposed.sourceType === "Development Page"
          ? "website-capture"
          : "other";
  return `npm run partner-reference:download -- --url "${proposed.sourceUrl}" --company "${COMPANY_FOLDER}" --brand "${proposed.sourceTitle.split(" — ")[0]}" --type ${typeFlag} --title "${proposed.label}" --brand-id ${brandId} --dry-run`;
}

export function classifyNextAction(brandRow) {
  const p0 = brandRow.proposedP0 || [];
  const readyLocal = p0.filter((s) => s.registrationStatus === "ready_to_register_local");
  if (readyLocal.length > 0) {
    return {
      action: "Register local PDF(s) then capture verified URLs",
      command: `npm run choice-legacy-brand-source-package -- --dry-run --brand ${brandRow.key}`,
    };
  }
  const needsCapture = p0.filter((s) => s.registrationStatus === "capture_needed_url");
  if (needsCapture.length > 0) {
    return {
      action: "Capture verified Choice URLs (dry-run download first)",
      command: needsCapture[0].captureCommand || brandRow.recommendedCaptureCommands[0],
    };
  }
  if (brandRow.missingSourceTypes.length > 0) {
    return {
      action: "Acquire missing P0 materials (PDF harvest / press kit)",
      command: brandRow.missingSourceTypes.includes("development PDF / one-pager")
        ? `node scripts/harvest-country-inn-choice-pdfs.mjs --dry-run`
        : `npm run partner-reference:search -- --operator "${brandRow.brandName}"`,
    };
  }
  return {
    action: "Review package plan — manual stewardship next",
    command: `npm run steward-partner-intelligence -- --entity-type brand --target-rec-id ${brandRow.recordId} --dry-run`,
  };
}

export function planChoiceLegacyBrandPackage(brandConfig, { existingSources = [], governanceRow = null, brandFields = null } = {}) {
  const localScan = scanLocalBrandFiles(brandConfig.referenceFolderCandidates);
  const { p0, jsShell } = buildP0Package(brandConfig, localScan);
  const p1 = buildP1Package(brandConfig, localScan);

  const p0Sources = p0.map((item) => assessProposedSourceRegistration(item, existingSources, brandConfig.recordId));
  const p1Sources = p1.map((item) => assessProposedSourceRegistration(item, existingSources, brandConfig.recordId));

  const sc = countSources(
    buildPackageFromRecords({
      sources: existingSources,
      facts: [],
      published: [],
      entityType: "brand",
      targetRecId: brandConfig.recordId,
    })
  );

  const missingSourceTypes = [];
  if (!p0Sources.find((s) => s.role === "p0_consumer_page" && !s.alreadyRegistered)) {
    if (!p0Sources.find((s) => s.role === "p0_consumer_page")?.alreadyRegistered) {
      missingSourceTypes.push("consumer brand page (PI registration)");
    }
  }
  for (const role of ["p0_development_page", "p0_development_pdf", "p0_press_kit"]) {
    const row = p0Sources.find((s) => s.role === role);
    if (!row) continue;
    if (row.alreadyRegistered) continue;
    if (row.registrationStatus === "blocked_missing" || row.confidence === "missing") {
      if (role === "p0_development_pdf") missingSourceTypes.push("development PDF / one-pager");
      if (role === "p0_press_kit" && row.confidence === "uncertain") {
        missingSourceTypes.push("press kit / media center");
      }
    }
    if (row.registrationStatus === "capture_needed_url") {
      missingSourceTypes.push(row.label);
    }
  }

  const registrationReady =
    p0Sources.some((s) => s.registrationReady) ||
    p0Sources.every((s) => s.alreadyRegistered || s.registrationStatus === "skip_already_registered");

  const explorer = brandFields ? assessExplorerActiveStatus(brandFields) : governanceRow?.explorerActiveDetail;

  const brandRow = {
    key: brandConfig.key,
    brandName: brandConfig.brandName,
    recordId: brandConfig.recordId,
    explorerActive: explorer?.active ?? governanceRow?.explorerActive ?? null,
    profileCompleteness: governanceRow?.profileCompleteness ?? null,
    piSourceCount: sc.total,
    approvedSourceCount: sc.approvedExplorer || 0,
    localFolderPath: `${COMPANY_FOLDER}/{${brandConfig.referenceFolderCandidates.join(" | ")}}`,
    localFilesFound: localScan.found,
    scannedFolders: localScan.scannedFolders,
    proposedP0: p0Sources,
    proposedP1: p1Sources,
    missingSourceTypes: [...new Set(missingSourceTypes)],
    registrationReady,
    jsShellRisk: jsShell,
    regionalCaveats: brandConfig.regionalCaveats || [],
    warnings: [
      jsShell.risk === "medium" ? jsShell.reason : null,
      brandConfig.regionalCaveats?.length
        ? `Regional: ${brandConfig.regionalCaveats[0]}`
        : null,
    ].filter(Boolean),
    recommendedCaptureCommands: p0Sources
      .map((s) => s.captureCommand)
      .filter(Boolean),
  };

  brandRow.nextAction = classifyNextAction(brandRow);
  return brandRow;
}

export async function fetchBrandSources(brandId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ brandId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

export async function buildChoiceLegacySourcePackageReport({ governanceReport = null, brandFilter = null } = {}) {
  const governanceById = new Map();
  if (governanceReport?.brands) {
    for (const row of governanceReport.brands) {
      if (row.recordId) governanceById.set(row.recordId, row);
    }
  }

  const brands = brandFilter
    ? CHOICE_LEGACY_BRANDS.filter((b) => b.key === brandFilter || b.recordId === brandFilter)
    : CHOICE_LEGACY_BRANDS;

  const rows = [];
  for (const brandConfig of brands) {
    if (brandConfig.recordId === RADISSON_BLU_CONTROL_ID) continue;
    const existingSources = await fetchBrandSources(brandConfig.recordId);
    const governanceRow = governanceById.get(brandConfig.recordId) || null;
    rows.push(
      planChoiceLegacyBrandPackage(brandConfig, {
        existingSources,
        governanceRow,
      })
    );
  }

  const summary = {
    totalBrands: rows.length,
    registrationReadyCount: rows.filter((r) => r.registrationReady).length,
    withLocalPdfs: rows.filter((r) => r.localFilesFound.some((f) => f.ext === ".pdf")).length,
    withJsShellWarning: rows.filter((r) => r.jsShellRisk?.risk === "medium").length,
    withRegionalCaveats: rows.filter((r) => r.regionalCaveats.length > 0).length,
    readyLocalRegister: rows.reduce(
      (n, r) => n + r.proposedP0.filter((s) => s.registrationStatus === "ready_to_register_local").length,
      0
    ),
    captureNeeded: rows.reduce(
      (n, r) => n + r.proposedP0.filter((s) => s.registrationStatus === "capture_needed_url").length,
      0
    ),
  };

  const recommendedFirstBrands = [...rows]
    .sort((a, b) => {
      const score = (r) =>
        (r.localFilesFound.some((f) => f.ext === ".pdf") ? 0 : 2) +
        (r.regionalCaveats.length ? 1 : 0) +
        (r.missingSourceTypes.length > 2 ? 1 : 0);
      return score(a) - score(b);
    })
    .slice(0, 3)
    .map((r) => ({ brandName: r.brandName, recordId: r.recordId, key: r.key }));

  return {
    packageVersion: PACKAGE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry_run",
    airtableModified: false,
    companyFolder: COMPANY_FOLDER,
    excludedControl: { brandName: "Radisson Blu by Choice", recordId: RADISSON_BLU_CONTROL_ID },
    summary,
    recommendedFirstBrands,
    brands: rows,
    doesNotDo: [
      "Rebuild Brand Explorer presentation content",
      "Overwrite populated Brand Setup fields",
      "Extract facts or approve sources automatically",
      "Publish governance or set Company Validated",
      "Auto-download uncertain URLs",
      "Register RHG global sources on Choice Americas brand rows",
    ],
  };
}

export async function applyLocalSourceRegistrations(report, { brandFilter = null } = {}) {
  const applied = [];
  const skipped = [];
  const errors = [];

  for (const brandRow of report.brands) {
    if (brandFilter && brandRow.key !== brandFilter && brandRow.recordId !== brandFilter) continue;
    for (const src of brandRow.proposedP0) {
      if (!src.localFilePath || src.registrationStatus !== "ready_to_register_local") {
        if (src.localFilePath) {
          skipped.push({ brand: brandRow.brandName, role: src.role, reason: src.registrationStatus });
        }
        continue;
      }
      try {
        const created = await createPartnerSource(src.validation.fields);
        applied.push({
          brand: brandRow.brandName,
          recordId: brandRow.recordId,
          role: src.role,
          sourceId: created.id,
          localFilePath: src.localFilePath,
        });
      } catch (err) {
        errors.push({
          brand: brandRow.brandName,
          role: src.role,
          message: err.message || String(err),
        });
      }
    }
  }

  return { applied, skipped, errors };
}

export function buildChoiceLegacySourcePackageMarkdown(report) {
  const lines = [
    "# Choice Legacy Brand Source Package v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Company folder: \`${report.companyFolder}\``,
    "",
    `> Control excluded: **${report.excludedControl.brandName}** (\`${report.excludedControl.recordId}\`) — platform-ready.`,
    "",
    "## Executive summary",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Brands planned | ${report.summary.totalBrands} |`,
    `| Brands with local PDFs on disk | ${report.summary.withLocalPdfs} |`,
    `| P0 local files ready to register | ${report.summary.readyLocalRegister} |`,
    `| P0 verified URLs needing capture | ${report.summary.captureNeeded} |`,
    `| JS-shell development page warnings | ${report.summary.withJsShellWarning} |`,
    `| Brands with regional/ownership caveats | ${report.summary.withRegionalCaveats} |`,
    "",
    "## Recommended first 3 brands to process",
    "",
  ];

  for (const b of report.recommendedFirstBrands) {
    lines.push(`1. **${b.brandName}** (\`${b.recordId}\`)`);
  }
  lines.push("");

  lines.push("## Brand packages", "");
  for (const row of report.brands) {
    lines.push(`### ${row.brandName}`, "");
    lines.push(
      `- Record: \`${row.recordId}\``,
      `- Explorer active: **${row.explorerActive ? "yes" : row.explorerActive === false ? "no" : "—"}**`,
      `- Profile completeness: **${row.profileCompleteness || "—"}**`,
      `- PI sources: **${row.approvedSourceCount}/${row.piSourceCount}** approved`,
      `- Local folder: \`${row.localFolderPath}\``,
      `- Registration ready (package): **${row.registrationReady ? "partial/yes" : "no"}**`,
      `- JS-shell risk: **${row.jsShellRisk?.risk}** — ${row.jsShellRisk?.reason}`,
      `- Next action: **${row.nextAction.action}**`
    );
    if (row.regionalCaveats.length) {
      lines.push(`- Regional caveats: ${row.regionalCaveats.join("; ")}`);
    }
    if (row.warnings.length) {
      lines.push("- Warnings:");
      for (const w of row.warnings) lines.push(`  - ${w}`);
    }

    lines.push("", "#### P0 sources", "");
    lines.push("| Role | Type | URL / Local | Confidence | Status |");
    lines.push("|------|------|-------------|------------|--------|");
    for (const s of row.proposedP0) {
      const loc = s.localFilePath ? `\`${s.localFilePath}\`` : s.sourceUrl || "—";
      lines.push(
        `| ${s.label} | ${s.sourceType} | ${loc} | ${s.confidence} | **${s.registrationStatus}** |`
      );
    }

    if (row.localFilesFound.length) {
      lines.push("", "#### Local files found", "");
      for (const f of row.localFilesFound) {
        lines.push(
          `- \`${f.relativePath}\` (${f.sizeBytes ?? "?"} bytes; text ${f.textLength ?? "n/a"})`
        );
      }
    } else {
      lines.push("", "_No local files found under scanned folders._", "");
    }

    if (row.missingSourceTypes.length) {
      lines.push(`- Missing: ${row.missingSourceTypes.join("; ")}`);
    }
    if (row.nextAction.command) {
      lines.push("", "```bash", row.nextAction.command, "```");
    }
    lines.push("");
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) lines.push(`- ${item}`);
  lines.push("");

  return lines.join("\n");
}
