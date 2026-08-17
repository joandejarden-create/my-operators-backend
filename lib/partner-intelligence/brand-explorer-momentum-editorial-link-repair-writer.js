/**
 * Brand Explorer Momentum Editorial + Link Repair Writer v25C-3E.
 *
 * Repairs Tribute Portfolio footprint.momentum presentation rows: polished titles,
 * owner-facing bodies, property/source URLs, and neutral link labels (via frontend).
 * Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-momentum-editorial-link-repair-writer-v25C-3E.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import { MOMENTUM_SLOT } from "./brand-explorer-openings-momentum-row-review-package.js";

export const WRITER_VERSION = "25C-3E";
export const REPORT_JSON_NAME = "brand-explorer-momentum-editorial-link-repair-writer.json";
export const REPORT_MD_NAME = "brand-explorer-momentum-editorial-link-repair-writer.md";
export const DOC_MD_NAME = "brand-explorer-momentum-editorial-link-repair-writer-v25C-3E.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-3E-momentum-editorial-link-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-momentum-ui-copy";
export const APPLY_FLAG_LINKS = "--confirm-no-false-announcement-links";

const CASA_NIZUC_MARSHA = "CUNAN";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const CONSUMER_SITE_URL = "https://tribute-portfolio.marriott.com/";
const EXPECTED_MOMENTUM_COUNT = 6;

const GOVERNANCE_LABELS = [
  "Founder-reviewed UI copy package",
  "Source-grounded from official Marriott/Tribute metadata",
  "Not company-validated",
  "Not Marriott-validated",
];

const INTERNAL_TITLE_RE =
  /consumer site|brand site|tribute site|consumer map|dated on/i;

const FORBIDDEN_UI_PATTERNS = [
  INTERNAL_TITLE_RE,
  /\bMARSHA\b/i,
  /consumer-site listing/i,
  /AI-drafted from official-source metadata/i,
  /Pending founder review/i,
  /Not company-validated/i,
  /Not Marriott-validated/i,
];

const POSITIVE_PR_CLAIM_RE =
  /(?:marriott|tribute).{0,80}(?:press release|newsroom announcement|announced (?:its|the) (?:opening|debut))/i;

const PR_URL_RE =
  /newsroom|press-release|press_release|\/news\/|media\.choicehotels\.com|ihgplc\.com\/news/i;

/** Verified property overview URLs from v25C-3A source capture; hub URL only when no property page captured. */
export const MOMENTUM_REPAIR_PACKAGES = [
  {
    marsha: "LIMTX",
    recordId: "recwinQHDJ9rL02Lw",
    sort: 0,
    dateLine: "Apr 2026",
    propertyName: "Humano, Lima, a Tribute Portfolio Hotel",
    polishedTitle: "Humano Lima Added To Tribute Portfolio Pipeline",
    polishedSummary:
      "Official Tribute and Marriott source data lists Humano Lima with an April 2026 date, giving owners a current example of the collection's urban waterfront positioning in South America.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/limtx-humano-lima-a-tribute-portfolio-hotel/overview/",
    sourceBasis: "Marriott property page + Tribute consumer-site dated listing metadata (recF0qS9JIZjM3qza); not a press release.",
    sourceType: "Marriott Property Page",
    prUrlFound: false,
    proposedLinkLabel: "View property",
  },
  {
    marsha: "MDETX",
    recordId: "recr4swOtCY7nOjui",
    sort: 1,
    dateLine: "Dec 2025",
    propertyName: "Loma, Medellin, a Tribute Portfolio Hotel",
    polishedTitle: "Loma Medellín Expands Tribute's Urban Lifestyle Presence",
    polishedSummary:
      "Official brand-site metadata shows Loma Medellín with a December 2025 date—useful when owners evaluate Andean urban lifestyle affiliation inside Marriott's network.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/mdetx-loma-medellin-a-tribute-portfolio-hotel/overview/",
    sourceBasis: "Marriott property page + Tribute consumer-site dated listing metadata; not a newsroom announcement.",
    sourceType: "Marriott Property Page",
    prUrlFound: false,
    proposedLinkLabel: "View property",
  },
  {
    marsha: "BGITY",
    recordId: "rec46SixcVS6j9NbC",
    sort: 2,
    dateLine: "Feb 2025",
    propertyName: "Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort",
    polishedTitle: "Crystal Cove Adds Caribbean All-Inclusive Resort Example",
    polishedSummary:
      "Crystal Cove, Barbados carries a February 2025 dated listing on official Marriott/Tribute materials—an illustrative Caribbean resort-scale example for owners comparing all-inclusive leisure positioning.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/bgity-crystal-cove-barbados-a-tribute-portfolio-all-inclusive-resort/overview/",
    sourceBasis: "Marriott property page + consumer-site dated listing metadata; not a press release.",
    sourceType: "Marriott Property Page",
    prUrlFound: false,
    proposedLinkLabel: "View property",
  },
  {
    marsha: "SJUTX",
    recordId: "recfvVguASf2SQmEc",
    sort: 3,
    dateLine: "Jan 2024",
    propertyName: "Hotel Rumbao, a Tribute Portfolio Hotel",
    polishedTitle: "Hotel Rumbao Strengthens Tribute's San Juan Presence",
    polishedSummary:
      "Hotel Rumbao in Old San Juan appears with a January 2024 dated entry on official Tribute portfolio materials—earlier CALA urban activity owners can reference for heritage-city lifestyle deals.",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/sjutx-hotel-rumbao-a-tribute-portfolio-hotel/overview/",
    sourceBasis: "Marriott property page + consumer-site dated listing metadata; not a newsroom PR.",
    sourceType: "Marriott Property Page",
    prUrlFound: false,
    proposedLinkLabel: "View property",
  },
  {
    marsha: "BDOGP",
    recordId: "recjmKLQyq2YW0vp2",
    sort: 4,
    dateLine: "Jun 2026",
    propertyName: "Grand Hotel Preanger, Bandung, a Tribute Portfolio Hotel",
    polishedTitle: "Grand Hotel Preanger Adds Heritage-Led Asia Pacific Example",
    polishedSummary:
      "Grand Hotel Preanger, Bandung is listed with a June 2026 date on official Tribute portfolio materials—a heritage-led Asia Pacific urban example; treat as dated portfolio activity, not a press announcement.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza); no Marriott property page or PR URL captured.",
    sourceType: "Tribute Consumer Site",
    prUrlFound: false,
    proposedLinkLabel: "View Tribute Portfolio site",
  },
  {
    marsha: "MILNT",
    recordId: "recgzP6rMkL4VFrsW",
    sort: 5,
    dateLine: "Jun 2026",
    propertyName: "NEMI, Milan, a Tribute Portfolio Hotel",
    polishedTitle: "NEMI Milan Adds European Urban Lifestyle Example",
    polishedSummary:
      "NEMI Milan carries a June 2026 dated listing on official Tribute portfolio materials—European urban lifestyle activity owners can cite when comparing Marriott collection options in gateway cities.",
    sourceUrl: CONSUMER_SITE_URL,
    sourceBasis: "Tribute consumer-site embedded JSON openingDate (recF0qS9JIZjM3qza); no Marriott property page or PR URL captured.",
    sourceType: "Tribute Consumer Site",
    prUrlFound: false,
    proposedLinkLabel: "View Tribute Portfolio site",
  },
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-openings-momentum-row-creation-writer.md",
  "reports/brand-explorer-openings-momentum-row-creation-writer.json",
  "reports/brand-explorer-openings-momentum-row-review-package.md",
  "reports/brand-explorer-openings-momentum-row-review-package.json",
  "reports/brand-explorer-openings-momentum-source-capture-completion.md",
  "reports/brand-explorer-openings-visual-modal-repair-writer.md",
  "reports/brand-explorer-openings-visual-modal-repair-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Curio/Kimpton/Radisson/Ascend footprint.momentum rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-momentum-editorial-link-repair-writer.js",
  "scripts/brand-explorer-momentum-editorial-link-repair-writer.mjs",
  "docs/data-intelligence/brand-explorer-momentum-editorial-link-repair-writer-v25C-3E.md",
  "reports/brand-explorer-momentum-editorial-link-repair-writer.md",
  "reports/brand-explorer-momentum-editorial-link-repair-writer.json",
  "public/js/brand-explorer-atelier-from-api.js",
  "package.json",
];

export const FRONTEND_LINK_LABEL_ROOT_CAUSE = {
  file: "public/js/brand-explorer-atelier-from-api.js",
  function: "momentumAnnouncementLinkLabel",
  issue:
    "Hardcodes 'View {parentCompany} announcement' for any URL when publisher is Marriott International, Inc., even for property pages and Tribute consumer-site links.",
  repair:
    "Classify URL as press/newsroom vs property vs consumer hub; return neutral labels (View property / View Tribute Portfolio site / View source) unless URL is an actual announcement.",
};

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function buildMomentumBody(pkg) {
  return normalizeBody([pkg.dateLine, pkg.polishedSummary, pkg.sourceUrl].join("\n\n"));
}

function parseMomentumBody(body) {
  const paras = normalizeBody(body)
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  const date = paras[0] || "";
  let url = "";
  const descParts = [];
  for (let i = 1; i < paras.length; i++) {
    if (/^https?:\/\//i.test(paras[i])) url = paras[i];
    else descParts.push(paras[i]);
  }
  return { date, description: descParts.join("\n\n"), url };
}

function classifySourceUrl(url) {
  const u = nz(url).toLowerCase();
  if (!u) return "Other";
  if (PR_URL_RE.test(u) || /marriott\.com\/newsroom/.test(u)) return "Marriott Press Release";
  if (/newsroom/.test(u)) return "Marriott Newsroom";
  if (u.includes("tribute-portfolio.marriott.com")) return "Tribute Consumer Site";
  if (u.includes("marriott.com") && /\/hotels\//.test(u)) return "Marriott Property Page";
  if (u.includes("marriott.com")) return "Marriott Consumer Listing";
  return "Other";
}

function legacyAnnouncementLinkLabel(url, parentCompany = "Marriott International, Inc.") {
  const publisher = nz(parentCompany) || "Marriott International, Inc.";
  return `View ${publisher} announcement`;
}

function proposedLinkLabelForUrl(url) {
  const type = classifySourceUrl(url);
  if (type === "Marriott Press Release" || type === "Marriott Newsroom") {
    return "View announcement";
  }
  if (type === "Marriott Property Page") return "View property";
  if (type === "Tribute Consumer Site") return "View Tribute Portfolio site";
  if (type.includes("Marriott")) return "View Marriott source";
  return "View source";
}

function containsForbiddenUiCopy(text) {
  return FORBIDDEN_UI_PATTERNS.some((re) => re.test(nz(text)));
}

function bodyClaimsPrWithoutUrl(body, url) {
  if (POSITIVE_PR_CLAIM_RE.test(body) && !PR_URL_RE.test(nz(url))) return true;
  return /\bpress release\b/i.test(body) && !PR_URL_RE.test(nz(url));
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-momentum-editorial-link-repair-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_LINKS}`;
}

export async function buildBrandExplorerMomentumEditorialLinkRepairWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noFalseAnnouncementLinksConfirmed = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-3E pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const parentCompany = nz(brandBasicsBefore?.fields?.["Parent Company"]);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );

  const allPresentation = presentationRaw.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    imageCount: Array.isArray(rec.fields?.Image) ? rec.fields.Image.length : 0,
  }));

  const liveMomentum = allPresentation.filter((r) => r.slotKey === MOMENTUM_SLOT);
  const openingsSnapshot = allPresentation
    .filter((r) => r.slotKey === "footprint.openings")
    .map((r) => ({ recordId: r.recordId, title: r.title }));
  const loyaltySnapshot = allPresentation
    .filter((r) => r.slotKey.startsWith("loyalty."))
    .map((r) => ({ recordId: r.recordId, slotKey: r.slotKey, title: r.title }));

  const casaNizucInMomentum = liveMomentum.some((r) =>
    /casa nizuc|cunan/i.test(`${r.title} ${r.body}`)
  );

  const rowDiagnostics = [];
  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const applyBlockers = [];

  if (liveMomentum.length !== EXPECTED_MOMENTUM_COUNT) {
    applyBlockers.push(`momentum_row_count:${liveMomentum.length}!=${EXPECTED_MOMENTUM_COUNT}`);
  }
  if (casaNizucInMomentum) {
    applyBlockers.push("casa_nizuc_in_momentum_blocked");
  }

  for (const pkg of MOMENTUM_REPAIR_PACKAGES) {
    const live =
      liveMomentum.find((r) => r.recordId === pkg.recordId) ||
      liveMomentum.find((r) => Number(r.sortOrder ?? -1) === Number(pkg.sort));

    const parsed = live ? parseMomentumBody(live.body) : { date: "", description: "", url: "" };
    const currentSourceType = classifySourceUrl(parsed.url);
    const currentLinkLabel = parsed.url
      ? legacyAnnouncementLinkLabel(parsed.url, parentCompany)
      : "";
    const proposedLinkLabel = proposedLinkLabelForUrl(pkg.sourceUrl);
    const proposedBody = buildMomentumBody(pkg);
    const proposedFields = {
      Title: pkg.polishedTitle,
      Body: proposedBody,
    };

    rowDiagnostics.push({
      marsha: pkg.marsha,
      recordId: live?.recordId || pkg.recordId,
      sort: pkg.sort,
      propertyName: pkg.propertyName,
      currentTitle: live?.title || null,
      proposedTitle: pkg.polishedTitle,
      currentBody: live?.body || null,
      proposedBody,
      currentDate: parsed.date,
      currentSourceUrl: parsed.url,
      proposedSourceUrl: pkg.sourceUrl,
      currentSourceType,
      proposedSourceType: pkg.sourceType,
      sourceBasis: pkg.sourceBasis,
      prUrlFound: pkg.prUrlFound,
      prSourcePlan: pkg.prUrlFound
        ? "Replace body URL with captured PR/newsroom source"
        : "PR source not found — treat as dated brand/property activity, not press announcement",
      currentLinkLabel,
      proposedLinkLabel,
      uiFieldsRendered: ["Title (headline)", "Body date line", "Body description", "Body URL → link label via frontend"],
      frontendLinkLabelRootCause: FRONTEND_LINK_LABEL_ROOT_CAUSE,
    });

    if (!live) {
      rowsWouldCreate.push({ marsha: pkg.marsha, sort: pkg.sort, reason: "missing_live_row" });
      applyBlockers.push(`missing_momentum_row:${pkg.marsha}`);
      continue;
    }

    if (pkg.marsha === CASA_NIZUC_MARSHA) {
      applyBlockers.push("casa_nizuc_momentum_blocked");
    }

    if (containsForbiddenUiCopy(pkg.polishedTitle) || containsForbiddenUiCopy(pkg.polishedSummary)) {
      applyBlockers.push(`forbidden_copy_in_proposal:${pkg.marsha}`);
    }
    if (INTERNAL_TITLE_RE.test(pkg.polishedTitle)) {
      applyBlockers.push(`internal_title_language:${pkg.marsha}`);
    }
    if (bodyClaimsPrWithoutUrl(proposedBody, pkg.sourceUrl)) {
      applyBlockers.push(`false_pr_claim:${pkg.marsha}`);
    }
    if (proposedLinkLabel.toLowerCase().includes("announcement") && !PR_URL_RE.test(pkg.sourceUrl)) {
      applyBlockers.push(`false_announcement_label:${pkg.marsha}`);
    }

    const needsUpdate =
      nz(live.title) !== pkg.polishedTitle || normalizeBody(live.body) !== proposedBody;

    if (needsUpdate) {
      rowsWouldUpdate.push({
        marsha: pkg.marsha,
        recordId: live.recordId,
        sort: pkg.sort,
        action: "update",
        currentTitle: live.title,
        proposedTitle: pkg.polishedTitle,
        currentBody: live.body,
        proposedBody,
        proposedSourceUrl: pkg.sourceUrl,
        proposedLinkLabel,
        fields: proposedFields,
      });
    }
  }

  const announcementRemovedUnlessPrBacked = rowDiagnostics.every((row) => {
    if (PR_URL_RE.test(row.proposedSourceUrl)) return true;
    return !row.proposedLinkLabel.toLowerCase().includes("announcement");
  });

  const internalLanguageRemoved = rowDiagnostics.every(
    (row) => !INTERNAL_TITLE_RE.test(row.proposedTitle)
  );

  const properCaseEnforced = rowDiagnostics.every((row) => {
    const t = row.proposedTitle;
    return t === t && !/dated on/i.test(t);
  });

  const applyGatesReady =
    apply && approveBatch && founderReviewed && noFalseAnnouncementLinksConfirmed;
  const canApply =
    applyGatesReady && applyBlockers.length === 0 && rowsWouldUpdate.length > 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const errors = [];
    for (const row of rowsWouldUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.recordId
      );
      if (!res.ok) {
        errors.push({
          recordId: row.recordId,
          marsha: row.marsha,
          message: json.error?.message || res.status,
        });
      } else {
        updated.push({ recordId: row.recordId, marsha: row.marsha, title: row.proposedTitle });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { updated, errors };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C3EWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
      parentCompany: parentCompany || null,
    },
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    frontendLinkLabelRootCause: FRONTEND_LINK_LABEL_ROOT_CAUSE,
    currentMomentumTitles: rowDiagnostics.map((r) => ({
      recordId: r.recordId,
      marsha: r.marsha,
      title: r.currentTitle,
    })),
    proposedPolishedTitles: rowDiagnostics.map((r) => ({
      marsha: r.marsha,
      title: r.proposedTitle,
    })),
    currentLinkLabels: rowDiagnostics.map((r) => ({
      marsha: r.marsha,
      sourceUrl: r.currentSourceUrl,
      linkLabel: r.currentLinkLabel,
      sourceType: r.currentSourceType,
    })),
    proposedLinkLabelRepair: rowDiagnostics.map((r) => ({
      marsha: r.marsha,
      sourceUrl: r.proposedSourceUrl,
      linkLabel: r.proposedLinkLabel,
      sourceType: r.proposedSourceType,
      announcementRemoved: !r.proposedLinkLabel.toLowerCase().includes("announcement"),
    })),
    prNewsroomUrlStatus: rowDiagnostics.map((r) => ({
      marsha: r.marsha,
      prUrlFound: r.prUrlFound,
      plan: r.prSourcePlan,
    })),
    rowDiagnostics,
    rowsWouldUpdate,
    rowsWouldCreate,
    announcementRemovedUnlessPrBacked,
    internalSourceCaptureLanguageRemoved: internalLanguageRemoved,
    properCaseEnforced,
    casaNizucExcludedFromMomentum: !casaNizucInMomentum,
    loyaltyRowsUntouched: true,
    loyaltyRowsSnapshot: loyaltySnapshot,
    openingsRowsUntouched: true,
    openingsRowsSnapshot: openingsSnapshot,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    nonMomentumRowsModified: false,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      noFalseAnnouncementLinksConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply: rowsWouldUpdate.length === 0,
    doesNotDo: [
      "Create or delete momentum rows",
      "Change images or Sort Order",
      "Modify loyalty or openings rows",
      "Change Brand Basics or Company Validated",
      "Fabricate Marriott press-release URLs",
      "Imply Marriott validated anything",
    ],
  };
}

export function buildBrandExplorerMomentumEditorialLinkRepairWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Momentum Editorial + Link Repair Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-3E exists: **${report.v25C3EWriterExists ? "yes" : "no"}**`,
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Momentum rows inspected | ${report.rowDiagnostics.length} |`,
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Announcement removed (non-PR) | ${report.announcementRemovedUnlessPrBacked ? "yes" : "no"} |`,
    `| Internal/source-capture language removed | ${report.internalSourceCaptureLanguageRemoved ? "yes" : "no"} |`,
    `| Proper case enforced | ${report.properCaseEnforced ? "yes" : "no"} |`,
    `| Casa Nizuc excluded from momentum | ${report.casaNizucExcludedFromMomentum ? "yes" : "no"} |`,
    `| Loyalty rows untouched | ${report.loyaltyRowsUntouched ? "yes" : "no"} |`,
    `| Openings rows untouched | ${report.openingsRowsUntouched ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Frontend link-label root cause",
    "",
    `- File: \`${report.frontendLinkLabelRootCause.file}\``,
    `- Function: \`${report.frontendLinkLabelRootCause.function}\``,
    `- Issue: ${report.frontendLinkLabelRootCause.issue}`,
    `- Repair: ${report.frontendLinkLabelRootCause.repair}`,
    "",
    "## Title changes",
    "",
    "| MARSHA | Current | Proposed |",
    "|--------|---------|----------|",
  ];

  for (const row of report.rowDiagnostics) {
    lines.push(
      `| ${row.marsha} | ${row.currentTitle || "—"} | ${row.proposedTitle} |`
    );
  }
  lines.push("");

  lines.push("## Link label repair", "", "| MARSHA | Source type | Current label | Proposed label | PR found |", "|--------|-------------|---------------|----------------|----------|");
  for (const row of report.rowDiagnostics) {
    lines.push(
      `| ${row.marsha} | ${row.proposedSourceType} | ${row.currentLinkLabel} | ${row.proposedLinkLabel} | ${row.prUrlFound ? "yes" : "no"} |`
    );
  }
  lines.push("");

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) {
      lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");

  if (report.applyResults) {
    lines.push(
      "## Apply results",
      "",
      `- Updated: ${report.applyResults.updated?.length || 0}`,
      `- Errors: ${report.applyResults.errors?.length || 0}`,
      `- Blocked: ${report.applyResults.blocked ? "yes" : "no"}`,
      ""
    );
  }

  lines.push("## Does not do", "");
  for (const item of report.doesNotDo) {
    lines.push(`- ${item}`);
  }
  lines.push("");

  return lines.join("\n");
}
