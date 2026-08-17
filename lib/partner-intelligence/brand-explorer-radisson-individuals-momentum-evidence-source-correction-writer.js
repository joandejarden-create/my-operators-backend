/**
 * Brand Explorer Radisson Individuals Momentum Evidence-Source Correction v31M-R3.
 *
 * Replaces property-listing momentum URLs with event-supporting press/trade
 * coverage per Tribute Portfolio momentum evidence rules.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer-v31M-R3.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME as TRIBUTE_BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { PRESS_KIT_URL } from "./brand-explorer-radisson-individuals-openings-rebuild-writer.js";
import {
  MOMENTUM_SLOT,
  TARGET_BRAND,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-radisson-individuals-momentum-editorial-repair-writer.js";
import {
  buildMomentumBody,
  classifyMomentumSourceType,
  followsTributeMomentumRules,
  isChoicePropertyListingUrl,
  isMomentumInappropriatePropertyListing,
  labelsAreDifferentiated,
  legacyMomentumLinkLabel,
  momentumLinkLabelForUrl,
  momentumLinkLabelStorage,
  parseMomentumPresentationBody,
} from "./brand-explorer-momentum-link-label.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const WRITER_VERSION = "31M-R3";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer-v31M-R3.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31M-R3-momentum-evidence-source-correction";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-momentum-sources";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

/** Curated momentum evidence sources — event/announcement coverage, not property listings. */
export const MOMENTUM_EVIDENCE_SOURCE_CATALOG = Object.freeze({
  hotelBusinessCalaDebut:
    "https://hotelbusiness.com/radisson-individuals-debuts-in-latin-america/",
  hotelManagementColombiaPanama:
    "https://www.hotelmanagement.net/development/radisson-opens-radisson-individuals-hotels-colombia-panama",
  eHotelierPortfolioSigning:
    "https://insights.ehotelier.com/properties/2022/03/11/radisson-individuals-debuts-in-latin-america-with-a-portfolio-signing-of-resort-hotels/",
  choicePressKit: PRESS_KIT_URL,
});

/**
 * Recommended evidence-source mapping per momentum row (v31M-R3).
 * Property listing URLs from v31M-R2 are replaced with trade/press coverage.
 */
export const MOMENTUM_EVIDENCE_CORRECTION_PACKAGES = Object.freeze([
  {
    recordId: "rec0an5blfW4FtMfE",
    sort: 0,
    dateLine: "2024",
    title: "Radisson Individuals Expands Across CALA",
    sourceUrl: MOMENTUM_EVIDENCE_SOURCE_CATALOG.hotelBusinessCalaDebut,
    fallbackSourceUrl: MOMENTUM_EVIDENCE_SOURCE_CATALOG.choicePressKit,
    sourceBasis:
      "Hotel Business trade coverage of Radisson Individuals Latin America debut — portfolio-scale CALA momentum event.",
    preferredLinkLabel: "View Hotel Business Article",
    fallbackLinkLabel: "View Choice Hotels Press Kit",
  },
  {
    recordId: "recb0WzRRu6jrev4c",
    sort: 1,
    dateLine: "2024",
    title: "Colombia Urban and Heritage Markets Add Individuals Properties",
    sourceUrl: MOMENTUM_EVIDENCE_SOURCE_CATALOG.hotelManagementColombiaPanama,
    fallbackSourceUrl: MOMENTUM_EVIDENCE_SOURCE_CATALOG.hotelBusinessCalaDebut,
    sourceBasis:
      "Hotel Management development coverage of Radisson Individuals hotels in Colombia — supports Colombia urban/heritage momentum row.",
    preferredLinkLabel: "View Hotel Management Article",
    fallbackLinkLabel: "View Hotel Business Article",
  },
  {
    recordId: "recpIgmBNBEMXVEda",
    sort: 2,
    dateLine: "2024",
    title: "Panama Capital Corridor Extends Individuals Reach",
    sourceUrl: MOMENTUM_EVIDENCE_SOURCE_CATALOG.eHotelierPortfolioSigning,
    fallbackSourceUrl: MOMENTUM_EVIDENCE_SOURCE_CATALOG.hotelManagementColombiaPanama,
    sourceBasis:
      "eHotelier portfolio-signing coverage spanning Radisson Individuals Latin America expansion including Panama corridor context.",
    preferredLinkLabel: "View eHotelier Article",
    fallbackLinkLabel: "View Hotel Management Article",
  },
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-momentum-source-link-parity-writer.md",
  "reports/brand-explorer-radisson-individuals-momentum-source-link-parity-writer.json",
  "reports/brand-explorer-radisson-individuals-openings-momentum-parity-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-momentum-parity-writer.json",
  "live Radisson Individuals footprint.momentum rows",
  "live Tribute Portfolio footprint.momentum rows",
  "live Radisson Individuals API response",
  "public/js/brand-explorer-atelier-from-api.js",
  "lib/partner-intelligence/brand-explorer-momentum-link-label.js",
  "api/brand-library.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-momentum-link-label.js",
  "lib/partner-intelligence/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.js",
  "scripts/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.mjs",
  "docs/data-intelligence/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer-v31M-R3.md",
  "reports/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.md",
  "reports/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.json",
  "public/js/brand-explorer-atelier-from-api.js",
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v31mR3WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31M-R3`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31M-R3 supports Radisson Individuals by Choice only; got: ${brandArg}`);
  }
  return TARGET_BRAND;
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

async function listPresentationRowsRaw(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, PRESENTATION_TABLE)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function normalizeBody(body) {
  if (Array.isArray(body)) return body.join("\n\n");
  return nz(body);
}

function normalizeMomentumRow(rec) {
  const f = rec.fields || {};
  if (nz(f["Slot Key"]) !== MOMENTUM_SLOT) return null;
  return {
    recordId: rec.id,
    title: nz(f.Title),
    body: normalizeBody(f.Body),
    sortOrder: f["Sort Order"],
  };
}

async function fetchBrandApiShape(brandRecordId) {
  const req = { query: { brandId: brandRecordId, refresh: "1" }, headers: {} };
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
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

export function extractTributeMomentumEvidenceRules(tributeRows, brandMeta = {}) {
  const hierarchy = [
    "Official company press release / newsroom",
    "Official brand/development announcement",
    "Credible hospitality trade coverage",
    "Property page only as last resort",
  ];
  const patterns = [];
  for (const row of tributeRows) {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    const sourceClass = classifyMomentumSourceType(parsed.sourceUrl);
    const tributeRules = followsTributeMomentumRules(parsed.sourceUrl);
    patterns.push({
      recordId: row.recordId,
      title: row.title,
      dateLine: parsed.dateLine,
      sourceUrl: parsed.sourceUrl,
      sourceCategory: sourceClass.category,
      sourceType: sourceClass.sourceType,
      uiLinkLabel: momentumLinkLabelForUrl(parsed.sourceUrl, brandMeta),
      followsMomentumRules: tributeRules.ok,
      ruleReason: tributeRules.reason,
    });
  }
  return {
    summary:
      "Tribute Recent Momentum links to event-supporting sources (newsroom, PR wire, trade press, owner announcements). Property Marriott pages appear only when directly property-specific and no stronger announcement exists.",
    preferredHierarchy: hierarchy,
    labelStorage: momentumLinkLabelStorage(),
    propertyListingInMomentum: "discouraged — belongs in Openings / Examples",
    tributeRowPatterns: patterns,
  };
}

function auditRadissonMomentumRow(row, brandMeta, apiBlock) {
  const parsed = parseMomentumPresentationBody(row.body, row.title);
  const sourceUrl = parsed.sourceUrl;
  const sourceClass = classifyMomentumSourceType(sourceUrl);
  const tributeCheck = followsTributeMomentumRules(sourceUrl);
  const brand = brandMeta;

  let sourceKind = "other";
  if (sourceClass.sourceType === "official_press_kit") sourceKind = "press_kit";
  else if (sourceClass.sourceType === "press_release") sourceKind = "press_release";
  else if (sourceClass.sourceType === "credible_trade_article") sourceKind = "credible_trade_article";
  else if (sourceClass.sourceType === "property_listing") sourceKind = "property_listing";
  else if (sourceClass.sourceType === "choice_development_news") sourceKind = "choice_development_news";
  else if (sourceClass.category === "official_company_page") sourceKind = "generic_brand_page";
  else if (/booking|\/hotels\//.test(nz(sourceUrl).toLowerCase()) && !isChoicePropertyListingUrl(sourceUrl)) {
    sourceKind = "booking_page";
  }

  return {
    recordId: row.recordId,
    title: row.title,
    body: row.body,
    dateLine: parsed.dateLine,
    sourceUrl,
    renderedLinkLabel: momentumLinkLabelForUrl(sourceUrl, brand),
    legacyLinkLabel: legacyMomentumLinkLabel(sourceUrl),
    sourceKind,
    sourceCategory: sourceClass.category,
    isPropertyListing: isChoicePropertyListingUrl(sourceUrl),
    followsTributeMomentumRules: tributeCheck.ok,
    tributeRuleReason: tributeCheck.reason,
    apiSourceUrl: apiBlock
      ? parseMomentumPresentationBody(apiBlock.body, apiBlock.title).sourceUrl
      : null,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-momentum-evidence-source-correction-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsMomentumEvidenceSourceCorrectionWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTargetBrand(brandArg);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(target.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandMeta = { name: target.name, parentCompany: "Choice Hotels International" };

  const [radissonRaw, tributeRaw, radissonApi] = await Promise.all([
    listPresentationRowsRaw(baseId, apiKey, target.recordId, target.name),
    listPresentationRowsRaw(baseId, apiKey, TRIBUTE_RECORD_ID, TRIBUTE_BRAND_NAME),
    fetchBrandApiShape(target.recordId),
  ]);

  const radissonMomentum = radissonRaw.map(normalizeMomentumRow).filter(Boolean);
  const tributeMomentum = tributeRaw.map(normalizeMomentumRow).filter(Boolean);
  const apiBlocks = (radissonApi?.brandExplorer?.blocks || []).filter(
    (b) => nz(b.slotKey) === MOMENTUM_SLOT
  );
  const apiBlockById = new Map(apiBlocks.map((b) => [nz(b.recordId || b.id), b]));

  const tributeMomentumEvidenceRules = extractTributeMomentumEvidenceRules(tributeMomentum, {
    name: TRIBUTE_BRAND_NAME,
    parentCompany: "Marriott International",
  });

  const radissonMomentumSourceAudit = radissonMomentum.map((row) =>
    auditRadissonMomentumRow(row, brandMeta, apiBlockById.get(row.recordId))
  );

  const sourceUrlBeforeAfter = [];
  const linkLabelBeforeAfter = [];
  const proposedMomentumUpdates = [];

  for (const pkg of MOMENTUM_EVIDENCE_CORRECTION_PACKAGES) {
    const live =
      radissonMomentum.find((r) => r.recordId === pkg.recordId) ||
      radissonMomentum.find((r) => Number(r.sortOrder ?? -1) === Number(pkg.sort));

    const parsedBefore = live
      ? parseMomentumPresentationBody(live.body, live.title)
      : { dateLine: pkg.dateLine, description: "", sourceUrl: "" };

    const labelBefore = momentumLinkLabelForUrl(parsedBefore.sourceUrl, brandMeta);
    const labelAfter =
      momentumLinkLabelForUrl(pkg.sourceUrl, brandMeta) || pkg.preferredLinkLabel;

    sourceUrlBeforeAfter.push({
      recordId: pkg.recordId,
      title: pkg.title,
      sourceUrlBefore: parsedBefore.sourceUrl || null,
      sourceUrlAfter: pkg.sourceUrl,
      sourceBasis: pkg.sourceBasis,
      wasPropertyListing: isChoicePropertyListingUrl(parsedBefore.sourceUrl),
    });

    linkLabelBeforeAfter.push({
      recordId: pkg.recordId,
      linkLabelBefore: labelBefore,
      linkLabelAfter: labelAfter,
    });

    const needsSourcePatch =
      nz(parsedBefore.sourceUrl) !== nz(pkg.sourceUrl) ||
      isMomentumInappropriatePropertyListing(parsedBefore.sourceUrl);

    if (needsSourcePatch && live) {
      const summary = parsedBefore.description || "";
      proposedMomentumUpdates.push({
        recordId: pkg.recordId,
        reason: isChoicePropertyListingUrl(parsedBefore.sourceUrl)
          ? "replace_property_listing_with_momentum_evidence"
          : "align_momentum_evidence_source",
        fields: {
          Body: buildMomentumBody({
            dateLine: parsedBefore.dateLine || pkg.dateLine,
            summary,
            sourceUrl: pkg.sourceUrl,
          }),
          "Brand Name": target.name,
          Brand: [target.recordId],
        },
        before: { sourceUrl: parsedBefore.sourceUrl, linkLabel: labelBefore },
        after: { sourceUrl: pkg.sourceUrl, linkLabel: labelAfter, sourceBasis: pkg.sourceBasis },
      });
    }
  }

  const projectedLabels = MOMENTUM_EVIDENCE_CORRECTION_PACKAGES.map((pkg) =>
    momentumLinkLabelForUrl(pkg.sourceUrl, brandMeta)
  );

  const applyBlockers = [];
  if (!labelsAreDifferentiated(projectedLabels)) {
    applyBlockers.push("all_rows_same_generic_label");
  }

  for (const audit of radissonMomentumSourceAudit) {
    const pkg = MOMENTUM_EVIDENCE_CORRECTION_PACKAGES.find((p) => p.recordId === audit.recordId);
    if (!pkg) continue;
    const willFix = proposedMomentumUpdates.some((u) => u.recordId === audit.recordId);
    if (audit.isPropertyListing && !willFix) {
      applyBlockers.push(`property_listing_remains_on_momentum:${audit.recordId}`);
    }
  }

  for (const u of proposedMomentumUpdates) {
    const src = u.after?.sourceUrl;
    if (!src || isTemporaryAirtableUrl(src)) {
      applyBlockers.push(`unsupported_source_url:${u.recordId}`);
    }
    const check = followsTributeMomentumRules(src);
    if (!check.ok) {
      applyBlockers.push(`source_fails_tribute_momentum_rules:${u.recordId}:${check.reason}`);
    }
    if (isChoicePropertyListingUrl(src)) {
      applyBlockers.push(`property_listing_would_be_written:${u.recordId}`);
    }
  }

  const frontendLabelResolverChanged = true;
  const frontendPatchNote =
    "brand-explorer-momentum-link-label.js + brand-explorer-atelier-from-api.js — trade-publication and Choice development labels; property listings remain last-resort only.";

  const hasWork = proposedMomentumUpdates.length > 0 || frontendLabelResolverChanged;
  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const dryRunClean = applyBlockers.length === 0 && hasWork;
  const canApply = applyGatesReady && dryRunClean;

  let airtableModified = false;
  let applyResults = { momentumUpdated: [], errors: [] };
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    for (const update of proposedMomentumUpdates) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
        update.recordId
      );
      if (!res.ok) {
        applyResults.errors.push({
          recordId: update.recordId,
          error: json.error?.message || "momentum patch failed",
        });
        continue;
      }
      applyResults.momentumUpdated.push(update.recordId);
      airtableModified = true;
      await new Promise((r) => setTimeout(r, 220));
    }
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);
  if (!companyValidatedUntouched && canApply) {
    applyBlockers.push("company_validated_changed");
  }

  const finalQaBefore = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: target.slug,
  }).catch((err) => ({ error: err.message }));

  const completeBuildBefore = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: target.slug,
    targetQuality: "active-profile",
    dryRun: true,
  }).catch((err) => ({ error: err.message }));

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    v31mR3WriterExists: v31mR3WriterExists(),
    targetBrand: target,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    tributeMomentumEvidenceRules,
    radissonMomentumSourceAudit,
    sourceUrlBeforeAfter,
    linkLabelBeforeAfter,
    momentumEvidenceCorrectionPackages: MOMENTUM_EVIDENCE_CORRECTION_PACKAGES,
    proposedMomentumUpdates,
    projectedLinkLabels: projectedLabels,
    frontendLabelResolverChanged,
    frontendPatchNote,
    applyBlockers,
    dryRunClean,
    canApply,
    applyResults,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    airtableModified,
    expectedUiResult: {
      momentumSources: MOMENTUM_EVIDENCE_CORRECTION_PACKAGES.map((p) => ({
        title: p.title,
        sourceUrl: p.sourceUrl,
        linkLabel: momentumLinkLabelForUrl(p.sourceUrl, brandMeta),
      })),
      note: "Recent Momentum links to trade/press evidence — no Choice property listing URLs.",
    },
    expectedActiveProfileResult: {
      finalQaBefore: finalQaBefore?.brandReports?.[0]?.scores || null,
      completeBuildBefore:
        (completeBuildBefore?.brandReports || []).find((b) => b.slug === target.slug)?.readiness ||
        completeBuildBefore?.summary ||
        null,
    },
    exactApplyCommand: dryRunClean ? buildApplyCommand({ brand: target.slug }) : null,
  };

  report.markdown = buildMarkdownReport(report);
  return report;
}

function buildMarkdownReport(report) {
  const lines = [
    `# Brand Explorer Radisson Individuals Momentum Evidence-Source Correction v31M-R3`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31M-R3 exists: **${report.v31mR3WriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    "",
    "## Tribute momentum evidence rules",
    "",
    report.tributeMomentumEvidenceRules.summary,
    "",
    "**Preferred hierarchy:**",
    ...report.tributeMomentumEvidenceRules.preferredHierarchy.map((h) => `- ${h}`),
    "",
    "## Radisson momentum source audit",
    "",
  ];

  for (const a of report.radissonMomentumSourceAudit) {
    lines.push(`### ${a.title}`);
    lines.push(`- Record: \`${a.recordId}\``);
    lines.push(`- Source: ${a.sourceUrl || "missing"} (${a.sourceKind})`);
    lines.push(`- Link label: *${a.renderedLinkLabel}*`);
    lines.push(
      `- Follows Tribute rules: **${a.followsTributeMomentumRules ? "yes" : "no"}** (${a.tributeRuleReason})`
    );
    lines.push("");
  }

  lines.push("## Source URL before/after", "");
  for (const s of report.sourceUrlBeforeAfter) {
    lines.push(`- **${s.title}**`);
    lines.push(`  - Before: ${s.sourceUrlBefore || "none"}${s.wasPropertyListing ? " _(property listing)_" : ""}`);
    lines.push(`  - After: ${s.sourceUrlAfter}`);
    lines.push("");
  }

  lines.push("## Link label before/after", "");
  for (const l of report.linkLabelBeforeAfter) {
    lines.push(`- \`${l.recordId}\`: *${l.linkLabelBefore}* → *${l.linkLabelAfter}*`);
  }

  lines.push("", "## Frontend", "");
  lines.push(`- Label resolver changed: **${report.frontendLabelResolverChanged ? "yes" : "no"}**`);
  lines.push(`- ${report.frontendPatchNote}`);

  lines.push("", "## Governance", "");
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);

  if (report.applyBlockers.length) {
    lines.push("", "## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }

  if (report.exactApplyCommand) {
    lines.push("", "## Exact apply command", "", "```bash", report.exactApplyCommand, "```");
  }

  return lines.join("\n");
}
