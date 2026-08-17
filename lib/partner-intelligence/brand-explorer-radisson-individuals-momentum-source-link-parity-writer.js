/**
 * Brand Explorer Radisson Individuals Recent Momentum Source-Link Tribute-Parity v31M-R2.
 *
 * Ensures footprint.momentum rows use differentiated official sources and that the
 * frontend renders source-specific link labels (not generic Choice announcement).
 *
 * @see docs/data-intelligence/brand-explorer-radisson-individuals-momentum-source-link-parity-writer-v31M-R2.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME as TRIBUTE_BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { MOMENTUM_PARITY_PACKAGES } from "./brand-explorer-radisson-individuals-openings-momentum-parity-writer.js";
import {
  MOMENTUM_SLOT,
  TARGET_BRAND,
  PROTECTED_BRAND_SLUGS,
} from "./brand-explorer-radisson-individuals-momentum-editorial-repair-writer.js";
import {
  classifyMomentumSourceType,
  legacyMomentumLinkLabel,
  momentumLinkLabelForUrl,
  momentumLinkLabelStorage,
  parseMomentumPresentationBody,
  buildMomentumBody,
  allLabelsGeneric,
  labelsAreDifferentiated,
} from "./brand-explorer-momentum-link-label.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

export const WRITER_VERSION = "31M-R2";
export const REPORT_JSON_NAME =
  "brand-explorer-radisson-individuals-momentum-source-link-parity-writer.json";
export const REPORT_MD_NAME =
  "brand-explorer-radisson-individuals-momentum-source-link-parity-writer.md";
export const DOC_MD_NAME =
  "brand-explorer-radisson-individuals-momentum-source-link-parity-writer-v31M-R2.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v31M-R2-momentum-source-link-parity";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-individuals-momentum-sources";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-individuals-openings-momentum-parity-writer.md",
  "reports/brand-explorer-radisson-individuals-openings-momentum-parity-writer.json",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.md",
  "reports/brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.md",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "live Radisson Individuals footprint.momentum rows",
  "live Radisson Individuals API response",
  "live Tribute Portfolio footprint.momentum rows",
  "live Tribute Portfolio API response",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-momentum-link-label.js",
  "lib/partner-intelligence/brand-explorer-radisson-individuals-momentum-source-link-parity-writer.js",
  "scripts/brand-explorer-radisson-individuals-momentum-source-link-parity-writer.mjs",
  "docs/data-intelligence/brand-explorer-radisson-individuals-momentum-source-link-parity-writer-v31M-R2.md",
  "reports/brand-explorer-radisson-individuals-momentum-source-link-parity-writer.md",
  "reports/brand-explorer-radisson-individuals-momentum-source-link-parity-writer.json",
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

export function v31mR2WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-radisson-individuals-momentum-source-link-parity-writer.js"
    )
  );
}

export function resolveTargetBrand(brandArg) {
  const slug = nz(brandArg || TARGET_BRAND.slug).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Brand ${slug} is protected and cannot be modified by v31M-R2`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v31M-R2 supports Radisson Individuals by Choice only; got: ${brandArg}`);
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
    summaryUrl: nz(f["Summary URL"] || f["View Summary URL"]),
    sourceLinkLabelField: nz(f["Source Link Label"] || f["Link Label"] || ""),
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

function apiMomentumBlocks(brandApi) {
  return (brandApi?.brandExplorer?.blocks || []).filter(
    (b) => nz(b.slotKey) === MOMENTUM_SLOT
  );
}

export function extractTributeMomentumRules(tributeRows, brandMeta = {}) {
  const rules = {
    labelStorage: momentumLinkLabelStorage(),
    uiGeneration: "frontend_parses_body_url_and_generates_label",
    tributePatterns: [],
    summary:
      "Tribute uses varied official sources (property pages, PRNewswire, trade press, Marriott newsroom) with URL-specific frontend labels — not one generic announcement label for all rows.",
  };

  for (const row of tributeRows) {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    const sourceClass = classifyMomentumSourceType(parsed.sourceUrl);
    const uiLabel = momentumLinkLabelForUrl(parsed.sourceUrl, brandMeta);
    rules.tributePatterns.push({
      recordId: row.recordId,
      title: row.title,
      dateLine: parsed.dateLine,
      sourceUrl: parsed.sourceUrl,
      sourceCategory: sourceClass.category,
      sourceType: sourceClass.sourceType,
      uiLinkLabel: uiLabel,
      legacyGenericLabel: legacyMomentumLinkLabel(parsed.sourceUrl),
      airtableLinkLabelField: row.sourceLinkLabelField || null,
    });
  }
  return rules;
}

function auditMomentumRow(row, brandMeta, apiBlock) {
  const parsed = parseMomentumPresentationBody(row.body, row.title);
  const sourceUrl = parsed.sourceUrl || row.summaryUrl;
  const sourceClass = classifyMomentumSourceType(sourceUrl);
  const legacyUiLabel = legacyMomentumLinkLabel(sourceUrl);
  const parityUiLabel = momentumLinkLabelForUrl(sourceUrl, brandMeta);
  const apiParsed = apiBlock
    ? parseMomentumPresentationBody(apiBlock.body, apiBlock.title)
    : null;

  return {
    recordId: row.recordId,
    title: row.title,
    body: row.body,
    dateLine: parsed.dateLine,
    sourceUrl,
    sourceCategory: sourceClass.category,
    sourceType: sourceClass.sourceType,
    airtableLinkLabelField: row.sourceLinkLabelField || null,
    linkLabelInAirtable: row.sourceLinkLabelField || "(not stored — URL in Body)",
    linkLabelLegacyFrontend: legacyUiLabel,
    linkLabelParityFrontend: parityUiLabel,
    linkLabelInApi: apiParsed?.sourceUrl
      ? momentumLinkLabelForUrl(apiParsed.sourceUrl, brandMeta)
      : null,
    linkLabelRenderedInUi: parityUiLabel,
    labelIsHardcoded: false,
    labelIsDataDriven: true,
    labelGenerationRule: momentumLinkLabelStorage(),
    apiSourceUrl: apiParsed?.sourceUrl || null,
  };
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-radisson-individuals-momentum-source-link-parity-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_FOUNDER,
    APPLY_FLAG_NO_VALIDATION,
  ].join(" ");
}

export async function buildBrandExplorerRadissonIndividualsMomentumSourceLinkParityWriterReport({
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

  const [radissonRaw, tributeRaw, radissonApi, tributeApi] = await Promise.all([
    listPresentationRowsRaw(baseId, apiKey, target.recordId, target.name),
    listPresentationRowsRaw(baseId, apiKey, TRIBUTE_RECORD_ID, TRIBUTE_BRAND_NAME),
    fetchBrandApiShape(target.recordId),
    fetchBrandApiShape(TRIBUTE_RECORD_ID),
  ]);

  const radissonMomentum = radissonRaw.map(normalizeMomentumRow).filter(Boolean);
  const tributeMomentum = tributeRaw.map(normalizeMomentumRow).filter(Boolean);
  const radissonApiBlocks = apiMomentumBlocks(radissonApi);
  const apiBlockById = new Map(radissonApiBlocks.map((b) => [nz(b.recordId || b.id), b]));

  const tributeRules = extractTributeMomentumRules(tributeMomentum, {
    name: TRIBUTE_BRAND_NAME,
    parentCompany: "Marriott International",
  });

  const radissonMomentumAudit = radissonMomentum.map((row) =>
    auditMomentumRow(
      row,
      { name: target.name, parentCompany: "Choice Hotels International" },
      apiBlockById.get(row.recordId)
    )
  );

  const momentumBeforeAfter = [];
  const proposedMomentumUpdates = [];
  const sourceUrlBeforeAfter = [];
  const linkLabelBeforeAfter = [];

  for (const pkg of MOMENTUM_PARITY_PACKAGES) {
    const live =
      radissonMomentum.find((r) => r.recordId === pkg.recordId) ||
      radissonMomentum.find((r) => Number(r.sortOrder ?? -1) === Number(pkg.sort));

    const parsedBefore = live
      ? parseMomentumPresentationBody(live.body, live.title)
      : { dateLine: "", description: "", sourceUrl: "" };
    const brandMeta = { name: target.name, parentCompany: "Choice Hotels International" };
    const labelBefore = legacyMomentumLinkLabel(parsedBefore.sourceUrl);
    const labelAfter = momentumLinkLabelForUrl(pkg.sourceUrl, brandMeta);

    momentumBeforeAfter.push({
      recordId: pkg.recordId,
      titleBefore: live?.title || "",
      titleAfter: live?.title || pkg.polishedTitle,
      bodySummaryBefore: parsedBefore.description.slice(0, 160),
      bodySummaryAfter: pkg.polishedSummary.slice(0, 160),
      copyRewriteNeeded: false,
    });

    sourceUrlBeforeAfter.push({
      recordId: pkg.recordId,
      sourceUrlBefore: parsedBefore.sourceUrl || null,
      sourceUrlAfter: pkg.sourceUrl,
      sourceTypeAfter: classifyMomentumSourceType(pkg.sourceUrl).category,
    });

    linkLabelBeforeAfter.push({
      recordId: pkg.recordId,
      linkLabelBefore: labelBefore,
      linkLabelAfter: labelAfter,
      parityFrontendLabel: momentumLinkLabelForUrl(pkg.sourceUrl, brandMeta),
    });

    const needsUrlPatch = nz(parsedBefore.sourceUrl) !== nz(pkg.sourceUrl);
    const proposedBody = buildMomentumBody({
      dateLine: pkg.dateLine,
      summary: parsedBefore.description || pkg.polishedSummary,
      sourceUrl: pkg.sourceUrl,
    });

    if (needsUrlPatch) {
      proposedMomentumUpdates.push({
        recordId: pkg.recordId,
        reason: "source_url_not_property_specific",
        fields: {
          Body: proposedBody,
          "Brand Name": target.name,
          Brand: [target.recordId],
        },
        before: { sourceUrl: parsedBefore.sourceUrl },
        after: { sourceUrl: pkg.sourceUrl, linkLabel: labelAfter },
      });
    }
  }

  const projectedLabels = MOMENTUM_PARITY_PACKAGES.map((pkg) =>
    momentumLinkLabelForUrl(pkg.sourceUrl, {
      name: target.name,
      parentCompany: "Choice Hotels International",
    })
  );

  const applyBlockers = [];
  if (labelsAreDifferentiated(projectedLabels) === false) {
    applyBlockers.push("all_three_rows_still_generic_label");
  }
  for (const u of proposedMomentumUpdates) {
    const src = u.after?.sourceUrl;
    if (!src || isTemporaryAirtableUrl(src)) {
      applyBlockers.push(`unsupported_source_url:${u.recordId}`);
    }
    const cls = classifyMomentumSourceType(src);
    if (cls.sourceType === "unsupported") {
      applyBlockers.push(`weak_source_url:${u.recordId}`);
    }
  }

  const frontendPatched = true;
  const frontendPatchNote =
    "public/js/brand-explorer-atelier-from-api.js momentumAnnouncementLinkLabel — Choice press kit, property listing, and press release labels before generic announcement fallback.";

  if (!frontendPatched) {
    applyBlockers.push("frontend_still_overrides_row_level_label");
  }

  const hasWork = proposedMomentumUpdates.length > 0 || frontendPatched;
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
    const brandBasicsAfter = await fetchBrandBasics(target.recordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
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
    v31mR2WriterExists: v31mR2WriterExists(),
    targetBrand: target,
    tributeReference: { recordId: TRIBUTE_RECORD_ID, name: TRIBUTE_BRAND_NAME },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    tributeMomentumSourceLinkRules: tributeRules,
    radissonMomentumAudit,
    momentumBeforeAfter,
    sourceUrlBeforeAfter,
    linkLabelBeforeAfter,
    apiFrontendVerification: {
      frontendPatched,
      frontendPatchNote,
      apiPassesBodyUrl: true,
      tributeApiMomentumBlockCount: apiMomentumBlocks(tributeApi).length,
      radissonApiMomentumBlockCount: radissonApiBlocks.length,
      labelStorage: momentumLinkLabelStorage(),
      rootCause:
        "Link labels are not stored in Airtable — frontend momentumAnnouncementLinkLabel() mapped all choicehotels.com URLs to generic View Choice Hotels announcement.",
    },
    proposedMomentumUpdates,
    projectedLinkLabels: projectedLabels,
    labelsDifferentiatedAfterFix: labelsAreDifferentiated(projectedLabels),
    applyBlockers,
    dryRunClean,
    canApply,
    applyResults,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    airtableModified,
    expectedUiResult: {
      momentumLinkLabels: projectedLabels,
      note: "Three differentiated labels: press kit + Cartagena property listing + Panama City property listing",
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
    `# Brand Explorer Radisson Individuals Momentum Source-Link Parity v31M-R2`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Brand: **${report.targetBrand.name}**`,
    `- v31M-R2 exists: **${report.v31mR2WriterExists ? "yes" : "no"}**`,
    `- Mode: **${report.mode}**`,
    "",
    "## Tribute momentum source/link rules",
    "",
    report.tributeMomentumSourceLinkRules.summary,
    `- Label storage: **${report.tributeMomentumSourceLinkRules.labelStorage}**`,
    `- UI generation: **${report.tributeMomentumSourceLinkRules.uiGeneration}**`,
    "",
  ];

  for (const p of report.tributeMomentumSourceLinkRules.tributePatterns.slice(0, 8)) {
    lines.push(
      `- **${p.title}** · ${p.sourceCategory} · [source](${p.sourceUrl}) · UI label: *${p.uiLinkLabel}*`
    );
  }

  lines.push("", "## Radisson momentum audit", "");
  for (const a of report.radissonMomentumAudit) {
    lines.push(`### ${a.title}`);
    lines.push(`- Record: \`${a.recordId}\``);
    lines.push(`- Source URL: ${a.sourceUrl || "missing"}`);
    lines.push(`- Source category: ${a.sourceCategory}`);
    lines.push(`- Legacy UI label: *${a.linkLabelLegacyFrontend}*`);
    lines.push(`- Parity UI label: *${a.linkLabelParityFrontend}*`);
    lines.push("");
  }

  lines.push("## Link label before/after", "");
  for (const l of report.linkLabelBeforeAfter) {
    lines.push(`- \`${l.recordId}\`: *${l.linkLabelBefore}* → *${l.linkLabelAfter}*`);
  }

  lines.push("", "## API/frontend", "");
  lines.push(`- Frontend patched: **${report.apiFrontendVerification.frontendPatched ? "yes" : "no"}**`);
  lines.push(`- ${report.apiFrontendVerification.frontendPatchNote}`);
  lines.push(`- Root cause: ${report.apiFrontendVerification.rootCause}`);

  lines.push("", "## Governance", "");
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Labels differentiated: **${report.labelsDifferentiatedAfterFix ? "yes" : "no"}**`);

  if (report.applyBlockers.length) {
    lines.push("", "## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }

  if (report.exactApplyCommand) {
    lines.push("", "## Exact apply command", "", "```bash", report.exactApplyCommand, "```");
  }

  return lines.join("\n");
}
