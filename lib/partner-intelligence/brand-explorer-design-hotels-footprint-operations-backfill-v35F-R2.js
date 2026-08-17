/**
 * Design Hotels Footprint + Operations Backfill v35F-R2.
 *
 * Backfills missing Footprint & Growth and Operating Model presentation slots
 * and normalizes region card format. Census alias + footprint patch is a
 * companion script: apply-design-hotels-census-footprint-fix.mjs
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import {
  TARGET_BRAND,
  APPLY_FLAG_NO_VALIDATION,
  APPLY_FLAG_NO_ACTIVE,
  APPLY_FLAG_NO_SUMMARY,
  APPLY_FLAG_AFFILIATION,
  APPLY_FLAG_DESIGN_HOTELS_ONLY,
} from "./brand-explorer-design-hotels-external-owner-cleanup-v35F-R1.js";
import {
  auditExternalOwnerPhrase,
  sanitizeAffiliationExternalCopy,
} from "./brand-explorer-external-owner-content-governance.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import {
  buildDesignHotelsFootprintOperationsContentPackagesV35FR2,
  regionBodyHasDirectionalFormat,
  V35F_R2_CONTENT_VERSION,
  V35F_R2_SLOT_KEYS,
} from "./brand-explorer-design-hotels-footprint-operations-content-v35F-R2.js";

export const V35F_R2_VERSION = "v35F-R2";
export const STAGING_RUN_ID = "v35F-R2-design-hotels-footprint-operations-backfill";

export { TARGET_BRAND };

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v35F-R2-design-hotels-footprint-operations-backfill";
export const APPLY_FLAG_NO_VISIBLE_URLS = "--confirm-no-visible-source-urls-in-owner-copy";
export const APPLY_FLAG_TRACEABILITY = "--confirm-source-traceability-preserved";
export const APPLY_FLAG_CENSUS = "--confirm-cala-census-footprint-companion-run";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "External Display Status",
  "Ready for Active Profile",
  "Active Profile Approved",
]);

const UNSAFE_COPY_RE = Object.freeze([
  { id: "franchise_flag", re: /\bfranchise flag\b/i },
  { id: "fdd", re: /\b(fdd|item\s*19|franchise disclosure)\b/i },
  { id: "fee_stack", re: /\bfee stack\b/i },
  { id: "adr", re: /\badr\b/i },
  { id: "revpar", re: /\brevpar\b/i },
  { id: "net_contribution", re: /\bnet contribution\b/i },
  { id: "brand_verified_wording", re: /\bbrand-verified content\b/i },
  { id: "company_validated_claim", re: /\bcompany validated\b|\bcompany-approved\b|\bofficial sign-off\b/i },
  { id: "performance_guarantee", re: /\brooms from loyalty\b|\brepeat demand guarantee\b|\bguaranteed returns?\b/i },
  { id: "sources_block", re: /\n\nSources:\s/i },
  { id: "http_url", re: /https?:\/\//i },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: Boolean(fields["Company Validated"]),
    companyValidationDate: fields["Company Validation Date"] || null,
  };
}

function parseCliFlags(argv = process.argv.slice(2)) {
  return {
    brandArg: argv.find((a) => !a.startsWith("--") && a !== "--apply") || TARGET_BRAND.slug,
    apply: argv.includes("--apply"),
    approveBackfill: argv.includes(APPLY_FLAG_APPROVE),
    confirmNoValidation: argv.includes(APPLY_FLAG_NO_VALIDATION),
    confirmNoActive: argv.includes(APPLY_FLAG_NO_ACTIVE),
    confirmNoSummary: argv.includes(APPLY_FLAG_NO_SUMMARY),
    confirmTraceability: argv.includes(APPLY_FLAG_TRACEABILITY),
    confirmNoVisibleUrls: argv.includes(APPLY_FLAG_NO_VISIBLE_URLS),
    confirmAffiliation: argv.includes(APPLY_FLAG_AFFILIATION),
    confirmDesignHotelsOnly: argv.includes(APPLY_FLAG_DESIGN_HOTELS_ONLY),
    confirmCensus: argv.includes(APPLY_FLAG_CENSUS),
  };
}

export function buildApplyCommand() {
  return [
    "npm run brand-explorer-design-hotels-footprint-operations-backfill --",
    `--brand ${TARGET_BRAND.slug}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_ACTIVE,
    APPLY_FLAG_NO_SUMMARY,
    APPLY_FLAG_TRACEABILITY,
    APPLY_FLAG_NO_VISIBLE_URLS,
    APPLY_FLAG_AFFILIATION,
    APPLY_FLAG_DESIGN_HOTELS_ONLY,
    APPLY_FLAG_CENSUS,
  ].join(" ");
}

export function validateCopySafety(text) {
  const errors = [];
  const blob = nz(text);
  if (!blob) errors.push("empty_copy");

  for (const pat of UNSAFE_COPY_RE) {
    if (pat.id === "franchise_flag") {
      if (
        /\bfranchise flag\b/i.test(blob) &&
        !/\b(not a|not an|without|avoid|non-)\b[\s\S]{0,40}\bfranchise flag\b/i.test(blob)
      ) {
        errors.push(pat.id);
      }
      continue;
    }
    if (pat.id === "fdd") {
      if (
        pat.re.test(blob) &&
        !/\b(no|not|without|avoid|do not|does not|no published|not a published)\b/i.test(blob)
      ) {
        errors.push(pat.id);
      }
      continue;
    }
    if (pat.id === "adr" || pat.id === "revpar" || pat.id === "net_contribution") {
      if (pat.re.test(blob) && !/\b(no|not|without|avoid|does not|do not)\b/i.test(blob)) {
        errors.push(pat.id);
      }
      continue;
    }
    if (pat.re.test(blob)) errors.push(pat.id);
  }

  if (/\binternal extraction\b|\bpaste into airtable\b/i.test(blob)) errors.push("internal");
  return { valid: errors.length === 0, errors };
}

function presentationFields({ slotKey, title, body, sort, brandRecordId, brandName }) {
  return {
    "Slot Key": slotKey,
    Title: title || "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort ?? 0,
  };
}

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);

  return rows.map((rec) => {
    const f = rec.fields || {};
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"] ?? 0,
      active: f.Active !== false,
      visible: f.Active !== false,
      caseSummaryOverview: nz(f["Case Summary Overview"]),
      caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
      caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
      caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
      caseSummaryTags: nz(f["Case Summary Tags"]),
    };
  });
}

async function loadApprovedSources(brandRecordId) {
  const sources = [];
  let offset;
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    sources.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  const approved = sources.filter(isApprovedExplorerSource);
  const byId = new Map(
    approved.map((s) => [
      s.id || s.recordId,
      {
        id: s.id || s.recordId,
        sourceTitle: s.sourceTitle || s.title,
        sourceUrl: s.sourceUrl || s.url,
      },
    ])
  );
  return { approved, byId };
}

function sanitizePackageBody(body, slotKey, sourcesById) {
  return sanitizeAffiliationExternalCopy(body, { slotKey, sourcesById });
}

function needsRegionFormatPatch(row, pkg) {
  if (!row) return false;
  if (row.slotKey.startsWith("footprint.region.")) {
    return !regionBodyHasDirectionalFormat(row.body) || nz(row.body) !== nz(pkg.body);
  }
  return nz(row.body) !== nz(pkg.body) || (pkg.title && nz(row.title) !== nz(pkg.title));
}

function planBackfill(presentationRows, packages, brandRecordId, brandName, sourcesById) {
  const creates = [];
  const patches = [];
  const founderReviewQueue = [];
  const missingBefore = [];
  const presentBefore = [];

  for (const slotKey of V35F_R2_SLOT_KEYS) {
    const existing = presentationRows.filter((r) => r.slotKey === slotKey && r.visible !== false);
    if (existing.length) presentBefore.push(slotKey);
    else missingBefore.push(slotKey);
  }

  for (const pkg of packages) {
    const body = sanitizePackageBody(pkg.body, pkg.slotKey, sourcesById);
    const safety = validateCopySafety(`${pkg.title}\n${body}`);
    const ownerHits = auditExternalOwnerPhrase(`${pkg.title}\n${body}`, pkg.slotKey);

    if (!safety.valid) {
      founderReviewQueue.push({ slotKey: pkg.slotKey, reason: "unsafe_copy_blocked", errors: safety.errors });
      continue;
    }
    if (ownerHits.some((h) => h.severity === "critical" || h.severity === "high")) {
      founderReviewQueue.push({
        slotKey: pkg.slotKey,
        reason: "external_owner_hit",
        hits: ownerHits,
      });
      continue;
    }
    if (!pkg.sourceIds?.length) {
      founderReviewQueue.push({ slotKey: pkg.slotKey, reason: "missing_source_support" });
      continue;
    }

    const existing = presentationRows
      .filter((r) => r.slotKey === pkg.slotKey && r.visible !== false)
      .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));
    const rowAtSort =
      existing.find((r) => (r.sortOrder ?? 0) === (pkg.sort ?? 0)) ||
      (existing.length > (pkg.sort ?? 0) ? existing[pkg.sort ?? 0] : existing[0] || null);

    const fields = presentationFields({
      slotKey: pkg.slotKey,
      title: pkg.title,
      body,
      sort: pkg.sort,
      brandRecordId,
      brandName,
    });

    if (!rowAtSort) {
      creates.push({
        slotKey: pkg.slotKey,
        tab: pkg.tab,
        fields,
        sourceIds: pkg.sourceIds,
        reason: "v35F-R2 create missing slot",
      });
      continue;
    }

    if (needsRegionFormatPatch(rowAtSort, { ...pkg, body })) {
      patches.push({
        recordId: rowAtSort.recordId,
        slotKey: pkg.slotKey,
        tab: pkg.tab,
        before: { title: rowAtSort.title, body: rowAtSort.body },
        after: { title: pkg.title || rowAtSort.title, body },
        fields: presentationFields({
          slotKey: pkg.slotKey,
          title: pkg.title || rowAtSort.title,
          body,
          sort: pkg.sort ?? rowAtSort.sortOrder,
          brandRecordId,
          brandName,
        }),
        sourceIds: pkg.sourceIds,
        reason: rowAtSort.slotKey.startsWith("footprint.region.")
          ? "v35F-R2 normalize region card format"
          : "v35F-R2 patch slot body",
      });
    }
  }

  return { creates, patches, founderReviewQueue, missingBefore, presentBefore };
}

function validateApplyBlockers({ flags, companyValidatedBefore, brandBasics, plan, projectedRows }) {
  const blockers = [];

  if (flags.apply) {
    if (!flags.approveBackfill) blockers.push("missing_approve_v35F_R2_design_hotels_footprint_operations_backfill");
    if (!flags.confirmNoValidation) blockers.push("missing_confirm_no_company_validation_claim");
    if (!flags.confirmNoActive) blockers.push("missing_confirm_no_active_profile_approval");
    if (!flags.confirmNoSummary) blockers.push("missing_confirm_no_summary_url_field");
    if (!flags.confirmTraceability) blockers.push("missing_confirm_source_traceability_preserved");
    if (!flags.confirmNoVisibleUrls) blockers.push("missing_confirm_no_visible_source_urls_in_owner_copy");
    if (!flags.confirmAffiliation) blockers.push("missing_confirm_affiliation_not_franchise_language");
    if (!flags.confirmDesignHotelsOnly) blockers.push("missing_confirm_design_hotels_only");
    if (!flags.confirmCensus) blockers.push("missing_confirm_cala_census_footprint_companion_run");
  }

  const cv = companyValidatedSnapshot(brandBasics);
  if (cv.companyValidated !== companyValidatedBefore.companyValidated) {
    blockers.push("company_validated_would_change");
  }

  for (const item of [...plan.creates, ...plan.patches]) {
    const safety = validateCopySafety(`${item.fields?.Title || ""}\n${item.fields?.Body || ""}`);
    if (!safety.valid) blockers.push(`unsafe_copy_planned:${item.slotKey}:${safety.errors.join(",")}`);
    if (!item.sourceIds?.length) blockers.push(`missing_source_support:${item.slotKey}`);
    for (const field of Object.keys(item.fields || {})) {
      if (BLOCKED_PRESENTATION_FIELDS.has(field)) blockers.push(`blocked_field:${field}`);
    }
    const hits = auditExternalOwnerPhrase(
      `${item.fields?.Title || ""}\n${item.fields?.Body || ""}`,
      item.slotKey
    );
    if (hits.some((h) => h.severity === "critical" || h.severity === "high")) {
      blockers.push(`external_owner_hit_planned:${item.slotKey}:${hits.map((h) => h.patternId).join(",")}`);
    }
  }

  if (flags.apply && plan.creates.length === 0 && plan.patches.length === 0) {
    blockers.push("no_presentation_changes_planned");
  }

  const readiness = evaluateExternalOwnerReadinessRule(projectedRows);
  if (flags.apply && !readiness.pass) {
    blockers.push(`external_owner_readiness_failed:${readiness.blockers.join(";")}`);
  }

  return blockers;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Design Hotels Footprint + Operations Backfill ${V35F_R2_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}**`);
  lines.push("");
  lines.push("## Scope");
  lines.push(`- Target slots: **${V35F_R2_SLOT_KEYS.length}**`);
  lines.push(`- Missing before: **${report.missingBefore.length}**`);
  lines.push(`- Present before: **${report.presentBefore.length}**`);
  lines.push("");
  lines.push("## Missing slots (before)");
  for (const s of report.missingBefore) lines.push(`- \`${s}\``);
  if (!report.missingBefore.length) lines.push("- (none)");
  lines.push("");
  lines.push("## Rows created");
  for (const c of report.rowsCreated) lines.push(`- \`${c.slotKey}\` (${c.tab})`);
  if (!report.rowsCreated.length) lines.push("- (none)");
  lines.push("");
  lines.push("## Rows patched");
  for (const p of report.rowsPatched) {
    lines.push(`- \`${p.slotKey}\` (\`${p.recordId}\`) — ${p.reason}`);
  }
  if (!report.rowsPatched.length) lines.push("- (none)");
  lines.push("");
  lines.push("## Founder review queue");
  for (const q of report.founderReviewQueue) lines.push(`- ${q.slotKey}: ${q.reason}`);
  if (!report.founderReviewQueue.length) lines.push("- (none)");
  lines.push("");
  lines.push("## Census companion (run separately before or after apply)");
  lines.push("```bash");
  lines.push("npm run apply-design-hotels-census-footprint-fix -- --dry-run");
  lines.push("npm run apply-design-hotels-census-footprint-fix");
  lines.push("```");
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "NO"}**`);
  lines.push(`- Active profile approval: **not set**`);
  lines.push(`- External owner ready (projected): **${report.externalOwnerReadiness?.pass ? "yes" : "no"}**`);
  lines.push("");
  if (report.exactApplyCommand) {
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  if (report.applyBlockers.length) {
    lines.push("");
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  return lines.join("\n");
}

async function airtableWrite(baseId, apiKey, recordId, fields, method) {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable ${method} failed: ${res.status}`);
  return json;
}

export async function buildDesignHotelsFootprintOperationsBackfillV35FR2Report(options = {}) {
  const flags = options.flags || parseCliFlags(options.argv);
  const brand = TARGET_BRAND;
  if (flags.brandArg !== brand.slug && flags.brandArg !== brand.recordId) {
    throw new Error(`v35F-R2 supports Design Hotels only; got: ${flags.brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const [brandBasics, presentationRows, sourceLoad] = await Promise.all([
    fetchBrandBasics(brand.recordId),
    listPresentationRows(baseId, apiKey, brand.recordId, brand.name),
    loadApprovedSources(brand.recordId),
  ]);

  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);
  const packages = buildDesignHotelsFootprintOperationsContentPackagesV35FR2(sourceLoad.byId);
  const plan = planBackfill(
    presentationRows,
    packages,
    brand.recordId,
    brand.name,
    sourceLoad.byId
  );

  const projectedRows = presentationRows.map((r) => ({ ...r }));
  for (const patch of plan.patches) {
    const row = projectedRows.find((x) => x.recordId === patch.recordId);
    if (row) {
      row.title = patch.after.title;
      row.body = patch.after.body;
    }
  }
  for (const create of plan.creates) {
    projectedRows.push({
      recordId: `v35f-r2-draft-${create.slotKey}`,
      slotKey: create.slotKey,
      title: create.fields.Title,
      body: create.fields.Body,
      sortOrder: create.fields["Sort Order"],
      visible: true,
      caseSummaryOverview: "",
      caseSummaryOwnerObjective: "",
      caseSummaryBrandRelevance: "",
      caseSummaryInterpretation: "",
      caseSummaryTags: "",
    });
  }

  const externalOwnerReadiness = evaluateExternalOwnerReadinessRule(projectedRows);
  const applyBlockers = validateApplyBlockers({
    flags,
    companyValidatedBefore,
    brandBasics,
    plan,
    projectedRows,
  });

  const dryRunClean = applyBlockers.length === 0;
  const canApply = flags.apply && dryRunClean;
  const applyResults = { created: [], patched: [], errors: [] };

  if (canApply) {
    for (const create of plan.creates) {
      try {
        const json = await airtableWrite(baseId, apiKey, "", create.fields, "POST");
        applyResults.created.push({ recordId: json.id, slotKey: create.slotKey });
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ slotKey: create.slotKey, message: err.message });
      }
    }
    for (const patch of plan.patches) {
      try {
        await airtableWrite(baseId, apiKey, patch.recordId, patch.fields, "PATCH");
        applyResults.patched.push({ recordId: patch.recordId, slotKey: patch.slotKey });
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, slotKey: patch.slotKey, message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(brand.recordId) : brandBasics;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  const companyValidatedUntouched =
    companyValidatedBefore.companyValidated === companyValidatedAfter.companyValidated &&
    companyValidatedBefore.companyValidationDate === companyValidatedAfter.companyValidationDate;

  let expectedVisualDefects = null;
  let expectedFinalQa = null;
  try {
    expectedVisualDefects = await buildBrandExplorerVisualDisplayDefectAuditReport({
      brandSlug: brand.slug,
      dryRun: true,
    });
    expectedFinalQa = await buildBrandExplorerFinalQaAuditorReport({ brandSlug: brand.slug, dryRun: true });
  } catch {
    /* optional QA projection */
  }

  const report = {
    version: V35F_R2_VERSION,
    contentVersion: V35F_R2_CONTENT_VERSION,
    stagingRunId: STAGING_RUN_ID,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand,
    flags,
    approvedSourceCount: sourceLoad.approved.length,
    targetSlotCount: V35F_R2_SLOT_KEYS.length,
    missingBefore: plan.missingBefore,
    presentBefore: plan.presentBefore,
    rowsCreated: plan.creates.map((c) => ({
      slotKey: c.slotKey,
      tab: c.tab,
      sourceIds: c.sourceIds,
      reason: c.reason,
    })),
    rowsPatched: plan.patches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      tab: p.tab,
      reason: p.reason,
      sourceIds: p.sourceIds,
    })),
    founderReviewQueue: plan.founderReviewQueue,
    sourceSupportUsed: sourceLoad.approved.map((s) => ({
      id: s.id || s.recordId,
      title: s.sourceTitle || s.title,
    })),
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    externalOwnerReadiness,
    expectedVisualDefects: expectedVisualDefects ? { defectCounts: expectedVisualDefects.defectCounts } : null,
    expectedFinalQa: expectedFinalQa
      ? { scores: expectedFinalQa.scores, overallActiveProfileReadiness: expectedFinalQa.scores?.overallActiveProfileReadiness }
      : null,
    applyBlockers,
    dryRunClean,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    applyResults: canApply ? applyResults : null,
    airtableModified: canApply && applyResults.errors.length === 0,
    censusCompanionScript: "npm run apply-design-hotels-census-footprint-fix",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export { parseCliFlags };
