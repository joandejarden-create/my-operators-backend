/**
 * Design Hotels External Owner Content Cleanup v35F-R1.
 *
 * Removes visible source citations, URLs, and internal governance language from
 * owner-facing presentation copy while preserving source traceability in reports.
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import {
  DESIGN_HOTELS_PROPERTY_CATALOG,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";
import {
  buildCalaPropertyOpeningTitle,
  CALA_SECTION_LABEL_DEFAULT,
} from "./brand-explorer-cala-property-example-rules.js";
import {
  auditPresentationRowExternalOwner,
  extractSourceFootnote,
  sanitizeAffiliationExternalCopy,
  auditExternalOwnerPhrase,
} from "./brand-explorer-external-owner-content-governance.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";

export const V35F_R1_VERSION = "v35F-R1";
export const STAGING_RUN_ID = "v35F-R1-design-hotels-external-owner-cleanup";

export const TARGET_BRAND = Object.freeze({
  slug: "design-hotels",
  recordId: "rec02zPClpWUTCyXM",
  name: "Design Hotels",
});

export const PROPERTY_RECORD_IDS = Object.freeze({
  "wake-biohotel": "rec5sNCVcRGZfTwbV",
  "condesa-df": "rec59aTn7CDtoZN7O",
  carlota: "recLtxEB4hSVkLuWl",
});

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v35F-R1-design-hotels-external-owner-cleanup";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_ACTIVE = "--confirm-no-active-profile-approval";
export const APPLY_FLAG_NO_SUMMARY = "--confirm-no-summary-url-field";
export const APPLY_FLAG_TRACEABILITY = "--confirm-source-traceability-preserved";
export const APPLY_FLAG_NO_VISIBLE_URLS = "--confirm-no-visible-source-urls-in-owner-copy";
export const APPLY_FLAG_AFFILIATION = "--confirm-affiliation-not-franchise-language";
export const APPLY_FLAG_DESIGN_HOTELS_ONLY = "--confirm-design-hotels-only";

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

const SLOT_EXTERNAL_BODY_REWRITES = Object.freeze({
  "economics.intro":
    "Design Hotels does not publish a full participation cost schedule or property-level operating detail in public materials. This section orients owner diligence categories—not confidential commercial terms.",
  "standards.intro":
    "Public Design Hotels materials describe a curated member collection—not a published owner participation checklist. Owners should confirm participation criteria, design review, distribution requirements, brand standards, agreement structure, and operating implications directly with the brand platform.",
  "economics.fee.change":
    "Renewal or repositioning standards review; membership exit or transfer provisions; design refresh or collection compliance updates. Confirm agreement structure and change triggers with counsel—not a published commercial terms schedule.",
  "standards.source_confidence": "Curated by Dealality from official public brand materials.",
  "loyalty.implications.2":
    "Dealality does not publish property-level loyalty performance guarantees for Design Hotels on this page.",
  "overview.differentiators.commercial":
    [
      "Selective participation in Marriott Bonvoy where property-level agreements support it—benefits and participation may vary by member hotel.",
      "Collection affiliation can support discovery through Marriott channels without implying a single uniform commercial package for every property.",
      "Owners should diligence distribution participation, recognition mechanics, and commercial fit during affiliation evaluation.",
      "Public materials describe a global member directory and collection positioning—not property-level performance representations.",
    ].join("\n"),
});

const SKIP_SLOT_PREFIXES = ["materials.gallery."];

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

export function parseCliFlags(argv = process.argv.slice(2)) {
  return {
    brandArg: argv.find((a) => !a.startsWith("--") && a !== "--apply") || TARGET_BRAND.slug,
    apply: argv.includes("--apply"),
    approveCleanup: argv.includes(APPLY_FLAG_APPROVE),
    confirmNoValidation: argv.includes(APPLY_FLAG_NO_VALIDATION),
    confirmNoActive: argv.includes(APPLY_FLAG_NO_ACTIVE),
    confirmNoSummary: argv.includes(APPLY_FLAG_NO_SUMMARY),
    confirmTraceability: argv.includes(APPLY_FLAG_TRACEABILITY),
    confirmNoVisibleUrls: argv.includes(APPLY_FLAG_NO_VISIBLE_URLS),
    confirmAffiliation: argv.includes(APPLY_FLAG_AFFILIATION),
    confirmDesignHotelsOnly: argv.includes(APPLY_FLAG_DESIGN_HOTELS_ONLY),
  };
}

export function buildApplyCommand() {
  return [
    "npm run brand-explorer-design-hotels-external-owner-cleanup --",
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
  ].join(" ");
}

function catalogByPropertyKey(key) {
  return DESIGN_HOTELS_PROPERTY_CATALOG.find((c) => c.propertyKey === key) || null;
}

export function buildDesignHotelsPropertyModalFields(catalog) {
  const loc = [catalog.marketCity, catalog.stateRegion].filter(Boolean).join(", ");
  const overview = `${catalog.propertyName} in ${loc} — a design-led member hotel illustrating ${catalog.meta?.replace(/Property Example · /i, "") || "collection fit"}.`;
  const relevance =
    catalog.ownerRelevance ||
    catalog.teaser ||
    "Reference for how Design Hotels curates independent hotels with distinctive local identity.";
  const suggests =
    "Illustrates curation of independent character, design narrative, and place-making—not a uniform chain prototype or performance benchmark.";
  const takeaway =
    "Use as an affiliation-fit reference only; confirm participation criteria, design standards, distribution scope, and commercial terms directly with Design Hotels.";
  const tags = catalog.chips || "CALA, Design-led, Independent";
  const body = [
    tags,
    loc,
    catalog.meta || "CALA Property Example",
    catalog.scenario || "PROPERTY EXAMPLE / CALA",
    catalog.teaser || relevance,
  ]
    .filter(Boolean)
    .join("\n\n");

  return {
    Title: buildCalaPropertyOpeningTitle(catalog),
    Body: body,
    "Case Summary Overview": overview,
    "Case Summary Brand Relevance": relevance,
    "Case Summary Owner Objective": suggests,
    "Case Summary Interpretation": takeaway,
    "Case Summary Tags": tags,
  };
}

function sanitizeCaseSummaryField(value, sourcesById) {
  return sanitizeAffiliationExternalCopy(nz(value), { sourcesById });
}

function buildRowPatch(row, approvedSourcesById) {
  const slotKey = nz(row.slotKey);
  if (SKIP_SLOT_PREFIXES.some((p) => slotKey.startsWith(p))) return null;

  const trace = extractSourceFootnote(row.body || "", approvedSourcesById);
  const sourceIds =
    trace.sourceIds.length > 0
      ? trace.sourceIds
      : [...approvedSourcesById.keys()].slice(0, 1);

  let fields = {
    "Slot Key": slotKey,
    Title: sanitizeAffiliationExternalCopy(row.title || "", { slotKey, sourcesById: approvedSourcesById }),
    Body: sanitizeAffiliationExternalCopy(row.body || "", { slotKey, sourcesById: approvedSourcesById }),
    "Sort Order": row.sortOrder ?? 0,
  };

  if (SLOT_EXTERNAL_BODY_REWRITES[slotKey]) {
    fields.Body = SLOT_EXTERNAL_BODY_REWRITES[slotKey];
  }

  if (!nz(fields.Body) && nz(fields.Title) && slotKey !== "footprint.openings") {
    fields.Body = fields.Title;
  }

  if (row.caseSummaryOverview) {
    fields["Case Summary Overview"] = sanitizeAffiliationExternalCopy(row.caseSummaryOverview, {
      slotKey,
      sourcesById: approvedSourcesById,
    });
  }
  if (row.caseSummaryBrandRelevance) {
    fields["Case Summary Brand Relevance"] = sanitizeAffiliationExternalCopy(row.caseSummaryBrandRelevance, {
      slotKey,
      sourcesById: approvedSourcesById,
    });
  }
  if (row.caseSummaryOwnerObjective) {
    fields["Case Summary Owner Objective"] = sanitizeAffiliationExternalCopy(row.caseSummaryOwnerObjective, {
      slotKey,
      sourcesById: approvedSourcesById,
    });
  }
  if (row.caseSummaryInterpretation) {
    fields["Case Summary Interpretation"] = sanitizeAffiliationExternalCopy(row.caseSummaryInterpretation, {
      slotKey,
      sourcesById: approvedSourcesById,
    });
  }
  if (row.caseSummaryTags) {
    fields["Case Summary Tags"] = sanitizeAffiliationExternalCopy(row.caseSummaryTags, {
      slotKey,
      sourcesById: approvedSourcesById,
    });
  }

  const propertyKey = Object.entries(PROPERTY_RECORD_IDS).find(([, id]) => id === row.recordId)?.[0];
  if (slotKey === "footprint.openings" && propertyKey) {
    const catalog = catalogByPropertyKey(propertyKey);
    if (catalog) {
      const modal = buildDesignHotelsPropertyModalFields(catalog);
      fields = {
        ...fields,
        Title: modal.Title,
        Body: modal.Body,
        "Case Summary Overview": modal["Case Summary Overview"],
        "Case Summary Brand Relevance": modal["Case Summary Brand Relevance"],
        "Case Summary Owner Objective": modal["Case Summary Owner Objective"],
        "Case Summary Interpretation": modal["Case Summary Interpretation"],
        "Case Summary Tags": modal["Case Summary Tags"],
      };
    }
  }

  const beforeCombined = [row.title, row.body].join("\n");
  const afterCombined = [fields.Title, fields.Body].join("\n");
  const changed = beforeCombined !== afterCombined || propertyKey != null;
  const afterHits = auditExternalOwnerPhrase(afterCombined, slotKey).filter(
    (h) => !(h.patternId === "http_url" && slotKey === "footprint.openings")
  );
  if (!changed && !afterHits.length) return null;

  return {
    recordId: row.recordId,
    slotKey,
    before: {
      title: row.title,
      body: row.body,
      caseSummaryOverview: row.caseSummaryOverview,
    },
    after: fields,
    internalSourceTrace: {
      extractedFootnote: trace.sourceFootnote,
      sourceIds,
      approvedSources: sourceIds.map((id) => approvedSourcesById.get(id)).filter(Boolean),
    },
    remainingHits: afterHits,
    reason: "v35F-R1 external owner cleanup",
  };
}

function planPatches(rows, approvedSourcesById) {
  const patches = [];
  const audits = [];
  const founderReviewQueue = [];

  for (const row of rows.filter((r) => r.visible !== false && r.active !== false)) {
    const audit = auditPresentationRowExternalOwner(row, approvedSourcesById);
    audits.push(audit);
    const patch = buildRowPatch(row, approvedSourcesById);
    if (patch) {
      if (patch.remainingHits.length) {
        founderReviewQueue.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          reason: "unsafe_after_sanitize",
          hits: patch.remainingHits,
        });
      } else {
        patches.push(patch);
      }
    } else if (audit.hits.length) {
      founderReviewQueue.push({
        recordId: audit.recordId,
        slotKey: audit.slotKey,
        reason: "audit_hit_no_safe_patch",
        hits: audit.hits,
      });
    }
  }

  return { patches, audits, founderReviewQueue };
}

function validateApplyBlockers({ flags, companyValidatedBefore, brandBasics, patches, projectedRows }) {
  const blockers = [];
  if (flags.apply) {
    if (!flags.approveCleanup) blockers.push("missing_approve_v35F_R1_design_hotels_external_owner_cleanup");
    if (!flags.confirmNoValidation) blockers.push("missing_confirm_no_company_validation_claim");
    if (!flags.confirmNoActive) blockers.push("missing_confirm_no_active_profile_approval");
    if (!flags.confirmNoSummary) blockers.push("missing_confirm_no_summary_url_field");
    if (!flags.confirmTraceability) blockers.push("missing_confirm_source_traceability_preserved");
    if (!flags.confirmNoVisibleUrls) blockers.push("missing_confirm_no_visible_source_urls_in_owner_copy");
    if (!flags.confirmAffiliation) blockers.push("missing_confirm_affiliation_not_franchise_language");
    if (!flags.confirmDesignHotelsOnly) blockers.push("missing_confirm_design_hotels_only");
  }

  const cv = companyValidatedSnapshot(brandBasics);
  if (cv.companyValidated !== companyValidatedBefore.companyValidated) {
    blockers.push("company_validated_would_change");
  }

  for (const patch of patches) {
    if (!patch.internalSourceTrace?.sourceIds?.length && !patch.internalSourceTrace?.extractedFootnote) {
      if (patch.internalSourceTrace?.approvedSources?.length === 0) {
        blockers.push(`missing_source_traceability:${patch.slotKey}`);
      }
    }
    const combined = `${patch.after.Title}\n${patch.after.Body}`;
    const hits = auditExternalOwnerPhrase(combined, patch.slotKey).filter(
      (h) => !(h.patternId === "http_url" && patch.slotKey === "footprint.openings")
    );
    if (hits.some((h) => h.severity === "critical" || h.severity === "high")) {
      blockers.push(`external_owner_hit_planned:${patch.slotKey}:${hits.map((h) => h.patternId).join(",")}`);
    }
    for (const field of Object.keys(patch.after)) {
      if (BLOCKED_PRESENTATION_FIELDS.has(field)) blockers.push(`blocked_field:${field}`);
    }
  }

  const readiness = evaluateExternalOwnerReadinessRule(projectedRows);
  if (flags.apply && !readiness.pass) {
    blockers.push(`external_owner_readiness_failed:${readiness.blockers.join(";")}`);
  }

  return blockers;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Design Hotels External Owner Cleanup ${V35F_R1_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}**`);
  lines.push("");
  lines.push("## External owner audit summary");
  lines.push(`- Rows audited: **${report.rowsAudited}**`);
  lines.push(`- Rows with hits (before): **${report.rowsWithHitsBefore}**`);
  lines.push(`- Rows patched: **${report.rowsPatched.length}**`);
  lines.push(`- Founder review queue: **${report.founderReviewQueue.length}**`);
  lines.push("");
  lines.push("## Property modal completeness");
  for (const p of report.propertyModalAudit || []) {
    lines.push(`- **${p.propertyName}** (\`${p.recordId}\`): ${p.status}`);
  }
  lines.push("");
  lines.push("## Rows patched");
  for (const p of report.rowsPatched) {
    lines.push(`- \`${p.slotKey}\` (\`${p.recordId}\`) — ${p.reason}`);
  }
  if (!report.rowsPatched.length) lines.push("- (none)");
  lines.push("");
  lines.push("## Audit hits (before → proposed fix)");
  for (const a of (report.auditsBefore || []).filter((x) => x.hits?.length).slice(0, 40)) {
    lines.push(`- \`${a.slotKey}\` (\`${a.recordId}\`): ${a.hits.map((h) => h.patternId).join(", ")}`);
  }
  lines.push("");
  lines.push("## Source traceability preserved (internal report only)");
  lines.push(`- Approved sources in library: **${report.approvedSourceCount}**`);
  lines.push(`- Patches with source trace: **${report.sourceTraceCount}**`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "NO"}**`);
  lines.push(`- Active profile approval: **not set**`);
  lines.push(`- Proof label: **${report.proofSectionLabelAfter}**`);
  lines.push("");
  lines.push("## External owner readiness");
  lines.push(`- Pass: **${report.externalOwnerReadiness?.pass ? "yes" : "no"}**`);
  if (report.externalOwnerReadiness?.emptyCardRows?.length) {
    lines.push("- Empty visible cards:");
    for (const c of report.externalOwnerReadiness.emptyCardRows) {
      lines.push(`  - \`${c.slotKey}\` (\`${c.recordId}\`) — ${c.title}`);
    }
  }
  if (report.externalOwnerReadiness?.blockers?.length) {
    for (const b of report.externalOwnerReadiness.blockers) lines.push(`  - ${b}`);
  }
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
    const externalDisplayStatus = nz(f["External Display Status"]);
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"] ?? 0,
      active: f.Active !== false,
      externalDisplayStatus,
      visible:
        f.Active !== false && !/do not display|internal only/i.test(externalDisplayStatus),
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

async function airtableWrite(baseId, apiKey, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable PATCH failed: ${res.status}`);
  return json;
}

function auditPropertyModals(rows) {
  return Object.entries(PROPERTY_RECORD_IDS).map(([key, recordId]) => {
    const row = rows.find((r) => r.recordId === recordId);
    const catalog = catalogByPropertyKey(key);
    if (!row || !catalog) {
      return { propertyName: key, recordId, status: "missing_row_or_catalog" };
    }
    const fields = buildDesignHotelsPropertyModalFields(catalog);
    const missing = [
      "Case Summary Overview",
      "Case Summary Brand Relevance",
      "Case Summary Owner Objective",
      "Case Summary Interpretation",
      "Case Summary Tags",
    ].filter((f) => !nz(fields[f]));
    return {
      propertyName: catalog.propertyName,
      recordId,
      status: missing.length ? `needs_fields:${missing.join(",")}` : "ready_after_patch",
    };
  });
}

export async function buildDesignHotelsExternalOwnerCleanupV35FR1Report(options = {}) {
  const flags = options.flags || parseCliFlags(options.argv);
  const brand = TARGET_BRAND;
  if (flags.brandArg !== brand.slug && flags.brandArg !== brand.recordId) {
    throw new Error(`v35F-R1 supports Design Hotels only; got: ${flags.brandArg}`);
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
  const plan = planPatches(presentationRows, sourceLoad.byId);

  const projectedRows = presentationRows.map((r) => {
    const patch = plan.patches.find((p) => p.recordId === r.recordId);
    if (!patch) return { ...r };
    return {
      ...r,
      title: patch.after.Title,
      body: patch.after.Body,
      caseSummaryOverview: patch.after["Case Summary Overview"] || r.caseSummaryOverview,
      caseSummaryBrandRelevance: patch.after["Case Summary Brand Relevance"] || r.caseSummaryBrandRelevance,
      caseSummaryOwnerObjective: patch.after["Case Summary Owner Objective"] || r.caseSummaryOwnerObjective,
      caseSummaryInterpretation: patch.after["Case Summary Interpretation"] || r.caseSummaryInterpretation,
      caseSummaryTags: patch.after["Case Summary Tags"] || r.caseSummaryTags,
    };
  });

  const applyBlockers = validateApplyBlockers({
    flags,
    companyValidatedBefore,
    brandBasics,
    patches: plan.patches,
    projectedRows,
  });
  const dryRunClean = applyBlockers.length === 0;
  const canApply = flags.apply && dryRunClean;

  const applyResults = { patched: [], errors: [] };
  if (canApply) {
    for (const patch of plan.patches) {
      try {
        const fields = {
          ...patch.after,
          "Slot Key": patch.slotKey,
          Brand: [brand.recordId],
          "Brand Name": brand.name,
          Active: true,
        };
        await airtableWrite(baseId, apiKey, patch.recordId, fields);
        applyResults.patched.push({ recordId: patch.recordId, slotKey: patch.slotKey });
      } catch (err) {
        applyResults.errors.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          message: err.message,
        });
      }
    }
  }

  const externalOwnerReadiness = evaluateExternalOwnerReadinessRule(projectedRows);
  const propertyModalAudit = auditPropertyModals(projectedRows);

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

  const brandBasicsAfter = canApply ? await fetchBrandBasics(brand.recordId) : brandBasics;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const report = {
    version: V35F_R1_VERSION,
    stagingRunId: STAGING_RUN_ID,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand,
    flags,
    approvedSourceCount: sourceLoad.approved.length,
    rowsAudited: presentationRows.filter((r) => r.visible !== false).length,
    rowsWithHitsBefore: plan.audits.filter((a) => a.hits.length).length,
    auditsBefore: plan.audits,
    rowsPatched: plan.patches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      reason: p.reason,
      internalSourceTrace: p.internalSourceTrace,
    })),
    founderReviewQueue: plan.founderReviewQueue,
    sourceTraceCount: plan.patches.filter((p) => p.internalSourceTrace?.sourceIds?.length).length,
    internalSourceTrace: plan.patches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      ...p.internalSourceTrace,
    })),
    propertyModalAudit,
    externalOwnerReadiness,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      companyValidatedBefore.companyValidated === companyValidatedAfter.companyValidated,
    activeProfileApproved: false,
    proofSectionLabelAfter: companyValidatedBefore.companyValidated
      ? "Brand-Verified Content · Curated by Dealality"
      : "AI-Assisted from Official Public Sources · Curated by Dealality",
    expectedVisualDefects: expectedVisualDefects ? { defectCounts: expectedVisualDefects.defectCounts } : null,
    expectedFinalQa: expectedFinalQa
      ? { scores: expectedFinalQa.scores, overallActiveProfileReadiness: expectedFinalQa.scores?.overallActiveProfileReadiness }
      : null,
    applyBlockers,
    dryRunClean,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    applyResults: canApply ? applyResults : null,
    airtableModified: canApply && applyResults.errors.length === 0,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export { buildMarkdown, parseCliFlags as parseDesignHotelsExternalOwnerCleanupFlags };
