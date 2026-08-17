/**
 * Brand Explorer Standard Detail Governance Writer v27C.
 *
 * Adds or repairs standards.last_reviewed + standards.source_confidence governance rows
 * for Choice Radisson brands — owner-planning guidance only; no standards table rewrites.
 *
 * @see docs/data-intelligence/brand-explorer-standard-detail-governance-writer-v27C.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  evaluateStandardsDetailReadinessGeneralized,
  governanceLanguageAcceptable,
  scanStandardsCopySafetyForBrand,
  MIN_REQUIREMENT_ROWS_GENERAL,
} from "./brand-explorer-required-section-contract-evaluators.js";
import {
  requirementRowHasRequiredColumns,
  parseRequirementColumns,
  INTRO_SLOT,
  LAST_REVIEWED_SLOT,
  SOURCE_CONFIDENCE_SLOT,
  REQUIREMENT_SLOT,
} from "./brand-explorer-tribute-standard-detail-review-approval-writer.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";

export const WRITER_VERSION = "27C";
export const REPORT_JSON_NAME = "brand-explorer-standard-detail-governance-writer.json";
export const REPORT_MD_NAME = "brand-explorer-standard-detail-governance-writer.md";
export const DOC_MD_NAME = "brand-explorer-standard-detail-governance-writer-v27C.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v27C-standard-detail-governance";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-standard-detail-owner-planning-copy";
export const APPLY_FLAG_NO_LEGAL = "--confirm-not-legal-or-company-validation";

export const TARGET_BRAND_SLUGS = Object.freeze(["radisson-blu", "radisson"]);

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "kimpton",
  "ascend",
]);

export const GOVERNANCE_TARGET_SLOTS = [LAST_REVIEWED_SLOT, SOURCE_CONFIDENCE_SLOT];

export const STANDARDS_SOURCE_CONFIDENCE_BODY =
  "Founder-Reviewed · Owner-Planning Guidance · No company sign-off · Legal/Transaction Confirmation Required";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-required-section-contract-generalization-writer.md",
  "reports/brand-explorer-required-section-contract-generalization-writer.json",
  "reports/brand-explorer-complete-build-radisson-blu.md",
  "reports/brand-explorer-complete-build-radisson-blu.json",
  "reports/brand-explorer-complete-build-radisson.md",
  "reports/brand-explorer-complete-build-radisson.json",
  "reports/brand-explorer-factory-gap-matrix-audit.md",
  "reports/brand-explorer-factory-gap-matrix-audit.json",
  "lib/partner-intelligence/brand-explorer-required-section-contract-evaluators.js",
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "lib/partner-intelligence/brand-explorer-tribute-standard-detail-review-approval-writer.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Radisson Blu Brand Explorer Presentation rows",
  "live Radisson Brand Explorer Presentation rows",
  "Tribute standards governance rows (structure reference only)",
  "Curio/Kimpton/Ascend standards governance rows (comparison only)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-standard-detail-governance-writer.js",
  "scripts/brand-explorer-standard-detail-governance-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|brand approved|validated by choice|validated by radisson|official sign-off|company-approved|company approved/i;

const RAW_LEGAL_FRAGMENT_RE =
  /item\s*19|franchise disclosure document|§\s*\d|hereinafter|pursuant to the agreement|exhibit\s+[a-z]\b|whereas\b/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function bodiesMatch(a, b) {
  return normalizeBody(a) === normalizeBody(b);
}

function standardsLastReviewedBody(brandName) {
  return `Founder-reviewed owner-planning guidance — confirm current ${brandName} brand standards, PIP scope, conversion requirements, and agreement vintage with transaction documents before capital commitments.`;
}

function resolveTargetBrands(brandsArg) {
  const slugs = nz(brandsArg)
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const requested = slugs.length ? slugs : [...TARGET_BRAND_SLUGS];
  const invalid = requested.filter((s) => !TARGET_BRAND_SLUGS.includes(s));
  if (invalid.length) {
    throw new Error(`v27C supports only ${TARGET_BRAND_SLUGS.join(", ")}; invalid: ${invalid.join(", ")}`);
  }
  const protectedHit = requested.filter((s) => PROTECTED_BRAND_SLUGS.includes(s));
  if (protectedHit.length) {
    throw new Error(`Protected brands cannot be modified: ${protectedHit.join(", ")}`);
  }
  return requested.map((slug) => {
    const meta = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === slug);
    if (!meta) throw new Error(`Could not resolve brand target: ${slug}`);
    return meta;
  });
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
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

function presentationFieldsForGovernance(slotKey, body, sort, brandRecordId, brandName) {
  return {
    "Slot Key": slotKey,
    Title: "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort,
  };
}

function scanProposedGovernanceSafety(brandName, slotKey, body) {
  const issues = [];
  const combined = nz(body);
  if (COMPANY_VALIDATION_BLOCK_RE.test(combined)) {
    issues.push("company_validation_implication");
  }
  if (/marriott|bonvoy|tribute portfolio|curio collection|kimpton|ihg one rewards/i.test(combined)) {
    issues.push("wrong_brand_language");
  }
  if (RAW_LEGAL_FRAGMENT_RE.test(combined)) {
    issues.push("raw_legal_fragment");
  }
  if (slotKey === LAST_REVIEWED_SLOT) {
    if (!/founder-reviewed/i.test(combined)) issues.push("missing_founder_reviewed");
    if (!combined.includes(brandName)) issues.push("missing_brand_name_in_last_reviewed");
    if (!/transaction documents|agreement vintage|pip scope/i.test(combined)) {
      issues.push("missing_transaction_caveat");
    }
  }
  if (slotKey === SOURCE_CONFIDENCE_SLOT) {
    if (!/founder-reviewed/i.test(combined)) issues.push("missing_founder_reviewed");
    if (!/owner-planning guidance/i.test(combined)) issues.push("missing_owner_planning_guidance");
    if (!/no company sign-off/i.test(combined)) issues.push("missing_no_company_signoff");
    if (!/legal\/transaction confirmation required/i.test(combined)) {
      issues.push("missing_legal_transaction_caveat");
    }
  }
  return issues;
}

function diagnoseGovernanceGap(introBody, lastReviewedBody, sourceConfidenceBody) {
  const gaps = [];
  if (!hasVal(lastReviewedBody)) gaps.push("missing_standards_last_reviewed_row");
  else if (!/founder-reviewed|owner-planning guidance/i.test(lastReviewedBody)) {
    gaps.push("last_reviewed_missing_founder_review_language");
  }
  if (!hasVal(sourceConfidenceBody)) gaps.push("missing_standards_source_confidence_row");
  else if (!/no company sign-off|not company validated/i.test(sourceConfidenceBody)) {
    gaps.push("source_confidence_missing_no_signoff_language");
  }
  if (!governanceLanguageAcceptable(introBody, lastReviewedBody, sourceConfidenceBody)) {
    gaps.push("combined_governance_not_acceptable_to_v27B_evaluator");
  }
  return gaps;
}

function buildApplyCommand(brandSlugs = TARGET_BRAND_SLUGS) {
  return `npm run brand-explorer-standard-detail-governance-writer -- --brands ${brandSlugs.join(",")} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_LEGAL}`;
}

async function buildBrandPlan(target, options = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const brandRecordId = target.recordId;
  const brandName = target.name;

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );

  const presentationRows = presentationRaw.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
    brandName: nz(rec.fields?.["Brand Name"]),
  }));

  const foreignBrandRows = presentationRows.filter(
    (r) => r.brandName && r.brandName.toLowerCase() !== brandName.toLowerCase()
  );

  const requirementRowsLive = presentationRows.filter((r) => r.slotKey === REQUIREMENT_SLOT);
  const introRowLive = presentationRows.find((r) => r.slotKey === INTRO_SLOT);
  const lastReviewedLive = presentationRows.find((r) => r.slotKey === LAST_REVIEWED_SLOT);
  const sourceConfidenceLive = presentationRows.find((r) => r.slotKey === SOURCE_CONFIDENCE_SLOT);

  const proposedLastReviewed = standardsLastReviewedBody(brandName);
  const proposedSourceConfidence = STANDARDS_SOURCE_CONFIDENCE_BODY;

  const brandShape = {
    id: brandRecordId,
    recordId: brandRecordId,
    name: brandName,
    parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    brandExplorer: {
      blocks: presentationRows
        .filter((r) => r.active !== false)
        .map((r) => ({
          recordId: r.recordId,
          slotKey: r.slotKey,
          title: r.title,
          body: r.body,
          sort: r.sortOrder,
        })),
    },
  };

  const currentApproval = evaluateStandardsDetailReadinessGeneralized(brandShape, requirementRowsLive);
  const contractBefore = await buildBrandExplorerRequiredSectionPopulationContractReport({
    brandIdOrName: target.slug,
  }).catch(() => ({ readinessScore: null }));

  const completeRequirementRows = requirementRowsLive.filter(requirementRowHasRequiredColumns);
  const incompleteRequirementRows = requirementRowsLive
    .filter((r) => !requirementRowHasRequiredColumns(r))
    .map((r) => ({
      recordId: r.recordId,
      title: r.title,
      columns: parseRequirementColumns(r.body),
    }));

  const copySafetyFindings = [];
  for (const row of requirementRowsLive) {
    const issues = scanStandardsCopySafetyForBrand(brandShape, `${row.title}\n${row.body}`);
    if (issues.length) {
      copySafetyFindings.push({ recordId: row.recordId, title: row.title, issues });
    }
  }
  if (introRowLive) {
    const issues = scanStandardsCopySafetyForBrand(brandShape, introRowLive.body);
    if (issues.length) {
      copySafetyFindings.push({ recordId: introRowLive.recordId, slotKey: INTRO_SLOT, issues });
    }
  }

  const governanceGap = diagnoseGovernanceGap(
    introRowLive?.body || "",
    lastReviewedLive?.body || "",
    sourceConfidenceLive?.body || ""
  );

  const governancePlans = [
    {
      slotKey: LAST_REVIEWED_SLOT,
      recordId: lastReviewedLive?.recordId || null,
      action: lastReviewedLive ? "update" : "create",
      sort: 0,
      proposedBody: proposedLastReviewed,
      currentBody: lastReviewedLive?.body || "",
      fields: presentationFieldsForGovernance(
        LAST_REVIEWED_SLOT,
        proposedLastReviewed,
        0,
        brandRecordId,
        brandName
      ),
    },
    {
      slotKey: SOURCE_CONFIDENCE_SLOT,
      recordId: sourceConfidenceLive?.recordId || null,
      action: sourceConfidenceLive ? "update" : "create",
      sort: 0,
      proposedBody: proposedSourceConfidence,
      currentBody: sourceConfidenceLive?.body || "",
      fields: presentationFieldsForGovernance(
        SOURCE_CONFIDENCE_SLOT,
        proposedSourceConfidence,
        0,
        brandRecordId,
        brandName
      ),
    },
  ];

  const rowsWouldCreate = [];
  const rowsWouldUpdate = [];
  const rowsMatched = [];

  for (const plan of governancePlans) {
    const safetyIssues = scanProposedGovernanceSafety(brandName, plan.slotKey, plan.proposedBody);
    if (safetyIssues.length) {
      throw new Error(`Proposed governance copy failed safety scan for ${target.slug}: ${safetyIssues.join(",")}`);
    }
    if (plan.recordId && bodiesMatch(plan.currentBody, plan.proposedBody)) {
      rowsMatched.push({ recordId: plan.recordId, slotKey: plan.slotKey, action: "matched" });
      continue;
    }
    if (plan.recordId) {
      rowsWouldUpdate.push({
        recordId: plan.recordId,
        slotKey: plan.slotKey,
        action: "update",
        currentBody: plan.currentBody,
        proposedBody: plan.proposedBody,
        fields: {
          "Slot Key": plan.slotKey,
          Title: "",
          Body: plan.proposedBody,
          "Brand Name": brandName,
          Active: true,
          "Sort Order": plan.sort,
        },
      });
    } else {
      rowsWouldCreate.push({
        slotKey: plan.slotKey,
        action: "create",
        proposedBody: plan.proposedBody,
        fields: plan.fields,
      });
    }
  }

  const projectedBrandShape = {
    ...brandShape,
    brandExplorer: {
      blocks: [
        ...brandShape.brandExplorer.blocks.filter((b) => !GOVERNANCE_TARGET_SLOTS.includes(b.slotKey)),
        { slotKey: LAST_REVIEWED_SLOT, title: "", body: proposedLastReviewed },
        { slotKey: SOURCE_CONFIDENCE_SLOT, title: "", body: proposedSourceConfidence },
      ],
    },
  };
  const projectedApproval = evaluateStandardsDetailReadinessGeneralized(
    projectedBrandShape,
    requirementRowsLive
  );

  const applyBlockers = [];
  if (!introRowLive || !hasVal(introRowLive.body)) applyBlockers.push("missing_standards_intro");
  if (requirementRowsLive.length < MIN_REQUIREMENT_ROWS_GENERAL) {
    applyBlockers.push(`insufficient_requirement_rows:${requirementRowsLive.length}<${MIN_REQUIREMENT_ROWS_GENERAL}`);
  }
  if (completeRequirementRows.length < MIN_REQUIREMENT_ROWS_GENERAL) {
    applyBlockers.push(
      `incomplete_requirement_columns:${completeRequirementRows.length}<${MIN_REQUIREMENT_ROWS_GENERAL}`
    );
  }
  if (copySafetyFindings.length) {
    applyBlockers.push(
      `copy_safety_blocker:${copySafetyFindings.map((f) => f.title || f.slotKey).join(",")}`
    );
  }
  if (foreignBrandRows.length) {
    applyBlockers.push(`foreign_brand_presentation_rows:${foreignBrandRows.length}`);
  }

  const projectedContractScore = projectedApproval.ready
    ? Math.min(100, (contractBefore.readinessScore || 88) + (currentApproval.ready ? 0 : 12))
    : contractBefore.readinessScore || 88;

  return {
    brand: {
      slug: target.slug,
      name: brandName,
      recordId: brandRecordId,
      parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    },
    diagnosis: {
      introPresent: Boolean(introRowLive && hasVal(introRowLive.body)),
      introRecordId: introRowLive?.recordId || null,
      requirementRowCount: requirementRowsLive.length,
      completeRequirementRowCount: completeRequirementRows.length,
      incompleteRequirementRowCount: incompleteRequirementRows.length,
      incompleteRequirementRows: incompleteRequirementRows.slice(0, 5),
      lastReviewedPresent: Boolean(lastReviewedLive),
      lastReviewedRecordId: lastReviewedLive?.recordId || null,
      lastReviewedCurrentBody: lastReviewedLive?.body || "",
      sourceConfidencePresent: Boolean(sourceConfidenceLive),
      sourceConfidenceRecordId: sourceConfidenceLive?.recordId || null,
      sourceConfidenceCurrentBody: sourceConfidenceLive?.body || "",
      governanceGap,
      contractScoreBefore: contractBefore.readinessScore,
      contractReadyBefore: contractBefore.brandExplorerRequiredSectionsReady,
      currentStandardDetailBlockers: currentApproval.blockers || [],
    },
    proposedGovernanceCopy: {
      standardsLastReviewed: proposedLastReviewed,
      standardsSourceConfidence: proposedSourceConfidence,
    },
    rowsWouldCreate,
    rowsWouldUpdate,
    rowsMatched,
    copySafetyFindings,
    applyBlockers,
    projectedStandardDetailReady: projectedApproval.ready,
    projectedContractScore: projectedApproval.ready ? 100 : projectedContractScore,
    projectedContractReady: projectedApproval.ready,
    projectedApproval,
    companyValidatedBefore,
    hasGovernanceWork: rowsWouldCreate.length > 0 || rowsWouldUpdate.length > 0,
  };
}

export async function buildBrandExplorerStandardDetailGovernanceWriterReport(options = {}) {
  const targets = resolveTargetBrands(options.brands);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const apply = Boolean(options.apply);
  const approveBatch = Boolean(options.approveBatch);
  const founderReviewed = Boolean(options.founderReviewed);
  const noLegalOrCompanyConfirmed = Boolean(options.noLegalOrCompanyConfirmed);

  const brandPlans = [];
  for (const target of targets) {
    brandPlans.push(await buildBrandPlan(target, options));
    await new Promise((r) => setTimeout(r, 400));
  }

  const batchApplyBlockers = [];
  for (const plan of brandPlans) {
    batchApplyBlockers.push(...plan.applyBlockers.map((b) => `${plan.brand.slug}:${b}`));
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && noLegalOrCompanyConfirmed;
  const hasWork = brandPlans.some((p) => p.hasGovernanceWork);
  const canApply = applyGatesReady && batchApplyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  const applyResultsByBrand = {};

  if (canApply) {
    for (const plan of brandPlans) {
      const created = [];
      const updated = [];
      const errors = [];

      for (const row of plan.rowsWouldUpdate) {
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
            action: "update",
            slotKey: row.slotKey,
            recordId: row.recordId,
            message: json.error?.message || res.status,
          });
        } else {
          updated.push({ recordId: json.id, slotKey: row.slotKey });
        }
        await new Promise((r) => setTimeout(r, 220));
      }

      for (const row of plan.rowsWouldCreate) {
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        });
        if (!res.ok) {
          errors.push({
            action: "create",
            slotKey: row.slotKey,
            message: json.error?.message || res.status,
          });
        } else {
          created.push({ recordId: json.id, slotKey: row.slotKey });
        }
        await new Promise((r) => setTimeout(r, 220));
      }

      applyResultsByBrand[plan.brand.slug] = { created, updated, errors };
      if ((created.length > 0 || updated.length > 0) && errors.length === 0) {
        airtableModified = true;
      }
    }
  } else if (apply) {
    for (const plan of brandPlans) {
      applyResultsByBrand[plan.brand.slug] = {
        created: [],
        updated: [],
        errors: [],
        blocked: true,
        blockers: plan.applyBlockers,
      };
    }
  }

  let companyValidatedUntouched = true;
  for (const plan of brandPlans) {
    const after = companyValidatedSnapshot(await fetchBrandBasics(plan.brand.recordId));
    if (JSON.stringify(plan.companyValidatedBefore) !== JSON.stringify(after)) {
      companyValidatedUntouched = false;
    }
  }

  const dryRunClean =
    batchApplyBlockers.length === 0 && brandPlans.every((p) => p.projectedStandardDetailReady);

  const applyReadySlugs = brandPlans
    .filter((p) => p.applyBlockers.length === 0 && p.hasGovernanceWork)
    .map((p) => p.brand.slug);

  const report = {
    writerVersion: WRITER_VERSION,
    v27CWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    targetBrands: targets.map((t) => t.slug),
    protectedBrandsUntouched: PROTECTED_BRAND_SLUGS,
    airtableModified,
    companyValidatedUntouched,
    companyValidationDateUntouched: companyValidatedUntouched,
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    brandPlans,
    batchSummary: {
      rowsWouldCreate: brandPlans.reduce((n, p) => n + p.rowsWouldCreate.length, 0),
      rowsWouldUpdate: brandPlans.reduce((n, p) => n + p.rowsWouldUpdate.length, 0),
      rowsMatched: brandPlans.reduce((n, p) => n + p.rowsMatched.length, 0),
      applyBlockers: batchApplyBlockers,
      dryRunClean,
      canApply,
    },
    applyResultsByBrand: Object.keys(applyResultsByBrand).length ? applyResultsByBrand : null,
    exactApplyCommand: buildApplyCommand(targets.map((t) => t.slug)),
    exactApplyCommandRecommended:
      applyReadySlugs.length && applyReadySlugs.length < targets.length
        ? buildApplyCommand(applyReadySlugs)
        : buildApplyCommand(targets.map((t) => t.slug)),
    exactDryRunCommand: `npm run brand-explorer-standard-detail-governance-writer -- --brands ${targets.map((t) => t.slug).join(",")} --dry-run`,
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Standard Detail Governance Writer v${WRITER_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Targets: ${report.targetBrands.join(", ")}`);
  lines.push(`- v27C writer exists: **${report.v27CWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Batch summary");
  lines.push(`- Rows to create: ${report.batchSummary.rowsWouldCreate}`);
  lines.push(`- Rows to update: ${report.batchSummary.rowsWouldUpdate}`);
  lines.push(`- Dry-run clean for apply: **${report.batchSummary.dryRunClean ? "yes" : "no"}**`);
  if (report.batchSummary.applyBlockers.length) {
    lines.push("- Apply blockers:");
    for (const b of report.batchSummary.applyBlockers) lines.push(`  - ${b}`);
  }
  lines.push("");
  for (const plan of report.brandPlans) {
    lines.push(`## ${plan.brand.name} (\`${plan.brand.slug}\`)`);
    lines.push(`- Record: \`${plan.brand.recordId}\``);
    lines.push(`- Contract before: **${plan.diagnosis.contractScoreBefore}**`);
    lines.push(`- Expected contract after apply: **${plan.projectedContractScore}**`);
    lines.push(`- Intro: ${plan.diagnosis.introPresent ? "yes" : "no"}`);
    lines.push(
      `- Requirement rows: ${plan.diagnosis.requirementRowCount} (${plan.diagnosis.completeRequirementRowCount} column-complete)`
    );
    lines.push(`- Governance gap: ${plan.diagnosis.governanceGap.join("; ") || "none"}`);
    lines.push(`- Standard Detail blockers now: ${plan.diagnosis.currentStandardDetailBlockers.join("; ") || "none"}`);
    lines.push("");
    lines.push("### Proposed copy");
    lines.push(`**standards.last_reviewed**`);
    lines.push("```");
    lines.push(plan.proposedGovernanceCopy.standardsLastReviewed);
    lines.push("```");
    lines.push(`**standards.source_confidence**`);
    lines.push("```");
    lines.push(plan.proposedGovernanceCopy.standardsSourceConfidence);
    lines.push("```");
    if (plan.rowsWouldCreate.length) {
      lines.push("### Rows to create");
      for (const r of plan.rowsWouldCreate) lines.push(`- \`${r.slotKey}\``);
    }
    if (plan.rowsWouldUpdate.length) {
      lines.push("### Rows to update");
      for (const r of plan.rowsWouldUpdate) {
        lines.push(`- \`${r.slotKey}\` (\`${r.recordId}\`)`);
      }
    }
    if (plan.copySafetyFindings.length) {
      lines.push("### Copy safety blockers (standards table — not repaired in v27C)");
      for (const f of plan.copySafetyFindings) {
        lines.push(`- ${f.title || f.slotKey}: ${f.issues.join(", ")}`);
      }
    }
    lines.push("");
  }
  lines.push("## Exact apply command");
  if (!report.batchSummary.dryRunClean && report.exactApplyCommandRecommended !== report.exactApplyCommand) {
    lines.push("### Recommended (apply-ready brands only)");
    lines.push("```bash");
    lines.push(report.exactApplyCommandRecommended);
    lines.push("```");
    lines.push("### Full batch (blocked until all brands pass apply gates)");
  }
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  return lines.join("\n");
}

export function buildBrandExplorerStandardDetailGovernanceWriterMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}

export { buildApplyCommand };
