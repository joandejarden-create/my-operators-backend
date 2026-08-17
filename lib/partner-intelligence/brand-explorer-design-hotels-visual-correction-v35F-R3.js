/**
 * Design Hotels visual correction v35F-R3 — loyalty KPIs, watchouts bullet, momentum copy.
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
  APPLY_FLAG_NO_VISIBLE_URLS,
  APPLY_FLAG_TRACEABILITY,
} from "./brand-explorer-design-hotels-external-owner-cleanup-v35F-R1.js";
import {
  auditExternalOwnerPhrase,
  sanitizeAffiliationExternalCopy,
} from "./brand-explorer-external-owner-content-governance.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import {
  buildDesignHotelsVisualCorrectionPackagesV35FR3,
} from "./brand-explorer-design-hotels-visual-correction-v35F-R3-content.js";

export const V35F_R3_VERSION = "v35F-R3";
export const STAGING_RUN_ID = "v35F-R3-design-hotels-visual-correction";
export { TARGET_BRAND };

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v35F-R3-design-hotels-visual-correction";

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
    approve: argv.includes(APPLY_FLAG_APPROVE),
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
    "npm run brand-explorer-design-hotels-visual-correction --",
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

function validateCopySafety(text) {
  const blob = nz(text);
  if (!blob) return { valid: false, errors: ["empty_copy"] };
  const errors = [];
  if (/\b(fdd|item\s*19|franchise disclosure)\b/i.test(blob) && !/\b(not|no|without)\b/i.test(blob)) {
    errors.push("fdd");
  }
  if (/\badr\b|\brevpar\b/i.test(blob) && !/\b(no|not|without|does not)\b/i.test(blob)) errors.push("metric");
  if (/https?:\/\//i.test(blob)) errors.push("http_url");
  if (/\n\nSources:\s/i.test(blob)) errors.push("sources_block");
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
    approved.map((s) => [s.id || s.recordId, { id: s.id || s.recordId, sourceTitle: s.sourceTitle || s.title }])
  );
  return { approved, byId };
}

function findRowForPackage(presentationRows, pkg) {
  const matches = presentationRows.filter(
    (r) => r.slotKey === pkg.slotKey && r.visible !== false
  );
  if (pkg.matchTitle) {
    return (
      matches.find((r) => nz(r.title) === nz(pkg.matchTitle)) ||
      matches.find((r) => (r.sortOrder ?? 0) === (pkg.sort ?? 0)) ||
      null
    );
  }
  return (
    matches.find((r) => (r.sortOrder ?? 0) === (pkg.sort ?? 0)) ||
    matches[0] ||
    null
  );
}

function planCorrections(presentationRows, packages, brandRecordId, brandName, sourcesById) {
  const creates = [];
  const patches = [];
  const founderReviewQueue = [];

  for (const pkg of packages) {
    const body = sanitizeAffiliationExternalCopy(pkg.body, { slotKey: pkg.slotKey, sourcesById });
    const safety = validateCopySafety(`${pkg.title}\n${body}`);
    const hits = auditExternalOwnerPhrase(`${pkg.title}\n${body}`, pkg.slotKey);

    if (!safety.valid) {
      founderReviewQueue.push({ slotKey: pkg.slotKey, reason: "unsafe_copy", errors: safety.errors });
      continue;
    }
    if (hits.some((h) => h.severity === "critical" || h.severity === "high")) {
      founderReviewQueue.push({ slotKey: pkg.slotKey, reason: "external_owner_hit", hits });
      continue;
    }
    if (!pkg.sourceIds?.length) {
      founderReviewQueue.push({ slotKey: pkg.slotKey, reason: "missing_source_support" });
      continue;
    }

    const row = findRowForPackage(presentationRows, pkg);
    const fields = presentationFields({
      slotKey: pkg.slotKey,
      title: pkg.title || row?.title || "",
      body,
      sort: pkg.sort ?? row?.sortOrder ?? 0,
      brandRecordId,
      brandName,
    });

    if (!row) {
      creates.push({
        slotKey: pkg.slotKey,
        tab: pkg.tab,
        fields,
        sourceIds: pkg.sourceIds,
        reason: "v35F-R3 create missing slot",
      });
      continue;
    }

    if (nz(row.body) !== nz(body) || (pkg.title && nz(row.title) !== nz(pkg.title))) {
      patches.push({
        recordId: row.recordId,
        slotKey: pkg.slotKey,
        tab: pkg.tab,
        fields,
        sourceIds: pkg.sourceIds,
        reason: pkg.matchTitle ? "v35F-R3 momentum copy fix" : "v35F-R3 patch slot body",
      });
    }
  }

  return { creates, patches, founderReviewQueue };
}

function validateApplyBlockers({ flags, companyValidatedBefore, brandBasics, plan, projectedRows }) {
  const blockers = [];
  if (flags.apply) {
    if (!flags.approve) blockers.push("missing_approve_v35F_R3_design_hotels_visual_correction");
    if (!flags.confirmNoValidation) blockers.push("missing_confirm_no_company_validation_claim");
    if (!flags.confirmNoActive) blockers.push("missing_confirm_no_active_profile_approval");
    if (!flags.confirmNoSummary) blockers.push("missing_confirm_no_summary_url_field");
    if (!flags.confirmTraceability) blockers.push("missing_confirm_source_traceability_preserved");
    if (!flags.confirmNoVisibleUrls) blockers.push("missing_confirm_no_visible_source_urls_in_owner_copy");
    if (!flags.confirmAffiliation) blockers.push("missing_confirm_affiliation_not_franchise_language");
    if (!flags.confirmDesignHotelsOnly) blockers.push("missing_confirm_design_hotels_only");
  }

  if (companyValidatedSnapshot(brandBasics).companyValidated !== companyValidatedBefore.companyValidated) {
    blockers.push("company_validated_would_change");
  }

  for (const item of [...plan.creates, ...plan.patches]) {
    const safety = validateCopySafety(`${item.fields?.Title || ""}\n${item.fields?.Body || ""}`);
    if (!safety.valid) blockers.push(`unsafe_copy_planned:${item.slotKey}`);
    for (const field of Object.keys(item.fields || {})) {
      if (BLOCKED_PRESENTATION_FIELDS.has(field)) blockers.push(`blocked_field:${field}`);
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

async function airtableWrite(baseId, apiKey, recordId, fields, method) {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`;
  const res = await fetch(url, {
    method,
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable ${method} failed: ${res.status}`);
  return json;
}

export async function buildDesignHotelsVisualCorrectionV35FR3Report(options = {}) {
  const flags = options.flags || parseCliFlags(options.argv);
  const brand = TARGET_BRAND;
  if (flags.brandArg !== brand.slug && flags.brandArg !== brand.recordId) {
    throw new Error(`v35F-R3 supports Design Hotels only; got: ${flags.brandArg}`);
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
  const packages = buildDesignHotelsVisualCorrectionPackagesV35FR3();
  const plan = planCorrections(
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
      row.title = patch.fields.Title;
      row.body = patch.fields.Body;
    }
  }
  for (const create of plan.creates) {
    projectedRows.push({
      recordId: `v35f-r3-draft-${create.slotKey}`,
      slotKey: create.slotKey,
      title: create.fields.Title,
      body: create.fields.Body,
      visible: true,
      caseSummaryOverview: "",
      caseSummaryOwnerObjective: "",
      caseSummaryBrandRelevance: "",
      caseSummaryInterpretation: "",
      caseSummaryTags: "",
    });
  }

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

  return {
    version: V35F_R3_VERSION,
    stagingRunId: STAGING_RUN_ID,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand,
    rowsCreated: plan.creates,
    rowsPatched: plan.patches,
    founderReviewQueue: plan.founderReviewQueue,
    externalOwnerReadiness: evaluateExternalOwnerReadinessRule(projectedRows),
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      companyValidatedBefore.companyValidated === companyValidatedAfter.companyValidated,
    applyBlockers,
    dryRunClean,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    applyResults: canApply ? applyResults : null,
    airtableModified: canApply && applyResults.errors.length === 0,
  };
}
