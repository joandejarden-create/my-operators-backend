/**
 * Brand Explorer Design Hotels Full Profile Content Population v35F.
 *
 * Populates underfilled Brand Explorer presentation tabs using approved Source
 * Library records and affiliation/curation language (not franchise soft-brand copy).
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import {
  ATELIER_PROOF_FALLBACK_HEADS,
} from "./brand-explorer-active-profile-factory-rules.js";
import {
  buildDesignHotelsContentPackages,
  DESIGN_HOTELS_TAB_DEFINITIONS,
  GENERIC_PROOF_TITLES,
  isGenericProofTitle,
  isThinRow,
  hasEmptyBullets,
  wordCount,
  V35F_CONTENT_VERSION,
} from "./brand-explorer-design-hotels-content-packages-v35F.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";
import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";

export const V35F_VERSION = "v35F";
export const STAGING_RUN_ID = "v35F-design-hotels-full-profile-content";

export const TARGET_BRAND = Object.freeze({
  slug: "design-hotels",
  recordId: "rec02zPClpWUTCyXM",
  name: "Design Hotels",
});

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v35F-design-hotels-full-profile-content";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_ACTIVE = "--confirm-no-active-profile-approval";
export const APPLY_FLAG_NO_SUMMARY = "--confirm-no-summary-url-field";
export const APPLY_FLAG_SOURCE_SUPPORTED = "--confirm-official-source-supported-copy";
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

const UNSAFE_COPY_RE = Object.freeze([
  { id: "franchise_flag", re: /\bfranchise flag\b/i },
  { id: "fdd", re: /\b(fdd|item\s*19|franchise disclosure)\b/i },
  { id: "fee_stack", re: /\bfee stack\b/i },
  { id: "adr", re: /\badr\b/i },
  { id: "revpar", re: /\brevpar\b/i },
  { id: "net_contribution", re: /\bnet contribution\b/i },
  { id: "pipeline_depth", re: /\bpipeline depth\b/i },
  { id: "brand_verified_wording", re: /\bbrand-verified content\b/i },
  { id: "company_validated_claim", re: /\bcompany validated\b|\bcompany-approved\b|\bofficial sign-off\b/i },
  { id: "performance_guarantee", re: /\brooms from loyalty\b|\brepeat demand guarantee\b|\bguaranteed returns?\b/i },
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
    approveContent: argv.includes(APPLY_FLAG_APPROVE),
    confirmNoValidation: argv.includes(APPLY_FLAG_NO_VALIDATION),
    confirmNoActive: argv.includes(APPLY_FLAG_NO_ACTIVE),
    confirmNoSummary: argv.includes(APPLY_FLAG_NO_SUMMARY),
    confirmSourceSupported: argv.includes(APPLY_FLAG_SOURCE_SUPPORTED),
    confirmAffiliation: argv.includes(APPLY_FLAG_AFFILIATION),
    confirmDesignHotelsOnly: argv.includes(APPLY_FLAG_DESIGN_HOTELS_ONLY),
  };
}

export function buildApplyCommand() {
  return [
    "npm run brand-explorer-design-hotels-full-profile-content --",
    `--brand ${TARGET_BRAND.slug}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_ACTIVE,
    APPLY_FLAG_NO_SUMMARY,
    APPLY_FLAG_SOURCE_SUPPORTED,
    APPLY_FLAG_AFFILIATION,
    APPLY_FLAG_DESIGN_HOTELS_ONLY,
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
  if (/\bsource capture\b|\bextraction run\b/i.test(blob)) errors.push("source_capture");

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

function slotMatchesTab(slotKey, tabDef) {
  return tabDef.prefixes.some((p) => slotKey.startsWith(p));
}

function auditTabDensity(presentationRows, packages) {
  const tabAudits = [];

  for (const tabDef of DESIGN_HOTELS_TAB_DEFINITIONS) {
    const tabRows = presentationRows.filter(
      (r) => r.visible && slotMatchesTab(r.slotKey, tabDef)
    );
    const tabPackages = packages.filter((p) => p.tab === tabDef.tab);
    const missingRows = [];
    const thinRows = [];
    const emptyBullets = [];
    const genericFallback = [];
    const unsafeLanguage = [];

    for (const pkg of tabPackages) {
      const matches = tabRows
        .filter((r) => r.slotKey === pkg.slotKey)
        .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));
      const row =
        matches.find((r) => (r.sortOrder ?? 0) === (pkg.sort ?? 0)) ||
        (matches.length > (pkg.sort ?? 0) ? matches[pkg.sort ?? 0] : matches[0]);
      if (!row) {
        missingRows.push({ slotKey: pkg.slotKey, reason: "row_missing", sort: pkg.sort ?? 0 });
        continue;
      }
      if (isThinRow({ ...row, slotKey: pkg.slotKey })) {
        thinRows.push({ recordId: row.recordId, slotKey: row.slotKey });
      }
      if (hasEmptyBullets(row.body, pkg.slotKey)) {
        emptyBullets.push({ recordId: row.recordId, slotKey: row.slotKey });
      }
      if (isGenericProofTitle(row.title) || ATELIER_PROOF_FALLBACK_HEADS.includes(row.title)) {
        genericFallback.push({ recordId: row.recordId, slotKey: row.slotKey, title: row.title });
      }
      const safety = validateCopySafety(`${row.title}\n${row.body}`);
      if (!safety.valid) {
        unsafeLanguage.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          issues: safety.errors,
        });
      }
    }

    const meaningfulRows = tabRows.filter((r) => wordCount(r.body) >= 12 || r.body.includes("\n"));
    tabAudits.push({
      tab: tabDef.tab,
      visibleRows: tabRows.length,
      meaningfulRows: meaningfulRows.length,
      minimumExpected: tabDef.minimumMeaningfulRows,
      packagesPlanned: tabPackages.length,
      missingRows,
      thinRows,
      emptyBullets,
      genericFallback,
      unsafeLanguage,
      ownerReady:
        missingRows.length === 0 &&
        thinRows.length === 0 &&
        emptyBullets.length === 0 &&
        genericFallback.length === 0 &&
        meaningfulRows.length >= Math.min(tabDef.minimumMeaningfulRows, tabPackages.length),
    });
  }

  return tabAudits;
}

function planContentPatches(presentationRows, packages, brandRecordId, brandName) {
  const creates = [];
  const patches = [];
  const founderReviewQueue = [];

  for (const pkg of packages) {
    if (!pkg.sourceIds?.length) {
      founderReviewQueue.push({
        slotKey: pkg.slotKey,
        reason: "missing_source_support",
      });
      continue;
    }

    const safety = validateCopySafety(`${pkg.title}\n${pkg.body}`);
    if (!safety.valid) {
      founderReviewQueue.push({
        slotKey: pkg.slotKey,
        reason: "unsafe_copy_blocked",
        errors: safety.errors,
      });
      continue;
    }

    const existing = presentationRows
      .filter((r) => r.slotKey === pkg.slotKey)
      .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));

    const rowAtSort =
      existing.find((r) => (r.sortOrder ?? 0) === (pkg.sort ?? 0)) ||
      (existing.length > (pkg.sort ?? 0) ? existing[pkg.sort ?? 0] : null);

    const needsPatch = (row) =>
      isThinRow(row) ||
      isGenericProofTitle(row.title) ||
      ATELIER_PROOF_FALLBACK_HEADS.includes(row.title) ||
      hasEmptyBullets(row.body) ||
      !validateCopySafety(`${row.title}\n${row.body}`).valid ||
      nz(row.body) !== nz(pkg.body) ||
      (pkg.title && nz(row.title) !== nz(pkg.title));

    if (!rowAtSort) {
      creates.push({
        slotKey: pkg.slotKey,
        tab: pkg.tab,
        fields: presentationFields({
          slotKey: pkg.slotKey,
          title: pkg.title,
          body: pkg.body,
          sort: pkg.sort,
          brandRecordId,
          brandName,
        }),
        sourceIds: pkg.sourceIds,
        reason: "v35F create missing row",
      });
      continue;
    }

    if (needsPatch(rowAtSort)) {
      patches.push({
        recordId: rowAtSort.recordId,
        slotKey: pkg.slotKey,
        tab: pkg.tab,
        before: { title: rowAtSort.title, body: rowAtSort.body },
        after: { title: pkg.title || rowAtSort.title, body: pkg.body },
        fields: presentationFields({
          slotKey: pkg.slotKey,
          title: pkg.title || rowAtSort.title,
          body: pkg.body,
          sort: pkg.sort ?? rowAtSort.sortOrder,
          brandRecordId,
          brandName,
        }),
        sourceIds: pkg.sourceIds,
        reason: "v35F patch thin/generic/unsafe row",
      });
    }
  }

  return { creates, patches, founderReviewQueue };
}

function validateApplyBlockers({
  flags,
  companyValidatedBefore,
  brandBasics,
  tabAuditsBefore,
  tabAuditsAfter,
  plan,
}) {
  const blockers = [];

  if (flags.apply) {
    if (!flags.approveContent) blockers.push("missing_approve_v35F_design_hotels_full_profile_content");
    if (!flags.confirmNoValidation) blockers.push("missing_confirm_no_company_validation_claim");
    if (!flags.confirmNoActive) blockers.push("missing_confirm_no_active_profile_approval");
    if (!flags.confirmNoSummary) blockers.push("missing_confirm_no_summary_url_field");
    if (!flags.confirmSourceSupported) blockers.push("missing_confirm_official_source_supported_copy");
    if (!flags.confirmAffiliation) blockers.push("missing_confirm_affiliation_not_franchise_language");
    if (!flags.confirmDesignHotelsOnly) blockers.push("missing_confirm_design_hotels_only");
  }

  const cv = companyValidatedSnapshot(brandBasics);
  if (cv.companyValidated !== companyValidatedBefore.companyValidated) {
    blockers.push("company_validated_would_change");
  }

  for (const item of [...plan.creates, ...plan.patches]) {
    const safety = validateCopySafety(`${item.fields?.Title || ""}\n${item.fields?.Body || ""}`);
    if (!safety.valid) blockers.push(`unsafe_copy_planned:${item.slotKey}:${safety.errors.join(",")}`);
    if (!item.sourceIds?.length) blockers.push(`missing_source_support:${item.slotKey}`);
  }

  const tabsStillEmpty = (tabAuditsAfter || []).filter(
    (t) => t.missingRows.length > 0 || t.meaningfulRows < Math.min(3, t.minimumExpected)
  );
  if (flags.apply && tabsStillEmpty.length > 0) {
    blockers.push(`materially_empty_tabs_remain:${tabsStillEmpty.map((t) => t.tab).join(",")}`);
  }

  const genericRemain = (tabAuditsAfter || []).flatMap((t) => t.genericFallback || []);
  if (flags.apply && genericRemain.length) {
    blockers.push(`generic_proof_cards_remain:${genericRemain.map((g) => g.slotKey).join(",")}`);
  }

  return blockers;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Design Hotels Full Profile Content ${V35F_VERSION}`);
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}**`);
  lines.push("");
  lines.push("## Tab density audit (before → after projected)");
  for (const tab of report.tabAuditsAfter || []) {
    const before = (report.tabAuditsBefore || []).find((t) => t.tab === tab.tab);
    lines.push(
      `- **${tab.tab}**: visible ${before?.visibleRows ?? 0}→${tab.visibleRows} · meaningful ${before?.meaningfulRows ?? 0}→${tab.meaningfulRows} · owner-ready: **${tab.ownerReady ? "yes" : "no"}**`
    );
  }
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
  lines.push("## Unsafe language removed (generic proof themes)");
  for (const g of report.genericProofRemoved) lines.push(`- ${g.slotKey}: ${g.title || "(title)"}`);
  lines.push("");
  lines.push("## Source support");
  lines.push(`- Approved sources used: **${report.approvedSourceCount}**`);
  lines.push(`- Rows with source linkage: **${report.rowsWithSourceSupport}**`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "NO"}**`);
  lines.push(`- Active profile approval: **not set**`);
  lines.push(`- UI proof label: **${report.proofSectionLabelAfter}**`);
  lines.push("");
  lines.push("## Expected QA (projected)");
  lines.push(`- Visual defects: ${report.expectedVisualDefects?.defectCounts?.total ?? "n/a"}`);
  lines.push(`- Required sections ready: ${report.expectedSectionContract?.ready ?? "n/a"}`);
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
  const body = recordId ? { fields, typecast: true } : { fields, typecast: true };
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable ${method} failed: ${res.status}`);
  return json;
}

export async function buildDesignHotelsFullProfileContentV35FReport(options = {}) {
  const flags = options.flags || parseCliFlags(options.argv);
  const brand = TARGET_BRAND;
  if (flags.brandArg !== brand.slug && flags.brandArg !== brand.recordId) {
    throw new Error(`v35F supports Design Hotels only; got: ${flags.brandArg}`);
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
  const packages = buildDesignHotelsContentPackages(sourceLoad.byId);
  const tabAuditsBefore = auditTabDensity(presentationRows, packages);
  const plan = planContentPatches(
    presentationRows,
    packages,
    brand.recordId,
    brand.name
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
      recordId: `v35f-draft-${create.slotKey}`,
      slotKey: create.slotKey,
      title: create.fields.Title,
      body: create.fields.Body,
      sortOrder: create.fields["Sort Order"],
      visible: true,
    });
  }

  const tabAuditsAfter = auditTabDensity(projectedRows, packages);
  const applyBlockers = validateApplyBlockers({
    flags,
    companyValidatedBefore,
    brandBasics,
    tabAuditsBefore,
    tabAuditsAfter,
    plan,
  });

  const dryRunClean = applyBlockers.length === 0;
  const canApply = flags.apply && dryRunClean;

  const applyResults = { created: [], patched: [], errors: [] };

  if (canApply) {
    for (const create of plan.creates) {
      for (const key of Object.keys(create.fields)) {
        if (BLOCKED_PRESENTATION_FIELDS.has(key)) {
          applyResults.errors.push({ slotKey: create.slotKey, message: `blocked_field:${key}` });
        }
      }
    }
    if (!applyResults.errors.length) {
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
          applyResults.errors.push({ recordId: patch.recordId, message: err.message });
        }
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(brand.recordId) : brandBasics;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  const companyValidatedUntouched =
    companyValidatedBefore.companyValidated === companyValidatedAfter.companyValidated &&
    companyValidatedBefore.companyValidationDate === companyValidatedAfter.companyValidationDate;

  const genericProofRemoved = (tabAuditsBefore || [])
    .flatMap((t) => t.genericFallback || [])
    .map((g) => ({ slotKey: g.slotKey, title: g.title }));

  const expectedVisualDefects = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: brand.slug,
  }).catch(() => null);
  const expectedSectionContract = await buildBrandExplorerRequiredSectionPopulationContractReport({
    brandIdOrName: brand.slug,
  }).catch(() => null);
  const expectedFinalQa = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: brand.slug,
  }).catch(() => null);

  const report = {
    version: V35F_VERSION,
    contentVersion: V35F_CONTENT_VERSION,
    stagingRunId: STAGING_RUN_ID,
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand,
    flags,
    approvedSourceCount: sourceLoad.approved.length,
    packagesPlanned: packages.length,
    tabAuditsBefore,
    tabAuditsAfter,
    rowsCreated: plan.creates.map((c) => ({ slotKey: c.slotKey, tab: c.tab, sourceIds: c.sourceIds })),
    rowsPatched: plan.patches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      tab: p.tab,
      reason: p.reason,
      sourceIds: p.sourceIds,
    })),
    founderReviewQueue: plan.founderReviewQueue,
    genericProofRemoved,
    rowsWithSourceSupport: plan.creates.length + plan.patches.length,
    unsafeLanguageRemoved: genericProofRemoved,
    sourceSupportUsed: sourceLoad.approved.map((s) => ({
      id: s.id || s.recordId,
      title: s.sourceTitle || s.title,
      url: s.sourceUrl || s.url,
    })),
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched,
    activeProfileApproved: false,
    proofSectionLabelBefore: "Brand-Verified Content · Curated by Dealality",
    proofSectionLabelAfter: companyValidatedBefore.companyValidated
      ? "Brand-Verified Content · Curated by Dealality"
      : "AI-Assisted from Official Public Sources · Curated by Dealality",
    expectedVisualDefects: expectedVisualDefects
      ? { defectCounts: expectedVisualDefects.defectCounts }
      : null,
    expectedSectionContract: expectedSectionContract
      ? { ready: expectedSectionContract.ready, readinessScore: expectedSectionContract.readinessScore }
      : null,
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

export { parseCliFlags, buildMarkdown, GENERIC_PROOF_TITLES };
