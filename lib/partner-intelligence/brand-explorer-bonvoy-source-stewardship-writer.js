/**
 * Brand Explorer Bonvoy Source Stewardship Writer v25C-2G-S.
 *
 * Stewards the three v25C-2F Marriott Bonvoy Source Library records so v25C-2G
 * can approve rich loyalty facts. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-bonvoy-source-stewardship-writer-v25C-2G-S.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { getPartnerSourceById, patchPartnerSource } from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";

export const WRITER_VERSION = "25C-2G-S";
export const REPORT_JSON_NAME = "brand-explorer-bonvoy-source-stewardship-writer.json";
export const REPORT_MD_NAME = "brand-explorer-bonvoy-source-stewardship-writer.md";
export const DOC_MD_NAME = "brand-explorer-bonvoy-source-stewardship-writer-v25C-2G-S.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-2G-source-stewardship";
export const APPLY_FLAG_CONFIRM = "--confirm-official-marriott-bonvoy-sources";

export const NEXT_BATCH = "brand-explorer-bonvoy-loyalty-rich-fact-approval-writer";
export const NEXT_BATCH_VERSION = "25C-2G";

const EXISTING_BONVOY_SOURCE_ID = "recu6AFRZBBBNiCQn";
const EXISTING_BONVOY_SOURCE_URL = "https://www.marriott.com/loyalty.mi";
const STEWARDSHIP_NOTE =
  "v25C-2G-S Bonvoy loyalty source stewardship for Tribute Portfolio rich loyalty enhancement; not Marriott validation.";

/** Only these three v25C-2F sources may be patched by this writer. */
export const TARGET_BONVOY_SOURCE_RECORDS = [
  {
    recordId: "rec8eRACSCyGnHRXH",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    purpose: "Elite tier summaries / member benefits",
    loyaltyRole: "member_benefits",
  },
  {
    recordId: "recc9NVMd7gvDKGBF",
    sourceUrl: "https://www.marriott.com/loyalty/earn.mi",
    purpose: "Earn mechanics",
    loyaltyRole: "earn_mechanics",
  },
  {
    recordId: "recaAmqeCbXN3n89z",
    sourceUrl: "https://www.marriott.com/loyalty/redeem.mi",
    purpose: "Redeem mechanics",
    loyaltyRole: "redeem_mechanics",
  },
];

const TARGET_RECORD_IDS = new Set(TARGET_BONVOY_SOURCE_RECORDS.map((s) => s.recordId));

const OFFICIAL_MARRIOTT_BONVOY_URLS = new Map(
  TARGET_BONVOY_SOURCE_RECORDS.map((s) => [s.recordId, normalizeUrl(s.sourceUrl)])
);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.json",
  "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.md",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.json",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/airtable-source.js",
  "live Tribute Source Library records",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeUrl(url) {
  return nz(url).replace(/\/+$/, "").toLowerCase();
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return TRIBUTE_RECORD_ID;
  }
  return nz(raw);
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function isOfficialMarriottBonvoyUrl(url) {
  return /^https:\/\/(www\.)?marriott\.com\/loyalty/i.test(nz(url));
}

function isSourceFullyStewarded(source) {
  return (
    nz(source?.approvedForExplorerUse) === "Yes" && nz(source?.approvedForExtraction) === "Yes"
  );
}

function validateSelect(fieldKey, value) {
  const allowed = VAL_PARTNER_SOURCE_SELECTS[fieldKey];
  if (!allowed) return true;
  return allowed.includes(value);
}

function buildStewardshipPatch(source) {
  const patch = {};
  const skipped = [];

  if (nz(source.approvedForExplorerUse) !== "Yes") {
    if (!validateSelect("approvedForExplorerUse", "Yes")) {
      skipped.push("unknown_select_option:approvedForExplorerUse");
    } else {
      patch[MAP_PARTNER_SOURCE.approvedForExplorerUse] = "Yes";
    }
  }

  if (nz(source.approvedForExtraction) !== "Yes") {
    if (!validateSelect("approvedForExtraction", "Yes")) {
      skipped.push("unknown_select_option:approvedForExtraction");
    } else {
      patch[MAP_PARTNER_SOURCE.approvedForExtraction] = "Yes";
    }
  }

  const priorNotes = nz(source.notes);
  if (!priorNotes.includes("v25C-2G-S")) {
    patch[MAP_PARTNER_SOURCE.notes] = priorNotes
      ? `${priorNotes}\n${STEWARDSHIP_NOTE}`
      : STEWARDSHIP_NOTE;
  }

  const stamp = new Date().toISOString().slice(0, 10);
  patch[MAP_PARTNER_SOURCE.lastReviewed] = stamp;

  if (!Object.keys(patch).length) {
    return { patch: null, skipped: ["already_stewarded_idempotent"], fieldsChanged: [] };
  }

  return {
    patch,
    skipped,
    fieldsChanged: Object.keys(patch),
  };
}

function assessTargetSource(source, spec, brandRecordId) {
  const blockers = [];
  if (!source) {
    blockers.push("source_record_not_found");
    return { eligible: false, blockers, recommendation: "exclude" };
  }
  if (source.id !== spec.recordId) {
    blockers.push("record_id_mismatch");
  }
  if (source.brandId !== brandRecordId) {
    blockers.push("wrong_brand_link");
  }
  if (!isOfficialMarriottBonvoyUrl(source.sourceUrl)) {
    blockers.push("non_official_marriott_bonvoy_url");
  }
  if (normalizeUrl(source.sourceUrl) !== OFFICIAL_MARRIOTT_BONVOY_URLS.get(spec.recordId)) {
    blockers.push("url_mismatch_from_v25C_2F_package");
  }
  if (nz(source.visibility) === "Restricted" || nz(source.visibility) === "Private") {
    blockers.push(`restricted_visibility:${source.visibility}`);
  }
  if (nz(source.status) === "Rejected" || nz(source.status) === "Stale") {
    blockers.push(`blocked_status:${source.status}`);
  }

  if (blockers.length) {
    return { eligible: false, blockers, recommendation: "exclude" };
  }

  if (isSourceFullyStewarded(source) && nz(source.notes).includes("v25C-2G-S")) {
    return { eligible: true, blockers: [], recommendation: "hold", reason: "already_stewarded" };
  }

  return { eligible: true, blockers: [], recommendation: "steward" };
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-bonvoy-source-stewardship-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_CONFIRM}`;
}

export function buildNextBatchCommand(brandSlug = "tribute-portfolio") {
  return `npm run ${NEXT_BATCH} -- --brand ${brandSlug} --dry-run`;
}

export function buildRichFactApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run ${NEXT_BATCH} -- --brand ${brandSlug} --apply --approve-brand-explorer-v25C-2G-rich-bonvoy-facts --founder-reviewed-rich-bonvoy-facts --confirm-bonvoy-sources-explorer-safe`;
}

export async function buildBrandExplorerBonvoySourceStewardshipWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  officialSourcesConfirmed = false,
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-2G-S pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const existingBonvoySource = await getPartnerSourceById(EXISTING_BONVOY_SOURCE_ID);
  const existingBonvoySourceReport = {
    recordId: EXISTING_BONVOY_SOURCE_ID,
    sourceUrl: EXISTING_BONVOY_SOURCE_URL,
    purpose: "Existing Bonvoy hub (report only — not patched by v25C-2G-S)",
    touched: false,
    approvedForExplorerUse: nz(existingBonvoySource?.approvedForExplorerUse) || "No",
    approvedForExtraction: nz(existingBonvoySource?.approvedForExtraction) || "No",
    alreadyApproved:
      nz(existingBonvoySource?.approvedForExplorerUse) === "Yes" &&
      nz(existingBonvoySource?.approvedForExtraction) === "Yes",
  };

  const sourceRecordsInspected = [];
  const proposedGovernanceUpdates = [];
  const sourcesWouldUpdate = [];
  const sourcesMatched = [];
  const excluded = [];
  const applyBlockers = [];

  for (const spec of TARGET_BONVOY_SOURCE_RECORDS) {
    const source = await getPartnerSourceById(spec.recordId);
    const assessment = assessTargetSource(source, spec, brandRecordId);
    const patchResult = source ? buildStewardshipPatch(source) : { patch: null, skipped: ["missing"] };

    const row = {
      recordId: spec.recordId,
      sourceUrl: spec.sourceUrl,
      purpose: spec.purpose,
      loyaltyRole: spec.loyaltyRole,
      sourceTitle: source?.sourceTitle || null,
      currentApprovedForExplorerUse: nz(source?.approvedForExplorerUse) || "No",
      currentApprovedForExtraction: nz(source?.approvedForExtraction) || "No",
      currentStatus: nz(source?.status) || null,
      currentVisibility: nz(source?.visibility) || null,
      officialMarriottBonvoyUrl: source ? isOfficialMarriottBonvoyUrl(source.sourceUrl) : false,
      assessment: assessment.recommendation,
      exclusionReasons: assessment.blockers,
      proposedPatch: patchResult.patch,
      proposedFieldsChanged: patchResult.fieldsChanged || [],
      wouldUpdate:
        assessment.recommendation === "steward" && Boolean(patchResult.patch),
    };

    sourceRecordsInspected.push(row);

    if (assessment.recommendation === "exclude") {
      excluded.push(row);
      applyBlockers.push(`source_excluded:${spec.recordId}:${assessment.blockers.join(",")}`);
      continue;
    }

    if (assessment.recommendation === "hold" && !patchResult.patch) {
      sourcesMatched.push(row);
      continue;
    }

    if (row.wouldUpdate) {
      proposedGovernanceUpdates.push({
        recordId: spec.recordId,
        sourceUrl: spec.sourceUrl,
        fields: patchResult.patch,
        approvedForExplorerUse: "Yes",
        approvedForExtraction: "Yes",
        notesAppend: STEWARDSHIP_NOTE,
      });
      sourcesWouldUpdate.push(row);
    }
  }

  const allTargetsStewardedAfterApply =
    sourceRecordsInspected.every(
      (r) =>
        r.assessment === "hold" ||
        (r.wouldUpdate && r.proposedPatch)
    ) && excluded.length === 0;

  const applyGatesReady = apply && approveBatch && officialSourcesConfirmed;
  const canApply =
    applyGatesReady &&
    excluded.length === 0 &&
    sourcesWouldUpdate.length > 0 &&
    applyBlockers.length === 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const skipped = [];
    const errors = [];
    for (const row of sourcesWouldUpdate) {
      try {
        await patchPartnerSource(row.recordId, row.proposedPatch);
        updated.push({ recordId: row.recordId, fields: row.proposedFieldsChanged });
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        errors.push({
          recordId: row.recordId,
          message: err?.message || String(err),
        });
      }
    }
    airtableModified = updated.length > 0 && errors.length === 0;
    applyResults = { updated, skipped, errors };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  } else if (apply) {
    applyResults = { updated: [], skipped: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const nonTargetSourcesTouched = false;

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C2GSWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: "tribute-portfolio",
    },
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-bonvoy-source-stewardship-writer.js",
      "scripts/brand-explorer-bonvoy-source-stewardship-writer.mjs",
      "docs/data-intelligence/brand-explorer-bonvoy-source-stewardship-writer-v25C-2G-S.md",
      "reports/brand-explorer-bonvoy-source-stewardship-writer.md",
      "reports/brand-explorer-bonvoy-source-stewardship-writer.json",
      "package.json",
    ],
    targetSourceRecordIds: [...TARGET_RECORD_IDS],
    sourceRecordsInspected,
    existingBonvoySourceReportOnly: existingBonvoySourceReport,
    currentExplorerExtractionStatus: sourceRecordsInspected.map((r) => ({
      recordId: r.recordId,
      approvedForExplorerUse: r.currentApprovedForExplorerUse,
      approvedForExtraction: r.currentApprovedForExtraction,
    })),
    proposedGovernanceUpdates,
    sourcesWouldUpdate: sourcesWouldUpdate.map((r) => r.recordId),
    sourcesMatched: sourcesMatched.map((r) => r.recordId),
    sourcesExcluded: excluded.map((r) => ({
      recordId: r.recordId,
      reasons: r.exclusionReasons,
    })),
    nonTargetSourcesTouched,
    partnerFactsUntouched: true,
    partnerFactsApproved: false,
    presentationRowsUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      officialSourcesConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand(),
    nextBatch: NEXT_BATCH,
    nextBatchVersion: NEXT_BATCH_VERSION,
    exactNextBatchCommand: buildNextBatchCommand(),
    exactRichFactApprovalDryRunCommand: buildNextBatchCommand(),
    exactRichFactApprovalApplyCommand: buildRichFactApplyCommand(),
    idempotentAfterApply: sourcesWouldUpdate.length === 0 && excluded.length === 0,
    allTargetsStewardedAfterApply,
    doesNotDo: [
      "Create or approve Partner Facts",
      "Create new Source Library records",
      "Patch recu6AFRZBBBNiCQn or any non-target source",
      "Update Brand Explorer Presentation rows",
      "Change Brand Basics or Company Validated",
      "Imply Marriott validated anything",
    ],
  };
}

export function buildBrandExplorerBonvoySourceStewardshipWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Bonvoy Source Stewardship Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-2G-S exists: **${report.v25C2GSWriterExists ? "yes" : "no"}**`,
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Target sources inspected | ${report.sourceRecordsInspected.length} |`,
    `| Sources would update | ${report.sourcesWouldUpdate.length} |`,
    `| Sources matched (idempotent) | ${report.sourcesMatched.length} |`,
    `| Sources excluded | ${report.sourcesExcluded.length} |`,
    `| Non-target sources touched | ${report.nonTargetSourcesTouched ? "yes" : "no"} |`,
    `| Partner facts untouched | ${report.partnerFactsUntouched ? "yes" : "no"} |`,
    `| Presentation rows untouched | ${report.presentationRowsUntouched ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Existing Bonvoy hub (report only)",
    "",
    `- \`${report.existingBonvoySourceReportOnly.recordId}\` · Explorer: **${report.existingBonvoySourceReportOnly.approvedForExplorerUse}** · Extraction: **${report.existingBonvoySourceReportOnly.approvedForExtraction}** · touched: **no**`,
    "",
    "## Source records inspected",
    "",
  ];

  for (const row of report.sourceRecordsInspected) {
    lines.push(
      `### ${row.recordId}`,
      "",
      `- URL: ${row.sourceUrl}`,
      `- Purpose: ${row.purpose}`,
      `- Official Marriott Bonvoy: **${row.officialMarriottBonvoyUrl ? "yes" : "no"}**`,
      `- Explorer approved: **${row.currentApprovedForExplorerUse}** · Extraction approved: **${row.currentApprovedForExtraction}**`,
      `- Assessment: **${row.assessment}** · would update: **${row.wouldUpdate ? "yes" : "no"}**`,
      ""
    );
  }

  if (report.proposedGovernanceUpdates?.length) {
    lines.push("## Proposed governance updates", "");
    for (const u of report.proposedGovernanceUpdates) {
      lines.push(
        `- \`${u.recordId}\`: Approved for Explorer Use → **Yes**; Approved for Extraction → **Yes**`
      );
    }
    lines.push("");
  }

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) {
      lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");
  lines.push("## v25C-2G re-run", "", "```bash", report.exactRichFactApprovalDryRunCommand, "```", "");
  lines.push(
    "## v25C-2G apply (after stewardship)",
    "",
    "```bash",
    report.exactRichFactApprovalApplyCommand,
    "```",
    ""
  );

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
