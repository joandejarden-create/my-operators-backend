/**
 * Brand Explorer Loyalty Fact Approval Writer v25C-2B.
 *
 * Gated founder approval for exactly five pending Tribute Portfolio loyalty facts.
 * Dry-run by default. Never writes Brand Explorer Presentation rows or Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-loyalty-fact-approval-writer-v25C-2B.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  V23_TRIBUTE_RULES,
  V23_WAVE,
  TARGETED_TAG,
  classifyLoadedSourceRole,
  loadApprovedTributeSources,
} from "./tribute-portfolio-targeted-extract.js";

export const WRITER_VERSION = "25C-2B";
export const REPORT_JSON_NAME = "brand-explorer-loyalty-fact-approval-writer.json";
export const REPORT_MD_NAME = "brand-explorer-loyalty-fact-approval-writer.md";
export const DOC_MD_NAME = "brand-explorer-loyalty-fact-approval-writer-v25C-2B.md";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v25C-2B-loyalty-facts";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-loyalty-facts";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

/** Exactly five v23 loyalty facts eligible for this batch. */
export const ELIGIBLE_LOYALTY_FACT_KEYS = [
  "be.loyalty.earnMechanics",
  "be.loyalty.redeemMechanics",
  "be.loyalty.eliteTierLadder",
  "be.loyalty.memberRatesBenefit",
  "be.loyalty.programScaleStatement",
];

export const TARGET_FUTURE_SLOTS = [
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
];

const EXCLUDED_FIELD_KEY_PATTERNS = [
  /^be\.standards\./i,
  /^standards\.requirement/i,
  /^loyalty\.kpi\./i,
  /^be\.meta\.fdd/i,
];

const BLOCKED_VALUE_PATTERNS = [
  /item\s*19/i,
  /franchise (agreement|fee|disclosure)/i,
  /fdd\b/i,
  /royalt(y|ies)/i,
  /initial franchise fee/i,
  /financial performance/i,
  /company validated/i,
  /marriott validated/i,
];

const SAFE_SOURCE_ROLES = new Set(["bonvoy_page", "consumer_page"]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/brand-explorer-required-section-source-capture-package.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-evidence-fact-review-package.md",
  "reports/brand-explorer-evidence-fact-review-package.json",
  "reports/tribute-portfolio-targeted-extract.md",
  "reports/tribute-portfolio-targeted-extract.json",
  "docs/brand-explorer-presentation-slots.md",
  "docs/partner-extracted-facts-airtable-fields.md",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/airtable-facts.js",
  "lib/partner-intelligence/tribute-portfolio-targeted-extract.js",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function short(text, max = 140) {
  const s = nz(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return TRIBUTE_RECORD_ID;
  }
  return nz(raw);
}

function ruleForKey(fieldKey) {
  return V23_TRIBUTE_RULES.find((r) => r.fieldKey === fieldKey) || null;
}

function isExcludedFieldKey(fieldKey) {
  return EXCLUDED_FIELD_KEY_PATTERNS.some((re) => re.test(fieldKey));
}

function isKpiFact(fieldKey) {
  return /^loyalty\.kpi\./i.test(fieldKey) || /\.kpi\./i.test(fieldKey);
}

function isV23LoyaltyFact(fact) {
  const runId = nz(fact.extractionRunId);
  const notes = nz(fact.reviewerNotes);
  return (
    runId.includes("tribute-targeted-v23") ||
    notes.includes(V23_WAVE) ||
    runId.startsWith("tribute-targeted")
  );
}

function isSourceSafeForLoyalty(source) {
  if (!source) {
    return { safe: false, reason: "source_record_missing" };
  }
  if (!isApprovedExplorerSource(source)) {
    return { safe: false, reason: "source_not_approved_for_explorer_use" };
  }
  if (nz(source.approvedForExtraction) !== "Yes") {
    return { safe: false, reason: "source_not_approved_for_extraction" };
  }
  const role = classifyLoadedSourceRole(source);
  if (role === "local_pdf" || /fdd|franchise disclosure/i.test(nz(source.sourceTitle))) {
    return { safe: false, reason: "fdd_or_local_pdf_source_not_eligible" };
  }
  if (!SAFE_SOURCE_ROLES.has(role)) {
    return { safe: false, reason: `source_role_not_public_loyalty:${role || "unknown"}` };
  }
  return { safe: true, reason: `approved_public_source:${role}` };
}

/**
 * @returns {{ eligible: boolean, recommendation: 'approve'|'exclude'|'hold', reasons: string[], sourceSafety: object }}
 */
export function assessEligibleLoyaltyFact(fact, source, brandRecordId) {
  const reasons = [];
  const fieldKey = nz(fact?.fieldName);

  if (!ELIGIBLE_LOYALTY_FACT_KEYS.includes(fieldKey)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["not_in_v25C_2B_allowlist"],
      sourceSafety: { safe: false, reason: "n/a" },
    };
  }

  if (isExcludedFieldKey(fieldKey) || isKpiFact(fieldKey)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["excluded_key_pattern"],
      sourceSafety: { safe: false, reason: "n/a" },
    };
  }

  if (fact.brandId !== brandRecordId) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["wrong_brand_link"],
      sourceSafety: { safe: false, reason: "n/a" },
    };
  }

  const visibility = nz(fact.publicVisibility);
  if (visibility === "Internal Only" || visibility === "Restricted") {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: [`internal_or_restricted_visibility:${visibility}`],
      sourceSafety: { safe: false, reason: "internal_only_fact" },
    };
  }

  const value = nz(fact.extractedValue);
  const evidence = nz(fact.evidenceText);
  if (!value || nz(fact.dataGap) === "Yes") {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["missing_value_or_data_gap"],
      sourceSafety: { safe: false, reason: "n/a" },
    };
  }

  if (!evidence) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["missing_evidence_text"],
      sourceSafety: { safe: false, reason: "n/a" },
    };
  }

  if (BLOCKED_VALUE_PATTERNS.some((re) => re.test(value) || re.test(evidence))) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["fdd_economics_or_legal_fragment"],
      sourceSafety: { safe: false, reason: "blocked_content_pattern" },
    };
  }

  const reviewStatus = nz(fact.humanReviewStatus);
  if (reviewStatus === "Approved" || reviewStatus === "Edited") {
    return {
      eligible: true,
      recommendation: "hold",
      reasons: ["already_approved_idempotent_skip"],
      sourceSafety: isSourceSafeForLoyalty(source),
    };
  }

  if (reviewStatus !== "Pending") {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: [`unexpected_review_status:${reviewStatus}`],
      sourceSafety: { safe: false, reason: "n/a" },
    };
  }

  if (!isV23LoyaltyFact(fact)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["outside_v23_targeted_extraction_run"],
      sourceSafety: { safe: false, reason: "n/a" },
    };
  }

  const sourceSafety = isSourceSafeForLoyalty(source);
  if (!sourceSafety.safe) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: [`source_unsafe:${sourceSafety.reason}`],
      sourceSafety,
    };
  }

  if (source.brandId && source.brandId !== brandRecordId) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["source_wrong_brand_link"],
      sourceSafety,
    };
  }

  const rule = ruleForKey(fieldKey);
  if (rule?.fddLegal) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["fdd_legal_rule_flag"],
      sourceSafety,
    };
  }

  return {
    eligible: true,
    recommendation: "approve",
    reasons: ["clean_pending_loyalty_fact"],
    sourceSafety,
  };
}

export function buildLoyaltyFactApprovalPatch(fact) {
  const reviewStatus = nz(fact.humanReviewStatus);
  if (reviewStatus === "Approved" || reviewStatus === "Edited") {
    return { patch: null, skipped: ["already_approved"] };
  }
  if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Approved")) {
    return { patch: null, skipped: ["unknown_select_option:humanReviewStatus"] };
  }

  const stamp = new Date().toISOString().slice(0, 10);
  const priorNotes = nz(fact.reviewerNotes);
  const noteLine = `${TARGETED_TAG} v25C-2B: founder-reviewed loyalty fact approved for Brand Explorer row-creation prep; not Marriott validation.`;
  const reviewerNotes = priorNotes.includes("v25C-2B")
    ? priorNotes
    : priorNotes
      ? `${priorNotes}\n${noteLine}`
      : noteLine;

  const fields = {
    [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
    [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
    [MAP_PARTNER_FACT.lastUpdated]: stamp,
  };
  if (!nz(fact.approvedValue) && nz(fact.extractedValue)) {
    fields[MAP_PARTNER_FACT.approvedValue] = fact.extractedValue;
  }
  return { patch: fields, skipped: [] };
}

async function fetchAllFacts(brandRecordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function buildFactRow(fact, source, assessment, duplicate = false) {
  const rule = ruleForKey(fact.fieldName);
  const patchResult = buildLoyaltyFactApprovalPatch(fact);
  return {
    fieldKey: fact.fieldName,
    factRecordId: fact.id,
    brandRecordId: fact.brandId,
    sourceRecordId: fact.sourceRecordId,
    sourceTitle: source?.sourceTitle || null,
    sourceUrl: nz(source?.sourceUrl) || null,
    sourceRole: source ? classifyLoadedSourceRole(source) : null,
    targetFutureSlots: rule?.targetExplorerSlots || [],
    extractedValuePreview: short(fact.extractedValue, 180),
    evidencePreview: short(fact.evidenceText, 180),
    currentHumanReviewStatus: nz(fact.humanReviewStatus) || "Pending",
    publicVisibility: nz(fact.publicVisibility),
    extractionRunId: nz(fact.extractionRunId),
    confidenceLevel: nz(fact.confidenceLevel),
    confidenceScore: fact.confidenceScore ?? null,
    duplicate,
    assessment: assessment.recommendation,
    exclusionReasons: assessment.reasons,
    sourceSafety: assessment.sourceSafety,
    proposedGovernanceChanges: patchResult.patch
      ? {
          humanReviewStatus: "Approved",
          approvedValue: patchResult.patch[MAP_PARTNER_FACT.approvedValue] || fact.approvedValue || fact.extractedValue,
          reviewerNotesAppend: "v25C-2B founder-reviewed loyalty fact",
          lastUpdated: patchResult.patch[MAP_PARTNER_FACT.lastUpdated],
        }
      : null,
    wouldApprove: assessment.recommendation === "approve" && !duplicate && Boolean(patchResult.patch),
  };
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-loyalty-fact-approval-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER}`;
}

export async function buildBrandExplorerLoyaltyFactApprovalWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  const brandSlug = brandRecordId === TRIBUTE_RECORD_ID ? "tribute-portfolio" : brandRecordId;

  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-2B pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const allFacts = await fetchAllFacts(brandRecordId);
  const sources = await loadApprovedTributeSources(brandRecordId);
  const sourcesById = new Map(sources.map((s) => [s.id, s]));

  const loyaltyFacts = allFacts.filter((f) => ELIGIBLE_LOYALTY_FACT_KEYS.includes(nz(f.fieldName)));
  const factsByKey = new Map();
  for (const fact of loyaltyFacts) {
    const key = nz(fact.fieldName);
    if (!factsByKey.has(key)) factsByKey.set(key, []);
    factsByKey.get(key).push(fact);
  }

  const inspected = [];
  const wouldApprove = [];
  const excluded = [];
  const missingKeys = [];
  const nonLoyaltyLeaked = [];

  for (const key of ELIGIBLE_LOYALTY_FACT_KEYS) {
    const matches = factsByKey.get(key) || [];
    if (matches.length === 0) {
      missingKeys.push(key);
      continue;
    }
    const duplicate = matches.length > 1;
    const fact = matches[0];
    const source =
      sourcesById.get(fact.sourceRecordId) ||
      (fact.sourceRecordId ? await getPartnerSourceById(fact.sourceRecordId) : null);
    const assessment = assessEligibleLoyaltyFact(fact, source, brandRecordId);
    const row = buildFactRow(fact, source, assessment, duplicate);

    if (duplicate) {
      row.assessment = "exclude";
      row.exclusionReasons = ["duplicate_fact_records", `count:${matches.length}`];
      row.wouldApprove = false;
      excluded.push(row);
    } else if (row.wouldApprove) {
      wouldApprove.push(row);
      inspected.push(row);
    } else if (assessment.recommendation === "exclude" || duplicate) {
      excluded.push(row);
      inspected.push(row);
    } else {
      inspected.push(row);
    }
  }

  const pendingNonEligible = allFacts.filter((f) => {
    const key = nz(f.fieldName);
    if (ELIGIBLE_LOYALTY_FACT_KEYS.includes(key)) return false;
    if (nz(f.humanReviewStatus) !== "Pending") return false;
    if (/loyalty|bonvoy/i.test(key) || isKpiFact(key) || isExcludedFieldKey(key)) {
      return true;
    }
    return false;
  });

  for (const fact of pendingNonEligible) {
    nonLoyaltyLeaked.push({
      fieldKey: fact.fieldName,
      factRecordId: fact.id,
      reason: isKpiFact(fact.fieldName)
        ? "unsupported_kpi_fact_excluded"
        : isExcludedFieldKey(fact.fieldName)
          ? "standards_or_fdd_fact_excluded"
          : "non_allowlisted_pending_loyalty_related",
    });
  }

  const unsupportedKpiIncluded = [...allFacts, ...nonLoyaltyLeaked].some((x) =>
    isKpiFact(nz(x.fieldKey || x.fieldName))
  );
  const internalOnlyIncluded = inspected.some((r) => r.publicVisibility === "Internal Only");
  const fddFactsIncluded = excluded.some((r) =>
    (r.exclusionReasons || []).some((reason) => /fdd|legal/i.test(reason))
  );

  const applyGatesReady = apply && approveBatch && founderReviewed;
  const canApply =
    applyGatesReady &&
    missingKeys.length === 0 &&
    wouldApprove.length === ELIGIBLE_LOYALTY_FACT_KEYS.length &&
    excluded.filter((r) => r.assessment === "exclude").length === 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const applied = [];
    const skipped = [];
    const errors = [];
    for (const row of wouldApprove) {
      const fact = loyaltyFacts.find((f) => f.id === row.factRecordId);
      const patchResult = buildLoyaltyFactApprovalPatch(fact);
      if (!patchResult.patch) {
        skipped.push({ factRecordId: row.factRecordId, reasons: patchResult.skipped });
        continue;
      }
      try {
        await patchPartnerFact(row.factRecordId, patchResult.patch);
        applied.push({ factRecordId: row.factRecordId, fieldKey: row.fieldKey, patch: patchResult.patch });
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        errors.push({
          factRecordId: row.factRecordId,
          fieldKey: row.fieldKey,
          message: err.message || String(err),
        });
      }
    }
    airtableModified = applied.length > 0;
    applyResults = { applied, skipped, errors };

    const brandBasicsAfter = await fetchBrandBasics(brandRecordId);
    companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: brandSlug,
    },
    pilot: "tribute-portfolio",
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-loyalty-fact-approval-writer.js",
      "scripts/brand-explorer-loyalty-fact-approval-writer.mjs",
      "docs/data-intelligence/brand-explorer-loyalty-fact-approval-writer-v25C-2B.md",
      "reports/brand-explorer-loyalty-fact-approval-writer.md",
      "reports/brand-explorer-loyalty-fact-approval-writer.json",
      "package.json",
    ],
    eligibleFactKeys: [...ELIGIBLE_LOYALTY_FACT_KEYS],
    targetFutureSlots: [...TARGET_FUTURE_SLOTS],
    factsInspected: inspected,
    factsWouldApprove: wouldApprove.map((r) => r.fieldKey),
    factsWouldApproveRows: wouldApprove,
    factsExcluded: excluded,
    missingEligibleFacts: missingKeys,
    sourceSafetyByFact: inspected.map((r) => ({
      fieldKey: r.fieldKey,
      factRecordId: r.factRecordId,
      safe: r.sourceSafety?.safe === true,
      reason: r.sourceSafety?.reason || null,
      sourceRecordId: r.sourceRecordId,
      sourceTitle: r.sourceTitle,
    })),
    currentStatusByFact: inspected.map((r) => ({
      fieldKey: r.fieldKey,
      factRecordId: r.factRecordId,
      humanReviewStatus: r.currentHumanReviewStatus,
      publicVisibility: r.publicVisibility,
    })),
    proposedGovernanceChangesByFact: inspected
      .filter((r) => r.proposedGovernanceChanges)
      .map((r) => ({
        fieldKey: r.fieldKey,
        factRecordId: r.factRecordId,
        changes: r.proposedGovernanceChanges,
      })),
    unsupportedKpiFactsIncluded: unsupportedKpiIncluded,
    internalOnlyOrFddFactsIncluded: internalOnlyIncluded || fddFactsIncluded,
    nonLoyaltyFactsLeakedIntoPlan: nonLoyaltyLeaked,
    presentationRowsUntouched: true,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      ready: applyGatesReady,
      canApply,
    },
    applyResults,
    exactApplyCommand: buildApplyCommand(brandSlug),
    nextRecommendedCommand:
      "npm run brand-explorer-required-section-source-capture-package -- --brand tribute-portfolio --dry-run",
    doesNotDo: [
      "Create or update Brand Explorer Presentation rows",
      "Change images, Sort Order, or Brand Basics content fields",
      "Set Company Validated or Company Validation Date",
      "Approve standards/FDD/internal-only facts or loyalty KPI counts",
      "Imply Marriott validated anything",
      "Approve facts outside the five eligible loyalty keys",
    ],
  };
}

export function buildBrandExplorerLoyaltyFactApprovalWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Loyalty Fact Approval Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Writer exists: **${report.writerExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Marriott validation implied: **no**`,
    "",
    "## Summary",
    "",
    `| Metric | Value |`,
    `|--------|-------|`,
    `| Eligible facts inspected | ${report.factsInspected.length} |`,
    `| Facts safe to approve | ${report.factsWouldApprove.length} |`,
    `| Facts excluded | ${report.factsExcluded.length} |`,
    `| Missing eligible facts | ${report.missingEligibleFacts.length} |`,
    `| Unsupported KPI facts in plan | ${report.unsupportedKpiFactsIncluded ? "yes" : "no"} |`,
    `| Internal-only/FDD in plan | ${report.internalOnlyOrFddFactsIncluded ? "yes" : "no"} |`,
    `| Non-loyalty facts leaked | ${report.nonLoyaltyFactsLeakedIntoPlan.length} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Presentation rows untouched | ${report.presentationRowsUntouched ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Eligible fact keys",
    "",
    ...report.eligibleFactKeys.map((k) => `- \`${k}\``),
    "",
    "## Target future slots",
    "",
    ...report.targetFutureSlots.map((k) => `- \`${k}\``),
    "",
  ];

  if (report.missingEligibleFacts.length) {
    lines.push("## Missing eligible facts", "");
    for (const key of report.missingEligibleFacts) {
      lines.push(`- \`${key}\``);
    }
    lines.push("");
  }

  lines.push("## Facts inspected", "");
  for (const row of report.factsInspected) {
    lines.push(
      `### ${row.fieldKey}`,
      "",
      `- Record: \`${row.factRecordId}\``,
      `- Current status: **${row.currentHumanReviewStatus}**`,
      `- Assessment: **${row.assessment}** · would approve: **${row.wouldApprove ? "yes" : "no"}**`,
      `- Source safety: **${row.sourceSafety?.safe ? "safe" : "unsafe"}** (${row.sourceSafety?.reason || "n/a"})`,
      `- Exclusion reasons: ${row.exclusionReasons?.length ? row.exclusionReasons.join(", ") : "—"}`,
      `- Value: ${row.extractedValuePreview}`,
      ""
    );
  }

  if (report.factsExcluded.length) {
    lines.push("## Facts excluded", "");
    for (const row of report.factsExcluded) {
      lines.push(
        `- \`${row.fieldKey}\` (\`${row.factRecordId}\`): ${row.exclusionReasons?.join(", ") || row.assessment}`
      );
    }
    lines.push("");
  }

  if (report.nonLoyaltyFactsLeakedIntoPlan.length) {
    lines.push("## Non-loyalty pending facts (not in apply plan)", "");
    for (const row of report.nonLoyaltyFactsLeakedIntoPlan) {
      lines.push(`- \`${row.fieldKey}\` (\`${row.factRecordId}\`): ${row.reason}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");

  if (report.applyResults) {
    lines.push(
      "## Apply results",
      "",
      `- Applied: ${report.applyResults.applied?.length || 0}`,
      `- Skipped: ${report.applyResults.skipped?.length || 0}`,
      `- Errors: ${report.applyResults.errors?.length || 0}`,
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
