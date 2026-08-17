/**
 * Brand Explorer Evidence Fact Review Package v23B.
 *
 * Read-only founder review package for v23 targeted-extraction candidate facts.
 * No fact approval, no presentation writes, no Company Validated changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  V23_WAVE,
  V23_TRIBUTE_RULES,
  V23_NEEDS_NEW_CAPTURE,
  loadApprovedTributeSources,
} from "./tribute-portfolio-targeted-extract.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";

export const PACKAGE_VERSION = "23B";
export const REPORT_JSON_NAME = "brand-explorer-evidence-fact-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-evidence-fact-review-package.md";
export const DOC_MD_NAME = "brand-explorer-evidence-fact-review-package-v23B.md";

const TARGETED_EXTRACT_PATH = "reports/tribute-portfolio-targeted-extract.json";
const READINESS_PLAN_PATH = "reports/brand-explorer-evidence-required-slot-readiness-plan.json";
const SORT_ORDER_AUDIT_PATH = "reports/brand-explorer-presentation-sort-order-audit.json";

const DEFAULT_BRAND_ID = TRIBUTE_RECORD_ID;

const REVIEW_BUCKET = {
  SAFE_FOUNDER: "safe_for_founder_review",
  INTERNAL_ONLY: "internal_only",
  NEEDS_CAPTURE: "needs_source_capture",
  NOT_SAFE_DISPLAY: "not_safe_for_display",
};

const V23B_TARGET_SLOTS = [
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "standards.last_reviewed",
  "standards.requirement",
  "loyalty.proof",
  "overview.proof_operator",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function short(text, max = 160) {
  const s = nz(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  return nz(raw);
}

function readJson(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

async function fetchAllFacts(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: recordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function isV23LiveFact(fact, proposedKeys) {
  const key = nz(fact.fieldName);
  if (!proposedKeys.includes(key)) return false;
  const runId = nz(fact.extractionRunId);
  const notes = nz(fact.reviewerNotes);
  return runId.includes("tribute-targeted-v23") || notes.includes(V23_WAVE) || runId.startsWith("tribute-targeted");
}

function proposedFieldKeys(extractReport) {
  const fromCandidates = (extractReport?.v23CandidateFacts || []).map((f) => f.fieldKey);
  const fromRules = V23_TRIBUTE_RULES.map((r) => r.fieldKey);
  return [...new Set([...fromRules, ...fromCandidates])];
}

function riskLevelForFact(proposed, live) {
  const key = proposed.fieldKey;
  const value = nz(proposed.value || live?.extractedValue);
  const visibility = nz(proposed.publicVisibility || live?.publicVisibility);

  if (visibility === "Internal Only") return "high";
  if (key === "be.loyalty.programScaleStatement" && /\d+\+?\s*hotels/i.test(value)) return "medium";
  if (key === "be.loyalty.earnMechanics" || key === "be.loyalty.redeemMechanics") {
    if (/earn and redeem points/i.test(value)) return "medium";
  }
  if (key === "be.loyalty.eliteTierLadder" && /nextTab|Member Silver Elite Gold/i.test(value)) return "medium";
  if (key === "be.meta.fddDocumentVintage") return "low";
  if (proposed.confidenceLevel === "Low" || (proposed.confidenceScore || 0) < 60) return "medium";
  if (proposed.fddLegal || /fdd|legal|franchise agreement/i.test(nz(proposed.evidenceNote))) return "medium";
  return "low";
}

function reviewBucketForFact(proposed, live, creationStatus) {
  const visibility = nz(proposed.publicVisibility || live?.publicVisibility);
  const risk = riskLevelForFact(proposed, live);

  if (creationStatus === "proposed_not_created") return REVIEW_BUCKET.NEEDS_CAPTURE;
  if (visibility === "Internal Only") return REVIEW_BUCKET.INTERNAL_ONLY;
  if (risk === "high") return REVIEW_BUCKET.NOT_SAFE_DISPLAY;
  if (
    proposed.fieldKey === "be.loyalty.programScaleStatement" ||
    proposed.fieldKey === "be.positioning.independentCollectionStatement"
  ) {
    return REVIEW_BUCKET.SAFE_FOUNDER;
  }
  if (risk === "medium") return REVIEW_BUCKET.SAFE_FOUNDER;
  return REVIEW_BUCKET.SAFE_FOUNDER;
}

function supportsFutureWriter(proposed, live, bucket) {
  if (bucket === REVIEW_BUCKET.INTERNAL_ONLY) return false;
  if (bucket === REVIEW_BUCKET.NEEDS_CAPTURE) return false;
  if (bucket === REVIEW_BUCKET.NOT_SAFE_DISPLAY) return false;
  if (!live && !proposed) return false;
  return creationStatusIsLive(live, proposed) === "pending_review_live";
}

function creationStatusIsLive(live, proposed) {
  if (!live) return "proposed_not_created";
  const status = nz(live.humanReviewStatus);
  if (status === "Pending" || status === "Pending Review" || proposed.reviewStatus === "Pending Review") {
    return "pending_review_live";
  }
  if (status === "Approved" || status === "Edited") return "approved_live";
  return `live_${status.toLowerCase().replace(/\s+/g, "_") || "unknown"}`;
}

function canApproveLater(proposed, live, bucket) {
  if (bucket === REVIEW_BUCKET.NEEDS_CAPTURE) return false;
  if (bucket === REVIEW_BUCKET.NOT_SAFE_DISPLAY) return false;
  const status = creationStatusIsLive(live, proposed);
  return status === "pending_review_live";
}

function buildFactReviewRow(proposed, liveFactsByKey, applyCreatedByKey) {
  const live = liveFactsByKey.get(proposed.fieldKey) || null;
  const applyCreated = applyCreatedByKey.get(proposed.fieldKey);
  const creationStatus = live
    ? creationStatusIsLive(live, proposed)
    : applyCreated
      ? "pending_review_live"
      : "proposed_not_created";

  const bucket = reviewBucketForFact(proposed, live, creationStatus);
  const risk = riskLevelForFact(proposed, live);

  return {
    fieldKey: proposed.fieldKey,
    proposedValue: proposed.value,
    liveValue: nz(live?.extractedValue) || null,
    factRecordId: live?.id || applyCreated?.id || null,
    sourceRecordId: proposed.sourceId || live?.sourceRecordId || null,
    sourceTitle: proposed.sourceTitle || "",
    sourceUrl: proposed.sourceUrl || "",
    localFilePath: proposed.localFilePath || "",
    evidenceSnippet: proposed.evidence || live?.evidenceText || "",
    evidenceNote: proposed.evidenceNote || nz(live?.reviewerNotes),
    targetExplorerSlots: proposed.targetExplorerSlots || [],
    externalDisplayEligibility: proposed.displayEligibility || "",
    reviewStatus: live ? nz(live.humanReviewStatus) || "Pending" : proposed.reviewStatus || "Proposed",
    publicVisibility: proposed.publicVisibility || nz(live?.publicVisibility) || "Public",
    extractionType: proposed.extractionType || nz(live?.extractionType),
    confidenceLevel: proposed.confidenceLevel || nz(live?.confidenceLevel),
    confidenceScore: proposed.confidenceScore ?? live?.confidenceScore ?? null,
    creationStatus,
    applyWasRun: Boolean(applyCreated || live),
    reviewBucket: bucket,
    riskLevel: risk,
    supportsFutureV23CWriter: supportsFutureWriter(proposed, live, bucket),
    canApproveLater: canApproveLater(proposed, live, bucket),
    founderReviewNotes:
      bucket === REVIEW_BUCKET.INTERNAL_ONLY
        ? "Keep Internal Only unless legal/compliance sign-off for external paraphrase."
        : proposed.fieldKey === "be.loyalty.earnMechanics" || proposed.fieldKey === "be.loyalty.redeemMechanics"
          ? "Earn/redeem share generic Bonvoy headline — founder should confirm slot-specific wording before approval."
          : proposed.fieldKey === "be.meta.fddDocumentVintage"
            ? "Governance vintage only — do not present as Dealality or company review date."
            : proposed.fieldKey === "be.loyalty.programScaleStatement"
              ? "Source-backed scale statement — confirm Tribute-specific framing; not performance proof."
              : "Confirm excerpt matches approved Marriott-controlled source; not Marriott validation.",
  };
}

function slotsCoveredByFacts(factRows) {
  const covered = new Set();
  for (const row of factRows) {
    if (!row.supportsFutureV23CWriter) continue;
    for (const slot of row.targetExplorerSlots || []) covered.add(slot);
  }
  return [...covered].sort();
}

function assessV23CWriter(factRows, slotsStillLacking) {
  const v23bSlots = new Set(V23B_TARGET_SLOTS);
  const covered = slotsCoveredByFacts(factRows).filter((s) => v23bSlots.has(s));
  const pendingLive = factRows.filter((r) => r.creationStatus === "pending_review_live");
  const approvableLater = factRows.filter((r) => r.canApproveLater);

  const readyAfterApproval =
    approvableLater.length >= 4 &&
    covered.includes("loyalty.earn") &&
    covered.includes("loyalty.redeem") &&
    covered.includes("loyalty.elite") &&
    (covered.includes("standards.last_reviewed") || covered.includes("standards.requirement"));

  return {
    readyAfterFactApproval: readyAfterApproval,
    slotsCoveredByPendingFacts: covered,
    slotsStillLackingEvidence: slotsStillLacking,
    pendingLiveFactCount: pendingLive.length,
    factsApprovableLater: approvableLater.map((r) => r.fieldKey),
    note: readyAfterApproval
      ? "After founder approves pending public facts, a gated v23C evidence writer can be built for supported slots."
      : "Complete fact stewardship and fill standards.requirement gap before v23C writer.",
  };
}

export async function buildBrandExplorerEvidenceFactReviewPackageReport(options = {}) {
  const brandRecordId = normalizeBrandInput(options.brandIdOrName);
  const extractReport = readJson(TARGETED_EXTRACT_PATH);
  if (!extractReport) {
    throw new Error(`Missing ${TARGETED_EXTRACT_PATH}. Run tribute-portfolio-targeted-extract first.`);
  }

  const readinessPlan = readJson(READINESS_PLAN_PATH);
  const sortOrderAudit = readJson(SORT_ORDER_AUDIT_PATH);
  const sources = await loadApprovedTributeSources(brandRecordId);
  const liveFacts = await fetchAllFacts(brandRecordId);

  const proposedKeys = proposedFieldKeys(extractReport);
  const proposedFacts =
    (extractReport.v23CandidateFacts || []).length > 0
      ? extractReport.v23CandidateFacts
      : V23_TRIBUTE_RULES.filter((r) => r.fieldKey !== "be.standards.designStandardsDelivery").map((rule) => ({
          fieldKey: rule.fieldKey,
          targetExplorerSlots: rule.targetExplorerSlots,
          category: rule.category,
          approvable: rule.approvable,
          humanReview: rule.humanReview,
          extractionType: rule.extractionType,
          confidenceLevel: rule.confidenceLevel,
          confidenceScore: rule.confidenceScore,
          reviewStatus: "Pending Review",
          publicVisibility: rule.fddLegal ? "Internal Only" : "Public",
          displayEligibility: rule.fddLegal
            ? "pending_human_review_before_external_display"
            : "pending_human_review",
          v23Wave: V23_WAVE,
          value: "",
          evidence: "",
          evidenceNote: "",
        }));

  const applyCreated = extractReport.v23ApplyResult?.created || [];
  const applyCreatedByKey = new Map(applyCreated.map((c) => [c.fieldKey, c]));

  const liveFactsByKey = new Map();
  for (const f of liveFacts) {
    if (!isV23LiveFact(f, proposedKeys)) continue;
    liveFactsByKey.set(f.fieldName, f);
  }
  for (const f of liveFacts) {
    if (proposedKeys.includes(f.fieldName) && !liveFactsByKey.has(f.fieldName)) {
      liveFactsByKey.set(f.fieldName, f);
    }
  }

  const applyWasRun = liveFactsByKey.size > 0 || applyCreated.length > 0;

  const factReviewRows = proposedFacts.map((p) => buildFactReviewRow(p, liveFactsByKey, applyCreatedByKey));

  const notCreatedProposed = (extractReport.v23NotFound || []).map((n) => ({
    fieldKey: n.fieldKey,
    targetExplorerSlots: n.targetExplorerSlots || [],
    reason: n.reason,
    reviewBucket: REVIEW_BUCKET.NEEDS_CAPTURE,
    creationStatus: "proposed_not_created",
    supportsFutureV23CWriter: false,
    canApproveLater: false,
  }));

  const pendingReviewLiveCount = factReviewRows.filter((r) => r.creationStatus === "pending_review_live").length;
  const allEightExistPending =
    proposedFacts.length === 8 &&
    pendingReviewLiveCount === 8 &&
    factReviewRows.every((r) => r.creationStatus === "pending_review_live");

  const coveredSlots = new Set(
    factReviewRows.flatMap((r) => (r.creationStatus === "pending_review_live" ? r.targetExplorerSlots || [] : []))
  );
  const readinessCapture = (readinessPlan?.slotsNeedingNewSourceCapture || extractReport.v23Unsupported?.map((u) => u.targetExplorerSlot) || [])
    .filter((slot) => !coveredSlots.has(slot));

  const byBucket = {
    safeForFounderReview: factReviewRows.filter((r) => r.reviewBucket === REVIEW_BUCKET.SAFE_FOUNDER).map((r) => r.fieldKey),
    internalOnly: factReviewRows.filter((r) => r.reviewBucket === REVIEW_BUCKET.INTERNAL_ONLY).map((r) => r.fieldKey),
    needsSourceCapture: [
      ...notCreatedProposed.map((n) => n.fieldKey),
      ...readinessCapture,
    ],
    notSafeForDisplay: factReviewRows.filter((r) => r.reviewBucket === REVIEW_BUCKET.NOT_SAFE_DISPLAY).map((r) => r.fieldKey),
  };

  const slotsStillLacking = [
    ...new Set([
      ...(readinessPlan?.slotsStillLackingEvidence ||
        extractReport.v23EvidenceReadiness?.slotsStillLackingEvidence ||
        []),
      ...V23_NEEDS_NEW_CAPTURE,
    ]),
  ].sort();

  const v23cAssessment = assessV23CWriter(factReviewRows, slotsStillLacking);

  return {
    packageVersion: PACKAGE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: { recordId: brandRecordId, name: BRAND_NAME },
    filesRead: [
      "AGENTS.md",
      TARGETED_EXTRACT_PATH.replace(/^reports\//, "reports/"),
      "reports/tribute-portfolio-targeted-extract.md",
      READINESS_PLAN_PATH.replace(/^reports\//, "reports/"),
      "reports/brand-explorer-evidence-required-slot-readiness-plan.md",
      "lib/partner-intelligence/tribute-portfolio-targeted-extract.js",
      SORT_ORDER_AUDIT_PATH.replace(/^reports\//, "reports/"),
      "reports/brand-explorer-presentation-sort-order-audit.md",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-evidence-fact-review-package.js",
      "scripts/brand-explorer-evidence-fact-review-package.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    v23BReviewPackageExists: true,
    extractionApplyWasRun: applyWasRun,
    extractionApplyRunId:
      extractReport.v23ApplyResult?.runId ||
      [...liveFactsByKey.values()].map((f) => nz(f.extractionRunId)).find((id) => id.includes("v23")) ||
      null,
    candidateFactsProposedCount: proposedFacts.length,
    candidateFactsCreatedCount: Math.max(applyCreated.length, liveFactsByKey.size),
    candidateFactsPendingReviewLiveCount: pendingReviewLiveCount,
    allEightCandidateFactsExistAsPendingReview: allEightExistPending,
    eightCandidateFactsNote: applyWasRun
      ? `${Math.max(applyCreated.length, liveFactsByKey.size)} of ${proposedFacts.length} proposed facts exist live as Pending Review.`
      : "Extraction apply not run — facts are proposed in report only.",
    factReviewRows,
    notCreatedProposedFacts: notCreatedProposed,
    factsByReviewBucket: byBucket,
    slotsStillLackingEvidence: slotsStillLacking,
    factsApprovableLater: factReviewRows.filter((r) => r.canApproveLater).map((r) => r.fieldKey),
    v23CWriterAssessment: v23cAssessment,
    sourceRecordsReferenced: sources
      .filter((s) => ["recu6AFRZBBBNiCQn", "recF0qS9JIZjM3qza", "recjVfKnl9q18MO5w"].includes(s.id))
      .map((s) => ({
        id: s.id,
        title: s.sourceTitle,
        approvedForExplorerUse: nz(s.approvedForExplorerUse),
        approvedForExtraction: nz(s.approvedForExtraction),
      })),
    sortOrderAuditContext: sortOrderAudit
      ? {
          tributeRowCount: sortOrderAudit.tributeRowCount,
          likelyWriterDefaultCount: sortOrderAudit.sortOrderAuditSummary?.likelyWriterDefaultCount,
          futureSortOrderWriterNeeded: sortOrderAudit.futureSortOrderCorrectionWriterNeeded,
          note: "Sort Order correction is separate from v23C evidence writer.",
        }
      : null,
    exactNextCommand: applyWasRun
      ? "Founder review Pending facts in Airtable, then re-run: npm run brand-explorer-evidence-fact-review-package -- --brand tribute-portfolio --dry-run"
      : "npm run tribute-portfolio-targeted-extract -- --apply",
  };
}

export function buildBrandExplorerEvidenceFactReviewPackageMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Evidence Fact Review Package v23B");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** · Airtable modified: **no**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- Extraction apply run: **${report.extractionApplyWasRun ? "yes" : "no"}**`);
  lines.push(
    `- Candidate facts: **${report.candidateFactsProposedCount} proposed** · **${report.candidateFactsCreatedCount} created** · **${report.candidateFactsPendingReviewLiveCount} Pending Review live**`
  );
  lines.push(`- ${report.eightCandidateFactsNote}`);
  lines.push("");
  lines.push("## Review buckets");
  lines.push(`- Safe for founder review: ${report.factsByReviewBucket.safeForFounderReview.map((k) => `\`${k}\``).join(", ") || "none"}`);
  lines.push(`- Internal-only: ${report.factsByReviewBucket.internalOnly.map((k) => `\`${k}\``).join(", ") || "none"}`);
  lines.push(`- Needs source capture: ${report.factsByReviewBucket.needsSourceCapture.slice(0, 8).map((k) => `\`${k}\``).join(", ")}${report.factsByReviewBucket.needsSourceCapture.length > 8 ? "…" : ""}`);
  lines.push(`- Not safe for display: ${report.factsByReviewBucket.notSafeForDisplay.map((k) => `\`${k}\``).join(", ") || "none"}`);
  lines.push("");
  lines.push("## Fact-by-fact review");
  lines.push("| Field | Live ID | Status | Risk | Bucket | Slots | Writer? |");
  lines.push("|-------|---------|--------|------|--------|-------|---------|");
  for (const row of report.factReviewRows) {
    lines.push(
      `| \`${row.fieldKey}\` | ${row.factRecordId || "—"} | ${row.creationStatus} | ${row.riskLevel} | ${row.reviewBucket} | ${(row.targetExplorerSlots || []).join(", ")} | ${row.supportsFutureV23CWriter ? "yes" : "no"} |`
    );
  }
  lines.push("");
  lines.push("## v23C writer (after approval)");
  lines.push(`- Ready after fact approval: **${report.v23CWriterAssessment.readyAfterFactApproval ? "yes" : "no"}**`);
  lines.push(`- ${report.v23CWriterAssessment.note}`);
  lines.push("");
  lines.push("## Exact next command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
