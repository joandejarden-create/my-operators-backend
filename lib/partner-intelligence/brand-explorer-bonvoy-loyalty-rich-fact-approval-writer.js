/**
 * Brand Explorer Bonvoy Loyalty Rich Fact Approval Writer v25C-2G.
 *
 * Gated founder approval for exactly thirteen pending rich Bonvoy facts from v25C-2F.
 * Dry-run by default. Never writes Brand Explorer Presentation rows or Company Validated.
 *
 * @see docs/data-intelligence/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer-v25C-2G.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { getPartnerSourceById, listPartnerSources } from "./airtable-source.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  PROPOSED_PENDING_FACTS,
  REPORT_JSON_NAME as ENHANCEMENT_JSON,
} from "./brand-explorer-bonvoy-loyalty-detail-enhancement-package.js";
import { ELIGIBLE_LOYALTY_FACT_KEYS } from "./brand-explorer-loyalty-fact-approval-writer.js";

export const WRITER_VERSION = "25C-2G";
export const REPORT_JSON_NAME = "brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.json";
export const REPORT_MD_NAME = "brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.md";
export const DOC_MD_NAME = "brand-explorer-bonvoy-loyalty-rich-fact-approval-writer-v25C-2G.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-2G-rich-bonvoy-facts";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-rich-bonvoy-facts";
export const APPLY_FLAG_SOURCES = "--confirm-bonvoy-sources-explorer-safe";

export const NEXT_BATCH = "brand-explorer-bonvoy-loyalty-row-enhancement-writer";
export const NEXT_BATCH_VERSION = "25C-2H";

const EXISTING_BONVOY_SOURCE_ID = "recu6AFRZBBBNiCQn";
const EXISTING_BONVOY_SOURCE_URL = "https://www.marriott.com/loyalty.mi";
const V25C2F_RUN_PREFIX = "tribute-bonvoy-rich-25C-2F";
const STEWARDSHIP_TAG = "v25C-2F rich Bonvoy loyalty enhancement";
const APPROVAL_TAG = "v25C-2G rich Bonvoy loyalty fact approval";

/** Exactly thirteen v25C-2F pending facts — record IDs from apply report. */
export const ELIGIBLE_RICH_FACT_RECORD_IDS = {
  "be.loyalty.earnEligibleSpend": "recnCQFgqTCkY3AKZ",
  "be.loyalty.earnWifiDirectBooking": "reclcSL6EkjUEbAwu",
  "be.loyalty.earnFreeNightsHeadline": "recKfOnCl3OKVc4vE",
  "be.loyalty.redeemFreeNights": "reckE45C1Q8oGBnnJ",
  "be.loyalty.redeemOnStayExperiences": "recESDD7EjXGrNqr8",
  "be.loyalty.redeemParticipatingNetwork": "recOXOyCTezxwZvxG",
  "be.loyalty.eliteMemberSummary": "rec179eABX8aNhPVF",
  "be.loyalty.eliteSilverSummary": "recnoxM5i3ksYSdeU",
  "be.loyalty.eliteGoldSummary": "rec8uCIAsKeMTk2KW",
  "be.loyalty.elitePlatinumSummary": "recK3Lx4ZfXClO86F",
  "be.loyalty.eliteTitaniumSummary": "rec0s2WeA16nRHpAj",
  "be.loyalty.eliteAmbassadorSummary": "recpgiySHIIwrVpkl",
  "be.loyalty.proofDirectBookingRelevance": "recGQAd6uebkyGCSQ",
};

export const ELIGIBLE_RICH_FACT_KEYS = Object.keys(ELIGIBLE_RICH_FACT_RECORD_IDS);

/** v25C-2F Source Library records to inspect for stewardship. */
export const V25C2F_SOURCE_LIBRARY_RECORDS = [
  {
    recordId: "rec8eRACSCyGnHRXH",
    sourceUrl: "https://www.marriott.com/loyalty/member-benefits.mi",
    purpose: "Elite tier summaries",
  },
  {
    recordId: "recc9NVMd7gvDKGBF",
    sourceUrl: "https://www.marriott.com/loyalty/earn.mi",
    purpose: "Earn mechanics",
  },
  {
    recordId: "recaAmqeCbXN3n89z",
    sourceUrl: "https://www.marriott.com/loyalty/redeem.mi",
    purpose: "Redeem mechanics",
  },
  {
    recordId: EXISTING_BONVOY_SOURCE_ID,
    sourceUrl: EXISTING_BONVOY_SOURCE_URL,
    purpose: "Existing Bonvoy hub (reused by v25C-2F facts)",
  },
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
  /\bfdd\b/i,
  /royalt(y|ies)/i,
  /initial franchise fee/i,
  /financial performance/i,
  /company validated/i,
  /marriott validated/i,
];

const REFERENCE_BRAND_COPY_PATTERNS = [
  /hilton honors/i,
  /choice privileges/i,
  /\bEQC\b/,
  /diamond reserve/i,
  /ihg one rewards/i,
  /radisson rewards/i,
  /fifth night free/i,
  /milestone rewards/i,
];

const OFFICIAL_BONVOY_URLS = new Set(
  [
    EXISTING_BONVOY_SOURCE_URL,
    "https://www.marriott.com/loyalty/member-benefits.mi",
    "https://www.marriott.com/loyalty/earn.mi",
    "https://www.marriott.com/loyalty/redeem.mi",
  ].map((u) => u.replace(/\/+$/, "").toLowerCase())
);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.md",
  "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.md",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.json",
  "reports/brand-explorer-loyalty-row-creation-writer.md",
  "reports/brand-explorer-loyalty-row-creation-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/partner-extracted-facts-airtable-fields.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/airtable-facts.js",
  "lib/partner-intelligence/airtable-source.js",
  "live Tribute Source Library records",
  "live Tribute Partner Facts",
  "live Tribute Brand Explorer Presentation rows",
  "live Curio/Kimpton/Radisson/Ascend loyalty rows",
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

function normalizeUrl(url) {
  return nz(url).replace(/\/+$/, "").toLowerCase();
}

function isExcludedFieldKey(fieldKey) {
  return EXCLUDED_FIELD_KEY_PATTERNS.some((re) => re.test(fieldKey));
}

function isKpiFact(fieldKey) {
  return /^loyalty\.kpi\./i.test(fieldKey) || /\.kpi\./i.test(fieldKey);
}

function containsReferenceBrandCopy(text) {
  return REFERENCE_BRAND_COPY_PATTERNS.some((re) => re.test(nz(text)));
}

function isV25C2FRichFact(fact) {
  const runId = nz(fact.extractionRunId);
  const notes = nz(fact.reviewerNotes);
  return (
    runId.includes(V25C2F_RUN_PREFIX) ||
    notes.includes(STEWARDSHIP_TAG) ||
    notes.includes("v25C-2F")
  );
}

function proposedPackageForKey(fieldKey) {
  return PROPOSED_PENDING_FACTS.find((f) => f.fieldKey === fieldKey) || null;
}

function isSourceStewarded(source) {
  if (!source) {
    return { sufficient: false, reason: "source_record_missing" };
  }
  if (!isApprovedExplorerSource(source)) {
    return { sufficient: false, reason: "source_not_approved_for_explorer_use" };
  }
  if (nz(source.approvedForExtraction) !== "Yes") {
    return { sufficient: false, reason: "source_not_approved_for_extraction" };
  }
  const url = normalizeUrl(source.sourceUrl);
  if (!OFFICIAL_BONVOY_URLS.has(url)) {
    return { sufficient: false, reason: "non_official_bonvoy_url" };
  }
  if (/fdd|franchise disclosure/i.test(nz(source.sourceTitle))) {
    return { sufficient: false, reason: "fdd_or_restricted_source_title" };
  }
  return { sufficient: true, reason: "explorer_and_extraction_approved" };
}

/**
 * @returns {{ eligible: boolean, recommendation: 'approve'|'exclude'|'hold', reasons: string[], sourceSafety: object }}
 */
export function assessEligibleRichBonvoyFact(fact, source, brandRecordId, expectedRecordId) {
  const reasons = [];
  const fieldKey = nz(fact?.fieldName);

  if (!ELIGIBLE_RICH_FACT_KEYS.includes(fieldKey)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["not_in_v25C_2G_allowlist"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  if (fact.id !== expectedRecordId) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: [`record_id_mismatch:expected_${expectedRecordId}_got_${fact.id}`],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  if (ELIGIBLE_LOYALTY_FACT_KEYS.includes(fieldKey)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["original_v25C_2B_fact_not_in_rich_batch"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  if (isExcludedFieldKey(fieldKey) || isKpiFact(fieldKey)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["excluded_key_pattern"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  if (fact.brandId !== brandRecordId) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["wrong_brand_link"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  const visibility = nz(fact.publicVisibility);
  if (visibility === "Internal Only" || visibility === "Restricted") {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: [`internal_or_restricted_visibility:${visibility}`],
      sourceSafety: { sufficient: false, reason: "internal_only_fact" },
    };
  }

  const value = nz(fact.extractedValue);
  const evidence = nz(fact.evidenceText);
  if (!value || nz(fact.dataGap) === "Yes") {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["missing_value_or_data_gap"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  if (!evidence) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["missing_evidence_text"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  if (BLOCKED_VALUE_PATTERNS.some((re) => re.test(value) || re.test(evidence))) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["fdd_economics_or_legal_fragment"],
      sourceSafety: { sufficient: false, reason: "blocked_content_pattern" },
    };
  }

  if (containsReferenceBrandCopy(value) || containsReferenceBrandCopy(evidence)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["reference_brand_copy_detected"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  const reviewStatus = nz(fact.humanReviewStatus);
  if (reviewStatus === "Approved" || reviewStatus === "Edited") {
    return {
      eligible: true,
      recommendation: "hold",
      reasons: ["already_approved_idempotent_skip"],
      sourceSafety: isSourceStewarded(source),
    };
  }

  if (reviewStatus !== "Pending") {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: [`unexpected_review_status:${reviewStatus}`],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  if (!isV25C2FRichFact(fact)) {
    return {
      eligible: false,
      recommendation: "exclude",
      reasons: ["outside_v25C_2F_rich_fact_batch"],
      sourceSafety: { sufficient: false, reason: "n/a" },
    };
  }

  const sourceSafety = isSourceStewarded(source);
  if (!sourceSafety.sufficient) {
    return {
      eligible: true,
      recommendation: "hold",
      reasons: [`source_stewardship_required:${sourceSafety.reason}`],
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

  const pkg = proposedPackageForKey(fieldKey);
  if (pkg && normalizeUrl(pkg.sourceUrl) !== normalizeUrl(source?.sourceUrl || "")) {
    reasons.push("source_url_differs_from_enhancement_package_note_only");
  }

  return {
    eligible: true,
    recommendation: "approve",
    reasons: reasons.length ? reasons : ["clean_pending_rich_bonvoy_fact"],
    sourceSafety,
  };
}

export function buildRichBonvoyFactApprovalPatch(fact) {
  const reviewStatus = nz(fact.humanReviewStatus);
  if (reviewStatus === "Approved" || reviewStatus === "Edited") {
    return { patch: null, skipped: ["already_approved"] };
  }
  if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Approved")) {
    return { patch: null, skipped: ["unknown_select_option:humanReviewStatus"] };
  }

  const stamp = new Date().toISOString().slice(0, 10);
  const priorNotes = nz(fact.reviewerNotes);
  const noteLine = `${APPROVAL_TAG}; not Marriott validation.`;
  const reviewerNotes = priorNotes.includes("v25C-2G")
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

function readJsonIfExists(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function buildFactRow(fact, source, assessment, pkg) {
  const patchResult = buildRichBonvoyFactApprovalPatch(fact);
  return {
    fieldKey: fact.fieldName,
    factRecordId: fact.id,
    brandRecordId: fact.brandId,
    sourceRecordId: fact.sourceRecordId,
    sourceTitle: source?.sourceTitle || null,
    sourceUrl: nz(source?.sourceUrl) || null,
    targetSlots: pkg?.targetSlots || [],
    tierTitle: pkg?.tierTitle || null,
    extractedValuePreview: short(fact.extractedValue, 180),
    evidencePreview: short(fact.evidenceText, 180),
    currentHumanReviewStatus: nz(fact.humanReviewStatus) || "Pending",
    publicVisibility: nz(fact.publicVisibility),
    extractionRunId: nz(fact.extractionRunId),
    confidenceLevel: nz(fact.confidenceLevel),
    confidenceScore: fact.confidenceScore ?? null,
    assessment: assessment.recommendation,
    exclusionReasons: assessment.reasons,
    sourceSafety: assessment.sourceSafety,
    proposedGovernanceChanges: patchResult.patch
      ? {
          humanReviewStatus: "Approved",
          approvedValue: patchResult.patch[MAP_PARTNER_FACT.approvedValue] || fact.approvedValue || fact.extractedValue,
          reviewerNotesAppend: APPROVAL_TAG,
          lastUpdated: patchResult.patch[MAP_PARTNER_FACT.lastUpdated],
        }
      : null,
    wouldApprove:
      assessment.recommendation === "approve" && Boolean(patchResult.patch),
  };
}

async function inspectSourceLibraryRecords(brandRecordId) {
  const inspected = [];
  for (const spec of V25C2F_SOURCE_LIBRARY_RECORDS) {
    const source = await getPartnerSourceById(spec.recordId);
    const stewardship = isSourceStewarded(source);
    inspected.push({
      recordId: spec.recordId,
      sourceUrl: spec.sourceUrl,
      purpose: spec.purpose,
      sourceTitle: source?.sourceTitle || null,
      approvedForExplorerUse: nz(source?.approvedForExplorerUse) || "No",
      approvedForExtraction: nz(source?.approvedForExtraction) || "No",
      stewardshipSufficient: stewardship.sufficient,
      stewardshipReason: stewardship.reason,
    });
  }
  return inspected;
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-bonvoy-loyalty-rich-fact-approval-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_SOURCES}`;
}

export function buildNextBatchCommand(brandSlug = "tribute-portfolio") {
  return `npm run ${NEXT_BATCH} -- --brand ${brandSlug} --package rich --dry-run`;
}

export async function buildBrandExplorerBonvoyLoyaltyRichFactApprovalWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  sourcesExplorerSafeConfirmed = false,
} = {}) {
  const brandRecordId = normalizeBrandInput(brandIdOrName);
  const brandSlug = brandRecordId === TRIBUTE_RECORD_ID ? "tribute-portfolio" : brandRecordId;

  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-2G pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const stewardshipReport = readJsonIfExists(
    "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json"
  );
  const enhancementReport = readJsonIfExists(`reports/${ENHANCEMENT_JSON}`);

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const sourceLibraryInspected = await inspectSourceLibraryRecords(brandRecordId);
  const sourceStewardshipSufficient = sourceLibraryInspected.every((s) => s.stewardshipSufficient);
  const sourcesRequiringStewardship = sourceLibraryInspected.filter((s) => !s.stewardshipSufficient);

  const allFacts = await fetchAllFacts(brandRecordId);
  const factsById = new Map(allFacts.map((f) => [f.id, f]));

  const inspected = [];
  const wouldApprove = [];
  const excluded = [];
  const heldForStewardship = [];
  const missingFacts = [];

  for (const [fieldKey, expectedRecordId] of Object.entries(ELIGIBLE_RICH_FACT_RECORD_IDS)) {
    const fact = factsById.get(expectedRecordId);
    const pkg = proposedPackageForKey(fieldKey);

    if (!fact) {
      missingFacts.push({ fieldKey, expectedRecordId, reason: "fact_record_not_found" });
      continue;
    }

    const source = fact.sourceRecordId
      ? await getPartnerSourceById(fact.sourceRecordId)
      : null;
    const assessment = assessEligibleRichBonvoyFact(fact, source, brandRecordId, expectedRecordId);
    const row = buildFactRow(fact, source, assessment, pkg);

    if (assessment.recommendation === "exclude") {
      excluded.push(row);
      inspected.push(row);
    } else if (assessment.recommendation === "hold") {
      if ((assessment.reasons || []).some((r) => r.startsWith("source_stewardship_required"))) {
        heldForStewardship.push(row);
      }
      inspected.push(row);
    } else if (row.wouldApprove) {
      wouldApprove.push(row);
      inspected.push(row);
    } else {
      inspected.push(row);
    }
  }

  const referenceBrandFactsLeaked = [
    ...inspected,
    ...excluded,
  ].filter((r) =>
    (r.exclusionReasons || []).includes("reference_brand_copy_detected")
  );

  const unsupportedKpiInPlan = [...inspected, ...excluded].some((r) =>
    isKpiFact(r.fieldKey)
  );
  const internalFddInPlan = [...inspected, ...excluded].some((r) =>
    (r.exclusionReasons || []).some((reason) => /fdd|internal|standards/i.test(reason))
  );

  const nonAllowlistedPendingRich = allFacts.filter((f) => {
    const key = nz(f.fieldName);
    if (ELIGIBLE_RICH_FACT_KEYS.includes(key)) return false;
    if (nz(f.humanReviewStatus) !== "Pending") return false;
    if (isV25C2FRichFact(f) || /be\.loyalty\./i.test(key)) return true;
    return false;
  });

  const applyBlockers = [];
  if (!sourceStewardshipSufficient) {
    applyBlockers.push("source_stewardship_required");
    for (const src of sourcesRequiringStewardship) {
      applyBlockers.push(`source_not_stewarded:${src.recordId}:${src.stewardshipReason}`);
    }
  }
  if (missingFacts.length) {
    applyBlockers.push(`missing_v25C_2F_facts:${missingFacts.map((m) => m.fieldKey).join(",")}`);
  }
  if (excluded.length) {
    applyBlockers.push(`excluded_facts:${excluded.map((r) => r.fieldKey).join(",")}`);
  }
  if (heldForStewardship.length && !sourceStewardshipSufficient) {
    applyBlockers.push(
      `held_for_stewardship:${heldForStewardship.map((r) => r.fieldKey).join(",")}`
    );
  }

  const applyGatesReady =
    apply && approveBatch && founderReviewed && sourcesExplorerSafeConfirmed;
  const canApply =
    applyGatesReady &&
    sourceStewardshipSufficient &&
    missingFacts.length === 0 &&
    excluded.length === 0 &&
    wouldApprove.length === ELIGIBLE_RICH_FACT_KEYS.length &&
    applyBlockers.length === 0;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const applied = [];
    const skipped = [];
    const errors = [];
    for (const row of wouldApprove) {
      const fact = factsById.get(row.factRecordId);
      const patchResult = buildRichBonvoyFactApprovalPatch(fact);
      if (!patchResult.patch) {
        skipped.push({ factRecordId: row.factRecordId, reasons: patchResult.skipped });
        continue;
      }
      try {
        await patchPartnerFact(row.factRecordId, patchResult.patch);
        applied.push({
          factRecordId: row.factRecordId,
          fieldKey: row.fieldKey,
          patch: patchResult.patch,
        });
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
  } else if (apply) {
    applyResults = { applied: [], skipped: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const factsSafeWhenStewarded = inspected.filter(
    (r) =>
      r.assessment !== "exclude" &&
      !(r.exclusionReasons || []).some((reason) =>
        /fdd|reference_brand|wrong_brand|outside_v25C/i.test(reason)
      )
  );

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C2GWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      name: BRAND_NAME,
      recordId: brandRecordId,
      slug: brandSlug,
    },
    pilot: "tribute-portfolio",
    marriottValidationImplied: false,
    sourcePackage: [
      "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json",
      "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.json",
    ],
    stewardshipReportExists: Boolean(stewardshipReport?.v25C2FWriterExists),
    enhancementReportExists: Boolean(enhancementReport?.v25C2EEnhancementPackageExists),
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.js",
      "scripts/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.mjs",
      "docs/data-intelligence/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer-v25C-2G.md",
      "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.md",
      "reports/brand-explorer-bonvoy-loyalty-rich-fact-approval-writer.json",
      "package.json",
    ],
    eligibleRichFactKeys: [...ELIGIBLE_RICH_FACT_KEYS],
    eligibleRichFactRecordIds: { ...ELIGIBLE_RICH_FACT_RECORD_IDS },
    sourceLibraryRecordsInspected: sourceLibraryInspected,
    sourceStewardshipSufficient,
    sourcesRequiringStewardship,
    factsInspected: inspected,
    factsWouldApprove: wouldApprove.map((r) => r.fieldKey),
    factsWouldApproveRows: wouldApprove,
    factsSafeToApproveWhenStewarded: factsSafeWhenStewarded.map((r) => r.fieldKey),
    factsHeldForSourceStewardship: heldForStewardship.map((r) => ({
      fieldKey: r.fieldKey,
      factRecordId: r.factRecordId,
      sourceRecordId: r.sourceRecordId,
      reason: (r.exclusionReasons || []).find((x) => x.startsWith("source_stewardship")) || null,
    })),
    factsExcluded: excluded,
    missingEligibleFacts: missingFacts,
    sourceSafetyByFact: inspected.map((r) => ({
      fieldKey: r.fieldKey,
      factRecordId: r.factRecordId,
      safe: r.sourceSafety?.sufficient === true,
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
    unsupportedKpiFactsIncluded: unsupportedKpiInPlan,
    internalFddFactsIncluded: internalFddInPlan,
    referenceBrandFactsLeakedIntoPlan: referenceBrandFactsLeaked.length > 0,
    referenceBrandFactsLeaked,
    nonAllowlistedPendingRichFacts: nonAllowlistedPendingRich.map((f) => ({
      fieldKey: f.fieldName,
      factRecordId: f.id,
      reason: "outside_v25C_2G_thirteen_fact_allowlist",
    })),
    presentationRowsUntouched: true,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    existingApprovedFactsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      sourcesExplorerSafeConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand(brandSlug),
    nextBatch: NEXT_BATCH,
    nextBatchVersion: NEXT_BATCH_VERSION,
    exactNextBatchCommand: buildNextBatchCommand(brandSlug),
    nextBatchRecommendation:
      "After all 13 rich facts are Approved and sources are stewarded, run v25C-2H brand-explorer-bonvoy-loyalty-row-enhancement-writer --package rich to upgrade loyalty presentation row bodies.",
    idempotentAfterApply:
      wouldApprove.length === 0 && inspected.every((r) => r.currentHumanReviewStatus === "Approved"),
    doesNotDo: [
      "Create or update Brand Explorer Presentation rows",
      "Create new Partner Facts or Source Library records",
      "Modify existing five approved v25C-2B Bonvoy facts",
      "Change images, Sort Order, or Brand Basics content fields",
      "Set Company Validated or Company Validation Date",
      "Approve standards/FDD/internal-only facts or loyalty KPI counts",
      "Approve facts outside the thirteen v25C-2F pending rich facts",
      "Imply Marriott validated anything",
    ],
  };
}

export function buildBrandExplorerBonvoyLoyaltyRichFactApprovalWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Bonvoy Loyalty Rich Fact Approval Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Writer exists: **${report.v25C2GWriterExists ? "yes" : "no"}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- Marriott validation implied: **no**`,
    "",
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Source stewardship sufficient | ${report.sourceStewardshipSufficient ? "yes" : "no"} |`,
    `| Facts inspected | ${report.factsInspected.length} |`,
    `| Facts would approve (now) | ${report.factsWouldApprove.length} |`,
    `| Facts safe when stewarded | ${report.factsSafeToApproveWhenStewarded.length} |`,
    `| Facts held for stewardship | ${report.factsHeldForSourceStewardship.length} |`,
    `| Facts excluded | ${report.factsExcluded.length} |`,
    `| Missing eligible facts | ${report.missingEligibleFacts.length} |`,
    `| Unsupported KPI in plan | ${report.unsupportedKpiFactsIncluded ? "yes" : "no"} |`,
    `| Internal/FDD in plan | ${report.internalFddFactsIncluded ? "yes" : "no"} |`,
    `| Reference-brand leak | ${report.referenceBrandFactsLeakedIntoPlan ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Presentation rows untouched | ${report.presentationRowsUntouched ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Source Library records inspected",
    "",
  ];

  for (const src of report.sourceLibraryRecordsInspected || []) {
    lines.push(
      `- \`${src.recordId}\` · ${src.purpose}`,
      `  - Explorer approved: **${src.approvedForExplorerUse}** · Extraction approved: **${src.approvedForExtraction}**`,
      `  - Stewardship: **${src.stewardshipSufficient ? "sufficient" : "required"}** (${src.stewardshipReason})`,
      ""
    );
  }

  if (report.sourcesRequiringStewardship?.length) {
    lines.push("## Sources requiring stewardship (blocks apply)", "");
    for (const src of report.sourcesRequiringStewardship) {
      lines.push(`- \`${src.recordId}\` (${src.sourceUrl}): ${src.stewardshipReason}`);
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
      `- Source safety: **${row.sourceSafety?.sufficient ? "safe" : "unsafe"}** (${row.sourceSafety?.reason || "n/a"})`,
      `- Reasons: ${row.exclusionReasons?.length ? row.exclusionReasons.join(", ") : "—"}`,
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

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) {
      lines.push(`- ${b}`);
    }
    lines.push("");
  }

  lines.push("## Exact apply command", "", "```bash", report.exactApplyCommand, "```", "");
  lines.push("## Next batch (v25C-2H)", "", "```bash", report.exactNextBatchCommand, "```", "");

  if (report.applyResults) {
    lines.push(
      "## Apply results",
      "",
      `- Applied: ${report.applyResults.applied?.length || 0}`,
      `- Skipped: ${report.applyResults.skipped?.length || 0}`,
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
