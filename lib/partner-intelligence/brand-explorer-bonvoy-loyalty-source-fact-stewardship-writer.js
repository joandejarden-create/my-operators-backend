/**
 * Brand Explorer Bonvoy Loyalty Rich Source + Fact Stewardship Writer v25C-2F.
 *
 * Creates missing Marriott Bonvoy Source Library records and Pending Review facts
 * for the v25C-2E rich loyalty enhancement path. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer-v25C-2F.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import {
  MAP_PARTNER_FACT,
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_FACT_SELECTS,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { getRegistryField } from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { listPartnerFacts, createPartnerFact } from "./airtable-facts.js";
import { createPartnerSource, listPartnerSources } from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  ELIGIBLE_LOYALTY_FACT_KEYS,
} from "./brand-explorer-loyalty-fact-approval-writer.js";
import {
  PROPOSED_PENDING_FACTS,
  PROPOSED_SOURCE_LIBRARY_RECORDS,
  REPORT_JSON_NAME as ENHANCEMENT_JSON,
} from "./brand-explorer-bonvoy-loyalty-detail-enhancement-package.js";

export const WRITER_VERSION = "25C-2F";
export const REPORT_JSON_NAME = "brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json";
export const REPORT_MD_NAME = "brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.md";
export const DOC_MD_NAME = "brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer-v25C-2F.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-2F-bonvoy-sources-facts";
export const APPLY_FLAG_SOURCES = "--approve-brand-explorer-v25C-2F-source-library-create";
export const APPLY_FLAG_FACTS = "--approve-brand-explorer-v25C-2F-pending-fact-create";

export const NEXT_BATCH = "brand-explorer-bonvoy-loyalty-rich-fact-approval-writer";
export const NEXT_BATCH_VERSION = "25C-2G";

const EXISTING_BONVOY_SOURCE_ID = "recu6AFRZBBBNiCQn";
const EXISTING_BONVOY_SOURCE_URL = "https://www.marriott.com/loyalty.mi";
const RUN_ID_PREFIX = "tribute-bonvoy-rich-25C-2F";
const STEWARDSHIP_TAG = "v25C-2F rich Bonvoy loyalty enhancement";

const EXCLUDED_FIELD_PATTERNS = [
  /^be\.standards\./i,
  /^be\.meta\.fdd/i,
  /^loyalty\.kpi\./i,
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

const MARriott_VALIDATION_PATTERNS = [
  /marriott validated/i,
  /company validated/i,
  /marriott endorsed/i,
  /marriott approved/i,
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.md",
  "reports/brand-explorer-bonvoy-loyalty-detail-enhancement-package.json",
  "reports/brand-explorer-loyalty-row-creation-writer.md",
  "reports/brand-explorer-loyalty-row-creation-writer.json",
  "reports/brand-explorer-loyalty-fact-approval-writer.md",
  "reports/brand-explorer-loyalty-fact-approval-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "docs/brand-explorer-presentation-slots.md",
  "docs/partner-extracted-facts-airtable-fields.md",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/airtable-facts.js",
  "lib/partner-intelligence/airtable-source.js",
  "live Tribute Source Library records",
  "live Tribute Partner Facts",
  "live Tribute Brand Explorer Presentation rows",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeUrl(url) {
  return nz(url).replace(/\/+$/, "").toLowerCase();
}

function isOfficialMarriottUrl(url) {
  return /^https:\/\/(www\.)?marriott\.com\//i.test(nz(url));
}

function isApprovedFact(fact) {
  const st = nz(fact?.humanReviewStatus);
  return st === "Approved" || st === "Edited";
}

function isExcludedFieldKey(fieldKey) {
  return EXCLUDED_FIELD_PATTERNS.some((re) => re.test(fieldKey));
}

function isKpiFact(fieldKey) {
  return /^loyalty\.kpi\./i.test(fieldKey) || /\.kpi\./i.test(fieldKey);
}

function containsReferenceBrandCopy(text) {
  return REFERENCE_BRAND_COPY_PATTERNS.some((re) => re.test(nz(text)));
}

function impliesMarriottValidation(text) {
  return MARriott_VALIDATION_PATTERNS.some((re) => re.test(nz(text)));
}

function readEnhancementPackage() {
  const full = path.join(ROOT, "reports", ENHANCEMENT_JSON);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function confidenceScoreFor(level) {
  if (level === "High") return 80;
  if (level === "Low") return 58;
  return 72;
}

function buildSourcePayload(proposed, brandRecordId) {
  return {
    [MAP_PARTNER_SOURCE.profileType]: "Brand",
    [MAP_PARTNER_SOURCE.brand]: [brandRecordId],
    [MAP_PARTNER_SOURCE.sourceTitle]: `Tribute Portfolio — ${proposed.proposedTitle}`,
    [MAP_PARTNER_SOURCE.sourceType]: "Website Capture",
    [MAP_PARTNER_SOURCE.sourceUrl]: proposed.proposedUrl,
    [MAP_PARTNER_SOURCE.sourceOrigin]: "Public Web",
    [MAP_PARTNER_SOURCE.status]: "Found",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
    [MAP_PARTNER_SOURCE.sourceQuality]: "Medium",
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: "No",
    [MAP_PARTNER_SOURCE.notes]: `${STEWARDSHIP_TAG} — official Marriott Bonvoy public page. Steward before extraction. Do not imply Marriott validated.`,
  };
}

function validateSourcePayload(payload) {
  const errors = [];
  for (const [selectKey, col] of [
    ["profileType", MAP_PARTNER_SOURCE.profileType],
    ["sourceOrigin", MAP_PARTNER_SOURCE.sourceOrigin],
    ["visibility", MAP_PARTNER_SOURCE.visibility],
    ["verifiedSource", MAP_PARTNER_SOURCE.verifiedSource],
    ["sourceQuality", MAP_PARTNER_SOURCE.sourceQuality],
    ["status", MAP_PARTNER_SOURCE.status],
    ["approvedForExtraction", MAP_PARTNER_SOURCE.approvedForExtraction],
    ["approvedForExplorerUse", MAP_PARTNER_SOURCE.approvedForExplorerUse],
  ]) {
    const allowed = VAL_PARTNER_SOURCE_SELECTS[selectKey];
    const value = payload[col];
    if (allowed && value != null && !allowed.includes(value)) {
      errors.push(`invalid_${selectKey}:${value}`);
    }
  }
  if (!isOfficialMarriottUrl(payload[MAP_PARTNER_SOURCE.sourceUrl])) {
    errors.push("non_official_marriott_url");
  }
  return errors;
}

function buildFactPayload(proposed, sourceIdByUrl, brandRecordId, runId, pendingSourceUrls = new Set()) {
  const sourceUrl = proposed.sourceUrl;
  const sourceRecordId =
    sourceIdByUrl.get(normalizeUrl(sourceUrl)) ||
    (normalizeUrl(sourceUrl) === normalizeUrl(EXISTING_BONVOY_SOURCE_URL)
      ? EXISTING_BONVOY_SOURCE_ID
      : null);
  const urlPending = pendingSourceUrls.has(normalizeUrl(sourceUrl));

  const reg = getRegistryField(proposed.fieldKey, "Brand Explorer");
  const evidence =
    proposed.evidenceNote ||
    `Paraphrased from official Marriott Bonvoy source (${sourceUrl}) for ${STEWARDSHIP_TAG}.`;

  const fields = {
    [MAP_PARTNER_FACT.profileType]: "Brand",
    [MAP_PARTNER_FACT.brand]: [brandRecordId],
    [MAP_PARTNER_FACT.sourceRecord]: sourceRecordId ? [sourceRecordId] : [],
    [MAP_PARTNER_FACT.explorerType]: "Brand Explorer",
    [MAP_PARTNER_FACT.explorerSection]: reg?.explorerSection || "Loyalty & Commercial",
    [MAP_PARTNER_FACT.fieldName]: proposed.fieldKey,
    [MAP_PARTNER_FACT.extractedValue]: proposed.proposedValue,
    [MAP_PARTNER_FACT.normalizedValue]: proposed.proposedValue,
    [MAP_PARTNER_FACT.evidenceText]: evidence,
    [MAP_PARTNER_FACT.pageSectionAnchor]: sourceUrl,
    [MAP_PARTNER_FACT.sourceQuality]: proposed.confidenceLevel === "High" ? "High" : "Medium",
    [MAP_PARTNER_FACT.confidenceScore]: confidenceScoreFor(proposed.confidenceLevel),
    [MAP_PARTNER_FACT.confidenceLevel]: proposed.confidenceLevel || "Medium",
    [MAP_PARTNER_FACT.extractionType]: proposed.extractionType || "Directly Stated",
    [MAP_PARTNER_FACT.publicVisibility]: "Public",
    [MAP_PARTNER_FACT.humanReviewStatus]: "Pending",
    [MAP_PARTNER_FACT.dataGap]: "No",
    [MAP_PARTNER_FACT.reviewerNotes]: `${STEWARDSHIP_TAG} — Pending founder review; not company-validated; not Marriott-validated. Target slots: ${(proposed.targetSlots || []).join(", ")}.`,
    [MAP_PARTNER_FACT.followUpQuestion]:
      "Confirm paraphrase matches current Marriott Bonvoy public pages before approval for Explorer display.",
    [MAP_PARTNER_FACT.lastUpdated]: new Date().toISOString().slice(0, 10),
    [MAP_PARTNER_FACT.extractionRunId]: runId,
  };

  return {
    fieldKey: proposed.fieldKey,
    proposedValue: proposed.proposedValue,
    sourceUrl,
    sourceRecordId,
    targetSlots: proposed.targetSlots || [],
    tierTitle: proposed.tierTitle || null,
    publicVisibility: "Public",
    externalDisplaySafety: sourceRecordId
      ? "public_pending_review"
      : urlPending
        ? "public_pending_review_after_source_create"
        : "blocked_missing_source_link",
    humanReviewStatus: "Pending",
    reviewerNotes: fields[MAP_PARTNER_FACT.reviewerNotes],
    fields,
  };
}

function validateFactPlan(plan, { pendingSourceUrls = new Set() } = {}) {
  const blockers = [];
  if (!plan.sourceUrl || !/^https?:\/\//i.test(plan.sourceUrl)) {
    blockers.push(`missing_source_url:${plan.fieldKey}`);
  }
  if (!isOfficialMarriottUrl(plan.sourceUrl)) {
    blockers.push(`non_marriott_source_url:${plan.fieldKey}`);
  }
  const urlPending = pendingSourceUrls.has(normalizeUrl(plan.sourceUrl));
  if (!plan.sourceRecordId && !urlPending) {
    blockers.push(`missing_source_record_link:${plan.fieldKey}`);
  }
  if (containsReferenceBrandCopy(plan.proposedValue) || containsReferenceBrandCopy(plan.reviewerNotes)) {
    blockers.push(`reference_brand_copy_detected:${plan.fieldKey}`);
  }
  if (impliesMarriottValidation(plan.proposedValue) || impliesMarriottValidation(plan.reviewerNotes)) {
    blockers.push(`marriott_validation_implied:${plan.fieldKey}`);
  }
  if (isExcludedFieldKey(plan.fieldKey) || isKpiFact(plan.fieldKey)) {
    blockers.push(`excluded_field_key:${plan.fieldKey}`);
  }
  const hrs = plan.fields[MAP_PARTNER_FACT.humanReviewStatus];
  if (hrs !== "Pending") {
    blockers.push(`fact_not_pending:${plan.fieldKey}:${hrs}`);
  }
  return blockers;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_SOURCES} ${APPLY_FLAG_FACTS}`;
}

export function buildNextBatchCommand(brandSlug = "tribute-portfolio") {
  return `npm run ${NEXT_BATCH} -- --brand ${brandSlug} --dry-run`;
}

export async function buildBrandExplorerBonvoyLoyaltySourceFactStewardshipReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  approveSources = false,
  approveFacts = false,
} = {}) {
  const brandRecordId = TRIBUTE_RECORD_ID;
  const enhancement = readEnhancementPackage();
  if (!enhancement?.v25C2EEnhancementPackageExists) {
    throw new Error(
      "v25C-2E enhancement package missing — run brand-explorer-bonvoy-loyalty-detail-enhancement-package first"
    );
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  let existingSources = [];
  let offset = "";
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    existingSources.push(...(page.sources || []));
    offset = page.offset || "";
  } while (offset);

  const sourceIdByUrl = new Map();
  for (const s of existingSources) {
    if (s.sourceUrl) sourceIdByUrl.set(normalizeUrl(s.sourceUrl), s.id);
  }
  if (!sourceIdByUrl.has(normalizeUrl(EXISTING_BONVOY_SOURCE_URL))) {
    sourceIdByUrl.set(normalizeUrl(EXISTING_BONVOY_SOURCE_URL), EXISTING_BONVOY_SOURCE_ID);
  }

  const sourcesAlreadyExisting = [];
  const sourcesWouldCreate = [];
  const exactSourceCreatePayloads = [];

  for (const proposed of PROPOSED_SOURCE_LIBRARY_RECORDS) {
    const normalized = normalizeUrl(proposed.proposedUrl);
    const existing = existingSources.find((s) => normalizeUrl(s.sourceUrl) === normalized);
    if (existing) {
      sourcesAlreadyExisting.push({
        proposedUrl: proposed.proposedUrl,
        recordId: existing.id,
        sourceTitle: existing.sourceTitle,
      });
      continue;
    }
    const fields = buildSourcePayload(proposed, brandRecordId);
    const validationErrors = validateSourcePayload(fields);
    sourcesWouldCreate.push({
      proposedTitle: proposed.proposedTitle,
      proposedUrl: proposed.proposedUrl,
      sourceRole: proposed.sourceRole,
      fields,
      validationErrors,
      action: validationErrors.length ? "blocked_invalid" : "create",
    });
    if (!validationErrors.length) {
      exactSourceCreatePayloads.push({ table: "Partner Intelligence - Source Library", fields });
    }
  }

  const allFacts = [];
  offset = null;
  do {
    const page = await listPartnerFacts({ brandId: brandRecordId, limit: 100, offset });
    allFacts.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);

  const existingApprovedFactsReused = ELIGIBLE_LOYALTY_FACT_KEYS.map((key) => {
    const fact = allFacts.find((f) => nz(f.fieldName) === key);
    return {
      fieldKey: key,
      factRecordId: fact?.id || null,
      humanReviewStatus: nz(fact?.humanReviewStatus) || "missing",
      approved: fact ? isApprovedFact(fact) : false,
      untouched: true,
    };
  });

  const factsExcluded = [];
  const duplicateFactsFound = [];
  const pendingFactsWouldCreate = [];
  const exactFactCreatePayloads = [];
  const applyBlockers = [];

  const proposedFacts = PROPOSED_PENDING_FACTS.filter((f) => {
    if (isKpiFact(f.fieldKey) || isExcludedFieldKey(f.fieldKey)) {
      factsExcluded.push({ fieldKey: f.fieldKey, reason: "kpi_or_internal_fdd_excluded" });
      return false;
    }
    return true;
  });

  const pendingSourceUrls = new Set(
    sourcesWouldCreate.filter((s) => s.action === "create").map((s) => normalizeUrl(s.proposedUrl))
  );

  const runId = `${RUN_ID_PREFIX}-${randomUUID().slice(0, 8)}`;

  for (const proposed of proposedFacts) {
    const dupe = allFacts.find(
      (f) =>
        nz(f.fieldName) === proposed.fieldKey &&
        nz(f.brandId) === brandRecordId &&
        nz(f.humanReviewStatus) !== "Rejected"
    );
    if (dupe) {
      duplicateFactsFound.push({
        fieldKey: proposed.fieldKey,
        existingRecordId: dupe.id,
        humanReviewStatus: dupe.humanReviewStatus,
        reason: "duplicate_fact_field_key",
      });
      continue;
    }

    const plan = buildFactPayload(proposed, sourceIdByUrl, brandRecordId, runId, pendingSourceUrls);
    const factBlockers = validateFactPlan(plan, { pendingSourceUrls });

    if (factBlockers.length) {
      factsExcluded.push({ fieldKey: proposed.fieldKey, reasons: factBlockers });
      applyBlockers.push(...factBlockers);
      continue;
    }

    pendingFactsWouldCreate.push({
      ...plan,
      approvalStatus: "Pending",
      autoApproved: false,
      readyForV25C2GApproval: false,
      factApprovalRequiredFirst: true,
    });
    exactFactCreatePayloads.push({ table: "Partner Intelligence - Extracted Facts", fields: plan.fields });
  }

  for (const src of sourcesWouldCreate) {
    if (src.validationErrors.length) {
      applyBlockers.push(`source_invalid:${src.proposedUrl}:${src.validationErrors.join(",")}`);
    }
  }

  const applyGatesReady = apply && approveBatch && approveSources && approveFacts;
  const canApply =
    applyGatesReady &&
    applyBlockers.length === 0 &&
    (sourcesWouldCreate.filter((s) => s.action === "create").length > 0 ||
      pendingFactsWouldCreate.length > 0);

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;
  const createdSourceIds = new Map(sourceIdByUrl);

  if (canApply) {
    const createdSources = [];
    const createdFacts = [];
    const errors = [];

    for (const src of sourcesWouldCreate.filter((s) => s.action === "create")) {
      try {
        const rec = await createPartnerSource(src.fields);
        createdSources.push({ recordId: rec.id, url: src.proposedUrl, title: src.proposedTitle });
        createdSourceIds.set(normalizeUrl(src.proposedUrl), rec.id);
      } catch (err) {
        errors.push({ type: "source", url: src.proposedUrl, message: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const proposed of proposedFacts) {
      if (duplicateFactsFound.some((d) => d.fieldKey === proposed.fieldKey)) continue;
      const plan = buildFactPayload(proposed, createdSourceIds, brandRecordId, runId);
      const factBlockers = validateFactPlan(plan);
      if (factBlockers.length) {
        errors.push({ type: "fact_skipped", fieldKey: proposed.fieldKey, blockers: factBlockers });
        continue;
      }
      try {
        const fact = await createPartnerFact(plan.fields);
        createdFacts.push({ recordId: fact.id, fieldKey: proposed.fieldKey });
      } catch (err) {
        errors.push({ type: "fact", fieldKey: proposed.fieldKey, message: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified = createdSources.length > 0 || createdFacts.length > 0;
    applyResults = { createdSources, createdFacts, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply && applyBlockers.length > 0) {
    applyResults = { createdSources: [], createdFacts: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const internalFddExcluded = allFacts
    .filter((f) => /^be\.standards\./i.test(nz(f.fieldName)) || /^be\.meta\.fdd/i.test(nz(f.fieldName)))
    .map((f) => ({ fieldKey: f.fieldName, factRecordId: f.id }));

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C2FWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { recordId: brandRecordId, name: BRAND_NAME, slug: "tribute-portfolio" },
    sourcePackage: ENHANCEMENT_JSON,
    marriottValidationImplied: false,
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.js",
      "scripts/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.mjs",
      "docs/data-intelligence/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer-v25C-2F.md",
      "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.md",
      "reports/brand-explorer-bonvoy-loyalty-source-fact-stewardship-writer.json",
      "package.json",
    ],
    sourceLibraryRecordsWouldCreate: sourcesWouldCreate,
    sourceLibraryRecordsAlreadyExisting: sourcesAlreadyExisting,
    pendingFactsWouldCreate,
    duplicateFactsFound,
    existingApprovedFactsReused,
    factsExcluded,
    anyFactsAutoApproved: false,
    kpiFactsExcluded: true,
    internalFddFactsExcluded: internalFddExcluded.length > 0,
    internalFddFactsExcludedDetails: internalFddExcluded,
    presentationRowsUntouched: true,
    presentationRowsUpdated: false,
    partnerFactsCreatedOnApply: applyResults?.createdFacts?.length || 0,
    sourceLibraryRowsCreatedOnApply: applyResults?.createdSources?.length || 0,
    imagesUntouched: true,
    sortOrderUntouched: true,
    brandBasicsUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    airtableModified,
    exactSourceCreatePayloads,
    exactFactCreatePayloads,
    applyGates: { apply, approveBatch, approveSources, approveFacts, ready: applyGatesReady, canApply },
    applyBlockers: [...new Set(applyBlockers)],
    applyResults,
    exactApplyCommand: buildApplyCommand(),
    nextBatch: NEXT_BATCH,
    nextBatchVersion: NEXT_BATCH_VERSION,
    exactNextBatchCommand: buildNextBatchCommand(),
    nextBatchRecommendation:
      "After apply: steward new Bonvoy sources for extraction, then run v25C-2G rich fact approval writer before bonvoy-loyalty-row-enhancement-writer.",
    idempotentAfterApply:
      sourcesWouldCreate.filter((s) => s.action === "create").length === 0 &&
      pendingFactsWouldCreate.length === 0 &&
      duplicateFactsFound.length >= 0,
    doesNotDo: [
      "Update Brand Explorer Presentation rows",
      "Approve new or existing facts automatically",
      "Modify existing approved Bonvoy facts",
      "Create loyalty.kpi.* facts",
      "Use FDD/internal-only facts",
      "Change images, Sort Order, Brand Basics, or Company Validated",
      "Imply Marriott validated anything",
    ],
  };
}

export function buildBrandExplorerBonvoyLoyaltySourceFactStewardshipMarkdown(report) {
  const lines = [
    `# Brand Explorer Bonvoy Loyalty Source + Fact Stewardship Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-2F exists: **${report.v25C2FWriterExists ? "yes" : "no"}**`,
    `- Sources would create: **${report.sourceLibraryRecordsWouldCreate.filter((s) => s.action === "create").length}**`,
    `- Sources already existing: **${report.sourceLibraryRecordsAlreadyExisting.length}**`,
    `- Pending facts would create: **${report.pendingFactsWouldCreate.length}**`,
    `- Duplicate facts: **${report.duplicateFactsFound.length}**`,
    `- KPI facts excluded: **${report.kpiFactsExcluded ? "yes" : "no"}**`,
    `- Any facts auto-approved: **${report.anyFactsAutoApproved ? "yes" : "no"}**`,
    `- Presentation rows untouched: **${report.presentationRowsUntouched ? "yes" : "no"}**`,
    `- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`,
    "",
    "## Exact apply command",
    "",
    "```bash",
    report.exactApplyCommand,
    "```",
    "",
    "## Next batch (v25C-2G)",
    "",
    "```bash",
    report.exactNextBatchCommand,
    "```",
  ];

  if (report.applyBlockers?.length) {
    lines.push("", "## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }

  return lines.join("\n");
}
