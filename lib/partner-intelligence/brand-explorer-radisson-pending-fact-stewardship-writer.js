/**
 * Brand Explorer Radisson Pending Fact Stewardship Writer v28G.
 *
 * Reviews the three remaining pending Explorer facts for Radisson by Choice.
 * Default: dry-run. Rejects/archives thin superseded extracts as Internal Only —
 * does not approve weak copy or touch Company Validated / presentation rows.
 *
 * @see docs/data-intelligence/brand-explorer-radisson-pending-fact-stewardship-writer-v28G.md
 */
import {
  MAP_PARTNER_FACT,
  VAL_PARTNER_FACT_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { getPartnerSourceById } from "./airtable-source.js";
import { fetchBrandBasics, fetchLiveState } from "./tribute-portfolio-package-pipeline.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isApprovedExplorerSource } from "./profile-governance-publish-readiness.js";

export const WRITER_VERSION = "28G";
export const REPORT_JSON_NAME = "brand-explorer-radisson-pending-fact-stewardship-writer.json";
export const REPORT_MD_NAME = "brand-explorer-radisson-pending-fact-stewardship-writer.md";
export const DOC_MD_NAME = "brand-explorer-radisson-pending-fact-stewardship-writer-v28G.md";

export const TARGET_BRAND_SLUG = "radisson";
export const TARGET_RECORD_ID = "recywbx1YQSTCPqW1";
export const TARGET_BRAND_NAME = "Radisson by Choice";

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v28G-radisson-pending-fact-stewardship";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-radisson-fact-copy";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";

export const STEWARDSHIP_TAG = "v28G-radisson-pending-fact-stewardship";

/** Only these three pending facts are in scope for v28G. */
export const TARGET_PENDING_FIELD_KEYS = Object.freeze([
  "be.footprint.geoIntro",
  "be.overview.typicalUseCase",
  "be.overview.whyValue",
]);

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "tribute-portfolio",
  "curio-collection",
  "kimpton",
  "radisson-blu",
  "ascend",
  "radisson-individuals-by-choice",
  "tapestry-collection-by-hilton",
  "autograph-collection",
  "design-hotels",
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-radisson-active-profile-repair-writer.md",
  "reports/brand-explorer-radisson-active-profile-repair-writer.json",
  "reports/brand-explorer-final-qa-auditor.md",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-radisson.md",
  "reports/brand-explorer-complete-build-radisson.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "api/lib/partner-intelligence-field-map.js",
  "lib/partner-intelligence/airtable-facts.js",
  "lib/partner-intelligence/airtable-source.js",
  "live Radisson Partner Facts",
  "live Radisson Source Library records",
  "live Radisson Brand Explorer Presentation rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-radisson-pending-fact-stewardship-writer.js",
  "scripts/brand-explorer-radisson-pending-fact-stewardship-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const COMPANY_VALIDATION_BLOCK_RE =
  /company validated|validated by choice|validated by radisson|approved by radisson|brand approved|company-approved|official sign-off/i;

const INTERNAL_SURFACE_RE =
  /\b(source capture|internal extraction|paste into airtable|franchise disclosure document|item\s*19)\b/i;

/** Presentation slots that already render owner-facing copy for each fact key. */
const SUPERSEDED_BY_PRESENTATION = {
  "be.footprint.geoIntro": "footprint.geo_intro",
  "be.overview.typicalUseCase": "overview.typical_use_case",
  "be.overview.whyValue": "overview.why_value",
};

/**
 * Optional founder-reviewed replacements (reference only — not approved when presentation supersedes).
 */
const OPTIONAL_REWRITE_REFERENCE = {
  "be.overview.typicalUseCase":
    "Gateway cities, regional hubs, airport corridors, and secondary-market conversions where upscale full-service distribution and loyalty participation matter.",
};

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

async function fetchBrandApiShape(brandId) {
  const req = { query: { brandId, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status() {
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function presentationBodyForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  const row = blocks.find((b) => nz(b.slotKey) === slotKey);
  return nz(row?.body) || nz(row?.title);
}

function resolveTarget(brandArg) {
  const normalized = nz(brandArg || TARGET_BRAND_SLUG).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(normalized) && normalized !== TARGET_BRAND_SLUG) {
    throw new Error(`Brand ${normalized} is protected and cannot be modified by v28G`);
  }
  if (normalized !== TARGET_BRAND_SLUG && brandArg !== TARGET_RECORD_ID) {
    throw new Error(`v28G supports Radisson by Choice only (${TARGET_BRAND_SLUG})`);
  }
  const meta = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === TARGET_BRAND_SLUG);
  if (!meta) throw new Error("Could not resolve Radisson brand target");
  return meta;
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

function isExplorerFact(fact) {
  return nz(fact.explorerType) === "Brand Explorer" || nz(fact.fieldName).startsWith("be.");
}

function factValue(fact) {
  return nz(fact.approvedValue || fact.normalizedValue || fact.extractedValue);
}

function sourceStewardship(source) {
  if (!source) return { sufficient: false, reason: "missing_source" };
  if (!isApprovedExplorerSource(source)) {
    return { sufficient: false, reason: "source_not_approved_for_explorer" };
  }
  if (source.brandId && source.brandId !== TARGET_RECORD_ID) {
    return { sufficient: false, reason: "source_wrong_brand" };
  }
  return { sufficient: true, reason: "approved_explorer_source" };
}

export function classifyRadissonPendingFact(fact, { presentationBody = "", source = null } = {}) {
  const fieldKey = nz(fact.fieldName);
  const value = factValue(fact);
  const status = nz(fact.humanReviewStatus);
  const supersededSlot = SUPERSEDED_BY_PRESENTATION[fieldKey];
  const presentationHasBody = wordCount(presentationBody) >= 12;
  const src = sourceStewardship(source);

  if (!TARGET_PENDING_FIELD_KEYS.includes(fieldKey)) {
    return {
      fieldKey,
      classification: "out_of_scope",
      proposedAction: "none",
      approveReady: false,
      rationale: "Not in v28G pending-fact batch",
    };
  }

  if (status !== "Pending") {
    return {
      fieldKey,
      classification: "not_pending",
      proposedAction: "none",
      approveReady: false,
      rationale: `Review status is ${status || "unknown"} — idempotent skip`,
    };
  }

  if (COMPANY_VALIDATION_BLOCK_RE.test(value) || INTERNAL_SURFACE_RE.test(value)) {
    return {
      fieldKey,
      classification: "reject_archive",
      proposedAction: "reject_internal",
      approveReady: false,
      rationale: "Blocked validation or internal/source-capture language",
    };
  }

  if (fieldKey === "be.footprint.geoIntro") {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale:
        "Extract is a dated geography fragment only (“Americas as of September 30, 2024.”). Active-profile UI uses presentation footprint.geo_intro instead.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
      optionalRewriteReference: null,
    };
  }

  if (fieldKey === "be.overview.typicalUseCase") {
    return {
      fieldKey,
      classification: presentationHasBody ? "keep_internal" : "needs_rewritten_founder_reviewed_copy",
      proposedAction: presentationHasBody ? "reject_archive" : "hold_pending",
      approveReady: false,
      rationale: presentationHasBody
        ? "Thin extract (“travelers worldwide.”) superseded by presentation overview.typical_use_case."
        : "Thin extract needs founder rewrite before any approval.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
      optionalRewriteReference: OPTIONAL_REWRITE_REFERENCE[fieldKey],
    };
  }

  if (fieldKey === "be.overview.whyValue") {
    return {
      fieldKey,
      classification: "keep_internal",
      proposedAction: "reject_archive",
      approveReady: false,
      rationale:
        "Placeholder extract (“value proposition.”) superseded by presentation overview.why_value owner bullets.",
      supersededByPresentationSlot: supersededSlot,
      presentationAuthoritative: presentationHasBody,
      sourceSupport: src,
      optionalRewriteReference: null,
    };
  }

  return {
    fieldKey,
    classification: "needs_source_confirmation",
    proposedAction: "hold_pending",
    approveReady: false,
    rationale: "Unhandled pending fact — keep in stewardship queue",
  };
}

export function buildRadissonFactStewardshipPatch(fact, diagnosis) {
  if (diagnosis.proposedAction !== "reject_archive") {
    return { patch: null, skipped: [`action_${diagnosis.proposedAction}`] };
  }
  if (!VAL_PARTNER_FACT_SELECTS.humanReviewStatus.includes("Rejected")) {
    return { patch: null, skipped: ["unknown_select_option:Rejected"] };
  }
  if (!VAL_PARTNER_FACT_SELECTS.publicVisibility.includes("Internal Only")) {
    return { patch: null, skipped: ["unknown_select_option:Internal Only"] };
  }

  const stamp = new Date().toISOString().slice(0, 10);
  const note = [
    STEWARDSHIP_TAG,
    diagnosis.rationale,
    diagnosis.supersededByPresentationSlot
      ? `Superseded by presentation ${diagnosis.supersededByPresentationSlot}.`
      : "",
    "Not company validation.",
  ]
    .filter(Boolean)
    .join(" ");

  const prior = nz(fact.reviewerNotes);
  const reviewerNotes = prior.includes(STEWARDSHIP_TAG) ? prior : prior ? `${prior}\n${note}` : note;

  return {
    patch: {
      [MAP_PARTNER_FACT.humanReviewStatus]: "Rejected",
      [MAP_PARTNER_FACT.publicVisibility]: "Internal Only",
      [MAP_PARTNER_FACT.reviewerNotes]: reviewerNotes,
      [MAP_PARTNER_FACT.dataGap]: "Yes",
      [MAP_PARTNER_FACT.lastUpdated]: stamp,
    },
    skipped: [],
  };
}

export function buildApplyCommand() {
  return `npm run brand-explorer-radisson-pending-fact-stewardship-writer -- --brand ${TARGET_BRAND_SLUG} --apply ${APPLY_FLAG_APPROVE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_VALIDATION}`;
}

export async function buildBrandExplorerRadissonPendingFactStewardshipWriterReport({
  brandIdOrName = TARGET_BRAND_SLUG,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noValidationClaim = false,
} = {}) {
  const target = resolveTarget(brandIdOrName);
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  }

  const [brandBasicsBefore, liveState, brandApi, allFacts] = await Promise.all([
    fetchBrandBasics(target.recordId),
    fetchLiveState(target.recordId),
    fetchBrandApiShape(target.recordId),
    fetchAllFacts(target.recordId),
  ]);

  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const explorerFacts = allFacts.filter(isExplorerFact);
  const pendingFacts = explorerFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const targetPending = pendingFacts.filter((f) => TARGET_PENDING_FIELD_KEYS.includes(nz(f.fieldName)));

  const factDiagnosis = [];
  const factsToApprove = [];
  const factsToReject = [];
  const factsToHold = [];
  const applyPatches = [];
  const applyBlockers = [];

  for (const fieldKey of TARGET_PENDING_FIELD_KEYS) {
    const fact = targetPending.find((f) => nz(f.fieldName) === fieldKey) ||
      explorerFacts.find((f) => nz(f.fieldName) === fieldKey);
    if (!fact) {
      applyBlockers.push(`missing_fact:${fieldKey}`);
      factDiagnosis.push({ fieldKey, factId: null, error: "fact_not_found" });
      continue;
    }
    if (nz(fact.brandId) && fact.brandId !== target.recordId) {
      applyBlockers.push(`wrong_brand:${fact.id}`);
      continue;
    }

    const presentationSlot = SUPERSEDED_BY_PRESENTATION[fieldKey];
    const presentationBody = presentationBodyForSlot(brandApi, presentationSlot);
    const source = fact.sourceRecordId
      ? await getPartnerSourceById(fact.sourceRecordId).catch(() => null)
      : null;

    const diagnosis = classifyRadissonPendingFact(fact, { presentationBody, source });
    const { patch, skipped } = buildRadissonFactStewardshipPatch(fact, diagnosis);

    const row = {
      factId: fact.id,
      fieldName: fieldKey,
      currentValue: factValue(fact),
      currentStatus: nz(fact.humanReviewStatus),
      evidenceText: nz(fact.evidenceText).slice(0, 200),
      sourceRecordId: fact.sourceRecordId || null,
      sourceApprovedForExplorer: source ? isApprovedExplorerSource(source) : false,
      sourceUrl: nz(source?.sourceUrl).slice(0, 120),
      presentationSlot,
      presentationExcerpt: presentationBody.slice(0, 200),
      ...diagnosis,
      patchPreview: patch,
      patchSkipped: skipped,
    };
    factDiagnosis.push(row);

    if (diagnosis.proposedAction === "reject_archive" && patch) {
      factsToReject.push(row);
      applyPatches.push({ factId: fact.id, fieldName: fieldKey, patch, diagnosis });
    } else if (diagnosis.approveReady) {
      factsToApprove.push(row);
      applyBlockers.push(`unexpected_approve_ready:${fieldKey}`);
    } else {
      factsToHold.push(row);
      if (diagnosis.proposedAction !== "reject_archive") {
        applyBlockers.push(`hold:${fieldKey}`);
      }
      if (diagnosis.proposedAction === "reject_archive" && !patch) {
        applyBlockers.push(`reject_patch_blocked:${fieldKey}:${skipped.join(",")}`);
      }
    }
  }

  const outOfScopePending = pendingFacts.filter(
    (f) => !TARGET_PENDING_FIELD_KEYS.includes(nz(f.fieldName))
  );
  if (outOfScopePending.length) {
    applyBlockers.push(`unexpected_pending_facts:${outOfScopePending.map((f) => f.fieldName).join(",")}`);
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && noValidationClaim;
  const hasRejectWork = applyPatches.length === 3;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasRejectWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updated = [];
    const errors = [];
    for (const item of applyPatches) {
      try {
        const result = await patchPartnerFact(item.factId, item.patch);
        updated.push({ factId: result.id, fieldName: item.fieldName, status: result.humanReviewStatus });
      } catch (err) {
        errors.push({ factId: item.factId, fieldName: item.fieldName, message: err.message });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
    airtableModified = updated.length === applyPatches.length && errors.length === 0;
    applyResults = { updated, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(target.recordId));
  } else if (apply) {
    applyResults = { updated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const projectedPendingAfter = canApply ? 0 : pendingFacts.length;
  const projectedGovernance = {
    pendingFactsBefore: pendingFacts.length,
    pendingFactsAfter: projectedPendingAfter,
    factApprovalNeeded: projectedPendingAfter > 0,
    governedPlatformReady: true,
    sourceCount: (liveState.sources || []).length,
    approvedExplorerSources: (liveState.sources || []).filter((s) =>
      isApprovedExplorerSource(s)
    ).length,
  };

  const dryRunClean = applyBlockers.length === 0 && hasRejectWork && factsToApprove.length === 0;

  const report = {
    writerVersion: WRITER_VERSION,
    v28GWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: {
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      parentCompany: nz(brandBasicsBefore?.fields?.["Parent Company"]),
    },
    protectedBrandsUntouched: PROTECTED_BRAND_SLUGS,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    targetPendingFieldKeys: [...TARGET_PENDING_FIELD_KEYS],
    factDiagnosis,
    factsToApprove,
    factsToReject,
    factsToHold,
    applyPatches: applyPatches.map((p) => ({
      factId: p.factId,
      fieldName: p.fieldName,
      patch: p.patch,
      proposedAction: p.diagnosis.proposedAction,
    })),
    applyBlockers,
    dryRunClean,
    companyValidatedBefore,
    companyValidatedAfter,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    choiceValidationImplied: false,
    presentationRowsModified: false,
    airtableModified,
    applyResults,
    projectedGovernance,
    expectedPostApplyFinalQa: {
      pendingFactsCleared: canApply,
      sourceGovernanceScore: canApply ? 100 : 91,
      note: canApply
        ? "0 pending Explorer facts; governance apply-safety should clear"
        : "Pending facts remain until v28G apply",
    },
    exactDryRunCommand: `npm run brand-explorer-radisson-pending-fact-stewardship-writer -- --brand ${TARGET_BRAND_SLUG} --dry-run`,
    exactApplyCommand: buildApplyCommand(),
  };

  report.markdown = buildMarkdown(report);
  return report;
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Radisson Pending Fact Stewardship Writer v${WRITER_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- v28G exists: **${report.v28GWriterExists ? "yes" : "no"}**`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Pending facts before: **${report.projectedGovernance.pendingFactsBefore}**`);
  lines.push(`- Pending facts after (projected): **${report.projectedGovernance.pendingFactsAfter}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Fact diagnosis");
  for (const row of report.factDiagnosis) {
    lines.push(`### ${row.fieldName} (\`${row.factId || "missing"}\`)`);
    lines.push(`- Classification: **${row.classification}**`);
    lines.push(`- Proposed action: **${row.proposedAction}**`);
    lines.push(`- Rationale: ${row.rationale}`);
    lines.push(`- Current value: \`${row.currentValue}\``);
    if (row.presentationSlot) {
      lines.push(`- Presentation slot: \`${row.presentationSlot}\` (${row.presentationAuthoritative ? "authoritative" : "thin/missing"})`);
      lines.push(`- Presentation excerpt: ${row.presentationExcerpt || "(empty)"}`);
    }
    if (row.optionalRewriteReference) {
      lines.push(`- Optional rewrite reference (not approved): ${row.optionalRewriteReference}`);
    }
    lines.push(`- Source: \`${row.sourceRecordId || "none"}\` (approved for Explorer: ${row.sourceApprovedForExplorer ? "yes" : "no"})`);
  }
  lines.push("");
  lines.push("## Summary");
  lines.push(`- Approve: **${report.factsToApprove.length}**`);
  lines.push(`- Reject / keep internal: **${report.factsToReject.length}**`);
  lines.push(`- Hold pending: **${report.factsToHold.length}**`);
  if (report.applyBlockers.length) {
    lines.push("");
    lines.push("## Apply blockers");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
  }
  lines.push("");
  lines.push("## Exact apply command");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  return lines.join("\n");
}

export function buildBrandExplorerRadissonPendingFactStewardshipWriterMarkdown(report) {
  return report.markdown || buildMarkdown(report);
}
