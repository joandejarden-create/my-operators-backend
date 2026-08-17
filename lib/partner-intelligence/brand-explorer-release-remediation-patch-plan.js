/**
 * v40 — Release remediation patch plan builders (dry-run).
 */
import {
  classifyOwnerCopyBlockerType,
  scrubPresentationRow,
  scrubOwnerFacingCopy,
  BRAND_MODEL_SCRUB_PROFILES,
} from "./brand-explorer-owner-copy-scrubber.js";
import { auditPresentationRowExternalOwner } from "./brand-explorer-external-owner-content-governance.js";

export const V40_PATCH_PLAN_VERSION = "v40";
export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const HIDE_DISPLAY_STATUS = "Do Not Display";

/** Product contract: minimum 3 visible property examples; extras allowed if clean. */
export const PROPERTY_EXAMPLE_POLICY = Object.freeze({
  minimum: 3,
  exactRequired: false,
  extrasAllowed: true,
  hideExtrasIfDuplicates: true,
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function tabFromSlot(slotKey) {
  return nz(slotKey).split(".")[0] || "other";
}

/**
 * Ingest v39-style external owner blockers + live row audits into classified blockers.
 */
export function ingestReleaseBlockers({
  brandSlug,
  presentationRows = [],
  v39FailedGates = [],
  gateInventory = null,
} = {}) {
  const blockers = [];

  for (const gateName of v39FailedGates || []) {
    if (gateName === "founder_visual_review_passed") {
      blockers.push({
        brand: brandSlug,
        type: "founder_review_missing",
        tab: null,
        slot: null,
        recordId: null,
        currentTitle: null,
        currentBody: null,
        forbiddenPhrase: null,
        ownerVisible: false,
        severity: "high",
        sourceOfTruth: "Brand Basics Founder Visual Review fields",
        proposedRemediation: "Founder visual review must pass after copy remediation; not patched in v40",
        airtableWriteRequired: true,
        rendererPatchRequired: false,
        founderJudgmentRequired: true,
      });
    }
    if (gateName === "active_profile_approval_set") {
      blockers.push({
        brand: brandSlug,
        type: "active_approval_missing",
        tab: null,
        slot: null,
        recordId: null,
        currentTitle: null,
        currentBody: null,
        forbiddenPhrase: null,
        ownerVisible: false,
        severity: "high",
        sourceOfTruth: "Brand Basics Active Profile Approved",
        proposedRemediation: "Blocked until gated active-release apply — not in v40",
        airtableWriteRequired: true,
        rendererPatchRequired: false,
        founderJudgmentRequired: true,
      });
    }
    if (gateName === "render_contract_pass") {
      blockers.push({
        brand: brandSlug,
        type: "render_contract_issue",
        tab: null,
        slot: null,
        recordId: null,
        currentTitle: null,
        currentBody: null,
        forbiddenPhrase: null,
        ownerVisible: false,
        severity: "medium",
        sourceOfTruth: "extendAssetPackWithRenderReadiness",
        proposedRemediation: "Re-evaluate after owner-copy scrub; may clear if copy was polluting readiness",
        airtableWriteRequired: false,
        rendererPatchRequired: false,
        founderJudgmentRequired: false,
      });
    }
  }

  for (const row of presentationRows || []) {
    if (row.visible === false) continue;
    const audit = auditPresentationRowExternalOwner(row);
    for (const hit of audit.hits || []) {
      if (!["critical", "high"].includes(hit.severity)) continue;
      const type = classifyOwnerCopyBlockerType(hit.patternId);
      blockers.push({
        brand: brandSlug,
        type,
        tab: tabFromSlot(row.slotKey),
        slot: row.slotKey,
        recordId: row.recordId,
        currentTitle: nz(row.title).slice(0, 120),
        currentBody: nz(row.body).slice(0, 200),
        forbiddenPhrase: hit.phrase,
        ownerVisible: true,
        severity: hit.severity,
        sourceOfTruth: "live Brand Explorer Presentation + auditExternalOwnerPhrase",
        proposedRemediation: "Scrub owner-facing Title/Body into diligence-safe language",
        airtableWriteRequired: true,
        rendererPatchRequired: false,
        founderJudgmentRequired: hit.severity === "critical",
        patternId: hit.patternId,
      });
    }
  }

  const openings = (presentationRows || []).filter(
    (r) => r.slotKey === "footprint.openings" && r.visible !== false && nz(r.imageUrl)
  );
  if (openings.length < PROPERTY_EXAMPLE_POLICY.minimum) {
    blockers.push({
      brand: brandSlug,
      type: "property_example_count_issue",
      tab: "footprint",
      slot: "footprint.openings",
      recordId: null,
      currentTitle: null,
      currentBody: null,
      forbiddenPhrase: null,
      ownerVisible: true,
      severity: "high",
      sourceOfTruth: "live Presentation imageUrl count",
      proposedRemediation: `Need >=${PROPERTY_EXAMPLE_POLICY.minimum} visible openings with imageUrl (have ${openings.length})`,
      airtableWriteRequired: true,
      rendererPatchRequired: false,
      founderJudgmentRequired: true,
    });
  }

  return blockers;
}

function buildPatchItem({
  brandSlug,
  recordId,
  slotKey,
  field,
  before,
  after,
  reason,
  blockerResolved,
  sourceSupportRetained,
  founderReviewRequired = false,
  safeForGenericApply = true,
  table = PRESENTATION_TABLE,
}) {
  const afterScrub = scrubOwnerFacingCopy(after, { slotKey, brandSlug });
  const brandModel = BRAND_MODEL_SCRUB_PROFILES[brandSlug];
  return {
    brand: brandSlug,
    recordId,
    table,
    slotKey,
    field,
    before,
    after,
    reason,
    blockerResolved,
    sourceSupportRetained: Boolean(sourceSupportRetained),
    externalOwnerCopyCheck: afterScrub.remainingForbidden.length === 0 ? "pass" : "fail",
    brandModelFitCheck: brandModel ? "pass" : "n/a",
    forbiddenTermCheck: afterScrub.remainingForbidden.length === 0 ? "pass" : "fail",
    founderReviewRequired: Boolean(founderReviewRequired),
    safeForGenericApply: Boolean(safeForGenericApply) && afterScrub.remainingForbidden.length === 0,
  };
}

/**
 * Build Presentation Body/Title scrub patches for dirty rows.
 */
export function buildOwnerCopyPatchPlan({ brandSlug, presentationRows = [] } = {}) {
  const patches = [];
  for (const row of presentationRows || []) {
    if (row.visible === false) continue;
    const audit = auditPresentationRowExternalOwner(row);
    const highHits = (audit.hits || []).filter((h) => ["critical", "high"].includes(h.severity));
    if (!highHits.length) continue;

    const scrub = scrubPresentationRow(row, { brandSlug });
    if (!scrub.changed) continue;
    for (const [field, after] of Object.entries(scrub.fields)) {
      const before =
        field === "Title"
          ? row.title
          : field === "Body"
            ? row.body
            : row[
                {
                  "Case Summary Overview": "caseSummaryOverview",
                  "Case Summary Brand Relevance": "caseSummaryBrandRelevance",
                  "Case Summary Owner Objective": "caseSummaryOwnerObjective",
                  "Case Summary Interpretation": "caseSummaryInterpretation",
                  "Case Summary Tags": "caseSummaryTags",
                }[field]
              ];
      if (nz(before) === nz(after)) continue;
      const blockerTypes = [
        ...new Set(highHits.map((h) => classifyOwnerCopyBlockerType(h.patternId))),
      ];
      patches.push(
        buildPatchItem({
          brandSlug,
          recordId: row.recordId,
          slotKey: row.slotKey,
          field,
          before: nz(before),
          after,
          reason: `Owner-copy scrub: ${blockerTypes.join(", ") || "forbidden_owner_copy"}`,
          blockerResolved: blockerTypes,
          sourceSupportRetained: scrub.sourceSupportRetained,
          founderReviewRequired: highHits.some((h) => h.severity === "critical"),
          safeForGenericApply: scrub.externalOwnerCopyClean,
        })
      );
    }
  }
  return patches;
}

/**
 * Property example count review — extras allowed by default (min 3).
 */
export function reviewPropertyExamples({ brandSlug, presentationRows = [] } = {}) {
  const openings = (presentationRows || [])
    .filter((r) => r.slotKey === "footprint.openings")
    .map((r) => ({
      recordId: r.recordId,
      title: nz(r.title),
      imageUrl: nz(r.imageUrl),
      visible: r.visible !== false,
      sortOrder: r.sortOrder ?? 0,
      externalDisplayStatus: r.externalDisplayStatus || "",
    }))
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));

  const visibleWithImage = openings.filter((o) => o.visible && o.imageUrl);
  const titles = new Map();
  const duplicates = [];
  for (const o of visibleWithImage) {
    const key = o.title.toLowerCase();
    if (titles.has(key)) duplicates.push(o);
    else titles.set(key, o);
  }

  const hidePatches = [];
  let recommendation = "keep_all_visible_extras";
  let note = `${visibleWithImage.length} visible property examples with imageUrl (minimum ${PROPERTY_EXAMPLE_POLICY.minimum}; extras allowed).`;

  if (visibleWithImage.length < PROPERTY_EXAMPLE_POLICY.minimum) {
    recommendation = "materialize_more_property_examples";
    note = `Only ${visibleWithImage.length}/${PROPERTY_EXAMPLE_POLICY.minimum} — cannot unlock until filled.`;
  } else if (duplicates.length && PROPERTY_EXAMPLE_POLICY.hideExtrasIfDuplicates) {
    recommendation = "hide_duplicate_extras";
    note = `Found ${duplicates.length} duplicate title(s); propose Do Not Display on duplicates only.`;
    for (const dup of duplicates) {
      hidePatches.push(
        buildPatchItem({
          brandSlug,
          recordId: dup.recordId,
          slotKey: "footprint.openings",
          field: "External Display Status",
          before: dup.externalDisplayStatus || "",
          after: HIDE_DISPLAY_STATUS,
          reason: "Hide duplicate property example; keep minimum 3 unique visible",
          blockerResolved: ["property_example_count_issue"],
          sourceSupportRetained: true,
          founderReviewRequired: true,
          safeForGenericApply: true,
        })
      );
    }
  } else if (visibleWithImage.length > PROPERTY_EXAMPLE_POLICY.minimum) {
    recommendation = "extras_acceptable_founder_confirm";
    note = `${visibleWithImage.length} visible examples exceed minimum 3; UI should handle extras. Founder confirm sort order is clean. No hide required.`;
  }

  return {
    brandSlug,
    policy: PROPERTY_EXAMPLE_POLICY,
    totalOpeningsRows: openings.length,
    visibleWithImageCount: visibleWithImage.length,
    duplicates: duplicates.map((d) => d.recordId),
    recommendation,
    note,
    hidePatches,
    visibleExamples: visibleWithImage.map((o) => ({
      recordId: o.recordId,
      title: o.title,
      sortOrder: o.sortOrder,
    })),
  };
}

/**
 * Assemble full patch plan for a brand.
 */
export function buildReleaseRemediationPatchPlan({
  brandSlug,
  presentationRows = [],
  v39FailedGates = [],
} = {}) {
  const blockers = ingestReleaseBlockers({ brandSlug, presentationRows, v39FailedGates });
  const copyPatches = buildOwnerCopyPatchPlan({ brandSlug, presentationRows });
  const propertyReview = reviewPropertyExamples({ brandSlug, presentationRows });
  const patches = [...copyPatches, ...propertyReview.hidePatches];

  return {
    version: V40_PATCH_PLAN_VERSION,
    brandSlug,
    blockers,
    blockerTypeCounts: blockers.reduce((acc, b) => {
      acc[b.type] = (acc[b.type] || 0) + 1;
      return acc;
    }, {}),
    patches,
    propertyReview,
    summary: {
      blockerCount: blockers.length,
      patchCount: patches.length,
      copyPatchCount: copyPatches.length,
      hidePatchCount: propertyReview.hidePatches.length,
      safeForGenericApplyCount: patches.filter((p) => p.safeForGenericApply).length,
      founderReviewRequiredCount: patches.filter((p) => p.founderReviewRequired).length,
    },
  };
}

export function renderBrandRemediationMarkdown(plan, projection) {
  const lines = [
    `# v40 Remediation — ${plan.brandSlug}`,
    "",
    `- Blockers: ${plan.summary.blockerCount}`,
    `- Patches: ${plan.summary.patchCount} (${plan.summary.copyPatchCount} copy, ${plan.summary.hidePatchCount} hide)`,
    `- Safe for generic apply: ${plan.summary.safeForGenericApplyCount}`,
    `- Property review: ${plan.propertyReview.recommendation} — ${plan.propertyReview.note}`,
    "",
    "## Blocker types",
  ];
  for (const [t, n] of Object.entries(plan.blockerTypeCounts || {})) {
    lines.push(`- \`${t}\`: ${n}`);
  }
  lines.push("", "## DOM projection");
  if (projection) {
    lines.push(`- forbidden strings: ${projection.forbiddenStringsBefore} → ${projection.forbiddenStringsAfter}`);
    lines.push(`- visible URLs: ${projection.visibleUrlsBefore} → ${projection.visibleUrlsAfter}`);
    lines.push(`- internal notes: ${projection.internalNotesBefore} → ${projection.internalNotesAfter}`);
    lines.push(`- empty cards: ${projection.emptyCardsBefore} → ${projection.emptyCardsAfter}`);
    lines.push(`- expected displayState after remediation: \`${projection.expectedDisplayStateAfter}\``);
    lines.push(`- still blocked by founder review: **${projection.stillBlockedByFounderReview ? "yes" : "no"}**`);
    lines.push(`- still blocked by active approval: **${projection.stillBlockedByActiveApproval ? "yes" : "no"}**`);
    lines.push(`- unlock in v40: **no**`);
  }
  lines.push("", "## Sample patches (first 15)");
  for (const p of (plan.patches || []).slice(0, 15)) {
    lines.push(`### ${p.slotKey} · ${p.field} (\`${p.recordId}\`)`);
    lines.push(`- Reason: ${p.reason}`);
    lines.push(`- Before: ${nz(p.before).slice(0, 180)}`);
    lines.push(`- After: ${nz(p.after).slice(0, 180)}`);
    lines.push(`- Checks: owner=${p.externalOwnerCopyCheck} forbidden=${p.forbiddenTermCheck} safeApply=${p.safeForGenericApply}`);
    lines.push("");
  }
  return lines.join("\n");
}
