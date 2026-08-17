/**
 * Tribute Portfolio Package Pipeline v1 — single-command orchestrator.
 *
 * Runs the source-backed text/governance path for one non-Choice brand
 * (Tribute Portfolio by Marriott) using the existing Dealality Intelligence
 * Factory primitives — no parallel one-off pipeline:
 *
 *   A. Source Registration     (createPartnerSource — web + local, skip dupes)
 *   B. Source Stewardship      (patchPartnerSource — Approved / Explorer / Extraction)
 *   C. Extraction              (runPartnerBrandExtraction — Pending facts only)
 *   D. Fact Stewardship        (patchPartnerFact — approve clean, HOLD FDD economics)
 *   E. Brand Setup Completion  (report/staging DRAFT only — never written)
 *   F. Governance Publish      (profile-governance readiness + diff — Company Materials)
 *   G. Verification            (live readiness re-check)
 *
 * Dry-run by default. Apply requires --approve-tribute-portfolio-package-pipeline
 * and runs only safe pending stages, skipping duplicates and halting on blockers.
 * Uses brand-source-auto-resolver so no manual source-ID wiring is required.
 *
 * Never: rebuilds Explorer, overwrites Brand Setup content/hero/image/logo,
 * downloads images, sets Company Validated / Company Validation Date, implies
 * Marriott validation, or publishes FDD economics externally.
 *
 * @see docs/data-intelligence/tribute-portfolio-package-pipeline-v1.md
 */
import Airtable from "airtable";
import {
  MAP_PARTNER_SOURCE,
  MAP_PARTNER_FACT,
  PARTNER_INTELLIGENCE_LINKS,
  PARTNER_INTELLIGENCE_FLAGS,
} from "../../api/lib/partner-intelligence-field-map.js";
import { createPartnerSource, patchPartnerSource, listPartnerSources } from "./airtable-source.js";
import { listPartnerFacts, patchPartnerFact } from "./airtable-facts.js";
import { runPartnerBrandExtraction } from "./run-extraction.js";
import { buildBrandSourceAllowlist } from "./brand-source-auto-resolver.js";
import {
  assessPackageReadiness,
  buildPublishPackages,
  isApprovedExplorerSource,
} from "./profile-governance-publish-readiness.js";
import { buildPublishPlanEntry } from "./profile-governance-publish.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  HELD_FACT_KEYS,
  buildTributePortfolioApplyPlanReport,
} from "./tribute-portfolio-package-apply-plan.js";
import {
  TARGETED_TAG,
  TARGETED_APPROVABLE_KEYS,
  TARGETED_HUMAN_REVIEW_KEYS,
  isTargetedFact,
  isPlaceholderFact,
  shouldUseTargetedExtraction,
  runTargetedExtractionForPipeline,
  buildFactAuditFromList,
} from "./tribute-portfolio-targeted-extract.js";

export const PIPELINE_VERSION = "1";
export const REPORT_JSON_NAME = "tribute-portfolio-package-pipeline.json";
export const REPORT_MD_NAME = "tribute-portfolio-package-pipeline.md";
export const APPLY_FLAG = "--approve-tribute-portfolio-package-pipeline";

export const PIPELINE_STAGE = {
  BLOCKED: "Blocked",
  SOURCE_REGISTRATION: "Source Registration Needed",
  SOURCE_STEWARDSHIP: "Source Stewardship Needed",
  EXTRACTION: "Extraction Needed",
  FACT_STEWARDSHIP: "Fact Stewardship Needed",
  GOVERNANCE_PUBLISH: "Governance Publish Needed",
  VERIFICATION: "Verification Needed",
  GOVERNED_PLATFORM_READY: "Governed Platform Ready",
};

export const STAGE_ORDER = [
  PIPELINE_STAGE.SOURCE_REGISTRATION,
  PIPELINE_STAGE.SOURCE_STEWARDSHIP,
  PIPELINE_STAGE.EXTRACTION,
  PIPELINE_STAGE.FACT_STEWARDSHIP,
  PIPELINE_STAGE.GOVERNANCE_PUBLISH,
  PIPELINE_STAGE.VERIFICATION,
  PIPELINE_STAGE.GOVERNED_PLATFORM_READY,
];

/** Target fact keys eligible for source-backed auto-approval during fact stewardship. */
export const APPROVABLE_TARGET_KEYS = new Set([
  ...TARGETED_APPROVABLE_KEYS,
]);

/** Extra hold heuristics for FDD economics / fees / Item 19 / legal. */
export const HOLD_FACT_PATTERN =
  /economic|fee|royalt|item\s*19|franchise (agreement|term|obligation)|legal|financial performance/i;

export function buildPipelineApplyCommand() {
  return `npm run tribute-portfolio-package-pipeline -- --apply ${APPLY_FLAG}`;
}

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

/* ------------------------------------------------------------------ */
/* Brand Basics (governance target) read/patch — reuses Airtable SDK  */
/* ------------------------------------------------------------------ */

function brandBasicsTable() {
  return PARTNER_INTELLIGENCE_LINKS.brandBasics;
}

export async function fetchBrandBasics(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const base = new Airtable({ apiKey }).base(baseId);
  try {
    const rec = await base(brandBasicsTable()).find(recordId);
    const fields = rec.fields || {};
    return {
      id: rec.id,
      entityType: "brand",
      name: nz(fields["Brand Name"] || fields.brand_name) || null,
      fields,
    };
  } catch {
    return null;
  }
}

async function patchBrandBasics(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const base = new Airtable({ apiKey }).base(baseId);
  const rec = await base(brandBasicsTable()).update(recordId, fields, { typecast: true });
  return { id: rec.id, fields: rec.fields || {} };
}

/* ------------------------------------------------------------------ */
/* Live state                                                          */
/* ------------------------------------------------------------------ */

async function fetchAllSources(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ brandId: recordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
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

export async function fetchLiveState(recordId = TRIBUTE_RECORD_ID) {
  const [sources, facts, brandBasics] = await Promise.all([
    fetchAllSources(recordId),
    fetchAllFacts(recordId),
    fetchBrandBasics(recordId),
  ]);
  const allowlist = buildBrandSourceAllowlist(sources, { recordId });
  return { recordId, sources, facts, brandBasics, allowlist };
}

/* ------------------------------------------------------------------ */
/* Stage detection                                                     */
/* ------------------------------------------------------------------ */

function isFullyStewarded(source) {
  return (
    (nz(source.status) === "Approved" || nz(source.status) === "Extracted") &&
    nz(source.approvedForExplorerUse) === "Yes"
  );
}

export function detectStage(liveState, plannedRegistrationCount) {
  const { sources, facts } = liveState;
  const targetFacts = facts.filter((f) => nz(f.explorerType) === "Brand Explorer" || nz(f.fieldName).startsWith("be."));
  const pending = targetFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const approved = targetFacts.filter((f) => nz(f.humanReviewStatus) === "Approved" || nz(f.humanReviewStatus) === "Edited");
  const stewarded = sources.filter(isFullyStewarded);
  const extractionReady = sources.filter(
    (s) => isFullyStewarded(s) && nz(s.approvedForExtraction) === "Yes"
  );

  if (sources.length === 0 && plannedRegistrationCount > 0) {
    return { stage: PIPELINE_STAGE.SOURCE_REGISTRATION, applyRecommended: true };
  }
  if (sources.length > 0 && stewarded.length < sources.length) {
    return { stage: PIPELINE_STAGE.SOURCE_STEWARDSHIP, applyRecommended: true };
  }
  if (extractionReady.length > 0 && targetFacts.length === 0) {
    return { stage: PIPELINE_STAGE.EXTRACTION, applyRecommended: true };
  }
  if (pending.length > 0) {
    return { stage: PIPELINE_STAGE.FACT_STEWARDSHIP, applyRecommended: true };
  }
  if (approved.length >= 3) {
    return { stage: PIPELINE_STAGE.GOVERNANCE_PUBLISH, applyRecommended: true };
  }
  if (approved.length > 0) {
    return { stage: PIPELINE_STAGE.VERIFICATION, applyRecommended: false };
  }
  return { stage: PIPELINE_STAGE.SOURCE_REGISTRATION, applyRecommended: plannedRegistrationCount > 0 };
}

/* ------------------------------------------------------------------ */
/* Stage A — registration actions                                      */
/* ------------------------------------------------------------------ */

function findDuplicateSource(entry, liveSources) {
  return liveSources.find((s) => {
    if (entry.origin === "local" && entry.localFilePath) {
      return nz(s.localFilePath) && nz(s.localFilePath) === nz(entry.localFilePath);
    }
    if (entry.sourceUrl) return nz(s.sourceUrl) === nz(entry.sourceUrl);
    return false;
  });
}

export function buildRegistrationActions(applyPlan, liveSources) {
  const entries = applyPlan.sourceRegistrationPlan.readyToRegister;
  return entries.map((e) => {
    const dupe = findDuplicateSource(e, liveSources);
    return {
      role: e.role,
      origin: e.origin,
      title: e.title,
      ref: e.sourceUrl || e.localFilePath,
      valid: e.valid,
      payload: e.payload,
      action: dupe ? "skip_duplicate" : e.valid ? "register" : "skip_invalid",
      duplicateOf: dupe?.id || null,
    };
  });
}

/* ------------------------------------------------------------------ */
/* Stage B — stewardship actions                                       */
/* ------------------------------------------------------------------ */

/** FDD/loyalty extraction restrictions are advisory; economics facts held at stage D. */
export function buildStewardshipActions(liveSources) {
  return liveSources.map((s) => {
    const needsStatus = nz(s.status) !== "Approved" && nz(s.status) !== "Extracted";
    const needsExplorer = nz(s.approvedForExplorerUse) !== "Yes";
    const needsExtraction = nz(s.approvedForExtraction) !== "Yes";
    const patch = {};
    if (needsStatus) patch[MAP_PARTNER_SOURCE.status] = "Approved";
    if (needsExplorer) patch[MAP_PARTNER_SOURCE.approvedForExplorerUse] = "Yes";
    if (needsExtraction) patch[MAP_PARTNER_SOURCE.approvedForExtraction] = "Yes";
    const isFdd = nz(s.sourceType) === "FDD";
    return {
      id: s.id,
      title: s.sourceTitle,
      sourceType: s.sourceType,
      action: Object.keys(patch).length ? "steward" : "already_stewarded",
      patch,
      extractionRestriction: isFdd
        ? "FDD extraction allowed; economics/fees/Item 19/legal facts HELD at fact stewardship."
        : /bonvoy|loyalty/i.test(`${s.sourceTitle} ${s.sourceUrl}`)
          ? "Loyalty page — extraction scoped to Bonvoy relationship only."
          : null,
    };
  });
}

/* ------------------------------------------------------------------ */
/* Stage D — fact stewardship actions                                  */
/* ------------------------------------------------------------------ */

export function isHeldFact(fact) {
  const key = nz(fact.fieldName);
  if (HELD_FACT_KEYS.has(key)) return true;
  const blob = `${key} ${nz(fact.evidenceText)} ${nz(fact.extractedValue)}`;
  return HOLD_FACT_PATTERN.test(blob);
}

export function buildFactStewardshipActions(liveFacts, { targetedPathActive = false } = {}) {
  const pending = liveFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const actions = [];
  for (const f of pending) {
    const key = nz(f.fieldName);
    const held = isHeldFact(f);
    const placeholder = isPlaceholderFact(f);
    const targeted = isTargetedFact(f);
    const isGap = nz(f.dataGap) === "Yes" || !nz(f.extractedValue);
    const weak =
      nz(f.extractionType) === "Inferred" ||
      nz(f.extractionType) === "Needs Confirmation" ||
      nz(f.confidenceLevel) === "Low";

    if (held) {
      actions.push({
        id: f.id,
        fieldName: key,
        decision: "hold_internal",
        patch: {
          [MAP_PARTNER_FACT.publicVisibility]: "Internal Only",
          [MAP_PARTNER_FACT.reviewerNotes]: "HOLD — FDD economics/legal/Item 19; human review before any external use.",
        },
      });
      continue;
    }

    if (placeholder && targetedPathActive) {
      actions.push({
        id: f.id,
        fieldName: key,
        decision: "reject_placeholder",
        patch: {
          [MAP_PARTNER_FACT.humanReviewStatus]: "Rejected",
          [MAP_PARTNER_FACT.reviewerNotes]: `${TARGETED_TAG}: generic data-gap placeholder superseded by targeted source-backed extraction — do not approve.`,
        },
        reason: "data_gap_placeholder_superseded",
      });
      continue;
    }

    if (placeholder) {
      actions.push({ id: f.id, fieldName: key, decision: "hold_human_review", patch: null, reason: "data_gap" });
      continue;
    }

    if (targeted && TARGETED_HUMAN_REVIEW_KEYS.has(key)) {
      actions.push({
        id: f.id,
        fieldName: key,
        decision: "hold_human_review",
        patch: {
          [MAP_PARTNER_FACT.reviewerNotes]: `${TARGETED_TAG}: AI-interpreted from company materials — human review before approval.`,
          [MAP_PARTNER_FACT.followUpQuestion]:
            "Confirm wording reflects company materials; not a Marriott endorsement.",
        },
        reason: "targeted_ai_interpreted",
      });
      continue;
    }

    if (targeted && TARGETED_APPROVABLE_KEYS.has(key) && !weak) {
      actions.push({
        id: f.id,
        fieldName: key,
        decision: "approve",
        patch: {
          [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
          [MAP_PARTNER_FACT.approvedValue]: f.normalizedValue || f.extractedValue,
          [MAP_PARTNER_FACT.reviewerNotes]: `${TARGETED_TAG}: source-backed fact approved.`,
        },
      });
      continue;
    }

    if (isGap || weak) {
      actions.push({ id: f.id, fieldName: key, decision: "hold_human_review", patch: null, reason: isGap ? "data_gap" : "weak_or_inferred" });
      continue;
    }
    if (APPROVABLE_TARGET_KEYS.has(key)) {
      actions.push({
        id: f.id,
        fieldName: key,
        decision: "approve",
        patch: {
          [MAP_PARTNER_FACT.humanReviewStatus]: "Approved",
          [MAP_PARTNER_FACT.approvedValue]: f.normalizedValue || f.extractedValue,
        },
      });
      continue;
    }
    actions.push({ id: f.id, fieldName: key, decision: "hold_human_review", patch: null, reason: "not_in_approvable_target_set" });
  }
  return {
    actions,
    approveCount: actions.filter((a) => a.decision === "approve").length,
    holdInternalCount: actions.filter((a) => a.decision === "hold_internal").length,
    holdReviewCount: actions.filter((a) => a.decision === "hold_human_review").length,
    rejectPlaceholderCount: actions.filter((a) => a.decision === "reject_placeholder").length,
  };
}

/* ------------------------------------------------------------------ */
/* Stage F — governance readiness + proposal                           */
/* ------------------------------------------------------------------ */

export function buildGovernancePlan(liveState) {
  const { sources, facts, brandBasics } = liveState;
  const packages = buildPublishPackages({ sources, facts, published: [] });
  const pkg =
    packages.find((p) => p.entityType === "brand" && p.recordId === liveState.recordId) ||
    packages[0] ||
    { entityType: "brand", recordId: liveState.recordId, sources: [], facts: [], published: [] };

  const targetProfile = brandBasics
    ? { id: brandBasics.id, entityType: "brand", name: brandBasics.name, fields: brandBasics.fields }
    : null;

  const readiness = assessPackageReadiness(pkg, targetProfile);
  const publishEntry =
    readiness.eligible && targetProfile
      ? buildPublishPlanEntry({
          packageEntry: {
            entityKey: `brand:${liveState.recordId}`,
            entityType: "brand",
            recordId: liveState.recordId,
            entityName: BRAND_NAME,
            proposed: readiness.proposal,
            blockReasons: readiness.blockReasons,
            changeClass: readiness.changeClass,
          },
          targetProfile,
          mode: "dry-run",
        })
      : null;

  return {
    eligible: readiness.eligible,
    blockReasons: readiness.blockReasons || [],
    changeClass: readiness.changeClass || null,
    approvedExplorerSourceCount: (sources || []).filter(isApprovedExplorerSource).length,
    approvedFactCount: readiness.publishScopeSourceCount != null ? readiness.factsUsedForProposal?.length || 0 : 0,
    proposed: readiness.proposal?.proposed || null,
    expectedGovernance: readiness.proposal?.expectedGovernance || null,
    fieldDiff: publishEntry?.fieldDiff || null,
    writePatch: publishEntry?.write?.patch || null,
    protectionBlocked: publishEntry?.protection?.blocked || false,
    protectionReasons: publishEntry?.protection?.reasons || [],
  };
}

/* ------------------------------------------------------------------ */
/* Orchestrator                                                        */
/* ------------------------------------------------------------------ */

export async function runTributePortfolioPackagePipeline({
  mode = "dry-run",
  recordId = TRIBUTE_RECORD_ID,
  probeUrls = true,
  applyPlanReport = null,
} = {}) {
  const isApply = mode === "apply";
  const applyLog = [];
  const stageResults = {};
  let halted = false;
  let haltReason = null;

  const applyPlan =
    applyPlanReport || (await buildTributePortfolioApplyPlanReport({ probeUrls }));

  let live = await fetchLiveState(recordId);
  let registrationActions = buildRegistrationActions(applyPlan, live.sources);
  const plannedRegistrations = registrationActions.filter((a) => a.action === "register").length;
  let stageInfo = detectStage(live, plannedRegistrations);

  const targetedPathActive = shouldUseTargetedExtraction(live.facts);
  let targetedPlan = await runTargetedExtractionForPipeline({
    recordId,
    mode: "dry-run",
    liveSources: live.sources,
    liveFacts: live.facts,
  });

  if (isApply) {
    /* A. Registration */
    const toRegister = registrationActions.filter((a) => a.action === "register");
    if (toRegister.length) {
      const registered = [];
      for (const a of toRegister) {
        try {
          const rec = await createPartnerSource(a.payload);
          registered.push({ id: rec.id, title: a.title });
          applyLog.push({ stage: "source_registration", id: rec.id, title: a.title });
        } catch (err) {
          halted = true;
          haltReason = `source_registration_error:${err.message || err}`;
          break;
        }
      }
      stageResults.source_registration = { registered: registered.length, skippedDuplicates: registrationActions.filter((x) => x.action === "skip_duplicate").length };
      live = await fetchLiveState(recordId);
      registrationActions = buildRegistrationActions(applyPlan, live.sources);
    } else {
      stageResults.source_registration = { skipped: true, reason: "already_registered_or_none" };
    }

    /* B. Stewardship */
    if (!halted) {
      const stewardActions = buildStewardshipActions(live.sources).filter((s) => s.action === "steward");
      if (stewardActions.length) {
        let applied = 0;
        for (const s of stewardActions) {
          try {
            await patchPartnerSource(s.id, s.patch);
            applied += 1;
            applyLog.push({ stage: "source_stewardship", id: s.id, title: s.title });
          } catch (err) {
            halted = true;
            haltReason = `source_stewardship_error:${err.message || err}`;
            break;
          }
        }
        stageResults.source_stewardship = { applied };
        live = await fetchLiveState(recordId);
      } else {
        stageResults.source_stewardship = { skipped: true, reason: "already_complete" };
      }
    }

    /* C. Extraction */
    if (!halted) {
      const extractionReady = live.sources.filter(
        (s) => isFullyStewarded(s) && nz(s.approvedForExtraction) === "Yes"
      );
      const targetFacts = live.facts.filter((f) => nz(f.fieldName).startsWith("be."));
      if (extractionReady.length && targetFacts.length === 0) {
        if (!PARTNER_INTELLIGENCE_FLAGS.extractionEnabled) {
          halted = true;
          haltReason = "extraction_disabled:set PARTNER_INTELLIGENCE_EXTRACTION_ENABLED=1 before apply";
          stageResults.extraction = { skipped: true, reason: "extraction_disabled" };
        } else {
          try {
            const result = await runPartnerBrandExtraction(recordId, { syncFolder: false });
            stageResults.extraction = { runId: result.runId, factsCreated: result.factsCreated };
            applyLog.push({ stage: "extraction", runId: result.runId, facts: result.factsCreated });
            live = await fetchLiveState(recordId);
          } catch (err) {
            halted = true;
            haltReason = `extraction_error:${err.message || err}`;
          }
        }
      } else {
        stageResults.extraction = { skipped: true, reason: targetFacts.length ? "facts_exist" : "no_extraction_ready_sources" };
      }
    }

    /* C2. Targeted extraction — when generic gaps dominate */
    if (!halted && shouldUseTargetedExtraction(live.facts)) {
      try {
        targetedPlan = await runTargetedExtractionForPipeline({
          recordId,
          mode: "apply",
          liveSources: live.sources,
          liveFacts: live.facts,
        });
        stageResults.targeted_extraction = {
          used: true,
          proposed: targetedPlan.proposedCount,
          created: targetedPlan.applyResult?.created?.length || 0,
          skippedDuplicate: targetedPlan.skippedDuplicate.length,
          runId: targetedPlan.applyResult?.runId || null,
          errors: targetedPlan.applyResult?.errors || [],
        };
        if (targetedPlan.applyResult?.created?.length) {
          applyLog.push({
            stage: "targeted_extraction",
            runId: targetedPlan.applyResult.runId,
            created: targetedPlan.applyResult.created.length,
          });
          live = await fetchLiveState(recordId);
        } else {
          stageResults.targeted_extraction.skipped = true;
          stageResults.targeted_extraction.reason = "no_new_targeted_facts";
        }
      } catch (err) {
        halted = true;
        haltReason = `targeted_extraction_error:${err.message || err}`;
      }
    } else if (!halted) {
      stageResults.targeted_extraction = {
        skipped: true,
        reason: targetedPlan.priorTargetedCount ? "targeted_facts_exist" : "not_needed",
      };
    }

    /* D. Fact stewardship */
    if (!halted) {
      const hasTargetedFacts =
        live.facts.some(isTargetedFact) || (targetedPlan.applyResult?.created?.length || 0) > 0;
      const useTargetedStewardship = hasTargetedFacts || shouldUseTargetedExtraction(live.facts);
      const factPlan = buildFactStewardshipActions(live.facts, { targetedPathActive: useTargetedStewardship });
      const writable = factPlan.actions.filter((a) => a.patch);
      if (writable.length) {
        let applied = 0;
        for (const a of writable) {
          try {
            await patchPartnerFact(a.id, a.patch);
            applied += 1;
            applyLog.push({ stage: "fact_stewardship", id: a.id, decision: a.decision, fieldName: a.fieldName });
          } catch (err) {
            halted = true;
            haltReason = `fact_stewardship_error:${err.message || err}`;
            break;
          }
        }
        stageResults.fact_stewardship = {
          applied,
          approve: factPlan.approveCount,
          holdInternal: factPlan.holdInternalCount,
          holdReview: factPlan.holdReviewCount,
          rejectPlaceholder: factPlan.rejectPlaceholderCount,
        };
        live = await fetchLiveState(recordId);
      } else {
        stageResults.fact_stewardship = { skipped: true, reason: "nothing_to_steward" };
      }
    }

    /* F. Governance publish */
    if (!halted) {
      const gov = buildGovernancePlan(live);
      if (gov.eligible && gov.writePatch && Object.keys(gov.writePatch).length && !gov.protectionBlocked) {
        try {
          await patchBrandBasics(recordId, gov.writePatch);
          stageResults.governance_publish = { applied: true, fields: Object.keys(gov.writePatch) };
          applyLog.push({ stage: "governance_publish", fields: Object.keys(gov.writePatch) });
          live = await fetchLiveState(recordId);
        } catch (err) {
          halted = true;
          haltReason = `governance_publish_error:${err.message || err}`;
        }
      } else {
        stageResults.governance_publish = {
          skipped: true,
          reason: gov.protectionBlocked ? "protected" : gov.eligible ? "no_changes" : "readiness_not_clean",
          blockReasons: gov.blockReasons,
        };
      }
    }

    registrationActions = buildRegistrationActions(applyPlan, live.sources);
    stageInfo = detectStage(live, registrationActions.filter((a) => a.action === "register").length);
  }

  return finalizeReport({
    mode,
    recordId,
    applyPlan,
    live,
    registrationActions,
    stageInfo,
    stageResults,
    applyLog,
    halted,
    haltReason,
    targetedPlan,
    targetedPathActive:
      targetedPathActive ||
      live.facts.some(isTargetedFact) ||
      (targetedPlan?.applyResult?.created?.length || 0) > 0,
  });
}

function finalizeReport(ctx) {
  const {
    mode,
    recordId,
    applyPlan,
    live,
    registrationActions,
    stageInfo,
    stageResults,
    applyLog,
    halted,
    haltReason,
    targetedPlan,
    targetedPathActive,
  } = ctx;

  const stewardshipActions = buildStewardshipActions(live.sources);
  const factPlan = buildFactStewardshipActions(live.facts, { targetedPathActive });
  const governance = buildGovernancePlan(live);
  const factAudit = buildFactAuditFromList(live.facts);

  const targetFacts = live.facts.filter((f) => nz(f.fieldName).startsWith("be."));
  const approvedFacts = targetFacts.filter((f) => nz(f.humanReviewStatus) === "Approved" || nz(f.humanReviewStatus) === "Edited");
  const pendingFacts = targetFacts.filter((f) => nz(f.humanReviewStatus) === "Pending");
  const heldInternal = targetFacts.filter((f) => nz(f.publicVisibility) === "Internal Only");

  const rejectedPlaceholders = targetFacts.filter(
    (f) => isPlaceholderFact(f) && nz(f.humanReviewStatus) === "Rejected"
  );
  const pendingPlaceholders = targetFacts.filter(
    (f) => isPlaceholderFact(f) && nz(f.humanReviewStatus) === "Pending"
  );
  const targetedFacts = targetFacts.filter(isTargetedFact);
  const approvedTargeted = targetedFacts.filter(
    (f) => nz(f.humanReviewStatus) === "Approved" || nz(f.humanReviewStatus) === "Edited"
  );

  const governedPlatformReady =
    approvedFacts.length >= 3 &&
    governance.approvedExplorerSourceCount >= 1 &&
    governance.eligible &&
    (nz(governance.proposed?.validationStatus) === "Company Published" ||
      nz(governance.expectedGovernance?.validationStatus) === "Company Published");

  return {
    pipelineVersion: PIPELINE_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified: mode === "apply" && applyLog.length > 0,
    halted,
    haltReason,
    brand: { name: BRAND_NAME, recordId },
    currentStage: stageInfo.stage,
    applyRecommended: mode === "dry-run" && stageInfo.applyRecommended,
    executiveSummary: {
      currentStage: stageInfo.stage,
      liveSources: live.sources.length,
      approvedExplorerSources: governance.approvedExplorerSourceCount,
      liveFacts: targetFacts.length,
      approvedFacts: approvedFacts.length,
      pendingFacts: pendingFacts.length,
      heldInternalFacts: heldInternal.length,
      governanceEligible: governance.eligible,
      governedPlatformReady,
      nextRecommendedAction: halted ? `Resolve ${haltReason}` : stageInfo.stage,
      exactApplyCommand: buildPipelineApplyCommand(),
    },
    stages: {
      sourceRegistration: {
        plannedTotal: registrationActions.length,
        toRegister: registrationActions.filter((a) => a.action === "register"),
        duplicatesSkipped: registrationActions.filter((a) => a.action === "skip_duplicate"),
        invalidSkipped: registrationActions.filter((a) => a.action === "skip_invalid"),
        provenanceOnly: applyPlan.sourceRegistrationPlan.provenanceOnly,
      },
      sourceStewardship: {
        actions: stewardshipActions,
        toSteward: stewardshipActions.filter((s) => s.action === "steward").length,
      },
      extraction: {
        targetFactKeys: applyPlan.extractionPlan.proposedFacts.map((f) => f.fieldKey),
        eligibleSourceCount: applyPlan.extractionPlan.extractionEligibleSources.length,
        proposedFacts: applyPlan.extractionPlan.proposedFacts,
        extractionEnabled: PARTNER_INTELLIGENCE_FLAGS.extractionEnabled,
        genericGapPlaceholders: factAudit.dataGapCount,
      },
      targetedExtraction: {
        used: targetedPathActive || targetedPlan?.used,
        reason: targetedPathActive
          ? "generic_extraction_produced_gap_placeholders"
          : targetedPlan?.priorTargetedCount
            ? "targeted_facts_already_present"
            : "not_needed",
        placeholderFactsDetected: factAudit.placeholderFacts || [],
        placeholderCount: factAudit.dataGapCount,
        proposedFacts: targetedPlan?.proposed || [],
        proposedCount: targetedPlan?.proposedCount || 0,
        approvableCount: targetedPlan?.approvableCount || 0,
        humanReviewCount: targetedPlan?.humanReviewCount || 0,
        skippedDuplicate: targetedPlan?.skippedDuplicate || [],
        createdOnApply: stageResults?.targeted_extraction?.created || 0,
        runId: stageResults?.targeted_extraction?.runId || targetedPlan?.applyResult?.runId || null,
      },
      factStewardship: {
        proposedApprove: applyPlan.extractionPlan.proposedApprovable,
        live: factPlan,
        heldFactAreas: applyPlan.extractionPlan.heldFactAreas,
        targetedPathActive,
        targetedFactsLive: targetedFacts.length,
        approvedTargetedFacts: approvedTargeted.length,
        rejectedPlaceholders: rejectedPlaceholders.length,
        pendingPlaceholders: pendingPlaceholders.length,
        placeholderDisposition:
          rejectedPlaceholders.length > 0
            ? "rejected_superseded"
            : pendingPlaceholders.length > 0
              ? "left_pending"
              : "none",
      },
      brandSetupCompletionDraft: applyPlan.brandSetupCompletionDraft,
      governancePublish: {
        target: applyPlan.governanceReadinessPath.expectedGovernance,
        readinessPath: applyPlan.governanceReadinessPath.gateSequence,
        live: governance,
      },
      verification: {
        governedPlatformReady,
        approvedFacts: approvedFacts.length,
        approvedExplorerSources: governance.approvedExplorerSourceCount,
        companyValidatedUntouched: true,
        companyValidationDateUntouched: true,
        expectedChip: "AI-Assisted Profile",
        sourceBasis: "Company Materials",
      },
    },
    assetGaps: applyPlan.assetGaps,
    prRecentOpeningGaps: applyPlan.prRecentOpeningGaps,
    stageResults,
    applyLog,
    doesNotDo: [
      "Rebuild Brand Explorer content or overwrite Brand Setup content/hero/image/logo fields",
      "Download images (asset governance is a future module)",
      "Auto-approve held/weak facts or FDD economics",
      "Publish FDD economics / Item 19 / legal detail externally",
      "Set Company Validated or Company Validation Date",
      "Imply Marriott validated the profile",
      "Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema",
    ],
  };
}

/* ------------------------------------------------------------------ */
/* Markdown                                                            */
/* ------------------------------------------------------------------ */

export function buildTributePortfolioPipelineMarkdown(report) {
  const es = report.executiveSummary;
  const s = report.stages;
  const lines = [
    "# Tribute Portfolio Package Pipeline v1",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**${report.halted ? ` · HALTED: ${report.haltReason}` : ""}`,
    `Brand: ${report.brand.name} \`${report.brand.recordId}\``,
    "",
    "## Executive summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Current stage | ${es.currentStage} |`,
    `| Live sources | ${es.liveSources} (approved explorer: ${es.approvedExplorerSources}) |`,
    `| Live facts | ${es.liveFacts} (approved: ${es.approvedFacts}, pending: ${es.pendingFacts}, held internal: ${es.heldInternalFacts}) |`,
    `| Governance eligible | ${es.governanceEligible ? "yes" : "no"} |`,
    `| Governed Platform Ready | ${es.governedPlatformReady ? "yes" : "no"} |`,
    `| Next action | ${es.nextRecommendedAction} |`,
    "",
    "### Apply command",
    "",
    "```bash",
    es.exactApplyCommand,
    "```",
    "",
    "## A. Source registration",
    "",
    `- To register: **${s.sourceRegistration.toRegister.length}** · duplicates skipped: ${s.sourceRegistration.duplicatesSkipped.length} · invalid: ${s.sourceRegistration.invalidSkipped.length}`,
    ...s.sourceRegistration.toRegister.map((a) => `- [${a.origin}] ${a.title} (${a.role})`),
    "",
    "**Provenance-only (never registered for extraction):**",
    ...(s.sourceRegistration.provenanceOnly.length ? s.sourceRegistration.provenanceOnly.map((p) => `- ${p.role} (${p.reason}) — ${p.ref}`) : ["- none"]),
    "",
    "## B. Source stewardship",
    "",
    `- Sources needing stewardship: **${s.sourceStewardship.toSteward}**`,
    ...s.sourceStewardship.actions.map((a) => `- ${a.title} → ${a.action}${a.extractionRestriction ? ` · ${a.extractionRestriction}` : ""}`),
    "",
    "## C. Extraction (Pending facts only)",
    "",
    `- Extraction enabled: **${s.extraction.extractionEnabled ? "yes" : "no (set PARTNER_INTELLIGENCE_EXTRACTION_ENABLED=1)"}**`,
    `- Generic gap placeholders detected: **${s.extraction.genericGapPlaceholders}**`,
    `- Target fact keys: ${s.extraction.targetFactKeys.map((k) => `\`${k}\``).join(", ")}`,
    "",
    "## C2. Targeted extraction (source-backed Tribute patterns)",
    "",
    `- Used: **${s.targetedExtraction.used ? "yes" : "no"}** · reason: ${s.targetedExtraction.reason}`,
    `- Placeholder facts detected: **${s.targetedExtraction.placeholderCount}**`,
    `- Proposed targeted facts: **${s.targetedExtraction.proposedCount}** (approvable: ${s.targetedExtraction.approvableCount}, human-review: ${s.targetedExtraction.humanReviewCount})`,
    `- Created on apply: **${s.targetedExtraction.createdOnApply || 0}**${s.targetedExtraction.runId ? ` · run \`${s.targetedExtraction.runId}\`` : ""}`,
    `- Skipped duplicates: ${s.targetedExtraction.skippedDuplicate.length}`,
    "",
    ...(s.targetedExtraction.proposedFacts.length
      ? [
          "| Field | Approvable | Source |",
          "|-------|------------|--------|",
          ...s.targetedExtraction.proposedFacts.map(
            (p) => `| \`${p.fieldKey}\` | ${p.approvable ? "yes" : "review"} | ${p.sourceRole} |`
          ),
          "",
        ]
      : []),
    "## D. Fact stewardship",
    "",
    `- Targeted path active: **${s.factStewardship.targetedPathActive ? "yes" : "no"}**`,
    `- Proposed approve (from plan): ${s.factStewardship.proposedApprove}`,
    `- Live pending decisions — approve: ${s.factStewardship.live.approveCount}, hold internal: ${s.factStewardship.live.holdInternalCount}, hold review: ${s.factStewardship.live.holdReviewCount}, reject placeholders: ${s.factStewardship.live.rejectPlaceholderCount || 0}`,
    `- Targeted facts live: ${s.factStewardship.targetedFactsLive} · approved targeted: ${s.factStewardship.approvedTargetedFacts}`,
    `- Placeholder disposition: **${s.factStewardship.placeholderDisposition}** (rejected: ${s.factStewardship.rejectedPlaceholders}, still pending: ${s.factStewardship.pendingPlaceholders})`,
    "",
    "**Held (FDD / economics / legal):**",
    ...s.factStewardship.heldFactAreas.map((h) => `- ${h.area} — ${h.reason}`),
    "",
    "## E. Brand Setup completion draft (staging only — never written)",
    "",
    "**Do not overwrite:**",
    ...s.brandSetupCompletionDraft.doNotOverwrite.map((f) => `- ${f}`),
    "",
    "**AI-drafted enhancements (Pending review):**",
    ...s.brandSetupCompletionDraft.aiDraftedEnhancements.map((r) => `- ${r.label}${r.factKey ? ` → \`${r.factKey}\`` : ""} (${r.type || r.action})`),
    "",
    "## F. Governance publish",
    "",
    `- Live readiness eligible: **${s.governancePublish.live.eligible ? "yes" : "no"}**${s.governancePublish.live.blockReasons.length ? ` · blockers: ${s.governancePublish.live.blockReasons.join("; ")}` : ""}`,
    "- Target posture: Company Published / Platform Display Allowed / Show Trust Label / AI-Assisted Profile / Company Materials",
    "- Company Validated + Company Validation Date: never written",
    "",
    "### Readiness gate sequence",
    "",
    ...s.governancePublish.readinessPath.map((g) => `  ${g.step}. ${g.gate} — ${g.detail}`),
    "",
    "## G. Verification",
    "",
    `- Governed Platform Ready: **${s.verification.governedPlatformReady ? "yes" : "no"}**`,
    `- Approved facts: ${s.verification.approvedFacts} · approved explorer sources: ${s.verification.approvedExplorerSources}`,
    `- Chip: **${s.verification.expectedChip}** · Basis: **${s.verification.sourceBasis}**`,
    `- Company Validated untouched: ${s.verification.companyValidatedUntouched} · Company Validation Date untouched: ${s.verification.companyValidationDateUntouched}`,
    "",
    "## Asset / image gaps (future module)",
    "",
    ...report.assetGaps.needed.map((a) => `- ${a}`),
    "",
    "## PR / recent-opening gaps",
    "",
    `- ${report.prRecentOpeningGaps.newsroom}`,
    `- Next: ${report.prRecentOpeningGaps.recommendedNextStep}`,
    "",
    "## Airtable modified",
    "",
    `- **${report.airtableModified ? "yes" : "no"}**`,
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
    "",
  ];
  return lines.join("\n");
}
