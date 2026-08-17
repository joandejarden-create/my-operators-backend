/**
 * Brand Explorer 62 — Webhound ↔ Airtable reconciliation (read-only).
 *
 * Maps merged Webhound claim-validation rows onto live Brand Explorer Presentation
 * / Brand Basics fields, classifies Airtable vs evidence, and builds a patch
 * remediation queue. Never writes Airtable / BE / Setup / Census / Status.
 *
 * Objective: brand-explorer-62-webhound-airtable-reconciliation-v1
 */
import fs from "node:fs";
import path from "node:path";
import {
  EXPECTED_ACTIVE_COUNT_62,
  FREEZE_DECISION_62,
  REPORT_JSON_62,
  ROOT,
} from "./brand-explorer-62-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  PRESENTATION_TABLE,
  BASICS_TABLE,
  publicTabFromSlot,
  mapRecommendedAction,
  riskLevelForResult,
} from "./brand-explorer-62-webhound-claim-validation.js";

export const RECON_VERSION = "brand-explorer-62-webhound-airtable-reconciliation-v1";
export const RECON_STATUS =
  "brand_explorer_62_webhound_airtable_reconciliation_v1_complete_ready_for_claim_patch_batch_review";
export const RECON_OBJECTIVE = "brand-explorer-62-webhound-airtable-reconciliation-v1";

export const CLAIM_VALIDATION_JSON =
  "brand-explorer-62-webhound-claim-validation-readonly.json";
export const CLAIM_PACK_JSON = "brand-explorer-62-webhound-claim-validation-pack.json";
export const V1_SESSION_ROWS_JSON =
  "brand-explorer-62-webhound-claim-validation-v1-session-rows.json";
export const PRIORITY_SESSION_ROWS_JSON =
  "brand-explorer-62-webhound-claim-validation-priority-session-rows.json";

export const REPORT_JSON = "brand-explorer-62-webhound-airtable-reconciliation-v1.json";
export const REPORT_MD = "brand-explorer-62-webhound-airtable-reconciliation-v1.md";
export const DOCS_MD = "brand-explorer-62-webhound-airtable-reconciliation-v1.md";

const REPORTS_DIR = path.join(ROOT, "reports", "brand-explorer");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

export const RECON_CLASSES = Object.freeze([
  "airtable_matches_evidence",
  "airtable_safer_than_evidence",
  "airtable_overclaims",
  "airtable_stale",
  "airtable_unsupported",
  "airtable_needs_softening",
  "airtable_needs_replacement",
  "airtable_needs_removal",
  "source_conflict",
  "no_matching_airtable_claim",
  "mapping_uncertain",
]);

export const MOMENTUM_ACTIONS = Object.freeze([
  "keep_supported",
  "soften_date_or_scope",
  "replace_with_supported_event",
  "remove_unsupported",
  "stale_hold",
  "steward_review",
]);

export const PATCH_ACTIONS = Object.freeze([
  "keep",
  "soften",
  "replace",
  "remove",
  "steward_review",
]);

function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.length ? String(v[0] ?? "").trim() : "";
  return String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function normText(s) {
  return nz(s)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[“”]/g, '"')
    .trim();
}

function readFrozenBaseline() {
  const p = path.join(ROOT, "reports", REPORT_JSON_62);
  if (!fs.existsSync(p)) throw new Error(`Missing freeze artifact reports/${REPORT_JSON_62}`);
  const frozen = JSON.parse(fs.readFileSync(p, "utf8"));
  if (frozen.freezeDecision !== FREEZE_DECISION_62 || !frozen.frozen) {
    throw new Error(
      `Expected frozen ${FREEZE_DECISION_62}; got ${frozen.freezeDecision} frozen=${frozen.frozen}`
    );
  }
  return frozen;
}

function loadJson(relPath) {
  const p = path.isAbsolute(relPath) ? relPath : path.join(ROOT, relPath);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

async function fetchRecord(baseId, token, table, recordId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    return { ok: false, error: json.error?.message || `HTTP ${res.status}`, fields: null };
  }
  return { ok: true, error: null, fields: json.fields || {}, id: json.id };
}

/**
 * Map Webhound validation_result + recommended action → reconciliation class.
 */
export function classifyReconciliation({
  validationResult,
  recommendedAction,
  airtableValuePresent,
  claimStillInAirtable,
  claimType,
}) {
  const vr = nz(validationResult).toLowerCase();
  const action = nz(recommendedAction)
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/replace_with_sourced_wording/, "replace");

  if (!airtableValuePresent) return "no_matching_airtable_claim";
  if (claimStillInAirtable === false) return "mapping_uncertain";

  if (vr === "conflicting_sources") return "source_conflict";
  if (vr === "stale") return "airtable_stale";
  if (vr === "overclaimed") return "airtable_overclaims";
  if (vr === "unsupported" || vr === "remove_candidate") {
    return action === "remove" || claimType === "recent_momentum"
      ? "airtable_needs_removal"
      : "airtable_unsupported";
  }
  if (vr === "partially_supported" || vr === "needs_softening") {
    if (action === "replace" || action === "replace_with_sourced_wording") {
      return "airtable_needs_replacement";
    }
    if (action === "remove") return "airtable_needs_removal";
    return "airtable_needs_softening";
  }
  if (vr === "supported_official" || vr === "supported_trusted_secondary") {
    return "airtable_matches_evidence";
  }
  if (vr === "not_factual_claim") return "airtable_safer_than_evidence";
  if (action === "remove") return "airtable_needs_removal";
  if (action === "replace" || action === "replace_with_sourced_wording") {
    return "airtable_needs_replacement";
  }
  if (action === "soften") return "airtable_needs_softening";
  if (action === "needs_steward_review" || action === "steward_review") return "source_conflict";
  return "mapping_uncertain";
}

export function momentumActionFor({ validationResult, reconClass }) {
  const vr = nz(validationResult).toLowerCase();
  if (vr === "supported_official" || vr === "supported_trusted_secondary") return "keep_supported";
  if (vr === "stale" || reconClass === "airtable_stale") return "stale_hold";
  if (vr === "conflicting_sources" || reconClass === "source_conflict") return "steward_review";
  if (
    vr === "unsupported" ||
    vr === "remove_candidate" ||
    reconClass === "airtable_needs_removal" ||
    reconClass === "airtable_unsupported"
  ) {
    return "remove_unsupported";
  }
  if (reconClass === "airtable_needs_replacement") return "replace_with_supported_event";
  if (
    vr === "partially_supported" ||
    vr === "needs_softening" ||
    reconClass === "airtable_needs_softening"
  ) {
    return "soften_date_or_scope";
  }
  return "steward_review";
}

export function patchActionFor(reconClass, recommendedAction) {
  const a = nz(recommendedAction)
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/replace_with_sourced_wording/, "replace")
    .replace(/needs_steward_review/, "steward_review");
  if (["keep", "soften", "replace", "remove", "steward_review"].includes(a)) return a;
  if (reconClass === "airtable_matches_evidence" || reconClass === "airtable_safer_than_evidence") {
    return "keep";
  }
  if (reconClass === "airtable_needs_softening") return "soften";
  if (reconClass === "airtable_needs_replacement") return "replace";
  if (reconClass === "airtable_needs_removal" || reconClass === "airtable_unsupported") {
    return "remove";
  }
  if (reconClass === "airtable_stale") return "remove";
  if (reconClass === "airtable_overclaims") return "soften";
  if (reconClass === "source_conflict") return "steward_review";
  return "steward_review";
}

export function priorityForRecon({ reconClass, claimType, priorityBand }) {
  if (reconClass === "source_conflict") return "blocker";
  if (
    claimType === "recent_momentum" &&
    ["airtable_needs_removal", "airtable_unsupported", "airtable_stale"].includes(reconClass)
  ) {
    return "blocker";
  }
  if (
    ["airtable_needs_removal", "airtable_unsupported", "airtable_overclaims", "airtable_stale"].includes(
      reconClass
    )
  ) {
    return "high";
  }
  if (["airtable_needs_softening", "airtable_needs_replacement"].includes(reconClass)) {
    return priorityBand === 1 || priorityBand === 2 ? "high" : "medium";
  }
  if (reconClass === "mapping_uncertain" || reconClass === "no_matching_airtable_claim") {
    return "low";
  }
  return "low";
}

function airtableTableForClaim(claim) {
  if (claim.presentationRecordId) return PRESENTATION_TABLE;
  if (claim.field === "Parent Company" || claim.field === "Brand Architecture") return BASICS_TABLE;
  if (claim.slotKey || claim.section?.includes(".")) return PRESENTATION_TABLE;
  return BASICS_TABLE;
}

function fieldValueFromPresentation(fields, fieldName) {
  const f = fields || {};
  const key = nz(fieldName);
  if (key && f[key] != null) return nz(f[key]);
  // Common presentation fields
  if (key === "Body") return nz(f.Body);
  if (key === "Title") return nz(f.Title);
  if (key.startsWith("Case Summary")) return nz(f[key]);
  return nz(f.Body) || nz(f.Title);
}

function claimContainedIn(airtableValue, claimText) {
  const a = normText(airtableValue);
  const c = normText(claimText);
  if (!a || !c) return null;
  if (a.includes(c)) return true;
  // tolerate truncation / punctuation drift
  const head = c.slice(0, Math.min(80, c.length));
  if (head.length >= 24 && a.includes(head)) return true;
  return false;
}

/**
 * Build candidate claim rows from slim pack + Webhound session exports.
 * Avoids loading the full ~20k-claim merged inventory JSON into memory.
 */
export function collectWebhoundRows({
  packClaims = [],
  v1Rows = [],
  priorityRows = [],
  mergedMeta = null,
}) {
  const byId = new Map();

  for (const c of packClaims) {
    byId.set(c.claimId, {
      ...c,
      sourcePack: "priority_pack",
    });
  }

  // Overlay authoritative priority-session classifications onto pack rows.
  for (const row of priorityRows) {
    const id = nz(row.claim_id || row.claimId);
    if (!id) continue;
    const existing = byId.get(id);
    if (existing) {
      existing.validationResult = nz(row.validation_result || row.validationResult) || existing.validationResult;
      existing.sourceFound = nz(row.source_title || row.sourceFound) || existing.sourceFound;
      existing.sourceUrl = nz(row.source_url || row.sourceUrl) || existing.sourceUrl;
      existing.sourceCategory =
        nz(row.source_category || row.sourceCategory || row.source_tier) || existing.sourceCategory;
      existing.sourceTier = nz(row.source_tier || row.sourceTier) || existing.sourceTier;
      existing.confidence = nz(row.confidence) || existing.confidence;
      existing.riskLevel = nz(row.risk_level || row.riskLevel) || existing.riskLevel;
      existing.recommendedAction =
        nz(row.recommended_action || row.recommendedAction) || existing.recommendedAction;
      existing.notes = nz(row.notes) || existing.notes;
      existing.publicTab = nz(row.public_tab || row.publicTab) || existing.publicTab;
      existing.sourcePack = "priority_session_overlay";
      continue;
    }
    byId.set(id, {
      claimId: id,
      brandSlug: nz(row.brand_slug || row.brandSlug),
      brandName: nz(row.brand_name || row.brandName),
      claimText: nz(row.claim_text || row.claimText),
      claimType: nz(row.claim_type || row.claimType),
      publicTab: nz(row.public_tab || row.publicTab),
      validationResult: nz(row.validation_result || row.validationResult),
      sourceFound: nz(row.source_title || row.sourceFound),
      sourceUrl: nz(row.source_url || row.sourceUrl),
      sourceCategory: nz(row.source_category || row.sourceCategory || row.source_tier),
      confidence: nz(row.confidence),
      recommendedAction: nz(row.recommended_action || row.recommendedAction),
      notes: nz(row.notes),
      sourcePack: "priority_session_unmapped",
      presentationRecordId: null,
      brandRecordId: null,
      field: null,
      slotKey: null,
    });
  }

  // v1 supplemental — match onto pack by brand + claim text when possible.
  for (const row of v1Rows) {
    const id = nz(row.claim_id || row.claimId);
    const slug = nz(row.brand_slug || row.brandSlug);
    const text = nz(row.claim_text || row.claimText);
    const existing = [...byId.values()].find(
      (c) =>
        c.brandSlug === slug &&
        (c.claimId === id || normText(c.claimText) === normText(text))
    );
    if (existing) {
      existing.v1Supplemental = true;
      existing.v1ValidationResult = nz(row.validation_result || row.validationResult);
      // Prefer priority overlay when already validated; else apply v1 result.
      if (
        !existing.validationResult ||
        existing.validationResult === "pending_webhound"
      ) {
        existing.validationResult = nz(row.validation_result || row.validationResult);
        existing.sourceFound = nz(row.source_title) || existing.sourceFound;
        existing.sourceUrl = nz(row.source_url) || existing.sourceUrl;
        existing.sourceCategory =
          nz(row.source_category || row.source_tier) || existing.sourceCategory;
        existing.recommendedAction =
          nz(row.recommended_action) || existing.recommendedAction;
        existing.notes = nz(row.notes) || existing.notes;
        existing.sourcePack = "v1_supplemental_matched";
      }
      continue;
    }
    byId.set(`v1_${id || normText(text).slice(0, 24)}`, {
      claimId: id || `v1_unmapped`,
      brandSlug: slug,
      brandName: slug,
      claimText: text,
      claimType: nz(row.claim_type || row.claimType) || "parent_brand_family",
      validationResult: nz(row.validation_result || row.validationResult),
      sourceFound: nz(row.source_title),
      sourceUrl: nz(row.source_url),
      sourceCategory: nz(row.source_category || row.source_tier),
      recommendedAction: nz(row.recommended_action),
      notes: nz(row.notes),
      sourcePack: "v1_supplemental_unmapped",
      presentationRecordId: null,
      brandRecordId: null,
      field: null,
      slotKey: null,
      mergedMetaNote: mergedMeta?.status || null,
    });
  }

  // Only reconcile rows that have a Webhound classification (skip still-pending pack rows).
  return [...byId.values()].filter((c) => {
    const vr = nz(c.validationResult);
    return vr && vr !== "pending_webhound";
  });
}

async function refreshAirtableSnapshots(rows, { token, baseId }) {
  const presentationIds = [
    ...new Set(rows.map((r) => r.presentationRecordId).filter(Boolean)),
  ];
  const basicsIds = [
    ...new Set(
      rows
        .filter((r) => !r.presentationRecordId && r.brandRecordId)
        .map((r) => r.brandRecordId)
    ),
  ];

  const presentationById = new Map();
  let i = 0;
  for (const id of presentationIds) {
    i += 1;
    if (i === 1 || i % 25 === 0 || i === presentationIds.length) {
      console.log(`[${RECON_VERSION}] presentation fetch ${i}/${presentationIds.length}`);
    }
    const rec = await fetchRecord(baseId, token, PRESENTATION_TABLE, id);
    presentationById.set(id, rec);
    await sleep(50);
  }

  const basicsById = new Map();
  let j = 0;
  for (const id of basicsIds) {
    j += 1;
    console.log(`[${RECON_VERSION}] basics fetch ${j}/${basicsIds.length}`);
    const rec = await fetchRecord(baseId, token, BASICS_TABLE, id);
    basicsById.set(id, rec);
    await sleep(50);
  }

  return { presentationById, basicsById };
}

function buildReconciledRow(claim, airtableCtx) {
  const table = airtableTableForClaim(claim);
  let currentValue = null;
  let airtableFetchOk = null;
  let airtableError = null;
  let slotKeyLive = claim.slotKey || null;

  if (claim.presentationRecordId && airtableCtx.presentationById.has(claim.presentationRecordId)) {
    const rec = airtableCtx.presentationById.get(claim.presentationRecordId);
    airtableFetchOk = rec.ok;
    airtableError = rec.error;
    if (rec.ok) {
      currentValue = fieldValueFromPresentation(rec.fields, claim.field || "Body");
      slotKeyLive = nz(rec.fields?.["Slot Key"]) || slotKeyLive;
    }
  } else if (claim.brandRecordId && airtableCtx.basicsById.has(claim.brandRecordId)) {
    const rec = airtableCtx.basicsById.get(claim.brandRecordId);
    airtableFetchOk = rec.ok;
    airtableError = rec.error;
    if (rec.ok) {
      currentValue = nz(rec.fields?.[claim.field || "Parent Company"]);
    }
  } else if (claim.claimText && (claim.presentationRecordId || claim.brandRecordId)) {
    // Had IDs but not refreshed (shouldn't happen) — fall back to extracted text
    currentValue = claim.claimText;
    airtableFetchOk = null;
  }

  const airtableValuePresent = Boolean(nz(currentValue) || nz(claim.claimText));
  const claimStillInAirtable =
    currentValue == null
      ? claim.presentationRecordId || claim.brandRecordId
        ? null
        : false
      : claimContainedIn(currentValue, claim.claimText);

  // Unmapped supplemental with no Airtable IDs
  const mapped =
    Boolean(claim.presentationRecordId || claim.brandRecordId) &&
    claim.sourcePack !== "v1_supplemental_unmapped" &&
    claim.sourcePack !== "priority_session_unmapped";

  let reconClass;
  if (!mapped && !claim.presentationRecordId && !claim.brandRecordId) {
    reconClass =
      claim.sourcePack?.includes("unmapped")
        ? "no_matching_airtable_claim"
        : "mapping_uncertain";
  } else if (airtableFetchOk === false) {
    reconClass = "mapping_uncertain";
  } else {
    reconClass = classifyReconciliation({
      validationResult: claim.validationResult,
      recommendedAction: claim.recommendedAction || mapRecommendedAction(claim.validationResult, claim.claimType),
      airtableValuePresent,
      claimStillInAirtable: claimStillInAirtable === false ? false : true,
      claimType: claim.claimType,
    });
    // If extract text drifted from live Airtable, flag mapping uncertainty unless supported keep
    if (claimStillInAirtable === false && reconClass === "airtable_matches_evidence") {
      reconClass = "mapping_uncertain";
    } else if (claimStillInAirtable === false && mapped) {
      // live field exists but claim sentence gone — still map to field for steward
      if (!["airtable_needs_removal", "airtable_unsupported"].includes(reconClass)) {
        reconClass = "mapping_uncertain";
      }
    }
  }

  const patchAction = patchActionFor(reconClass, claim.recommendedAction);
  const publicTab =
    claim.publicTab || publicTabFromSlot(slotKeyLive || claim.slotKey || claim.section);
  const momentumAction =
    claim.claimType === "recent_momentum" || /^footprint\.momentum/i.test(nz(slotKeyLive))
      ? momentumActionFor({ validationResult: claim.validationResult, reconClass })
      : null;

  const needsAction = ![
    "airtable_matches_evidence",
    "airtable_safer_than_evidence",
  ].includes(reconClass);

  return {
    claimId: claim.claimId,
    brand: claim.brandName || claim.brandSlug,
    brandSlug: claim.brandSlug,
    brandSetupRecordId: claim.brandRecordId || null,
    presentationRecordId: claim.presentationRecordId || null,
    childTableRecordId: null, // Presentation is the BE surface; child tables not claim SoT here
    publicTab,
    section: claim.section || slotKeyLive || null,
    slotKey: slotKeyLive || claim.slotKey || null,
    airtableTable: table,
    airtableField: claim.field || (table === BASICS_TABLE ? "Parent Company" : "Body"),
    currentAirtableValue: currentValue,
    extractedClaimText: claim.claimText,
    claimStillInAirtable,
    airtableFetchOk,
    airtableError,
    claimType: claim.claimType,
    webhoundClassification: claim.validationResult,
    sourceFound: claim.sourceFound || null,
    sourceUrl: claim.sourceUrl || null,
    sourceCategory: claim.sourceCategory || claim.sourceTier || null,
    confidence: claim.confidence || null,
    webhoundNotes: claim.notes || null,
    reconciliationClassification: reconClass,
    recommendedAction: patchAction,
    momentumAction,
    priorityBand: claim.priorityBand ?? null,
    priorityLabel: claim.priority || null,
    riskLevel: claim.riskLevel || riskLevelForResult(claim.validationResult, claim.priorityBand || 9),
    sourcePack: claim.sourcePack || null,
    needsAction,
    futurePatchTarget: needsAction
      ? {
          table,
          recordId: claim.presentationRecordId || claim.brandRecordId || null,
          field: claim.field || (table === BASICS_TABLE ? "Parent Company" : "Body"),
          slotKey: slotKeyLive || claim.slotKey || null,
        }
      : null,
  };
}

function buildRemediationQueue(reconciled) {
  const actionable = reconciled.filter(
    (r) =>
      r.needsAction &&
      r.futurePatchTarget?.recordId &&
      !["no_matching_airtable_claim", "mapping_uncertain"].includes(r.reconciliationClassification)
  );
  return actionable
    .map((r, i) => {
      const priority = priorityForRecon({
        reconClass: r.reconciliationClassification,
        claimType: r.claimType,
        priorityBand: r.priorityBand,
      });
      return {
        id: `recon-rem-${i + 1}`,
        priority,
        brand: r.brand,
        brandSlug: r.brandSlug,
        publicTab: r.publicTab,
        section: r.section,
        airtableTable: r.airtableTable,
        recordId: r.presentationRecordId || r.brandSetupRecordId,
        fieldName: r.airtableField,
        slotKey: r.slotKey,
        currentValue: r.currentAirtableValue || r.extractedClaimText,
        issueClassification: r.reconciliationClassification,
        webhoundClassification: r.webhoundClassification,
        claimType: r.claimType,
        claimId: r.claimId,
        sourceUrl: r.sourceUrl,
        evidenceSummary: [r.sourceFound, r.sourceCategory, r.webhoundNotes]
          .filter(Boolean)
          .join(" · ")
          .slice(0, 500),
        recommendedAction: r.recommendedAction,
        momentumAction: r.momentumAction,
        proposedSaferWording: null, // do not write proposed wording yet
        reason: `Webhound=${r.webhoundClassification}; recon=${r.reconciliationClassification}; claimStillInAirtable=${r.claimStillInAirtable}`,
        rollbackNote:
          "Revert Presentation/Basics field to pre-patch snapshot from this reconciliation JSON if a later patch batch is applied incorrectly.",
        remediateNow: false,
      };
    })
    .sort((a, b) => {
      const order = { blocker: 0, high: 1, medium: 2, low: 3 };
      return (order[a.priority] ?? 9) - (order[b.priority] ?? 9);
    });
}

function proposedBatches(queue) {
  const batch = (name, pred) => ({
    batch: name,
    count: queue.filter(pred).length,
    itemIds: queue.filter(pred).map((q) => q.id),
  });
  return [
    batch("A_recent_momentum_blockers", (q) => q.claimType === "recent_momentum" && q.priority === "blocker"),
    batch(
      "B_recent_momentum_other",
      (q) => q.claimType === "recent_momentum" && q.priority !== "blocker"
    ),
    batch(
      "C_public_tab_high",
      (q) =>
        q.claimType !== "recent_momentum" &&
        q.claimType !== "property_example" &&
        q.claimType !== "parent_brand_family" &&
        q.claimType !== "collection_soft_brand" &&
        (q.priority === "high" || q.priority === "blocker")
    ),
    batch("D_property_examples", (q) => q.claimType === "property_example"),
    batch(
      "E_parent_family_soft_brand",
      (q) => q.claimType === "parent_brand_family" || q.claimType === "collection_soft_brand"
    ),
    batch("F_steward_review", (q) => q.recommendedAction === "steward_review"),
  ].filter((b) => b.count > 0);
}

/**
 * Run full reconciliation (Airtable reads only).
 */
export async function runWebhoundAirtableReconciliation({ token, baseId } = {}) {
  const apiKey = token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const bid = baseId || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !bid) throw new Error("Set AIRTABLE_API_KEY/AIRTABLE_PAT and AIRTABLE_BASE_ID");

  const frozen = readFrozenBaseline();
  const live = await loadActiveUniverse({ includeBrandApi: false });
  if (live.totalCount !== EXPECTED_ACTIVE_COUNT_62) {
    throw new Error(`Active universe ${live.totalCount} ≠ ${EXPECTED_ACTIVE_COUNT_62}`);
  }

  const packFile = loadJson(path.join("reports", "brand-explorer", CLAIM_PACK_JSON));
  if (!packFile?.claims?.length) {
    throw new Error(`Missing slim claim pack reports/brand-explorer/${CLAIM_PACK_JSON}`);
  }

  // Prefer pack/session artifacts; do not load full merged claims[] inventory.
  const v1File = loadJson(path.join("reports", "brand-explorer", V1_SESSION_ROWS_JSON));
  const priorityFile = loadJson(path.join("reports", "brand-explorer", PRIORITY_SESSION_ROWS_JSON));
  const mergedMeta = {
    status: "brand_explorer_62_webhound_claim_validation_readonly_complete_ready_for_claim_remediation_queue",
    datasetRowsMerged: Number(priorityFile?.rowCount || 0),
  };

  console.log(
    `[${RECON_VERSION}] pack=${packFile.claims.length} priorityRows=${priorityFile?.rows?.length || 0} v1Rows=${v1File?.rows?.length || 0}`
  );

  const rows = collectWebhoundRows({
    packClaims: packFile.claims,
    v1Rows: v1File?.rows || [],
    priorityRows: priorityFile?.rows || [],
    mergedMeta,
  });
  console.log(`[${RECON_VERSION}] webhound rows to reconcile=${rows.length}`);

  // Refresh live Airtable for mapped presentation/basics targets
  const refreshTargets = rows.filter((r) => r.presentationRecordId || r.brandRecordId);
  const airtableCtx = await refreshAirtableSnapshots(refreshTargets, {
    token: apiKey,
    baseId: bid,
  });

  const reconciled = rows.map((r) => buildReconciledRow(r, airtableCtx));
  const classCounts = Object.fromEntries(RECON_CLASSES.map((k) => [k, 0]));
  for (const r of reconciled) {
    classCounts[r.reconciliationClassification] =
      (classCounts[r.reconciliationClassification] || 0) + 1;
  }

  const mapped = reconciled.filter(
    (r) =>
      r.presentationRecordId ||
      r.brandSetupRecordId ||
      !["no_matching_airtable_claim", "mapping_uncertain"].includes(r.reconciliationClassification)
  );
  const notMapped = reconciled.filter((r) =>
    ["no_matching_airtable_claim", "mapping_uncertain"].includes(r.reconciliationClassification)
  );

  const momentumRows = reconciled.filter(
    (r) => r.claimType === "recent_momentum" || r.momentumAction
  );
  const momentumActionCounts = Object.fromEntries(MOMENTUM_ACTIONS.map((k) => [k, 0]));
  for (const r of momentumRows) {
    if (r.momentumAction) momentumActionCounts[r.momentumAction] += 1;
  }

  const remediationQueue = buildRemediationQueue(reconciled);
  const batches = proposedBatches(remediationQueue);

  const byArea = {
    recentMomentum: remediationQueue.filter((q) => q.claimType === "recent_momentum").length,
    publicTabs: remediationQueue.filter(
      (q) =>
        ![
          "recent_momentum",
          "property_example",
          "parent_brand_family",
          "collection_soft_brand",
        ].includes(q.claimType)
    ).length,
    propertyExamples: remediationQueue.filter((q) => q.claimType === "property_example").length,
    parentFamily: remediationQueue.filter(
      (q) => q.claimType === "parent_brand_family" || q.claimType === "collection_soft_brand"
    ).length,
  };

  return {
    version: RECON_VERSION,
    objective: RECON_OBJECTIVE,
    generatedAt: new Date().toISOString(),
    status: RECON_STATUS,
    mode: "readonly",
    airtableWrites: false,
    brandExplorerWrites: false,
    brandSetupWrites: false,
    recentMomentumWrites: false,
    censusWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    companyValidatedWrites: false,
    brandVerifiedWrites: false,
    proposedWordingWritten: false,
    freezeDecision: frozen.freezeDecision,
    freezeUnchanged: frozen.freezeDecision === FREEZE_DECISION_62 && frozen.frozen === true,
    activeUniverse: live.totalCount,
    inputs: {
      claimPack: `reports/brand-explorer/${CLAIM_PACK_JSON}`,
      mergedClaimValidationMeta: `reports/brand-explorer/${CLAIM_VALIDATION_JSON}`,
      prioritySessionRows: priorityFile
        ? `reports/brand-explorer/${PRIORITY_SESSION_ROWS_JSON}`
        : null,
      v1SessionRows: v1File ? `reports/brand-explorer/${V1_SESSION_ROWS_JSON}` : null,
      prioritySessionId: "593c9135-18a1-4a72-96d2-7fc6a0b57368",
      v1SessionId: "e7b141d0-2359-4fd5-8898-11ca14d98b19",
      mergedClaimValidationStatus: mergedMeta?.status || null,
    },
    summary: {
      webhoundRowsReconciled: reconciled.length,
      rowsMappedToAirtable: mapped.length,
      rowsNotMapped: notMapped.length,
      airtableMatchesEvidence: classCounts.airtable_matches_evidence,
      airtableSaferThanEvidence: classCounts.airtable_safer_than_evidence,
      airtableOverclaims: classCounts.airtable_overclaims,
      airtableStale: classCounts.airtable_stale,
      airtableUnsupported: classCounts.airtable_unsupported,
      needsSoftening: classCounts.airtable_needs_softening,
      needsReplacement: classCounts.airtable_needs_replacement,
      needsRemoval: classCounts.airtable_needs_removal,
      sourceConflict: classCounts.source_conflict,
      noMatchingAirtableClaim: classCounts.no_matching_airtable_claim,
      mappingUncertain: classCounts.mapping_uncertain,
      recentMomentumItemsReviewed: momentumRows.length,
      recentMomentumActions: momentumActionCounts,
      publicTabActions: byArea.publicTabs,
      propertyExampleActions: byArea.propertyExamples,
      parentFamilyActions: byArea.parentFamily,
      remediationQueueCount: remediationQueue.length,
      presentationRecordsRefreshed: airtableCtx.presentationById.size,
      basicsRecordsRefreshed: airtableCtx.basicsById.size,
    },
    classCounts,
    proposedRemediationBatches: batches,
    remediationQueue,
    reconciledRows: reconciled,
    confirmations: {
      readOnlyMode: true,
      activeUniverseRemains62: live.totalCount === 62,
      frozenBaselineUnchanged: frozen.freezeDecision === FREEZE_DECISION_62,
      noBrandExplorerWrites: true,
      noBrandSetupWrites: true,
      noRecentMomentumWrites: true,
      noBrandStatusChanges: true,
      noReleaseFieldChanges: true,
      noCompanyValidatedOrBrandVerifiedWrites: true,
      noHotelPropertyCensusWrites: true,
      noProposedWordingWritten: true,
    },
  };
}

export function writeReconciliationArtifacts(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD);
  const docsPath = path.join(DOCS_DIR, DOCS_MD);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  const md = renderMarkdown(report);
  fs.writeFileSync(mdPath, md);
  fs.writeFileSync(docsPath, md);
  return { jsonPath, mdPath, docsPath };
}

function renderMarkdown(report) {
  const s = report.summary || {};
  const lines = [];
  lines.push("# Brand Explorer 62 — Webhound ↔ Airtable Reconciliation v1");
  lines.push("");
  lines.push(`**Status:** \`${report.status}\``);
  lines.push(`**Objective:** \`${report.objective}\``);
  lines.push(`**Generated:** ${report.generatedAt}`);
  lines.push(`**Freeze:** \`${report.freezeDecision}\` · unchanged=${report.freezeUnchanged}`);
  lines.push("**Mode:** read-only · no patch wording written · no Airtable writes");
  lines.push("");
  lines.push("## Verdict");
  lines.push("");
  lines.push(
    `Reconciled **${s.webhoundRowsReconciled}** Webhound rows against live Airtable. Mapped **${s.rowsMappedToAirtable}** · not mapped **${s.rowsNotMapped}**. Matches evidence **${s.airtableMatchesEvidence}** · safer **${s.airtableSaferThanEvidence}** · overclaims **${s.airtableOverclaims}** · stale **${s.airtableStale}** · unsupported **${s.airtableUnsupported}** · soften **${s.needsSoftening}** · replace **${s.needsReplacement}** · remove **${s.needsRemoval}** · conflicts **${s.sourceConflict}**. Remediation queue **${s.remediationQueueCount}**.`
  );
  lines.push("");
  lines.push("## Recent Momentum");
  lines.push("");
  lines.push(`Items reviewed: **${s.recentMomentumItemsReviewed}**`);
  lines.push("");
  for (const [k, v] of Object.entries(s.recentMomentumActions || {})) {
    lines.push(`- \`${k}\`: **${v}**`);
  }
  lines.push("");
  lines.push("## Action areas");
  lines.push("");
  lines.push(`- Public tab actions: **${s.publicTabActions}**`);
  lines.push(`- Property example actions: **${s.propertyExampleActions}**`);
  lines.push(`- Parent/family actions: **${s.parentFamilyActions}**`);
  lines.push("");
  lines.push("## Proposed remediation batches");
  lines.push("");
  for (const b of report.proposedRemediationBatches || []) {
    lines.push(`- **${b.batch}**: ${b.count}`);
  }
  lines.push("");
  lines.push("## Remediation queue (top 50 — queue only)");
  lines.push("");
  for (const item of (report.remediationQueue || []).slice(0, 50)) {
    lines.push(
      `- **[${item.priority}]** ${item.brandSlug} · ${item.publicTab} · ${item.issueClassification} · ${item.recommendedAction} · \`${item.airtableTable}\`/\`${item.fieldName}\` · ${String(item.currentValue || "").slice(0, 100)}`
    );
  }
  if ((report.remediationQueue || []).length > 50) {
    lines.push(`- … +${report.remediationQueue.length - 50} more (see JSON)`);
  }
  lines.push("");
  lines.push("## Confirmations");
  lines.push("");
  for (const [k, v] of Object.entries(report.confirmations || {})) {
    lines.push(`- \`${k}\`: **${v}**`);
  }
  lines.push("");
  lines.push(`**Final status:** \`${report.status}\``);
  lines.push("");
  return `${lines.join("\n")}\n`;
}
