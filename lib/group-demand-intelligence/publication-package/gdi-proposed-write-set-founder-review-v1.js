/**
 * Read-only founder review of locked proposed write-sets vs STAGED/CURRENT.
 * Does not activate STAGED, publish CURRENT, or mutate Airtable.
 *
 * Wyndham locked artifact:
 * reports/gdi-wyndham-final-commercial-completion-v1/08-PROPOSED_WRITE_SET_QA.json
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "./index.js";

export const GDI_PROPOSED_WRITE_SET_FOUNDER_REVIEW_V1 =
  "gdi-proposed-write-set-founder-review-v1";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);

const WYNDHAM_HOTEL_ID = "recnPB3Q02NO8Ma1o";
const WYNDHAM_LOCKED_WRITE_SET = path.join(
  ROOT,
  "reports/gdi-wyndham-final-commercial-completion-v1/08-PROPOSED_WRITE_SET_QA.json",
);

function text(v) {
  return String(v == null ? "" : v).trim();
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function packageOpportunityIds(pkg) {
  const opps = Array.isArray(pkg?.opportunities) ? pkg.opportunities : [];
  return opps
    .map((o) => text(o?.id || o?.opportunityId))
    .filter(Boolean);
}

function loadPointerInventory(hotelId, kind) {
  const store = createGdiPublicationPackageStore({});
  const pointer = store.readPointer(hotelId, kind);
  if (!pointer?.publicationVersion) {
    return {
      available: false,
      kind,
      publicationVersion: null,
      opportunityIds: [],
      count: 0,
    };
  }
  const loaded = store.loadPackage(hotelId, pointer.publicationVersion);
  if (!loaded?.ok) {
    return {
      available: false,
      kind,
      publicationVersion: pointer.publicationVersion,
      opportunityIds: [],
      count: 0,
      error: loaded?.error || "PACKAGE_LOAD_FAILED",
    };
  }
  const pkg = loaded.package;
  const ids = packageOpportunityIds(pkg);
  return {
    available: true,
    kind,
    publicationVersion: pointer.publicationVersion,
    publicationId: pointer.publicationId || null,
    lifecycleState: pointer.lifecycleState || pkg?.lifecycleState || null,
    opportunityIds: ids,
    count: ids.length,
  };
}

/**
 * Normalize locked Wyndham QA write-set into founder-review buckets.
 */
export function loadWyndhamLockedProposedWriteSet(opts = {}) {
  const artifactPath = text(opts.artifactPath) || WYNDHAM_LOCKED_WRITE_SET;
  if (!fs.existsSync(artifactPath)) {
    return {
      ok: false,
      error: "LOCKED_WRITE_SET_ARTIFACT_MISSING",
      artifactPath,
    };
  }
  const doc = readJson(artifactPath);
  const clean11 = (doc.clean11Preserve || []).map((id) => text(id)).filter(Boolean);
  const prior = Array.isArray(doc.priorValidated) ? doc.priorValidated : [];
  const neu = Array.isArray(doc.newWriteReady) ? doc.newWriteReady : [];
  const hold = Array.isArray(doc.holdPendingRoleReview)
    ? doc.holdPendingRoleReview
    : [];

  const proposedQualified = [
    ...clean11.map((id) => ({
      opportunityId: id,
      org: null,
      bucket: "CLEAN11_PRESERVE",
      disposition: "STAGED_OR_PRESERVE",
      qualifiedProposed: true,
    })),
    ...prior.map((row) => ({
      opportunityId: text(row.id),
      org: row.org || null,
      contact: row.contact || null,
      source: row.source || null,
      bucket: "PRIOR_VALIDATED_ADDITION",
      disposition: "WRITE_READY",
      qualifiedProposed: true,
    })),
    ...neu.map((row) => ({
      opportunityId: text(row.id),
      org: row.org || null,
      contact: row.contact || null,
      role: row.role || null,
      segment: row.seg || null,
      notes: row.notes || [],
      bucket: "NEW_WRITE_READY_ADDITION",
      disposition: row.disposition || "WRITE_READY",
      qualifiedProposed: true,
    })),
  ].filter((r) => r.opportunityId);

  const holdRows = hold.map((row) => ({
    opportunityId: text(row.id),
    org: row.org || null,
    contact: row.contact || null,
    role: row.role || null,
    segment: row.seg || null,
    notes: row.notes || [],
    bucket: "HOLD_PENDING_ROLE_REVIEW",
    disposition: "HOLD",
    qualifiedProposed: false,
  }));

  return {
    ok: true,
    hotelId: WYNDHAM_HOTEL_ID,
    hotelName: "Wyndham San Diego Bayside",
    artifactPath,
    policy: doc.policy || null,
    counts: doc.counts || null,
    proposedQualified,
    hold: holdRows,
    proposedQualifiedIds: proposedQualified.map((r) => r.opportunityId),
    holdIds: holdRows.map((r) => r.opportunityId),
  };
}

/**
 * Founder review surface: distinguish STAGED / proposed additions / HOLD / CURRENT.
 * Read-only. Never presents 161 discovery or WHO/HOW continuations as qualified.
 */
export function buildWyndhamProposedWriteSetFounderReview(opts = {}) {
  const locked = loadWyndhamLockedProposedWriteSet(opts);
  if (!locked.ok) return locked;

  const staged = loadPointerInventory(WYNDHAM_HOTEL_ID, "STAGED");
  const current = loadPointerInventory(WYNDHAM_HOTEL_ID, "CURRENT");
  const stagedSet = new Set(staged.opportunityIds);
  const currentSet = new Set(current.opportunityIds);

  const rows = locked.proposedQualified.map((row) => {
    const inStaged = stagedSet.has(row.opportunityId);
    const inCurrent = currentSet.has(row.opportunityId);
    let visibilityState = "PROPOSED_NOT_IN_STAGED";
    if (inCurrent) visibilityState = "CURRENT_CUSTOMER_VISIBLE";
    else if (inStaged) visibilityState = "STAGED_FOUNDER_PREVIEW";
    return {
      ...row,
      inStaged,
      inCurrent,
      visibilityState,
      publicationStatus: inCurrent
        ? "CURRENT"
        : inStaged
          ? "STAGED"
          : "NOT_PUBLISHED",
    };
  });

  const holdRows = locked.hold.map((row) => ({
    ...row,
    inStaged: stagedSet.has(row.opportunityId),
    inCurrent: currentSet.has(row.opportunityId),
    visibilityState: "HOLD_NOT_QUALIFIED",
    publicationStatus: "HOLD",
  }));

  const missingFromStaged = rows.filter((r) => !r.inStaged);
  const presentInStaged = rows.filter((r) => r.inStaged);

  return {
    ok: true,
    contract: GDI_PROPOSED_WRITE_SET_FOUNDER_REVIEW_V1,
    hotelId: locked.hotelId,
    hotelName: locked.hotelName,
    governance: {
      noPaidResearch: true,
      noAirtableWrites: true,
      noStagedActivate: true,
      noCurrentPublish: true,
      lockedWriteSetImmutable: true,
      customerFacingUnchanged: true,
    },
    artifactPath: locked.artifactPath,
    policy: locked.policy,
    counts: {
      lockedProposedQualified: rows.length,
      hold: holdRows.length,
      stagedPackageCount: staged.count,
      currentPackageCount: current.count,
      proposedPresentInStaged: presentInStaged.length,
      proposedMissingFromStaged: missingFromStaged.length,
      expected: {
        clean11: 11,
        turner: 1,
        acg: 1,
        additionalQaWriteReady: 13,
        totalLockedProposed: 26,
        hold: 2,
      },
    },
    staged,
    current,
    rootCause:
      staged.count === 11 && missingFromStaged.length === 15
        ? "STAGED_POINTER_IS_CLEAN11_ONLY — locked 26 write-set not applied; not an Executive Summary inventory regression"
        : "SEE_ROW_DIFF",
    proposedQualified: rows,
    hold: holdRows,
    researchContinuationsExcluded: {
      note: "WHO/HOW continuation candidates (25) and raw discovery (161) are intentionally excluded from qualified review",
      includedAsQualified: false,
    },
  };
}

export function assertWyndhamLocked26Shape(review) {
  const ids = (review.proposedQualified || []).map((r) => r.opportunityId);
  const unique = new Set(ids);
  return {
    ok:
      ids.length === 26 &&
      unique.size === 26 &&
      (review.hold || []).length === 2,
    proposedCount: ids.length,
    uniqueCount: unique.size,
    holdCount: (review.hold || []).length,
  };
}
