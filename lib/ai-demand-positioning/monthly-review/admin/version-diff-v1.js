/**
 * Compare two monthly review generations — composition vs data changes.
 */

import { loadReviewPayload } from "./archive-store-v1.js";

function pick(obj, keys) {
  const out = {};
  for (const k of keys) out[k] = obj?.[k] ?? null;
  return out;
}

function stableString(value) {
  return JSON.stringify(value, (_k, v) => (v === undefined ? null : v));
}

function diffField(label, a, b) {
  const sa = stableString(a);
  const sb = stableString(b);
  if (sa === sb) return { label, status: "UNCHANGED", before: a, after: b };
  if (a == null && b != null) return { label, status: "ADDED", before: a, after: b };
  if (a != null && b == null) return { label, status: "REMOVED", before: a, after: b };
  return { label, status: "CHANGED", before: a, after: b };
}

function actionKey(a) {
  return a?.actionId || a?.actionPatternId || a?.actionTitle || stableString(a);
}

function diffActionLists(aList = [], bList = []) {
  const aMap = new Map(aList.map((x) => [actionKey(x), x]));
  const bMap = new Map(bList.map((x) => [actionKey(x), x]));
  const keys = new Set([...aMap.keys(), ...bMap.keys()]);
  const rows = [];
  for (const k of keys) {
    const before = aMap.get(k);
    const after = bMap.get(k);
    if (before && !after) rows.push({ key: k, status: "REMOVED", before, after: null });
    else if (!before && after) rows.push({ key: k, status: "ADDED", before: null, after });
    else if (stableString(before) !== stableString(after)) {
      rows.push({ key: k, status: "CHANGED", before, after });
    } else {
      rows.push({ key: k, status: "UNCHANGED", before, after });
    }
  }
  return rows;
}

/**
 * @returns {object} ADP_MONTHLY_REVIEW_VERSION_DIFF integrity payload
 */
export function compareReviewVersions(reviewIdA, reviewIdB) {
  const packA = loadReviewPayload(reviewIdA);
  const packB = loadReviewPayload(reviewIdB);
  if (!packA || !packB) {
    throw new Error("compare_requires_two_existing_reviews");
  }
  if (packA.meta.propertyId !== packB.meta.propertyId) {
    throw new Error("compare_same_property_required");
  }

  const a = packA.review;
  const b = packB.review;

  const sameSourcePeriods =
    a.reporting?.currentPeriodId === b.reporting?.currentPeriodId &&
    a.reporting?.priorPeriodId === b.reporting?.priorPeriodId;

  const dataChanges = [
    diffField("currentPeriodId", a.reporting?.currentPeriodId, b.reporting?.currentPeriodId),
    diffField("priorPeriodId", a.reporting?.priorPeriodId, b.reporting?.priorPeriodId),
    diffField(
      "kpiValues",
      (a.kpis || []).map((k) => pick(k, ["id", "currentValue", "priorValue", "deltaDisplay"])),
      (b.kpis || []).map((k) => pick(k, ["id", "currentValue", "priorValue", "deltaDisplay"]))
    ),
  ];

  const compositionChanges = [
    diffField("executiveAssessment", a.executiveAssessment, b.executiveAssessment),
    diffField("materialMovements", a.materialMovements, b.materialMovements),
    diffField("evidenceReview", a.evidenceReview, b.evidenceReview),
    diffField("decisionsRequired", a.decisionsRequired, b.decisionsRequired),
    diffField("nextMonitoringPriorities", a.nextMonitoringPriorities, b.nextMonitoringPriorities),
    diffField("meetingMode", a.meetingMode, b.meetingMode),
    diffField("builderVersion", packA.meta.builderVersion, packB.meta.builderVersion),
    diffField(
      "actionLibraryVersion",
      packA.meta.actionLibraryVersion,
      packB.meta.actionLibraryVersion
    ),
    diffField("pdfFingerprint", packA.meta.pdfFingerprint, packB.meta.pdfFingerprint),
    diffField(
      "contentFingerprint",
      packA.meta.contentFingerprint,
      packB.meta.contentFingerprint
    ),
  ];

  const actionDiff = diffActionLists(a.actionAgenda || [], b.actionAgenda || []);

  const dataChanged = dataChanges.some((d) => d.status !== "UNCHANGED");
  const compositionChanged =
    compositionChanges.some((d) => d.status !== "UNCHANGED") ||
    actionDiff.some((d) => d.status !== "UNCHANGED");

  // When source periods identical, KPI value changes must not be narrated as measurement drift.
  const integrity = {
    gate: "ADP_MONTHLY_REVIEW_VERSION_DIFF_INTEGRITY",
    sameSourcePeriods,
    dataChangesSeparated: true,
    compositionChangesSeparated: true,
    pass:
      !sameSourcePeriods ||
      !dataChanged ||
      // Allow identical source periods with unchanged KPI values
      dataChanges
        .filter((d) => d.label === "kpiValues")
        .every((d) => d.status === "UNCHANGED"),
    note: sameSourcePeriods
      ? "Source periods identical — treat KPI identity as certified data; highlight composition/copy/action changes only."
      : "Source periods differ — data and composition changes both expected.",
  };

  return {
    ok: true,
    left: {
      reviewId: reviewIdA,
      versionLabel: packA.meta.versionLabel,
      generatedAt: packA.meta.generatedAt,
      reviewStatus: packA.meta.reviewStatus,
      builderVersion: packA.meta.builderVersion,
      actionLibraryVersion: packA.meta.actionLibraryVersion,
    },
    right: {
      reviewId: reviewIdB,
      versionLabel: packB.meta.versionLabel,
      generatedAt: packB.meta.generatedAt,
      reviewStatus: packB.meta.reviewStatus,
      builderVersion: packB.meta.builderVersion,
      actionLibraryVersion: packB.meta.actionLibraryVersion,
    },
    DATA_CHANGES: dataChanges,
    COMPOSITION_COPY_ACTION_CHANGES: {
      fields: compositionChanges,
      actions: actionDiff,
    },
    summary: {
      dataChanged,
      compositionChanged,
      actionsAdded: actionDiff.filter((x) => x.status === "ADDED").length,
      actionsRemoved: actionDiff.filter((x) => x.status === "REMOVED").length,
      actionsChanged: actionDiff.filter((x) => x.status === "CHANGED").length,
    },
    integrity,
  };
}
