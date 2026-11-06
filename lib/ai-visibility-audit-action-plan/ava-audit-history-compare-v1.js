/**
 * Compare two immutable AVA audit runs for the same hotel.
 * Missing findings are NOT classified as resolved unless explicitly marked resolved.
 */
import { loadAuditRun } from "./ava-audit-run-archive-v1.js";

function stableFindingId(f, idx) {
  return (
    f.stableFindingId ||
    f.findingId ||
    f.id ||
    f.actionId ||
    `finding_${idx}_${String(f.title || f.headline || "").slice(0, 40)}`
  );
}

function stableActionId(a, idx) {
  return a.stableActionId || a.actionId || a.id || `action_${idx}`;
}

function extractFindings(payload) {
  const lists = [
    ...(payload?.priorityFindings || []),
    ...(payload?.findings || []),
    ...(payload?.executiveSummary?.themes || []).map((t, i) => ({
      title: t.title,
      body: t.what,
      priority: "THEME",
      category: "executive_theme",
      stableFindingId: `theme_${i}_${String(t.title || "").slice(0, 24)}`,
    })),
  ];
  return lists.map((f, i) => ({
    id: stableFindingId(f, i),
    title: f.title || f.headline || f.action || "",
    priority: f.priority || null,
    category: f.category || f.claimType || null,
    body: f.body || f.what || f.whatWeFound || f.description || "",
    status: f.status || "OBSERVED",
    raw: f,
  }));
}

function extractActions(payload) {
  const actions =
    payload?.customerActionRegister?.actions ||
    payload?.actions ||
    payload?.recommendedActions ||
    [];
  return actions.map((a, i) => ({
    id: stableActionId(a, i),
    title: a.actionTitle || a.headline || a.action || a.title || "",
    priority: a.priority || null,
    workstream: a.workstreamTheme || a.owner || null,
    raw: a,
  }));
}

function extractMetrics(payload, certification) {
  const out = [];
  for (const kpi of payload?.kpis || []) {
    out.push({
      metricId: String(kpi.label || "")
        .toUpperCase()
        .replace(/\s+/g, "_"),
      label: kpi.label,
      value: kpi.value,
      meta: kpi.meta || null,
    });
  }
  const cross = certification?.crossReportIdentity || payload?.crossReportIdentity || {};
  if (cross.adpPeriodId) {
    out.push({
      metricId: "ADP_PERIOD_ID",
      label: "ADP Period",
      value: cross.adpPeriodId,
    });
  }
  return out;
}

/**
 * @param {string} earlierAuditRunId
 * @param {string} laterAuditRunId
 */
export function compareAvaAuditRuns(earlierAuditRunId, laterAuditRunId, env = process.env) {
  const earlier = loadAuditRun(earlierAuditRunId, env);
  const later = loadAuditRun(laterAuditRunId, env);
  if (!earlier.ok) return { ok: false, error: earlier.error, stage: "earlier" };
  if (!later.ok) return { ok: false, error: later.error, stage: "later" };
  if (earlier.propertyId !== later.propertyId) {
    return {
      ok: false,
      error: "PROPERTY_MISMATCH",
      earlier: earlier.propertyId,
      later: later.propertyId,
    };
  }

  const eFindings = extractFindings(earlier.payload || {});
  const lFindings = extractFindings(later.payload || {});
  const eMap = new Map(eFindings.map((f) => [f.id, f]));
  const lMap = new Map(lFindings.map((f) => [f.id, f]));

  const newFindings = lFindings.filter((f) => !eMap.has(f.id));
  const persistentFindings = lFindings.filter((f) => eMap.has(f.id));
  const missingInLater = eFindings.filter((f) => !lMap.has(f.id));
  const resolvedFindings = missingInLater.filter((f) =>
    /RESOLVED|CLOSED|FIXED/i.test(String(f.status || ""))
  );
  const notResearchedOrDropped = missingInLater.filter(
    (f) => !/RESOLVED|CLOSED|FIXED/i.test(String(f.status || ""))
  );

  const eActions = extractActions(earlier.payload || {});
  const lActions = extractActions(later.payload || {});
  const eAct = new Map(eActions.map((a) => [a.id, a]));
  const lAct = new Map(lActions.map((a) => [a.id, a]));
  const newActions = lActions.filter((a) => !eAct.has(a.id));
  const removedActions = eActions.filter((a) => !lAct.has(a.id));
  const priorityChanges = persistentFindings
    .map((f) => {
      const prev = eMap.get(f.id);
      if (String(prev.priority || "") === String(f.priority || "")) return null;
      return { id: f.id, title: f.title, from: prev.priority, to: f.priority };
    })
    .filter(Boolean);

  const eMetrics = extractMetrics(earlier.payload, earlier.certification);
  const lMetrics = extractMetrics(later.payload, later.certification);
  const eMet = new Map(eMetrics.map((m) => [m.metricId, m]));
  const metricsChanged = [];
  for (const m of lMetrics) {
    const prev = eMet.get(m.metricId);
    if (!prev) {
      metricsChanged.push({ metricId: m.metricId, from: null, to: m.value, kind: "ADDED" });
      continue;
    }
    if (String(prev.value) !== String(m.value)) {
      metricsChanged.push({
        metricId: m.metricId,
        from: prev.value,
        to: m.value,
        kind: "CHANGED",
      });
    }
  }

  return {
    ok: true,
    propertyId: earlier.propertyId,
    earlierAuditRunId,
    laterAuditRunId,
    metricsChanged,
    newFindings: newFindings.map((f) => ({ id: f.id, title: f.title, priority: f.priority })),
    resolvedFindings: resolvedFindings.map((f) => ({ id: f.id, title: f.title })),
    persistentFindings: persistentFindings.map((f) => ({
      id: f.id,
      title: f.title,
      priority: f.priority,
    })),
    notResearchedOrDropped: notResearchedOrDropped.map((f) => ({
      id: f.id,
      title: f.title,
      note: "Absent in later audit without explicit resolved status — not counted as resolved",
    })),
    priorityChanges,
    newActions: newActions.map((a) => ({ id: a.id, title: a.title, priority: a.priority })),
    removedActions: removedActions.map((a) => ({ id: a.id, title: a.title })),
    evidenceConfidenceChanges: [],
  };
}
