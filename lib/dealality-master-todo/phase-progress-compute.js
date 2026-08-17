/**
 * Compute phase progress stats from Founder Project Plan task rows.
 */

export function isPhaseRollup(task) {
  return String(task || "").startsWith("[Phase rollup]");
}

export function isCompletedStatus(status) {
  return String(status || "").trim() === "Completed";
}

export function pct(completed, total) {
  if (!total) return 0;
  return Math.round((completed / total) * 1000) / 10;
}

/**
 * @param {Array<{ id: string, fields?: Record<string, unknown> }>} records
 */
export function computePhaseProgress(records) {
  const tasks = records.filter((r) => !isPhaseRollup(r.fields?.Task));

  const byPhase = new Map();
  for (const rec of tasks) {
    const phase = String(rec.fields?.Phase || "(No Phase)");
    if (!byPhase.has(phase)) {
      byPhase.set(phase, {
        phase,
        total: 0,
        completed: 0,
        inProgress: 0,
        notStarted: 0,
        other: 0,
        byStatus: {},
      });
    }
    const bucket = byPhase.get(phase);
    bucket.total += 1;
    const status = String(rec.fields?.Status || "(blank)");
    bucket.byStatus[status] = (bucket.byStatus[status] || 0) + 1;
    if (isCompletedStatus(status)) bucket.completed += 1;
    else if (status === "In Progress") bucket.inProgress += 1;
    else if (status === "Not Started") bucket.notStarted += 1;
    else bucket.other += 1;
  }

  const phases = [...byPhase.values()]
    .map((p) => ({
      ...p,
      percentCompleted: pct(p.completed, p.total),
    }))
    .sort((a, b) => b.total - a.total);

  const totals = phases.reduce(
    (acc, p) => {
      acc.total += p.total;
      acc.completed += p.completed;
      acc.inProgress += p.inProgress;
      acc.notStarted += p.notStarted;
      acc.other += p.other;
      return acc;
    },
    { total: 0, completed: 0, inProgress: 0, notStarted: 0, other: 0 }
  );
  totals.percentCompleted = pct(totals.completed, totals.total);

  return { tasks, phases, totals };
}
