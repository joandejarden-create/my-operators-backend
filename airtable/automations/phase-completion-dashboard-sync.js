/**
 * Airtable Automation script — paste into Automations → Run script.
 *
 * Trigger options (pick one):
 *   A) When a record is updated → table: Founder Project Plan
 *   B) At a scheduled time → every 1 hour (backup / lighter load)
 *
 * Updates: Phase Completion Dashboard (tblilet6ncUmmJQDv)
 * Source:   Founder Project Plan (tblpCg0QZ0kIPXihE)
 */
const FOUNDER_TABLE_NAME = "Founder Project Plan";
const DASHBOARD_TABLE_NAME = "Phase Completion Dashboard";
const TOTALS_ROW_LABEL = "— ALL PHASES —";
const ROLLUP_PREFIX = "[Phase rollup]";

const F = {
  task: "Task",
  phase: "Phase",
  status: "Status",
  dPhase: "Phase",
  total: "Total Tasks",
  completed: "Completed",
  inProgress: "In Progress",
  notStarted: "Not Started",
  other: "Other Status",
  percent: "Percent Done",
  synced: "Last Synced",
};

function cellName(value) {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "object" && value.name) return value.name;
  return String(value);
}

function isRollup(task) {
  return String(task || "").startsWith(ROLLUP_PREFIX);
}

function pct(completed, total) {
  if (!total) return 0;
  return Math.round((completed / total) * 1000) / 10;
}

function bucketFor(map, phase) {
  if (!map.has(phase)) {
    map.set(phase, {
      phase,
      total: 0,
      completed: 0,
      inProgress: 0,
      notStarted: 0,
      other: 0,
    });
  }
  return map.get(phase);
}

const founderTable = base.getTable(FOUNDER_TABLE_NAME);
const dashboardTable = base.getTable(DASHBOARD_TABLE_NAME);

const founderQuery = await founderTable.selectRecordsAsync({
  fields: [F.task, F.phase, F.status],
});

const byPhase = new Map();
for (const rec of founderQuery.records) {
  const task = cellName(rec.getCellValue(F.task));
  if (isRollup(task)) continue;

  const phase = cellName(rec.getCellValue(F.phase)) || "(No Phase)";
  const status = cellName(rec.getCellValue(F.status)) || "(blank)";
  const b = bucketFor(byPhase, phase);
  b.total += 1;
  if (status === "Completed") b.completed += 1;
  else if (status === "In Progress") b.inProgress += 1;
  else if (status === "Not Started") b.notStarted += 1;
  else b.other += 1;
}

const phases = [...byPhase.values()].sort((a, b) => b.total - a.total);
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

const allRows = [
  ...phases.map((p) => ({ ...p, percentCompleted: pct(p.completed, p.total) })),
  {
    phase: TOTALS_ROW_LABEL,
    total: totals.total,
    completed: totals.completed,
    inProgress: totals.inProgress,
    notStarted: totals.notStarted,
    other: totals.other,
    percentCompleted: pct(totals.completed, totals.total),
  },
];

const dashQuery = await dashboardTable.selectRecordsAsync({ fields: [F.dPhase] });
const dashByPhase = new Map();
for (const rec of dashQuery.records) {
  dashByPhase.set(cellName(rec.getCellValue(F.dPhase)), rec.id);
}

const syncedAt = new Date();
let updated = 0;
let created = 0;

for (const row of allRows) {
  const fields = {
    [F.dPhase]: row.phase,
    [F.total]: row.total,
    [F.completed]: row.completed,
    [F.inProgress]: row.inProgress,
    [F.notStarted]: row.notStarted,
    [F.other]: row.other,
    [F.percent]: row.percentCompleted / 100,
    [F.synced]: syncedAt,
  };

  const existingId = dashByPhase.get(row.phase);
  if (existingId) {
    await dashboardTable.updateRecordAsync(existingId, fields);
    updated += 1;
  } else {
    await dashboardTable.createRecordAsync(fields);
    created += 1;
  }
}

output.set("updated", updated);
output.set("created", created);
output.set("phases", phases.length);
output.set("syncedAt", syncedAt.toISOString());
