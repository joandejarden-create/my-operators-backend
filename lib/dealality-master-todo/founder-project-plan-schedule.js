/**
 * Forward schedule for Founder Project Plan — aligned to cleaned statuses (Jul 2026).
 * Kickoff: Mon Jul 7, 2026. Platform work largely shipped; remaining dates pulled forward.
 */
import { MAP_MASTER_TODO, COMPLETED_STATUS_VALUES } from "./master-todo-field-map.js";
import { MASTER_FIELD_ENRICHMENT, parseSeedId } from "./dealality-airtable-field-fill.js";

/** Monday Jul 7, 2026 — operational work kickoff. */
export const SCHEDULE_KICKOFF = "2026-07-07";

/** When normalizing recently completed platform work without historical dates. */
export const SCHEDULE_RECENT_COMPLETE = "2026-07-03";

const F = MAP_MASTER_TODO;

/** Master to-do rows — explicit dates (overrides phase window logic). */
export const MASTER_TODO_SCHEDULE = {
  "mt-04": { start: "2026-07-07", end: "2026-07-11" },
  "mt-05": { start: "2026-07-07", end: "2026-07-11" },
  "mt-06": { start: "2026-07-07", end: "2026-07-11" },
  "mt-07": { start: "2026-07-02", end: "2026-07-11" },
  "mt-08": { start: "2026-07-02", end: "2026-07-18" },
  "mt-09": { start: "2026-07-03", end: "2026-07-11" },
  "mt-10": { start: "2026-07-14", end: "2026-07-22" },
  "mt-11": { start: "2026-07-14", end: "2026-07-22" },
  "mt-12": { start: "2026-07-28", end: "2026-08-08" },
  "mt-13": { start: "2026-08-10", end: "2026-08-21" },
  "mt-14": { start: "2026-07-07", end: "2026-07-14" },
  "mt-15": { start: "2026-07-14", end: "2026-07-18" },
  "mt-16": { start: "2026-07-21", end: "2026-08-07" },
  "mt-17": { start: "2026-08-10", end: "2026-08-21" },
  "mt-18": { start: "2026-08-10", end: "2026-08-28" },
  "mt-19": { start: "2026-07-21", end: "2026-07-28" },
  "mt-20": { start: "2026-07-14", end: "2026-07-18" },
  "mt-21": { start: "2026-07-21", end: "2026-07-28" },
  "mt-22": { start: "2026-07-28", end: "2026-08-14" },
  "mt-23": { start: "2026-07-07", end: "2026-07-18" },
  "mt-24": { start: "2026-07-07", end: "2026-07-11" },
  "mt-25": { start: "2026-10-01", end: "2026-12-31" },
  "mt-26": { start: "2026-07-15", end: "2026-07-18" },
  "mt-27": { start: "2026-07-16", end: "2026-07-31" },
  "mt-28": { start: "2026-08-03", end: "2026-08-08" },
  "mt-29": { start: "2026-08-10", end: "2026-08-22" },
  "mt-30": { start: "2026-08-24", end: "2026-09-12" },
  "mt-31": { start: "2026-07-28", end: "2026-08-08" },
};

/**
 * Phase windows — roadmap order; platform phases pulled forward from original 2027 plan.
 */
export const PHASE_SCHEDULE_WINDOWS = {
  "Strategy & Foundations": { start: "2026-08-04", end: "2026-11-28", concurrent: 2 },
  "Product Definition": { start: "2026-07-07", end: "2026-07-25", concurrent: 2 },
  "Strategy & Design": { start: "2026-08-04", end: "2026-10-31", concurrent: 1 },
  "Resources / Collateral": { start: "2026-07-07", end: "2026-08-14", concurrent: 1 },
  "GTM / Outreach": { start: "2026-07-07", end: "2026-08-21", concurrent: 2 },
  "Pilot Conversion": { start: "2026-07-07", end: "2026-08-28", concurrent: 2 },
  "Pilot Delivery": { start: "2026-07-14", end: "2026-09-18", concurrent: 2 },
  "Product / Access": { start: "2026-07-07", end: "2026-07-25", concurrent: 1 },
  "Platform Design": { start: "2026-07-14", end: "2026-10-31", concurrent: 2 },
  "Platform Build": { start: "2026-07-21", end: "2026-11-28", concurrent: 2 },
  "Content & GTM": { start: "2026-07-07", end: "2026-09-04", concurrent: 2 },
  "Testing & Pilot": { start: "2026-08-04", end: "2026-10-31", concurrent: 2 },
  "Launch & Operations": { start: "2026-10-01", end: "2027-01-30", concurrent: 2 },
  "Scale & Optimize": { start: "2027-02-01", end: "2027-08-31", concurrent: 2 },
  Later: { start: "2026-10-01", end: "2026-12-31", concurrent: 1 },
};

const SKIP_STATUSES = new Set(["Completed", "Done", "Not Needed"]);

const DURATION_BY_PRIORITY = {
  P0: 5,
  P1: 8,
  P2: 10,
  P3: 15,
};

function parsePriorityRank(priority) {
  const p = String(priority ?? "").trim();
  if (/^P0\b/.test(p)) return 0;
  if (/^P1\b/.test(p)) return 1;
  if (/^P2\b/.test(p)) return 2;
  if (/^P3\b/.test(p)) return 3;
  return 2;
}

function durationForPriority(priority) {
  const rank = parsePriorityRank(priority);
  return DURATION_BY_PRIORITY[`P${rank}`] ?? 10;
}

function toDate(iso) {
  return new Date(`${iso}T12:00:00`);
}

function formatDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function isWeekend(d) {
  const day = d.getDay();
  return day === 0 || day === 6;
}

export function addBusinessDays(iso, days) {
  const d = toDate(iso);
  let remaining = Math.max(0, days);
  while (remaining > 0) {
    d.setDate(d.getDate() + 1);
    if (!isWeekend(d)) remaining -= 1;
  }
  return formatDate(d);
}

function maxDate(a, b) {
  return toDate(a) >= toDate(b) ? a : b;
}

function minDate(a, b) {
  return toDate(a) <= toDate(b) ? a : b;
}

function isPhaseRollup(task) {
  return String(task || "").trim().startsWith("[Phase rollup]");
}

function isCompleted(fields) {
  return COMPLETED_STATUS_VALUES.has(String(fields?.[F.status] || "").trim().toLowerCase());
}

function statusWeight(status) {
  const s = String(status || "").trim();
  if (s === "In Progress") return 0;
  if (s === "Needs Review") return 1;
  if (s === "Waiting" || s === "Blocked") return 2;
  if (s === "Deferred") return 9;
  if (s === "Not Started") return 3;
  return 4;
}

function stepNumber(fields) {
  const n = Number(fields?.[F.stepNumber]);
  return Number.isFinite(n) ? n : 9999;
}

function isFounderPmoTracker(fields) {
  const task = String(fields?.[F.task] || "").toLowerCase();
  return /^track .+ phase completion/.test(task);
}

/**
 * Normalize Start / End / Completed Date for completed rows.
 */
export function normalizeCompletedTaskSchedule(fields) {
  if (!isCompleted(fields)) return null;

  const cd = fields[F.completedDate] || null;
  const start = fields[F.startDate] || null;
  const end = fields[F.dueDate] || null;
  const patch = {};

  if (end?.startsWith("2025") && start && start <= end) {
    if (cd === SCHEDULE_RECENT_COMPLETE || (cd && cd > end)) {
      patch.completedDate = end;
    }
    return Object.keys(patch).length ? { ...patch, source: "completed-historical" } : null;
  }

  const targetEnd = cd || end || SCHEDULE_RECENT_COMPLETE;
  if (!end || end !== targetEnd) patch.end = targetEnd;
  if (!cd || cd !== targetEnd) patch.completedDate = targetEnd;

  let targetStart = start;
  if (!targetStart || targetStart > targetEnd) {
    targetStart = addBusinessDays(targetEnd, -4);
  }
  if (toDate(targetStart) > toDate(targetEnd)) targetStart = targetEnd;
  if (targetStart !== start) patch.start = targetStart;

  if (Object.keys(patch).length === 0) return null;
  return { ...patch, source: "completed-align" };
}

function schedulePhaseTasks(tasks, window) {
  const sorted = [...tasks].sort((a, b) => {
    const stepA = stepNumber(a.fields);
    const stepB = stepNumber(b.fields);
    if (stepA !== stepB) return stepA - stepB;
    const pa = parsePriorityRank(a.fields?.[F.priority]);
    const pb = parsePriorityRank(b.fields?.[F.priority]);
    if (pa !== pb) return pa - pb;
    const sa = statusWeight(a.fields?.[F.status]);
    const sb = statusWeight(b.fields?.[F.status]);
    if (sa !== sb) return sa - sb;
    return String(a.fields?.[F.task] || "").localeCompare(String(b.fields?.[F.task] || ""));
  });

  const slots = Array.from({ length: window.concurrent }, () => window.start);
  const out = new Map();

  for (const rec of sorted) {
    const priority = rec.fields?.[F.priority];
    let duration = durationForPriority(priority);
    const status = rec.fields?.[F.status];
    if (status === "In Progress") duration = Math.max(5, Math.ceil(duration * 0.65));
    if (status === "Needs Review") duration = Math.max(3, Math.ceil(duration * 0.4));

    slots.sort((a, b) => toDate(a) - toDate(b));
    const existingStart = rec.fields?.[F.startDate];
    let start = maxDate(slots[0], window.start);
    if (status === "In Progress" && existingStart && existingStart < start) {
      const floor = addBusinessDays(window.start, -10);
      if (toDate(existingStart) >= toDate(floor)) start = existingStart;
    }

    let end = addBusinessDays(start, duration - 1);
    if (toDate(end) > toDate(window.end)) {
      end = window.end;
      start = addBusinessDays(end, -(duration - 1));
      start = maxDate(start, window.start);
    }
    if (toDate(start) > toDate(end)) {
      start = window.start;
      end = window.end;
    }
    slots[0] = addBusinessDays(end, 1);
    out.set(rec.id, { start, end });
  }

  return out;
}

/**
 * Build Start / End (and Completed Date fixes) for all FPP records.
 * @param {Array<{ id: string, fields: object }>} records
 */
export function computeFounderProjectPlanSchedule(records) {
  const schedule = new Map();
  const byPhase = new Map();

  for (const rec of records) {
    const fields = rec.fields || {};

    if (isCompleted(fields)) {
      const normalized = normalizeCompletedTaskSchedule(fields);
      if (normalized) schedule.set(rec.id, normalized);
      continue;
    }

    if (SKIP_STATUSES.has(fields[F.status])) continue;

    const seedId = parseSeedId(fields);
    if (seedId && MASTER_TODO_SCHEDULE[seedId]) {
      schedule.set(rec.id, { ...MASTER_TODO_SCHEDULE[seedId], source: "master-todo" });
      continue;
    }

    const phase = fields[F.phase] || "Later";
    if (isPhaseRollup(fields[F.task])) continue;

    if (fields[F.status] === "Deferred") {
      schedule.set(rec.id, {
        start: "2026-10-01",
        end: "2026-12-31",
        source: "deferred",
      });
      continue;
    }

    if (!byPhase.has(phase)) byPhase.set(phase, []);
    byPhase.get(phase).push(rec);
  }

  for (const [phase, tasks] of byPhase) {
    const window = PHASE_SCHEDULE_WINDOWS[phase] || {
      start: "2026-10-01",
      end: "2026-12-31",
      concurrent: 1,
    };
    const pmoTrackers = tasks.filter((r) => isFounderPmoTracker(r.fields));
    const regular = tasks.filter((r) => !isFounderPmoTracker(r.fields));
    const phaseSchedule = schedulePhaseTasks(regular, window);
    for (const [id, dates] of phaseSchedule) {
      schedule.set(id, { ...dates, source: `phase:${phase}` });
    }
    if (pmoTrackers.length) {
      const scheduled = [...phaseSchedule.values()];
      const start = scheduled.length
        ? scheduled.reduce((min, d) => minDate(d.start, min), scheduled[0].start)
        : window.start;
      const end = scheduled.length
        ? scheduled.reduce((max, d) => maxDate(d.end, max), scheduled[0].end)
        : window.end;
      for (const rec of pmoTrackers) {
        schedule.set(rec.id, { start, end, source: `phase-pmo:${phase}` });
      }
    }
  }

  for (const rec of records) {
    const fields = rec.fields || {};
    if (!isPhaseRollup(fields[F.task])) continue;
    if (isCompleted(fields)) continue;

    const phase = fields[F.phase];
    const siblings = records.filter(
      (r) =>
        r.fields?.[F.phase] === phase &&
        !isPhaseRollup(r.fields?.[F.task]) &&
        !isCompleted(r.fields) &&
        !SKIP_STATUSES.has(r.fields?.[F.status])
    );

    const dates = siblings.map((r) => schedule.get(r.id)).filter(Boolean);
    if (!dates.length) continue;

    const start = dates.reduce((min, d) => minDate(d.start, min), dates[0].start);
    const end = dates.reduce((max, d) => maxDate(d.end, max), dates[0].end);
    schedule.set(rec.id, { start, end, source: "phase-rollup" });
  }

  return schedule;
}

export function scheduleDiff(fields, proposed) {
  const before = {
    start: fields?.[F.startDate] || null,
    end: fields?.[F.dueDate] || null,
    completedDate: fields?.[F.completedDate] || null,
  };
  const after = {
    start: proposed.start ?? before.start,
    end: proposed.end ?? before.end,
    completedDate: proposed.completedDate ?? before.completedDate,
  };

  if (
    before.start === after.start &&
    before.end === after.end &&
    before.completedDate === after.completedDate
  ) {
    return null;
  }
  return { before, after };
}

export function getMasterEnrichmentDatePatch() {
  const patch = {};
  for (const [seedId, dates] of Object.entries(MASTER_TODO_SCHEDULE)) {
    if (MASTER_FIELD_ENRICHMENT[seedId]) {
      patch[seedId] = { start: dates.start, end: dates.end };
    }
  }
  return patch;
}
