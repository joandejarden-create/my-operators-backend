#!/usr/bin/env node
/**
 * Read-only FPP search for Bethesda Pilot 001 related tasks.
 * Does not write. Does not print secrets.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";

const token =
  process.env.AIRTABLE_GTM_API_KEY ||
  process.env.AIRTABLE_PAT ||
  process.env.AIRTABLE_TOKEN;
const base = process.env.AIRTABLE_GTM_BASE_ID || "appKZuK006BWIVjNW";
const table = "tblpCg0QZ0kIPXihE";
const outDir = path.resolve("reports/bethesda-pilot/day1/2026-10-01");
fs.mkdirSync(outDir, { recursive: true });

if (!token) {
  const payload = {
    FOUNDER_PROJECT_PLAN_LIVE_READ: "UNAVAILABLE",
    reason: "NO_GTM_TOKEN",
    searchedAt: new Date().toISOString(),
  };
  fs.writeFileSync(
    path.join(outDir, "fpp-live-read.json"),
    JSON.stringify(payload, null, 2)
  );
  console.log(JSON.stringify(payload, null, 2));
  process.exit(0);
}

const formula =
  "OR(" +
  "FIND('Bethesda', {Task})," +
  "FIND('Pilot 001', {Task})," +
  "FIND('Founding Hotel', {Task})," +
  "FIND('ADP + GDI', {Task})," +
  "FIND('GDI Founding', {Task})," +
  "FIND('Marriott Pilot', {Task})" +
  ")";

const url =
  `https://api.airtable.com/v0/${base}/${table}` +
  `?filterByFormula=${encodeURIComponent(formula)}` +
  `&maxRecords=100`;

const res = await fetch(url, {
  headers: { Authorization: `Bearer ${token}` },
});
const json = await res.json().catch(() => ({}));
const records = Array.isArray(json.records) ? json.records : [];

const mapped = records.map((r) => {
  const f = r.fields || {};
  return {
    id: r.id,
    task: f.Task || f.Name || null,
    status: f.Status || null,
    priority: f.Priority || null,
    phase: f.Phase || null,
    dueDate: f["Due Date"] || f.Due || null,
    dependency: f.Dependency || f.Dependencies || null,
    blocker: f.Blocker || f.Blockers || null,
    nextAction: f["Next Action"] || null,
    milestone: f.Milestone || null,
    deliverable: f.Deliverable || f.Deliverables || null,
    successMetric: f["Success Metric"] || null,
  };
});

const payload = {
  FOUNDER_PROJECT_PLAN_LIVE_READ: res.ok ? "AVAILABLE" : "UNAVAILABLE",
  httpStatus: res.status,
  count: mapped.length,
  records: mapped,
  searchedAt: new Date().toISOString(),
  formula,
};

fs.writeFileSync(
  path.join(outDir, "fpp-live-read.json"),
  JSON.stringify(payload, null, 2)
);
console.log(
  JSON.stringify(
    {
      FOUNDER_PROJECT_PLAN_LIVE_READ: payload.FOUNDER_PROJECT_PLAN_LIVE_READ,
      httpStatus: payload.httpStatus,
      count: payload.count,
      tasks: mapped.map((r) => ({
        id: r.id,
        task: r.task,
        status: r.status,
      })),
    },
    null,
    2
  )
);
