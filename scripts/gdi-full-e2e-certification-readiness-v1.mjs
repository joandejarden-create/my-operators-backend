/**
 * Readiness check for the existing GDI Full E2E engine.
 * Confirms entry points and catalog coverage. Does not run research or score quality.
 */
import fs from "node:fs";
import { SUPERVISOR_EXPECTED_SEGMENTS } from "../lib/group-demand-intelligence/research/research-constants-v1.js";
import { FOURTEEN_WORKFLOWS } from "../lib/group-demand-intelligence/research/discovery-to-decision/pipeline-trace-v1.js";

const REPRESENTATIVE_HOTELS = [
  { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott" },
  { id: "recG66DQJKP2c0UNh", name: "Renaissance Times Square" },
  { id: "recnPB3Q02NO8Ma1o", name: "Wyndham San Diego Bayside" },
  { name: "Spice Island Beach Resort", note: "international portfolio property already in the published inventory" },
];

function assert(cond, message) {
  if (!cond) throw new Error(message);
}

const entryPoints = [
  "scripts/gdi-research-to-page-workflow-v1.mjs",
  "lib/group-demand-intelligence/research/research-supervisor-v2.js",
  "lib/group-demand-intelligence/workflow/gdi-three-hotel-full-e2e-v1.js",
  "lib/group-demand-intelligence/need-date-demand/need-period-hotel-store-v1.js",
];
for (const file of entryPoints) assert(fs.existsSync(file), `missing ${file}`);
assert(FOURTEEN_WORKFLOWS.length === 14, "14 workflows");
assert(SUPERVISOR_EXPECTED_SEGMENTS.length === 11, "11 supervisor segments");
const eventsFixture = fs.existsSync(
  "lib/group-demand-intelligence/research/exa-agent/fixtures/gdi-events-mock-provider-responses-v1.js"
);

console.log(JSON.stringify({
  ok: true,
  standard: "gdi-full-e2e-certification-standard-v1",
  executionPath: "PRESENT",
  researchQuality: "NOT_VERIFIED",
  customerDeliveryCertification: "NOT_VERIFIED",
  paidExecution: "NOT_RUN",
  workflows: FOURTEEN_WORKFLOWS.map((row) => row.id),
  segments: SUPERVISOR_EXPECTED_SEGMENTS.length,
  representativeHotels: REPRESENTATIVE_HOTELS.map((row) => row.name),
  eventsFixture: eventsFixture ? "PRESENT" : "MISSING",
}, null, 2));
