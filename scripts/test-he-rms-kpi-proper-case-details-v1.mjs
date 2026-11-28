#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rms = fs.readFileSync(
  path.join(root, "public/js/hotel-explorer-rooms-meeting-space.js"),
  "utf8"
);
const radar = fs.readFileSync(
  path.join(root, "public/deal-capture-radar-with-ranked-list.html"),
  "utf8"
);

const labels = [
  "Total Guestrooms",
  "Single-Bed Rooms",
  "Double-Bed Rooms",
  "Suites",
  "Total Meeting Space",
  "Meeting Rooms (Published)",
  "Largest Room",
  "Marketing Private-Space Figure",
];

for (const label of labels) {
  assert.ok(rms.includes(`"${label}"`), `missing Proper Case KPI label: ${label}`);
}

assert.ok(!rms.includes('"Total guestrooms"'));
assert.ok(!rms.includes('"Single-bed rooms"'));
assert.ok(!rms.includes('"Double-bed rooms"'));
assert.ok(!rms.includes('"Total meeting space"'));
assert.ok(!rms.includes('"Meeting rooms (published)"'));
assert.ok(!rms.includes('"Largest room"'));
assert.ok(!rms.includes('"Marketing private-space figure"'));

assert.ok(rms.includes("_hexRmsDetailBound"), "Details toggle must bind once");
assert.ok(rms.includes("data-hex-rms-toggle"), "Details expandable control retained");
assert.ok(rms.includes("hex-rms-row-detail"), "Details panel retained");
assert.ok(radar.includes("hotel-explorer-rooms-meeting-space.js?v=6"));

console.log("test-he-rms-kpi-proper-case-details-v1: PASS");
