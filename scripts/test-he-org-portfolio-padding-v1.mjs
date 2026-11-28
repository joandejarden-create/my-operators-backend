#!/usr/bin/env node
/**
 * Organization & Portfolio — section-padding content grid gates.
 * Sections keep .hiw-section 16px inset; tables full-bleed via negative margin.
 * Wired into Invoke-HotelExplorerFactory.ps1 ConsistencyModes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const hexCss = read("public/css/hotel-explorer.css");

// Must NOT zero section horizontal padding (root cause of flush Strategic Context).
const sectionBlock = hexCss.match(
  /\.hex-root \.hex-owner-portfolio > \.hiw-section \{[^}]*\}/
);
assert.ok(sectionBlock, "owner-portfolio section rule present");
assert.doesNotMatch(sectionBlock[0], /padding-left:\s*0/);
assert.doesNotMatch(sectionBlock[0], /padding-right:\s*0/);

// Base section still supplies 16px horizontal padding.
assert.match(
  hexCss,
  /\.hex-root \.hiw-section \{[\s\S]*?padding:\s*14px\s+16px\s+16px/
);

// Strategic Context uses flex + gap (not fragile direct-child re-padding).
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-strategic \{[\s\S]*?display:\s*flex;[\s\S]*?gap:\s*16px/
);

// Tables break out of section inset — no double padding on prose + table.
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-table-wrap,[\s\S]*?margin-left:\s*-16px;[\s\S]*?width:\s*calc\(100% \+ 32px\)/
);

assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hex-op-group__body \{[\s\S]*?padding-left:\s*0/
);

const hostBlock = hexCss.match(
  /\.hex-root \.hex-owner-portfolio \[data-hex-key-people-host\] \{[^}]*\}/
);
assert.ok(hostBlock, "key-people host rule present");
assert.match(hostBlock[0], /padding-left:\s*0/);
assert.doesNotMatch(hostBlock[0], /padding-left:\s*16px/);

const ps1 = read("scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1");
assert.match(ps1, /test:he-org-portfolio-padding-v1/);
assert.match(ps1, /test:he-org-portfolio-final-regression-v1/);

const pkg = read("package.json");
assert.match(pkg, /"test:he-org-portfolio-padding-v1"/);
assert.match(pkg, /"test:he-org-portfolio-final-regression-v1"/);

console.log("ok - he-org-portfolio-padding-v1");
