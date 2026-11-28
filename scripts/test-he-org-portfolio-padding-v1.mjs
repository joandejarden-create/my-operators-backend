#!/usr/bin/env node
/**
 * Organization & Portfolio — consistent 16px content inset gates.
 * Wired into Invoke-HotelExplorerFactory.ps1 ConsistencyModes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

const hexCss = read("public/css/hotel-explorer.css");

assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio > \.hiw-section > \.hiw-section__h[\s\S]*?padding-left:\s*16px/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio > \.hiw-section > \.hiw-muted[\s\S]*?padding-left:\s*16px/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio > \.hiw-section > \.hex-facts[\s\S]*?padding-left:\s*16px/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-strategic \{[\s\S]*?padding-left:\s*16px;[\s\S]*?gap:\s*16px/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-section-footnote \{[\s\S]*?margin:\s*10px\s+16px\s+0/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hex-op-group__body \{[\s\S]*?padding-left:\s*0/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-snap-sub \{[\s\S]*?padding-left:\s*0/
);
assert.match(
  hexCss,
  /\.hex-root \.hex-owner-portfolio \.hiw-snap-sub__title[\s\S]*?padding-left:\s*16px/
);

// Tables / key-people host must stay at 0 horizontal padding (no double inset).
const hostBlock = hexCss.match(
  /\.hex-root \.hex-owner-portfolio \[data-hex-key-people-host\] \{[^}]*\}/
);
assert.ok(hostBlock, "key-people host rule present");
assert.match(hostBlock[0], /padding-left:\s*0/);
assert.doesNotMatch(hostBlock[0], /padding-left:\s*16px/);

const ps1 = read("scripts/hotel-explorer/Invoke-HotelExplorerFactory.ps1");
assert.match(ps1, /test:he-org-portfolio-padding-v1/);

const pkg = read("package.json");
assert.match(pkg, /"test:he-org-portfolio-padding-v1"/);

console.log("ok - he-org-portfolio-padding-v1");
