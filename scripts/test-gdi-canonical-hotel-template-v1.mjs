#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_CANONICAL_HOTEL_TEMPLATE_V1,
  getCanonicalHotelTemplateManifest,
} from "../lib/group-demand-intelligence/presentation/gdi-canonical-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const m = getCanonicalHotelTemplateManifest();
assert.equal(m.version, GDI_CANONICAL_HOTEL_TEMPLATE_V1);
assert.equal(m.hotelSpecificUiForbidden, true);
assert.equal(m.tabs.length, 3);
assert.ok(m.tabs.every((t) => t.id && t.label));

const freeze = m.freeze;
for (const rel of [freeze.htmlShell, freeze.css, freeze.ui, freeze.executiveSummary, freeze.listTable]) {
  assert.ok(fs.existsSync(path.join(ROOT, rel)), rel);
}
const css = fs.readFileSync(path.join(ROOT, freeze.css), "utf8");
for (const sel of freeze.requiredCssSelectors) {
  assert.ok(css.includes(sel.replace(/^\./, ".") || sel) || css.includes(sel.slice(1)), sel);
}
const jsBundle = [
  freeze.ui,
  freeze.executiveSummary,
  freeze.listTable,
]
  .map((p) => fs.readFileSync(path.join(ROOT, p), "utf8"))
  .join("\n");
for (const s of freeze.requiredJsSubstrings) {
  assert.ok(jsBundle.includes(s), s);
}
assert.ok(fs.existsSync(path.join(ROOT, "docs/ai-build-system/GDI_CANONICAL_HOTEL_TEMPLATE_V1.md")));
console.log("OK test:gdi-canonical-hotel-template-v1");
