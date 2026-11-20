#!/usr/bin/env node
import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import {
  validateBrandPositioning,
  POSITIONING_SLASH_RE,
  normalizeBrandNameKey,
} from "../lib/brand-explorer/be-brand-positioning-canonical-v1.js";

const mapping = JSON.parse(
  readFileSync(join(process.cwd(), "fixtures/brand-positioning-master-mapping-v1.json"), "utf8")
);
let n = 0;
for (const rows of Object.values(mapping.batches)) {
  for (const row of rows) {
    n += 1;
    assert.ok(row.brand && row.positioning, "row incomplete");
    assert.ok(!POSITIONING_SLASH_RE.test(row.positioning), `slash in positioning: ${row.brand} → ${row.positioning}`);
    const v = validateBrandPositioning(row.positioning);
    assert.ok(v.ok, `${row.brand}: ${v.codes}`);
  }
}
assert.equal(n, 233, `expected 233 mappings, got ${n}`);
assert.ok(validateBrandPositioning("Upscale / Full-Service").ok === false);
assert.ok(validateBrandPositioning("Upscale Select Service").ok);
assert.equal(normalizeBrandNameKey("Le Méridien"), "le meridien");
console.log(`test-be-brand-positioning-canonical-v1: PASS (${n} mappings)`);
