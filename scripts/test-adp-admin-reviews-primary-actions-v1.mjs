#!/usr/bin/env node
/**
 * Static contract: AI Demand Reviews compact actions expose Download PDF + Copy URL
 * as primary, with client-view / action plan / regenerate / history in More.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const src = readFileSync(
  join(root, "public/js/admin-ai-demand-reviews.js"),
  "utf8"
);

const fnMatch = src.match(
  /function compactCoverageActions\(r\) \{[\s\S]*?\n  \}/
);
assert.ok(fnMatch, "compactCoverageActions not found");
const body = fnMatch[0];

assert.match(body, /label:\s*"Download PDF"/);
assert.match(body, /act:\s*"download-pdf"/);
assert.match(body, /label:\s*"Copy URL"/);
assert.match(body, /act:\s*"copy-adp-client"/);
assert.match(body, /primary\.push\(\{[\s\S]*Download PDF/);
assert.match(body, /primary\.push\(\{[\s\S]*Copy URL/);
assert.match(body, /Open review in client view/);
assert.match(body, /act:\s*"open-adp-client"/);
assert.match(body, /Open action plan/);
assert.match(body, /Regenerate review/);
assert.match(body, /View report history/);
assert.match(body, /act:\s*"open-archive"/);

// Must not keep Download PDF / Copy URL only in More as sole access path.
assert.ok(
  /primary\.push\([\s\S]*download-pdf/.test(body),
  "Download PDF must be primary"
);
assert.ok(
  /primary\.push\([\s\S]*copy-adp-client/.test(body),
  "Copy URL must be primary"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-admin-reviews-primary-actions-v1",
      primary: ["Download PDF", "Copy URL"],
      more: [
        "Open review in client view",
        "Open action plan",
        "Regenerate review",
        "View report history",
      ],
    },
    null,
    2
  )
);
