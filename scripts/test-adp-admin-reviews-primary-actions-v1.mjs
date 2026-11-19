#!/usr/bin/env node
/**
 * Static contract: AI Demand Reviews primary icons are
 * View PDF → Download PDF → Copy URL → More.
 * Client view / action plan / regenerate / history stay in More.
 * View/Download PDF must not be duplicated in the overflow menu.
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

const viewIdx = body.indexOf('label: "View PDF"');
const downloadIdx = body.indexOf('label: "Download PDF"');
const copyIdx = body.indexOf('label: "Copy URL"');
assert.ok(viewIdx > -1, "View PDF missing");
assert.ok(downloadIdx > -1, "Download PDF missing");
assert.ok(copyIdx > -1, "Copy URL missing");
assert.ok(viewIdx < downloadIdx, "View PDF must precede Download PDF");
assert.ok(downloadIdx < copyIdx, "Download PDF must precede Copy URL");

assert.match(body, /act:\s*"view-pdf"/);
assert.match(body, /act:\s*"download-pdf"/);
assert.match(body, /act:\s*"copy-adp-client"/);
assert.match(body, /Open review in client view/);
assert.match(body, /act:\s*"open-adp-client"/);
assert.match(body, /Open action plan/);
assert.match(body, /Regenerate review/);
assert.match(body, /View report history/);
assert.match(body, /act:\s*"open-archive"/);

assert.ok(
  /primary\.push\([\s\S]*view-pdf/.test(body),
  "View PDF must be primary"
);
assert.ok(
  /primary\.push\([\s\S]*download-pdf/.test(body),
  "Download PDF must be primary"
);
assert.ok(
  /primary\.push\([\s\S]*copy-adp-client/.test(body),
  "Copy URL must be primary"
);

// Overflow must not re-add View/Download PDF.
const moreSection = body.slice(body.indexOf("// More:"));
assert.ok(
  !/act:\s*"view-pdf"/.test(moreSection),
  "View PDF must not be duplicated in More"
);
assert.ok(
  !/act:\s*"download-pdf"/.test(moreSection),
  "Download PDF must not be duplicated in More"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:adp-admin-reviews-primary-actions-v1",
      primary: ["View PDF", "Download PDF", "Copy URL"],
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
