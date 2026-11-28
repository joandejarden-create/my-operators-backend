#!/usr/bin/env node
/**
 * Regression: Sources & Evidence DATE/SUPPORTS must wrap; no nowrap pair on cols 5+6.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const css = fs.readFileSync(
  path.join(root, "public/css/hotel-explorer.css"),
  "utf8"
);

assert.match(css, /\.hex-root \.hiw-table--sources/, "sources table rules present");
assert.match(
  css,
  /td:nth-child\(6\)[\s\S]{0,220}white-space:\s*normal/,
  "SUPPORTS column allows wrapping"
);
assert.match(
  css,
  /td:nth-child\(5\)[\s\S]{0,280}white-space:\s*normal/,
  "DATE column allows wrapping"
);
assert.match(
  css,
  /td:last-child[\s\S]{0,180}white-space:\s*nowrap/,
  "LINK column stays nowrap for Open · Evidence"
);
assert.match(css, /min-width:\s*1180px/, "sources table has horizontal scroll floor");
assert.match(css, /overflow-wrap:\s*break-word/, "break-word enabled for long cells");

// Forbid the pre-DATE-consolidation pattern that forced nowrap on both date cols.
const forbidden =
  /td:nth-child\(5\),\s*\n\.hex-root \.hiw-table--sources th:nth-child\(5\),\s*\n\.hex-root \.hiw-table--sources td:nth-child\(6\),\s*\n\.hex-root \.hiw-table--sources th:nth-child\(6\) \{\s*\n\s*width:\s*9%\s*!important;\s*\n\s*white-space:\s*nowrap;/;
assert.doesNotMatch(
  css,
  forbidden,
  "must not force nowrap+9% on both Date and Supports"
);

const workspace = fs.readFileSync(
  path.join(root, "public/js/hotel-intelligence-workspace.js"),
  "utf8"
);
assert.match(workspace, /th\("Date"\)/);
assert.match(workspace, /resolveCustomerSupportsLabel/);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-he-sources-table-wrap-align-v1",
      note: "CSS gate for Sources DATE/SUPPORTS wrap + Link nowrap",
    },
    null,
    2
  )
);
console.log("OK — HE sources table wrap/align gate");
