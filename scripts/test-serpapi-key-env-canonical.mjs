/**
 * Ensure SERPAPI_KEY is canonical (boolean check only — never print key).
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

test("client.js documents SERPAPI_KEY as canonical with SERPAPI_API_KEY fallback", () => {
  const src = fs.readFileSync(
    path.join(root, "lib/research-engine-v2/providers/serpapi-google-hotels/client.js"),
    "utf8"
  );
  assert.match(src, /SERPAPI_KEY/);
  assert.match(src, /SERPAPI_API_KEY/);
  assert.match(src, /process\.env\.SERPAPI_KEY \|\| process\.env\.SERPAPI_API_KEY/);
});

test(".env.example documents SERPAPI_KEY only as canonical", () => {
  const src = fs.readFileSync(path.join(root, ".env.example"), "utf8");
  assert.match(src, /# SERPAPI_KEY=/);
  assert.doesNotMatch(src, /SERPAPI_API_KEY=/);
});

test("v302 deep research accepts SERPAPI_KEY", () => {
  const src = fs.readFileSync(
    path.join(root, "lib/research-engine-v2/census-autopilot-v3/v302-deep-research.js"),
    "utf8"
  );
  assert.match(src, /SERPAPI_KEY \|\| process\.env\.SERPAPI_API_KEY/);
});
