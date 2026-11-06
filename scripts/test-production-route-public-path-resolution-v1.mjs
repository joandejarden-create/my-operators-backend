#!/usr/bin/env node
/**
 * Regression: production URL → public/ file resolution for route smoke.
 * Guards the AVA Admin false-positive where /app/admin/*.html maps to public/app/admin/.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePublicPathForRoute,
  routeHasPublicFile,
} from "../lib/production-deploy/route-public-path-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const adminUrl = "/app/admin/ai-demand-admin.html";
const resolved = resolvePublicPathForRoute(adminUrl, ROOT);
const expected = path.join(ROOT, "public", "app", "admin", "ai-demand-admin.html");

assert.equal(path.normalize(resolved), path.normalize(expected));
assert.equal(fs.existsSync(expected), true, "admin HTML must exist on disk");
assert.equal(routeHasPublicFile(adminUrl, ROOT), true);

assert.equal(
  path.normalize(resolvePublicPathForRoute("/js/admin-ai-visibility-audit.js", ROOT)),
  path.normalize(path.join(ROOT, "public", "js", "admin-ai-visibility-audit.js"))
);

const wrongLeaf = path.join(ROOT, "public", "ai-demand-admin.html");
assert.notEqual(path.normalize(resolved), path.normalize(wrongLeaf));

console.log(
  JSON.stringify(
    {
      ok: true,
      adminUrl,
      resolved,
      exists: true,
      contract: "PRODUCTION_ROUTE_PUBLIC_PATH_RESOLUTION_V1",
    },
    null,
    2
  )
);
