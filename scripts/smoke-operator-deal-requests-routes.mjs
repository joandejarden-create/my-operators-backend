#!/usr/bin/env node
/**
 * HTTP smoke test: Operator Deal Requests routes must exist and enforce auth.
 *
 * Usage:
 *   node scripts/smoke-operator-deal-requests-routes.mjs
 *   node scripts/smoke-operator-deal-requests-routes.mjs --base-url http://localhost:8080
 *
 * Optional JWT env vars for role checks (Memberstack eyJ… tokens):
 *   ODR_SMOKE_OPERATOR_JWT
 *   ODR_SMOKE_BRAND_JWT
 *   ODR_SMOKE_ADMIN_JWT
 */

import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function fail(msg) {
  console.error("FAIL:", msg);
  process.exitCode = 1;
}

function pass(msg) {
  console.log("PASS:", msg);
}

function warn(msg) {
  console.warn("SKIP:", msg);
}

function parseArgs(argv) {
  const out = { baseUrl: process.env.SMOKE_BASE_URL || "http://localhost:8080" };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === "--base-url" && argv[i + 1]) {
      out.baseUrl = argv[i + 1].replace(/\/$/, "");
      i++;
    }
  }
  return out;
}

async function request(baseUrl, path, token) {
  const headers = { Accept: "application/json" };
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(`${baseUrl}${path}`, { headers });
  let body = null;
  const text = await res.text();
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text.slice(0, 200) };
  }
  return { status: res.status, body };
}

function assertNotRouteMissing(label, result) {
  if (
    result.status === 404 &&
    result.body &&
    result.body.error === "API route not found"
  ) {
    fail(`${label}: got 404 API route not found — route not mounted on running server`);
    return false;
  }
  return true;
}

function checkServerSourceWiring() {
  const serverJs = readFileSync(join(root, "server.js"), "utf8");
  const serverUpload = readFileSync(join(root, "server.upload-ready.js"), "utf8");

  if (!serverJs.includes('import { getMe } from "./api/me.js"')) {
    fail("server.js missing import { getMe } from ./api/me.js (server will not boot)");
  } else {
    pass("server.js imports getMe");
  }

  const requiredRoutes = [
    'app.get("/api/operator-deal-requests/deal-meta"',
    'app.get("/api/operator-deal-requests/activity"',
    'app.get("/api/operator-deal-requests"',
    'app.get("/api/operator-deal-requests/:requestId"',
    'app.patch("/api/operator-deal-requests/:requestId"',
    'app.post("/api/operator-deal-requests/bulk-update"',
  ];

  for (const fileLabel of ["server.js", "server.upload-ready.js"]) {
    const src = fileLabel === "server.js" ? serverJs : serverUpload;
    for (const route of requiredRoutes) {
      if (!src.includes(route)) {
        fail(`${fileLabel} missing ${route}`);
      } else {
        const idx = src.indexOf(route);
        const dealMetaIdx = src.indexOf('app.get("/api/operator-deal-requests/deal-meta"');
        const paramIdx = src.indexOf('app.get("/api/operator-deal-requests/:requestId"');
        if (route.includes("deal-meta") && paramIdx !== -1 && dealMetaIdx > paramIdx) {
          fail(`${fileLabel}: deal-meta must register before :requestId`);
        }
        const slice = src.slice(Math.max(0, idx - 140), idx + route.length + 60);
        if (!slice.includes("operatorDealsAuth")) {
          fail(`${fileLabel}: ${route} must use operatorDealsAuth`);
        } else {
          pass(`${fileLabel}: ${route} wired with operatorDealsAuth`);
        }
      }
    }
  }

  if (
    !serverJs.includes("memberstackAuth") ||
    !serverJs.includes("requireDealalityUser") ||
    !serverJs.includes("requireOperatorDealsAccess")
  ) {
    fail("server.js operatorDealsAuth stack incomplete");
  } else {
    pass("server.js operatorDealsAuth uses memberstackAuth → requireDealalityUser → requireOperatorDealsAccess");
  }
}

async function runHttpSmoke(baseUrl) {
  console.log("\nHTTP smoke against", baseUrl);

  let unauthList;
  try {
    unauthList = await request(baseUrl, "/api/operator-deal-requests");
  } catch (err) {
    fail(`Could not reach ${baseUrl}: ${err.message}`);
    warn("Start the server: npm start (package.json → node server.js, default PORT 8080)");
    return;
  }

  if (!assertNotRouteMissing("GET /api/operator-deal-requests (unauthenticated)", unauthList)) {
    return;
  }
  if (unauthList.status === 401 || unauthList.status === 403) {
    pass(`unauthenticated list → ${unauthList.status} (route mounted)`);
  } else {
    fail(`unauthenticated list expected 401/403, got ${unauthList.status}`);
  }

  const unauthMeta = await request(
    baseUrl,
    "/api/operator-deal-requests/deal-meta?ids=recTEST0000000000"
  );
  if (!assertNotRouteMissing("GET /api/operator-deal-requests/deal-meta (unauthenticated)", unauthMeta)) {
    return;
  }
  if (unauthMeta.status === 401 || unauthMeta.status === 403) {
    pass(`unauthenticated deal-meta → ${unauthMeta.status} (route mounted)`);
  } else {
    fail(`unauthenticated deal-meta expected 401/403, got ${unauthMeta.status}`);
  }

  const roleChecks = [
    { label: "brand", env: "ODR_SMOKE_BRAND_JWT", expect: [403] },
    { label: "operator", env: "ODR_SMOKE_OPERATOR_JWT", expect: [200] },
    { label: "admin", env: "ODR_SMOKE_ADMIN_JWT", expect: [200] },
  ];

  for (const role of roleChecks) {
    const token = process.env[role.env];
    if (!token) {
      warn(`${role.label}: set ${role.env} to run authenticated check`);
      continue;
    }
    const list = await request(baseUrl, "/api/operator-deal-requests", token);
    if (!assertNotRouteMissing(`GET list (${role.label})`, list)) continue;
    if (role.expect.includes(list.status)) {
      pass(`${role.label} list → ${list.status}`);
    } else {
      fail(`${role.label} list expected ${role.expect.join("|")}, got ${list.status}`);
    }

    if (role.label === "operator" || role.label === "admin") {
      const meta = await request(
        baseUrl,
        "/api/operator-deal-requests/deal-meta?ids=recTEST0000000000",
        token
      );
      if (!assertNotRouteMissing(`GET deal-meta (${role.label})`, meta)) continue;
      if (meta.status === 200) {
        pass(`${role.label} deal-meta → 200`);
      } else if (meta.status === 403) {
        fail(`${role.label} deal-meta unexpected 403`);
      } else {
        fail(`${role.label} deal-meta expected 200, got ${meta.status}`);
      }
    }
  }
}

checkServerSourceWiring();
const { baseUrl } = parseArgs(process.argv);
await runHttpSmoke(baseUrl);

if (!process.exitCode) {
  console.log("\nOperator deal requests route smoke passed.");
}
