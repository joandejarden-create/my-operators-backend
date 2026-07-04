/**
 * Quick smoke: app.js ROUTES → public file existence + optional HTTP HEAD.
 * Usage: node scripts/smoke-dealality-routes.mjs [--http BASE_URL]
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const httpBase = process.argv.includes("--http")
  ? process.argv[process.argv.indexOf("--http") + 1]
  : null;

const appJs = fs.readFileSync(path.join(root, "public/app.js"), "utf8");
const routeBlock = appJs.match(/var ROUTES = \{([\s\S]*?)\n    \};/);
if (!routeBlock) {
  console.error("Could not parse ROUTES from public/app.js");
  process.exit(1);
}

const routes = {};
for (const line of routeBlock[1].split("\n")) {
  const routeMatch = line.match(/^\s+'([^']+)':\s*\{([^}]*)\}/);
  if (routeMatch) {
    const route = routeMatch[1];
    const body = routeMatch[2];
    routes[route] = {};
    const fileMatch = body.match(/file:\s*'([^']+)'/);
    if (fileMatch) routes[route].file = fileMatch[1];
    if (/placeholder:\s*true/.test(body)) routes[route].placeholder = true;
  }
}

const missing = [];
const placeholders = [];
const ok = [];

for (const [route, meta] of Object.entries(routes)) {
  if (meta.placeholder) {
    placeholders.push(route);
    continue;
  }
  if (!meta.file) {
    missing.push({ route, reason: "no file mapping" });
    continue;
  }
  const publicPath = path.join(root, "public", meta.file.replace(/^\//, ""));
  if (!fs.existsSync(publicPath)) {
    missing.push({ route, file: meta.file, path: publicPath });
  } else {
    ok.push({ route, file: meta.file });
  }
}

console.log("=== ROUTE FILE EXISTENCE ===");
console.log("Total routes:", Object.keys(routes).length);
console.log("Mapped files OK:", ok.length);
console.log("Placeholders:", placeholders.join(", ") || "(none)");
console.log("MISSING:", missing.length);
for (const m of missing) {
  console.log("  FAIL", m.route, "->", m.file || m.reason);
}

const shellFiles = [
  "public/app.html",
  "public/app.js",
  "public/js/dealality-app-shell-auth.js",
  "public/js/dealality-memberstack-auth.js",
];
console.log("\n=== APP SHELL ===");
for (const f of shellFiles) {
  const exists = fs.existsSync(path.join(root, f));
  console.log(exists ? "OK" : "MISSING", f);
}

if (httpBase) {
  console.log("\n=== HTTP HEAD (" + httpBase + ") ===");
  let httpFail = 0;
  const head = async (url) => {
    const res = await fetch(url, { method: "HEAD", redirect: "follow" });
    return res;
  };
  const get = async (url) => {
    const res = await fetch(url, { method: "GET", redirect: "follow" });
    return res;
  };
  for (const { route, file } of ok) {
    const url = httpBase.replace(/\/$/, "") + file + "?embed=1";
    try {
      const res = await head(url);
      const tag = res.ok ? "OK" : "FAIL";
      if (!res.ok) httpFail++;
      console.log(`${tag} ${res.status} ${route} -> ${file}`);
    } catch (err) {
      httpFail++;
      console.log(`ERR  ${route} -> ${file}: ${err.message}`);
    }
  }
  const extras = [
    { path: "/app.html", method: "HEAD" },
    { path: "/api/marketing/demo-embeds", method: "HEAD" },
    { path: "/api/signup/config", method: "GET" },
    { path: "/api/dashboard/home", method: "GET" },
    { path: "/api/me", method: "GET" },
  ];
  for (const { path: p, method } of extras) {
    try {
      const res = method === "GET" ? await get(httpBase.replace(/\/$/, "") + p) : await head(httpBase.replace(/\/$/, "") + p);
      const tag = res.ok || (p === "/api/me" && res.status === 401) ? "OK" : "FAIL";
      if (tag === "FAIL") httpFail++;
      console.log(`${tag} ${res.status} ${p}`);
    } catch (err) {
      httpFail++;
      console.log(`ERR  ${p}: ${err.message}`);
    }
  }
  console.log(`HTTP failures: ${httpFail}`);
}

process.exit(missing.length ? 1 : 0);
