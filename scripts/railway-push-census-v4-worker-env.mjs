/**
 * Push Census V4 worker env to Railway without printing secret values.
 * Usage: node scripts/railway-push-census-v4-worker-env.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SERVICE = "Dealality Census Worker";

const REQUIRED_ENABLE = {
  ENABLE_CENSUS_V4_WORKER: "1",
  ENABLE_CENSUS_AUTOPILOT_V4: "1",
  ENABLE_VERIFIED_CENSUS_WRITES: "1",
  NODE_ENV: "production",
  // Railpack/Nixpacks start override — do not use web `node server.js`
  NIXPACKS_START_CMD: "npm run census:v4-full-build-worker",
};

const COPY_IF_PRESENT = [
  "AIRTABLE_API_KEY",
  "AIRTABLE_PAT",
  "AIRTABLE_BASE_ID",
  "AIRTABLE_BASE_ID_ALT",
  "AIRTABLE_API_KEY_READONLY",
  "SERPAPI_KEY",
  "SERPAPI_API_KEY",
  "MAPBOX_ACCESS_TOKEN",
  "DATAFORSEO_LOGIN",
  "DATAFORSEO_PASSWORD",
  "DATAFORSEO_ENABLED",
  "WEBHOUND_API_KEY",
  "WEBHOUND_API_TOKEN",
];

function parseEnvFile(fp) {
  const out = {};
  if (!fs.existsSync(fp)) return out;
  for (const line of fs.readFileSync(fp, "utf8").split(/\r?\n/)) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const i = t.indexOf("=");
    if (i < 1) continue;
    const k = t.slice(0, i).trim();
    let v = t.slice(i + 1).trim();
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    out[k] = v;
  }
  return out;
}

function setVar(key, value) {
  // PowerShell/cmd: quote service name; keep KEY=VALUE as one argv via nested quotes
  const cmd = `railway variable set ${JSON.stringify(`${key}=${value}`)} -s ${JSON.stringify(SERVICE)} --skip-deploys`;
  const r = spawnSync(cmd, {
    cwd: ROOT,
    encoding: "utf8",
    shell: true,
    windowsHide: true,
  });
  if (r.status !== 0) {
    const err = `${r.stderr || ""} ${r.stdout || ""}`.trim();
    console.error(`FAIL set ${key}: ${err.slice(0, 300)}`);
    return false;
  }
  console.log(`SET ${key} (len=${String(value).length})`);
  return true;
}

const local = {
  ...parseEnvFile(path.join(ROOT, ".env")),
  ...parseEnvFile(path.join(ROOT, ".env.local")),
};

let ok = 0;
let fail = 0;
for (const [k, v] of Object.entries(REQUIRED_ENABLE)) {
  if (setVar(k, v)) ok++;
  else fail++;
}

// Prefer PAT for Census scripts
if (local.AIRTABLE_PAT && !local.AIRTABLE_API_KEY) {
  local.AIRTABLE_API_KEY = local.AIRTABLE_PAT;
}

for (const k of COPY_IF_PRESENT) {
  const v = local[k];
  if (v == null || v === "") {
    console.log(`SKIP ${k} (not in local env)`);
    continue;
  }
  if (setVar(k, v)) ok++;
  else fail++;
}

// Mirror PAT into API_KEY if only PAT set on Railway path
if (local.AIRTABLE_PAT) {
  if (setVar("AIRTABLE_API_KEY", local.AIRTABLE_PAT)) ok++;
}

console.log(JSON.stringify({ service: SERVICE, ok, fail, secrets_printed: false }, null, 2));
process.exit(fail ? 1 : 0);
