#!/usr/bin/env node
/**
 * Post-deploy production smoke against Railway URL.
 * Marks deploy FAILED if critical static surfaces are not HTTP 200.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(
  fs.readFileSync(path.join(root, "config", "production-required-assets.json"), "utf8")
);

const base =
  process.env.PRODUCTION_BASE_URL ||
  manifest.productionBaseUrl ||
  "https://my-operators-backend-production.up.railway.app";

const paths = [...new Set(manifest.postdeploy_smoke?.static_paths || [])];
const hotelIds = manifest.goldenFourHotelIds || [];
const q = manifest.postdeploy_smoke?.golden_four_query || "autopen=1&share=1";

async function head(url) {
  const res = await fetch(url, { method: "GET", redirect: "follow" });
  return { url, status: res.status };
}

const checks = [];
for (const p of paths) checks.push(await head(`${base}${p}`));
for (const id of hotelIds) {
  checks.push(
    await head(
      `${base}/hotel-intelligence-golden-demo.html?${q}&hotelExplorer=${id}&_cb=prodSmoke`
    )
  );
}

const failed = checks.filter((c) => c.status !== 200);
const report = {
  ok: failed.length === 0,
  base,
  generatedAt: new Date().toISOString(),
  checks,
  failed: failed.map((f) => f.url),
};

const outDir = path.join(root, "reports", "deployments");
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "latest-postdeploy-smoke.json"),
  JSON.stringify(report, null, 2)
);

if (failed.length) {
  console.error("FAIL PRODUCTION_POSTDEPLOY_SMOKE");
  for (const f of failed) console.error(`  ${f.status} ${f.url}`);
  process.exit(1);
}

console.log("PASS PRODUCTION_POSTDEPLOY_SMOKE");
for (const c of checks) console.log(`  ${c.status} ${c.url}`);
process.exit(0);
