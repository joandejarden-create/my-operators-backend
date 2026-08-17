/**
 * Build a lean Railway upload bundle for Dealality Census Worker.
 * Traces static ESM imports from supervisor/controller entrypoints.
 * Runtime-only; does not change Census business logic.
 */
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DST = path.join(
  ROOT,
  process.env.CENSUS_V4_DEPLOY_DIR || "deploy-census-v4-worker"
);
const ENTRIES = [
  "scripts/v4-full-build-supervisor.mjs",
  "scripts/v4-full-build-controller.mjs",
];

const IMPORT_RE =
  /(?:from\s+|import\s*\(\s*|export\s+\*\s+from\s+)['"](\.\.?\/[^'"]+)['"]/g;

function resolveModule(fromFile, spec) {
  const base = path.resolve(path.dirname(fromFile), spec);
  const candidates = [
    base,
    `${base}.js`,
    `${base}.mjs`,
    `${base}.json`,
    `${base}.cjs`,
    path.join(base, "index.js"),
    path.join(base, "index.mjs"),
    path.join(base, "index.json"),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c) && fs.statSync(c).isFile()) return c;
  }
  return null;
}

function trace() {
  const seen = new Set();
  const missing = [];
  const queue = ENTRIES.map((e) => path.join(ROOT, e));
  while (queue.length) {
    const abs = queue.shift();
    if (seen.has(abs)) continue;
    seen.add(abs);
    if (!fs.existsSync(abs)) {
      missing.push(path.relative(ROOT, abs));
      continue;
    }
    if (!/\.(mjs|js|cjs)$/i.test(abs)) continue;
    const txt = fs.readFileSync(abs, "utf8");
    let m;
    IMPORT_RE.lastIndex = 0;
    while ((m = IMPORT_RE.exec(txt))) {
      const resolved = resolveModule(abs, m[1]);
      if (!resolved) {
        missing.push(`${path.relative(ROOT, abs)} -> ${m[1]}`);
        continue;
      }
      queue.push(resolved);
    }
  }
  return { files: [...seen], missing };
}

function rmrf(p) {
  if (fs.existsSync(p)) fs.rmSync(p, { recursive: true, force: true });
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function copyFileRel(rel) {
  const src = path.join(ROOT, rel);
  const dst = path.join(DST, rel);
  ensureDir(path.dirname(dst));
  fs.copyFileSync(src, dst);
}

function copyTreeFiltered(srcRel, predicate) {
  const srcRoot = path.join(ROOT, srcRel);
  if (!fs.existsSync(srcRoot)) return 0;
  let n = 0;
  const walk = (dir) => {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const abs = path.join(dir, ent.name);
      if (ent.isDirectory()) walk(abs);
      else if (predicate(abs)) {
        const rel = path.relative(ROOT, abs);
        copyFileRel(rel);
        n += 1;
      }
    }
  };
  walk(srcRoot);
  return n;
}

function main() {
  const { files, missing } = trace();
  if (missing.length) {
    console.error("UNRESOLVED_IMPORTS", missing.slice(0, 40));
    process.exitCode = 2;
  }

  rmrf(DST);
  ensureDir(DST);

  // Core package + railway config
  for (const f of [
    "package.json",
    "package-lock.json",
    "railway.census-v4-worker.toml",
  ]) {
    if (fs.existsSync(path.join(ROOT, f))) copyFileRel(f);
  }
  fs.copyFileSync(
    path.join(ROOT, "railway.census-v4-worker.toml"),
    path.join(DST, "railway.toml")
  );
  fs.writeFileSync(
    path.join(DST, ".railwayignore"),
    ["node_modules/", ".git/", "*.log", ".env", ".env.*"].join("\n") + "\n"
  );

  // Traced code
  for (const abs of files) {
    if (!fs.existsSync(abs)) continue;
    copyFileRel(path.relative(ROOT, abs));
  }

  // Sidecar JSON loaded via readFileSync (not visible to ESM import tracer).
  const apiLib = path.join(ROOT, "api/lib");
  if (fs.existsSync(apiLib)) {
    for (const ent of fs.readdirSync(apiLib)) {
      if (!ent.endsWith(".json")) continue;
      copyFileRel(path.join("api/lib", ent));
    }
  }

  // Seed outside the Railway volume mount path so first boot can hydrate the volume.
  // Volume mount: /app/data/research-engine-v2/census-autopilot-v4-full-universe
  const dataRoot = "data/research-engine-v2/census-autopilot-v4-full-universe";
  const seedRoot = path.join(DST, "seed-census-v4-full-universe");
  const seedNames = new Set([
    "24-full-build-status.json",
    "40-actionable-work-function.json",
    "43-controller-checkpoint-state.json",
    "46-post-fix-universe-ledger.json",
    "48-full-build-controller-status.json",
    "50-auto-resume-ticket.json",
    "34b-controller-discovery-cache-meta.json",
  ]);
  ensureDir(seedRoot);
  const srcData = path.join(ROOT, dataRoot);
  const copySeed = (fromAbs, toAbs) => {
    ensureDir(path.dirname(toAbs));
    fs.copyFileSync(fromAbs, toAbs);
  };
  for (const name of seedNames) {
    const from = path.join(srcData, name);
    if (fs.existsSync(from)) copySeed(from, path.join(seedRoot, name));
  }
  const ledgerSrc = path.join(srcData, "27-universe-ledger");
  if (fs.existsSync(ledgerSrc)) {
    // Compress ledger to keep railway up under upload timeout.
    const tgz = path.join(seedRoot, "27-universe-ledger.tgz");
    execFileSync("tar", ["-czf", tgz, "-C", srcData, "27-universe-ledger"], {
      stdio: "inherit",
    });
  }

  const freeze =
    "data/research-engine-v2/census-autopilot-v2-3-independent-universe/08-independent-universe-freeze.json";
  if (fs.existsSync(path.join(ROOT, freeze))) copyFileRel(freeze);

  // Official-directory discovery needs Active/Live baseline + parent extracts.
  // Railpack often drops top-level reports/; ship under lib/ (always in image).
  const assetsDir = path.join(DST, "lib/census-v4-worker-assets");
  ensureDir(assetsDir);
  const reportFiles = [
    "reports/brand-explorer-62-active-public-full-baseline.json",
    "reports/ihg-cala-directory-extract.json",
    "reports/cala-tribute-property-visual-discovery.json",
  ];
  for (const rel of reportFiles) {
    const abs = path.join(ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    fs.copyFileSync(abs, path.join(assetsDir, path.basename(rel)));
  }
  // Also mirror to reports/ for local parity.
  const reportsDir = path.join(DST, "reports");
  ensureDir(reportsDir);
  for (const rel of reportFiles) {
    const abs = path.join(ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    fs.copyFileSync(abs, path.join(reportsDir, path.basename(rel)));
  }
  // Large Choice extract: compressed under lib assets; supervisor expands on boot.
  const choice =
    "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";
  if (fs.existsSync(path.join(ROOT, choice))) {
    execFileSync(
      "tar",
      [
        "-czf",
        path.join(assetsDir, "choice-directory-extract.tgz"),
        "-C",
        path.join(ROOT, "reports"),
        "independent-census-choice-property-url-extract-cala-2026-05-20.json",
      ],
      { stdio: "inherit" }
    );
  }

  // Size report
  let bytes = 0;
  const walk = (d) => {
    for (const ent of fs.readdirSync(d, { withFileTypes: true })) {
      const abs = path.join(d, ent.name);
      if (ent.isDirectory()) walk(abs);
      else bytes += fs.statSync(abs).size;
    }
  };
  walk(DST);

  const report = {
    traced_files: files.length,
    unresolved: missing.length,
    unresolved_samples: missing.slice(0, 20),
    bundle_mb: Math.round((bytes / (1024 * 1024)) * 10) / 10,
    dst: DST,
  };
  fs.writeFileSync(
    path.join(ROOT, "data/research-engine-v2/census-autopilot-v4-full-universe/63-railway-deploy-bundle.json"),
    JSON.stringify(report, null, 2)
  );
  console.log(JSON.stringify(report, null, 2));
}

main();
