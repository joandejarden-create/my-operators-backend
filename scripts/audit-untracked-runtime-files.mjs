#!/usr/bin/env node
/**
 * Fail if untracked (or unexpectedly ignored) runtime/source files exist under
 * protected Dealality application paths.
 *
 * CRITICAL UNTRACKED RUNTIME FILES — exit 1
 *
 * Protected roots:
 *   api/, lib/, public/js/, public/css/, public/app/, public/*.html (top-level),
 *   config/ (excl. client-share token registries), fixtures/ when present as untracked
 *
 * Intentionally-ignored WIP (Capital Explorer) is reported as WARNING / KNOWN_IGNORED_WIP
 * so a clean checkout risk stays visible without forcing those files into Git.
 */
import { execSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

const PROTECTED_PREFIXES = [
  "api/",
  "lib/",
  "public/js/",
  "public/css/",
  "public/app/",
  "public/marketing/",
  "public/archive/",
  "public/data/",
  "fixtures/",
  "middleware/",
];

/** Top-level public HTML that must not be untracked. */
const PROTECTED_PUBLIC_HTML = /^public\/[^/]+\.html$/;

/** Config source — not volatile token registries. */
const PROTECTED_CONFIG = /^config\/(?!client-share\/).+/;

/**
 * Local-only WIP explicitly gitignored. Still a clean-checkout risk — flagged,
 * but does not fail the audit until founder decides to track or remove.
 */
const KNOWN_IGNORED_WIP = [
  "public/capital-provider-explorer.html",
  "public/capital-provider-explorer-detail.html",
  "public/css/capital-provider-explorer.css",
  "public/js/capital-provider-explorer.js",
  "public/js/capital-provider-explorer-detail.js",
  "public/js/capital-explorer-favorites.js",
  "lib/capital-provider-explorer-seed-data.js",
  "lib/capital-provider-profile-presentation.js",
];

function git(args) {
  try {
    return execSync(`git ${args}`, {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  } catch (err) {
    const out = (err.stdout || "") + (err.stderr || "");
    return String(out).trim();
  }
}

function isProtected(rel) {
  const n = rel.replace(/\\/g, "/");
  if (PROTECTED_PREFIXES.some((p) => n.startsWith(p))) return true;
  if (PROTECTED_PUBLIC_HTML.test(n)) return true;
  if (PROTECTED_CONFIG.test(n)) return true;
  return false;
}

function listUntracked() {
  const raw = git("ls-files --others --exclude-standard");
  if (!raw) return [];
  return raw.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
}

function listIgnoredExisting() {
  const found = [];
  for (const rel of KNOWN_IGNORED_WIP) {
    const abs = path.join(root, rel);
    if (!fs.existsSync(abs)) continue;
    const ignoreInfo = git(`check-ignore -v -- ${JSON.stringify(rel).slice(1, -1)}`);
    const tracked = git(`ls-files -- ${JSON.stringify(rel).slice(1, -1)}`);
    found.push({
      path: rel,
      ignored: Boolean(ignoreInfo),
      ignoreRule: ignoreInfo || null,
      tracked: Boolean(tracked),
    });
  }
  return found;
}

const untracked = listUntracked();
const critical = untracked.filter(isProtected);
const ignoredWip = listIgnoredExisting().filter((x) => x.ignored && !x.tracked);

const report = {
  ok: critical.length === 0,
  criticalUntrackedCount: critical.length,
  criticalUntracked: critical,
  knownIgnoredWipPresent: ignoredWip,
  untrackedTotal: untracked.length,
  untrackedSample: untracked.slice(0, 50),
  protectedPrefixes: PROTECTED_PREFIXES,
  generatedAt: new Date().toISOString(),
};

const outDir = path.join(root, "reports");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "untracked-runtime-files-audit.json");
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

if (critical.length > 0) {
  console.error("CRITICAL UNTRACKED RUNTIME FILES");
  for (const p of critical) console.error(`  - ${p}`);
  console.error(`Report: ${path.relative(root, outPath)}`);
  process.exit(1);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      criticalUntrackedCount: 0,
      knownIgnoredWipCount: ignoredWip.length,
      knownIgnoredWipPresent: ignoredWip.map((x) => x.path),
      untrackedTotal: untracked.length,
      report: path.relative(root, outPath).replace(/\\/g, "/"),
    },
    null,
    2
  )
);

if (ignoredWip.length > 0) {
  console.log(
    "WARNING: intentionally gitignored local WIP runtime files exist (clean checkout will not include them):"
  );
  for (const x of ignoredWip) console.log(`  - ${x.path} (${x.ignoreRule})`);
}

process.exit(0);
