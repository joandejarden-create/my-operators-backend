#!/usr/bin/env node
/**
 * SEC EDGAR cache → `.json.gz` migration utility.
 *
 * Default: dry-run (no writes, no deletes).
 * Never deletes uncompressed originals without:
 *   --apply --delete-original --authorize-delete AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE
 *
 * Does not call SEC / paid research APIs.
 *
 * Usage:
 *   node scripts/sec-edgar-cache-migrate-gzip-v1.mjs
 *   node scripts/sec-edgar-cache-migrate-gzip-v1.mjs --cache-dir data/sec-edgar/cache --limit 20
 *   node scripts/sec-edgar-cache-migrate-gzip-v1.mjs --apply --limit 1
 *   node scripts/sec-edgar-cache-migrate-gzip-v1.mjs --apply --delete-original --authorize-delete AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE
 */

import fs from "node:fs";
import path from "node:path";
import {
  AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE,
  createCacheMonitor,
  migrateJsonCacheFile,
  monitorSnapshot,
  scanCacheDirectory,
} from "../lib/hotel-intelligence/sec-edgar/sec-edgar-cache-store-v1.js";

function parseArgs(argv) {
  const out = {
    cacheDir: path.join(process.cwd(), "data", "sec-edgar", "cache"),
    dryRun: true,
    apply: false,
    deleteOriginal: false,
    authorizeDelete: "",
    limit: 0,
    reportPath: "",
    help: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--help" || a === "-h") out.help = true;
    else if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") {
      out.dryRun = true;
      out.apply = false;
    } else if (a === "--delete-original") out.deleteOriginal = true;
    else if (a === "--authorize-delete") out.authorizeDelete = String(argv[++i] || "");
    else if (a === "--cache-dir") out.cacheDir = path.resolve(String(argv[++i] || ""));
    else if (a === "--limit") out.limit = Math.max(0, Number(argv[++i] || 0) || 0);
    else if (a === "--report") out.reportPath = path.resolve(String(argv[++i] || ""));
  }
  return out;
}

function printHelp() {
  console.log(`SEC EDGAR cache gzip migration (dry-run default)

Options:
  --cache-dir <path>     Cache directory (default: data/sec-edgar/cache)
  --dry-run              Plan only (default)
  --apply                Write .json.gz after SHA-256 verify
  --delete-original      Delete .json after verified gzip (requires authorize token)
  --authorize-delete ${AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE}
  --limit <n>            Max files to process (0 = all)
  --report <path>        Write JSON report
  --help

Safety:
  - No paid SEC/research calls
  - Skips busy/open files
  - Aborts delete when .json and .json.gz content hashes differ for the same basename
  - Bulk production migration requires separate founder authorization beyond this tool's flags
`);
}

function listJsonCacheFiles(cacheDir) {
  if (!fs.existsSync(cacheDir)) return [];
  return fs
    .readdirSync(cacheDir)
    .filter((n) => n.toLowerCase().endsWith(".json") && !n.toLowerCase().endsWith(".json.gz"))
    .filter((n) => !n.includes(".tmp."))
    .map((n) => path.join(cacheDir, n))
    .sort();
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    printHelp();
    process.exit(0);
  }

  if (args.deleteOriginal && args.authorizeDelete !== AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE) {
    console.error(
      `[sec-edgar-cache-migrate] Refusing --delete-original without --authorize-delete ${AUTHORIZE_DELETE_UNCOMPRESSED_SEC_CACHE}`,
    );
    process.exit(2);
  }

  const scanBefore = scanCacheDirectory(args.cacheDir, { sampleLimit: 5 });
  const files = listJsonCacheFiles(args.cacheDir);
  const selected = args.limit > 0 ? files.slice(0, args.limit) : files;
  const monitor = createCacheMonitor();
  const results = [];

  console.log(
    `[sec-edgar-cache-migrate] mode=${args.dryRun ? "DRY_RUN" : "APPLY"} cacheDir=${args.cacheDir}`,
  );
  console.log(
    `[sec-edgar-cache-migrate] jsonFiles=${scanBefore.jsonFiles || 0} gzipFiles=${scanBefore.gzipFiles || 0} selected=${selected.length}`,
  );

  let okCount = 0;
  let failCount = 0;
  let skipCount = 0;
  let bytesIn = 0;
  let bytesOut = 0;

  for (const file of selected) {
    const r = migrateJsonCacheFile(file, {
      dryRun: args.dryRun,
      deleteOriginal: args.deleteOriginal,
      authorizeDelete: args.authorizeDelete,
      monitor,
    });
    results.push(r);
    if (r.skipped) skipCount += 1;
    else if (r.ok) {
      okCount += 1;
      bytesIn += Number(r.uncompressedBytes || 0);
      bytesOut += Number(r.compressedBytes || 0);
    } else failCount += 1;
  }

  const report = {
    version: "sec-edgar-cache-migrate-gzip-v1",
    generatedAt: new Date().toISOString(),
    args: {
      cacheDir: args.cacheDir,
      dryRun: args.dryRun,
      apply: args.apply,
      deleteOriginal: args.deleteOriginal,
      authorizeDeleteProvided: Boolean(args.authorizeDelete),
      limit: args.limit,
    },
    scanBefore,
    scanAfter: args.dryRun ? null : scanCacheDirectory(args.cacheDir, { sampleLimit: 3 }),
    summary: {
      selected: selected.length,
      ok: okCount,
      failed: failCount,
      skipped: skipCount,
      uncompressedBytes: bytesIn,
      compressedBytes: bytesOut,
      savings: bytesIn > 0 ? 1 - bytesOut / bytesIn : null,
    },
    monitor: monitorSnapshot(monitor),
    results,
    migrationPlan: {
      status: "IMPLEMENTATION_ONLY",
      bulkMigrationAuthorized: false,
      note:
        "Do not run bulk --apply --delete-original on production caches until implementation is certified and separately authorized.",
      recommendedSteps: [
        "1. Run dry-run across each workspace cache dir and review report savings/mismatches",
        "2. Certify reader/writer tests (npm run test:sec-edgar-cache-gzip-v1)",
        "3. Apply without delete on a small --limit sample; verify HIF/HE cache hits",
        "4. Obtain founder authorization for bulk apply",
        "5. Apply with --delete-original only after verified dual-read window",
      ],
    },
  };

  const outPath =
    args.reportPath ||
    path.join(
      process.cwd(),
      "reports",
      "sec-edgar-cache-gzip",
      `migrate-${args.dryRun ? "dry-run" : "apply"}-${Date.now()}.json`,
    );
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  console.log(
    `[sec-edgar-cache-migrate] ok=${okCount} failed=${failCount} skipped=${skipCount} savings=${
      report.summary.savings != null ? (report.summary.savings * 100).toFixed(2) + "%" : "n/a"
    }`,
  );
  console.log(`[sec-edgar-cache-migrate] report=${outPath}`);

  if (failCount > 0) process.exit(1);
}

main().catch((err) => {
  console.error("[sec-edgar-cache-migrate] fatal:", err);
  process.exit(1);
});
