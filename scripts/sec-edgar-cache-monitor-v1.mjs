#!/usr/bin/env node
/**
 * Read-only SEC EDGAR cache size / compression monitor.
 * Does not mutate cache files and does not call paid research APIs.
 *
 * Usage:
 *   node scripts/sec-edgar-cache-monitor-v1.mjs
 *   node scripts/sec-edgar-cache-monitor-v1.mjs --cache-dir data/sec-edgar/cache --sample-limit 10
 */

import fs from "node:fs";
import path from "node:path";
import { scanCacheDirectory } from "../lib/hotel-intelligence/sec-edgar/sec-edgar-cache-store-v1.js";

function parseArgs(argv) {
  const out = {
    cacheDirs: [],
    sampleLimit: 5,
    reportPath: "",
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--cache-dir") out.cacheDirs.push(path.resolve(String(argv[++i] || "")));
    else if (a === "--sample-limit") out.sampleLimit = Math.max(0, Number(argv[++i] || 0) || 0);
    else if (a === "--report") out.reportPath = path.resolve(String(argv[++i] || ""));
  }
  if (!out.cacheDirs.length) {
    out.cacheDirs = [
      path.join(process.cwd(), "data", "sec-edgar", "cache"),
      path.join(process.cwd(), "data", "sec-edgar", "sec-api-io-cache"),
      path.join(process.cwd(), "data", "sec-edgar", "exa-agent-cache"),
      path.join(process.cwd(), "data", "sec-edgar", "exa-contract-timing-cache"),
    ];
  }
  return out;
}

function gib(n) {
  return (Number(n || 0) / (1024 ** 3)).toFixed(3);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const scans = args.cacheDirs.map((cacheDir) =>
    scanCacheDirectory(cacheDir, { sampleLimit: args.sampleLimit, sampleCompress: true }),
  );

  const totals = scans.reduce(
    (acc, s) => {
      acc.jsonFiles += s.jsonFiles || 0;
      acc.gzipFiles += s.gzipFiles || 0;
      acc.jsonBytes += s.jsonBytes || 0;
      acc.gzipBytes += s.gzipBytes || 0;
      return acc;
    },
    { jsonFiles: 0, gzipFiles: 0, jsonBytes: 0, gzipBytes: 0 },
  );

  const report = {
    version: "sec-edgar-cache-monitor-v1",
    generatedAt: new Date().toISOString(),
    totals: {
      ...totals,
      totalFiles: totals.jsonFiles + totals.gzipFiles,
      totalGiB: Number(gib(totals.jsonBytes + totals.gzipBytes)),
      jsonGiB: Number(gib(totals.jsonBytes)),
      gzipGiB: Number(gib(totals.gzipBytes)),
    },
    directories: scans,
  };

  const outPath =
    args.reportPath ||
    path.join(process.cwd(), "reports", "sec-edgar-cache-gzip", `monitor-${Date.now()}.json`);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  console.log(`[sec-edgar-cache-monitor] files=${report.totals.totalFiles} json=${totals.jsonFiles} gzip=${totals.gzipFiles}`);
  console.log(
    `[sec-edgar-cache-monitor] size json=${gib(totals.jsonBytes)} GiB gzip=${gib(totals.gzipBytes)} GiB total=${gib(
      totals.jsonBytes + totals.gzipBytes,
    )} GiB`,
  );
  for (const s of scans) {
    if (!s.ok) {
      console.log(`  miss ${s.cacheDir}`);
      continue;
    }
    const avg =
      s.sampleAverageSavings != null ? `${(s.sampleAverageSavings * 100).toFixed(1)}%` : "n/a";
    console.log(
      `  ${s.cacheDir}: json=${s.jsonFiles} gzip=${s.gzipFiles} jsonGiB=${gib(s.jsonBytes)} sampleSavings=${avg}`,
    );
  }
  console.log(`[sec-edgar-cache-monitor] report=${outPath}`);
}

main().catch((err) => {
  console.error("[sec-edgar-cache-monitor] fatal:", err);
  process.exit(1);
});
